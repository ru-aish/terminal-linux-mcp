#!/usr/bin/env node

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { resolve } from "node:path";
import { createInterface } from "node:readline";
import { solveTurnstile } from "./chat_direct_sentinel.mjs";

const BASE_URL = process.env.MCP_CHAT_DIRECT_BASE_URL || "https://chatgpt.com/backend-api";
const USER_AGENT = process.env.MCP_CHAT_DIRECT_USER_AGENT ||
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ChatGPT/1.2026.224 Chrome/140.0.7339.214 Electron/38.2.1 Safari/537.36";
const AUTH_PATH = resolve(process.env.MCP_CHAT_DIRECT_AUTH_FILE || `${homedir()}/.codex/auth.json`);
const activeStreams = new Map();
let cachedAuth = null;
let authMtime = -1;
let refreshPromise = null;
let writeLane = Promise.resolve();

function installBrowserShims() {
  Object.defineProperty(globalThis, "window", { value: globalThis, configurable: true });
  Object.defineProperty(globalThis, "navigator", { value: {
    userAgent: USER_AGENT, language: "en-US", languages: ["en-US", "en"],
    hardwareConcurrency: 8, deviceMemory: 8, vendor: "Google Inc.",
    platform: "Linux x86_64", maxTouchPoints: 0,
    storage: { estimate: async () => ({ quota: 1_073_741_824, usage: 0 }) },
  }, configurable: true });
  const storage = new Map();
  Object.defineProperty(globalThis, "localStorage", { value: {
    getItem: key => storage.get(String(key)) ?? null,
    setItem: (key, value) => storage.set(String(key), String(value)),
    removeItem: key => storage.delete(String(key)), clear: () => storage.clear(),
  }, configurable: true });
  Object.defineProperty(globalThis, "history", { value: { length: 1 }, configurable: true });
  const info = { UNMASKED_VENDOR_WEBGL: 37445, UNMASKED_RENDERER_WEBGL: 37446 };
  const gl = { getExtension: name => name === "WEBGL_debug_renderer_info" ? info : null,
    getParameter: value => value === info.UNMASKED_VENDOR_WEBGL ? "Google Inc. (NVIDIA)" : value === info.UNMASKED_RENDERER_WEBGL ? "ANGLE (NVIDIA, Vulkan)" : null };
  class ElementShim {
    constructor(tag) { this.tagName = String(tag).toUpperCase(); this.style = {}; this.children = []; this.attributes = {}; }
    setAttribute(k,v) { this.attributes[k] = String(v); } getAttribute(k) { return this.attributes[k] ?? null; }
    appendChild(v) { this.children.push(v); return v; } removeChild(v) { this.children = this.children.filter(x => x !== v); return v; }
    getContext(kind) { return /webgl/i.test(String(kind)) ? gl : null; }
    getBoundingClientRect() { return {x:0,y:0,width:0,height:0,top:0,right:0,bottom:0,left:0}; }
  }
  const page = new URL("https://chatgpt.com/");
  Object.defineProperty(globalThis, "screen", { value: {width:1920,height:1080,availWidth:1920,availHeight:1040,availLeft:0,availTop:0,colorDepth:24,pixelDepth:24}, configurable:true });
  Object.defineProperty(globalThis, "document", { value: {scripts:[],documentElement:new ElementShim("html"),body:new ElementShim("body"),location:page,createElement:tag=>new ElementShim(tag)}, configurable:true });
  Object.defineProperty(globalThis, "location", { value: page, configurable:true });
}

function randomItem(values) { return values[Math.floor(Math.random() * values.length)]; }
function navigatorProbe() { const key = randomItem(Object.keys(Object.getPrototypeOf(navigator))); try { return `${key}−${String(navigator[key])}`; } catch { return key; } }
function encodeJson(value) { return btoa(String.fromCharCode(...new TextEncoder().encode(JSON.stringify(value)))); }
function fingerprint() {
  const memory = performance.memory;
  return [screen.width+screen.height,String(new Date()),memory?.jsHeapSizeLimit??null,Math.random(),navigator.userAgent,
    randomItem(Array.from(document.scripts).map(x=>x?.src).filter(Boolean)),
    (Array.from(document.scripts).map(x=>x?.src?.match("c/[^/]*/_")).filter(x=>x?.length)[0]??[])[0]??document.documentElement.getAttribute("data-build"),
    navigator.language,navigator.languages?.join(","),Math.random(),navigatorProbe(),randomItem(Object.keys(document)),randomItem(Object.keys(window)),performance.now(),randomUUID(),
    [...new URLSearchParams(window.location.search).keys()].join(","),navigator.hardwareConcurrency,performance.timeOrigin,
    Number("ai" in window),Number("createPRNG" in window),Number("cache" in window),Number("data" in window),Number("solana" in window),Number("dump" in window),Number("InstallTrigger" in window)];
}
function sentinelHash(value) { let hash=2166136261; for(let i=0;i<value.length;i++){hash^=value.charCodeAt(i);hash=Math.imul(hash,16777619)>>>0;} hash^=hash>>>16;hash=Math.imul(hash,2246822507)>>>0;hash^=hash>>>13;hash=Math.imul(hash,3266489909)>>>0;hash^=hash>>>16;return(hash>>>0).toString(16).padStart(8,"0"); }
function makeRequirementsKey() { const start=performance.now(), value=fingerprint(); value[3]=1;value[9]=performance.now()-start;return `gAAAAAC${encodeJson(value)}`; }
function solveProofOfWork(seed,difficulty) { const start=performance.now(),value=fingerprint();for(let i=0;i<500000;i++){value[3]=i;value[9]=Math.round(performance.now()-start);const candidate=encodeJson(value);if(sentinelHash(`${seed}${candidate}`).substring(0,difficulty.length)<=difficulty)return `gAAAAAB${candidate}~S`;}throw new Error("proof-of-work search exhausted"); }

function jwtClaims(token) { const part=String(token).split(".")[1]; if(!part) return {}; return JSON.parse(Buffer.from(part,"base64url").toString("utf8")); }
function accountId(token, stored) { const c=jwtClaims(token); return stored || c["https://api.openai.com/auth"]?.chatgpt_account_id || c["https://api.openai.com/auth.chatgpt_account_id"]; }
function tokenFresh(token) { const exp=Number(jwtClaims(token).exp || 0); return exp*1000 > Date.now()+300000; }

async function readStoredAuth({ allowStale=false }={}) {
  const metadata = await stat(AUTH_PATH);
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) throw new Error("Codex auth file is not owned by this user");
  if ((metadata.mode & 0o077) !== 0) throw new Error("Codex auth file permissions are too broad");
  if (cachedAuth && metadata.mtimeMs === authMtime && (allowStale || tokenFresh(cachedAuth.token))) return cachedAuth;
  const payload = JSON.parse(await readFile(AUTH_PATH,"utf8"));
  const token = payload?.tokens?.access_token;
  if (!token) throw new Error("Codex auth file has no access token");
  cachedAuth = { token, accountId: accountId(token,payload?.tokens?.account_id) };
  authMtime = metadata.mtimeMs;
  if (!cachedAuth.accountId) throw new Error("ChatGPT account id is missing from Codex auth");
  if (!allowStale && !tokenFresh(token)) return refreshAuth();
  return cachedAuth;
}

async function refreshAuth() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = new Promise((resolvePromise,rejectPromise) => {
    const child=spawn(process.env.MCP_CHAT_DIRECT_CODEX || "codex",["app-server"],{stdio:["pipe","pipe","ignore"]});
    let buffer="", done=false;
    const finish=(error,value)=>{if(done)return;done=true;clearTimeout(timer);child.kill();error?rejectPromise(error):resolvePromise(value);};
    const timer=setTimeout(()=>finish(new Error("Codex auth refresh timed out")),15000);
    child.on("error",finish);
    child.stdout.setEncoding("utf8");
    child.stdout.on("data",chunk=>{buffer+=chunk;for(;;){const at=buffer.indexOf("\n");if(at<0)break;const line=buffer.slice(0,at);buffer=buffer.slice(at+1);try{const msg=JSON.parse(line);if(msg.id===2){const token=msg.result?.authToken;if(!token)return finish(new Error("Codex auth refresh returned no token"));cachedAuth={token,accountId:accountId(token,msg.result?.accountId)};authMtime=-1;finish(null,cachedAuth);}}catch{}}});
    child.stdin.write(`${JSON.stringify({id:1,method:"initialize",params:{clientInfo:{name:"terminal-mcp-direct",version:"1"},capabilities:{}}})}\n`);
    child.stdin.write(`${JSON.stringify({id:2,method:"getAuthStatus",params:{includeToken:true,refreshToken:true}})}\n`);
  }).finally(()=>{refreshPromise=null;});
  return refreshPromise;
}

function headersFor(auth) { return {authorization:`Bearer ${auth.token}`,"chatgpt-account-id":auth.accountId,originator:"Codex Desktop","user-agent":USER_AGENT}; }
const delay = ms => new Promise(resolvePromise => setTimeout(resolvePromise,ms));
async function backendFetch(path, options={}, {read=false, refresh=true}={}) {
  let auth=await readStoredAuth();
  const attempts=read?5:1;
  for(let attempt=0;attempt<attempts;attempt++){
    try{
      const response=await fetch(`${BASE_URL}${path}`,{...options,headers:{...headersFor(auth),...(options.headers||{})},signal:options.signal||AbortSignal.timeout(45000)});
      if(response.status===401 && refresh){auth=await refreshAuth();return backendFetch(path,options,{read,refresh:false});}
      if(read && [408,425,429,500,502,503,504].includes(response.status) && attempt+1<attempts){await delay(Math.min(8000,300*2**attempt+Math.random()*300));continue;}
      return response;
    }catch(error){if(!read || attempt+1>=attempts)throw error;await delay(Math.min(8000,300*2**attempt+Math.random()*300));}
  }
}
async function jsonRequest(path,options={},flags={read:true}) { const response=await backendFetch(path,options,flags);if(!response.ok){const error=new Error(`ChatGPT backend ${response.status}`);error.status=response.status;throw error;}return response.json(); }

async function prepareIntegrity() {
  const key=makeRequirementsKey();
  const requirements=await jsonRequest("/sentinel/chat-requirements/prepare",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({p:key})},{read:true});
  const proof=requirements.proofofwork?.required?solveProofOfWork(requirements.proofofwork.seed,requirements.proofofwork.difficulty):null;
  const turnstile=requirements.turnstile?.required?await solveTurnstile(requirements.turnstile.dx,key):null;
  return {...(requirements.token?{"OpenAI-Sentinel-Chat-Requirements-Token":requirements.token}:{"OpenAI-Sentinel-Chat-Requirements-Prepare-Token":requirements.prepare_token}),...(proof?{"OpenAI-Sentinel-Proof-Token":proof}:{}),...(turnstile?{"OpenAI-Sentinel-Turnstile-Token":turnstile}:{})};
}
function userMessage(text,id) { return {id,author:{role:"user"},content:{content_type:"text",parts:[text]},create_time:Date.now()/1000,end_turn:null,metadata:{},recipient:"all",status:"finished_successfully",weight:1}; }
function findConversationId(value) { const queue=[value];while(queue.length){const current=queue.pop();if(!current||typeof current!=="object")continue;const id=current.conversation_id??current.conversationId;if(typeof id==="string"&&id)return id;queue.push(...Object.values(current));}return null; }

async function startCompletion(params,continuation={}) {
  const controller=new AbortController();
  const integrity=await prepareIntegrity();
  const body={action:"next",model:params.preferred_model||"gpt-5-6-thinking",messages:[userMessage(params.prompt??params.message,params.user_message_id)],supported_encodings:["v1"],timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,timezone_offset_min:new Date().getTimezoneOffset(),...continuation};
  if(params.thinking_effort)body.thinking_effort=params.thinking_effort;
  if(params.project_id){body.gizmo_id=params.project_id;body.conversation_mode={kind:"gizmo_interaction",gizmo_id:params.project_id};}
  const response=await backendFetch("/f/conversation",{method:"POST",headers:{...integrity,"content-type":"application/json",accept:"text/event-stream"},body:JSON.stringify(body),signal:controller.signal},{read:false});
  if(!response.ok)throw new Error(`ChatGPT completion submission failed: ${response.status}`);
  let observedId=continuation.conversation_id||null, resolveObserved, rejectObserved;
  const observed=new Promise((resolvePromise,rejectPromise)=>{resolveObserved=resolvePromise;rejectObserved=rejectPromise;});
  if(observedId)resolveObserved(observedId);
  const drain=(async()=>{let buffer="";const decoder=new TextDecoder();try{for await(const chunk of response.body){buffer+=decoder.decode(chunk,{stream:true});for(;;){const at=buffer.indexOf("\n");if(at<0)break;const line=buffer.slice(0,at).trimEnd();buffer=buffer.slice(at+1);if(!line.startsWith("data: ")||line==="data: [DONE]")continue;try{observedId??=findConversationId(JSON.parse(line.slice(6)));if(observedId){activeStreams.set(observedId,{controller,done:drain});resolveObserved(observedId);}}catch{}}}if(!observedId)rejectObserved(new Error("completion stream did not expose a conversation id"));}catch(error){rejectObserved(error);throw error;}finally{if(observedId)activeStreams.delete(observedId);}})();
  drain.catch(()=>{});
  const id=await Promise.race([observed,new Promise((_,rejectPromise)=>setTimeout(()=>rejectPromise(new Error("conversation id was not observed before timeout")),30000))]);
  if(params.wait_for_completion)await drain;
  let after=null;
  for(let attempt=0;attempt<3&&!after;attempt++){
    try{after=await jsonRequest(`/conversation/${encodeURIComponent(id)}`);}catch(error){if(attempt===2)break;await delay(250*2**attempt);}
  }
  const messages=after?.mapping&&typeof after.mapping==="object"?Object.values(after.mapping).map(node=>node?.message).filter(Boolean):[];
  const persisted=messages.some(message=>String(message?.id||"")===params.user_message_id);
  return {accepted:true,sent:true,observed:persisted,conversation_id:id,message_id:params.user_message_id,user_message_id:params.user_message_id,running:activeStreams.has(id),after_raw:after,chat_url:params.project_id?`https://chatgpt.com/g/${params.project_id}/c/${id}`:`https://chatgpt.com/c/${id}`};
}

async function dispatch(method,p) {
  if(method==="health"){const auth=await readStoredAuth();const catalog=await jsonRequest("/models?iim=false&include_icons=false");return {ready:true,transport:"direct",account_id_present:Boolean(auth.accountId),desktop_required:false,model_count:Array.isArray(catalog?.models)?catalog.models.length:0};}
  if(method==="models")return jsonRequest("/models?iim=false&include_icons=false");
  if(method==="get_thread"){const value=await jsonRequest(`/conversation/${encodeURIComponent(p.conversation_id)}`);return {...value,owned_stream:activeStreams.has(p.conversation_id)};}
  if(method==="list_projects"){const q=new URLSearchParams({conversations_per_gizmo:"0",limit:String(p.limit||20),owned_only:String(p.owned_only!==false)});if(p.cursor)q.set("cursor",p.cursor);return jsonRequest(`/gizmos/snorlax/sidebar?${q}`);}
  if(method==="get_project")return jsonRequest(`/gizmos/${encodeURIComponent(p.project_id)}`);
  if(method==="list_project_threads"){const q=new URLSearchParams({limit:String(p.limit||20),owned_only:String(p.owned_only!==false)});if(p.cursor)q.set("cursor",p.cursor);return jsonRequest(`/gizmos/${encodeURIComponent(p.project_id)}/conversations?${q}`);}
  if(method==="create_thread")return enqueueWrite(()=>startCompletion(p));
  if(method==="continue_thread")return enqueueWrite(async()=>{const before=await jsonRequest(`/conversation/${encodeURIComponent(p.conversation_id)}`);const messages=before?.mapping&&typeof before.mapping==="object"?Object.values(before.mapping).map(node=>node?.message).filter(Boolean):[];if(messages.some(message=>String(message?.id||"")===p.user_message_id))return {sent:true,accepted:true,observed:true,running:activeStreams.has(p.conversation_id),conversation_id:p.conversation_id,message_id:p.user_message_id,user_message_id:p.user_message_id,reconciled:true};const current=String(before.current_node||before.currentNode||"");if(current!==p.expected_current_node)return {sent:false,accepted:false,running:false,reason:"canonical current_node changed before send",conversation_id:p.conversation_id,user_message_id:p.user_message_id};if(activeStreams.has(p.conversation_id)&&!p.force)return {sent:false,accepted:false,running:true,reason:"thread is running",conversation_id:p.conversation_id,user_message_id:p.user_message_id};return startCompletion(p,{conversation_id:p.conversation_id,parent_message_id:current});});
  if(method==="cancel_thread"){const stream=activeStreams.get(p.conversation_id);if(!stream)return {cancelled:false,reason:"no direct transport-owned stream"};stream.controller.abort();return {cancelled:true,conversation_id:p.conversation_id};}
  if(method==="delete_thread"){const response=await backendFetch(`/conversation/id/${encodeURIComponent(p.conversation_id)}`,{method:"DELETE"},{read:false});return {accepted:response.ok,conversation_id:p.conversation_id,running:false};}
  throw new Error(`unknown direct worker method: ${method}`);
}
function enqueueWrite(action){const result=writeLane.then(action,action);writeLane=result.then(()=>undefined,()=>undefined);return result;}
function publicError(error){return {message:String(error?.message||error).replace(/Bearer\s+\S+/gi,"Bearer [redacted]").slice(0,500),status:Number(error?.status||0)||undefined};}

installBrowserShims();
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on("line",line=>{let request;try{request=JSON.parse(line);}catch{return;}if(request.method==="shutdown"){process.exit(0);return;}Promise.resolve(dispatch(request.method,request.params||{})).then(result=>process.stdout.write(`${JSON.stringify({id:request.id,ok:true,result})}\n`),error=>process.stdout.write(`${JSON.stringify({id:request.id,ok:false,error:publicError(error)})}\n`));});
lines.on("close",()=>process.exit(0));
process.on("SIGTERM",()=>process.exit(0));
