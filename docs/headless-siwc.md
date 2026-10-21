# Headless ChatGPT-plan inference: Terminal MCP experiment

This opt-in prototype uses the **official** Sign in with ChatGPT public-client OAuth flow and the public Responses API. It does not launch the ChatGPT desktop app, reuse `.codex/auth.json`, simulate browser fingerprints, solve anti-bot challenges, or access `chatgpt.com/backend-api`. A regular system browser is needed **only for initial authorization or reauthorization**.

## One-time sign-in

Install the additional Python requirement `PyJWT[crypto]`. From this fork checkout run:

```bash
python -m siwc_gateway login
# For terminals without a browser:
python -m siwc_gateway login --no-browser
python -m siwc_gateway status
```

The loopback OAuth callback must be opened in a browser **on the same machine** as the CLI (it binds `127.0.0.1` to a random port). For headless remote hosts, perform this locally and transfer the protected credential record securely according to OpenAI's documented self-hosted VM flow. Do not publish OAuth tokens or auth URLs containing ID-token hints. Files are under `~/.config/terminal-mcp-t3/siwc/`, owner-only `0600` with `0700` directory, and never printed by the CLI. Do not commit the files.

The authorization scope includes `chatgpt.tokens.use.direct`. Access tokens expire and rotate with refresh tokens. The daemon refreshes on demand; this initial prototype supports one local account/grant at a time. Account switching requires a deliberate reauthorization.

## Optional routes in the existing T3 daemon

Add `MCP_SIWC_ENABLED=1` **only to an isolated test deployment** of `t3_terminal_server.py`. This adds routes to the already-private `127.0.0.1` control port; it does **not** modify or expose the public MCP endpoint. Use the existing private control-plane bearer token for all calls. Do **not** expose the private control port to the public internet or to other users.

- `GET /v1/siwc/status` — local connection status (no raw credentials or account details).
- `GET /v1/siwc/capabilities` — discover documented supported features and unavailable ChatGPT-only features without trial requests.
- `GET /v1/siwc/models` — account-specific visible model slugs.
- `POST /v1/siwc/responses` — text/image/file (model permitting), multiturn and function/custom-tool Responses stream. Body: `{"model":"<chosen-slug>","input":[{"role":"user","content":"Hello"}]}`. The daemon forces `store=false`, `stream=true`. Optional `instructions`, `reasoning`, `tools` (namespaced function/custom tools or eligible web search), and `tool_choice` may be supplied. Send the **full conversation input** for every turn.
  For local tools, pass a namespace in `tools`, handle the returned `response.output_item.done` event containing a `function_call`, execute that tool in the local application under user consent, then issue a second call whose `input` includes the prior items and a `{"type":"function_call_output","call_id":"<returned-call-id>","output":"<result>"}` item. If the model emitted a `reasoning` item, preserve it on the next turn too. The daemon never executes arbitrary tools based on model output by itself.
- `POST /v1/siwc/cancel/{request_id}` — request cancellation. The request ID is returned in `X-Inference-Request-Id` and the first `gateway.started` event.

SSE streams preserve upstream `response.*` events and include gateway status events. **Only** `response.completed` indicates success; `response.failed`, `response.incomplete`, network interruption and explicit cancellation are not successes. A stream without a terminal event ends with `gateway.failed`.

This prototype deliberately does not implement a T3 provider adapter, ChatGPT conversation import/history, durable conversation persistence, unsupported hosted OpenAI tools or Android packaging. The first deployment should remain isolated until the T3 adapter supports these endpoints and lifecycle guarantees. Existing ChatGPT UI provider remains unchanged.

### Sources

- https://developers.openai.com/siwc/token-sharing-open-source/sign-in
- https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference
- https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations
- https://developers.openai.com/siwc/token-sharing-open-source/self-hosted-vms

## Endpoint compatibility and verification

The authorized SIWC token is for **documented `/v1/models` and `/v1/responses` usage**, not every OpenAI API or ChatGPT `backend-api` endpoint. Simply changing authorization headers cannot make unsupported API endpoints work. For T3, implement missing features inside the local application: session storage and resume in a local database; tool execution via local MCP; subagent orchestration through the app; device/voice integrations through native Android components. None of these require the ChatGPT desktop app for inference.

| Capability | Implementation | Status |
| --- | --- | --- |
| OAuth grant, renewal | Official SIWC public-client PKCE | Implemented; account sign-in required |
| Model listing | Private `/v1/siwc/models` → public `/v1/models` | Live-verified (HTTP 200) |
| Text streaming | Private `/v1/siwc/responses` → public `/v1/responses` | Live-verified (`response.completed`) |
| Function calls and tool-result continuation | Namespaced `function`/`custom`, full client-owned input history | Live-verified with local echo and a second request |
| In-flight HTTP request cancellation | Private `/v1/siwc/cancel/{request_id}` | Mocked concurrent cancellation test; not yet live-verified |
| Authenticated status/feature discovery | Private `/v1/siwc/status` and `/v1/siwc/capabilities` | Tested |
| Durable local conversation history/resume | To be implemented by T3 local session adapter | Not implemented here |
| ChatGPT account conversation history, native hosted connectors | Not granted by SIWC | Unavailable |
| Image-generation/hosted file-search/hosted Code Interpreter/native computer-use APIs | Not supported by SIWC plan usage | Unavailable; use separate authorized services or local application tools |

All private endpoints require the daemon's local control bearer; these paths are not a public general-purpose API or an authorization bypass. For ChatGPT-plan inference, follow the [SIWC terms](https://openai.com/policies/sign-in-with-chatgpt-terms/): only the connected application, the authenticated user, and user-controlled runtime or explicitly authorized automation may use the grant.

References: [SIWC model/inference contract](https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference), [preview limits](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations).

## Mapping legacy ChatGPT application calls onto app-owned behavior

The previous experimental direct worker (`chat_direct_worker.mjs`) relied on private ChatGPT service routes and account/session reuse. For a stable standalone daemon, do **not** forward those routes or attempt to recreate fingerprint/anti-bot prerequisites. Route the *T3 feature* to a supported implementation instead:

| Legacy behavior | New implementation boundary |
| --- | --- |
| Account readiness (`health`) | `/v1/siwc/status`, OAuth token renewal, capability discovery |
| Model menu | `/v1/siwc/models` backed by official account-specific model catalog |
| Create/continue a completion | `/v1/siwc/responses`, caller-provided full history, streaming terminal lifecycle |
| Local function/MCP tool calls | Namespaced function or custom tool requests; execute tools locally and send tool-output items on the next turn |
| Stop running generation | `/v1/siwc/cancel/{request_id}` closes the in-flight HTTP stream; retain local state and confirm terminal cancellation |
| List/get/delete chats | **Local T3 conversation store**, not the ChatGPT account's historical conversations; adapter still required |
| Project conversation list | **Local T3 project-to-thread index**; adapter still required |
| ChatGPT project/gizmo and hosted plugins | Cannot be recreated with SIWC grant; implement app-owned equivalents if needed |
| Sentinel or ChatGPT browser challenge endpoints | Not part of this architecture; no calls required |

The remaining **engineering task** is connecting T3's provider driver to these local daemon methods, using the existing T3 thread store for durable chat/project operations, and wiring explicit cancellation and tool outputs to its orchestration events. Then the standalone daemon can be packaged for Linux/Android. This is not native ChatGPT chat/account endpoint parity: importing or modifying ChatGPT's own chat history requires a separately supported delegated API permission that SIWC currently does not provide.
