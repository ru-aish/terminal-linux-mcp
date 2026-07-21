const state = { snapshot: null, query: "" };
const $ = (id) => document.getElementById(id);
const setText = (id, value) => { const node = $(id); if (node) node.textContent = value; };
const make = (tag, className, content) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (content !== undefined) node.textContent = content;
  return node;
};

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(Number(value) * 1000);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(date);
}

function relativeTime(value, empty = "not scheduled") {
  if (!value) return empty;
  const seconds = Number(value) - Date.now() / 1000;
  if (seconds <= 1) return "now";
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}m`;
  return `${Math.ceil(seconds / 3600)}h`;
}

function shortId(value) {
  const source = String(value || "");
  return source.length > 24 ? `${source.slice(0, 12)}…${source.slice(-8)}` : source || "—";
}

function laneClass(value) {
  const lane = String(value || "").toLowerCase();
  if (lane.includes("read")) return "read";
  if (lane.includes("heavy")) return "heavy";
  if (lane.includes("cleanup")) return "cleanup";
  return "metadata";
}

function renderOperations(operations = []) {
  const rail = $("operationRail");
  rail.replaceChildren();
  if (!operations.length) {
    rail.append(make("li", "empty", "No queued provider operations."));
    return;
  }
  [...operations].sort((a, b) => Number(a.due_at || 0) - Number(b.due_at || 0)).slice(0, 30).forEach((operation, index) => {
    const row = make("li", "operation");
    row.append(make("span", "operation-index", String(index + 1).padStart(2, "0")));
    const main = make("div", "operation-main");
    const top = make("div", "operation-top");
    top.append(make("strong", "operation-name", `${operation.type || "operation"} · ${shortId(operation.agent_id)}`));
    top.append(make("span", `badge ${laneClass(operation.lane)}`, operation.lane || "local"));
    main.append(top);
    const meta = make("div", "operation-meta");
    meta.append(make("span", "", `due ${relativeTime(operation.due_at)}`));
    meta.append(make("span", "", `state ${operation.state || "—"}`));
    meta.append(make("span", "", `attempt ${operation.attempts || 0}`));
    main.append(meta);
    if (operation.last_error) main.append(make("p", "operation-error", operation.last_error));
    row.append(main);
    rail.append(row);
  });
}

function renderCircuits(circuits = []) {
  const list = $("circuits");
  list.replaceChildren();
  if (!circuits.length) {
    list.append(make("p", "empty", "No circuit records yet. Closed by default."));
    return;
  }
  circuits.forEach((circuit) => {
    const card = make("article", "circuit");
    card.dataset.state = circuit.state || "CLOSED";
    card.append(make("strong", "", circuit.scope || "gateway"));
    card.append(make("b", "", circuit.state || "CLOSED"));
    card.append(make("small", "", `retry ${relativeTime(circuit.retry_at)} · probe failures ${circuit.probe_failures || 0} · half-open successes ${circuit.half_open_successes || 0}`));
    list.append(card);
  });
}

function renderRequests(requests = []) {
  const rail = $("requestRail");
  rail.replaceChildren();
  if (!requests.length) {
    rail.append(make("li", "empty", "No physical requests recorded."));
    return;
  }
  requests.slice(0, 20).forEach((request) => {
    const outcome = String(request.outcome || "");
    const row = make("li", `request-item ${request.is_rate_limit ? "rate" : outcome === "ERROR" ? "error" : ""}`);
    row.append(make("i", ""));
    const body = make("div", "");
    body.append(make("strong", "", `${request.lane || "request"} · ${outcome || "RESERVED"}`));
    const duration = request.duration == null ? "in flight" : `${Number(request.duration).toFixed(2)}s`;
    body.append(make("small", "", `${shortId(request.operation_id)} · ${duration}${request.status_code ? ` · HTTP ${request.status_code}` : ""}`));
    row.append(body);
    row.append(make("time", "", formatTime(request.started_at)));
    rail.append(row);
  });
}

function agentMatches(agent) {
  if (!state.query) return true;
  const task = agent.task || {};
  return [agent.agent_id, agent.title, agent.status, agent.gateway_state, agent.chat_id, agent.project_id, agent.working_directory, task.status]
    .join(" ").toLowerCase().includes(state.query);
}

function renderAgents(agents = []) {
  const list = $("agentList");
  list.replaceChildren();
  const filtered = agents.filter(agentMatches);
  if (!filtered.length) {
    list.append(make("p", "empty", state.query ? "No agents match this filter." : "No agents registered."));
    return;
  }
  filtered.forEach((agent) => {
    const task = agent.task || {};
    const card = make("article", `agent-card ${agent.parent_agent_id ? "child" : "root"}`);
    const head = make("div", "agent-card-head");
    const title = make("div", "agent-title");
    title.append(make("strong", "", agent.title || (agent.parent_agent_id ? "Untitled child" : "Root conversation")));
    title.append(make("code", "", agent.agent_id || "—"));
    head.append(title);
    head.append(make("span", `state ${String(agent.status || "unknown").toLowerCase()}`, agent.status || "unknown"));
    card.append(head);

    const facts = make("dl", "agent-facts");
    [
      ["Gateway", agent.gateway_state || "unmapped"],
      ["Next read", relativeTime(agent.next_inspection_at)],
      ["Mailbox", String(agent.pending_mailbox || 0)],
      ["Project", shortId(agent.project_id)],
      ["Conversation", shortId(agent.chat_id)],
      ["Parent", shortId(agent.parent_agent_id)],
      ["Task", task.status || "none"],
      ["Directory", agent.working_directory || "none"],
    ].forEach(([label, value]) => {
      const group = make("div", "");
      group.append(make("dt", "", label));
      group.append(make("dd", "", value));
      facts.append(group);
    });
    card.append(facts);
    if (agent.last_error) card.append(make("p", "agent-error", agent.last_error));
    list.append(card);
  });
}

function render(snapshot) {
  state.snapshot = snapshot;
  const capacity = snapshot.capacity || {};
  const counts = snapshot.counts || {};
  const requests = Array.isArray(snapshot.requests) ? snapshot.requests : [];
  const requestSummary = Array.isArray(snapshot.request_summary) ? snapshot.request_summary : [];
  const requestTotal = requestSummary.reduce((sum, row) => sum + Number(row.count || 0), 0);
  const rateLimitedTotal = requestSummary.reduce((sum, row) => sum + Number(row.rate_limits || 0), 0);
  setText("activeChildren", capacity.active_children ?? counts.active ?? 0);
  setText("capacityLimit", `of ${capacity.maximum_active_children ?? 5} slots`);
  setText("queuedOperations", counts.queued_operations ?? 0);
  setText("terminalAgents", counts.terminal ?? 0);
  setText("requestCount", requestTotal);
  setText("rateLimitedCount", `${rateLimitedTotal} rate limited`);
  setText("nextSlot", snapshot.next_eligible_at ? relativeTime(snapshot.next_eligible_at) : "idle");
  setText("generatedAt", `updated ${formatTime(snapshot.generated_at)}`);
  renderOperations(snapshot.operations || []);
  renderCircuits(snapshot.circuits || []);
  renderRequests(requests);
  renderAgents(snapshot.agents || []);
}

async function refresh() {
  const connection = $("connection");
  try {
    const response = await fetch("/dashboard/agents/api", { headers: { accept: "application/json" }, cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
    connection.dataset.state = "live";
    connection.querySelector("span").textContent = "Live";
  } catch (error) {
    connection.dataset.state = "error";
    connection.querySelector("span").textContent = "Unavailable";
    console.error(error);
  }
}

$("agentFilter")?.addEventListener("input", (event) => {
  state.query = String(event.target.value || "").trim().toLowerCase();
  renderAgents(state.snapshot?.agents || []);
});

refresh();
setInterval(refresh, 5000);
