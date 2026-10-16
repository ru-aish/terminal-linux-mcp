const state = { snapshot: null, query: "" };
const byId = (id) => document.getElementById(id);
const setText = (id, value) => {
  const node = byId(id);
  if (node) node.textContent = value;
};
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
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function relativeTime(value, empty = "not scheduled") {
  if (!value) return empty;
  const seconds = Number(value) - Date.now() / 1000;
  const absolute = Math.abs(seconds);
  const suffix = seconds < -1 ? " ago" : "";
  if (absolute <= 1) return "now";
  if (absolute < 60) return String(Math.ceil(absolute)) + "s" + suffix;
  if (absolute < 3600) return String(Math.ceil(absolute / 60)) + "m" + suffix;
  if (absolute < 86400) return String(Math.ceil(absolute / 3600)) + "h" + suffix;
  return String(Math.ceil(absolute / 86400)) + "d" + suffix;
}

function shortId(value) {
  const source = String(value || "");
  if (!source) return "—";
  return source.length > 28
    ? source.slice(0, 14) + "…" + source.slice(-8)
    : source;
}

function laneClass(value) {
  const lane = String(value || "").toLowerCase();
  if (lane.includes("read")) return "read";
  if (lane.includes("heavy")) return "heavy";
  if (lane.includes("cleanup")) return "cleanup";
  return "metadata";
}

function automationLabel(item) {
  return item.kind === "wakeup" ? "Wake timer" : "Completion gate";
}

function renderAutomations(automations = []) {
  const rail = byId("automationRail");
  rail.replaceChildren();
  if (!automations.length) {
    rail.append(make("li", "empty", "No durable automations."));
    return;
  }
  const terminal = new Set(["delivered", "cancelled", "failed"]);
  const activeFirst = [...automations].sort((a, b) => {
    const stateOrder = Number(terminal.has(a.status)) - Number(terminal.has(b.status));
    if (stateOrder) return stateOrder;
    return Number(a.due_at || a.created_at || 0) - Number(b.due_at || b.created_at || 0);
  });
  activeFirst.slice(0, 40).forEach((item) => {
    const row = make("li", "automation " + String(item.kind || "unknown"));
    const railMark = make("div", "automation-mark");
    railMark.append(make("i", ""));
    railMark.append(make("span", "", item.kind === "wakeup" ? "T" : "M"));
    row.append(railMark);

    const body = make("div", "automation-body");
    const top = make("div", "automation-top");
    top.append(make("strong", "", automationLabel(item)));
    top.append(
      make(
        "span",
        "state " + String(item.status || "unknown").toLowerCase(),
        item.status || "unknown",
      ),
    );
    body.append(top);

    const route = item.kind === "wakeup"
      ? (item.target_title || shortId(item.target_agent_id)) + " · due " + relativeTime(item.due_at)
      : (item.source_title || shortId(item.source_agent_id)) + " → " +
        (item.target_title || shortId(item.target_agent_id));
    body.append(make("p", "automation-route", route));
    body.append(make("p", "automation-message", item.message || "—"));
    if (item.kind === "after_completion") {
      body.append(make("code", "marker", item.completion_marker || "marker unavailable"));
    }
    if (item.last_error) body.append(make("p", "automation-error", item.last_error));
    row.append(body);
    rail.append(row);
  });
}

function renderOperations(operations = [], agents = []) {
  const rail = byId("operationRail");
  rail.replaceChildren();
  const names = new Map(
    agents.map((agent) => [agent.gateway_agent_id, agent.title || agent.agent_id]),
  );
  if (!operations.length) {
    rail.append(make("li", "empty", "No queued provider operations."));
    return;
  }
  [...operations]
    .sort((a, b) => Number(a.due_at || 0) - Number(b.due_at || 0))
    .slice(0, 30)
    .forEach((operation, index) => {
      const row = make("li", "operation");
      row.append(
        make("span", "operation-index", String(index + 1).padStart(2, "0")),
      );
      const main = make("div", "operation-main");
      const top = make("div", "operation-top");
      const owner = names.get(operation.agent_id) || shortId(operation.agent_id);
      top.append(
        make(
          "strong",
          "operation-name",
          String(operation.type || "operation") + " · " + owner,
        ),
      );
      top.append(
        make(
          "span",
          "badge " + laneClass(operation.lane),
          operation.lane || "local",
        ),
      );
      main.append(top);
      const meta = make("div", "operation-meta");
      meta.append(make("span", "", "due " + relativeTime(operation.due_at)));
      meta.append(make("span", "", "state " + String(operation.state || "—")));
      meta.append(make("span", "", "attempt " + String(operation.attempts || 0)));
      main.append(meta);
      if (operation.last_error) {
        main.append(make("p", "operation-error", operation.last_error));
      }
      row.append(main);
      rail.append(row);
    });
}

function renderCircuits(circuits = []) {
  const list = byId("circuits");
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
    card.append(
      make(
        "small",
        "",
        "retry " + relativeTime(circuit.retry_at) +
          " · probe failures " + String(circuit.probe_failures || 0) +
          " · half-open successes " + String(circuit.half_open_successes || 0),
      ),
    );
    list.append(card);
  });
}

function renderRequests(requests = []) {
  const rail = byId("requestRail");
  rail.replaceChildren();
  if (!requests.length) {
    rail.append(make("li", "empty", "No physical requests recorded."));
    return;
  }
  requests.slice(0, 20).forEach((request) => {
    const outcome = String(request.outcome || "");
    const errorClass = request.is_rate_limit
      ? "rate"
      : outcome === "ERROR"
        ? "error"
        : "";
    const row = make("li", "request-item " + errorClass);
    row.append(make("i", ""));
    const body = make("div", "");
    body.append(
      make(
        "strong",
        "",
        String(request.lane || "request") + " · " + (outcome || "RESERVED"),
      ),
    );
    const duration = request.duration == null
      ? "in flight"
      : Number(request.duration).toFixed(2) + "s";
    const status = request.status_code ? " · HTTP " + String(request.status_code) : "";
    body.append(
      make(
        "small",
        "",
        shortId(request.operation_id) + " · " + duration + status,
      ),
    );
    row.append(body);
    row.append(make("time", "", formatTime(request.started_at)));
    rail.append(row);
  });
}

function agentMatches(agent) {
  if (!state.query) return true;
  const task = agent.task || {};
  return [
    agent.agent_id,
    agent.title,
    agent.status,
    agent.gateway_state,
    agent.chat_id,
    agent.project_id,
    agent.working_directory,
    task.status,
    ...(agent.path || []),
  ].join(" ").toLowerCase().includes(state.query);
}

function renderAgents(agents = []) {
  const list = byId("agentList");
  list.replaceChildren();
  const filtered = agents.filter(agentMatches);
  if (!filtered.length) {
    list.append(
      make(
        "p",
        "empty",
        state.query ? "No agents match this filter." : "No agents registered.",
      ),
    );
    return;
  }
  filtered.forEach((agent) => {
    const task = agent.task || {};
    const depth = Math.max(0, Math.min(Number(agent.depth || 0), 8));
    const card = make("article", "agent-card " + (depth ? "child" : "root"));
    card.style.setProperty("--depth", String(depth));
    const branch = make("div", "branch-mark");
    branch.append(
      make("span", "", depth ? String(depth).padStart(2, "0") : "R"),
    );
    card.append(branch);

    const content = make("div", "agent-content");
    const head = make("div", "agent-card-head");
    const title = make("div", "agent-title");
    title.append(
      make(
        "strong",
        "",
        agent.title || (depth ? "Untitled child" : "Root conversation"),
      ),
    );
    title.append(make("code", "", agent.agent_id || "—"));
    head.append(title);
    head.append(
      make(
        "span",
        "state " + String(agent.status || "unknown").toLowerCase(),
        agent.status || "unknown",
      ),
    );
    content.append(head);

    const facts = make("dl", "agent-facts");
    [
      ["Gateway", agent.gateway_state || "unmapped"],
      ["Next read", relativeTime(agent.next_inspection_at)],
      ["Last read", agent.last_inspected_at ? relativeTime(agent.last_inspected_at) : "never"],
      ["Mailbox", String(agent.pending_mailbox || 0)],
      ["Children", String(agent.children_count || 0)],
      ["Conversation", shortId(agent.chat_id)],
      ["Task", task.status || "none"],
      ["Directory", agent.working_directory || "none"],
    ].forEach(([label, value]) => {
      const group = make("div", "");
      group.append(make("dt", "", label));
      group.append(make("dd", "", value));
      facts.append(group);
    });
    content.append(facts);
    if (agent.last_error) {
      content.append(make("p", "agent-error", agent.last_error));
    }
    card.append(content);
    list.append(card);
  });
}

function renderSync(snapshot) {
  const sync = snapshot.sync_service || {};
  const reasoning = snapshot.reasoning || {};
  const result = sync.last_result || {};
  setText(
    "syncState",
    sync.running ? "running" : sync.enabled === false ? "disabled" : "idle",
  );
  let syncDetail = "not started";
  if (sync.last_error) {
    syncDetail = "error · " + sync.last_error;
  } else if (sync.last_sync_at) {
    syncDetail = "last tick " + relativeTime(sync.last_sync_at) +
      " · " + String(result.physical_requests || 0) + " request";
  }
  setText("lastSync", syncDetail);
  setText(
    "nextWake",
    snapshot.next_wakeup_at ? relativeTime(snapshot.next_wakeup_at) : "idle",
  );
  const effort = reasoning.thinking_effort || "unspecified";
  setText(
    "reasoningMode",
    reasoning.require_high_reasoning ? "high · " + effort : effort,
  );
  setText("reasoningModel", reasoning.model || "model not reported");
}

function render(snapshot) {
  state.snapshot = snapshot;
  const capacity = snapshot.capacity || {};
  const counts = snapshot.counts || {};
  const requests = Array.isArray(snapshot.requests) ? snapshot.requests : [];
  const requestSummary = Array.isArray(snapshot.request_summary)
    ? snapshot.request_summary
    : [];
  const requestTotal = requestSummary.reduce(
    (sum, row) => sum + Number(row.count || 0),
    0,
  );
  const rateLimitedTotal = requestSummary.reduce(
    (sum, row) => sum + Number(row.rate_limits || 0),
    0,
  );
  setText("activeChildren", capacity.active_children ?? counts.active ?? 0);
  setText(
    "capacityLimit",
    "of " + String(capacity.maximum_active_children ?? 5) + " slots",
  );
  setText("queuedOperations", counts.queued_operations ?? 0);
  setText("scheduledWakeups", counts.scheduled_wakeups ?? 0);
  setText("completionTriggers", counts.completion_triggers ?? 0);
  setText("terminalAgents", counts.terminal ?? 0);
  setText("requestCount", requestTotal);
  setText("rateLimitedCount", String(rateLimitedTotal) + " rate limited");
  setText(
    "nextSlot",
    snapshot.next_eligible_at ? relativeTime(snapshot.next_eligible_at) : "idle",
  );
  setText("generatedAt", "updated " + formatTime(snapshot.generated_at));
  renderSync(snapshot);
  renderAutomations(snapshot.automations || []);
  renderOperations(snapshot.operations || [], snapshot.agents || []);
  renderCircuits(snapshot.circuits || []);
  renderRequests(requests);
  renderAgents(snapshot.agents || []);
}

async function refresh() {
  const connection = byId("connection");
  try {
    const response = await fetch("/dashboard/agents/api", {
      headers: { accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) throw new Error("HTTP " + String(response.status));
    render(await response.json());
    connection.dataset.state = "live";
    connection.querySelector("span").textContent = "Live";
  } catch (error) {
    connection.dataset.state = "error";
    connection.querySelector("span").textContent = "Unavailable";
    console.error(error);
  }
}

byId("agentFilter")?.addEventListener("input", (event) => {
  state.query = String(event.target.value || "").trim().toLowerCase();
  renderAgents(state.snapshot?.agents || []);
});

refresh();
setInterval(refresh, 5000);
