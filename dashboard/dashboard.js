(() => {
  "use strict";

  const state = {
    hours: 24,
    snapshot: null,
    source: null,
    fallbackTimer: null,
    seenEventIds: new Set(),
    query: "",
  };

  const $ = (id) => document.getElementById(id);
  const number = new Intl.NumberFormat("en-US");
  const compact = new Intl.NumberFormat("en-US", {
    notation: "compact",
    compactDisplay: "short",
    maximumFractionDigits: 1,
  });

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function formatCount(value) {
    const n = Number(value || 0);
    return n >= 10000 ? compact.format(n) : number.format(n);
  }

  function formatBytes(value) {
    let n = Number(value || 0);
    const units = ["B", "KB", "MB", "GB"];
    let index = 0;
    while (n >= 1024 && index < units.length - 1) {
      n /= 1024;
      index += 1;
    }
    return `${n.toFixed(index ? 1 : 0)} ${units[index]}`;
  }

  function relativeTime(value) {
    if (!value) return "never";
    const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 5) return "now";
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  }

  function localTime(value, options = {}) {
    if (!value) return "—";
    return new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      ...options,
    }).format(new Date(value));
  }

  function setConnection(mode, label) {
    const node = $("connection");
    node.dataset.state = mode;
    $("connectionLabel").textContent = label;
  }

  function linePath(points) {
    if (!points.length) return "";
    return points.map((point, index) => `${index ? "L" : "M"}${point[0].toFixed(1)},${point[1].toFixed(1)}`).join(" ");
  }

  function renderChart(series) {
    const wrap = $("chartWrap");
    if (!series?.length || !series.some((item) => item.events)) {
      wrap.innerHTML = '<div class="chart-empty">No accounting events in this window.</div>';
      return;
    }

    const width = 1000;
    const height = 310;
    const pad = { left: 36, right: 24, top: 28, bottom: 48 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const maxTokens = Math.max(1, ...series.flatMap((item) => [item.exact_tokens, item.estimated_tokens]));
    const maxCalls = Math.max(1, ...series.map((item) => item.tool_calls));
    const x = (index) => pad.left + (series.length === 1 ? plotWidth / 2 : index * plotWidth / (series.length - 1));
    const y = (value) => pad.top + plotHeight - (value / maxTokens) * plotHeight;
    const exact = series.map((item, index) => [x(index), y(item.exact_tokens)]);
    const estimated = series.map((item, index) => [x(index), y(item.estimated_tokens)]);
    const barWidth = Math.max(3, Math.min(18, plotWidth / series.length - 3));
    const grid = [0, .25, .5, .75, 1].map((portion) => {
      const gy = pad.top + plotHeight * portion;
      const label = formatCount(Math.round(maxTokens * (1 - portion)));
      return `<line x1="${pad.left}" y1="${gy}" x2="${width - pad.right}" y2="${gy}" stroke="var(--rule)" stroke-width="1" opacity=".55"/><text x="${pad.left}" y="${gy - 6}" fill="var(--muted)" font-size="17" font-family="var(--mono)">${label}</text>`;
    }).join("");
    const bars = series.map((item, index) => {
      const barHeight = item.tool_calls ? Math.max(3, item.tool_calls / maxCalls * 34) : 0;
      return `<rect x="${x(index) - barWidth / 2}" y="${height - pad.bottom + 7 - barHeight}" width="${barWidth}" height="${barHeight}" rx="2" fill="var(--calls)" opacity=".65"><title>${item.tool_calls} tool calls</title></rect>`;
    }).join("");
    const labelIndexes = [...new Set([0, Math.floor((series.length - 1) / 2), series.length - 1])];
    const labels = labelIndexes.map((index) => `<text x="${x(index)}" y="${height - 12}" text-anchor="${index === 0 ? "start" : index === series.length - 1 ? "end" : "middle"}" fill="var(--muted)" font-size="18" font-family="var(--mono)">${escapeHtml(localTime(series[index].at))}</text>`).join("");

    wrap.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Exact and estimated token usage over time">
        ${grid}
        ${bars}
        <path d="${linePath(estimated)}" fill="none" stroke="var(--estimated)" stroke-width="5" stroke-linejoin="round" stroke-linecap="round"/>
        <path d="${linePath(exact)}" fill="none" stroke="var(--exact)" stroke-width="5" stroke-linejoin="round" stroke-linecap="round"/>
        ${estimated.map((point, index) => `<circle cx="${point[0]}" cy="${point[1]}" r="${series[index].estimated_tokens ? 4 : 0}" fill="var(--surface)" stroke="var(--estimated)" stroke-width="3"><title>${formatCount(series[index].estimated_tokens)} estimated tokens</title></circle>`).join("")}
        ${exact.map((point, index) => `<circle cx="${point[0]}" cy="${point[1]}" r="${series[index].exact_tokens ? 4 : 0}" fill="var(--surface)" stroke="var(--exact)" stroke-width="3"><title>${formatCount(series[index].exact_tokens)} exact tokens</title></circle>`).join("")}
        ${labels}
      </svg>`;
  }

  function createToolRow(tool) {
    const row = document.createElement("div");
    row.className = "tool-row";
    row.dataset.toolName = tool.name;
    row.innerHTML = `
      <span class="tool-name"></span>
      <strong class="tool-count"></strong>
      <div class="tool-track" aria-hidden="true"><div class="tool-fill"></div></div>
      <span class="tool-meta"></span>`;
    return row;
  }

  function updateToolRow(row, tool, max) {
    const name = row.querySelector(".tool-name");
    const count = row.querySelector(".tool-count");
    const fill = row.querySelector(".tool-fill");
    const meta = row.querySelector(".tool-meta");

    name.textContent = tool.name;
    name.title = tool.name;
    count.textContent = formatCount(tool.calls);
    count.title = number.format(tool.calls || 0);
    fill.style.width = `${Math.max(3, tool.calls / max * 100).toFixed(1)}%`;
    meta.textContent = `${formatCount(tool.input_tokens)} result tokens · ${formatCount(tool.output_tokens)} call tokens · ${relativeTime(tool.last_seen)}`;
  }

  function renderTools(tools) {
    const list = $("toolList");
    if (!tools?.length) {
      list.replaceChildren(Object.assign(document.createElement("div"), {
        className: "empty-row",
        textContent: "No tool calls in this window.",
      }));
      return;
    }

    const max = Math.max(...tools.map((tool) => tool.calls), 1);
    const existing = new Map(
      [...list.querySelectorAll(".tool-row")].map((row) => [row.dataset.toolName, row]),
    );
    const activeNames = new Set(tools.map((tool) => tool.name));

    list.querySelectorAll(".empty-row").forEach((row) => row.remove());
    existing.forEach((row, name) => {
      if (!activeNames.has(name)) row.remove();
    });

    tools.forEach((tool) => {
      const row = existing.get(tool.name) || createToolRow(tool);
      updateToolRow(row, tool, max);
      list.appendChild(row);
    });
  }

  function renderEvents(events) {
    const rail = $("eventRail");
    if (!events?.length) {
      rail.innerHTML = '<li class="empty-row">Waiting for the first event.</li>';
      return;
    }
    const newest = events.slice(0, 14);
    rail.innerHTML = newest.map((event) => {
      const isNew = !state.seenEventIds.has(event.id);
      const name = event.tool_name || event.model || event.event_type;
      const tokens = Number(event.input_tokens || 0) + Number(event.output_tokens || 0);
      return `<li class="event-item ${event.is_exact ? "exact" : "estimated"} ${isNew ? "new" : ""}">
        <div class="event-head">
          <span class="event-name" title="${escapeHtml(name)}">${escapeHtml(name)}</span>
          <time class="event-time" datetime="${escapeHtml(event.created_at)}">${relativeTime(event.created_at)}</time>
        </div>
        <div class="event-detail">
          <span class="event-badge">${event.is_exact ? "exact" : "estimate"}</span>
          <span>${formatCount(tokens)} tokens</span>
          <span title="${escapeHtml(event.thread_id)}">${escapeHtml(event.thread_id)}</span>
        </div>
      </li>`;
    }).join("");
    newest.forEach((event) => state.seenEventIds.add(event.id));
  }

  function renderThreads() {
    const rows = state.snapshot?.threads || [];
    const query = state.query.trim().toLowerCase();
    const filtered = query ? rows.filter((row) => `${row.thread_id} ${row.cwd}`.toLowerCase().includes(query)) : rows;
    const target = $("threadRows");
    if (!filtered.length) {
      target.innerHTML = `<div class="empty-row">${query ? "No thread matches this filter." : "No threads recorded."}</div>`;
      return;
    }
    target.innerHTML = filtered.map((row) => `
      <div class="thread-row" role="row">
        <div class="thread-main" role="cell">
          <span class="thread-id" title="${escapeHtml(row.thread_id)}">${escapeHtml(row.thread_id)}</span>
          <span class="thread-path" title="${escapeHtml(row.cwd)}">${escapeHtml(row.cwd)}</span>
        </div>
        <strong class="thread-calls" role="cell">${formatCount(row.tool_calls)}</strong>
        <div class="thread-metrics">
          <span role="cell"><b>${formatCount(row.exact_tokens)}</b> exact</span>
          <span role="cell"><b>${formatCount(row.estimated_tokens)}</b> estimated</span>
          <span role="cell">${relativeTime(row.last_event_at || row.updated_at)}</span>
        </div>
      </div>`).join("");
  }

  function render(snapshot) {
    state.snapshot = snapshot;
    const totals = snapshot.totals;
    $("exactTokens").textContent = formatCount(totals.exact_tokens);
    $("exactTokens").title = number.format(totals.exact_tokens || 0);
    $("exactInput").textContent = formatCount(totals.exact_input_tokens);
    $("exactOutput").textContent = formatCount(totals.exact_output_tokens);
    $("estimatedTokens").textContent = formatCount(totals.estimated_tokens);
    $("estimatedTokens").title = number.format(totals.estimated_tokens || 0);
    $("toolCalls").textContent = formatCount(totals.tool_calls);
    $("callRate").textContent = Number(snapshot.window.calls_per_minute_5m || 0).toFixed(1);
    $("activeThreads").textContent = formatCount(totals.active_threads);
    $("threadTotal").textContent = formatCount(totals.threads);
    $("windowSummary").textContent = `${formatCount(snapshot.window.tool_calls)} calls · ${formatCount(snapshot.window.estimated_tokens)} estimated · ${formatCount(snapshot.window.exact_tokens)} exact`;
    $("lastUpdated").textContent = `Updated ${relativeTime(snapshot.generated_at)}`;
    $("databaseSize").textContent = `DB ${formatBytes(snapshot.database_bytes)}`;
    $("eventCount").textContent = `${formatCount(snapshot.window.events)} events`;
    $("databasePath").textContent = snapshot.database;
    $("databasePath").title = snapshot.database;
    $("accountingNote").textContent = snapshot.accounting_note;
    renderChart(snapshot.series);
    renderTools(snapshot.tools);
    renderEvents(snapshot.recent_events);
    renderThreads();
  }

  async function fetchSnapshot() {
    const response = await fetch(`/dashboard/api?hours=${state.hours}&limit=40`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Dashboard API returned ${response.status}`);
    render(await response.json());
  }

  function clearFallback() {
    if (state.fallbackTimer) window.clearInterval(state.fallbackTimer);
    state.fallbackTimer = null;
  }

  function beginFallback() {
    if (state.fallbackTimer) return;
    fetchSnapshot().catch(() => {});
    state.fallbackTimer = window.setInterval(() => fetchSnapshot().catch(() => {}), 5000);
  }

  function connect() {
    clearFallback();
    if (state.source) state.source.close();
    setConnection("connecting", "Connecting");
    const source = new EventSource(`/dashboard/events?hours=${state.hours}&limit=40`);
    state.source = source;
    source.addEventListener("open", () => {
      clearFallback();
      setConnection("live", "Live");
    });
    source.addEventListener("snapshot", (event) => {
      try {
        render(JSON.parse(event.data));
        setConnection("live", "Live");
      } catch (error) {
        console.error("Could not parse dashboard snapshot", error);
      }
    });
    source.addEventListener("error", () => {
      setConnection("error", "Reconnecting");
      beginFallback();
    });
  }

  document.querySelectorAll("[data-hours]").forEach((button) => {
    button.addEventListener("click", () => {
      const hours = Number(button.dataset.hours);
      if (!hours || hours === state.hours) return;
      state.hours = hours;
      document.querySelectorAll("[data-hours]").forEach((candidate) => candidate.setAttribute("aria-pressed", String(candidate === button)));
      connect();
    });
  });

  $("threadSearch").addEventListener("input", (event) => {
    state.query = event.currentTarget.value;
    renderThreads();
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && (!state.source || state.source.readyState === EventSource.CLOSED)) connect();
  });

  window.addEventListener("beforeunload", () => {
    if (state.source) state.source.close();
    clearFallback();
  });

  connect();
})();
