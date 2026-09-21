function render({ model, el }) {
  const scene = model.get("scene") || {};
  const root = document.createElement("div");
  root.className = "mc-studio";
  root.innerHTML = `
    <header class="mc-header"><div class="mc-brand"><svg class="mc-brand-mark" viewBox="0 0 32 32" aria-hidden="true"><path d="M8 3h14a7 7 0 0 1 3 13l-12 5C2 25-3 7 8 3" fill="#b7e5bb"/><circle cx="6" cy="27" r="5" fill="#ff9175"/><rect x="14" y="21" width="17" height="10" rx="5" fill="#d7acf0"/></svg><div><div class="mc-kicker">MINIPLACE / LIVE COORDINATION</div><h2 data-role="title"></h2></div></div><div class="mc-status"><i class="mc-status-dot"></i><span data-role="status"></span><span class="mc-clock" data-role="clock">0:00</span></div></header>
    <div class="mc-metrics"><div class="mc-metric"><b data-role="jobs">0/0</b><span>Active worker jobs</span><small data-role="unused"></small></div><div class="mc-metric"><b data-role="api">0</b><span>Model requests</span><small data-role="api-errors"></small></div><div class="mc-metric"><b data-role="painting">0</b><span>Brushes painting</span><small data-role="queued"></small></div><div class="mc-metric"><b data-role="remaining">0</b><span>Pixels to repair</span><small data-role="calls"></small></div></div>
    <div class="mc-layout"><main class="mc-main"><section class="mc-panel"><div class="mc-panel-head"><div><div class="mc-panel-title">The shared canvas</div><div class="mc-panel-subtitle" data-role="dimensions"></div></div><div class="mc-controls"><button data-role="zoom-out" aria-label="Zoom out">−</button><span class="mc-zoom-value" data-role="zoom-value">100%</span><button data-role="zoom-in" aria-label="Zoom in">+</button><button data-role="focus" aria-pressed="false">Focus canvas</button></div></div><div class="mc-viewport" tabindex="0" aria-label="Canvas viewport"><div class="mc-surface"><img class="mc-canvas-img" alt="Shared live canvas"><div class="mc-overlay-host"></div></div></div><div class="mc-canvas-footer"><div class="mc-progress-track"><div class="mc-progress-fill"></div></div><div class="mc-progress-label"><span><b data-role="repaired">0%</b> of initial errors repaired</span><span data-role="corrected"></span></div><div class="mc-legend"><span><i class="api"></i>Model inference</span><span><i class="paint"></i>Painting</span><span>Dashed outlines = advisory work plans</span></div></div></section><details class="mc-panel mc-bottom" open><summary>Live communication & concurrent activity</summary><div class="mc-network-host"></div><div class="mc-timeline"></div></details></main>
    <aside class="mc-sidebar"><section class="mc-panel"><div class="mc-reference-row"><img class="mc-reference-img" alt="Shared target reference"><div class="mc-reference-info"><b>One shared target</b><span data-role="model-label"></span><span>Text-only model observations</span><div class="mc-swatches"></div></div></div></section><section class="mc-panel mc-crew-panel"><div class="mc-panel-head"><span class="mc-panel-title">The crew</span><span class="mc-crew-count" data-role="crew-count"></span></div><div class="mc-agent-grid"></div></section>
    <section class="mc-panel mc-log-panel"><div class="mc-panel-head"><span class="mc-panel-title">Agent activity</span><div class="mc-log-tools"><select aria-label="Filter agent logs" data-role="agent-filter"></select><button data-role="follow" aria-pressed="true">Following live</button></div></div><div class="mc-inspector"><div class="mc-inspector-model" data-role="inspector-model"></div><div class="mc-inspector-task" data-role="inspector-task"></div><div class="mc-stream-status" data-role="stream"></div></div><div class="mc-log-options"><label><input type="checkbox" data-role="details">Include brush rows</label><span class="mc-new-events" data-role="new-events"></span></div><div class="mc-log-scroll" tabindex="0" role="log" aria-live="polite"></div></section></aside></div>`;
  el.append(root);
  const $ = selector => root.querySelector(selector);
  const role = name => $(`[data-role="${name}"]`);
  const setText = (node, text) => { const value = String(text ?? ""); if (node.textContent !== value) node.textContent = value; };
  const count = value => Number(value || 0).toLocaleString();
  const clock = value => `${Math.floor(value / 60)}:${String(Math.floor(value % 60)).padStart(2, "0")}`;
  const fullscreenButton = document.createElement("button"); fullscreenButton.type = "button";
  fullscreenButton.className = "mc-fullscreen-button"; fullscreenButton.dataset.role = "fullscreen";
  fullscreenButton.setAttribute("aria-label", "Toggle dashboard full screen"); fullscreenButton.textContent = "Full screen";
  $(".mc-header").append(fullscreenButton);
  let fullscreenAnchor = null, oldBodyOverflow = "";
  function fullscreenLabel() { const active = document.fullscreenElement === root || root.classList.contains("mc-fullscreen-fallback"); setText(fullscreenButton, active ? "Exit full screen" : "Full screen"); fullscreenButton.setAttribute("aria-pressed", String(active)); }
  function exitFallback() {
    if (!fullscreenAnchor) return;
    fullscreenAnchor.replaceWith(root); fullscreenAnchor = null;
    root.classList.remove("mc-fullscreen-fallback"); document.body.style.overflow = oldBodyOverflow; fullscreenLabel();
  }
  function enterFallback() {
    fullscreenAnchor = document.createComment("MiniPlace fullscreen anchor"); root.before(fullscreenAnchor);
    oldBodyOverflow = document.body.style.overflow; document.body.style.overflow = "hidden";
    document.body.append(root); root.classList.add("mc-fullscreen-fallback"); fullscreenLabel();
  }
  fullscreenButton.onclick = async () => {
    if (fullscreenAnchor) { exitFallback(); return; }
    if (document.fullscreenElement === root) { await document.exitFullscreen(); return; }
    try { if (!root.requestFullscreen) throw new Error("Fullscreen unavailable"); await root.requestFullscreen(); }
    catch { enterFallback(); }
    fullscreenLabel();
  };
  const fullscreenEscape = event => { if (event.key === "Escape") exitFallback(); };
  document.addEventListener("fullscreenchange", fullscreenLabel);
  document.addEventListener("keydown", fullscreenEscape);
  fullscreenLabel();
  const activityPanel = $(".mc-bottom");
  $(".mc-sidebar").insertBefore(activityPanel, $(".mc-crew-panel"));
  activityPanel.classList.add("mc-activity-panel");
  const expandActivity = document.createElement("button"); expandActivity.type = "button"; expandActivity.textContent = "Expand graph";
  activityPanel.querySelector("summary").append(expandActivity);
  const activityDialog = document.createElement("dialog"); activityDialog.className = "mc-activity-dialog";
  activityDialog.innerHTML = '<header><div><b>Live communication & concurrent activity</b><p>Purple = inference · colored nodes = painting · links = recent messages/actions</p></div><button type="button" data-role="close-activity">Close</button></header><div class="mc-expanded-network"></div>';
  root.append(activityDialog);
  expandActivity.onclick = event => { event.preventDefault(); event.stopPropagation(); $(".mc-expanded-network").innerHTML = frame.network || ""; renderAgentActivity(); activityDialog.showModal(); };
  role("close-activity").onclick = () => activityDialog.close();
  $(".mc-expanded-network").onclick = event => { const id = event.target.closest("[data-agent]")?.dataset.agent; if (agentNodes.has(id)) { selectAgent(id); activityDialog.close(); } };
  const canvasPanel = $(".mc-main > .mc-panel"), tabs = document.createElement("div");
  tabs.className = "mc-view-tabs";
  tabs.innerHTML = '<button data-role="canvas-tab" aria-pressed="true">Canvas</button><button data-role="board-tab" aria-pressed="false">Work board <span data-role="board-count">0</span></button><span>Model-chosen intentions</span>';
  canvasPanel.prepend(tabs);
  for (const node of [$(".mc-main .mc-panel-head"), $(".mc-viewport"), $(".mc-canvas-footer")]) node.classList.add("mc-canvas-chrome");
  const boardPanel = document.createElement("div"); boardPanel.className = "mc-workboard";
  boardPanel.innerHTML = '<div class="mc-workboard-head"><div><b>Shared work board</b><p>Who is doing what, where, and how it is progressing.</p></div><div class="mc-log-tools"><select data-role="board-filter" aria-label="Filter work board"><option value="all">All intentions</option><option value="active">Active intentions</option><option value="conflicts">Overlapping plans</option><option value="done">Reported done</option></select><button data-role="board-history">Board history</button></div></div><div class="mc-board-summary" data-role="board-summary"></div><div class="mc-work-cards"></div>';
  canvasPanel.append(boardPanel);
  function showBoard(show) { root.classList.toggle("mc-show-board", show); role("board-tab").setAttribute("aria-pressed", String(show)); role("canvas-tab").setAttribute("aria-pressed", String(!show)); }
  role("canvas-tab").onclick = () => showBoard(false);
  role("board-tab").onclick = () => showBoard(true);
  setText(role("title"), scene.mode === "hierarchy" ? "Orchestrator + crew" : "Self-organizing peers");
  setText(role("dimensions"), `${scene.width} × ${scene.height} pixels · shared, unrestricted workspace`);
  setText(role("model-label"), scene.mode === "hierarchy" ? `${scene.coordinator_model} → ${scene.worker_model}` : scene.worker_model);
  $(".mc-reference-img").src = scene.reference_png || "";
  const canvas = $(".mc-canvas-img");
  canvas.width = scene.width; canvas.height = scene.height;
  $(".mc-viewport").style.aspectRatio = `${scene.width}/${scene.height}`;
  for (const [glyph, color] of Object.entries(scene.palette || {})) {
    const swatch = document.createElement("i"); swatch.style.background = color; swatch.title = `${glyph}: ${color}`;
    $(".mc-swatches").append(swatch);
  }
  let zoom = 1;
  function changeZoom(delta) { zoom = Math.max(1, Math.min(4, zoom + delta)); $(".mc-surface").style.width = `${zoom * 100}%`; setText(role("zoom-value"), `${zoom * 100}%`); }
  role("zoom-in").onclick = () => changeZoom(.5);
  role("zoom-out").onclick = () => changeZoom(-.5);
  role("focus").onclick = () => { const active = root.classList.toggle("mc-focus"); role("focus").setAttribute("aria-pressed", String(active)); setText(role("focus"), active ? "Show crew" : "Focus canvas"); };

  const grid = $(".mc-agent-grid"), agentNodes = new Map(), filter = role("agent-filter");
  for (const [id, label] of [["all", "All agents"], ["board", "Shared work board"], ...(scene.agents || []).map(id => [id, id])]) {
    const option = document.createElement("option"); option.value = id; option.textContent = label; filter.append(option);
  }
  for (const id of scene.agents || []) {
    const button = document.createElement("button"); button.className = "mc-agent"; button.type = "button"; button.dataset.agent = id;
    button.innerHTML = '<span class="mc-agent-name"></span><span class="mc-agent-phase"></span><span class="mc-agent-count"></span><span class="mc-agent-indicators"><i></i><i></i></span>';
    setText(button.querySelector(".mc-agent-name"), id); button.onclick = () => selectAgent(id); grid.append(button); agentNodes.set(id, button);
  }

  const log = $(".mc-log-scroll"), eventCache = new Map(), logNodes = new Map();
  const boardNodes = new Map();
  let selected = "all", follow = true, includeDetails = false, pendingEvents = 0;
  let frame = {}, receivedAt = performance.now(), priorSequence = -1, runId = null, ignoreScrollUntil = 0;
  function matches(event) { return (includeDetails || event.kind !== "brush") && (selected === "all" || (selected === "board" ? event.target === "Work board" : event.source === selected || event.target === selected)); }
  function followLabel() { role("follow").setAttribute("aria-pressed", String(follow)); setText(role("follow"), follow ? "Following live" : "Resume live"); setText(role("new-events"), pendingEvents ? `${count(pendingEvents)} new · view paused` : follow ? "Live" : "Reading history"); log.setAttribute("aria-live", follow ? "polite" : "off"); }
  function renderLogs(force = false) {
    if (!follow && !force) { followLabel(); return; }
    const events = [...eventCache.values()].filter(matches).slice(-400), wanted = new Set(events.map(event => event.id));
    for (const [id, node] of logNodes) if (!wanted.has(id)) { node.remove(); logNodes.delete(id); }
    const empty = log.querySelector(".mc-log-empty"); if (empty && events.length) empty.remove();
    if (!events.length && !empty) { const node = document.createElement("div"); node.className = "mc-log-empty"; node.textContent = "Waiting for activity in this view…"; log.append(node); }
    for (const event of events) {
      if (logNodes.has(event.id)) continue;
      const node = document.createElement("article"); node.className = "mc-log-entry"; node.dataset.event = String(event.id);
      if (/error|retry|recovery|failed/i.test(event.kind + " " + event.text)) node.classList.add("is-error");
      if (/message|board update|delegation|steer/i.test(event.kind)) node.classList.add("is-message");
      const header = document.createElement("header"), at = document.createElement("span"), author = document.createElement("b"), kind = document.createElement("span");
      setText(at, `${Number(event.time || 0).toFixed(1)}s`); setText(author, `${event.source} → ${event.target}`); setText(kind, event.kind);
      header.append(at, author, kind); const text = document.createElement("p"); text.textContent = event.text;
      node.append(header, text); log.append(node); logNodes.set(event.id, node);
    }
    if (follow) { ignoreScrollUntil = performance.now() + 100; log.scrollTop = log.scrollHeight; pendingEvents = 0; }
    followLabel();
  }
  function selectAgent(id) { selected = id; filter.value = id; follow = true; pendingEvents = 0; renderLogs(true); renderInspector(); for (const [name, node] of agentNodes) node.classList.toggle("is-selected", name === id); }
  filter.onchange = () => selectAgent(filter.value);
  role("board-filter").onchange = () => renderBoard();
  role("board-history").onclick = () => { selectAgent("board"); $(".mc-log-panel").scrollIntoView({behavior: "smooth", block: "nearest"}); };
  role("follow").onclick = () => { follow = !follow; renderLogs(follow); followLabel(); };
  role("details").onchange = () => { includeDetails = role("details").checked; renderLogs(true); };
  log.addEventListener("wheel", event => { ignoreScrollUntil = 0; if (event.deltaY < 0) { follow = false; followLabel(); } }, {passive: true});
  log.addEventListener("pointerdown", () => { ignoreScrollUntil = 0; });
  log.addEventListener("keydown", () => { ignoreScrollUntil = 0; });
  log.addEventListener("scroll", () => { if (performance.now() < ignoreScrollUntil) return; follow = log.scrollHeight - log.clientHeight - log.scrollTop < 14; followLabel(); }, {passive: true});
  $(".mc-network-host").onclick = event => { const id = event.target.closest("[data-agent]")?.dataset.agent; if (agentNodes.has(id)) selectAgent(id); };

  function elapsedNow() { return Number(frame.elapsed || 0) + (frame.running ? (performance.now() - receivedAt) / 1000 : 0); }
  function requestPhase(agent) { return ({waiting: "API wait", thinking: "thinking", text: "responding", tools: "drafting tools", finishing: "awaiting end", complete: "response ready"})[agent.stream?.phase] || "API wait"; }
  function secondsSince(time) { return Math.max(0, Math.floor(elapsedNow() - Number(time))); }
  function requestTiming(agent) {
    if (!agent.api || agent.request_started == null) return "";
    const progress = agent.stream?.last_progress_at;
    const remaining = agent.request_deadline == null ? "" : ` · ${Math.max(0, Math.ceil(agent.request_deadline - elapsedNow()))}s to deadline`;
    return `request ${secondsSince(agent.request_started)}s · ${progress == null ? "no output yet" : `last output ${secondsSince(progress)}s ago`}${remaining}`;
  }
  function renderAgentActivity() {
    for (const agent of frame.agents || []) {
      const node = agentNodes.get(agent.id); if (!node) continue;
      const phase = agent.api ? requestPhase(agent) + (agent.painting ? " + brush" : "") : agent.painting ? "painting" : agent.state;
      const age = agent.api && agent.request_started != null ? ` · ${secondsSince(agent.request_started)}s` : "";
      node.classList.toggle("is-api", agent.api); node.classList.toggle("is-painting", agent.painting); node.classList.toggle("is-error", Boolean(agent.error));
      setText(node.querySelector(".mc-agent-phase"), phase + age);
      setText(node.querySelector(".mc-agent-count"), `${agent.calls} calls · +${count(agent.corrected)} px`);
      node.title = [phase, requestTiming(agent), agent.plan || agent.task].filter(Boolean).join(" · ");
      const graphPhase = agent.api && agent.painting ? "API + brush" : !agent.api && phase?.includes("waiting") ? "queued" : phase;
      for (const label of root.querySelectorAll(`[data-phase-agent="${agent.id}"]`)) setText(label, graphPhase + age);
    }
  }
  function renderInspector() {
    const agent = (frame.agents || []).find(agent => agent.id === selected);
    if (!agent) {
      setText(role("inspector-model"), selected === "board" ? "SHARED WORK BOARD" : "ALL AGENTS / LIVE EVENT STREAM");
      setText(role("inspector-task"), selected === "board" ? "Durable work intentions, progress updates, and handoffs." : "Select an agent to inspect its task, streaming response, and activity.");
      setText(role("stream"), `${count(frame.metrics?.api_errors)} API errors · ${count(frame.metrics?.no_actions)} no-action responses`); return;
    }
    setText(role("inspector-model"), `${agent.id} · ${agent.model}`);
    setText(role("inspector-task"), agent.error || agent.plan || agent.task || "No work intention announced yet.");
    const stream = agent.stream || {};
    const drafting = (stream.tool_names || []).filter(Boolean).join(", ");
    const context = ` · ${count(agent.context_tokens)} context tokens · ${agent.context_compactions || 0} compactions`;
    setText(role("stream"), (agent.api ? `${requestPhase(agent)}${drafting ? `: ${drafting}` : ""} · ${requestTiming(agent)} · ${count(stream.output_chars)} streamed chars` : `${agent.state} · ${count(agent.corrected)} net pixels corrected · ${agent.pending} paint jobs queued`) + context);
  }
  function renderBoard() {
    const entries = frame.board || [], owners = new Set(entries.map(entry => entry.agent)), cards = $(".mc-work-cards");
    for (const [owner, node] of boardNodes) if (!owners.has(owner)) { node.remove(); boardNodes.delete(owner); }
    const choice = role("board-filter").value;
    let shown = 0;
    for (const entry of entries) {
      let node = boardNodes.get(entry.agent);
      if (!node) {
        node = document.createElement("button"); node.type = "button"; node.className = "mc-work-card"; node.dataset.owner = entry.agent;
        node.innerHTML = '<div class="mc-work-card-top"><b></b><span class="mc-work-status"></span></div><p class="mc-work-description"></p><div class="mc-work-region"></div><div class="mc-work-progress"><i></i></div><div class="mc-work-numbers"></div><div class="mc-work-update"></div><div class="mc-work-overlap"></div>';
        node.onclick = () => selectAgent(entry.agent); cards.append(node); boardNodes.set(entry.agent, node);
      }
      const done = entry.status === "reported_done", conflicts = entry.overlaps || [];
      const visible = choice === "all" || (choice === "active" && !done) || (choice === "conflicts" && conflicts.length > 0) || (choice === "done" && done);
      node.style.display = visible ? "" : "none"; if (visible) shown++;
      node.classList.toggle("has-overlap", conflicts.length > 0); node.classList.toggle("is-selected", selected === entry.agent);
      setText(node.querySelector("b"), entry.agent);
      setText(node.querySelector(".mc-work-status"), entry.error ? "Needs attention" : done ? "Reported done" : entry.progress?.wrong === 0 ? "Area correct" : entry.state);
      setText(node.querySelector(".mc-work-description"), entry.description);
      const r = entry.region;
      setText(node.querySelector(".mc-work-region"), r ? `rows ${r.row}–${r.row + r.height - 1} · cols ${r.col}–${r.col + r.width - 1}` : "Worker is choosing its scope");
      const progress = entry.progress;
      node.querySelector(".mc-work-progress i").style.width = `${progress ? 100 * progress.matched / Math.max(1, progress.total) : 0}%`;
      setText(node.querySelector(".mc-work-numbers"), progress ? `${count(progress.matched)} / ${count(progress.total)} correct · ${count(progress.wrong)} remaining` : "Awaiting an announced work area");
      setText(node.querySelector(".mc-work-update"), entry.error || entry.update || "");
      setText(node.querySelector(".mc-work-overlap"), conflicts.length ? `Advisory overlap with ${conflicts.join(", ")}` : "");
    }
    setText(role("board-count"), entries.length);
    setText(role("board-summary"), `${shown} intentions shown · ${entries.filter(entry => (entry.overlaps || []).length).length} with overlap · click a card to inspect its agent`);
    let empty = cards.querySelector(".mc-board-empty");
    if (!shown && !empty) { empty = document.createElement("p"); empty.className = "mc-board-empty"; empty.textContent = "Work intentions appear here as agents announce their plans."; cards.append(empty); }
    if (shown && empty) empty.remove();
  }
  function renderTimeline() {
    const host = $(".mc-timeline"), elapsed = Number(frame.elapsed || 0), start = Math.max(0, elapsed - 60), duration = Math.max(1, elapsed - start);
    const ids = selected !== "all" && selected !== "board" ? [selected] : (frame.agents || []).filter(a => a.api || a.painting).slice(0, 8).map(a => a.id);
    host.replaceChildren();
    for (const id of ids) {
      const row = document.createElement("div"); row.className = "mc-time-row"; const name = document.createElement("span"); name.className = "mc-time-name"; name.textContent = id;
      const track = document.createElement("div"); track.className = "mc-time-track";
      for (const span of frame.spans || []) {
        if (span.agent !== id || !["api", "paint"].includes(span.kind)) continue;
        const end = span.end ?? elapsed; if (end < start) continue;
        const bar = document.createElement("i"); bar.className = `mc-time-span ${span.kind}`;
        bar.style.left = `${100 * Math.max(0, span.start - start) / duration}%`;
        bar.style.width = `${Math.max(.5, 100 * (end - Math.max(start, span.start)) / duration)}%`;
        bar.title = `${id} ${span.kind}: ${span.start.toFixed(2)}–${end.toFixed(2)}s`; track.append(bar);
      }
      row.append(name, track); host.append(row);
    }
  }
  function update() {
    const next = model.get("frame") || {}; if (!next.metrics) return;
    if (runId !== next.run_id || Number(next.sequence) < priorSequence) { eventCache.clear(); logNodes.clear(); log.replaceChildren(); follow = true; pendingEvents = 0; }
    frame = next; receivedAt = performance.now(); runId = frame.run_id; priorSequence = Number(frame.sequence);
    const metrics = frame.metrics, workforce = frame.workforce || {};
    setText(role("status"), frame.reason); $(".mc-status").classList.toggle("is-running", Boolean(frame.running));
    setText(role("jobs"), `${workforce.active_count || 0}/${workforce.total_workers || 0}`);
    setText(role("unused"), `${(workforce.available_workers || []).length} available · ${(workforce.never_started_workers || []).length} never started`);
    setText(role("api"), metrics.api); setText(role("api-errors"), `${count(metrics.api_errors)} errors · ${count(metrics.compactions)} context compactions`);
    setText(role("painting"), metrics.painting); setText(role("queued"), `${count(metrics.queued)} jobs pending`);
    setText(role("remaining"), count(metrics.wrong)); setText(role("calls"), `${count(metrics.requests)} model requests`);
    setText(role("repaired"), `${metrics.repaired.toFixed(1)}%`); setText(role("corrected"), `+${count(metrics.corrected)} net correct pixels`);
    $(".mc-progress-fill").style.width = `${Math.max(0, Math.min(100, metrics.repaired))}%`;
    if (frame.canvas_png && canvas.src !== frame.canvas_png) canvas.src = frame.canvas_png;
    if ($(".mc-overlay-host").dataset.version !== frame.overlay) { $(".mc-overlay-host").innerHTML = frame.overlay || ""; $(".mc-overlay-host").dataset.version = frame.overlay || ""; }
    if ($(".mc-network-host").dataset.version !== frame.network) { $(".mc-network-host").innerHTML = frame.network || ""; $(".mc-network-host").dataset.version = frame.network || ""; }
    if (activityDialog.open) $(".mc-expanded-network").innerHTML = frame.network || "";
    setText(role("crew-count"), `${(frame.agents || []).length} agents · click to inspect`);
    for (const event of frame.events || []) { if (!eventCache.has(event.id) && matches(event) && !follow) pendingEvents++; eventCache.set(event.id, event); }
    while (eventCache.size > 6000) eventCache.delete(eventCache.keys().next().value);
    renderLogs(); renderBoard(); renderTimeline(); tick();
  }
  function tick() { setText(role("clock"), clock(elapsedNow())); renderAgentActivity(); renderInspector(); }
  const timer = setInterval(tick, 500);
  model.on("change:frame", update); update();
  return () => { clearInterval(timer); model.off("change:frame", update); exitFallback(); document.removeEventListener("fullscreenchange", fullscreenLabel); document.removeEventListener("keydown", fullscreenEscape); root.remove(); };
}

export default { render };
