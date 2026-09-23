// The shell: picks a session, keeps the feeds fed, and hands one bucket to
// each view.
//
// Everything on screen is a reading of one selected bucket, or of the axis
// that selects it. The flame graph and the call graph are two ways of looking
// at the same call tree over the same two seconds.

import { Feed } from "./feed.js";
import { fold, totalSelfNs, GRANULARITIES, DEFAULT_GRANULARITY_MS } from "./buckets.js";
import { Palette, groupOf } from "./palette.js";
import { Flame, buildTree } from "./flame.js";
import { Strip, cpuSeries, cpuMeasure, memorySeries } from "./timeline.js";
import { Force, graphOf, DEFAULT_BUDGET } from "./force.js";

const SESSION_POLL_MS = 3000;
const FALLBACK_POLL_MS = 1000;
const RECONNECT_MS = 1500;
const REDRAW_MS = 250;

const el = (id) => document.getElementById(id);
const ui = {
  sessions: el("sessions"),
  granularity: el("granularity"),
  resolution: el("resolution"),
  live: el("live"),
  link: el("link"),
  detail: el("detail"),
  tabs: { resources: el("tab-resources"), calls: el("tab-calls") },
  panels: { resources: el("panel-resources"), calls: el("panel-calls") },
  flame: el("flame"),
  cpuStrip: el("cpu-strip"),
  memoryStrip: el("memory-strip"),
  graph: el("graph"),
  flameNote: el("flame-note"),
  flameWrap: el("flame-wrap"),
  flat: el("flat"),
  graphWrap: el("graph-wrap"),
  graphFlat: el("graph-flat"),
  cpuStripLabel: el("cpu-strip-label"),
  axisNote: el("axis-note"),
  graphNote: el("graph-note"),
  legend: el("legend"),
  flameTable: el("flame-table"),
  graphTable: el("graph-table"),
  axisStart: el("axis-start"),
  axisEnd: el("axis-end"),
  threading: el("threading"),
  tip: el("tip"),
};

const view = {
  chosenId: null,
  session: null,
  feeds: new Map(),
  socket: null,
  fallback: null,
  bucketMs: DEFAULT_GRANULARITY_MS,
  resolution: "fine",
  selected: -1,
  following: true,
  tab: "resources",
  buckets: [],
  palette: new Palette(),
  graph: { nodes: [], edges: [], hidden: 0 },
};

const flame = new Flame(ui.flame, null);
const force = new Force(ui.graph);
const cpuStrip = new Strip(ui.cpuStrip, {
  height: 64,
  colorOf: (group) => view.palette.colorOf(group),
});
const memoryStrip = new Strip(ui.memoryStrip, {
  height: 48,
  colorOf: (name) => typeColor(name),
});

// ---- formatting ----

const bytes = (value) => {
  const units = ["B", "KiB", "MiB", "GiB"];
  let at = 0;
  let scaled = value;
  while (scaled >= 1024 && at < units.length - 1) {
    scaled /= 1024;
    at += 1;
  }
  return `${scaled < 10 && at > 0 ? scaled.toFixed(1) : Math.round(scaled)} ${units[at]}`;
};

const duration = (ns) => {
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(2)} s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(1)} ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(1)} µs`;
  return `${Math.round(ns)} ns`;
};

const seconds = (tNs) => `${(tNs / 1e9).toFixed(1)}s`;

/// Types get the categorical hues from the end of the order, so a type and a
/// module never share a slot on screen at the same time.
const TYPE_HUES = ["#e66767", "#9085e9", "#c98500", "#199e70"];
const typeSlots = new Map();
function typeColor(name) {
  if (!typeSlots.has(name)) typeSlots.set(name, typeSlots.size);
  const slot = typeSlots.get(name);
  return slot < TYPE_HUES.length ? TYPE_HUES[slot] : "#6d7684";
}

// ---- data ----

async function json(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  return response.json();
}

async function refreshSessions() {
  try {
    const { sessions } = await json("/api/sessions");
    chooseSession(sessions);
  } catch {
    setState("connecting");
  }
}

function shapeOf(session) {
  return session ? `${session.cpu?.process_id ?? "-"}|${session.memory?.process_id ?? "-"}` : "";
}

function chooseSession(sessions) {
  paintPicker(sessions);
  const chosen =
    sessions.find((session) => session.id === view.chosenId) ??
    sessions.find((session) => session.connected) ??
    sessions[0] ??
    null;
  if (shapeOf(chosen) !== shapeOf(view.session)) attach(chosen);
  else view.session = chosen;
}

function paintPicker(sessions) {
  const wanted = sessions.map((session) => `${session.id}:${session.connected}`).join(",");
  if (ui.sessions.dataset.shape === wanted) return;
  ui.sessions.dataset.shape = wanted;
  ui.sessions.replaceChildren();
  for (const session of sessions) {
    const option = document.createElement("option");
    option.value = session.id;
    option.textContent = `${session.app} · pid ${session.pid}${session.connected ? "" : " (ended)"}`;
    ui.sessions.append(option);
  }
  if (view.chosenId) ui.sessions.value = view.chosenId;
}

async function attach(session) {
  view.socket?.close();
  view.socket = null;
  stopFallback();
  view.session = session;
  view.feeds = new Map();
  view.palette = new Palette();
  view.selected = -1;
  view.following = true;
  force.placed.clear();

  if (!session) {
    setState("idle");
    return;
  }
  for (const feed of ["cpu", "memory"]) {
    if (session[feed]) view.feeds.set(feed, new Feed(session[feed].process_id));
  }
  await Promise.all([...view.feeds.values()].map(loadSnapshot));
  connect(session);
}

async function loadSnapshot(feed) {
  try {
    const query = `?resolution=${view.resolution}&limit=900`;
    feed.applySnapshot(await json(`/api/processes/${feed.processId}/snapshot${query}`));
  } catch {
    // A snapshot that fails is not fatal: the stream refills the same state.
  }
}

function connect(session) {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/api/sessions/${session.id}/stream`);
  view.socket = socket;
  setState("connecting");

  socket.onopen = () => {
    stopFallback();
    setState("streaming");
  };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    const feed = view.feeds.get(message.feed);
    if (!feed) return;
    if (message.type === "window") feed.applyWindow(message.window);
    if (message.type === "process_ended" && everyFeedEnded(message)) setState("ended");
    if (feed.needsNames()) loadSnapshot(feed);
  };
  socket.onclose = () => {
    if (view.socket !== socket) return;
    startFallback();
    setTimeout(() => {
      if (view.socket === socket && view.session) connect(view.session);
    }, RECONNECT_MS);
  };
}

function everyFeedEnded(message) {
  const session = view.session;
  if (!session) return true;
  return ["cpu", "memory"].every(
    (feed) => !session[feed] || session[feed].process_id === message.process_id,
  );
}

function startFallback() {
  if (view.fallback) return;
  setState("polling");
  view.fallback = setInterval(() => {
    for (const feed of view.feeds.values()) loadSnapshot(feed);
  }, FALLBACK_POLL_MS);
}

function stopFallback() {
  if (!view.fallback) return;
  clearInterval(view.fallback);
  view.fallback = null;
}

function setState(state) {
  ui.link.dataset.state = state;
  ui.link.textContent = state;
}

// ---- painting ----

function paint() {
  const session = view.session;
  if (!session) {
    ui.detail.textContent = "No session. Start a target with the probe, or point the bridge at one.";
    return;
  }

  const cpu = view.feeds.get("cpu");
  const memory = view.feeds.get("memory");
  const emitters = ["cpu", "memory"]
    .filter((feed) => session[feed])
    .map((feed) => session[feed].source);
  ui.detail.textContent =
    `${session.app} · pid ${session.pid} · ${[...new Set(emitters)].join(" + ") || "no emitter"}`;

  // Stacks come from the probe. The hotpath bridge has no call tree, which is
  // a property of its source rather than something to work around here.
  const treeFeed = [cpu, memory].find((feed) => feed && feed.paths.size > 0) ?? cpu ?? memory;
  if (!treeFeed) return;
  flame.feed = treeFeed;
  view.palette.learn(treeFeed.functions);

  view.buckets = fold(treeFeed.windows, treeFeed.windowMs, view.bucketMs);
  const memoryBuckets = memory
    ? fold(memory.windows, memory.windowMs, view.bucketMs)
    : view.buckets;

  if (view.following || view.selected < 0 || view.selected >= view.buckets.length) {
    view.selected = view.buckets.length - 1;
  }
  const bucket = view.buckets[view.selected];

  paintAxis(treeFeed, memory ?? treeFeed, memoryBuckets);
  if (view.tab === "resources") paintFlame(treeFeed, bucket);
  else paintGraph(treeFeed, bucket);

  const cores = treeFeed.cores();
  ui.threading.textContent =
    `A call tree is process-wide, so several busy threads put more than a second of ` +
    `self time into a second of wall clock. Widths are a share of the bucket, never of the clock. ` +
    `Right now: ${cores.toFixed(2)} cores' worth.`;
}

function paintAxis(treeFeed, memoryFeed, memoryBuckets) {
  const cpu = cpuSeries(treeFeed, view.buckets, view.palette);
  const mem = memorySeries(memoryFeed, memoryBuckets, view.palette);

  cpuStrip.selected = view.selected;
  memoryStrip.selected = view.selected;
  cpuStrip.draw(cpu);
  memoryStrip.draw(mem);

  const first = view.buckets[0];
  const last = view.buckets[view.buckets.length - 1];
  ui.axisStart.textContent = first ? seconds(first.startTNs) : "";
  ui.axisEnd.textContent = last ? `${seconds(last.endTNs)} since start` : "";

  ui.cpuStripLabel.textContent = `cpu · ${cpuMeasure(treeFeed)}`;

  const span = (view.buckets.length * view.bucketMs) / 1000;
  const census = mem.keys.length > 0 ? "" : " No census in this session, so the memory strip is empty.";
  ui.axisNote.textContent =
    `${view.buckets.length} buckets of ${view.bucketMs / 1000}s, ${span.toFixed(0)}s in all, ` +
    `folded from ${treeFeed.windowMs}ms windows. Click a bucket to pin it.${census}`;

  ui.legend.replaceChildren(
    ...view.palette.legend().map((entry) => swatch(entry.color, entry.group)),
    ...mem.keys.map((name) => swatch(typeColor(name), `${name} (bytes)`)),
  );
}

function swatch(color, label) {
  const li = document.createElement("li");
  const dot = document.createElement("span");
  dot.className = "swatch";
  dot.style.background = color;
  li.append(dot, document.createTextNode(label));
  return li;
}

function paintFlame(feed, bucket) {
  if (!bucket) return;

  // A source with no call tree gets the list it can actually support, and an
  // explanation. Drawing an empty flame graph and calling the program idle
  // would be a lie about the program rather than about the feed.
  if (!feed.hasTree()) {
    ui.flameWrap.hidden = true;
    ui.flameNote.textContent = "";
    paintFlatFunctions(feed, bucket);
    return;
  }
  ui.flameWrap.hidden = false;
  ui.flat.replaceChildren();

  const tree = buildTree(feed, bucket);
  flame.draw(tree, view.palette);

  const total = totalSelfNs(bucket);
  ui.flameNote.textContent = total
    ? `${duration(total)} of self time across ${tree.count} calling contexts, ` +
      `in the ${view.bucketMs / 1000}s from ${seconds(bucket.startTNs)}. Width is a share of that.`
    : "Nothing ran in this bucket.";

  paintFlameTable(feed, tree);
}

/// The degraded view: what a feed carrying only per-function totals can say.
function paintFlatFunctions(feed, bucket) {
  const rows = [...bucket.calls]
    .map(([funcId, stat]) => ({ funcId, ...stat }))
    .sort((a, b) => b.ns - a.ns);

  const seconds_ = view.bucketMs / 1000;
  ui.flat.replaceChildren(
    why(
      `This session's only emitter is ${sourcesOf(view.session)}, which reports each ` +
        `function's own totals and nothing about which function called which. ` +
        `There is no call tree here, so there is no flame graph and no call graph ` +
        `to draw — not because the program was idle, but because this source cannot ` +
        `see it. Run the target with the Rust probe (#[lightwatch::measure]) for ` +
        `stacks, edges and a live-object census.`,
    ),
    table(
      ["function", "module", "time in bucket", "calls"],
      rows.slice(0, 40).map((row) => [
        { text: feed.nameOf(row.funcId), className: "chain" },
        { text: groupOf(feed.functions.get(row.funcId)) },
        { text: duration(row.ns), className: "num" },
        { text: `${(row.calls / seconds_).toFixed(0)}/s`, className: "num" },
      ]),
    ),
  );
}

function sourcesOf(session) {
  const names = ["cpu", "memory"]
    .filter((feed) => session?.[feed])
    .map((feed) => session[feed].source);
  return [...new Set(names)].join(" and ") || "an unknown emitter";
}

function why(text) {
  const p = document.createElement("p");
  p.className = "why";
  p.textContent = text;
  return p;
}

function paintFlameTable(feed, tree) {
  const rows = [];
  const walk = (node) => {
    rows.push(node);
    node.children.forEach(walk);
  };
  tree.roots.forEach(walk);
  rows.sort((a, b) => b.inclusive - a.inclusive);

  ui.flameTable.replaceChildren(
    table(
      ["context", "inclusive", "self", "calls"],
      rows.slice(0, 60).map((node) => [
        { text: feed.chainOf(node.id).join(" › "), className: "chain" },
        { text: duration(node.inclusive), className: "num" },
        { text: duration(node.selfNs), className: "num" },
        { text: node.calls.toLocaleString(), className: "num" },
      ]),
    ),
  );
}

function paintGraph(feed, bucket) {
  if (!bucket) return;

  if (!feed.hasTree()) {
    ui.graphWrap.hidden = true;
    ui.graphNote.textContent = "";
    ui.graphTable.replaceChildren();
    ui.graphFlat.replaceChildren(
      why(
        `Nothing to lay out. ${sourcesOf(view.session)} carries no call tree, so this ` +
          `daemon has never been told that one of these functions called another. ` +
          `The Rust probe records that from a thread-local stack; a bridge over an ` +
          `external profiler has no stack to read.`,
      ),
    );
    return;
  }
  ui.graphWrap.hidden = false;
  ui.graphFlat.replaceChildren();

  view.graph = graphOf(feed, bucket, DEFAULT_BUDGET);
  force.settle(view.graph);
  drawGraph(feed);

  const hidden = view.graph.hidden
    ? `, ${view.graph.hidden} quieter ones left out`
    : "";
  ui.graphNote.textContent = view.graph.nodes.length
    ? `${view.graph.nodes.length} functions and ${view.graph.edges.length} edges ran in the ` +
      `${view.bucketMs / 1000}s from ${seconds(bucket.startTNs)}${hidden}. Positions are kept between buckets.`
    : "Nothing ran in this bucket.";

  const edges = [...view.graph.edges].sort((a, b) => b.calls - a.calls);
  ui.graphTable.replaceChildren(
    table(
      ["caller", "callee", "calls"],
      edges.slice(0, 80).map((edge) => [
        { text: feed.nameOf(edge.from), className: "chain" },
        { text: feed.nameOf(edge.to), className: "chain" },
        { text: edge.calls.toLocaleString(), className: "num" },
      ]),
    ),
  );
}

function drawGraph(feed) {
  const canvas = ui.graph;
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, width, height);

  const graph = view.graph;
  const busiest = Math.max(1, ...graph.nodes.map((node) => node.selfNs));
  const heaviest = Math.max(1, ...graph.edges.map((edge) => edge.calls));

  ctx.strokeStyle = "#39414f";
  for (const edge of graph.edges) {
    const from = force.placed.get(edge.from);
    const to = force.placed.get(edge.to);
    if (!from || !to) continue;
    ctx.lineWidth = 0.6 + (edge.calls / heaviest) * 2.4;
    ctx.beginPath();
    ctx.moveTo(from.x, from.y);
    ctx.lineTo(to.x, to.y);
    ctx.stroke();
    arrow(ctx, from, to);
  }

  // Labels go on the busiest handful only. A name under every one of two
  // hundred nodes is a grey haze, and the table below and the hover layer
  // already answer "which one is that".
  const labelled = new Set(
    [...graph.nodes].sort((a, b) => b.selfNs - a.selfNs).slice(0, 12).map((node) => node.func),
  );

  ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, monospace";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  graph.hitboxes = [];
  for (const node of graph.nodes) {
    const point = force.placed.get(node.func);
    if (!point) continue;
    // A fourth root, not a square root. One function that sleeps can hold a
    // hundred times the self time of everything around it, and a linear or
    // square-root scale against that maximum renders every other node as the
    // same 4px dot. Compressing the range keeps the ordering readable and
    // still says which is biggest.
    const radius = 5 + Math.pow(node.selfNs / busiest, 0.25) * 13;
    const fn = feed.functions.get(node.func);
    const hovered = force.hover === node.func;
    ctx.fillStyle = hovered ? "#ffffff" : view.palette.colorOf(groupOf(fn));
    ctx.beginPath();
    ctx.arc(point.x, point.y, radius, 0, Math.PI * 2);
    ctx.fill();
    // A 2px surface ring, so two overlapping nodes stay two nodes.
    ctx.strokeStyle = "#161b22";
    ctx.lineWidth = 2;
    ctx.stroke();
    // The hit target is bigger than the mark, so a small node is still
    // reachable with a mouse.
    graph.hitboxes.push({ func: node.func, x: point.x, y: point.y, r: Math.max(radius, 10) });

    if (labelled.has(node.func) || hovered) {
      ctx.fillStyle = "#dfe5ee";
      ctx.fillText(feed.nameOf(node.func), point.x, point.y + radius + 9);
    }
  }
  ctx.textAlign = "left";
}

function arrow(ctx, from, to) {
  const angle = Math.atan2(to.y - from.y, to.x - from.x);
  const back = 16;
  const tipX = to.x - Math.cos(angle) * back;
  const tipY = to.y - Math.sin(angle) * back;
  ctx.beginPath();
  ctx.moveTo(tipX, tipY);
  ctx.lineTo(tipX - Math.cos(angle - 0.4) * 7, tipY - Math.sin(angle - 0.4) * 7);
  ctx.moveTo(tipX, tipY);
  ctx.lineTo(tipX - Math.cos(angle + 0.4) * 7, tipY - Math.sin(angle + 0.4) * 7);
  ctx.stroke();
}

function table(headers, rows) {
  const element = document.createElement("table");
  const head = element.createTHead().insertRow();
  for (const header of headers) {
    const th = document.createElement("th");
    th.textContent = header;
    head.append(th);
  }
  const body = element.createTBody();
  for (const cells of rows) {
    const row = body.insertRow();
    for (const cell of cells) {
      const td = row.insertCell();
      td.textContent = cell.text;
      if (cell.className) td.className = cell.className;
    }
  }
  return element;
}

// ---- hover ----

function showTip(event, title, chain, pairs) {
  ui.tip.hidden = false;
  ui.tip.replaceChildren();

  const head = document.createElement("p");
  head.className = "head";
  head.textContent = title;
  head.style.margin = "0";
  ui.tip.append(head);

  if (chain) {
    const line = document.createElement("p");
    line.className = "chain";
    line.textContent = chain;
    line.style.margin = "4px 0 0";
    ui.tip.append(line);
  }

  const list = document.createElement("dl");
  for (const [label, value] of pairs) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value;
    list.append(dt, dd);
  }
  ui.tip.append(list);

  const box = ui.tip.getBoundingClientRect();
  const x = Math.min(event.clientX + 14, window.innerWidth - box.width - 8);
  const y = Math.min(event.clientY + 14, window.innerHeight - box.height - 8);
  ui.tip.style.transform = `translate(${x}px, ${y}px)`;
}

const hideTip = () => {
  ui.tip.hidden = true;
};

ui.flame.addEventListener("mousemove", (event) => {
  const rect = ui.flame.getBoundingClientRect();
  const box = flame.at(event.clientX - rect.left, event.clientY - rect.top);
  flame.hover = box;
  if (!box) {
    hideTip();
    return;
  }
  const feed = flame.feed;
  const bucket = view.buckets[view.selected];
  const total = bucket ? totalSelfNs(bucket) : 0;
  showTip(event, feed.nameOf(box.node.func), feed.chainOf(box.node.id).join(" › "), [
    ["inclusive", duration(box.node.inclusive)],
    ["self", duration(box.node.selfNs)],
    ["calls", box.node.calls.toLocaleString()],
    ["share of bucket", total ? `${((box.node.inclusive / total) * 100).toFixed(1)}%` : "—"],
  ]);
});
ui.flame.addEventListener("mouseleave", () => {
  flame.hover = null;
  hideTip();
});

for (const [strip, canvas] of [
  [cpuStrip, ui.cpuStrip],
  [memoryStrip, ui.memoryStrip],
]) {
  canvas.addEventListener("mousemove", (event) => {
    const rect = canvas.getBoundingClientRect();
    const at = strip.indexAt(event.clientX - rect.left);
    cpuStrip.hover = at;
    memoryStrip.hover = at;
    const bucket = view.buckets[at];
    if (!bucket) {
      hideTip();
      return;
    }
    const memoryBucket = bucket;
    const live = [...memoryBucket.census].map(
      ([ty, reading]) =>
        [flame.feed?.typeNameOf(ty) ?? `type ${ty}`, `${reading.live.toLocaleString()} · ${bytes(reading.bytes)}`],
    );
    showTip(event, `${seconds(bucket.startTNs)} – ${seconds(bucket.endTNs)}`, null, [
      ["cpu self time", duration(totalSelfNs(bucket))],
      ...live,
    ]);
  });
  canvas.addEventListener("mouseleave", () => {
    cpuStrip.hover = -1;
    memoryStrip.hover = -1;
    hideTip();
  });
  canvas.addEventListener("click", (event) => {
    const rect = canvas.getBoundingClientRect();
    const at = strip.indexAt(event.clientX - rect.left);
    if (at < 0) return;
    view.selected = at;
    view.following = at === view.buckets.length - 1;
    ui.live.dataset.on = String(view.following);
    paint();
  });
}

ui.graph.addEventListener("mousemove", (event) => {
  const rect = ui.graph.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const hit = (view.graph.hitboxes ?? []).find(
    (box) => Math.hypot(box.x - x, box.y - y) <= box.r,
  );
  force.hover = hit?.func ?? null;
  if (!hit || !flame.feed) {
    hideTip();
    return;
  }
  const feed = flame.feed;
  const node = view.graph.nodes.find((held) => held.func === hit.func);
  const callers = view.graph.edges.filter((edge) => edge.to === hit.func);
  const callees = view.graph.edges.filter((edge) => edge.from === hit.func);
  showTip(event, feed.nameOf(hit.func), groupOf(feed.functions.get(hit.func)), [
    ["self time", duration(node?.selfNs ?? 0)],
    ["calls", (node?.calls ?? 0).toLocaleString()],
    ["called by", callers.map((edge) => feed.nameOf(edge.from)).join(", ") || "nothing measured"],
    ["calls into", callees.map((edge) => feed.nameOf(edge.to)).join(", ") || "nothing measured"],
  ]);
  drawGraph(feed);
});
ui.graph.addEventListener("mouseleave", () => {
  force.hover = null;
  hideTip();
  if (flame.feed) drawGraph(flame.feed);
});

// ---- controls ----

for (const ms of GRANULARITIES) {
  const option = document.createElement("option");
  option.value = String(ms);
  option.textContent = ms >= 1000 ? `${ms / 1000}s` : `${ms}ms`;
  ui.granularity.append(option);
}
ui.granularity.value = String(DEFAULT_GRANULARITY_MS);

ui.granularity.addEventListener("change", () => {
  view.bucketMs = Number(ui.granularity.value);
  // Re-folding is local: no refetch, and the same nanoseconds regrouped.
  view.following = true;
  ui.live.dataset.on = "true";
  paint();
});

ui.resolution.addEventListener("change", async () => {
  view.resolution = ui.resolution.value;
  await Promise.all([...view.feeds.values()].map(loadSnapshot));
  view.following = true;
  ui.live.dataset.on = "true";
  paint();
});

ui.live.addEventListener("click", () => {
  view.following = !view.following;
  ui.live.dataset.on = String(view.following);
  paint();
});

ui.sessions.addEventListener("change", () => {
  view.chosenId = ui.sessions.value;
  refreshSessions();
});

for (const [name, tab] of Object.entries(ui.tabs)) {
  tab.addEventListener("click", () => {
    view.tab = name;
    for (const [other, button] of Object.entries(ui.tabs)) {
      button.setAttribute("aria-selected", String(other === name));
      ui.panels[other].hidden = other !== name;
    }
    if (name === "calls") force.alpha = Math.max(force.alpha, 0.6);
    paint();
  });
}

// The simulation runs only while it is moving and only while its tab is
// showing, so a settled graph costs nothing.
function animate() {
  if (view.tab === "calls" && flame.feed && force.tick(view.graph)) drawGraph(flame.feed);
  requestAnimationFrame(animate);
}

refreshSessions();
setInterval(refreshSessions, SESSION_POLL_MS);
setInterval(paint, REDRAW_MS);
requestAnimationFrame(animate);
