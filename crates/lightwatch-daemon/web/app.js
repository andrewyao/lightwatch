// Drives the page from one session's stream, with a snapshot poll as the
// fallback whenever that socket is not up.

import { Feed } from "./feed.js";

const REDRAW_MS = 200;
const SESSION_POLL_MS = 3000;
const FALLBACK_POLL_MS = 1000;
const RECONNECT_MS = 1500;
const ROWS = 14;

const el = {
  sessions: document.getElementById("sessions"),
  link: document.getElementById("link"),
  detail: document.getElementById("detail"),
  cpu: document.getElementById("cpu"),
  cpuNote: document.getElementById("cpu-note"),
  memory: document.getElementById("memory"),
  memoryNote: document.getElementById("memory-note"),
};

const view = {
  sessions: [],
  session: null,
  chosenId: null,
  feeds: new Map(), // "cpu" | "memory" -> Feed
  socket: null,
  state: "connecting",
  fallback: null,
};

async function json(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return response.json();
}

// The set of process ids a session is made of. Re-attaching on a change is
// what picks up the other emitter when it starts after the page loaded.
function shape(session) {
  if (!session) return "";
  return ["cpu", "memory"].map((feed) => session[feed]?.process_id ?? "-").join("|");
}

// One at a time. Each refresh awaits a fetch and then a snapshot per feed, so
// two of them overlapping can attach in one order and repaint in the other,
// which shows as a picker naming a session the page is not watching.
let refreshing = false;

async function refreshSessions() {
  if (refreshing) return;
  refreshing = true;
  try {
    await chooseSession();
  } finally {
    refreshing = false;
  }
}

async function chooseSession() {
  let sessions;
  try {
    ({ sessions } = await json("/api/sessions"));
  } catch {
    // The daemon is down or restarting. The socket's own close already put the
    // page into its fallback; the next tick is what reattaches.
    return;
  }
  view.sessions = sessions;

  const wanted =
    sessions.find((s) => s.id === view.chosenId) ??
    sessions.find((s) => s.connected) ??
    sessions[0] ??
    null;
  if (shape(wanted) !== shape(view.session)) await attach(wanted);
  else view.session = wanted;
}

// The selection is written into the markup rather than assigned after it, and
// painted in the same pass as everything else, so the picker cannot name a
// session the rest of the page is not showing.
function paintPicker() {
  const options = view.sessions
    .map((session) => {
      const halves = ["cpu", "memory"].filter((feed) => session[feed]).join(" + ") || "none";
      const live = session.connected ? "" : " (ended)";
      const chosen = session.id === view.session?.id ? " selected" : "";
      return `<option value="${session.id}"${chosen}>${session.app} · pid ${session.pid} · ${halves}${live}</option>`;
    })
    .join("");
  if (el.sessions.innerHTML !== options) el.sessions.innerHTML = options;
}

async function attach(session) {
  view.socket?.close();
  view.socket = null;
  stopFallback();
  view.session = session;
  view.feeds = new Map();
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
    feed.applySnapshot(await json(`/api/processes/${feed.processId}/snapshot`));
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
  const others = [...view.feeds.keys()].filter((feed) => feed !== message.feed);
  return others.every((feed) => !view.session?.[feed]?.connected);
}

// While the socket is down the snapshot route is the whole picture, so it is
// polled rather than left stale.
function startFallback() {
  if (view.fallback) return;
  setState("polling");
  view.fallback = setInterval(() => {
    for (const feed of view.feeds.values()) loadSnapshot(feed);
  }, FALLBACK_POLL_MS);
}

function stopFallback() {
  clearInterval(view.fallback);
  view.fallback = null;
}

function setState(state) {
  view.state = state;
  el.link.dataset.state = state;
  el.link.textContent = { streaming: "streaming", polling: "polling", connecting: "connecting", ended: "process ended", idle: "no session" }[state];
}

function bytes(n) {
  const units = ["B", "KB", "MB", "GB"];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${value} B` : `${value.toFixed(1)} ${units[unit]}`;
}

function row(name, primary, secondary, fraction) {
  const li = document.createElement("li");
  li.innerHTML = `
    <span class="name" title="${name}">${name}</span>
    <span class="value">${primary}</span>
    <span class="second name">${secondary.detail}</span>
    <span class="second value">${secondary.value}</span>
    <span class="track"><span class="fill" style="width:${Math.min(100, fraction * 100).toFixed(1)}%"></span></span>`;
  return li;
}

function paintRows(list, rows, empty) {
  list.replaceChildren();
  if (rows.length === 0) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = empty;
    list.append(li);
    return;
  }
  list.append(...rows);
}

function paint() {
  paintPicker();
  const session = view.session;
  if (!session) {
    el.detail.textContent = "No session. Start a target with the probe, or point the bridge at one.";
    paintRows(el.cpu, [], "");
    paintRows(el.memory, [], "");
    return;
  }

  const where = (feed) =>
    session[feed] ? `${feed} via ${session[feed].source} (${session[feed].process_id})` : `no ${feed} feed`;
  const superseded = session.superseded.length
    ? ` · ${session.superseded.length} superseded entry for the same pid`
    : "";
  el.detail.textContent = `${session.app} · pid ${session.pid} · ${where("cpu")} · ${where("memory")}${superseded}`;

  const cpuFeed = view.feeds.get("cpu");
  const cpuRows = cpuFeed ? cpuFeed.cpu() : [];
  el.cpuNote.textContent = cpuFeed
    ? "Share of one core over the last second, and calls per second."
    : "No timing feed in this session.";
  paintRows(
    el.cpu,
    cpuRows.slice(0, ROWS).map((fn) =>
      row(
        fn.name,
        `${(fn.cores * 100).toFixed(1)}%`,
        { detail: fn.module ?? "", value: `${fn.callsPerSec.toFixed(0)}/s` },
        fn.cores,
      ),
    ),
    cpuFeed ? "idle" : "nothing to show",
  );

  const memoryFeed = view.feeds.get("memory");
  const memoryRows = memoryFeed ? memoryFeed.memory() : [];
  const widest = Math.max(1, ...memoryRows.map((type) => type.bytes));
  const total = memoryRows.reduce((sum, type) => sum + type.bytes, 0);
  const counted = memoryRows.length === 1 ? "1 tracked type" : `${memoryRows.length} tracked types`;
  el.memoryNote.textContent = memoryFeed
    ? `Live instances now. ${counted}, ${bytes(total)} in all.`
    : "No census feed in this session. A bridge over an external profiler cannot take one.";
  paintRows(
    el.memory,
    memoryRows.slice(0, ROWS).map((type) =>
      row(
        type.name,
        `${type.live.toLocaleString()} live`,
        { detail: "", value: `~${bytes(type.bytes)}` },
        type.bytes / widest,
      ),
    ),
    memoryFeed ? "no type has reported a census yet" : "nothing to show",
  );
}

el.sessions.addEventListener("change", () => {
  view.chosenId = el.sessions.value;
  refreshSessions();
});

refreshSessions();
setInterval(refreshSessions, SESSION_POLL_MS);
setInterval(paint, REDRAW_MS);
