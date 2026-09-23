// One process's state, as much of it as the daemon still holds.
//
// The previous version kept a one-second sliding buffer and threw the rest
// away, because a list of the top fourteen functions needs nothing else. A
// time axis needs the history, so this keeps every window the daemon served
// and every window the stream has pushed since.

/// How many windows to retain. The coarse ring is nine hundred deep, and
/// nothing is served past that.
const MAX_WINDOWS = 900;

/// The trailing span a per-second rate is averaged over, for the header.
const RATE_WINDOW_MS = 1000;

export class Feed {
  constructor(processId) {
    this.processId = processId;
    this.functions = new Map();
    this.types = new Map();
    this.paths = new Map();
    this.windows = [];
    this.windowMs = 100;
    this.resolution = "fine";
    this.unknownIds = new Set();
    this.lastSeenAt = 0;
  }

  applySnapshot(snapshot) {
    this.windowMs = snapshot.resolution_ms || 100;
    this.resolution = snapshot.resolution || "fine";
    this.unknownIds.clear();

    for (const fn of snapshot.functions) {
      this.functions.set(fn.id, { name: fn.name, module: fn.module });
    }
    for (const type of snapshot.types) {
      this.types.set(type.id, { name: type.name });
    }
    for (const path of snapshot.paths ?? []) {
      this.paths.set(path.id, { parent: path.parent, func: path.func });
    }

    // The snapshot is the authority on history: it replaces what the stream
    // has pushed rather than merging with it, so a reconnect cannot leave a
    // window counted twice.
    this.windows = snapshot.windows.slice(-MAX_WINDOWS);
    this.lastSeenAt = performance.now();
  }

  applyWindow(window) {
    this.lastSeenAt = performance.now();

    // A window can be pushed again after a reconnect. Replacing by index
    // rather than appending keeps its deltas from being counted twice.
    const last = this.windows[this.windows.length - 1];
    if (last && window.index <= last.index) {
      const at = this.windows.findIndex((held) => held.index === window.index);
      if (at >= 0) this.windows[at] = window;
      return;
    }
    this.windows.push(window);
    if (this.windows.length > MAX_WINDOWS) this.windows.shift();

    for (const stack of window.stacks ?? []) {
      if (!this.paths.has(stack.path)) this.unknownIds.add(`path:${stack.path}`);
    }
    for (const call of window.calls ?? []) {
      if (!this.functions.has(call.func)) this.unknownIds.add(`func:${call.func}`);
    }
    for (const reading of window.census ?? []) {
      if (!this.types.has(reading.ty)) this.unknownIds.add(`type:${reading.ty}`);
    }
  }

  /// Whether this source carries a call tree.
  ///
  /// The Rust probe does. A bridge over an external profiler does not: it
  /// reports per-function totals and has no stack to report. Everything that
  /// needs a tree has to ask before drawing an empty one and calling the
  /// program idle.
  hasTree() {
    return this.paths.size > 0;
  }

  /// True when the stream named something this feed has no name for, which a
  /// fresh snapshot fixes.
  needsNames() {
    return this.unknownIds.size > 0;
  }

  nameOf(funcId) {
    return this.functions.get(funcId)?.name ?? `function ${funcId}`;
  }

  typeNameOf(typeId) {
    return this.types.get(typeId)?.name ?? `type ${typeId}`;
  }

  /// The chain of names leading to a context, outermost first.
  chainOf(pathId) {
    const chain = [];
    let at = pathId;
    // Bounded: a node's id always exceeds its parent's, so this descends.
    while (at && chain.length < 64) {
      const node = this.paths.get(at);
      if (!node) break;
      chain.push(this.nameOf(node.func));
      at = node.parent;
    }
    return chain.reverse();
  }

  /// Share of one core per function over the trailing second, for the header
  /// line. Self time, so the shares of a caller and its callee do not overlap
  /// and the total is what the process actually spent.
  cores(now = performance.now()) {
    const held = Math.max(1, Math.round(RATE_WINDOW_MS / this.windowMs));
    const recent = this.windows.slice(-held);
    let selfNs = 0;
    for (const window of recent) {
      for (const stack of window.stacks ?? []) selfNs += stack.self_ns;
    }
    const seconds = (recent.length * this.windowMs) / 1000;
    return seconds > 0 ? selfNs / 1e9 / seconds : 0;
  }
}
