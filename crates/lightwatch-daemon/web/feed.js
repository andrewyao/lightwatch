// What one emitter has told us, folded into what the page draws.
//
// Rates are computed here rather than read off a snapshot, because the stream
// only ever carries a window's own delta. Windows are kept by the wall clock
// they arrived on, not by their index: a function that stops being called
// stops producing windows, and its rate has to fall to zero on its own.

const RATE_WINDOW_MS = 1000;

export class Feed {
  constructor(processId) {
    this.processId = processId;
    this.functions = new Map();
    this.types = new Map();
    this.census = new Map();
    this.deltas = [];
    this.frames = 0;
    this.windowMs = 100;
    this.lastSeenAt = 0;
    this.unknownIds = new Set();
  }

  applySnapshot(snapshot) {
    this.frames = snapshot.process.counts.frames;
    this.windowMs = snapshot.resolution_ms || 100;
    this.unknownIds.clear();
    for (const fn of snapshot.functions) {
      this.functions.set(fn.id, { name: fn.name, module: fn.module });
    }
    for (const type of snapshot.types) {
      this.types.set(type.id, { name: type.name });
      if (type.census) this.census.set(type.id, type.census);
    }

    // Seed the ring from the snapshot's own windows, spaced as if they had
    // just arrived, so the first paint is not an empty one.
    const held = Math.max(1, Math.round(RATE_WINDOW_MS / this.windowMs));
    const recent = snapshot.windows.slice(-held);
    const now = performance.now();
    this.deltas = recent.map((window, index) => ({
      at: now - (recent.length - 1 - index) * this.windowMs,
      calls: window.calls,
    }));
    this.lastSeenAt = now;
  }

  applyWindow(window) {
    this.frames += 1;
    this.lastSeenAt = performance.now();
    if (window.calls.length > 0) {
      this.deltas.push({ at: this.lastSeenAt, calls: window.calls });
    }
    for (const reading of window.census) {
      this.census.set(reading.ty, { live: reading.live, bytes: reading.bytes });
      if (!this.types.has(reading.ty)) this.unknownIds.add(`type:${reading.ty}`);
    }
    for (const call of window.calls) {
      if (!this.functions.has(call.func)) this.unknownIds.add(`func:${call.func}`);
    }
  }

  // True when the stream named an id no snapshot has explained yet, which is
  // how a function registered after the page loaded shows up.
  needsNames() {
    return this.unknownIds.size > 0;
  }

  // Per function over the last second: share of one core, and calls.
  cpu(now = performance.now()) {
    const cutoff = now - RATE_WINDOW_MS;
    this.deltas = this.deltas.filter((delta) => delta.at > cutoff);

    const totals = new Map();
    for (const delta of this.deltas) {
      for (const call of delta.calls) {
        const held = totals.get(call.func) ?? { calls: 0, ns: 0 };
        held.calls += call.calls;
        held.ns += call.ns;
        totals.set(call.func, held);
      }
    }

    const seconds = RATE_WINDOW_MS / 1000;
    return [...totals]
      .map(([id, total]) => ({
        id,
        name: this.functions.get(id)?.name ?? `function ${id}`,
        module: this.functions.get(id)?.module ?? null,
        cores: total.ns / 1e9 / seconds,
        callsPerSec: total.calls / seconds,
      }))
      .filter((row) => row.cores > 0 || row.callsPerSec > 0)
      .sort((a, b) => b.cores - a.cores || b.callsPerSec - a.callsPerSec);
  }

  // Per type, at the latest reading. Never summed over time.
  memory() {
    return [...this.census]
      .map(([id, reading]) => ({
        id,
        name: this.types.get(id)?.name ?? `type ${id}`,
        live: reading.live,
        bytes: reading.bytes,
      }))
      .sort((a, b) => b.bytes - a.bytes || b.live - a.live);
  }
}
