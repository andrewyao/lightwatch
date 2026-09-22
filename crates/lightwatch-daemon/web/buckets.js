// Folding the daemon's windows into the buckets every view shares.
//
// The one thing to get right, and the API says so too: `calls`, `edges` and
// `stacks` are deltas and add up. A `census` is a reading taken at an instant
// and replaces the one before it. Summing a census produces a number that
// climbs forever and means nothing, and it looks entirely plausible while
// doing so, which is why it is worth a module of its own with this comment at
// the top of it.
//
// The Rust side has `Cumulative` and `Absolute` to make the mistake a compile
// error. Here there is only this rule, and the check in the browser that
// memory must fall as well as rise.

/// Bucket sizes the axis offers, in milliseconds.
export const GRANULARITIES = [500, 1000, 2000, 5000, 10000];
export const DEFAULT_GRANULARITY_MS = 2000;

/// Folds raw windows into buckets of `bucketMs`.
///
/// Buckets are contiguous: an idle stretch is empty buckets rather than a gap,
/// so the axis stays a time axis and does not silently compress.
export function fold(windows, windowMs, bucketMs) {
  if (windows.length === 0) return [];
  const per = Math.max(1, Math.round(bucketMs / windowMs));
  const span = per * windowMs * 1e6;

  const first = Math.floor(windows[0].index / per);
  const last = Math.floor(windows[windows.length - 1].index / per);

  const buckets = [];
  for (let key = first; key <= last; key += 1) {
    buckets.push({
      key,
      startTNs: key * span,
      endTNs: (key + 1) * span,
      stacks: new Map(),
      calls: new Map(),
      census: new Map(),
      empty: true,
    });
  }

  for (const window of windows) {
    const bucket = buckets[Math.floor(window.index / per) - first];
    if (!bucket) continue;

    // Deltas add.
    for (const stack of window.stacks ?? []) {
      const held = bucket.stacks.get(stack.path) ?? { calls: 0, selfNs: 0 };
      held.calls += stack.calls;
      held.selfNs += stack.self_ns;
      bucket.stacks.set(stack.path, held);
      bucket.empty = false;
    }
    for (const call of window.calls ?? []) {
      const held = bucket.calls.get(call.func) ?? { calls: 0, ns: 0 };
      held.calls += call.calls;
      held.ns += call.ns;
      bucket.calls.set(call.func, held);
      bucket.empty = false;
    }
    // Readings replace. Windows arrive in ascending order, so the last one to
    // write is the latest one in the bucket, which is the reading that was
    // true when the bucket closed.
    for (const reading of window.census ?? []) {
      bucket.census.set(reading.ty, { live: reading.live, bytes: reading.bytes });
      bucket.empty = false;
    }
  }

  // A type reports when it has something to say. A bucket that heard nothing
  // about a type is not a bucket where that type vanished, so the last reading
  // carries forward. `carried` marks it, so a view can show a dead feed as
  // held rather than as steady.
  let previous = new Map();
  for (const bucket of buckets) {
    for (const [ty, reading] of previous) {
      if (!bucket.census.has(ty)) bucket.census.set(ty, { ...reading, carried: true });
    }
    previous = bucket.census;
  }

  return buckets;
}

/// Total self time in a bucket. The denominator a flame graph normalises to.
///
/// Deliberately not the bucket's wall-clock duration: the call tree is
/// process-wide, so N busy threads put N times the wall clock into one bucket,
/// and dividing by the clock would draw 800% of a flame graph.
export function totalSelfNs(bucket) {
  let total = 0;
  for (const stack of bucket.stacks.values()) total += stack.selfNs;
  return total;
}
