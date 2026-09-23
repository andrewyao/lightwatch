// The axis every view hangs off: two strips, one x.
//
// The top strip is CPU, stacked by module. The bottom is memory, stacked by
// type. They share a bucket index, so a spike in one lines up with whatever
// the other was doing at the same moment, which is the whole reason to draw
// them against one axis rather than side by side.
//
// The flame graph and the call graph both draw the bucket selected here.

import { groupOf } from "./palette.js";

const GAP = 2;

/// The least a bucket with any activity in it may be drawn as.
const MIN_VISIBLE = 2;

/// Per bucket, time by module.
///
/// Self time when the source carries a call tree, because self time adds up
/// across functions without counting a nanosecond twice. When it does not,
/// the only thing on the wire is each function's own inclusive duration, and
/// stacking those overstates any bucket where one measured function called
/// another. It is what there is, and `cpuMeasure` says which one is drawn so
/// the label can too.
export function cpuSeries(feed, buckets, palette) {
  const fromTree = feed.hasTree();
  return stackBy(buckets, (bucket) => {
    const byGroup = new Map();
    if (fromTree) {
      for (const [pathId, stat] of bucket.stacks) {
        const node = feed.paths.get(pathId);
        if (!node) continue;
        const group = groupOf(feed.functions.get(node.func));
        byGroup.set(group, (byGroup.get(group) ?? 0) + stat.selfNs);
      }
    } else {
      for (const [funcId, stat] of bucket.calls) {
        const group = groupOf(feed.functions.get(funcId));
        byGroup.set(group, (byGroup.get(group) ?? 0) + stat.ns);
      }
    }
    return byGroup;
  }, palette);
}

/// What `cpuSeries` was able to measure, for the strip's label.
export function cpuMeasure(feed) {
  return feed.hasTree() ? "self time by module" : "time in calls by module, no call tree in this feed";
}

/// Per bucket, live bytes by type. A reading, never a sum: this series must be
/// able to fall, and if it only ever climbs the fold is wrong.
export function memorySeries(feed, buckets, palette) {
  return stackBy(buckets, (bucket) => {
    const byType = new Map();
    for (const [typeId, reading] of bucket.census) {
      byType.set(feed.typeNameOf(typeId), reading.bytes);
    }
    return byType;
  }, palette);
}

function stackBy(buckets, extract, palette) {
  const columns = buckets.map(extract);
  const keys = [...new Set(columns.flatMap((column) => [...column.keys()]))];
  return { columns, keys, ...ceilingFor(columns), palette };
}

/// How tall the strip's top is worth, in the series' own units.
///
/// Not the largest bucket. A profile is spiky by nature — one garbage
/// collection, one cold cache, one window where the user did something — and
/// scaling to that bucket leaves every ordinary one under a pixel. Measured
/// against a real feed: the busiest bucket held 208ms and the ninetieth
/// percentile held 10ms, so nine buckets in ten drew at 3px in a 64px strip
/// and the chart showed one spike above a flat line that was not flat.
///
/// So the ceiling is the ninety-fifth percentile of the buckets that did
/// something, and only when the largest genuinely dwarfs it. Bars still grow
/// from zero, so the encoding stays linear and honest over the range that is
/// drawn; what is above the ceiling is marked rather than silently flattened.
function ceilingFor(columns) {
  const totals = columns.map((column) => sum(column.values())).filter((total) => total > 0);
  if (totals.length === 0) return { peak: 1, clipped: false, busiest: 0 };

  totals.sort((a, b) => a - b);
  const busiest = totals[totals.length - 1];
  const percentile = totals[Math.floor((totals.length - 1) * 0.95)];

  // Under three to one there is no outlier problem to solve, and clipping
  // would only throw away range for nothing.
  const ceiling = busiest > percentile * 3 ? percentile : busiest;
  return { peak: Math.max(1, ceiling), clipped: ceiling < busiest, busiest };
}

const sum = (values) => [...values].reduce((total, value) => total + value, 0);

export class Strip {
  constructor(canvas, { colorOf, height }) {
    this.canvas = canvas;
    this.colorOf = colorOf;
    this.height = height;
    this.selected = -1;
    this.hover = -1;
    this.count = 0;
  }

  draw(series) {
    const canvas = this.canvas;
    const ratio = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    canvas.width = width * ratio;
    canvas.height = this.height * ratio;
    canvas.style.height = `${this.height}px`;
    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    ctx.clearRect(0, 0, width, this.height);

    this.count = series.columns.length;
    if (this.count === 0) return;
    const step = width / this.count;

    for (let at = 0; at < this.count; at += 1) {
      const column = series.columns[at];
      const x = at * step;

      if (at === this.selected) {
        ctx.fillStyle = "#222834";
        ctx.fillRect(x, 0, Math.max(1, step), this.height);
      }

      const total = [...column.values()].reduce((sum, value) => sum + value, 0);
      // A bucket where something happened is never allowed to round away to
      // nothing: "barely any" and "none at all" are different answers.
      const scale =
        total > 0
          ? Math.max(this.height / series.peak, MIN_VISIBLE / total)
          : 0;

      let y = this.height;
      for (const key of series.keys) {
        const value = column.get(key) ?? 0;
        if (value <= 0) continue;
        const h = value * scale;
        if (y - h < -this.height) break;
        ctx.fillStyle = this.colorOf(key);
        // A 2px surface gap between segments, so a stack of one module's hue
        // does not read as a single block. The gap comes out of the bar, so a
        // segment thinner than the gap would vanish; give it a floor.
        const drawn = Math.max(1, h - GAP);
        ctx.fillRect(x + 0.5, y - h + GAP / 2, Math.max(1, step - 1), drawn);
        // Advance by what was actually drawn when the floor kicked in, or a
        // stack of thin segments paints them all on top of each other.
        y -= Math.max(h, drawn + GAP);
      }

      // Over the ceiling. Marked, so an outlier reads as an outlier rather
      // than as a bucket that happens to reach the top.
      if (total * scale > this.height + 0.5) {
        ctx.fillStyle = "#dfe5ee";
        ctx.fillRect(x + 0.5, 0, Math.max(1, step - 1), 2);
      }

      if (at === this.hover) {
        ctx.strokeStyle = "#dfe5ee";
        ctx.lineWidth = 1;
        ctx.strokeRect(x + 0.5, 0.5, Math.max(1, step - 1), this.height - 1);
      }
    }
  }

  /// Which bucket the pointer is over.
  indexAt(x) {
    if (this.count === 0) return -1;
    const step = this.canvas.clientWidth / this.count;
    const at = Math.floor(x / step);
    return at >= 0 && at < this.count ? at : -1;
  }
}
