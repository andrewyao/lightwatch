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

/// Per bucket, self time by module.
export function cpuSeries(feed, buckets, palette) {
  return stackBy(buckets, (bucket) => {
    const byGroup = new Map();
    for (const [pathId, stat] of bucket.stacks) {
      const node = feed.paths.get(pathId);
      if (!node) continue;
      const group = groupOf(feed.functions.get(node.func));
      byGroup.set(group, (byGroup.get(group) ?? 0) + stat.selfNs);
    }
    return byGroup;
  }, palette);
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
  const peak = Math.max(1, ...columns.map((column) => sum(column.values())));
  return { columns, keys, peak, palette };
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

      let y = this.height;
      for (const key of series.keys) {
        const value = column.get(key) ?? 0;
        if (value <= 0) continue;
        const h = (value / series.peak) * this.height;
        ctx.fillStyle = this.colorOf(key);
        // A 2px surface gap between segments, so a stack of one module's hue
        // does not read as a single block.
        ctx.fillRect(x + 0.5, y - h + GAP / 2, Math.max(1, step - 1), Math.max(1, h - GAP));
        y -= h;
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
