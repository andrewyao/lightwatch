// The flame graph: one bucket's call tree, drawn as nested boxes.
//
// A box's width is its context's inclusive time, which is its own self time
// plus everything underneath it. Self time is what the wire carries, so the
// tree is summed once on the way in and the widths follow from it.
//
// Root at the top. Depth grows downward, so the row you are reading does not
// move when the tree gets deeper between one bucket and the next.

import { groupOf, shade } from "./palette.js";

const ROW_HEIGHT = 20;
const MIN_WIDTH = 0.5;
const LABEL_MIN_WIDTH = 34;

/// Builds the drawable tree for one bucket.
///
/// Contexts arrive as a flat list of leaves with self time. Every ancestor of
/// a reported context belongs in the picture even when it reported nothing
/// itself, or the tree would have holes in it where a caller spent all its
/// time in its callees.
export function buildTree(feed, bucket) {
  const nodes = new Map();

  const ensure = (pathId) => {
    if (nodes.has(pathId)) return nodes.get(pathId);
    const source = feed.paths.get(pathId);
    if (!source) return null;
    const node = {
      id: pathId,
      func: source.func,
      parent: source.parent,
      selfNs: 0,
      calls: 0,
      inclusive: 0,
      children: [],
    };
    nodes.set(pathId, node);
    if (source.parent) {
      const parent = ensure(source.parent);
      if (parent) parent.children.push(node);
    }
    return node;
  };

  for (const [pathId, stat] of bucket.stacks) {
    const node = ensure(pathId);
    if (!node) continue;
    node.selfNs += stat.selfNs;
    node.calls += stat.calls;
  }

  const roots = [...nodes.values()].filter((node) => !node.parent || !nodes.has(node.parent));

  const measure = (node) => {
    node.inclusive = node.selfNs;
    for (const child of node.children) node.inclusive += measure(child);
    node.children.sort((a, b) => b.inclusive - a.inclusive);
    return node.inclusive;
  };
  roots.forEach(measure);
  roots.sort((a, b) => b.inclusive - a.inclusive);

  const total = roots.reduce((sum, node) => sum + node.inclusive, 0);
  return { roots, total, count: nodes.size };
}

/// Lays the tree out into boxes, in pixels. Anything too thin to see is left
/// out rather than drawn as a sliver nobody can hit.
export function layout(tree, width) {
  const boxes = [];
  if (tree.total <= 0) return boxes;
  const scale = width / tree.total;

  const place = (node, x, depth) => {
    const w = node.inclusive * scale;
    if (w < MIN_WIDTH) return;
    boxes.push({ node, x, y: depth * ROW_HEIGHT, w, depth });
    let at = x;
    for (const child of node.children) {
      place(child, at, depth + 1);
      at += child.inclusive * scale;
    }
  };

  let at = 0;
  for (const root of tree.roots) {
    place(root, at, 0);
    at += root.inclusive * scale;
  }
  return boxes;
}

export class Flame {
  constructor(canvas, feed) {
    this.canvas = canvas;
    this.feed = feed;
    this.boxes = [];
    this.hover = null;
  }

  draw(tree, palette) {
    const canvas = this.canvas;
    const ratio = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const depth = maxDepth(tree.roots);
    const height = Math.max(ROW_HEIGHT, depth * ROW_HEIGHT);

    canvas.width = width * ratio;
    canvas.height = height * ratio;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    ctx.clearRect(0, 0, width, height);

    this.boxes = layout(tree, width);
    ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, monospace";
    ctx.textBaseline = "middle";

    for (const box of this.boxes) {
      const fn = this.feed.functions.get(box.node.func);
      const base = palette.colorOf(groupOf(fn));
      ctx.fillStyle = box.node === this.hover?.node ? "#ffffff" : shade(base, box.depth);
      // A 2px gap between neighbours, so two boxes of one module's hue do not
      // read as one wide box.
      ctx.fillRect(box.x + 1, box.y + 1, Math.max(MIN_WIDTH, box.w - 2), ROW_HEIGHT - 2);

      if (box.w >= LABEL_MIN_WIDTH) {
        ctx.save();
        ctx.beginPath();
        ctx.rect(box.x + 4, box.y, box.w - 8, ROW_HEIGHT);
        ctx.clip();
        ctx.fillStyle = box.node === this.hover?.node ? "#0e1116" : "#ffffff";
        ctx.fillText(this.feed.nameOf(box.node.func), box.x + 5, box.y + ROW_HEIGHT / 2);
        ctx.restore();
      }
    }
    return height;
  }

  at(x, y) {
    return this.boxes.find(
      (box) => x >= box.x && x <= box.x + box.w && y >= box.y && y <= box.y + ROW_HEIGHT,
    ) ?? null;
  }
}

function maxDepth(roots) {
  let deepest = 0;
  const walk = (node, depth) => {
    deepest = Math.max(deepest, depth);
    for (const child of node.children) walk(child, depth + 1);
  };
  roots.forEach((root) => walk(root, 1));
  return deepest;
}
