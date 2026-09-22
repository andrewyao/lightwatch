// The call graph, laid out by a force simulation.
//
// Hand-rolled, because `web/` is four files with no build step and no
// dependencies, and keeping it that way is worth more than Barnes-Hut at a
// node count this view caps anyway.
//
// Positions live in one map keyed by function id and survive a change of
// bucket. Scrubbing one bucket forward must not rescramble the layout: a node
// that was on screen stays where it was, a node that appears starts near
// whatever called it, and a node that went quiet fades instead of vanishing.

// Fruchterman-Reingold. The ideal distance between two nodes is derived from
// the canvas area and the node count, so a graph of thirty and a graph of two
// hundred both fill the space instead of both becoming a blob in the middle.
// A fixed repulsion constant cannot do that: whatever value reads well at one
// node count is wrong at the other.
const SPREAD = 0.62;
// Gravity has to be on the same scale as the other two forces, which both
// produce a displacement of roughly `ideal` at a distance of `ideal`. A call
// tree is mostly leaves with one edge each, and without a pull of comparable
// strength the repulsion parks every one of them against a margin.
const GRAVITY = 0.15;
const COOLING = 0.985;

export const DEFAULT_BUDGET = 150;

/// The nodes and edges active in one bucket.
///
/// Over the budget, the busiest are kept and the rest reported as a count. A
/// graph with four hundred nodes in it is not a graph, it is a texture.
export function graphOf(feed, bucket, budget = DEFAULT_BUDGET) {
  const nodes = new Map();
  const edges = new Map();

  for (const [pathId, stat] of bucket.stacks) {
    const node = feed.paths.get(pathId);
    if (!node) continue;

    const held = nodes.get(node.func) ?? { func: node.func, selfNs: 0, calls: 0 };
    held.selfNs += stat.selfNs;
    held.calls += stat.calls;
    nodes.set(node.func, held);

    if (!node.parent) continue;
    const parent = feed.paths.get(node.parent);
    if (!parent) continue;
    const key = `${parent.func}>${node.func}`;
    const edge = edges.get(key) ?? { from: parent.func, to: node.func, calls: 0 };
    edge.calls += stat.calls;
    edges.set(key, edge);
  }

  const ranked = [...nodes.values()].sort((a, b) => b.selfNs - a.selfNs);
  const kept = new Set(ranked.slice(0, budget).map((node) => node.func));
  const hidden = ranked.length - kept.size;

  return {
    nodes: ranked.filter((node) => kept.has(node.func)),
    edges: [...edges.values()].filter((edge) => kept.has(edge.from) && kept.has(edge.to)),
    hidden,
  };
}

/// The widest a layout box may be for its height.
///
/// A panel spanning a wide window gives the canvas an aspect around 5:1, and
/// a force layout in a letterbox is not a graph: the ideal distance comes out
/// larger than the available height, so every node clamps against the top or
/// bottom edge and the picture becomes two rows of dots. Laying out inside a
/// centred box with a sane aspect costs some empty margin and gives back a
/// readable graph.
const MAX_ASPECT = 2.1;

export class Force {
  constructor(canvas) {
    this.canvas = canvas;
    this.placed = new Map();
    this.alpha = 0;
    this.hover = null;
    this.drawn = [];
  }

  /// The region the layout lives in: centred, and never wider than
  /// `MAX_ASPECT` times its height.
  bounds() {
    const width = this.canvas.clientWidth || 800;
    const height = this.canvas.clientHeight || 520;
    const boxWidth = Math.min(width, height * MAX_ASPECT);
    return {
      x: (width - boxWidth) / 2,
      y: 0,
      width: boxWidth,
      height,
      centerX: width / 2,
      centerY: height / 2,
    };
  }

  /// Seeds any node not placed yet, near its caller when it has one, and
  /// wakes the simulation only when the visible set actually changed.
  settle(graph) {
    const box = this.bounds();
    const callerOf = new Map(graph.edges.map((edge) => [edge.to, edge.from]));

    let appeared = false;
    for (const node of graph.nodes) {
      if (this.placed.has(node.func)) continue;
      appeared = true;
      const caller = this.placed.get(callerOf.get(node.func));
      this.placed.set(node.func, {
        // Seeded near its caller, so a node that appears joins the picture
        // where it belongs rather than flying in from the middle.
        x: (caller?.x ?? box.centerX) + (Math.random() - 0.5) * box.width * 0.4,
        y: (caller?.y ?? box.centerY) + (caller ? 70 : (Math.random() - 0.5) * box.height * 0.4),
        dx: 0,
        dy: 0,
      });
    }
    if (appeared) this.alpha = 1;
  }

  /// One step. Returns whether the layout is still moving, so the caller can
  /// stop asking for frames instead of burning a core on a settled graph.
  tick(graph) {
    if (this.alpha <= 0.004 || graph.nodes.length === 0) return false;
    const box = this.bounds();

    const points = graph.nodes.map((node) => this.placed.get(node.func)).filter(Boolean);
    if (points.length === 0) return false;

    const ideal = Math.sqrt((box.width * box.height) / points.length) * SPREAD;
    for (const point of points) {
      point.dx = 0;
      point.dy = 0;
    }

    for (let i = 0; i < points.length; i += 1) {
      for (let j = i + 1; j < points.length; j += 1) {
        const a = points[i];
        const b = points[j];
        let dx = b.x - a.x;
        let dy = b.y - a.y;
        let distance = Math.hypot(dx, dy);
        if (distance < 0.01) {
          // Two nodes exactly on top of each other have no direction to
          // separate along, so give them one rather than dividing by zero.
          dx = Math.random() - 0.5;
          dy = Math.random() - 0.5;
          distance = 0.01;
        }
        const push = (ideal * ideal) / distance;
        const ux = (dx / distance) * push;
        const uy = (dy / distance) * push;
        a.dx -= ux;
        a.dy -= uy;
        b.dx += ux;
        b.dy += uy;
      }
    }

    for (const edge of graph.edges) {
      const from = this.placed.get(edge.from);
      const to = this.placed.get(edge.to);
      if (!from || !to) continue;
      const dx = to.x - from.x;
      const dy = to.y - from.y;
      const distance = Math.max(0.01, Math.hypot(dx, dy));
      const pull = (distance * distance) / ideal;
      const ux = (dx / distance) * pull;
      const uy = (dy / distance) * pull;
      from.dx += ux;
      from.dy += uy;
      to.dx -= ux;
      to.dy -= uy;
    }

    // Temperature: how far any one node may move this step. It falls with
    // alpha, so the layout makes its big rearrangements early and only
    // nudges itself at the end rather than jittering forever.
    const temperature = Math.max(box.width, box.height) * 0.08 * this.alpha;
    const margin = 46;
    for (const point of points) {
      point.dx += (box.centerX - point.x) * GRAVITY;
      point.dy += (box.centerY - point.y) * GRAVITY;

      const travel = Math.max(0.01, Math.hypot(point.dx, point.dy));
      const step = Math.min(travel, temperature);
      point.x += (point.dx / travel) * step;
      point.y += (point.dy / travel) * step;
      point.x = Math.max(box.x + margin, Math.min(box.x + box.width - margin, point.x));
      point.y = Math.max(box.y + margin, Math.min(box.y + box.height - margin, point.y));
    }

    this.alpha *= COOLING;
    return true;
  }
}
