// Colour, assigned by module rather than by function.
//
// A flame graph traditionally hashes the function name to a hue, which gives
// every box its own colour and tells you nothing: neighbouring boxes differ
// because their names differ, not because they are different kinds of work.
// Grouping by module means the picture answers "where is the time going" at a
// glance, and it keeps the number of hues inside what a categorical palette
// can actually distinguish.
//
// The eight hues are the validated dark-mode categorical order. They are
// assigned in fixed order and never cycled: a ninth module is "other", drawn
// in grey, rather than a repeat of slot one pretending to be distinct.

const HUES = [
  "#3987e5", // blue
  "#d95926", // orange
  "#199e70", // aqua
  "#c98500", // yellow
  "#d55181", // magenta
  "#008300", // green
  "#9085e9", // violet
  "#e66767", // red
];

const OTHER = "#6d7684";

/// The part of a module path worth colouring by: its last segment, which is
/// the module a function was declared in.
export function groupOf(fn) {
  if (!fn || !fn.module) return "—";
  const parts = fn.module.split("::");
  return parts[parts.length - 1] || fn.module;
}

export class Palette {
  constructor() {
    this.slots = new Map();
  }

  /// Assigns each group a slot in the order the process first named it.
  ///
  /// Function ids are handed out in intern order and never reused, so the
  /// lowest id in a group is a stable key: a module that appears later takes
  /// the next free slot instead of shuffling the ones already on screen. A
  /// filter that hides half the graph must not repaint the survivors.
  learn(functions) {
    const firstSeen = new Map();
    for (const [id, fn] of functions) {
      const group = groupOf(fn);
      const held = firstSeen.get(group);
      if (held === undefined || id < held) firstSeen.set(group, id);
    }
    const ordered = [...firstSeen].sort((a, b) => a[1] - b[1]).map(([group]) => group);
    for (const group of ordered) {
      if (!this.slots.has(group)) this.slots.set(group, this.slots.size);
    }
  }

  colorOf(group) {
    const slot = this.slots.get(group);
    return slot === undefined || slot >= HUES.length ? OTHER : HUES[slot];
  }

  /// The groups that have a hue of their own, in slot order, for a legend.
  /// Identity is never colour alone, so every view that paints by group also
  /// shows this.
  legend() {
    const named = [...this.slots]
      .filter(([, slot]) => slot < HUES.length)
      .sort((a, b) => a[1] - b[1])
      .map(([group]) => ({ group, color: HUES[this.slots.get(group)] }));
    const overflow = [...this.slots].filter(([, slot]) => slot >= HUES.length).length;
    if (overflow > 0) named.push({ group: `other (${overflow})`, color: OTHER });
    return named;
  }
}

/// A box's fill, dimmed with depth so a deep tree still reads as a tree when
/// a whole subtree shares one module's hue.
export function shade(hex, depth) {
  const fade = Math.min(0.42, depth * 0.07);
  const [r, g, b] = [1, 3, 5].map((at) => parseInt(hex.slice(at, at + 2), 16));
  const mix = (channel) => Math.round(channel + (0x16 - channel) * fade);
  return `rgb(${mix(r)},${mix(g)},${mix(b)})`;
}
