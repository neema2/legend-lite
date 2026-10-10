// A PAGE'S LAYOUT, AS BANDS (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md, §3.2): a stack of bands, top to bottom; a band
// divided into columns, a column divided again (across into stacked parts, or into columns), down to tiles. Every band
// has a height, as a share of one screenful; a page that fits its window shares the window among its bands instead,
// and one that does not scrolls when they are taller than it.
//
// PURE: no DOM, no pixels, no state. Every function takes a layout and returns a new one, so
// what every gesture does -- a drop on a tile's edge or between bands, a divider moved, a preset, a tile added or
// removed -- is testable in node, and the pointer layer only turns the pointer into these calls.
//
// A STACK (the design's §7.4) is one place holding two tiles or more, one in front, the others behind it: a place like a
// tile -- divided, moved, swapped and arranged as one -- whose tiles show one at a time.
//
// The rules, enforced by construction and checked by `problems`:
//   - every tile is in the layout once;
//   - a stack holds two tiles or more, and the one in front is one of them;
//   - a split has two parts or more, and their shares are positive and sum to 1 (a split left with one part becomes
//     that part; a band left with nothing goes);
//   - a split's parts never split the same way it does (a row in a row is one row): so the tree is the fewest
//     dividers that draw the page, and a divider means one thing;
//   - a band's height is positive.

/**
 * A place in a band: a tile, a stack of tiles shown one at a time, or a split of places side by side (`row`) or one
 * above another (`column`).
 */
export type Node = TileNode | StackNode | SplitNode;

export interface TileNode {
  readonly tile: string;
}

/** Tiles in one place, one shown: their tabs in this order, `front` the one in front (absent: the first). */
export interface StackNode {
  readonly stack: readonly string[];
  readonly front?: string;
}

/** A place that is not divided: a tile, or a stack (what a split divides, and what moves as one). */
export type Leaf = TileNode | StackNode;

export interface SplitNode {
  readonly split: 'row' | 'column';
  readonly parts: readonly Part[];
}

/** A part of a split: its place, and its share of the split's width (a row) or height (a column). */
export interface Part {
  readonly node: Node;
  readonly size: number;
}

export interface Band {
  /** Its height, in screenfuls: 0.5 is half the window's height (a page that fits its window shares it instead). */
  readonly height: number;
  readonly node: Node;
}

export interface Bands {
  /** The page fits its window: its bands share the window's height, and nothing scrolls. */
  readonly fit: boolean;
  readonly bands: readonly Band[];
}

/**
 * Where a dragged tile can land: beside or above or below a tile (its place), in that tile's place with it (a stack),
 * in place of it (a swap: the keyboard's), or as a band of its own.
 */
export type Drop =
  | { readonly onto: string; readonly edge: 'left' | 'right' | 'top' | 'bottom' }
  | { readonly stack: string }
  | { readonly swap: string }
  | { readonly band: number };

/** A band's height when nothing says otherwise: half a screen. */
export const BAND_HEIGHT = 0.5;
/** A part's least share of its split, and a band's least height: a divider stops there. */
export const MIN_SHARE = 0.1;
export const MIN_BAND_HEIGHT = 0.15;
/** Smart placement's widest band: a new tile beside its neighbour while the band holds fewer than this. */
export const MAX_COLUMNS = 4;

export const EMPTY: Bands = { fit: false, bands: [] };

const isTile = (node: Node): node is TileNode => 'tile' in node;
const isStack = (node: Node): node is StackNode => 'stack' in node;
const isLeaf = (node: Node): node is Leaf => isTile(node) || isStack(node);

/** A leaf's tiles: a tile's own, a stack's in their tabs' order. */
const leafTiles = (leaf: Leaf): readonly string[] => (isTile(leaf) ? [leaf.tile] : leaf.stack);

/** A stack's tile in front. */
export const frontOf = (stack: StackNode): string => stack.front ?? stack.stack[0]!;

/** The tiles, in reading order: band by band, left to right, top to bottom within a band, a stack's in its tabs' order. */
export function tiles(layout: Bands): string[] {
  const out: string[] = [];
  const walk = (node: Node): void => {
    if (isLeaf(node)) out.push(...leafTiles(node));
    else for (const part of node.parts) walk(part.node);
  };
  for (const band of layout.bands) walk(band.node);
  return out;
}

/** The places, in reading order: each a tile or a stack (whose tiles are one place). */
export function leaves(layout: Bands): Leaf[] {
  const out: Leaf[] = [];
  const walk = (node: Node): void => {
    if (isLeaf(node)) out.push(node);
    else for (const part of node.parts) walk(part.node);
  };
  for (const band of layout.bands) walk(band.node);
  return out;
}

/** The place holding `tile`: the tile, or the stack it is in. */
export function leafOf(layout: Bands, tile: string): Leaf | undefined {
  return leaves(layout).find((leaf) => leafTiles(leaf).includes(tile));
}

/** The places' ids, in reading order, as the layouts picker counts them: a tile's, a stack's tile in front. */
export function places(layout: Bands): string[] {
  return leaves(layout).map((leaf) => (isTile(leaf) ? leaf.tile : frontOf(leaf)));
}

/** The tiles stacked with `tile`, itself included, in their tabs' order (just itself, when it is not in a stack). */
export function stackOf(layout: Bands, tile: string): readonly string[] {
  const leaf = leafOf(layout, tile);
  return leaf ? leafTiles(leaf) : [];
}

/** What is wrong with a layout (nothing, for every layout these functions return). */
export function problems(layout: Bands): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const walk = (node: Node, parent: SplitNode['split'] | null, where: string): void => {
    if (isLeaf(node)) {
      for (const tile of leafTiles(node)) {
        if (seen.has(tile)) out.push(`${tile} is in the layout twice`);
        seen.add(tile);
      }
      if (isStack(node) && node.stack.length < 2) out.push(`${where}: a stack of ${node.stack.length} tile`);
      if (isStack(node) && node.front !== undefined && !node.stack.includes(node.front)) {
        out.push(`${where}: a stack whose front, ${node.front}, is not in it`);
      }
      return;
    }
    if (node.parts.length < 2) out.push(`${where}: a split of ${node.parts.length} part`);
    if (node.split === parent) out.push(`${where}: a ${node.split} inside a ${parent}`);
    const total = node.parts.reduce((sum, part) => sum + part.size, 0);
    if (Math.abs(total - 1) > 1e-9) out.push(`${where}: shares summing to ${total}`);
    node.parts.forEach((part, i) => {
      if (!(part.size > 0)) out.push(`${where}.${i}: a share of ${part.size}`);
      walk(part.node, node.split, `${where}.${i}`);
    });
  };
  layout.bands.forEach((band, i) => {
    if (!(band.height > 0)) out.push(`band ${i}: a height of ${band.height}`);
    walk(band.node, null, `band ${i}`);
  });
  return out;
}

// -- the tree's own arithmetic --------------------------------------

/** A split of `nodes` in `direction`, its shares `sizes` (equal when not given); one node is itself. */
function split(direction: SplitNode['split'], nodes: readonly Node[], sizes?: readonly number[]): Node {
  if (nodes.length === 1) return nodes[0]!;
  const shares = sizes ?? nodes.map(() => 1 / nodes.length);
  // a part splitting the same way is folded in: its parts take its share between them
  const parts: Part[] = [];
  nodes.forEach((node, i) => {
    const share = shares[i]!;
    if (!isLeaf(node) && node.split === direction) {
      for (const inner of node.parts) parts.push({ node: inner.node, size: inner.size * share });
    } else {
      parts.push({ node, size: share });
    }
  });
  return { split: direction, parts: normalized(parts) };
}

/** Shares summing to 1. */
function normalized(parts: readonly Part[]): Part[] {
  const total = parts.reduce((sum, part) => sum + part.size, 0);
  return parts.map((part) => ({ node: part.node, size: part.size / total }));
}

/**
 * `node` without `tile` (null when nothing is left), the hole closed: the neighbours take its share. Out of a stack, the
 * stack keeps its place; one left with one tile is that tile, and one whose front went shows the tab after it.
 */
function without(node: Node, tile: string): Node | null {
  if (isTile(node)) return node.tile === tile ? null : node;
  if (isStack(node)) {
    const at = node.stack.indexOf(tile);
    if (at < 0) return node;
    const rest = node.stack.filter((t) => t !== tile);
    if (rest.length === 1) return { tile: rest[0]! };
    const front = frontOf(node) === tile ? rest[Math.min(at, rest.length - 1)]! : node.front;
    return { stack: rest, ...(front !== undefined ? { front } : {}) };
  }
  const kept: Part[] = [];
  for (const part of node.parts) {
    const rest = without(part.node, tile);
    if (rest !== null) kept.push({ node: rest, size: part.size });
  }
  if (kept.length === 0) return null;
  if (kept.length === 1) return kept[0]!.node;
  return split(node.split, kept.map((part) => part.node), normalized(kept).map((part) => part.size));
}

/** `node` with the place holding `tile` (it, or its stack) replaced by `replacement`. */
function replaced(node: Node, tile: string, replacement: Node): Node {
  if (isLeaf(node)) return leafTiles(node).includes(tile) ? replacement : node;
  return split(node.split, node.parts.map((part) => replaced(part.node, tile, replacement)),
    node.parts.map((part) => part.size));
}

function contains(node: Node, tile: string): boolean {
  return isLeaf(node) ? leafTiles(node).includes(tile) : node.parts.some((part) => contains(part.node, tile));
}

/**
 * A STACK'S TILE BROUGHT TO THE FRONT (its tab clicked): what it shows, not where anything is -- a page does not save it
 * (the design's §5, 7). Not in a stack, the layout as it was.
 */
export function bringToFront(layout: Bands, tile: string): Bands {
  const leaf = leafOf(layout, tile);
  if (!leaf || !isStack(leaf) || frontOf(leaf) === tile) return layout;
  const map = (node: Node): Node => (isStack(node)
    ? (node.stack.includes(tile) && frontOf(node) !== tile ? { stack: node.stack, front: tile } : node)
    : isTile(node) ? node
      : { split: node.split, parts: node.parts.map((part) => ({ node: map(part.node), size: part.size })) });
  return mapBands(layout, map);
}

/** A stack's tile moved to `to` among its tabs (counted with it taken out). */
export function reorderStack(layout: Bands, tile: string, to: number): Bands {
  const leaf = leafOf(layout, tile);
  // not in a stack, or put where it is: the layout as it was
  if (!leaf || !isStack(leaf) || leaf.stack.indexOf(tile) === Math.max(0, Math.min(leaf.stack.length - 1, to))) return layout;
  const map = (node: Node): Node => {
    if (isStack(node)) {
      if (!node.stack.includes(tile)) return node;
      const rest = node.stack.filter((t) => t !== tile);
      rest.splice(Math.max(0, Math.min(rest.length, to)), 0, tile);
      return { stack: rest, ...(node.front !== undefined ? { front: node.front } : {}) };
    }
    return isTile(node) ? node : { split: node.split, parts: node.parts.map((part) => ({ node: map(part.node), size: part.size })) };
  };
  return mapBands(layout, map);
}

/** The layout as a page saves it: every stack without its tile in front (it reopens on its first tab). */
export function asSaved(layout: Bands): Bands {
  const map = (node: Node): Node => (isStack(node) ? { stack: node.stack }
    : isTile(node) ? node : { split: node.split, parts: node.parts.map((part) => ({ node: map(part.node), size: part.size })) });
  return mapBands(layout, map);
}

function mapBands(layout: Bands, map: (node: Node) => Node): Bands {
  return { fit: layout.fit, bands: layout.bands.map((band) => ({ height: band.height, node: map(band.node) })) };
}

/** The band holding `tile`, or -1. */
export function bandOf(layout: Bands, tile: string): number {
  return layout.bands.findIndex((band) => contains(band.node, tile));
}

/** The layout without `tile`: its place closed by its neighbours, an emptied band gone. */
export function remove(layout: Bands, tile: string): Bands {
  const bands: Band[] = [];
  for (const band of layout.bands) {
    const node = without(band.node, tile);
    if (node !== null) bands.push({ height: band.height, node });
  }
  return { fit: layout.fit, bands };
}

/**
 * A NEW TILE, placed where it is wanted (smart placement): beside `near` -- the tile it came from -- while `near`'s band
 * is a row of fewer than `columns` columns (MAX_COLUMNS, or fewer where the board is too narrow for them to be
 * readable), otherwise as a band of its own below `near`'s; with no `near`, a band at the bottom of the page.
 */
export function add(layout: Bands, tile: string, near?: string, columns = MAX_COLUMNS): Bands {
  if (tiles(layout).includes(tile)) return layout;
  const at = near === undefined ? -1 : bandOf(layout, near);
  if (at < 0) return { fit: layout.fit, bands: [...layout.bands, { height: BAND_HEIGHT, node: { tile } }] };
  const band = layout.bands[at]!;
  const has = isLeaf(band.node) ? 1 : band.node.split === 'row' ? band.node.parts.length : 1;
  if (has < Math.min(columns, MAX_COLUMNS)) {
    const node = isLeaf(band.node) || band.node.split !== 'row'
      ? split('row', [band.node, { tile }])
      : split('row', [...band.node.parts.map((part) => part.node), { tile }]);
    return withBand(layout, at, { height: band.height, node });
  }
  const bands = [...layout.bands];
  bands.splice(at + 1, 0, { height: BAND_HEIGHT, node: { tile } });
  return { fit: layout.fit, bands };
}

function withBand(layout: Bands, at: number, band: Band): Bands {
  return { fit: layout.fit, bands: layout.bands.map((b, i) => (i === at ? band : b)) };
}

/**
 * A DRAGGED TILE LET GO: onto a tile's edge (that tile's place -- it, or its stack -- divided there, the two sharing
 * it), onto a tile's middle (the two stacked in its place, the dragged one in front), between bands (a band of its own
 * at that place, `band` counting the bands above it), or -- the keyboard -- swapping places with a tile (a stack moving
 * as one). Out of a stack, it leaves the rest stacked. A drop onto itself changes nothing.
 */
export function drop(layout: Bands, tile: string, where: Drop): Bands {
  if (!tiles(layout).includes(tile)) return layout;
  if ('swap' in where) {
    const a = leafOf(layout, tile);
    const b = leafOf(layout, where.swap);
    if (!a || !b || a === b) return layout;
    const swapped = (node: Node): Node => (isLeaf(node) ? (node === a ? b : node === b ? a : node)
      : { split: node.split, parts: node.parts.map((part) => ({ node: swapped(part.node), size: part.size })) });
    return mapBands(layout, swapped);
  }
  if ('stack' in where) {
    if (where.stack === tile || !tiles(layout).includes(where.stack)) return layout;
    const rest = remove(layout, tile);
    const target = leafOf(rest, where.stack)!;
    const stacked: StackNode = { stack: [...leafTiles(target), tile], front: tile };
    return mapBands(rest, (node) => replaced(node, where.stack, stacked));
  }
  if ('band' in where) {
    // a tile alone in its band, dropped on the line above or below that band, is where it was
    const own = bandOf(layout, tile);
    const alone = isTile(layout.bands[own]!.node);
    if (alone && (where.band === own || where.band === own + 1)) return layout;
    // the band counted as the page is without the tile: a band it empties is no longer there
    const before = layout.bands.slice(0, where.band).filter((band) => !(isTile(band.node) && band.node.tile === tile)).length;
    const rest = remove(layout, tile);
    const bands = [...rest.bands];
    bands.splice(Math.min(before, bands.length), 0, { height: BAND_HEIGHT, node: { tile } });
    return { fit: layout.fit, bands };
  }
  if (where.onto === tile || !tiles(layout).includes(where.onto)) return layout;
  const rest = remove(layout, tile);
  const at = bandOf(rest, where.onto);
  const band = rest.bands[at]!;
  // the place the edge divides: the tile, or the whole stack it is in
  const target: Node = leafOf(rest, where.onto)!;
  const moved: Node = { tile };
  const pair = where.edge === 'left' ? split('row', [moved, target])
    : where.edge === 'right' ? split('row', [target, moved])
      : where.edge === 'top' ? split('column', [moved, target])
        : split('column', [target, moved]);
  return withBand(rest, at, { height: band.height, node: replaced(band.node, where.onto, pair) });
}

/**
 * A DIVIDER MOVED: in the split found by `path` (the band's index, then a part's index at each split down to the
 * split), the boundary after part `after` by `delta` of the split's size; the two parts it sits between trade share,
 * neither going below MIN_SHARE (or half of what the two hold, when that is less).
 */
export function resize(layout: Bands, path: readonly number[], after: number, delta: number): Bands {
  const [bandIndex, ...inner] = path;
  const band = layout.bands[bandIndex ?? -1];
  if (band === undefined) return layout;
  const at = (node: Node, rest: readonly number[]): Node => {
    if (isLeaf(node)) return node;
    if (rest.length > 0) {
      const [i, ...more] = rest;
      return { split: node.split, parts: node.parts.map((part, j) => (j === i ? { node: at(part.node, more), size: part.size } : part)) };
    }
    const left = node.parts[after];
    const right = node.parts[after + 1];
    if (!left || !right) return node;
    const total = left.size + right.size;
    // each keeps its least share -- or half of what the two have, when that is less (parts deep in a page can be small)
    const floor = Math.min(MIN_SHARE, total / 2);
    const size = Math.min(Math.max(left.size + delta, floor), total - floor);
    return {
      split: node.split,
      parts: node.parts.map((part, j) => (j === after ? { node: part.node, size } : j === after + 1 ? { node: part.node, size: total - size } : part)),
    };
  };
  return withBand(layout, bandIndex!, { height: band.height, node: at(band.node, inner) });
}

/** A BAND'S HEIGHT SET (its bottom edge dragged), in screenfuls, never below MIN_BAND_HEIGHT. */
export function resizeBand(layout: Bands, band: number, height: number): Bands {
  const it = layout.bands[band];
  if (it === undefined) return layout;
  return withBand(layout, band, { height: Math.max(height, MIN_BAND_HEIGHT), node: it.node });
}

/** EVEN OUT the split found by `path` (a divider's double click): its parts share alike. */
export function evenOut(layout: Bands, path: readonly number[]): Bands {
  const [bandIndex, ...inner] = path;
  const band = layout.bands[bandIndex ?? -1];
  if (band === undefined) return layout;
  const at = (node: Node, rest: readonly number[]): Node => {
    if (isLeaf(node)) return node;
    if (rest.length > 0) {
      const [i, ...more] = rest;
      return { split: node.split, parts: node.parts.map((part, j) => (j === i ? { node: at(part.node, more), size: part.size } : part)) };
    }
    return { split: node.split, parts: node.parts.map((part) => ({ node: part.node, size: 1 / node.parts.length })) };
  };
  return withBand(layout, bandIndex!, { height: band.height, node: at(band.node, inner) });
}

/** EVEN OUT THE WHOLE PAGE (the layout picker's Even out): every split's parts share alike, every band as tall as the rest. */
export function evenAll(layout: Bands): Bands {
  const even = (node: Node): Node => (isLeaf(node) ? node
    : { split: node.split, parts: node.parts.map((part) => ({ node: even(part.node), size: 1 / node.parts.length })) });
  const height = layout.bands.reduce((sum, band) => sum + band.height, 0) / Math.max(1, layout.bands.length);
  return { fit: layout.fit, bands: layout.bands.map((band) => ({ height, node: even(band.node) })) };
}

/** The page fitting its window, or scrolling past it. */
export function fitted(layout: Bands, fit: boolean): Bands {
  return { fit, bands: layout.bands };
}

// -- presets ---------------------------------------------------------

/**
 * A LAYOUT THE PICKER OFFERS (§3.3), by what it is, for any number of tiles:
 *   'side-by-side'     every tile in one row, a screenful high
 *   'stacked'          every tile a band of its own
 *   'rows:3-2'         bands of 3 and 2 tiles side by side (in reading order)
 *   'columns:3-2'      one screenful divided into columns of 3 and 2 tiles, one under another
 *   'focus-left'       the first tile on the left, all the page's height, the rest beside it (also -right, -top, -bottom)
 */
export type Preset = 'side-by-side' | 'stacked' | 'focus-left' | 'focus-right' | 'focus-top' | 'focus-bottom'
  | `rows:${string}` | `columns:${string}`;

/** `n` shared out into `parts` near-equal counts, the larger ones first (or last). */
function balanced(n: number, parts: number, largerFirst: boolean): number[] {
  const base = Math.floor(n / parts);
  const extra = n % parts;
  const counts = Array.from({ length: parts }, (_, i) => base + (i < extra ? 1 : 0));
  return largerFirst ? counts : counts.reverse();
}

/** A person's name for a preset, for the picker's labels. */
function labelOf(preset: Preset): string {
  switch (preset) {
    case 'side-by-side': return 'All side by side';
    case 'stacked': return 'All stacked';
    case 'focus-left': return 'One on the left, the rest beside it';
    case 'focus-right': return 'One on the right, the rest beside it';
    case 'focus-top': return 'One on top, the rest below';
    case 'focus-bottom': return 'One below, the rest above';
  }
  const [kind, list] = preset.split(':') as [string, string];
  const counts = list.split('-');
  const even = counts.every((c) => c === counts[0]);
  if (kind === 'rows') return even ? `${counts.length} rows of ${counts[0]}` : `Rows of ${counts.join(', ')}`;
  return even ? `${counts.length} columns of ${counts[0]}` : `Columns of ${counts.join(', ')}`;
}

/** Every way to cut `n` into `parts` counts in order, each 1 to `most`, the larger first counts first. */
function compositions(n: number, parts: number, most: number): number[][] {
  if (parts === 1) return n >= 1 && n <= most ? [[n]] : [];
  const out: number[][] = [];
  for (let first = Math.min(most, n - (parts - 1)); first >= 1; first -= 1) {
    for (const rest of compositions(n - first, parts - 1, most)) out.push([first, ...rest]);
  }
  return out;
}

/** The three kinds of layout the picker shows under their own headings. */
export type LayoutGroup = 'rows' | 'large' | 'columns';

/** The rows of a grid of `n` tiles as square as it goes: ceil(sqrt(n)) to a row, the rows near-equal, larger first. */
export function gridRows(n: number): number[] {
  const count = Math.max(1, n);
  const across = Math.ceil(Math.sqrt(count));
  return balanced(count, Math.ceil(count / across), true);
}

/** A layout as the picker offers it: what it is, its name, its kind, and whether it is one of the few shown first. */
export interface OfferedLayout {
  readonly id: Preset;
  readonly label: string;
  readonly group: LayoutGroup;
  /** One of the standard shapes, shown first and always in the same order; the rest wait behind "More layouts". */
  readonly featured: boolean;
}

/**
 * THE LAYOUTS FOR `n` TILES, as the picker offers them (the user, 2026-10-09: "default 8-9 shapes with option to see
 * more and option to do custom"). First the standard shapes, the same nine in the same order for any number of tiles
 * (featured): all side by side, all stacked, a grid as square as it goes, one tile on each side with the rest beside
 * it, two rows, two columns -- each split evenly (the user, 2026-10-09: sizes are a divider's drag away). Then, behind "More layouts", EVERY way to cut the tiles into rows (several on top
 * then one and one; one, several, one), MAX_COLUMNS to a row, in up to four rows (three for six tiles or more), and
 * columns of near-equal size. A layout that looks the same as one before it -- the same boxes, whichever tile is where
 * -- is left out, so each thumbnail is a different shape.
 */
export function layoutsFor(n: number): readonly OfferedLayout[] {
  const count = Math.max(1, n);
  const two = count >= 2;
  const candidates: { id: Preset; group: LayoutGroup; featured: boolean; label?: string }[] = [
    { id: 'side-by-side', group: 'rows', featured: true },
    { id: 'stacked', group: 'rows', featured: true },
    { id: `rows:${gridRows(count).join('-')}`, group: 'rows', featured: true, label: `Grid (${gridRows(count).join(', ')})` },
    ...(two ? (['focus-left', 'focus-right', 'focus-top', 'focus-bottom'] as const)
      .map((id) => ({ id, group: 'large' as const, featured: true })) : []),
    ...(two ? [
      { id: `rows:${balanced(count, 2, true).join('-')}` as Preset, group: 'rows' as const, featured: true, label: 'Two rows' },
      { id: `columns:${balanced(count, 2, true).join('-')}` as Preset, group: 'columns' as const, featured: true, label: 'Two columns' },
    ] : []),
  ];
  for (let r = 2; r <= Math.min(count <= 5 ? 4 : 3, count); r += 1) {
    for (const counts of compositions(count, r, MAX_COLUMNS)) candidates.push({ id: `rows:${counts.join('-')}`, group: 'rows', featured: false });
  }
  for (let c = 2; c <= Math.min(4, count - 1); c += 1) {
    for (const first of [true, false]) {
      candidates.push({ id: `columns:${balanced(count, c, first).join('-')}`, group: 'columns', featured: false });
    }
  }
  const order = Array.from({ length: count }, (_, i) => `t${i}`);
  const seen = new Set<string>();
  const out: OfferedLayout[] = [];
  for (const { id, group, featured, label } of candidates) {
    const boxes = [...draw(fitted(arrange(EMPTY, id, order), true), 120, 80, 0, 0).tiles.values()]
      .map((b) => `${b.x},${b.y},${b.w},${b.h}`).sort().join(' ');
    if (seen.has(boxes)) continue;
    seen.add(boxes);
    out.push({ id, label: label ?? labelOf(id), group, featured });
  }
  return out;
}

/** A band's height in a preset of `count` bands: they share one screenful, three to a screen at most. */
/** The bands the layout picker fits to a screen, at most: more, and each is a third of one (the page scrolls). */
export const SCREEN_BANDS = 3;

const bandHeight = (count: number): number => 1 / Math.min(Math.max(count, 1), SCREEN_BANDS);

/** `ids` cut into runs of `counts` (the last count again for tiles past them; fewer tiles, fewer runs). */
function runs(ids: readonly string[], counts: readonly number[]): string[][] {
  const out: string[][] = [];
  let at = 0;
  for (let i = 0; at < ids.length; i += 1) {
    const size = Math.max(1, counts[Math.min(i, counts.length - 1)] ?? 1);
    out.push(ids.slice(at, at + size));
    at += size;
  }
  return out;
}

/** The counts a 'rows:' or 'columns:' preset names, refused by name when it names none. */
function countsOf(preset: string): number[] {
  const list = preset.slice(preset.indexOf(':') + 1).split('-').map(Number);
  if (list.length === 0 || list.some((n) => !Number.isInteger(n) || n < 1)) throw new Error(`not a layout: ${preset}`);
  return list;
}

/**
 * THE PLACES OF THE TILES IN `order` ARRANGED AS A PRESET: the first tile's place takes the preset's first slot, and so
 * on in reading order -- a stack is one place, and stays one. More places than slots: the rest go on as the preset
 * goes on (rows of 2 and 2 continue in rows of 2). Fewer: the preset closes up. The page's fit is kept.
 */
export function arrange(layout: Bands, preset: Preset, order: readonly string[] = tiles(layout)): Bands {
  // each tile's place, once: a stack's first tile named brings the stack
  const units: Leaf[] = [];
  for (const tile of order) {
    const unit = leafOf(layout, tile) ?? { tile };
    if (!units.some((u) => leafTiles(u).includes(tile))) units.push(unit);
  }
  const ids = units.map((unit) => leafTiles(unit)[0]!);
  if (ids.length === 0) return { fit: layout.fit, bands: [] };
  const leaf = (tile: string): Node => units[ids.indexOf(tile)]!;
  const row = (run: readonly string[]): Node => split('row', run.map(leaf));
  const column = (run: readonly string[]): Node => split('column', run.map(leaf));
  const [first, ...rest] = ids;
  let bands: Band[];
  if (preset === 'side-by-side') {
    bands = [{ height: 1, node: row(ids) }];
  } else if (preset === 'stacked') {
    bands = ids.map((tile) => ({ height: bandHeight(ids.length), node: leaf(tile) }));
  } else if (preset.startsWith('rows:')) {
    const made = runs(ids, countsOf(preset));
    bands = made.map((run) => ({ height: bandHeight(made.length), node: row(run) }));
  } else if (preset.startsWith('columns:')) {
    bands = [{ height: 1, node: split('row', runs(ids, countsOf(preset)).map(column)) }];
  } else if (rest.length === 0) {
    bands = [{ height: 1, node: leaf(first!) }];
  } else if (preset === 'focus-left' || preset === 'focus-right') {
    // the one, half the page's width and all its height; the rest one under another beside it, or -- more than four
    // -- in rows of two. Even, as every standard shape is (the user, 2026-10-09): a divider's drag makes it more
    const beside = rest.length <= 4 ? column(rest) : split('column', runs(rest, [2]).map(row));
    bands = [{ height: 1, node: preset === 'focus-left'
      ? split('row', [leaf(first!), beside])
      : split('row', [beside, leaf(first!)]) }];
  } else if (preset === 'focus-top' || preset === 'focus-bottom') {
    // the one a band of its own, the rest side by side in bands of up to MAX_COLUMNS, near-equal (six tiles: one, then
    // 3 and 2 -- never a lone tile left over), every band as tall as the others
    const counts = balanced(rest.length, Math.ceil(rest.length / MAX_COLUMNS), true);
    const height = bandHeight(counts.length + 1);
    const others = runs(rest, counts).map((run) => ({ height, node: row(run) }));
    const one = { height, node: leaf(first!) };
    bands = preset === 'focus-top' ? [one, ...others] : [...others, one];
  } else {
    throw new Error(`not a layout: ${String(preset)}`);
  }
  return { fit: layout.fit, bands };
}

// -- narrow windows ----------------------------------------------------

/**
 * THE PAGE ON A NARROW WINDOW (§3.2, the user: stacked): every tile one under another, in reading order, each a band of
 * its own. Derived, never stored: the layout itself is unchanged, and comes back when the window widens.
 */
export function stacked(layout: Bands): Bands {
  // a place to a band: a stack of tiles stays one
  return { fit: false, bands: leaves(layout).map((node) => ({ height: BAND_HEIGHT, node })) };
}

// -- drawing: where everything sits, in pixels --------------------------

/** A box on the board, in pixels from its top left. */
export interface Box {
  readonly x: number;
  readonly y: number;
  readonly w: number;
  readonly h: number;
}

/** A divider between two parts of a split: dragging it is `resize(layout, path, after, ...)`. */
export interface Divider {
  readonly path: readonly number[];
  readonly after: number;
  /** A row's divider stands between columns (it moves left and right); a column's lies between stacked parts. */
  readonly split: SplitNode['split'];
  readonly box: Box;
  /** The split's own length along the divider's way of moving: a pixel moved is 1 / length of a share. */
  readonly length: number;
}

/**
 * What the board draws: each tile's box (a stack's tile in front: its place's box; the others behind it, none), the
 * stacks (by their tile in front, their tiles in their tabs' order), the dividers, each band's box, and the page's whole
 * height.
 */
export interface Drawn {
  readonly tiles: ReadonlyMap<string, Box>;
  readonly stacks: ReadonlyMap<string, readonly string[]>;
  readonly dividers: readonly Divider[];
  readonly bands: readonly Box[];
  readonly height: number;
}

/**
 * THE LAYOUT IN PIXELS on a board `width` wide, a screenful `screen` high: bands one under another, `gap` apart, each
 * its height in screenfuls (never less than `least` pixels) -- or, when the page fits its window, sharing the screen as
 * their heights share their sum. A page that fits its window but has more bands than the screen holds at `fitLeast`
 * pixels each, on average, is drawn as one that scrolls instead: fitting would squeeze each band too short to read
 * (the user, 2026-10-09: fit by default, with that floor). Inside a band, parts share their split's length as their
 * shares do, `gap` between them. Edges are rounded, not lengths, so neighbours always meet and nothing drifts a pixel.
 */
export function draw(layout: Bands, width: number, screen: number, gap = 8, least = 120, fitLeast = 0): Drawn {
  const tileBoxes = new Map<string, Box>();
  const stacks = new Map<string, readonly string[]>();
  const dividers: Divider[] = [];
  const bandBoxes: Box[] = [];
  const count = layout.bands.length;
  const total = layout.bands.reduce((sum, band) => sum + band.height, 0);
  const free = Math.max(0, screen - gap * (count - 1));
  const fits = layout.fit && total > 0 && free >= fitLeast * count;
  const heights = layout.bands.map((band) => (fits
    ? (free * band.height) / total
    : Math.max(least, band.height * screen)));
  /** Lengths `share`d out of `length` (gaps between): each part's start and end, rounded at its edges. */
  const spans = (start: number, length: number, shares: readonly number[]): [number, number][] => {
    const room = Math.max(0, length - gap * (shares.length - 1));
    const out: [number, number][] = [];
    let at = 0;
    shares.forEach((share, i) => {
      const from = Math.round(start + at + gap * i);
      at += room * share;
      const to = i === shares.length - 1 ? Math.round(start + length) : Math.round(start + at + gap * i);
      out.push([from, to]);
    });
    return out;
  };
  const place = (node: Node, box: Box, path: number[]): void => {
    if (isTile(node)) {
      tileBoxes.set(node.tile, box);
      return;
    }
    if (isStack(node)) {
      tileBoxes.set(frontOf(node), box);
      stacks.set(frontOf(node), node.stack);
      return;
    }
    const across = node.split === 'row';
    const length = across ? box.w : box.h;
    const parts = spans(across ? box.x : box.y, length, node.parts.map((part) => part.size));
    parts.forEach(([from, to], i) => {
      place(node.parts[i]!.node,
        across ? { x: from, y: box.y, w: to - from, h: box.h } : { x: box.x, y: from, w: box.w, h: to - from },
        [...path, i]);
      if (i < parts.length - 1) {
        const next = parts[i + 1]![0];
        dividers.push({
          path, after: i, split: node.split, length,
          box: across ? { x: to, y: box.y, w: next - to, h: box.h } : { x: box.x, y: to, w: box.w, h: next - to },
        });
      }
    });
  };
  let y = 0;
  layout.bands.forEach((band, i) => {
    const top = Math.round(y);
    const bottom = Math.round(y + heights[i]!);
    const box = { x: 0, y: top, w: Math.round(width), h: bottom - top };
    bandBoxes.push(box);
    place(band.node, box, [i]);
    y += heights[i]! + gap;
  });
  return { tiles: tileBoxes, stacks, dividers, bands: bandBoxes, height: count === 0 ? 0 : Math.round(y - gap) };
}

/** How far into a band, from its top or bottom edge, a drop makes a new band there (pixels). */
export const BAND_EDGE = 10;

/**
 * Where a dragged tile would land with the pointer at (`x`, `y`) on a board drawn as `drawn`: between bands (within
 * BAND_EDGE of a band's top or bottom edge, in the gap between two, above the first or below the last), on a tile's
 * edge (the quarter of it nearest that edge), or on a tile (its middle: stacked with it). Over the dragged tile itself,
 * or nowhere, undefined.
 */
export function dropAt(drawn: Drawn, tile: string, x: number, y: number): Drop | undefined {
  const bands = drawn.bands;
  if (bands.length === 0) return undefined;
  for (let i = 0; i <= bands.length; i += 1) {
    const above = bands[i - 1];
    const below = bands[i];
    const from = above === undefined ? -Infinity : above.y + above.h - BAND_EDGE;
    const to = below === undefined ? Infinity : below.y + BAND_EDGE;
    if (y >= from && y < to) return { band: i };
  }
  for (const [id, box] of drawn.tiles) {
    if (x < box.x || x >= box.x + box.w || y < box.y || y >= box.y + box.h) continue;
    if (id === tile) return undefined;
    const fx = (x - box.x) / box.w;
    const fy = (y - box.y) / box.h;
    const nearest = Math.min(fx, 1 - fx, fy, 1 - fy);
    if (nearest > 0.25) return { stack: id };
    if (nearest === fx) return { onto: id, edge: 'left' };
    if (nearest === 1 - fx) return { onto: id, edge: 'right' };
    if (nearest === fy) return { onto: id, edge: 'top' };
    return { onto: id, edge: 'bottom' };
  }
  return undefined;
}

/**
 * The edge between band `after` and the next moved, in a page that fits its window: the two bands trade height
 * (`delta` in the bands' own measure), neither below its least (or half of what the two hold, when that is less).
 */
export function tradeBands(layout: Bands, after: number, delta: number): Bands {
  const top = layout.bands[after];
  const bottom = layout.bands[after + 1];
  if (!top || !bottom) return layout;
  const total = top.height + bottom.height;
  const floor = Math.min(MIN_BAND_HEIGHT, total / 2);
  const height = Math.min(Math.max(top.height + delta, floor), total - floor);
  return {
    fit: layout.fit,
    bands: layout.bands.map((band, i) => (i === after ? { height, node: band.node }
      : i === after + 1 ? { height: total - height, node: band.node } : band)),
  };
}

/** A direction on the board, for the keyboard. */
export type Toward = 'left' | 'right' | 'up' | 'down';

/**
 * The tile next to `tile` toward a direction, on a board drawn as `drawn`: of the tiles wholly past its edge that way
 * and overlapping it across, the nearest, then the one overlapping it most. None, undefined.
 */
export function neighbour(drawn: Drawn, tile: string, toward: Toward): string | undefined {
  const of = drawn.tiles.get(tile);
  if (!of) return undefined;
  const across = toward === 'left' || toward === 'right';
  let best: { id: string; distance: number; overlap: number } | undefined;
  for (const [id, box] of drawn.tiles) {
    if (id === tile) continue;
    const distance = toward === 'left' ? of.x - (box.x + box.w)
      : toward === 'right' ? box.x - (of.x + of.w)
        : toward === 'up' ? of.y - (box.y + box.h)
          : box.y - (of.y + of.h);
    const overlap = across
      ? Math.min(of.y + of.h, box.y + box.h) - Math.max(of.y, box.y)
      : Math.min(of.x + of.w, box.x + box.w) - Math.max(of.x, box.x);
    if (distance < 0 || overlap <= 0) continue;
    if (!best || distance < best.distance || (distance === best.distance && overlap > best.overlap)) {
      best = { id, distance, overlap };
    }
  }
  return best?.id;
}

/** The divider along one side of `tile`, on a board drawn as `drawn`: the one that bounds it there, if any. */
export function dividerBeside(drawn: Drawn, tile: string, side: 'left' | 'right' | 'top' | 'bottom'): Divider | undefined {
  const of = drawn.tiles.get(tile);
  if (!of) return undefined;
  return drawn.dividers.find(({ split, box }) => (side === 'left' || side === 'right'
    ? split === 'row' && (side === 'right' ? box.x === of.x + of.w : box.x + box.w === of.x)
      && box.y <= of.y && box.y + box.h >= of.y + of.h
    : split === 'column' && (side === 'bottom' ? box.y === of.y + of.h : box.y + box.h === of.y)
      && box.x <= of.x && box.x + box.w >= of.x + of.w));
}

// -- whole cells: an export, and a page saved before bands ---------------------

/** A box in whole cells, as an export lays a page out and as a page saved before bands placed its tiles. */
export interface Cell {
  readonly id: string;
  readonly x: number;
  readonly y: number;
  readonly w: number;
  readonly h: number;
}

/** The most tiles side by side anywhere in `node`, and the most one above another: the least cells it needs. */
const across = (node: Node): number => (isLeaf(node) ? 1
  : node.split === 'row' ? node.parts.reduce((sum, part) => sum + across(part.node), 0)
    : Math.max(...node.parts.map((part) => across(part.node))));
const down = (node: Node): number => (isLeaf(node) ? 1
  : node.split === 'column' ? node.parts.reduce((sum, part) => sum + down(part.node), 0)
    : Math.max(...node.parts.map((part) => down(part.node))));

/**
 * `total` whole cells shared out as `shares` share it, each part at least its `least` (the total grown when the leasts
 * need more): what is left over the leasts goes by the largest remainder, so the parts always sum to the total.
 */
function shareOut(total: number, shares: readonly number[], least: readonly number[]): number[] {
  const floor = least.reduce((sum, n) => sum + n, 0);
  const room = Math.max(total, floor);
  const free = room - floor;
  const wants = shares.map((share, i) => Math.max(0, room * share - least[i]!));
  const wanted = wants.reduce((sum, n) => sum + n, 0);
  const quotas = wants.map((n) => (wanted > 0 ? (n / wanted) * free : free / shares.length));
  const out = quotas.map((q, i) => least[i]! + Math.floor(q));
  let left = room - out.reduce((sum, n) => sum + n, 0);
  const order = quotas.map((q, i) => ({ i, rest: q - Math.floor(q) })).sort((a, b) => b.rest - a.rest || a.i - b.i);
  for (const { i } of order) {
    if (left <= 0) break;
    out[i]! += 1;
    left -= 1;
  }
  return out;
}

/**
 * THE LAYOUT IN WHOLE CELLS, for an export: `cols` across (more when a band holds more tiles side by side) and `rows`
 * to a screenful. Every tile at least one cell each way, neighbours meeting exactly, nothing overlapping: each split
 * shares out its cells whole, each part at least what the tiles inside it need. On a page that fits its window, the
 * bands share one screenful's rows.
 */
export function cells(layout: Bands, cols = 24, rows = 24): { readonly cols: number; readonly rows: number; readonly tiles: readonly Cell[] } {
  const width = Math.max(cols, ...layout.bands.map((band) => across(band.node)));
  const out: Cell[] = [];
  const place = (node: Node, x: number, y: number, w: number, h: number): void => {
    if (isLeaf(node)) {
      // a stack exports its tile in front, in its place
      out.push({ id: isTile(node) ? node.tile : frontOf(node), x, y, w, h });
      return;
    }
    const row = node.split === 'row';
    const lengths = shareOut(row ? w : h, node.parts.map((part) => part.size),
      node.parts.map((part) => (row ? across(part.node) : down(part.node))));
    let at = row ? x : y;
    node.parts.forEach((part, i) => {
      const length = lengths[i]!;
      if (row) place(part.node, at, y, length, h);
      else place(part.node, x, at, w, length);
      at += length;
    });
  };
  const total = layout.bands.reduce((sum, band) => sum + band.height, 0);
  const heights = layout.fit && total > 0
    ? shareOut(rows, layout.bands.map((band) => band.height / total), layout.bands.map((band) => down(band.node)))
    : layout.bands.map((band) => Math.max(down(band.node), Math.round(band.height * rows)));
  let y = 0;
  layout.bands.forEach((band, i) => {
    place(band.node, 0, y, width, heights[i]!);
    y += heights[i]!;
  });
  return { cols: width, rows: y, tiles: out };
}

/**
 * A PAGE SAVED BEFORE BANDS (tiles on a grid `cols` wide, `rows` to a screenful), read as bands: cut where no tile
 * crosses -- across the page into bands, then inside each band down into columns or across into parts, as often as a
 * straight cut divides what is left -- each part's share its span. Tiles no straight cut divides (four round a middle)
 * are read side by side, in order across, each its width's share. Every tile is kept; the page scrolls, as it did.
 */
export function fromCells(tiles: readonly Cell[], rows: number): Bands {
  /** `rects` in groups no line across `axis` crosses, in order along it. */
  const cut = (rects: readonly Cell[], axis: 'x' | 'y'): Cell[][] => {
    const size = axis === 'x' ? 'w' : 'h';
    const sorted = [...rects].sort((a, b) => a[axis] - b[axis] || a.id.localeCompare(b.id));
    const groups: Cell[][] = [];
    let end = -Infinity;
    // in order along the axis, the furthest any rect so far reaches: one starting at or past it starts a new group
    for (const rect of sorted) {
      if (rect[axis] >= end) groups.push([rect]);
      else groups[groups.length - 1]!.push(rect);
      end = Math.max(end, rect[axis] + rect[size]);
    }
    return groups;
  };
  const span = (rects: readonly Cell[], axis: 'x' | 'y'): number => {
    const size = axis === 'x' ? 'w' : 'h';
    return Math.max(...rects.map((r) => r[axis] + r[size])) - Math.min(...rects.map((r) => r[axis]));
  };
  const nodeOf = (rects: readonly Cell[]): Node => {
    if (rects.length === 1) return { tile: rects[0]!.id };
    for (const [axis, direction] of [['x', 'row'], ['y', 'column']] as const) {
      const groups = cut(rects, axis);
      if (groups.length > 1) {
        const spans = groups.map((g) => span(g, axis));
        const sum = spans.reduce((a, b) => a + b, 0);
        return split(direction, groups.map(nodeOf), spans.map((s) => s / sum));
      }
    }
    const across = [...rects].sort((a, b) => a.x - b.x || a.y - b.y || a.id.localeCompare(b.id));
    const sum = across.reduce((a, r) => a + r.w, 0);
    return split('row', across.map((r) => ({ tile: r.id })), across.map((r) => r.w / sum));
  };
  const unique = tiles.filter((t, i) => tiles.findIndex((u) => u.id === t.id) === i);
  if (unique.length === 0) return EMPTY;
  return {
    fit: false,
    bands: cut(unique, 'y').map((group) => ({
      height: Math.max(MIN_BAND_HEIGHT, span(group, 'y') / Math.max(1, rows)),
      node: nodeOf(group),
    })),
  };
}

/** Where a divider snaps, as a share of its split: quarters, thirds and the half (§3.3). */
export const SNAPS: readonly number[] = [1 / 4, 1 / 3, 1 / 2, 2 / 3, 3 / 4];

/** The split found by `path` (the band's index, then a part's index at each split down to it), or undefined. */
function splitAt(layout: Bands, path: readonly number[]): SplitNode | undefined {
  const [bandIndex, ...inner] = path;
  let node: Node | undefined = layout.bands[bandIndex ?? -1]?.node;
  for (const i of inner) node = node === undefined || isLeaf(node) ? undefined : node.parts[i]?.node;
  return node === undefined || isLeaf(node) ? undefined : node;
}

/**
 * A divider's place in its split (the split found by `path`, the boundary after part `after`): the share of the
 * split before it. Undefined when there is no such divider.
 */
export function boundary(layout: Bands, path: readonly number[], after: number): number | undefined {
  const node = splitAt(layout, path);
  if (node === undefined || after + 1 >= node.parts.length) return undefined;
  return node.parts.slice(0, after + 1).reduce((sum, part) => sum + part.size, 0);
}

/** The two parts either side of a divider (the boundary after part `after`), each as its share of the split. */
export function sharesBeside(layout: Bands, path: readonly number[], after: number): [number, number] | undefined {
  const node = splitAt(layout, path);
  const left = node?.parts[after];
  const right = node?.parts[after + 1];
  return left && right ? [left.size, right.size] : undefined;
}

/** A share as a person reads it: a quarter, a third, the half (and their kin) by their sign; else a whole percent. */
export function shareText(share: number): string {
  const signs: readonly [number, string][] = [[1 / 4, '\u00bc'], [1 / 3, '\u2153'], [1 / 2, '\u00bd'], [2 / 3, '\u2154'], [3 / 4, '\u00be'], [1, '1']];
  const sign = signs.find(([value]) => Math.abs(share - value) < 0.005);
  return sign ? sign[1] : `${Math.round(share * 100)}%`;
}

/**
 * A divider's move, snapped: `delta` (a share of the split) changed so the divider lands on a quarter, a third or the
 * half of its split when it would come within `reach` of one (also a share: the snapping distance over the split's
 * length). Otherwise `delta` as it is.
 */
export function snapped(layout: Bands, path: readonly number[], after: number, delta: number, reach: number): number {
  const from = boundary(layout, path, after);
  if (from === undefined) return delta;
  const to = from + delta;
  const near = SNAPS.filter((s) => Math.abs(to - s) <= reach).sort((a, b) => Math.abs(to - a) - Math.abs(to - b))[0];
  return near === undefined ? delta : near - from;
}
