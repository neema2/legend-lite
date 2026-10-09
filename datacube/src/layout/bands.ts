// A PAGE'S LAYOUT, AS BANDS (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md, §3.2): a stack of bands, top to bottom; a band
// divided into columns, a column divided again (across into stacked parts, or into columns), down to tiles. Every band
// has a height, as a share of one screenful; a page that fits its window shares the window among its bands instead,
// and one that does not scrolls when they are taller than it.
//
// PURE: no DOM, no pixels, no state. Every function takes a layout and returns a new one, so
// what every gesture does -- a drop on a tile's edge or between bands, a divider moved, a preset, a tile added or
// removed -- is testable in node, and the pointer layer only turns the pointer into these calls.
//
// The rules, enforced by construction and checked by `problems`:
//   - every tile is in the layout once;
//   - a split has two parts or more, and their shares are positive and sum to 1 (a split left with one part becomes
//     that part; a band left with nothing goes);
//   - a split's parts never split the same way it does (a row in a row is one row): so the tree is the fewest
//     dividers that draw the page, and a divider means one thing;
//   - a band's height is positive.

/** A place in a band: a tile, or a split of places side by side (`row`) or one above another (`column`). */
export type Node = TileNode | SplitNode;

export interface TileNode {
  readonly tile: string;
}

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

/** Where a dragged tile can land: beside or above or below a tile, in place of it, or as a band of its own. */
export type Drop =
  | { readonly onto: string; readonly edge: 'left' | 'right' | 'top' | 'bottom' }
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

/** The tiles, in reading order: band by band, left to right, top to bottom within a band. */
export function tiles(layout: Bands): string[] {
  const out: string[] = [];
  const walk = (node: Node): void => {
    if (isTile(node)) out.push(node.tile);
    else for (const part of node.parts) walk(part.node);
  };
  for (const band of layout.bands) walk(band.node);
  return out;
}

/** What is wrong with a layout (nothing, for every layout these functions return). */
export function problems(layout: Bands): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const walk = (node: Node, parent: SplitNode['split'] | null, where: string): void => {
    if (isTile(node)) {
      if (seen.has(node.tile)) out.push(`${node.tile} is in the layout twice`);
      seen.add(node.tile);
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
    if (!isTile(node) && node.split === direction) {
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

/** `node` without `tile` (null when nothing is left), the hole closed: the neighbours take its share. */
function without(node: Node, tile: string): Node | null {
  if (isTile(node)) return node.tile === tile ? null : node;
  const kept: Part[] = [];
  for (const part of node.parts) {
    const rest = without(part.node, tile);
    if (rest !== null) kept.push({ node: rest, size: part.size });
  }
  if (kept.length === 0) return null;
  if (kept.length === 1) return kept[0]!.node;
  return split(node.split, kept.map((part) => part.node), normalized(kept).map((part) => part.size));
}

/** `node` with `tile`'s place replaced by `replacement`. */
function replaced(node: Node, tile: string, replacement: Node): Node {
  if (isTile(node)) return node.tile === tile ? replacement : node;
  return split(node.split, node.parts.map((part) => replaced(part.node, tile, replacement)),
    node.parts.map((part) => part.size));
}

function contains(node: Node, tile: string): boolean {
  return isTile(node) ? node.tile === tile : node.parts.some((part) => contains(part.node, tile));
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
  const has = isTile(band.node) ? 1 : band.node.split === 'row' ? band.node.parts.length : 1;
  if (has < Math.min(columns, MAX_COLUMNS)) {
    const node = isTile(band.node) || band.node.split !== 'row'
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
 * A DRAGGED TILE LET GO: onto a tile's edge (that tile divided there, the two sharing its place), onto a tile itself
 * (the two swapping places), or between bands (a band of its own at that place, `band` counting the bands above it).
 * A drop onto itself changes nothing.
 */
export function drop(layout: Bands, tile: string, where: Drop): Bands {
  if (!tiles(layout).includes(tile)) return layout;
  if ('swap' in where) {
    if (where.swap === tile || !tiles(layout).includes(where.swap)) return layout;
    const swapped = (node: Node): Node => (isTile(node)
      ? { tile: node.tile === tile ? where.swap : node.tile === where.swap ? tile : node.tile }
      : { split: node.split, parts: node.parts.map((part) => ({ node: swapped(part.node), size: part.size })) });
    return { fit: layout.fit, bands: layout.bands.map((band) => ({ height: band.height, node: swapped(band.node) })) };
  }
  if ('band' in where) {
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
  const target: Node = { tile: where.onto };
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
    if (isTile(node)) return node;
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
    if (isTile(node)) return node;
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
  const even = (node: Node): Node => (isTile(node) ? node
    : { split: node.split, parts: node.parts.map((part) => ({ node: even(part.node), size: 1 / node.parts.length })) });
  const height = layout.bands.reduce((sum, band) => sum + band.height, 0) / Math.max(1, layout.bands.length);
  return { fit: layout.fit, bands: layout.bands.map((band) => ({ height, node: even(band.node) })) };
}

/** The page fitting its window, or scrolling past it. */
export function fitted(layout: Bands, fit: boolean): Bands {
  return { fit, bands: layout.bands };
}

// -- presets ---------------------------------------------------------

/** The common layouts the picker offers (§3.3). */
export type Preset = 'side-by-side' | 'stacked' | 'grid-2' | 'grid-3' | 'top-and-row' | 'left-and-column'
  | 'right-and-column' | 'large-and-two';

/** What the picker calls each. */
export const PRESETS: readonly { readonly id: Preset; readonly label: string }[] = [
  { id: 'side-by-side', label: 'Side by side' },
  { id: 'stacked', label: 'Stacked' },
  { id: 'grid-2', label: '2 x 2' },
  { id: 'grid-3', label: '3 x 3' },
  { id: 'top-and-row', label: 'One on top, the rest below' },
  { id: 'left-and-column', label: 'One on the left, the rest stacked on the right' },
  { id: 'right-and-column', label: 'One on the right, the rest stacked on the left' },
  { id: 'large-and-two', label: 'One large, two beside it' },
];

const rows = (order: readonly string[], perRow: number): Band[] => {
  const out: Band[] = [];
  for (let i = 0; i < order.length; i += perRow) {
    out.push({ height: BAND_HEIGHT, node: split('row', order.slice(i, i + perRow).map((tile) => ({ tile }))) });
  }
  return out;
};

/**
 * THE TILES IN `order` ARRANGED AS A PRESET: the first tile takes the preset's first slot, and so on in reading order.
 * More tiles than slots: the rest go on as the preset goes on (2 x 2 continues in rows of two; one on top and the rest
 * below puts the rest in rows of MAX_COLUMNS). Fewer: the preset closes up (three tiles in 2 x 2 are a row of two and a
 * band of one). The page's fit is kept; a preset that is one screen (side by side, one with a column beside it) fills
 * its band's screen.
 */
export function arrange(layout: Bands, preset: Preset, order: readonly string[] = tiles(layout)): Bands {
  const ids = order.filter((tile, i) => order.indexOf(tile) === i);
  if (ids.length === 0) return { fit: layout.fit, bands: [] };
  const leaf = (tile: string): Node => ({ tile });
  const screen = (node: Node): Band[] => [{ height: 1, node }];
  let bands: Band[];
  switch (preset) {
    case 'side-by-side':
      bands = screen(split('row', ids.map(leaf)));
      break;
    case 'stacked':
      bands = ids.map((tile) => ({ height: BAND_HEIGHT, node: leaf(tile) }));
      break;
    case 'grid-2':
      bands = rows(ids, 2);
      break;
    case 'grid-3':
      bands = rows(ids, 3).map((band) => ({ height: 1 / 3, node: band.node }));
      break;
    case 'top-and-row':
      bands = [{ height: BAND_HEIGHT, node: leaf(ids[0]!) }, ...rows(ids.slice(1), MAX_COLUMNS)];
      break;
    case 'left-and-column':
    case 'right-and-column': {
      const [first, ...rest] = ids;
      const column = rest.length === 0 ? null : split('column', rest.map(leaf));
      const pair = column === null ? [leaf(first!)] : preset === 'left-and-column' ? [leaf(first!), column] : [column, leaf(first!)];
      bands = screen(split('row', pair, pair.length === 2 ? (preset === 'left-and-column' ? [0.6, 0.4] : [0.4, 0.6]) : undefined));
      break;
    }
    case 'large-and-two': {
      const [first, second, third, ...rest] = ids;
      const beside = [second, third].filter((tile): tile is string => tile !== undefined);
      const top = beside.length === 0 ? leaf(first!)
        : split('row', [leaf(first!), split('column', beside.map(leaf))], [2 / 3, 1 / 3]);
      bands = [{ height: rest.length > 0 ? 0.6 : 1, node: top }, ...rows(rest, MAX_COLUMNS)];
      break;
    }
  }
  return { fit: layout.fit, bands };
}

// -- narrow windows ----------------------------------------------------

/**
 * THE PAGE ON A NARROW WINDOW (§3.2, the user: stacked): every tile one under another, in reading order, each a band of
 * its own. Derived, never stored: the layout itself is unchanged, and comes back when the window widens.
 */
export function stacked(layout: Bands): Bands {
  return { fit: false, bands: tiles(layout).map((tile) => ({ height: BAND_HEIGHT, node: { tile } })) };
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

/** What the board draws: each tile's box, the dividers, each band's box, and the page's whole height. */
export interface Drawn {
  readonly tiles: ReadonlyMap<string, Box>;
  readonly dividers: readonly Divider[];
  readonly bands: readonly Box[];
  readonly height: number;
}

/**
 * THE LAYOUT IN PIXELS on a board `width` wide, a screenful `screen` high: bands one under another, `gap` apart, each
 * its height in screenfuls (never less than `least` pixels) -- or, when the page fits its window, sharing the screen as
 * their heights share their sum. Inside a band, parts share their split's length as their shares do, `gap` between
 * them. Edges are rounded, not lengths, so neighbours always meet and nothing drifts a pixel.
 */
export function draw(layout: Bands, width: number, screen: number, gap = 8, least = 120): Drawn {
  const tileBoxes = new Map<string, Box>();
  const dividers: Divider[] = [];
  const bandBoxes: Box[] = [];
  const count = layout.bands.length;
  const total = layout.bands.reduce((sum, band) => sum + band.height, 0);
  const free = Math.max(0, screen - gap * (count - 1));
  const heights = layout.bands.map((band) => (layout.fit && total > 0
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
  return { tiles: tileBoxes, dividers, bands: bandBoxes, height: count === 0 ? 0 : Math.round(y - gap) };
}

/** How far into a band, from its top or bottom edge, a drop makes a new band there (pixels). */
export const BAND_EDGE = 10;

/**
 * Where a dragged tile would land with the pointer at (`x`, `y`) on a board drawn as `drawn`: between bands (within
 * BAND_EDGE of a band's top or bottom edge, in the gap between two, above the first or below the last), on a tile's
 * edge (the quarter of it nearest that edge), or on a tile (its middle: a swap). Over the dragged tile itself, or
 * nowhere, undefined.
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
    if (nearest > 0.25) return { swap: id };
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
const across = (node: Node): number => (isTile(node) ? 1
  : node.split === 'row' ? node.parts.reduce((sum, part) => sum + across(part.node), 0)
    : Math.max(...node.parts.map((part) => across(part.node))));
const down = (node: Node): number => (isTile(node) ? 1
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
    if (isTile(node)) {
      out.push({ id: node.tile, x, y, w, h });
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

/**
 * A divider's place in its split (the split found by `path`, the boundary after part `after`): the share of the
 * split before it. Undefined when there is no such divider.
 */
export function boundary(layout: Bands, path: readonly number[], after: number): number | undefined {
  const [bandIndex, ...inner] = path;
  let node: Node | undefined = layout.bands[bandIndex ?? -1]?.node;
  for (const i of inner) node = node === undefined || isTile(node) ? undefined : node.parts[i]?.node;
  if (node === undefined || isTile(node) || after + 1 >= node.parts.length) return undefined;
  return node.parts.slice(0, after + 1).reduce((sum, part) => sum + part.size, 0);
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
