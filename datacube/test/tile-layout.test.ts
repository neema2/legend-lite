// The tile layout model, as datacube/test/window.test.ts tests window.ts:
// the arithmetic, in node, with no DOM. Run:
//   bazel test //datacube:tile_layout_test

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  type Layout,
  type Tile,
  beside,
  addToRow,
  below,
  dragTile,
  liftTile,
  removeTile,
  resizeShared,
  stepTile,
  bottom,
  collides,
  compact,
  describe as say,
  edgeRect,
  fitToColumns,
  moveBy,
  moveTile,
  place,
  problems,
  resizeBy,
  resizeRect,
  resizeTile,
} from '../src/layout/tile-layout.ts';

const T = (id: string, x: number, y: number, w: number, h: number, extra: Partial<Tile> = {}): Tile =>
  ({ id, x, y, w, h, ...extra });

/** id -> [x, y, w, h], for compact assertions. */
const pos = (l: Layout): Record<string, [number, number, number, number]> =>
  Object.fromEntries(l.map((t) => [t.id, [t.x, t.y, t.w, t.h]]));

const legal = (l: Layout, cols: number): void =>
  assert.deepEqual(problems(l, cols), [], JSON.stringify(pos(l)));

/** An ASCII picture of the page, for failure messages and a couple of exact checks. */
const draw = (l: Layout, cols: number): string => {
  const rows = Array.from({ length: bottom(l) }, () => Array(cols).fill('.'));
  for (const t of l) for (let y = t.y; y < t.y + t.h; y++) for (let x = t.x; x < t.x + t.w; x++) rows[y]![x] = t.id;
  return rows.map((r) => r.join('')).join('\n');
};

describe('collides', () => {
  it('shares a cell; touching edges is not a collision', () => {
    assert.equal(collides(T('a', 0, 0, 2, 2), T('b', 1, 1, 2, 2)), true);
    assert.equal(collides(T('a', 0, 0, 2, 2), T('b', 2, 0, 2, 2)), false);
    assert.equal(collides(T('a', 0, 0, 2, 2), T('b', 0, 2, 2, 2)), false);
    assert.equal(collides(T('a', 0, 0, 12, 1), T('b', 11, 0, 1, 1)), true);
  });
});

describe('compact (gravity)', () => {
  it('floats everything up, in reading order', () => {
    const l = compact([T('a', 0, 3, 6, 2), T('b', 6, 5, 6, 2), T('c', 0, 9, 12, 1)]);
    assert.deepEqual(pos(l), { a: [0, 0, 6, 2], b: [6, 0, 6, 2], c: [0, 2, 12, 1] });
  });

  it('never passes a tile THROUGH another to reach a gap above it', () => {
    // b cannot rise past a; the hole at row 0 under nothing stays a hole
    // for b, because b would have to go through a.
    const l = compact([T('a', 0, 1, 12, 1), T('b', 0, 4, 4, 1)]);
    assert.deepEqual(pos(l), { a: [0, 0, 12, 1], b: [0, 1, 4, 1] });
  });

  it('holds static tiles and stops others at them', () => {
    const l = compact([T('s', 0, 2, 12, 1, { static: true }), T('a', 0, 6, 4, 1), T('b', 4, 0, 4, 1)]);
    assert.deepEqual(pos(l), { s: [0, 2, 12, 1], a: [0, 3, 4, 1], b: [4, 0, 4, 1] });
  });

  it('holds a pinned tile (the one under the pointer)', () => {
    const l = compact([T('a', 0, 5, 4, 1), T('b', 4, 5, 4, 1)], new Set(['a']));
    assert.deepEqual(pos(l), { a: [0, 5, 4, 1], b: [4, 0, 4, 1] });
  });
});

describe('moveTile', () => {
  const stack = (): Tile[] => [T('a', 0, 0, 12, 2), T('b', 0, 2, 12, 2), T('c', 0, 4, 12, 2)];

  it('swaps with the tile below once dragged at least halfway onto it', () => {
    assert.deepEqual(pos(moveTile(stack(), 'a', 0, 2, 12)),
      { a: [0, 2, 12, 2], b: [0, 0, 12, 2], c: [0, 4, 12, 2] });
  });

  it('does NOT swap on a nudge less than that (the page settles back)', () => {
    assert.deepEqual(pos(moveTile(stack(), 'a', 0, 1, 12)), pos(stack()));
  });

  it('moving UP pushes the tile above down below it', () => {
    assert.deepEqual(pos(moveTile(stack(), 'c', 0, 0, 12)),
      { c: [0, 0, 12, 2], a: [0, 2, 12, 2], b: [0, 4, 12, 2] });
  });

  it('cascades pushes through a column of tiles', () => {
    // a 3-tall tile dropped at the top of a column of five
    const col = [T('x', 6, 0, 6, 3), ...['p', 'q', 'r', 's', 't'].map((id, i) => T(id, 0, i, 6, 1))];
    const l = moveTile(col, 'x', 0, 0, 12);
    legal(l, 12);
    assert.deepEqual(pos(l).x, [0, 0, 6, 3]);
    assert.deepEqual(['p', 'q', 'r', 's', 't'].map((id) => pos(l)[id]![1]), [3, 4, 5, 6, 7]);
  });

  it('cascades SIDEWAYS-overlapping pushes (a pushed tile lands on a third)', () => {
    // m is dropped over a; a is pushed onto b (which only a overlaps), b onto c.
    const l0 = [T('m', 8, 0, 4, 2), T('a', 0, 0, 4, 2), T('b', 2, 2, 4, 2), T('c', 4, 4, 4, 2)];
    const l = moveTile(l0, 'm', 0, 0, 12, { swap: true, hold: true });
    legal(l, 12);
    assert.deepEqual(pos(l), { m: [0, 0, 4, 2], a: [0, 2, 4, 2], b: [2, 4, 4, 2], c: [4, 6, 4, 2] }, draw(l, 12));
  });

  it('moves up into a gap without disturbing anyone', () => {
    // b is tall, so there is a hole under a at x 0-5, rows 2-3.
    const l0 = [T('a', 0, 0, 6, 2), T('b', 6, 0, 6, 4), T('c', 0, 4, 12, 2), T('d', 0, 6, 6, 2)];
    const l = moveTile(l0, 'd', 0, 2, 12);
    assert.deepEqual(pos(l), { a: [0, 0, 6, 2], b: [6, 0, 6, 4], c: [0, 4, 12, 2], d: [0, 2, 6, 2] });
  });

  it('a tile dropped into empty space below floats back up (gravity)', () => {
    const l = moveTile([T('a', 0, 0, 4, 2), T('b', 4, 0, 4, 2)], 'b', 8, 10, 12);
    assert.deepEqual(pos(l).b, [8, 0, 4, 2]);
  });

  it('while held (mid-drag) the tile stays under the pointer', () => {
    const l = moveTile([T('a', 0, 0, 4, 2), T('b', 4, 0, 4, 2)], 'b', 8, 10, 12, { hold: true });
    assert.deepEqual(pos(l).b, [8, 10, 4, 2]);
  });

  it('clamps to the columns', () => {
    const l = moveTile([T('a', 0, 0, 4, 2)], 'a', 11, -3, 12);
    assert.deepEqual(pos(l).a, [8, 0, 4, 2]);
  });

  it('never moves a static tile, by drag or by push', () => {
    const l0 = [T('s', 0, 2, 12, 1, { static: true }), T('a', 0, 0, 12, 2), T('b', 0, 3, 12, 2)];
    assert.deepEqual(pos(moveTile(l0, 's', 0, 0, 12)), pos(l0));
    // b dragged to the top: a is pushed down, but not through s -- it lands below s.
    const l = moveTile(l0, 'b', 0, 0, 12);
    legal(l, 12);
    assert.deepEqual(pos(l), { s: [0, 2, 12, 1], a: [0, 3, 12, 2], b: [0, 0, 12, 2] });
  });

  it('a tile dropped ON a static tile lands below it', () => {
    const l0 = [T('s', 0, 2, 12, 2, { static: true }), T('a', 0, 0, 6, 2), T('b', 6, 0, 6, 2)];
    const l = moveTile(l0, 'a', 0, 3, 12, { swap: true, hold: true });
    assert.deepEqual(pos(l).a, [0, 4, 6, 2]);
    assert.deepEqual(pos(l).s, [0, 2, 12, 2]);
  });

  it('the push cascade stops at a static and routes around it', () => {
    const l0 = [T('m', 0, 8, 12, 2), T('a', 0, 0, 6, 1), T('s', 0, 2, 6, 1, { static: true }), T('b', 0, 3, 6, 1)];
    const l = moveTile(l0, 'm', 0, 0, 12);
    legal(l, 12);
    assert.deepEqual(pos(l).s, [0, 2, 6, 1]);
    assert.deepEqual(pos(l).m, [0, 0, 12, 2]);
    assert.deepEqual(pos(l).a, [0, 3, 6, 1], draw(l, 12));
  });
});

describe('resizeTile', () => {
  it('growing into a neighbour below pushes it down', () => {
    const l = resizeTile([T('a', 0, 0, 6, 2), T('b', 0, 2, 6, 2)], 'a', 6, 4, 12);
    assert.deepEqual(pos(l), { a: [0, 0, 6, 4], b: [0, 4, 6, 2] });
  });

  it('growing SIDEWAYS into a neighbour pushes it below, and gravity keeps it there', () => {
    const l = resizeTile([T('a', 0, 0, 6, 2), T('b', 6, 0, 6, 2)], 'a', 8, 2, 12);
    assert.deepEqual(pos(l), { a: [0, 0, 8, 2], b: [6, 2, 6, 2] });
  });

  it('shrinking lets the tiles below float up', () => {
    const l = resizeTile([T('a', 0, 0, 12, 4), T('b', 0, 4, 12, 2)], 'a', 12, 1, 12);
    assert.deepEqual(pos(l).b, [0, 1, 12, 2]);
  });

  it('honours min/max and the column edge', () => {
    const a = T('a', 8, 0, 4, 4, { minW: 3, minH: 2, maxH: 6 });
    assert.deepEqual(pos(resizeTile([a], 'a', 1, 1, 12)).a, [8, 0, 3, 2]);
    assert.deepEqual(pos(resizeTile([a], 'a', 99, 99, 12)).a, [0, 0, 12, 6],
      'wider than the room right of it: the tile slides left to fit (normalize)');
  });

  it('a static tile stops growth on the axis that hits it, not the other', () => {
    const l0 = [T('a', 0, 0, 4, 2), T('s', 4, 0, 2, 1, { static: true })];
    const l = resizeTile(l0, 'a', 6, 4, 12);
    assert.deepEqual(pos(l).a, [0, 0, 4, 4]);
  });

  it('a north-west edge drag moves the corner and keeps the opposite one', () => {
    const t = T('a', 4, 4, 4, 4, { minW: 2, minH: 2 });
    assert.deepEqual(edgeRect(t, 'nw', -2, -1), { x: 2, y: 3, w: 6, h: 5 });
    assert.deepEqual(edgeRect(t, 'nw', 3, 3), { x: 6, y: 6, w: 2, h: 2 }, 'the minimum stops the edge');
    assert.deepEqual(edgeRect(t, 'w', -10, 0), { x: 0, y: 4, w: 8, h: 4 }, 'and the page edge');
  });

  it('a west-edge resize in the layout pushes what it covers', () => {
    const l0 = [T('a', 0, 0, 4, 2), T('b', 4, 0, 4, 2)];
    const l = resizeRect(l0, 'b', edgeRect(l0[1]!, 'w', -2, 0), 12);
    legal(l, 12);
    assert.deepEqual(pos(l).b, [2, 0, 6, 2]);
    assert.deepEqual(pos(l).a, [0, 2, 4, 2]);
  });
});

describe('keyboard (edit mode)', () => {
  const stack = (): Tile[] => [T('a', 0, 0, 12, 2), T('b', 0, 2, 12, 2), T('c', 0, 4, 12, 2)];

  it('ArrowDown swaps with the next tile below, not a no-op one-row nudge', () => {
    const r = moveBy(stack(), 'a', 0, 1, 12);
    assert.equal(r.changed, true);
    assert.deepEqual(pos(r.layout), { a: [0, 2, 12, 2], b: [0, 0, 12, 2], c: [0, 4, 12, 2] });
    const r2 = moveBy(r.layout, 'a', 0, 1, 12);
    assert.deepEqual(pos(r2.layout).a, [0, 4, 12, 2]);
  });

  it('ArrowDown on the last tile is blocked', () => {
    const r = moveBy(stack(), 'c', 0, 1, 12);
    assert.equal(r.changed, false);
  });

  it('ArrowUp swaps with the tile above; at the top it is blocked', () => {
    assert.deepEqual(pos(moveBy(stack(), 'b', 0, -1, 12).layout).b, [0, 0, 12, 2]);
    assert.equal(moveBy(stack(), 'a', 0, -1, 12).changed, false);
  });

  it('ArrowLeft/Right move one column and stop at the edges', () => {
    const l0 = [T('a', 0, 0, 4, 2)];
    assert.deepEqual(pos(moveBy(l0, 'a', 1, 0, 12).layout).a, [1, 0, 4, 2]);
    assert.equal(moveBy(l0, 'a', -1, 0, 12).changed, false);
    assert.equal(moveBy([T('a', 8, 0, 4, 2)], 'a', 1, 0, 12).changed, false);
  });

  it('POLICY (open): a sideways step into a neighbour pushes it down, as a drag does', () => {
    // a is 4 wide beside an 8-wide b; one step right overlaps b at every
    // x, so b is pushed below a. Honest but heavy for one key press --
    // gridstack swaps equal-sized tiles here; a product call, pinned here.
    const r = moveBy([T('a', 0, 0, 4, 2), T('b', 4, 0, 8, 4), T('c', 0, 2, 4, 2)], 'a', 1, 0, 12);
    assert.deepEqual(pos(r.layout), { a: [1, 0, 4, 2], b: [4, 2, 8, 4], c: [0, 2, 4, 2] });
  });

  it('a static tile ignores the keyboard', () => {
    assert.equal(moveBy([T('s', 0, 0, 4, 2, { static: true })], 's', 1, 0, 12).changed, false);
    assert.equal(resizeBy([T('s', 0, 0, 4, 2, { static: true })], 's', 1, 0, 12).changed, false);
  });

  it('Shift+arrows resize, bounded by min size and the page edge', () => {
    const l0 = [T('a', 0, 0, 4, 2, { minW: 3 }), T('b', 0, 2, 12, 1)];
    const grow = resizeBy(l0, 'a', 0, 1, 12);
    assert.deepEqual(pos(grow.layout), { a: [0, 0, 4, 3], b: [0, 3, 12, 1] });
    assert.equal(resizeBy(resizeBy(l0, 'a', -1, 0, 12).layout, 'a', -1, 0, 12).changed, false);
    assert.equal(resizeBy([T('a', 0, 0, 12, 2)], 'a', 1, 0, 12).changed, false);
  });

  it('says where the tile is, 1-based, for aria-live', () => {
    assert.equal(say(T('Sales', 0, 2, 6, 4), 12), 'column 1 of 12, row 3, 6 wide, 4 tall');
  });
});

describe('fitToColumns (responsive)', () => {
  // A 12-column page: two halves, then a full-width chart, then three thirds.
  const page = (): Tile[] => [
    T('grid', 0, 0, 6, 4), T('pie', 6, 0, 6, 4), T('trend', 0, 4, 12, 3),
    T('k1', 0, 7, 4, 2), T('k2', 4, 7, 4, 2), T('k3', 8, 7, 4, 2),
  ];

  it('12 -> 6 halves every width and keeps the picture', () => {
    const l = fitToColumns(page(), 12, 6);
    legal(l, 6);
    assert.deepEqual(pos(l), {
      grid: [0, 0, 3, 4], pie: [3, 0, 3, 4], trend: [0, 4, 6, 3],
      k1: [0, 7, 2, 2], k2: [2, 7, 2, 2], k3: [4, 7, 2, 2],
    });
  });

  it('12 -> 1 stacks the tiles in reading order', () => {
    const l = fitToColumns(page(), 12, 1);
    legal(l, 1);
    const order = [...l].sort((a, b) => a.y - b.y).map((t) => t.id);
    assert.deepEqual(order, ['grid', 'pie', 'trend', 'k1', 'k2', 'k3']);
    assert.ok(l.every((t) => t.x === 0 && t.w === 1));
    assert.equal(bottom(l), 4 + 4 + 3 + 2 + 2 + 2, 'heights kept, nothing overlapping or gapped');
  });

  it('reading order wins even for a tall tile on the right (1 column)', () => {
    // pie is on row 0 at the right, grid starts row 1 on the left: pie reads first.
    const l = fitToColumns([T('grid', 0, 1, 6, 2), T('pie', 6, 0, 6, 6)], 12, 1);
    assert.deepEqual(pos(l), { grid: [0, 6, 1, 2], pie: [0, 0, 1, 6] });
  });

  it('an uneven ratio (12 -> 5) rounds, never overlaps, keeps order', () => {
    const l = fitToColumns(page(), 12, 5);
    legal(l, 5);
    const ord = [...l].sort((a, b) => a.y - b.y || a.x - b.x).map((t) => t.id);
    assert.deepEqual(ord, ['grid', 'pie', 'trend', 'k1', 'k2', 'k3']);
  });

  it('an uneven ratio keeps side-by-side tiles side by side (edges scaled, not widths)', () => {
    // Regression found while prototyping: rounding x and w separately put
    // grid at 0-3 and pie at 2-5 on 5 columns, so pie was pushed under grid.
    const l = fitToColumns(page(), 12, 5);
    assert.equal(pos(l).grid![1], pos(l).pie![1], 'same row');
    assert.equal(pos(l).k1![1], pos(l).k3![1], 'the three KPIs stay on one row');
    assert.equal(bottom(l), 4 + 3 + 2, 'no staircase');
  });

  it('a tile narrower than one column on the small page keeps a column', () => {
    const l = fitToColumns([T('a', 0, 0, 1, 1), T('b', 1, 0, 1, 1), T('c', 2, 0, 10, 1)], 12, 3);
    legal(l, 3);
    assert.ok(l.every((t) => t.w >= 1));
  });

  it('is derived, not destructive: 12 -> 1 -> back is the canonical page', () => {
    const canonical = page();
    fitToColumns(canonical, 12, 1);
    assert.deepEqual(fitToColumns(canonical, 12, 12), canonical);
  });

  it('a minW wider than the page gives way to the page', () => {
    const l = fitToColumns([T('wide', 0, 0, 12, 2, { minW: 8 })], 12, 1);
    assert.deepEqual(pos(l).wide, [0, 0, 1, 2]);
  });
});

describe('invariants under random gestures (fuzz)', () => {
  // A seeded LCG: a failure reproduces.
  const rng = (seed: number) => () => (seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;

  for (const seed of [1, 2, 3, 4, 5, 42, 99, 2026]) {
    it(`seed ${seed}: 400 gestures keep the page legal and statics fixed`, () => {
      const r = rng(seed);
      const cols = 12;
      const n = 12;
      let l: Tile[] = compact(Array.from({ length: n }, (_, i) =>
        T(`t${i}`, (i * 4) % 12, Math.floor(i / 3) * 2, 4, 2, i % 7 === 3 ? { static: true } : {})));
      legal(l, cols);
      const statics = JSON.stringify(l.filter((t) => t.static));
      for (let k = 0; k < 400; k++) {
        const t = l[Math.floor(r() * n)]!;
        const op = Math.floor(r() * 5);
        const before = l;
        if (op === 0) l = moveTile(l, t.id, Math.floor(r() * 14) - 1, Math.floor(r() * 20) - 1, cols);
        else if (op === 1) l = resizeTile(l, t.id, 1 + Math.floor(r() * 8), 1 + Math.floor(r() * 5), cols);
        else if (op === 2) l = moveBy(l, t.id, Math.floor(r() * 3) - 1, Math.floor(r() * 3) - 1, cols).layout;
        else if (op === 3) l = resizeBy(l, t.id, Math.floor(r() * 3) - 1, Math.floor(r() * 3) - 1, cols).layout;
        else {
          // a drag: held steps must be legal too; then the drop settles it
          const x = Math.floor(r() * 12);
          const y = Math.floor(r() * 12);
          legal(place(l, t.id, { x, y, w: t.w, h: t.h }, cols, { hold: true, swap: true }), cols);
          l = moveTile(l, t.id, x, y, cols);
        }
        const bad = problems(l, cols);
        assert.deepEqual(compact(l), l, `step ${k}: not settled (gravity left a gap)`);
        assert.deepEqual(bad, [], `step ${k} op ${op} on ${t.id}\nbefore:\n${draw(before, cols)}\nafter:\n${draw(l, cols)}`);
        assert.equal(JSON.stringify(l.filter((t) => t.static)), statics, `step ${k}: a static tile moved`);
      }
      for (const c of [6, 4, 3, 1]) legal(fitToColumns(l, cols, c), c);
    });
  }

  it('gravity is a fixed point: compacting a settled page changes nothing', () => {
    const r = rng(7);
    let l: Tile[] = compact(Array.from({ length: 10 }, (_, i) => T(`t${i}`, (i * 3) % 12, i, 3, 1 + (i % 3))));
    for (let k = 0; k < 200; k++) {
      const t = l[Math.floor(r() * l.length)]!;
      l = moveTile(l, t.id, Math.floor(r() * 12), Math.floor(r() * 12), 12);
      assert.deepEqual(compact(l), l, `step ${k}`);
    }
  });
});

describe('scale', () => {
  // what a drag over 200 tiles must be: legal at every step. How fast it is belongs to a benchmark, not a verdict
  // (a wall-clock budget is the machine's: Bazel workplan P3-16)
  it('200 tiles: every drag step leaves a legal layout', () => {
    const l: Tile[] = compact(Array.from({ length: 200 }, (_, i) => T(`t${i}`, (i * 3) % 12, i, 3, 2)));
    for (let k = 0; k < 60; k++) legal(moveTile(l, 't150', k % 10, k % 30, 12, { hold: true, swap: true }), 12);
  });
});

describe('beside', () => {
  it('gives the main tile the whole width when it is alone', () => {
    assert.deepEqual(beside('grid', [], 12, 24, 7), [{ id: 'grid', x: 0, y: 0, w: 12, h: 24, anchor: true }]);
  });

  it('stacks the others on its right, sharing the height evenly', () => {
    const l = beside('grid', ['a', 'b'], 12, 24, 7);
    assert.deepEqual(l, [
      { id: 'grid', x: 0, y: 0, w: 7, h: 24, anchor: true },
      { id: 'a', x: 7, y: 0, w: 5, h: 12 },
      { id: 'b', x: 7, y: 12, w: 5, h: 12 },
    ]);
    assert.deepEqual(problems(l, 12), []);
  });

  it('hands the rows left over to the first tiles, and runs past the screen rather than below the least height', () => {
    assert.deepEqual(beside('g', ['a', 'b', 'c', 'd', 'e'], 12, 24, 7).slice(1).map((t) => t.h), [5, 5, 5, 5, 4]);
    const tall = beside('g', ['a', 'b', 'c', 'd', 'e'], 12, 24, 7, 6);
    assert.deepEqual(tall.slice(1).map((t) => t.h), [6, 6, 6, 6, 6]);
    assert.deepEqual(problems(tall, 12), []);
  });
});

describe('below', () => {
  it('puts the charts in a row under the main tile, sharing the width', () => {
    assert.deepEqual(below('grid', ['a', 'b', 'c'], 12, 24, 14, 4, 6), [
      { id: 'grid', x: 0, y: 0, w: 12, h: 14, anchor: true },
      { id: 'a', x: 0, y: 14, w: 4, h: 10 },
      { id: 'b', x: 4, y: 14, w: 4, h: 10 },
      { id: 'c', x: 8, y: 14, w: 4, h: 10 },
    ]);
  });

  it('starts a second row after four, and gives the main tile the page when alone', () => {
    const l = below('g', ['a', 'b', 'c', 'd', 'e'], 12, 24, 14, 4, 6);
    assert.deepEqual(l.filter((t) => t.y === 24).map((t) => [t.id, t.x, t.w]), [['e', 0, 12]]);
    assert.deepEqual(problems(l, 12), []);
    assert.deepEqual(below('g', [], 12, 24, 14), [{ id: 'g', x: 0, y: 0, w: 12, h: 24, anchor: true }]);
  });
});

/** Drag `id` by the cell one in from its top-left corner to `to`, as the board does. */
const dragTo = (l: Layout, id: string, to: [number, number]): Tile[] => {
  const t = l.find((u) => u.id === id)!;
  return dragTile(l, id, { x: to[0] - 1, y: to[1], w: t.w, h: t.h }, { x: to[0], y: to[1] }, 12);
};
const DEFAULT = below('grid', ['c1', 'c2', 'c3'], 12, 24, 14, 4, 6);

describe('dragTile: nothing moves unless the pointer is on it', () => {
  it('changes nothing while the pointer is still on the tile\'s own place', () => {
    assert.deepEqual(dragTo(DEFAULT, 'c3', [9, 15]), DEFAULT);
  });

  it('reorders a row: a chart dragged along it goes where the pointer is, the rest keep their order', () => {
    assert.deepEqual(pos(dragTo(DEFAULT, 'c1', [9, 16])), {
      grid: [0, 0, 12, 14], c1: [8, 14, 4, 10], c2: [0, 14, 4, 10], c3: [4, 14, 4, 10],
    });
  });

  it('swaps two tiles of the same size that are not a row', () => {
    const l = [T('a', 0, 0, 6, 6), T('b', 6, 0, 6, 6), T('c', 0, 6, 6, 6), T('d', 6, 6, 3, 6)];
    // a and c are the same size, on different rows: dropping a on c swaps them
    assert.deepEqual(pos(dragTile(l, 'a', { x: 0, y: 7, w: 6, h: 6 }, { x: 1, y: 7 }, 12)),
      { a: [0, 6, 6, 6], b: [6, 0, 6, 6], c: [0, 0, 6, 6], d: [6, 6, 3, 6] });
  });

  it('puts a chart beside the grid, as tall as it, and the row it left closes up', () => {
    assert.deepEqual(pos(dragTo(DEFAULT, 'c3', [11, 6])), {
      grid: [0, 0, 8, 14], c1: [0, 14, 6, 10], c2: [6, 14, 6, 10], c3: [8, 0, 4, 14],
    });
  });

  it('puts it back into the row, at the row\'s height, where the pointer is; the grid widens again', () => {
    const side = dragTo(DEFAULT, 'c3', [11, 6]);
    assert.deepEqual(pos(dragTo(side, 'c3', [6, 20])), {
      grid: [0, 0, 12, 14], c1: [0, 14, 4, 10], c2: [8, 14, 4, 10], c3: [4, 14, 4, 10],
    });
  });

  it('puts a chart above the grid from the grid\'s top edge', () => {
    const l = removeTile(removeTile(DEFAULT, 'c2', 12), 'c3', 12);
    assert.deepEqual(pos(dragTo(l, 'c1', [5, 1])), { grid: [0, 10, 12, 14], c1: [0, 0, 12, 10] });
  });

  it('never squeezes a tile to less than half its width to make room beside it', () => {
    // the grid is 7 wide: a 5-wide chart on its edge is narrowed to 3, the grid keeps 4
    const l = beside('grid', ['c1', 'c2', 'c3'], 12, 24, 7, 6);
    const r = dragTo(l, 'c1', [6, 7]);
    assert.deepEqual([pos(r).grid, pos(r).c1], [[0, 0, 4, 24], [4, 0, 3, 24]]);
    // with the chart's minimum 4, it cannot: it goes below the grid instead
    const min4 = l.map((t) => (t.id === 'c1' ? { ...t, minW: 4 } : t));
    assert.equal(pos(dragTo(min4, 'c1', [6, 7])).grid![2], 7);
  });

  // the moves a user made, from a grid with its charts stacked beside it
  it('moves every chart from the side to under the grid, one at a time', () => {
    let l = beside('grid', ['c1', 'c2', 'c3'], 12, 24, 7, 6);
    l = dragTo(l, 'c1', [1, 26]);
    assert.deepEqual(pos(l).c1, [0, 24, 5, 8], 'the first goes under the grid, the grid unmoved');
    assert.deepEqual(pos(l).grid, [0, 0, 7, 24]);
    l = dragTo(l, 'c2', [6, 26]);
    assert.deepEqual(pos(l).c2, [5, 24, 5, 8], 'the second beside it');
    l = dragTo(l, 'c3', [11, 25]);
    assert.deepEqual(pos(l), {
      grid: [0, 0, 12, 24], c1: [0, 24, 4, 8], c2: [4, 24, 4, 8], c3: [8, 24, 4, 8],
    }, 'the last: the grid takes its width back, the three share the row');
    assert.deepEqual(problems(l, 12), []);
  });

  it('leaves the page legal whatever the pointer does', () => {
    let seed = 7;
    const rnd = (n: number) => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
    let l: Tile[] = DEFAULT;
    for (let i = 0; i < 400; i++) {
      const id = l[rnd(l.length)]!.id;
      l = dragTo(l, id, [rnd(12), rnd(40)]);
      assert.deepEqual(problems(l, 12), [], `step ${i}`);
      assert.equal(l.length, 4);
    }
  });
});

describe('liftTile / removeTile', () => {
  it('closes a row over the gap: the rest share its width', () => {
    assert.deepEqual(pos(removeTile(DEFAULT, 'c2', 12)), {
      grid: [0, 0, 12, 14], c1: [0, 14, 6, 10], c3: [6, 14, 6, 10],
    });
  });

  it('gives the grid its width back when the chart beside it goes', () => {
    const side = dragTo(DEFAULT, 'c3', [11, 6]);
    assert.deepEqual(pos(liftTile(side, 'c3', 12)).grid, [0, 0, 12, 14]);
  });

  it('does not widen into space another tile needs', () => {
    const l = beside('grid', ['c1', 'c2'], 12, 24, 7, 6);
    // c1 goes, c2 floats up into its place: the grid stays 7
    assert.deepEqual(pos(removeTile(l, 'c1', 12)).grid, [0, 0, 7, 24]);
  });
});

describe('the side column', () => {
  const side = dragTo(DEFAULT, 'c3', [11, 6]); // grid 8 wide, c3 beside it, c1 c2 6 wide below

  it('takes a wider chart beside the grid, narrowed to fit', () => {
    const l = dragTo(side, 'c1', [7, 6]);
    assert.deepEqual(pos(l).grid, [0, 0, 4, 14]);
    assert.deepEqual(pos(l).c1, [4, 0, 4, 14]);
    assert.deepEqual(problems(l, 12), []);
  });

  it('stacks a chart under the side chart, splitting its height', () => {
    const l = dragTo(side, 'c1', [10, 10]);
    assert.deepEqual(pos(l), {
      grid: [0, 0, 8, 14], c3: [8, 0, 4, 7], c1: [8, 7, 4, 7], c2: [0, 14, 12, 10],
    });
  });

  it('and above it, from its upper half', () => {
    const l = dragTo(side, 'c1', [10, 2]);
    assert.deepEqual(pos(l).c1, [8, 0, 4, 7]);
    assert.deepEqual(pos(l).c3, [8, 7, 4, 7]);
  });

  it('becomes one tile again when either half leaves', () => {
    const l = dragTo(side, 'c1', [10, 10]);
    assert.deepEqual(pos(removeTile(l, 'c1', 12)).c3, [8, 0, 4, 14]);
    assert.deepEqual(pos(removeTile(l, 'c3', 12)).c1, [8, 0, 4, 14]);
  });
});

describe('addToRow', () => {
  it('puts a new chart at the end of the bottom row, the row sharing the width', () => {
    const l = removeTile(DEFAULT, 'c3', 12); // c1 c2 at 6 wide
    assert.deepEqual(pos(addToRow(l, 'n', 12, 10)), {
      grid: [0, 0, 12, 14], c1: [0, 14, 4, 10], c2: [4, 14, 4, 10], n: [8, 14, 4, 10],
    });
  });

  it('shares the bottom row with a lone chart, however wide', () => {
    const l = [T('grid', 0, 0, 12, 14, { anchor: true }), T('a', 0, 14, 12, 10)];
    assert.deepEqual(pos(addToRow(l, 'n', 12, 10)), { grid: [0, 0, 12, 14], a: [0, 14, 6, 10], n: [6, 14, 6, 10] });
  });

  it('starts a new row when the bottom row is full, or is only the grid', () => {
    const four = below('grid', ['a', 'b', 'c', 'd'], 12, 24, 14, 4, 6);
    assert.deepEqual(pos(addToRow(four, 'n', 12, 10)).n, [0, 24, 12, 10]);
    assert.deepEqual(pos(addToRow([T('grid', 0, 0, 12, 24, { anchor: true })], 'n', 12, 10)).n, [0, 24, 12, 10]);
    // the grid with a chart beside it is the top row, not one to join
    assert.deepEqual(pos(addToRow([T('grid', 0, 0, 8, 14, { anchor: true }), T('s', 8, 0, 4, 14)], 'n', 12, 10)).n, [0, 14, 12, 10]);
  });
});

describe('resizeShared: a resize moves the edge it shares', () => {
  it('widens a chart in a row by narrowing its neighbour', () => {
    const l = resizeShared(DEFAULT, 'c1', 6, 10, 12);
    assert.deepEqual(pos(l), { grid: [0, 0, 12, 14], c1: [0, 14, 6, 10], c2: [6, 14, 2, 10], c3: [8, 14, 4, 10] });
  });

  it('makes the grid taller by shortening the row under it, keeping the screenful', () => {
    const l = resizeShared(DEFAULT, 'grid', 12, 16, 12);
    assert.deepEqual(pos(l), { grid: [0, 0, 12, 16], c1: [0, 16, 4, 8], c2: [4, 16, 4, 8], c3: [8, 16, 4, 8] });
  });

  it('pushes down the old way when a neighbour would go below its minimum', () => {
    const min = DEFAULT.map((t) => (t.id === 'c2' ? { ...t, minW: 3 } : t));
    const l = resizeShared(min, 'c1', 6, 10, 12);
    assert.deepEqual(problems(l, 12), []);
    assert.equal(pos(l).c2![2], 4, 'the neighbour keeps its width');
  });

  it('leaves the page legal whatever the sizes asked', () => {
    let seed = 11;
    const rnd = (n: number) => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
    let l: Tile[] = DEFAULT;
    for (let i = 0; i < 300; i++) {
      const t = l[rnd(l.length)]!;
      l = resizeShared(l, t.id, 1 + rnd(12), 1 + rnd(20), 12);
      assert.deepEqual(problems(l, 12), [], `step ${i}`);
    }
  });
});

describe('stepTile: the keyboard moves a tile as the drag would', () => {
  it('swaps along a row', () => {
    const r = stepTile(DEFAULT, 'c1', 1, 0, 12);
    assert.ok(r.changed);
    assert.deepEqual([pos(r.layout).c1![0], pos(r.layout).c2![0]], [4, 0]);
  });

  it('goes above the grid from the row under it, as wide as the grid', () => {
    const r = stepTile(removeTile(removeTile(DEFAULT, 'c2', 12), 'c3', 12), 'c1', 0, -1, 12);
    assert.deepEqual(pos(r.layout), { grid: [0, 10, 12, 14], c1: [0, 0, 12, 10] });
  });

  it('says when nothing moved', () => {
    assert.equal(stepTile(DEFAULT, 'c1', -1, 0, 12).changed, false);
    assert.equal(stepTile(DEFAULT, 'grid', 0, -1, 12).changed, false);
  });
});

describe('above a tile', () => {
  it('a chart dropped on the grid\'s top goes above it as wide as it, no gap', () => {
    const l = removeTile(removeTile(DEFAULT, 'c2', 12), 'c3', 12);
    assert.deepEqual(pos(dragTo(l, 'c1', [9, 1])), { grid: [0, 10, 12, 14], c1: [0, 0, 12, 10] });
  });

  it('joins the row already above it', () => {
    let l = removeTile(DEFAULT, 'c3', 12); // c1 c2 below at 6
    l = dragTo(l, 'c1', [5, 1]);           // c1 above the grid, full width
    const r = dragTo(l, 'c2', [9, 11]);    // c2 onto the grid's top band
    assert.deepEqual([pos(r).c1![1], pos(r).c2![1]], [0, 0], 'both in the row above');
    assert.deepEqual(problems(r, 12), []);
  });
});
