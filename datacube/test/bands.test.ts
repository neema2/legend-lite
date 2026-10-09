// A page's layout as bands (src/layout/bands.ts): the arithmetic, in node, with
// no DOM -- every gesture, every preset, the narrow page, and a fuzz of random gestures that must keep every rule.
// Run:
//   bazel test //datacube:bands_test

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  type Bands,
  type Node,
  EMPTY,
  MAX_COLUMNS,
  MIN_SHARE,
  MIN_BAND_HEIGHT,
  add,
  arrange,
  asSaved,
  bandOf,
  bringToFront,
  places,
  reorderStack,
  stackOf,
  shareText,
  sharesBeside,
  boundary,
  cells,
  draw,
  drop,
  dividerBeside,
  dropAt,
  evenAll,
  evenOut,
  fitted,
  fromCells,
  layoutsFor,
  neighbour,
  problems,
  remove,
  resize,
  resizeBand,
  snapped,
  stacked,
  tiles,
  tradeBands,
} from '../src/layout/bands.ts';

/**
 * A layout as a picture: bands on lines, `|` between columns, `/` between stacked parts, shares rounded; a stack of
 * tiles `{a,b*}`, its tile in front starred.
 */
function picture(layout: Bands): string[] {
  const draw = (node: Node): string => ('tile' in node ? node.tile
    : 'stack' in node ? `{${node.stack.map((t) => (t === (node.front ?? node.stack[0]) ? `${t}*` : t)).join(',')}}`
    : `${node.split === 'row' ? '[' : '('}${node.parts.map((p) => `${draw(p.node)}:${Math.round(p.size * 100)}`)
      .join(node.split === 'row' ? ' | ' : ' / ')}${node.split === 'row' ? ']' : ')'}`);
  return layout.bands.map((band) => draw(band.node));
}

const sound = (layout: Bands): Bands => {
  assert.deepEqual(problems(layout), []);
  return layout;
};

const page = (...ids: string[]): Bands => ids.reduce((layout, id) => add(layout, id), EMPTY);

describe('adding a tile places it where it is wanted', () => {
  it('beside the tile it came from, while that band has room', () => {
    let layout = add(EMPTY, 'grid');
    layout = add(layout, 'chart-1', 'grid');
    layout = add(layout, 'chart-2', 'grid');
    assert.deepEqual(picture(sound(layout)), ['[grid:33 | chart-1:33 | chart-2:33]']);
  });

  it('in a band of its own below, once that band is full', () => {
    let layout = add(EMPTY, 'a');
    for (let i = 1; i < MAX_COLUMNS; i += 1) layout = add(layout, `t${i}`, 'a');
    layout = add(layout, 'next', 'a');
    assert.equal(sound(layout).bands.length, 2);
    assert.deepEqual(picture(layout)[1], 'next');
  });

  it('at the bottom of the page when it comes from nowhere; and a tile already there stays as it is', () => {
    const layout = page('a', 'b');
    assert.deepEqual(picture(sound(layout)), ['a', 'b']);
    assert.equal(add(layout, 'a'), layout);
  });
});

describe('removing a tile closes its place', () => {
  it('its neighbours take its share; a split of one is that one; an emptied band goes', () => {
    let layout = add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'a');
    layout = remove(layout, 'b');
    assert.deepEqual(picture(sound(layout)), ['[a:50 | c:50]']);
    layout = remove(layout, 'c');
    assert.deepEqual(picture(sound(layout)), ['a']);
    assert.deepEqual(sound(remove(layout, 'a')).bands, []);
  });

  it('a row left inside a row folds into it (a divider means one thing)', () => {
    // a | (b / [c | d]): without b, the column is [c | d], and [a | [c | d]] is [a | c | d]
    let layout = page('a');
    layout = drop(add(layout, 'b'), 'b', { onto: 'a', edge: 'right' });
    layout = drop(add(layout, 'c'), 'c', { onto: 'b', edge: 'bottom' });
    layout = drop(add(layout, 'd'), 'd', { onto: 'c', edge: 'right' });
    layout = remove(layout, 'b');
    assert.equal(sound(layout).bands.length, 1);
    assert.deepEqual(tiles(layout), ['a', 'c', 'd']);
    assert.match(picture(layout)[0]!, /^\[a:\d+ \| c:\d+ \| d:\d+\]$/);
  });
});

describe('a dragged tile let go', () => {
  it('on an edge divides that tile there: left, right, top, bottom', () => {
    const cases: ['left' | 'right' | 'top' | 'bottom', string][] = [
      ['left', '[b:50 | a:50]'], ['right', '[a:50 | b:50]'], ['top', '(b:50 / a:50)'], ['bottom', '(a:50 / b:50)'],
    ];
    for (const [edge, want] of cases) {
      const layout = drop(page('a', 'b'), 'b', { onto: 'a', edge });
      assert.deepEqual(picture(sound(layout)), [want], edge);
    }
  });

  it('one big on the left, two stacked on the right (the user, 2026-10-09)', () => {
    let layout = drop(page('big', 'top'), 'top', { onto: 'big', edge: 'right' });
    layout = drop(add(layout, 'bottom'), 'bottom', { onto: 'top', edge: 'bottom' });
    assert.deepEqual(picture(sound(layout)), ['[big:50 | (top:50 / bottom:50):50]']);
  });

  it('on a tile swaps the two', () => {
    const layout = drop(add(add(EMPTY, 'a'), 'b', 'a'), 'a', { swap: 'b' });
    assert.deepEqual(picture(sound(layout)), ['[b:50 | a:50]']);
  });

  it('between bands makes a band of its own there, counted as the page is without it', () => {
    const layout = page('a', 'b', 'c');
    assert.deepEqual(picture(sound(drop(layout, 'c', { band: 0 }))), ['c', 'a', 'b']);
    assert.deepEqual(picture(sound(drop(layout, 'a', { band: 3 }))), ['b', 'c', 'a']);
    // out of a band it shared: the band keeps the rest
    const shared = add(add(EMPTY, 'x'), 'y', 'x');
    assert.deepEqual(picture(sound(drop(shared, 'y', { band: 1 }))), ['x', 'y']);
  });

  it('on itself, or on a tile not there, changes nothing', () => {
    const layout = page('a', 'b');
    assert.equal(drop(layout, 'a', { onto: 'a', edge: 'left' }), layout);
    assert.equal(drop(layout, 'a', { swap: 'a' }), layout);
    assert.equal(drop(layout, 'a', { onto: 'nope', edge: 'left' }), layout);
    assert.equal(drop(layout, 'nope', { band: 0 }), layout);
  });
});

describe('dividers and band heights', () => {
  it('a divider trades share between its two parts, never below the least share', () => {
    const layout = add(add(EMPTY, 'a'), 'b', 'a');
    assert.deepEqual(picture(sound(resize(layout, [0], 0, 0.2))), ['[a:70 | b:30]']);
    const far = sound(resize(layout, [0], 0, 5)).bands[0]!.node;
    assert.ok('parts' in far && Math.abs(far.parts[1]!.size - MIN_SHARE) < 1e-9, 'the right part keeps its least share');
  });

  it('a divider inside a part, found by its path', () => {
    let layout = drop(page('big', 'top'), 'top', { onto: 'big', edge: 'right' });
    layout = drop(add(layout, 'bottom'), 'bottom', { onto: 'top', edge: 'bottom' });
    assert.deepEqual(picture(sound(resize(layout, [0, 1], 0, -0.2))), ['[big:50 | (top:30 / bottom:70):50]']);
  });

  it('even out: a split\'s parts share alike', () => {
    const layout = resize(add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'a'), [0], 0, 0.2);
    assert.deepEqual(picture(sound(evenOut(layout, [0]))), ['[a:33 | b:33 | c:33]']);
  });

  it('a band\'s height, never below its least; the page fits its window or scrolls', () => {
    const layout = page('a');
    assert.equal(resizeBand(layout, 0, 0.9).bands[0]!.height, 0.9);
    assert.equal(resizeBand(layout, 0, 0).bands[0]!.height, MIN_BAND_HEIGHT);
    assert.equal(fitted(layout, true).fit, true);
  });
});

describe('presets arrange the tiles in reading order', () => {
  const five = ['a', 'b', 'c', 'd', 'e'];
  const six = [...five, 'f'];
  const cases: [Parameters<typeof arrange>[1], string[], string[]][] = [
    ['side-by-side', ['a', 'b'], ['[a:50 | b:50]']],
    ['stacked', ['a', 'b'], ['a', 'b']],
    ['rows:2-2', ['a', 'b', 'c', 'd'], ['[a:50 | b:50]', '[c:50 | d:50]']],
    ['rows:2-2', ['a', 'b', 'c'], ['[a:50 | b:50]', 'c']],
    ['rows:2-2', five, ['[a:50 | b:50]', '[c:50 | d:50]', 'e']],
    ['rows:3-1', ['a', 'b', 'c', 'd'], ['[a:33 | b:33 | c:33]', 'd']],
    ['rows:1-3', ['a', 'b', 'c', 'd'], ['a', '[b:33 | c:33 | d:33]']],
    ['rows:3-1-1', five, ['[a:33 | b:33 | c:33]', 'd', 'e']],
    ['rows:1-3-1', five, ['a', '[b:33 | c:33 | d:33]', 'e']],
    ['focus-top', ['a', 'b', 'c'], ['a', '[b:50 | c:50]']],
    ['focus-bottom', ['a', 'b', 'c'], ['[b:50 | c:50]', 'a']],
    // even, as every standard shape is (the user, 2026-10-09): the one is the largest by spanning the page
    ['focus-left', ['a', 'b', 'c'], ['[a:50 | (b:50 / c:50):50]']],
    ['focus-right', ['a', 'b', 'c'], ['[(b:50 / c:50):50 | a:50]']],
    ['focus-left', six, ['[a:50 | ([b:50 | c:50]:33 / [d:50 | e:50]:33 / f:33):50]']],
    // the rest in near-equal rows: 3 and 2, never 4 and a lone 1 (the user, 2026-10-09, checking the standard shapes)
    ['focus-top', six, ['a', '[b:33 | c:33 | d:33]', '[e:50 | f:50]']],
    ['focus-bottom', [...six, 'g', 'h', 'i'], ['[b:25 | c:25 | d:25 | e:25]', '[f:25 | g:25 | h:25 | i:25]', 'a']],
    ['columns:2-1', ['a', 'b', 'c'], ['[(a:50 / b:50):50 | c:50]']],
  ];
  for (const [preset, order, want] of cases) {
    it(`${preset} of ${order.length}`, () => {
      assert.deepEqual(picture(sound(arrange(EMPTY, preset, order))), want);
    });
  }

  it('every layout offered, for every count from 1 to 10, keeps every rule and every tile, the first in its first slot', () => {
    for (let n = 1; n <= 10; n += 1) {
      const order = Array.from({ length: n }, (_, i) => `t${i}`);
      for (const { id } of layoutsFor(n)) {
        const layout = sound(arrange(EMPTY, id, order));
        // in reading order -- but for one large on the right or below, whose main tile reads after the rest
        if (id === 'focus-right' || id === 'focus-bottom') assert.deepEqual([...tiles(layout)].sort(), [...order].sort(), `${id} of ${n}`);
        else assert.deepEqual(tiles(layout), order, `${id} of ${n}`);
      }
    }
  });

  it('a layout it does not know is refused by name', () => {
    assert.throws(() => arrange(EMPTY, 'rows:2-x' as never, ['a']), /not a layout: rows:2-x/);
    assert.throws(() => arrange(EMPTY, 'diagonal' as never, ['a', 'b']), /not a layout: diagonal/);
  });
});

describe('the layouts offered for a number of tiles (the user, 2026-10-09: several on top then one and one, too)', () => {
  /** A layout's shape: its boxes, whichever tile is where. */
  const shape = (id: Parameters<typeof arrange>[1], n: number): string =>
    [...draw(fitted(arrange(EMPTY, id, Array.from({ length: n }, (_, i) => `t${i}`)), true), 120, 80, 0, 0).tiles.values()]
      .map((b) => `${b.x},${b.y},${b.w},${b.h}`).sort().join(' ');
  it('one tile: one layout', () => {
    assert.deepEqual(layoutsFor(1).map((l) => l.id), ['side-by-side']);
  });
  it('four tiles: every way into rows, one large on each side, and columns -- each a different shape', () => {
    const four = layoutsFor(4);
    // 3 over 1 and 1 over 3 are the even "one below" and "one on top": shown there, once
    assert.deepEqual(four.filter((l) => l.group === 'rows').map((l) => l.id), [
      'side-by-side', 'stacked', 'rows:2-2', 'rows:2-1-1', 'rows:1-2-1', 'rows:1-1-2',
    ]);
    assert.deepEqual(four.filter((l) => l.featured).map((l) => l.id),
      ['side-by-side', 'stacked', 'rows:2-2', 'focus-left', 'focus-right', 'focus-top', 'focus-bottom'],
      'the standard shapes, first: the grid is two rows of two, and two columns of two the same shape');
    assert.deepEqual(four.filter((l) => l.group === 'large').map((l) => l.id), ['focus-left', 'focus-right', 'focus-top', 'focus-bottom']);
    assert.deepEqual(four.filter((l) => l.group === 'columns').map((l) => l.id), ['columns:2-1-1', 'columns:1-1-2'],
      'two columns of two is 2 rows of 2: left out');
    assert.equal(new Set(four.map((l) => shape(l.id, 4))).size, four.length);
    assert.equal(four.find((l) => l.id === 'rows:2-1-1')!.label, 'Rows of 2, 1, 1');
    assert.equal(four.find((l) => l.id === 'focus-top')!.label, 'One on top, the rest below');
    assert.equal(four.find((l) => l.id === 'rows:2-2')!.label, 'Grid (2, 2)', 'named as the standard shape it is');
    assert.equal(layoutsFor(6).find((l) => l.id === 'columns:3-3')!.label, 'Two columns');
  });
  it('five tiles: several on top then one and one, and one, several, one', () => {
    const ids = layoutsFor(5).map((l) => l.id);
    // four over one and one over four: "one below" and "one on top", even
    for (const id of ['rows:3-1-1', 'rows:1-3-1', 'rows:1-1-3', 'rows:2-2-1', 'focus-top', 'focus-bottom'] as const) assert.ok(ids.includes(id), id);
  });
  it('many tiles: rows of four at most, three rows at most, so the list stays one to read', () => {
    for (let n = 6; n <= 12; n += 1) {
      const all = layoutsFor(n);
      // at most 26 (seven tiles); the picker scrolls past a screen of them
      assert.ok(all.length <= 30, `${n} tiles: ${all.length} layouts`);
      // the standard shapes first, nine at most and in the same order; the rest, rows of four at most
      const featured = all.filter((l) => l.featured);
      assert.ok(featured.length <= 9 && all.slice(0, featured.length).every((l) => l.featured), `${n} tiles: the standard shapes first`);
      for (const { id } of all.filter((l) => !l.featured && l.id.startsWith('rows:'))) {
        const counts = id.slice(5).split('-').map(Number);
        assert.ok(counts.length <= 3 && counts.every((c) => c <= 4), `${n} tiles: ${id}`);
      }
      assert.equal(new Set(all.map((l) => shape(l.id, n))).size, all.length, `${n} tiles: a shape twice`);
    }
  });
});

describe('a narrow window stacks the page', () => {
  it('every tile a band of its own, in reading order; the layout itself unchanged', () => {
    const layout = add(arrange(EMPTY, 'focus-left', ['a', 'b', 'c']), 'd');
    const narrow = sound(stacked(layout));
    assert.deepEqual(picture(narrow), ['a', 'b', 'c', 'd']);
    assert.equal(narrow.fit, false);
    assert.equal(bandOf(layout, 'd'), 1);
  });
});

/** `layout` in whole cells: every tile at least a cell each way, inside the page, and the areas summing to the page's. */
function exactCover(layout: Bands, where: string): void {
  const grid = cells(layout);
  // every place once: a stack as its tile in front
  assert.deepEqual(grid.tiles.map((t) => t.id).sort(), [...places(layout)].sort(), where);
  const taken = new Set<string>();
  for (const t of grid.tiles) {
    assert.ok(t.w >= 1 && t.h >= 1 && t.x >= 0 && t.y >= 0 && t.x + t.w <= grid.cols && t.y + t.h <= grid.rows,
      `${where}: ${JSON.stringify(t)} in ${grid.cols} x ${grid.rows}`);
    for (let x = t.x; x < t.x + t.w; x += 1) {
      for (let y = t.y; y < t.y + t.h; y += 1) {
        assert.ok(!taken.has(`${x},${y}`), `${where}: ${t.id} overlaps at ${x},${y}`);
        taken.add(`${x},${y}`);
      }
    }
  }
  assert.equal(taken.size, grid.cols * grid.rows, `${where}: every cell is a tile's`);
}

describe('a fuzz of gestures keeps every rule', () => {
  // a small seeded generator, so a failure is the same failure every run
  const random = (seed: number): (() => number) => () => {
    seed = (seed * 1103515245 + 12345) % 2 ** 31;
    return seed / 2 ** 31;
  };
  for (const seed of [1, 7, 42, 1999, 2026]) {
    it(`seed ${seed}: 3,000 gestures`, () => {
      const next = random(seed);
      const pick = <T>(items: readonly T[]): T => items[Math.floor(next() * items.length)]!;
      let layout: Bands = EMPTY;
      let made = 0;
      for (let step = 0; step < 3000; step += 1) {
        const ids = tiles(layout);
        const gesture = ids.length === 0 ? 0 : Math.floor(next() * 17);
        /** A split somewhere in the page, by its path, and one of its dividers: nested ones too. */
        const somewhere = (): { path: number[]; after: number } | undefined => {
          const band = Math.floor(next() * layout.bands.length);
          let node = layout.bands[band]!.node;
          const path = [band];
          while ('parts' in node && next() < 0.5) {
            const i = Math.floor(next() * node.parts.length);
            if ('tile' in node.parts[i]!.node) break;
            path.push(i);
            node = node.parts[i]!.node;
          }
          return 'parts' in node ? { path, after: Math.floor(next() * (node.parts.length - 1)) } : undefined;
        };
        switch (gesture) {
          case 0: case 1: layout = add(layout, `t${made++}`, ids.length > 0 && next() < 0.7 ? pick(ids) : undefined); break;
          case 2: layout = remove(layout, pick(ids)); break;
          case 3: layout = drop(layout, pick(ids), { onto: pick(ids), edge: pick(['left', 'right', 'top', 'bottom'] as const) }); break;
          case 4: layout = drop(layout, pick(ids), { swap: pick(ids) }); break;
          case 5: layout = drop(layout, pick(ids), { band: Math.floor(next() * (layout.bands.length + 1)) }); break;
          case 6: layout = resize(layout, [Math.floor(next() * layout.bands.length)], 0, next() - 0.5); break;
          case 7: layout = resizeBand(layout, Math.floor(next() * layout.bands.length), next() * 1.5); break;
          case 8: layout = arrange(layout, pick(layoutsFor(ids.length)).id); break;
          case 9: {
            const at = somewhere();
            if (at) layout = resize(layout, at.path, at.after, snapped(layout, at.path, at.after, next() - 0.5, next() * 0.05));
            break;
          }
          case 10: {
            const at = somewhere();
            if (at) layout = evenOut(layout, at.path);
            break;
          }
          case 11: layout = evenAll(layout); break;
          case 12: layout = tradeBands(layout, Math.floor(next() * layout.bands.length), next() - 0.5); break;
          case 13: layout = fitted(layout, next() < 0.5); break;
          // stacks: a tile dropped on another's middle, a tab brought to the front, a tab moved among its stack's
          case 14: layout = drop(layout, pick(ids), { stack: pick(ids) }); break;
          case 15: layout = bringToFront(layout, pick(ids)); break;
          case 16: layout = reorderStack(layout, pick(ids), Math.floor(next() * 4)); break;
        }
        assert.deepEqual(problems(layout), [], `seed ${seed}, step ${step}, gesture ${gesture}`);
        // a gesture moves tiles, never loses or makes one (but add and remove)
        if (gesture >= 3) assert.deepEqual([...tiles(layout)].sort(), [...ids].sort());
        // now and then: the page in whole cells covers it exactly, and reads back as bands with every tile
        if (step % 50 === 0) {
          exactCover(layout, `seed ${seed}, step ${step}`);
          const back = fromCells(cells(layout).tiles, 24);
          assert.deepEqual(problems(back), [], `seed ${seed}, step ${step}: read back`);
          assert.deepEqual([...tiles(back)].sort(), [...places(layout)].sort());
          // as a page saves it: the same tiles, every rule kept, each stack on its first tab
          const saved = asSaved(layout);
          assert.deepEqual(problems(saved), []);
          assert.deepEqual(tiles(saved), tiles(layout));
        }
      }
    });
  }
});

describe('a stack: tiles in one place, one shown', () => {
  const three = (): Bands => drop(add(add(EMPTY, 'a'), 'b', 'a'), 'c', { band: 1 });
  const base = (): Bands => add(three(), 'c', 'b');
  it('is made by a drop on a tile\'s middle, in that tile\'s place, the dropped tile in front', () => {
    const layout = sound(drop(add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'b'), 'c', { stack: 'a' }));
    assert.deepEqual(picture(layout), ['[{a,c*}:50 | b:50]']);
    assert.deepEqual(stackOf(layout, 'a'), ['a', 'c']);
    assert.deepEqual(places(layout), ['c', 'b'], 'one place, its tile in front naming it');
    // a third onto it joins it
    const more = sound(drop(layout, 'b', { stack: 'c' }));
    assert.deepEqual(picture(more), ['{a,c,b*}']);
    // onto itself, or onto its own stack's tile in front: nothing changes
    assert.equal(drop(layout, 'c', { stack: 'c' }), layout);
  });
  it('gives up a tile as a removal does: the rest stay stacked, one left is a tile again, a front gone shows the next tab', () => {
    const stack = drop(drop(base(), 'b', { stack: 'a' }), 'c', { stack: 'a' });
    assert.deepEqual(picture(stack), ['{a,b,c*}']);
    assert.deepEqual(picture(sound(remove(stack, 'c'))), ['{a,b*}']);
    assert.deepEqual(picture(sound(remove(remove(stack, 'c'), 'a'))), ['b']);
    // dragged out of it to an edge, between bands
    assert.deepEqual(picture(sound(drop(stack, 'a', { band: 1 }))), ['{b,c*}', 'a']);
  });
  it('moves as one place: an edge of a stacked tile divides the stack\'s place, a swap moves the whole stack', () => {
    const layout = drop(add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'b'), 'c', { stack: 'a' });
    assert.deepEqual(picture(sound(drop(layout, 'b', { onto: 'a', edge: 'bottom' }))), ['({a,c*}:50 / b:50)']);
    assert.deepEqual(picture(sound(drop(layout, 'b', { swap: 'a' }))), ['[b:50 | {a,c*}:50]']);
    assert.equal(drop(layout, 'a', { swap: 'c' }), layout, 'two tiles of one stack: one place');
  });
  it('brings a tab to the front, and moves a tab among its stack\'s, saved without its front (it reopens on its first tab)', () => {
    const layout = drop(drop(base(), 'b', { stack: 'a' }), 'c', { stack: 'a' });
    const front = sound(bringToFront(layout, 'a'));
    assert.deepEqual(picture(front), ['{a*,b,c}']);
    assert.equal(bringToFront(front, 'a'), front);
    assert.deepEqual(picture(sound(reorderStack(front, 'c', 0))), ['{c,a*,b}']);
    assert.deepEqual(asSaved(front), { fit: front.fit, bands: [{ height: front.bands[0]!.height, node: { stack: ['a', 'b', 'c'] } }] });
  });
  it('is one place to Arrange, and stays a stack', () => {
    const layout = drop(add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'b'), 'c', { stack: 'a' });
    assert.deepEqual(picture(sound(arrange(layout, 'stacked'))), ['{a,c*}', 'b']);
    assert.deepEqual(picture(sound(arrange(layout, 'side-by-side', ['b', 'a']))), ['[b:50 | {a,c*}:50]']);
    assert.equal(layoutsFor(places(layout).length).length > 0, true);
  });
  it('is drawn as its tile in front, in the place\'s box; exported as that tile; refused when it breaks a rule', () => {
    const layout = drop(add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'b'), 'c', { stack: 'a' });
    const drawn = draw(layout, 1000, 800, 8);
    assert.deepEqual([...drawn.tiles.keys()].sort(), ['b', 'c']);
    assert.deepEqual(drawn.stacks.get('c'), ['a', 'c']);
    assert.deepEqual(cells(layout).tiles.map((t) => t.id).sort(), ['b', 'c']);
    assert.deepEqual(problems({ fit: false, bands: [{ height: 1, node: { stack: ['a'] } }] }), ['band 0: a stack of 1 tile']);
    assert.deepEqual(problems({ fit: false, bands: [{ height: 1, node: { stack: ['a', 'b'], front: 'z' } }] }),
      ['band 0: a stack whose front, z, is not in it']);
  });
});

describe('drawn in pixels', () => {
  it('bands one under another, parts sharing their split, gaps between, edges meeting', () => {
    const layout = drop(add(add(EMPTY, 'big'), 'top'), 'top', { onto: 'big', edge: 'right' });
    const drawn = draw(drop(add(layout, 'bottom'), 'bottom', { onto: 'top', edge: 'bottom' }), 1000, 800, 8);
    assert.deepEqual(drawn.tiles.get('big'), { x: 0, y: 0, w: 496, h: 400 });
    assert.deepEqual(drawn.tiles.get('top'), { x: 504, y: 0, w: 496, h: 196 });
    assert.deepEqual(drawn.tiles.get('bottom'), { x: 504, y: 204, w: 496, h: 196 });
    assert.equal(drawn.dividers.length, 2);
    assert.deepEqual(drawn.dividers.find((d) => d.split === 'row')?.box, { x: 496, y: 0, w: 8, h: 400 });
    assert.equal(drawn.height, 400);
  });

  it('a page that fits shares the screen; one that scrolls keeps its heights, never below the least', () => {
    const layout = page('a', 'b', 'c');
    const fit = draw(fitted(layout, true), 600, 616, 8);
    assert.deepEqual(fit.bands.map((b) => b.h), [200, 200, 200]);
    assert.equal(fit.height, 616);
    const scroll = draw(resizeBand(layout, 2, MIN_BAND_HEIGHT), 600, 400, 8, 120);
    assert.deepEqual(scroll.bands.map((b) => b.h), [200, 200, 120]);
  });

  it('three columns of an uneven width still meet: rounded at the edges', () => {
    const layout = add(add(add(EMPTY, 'a'), 'b', 'a'), 'c', 'a');
    const boxes = [...draw(layout, 1001, 600, 8).tiles.values()];
    assert.equal(boxes[0]!.x, 0);
    assert.equal(boxes[1]!.x, boxes[0]!.x + boxes[0]!.w + 8);
    assert.equal(boxes[2]!.x + boxes[2]!.w, 1001);
  });
});

describe('a tile alone in its band, dropped beside its own band', () => {
  it('stays as it was: the same layout, so no step to undo', () => {
    const layout = resizeBand(page('a', 'b'), 0, 0.8);
    assert.equal(drop(layout, 'a', { band: 0 }), layout);
    assert.equal(drop(layout, 'a', { band: 1 }), layout);
    assert.notEqual(drop(layout, 'a', { band: 2 }), layout, 'below the next band: it moves');
  });
});

describe('where a drop lands', () => {
  const layout = drop(page('a', 'b'), 'b', { onto: 'a', edge: 'right' });
  const two = add(layout, 'c');
  const drawn = draw(two, 1000, 800, 8);
  it('a tile\'s edges and middle', () => {
    const a = drawn.tiles.get('a')!;
    assert.deepEqual(dropAt(drawn, 'c', a.x + 5, a.y + a.h / 2), { onto: 'a', edge: 'left' });
    assert.deepEqual(dropAt(drawn, 'c', a.x + a.w - 5, a.y + a.h / 2), { onto: 'a', edge: 'right' });
    // its middle: stacked with it
    assert.deepEqual(dropAt(drawn, 'c', a.x + a.w / 2, a.y + a.h / 2), { stack: 'a' });
  });
  it('between bands, above the first and below the last', () => {
    const first = drawn.bands[0]!;
    assert.deepEqual(dropAt(drawn, 'c', 100, first.y + first.h + 4), { band: 1 });
    assert.deepEqual(dropAt(drawn, 'c', 100, -5), { band: 0 });
    assert.deepEqual(dropAt(drawn, 'a', 100, drawn.height + 30), { band: 2 });
  });
  it('over the dragged tile itself: nowhere', () => {
    const c = drawn.tiles.get('c')!;
    assert.equal(dropAt(drawn, 'c', c.x + c.w / 2, c.y + c.h / 2), undefined);
  });
});

describe('the keyboard\'s neighbours and sides', () => {
  // a large on the left; b over c on the right; d a band below
  const layout = add(arrange(EMPTY, 'focus-left', ['a', 'b', 'c']), 'd');
  const drawn = draw(layout, 900, 600, 8);
  it('the tile next to another, each way, across bands too', () => {
    assert.equal(neighbour(drawn, 'a', 'right'), 'b', 'b and c both beside it: b overlaps it as much, and comes first');
    assert.equal(neighbour(drawn, 'c', 'left'), 'a');
    assert.equal(neighbour(drawn, 'b', 'down'), 'c');
    assert.equal(neighbour(drawn, 'c', 'down'), 'd');
    assert.equal(neighbour(drawn, 'd', 'up'), 'a', 'a overlaps d most');
    assert.equal(neighbour(drawn, 'a', 'left'), undefined);
    assert.equal(neighbour(drawn, 'b', 'up'), undefined);
  });
  it('the divider along a tile\'s side, if one bounds it there', () => {
    assert.deepEqual(dividerBeside(drawn, 'a', 'right')?.path, [0]);
    assert.deepEqual(dividerBeside(drawn, 'b', 'left')?.path, [0], 'the same divider, seen from the other side');
    assert.deepEqual(dividerBeside(drawn, 'b', 'bottom')?.path, [0, 1]);
    assert.deepEqual(dividerBeside(drawn, 'c', 'top')?.path, [0, 1]);
    assert.equal(dividerBeside(drawn, 'a', 'left'), undefined);
    assert.equal(dividerBeside(drawn, 'a', 'bottom'), undefined, 'a band\'s edge is not a divider');
    assert.equal(dividerBeside(drawn, 'd', 'top'), undefined);
  });
});

describe('two bands trading height (a page that fits its window)', () => {
  const layout = fitted(page('a', 'b', 'c'), true);
  it('the edge moved: the band above grows by what the one below gives, the rest untouched', () => {
    const next = tradeBands(layout, 0, 0.1);
    assert.deepEqual(next.bands.map((b) => Math.round(b.height * 100) / 100), [0.6, 0.4, 0.5]);
    assert.deepEqual(problems(next), []);
  });
  it('neither below its least, and the last band has no edge below it to move', () => {
    const near = (actual: number, expected: number) => assert.ok(Math.abs(actual - expected) < 1e-9, `${actual} ~ ${expected}`);
    near(tradeBands(layout, 0, 5).bands[1]!.height, MIN_BAND_HEIGHT);
    near(tradeBands(layout, 0, -5).bands[0]!.height, MIN_BAND_HEIGHT);
    assert.equal(tradeBands(layout, 2, 0.1), layout);
  });
});

describe('evening out the whole page', () => {
  it('every split\'s parts alike, at every depth; every band as tall, the page as tall as it was', () => {
    let layout = add(arrange(EMPTY, 'focus-left', ['a', 'b', 'c']), 'd');
    layout = resize(resize(layout, [0], 0, 0.1), [0, 1], 0, 0.2);
    layout = resizeBand(layout, 1, 0.9);
    const even = evenAll(layout);
    assert.deepEqual(problems(even), []);
    const top = even.bands[0]!.node as Extract<Node, { split: unknown }>;
    assert.deepEqual(top.parts.map((p) => p.size), [0.5, 0.5]);
    const column = top.parts[1]!.node as Extract<Node, { split: unknown }>;
    assert.deepEqual(column.parts.map((p) => p.size), [0.5, 0.5]);
    assert.equal(even.bands[0]!.height, even.bands[1]!.height);
    assert.ok(Math.abs(even.bands[0]!.height + even.bands[1]!.height - (1 + 0.9)) < 1e-9);
    assert.deepEqual(tiles(even), tiles(layout));
  });
});

describe('the page in whole cells (an export)', () => {
  it('shares each split\'s cells whole, by its shares, edges meeting', () => {
    const grid = cells(arrange(EMPTY, 'focus-left', ['a', 'b', 'c']));
    assert.deepEqual(grid, { cols: 24, rows: 24, tiles: [
      { id: 'a', x: 0, y: 0, w: 12, h: 24 },
      { id: 'b', x: 12, y: 0, w: 12, h: 12 },
      { id: 'c', x: 12, y: 12, w: 12, h: 12 },
    ] });
  });
  it('a page that fits its window shares one screenful; one that scrolls keeps its heights', () => {
    const layout = page('a', 'b', 'c');
    assert.deepEqual(cells(fitted(layout, true)).tiles.map((t) => t.h), [8, 8, 8]);
    assert.deepEqual(cells(layout).tiles.map((t) => t.h), [12, 12, 12]);
  });
  it('more tiles side by side than cells: the page grows wider, every tile a cell at least', () => {
    const many = Array.from({ length: 30 }, (_, i) => `t${i}`);
    const grid = cells(arrange(EMPTY, 'side-by-side', many));
    assert.equal(grid.cols, 30);
    assert.ok(grid.tiles.every((t) => t.w === 1));
    exactCover(arrange(EMPTY, 'side-by-side', many), 'thirty side by side');
  });
  it('a tile far smaller than a cell still gets one, its neighbours giving it room', () => {
    let layout = arrange(EMPTY, 'side-by-side', ['a', 'b']);
    layout = drop(add(layout, 'c'), 'c', { onto: 'b', edge: 'right' });
    for (let i = 0; i < 20; i += 1) layout = resize(layout, [0], 1, 0.5);
    exactCover(layout, 'squeezed');
  });
});

describe('a page saved before bands, read as bands', () => {
  it('the grid on top and charts in a row below: two bands', () => {
    const layout = fromCells([
      { id: 'grid', x: 0, y: 0, w: 12, h: 14 },
      { id: 'chart-1', x: 0, y: 14, w: 6, h: 10 },
      { id: 'chart-2', x: 6, y: 14, w: 6, h: 10 },
    ], 24);
    assert.deepEqual(layout, { fit: false, bands: [
      { height: 14 / 24, node: { tile: 'grid' } },
      { height: 10 / 24, node: { split: 'row', parts: [{ node: { tile: 'chart-1' }, size: 0.5 }, { node: { tile: 'chart-2' }, size: 0.5 }] } },
    ] });
  });
  it('one big on the left and two stacked on the right: one band, its right column divided', () => {
    const layout = fromCells([
      { id: 'grid', x: 0, y: 0, w: 7, h: 16 },
      { id: 'top', x: 7, y: 0, w: 5, h: 8 },
      { id: 'bottom', x: 7, y: 8, w: 5, h: 8 },
    ], 24);
    assert.equal(layout.bands.length, 1);
    assert.deepEqual(layout.bands[0]!.node, { split: 'row', parts: [
      { node: { tile: 'grid' }, size: 7 / 12 },
      { node: { split: 'column', parts: [{ node: { tile: 'top' }, size: 0.5 }, { node: { tile: 'bottom' }, size: 0.5 }] }, size: 5 / 12 },
    ] });
  });
  it('four round a middle (no straight cut divides them): side by side, every tile kept', () => {
    const layout = fromCells([
      { id: 'n', x: 0, y: 0, w: 8, h: 4 },
      { id: 'e', x: 8, y: 0, w: 4, h: 8 },
      { id: 's', x: 4, y: 8, w: 8, h: 4 },
      { id: 'w', x: 0, y: 4, w: 4, h: 8 },
      { id: 'm', x: 4, y: 4, w: 4, h: 4 },
    ], 24);
    assert.deepEqual(problems(layout), []);
    assert.deepEqual(tiles(layout), ['n', 'w', 'm', 's', 'e']);
  });
  it('every preset read back from its own cells keeps its shape', () => {
    for (const { id } of layoutsFor(5)) {
      const layout = arrange(EMPTY, id, ['a', 'b', 'c', 'd', 'e']);
      const back = fromCells(cells(layout).tiles, 24);
      assert.deepEqual(tiles(back), tiles(layout), id);
      assert.equal(back.bands.length, layout.bands.length, id);
    }
  });
});

describe('a divider snapping', () => {
  const layout = arrange(EMPTY, 'focus-left', ['a', 'b', 'c']);
  it('knows where each divider is: the share of its split before it', () => {
    assert.equal(boundary(layout, [0], 0), 0.5);
    assert.equal(boundary(layout, [0, 1], 0), 0.5);
    assert.equal(boundary(layout, [0], 1), undefined);
    assert.equal(boundary(layout, [3], 0), undefined);
  });
  it('lands on a quarter, a third or the half within reach, and moves freely elsewhere', () => {
    assert.ok(Math.abs(0.5 + snapped(layout, [0], 0, 0.16, 0.01) - 2 / 3) < 1e-12, 'onto two thirds');
    assert.equal(snapped(layout, [0], 0, 0.03, 0.01), 0.03, 'nothing near');
    assert.ok(Math.abs(0.5 + snapped(layout, [0], 0, -0.17, 0.01) - 1 / 3) < 1e-12, 'onto a third');
    assert.equal(snapped(layout, [2], 0, 0.1, 0.01), 0.1, 'no such divider: as it is');
  });
});

describe('sizes as a person reads them (the readout while a divider is dragged)', () => {
  it('a quarter, a third, the half and their kin by their sign; else a whole percent', () => {
    assert.deepEqual([1 / 4, 1 / 3, 0.5, 2 / 3, 0.75, 0.62, 0.381].map(shareText), ['\u00bc', '\u2153', '\u00bd', '\u2154', '\u00be', '62%', '38%']);
  });
  it('the two parts beside a divider, each its share of the split', () => {
    const layout = resize(arrange(EMPTY, 'side-by-side', ['a', 'b', 'c']), [0], 0, 0.1);
    const [left, right] = sharesBeside(layout, [0], 0)!;
    assert.ok(Math.abs(left - (1 / 3 + 0.1)) < 1e-12 && Math.abs(right - (1 / 3 - 0.1)) < 1e-12);
    assert.equal(sharesBeside(layout, [0], 2), undefined);
  });
});
