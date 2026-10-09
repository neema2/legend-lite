// A page's layout as bands (src/layout/bands.ts), as tile-layout.test.ts tests the grid: the arithmetic, in node, with
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
  PRESETS,
  add,
  arrange,
  bandOf,
  draw,
  drop,
  dividerBeside,
  dropAt,
  evenAll,
  evenOut,
  fitted,
  neighbour,
  problems,
  remove,
  resize,
  resizeBand,
  stacked,
  tiles,
  tradeBands,
} from '../src/layout/bands.ts';

/** A layout as a picture: bands on lines, `|` between columns, `/` between stacked parts, shares rounded. */
function picture(layout: Bands): string[] {
  const draw = (node: Node): string => ('tile' in node ? node.tile
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
  const cases: [string, string[], string[]][] = [
    ['side-by-side', ['a', 'b'], ['[a:50 | b:50]']],
    ['stacked', ['a', 'b'], ['a', 'b']],
    ['grid-2', ['a', 'b', 'c', 'd'], ['[a:50 | b:50]', '[c:50 | d:50]']],
    ['grid-2', ['a', 'b', 'c'], ['[a:50 | b:50]', 'c']],
    ['grid-3', ['a', 'b', 'c', 'd'], ['[a:33 | b:33 | c:33]', 'd']],
    ['top-and-row', ['a', 'b', 'c'], ['a', '[b:50 | c:50]']],
    ['left-and-column', ['a', 'b', 'c'], ['[a:60 | (b:50 / c:50):40]']],
    ['right-and-column', ['a', 'b', 'c'], ['[(b:50 / c:50):40 | a:60]']],
    ['large-and-two', five, ['[a:67 | (b:50 / c:50):33]', '[d:50 | e:50]']],
  ];
  for (const [preset, order, want] of cases) {
    it(`${preset} of ${order.length}`, () => {
      assert.deepEqual(picture(sound(arrange(EMPTY, preset as never, order))), want);
    });
  }

  it('every preset, every count from 1 to 10, keeps every rule and every tile, the first in its first slot', () => {
    for (const { id } of PRESETS) {
      for (let n = 1; n <= 10; n += 1) {
        const order = Array.from({ length: n }, (_, i) => `t${i}`);
        const layout = sound(arrange(EMPTY, id, order));
        // in reading order -- but for one on the right, whose main tile reads after the column beside it
        if (id === 'right-and-column') assert.deepEqual([...tiles(layout)].sort(), [...order].sort(), `${id} of ${n}`);
        else assert.deepEqual(tiles(layout), order, `${id} of ${n}`);
      }
    }
  });
});

describe('a narrow window stacks the page', () => {
  it('every tile a band of its own, in reading order; the layout itself unchanged', () => {
    const layout = arrange(EMPTY, 'large-and-two', ['a', 'b', 'c', 'd']);
    const narrow = sound(stacked(layout));
    assert.deepEqual(picture(narrow), ['a', 'b', 'c', 'd']);
    assert.equal(narrow.fit, false);
    assert.equal(bandOf(layout, 'd'), 1);
  });
});

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
        const gesture = ids.length === 0 ? 0 : Math.floor(next() * 9);
        switch (gesture) {
          case 0: case 1: layout = add(layout, `t${made++}`, ids.length > 0 && next() < 0.7 ? pick(ids) : undefined); break;
          case 2: layout = remove(layout, pick(ids)); break;
          case 3: layout = drop(layout, pick(ids), { onto: pick(ids), edge: pick(['left', 'right', 'top', 'bottom'] as const) }); break;
          case 4: layout = drop(layout, pick(ids), { swap: pick(ids) }); break;
          case 5: layout = drop(layout, pick(ids), { band: Math.floor(next() * (layout.bands.length + 1)) }); break;
          case 6: layout = resize(layout, [Math.floor(next() * layout.bands.length)], 0, next() - 0.5); break;
          case 7: layout = resizeBand(layout, Math.floor(next() * layout.bands.length), next() * 1.5); break;
          case 8: layout = arrange(layout, pick(PRESETS).id); break;
        }
        assert.deepEqual(problems(layout), [], `seed ${seed}, step ${step}, gesture ${gesture}`);
        // a gesture moves tiles, never loses or makes one (but add and remove)
        if (gesture >= 3) assert.deepEqual([...tiles(layout)].sort(), [...ids].sort());
      }
    });
  }
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

describe('where a drop lands', () => {
  const layout = drop(page('a', 'b'), 'b', { onto: 'a', edge: 'right' });
  const two = add(layout, 'c');
  const drawn = draw(two, 1000, 800, 8);
  it('a tile\'s edges and middle', () => {
    const a = drawn.tiles.get('a')!;
    assert.deepEqual(dropAt(drawn, 'c', a.x + 5, a.y + a.h / 2), { onto: 'a', edge: 'left' });
    assert.deepEqual(dropAt(drawn, 'c', a.x + a.w - 5, a.y + a.h / 2), { onto: 'a', edge: 'right' });
    assert.deepEqual(dropAt(drawn, 'c', a.x + a.w / 2, a.y + a.h / 2), { swap: 'a' });
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
  const layout = add(arrange(EMPTY, 'large-and-two', ['a', 'b', 'c']), 'd');
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
    let layout = arrange(EMPTY, 'large-and-two', ['a', 'b', 'c', 'd']);
    layout = resize(resize(layout, [0], 0, 0.1), [0, 1], 0, 0.2);
    layout = resizeBand(layout, 1, 0.9);
    const even = evenAll(layout);
    assert.deepEqual(problems(even), []);
    const top = even.bands[0]!.node as Extract<Node, { split: unknown }>;
    assert.deepEqual(top.parts.map((p) => p.size), [0.5, 0.5]);
    const column = top.parts[1]!.node as Extract<Node, { split: unknown }>;
    assert.deepEqual(column.parts.map((p) => p.size), [0.5, 0.5]);
    assert.equal(even.bands[0]!.height, even.bands[1]!.height);
    assert.ok(Math.abs(even.bands[0]!.height + even.bands[1]!.height - (0.6 + 0.9)) < 1e-9);
    assert.deepEqual(tiles(even), tiles(layout));
  });
});
