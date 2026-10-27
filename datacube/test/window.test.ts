// The dialogs were not dialogs: a block appended after the grid, so
// opening one pushed the page down and showed it BELOW the data it
// was about. These are the arithmetic and the wiring of the floating
// window that replaced it -- DataCube's own figures, from
// DataCubeLayoutService.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  BESIDE_GAP,
  WINDOW_HEIGHT,
  WINDOW_MIN_HEIGHT,
  WINDOW_MIN_WIDTH,
  WINDOW_OFFSET,
  WINDOW_WIDTH,
  besideWindow,
  fitWindow,
  onScreen,
  resize,
  type ScreenBox,
  type WindowSpec,
} from '../src/ui/window.ts';

describe('fitting a window to its container', () => {
  it('centres it at the default size when there is room', () => {
    const spec = fitWindow({ width: 1400, height: 900 });
    assert.equal(spec.width, WINDOW_WIDTH);
    assert.equal(spec.height, WINDOW_HEIGHT);
    assert.equal(spec.x, (1400 - WINDOW_WIDTH) / 2);
    assert.equal(spec.y, (900 - WINDOW_HEIGHT) / 2);
  });

  it('SHRINKS rather than overflowing a small container', () => {
    // A window wider than the window is not a window. Upstream takes
    // the container less a margin on each side.
    const spec = fitWindow({ width: 600, height: 500 });
    assert.equal(spec.width, 600 - WINDOW_OFFSET * 2);
    assert.equal(spec.height, 500 - WINDOW_OFFSET * 2);
    assert.equal(spec.x, WINDOW_OFFSET);
  });

  it('never shrinks below the minimum, however small the container', () => {
    // A 40px window cannot be grabbed to make it bigger again, so the
    // floor wins over the fit.
    const spec = fitWindow({ width: 200, height: 150 });
    assert.equal(spec.width, WINDOW_MIN_WIDTH);
    assert.equal(spec.height, WINDOW_MIN_HEIGHT);
  });

  it('reads a NEGATIVE coordinate from the far edge', () => {
    // How a window asks to sit bottom-right without being told how
    // big the container is.
    const spec = fitWindow({ width: 1000, height: 800 }, {
      center: false, x: -20, y: -20, width: 400, height: 300,
    });
    assert.equal(spec.x, 1000 - 20 - 400);
    assert.equal(spec.y, 800 - 20 - 300);
  });
});

describe('resizing from an edge', () => {
  const from = { x: 100, y: 100, width: 400, height: 300 };

  it('grows to the east without moving', () => {
    assert.deepEqual(resize(from, 'e', 50, 0),
      { x: 100, y: 100, width: 450, height: 300 });
  });

  it('MOVES as it shrinks from the west', () => {
    // The right edge stays put, so x follows the width it did not
    // get. Getting this wrong makes the window jump sideways.
    assert.deepEqual(resize(from, 'w', 50, 0),
      { x: 150, y: 100, width: 350, height: 300 });
  });

  it('stops the dragged edge at the minimum, not the opposite one', () => {
    // Dragging west past the floor must leave the RIGHT edge where it
    // was: x + width has to stay at 500.
    const out = resize(from, 'w', 1000, 0, 300, 300);
    assert.equal(out.width, 300);
    assert.equal(out.x + out.width, 500);
  });

  it('takes both axes from a corner', () => {
    assert.deepEqual(resize(from, 'se', 25, 40),
      { x: 100, y: 100, width: 425, height: 340 });
  });

  it('moves x and y together from the north-west corner', () => {
    // Explicit minimums: the fixture is 300 tall and the DEFAULT
    // floor is 300, so with the defaults it cannot shrink at all and
    // the test would be measuring the floor rather than the corner.
    const out = resize(from, 'nw', 20, 30, 100, 100);
    assert.deepEqual(out, { x: 120, y: 130, width: 380, height: 270 });
  });

  it('cannot shrink below the floor even from a corner', () => {
    const out = resize(from, 'nw', 1000, 1000);
    assert.equal(out.width, WINDOW_MIN_WIDTH);
    assert.equal(out.height, WINDOW_MIN_HEIGHT);
    // And the far corner stays where it was.
    assert.equal(out.x + out.width, 500);
    assert.equal(out.y + out.height, 400);
  });
});


// A chart's options (the user, 2026-10-25): "make sure the chart dialog opens near the chart but not covering, and not
// half off the screen when open".
describe('a window beside a box: next to it, not over it, wholly on the screen', () => {
  const screen = { width: 1200, height: 800 };
  const wanted = { width: 280, height: 560, minWidth: 220, minHeight: 200 };
  const box = (left: number, top: number, right: number, bottom: number): ScreenBox => ({ left, top, right, bottom });
  const overlaps = (w: WindowSpec, b: ScreenBox): boolean =>
    w.x < b.right && w.x + w.width > b.left && w.y < b.bottom && w.y + w.height > b.top;
  const inside = (w: WindowSpec): boolean =>
    w.x >= BESIDE_GAP && w.y >= BESIDE_GAP && w.x + w.width <= screen.width - BESIDE_GAP
    && w.y + w.height <= screen.height - BESIDE_GAP;

  it('goes to its right where it fits, its top level with the box', () => {
    const chart = box(100, 120, 500, 420);
    const w = besideWindow(chart, screen, wanted);
    assert.deepEqual(w, { x: 508, y: 120, width: 280, height: 560 });
    assert.ok(!overlaps(w, chart) && inside(w));
  });

  it('to its left when the right has no room: a chart at the screen\'s right edge', () => {
    const chart = box(800, 100, 1190, 400);
    const w = besideWindow(chart, screen, wanted);
    assert.equal(w.x + w.width, 800 - BESIDE_GAP, 'its right edge a gap from the chart');
    assert.ok(!overlaps(w, chart) && inside(w));
  });

  it('below, then above, a chart as wide as the screen', () => {
    const top = box(0, 0, 1200, 200);
    const under = besideWindow(top, screen, wanted);
    assert.equal(under.y, 200 + BESIDE_GAP);
    assert.ok(!overlaps(under, top) && inside(under));
    const bottom = box(0, 600, 1200, 800);
    const over = besideWindow(bottom, screen, { ...wanted, height: 400 });
    assert.equal(over.y + over.height, 600 - BESIDE_GAP);
    assert.ok(!overlaps(over, bottom) && inside(over));
  });

  it('made smaller to the side with the most room where it fits nowhere at its size', () => {
    // 250px of room at the left, less at the right: narrower, still not over it
    const chart = box(266, 0, 1000, 800);
    const w = besideWindow(chart, screen, wanted);
    assert.equal(w.width, 250);
    assert.ok(!overlaps(w, chart) && inside(w));
    // the top band of a tall window: shorter, below the chart
    const wide = box(0, 0, 1200, 400);
    const short = besideWindow(wide, screen, wanted);
    assert.equal(short.height, 800 - 400 - 2 * BESIDE_GAP);
    assert.ok(!overlaps(short, wide) && inside(short));
  });

  it('over the box, on the screen, only where no side has its minimum: a chart filling the screen', () => {
    const w = besideWindow(box(0, 0, 1200, 800), screen, wanted);
    assert.ok(inside(w), JSON.stringify(w));
    assert.deepEqual([w.width, w.height], [280, 560]);
  });

  it('on the screen when its box is half off it, and never bigger than the screen', () => {
    // a chart scrolled half above the screen: the window's top on it
    const up = besideWindow(box(100, -300, 500, 200), screen, wanted);
    assert.ok(inside(up), JSON.stringify(up));
    // a chart scrolled wholly above it: below where it was, on the screen
    const gone = besideWindow(box(0, -900, 1200, -100), screen, wanted);
    assert.ok(inside(gone), JSON.stringify(gone));
    // a phone: the window as wide and tall as the screen lets it be
    const phone = besideWindow(box(0, 0, 360, 300), { width: 360, height: 640 }, wanted);
    assert.ok(phone.width <= 360 - 2 * BESIDE_GAP && phone.height <= 640 - 2 * BESIDE_GAP, JSON.stringify(phone));
    assert.ok(phone.x >= BESIDE_GAP && phone.y >= BESIDE_GAP && phone.x + phone.width <= 360 - BESIDE_GAP
      && phone.y + phone.height <= 640 - BESIDE_GAP, JSON.stringify(phone));
  });
});

describe('a window kept on the screen', () => {
  it('moved back onto a smaller screen, and made no bigger than it', () => {
    assert.deepEqual(onScreen({ x: 900, y: 700, width: 280, height: 400 }, { width: 1000, height: 800 }),
      { x: 720, y: 400, width: 280, height: 400 });
    assert.deepEqual(onScreen({ x: -50, y: -20, width: 1400, height: 300 }, { width: 1000, height: 800 }),
      { x: 0, y: 0, width: 1000, height: 300 });
  });
});
