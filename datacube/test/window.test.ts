// The dialogs were not dialogs: a block appended after the grid, so
// opening one pushed the page down and showed it BELOW the data it
// was about. These are the arithmetic and the wiring of the floating
// window that replaced it -- DataCube's own figures, from
// DataCubeLayoutService.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  WINDOW_HEIGHT,
  WINDOW_MIN_HEIGHT,
  WINDOW_MIN_WIDTH,
  WINDOW_OFFSET,
  WINDOW_WIDTH,
  fitWindow,
  makeWindow,
  resize,
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

describe('a window in the DOM', () => {
  let dom: JSDOM;
  let container: HTMLElement;
  let el: HTMLElement;
  let head: HTMLElement;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="c"></div></body>');
    container = dom.window.document.getElementById('c') as HTMLElement;
    Object.defineProperty(container, 'getBoundingClientRect', {
      value: () => ({ width: 1000, height: 800, top: 0, left: 0, right: 1000,
        bottom: 800, x: 0, y: 0, toJSON: () => ({}) }),
      configurable: true,
    });
    el = dom.window.document.createElement('div');
    head = dom.window.document.createElement('div');
    el.append(head);
    container.append(el);
  });

  it('positions and sizes the element', () => {
    const spec = makeWindow(el, head, container);
    assert.equal(el.style.left, `${spec.x}px`);
    assert.equal(el.style.top, `${spec.y}px`);
    assert.equal(el.style.width, `${spec.width}px`);
    assert.equal(el.style.height, `${spec.height}px`);
    assert.ok(el.classList.contains('dc-window'));
  });

  it('adds a grip for all EIGHT directions', () => {
    makeWindow(el, head, container);
    const grips = [...el.querySelectorAll('.dc-window-grip')]
      .map((g) => [...g.classList].find((c) => /^dc-window-[nsew]{1,2}$/
        .test(c)));
    assert.deepEqual(grips.sort(), [
      'dc-window-e', 'dc-window-n', 'dc-window-ne', 'dc-window-nw',
      'dc-window-s', 'dc-window-se', 'dc-window-sw', 'dc-window-w',
    ]);
  });

  it('marks the title bar as the handle', () => {
    makeWindow(el, head, container);
    assert.ok(head.classList.contains('dc-window-handle'));
  });

  it('restores a remembered spec instead of centring again', () => {
    // Reopening a dialog should find it where it was left; one that
    // jumps back to the middle is one you move again every time.
    const spec = { x: 11, y: 22, width: 333, height: 444 };
    const out = makeWindow(el, head, container, { spec });
    assert.deepEqual(out, spec);
    assert.equal(el.style.left, '11px');
    assert.equal(el.style.width, '333px');
  });
});
