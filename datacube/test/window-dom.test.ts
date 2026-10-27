// window.test.ts's screen tests, in jsdom: a window in the DOM. The others are in window.test.ts, which
// runs in the headless lane too (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6).

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import { JSDOM } from 'jsdom';
import {
  makeWindow,
  raiseWindow,
  screenOf,
  type WindowChange,
} from '../src/ui/window.ts';

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

// A chart's options (the user, 2026-10-25): a window kept on the screen -- never half off it, however it is dragged,
// resized, or the browser's window shrinks.
describe('a window kept on the screen (viewport)', () => {
  let dom: JSDOM;
  let el: HTMLElement;
  let head: HTMLElement;
  // jsdom's window: 1024 x 768
  const W = 1024;
  const H = 768;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="c"></div></body>');
    const container = dom.window.document.getElementById('c') as HTMLElement;
    el = dom.window.document.createElement('div');
    head = dom.window.document.createElement('div');
    el.append(head);
    container.append(el);
  });

  const pointer = (on: Element, type: string, x: number, y: number): void => {
    on.dispatchEvent(new dom.window.PointerEvent(type, { pointerId: 1, button: 0, clientX: x, clientY: y, bubbles: true }));
  };
  const placed = (): number[] => ['left', 'top', 'width', 'height'].map((k) => parseFloat(el.style.getPropertyValue(k)));

  it('fixed, at its spec in the screen\'s coordinates, moved onto the screen if asked to start off it', () => {
    makeWindow(el, head, el.parentElement!, { viewport: true, spec: { x: 900, y: 700, width: 280, height: 400 } });
    assert.ok(el.classList.contains('dc-window-fixed'));
    assert.deepEqual(placed(), [W - 280, H - 400, 280, 400]);
  });

  it('dragged, all of it stays on the screen -- not a title bar\'s worth', () => {
    makeWindow(el, head, el.parentElement!, { viewport: true, spec: { x: 100, y: 100, width: 280, height: 400 } });
    pointer(head, 'pointerdown', 110, 110);
    pointer(head, 'pointermove', 2000, 2000);
    pointer(head, 'pointerup', 2000, 2000);
    assert.deepEqual(placed(), [W - 280, H - 400, 280, 400]);
    pointer(head, 'pointerdown', 110, 110);
    pointer(head, 'pointermove', -2000, -2000);
    pointer(head, 'pointerup', -2000, -2000);
    assert.deepEqual(placed().slice(0, 2), [0, 0]);
  });

  it('resized, the dragged edge stops at the screen\'s edge and the opposite one stays put', () => {
    makeWindow(el, head, el.parentElement!, { viewport: true, spec: { x: 600, y: 100, width: 280, height: 400 }, minWidth: 100, minHeight: 100 });
    const east = el.querySelector('.dc-window-e')!;
    pointer(east, 'pointerdown', 880, 300);
    pointer(east, 'pointermove', 5000, 300);
    pointer(east, 'pointerup', 5000, 300);
    assert.deepEqual(placed(), [600, 100, W - 600, 400]);
    const north = el.querySelector('.dc-window-n')!;
    pointer(north, 'pointerdown', 700, 100);
    pointer(north, 'pointermove', 700, -500);
    pointer(north, 'pointerup', 700, -500);
    assert.deepEqual(placed(), [600, 0, W - 600, 500], 'its bottom where it was');
  });

  it('the browser\'s window shrinks: moved back onto it, told; closed (its signal), it listens no more', () => {
    const told: unknown[] = [];
    const closing = new AbortController();
    makeWindow(el, head, el.parentElement!, {
      viewport: true, spec: { x: 700, y: 300, width: 280, height: 400 }, signal: closing.signal, onChange: (spec) => told.push(spec),
    });
    Object.defineProperty(dom.window, 'innerWidth', { value: 800, configurable: true });
    Object.defineProperty(dom.window, 'innerHeight', { value: 600, configurable: true });
    dom.window.dispatchEvent(new dom.window.Event('resize'));
    assert.deepEqual(placed(), [520, 200, 280, 400]);
    assert.deepEqual(told, [{ x: 520, y: 200, width: 280, height: 400 }]);
    closing.abort();
    Object.defineProperty(dom.window, 'innerWidth', { value: 400, configurable: true });
    dom.window.dispatchEvent(new dom.window.Event('resize'));
    assert.equal(told.length, 1, 'closed: no more');
  });

  it('tells what changed it: a drag or a press on its title bar moved, a grip resized (or moved, where no size changed), the browser the screen', () => {
    // a chart's options keep a size only a grip gave (the 2026-10-25 review)
    const told: WindowChange[] = [];
    makeWindow(el, head, el.parentElement!, {
      viewport: true, spec: { x: 100, y: 100, width: 280, height: 400 }, minWidth: 100, minHeight: 100,
      onChange: (_spec, by) => told.push(by),
    });
    pointer(head, 'pointerdown', 110, 110);
    pointer(head, 'pointerup', 110, 110);
    pointer(head, 'pointerdown', 110, 110);
    pointer(head, 'pointermove', 150, 110);
    pointer(head, 'pointerup', 150, 110);
    const east = el.querySelector('.dc-window-e')!;
    pointer(east, 'pointerdown', 420, 300);
    pointer(east, 'pointermove', 460, 300);
    pointer(east, 'pointerup', 460, 300);
    pointer(east, 'pointerdown', 460, 300);
    pointer(east, 'pointerup', 460, 300);
    dom.window.dispatchEvent(new dom.window.Event('resize'));
    assert.deepEqual(told, ['moved', 'moved', 'resized', 'moved', 'screen']);
  });

  it("kept off the page's scrollbars: the screen is the viewport without them where it is laid out", () => {
    // the 2026-10-25 review: innerWidth counts a scrollbar, and a window at the edge sat under it
    const root = dom.window.document.documentElement;
    Object.defineProperty(root, 'clientWidth', { value: W - 15, configurable: true });
    Object.defineProperty(root, 'clientHeight', { value: H - 15, configurable: true });
    assert.deepEqual(screenOf(dom.window as unknown as Window), { width: W - 15, height: H - 15 });
    makeWindow(el, head, el.parentElement!, { viewport: true, spec: { x: 900, y: 700, width: 280, height: 400 } });
    assert.deepEqual(placed(), [W - 15 - 280, H - 15 - 400, 280, 400]);
    // where nothing is laid out, the browser window's inner size
    Object.defineProperty(root, 'clientWidth', { value: 0, configurable: true });
    Object.defineProperty(root, 'clientHeight', { value: 0, configurable: true });
    assert.deepEqual(screenOf(dom.window as unknown as Window), { width: W, height: H });
  });

  it('raised, on top of the windows sharing its host; raised again while on top, left as it is', () => {
    // the 2026-10-25 review: every press in a window raised it again, and after a thousand or so it rose over the menus
    const host = el.parentElement!;
    const other = dom.window.document.createElement('div');
    host.append(other);
    raiseWindow(el, host);
    const first = Number(el.style.zIndex);
    for (let i = 0; i < 50; i++) raiseWindow(el, host);
    assert.equal(Number(el.style.zIndex), first, 'on top already: unchanged');
    raiseWindow(other, host);
    assert.ok(Number(other.style.zIndex) > first, 'the other above it');
    raiseWindow(el, host);
    assert.ok(Number(el.style.zIndex) > Number(other.style.zIndex), 'and it above the other again');
  });

  it('lands where its spec says on the screen inside an ancestor that frames fixed elements (a transform)', () => {
    // the frame's corner at (40, 30) on the screen: what jsdom cannot lay out, said by the element's own box
    Object.defineProperty(el, 'getBoundingClientRect', {
      value: () => {
        const left = parseFloat(el.style.left) + 40;
        const top = parseFloat(el.style.top) + 30;
        return { left, top, right: left, bottom: top, width: 0, height: 0, x: left, y: top, toJSON: () => ({}) };
      },
      configurable: true,
    });
    makeWindow(el, head, el.parentElement!, { viewport: true, spec: { x: 100, y: 100, width: 280, height: 400 } });
    assert.deepEqual(placed().slice(0, 2), [60, 70]);
  });
});
