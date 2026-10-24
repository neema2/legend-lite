// A long press is the right-click on a touch screen (src/ui/long-press.ts; the user, 2026-10-23: the grid's menu button
// went, and an iPad's Safari fires no right-click of its own): a finger held still opens the menu there, once; a
// moved, lifted or cancelled one does not; the browser's own long press (Android) is not doubled; the release's click
// is not let through to the menu entry under the finger.
//
// The user, 2026-10-24, on an iPad: the menu's first submenu opened by itself, and columns could not be dragged or
// resized. From a long press's menu until the next touch the document carries PRESSED_CLASS (the entry left hovered under
// the released finger opens no submenu, grid.css); on what the browser drags itself the menu waits for the release, and a
// drag is no menu; a resize grip is never a menu.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';

import { HOLD_MS, PRESSED_CLASS, longPressMenus } from '../src/ui/long-press.ts';

describe('a long press opens the right-click menu', () => {
  let dom: JSDOM;
  let cell: HTMLElement;
  let other: HTMLElement;
  /** A column's header: what the browser drags itself. */
  let head: HTMLElement;
  /** A column's resize grip. */
  let grip: HTMLElement;
  /** Each right-click the page heard: where, and on what. */
  let heard: string[];
  /** Each click that reached the cell or the other element. */
  let clicks: string[];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="cell">AMER</div><div id="other">item</div>'
      + '<div id="head" draggable="true"><span id="label">Desk</span><div id="grip" class="dc-col-resize"></div></div></body>');
    const doc = dom.window.document;
    cell = doc.getElementById('cell') as HTMLElement;
    other = doc.getElementById('other') as HTMLElement;
    head = doc.getElementById('head') as HTMLElement;
    grip = doc.getElementById('grip') as HTMLElement;
    heard = [];
    clicks = [];
    doc.body.addEventListener('contextmenu', (e) => {
      heard.push(`${(e.target as HTMLElement).id}@${e.clientX},${e.clientY}`);
    });
    for (const el of [cell, other, head]) el.addEventListener('click', () => clicks.push(el.id));
    longPressMenus(doc);
  });

  const pointer = (el: Element, type: string, o: { x?: number; y?: number; kind?: string; id?: number; primary?: boolean } = {}): void => {
    el.dispatchEvent(new dom.window.PointerEvent(type, {
      bubbles: true, cancelable: true, clientX: o.x ?? 10, clientY: o.y ?? 20,
      pointerType: o.kind ?? 'touch', pointerId: o.id ?? 1, isPrimary: o.primary ?? true,
    }));
  };
  const click = (el: Element): void => {
    el.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
  };
  /** A drag event of the browser's own (jsdom has no DragEvent: a mouse event of that name, as the listeners read it). */
  const drag = (el: Element, type: string, x = 0, y = 0): void => {
    el.dispatchEvent(new dom.window.MouseEvent(type, { bubbles: true, cancelable: true, clientX: x, clientY: y }));
  };
  const pressed = (): boolean => dom.window.document.documentElement.classList.contains(PRESSED_CLASS);

  it('a finger held still: the right-click on what it pressed, at the finger, once', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown', { x: 30, y: 40 });
    t.mock.timers.tick(HOLD_MS - 1);
    assert.deepEqual(heard, [], 'not before the hold');
    t.mock.timers.tick(1);
    assert.deepEqual(heard, ['cell@30,40']);
    t.mock.timers.tick(HOLD_MS * 3);
    assert.deepEqual(heard, ['cell@30,40'], 'once');
  });

  it('a finger that moves past the slop, lifts or is cancelled first: no menu; a small wobble still is one', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown', { x: 30, y: 40 });
    pointer(cell, 'pointermove', { x: 30, y: 52 });
    t.mock.timers.tick(HOLD_MS);
    pointer(cell, 'pointerdown');
    pointer(cell, 'pointerup');
    t.mock.timers.tick(HOLD_MS);
    pointer(cell, 'pointerdown');
    pointer(cell, 'pointercancel');
    t.mock.timers.tick(HOLD_MS);
    assert.deepEqual(heard, [], 'a scroll, a tap, a cancelled touch');
    pointer(cell, 'pointerdown', { x: 30, y: 40 });
    pointer(cell, 'pointermove', { x: 33, y: 44 });
    t.mock.timers.tick(HOLD_MS);
    assert.deepEqual(heard, ['cell@30,40'], 'a wobble of 5px is still a press');
  });

  it('a mouse, a pen or a second finger: none (they have their own right-click, or are a pinch)', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown', { kind: 'mouse' });
    t.mock.timers.tick(HOLD_MS);
    pointer(cell, 'pointerdown', { kind: 'pen' });
    t.mock.timers.tick(HOLD_MS);
    pointer(cell, 'pointerdown', { id: 1 });
    pointer(cell, 'pointerdown', { id: 2, primary: false });
    t.mock.timers.tick(HOLD_MS);
    assert.deepEqual(heard, []);
  });

  it("the browser's own long press (Android): it opens the menu, this does not as well", (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown');
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: 10, clientY: 20 }));
    t.mock.timers.tick(HOLD_MS);
    assert.deepEqual(heard, ['cell@10,20'], 'the browser\'s, alone');
    // and one of the browser's after this one's, the finger still down, is not let through
    pointer(other, 'pointerdown', { id: 3 });
    t.mock.timers.tick(HOLD_MS);
    const late = new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true });
    other.dispatchEvent(late);
    assert.deepEqual(heard, ['cell@10,20', 'other@10,20']);
    assert.equal(late.defaultPrevented, true);
  });

  it("the release's click is swallowed -- not the menu entry under the finger -- and the next tap is a tap", (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown');
    t.mock.timers.tick(HOLD_MS);
    pointer(other, 'pointerup');
    click(other);
    assert.deepEqual(clicks, [], 'the release lands on what the menu put under the finger: not picked');
    click(other);
    assert.deepEqual(clicks, ['other'], 'one click swallowed, no more');
    // a release with no click of its own (Safari's way), then a tap: the tap is a tap
    pointer(cell, 'pointerdown');
    t.mock.timers.tick(HOLD_MS);
    pointer(cell, 'pointerup');
    pointer(other, 'pointerdown', { id: 4 });
    pointer(other, 'pointerup', { id: 4 });
    click(other);
    assert.deepEqual(clicks, ['other', 'other']);
    // and a plain tap is never swallowed
    pointer(cell, 'pointerdown');
    pointer(cell, 'pointerup');
    click(cell);
    assert.deepEqual(clicks, ['other', 'other', 'cell']);
  });

  it('installed twice on one document: one menu a press, not two', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    longPressMenus(dom.window.document);
    pointer(cell, 'pointerdown');
    t.mock.timers.tick(HOLD_MS);
    assert.deepEqual(heard, ['cell@10,20']);
  });
  it('the menu says it came from a finger until the next touch: the entry left hovered opens no submenu', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    pointer(cell, 'pointerdown');
    assert.equal(pressed(), false);
    t.mock.timers.tick(HOLD_MS);
    assert.equal(pressed(), true, 'the menu open under the finger');
    pointer(cell, 'pointerup');
    assert.equal(pressed(), true, 'the finger lifted, its hover left behind');
    pointer(other, 'pointerdown', { id: 2 });
    assert.equal(pressed(), false, 'the next touch: a hover is a touch again');
    // the browser's own long press (Android) is a finger's menu as well; a mouse's right-click is not
    pointer(cell, 'pointerdown', { id: 3 });
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    assert.equal(pressed(), true);
    pointer(cell, 'pointerdown', { kind: 'mouse', id: 4 });
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    assert.equal(pressed(), false);
  });

  it('on what the browser drags (a header, a chip): the menu as a finger held still lifts; a tap, or a move, is none', (t) => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
    pointer(head.firstElementChild as Element, 'pointerdown', { x: 30, y: 40 });
    t.mock.timers.tick(HOLD_MS * 2);
    assert.deepEqual(heard, [], 'no menu while the finger is down: an iPad lifts it to drag');
    pointer(head, 'pointerup');
    assert.deepEqual(heard, ['label@30,40'], 'held still, let go: the menu, on what was pressed, where');
    assert.equal(pressed(), true);
    click(head);
    assert.deepEqual(clicks, [], "the release's click: not a sort");
    // a tap on the header is a tap
    pointer(head, 'pointerdown', { id: 2 });
    t.mock.timers.tick(HOLD_MS - 100);
    pointer(head, 'pointerup', { id: 2 });
    click(head);
    assert.deepEqual(clicks, ['head']);
    // held, then moved away: a drag of its own (one this cannot see as a drag), no menu
    pointer(head, 'pointerdown', { id: 3, x: 30, y: 40 });
    pointer(head, 'pointermove', { id: 3, x: 90, y: 40 });
    t.mock.timers.tick(HOLD_MS);
    pointer(head, 'pointerup', { id: 3, x: 90, y: 40 });
    assert.deepEqual(heard, ['label@30,40']);
  });

  it("lifted into the browser's drag (an iPad's): dragged away, no menu; let go where it was pressed after the hold, the menu", (t) => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
    // the finger cancelled as the drag lifts, then dragged to another column
    pointer(head, 'pointerdown', { x: 30, y: 40 });
    t.mock.timers.tick(200);
    pointer(head, 'pointercancel', { x: 30, y: 40 });
    drag(head, 'dragstart', 30, 40);
    drag(other, 'dragover', 80, 40);
    t.mock.timers.tick(HOLD_MS);
    drag(head, 'dragend', 80, 40);
    assert.deepEqual(heard, [], 'a drag');
    // lifted and dropped at once, before the hold: a tap that wobbled, no menu
    pointer(head, 'pointerdown', { x: 30, y: 40 });
    t.mock.timers.tick(100);
    pointer(head, 'pointercancel');
    drag(head, 'dragend');
    assert.deepEqual(heard, []);
    // the drag starting before the finger is cancelled, its positions "none" (0,0) or a wobble, let go after the hold
    pointer(head, 'pointerdown', { x: 30, y: 40 });
    t.mock.timers.tick(200);
    drag(head, 'dragstart', 30, 40);
    pointer(head, 'pointercancel');
    drag(head, 'dragover', 0, 0);
    drag(head, 'dragover', 33, 44);
    t.mock.timers.tick(HOLD_MS);
    drag(head, 'dragend', 33, 44);
    assert.deepEqual(heard, ['head@30,40'], 'held where it was pressed: the long press it was');
    click(head);
    assert.deepEqual(clicks, [], 'and its click swallowed');
  });

  it("a column's resize grip: a press there is a resize, never a menu", (t) => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
    pointer(grip, 'pointerdown');
    t.mock.timers.tick(HOLD_MS * 3);
    pointer(grip, 'pointerup');
    assert.deepEqual(heard, []);
    assert.equal(pressed(), false);
  });
  it("grid.css: a long press's menu opens no submenu by hover alone; the grip is a finger wide on a touch screen", () => {
    const css = readFileSync(runfileFromEnv('GRID_CSS'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
    const rule = new RegExp(`:root\\.${PRESSED_CLASS} \\.dc-menu-item\\.dc-has-submenu:hover:not\\(:focus-within\\) > \\.dc-submenu \\{\\s*display: none;`);
    assert.match(css, rule, 'the hover left by the released finger: no submenu (a tap, or the keyboard, still opens one)');
    assert.match(css, /@media \(pointer: coarse\) \{\s*\.dc-col-resize \{ width: 18px;/);
  });
});
