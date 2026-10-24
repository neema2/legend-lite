// A long press is the right-click on a touch screen (src/ui/long-press.ts; the user, 2026-10-23: the grid's menu button
// went, and an iPad's Safari fires no right-click of its own): a finger held still opens the menu there, once; a
// moved, lifted or cancelled one does not; the browser's own long press (Android) is not doubled; the release's click
// is not let through to the menu entry under the finger.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { HOLD_MS, longPressMenus } from '../src/ui/long-press.ts';

describe('a long press opens the right-click menu', () => {
  let dom: JSDOM;
  let cell: HTMLElement;
  let other: HTMLElement;
  /** Each right-click the page heard: where, and on what. */
  let heard: string[];
  /** Each click that reached the cell or the other element. */
  let clicks: string[];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="cell">AMER</div><div id="other">item</div></body>');
    const doc = dom.window.document;
    cell = doc.getElementById('cell') as HTMLElement;
    other = doc.getElementById('other') as HTMLElement;
    heard = [];
    clicks = [];
    doc.body.addEventListener('contextmenu', (e) => {
      heard.push(`${(e.target as HTMLElement).id}@${e.clientX},${e.clientY}`);
    });
    for (const el of [cell, other]) el.addEventListener('click', () => clicks.push(el.id));
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
});
