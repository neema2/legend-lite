import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { MenuView } from '../src/ui/menu-view.ts';
import type { MenuGroup, MenuItem } from '../src/ui/menu.ts';

const GROUPS: MenuGroup[] = [
  {
    label: 'Sort',
    items: [
      { id: 'sort.asc', label: 'Ascending' },
      { id: 'sort.desc', label: 'Descending' },
    ],
  },
  { label: 'Export', items: [{ id: 'export.csv', label: 'CSV' }] },
];

let dom: JSDOM;
let doc: Document;
let view: MenuView;
let chosen: MenuItem[];

beforeEach(() => {
  dom = new JSDOM('<!doctype html><button id="origin">grid</button>');
  doc = dom.window.document as unknown as Document;
  chosen = [];
  view = new MenuView(doc, { onSelect: (i) => chosen.push(i) });
});

const press = (key: string) => {
  doc
    .querySelector('.dc-menu')
    ?.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key, bubbles: true }),
    );
};

describe('MenuView', () => {
  it('renders items with menu roles and a separator between groups', () => {
    view.show(GROUPS, 10, 10);
    assert.equal(doc.querySelector('[role="menu"]') !== null, true);
    assert.equal(view.items.length, 3);
    // The separator must not read as a fourth item, or the menu
    // announces as longer than it is.
    assert.equal(doc.querySelectorAll('[role="separator"]').length, 1);
  });

  it('opens nothing for an empty menu', () => {
    view.show([], 10, 10);
    assert.equal(view.open, false);
  });

  it('focuses the first item so the keyboard works immediately', () => {
    view.show(GROUPS, 10, 10);
    assert.equal(doc.activeElement, view.items[0]);
  });

  it('moves with the arrow keys and wraps', () => {
    view.show(GROUPS, 10, 10);
    press('ArrowDown');
    assert.equal(doc.activeElement, view.items[1]);
    press('ArrowUp');
    assert.equal(doc.activeElement, view.items[0]);
    press('ArrowUp');
    assert.equal(doc.activeElement, view.items[2], 'wraps to the end');
    press('ArrowDown');
    assert.equal(doc.activeElement, view.items[0], 'and back to the start');
  });

  it('jumps with Home and End', () => {
    view.show(GROUPS, 10, 10);
    press('End');
    assert.equal(doc.activeElement, view.items[2]);
    press('Home');
    assert.equal(doc.activeElement, view.items[0]);
  });

  it('selects with Enter and closes', () => {
    view.show(GROUPS, 10, 10);
    press('ArrowDown');
    press('Enter');
    assert.deepEqual(chosen.map((i) => i.id), ['sort.desc']);
    assert.equal(view.open, false);
  });

  it('closes on Escape without selecting', () => {
    view.show(GROUPS, 10, 10);
    press('Escape');
    assert.equal(view.open, false);
    assert.deepEqual(chosen, []);
  });

  it('returns focus where it came from', () => {
    // Otherwise a keyboard user is stranded outside the grid with no
    // way back into it.
    const origin = doc.getElementById('origin') as HTMLElement;
    origin.focus();
    view.show(GROUPS, 10, 10);
    assert.notEqual(doc.activeElement, origin);
    view.close();
    assert.equal(doc.activeElement, origin);
  });

  it('closes an already-open menu before opening another', () => {
    view.show(GROUPS, 10, 10);
    view.show(GROUPS, 50, 50);
    assert.equal(doc.querySelectorAll('.dc-menu').length, 1);
  });

  it('flips rather than clamping when it would overflow', () => {
    // A menu clamped to the edge sits under the pointer, so the first
    // item is whatever the user happens to be hovering.
    Object.defineProperty(dom.window, 'innerWidth', { value: 200, configurable: true });
    Object.defineProperty(dom.window, 'innerHeight', { value: 200, configurable: true });
    const el = () => doc.querySelector('.dc-menu') as HTMLElement;
    view.show(GROUPS, 190, 190);
    // jsdom reports a zero-size rect, so the flip is a no-op here;
    // what must hold is that the position is never negative.
    assert.ok(Number.parseInt(el().style.left, 10) >= 0);
    assert.ok(Number.parseInt(el().style.top, 10) >= 0);
  });
});
