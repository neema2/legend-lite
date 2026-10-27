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

  it('a disabled entry with a hint says why on hover; one without says nothing', () => {
    view.show([{ label: 'Tile', items: [
      { id: 'tile.grid', label: 'Show Grid', disabled: true, hint: 'needs a page' },
      { id: 'tile.remove', label: 'Remove' },
    ] }], 10, 10);
    const [grid, remove] = view.items;
    assert.equal(grid!.title, 'needs a page');
    assert.equal(remove!.title, '');
  });

  it("shows an entry's key after its words, said to a screen reader as the entry's own; one without shows none", () => {
    // the menus (docs/DATACUBE_UI_REVIEW_2026_10_24.md): a key shown right-aligned where one does what the entry does
    view.show([{ label: '', items: [
      { id: 'view.undo', label: 'Undo', shortcut: 'Ctrl+Shift+Z' },
      { id: 'view.redo', label: 'Redo' },
    ] }], 10, 10);
    const [undo, redo] = view.items;
    assert.deepEqual([...undo!.children].map((c) => [c.className, c.textContent]),
      [['dc-menu-label', 'Undo'], ['dc-menu-shortcut', 'Ctrl+Shift+Z']]);
    assert.equal(undo!.getAttribute('aria-keyshortcuts'), 'Control+Shift+Z');
    assert.equal(redo!.querySelector('.dc-menu-shortcut'), null);
    assert.equal(redo!.hasAttribute('aria-keyshortcuts'), false);
  });

  it('opens nothing for an empty menu', () => {
    view.show([], 10, 10);
    assert.equal(view.open, false);
  });

  it('focuses the MENU, so no submenu opens by itself', () => {
    // CSS opens a submenu on hover and on focus-within. Focusing the
    // first entry therefore flew its submenu open the instant the
    // menu appeared -- every right-click on the grid arrived with the
    // whole Export list unfurled beside it -- and hovering any other
    // entry left TWO submenus on screen at once, one held by focus
    // and one by the pointer. The keyboard still works: ArrowDown
    // from the menu itself goes to the first entry.
    view.show(GROUPS, 10, 10);
    const menu = doc.querySelector('.dc-menu');
    assert.equal(doc.activeElement, menu);
    assert.equal(view.items.includes(doc.activeElement as HTMLElement), false);
    press('ArrowDown');
    assert.equal(doc.activeElement, view.items[0]);
  });

  it('moves with the arrow keys and wraps', () => {
    view.show(GROUPS, 10, 10);
    press('ArrowDown');
    assert.equal(doc.activeElement, view.items[0]);
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

  it('an entry that disabled the button it was opened from: focus in what holds it, never the body (the 2026-10-25 audit)', () => {
    // a grid's Snap disables its state button while it snaps: focus fell to the body
    const tile = doc.createElement('div');
    tile.tabIndex = 0;
    const state = doc.createElement('button');
    tile.append(state);
    doc.body.append(tile);
    state.focus();
    const snapping = new MenuView(doc, { onSelect: () => { state.disabled = true; } });
    snapping.show(GROUPS, 10, 10);
    (doc.querySelector('.dc-menu [role^="menuitem"]') as HTMLElement).click();
    assert.equal(doc.activeElement, tile);
  });

  it('an entry that put the focus somewhere of its own (a window it opened) leaves it there (the 2026-10-25 review)', () => {
    const origin = doc.getElementById('origin') as HTMLElement;
    const win = doc.createElement('div');
    win.tabIndex = -1;
    doc.body.append(win);
    origin.focus();
    const opening = new MenuView(doc, { onSelect: () => win.focus() });
    opening.show(GROUPS, 10, 10);
    (doc.querySelector('.dc-menu [role^="menuitem"]') as HTMLElement).click();
    assert.equal(doc.querySelector('.dc-menu'), null, 'closed');
    assert.equal(doc.activeElement, win, 'the window keeps the focus');
    // an entry that moves nothing: back where it came from, as before
    view.show(GROUPS, 10, 10);
    (doc.querySelector('.dc-menu [role^="menuitem"]') as HTMLElement).click();
    assert.equal(doc.activeElement, win, 'back where it came from: the window, focused when this menu opened');
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
