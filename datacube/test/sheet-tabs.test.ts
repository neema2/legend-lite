// A page's sheet tabs (src/ui/sheet-tabs.ts; docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §7.1), alone: the sheet shown
// raised and the only tab in the Tab order, the arrow keys moving between sheets, a rename kept on Enter and dropped on
// Escape, a tab's menu offering only what can be done (no Move Left for the first, no Delete for the last sheet), a
// locked page's tabs still switching but changing nothing, and the tabs kept from paint to paint.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { SheetTabs, type SheetTab } from '../src/ui/sheet-tabs.ts';

let dom: JSDOM;
let tabs: SheetTabs;
let asked: string[];

const SHEETS: readonly SheetTab[] = [
  { id: 'sheet-1', label: 'trades.csv' },
  { id: 'sheet-2', label: 'Charts' },
  { id: 'sheet-3', label: 'Sheet 3' },
];

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body></body>');
  asked = [];
  tabs = new SheetTabs(dom.window.document, {
    onShow: (id) => asked.push(`show ${id}`),
    onAdd: () => asked.push('add'),
    onRename: (id, name) => asked.push(`rename ${id} ${name}`),
    onMove: (id, to) => asked.push(`move ${id} ${to}`),
    onRemove: (id) => asked.push(`remove ${id}`),
  });
  dom.window.document.body.append(tabs.element);
  tabs.paint(SHEETS, 'sheet-2', true);
});

const tab = (id: string): HTMLElement => tabs.element.querySelector(`.dc-sheet-tab[data-sheet="${id}"]`) as HTMLElement;
const key = (el: HTMLElement, k: string): void => {
  el.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: k, bubbles: true }));
};
/** A tab's menu, opened: its entries by label, each with whether it is disabled. */
function menuOf(id: string): Map<string, HTMLElement> {
  tab(id).dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  const items = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
  return new Map(items.map((i) => [i.querySelector('.dc-menu-label')?.textContent ?? '', i]));
}
const disabled = (el: HTMLElement | undefined): boolean => el?.getAttribute('aria-disabled') === 'true';

describe('a page\'s sheet tabs', () => {
  it('say the sheets, the shown one raised and alone in the Tab order, in a tablist', () => {
    assert.equal(tabs.element.querySelector('[role="tablist"]')?.getAttribute('aria-label'), 'Sheets');
    assert.deepEqual([...tabs.element.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent), ['trades.csv', 'Charts', 'Sheet 3']);
    assert.equal(tab('sheet-2').getAttribute('aria-selected'), 'true');
    assert.deepEqual(SHEETS.map((s) => tab(s.id).tabIndex), [-1, 0, -1]);
    assert.ok(tab('sheet-2').classList.contains('dc-sheet-tab-shown'));
  });

  it('move between sheets with the arrow keys, Home and End, showing each', () => {
    key(tab('sheet-2'), 'ArrowRight');
    key(tab('sheet-2'), 'ArrowLeft');
    key(tab('sheet-2'), 'End');
    key(tab('sheet-2'), 'Home');
    assert.deepEqual(asked, ['show sheet-3', 'show sheet-1', 'show sheet-3', 'show sheet-1']);
  });

  it('rename in place: Enter keeps the name, Escape leaves it, an emptied name asks to be named again', () => {
    tabs.rename('sheet-2');
    let field = tab('sheet-2').querySelector('input')!;
    field.value = 'Summary';
    key(field, 'Enter');
    tabs.rename('sheet-3');
    field = tab('sheet-3').querySelector('input')!;
    field.value = 'Nope';
    key(field, 'Escape');
    assert.equal(tab('sheet-3').querySelector('.dc-sheet-label')?.textContent, 'Sheet 3', 'its label back as it was');
    tabs.rename('sheet-1');
    field = tab('sheet-1').querySelector('input')!;
    field.value = '  ';
    key(field, 'Enter');
    assert.deepEqual(asked, ['rename sheet-2 Summary', 'rename sheet-1 ']);
  });

  it('carry a × that deletes the sheet, none for the last sheet or on a locked page', () => {
    const close = (id: string): HTMLButtonElement => tab(id).querySelector<HTMLButtonElement>('.dc-sheet-close')!;
    assert.equal(close('sheet-2').hidden, false);
    close('sheet-3').click();
    assert.deepEqual(asked, ['remove sheet-3']);
    tabs.paint([SHEETS[0]!], 'sheet-1', true);
    assert.equal(close('sheet-1').hidden, true, 'the last sheet stays');
    tabs.paint(SHEETS, 'sheet-1', false);
    assert.equal(close('sheet-2').hidden, true, 'a locked page deletes none');
  });

  it('keep a rename\'s field, and its focus, through a repaint (a grid\'s view landing paints the tabs again)', () => {
    tabs.rename('sheet-2');
    const field = tab('sheet-2').querySelector('input')!;
    field.value = 'Q3 r';
    tabs.paint(SHEETS, 'sheet-2', true);
    assert.equal(tab('sheet-2').querySelector('input'), field, 'still being renamed');
    assert.equal(dom.window.document.activeElement, field);
    assert.deepEqual(asked, [], 'nothing renamed yet');
  });

  it('offer in a tab\'s menu only what can be done: no Move Left first, no Move Right last, no Delete of the last sheet', () => {
    let menu = menuOf('sheet-1');
    assert.equal(disabled(menu.get('Move Left')), true);
    assert.equal(disabled(menu.get('Move Right')), false);
    menu.get('Move Right')!.click();
    menu = menuOf('sheet-3');
    assert.equal(disabled(menu.get('Move Right')), true);
    menu.get('Delete')!.click();
    assert.deepEqual(asked, ['move sheet-1 1', 'remove sheet-3']);
    tabs.paint([SHEETS[0]!], 'sheet-1', true);
    assert.equal(disabled(menuOf('sheet-1').get('Delete')), true);
  });

  it('on a locked page still switch sheets, and add, rename, move and delete none', () => {
    tabs.paint(SHEETS, 'sheet-1', false);
    assert.equal(tabs.element.querySelector<HTMLElement>('.dc-sheet-add')!.hidden, true);
    tabs.rename('sheet-1');
    assert.equal(tab('sheet-1').querySelector('input'), null);
    const menu = menuOf('sheet-2');
    assert.ok(['Rename', 'Move Left', 'Move Right', 'Delete'].every((l) => disabled(menu.get(l))));
    key(tab('sheet-1'), 'ArrowRight');
    assert.deepEqual(asked, ['show sheet-2']);
  });

  it('keep each tab from paint to paint (a click that shows a sheet leaves its tab, so a double click renames it)', () => {
    const before = tab('sheet-1');
    tabs.paint([...SHEETS].reverse(), 'sheet-1', true);
    assert.equal(tab('sheet-1'), before);
    assert.deepEqual([...tabs.element.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent), ['Sheet 3', 'Charts', 'trades.csv']);
    tabs.paint(SHEETS.slice(0, 2), 'sheet-1', true);
    assert.equal(tab('sheet-3'), null, 'a sheet gone takes its tab');
    tabs.element.querySelector<HTMLElement>('.dc-sheet-add')!.click();
    assert.deepEqual(asked, ['add']);
  });
});
