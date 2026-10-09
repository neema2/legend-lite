// The layout picker in a DOM (ui/layout-picker.ts): a thumbnail per layout, each the page's own tiles arranged that
// way with the tile it was opened from marked; previewed while pointed at or focused, never just by opening; arranged
// on a click with no flash back; Fit to window and Even out; and closing by Escape, a press outside or its own button.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { LayoutPicker, type LayoutPickerChoices } from '../src/ui/layout-picker.ts';
import { layoutsFor, type Preset } from '../src/layout/bands.ts';

let dom: JSDOM;
let page: HTMLElement;
let anchor: HTMLButtonElement;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body><div id="page"><button id="anchor">⊞</button></div><p id="away">x</p></body>');
  page = dom.window.document.getElementById('page')!;
  anchor = dom.window.document.getElementById('anchor') as HTMLButtonElement;
});

interface Told {
  previews: (Preset | null)[];
  picks: Preset[];
  fits: boolean[];
  evens: number;
}

function choices(extra: Partial<LayoutPickerChoices> = {}): { choices: LayoutPickerChoices; told: Told } {
  const told: Told = { previews: [], picks: [], fits: [], evens: 0 };
  return {
    told,
    choices: {
      tiles: ['a', 'b', 'c'],
      first: 'c',
      fit: false,
      onPreview: (p) => told.previews.push(p),
      onPick: (p) => told.picks.push(p),
      onFit: (f) => told.fits.push(f),
      onEvenOut: () => { told.evens += 1; },
      ...extra,
    },
  };
}

function option(picker: LayoutPicker, preset: Preset): HTMLButtonElement {
  return picker.options.find((o) => o.dataset['preset'] === preset)!;
}

function pointer(el: Element, type: string): void {
  el.dispatchEvent(new dom.window.PointerEvent(type, { bubbles: type === 'pointerdown' }));
}

describe('the layout picker', () => {
  it('shows the standard shapes first, as the page\'s own tiles, the one it was opened from marked, in the page', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    anchor.focus();
    picker.show(anchor, c);
    assert.ok(page.querySelector('.dc-layout-picker'), 'in the page, not the document\'s body');
    assert.deepEqual(picker.options.map((o) => o.dataset['preset']), layoutsFor(3).filter((l) => l.featured).map((l) => l.id));
    // three tiles: one below is the grid's shape, and two columns one on the right's -- each shown once
    assert.deepEqual(picker.options.map((o) => o.dataset['preset']),
      ['side-by-side', 'stacked', 'rows:2-1', 'focus-left', 'focus-right', 'focus-top']);
    assert.equal(option(picker, 'rows:2-1').getAttribute('aria-label'), 'Grid (2, 1)');
    for (const o of picker.options) assert.equal(o.querySelectorAll('.dc-layout-cell').length, 3);
    // one large on the left, the rest beside it: c, the tile it was opened from, is the large one
    const cells = [...option(picker, 'focus-left').querySelectorAll<HTMLElement>('.dc-layout-cell')];
    const marked = cells.filter((cell) => cell.classList.contains('dc-layout-cell-mark'));
    assert.equal(marked.length, 1);
    assert.equal(marked[0]!.style.left, '0px');
    assert.equal(marked[0]!.style.height, '44px');
    assert.equal(option(picker, 'focus-left').getAttribute('aria-label'), 'One on the left, the rest beside it');
    assert.deepEqual(told.previews, [], 'opening previews nothing');
  });

  it('More layouts opens every other shape under its kind\'s heading, and closes it again', () => {
    const picker = new LayoutPicker(page);
    const { choices: c } = choices({ tiles: ['a', 'b', 'c', 'd', 'e'], first: 'a' });
    picker.show(anchor, c);
    const more = [...page.querySelectorAll<HTMLButtonElement>('.dc-layout-toggle')].find((b) => b.textContent === 'More layouts')!;
    assert.equal(more.getAttribute('aria-expanded'), 'false');
    const standard = picker.options.length;
    more.click();
    assert.equal(more.getAttribute('aria-expanded'), 'true');
    assert.deepEqual(picker.options.map((o) => o.dataset['preset']), layoutsFor(5).map((l) => l.id), 'every layout for five');
    assert.ok(option(picker, 'rows:3-1-1') && option(picker, 'rows:1-3-1'), 'several on top then one and one; one, several, one');
    assert.deepEqual([...page.querySelectorAll('.dc-layout-more .dc-layout-group')].map((h) => h.textContent), ['Rows', 'Columns']);
    // open, the picker is wider: six to a row, the keyboard moving by them
    const el = page.querySelector<HTMLElement>('.dc-layout-picker')!;
    assert.equal(el.classList.contains('dc-layout-picker-wide'), true);
    picker.options[0]!.focus();
    picker.options[0]!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
    assert.equal(dom.window.document.activeElement, picker.options[6], 'down by six');
    more.click();
    assert.equal(picker.options.length, standard);
    assert.equal(el.classList.contains('dc-layout-picker-wide'), false);
  });

  it('Custom builds rows by hand: each row\'s count up and down, rows added and taken away, previewed, then applied', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    [...page.querySelectorAll<HTMLButtonElement>('.dc-layout-toggle')].find((b) => b.textContent === 'Custom\u2026')!.click();
    const counts = (): string[] => [...page.querySelectorAll('.dc-layout-custom-count')].map((e) => e.textContent ?? '');
    const status = (): string => page.querySelector('.dc-layout-custom-status')!.textContent ?? '';
    const step = (label: string): HTMLButtonElement => page.querySelector<HTMLButtonElement>(`.dc-layout-custom [aria-label="${label}"]`)!;
    const apply = page.querySelector<HTMLButtonElement>('.dc-layout-apply')!;
    assert.deepEqual(counts(), ['2', '1'], 'it starts as the grid');
    assert.equal(status(), '3 tiles in 2 rows');
    assert.deepEqual(told.previews, ['rows:2-1'], 'shown on the page');
    assert.equal(step('One more tile in row 2').disabled, true, 'every tile placed: no more');
    step('One fewer tile in row 1').click();
    assert.deepEqual(counts(), ['1', '1']);
    assert.equal(status(), '2 of 3 tiles placed: 1 more to place');
    assert.equal(apply.disabled, true);
    step('Add a row of one tile').click();
    assert.deepEqual(counts(), ['1', '1', '1']);
    assert.equal(told.previews.at(-1), 'rows:1-1-1');
    step('Take row 3 away').click();
    step('One more tile in row 2').click();
    assert.deepEqual(counts(), ['1', '2']);
    assert.equal(apply.disabled, false);
    apply.click();
    assert.deepEqual(told.picks, ['rows:1-2']);
    assert.equal(picker.open, false);
  });

  it('previews a layout while it is pointed at, and the page as it is when the pointer leaves', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    pointer(option(picker, 'rows:2-1'), 'pointerenter');
    pointer(option(picker, 'focus-top'), 'pointerenter');
    pointer(page.querySelector('.dc-layout-lists')!, 'pointerleave');
    assert.deepEqual(told.previews, ['rows:2-1', 'focus-top', null]);
  });

  it('arranges on a click and closes, with no flash back to the old layout between', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    anchor.focus();
    picker.show(anchor, c);
    pointer(option(picker, 'stacked'), 'pointerenter');
    option(picker, 'stacked').click();
    assert.deepEqual(told.picks, ['stacked']);
    assert.deepEqual(told.previews, ['stacked'], 'not put back before the arrangement lands');
    assert.equal(picker.open, false);
    assert.equal(page.querySelector('.dc-layout-picker'), null);
    assert.equal(dom.window.document.activeElement, anchor, 'focus back where it was');
  });

  it('works from the keyboard: an arrow reaches the thumbnails, each previewed as it is reached', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    const el = page.querySelector<HTMLElement>('.dc-layout-picker')!;
    const key = (k: string) => dom.window.document.activeElement!.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: k, bubbles: true }));
    assert.equal(dom.window.document.activeElement, el);
    key('ArrowRight');
    assert.equal(dom.window.document.activeElement, option(picker, 'side-by-side'));
    key('ArrowRight');
    key('ArrowDown');
    assert.equal(dom.window.document.activeElement, option(picker, 'focus-top'), 'down: the next row as drawn, the same place in it');
    key('ArrowDown');
    assert.equal(dom.window.document.activeElement, option(picker, 'focus-top'), 'no row below: it stays');
    key('ArrowUp');
    assert.equal(dom.window.document.activeElement, option(picker, 'stacked'));
    key('Home');
    key('End');
    assert.equal(dom.window.document.activeElement, option(picker, 'focus-top'));
    assert.deepEqual(told.previews, ['side-by-side', 'stacked', 'focus-top', 'stacked', 'side-by-side', 'focus-top']);
    // Tab away from the thumbnails: the page as it is
    dom.window.document.querySelector<HTMLInputElement>('.dc-layout-fit input')!.focus();
    assert.equal(told.previews.at(-1), null);
  });

  it('sets the page to fit its window, and evens it out', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    const fit = page.querySelector<HTMLInputElement>('.dc-layout-fit input')!;
    assert.equal(fit.checked, false);
    fit.click();
    assert.deepEqual(told.fits, [true]);
    assert.equal(picker.open, true, 'still open: the layouts can be tried on a fitted page');
    page.querySelector<HTMLButtonElement>('.dc-layout-even')!.click();
    assert.equal(told.evens, 1);
    assert.equal(picker.open, false);
  });

  it('closes on Escape and on a press outside, putting a preview back; its own button toggles it', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    pointer(option(picker, 'rows:2-1'), 'pointerenter');
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(picker.open, false);
    assert.deepEqual(told.previews, ['rows:2-1', null]);
    picker.show(anchor, c);
    pointer(anchor, 'pointerdown');
    assert.equal(picker.open, true, 'a press on its own button is that button\'s click, which toggles it');
    picker.show(anchor, c);
    assert.equal(picker.open, false);
    picker.show(anchor, c);
    pointer(dom.window.document.getElementById('away')!, 'pointerdown');
    assert.equal(picker.open, false);
  });

  it('opened from the page (no tile), the page\'s tiles in reading order and none marked', () => {
    const picker = new LayoutPicker(page);
    const { first: _first, ...c } = choices({ tiles: ['a', 'b'] }).choices;
    picker.show(anchor, c);
    assert.equal(page.querySelectorAll('.dc-layout-cell-mark').length, 0);
    assert.equal(option(picker, 'stacked').querySelectorAll('.dc-layout-cell').length, 2);
    assert.deepEqual(picker.options.map((o) => o.dataset['preset']), layoutsFor(2).map((p) => p.id), 'the layouts for two tiles');
  });
});
