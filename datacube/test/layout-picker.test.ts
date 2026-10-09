// The layout picker in a DOM (ui/layout-picker.ts): a thumbnail per layout, each the page's own tiles arranged that
// way with the tile it was opened from marked; previewed while pointed at or focused, never just by opening; arranged
// on a click with no flash back; Fit to window and Even out; and closing by Escape, a press outside or its own button.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { LayoutPicker, type LayoutPickerChoices } from '../src/ui/layout-picker.ts';
import { PRESETS, type Preset } from '../src/layout/bands.ts';

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
  it('shows every layout as the page\'s own tiles, the one it was opened from marked, in the page', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    anchor.focus();
    picker.show(anchor, c);
    assert.ok(page.querySelector('.dc-layout-picker'), 'in the page, not the document\'s body');
    assert.deepEqual(picker.options.map((o) => o.dataset['preset']), PRESETS.map((p) => p.id));
    for (const o of picker.options) assert.equal(o.querySelectorAll('.dc-layout-cell').length, 3);
    // one on the left, the rest stacked on the right: c, the tile it was opened from, is the big one on the left
    const cells = [...option(picker, 'left-and-column').querySelectorAll<HTMLElement>('.dc-layout-cell')];
    const marked = cells.filter((cell) => cell.classList.contains('dc-layout-cell-mark'));
    assert.equal(marked.length, 1);
    assert.equal(marked[0]!.style.left, '0px');
    assert.equal(marked[0]!.style.height, '44px');
    assert.equal(option(picker, 'left-and-column').getAttribute('aria-label'),
      'One on the left, the rest stacked on the right');
    assert.deepEqual(told.previews, [], 'opening previews nothing');
  });

  it('previews a layout while it is pointed at, and the page as it is when the pointer leaves', () => {
    const picker = new LayoutPicker(page);
    const { choices: c, told } = choices();
    picker.show(anchor, c);
    pointer(option(picker, 'grid-2'), 'pointerenter');
    pointer(option(picker, 'grid-3'), 'pointerenter');
    pointer(page.querySelector('.dc-layout-options')!, 'pointerleave');
    assert.deepEqual(told.previews, ['grid-2', 'grid-3', null]);
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
    assert.equal(dom.window.document.activeElement, option(picker, 'left-and-column'), 'down a row of four');
    key('End');
    assert.equal(dom.window.document.activeElement, option(picker, 'large-and-two'));
    assert.deepEqual(told.previews, ['side-by-side', 'stacked', 'left-and-column', 'large-and-two']);
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
    pointer(option(picker, 'grid-2'), 'pointerenter');
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(picker.open, false);
    assert.deepEqual(told.previews, ['grid-2', null]);
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
    assert.equal(option(picker, 'grid-3').querySelectorAll('.dc-layout-cell').length, 2);
  });
});
