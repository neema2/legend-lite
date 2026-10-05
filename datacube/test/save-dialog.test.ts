// Save and Save As (src/ui/save-dialog.ts): over the copy this was opened from or as a new cube,
// what is saved and what is not said, a name asked for, a refusal kept in the window, and --
// when saving over would drop something -- that said before anything is written.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { saveDialog, type SaveDialogOptions } from '../src/ui/save-dialog.ts';

let dom: JSDOM;
let doc: Document;
let saved: string[];
const settle = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};
const $ = <T extends Element>(sel: string): T => {
  const e = doc.querySelector<T>(sel);
  assert.ok(e, sel);
  return e;
};
const button = (label: string): HTMLButtonElement =>
  [...doc.querySelectorAll<HTMLButtonElement>('.dc-save-foot button')].find((b) => b.textContent === label)
    ?? assert.fail(`no button ${label}: ${[...doc.querySelectorAll('.dc-save-foot button')].map((b) => b.textContent).join(', ')}`);

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body></body>');
  doc = dom.window.document;
  saved = [];
});

const base = (more: Partial<SaveDialogOptions> = {}): SaveDialogOptions => ({
  purpose: 'save',
  name: 'Trades by region',
  where: 'in this browser',
  keeps: ['Grouped by region', '2 visualizations, and the page’s layout'],
  leaves: 'Not the rows: opening it reads trades.csv again, from your computer.',
  save: async (name, asNew) => { saved.push(`${name} ${asNew ? 'new' : 'over'}`); },
  ...more,
});

describe('the Save window', () => {
  it('Save over the copy this was opened from, saying so -- or Save as new', async () => {
    // the dialog's clock is the test's: "2 hours ago" is a fact of the two times, not of when the test runs
    const now = Date.parse('2026-10-05T12:00:00Z');
    const done = saveDialog(doc, base({ over: { name: 'Trades by region', savedAt: now - 2 * 3600_000 }, now: () => now }));
    assert.equal($('.dc-picker-title').textContent, 'Save');
    assert.match($('.dc-save-where').textContent ?? '', /Saves over “Trades by region”, saved 2 hours ago/);
    assert.deepEqual([...doc.querySelectorAll('.dc-save-keep')].map((l) => l.textContent),
      ['Grouped by region', '2 visualizations, and the page’s layout']);
    assert.match($('.dc-save-leave').textContent ?? '', /Not the rows/);
    button('Save').click();
    assert.deepEqual(await done, { name: 'Trades by region', asNew: false });
    assert.deepEqual(saved, ['Trades by region over']);
    assert.equal(doc.querySelector('.dc-save'), null, 'the window is gone');
  });

  it('Save As is always a new cube, its name ready to type over', async () => {
    const done = saveDialog(doc, base({ purpose: 'saveAs', over: { name: 'Trades by region' } }));
    assert.equal($('.dc-picker-title').textContent, 'Save as');
    assert.equal(doc.querySelectorAll('.dc-save-foot button').length, 2, 'Cancel and Save: no "as new" to choose');
    const name = $<HTMLInputElement>('.dc-save-input');
    name.value = 'Trades by desk';
    name.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    assert.deepEqual(await done, { name: 'Trades by desk', asNew: true });
  });

  it('asks for a name rather than saving without one', async () => {
    const done = saveDialog(doc, base());
    $<HTMLInputElement>('.dc-save-input').value = '  ';
    button('Save').click();
    await settle();
    assert.deepEqual(saved, []);
    assert.match($('.dc-picker-status').textContent ?? '', /Give the cube a name/);
    button('Cancel').click();
    assert.equal(await done, undefined);
  });

  it('says what saving over drops, then saves over it -- or as new -- only when asked again', async () => {
    const done = saveDialog(doc, base({
      over: { name: 'Trades by region' },
      warning: 'The saved “Trades by region” has parts this file cannot show: the column book.',
    }));
    button('Save').click();
    await settle();
    assert.deepEqual(saved, [], 'nothing written before the person decides');
    assert.equal($<HTMLElement>('.dc-save-warning').hidden, false);
    assert.match($('.dc-save-warning').textContent ?? '', /the column book/);
    button('Save as new instead').click();
    assert.deepEqual(await done, { name: 'Trades by region', asNew: true });

    const again = saveDialog(doc, base({ over: { name: 'Trades by region' }, warning: 'drops the column book' }));
    button('Save').click();
    await settle();
    button('Save over it anyway').click();
    assert.deepEqual(await again, { name: 'Trades by region', asNew: false });
    assert.deepEqual(saved, ['Trades by region new', 'Trades by region over']);
  });

  it('a refusal stays in the window, which stays open', async () => {
    const done = saveDialog(doc, base({ save: async () => { throw new Error('only cubes over a file can be saved'); } }));
    button('Save').click();
    await settle();
    assert.match($('.dc-picker-status').textContent ?? '', /only cubes over a file/);
    assert.ok(doc.querySelector('.dc-save'), 'still open');
    doc.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(await done, undefined);
  });
});
