// The calculated-column editor, in CI.
//
// Its fixes (census §1, C1-C6) were proven in the browser harness,
// which runs by hand; these pin the editor's own half where every
// build runs them.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CalcEditor, type CalcStart } from '../src/ui/calc-editor.ts';
import type { CubeSnapshot, DerivedColumn } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [{ name: 'uplift', expression: '$x.notional * 1.1', kind: 'measure' }],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

let dom: JSDOM;
let root: HTMLElement;
let calls: { row: readonly DerivedColumn[]; rename?: { from: string; to: string } }[];
let refusal: string | null;

function open(start?: CalcStart, snapshot: CubeSnapshot = CUBE): void {
  new CalcEditor(root, {
    snapshot,
    onChange: async (row, _group, rename) => {
      calls.push({ row, ...(rename ? { rename } : {}) });
      return refusal;
    },
    ...(start ? { start } : {}),
  });
}
const $ = <T extends Element>(sel: string): T | null => root.querySelector<T>(sel);
const type = (sel: string, value: string): void => {
  const el = $<HTMLInputElement>(sel);
  assert.ok(el, sel);
  el.value = value;
  el.dispatchEvent(new dom.window.Event('input'));
};
const save = async (): Promise<void> => {
  $<HTMLButtonElement>('.dc-calc-save')?.click();
  await new Promise((r) => setTimeout(r, 0));
};

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="r"></div>');
  root = dom.window.document.getElementById('r') as unknown as HTMLElement;
  calls = [];
  refusal = null;
});

describe('the calculated-column editor', () => {
  it('C1: a new column defaults to MEASURE, named col_N', () => {
    open({ stage: 'row' });
    assert.equal($<HTMLInputElement>('.dc-calc-kind-input:checked')?.value, 'measure');
    assert.match($<HTMLInputElement>('.dc-calc-input-name')?.value ?? '', /^col_\d+$/);
  });

  it('C6: Extend seeds the reference and the kind', () => {
    open({ stage: 'row', expression: '$x.region', kind: 'dimension' });
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr')?.value, '$x.region');
    assert.equal($<HTMLInputElement>('.dc-calc-kind-input:checked')?.value, 'dimension');
  });

  it('C6: Edit opens on the named column', () => {
    open({ edit: 'uplift' });
    assert.equal($<HTMLInputElement>('.dc-calc-input-name')?.value, 'uplift');
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr')?.value, '$x.notional * 1.1');
  });

  it('C5: a refusal keeps the form, the text, and says why IN the form', async () => {
    refusal = "relation has no column 'nope'";
    open({ stage: 'row' });
    type('.dc-calc-input-name', 'bogus');
    type('.dc-calc-input-expr', '$x.nope * 2');
    await save();
    assert.ok($('.dc-calc-form'), 'the form closed');
    assert.equal($<HTMLInputElement>('.dc-calc-input-name')?.value, 'bogus');
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr')?.value, '$x.nope * 2');
    assert.match($('.dc-calc-problem')?.textContent ?? '', /nope/);
    // Nothing listed that the cube did not take.
    assert.ok(![...root.querySelectorAll('.dc-calc-name')].some((e) => e.textContent === 'bogus'));
  });

  it('a rename is REPORTED, so the cube can carry it through', async () => {
    open({ edit: 'uplift' });
    type('.dc-calc-input-name', 'boost');
    await save();
    assert.deepEqual(calls.at(-1)?.rename, { from: 'uplift', to: 'boost' });
  });
});
