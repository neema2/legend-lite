// The calculated-column editor: one column per window, as upstream's
// DataCubeColumnEditor. The compile is a stub here -- what matters is
// that the editor COMPILES (never runs), waits for it, and shows the
// compiler's answer where the user is looking.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  ColumnEditor,
  caretFor,
  type ColumnEditorStart,
  type CompileOutcome,
} from '../src/ui/column-editor.ts';
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
let compiled: CubeSnapshot[];
let refuse: string | null;
let applied: { row: readonly DerivedColumn[]; group: readonly DerivedColumn[];
  rename?: { from: string; to: string } }[];
let applyRefusal: string | null;
let closed: number;
let canCompile: boolean;

function open(start: ColumnEditorStart): ColumnEditor {
  return new ColumnEditor(root, {
    snapshot: () => CUBE,
    start,
    debounceMs: 0,
    compile: async (candidate): Promise<CompileOutcome | undefined> => {
      compiled.push(candidate);
      return canCompile ? { pure: 't->extend(~[x: x|1])', refusal: refuse } : undefined;
    },
    apply: async (row, group, rename) => {
      applied.push({ row, group, ...(rename ? { rename } : {}) });
      return applyRefusal;
    },
    onClose: () => { closed += 1; },
  });
}
const $ = <T extends Element>(sel: string): T => {
  const e = root.querySelector<T>(sel);
  assert.ok(e, sel);
  return e;
};
const type = (sel: string, value: string): void => {
  const e = $<HTMLInputElement>(sel);
  e.value = value;
  e.dispatchEvent(new dom.window.Event('input'));
};
const settle = async (): Promise<void> => {
  for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
};

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="r"></div>');
  root = dom.window.document.getElementById('r') as unknown as HTMLElement;
  compiled = [];
  refuse = null;
  applied = [];
  applyRefusal = null;
  closed = 0;
  canCompile = true;
});

describe('a new column', () => {
  it("is upstream's: col_N, a Leaf Level Measure, and no Delete or Reset", () => {
    open({});
    assert.match($<HTMLInputElement>('.dc-calc-input-name').value, /^col_\d+$/);
    assert.equal($<HTMLSelectElement>('.dc-calc-level').value, 'measure');
    assert.equal(root.querySelector('.dc-calc-delete'), null);
    assert.equal(root.querySelector('.dc-calc-reset'), null);
  });

  it('compiles the cube WITH the draft, and OK waits for a clean compile', async () => {
    open({});
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, true, 'no expression yet');
    type('.dc-calc-input-expr', '$x.notional * 2');
    await settle();
    const last = compiled.at(-1);
    assert.ok(last?.derived.some((d) => d.expression === '$x.notional * 2'));
    assert.match($('.dc-calc-check').textContent ?? '', /Compiles/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });

  it("a refusal disables OK and shows the compiler's own words", async () => {
    refuse = "unknown function 'nope'";
    open({ expression: '$x.notional->nope()' });
    await settle();
    assert.match($('.dc-calc-check').textContent ?? '', /unknown function 'nope'/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, true);
  });

  it('the name says ✓ or ✗ as it is typed', () => {
    open({});
    type('.dc-calc-input-name', 'region');
    assert.equal($('.dc-calc-namemark').textContent, '✗');
    assert.match($('.dc-calc-problem').textContent ?? '', /already a column/);
    type('.dc-calc-input-name', 'fresh');
    assert.equal($('.dc-calc-namemark').textContent, '✓');
  });

  it('Group Level goes to the group stage, with no kind', async () => {
    open({ expression: '1', level: 'group' });
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(applied[0]?.group.at(-1)?.expression, '1');
    assert.equal(applied[0]?.group.at(-1)?.kind, undefined);
    assert.equal(closed, 1, 'OK that the cube took closes the window');
  });

  it('never runs anything to check: a plane that cannot compile checks on OK', async () => {
    canCompile = false;
    open({ expression: '$x.notional' });
    await settle();
    assert.match($('.dc-calc-check').textContent ?? '', /checked on OK/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });
});

describe('an existing column', () => {
  it('opens on it, and a rename is REPORTED', async () => {
    open({ edit: 'uplift' });
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr').value, '$x.notional * 1.1');
    type('.dc-calc-input-name', 'boost');
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.deepEqual(applied[0]?.rename, { from: 'uplift', to: 'boost' });
    assert.deepEqual(applied[0]?.row.map((d) => d.name), ['boost']);
  });

  it('Reset puts back what it opened with', async () => {
    open({ edit: 'uplift' });
    type('.dc-calc-input-expr', '$x.notional * 9');
    $<HTMLButtonElement>('.dc-calc-reset').click();
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr').value, '$x.notional * 1.1');
  });

  it('Delete takes it out; a refusal keeps the window and says why', async () => {
    applyRefusal = "another column uses 'uplift'";
    open({ edit: 'uplift' });
    await settle();
    $<HTMLButtonElement>('.dc-calc-delete').click();
    await settle();
    assert.deepEqual(applied[0]?.row, []);
    assert.equal(closed, 0);
    assert.match($('.dc-calc-problem').textContent ?? '', /another column/);
  });

  it("an OK the cube refuses keeps the user's text", async () => {
    applyRefusal = 'refused by the cube';
    open({ edit: 'uplift' });
    type('.dc-calc-input-expr', '$x.notional * 3');
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(closed, 0);
    assert.equal($<HTMLTextAreaElement>('.dc-calc-input-expr').value, '$x.notional * 3');
    assert.match($('.dc-calc-problem').textContent ?? '', /refused by the cube/);
  });
});

describe('caretFor', () => {
  it('points INSIDE the expression when the compiler names a position there', () => {
    const pure = "t->extend(~[m: x|$x.a + $x.nope])";
    const col = pure.indexOf('$x.nope') + 1;
    assert.equal(caretFor(pure, `no column 'nope' [1:${col}]`, 'm', '$x.a + $x.nope'),
      '$x.a + $x.nope\n       ^');
  });

  it('says nothing when there is no position, or it is elsewhere', () => {
    const pure = "t->extend(~[m: x|$x.a])";
    assert.equal(caretFor(pure, 'no position', 'm', '$x.a'), undefined);
    assert.equal(caretFor(pure, 'elsewhere [1:1]', 'm', '$x.a'), undefined);
  });
});
