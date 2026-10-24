// The calculated-column editor: one column per window, as upstream's
// DataCubeColumnEditor. The person edits the whole lambda (`x|...`), the
// compiler parses it (lite's own parser here) and prints a column back
// when it reopens. The compile is a stub -- what matters is that the
// editor COMPILES (never runs), waits for it, and shows the compiler's
// answer where the user is looking.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import type { CompileOutcome } from '../src/cube.ts';
import {
  ColumnEditor,
  caretFor,
  type ColumnEditorStart,
} from '../src/ui/column-editor.ts';
import type { CalcStage } from '../src/calc.ts';
import type { CubeSnapshot, DerivedColumn } from '../src/snapshot.ts';
import type { JsonColumnReader } from '../src/ui/json-fields.ts';
import { element } from '../../pure-protocol/src/index.ts';
import { someQuery } from './fake-planner.ts';
import { liteParse, litePrint, print, row } from './lite-compiler.ts';

/** A column's lambda as the compiler prints it, or undefined when it has none. */
const printed = async (d: DerivedColumn | undefined): Promise<string | undefined> => (d?.lambda ? (await print(d.lambda)) : undefined);

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [{ name: 'uplift', lambda: (await row('$x.notional * 1.1')), kind: 'measure' }],
  rows: [],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

let dom: JSDOM;
let root: HTMLElement;
let compiled: CubeSnapshot[];
let refuse: string | null;
/** Each edit the editor offered the cube (its edit and remove: the grid commands Python's edit_calculated and
 *  remove_calculated build). */
let edited: { name: string; level?: CalcStage; column?: DerivedColumn }[];
/** Each new column the editor offered the cube (its add: the grid command Python's calculate builds). */
let added: { level: CalcStage; column: DerivedColumn }[];
let applyRefusal: string | null;
let closed: number;
let canCompile: boolean;
/** Set: the compile FAILS (not a refusal, not an abort) with this. */
let compileFails: Error | null;

function open(start: ColumnEditorStart, snapshot: CubeSnapshot = CUBE): ColumnEditor {
  return new ColumnEditor(root, {
    snapshot: () => snapshot,
    start,
    debounceMs: 0,
    parse: liteParse,
    print: litePrint,
    compile: async (candidate): Promise<CompileOutcome | undefined> => {
      compiled.push(candidate);
      if (compileFails) throw compileFails;
      return canCompile ? { query: someQuery(), refusal: refuse } : undefined;
    },
    edit: async (name, level, column) => {
      edited.push({ name, level, column });
      return applyRefusal;
    },
    remove: async (name) => {
      edited.push({ name });
      return applyRefusal;
    },
    add: async (level, column) => {
      added.push({ level, column });
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
/** The formula as written: the lambda's head shown fixed, then what is in the box. */
const formula = (): string =>
  (root.querySelector('.dc-xc-prefix')?.textContent ?? '') + ($<HTMLTextAreaElement>('.dc-calc-input-expr').value);
/** The Use as toggle's choice: measure or dimension. */
const useAs = (): string | undefined => root.querySelector<HTMLElement>('.dc-xc-use .dc-on')?.dataset['value'];
/** The rail's chosen kind. */
const kindOn = (): string | undefined => root.querySelector<HTMLElement>('.dc-xc-kind.dc-on')?.dataset['kind'];
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
  edited = [];
  added = [];
  applyRefusal = null;
  closed = 0;
  canCompile = true;
  compileFails = null;
});

describe('a new column', () => {
  it("is upstream's: col_N, a Leaf Level Measure, and no Delete or Reset", () => {
    open({});
    assert.match($<HTMLInputElement>('.dc-calc-input-name').value, /^col_\d+$/);
    assert.equal(useAs(), 'measure');
    assert.equal(kindOn(), 'formula');
    assert.equal(root.querySelector('.dc-calc-delete'), null);
    assert.equal(root.querySelector('.dc-calc-reset'), null);
  });

  it('compiles the cube WITH the draft, and OK waits for a clean compile', async () => {
    open({});
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, true, 'no expression yet');
    type('.dc-calc-input-expr', 'x|$x.notional * 2');
    await settle();
    const last = compiled.at(-1);
    assert.ok((await Promise.all((last?.derived ?? []).map((d) => printed(d)))).includes('x|$x.notional * 2'));
    assert.match($('.dc-calc-check').textContent ?? '', /Compiles/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });

  it("a refusal disables OK and shows the compiler's own words", async () => {
    refuse = "unknown function 'nope'";
    open({ expression: 'x|$x.notional->nope()' });
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
    open({ expression: 'x|1', level: 'group' });
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    // a new column is offered as one, at its level -- not as the cube's two lists
    assert.equal(added[0]?.level, 'group');
    assert.equal((await printed(added[0]?.column)), 'x|1');
    assert.equal(added[0]?.column.kind, undefined);
    assert.deepEqual(edited, []);
    assert.equal(closed, 1, 'OK that the cube took closes the window');
  });

  it('never runs anything to check: a plane that cannot compile checks on OK', async () => {
    canCompile = false;
    open({ expression: 'x|$x.notional' });
    await settle();
    assert.match($('.dc-calc-check').textContent ?? '', /checked on OK/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });
});

describe('an existing column', () => {
  it('opens on it, and a rename is REPORTED', async () => {
    open({ edit: 'uplift' });
    await settle();
    // the compiler's print of the column's lambda
    assert.equal(formula(), 'x|$x.notional * 1.1');
    type('.dc-calc-input-name', 'boost');
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    // the column it edits, by the name it had, replaced by the draft's at its level
    assert.equal(edited[0]?.name, 'uplift');
    assert.equal(edited[0]?.level, 'row');
    assert.equal(edited[0]?.column?.name, 'boost');
  });

  it('Reset puts back what it opened with', async () => {
    open({ edit: 'uplift' });
    await settle();
    type('.dc-calc-input-expr', 'x|$x.notional * 9');
    $<HTMLButtonElement>('.dc-calc-reset').click();
    assert.equal(formula(), 'x|$x.notional * 1.1');
  });

  it('Delete takes it out; a refusal keeps the window and says why', async () => {
    applyRefusal = "another column uses 'uplift'";
    open({ edit: 'uplift' });
    await settle();
    $<HTMLButtonElement>('.dc-calc-delete').click();
    await settle();
    assert.deepEqual(edited, [{ name: 'uplift' }], 'the column taken out, by its name');
    assert.equal(closed, 0);
    assert.match($('.dc-calc-problem').textContent ?? '', /another column/);
  });

  it("an OK the cube refuses keeps the user's text", async () => {
    applyRefusal = 'refused by the cube';
    open({ edit: 'uplift' });
    await settle();
    type('.dc-calc-input-expr', 'x|$x.notional * 3');
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(closed, 0);
    assert.equal(formula(), 'x|$x.notional * 3');
    assert.match($('.dc-calc-problem').textContent ?? '', /refused by the cube/);
  });

  it("a new column the cube refuses keeps the user's text too (its add's refusal)", async () => {
    applyRefusal = 'refused by the cube';
    open({ expression: 'x|$x.notional * 3' });
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(added.length, 1);
    assert.equal(closed, 0);
    assert.equal(formula(), 'x|$x.notional * 3');
    assert.match($('.dc-calc-problem').textContent ?? '', /refused by the cube/);
  });
});

describe('caretFor', () => {
  it('points where the parser stopped, in the text the person typed', () => {
    const text = 'x|$x.a + $x.nope)';
    const col = text.indexOf(')') + 1;
    assert.equal(caretFor(text, `unexpected ')' [1:${col}]`), `${text}\n${' '.repeat(col - 1)}^`);
  });

  it('places a position on a later line', () => {
    assert.equal(caretFor('x|$x.a +\n  $x.b)', 'unexpected [2:7]'), '  $x.b)\n      ^');
  });

  it('says nothing when the refusal names no position, or one outside the text', () => {
    assert.equal(caretFor('x|$x.a', 'no position'), undefined);
    assert.equal(caretFor('x|$x.a', 'elsewhere [9:1]'), undefined);
  });

  it("marks lite's own parse refusal of a draft", async () => {
    const text = 'x|$x.a + )';
    const refusal = await liteParse(text).then(() => '', (e: unknown) => String(e));
    assert.match(caretFor(text, refusal) ?? '', /\^$/, refusal);
  });
});

describe('a window column', () => {
  const choose = (sel: string, value: string): void => {
    const e = $<HTMLSelectElement>(sel);
    e.value = value;
    e.dispatchEvent(new dom.window.Event('change'));
  };
  /** The rail's kind, chosen as a person does. */
  const kind = (k: string): void => $<HTMLButtonElement>(`.dc-xc-kind[data-kind="${k}"]`).click();

  it('builds a running sum from the form: function, column, partition, order, frame', async () => {
    open({});
    kind('window');
    assert.equal(root.querySelector('.dc-calc-input-expr'), null, 'no formula box for a window');
    // Unfinished: the form says what is missing and OK waits.
    await settle();
    assert.match($<HTMLElement>('.dc-calc-check').textContent ?? '', /column the window reads/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, true);
    choose('.dc-win-column', 'notional');
    const region = [...root.querySelectorAll<HTMLInputElement>('.dc-win-part-check')]
      .find((b) => b.value === 'region');
    assert.ok(region);
    region.checked = true;
    region.dispatchEvent(new dom.window.Event('change'));
    $<HTMLButtonElement>('.dc-win-order-add').click();
    choose('.dc-win-order-column', 'notional');
    choose('.dc-win-order-direction', 'desc');
    await settle();
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(added.at(-1)?.level, 'row');
    const made = added.at(-1)?.column;
    assert.deepEqual(made?.window, {
      fn: 'sum', column: 'notional', partition: ['region'],
      order: [{ column: 'notional', direction: 'desc' }], frame: 'running',
    });
    assert.equal(made?.lambda, undefined);
    // And the compile saw the cube with it.
    assert.ok(compiled.at(-1)?.derived.some((d) => d.window?.fn === 'sum'));
  });

  it('a rank takes no column and no frame; a moving average takes N rows', async () => {
    open({});
    kind('window');
    choose('.dc-win-fn', 'rank');
    assert.equal(root.querySelector('.dc-win-column'), null);
    assert.equal(root.querySelector('.dc-win-frame'), null);
    await settle();
    assert.match($<HTMLElement>('.dc-calc-check').textContent ?? '', /order/);
    choose('.dc-win-fn', 'average');
    choose('.dc-win-column', 'notional');
    $<HTMLButtonElement>('.dc-win-order-add').click();
    choose('.dc-win-frame', 'last');
    type('.dc-win-rows', '5');
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.deepEqual(added.at(-1)?.column.window?.frame, { lastRows: 5 });
  });

  it('at the group level an empty order means the grid order, and says so', async () => {
    open({ level: 'group' });
    kind('window');
    choose('.dc-win-fn', 'rowNumber');
    await settle();
    assert.match(root.querySelector('.dc-win-hint')?.textContent ?? '', /order the grid shows/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });

  it('an existing window column opens on its form', () => {
    const w = { fn: 'lag' as const, column: 'notional', partition: [], order: [{ column: 'region', direction: 'asc' as const }], offset: 2 };
    const cube: CubeSnapshot = { ...CUBE, derived: [...CUBE.derived, { name: 'prev', kind: 'measure', window: w }] };
    new ColumnEditor(root, {
      snapshot: () => cube, start: { edit: 'prev' }, debounceMs: 0, parse: liteParse, print: litePrint,
      compile: async () => ({ query: someQuery(), refusal: null }), edit: async () => null, remove: async () => null,
      add: async () => null,
      onClose: () => {},
    });
    assert.equal(kindOn(), 'window');
    assert.equal($<HTMLSelectElement>('.dc-win-fn').value, 'lag');
    assert.equal($<HTMLInputElement>('.dc-win-offset').value, '2');
  });
});

describe('picking a JSON field', () => {
  const ORDERS: CubeSnapshot = {
    ...CUBE,
    columns: [...CUBE.columns, { name: 'customer', type: 'Variant' }],
    derived: [],
  };
  const CELLS = ['{"tier":"gold","contact":{"email":"a@x"}}', '{"tier":"silver"}'];
  /** The whole column: the sample's two rows, then one only a full read sees. */
  const EVERY_ROW = [[CELLS[0]], [CELLS[1], '{"tier":"bronze","since":2020}']];

  /** A reader over those cells; `all` can be made to wait until it is cancelled. */
  function reader(sampled: string[], column: string, wait = false, cells: readonly string[] = CELLS): JsonColumnReader {
    return {
      sample: async () => { sampled.push(column); return { cells, total: 3 }; },
      all: (onChunk, signal) => new Promise<void>((resolve, reject) => {
        if (wait) {
          signal?.addEventListener('abort', () => reject(new Error('aborted')));
          return;
        }
        for (const chunk of EVERY_ROW) onChunk(chunk);
        resolve();
      }),
    };
  }

  function openJson(start: ColumnEditorStart, sampled: string[] = [], wait = false,
    cells: readonly string[] = CELLS): ColumnEditor {
    return new ColumnEditor(root, {
      snapshot: () => ORDERS,
      start,
      debounceMs: 0,
      parse: liteParse,
      print: litePrint,
      compile: async (candidate) => {
        compiled.push(candidate);
        return { query: someQuery(), refusal: null };
      },
      edit: async (name, level, column) => { edited.push({ name, level, column }); return null; },
      remove: async (name) => { edited.push({ name }); return null; },
      add: async (level, column) => { added.push({ level, column }); return null; },
      onClose: () => { closed += 1; },
      readJson: (column) => reader(sampled, column, wait, cells),
    });
  }
  const button = (label: string): HTMLButtonElement => {
    const b = [...root.querySelectorAll<HTMLButtonElement>('.dc-jsonfields-add')]
      .find((x) => x.textContent === label);
    assert.ok(b, `no '${label}' among ${[...root.querySelectorAll('.dc-jsonfields-add')]
      .map((x) => x.textContent).join(', ')}`);
    return b!;
  };

  it('opens on the JSON column it was started from, sampled', async () => {
    const sampled: string[] = [];
    openJson({ json: 'customer' }, sampled);
    await settle();
    assert.deepEqual(sampled, ['customer']);
    assert.equal($<HTMLSelectElement>('.dc-calc-json-column').value, 'customer');
  });

  it('fills the name, kind and expression, and compiles them; OK adds it', async () => {
    openJson({ json: 'customer' });
    await settle();
    button('as String').click();
    await settle();
    assert.equal($<HTMLInputElement>('.dc-calc-input-name').value, 'customer_contact_email');
    // the lambda's head shown fixed in front, the person's expression in the box
    assert.equal(formula(),
      "x|$x.customer->get('contact')->get('email')->to(@String)");
    assert.equal(useAs(), 'dimension');
    assert.ok((await Promise.all(compiled.flatMap((c) => c.derived).map((d) => printed(d))))
      .some((t) => (t ?? '').includes("get('email')")), 'the pick was compiled');
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.equal(added.at(-1)?.column.name, 'customer_contact_email');
  });

  it('pulls a nested object out as a JSON column', async () => {
    openJson({ json: 'customer' });
    await settle();
    button('as JSON').click();
    await settle();
    assert.equal(formula(),
      "x|$x.customer->get('contact')");
  });

  it('explodes to ONE column: the fields ticked, a tuple; untick to a field, or to the element', async () => {
    openJson({ json: 'customer' }, [], false, ['{"addresses":[{"kind":"billing","city":"Paris"}]}']);
    await settle();
    // the page is rebuilt with the pick: the checkbox is looked up again, as it shows
    const explode = (): HTMLInputElement => $<HTMLInputElement>('.dc-calc-input-explode');
    assert.equal(explode().checked, false);
    button('one row per element (explode)').click();
    await settle();
    assert.equal(explode().checked, true);
    const nameOf = (): string => $<HTMLInputElement>('.dc-calc-input-name').value;
    const exprOf = (): string => formula();
    // every field ticked to start, in the data's order: one (kind, city) column
    assert.equal(nameOf(), 'addresses_kind_city');
    assert.match(exprOf(), /\['kind'->pair\(.*'city'->pair\(.*\]->newMap\(\)->toVariant\(\)/);
    const tick = (key: string): HTMLInputElement =>
      [...root.querySelectorAll<HTMLInputElement>('.dc-calc-explode-tick')].find((t) => t.value === key)!;
    tick('city').click();
    await settle();
    assert.equal(nameOf(), 'addresses_kind');
    assert.equal(exprOf(), "x|$x.customer->get('addresses')->toMany(@Variant)->map(e|$e->get('kind')->to(@String))");
    tick('city').click();
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    const made = added.at(-1)?.column;
    assert.equal(made?.name, 'addresses_kind_city');
    assert.equal(made?.unnest, true);
  });

  it('keeps a name the user typed', async () => {
    openJson({ json: 'customer' });
    await settle();
    type('.dc-calc-input-name', 'mine');
    button('as String').click();
    assert.equal($<HTMLInputElement>('.dc-calc-input-name').value, 'mine');
  });

  it('says a sample is a sample, and reads every row on request', async () => {
    openJson({ json: 'customer' });
    await settle();
    const note = (): string => $<HTMLElement>('.dc-jsonfields-note').textContent ?? '';
    assert.match(note(), /2 of 3 rows sampled/);
    assert.ok(!root.textContent?.includes('since'), 'the sample has no since');
    $<HTMLButtonElement>('.dc-jsonfields-all').click();
    await settle();
    assert.match(note(), /All 3 rows read/);
    assert.ok(root.textContent?.includes('since'), 'every row shows the field only the third row has');
    assert.equal($<HTMLButtonElement>('.dc-jsonfields-all').hidden, true);
  });

  it('a full read can be cancelled, and the sample stays', async () => {
    openJson({ json: 'customer' }, [], true);
    await settle();
    const all = $<HTMLButtonElement>('.dc-jsonfields-all');
    all.click();
    assert.equal(all.textContent, 'Cancel');
    all.click();
    await settle();
    assert.match($<HTMLElement>('.dc-jsonfields-note').textContent ?? '', /Stopped reading every row/);
    assert.equal(all.textContent, 'Read every row');
    button('as String');
  });

  it('offers no JSON section on a cube without JSON columns', async () => {
    new ColumnEditor(root, {
      snapshot: () => CUBE, start: {}, debounceMs: 0, parse: liteParse, print: litePrint,
      compile: async () => ({ query: someQuery(), refusal: null }),
      edit: async () => null, remove: async () => null, add: async () => null, onClose: () => {},
      readJson: () => ({ sample: async () => ({ cells: [], total: 0 }), all: async () => undefined }),
    });
    assert.equal(root.querySelector('.dc-calc-json'), null);
  });
});

describe('a compile that FAILS (P2-152)', () => {
  it('says so, and never sits on "Compiling..." with OK disabled for good', async () => {
    compileFails = new Error("cannot pivot on 'meta': it holds JSON");
    open({ expression: 'x|$x.notional * 2' });
    await settle();
    assert.match($('.dc-calc-check').textContent ?? '', /holds JSON/);
    assert.doesNotMatch($('.dc-calc-check').textContent ?? '', /Compiling/);
  });
});

describe('arithmetic over a possibly-empty column (src/calc-fix.ts)', () => {
  it('names the column, offers Empty or As if zero, and writes the chosen one into the box', async () => {
    open({});
    refuse = 'Collection element must have a multiplicity [1], found [0..1]';
    type('.dc-calc-input-expr', 'x|$x.notional * 1.1');
    await settle();
    assert.match($('.dc-calc-fix').textContent ?? '', /'notional' can be empty/);
    // nothing is changed until the person chooses
    assert.equal(formula(), 'x|$x.notional * 1.1');
    refuse = null;
    $<HTMLButtonElement>('.dc-calc-fix-button[data-as="blank"]').click();
    await settle();
    assert.match(formula(), /\$x\.notional->toOne\(\) \* 1\.1/);
    assert.equal(root.querySelector('.dc-calc-fix'), null, 'compiled again, and taken');
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, false);
  });

  it('As if zero: a zero of the column\'s type', async () => {
    open({});
    refuse = 'Collection element must have a multiplicity [1] - Context:[Applying times], multiplicity:[0..1]';
    type('.dc-calc-input-expr', 'x|$x.notional * 1.1');
    await settle();
    refuse = null;
    $<HTMLButtonElement>('.dc-calc-fix-button[data-as="zero"]').click();
    await settle();
    assert.match(formula(), /\$x\.notional->coalesce\(0\.0\) \* 1\.1/);
  });

  it('offers nothing for another refusal', async () => {
    open({});
    refuse = "the source has no column 'nope'";
    type('.dc-calc-input-expr', 'x|$x.notional * 1.1');
    await settle();
    assert.equal(root.querySelector('.dc-calc-fix'), null);
  });
});

describe('Parent or grand total: a measure at a coarser grain', () => {
  const GROUPED: CubeSnapshot = {
    ...CUBE,
    rows: ['region'],
    groupDerived: [{ name: 'parent_notional', grain: { of: 'notional', at: 'parent' } }],
  };

  it('opens on a grain column as it is -- never as an empty formula -- and its OK keeps it one', async () => {
    open({ edit: 'parent_notional' }, GROUPED);
    await settle();
    assert.equal($('.dc-xc-kind[data-kind="grain"]').getAttribute('aria-checked'), 'true');
    assert.equal($<HTMLSelectElement>('.dc-grain-of').value, 'notional');
    assert.equal($<HTMLSelectElement>('.dc-grain-at').value, 'parent');
    const at = $<HTMLSelectElement>('.dc-grain-at');
    at.value = 'all';
    at.dispatchEvent(new dom.window.Event('change'));
    await settle();
    $<HTMLButtonElement>('.dc-calc-ok').click();
    await settle();
    assert.deepEqual(edited, [{ name: 'parent_notional', level: 'group',
      column: { name: 'parent_notional', grain: { of: 'notional', at: 'all' } } }]);
  });

  it('a new one is a group-level column; it waits for its measure, and a pivot greys it out', async () => {
    open({ expression: 'x|1' }, GROUPED);
    $<HTMLButtonElement>('.dc-xc-kind[data-kind="grain"]').click();
    await settle();
    assert.match($('.dc-calc-check').textContent ?? '', /Choose the measure/);
    assert.equal($<HTMLButtonElement>('.dc-calc-ok').disabled, true);
    root.replaceChildren();
    open({}, { ...GROUPED, pivotOn: ['region'] });
    const grain = $<HTMLButtonElement>('.dc-xc-kind[data-kind="grain"]');
    assert.equal(grain.disabled, true);
    assert.match(grain.title, /pivots its columns/);
  });
});

describe('the rail keeps its shape', () => {
  it('offers every kind, greying out the ones this cube cannot take, and says why', () => {
    open({});
    const kinds = [...root.querySelectorAll<HTMLButtonElement>('.dc-xc-kind')];
    assert.deepEqual(kinds.map((k) => k.dataset['kind']), ['formula', 'window', 'ratio', 'children', 'grain', 'json']);
    const json = kinds.find((k) => k.dataset['kind'] === 'json')!;
    assert.equal(json.disabled, true, 'no JSON column here');
    assert.match(json.title, /no JSON column/);
    assert.equal(kinds.find((k) => k.dataset['kind'] === 'formula')!.disabled, false);
  });
});
