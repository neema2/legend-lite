// The Join… window (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5; src/ui/join-dialog.ts): their grid's columns read
// with mine, a plain key preset, the join's `how` built from what the window says and asked what it will do (shown in
// words), each form shaping the window, a condition written in Pure read by the host's compiler, the compiler's refusal
// shown as it says it, and Join making the grid with the `how` shown.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { accessor, type Lambda } from '../../pure-protocol/src/index.ts';
import type { JoinHow, JoinSide, JoinSummaryCounts } from '../src/join.ts';
import { joinDialog, type JoinDialogOptions } from '../src/ui/join-dialog.ts';

let doc: Document;
const settle = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};
/** Long enough for the window's 250 ms wait after a change. */
const debounce = async (): Promise<void> => {
  await new Promise((r) => setTimeout(r, 300));
  await settle();
};
beforeEach(() => {
  doc = new JSDOM('<!doctype html><body></body>').window.document;
});

const side = (name: string, columns: [string, string][]): JoinSide => ({
  name, relation: accessor('local::DB', name), columns: columns.map(([n, type]) => ({ name: n, type })),
});
const TRADES = side('trades', [['id', 'Integer'], ['customer_id', 'Integer'], ['qty', 'Float']]);
const CUSTOMERS = side('customers', [['id', 'Integer'], ['region', 'String']]);
const COUNTS: JoinSummaryCounts = { mine: 5, matched: 3, joined: 6, repeated: { keys: 1, most: 2 } };

/** The window over a host that records what it was asked. */
function open(over: Partial<JoinDialogOptions> = {}): { asked: JoinHow[]; applied: JoinHow[]; made: Promise<boolean> } {
  const asked: JoinHow[] = [];
  const applied: JoinHow[] = [];
  const made = joinDialog(doc, {
    mine: 'trades',
    others: [{ id: 'grid-2', name: 'customers' }],
    inputs: async () => ({ left: TRADES, right: CUSTOMERS }),
    parse: async () => { throw new Error('no Pure here'); },
    summary: async (_their, how) => { asked.push(how); return COUNTS; },
    apply: async (_their, how) => { applied.push(how); },
    ...over,
  });
  return { asked, applied, made };
}
const $ = <T extends Element>(selector: string): T => doc.querySelector<T>(selector)!;
const choose = (selector: string, value: string): void => {
  const s = $<HTMLSelectElement>(selector);
  s.value = value;
  s.dispatchEvent(new doc.defaultView!.Event('change'));
};
const status = (): string => $('.dc-join .dc-picker-status').textContent ?? '';
const will = (): string[] => [...doc.querySelectorAll('.dc-join-will-line')].map((l) => l.textContent ?? '');

describe('the Join window', () => {
  it('reads their columns with mine, presets the plain key, and says what it will do before it joins', async () => {
    const { asked, applied, made } = open();
    await settle();
    await debounce();
    assert.deepEqual(asked.at(-1), { form: 'join', kind: 'left', on: { rows: [{ left: 'customer_id', op: '==', right: 'id' }] }, prefix: 'their_', keep: null });
    assert.deepEqual(will(), [
      '3 rows of my 5 rows find a match, 2 rows do not.',
      '6 rows after it: more than mine -- a row of mine repeats once for each of theirs it matches.',
      '1 key of theirs appears more than once (at most 2 times): the rows of mine with it repeat, and totals count them again.',
    ]);
    // their id, a name mine has, said as it will come out
    assert.match($('.dc-join-column-list').textContent ?? '', /id → their_id/);
    assert.equal($<HTMLButtonElement>('.dc-join .dc-primary').disabled, false);
    $<HTMLButtonElement>('.dc-join .dc-primary').click();
    assert.equal(await made, true);
    assert.deepEqual(applied, [asked.at(-1)]);
    assert.equal(doc.querySelector('.dc-join'), null, 'closed once joined');
  });

  it('keeps only the columns checked, under the prefix given, and takes a condition row away', async () => {
    const { asked } = open();
    await settle();
    const [id] = [...doc.querySelectorAll<HTMLInputElement>('.dc-join-column-list input')];
    id!.checked = false;
    id!.dispatchEvent(new doc.defaultView!.Event('change'));
    const prefix = $<HTMLInputElement>('.dc-join-prefix');
    prefix.value = 'c_';
    prefix.dispatchEvent(new doc.defaultView!.Event('input'));
    await debounce();
    assert.deepEqual(asked.at(-1), { form: 'join', kind: 'left', on: { rows: [{ left: 'customer_id', op: '==', right: 'id' }] }, prefix: 'c_', keep: ['region'] });
    $<HTMLButtonElement>('.dc-join-remove').click();
    await debounce();
    assert.deepEqual((asked.at(-1) as Extract<JoinHow, { form: 'join' }>).on, { rows: [] }, 'no condition: every pair');
  });

  it('shapes itself to each form: as of asks its match, semi and anti no columns, append nothing more', async () => {
    const { asked } = open();
    await settle();
    choose('.dc-join-form', 'asOf');
    assert.equal($<HTMLElement>('.dc-join-match').hidden, false);
    choose('.dc-join-match .dc-join-left', 'id');
    choose('.dc-join-match .dc-join-op', '<=');
    choose('.dc-join-match .dc-join-right', 'id');
    await debounce();
    assert.deepEqual(asked.at(-1), {
      form: 'asOf', match: { left: 'id', op: '<=', right: 'id' }, on: { rows: [{ left: 'customer_id', op: '==', right: 'id' }] }, prefix: 'their_', keep: null,
    });
    choose('.dc-join-form', 'anti');
    assert.equal($<HTMLElement>('.dc-join-columns').hidden, true);
    await debounce();
    assert.deepEqual(asked.at(-1), { form: 'anti', on: { rows: [{ left: 'customer_id', op: '==', right: 'id' }] } });
    assert.deepEqual(will(), ['3 rows of my 5 rows have a match, 2 rows none.', 'It keeps 2 rows, each once.']);
    choose('.dc-join-form', 'append');
    assert.equal($<HTMLElement>('.dc-join-condition').hidden, true);
    await debounce();
    assert.deepEqual(asked.at(-1), { form: 'append' });
  });

  it('asks nothing while a condition lacks a column, and says what it needs', async () => {
    const { asked } = open();
    await settle();
    await debounce();
    const before = asked.length;
    $<HTMLButtonElement>('.dc-join-add').click();
    await debounce();
    assert.equal(asked.length, before);
    assert.match(status(), /Choose both columns of each condition/);
    assert.equal($<HTMLButtonElement>('.dc-join .dc-primary').disabled, true);
  });

  it('reads a condition written in Pure through the host, and shows the compiler\'s refusal as it says it', async () => {
    const lambda: Lambda = { _type: 'lambda', parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 'c' }], body: [{ _type: 'boolean', value: true }] };
    const parsed: string[] = [];
    const { asked } = open({
      parse: async (text) => { parsed.push(text); return lambda; },
      summary: async (_their, how) => {
        asked.push(how);
        throw new Error("relation has no column 'nope'");
      },
    });
    await settle();
    const on = $<HTMLInputElement>('.dc-join-pure-on');
    on.checked = true;
    on.dispatchEvent(new doc.defaultView!.Event('change'));
    assert.equal($<HTMLElement>('.dc-join-pure').hidden, false);
    const pure = $<HTMLTextAreaElement>('.dc-join-pure');
    pure.value = '{t, c | $t.nope == $c.id}';
    pure.dispatchEvent(new doc.defaultView!.Event('input'));
    await debounce();
    assert.deepEqual(parsed.at(-1), '{t, c | $t.nope == $c.id}');
    assert.deepEqual((asked.at(-1) as Extract<JoinHow, { form: 'join' }>).on, { lambda });
    assert.match(status(), /relation has no column 'nope'/);
    assert.equal($<HTMLButtonElement>('.dc-join .dc-primary').disabled, true);
  });

  it('says why their grid cannot be read, and closes on Escape without joining', async () => {
    const { made } = open({ inputs: async () => { throw new Error('customers is a warehouse table, read live'); } });
    await settle();
    assert.match(status(), /warehouse table/);
    doc.dispatchEvent(new doc.defaultView!.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(await made, false);
  });

  it('fills the form with the grid chosen last, an earlier one\'s columns arriving later notwithstanding', async () => {
    const PRICES = side('prices', [['sym', 'String'], ['px', 'Float']]);
    let slow!: () => void;
    const { asked } = open({
      others: [{ id: 'grid-2', name: 'customers' }, { id: 'grid-3', name: 'prices' }],
      // the first grid read is slow: its columns land after the second's
      inputs: async (their) => {
        if (their === 'grid-2') await new Promise<void>((r) => { slow = r; });
        return { left: TRADES, right: their === 'grid-2' ? CUSTOMERS : PRICES };
      },
    });
    await settle();
    choose('.dc-join-their', 'grid-3');
    await settle();
    slow();
    await settle();
    const theirColumns = [...doc.querySelectorAll<HTMLOptionElement>('.dc-join-rows .dc-join-right option')].map((o) => o.value).filter(Boolean);
    assert.deepEqual(theirColumns, ['sym', 'px'], 'prices\' columns, not customers\'');
    assert.deepEqual([...doc.querySelectorAll<HTMLInputElement>('.dc-join-column-list input')].map((b) => b.value), ['sym', 'px']);
    // nothing asked of customers' columns against prices
    assert.ok(asked.every((h) => h.form !== 'join' || 'lambda' in h.on || h.on.rows.every((r) => r.right !== 'region')));
  });

  it('cannot be closed while Join reads what it says: a condition in Pure, parsed, then joined', async () => {
    let parsed!: () => void;
    const lambda: Lambda = { _type: 'lambda', parameters: [{ _type: 'var', name: 'a' }, { _type: 'var', name: 'b' }], body: [{ _type: 'boolean', value: true }] };
    let parses = 0;
    const { applied, made } = open({
      // the window's own asking parses at once; Join's parse waits
      parse: async () => {
        parses += 1;
        if (parses > 1) await new Promise<void>((r) => { parsed = r; });
        return lambda;
      },
    });
    await settle();
    const on = $<HTMLInputElement>('.dc-join-pure-on');
    on.checked = true;
    on.dispatchEvent(new doc.defaultView!.Event('change'));
    const pure = $<HTMLTextAreaElement>('.dc-join-pure');
    pure.value = '{a, b | true}';
    pure.dispatchEvent(new doc.defaultView!.Event('input'));
    await debounce();
    $<HTMLButtonElement>('.dc-join .dc-primary').click();
    await settle();
    doc.dispatchEvent(new doc.defaultView!.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.ok(doc.querySelector('.dc-join'), 'not closed while Join is under way');
    parsed();
    assert.equal(await made, true);
    assert.equal(applied.length, 1);
  });

  it('asks for a prefix when it is emptied, never putting one of its own in its place', async () => {
    const { asked } = open();
    await settle();
    await debounce();
    const before = asked.length;
    const prefix = $<HTMLInputElement>('.dc-join-prefix');
    prefix.value = '';
    prefix.dispatchEvent(new doc.defaultView!.Event('input'));
    await debounce();
    assert.equal(asked.length, before);
    assert.match(status(), /Give their names that mine has a prefix/);
  });

  it('keeps the window open, saying why, when joining is refused', async () => {
    const { made } = open({ apply: async () => { throw new Error('the page has no database of its own'); } });
    await settle();
    await debounce();
    $<HTMLButtonElement>('.dc-join .dc-primary').click();
    await settle();
    assert.match(status(), /no database of its own/);
    assert.ok(doc.querySelector('.dc-join'), 'still open');
    doc.querySelector<HTMLButtonElement>('.dc-join .dc-picker-close')!.click();
    assert.equal(await made, false);
  });
});
