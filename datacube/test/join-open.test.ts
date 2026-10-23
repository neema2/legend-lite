// A join opened over one model and run (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3, §17.4; src/join-open.ts): two
// tables of a DuckDB, the writer's model of both, each input a grid's result over its table (its rows, or its groups),
// every form planned by the tab's own module and its rows read back -- an empty key matching nothing, a grouped input
// joined, as-of, semi and anti, an append's missing column, every pair, a join inside an input -- the dialog's questions
// answered, and what cannot be joined here refused in words.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { readJoinHow, joinSummary, type JoinHow } from '../src/join.ts';
import { TAB_TABLES, leafRefusal, openInputs, openJoin, summaryCounts, type JoinOpening } from '../src/join-open.ts';
import {
  leavesOf, rawRows, readCube, cubeToJson, type CubeSource, type FileSource, type JoinInput, type JoinSource, type LeafSource, type SavedQuery,
} from '../src/cube-document.ts';
import { TOTAL_ROWS_COLUMN } from '../src/query.ts';
import { catalogColumns } from '../src/upload.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { plannerFor } from './catalog-builder.ts';
import type { LitePlanner } from '../src/lite-planner.ts';

const file = (name: string, columns: [string, string][]): FileSource => ({
  _type: 'file', name: `${name}.csv`, format: 'csv', size: 1, sha256: name.padEnd(64, '0'),
  columns: columns.map(([n, type]) => ({ name: n, type })),
});
const TRADES = file('trades', [['id', 'Integer'], ['cust', 'Integer'], ['sym', 'String'], ['at', 'DateTime'], ['qty', 'Float']]);
const CUSTOMERS = file('customers', [['id', 'Integer'], ['region', 'String']]);
const PRICES = file('prices', [['sym', 'String'], ['at', 'DateTime'], ['px', 'Float']]);
const MORE = file('more', [['sym', 'String'], ['id', 'Integer'], ['venue', 'String']]);
const LABELS = file('labels', [['id', 'String']]);
const TWINS = file('twins', [['id', 'Integer'], ['c_id', 'Integer']]);
const TAGGED = file('tagged', [['id', 'Integer'], ['tags', 'Variant']]);
const PRICED = file('priced', [['id', 'Integer'], ['px', 'Decimal']]);

/** A grid's saved query: its rows as they are, or `over` it. */
const query = (s: FileSource, over: Partial<SavedQuery> = {}): SavedQuery => {
  const { source: _s, epoch: _e, window: _w, ...q } = rawRows({ _type: 'string', value: '' }, s.columns.map((c) => ({ ...c })));
  return { ...q, ...over };
};
const input = (s: CubeSource, name: string, q?: SavedQuery): JoinInput => ({
  name, source: s, query: q ?? query(s as FileSource),
});
const join = (left: JoinInput, right: JoinInput, how: JoinHow): JoinSource => ({
  _type: 'join', name: `${left.name} ⋈ ${right.name}`, left, right, how, columns: [],
});
const LEFT_ON_CUST: JoinHow = { form: 'join', kind: 'left', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] }, prefix: 'c_', keep: null };

describe('a join opened over one model, its rows from DuckDB', () => {
  let engine: DuckDbEngine;
  let models: LitePlanner;
  const planners: LitePlanner[] = [];
  const opening = (tables: string[]): JoinOpening => ({
    here: TAB_TABLES, tables, models, databaseType: 'DuckDB',
    catalog: (table) => catalogColumns(engine, models, table),
    typer: (model, runtime) => {
      const p = plannerFor(model, runtime);
      planners.push(p);
      return p;
    },
  });
  /** The rows of a query over the join's model, column by column. */
  const rows = async (model: string, runtime: string, lambda: Parameters<LitePlanner['plan']>[0]): Promise<Record<string, unknown[]>> => {
    const p = plannerFor(model, runtime);
    planners.push(p);
    const answer = await engine.run((await p.plan(lambda)).sql, 0);
    return Object.fromEntries(answer.columns.map((c) => [c.name, c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v))]));
  };
  const count = async (model: string, runtime: string, lambda: Parameters<LitePlanner['plan']>[0]): Promise<number> =>
    Number((await rows(model, runtime, lambda))[TOTAL_ROWS_COLUMN]![0]);

  before(async () => {
    const db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    models = plannerFor('', 'local::RT');
    await engine.run('CREATE TABLE trades (id BIGINT NOT NULL, cust INTEGER, sym VARCHAR, "at" TIMESTAMP, qty DOUBLE)', 0);
    await engine.run(`INSERT INTO trades VALUES (1, 10, 'A', '2026-01-01 10:00', 5), (2, 10, 'B', '2026-01-01 11:00', 7),
      (3, 20, 'A', '2026-01-01 12:00', 1), (4, NULL, 'C', '2026-01-01 13:00', 2), (5, 99, 'A', '2026-01-01 09:00', 3)`, 0);
    await engine.run('CREATE TABLE customers (id INTEGER NOT NULL, region VARCHAR)', 0);
    // customer 20 twice: a key that repeats
    await engine.run("INSERT INTO customers VALUES (10, 'EMEA'), (20, 'AMER'), (20, 'APAC'), (30, 'EMEA')", 0);
    await engine.run('CREATE TABLE prices (sym VARCHAR, "at" TIMESTAMP, px DOUBLE)', 0);
    await engine.run(`INSERT INTO prices VALUES ('A', '2026-01-01 09:30', 100), ('A', '2026-01-01 11:30', 101),
      ('B', '2026-01-01 10:30', 50)`, 0);
    await engine.run('CREATE TABLE more (sym VARCHAR, id BIGINT NOT NULL, venue VARCHAR)', 0);
    await engine.run("INSERT INTO more VALUES ('D', 6, 'X')", 0);
    await engine.run('CREATE TABLE labels (id VARCHAR)', 0);
    await engine.run('CREATE TABLE twins (id INTEGER, c_id INTEGER)', 0);
    // a column trades lack of JSON (a Variant), and one of a precise decimal
    await engine.run('CREATE TABLE tagged (id BIGINT NOT NULL, tags JSON)', 0);
    await engine.run(`INSERT INTO tagged VALUES (7, '["a","b"]')`, 0);
    await engine.run('CREATE TABLE priced (id BIGINT NOT NULL, px DECIMAL(10,2))', 0);
    await engine.run('INSERT INTO priced VALUES (7, 12.50)', 0);
  });

  after(async () => {
    for (const p of planners) p.dispose();
    models?.dispose();
    await engine?.close();
  });

  it('a left join: their names mine has prefixed; an empty key matches no row; a repeated key repeats my row', async () => {
    const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), LEFT_ON_CUST), opening(['trades', 'customers']));
    assert.deepEqual(opened.columns.map((c) => [c.name, c.type]),
      [['id', 'Integer'], ['cust', 'Integer'], ['sym', 'String'], ['at', 'DateTime'], ['qty', 'Float'], ['c_id', 'Integer'], ['region', 'String']]);
    assert.equal(opened.model.match(/Database local::DB/g)?.length, 1, 'one Database of both tables');
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    const pairs = got['id']!.map((id, i) => `${String(id)}:${String(got['region']![i] ?? '-')}`).sort();
    assert.deepEqual(pairs, ['1:EMEA', '2:EMEA', '3:AMER', '3:APAC', '4:-', '5:-']);
    assert.deepEqual(opened.notes, []);
  });

  it('answers the dialog\'s questions: my rows, those with a match, the rows after it, their keys that repeat', async () => {
    const inputs = await openInputs(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), opening(['trades', 'customers']));
    const q = joinSummary(inputs.left, inputs.right, LEFT_ON_CUST);
    const p = plannerFor(inputs.model, inputs.runtime);
    planners.push(p);
    const counts = await summaryCounts(q, async (query) => engine.run((await p.plan(query)).sql, 0));
    // trade 4 has no key, trade 5 no customer; customer 20 twice: trade 3 twice
    assert.deepEqual(counts, { mine: 5, matched: 3, joined: 6, repeated: { keys: 1, most: 2 } });
    // their region repeats (EMEA twice) whether or not a trade matches it: a key of theirs said as it is
    const byRegion: JoinHow = { ...LEFT_ON_CUST, on: { rows: [{ left: 'sym', op: '==', right: 'region' }] } };
    const regions = await summaryCounts(joinSummary(inputs.left, inputs.right, byRegion), async (query) => engine.run((await p.plan(query)).sql, 0));
    assert.deepEqual(regions, { mine: 5, matched: 0, joined: 5, repeated: { keys: 1, most: 2 } });
    // no key repeats (each trade's id its own): a count over no rows is none, not a missing answer
    const self = await openInputs(input(TRADES, 'trades'), input(TRADES, 'again'), opening(['trades', 'trades']));
    const q2 = joinSummary(self.left, self.right, { ...LEFT_ON_CUST, on: { rows: [{ left: 'id', op: '==', right: 'id' }] } });
    const p2 = plannerFor(self.model, self.runtime);
    planners.push(p2);
    const unique = await summaryCounts(q2, async (query) => engine.run((await p2.plan(query)).sql, 0));
    assert.deepEqual(unique, { mine: 5, matched: 5, joined: 5, repeated: { keys: 0, most: 0 } });
  });

  it('joins a grouped grid\'s result: a join after an aggregate', async () => {
    // customers grouped by region, counting them: one row per region
    const byRegion = query(CUSTOMERS, { rows: ['region'], columns: [{ name: 'id', type: 'Integer', aggregate: 'count' }, { name: 'region', type: 'String' }] });
    const how: JoinHow = { form: 'join', kind: 'inner', on: { rows: [{ left: 'sym', op: '!=', right: 'region' }] }, prefix: 'c_', keep: ['id'] };
    const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'regions', byRegion), how), opening(['trades', 'customers']));
    assert.deepEqual(opened.right.columns.map((c) => c.name), ['region', 'id'], 'the grouped grid\'s result: its key, its measure');
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'c_id']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.equal(got['id']!.length, 15, 'every trade with each of three regions');
    assert.deepEqual([...new Set(got['c_id']!.map(Number))].sort(), [1, 2], 'AMER and APAC one each, EMEA two');
    // the questions over a grouped input: matched a correlated exists over its groups
    const p = plannerFor(opened.model, opened.runtime);
    planners.push(p);
    const counts = await summaryCounts(joinSummary(opened.left, opened.right, how), async (query) => engine.run((await p.plan(query)).sql, 0));
    assert.deepEqual(counts, { mine: 5, matched: 5, joined: 15 });
  });

  it('as of: each trade with the last price at or before it, of its symbol', async () => {
    const how = readJoinHow({ form: 'asOf', match: { left: 'at', op: '>=', right: 'at' }, on: { rows: [{ left: 'sym', op: '==', right: 'sym' }] }, prefix: 'p_', keep: ['px'] });
    const opened = await openJoin(join(input(TRADES, 'trades'), input(PRICES, 'prices'), how), opening(['trades', 'prices']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'px']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    const px = Object.fromEntries(got['id']!.map((id, i) => [Number(id), got['px']![i] ?? null]));
    assert.deepEqual(px, { 1: 100, 2: 50, 3: 101, 4: null, 5: null });
  });

  it('semi and anti: my rows with a match, and with none', async () => {
    const on = { rows: [{ left: 'cust', op: '==' as const, right: 'id' }] };
    for (const [form, ids] of [['semi', [1, 2, 3]], ['anti', [4, 5]]] as const) {
      const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), { form, on }), opening(['trades', 'customers']));
      assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty']);
      const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
      assert.deepEqual(got['id']!.map(Number).sort(), ids, `${form}: each row once, customer 20's two rows notwithstanding`);
    }
  });

  it('append: columns by name, a column one side lacks empty on its rows', async () => {
    const opened = await openJoin(join(input(TRADES, 'trades'), input(MORE, 'more'), { form: 'append' }), opening(['trades', 'more']));
    assert.deepEqual(opened.columns.map((c) => [c.name, c.type]),
      [['id', 'Integer'], ['cust', 'Integer'], ['sym', 'String'], ['at', 'DateTime'], ['qty', 'Float'], ['venue', 'String']]);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    const six = got['id']!.map(Number).indexOf(6);
    assert.deepEqual([got['sym']![six], got['venue']![six], got['qty']![six]], ['D', 'X', null]);
    assert.equal(got['id']!.length, 6);
  });

  it('a row of != pairs only rows that both have a value: an empty key matches no row (the audit\'s 2)', async () => {
    const how: JoinHow = { form: 'join', kind: 'inner', on: { rows: [{ left: 'cust', op: '!=', right: 'id' }] }, prefix: 'c_', keep: ['region'] };
    const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), how), opening(['trades', 'customers']));
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    // trades 1 and 2 (customer 10) with the other three customers' rows each, trade 3 (20) with two, trade 5 (99) with all four
    assert.equal(got['id']!.length, 12);
    assert.ok(!got['id']!.map(Number).includes(4), 'trade 4, of no customer, pairs with none');
    const inputs = await openInputs(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), opening(['trades', 'customers']));
    const p = plannerFor(inputs.model, inputs.runtime);
    planners.push(p);
    const counts = await summaryCounts(joinSummary(inputs.left, inputs.right, how), async (query) => engine.run((await p.plan(query)).sql, 0));
    assert.equal(counts.matched, 4, 'trade 4 not among those with a match');
  });

  it('append: a JSON column one side lacks, empty on its rows', async () => {
    const opened = await openJoin(join(input(TRADES, 'trades'), input(TAGGED, 'tagged'), { form: 'append' }), opening(['trades', 'tagged']));
    assert.deepEqual(opened.columns.map((c) => [c.name, c.type]).slice(-1), [['tags', 'Variant']]);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.equal(got['id']!.length, 6);
    assert.equal(got['tags']![got['id']!.map(Number).indexOf(1)], null);
    assert.notEqual(got['tags']![got['id']!.map(Number).indexOf(7)], null);
  });

  it('append: a precise decimal column one side lacks, empty on its rows, its value exact on the others', async () => {
    // `[]->cast(@Decimal(10,2))` in an extend types as Decimal, which concatenate unites with Decimal(10,2) since core's
    // 08113f082 (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.7: the typing gap this test pinned as refused, closed)
    const opened = await openJoin(join(input(TRADES, 'trades'), input(PRICED, 'priced'), { form: 'append' }), opening(['trades', 'priced']));
    assert.deepEqual(opened.columns.map((c) => c.name).slice(-1), ['px']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.equal(got['id']!.length, 6);
    assert.equal(got['px']![got['id']!.map(Number).indexOf(1)], null);
    assert.equal(Number(got['px']![got['id']!.map(Number).indexOf(7)]), 12.5);
  });

  it('every pair: a join on nothing; a table joined to itself is one table of the model', async () => {
    const how: JoinHow = { form: 'join', kind: 'inner', on: { rows: [] }, prefix: 'b_', keep: ['id'] };
    const opened = await openJoin(join(input(TRADES, 'trades'), input(TRADES, 'again'), how), opening(['trades', 'trades']));
    assert.equal(opened.model.match(/Table trades/g)?.length, 1);
    const inputs = await openInputs(input(TRADES, 'trades'), input(TRADES, 'again'), opening(['trades', 'trades']));
    assert.equal(await count(opened.model, opened.runtime, joinSummary(inputs.left, inputs.right, how).joined!), 25);
  });

  it('a join inside an input: (trades ⋈ customers) ⋈ prices, three tables of one model', async () => {
    const inner = join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), { ...LEFT_ON_CUST, keep: ['region'] });
    const innerQuery = query(file('x', [['id', 'Integer'], ['cust', 'Integer'], ['sym', 'String'], ['at', 'DateTime'], ['qty', 'Float'], ['region', 'String']]));
    const how: JoinHow = { form: 'join', kind: 'left', on: { rows: [{ left: 'sym', op: '==', right: 'sym' }] }, prefix: 'p_', keep: ['px'] };
    const outer = join(input(inner, 'trades ⋈ customers', innerQuery), input(PRICES, 'prices'), how);
    assert.deepEqual(leavesOf(outer).map((l) => l.name), ['trades.csv', 'customers.csv', 'prices.csv']);
    const opened = await openJoin(outer, opening(['trades', 'customers', 'prices']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'region', 'px']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    // trade 1 (A): two prices; 2 (B): one; 3 (A, two customers): four; 4 (C): none, kept; 5 (A): two
    assert.equal(got['id']!.length, 10);
  });

  it('says what drifted, and leaves out a kept column of theirs that is gone', async () => {
    // saved when customers had a 'tier' it keeps: gone now
    const saved = { ...input(CUSTOMERS, 'customers'), query: query(file('c', [['id', 'Integer'], ['region', 'String'], ['tier', 'String']])) };
    const opened = await openJoin(join(input(TRADES, 'trades'), saved, { ...LEFT_ON_CUST, keep: ['region', 'tier'] }), opening(['trades', 'customers']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'region']);
    assert.deepEqual(opened.notes, ['customers: 1 column no longer in the file: tier', "left out customers's tier (no longer in it)"]);
    assert.ok(opened.how.form === 'join' && opened.how.keep?.join() === 'region');
  });

  it('refuses in words what is not read here, a pivoted input, a key either input lacks', async () => {
    const warehouse: LeafSource = { _type: 'warehouseTable', name: 'sales.orders', warehouse: 'https://w', catalog: 'main', schema: 'sales', table: 'orders', columns: [] };
    assert.match(leafRefusal(warehouse, TAB_TABLES)!, /one place, this page's own database \(files, remote files\); sales\.orders is a warehouse table, read live/);
    await assert.rejects(openJoin(join(input(TRADES, 'trades'), input(warehouse, 'orders', query(CUSTOMERS)), LEFT_ON_CUST), opening(['trades', 'x'])),
      (e: unknown) => e instanceof CubeRefusal && /sales\.orders is a warehouse table/.test(e.message));
    const pivoted = query(CUSTOMERS, { pivotOn: ['region'] });
    await assert.rejects(openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers', pivoted), LEFT_ON_CUST), opening(['trades', 'customers'])),
      (e: unknown) => e instanceof CubeRefusal && /customers is pivoted/.test(e.message));
  });

  it('leaves to the compiler what it judges: a column an input lacks, a name taken twice, an append of two types', async () => {
    const refusedBy = async (source: JoinSource, tables: string[], words: RegExp): Promise<void> => {
      await assert.rejects(openJoin(source, opening(tables)), (e: unknown) => e instanceof Error && words.test(e.message), String(words));
    };
    const noKey: JoinHow = { ...LEFT_ON_CUST, on: { rows: [{ left: 'cust', op: '==', right: 'customer_id' }] } };
    await refusedBy(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), noKey), ['trades', 'customers'], /no column 'customer_id'/);
    // their id prefixed onto the c_id they have already: a name taken twice
    await refusedBy(join(input(TRADES, 'trades'), input(TWINS, 'twins'), { ...LEFT_ON_CUST, on: { rows: [] } }),
      ['trades', 'twins'], /already exists/);
    // labels' id is text, trades' a number: one name, two types
    await refusedBy(join(input(TRADES, 'trades'), input(LABELS, 'labels'), { form: 'append' }), ['trades', 'labels'], /cannot unite/);
  });

  it('a join source, saved and read back, opens the same', async () => {
    const source: JoinSource = { ...join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), LEFT_ON_CUST), columns: [{ name: 'id', type: 'Integer' }] };
    const doc = readCube(cubeToJson({
      kind: 'datacube.cube', version: 2, name: source.name, source, query: query(TRADES), configuration: {}, tree: { open: [], showTotals: false },
    }));
    assert.deepEqual(doc.source, source);
    const opened = await openJoin(doc.source as JoinSource, opening(['trades', 'customers']));
    assert.equal(opened.columns.length, 7);
  });
});
