// A join opened over one model and run (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3, §17.4; src/join-open.ts): two
// tables of a DuckDB, the writer's model of both, each input a grid's result over its table (its rows, or its groups),
// every form planned by the tab's own module and its rows read back -- an empty key matching nothing, a grouped input
// joined, as-of, semi and anti, an append's missing column, every pair, a join inside an input -- the dialog's questions
// answered, and what cannot be joined here refused in words.

import assert from 'node:assert/strict';
import { after, afterEach, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { typedByPlan } from '../../engine-client/src/engine.ts';
import { joinRelation, joinRepeats, joinSummary, readJoinHow, summaryWords, type JoinHow } from '../src/join.ts';
import { TAB_TABLES, eachRelation, leafRefusal, openInputs, openJoin, openRows, openedLeaves, summaryCounts, type JoinOpening } from '../src/join-open.ts';
import {
  leavesOf, rawRows, readCube, cubeToJson, writeCube, type CubeSource, type FileSource, type JoinInput, type JoinSource, type LeafSource,
  type RowsSource, type SavedQuery,
} from '../src/cube-document.ts';
import { TOTAL_ROWS_COLUMN } from '../src/query.ts';
import { topValuesLambda, topValuesOf } from '../src/profile.ts';
import { catalogColumns } from '../src/upload.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';
import { plannerFor } from './catalog-builder.ts';
import { outputCsv, type OutputQueries } from '../src/flow-output.ts';
import { countRecordsCommands } from '../src/count-records.ts';
import { applyGridCommands, type GridCommand } from '../src/grid-commands.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { openedState } from '../src/cube-state.ts';
import { TreeState } from '../src/tree.ts';
import { boxOmitted } from '../src/box-columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { lambda, lit, type Lambda } from '../../pure-protocol/src/index.ts';
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

  // each test's planners let go as it ends: a planner is a WebAssembly instance, and kept to the file's end they filled
  // the test's heap (2026-10-26, two tests added)
  afterEach(() => {
    for (const p of planners.splice(0)) p.dispose();
  });

  after(async () => {
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

  it("each of mine once (a join's once, C13): a key theirs repeats matches its first row -- the same every time -- and no row of mine repeats", async () => {
    const once: JoinHow = { ...LEFT_ON_CUST, once: true };
    const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), once), opening(['trades', 'customers']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'c_id', 'region']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    // customer 20 twice (AMER, APAC): trade 3 once, with the first of its rows in an order of their other columns, rising
    const pairs = got['id']!.map((id, i) => `${String(id)}:${String(got['region']![i] ?? '-')}`).sort();
    assert.deepEqual(pairs, ['1:EMEA', '2:EMEA', '3:AMER', '4:-', '5:-']);
    // the dialog's questions of it: as many rows after it as mine, its repeated key said matched once
    const inputs = await openInputs(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), opening(['trades', 'customers']));
    const p = plannerFor(inputs.model, inputs.runtime);
    planners.push(p);
    const counts = await summaryCounts(joinSummary(inputs.left, inputs.right, once), async (query) => engine.run((await p.plan(query)).sql, 0));
    assert.deepEqual(counts, { mine: 5, matched: 3, joined: 5, repeated: { keys: 1, most: 2 } });
    assert.match(summaryWords(once, counts).join(' '), /each row of mine with it takes the first of theirs, once/);
    assert.equal(joinRepeats(once, counts), false);
    // once is a key's: rows of equal keys alone, refused in words for any other condition
    assert.throws(() => joinRelation(inputs.left, inputs.right, { ...once, on: { rows: [{ left: 'cust', op: '<', right: 'id' }] } }), /takes a key/);
    assert.throws(() => readJoinHow({ ...once, on: { rows: [] } }), /takes a key/);
    assert.throws(() => readJoinHow({ ...LEFT_ON_CUST, once: false }), /once is true or left out/);
    // a score (a Fuzzy Match's) is of a similar row's pairs, matching rows only
    const similar = { form: 'join', kind: 'inner', on: { rows: [{ left: 'cust', op: '~', right: 'id' }] }, prefix: 'c_', keep: null };
    assert.deepEqual(readJoinHow({ ...similar, score: 'score' }), { ...similar, score: 'score' });
    assert.throws(() => readJoinHow({ ...similar, kind: 'left', score: 'score' }), /a score is each matched pair's: it takes matching rows only/);
    assert.throws(() => readJoinHow({ ...LEFT_ON_CUST, kind: 'inner', score: 'score' }), /a score is a Fuzzy Match's/);
    assert.throws(() => readJoinHow({ ...similar, score: ' ' }), /a join's score is the name of its column/);
    assert.deepEqual(readJoinHow(JSON.parse(JSON.stringify(once))), once);
  });

  it("a Profile's column opened: its most frequent values counted by the database, the most first, of as many the smallest, an empty last", async () => {
    const opened = await openJoin(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), LEFT_ON_CUST), opening(['trades', 'customers']));
    const p = plannerFor(opened.model, opened.runtime);
    planners.push(p);
    const asked = async (n?: number) => topValuesOf(await engine.run((await p.plan(topValuesLambda(opened.relation, 'region', n))).sql, 0) as never, 'region')
      .map((t) => ({ ...t, count: Number(t.count) }));
    // trades 1 and 2 EMEA, trade 3 AMER and APAC (customer 20 twice), trades 4 and 5 none: 2, 2 (empty, after), 1, 1
    assert.deepEqual(await asked(), [{ value: 'EMEA', count: 2 }, { value: null, count: 2 }, { value: 'AMER', count: 1 }, { value: 'APAC', count: 1 }]);
    assert.deepEqual((await asked(2)).map((t) => t.value), ['EMEA', null]);
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

  it("another grid's rows: a chain of boxes over a table, each its input's rows at their finest level (flow mode §3.1)", async () => {
    // a Filter box over trades (qty over 2), then a Summarize box after it grouped by sym
    const big = query(TRADES, { filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 2 } });
    const first: RowsSource = { _type: 'rows', name: 'trades', input: input(TRADES, 'trades', big), columns: [] };
    const opened = await openRows(first, opening(['trades']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty'], "its input's columns");
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual([...got['id']!].sort(), [1, 2, 5], 'the rows its input keeps');
    const bySym = query(TRADES, { rows: ['sym'], columns: [{ name: 'qty', type: 'Float', aggregate: 'sum' }, { name: 'sym', type: 'String' }] });
    const second: RowsSource = { _type: 'rows', name: 'trades', columns: [], input: { name: 'Big', source: first, query: bySym } };
    const after = await openRows(second, opening(['trades']));
    // a grouped input's rows: its groups, its key first and its measure beside it (its other columns, new to this query,
    // come as their Aggregation takes them)
    assert.equal(after.columns[0]!.name, 'sym', "a grouped input's rows: its key first");
    assert.ok(after.columns.some((c) => c.name === 'qty'), 'and its measure');
    const groups = await rows(after.model, after.runtime, { _type: 'lambda', parameters: [], body: [after.relation] });
    const pairs = groups['sym']!.map((sym, i) => `${String(sym)}:${String(groups['qty']![i])}`).sort();
    assert.deepEqual(pairs, ['A:8', 'B:7'], 'the second over the first: A 5 + 3, B 7');
    // its leaves are the chain's: one table, read once
    assert.deepEqual(leavesOf(second), [TRADES]);
  });

  it("an input read by its grid's copy (a cached box, FL10): the copy's table, its query over it -- a box over it, a join of it with a file", async () => {
    // the copy: trades 1 and 2 only, so what reads it is told apart from what reads trades live
    await engine.run('CREATE TABLE trades_copy AS SELECT * FROM trades WHERE id <= 2', 0);
    const copy = { table: 'trades_copy', label: 'Snap 14:02', takenAt: '2026-10-26T14:02:00.000Z' };
    const big = query(TRADES, { filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 2 } });
    const box: RowsSource = { _type: 'rows', name: 'trades', input: { ...input(TRADES, 'Big', big), copy }, columns: [] };
    assert.deepEqual(openedLeaves(box), [{ _type: 'copy', ...copy }], 'one leaf: the copy, not trades');
    const opened = await openRows(box, opening(['trades_copy']));
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual([...got['id']!].sort(), [1, 2], "the copy's rows its query keeps (trades 1 and 2: qty 5 and 7)");
    // a join of the copy with a file: both tables of this database, one model
    const joined = await openJoin(join({ ...input(TRADES, 'Big', big), copy }, input(CUSTOMERS, 'customers'), LEFT_ON_CUST), opening(['trades_copy', 'customers']));
    const pairs = await rows(joined.model, joined.runtime, { _type: 'lambda', parameters: [], body: [joined.relation] });
    assert.deepEqual(pairs['id']!.map((id, i) => `${String(id)}:${String(pairs['region']![i])}`).sort(), ['1:EMEA', '2:EMEA']);
    await engine.run('DROP TABLE trades_copy', 0);
  });

  it("another grid's rows unresolved is refused, never guessed: its link is its page's", async () => {
    const live: RowsSource = { _type: 'rows', name: 'trades', input: { grid: 'grid-1' }, columns: [] };
    await assert.rejects(openRows(live, opening([])), /resolve it against the page first/);
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

  it("an input's rows in its box's order (JoinInput.order: a Select's), those it does not list after them", async () => {
    const ordered: RowsSource = { _type: 'rows', name: 'trades', input: { ...input(TRADES, 'trades'), order: ['qty', 'sym'] }, columns: [] };
    const opened = await openRows(ordered, opening(['trades']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['qty', 'sym', 'id', 'cust', 'at']);
  });

  it("a Sort box's order reaches the box after it, every row of it (query.ts handedOnLambda; the review of 2026-10-26's C1)", async () => {
    const sorted = query(TRADES, { sorts: [{ column: 'qty', direction: 'desc' }] });
    const box: RowsSource = { _type: 'rows', name: 'trades', input: input(TRADES, 'trades', sorted), columns: [] };
    const opened = await openRows(box, opening(['trades']));
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual(got['id'], [2, 1, 5, 4, 3], 'qty 7, 5, 3, 2, 1');
  });

  it("a Select's swap, a as b and b as a, handed on through names of its own (join-open.ts handedOnRows; the review's C11)", async () => {
    const swapped: RowsSource = { _type: 'rows', name: 'trades', columns: [], input: { ...input(TRADES, 'trades'),
      omit: ['at', 'qty'], rename: [{ from: 'cust', to: 'sym' }, { from: 'sym', to: 'cust' }] } };
    const opened = await openRows(swapped, opening(['trades']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'sym', 'cust']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual(Object.keys(got), ['id', 'sym', 'cust'], 'no name of its own left');
    const first = got['id']!.indexOf(1);
    assert.deepEqual([got['sym']![first], got['cust']![first]], [10, 'A'], "trade 1's customer as sym, its symbol as cust");
  });

  it("an Output's file: every row in its sorts' order, the columns it hands on under their names, each value as it is (flow-output.ts; the review's B2, C9)", async () => {
    const opened = await openRows({ _type: 'rows', name: 'trades', input: input(TRADES, 'trades'), columns: [] }, opening(['trades']));
    const p = plannerFor(opened.model, opened.runtime);
    planners.push(p);
    const run = async (q: Lambda): Promise<ResultTable> => {
      const plan = await p.plan(q);
      return typedByPlan(await engine.run(plan.sql, 0), plan);
    };
    const queries: OutputQueries = {
      runQuery: async (q) => ({ rows: await run(q), sql: '' }),
      streamQuery: async (q, _s, onChunk) => onChunk(await run(q)),
    };
    const s: CubeSnapshot = { ...rawRows(opened.relation, opened.columns.map((c) => ({ ...c }))), sorts: [{ column: 'qty', direction: 'desc' }],
      filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 2 } };
    // its customer left out, its symbol written as `symbol`, its quantity before the rest (a Select's order)
    const configuration = { columns: { cust: { hidden: true }, sym: { displayName: 'symbol' }, at: { hidden: true } }, columnOrder: ['qty'] };
    const csv = await outputCsv(s, configuration, queries, false);
    assert.equal(csv, 'qty,id,symbol\r\n7,2,B\r\n5,1,A\r\n3,5,A\r\n');
    // none: its head row alone, the BOM a spreadsheet reads first
    const none = await outputCsv({ ...s, filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 99 } }, configuration, queries);
    assert.equal(none, '\ufeffqty,id,symbol\r\n');
  });

  it("a Top N box's rank is none of what it hands on: the box after it reads its rows' columns alone (the review of 2026-10-26)", async () => {
    const top: RowsSource = { _type: 'rows', name: 'trades', columns: [], input: input(TRADES, 'trades', query(TRADES, { topN: { by: 'qty', n: 2 } })) };
    const opened = await openRows(top, opening(['trades']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual([...got['id']!].sort(), [1, 2], 'qty 5 and 7');
  });

  it("Count Records of no rows: one row, 0 -- a grouping by one value is every row aggregated (query.ts; the review's C4)", async () => {
    const box = (empty: boolean): RowsSource => {
      const s = rawRows({ _type: 'string', value: '' }, TRADES.columns.map((c) => ({ ...c })));
      const none: GridCommand = { kind: 'refilter', remove: [], add: [{ kind: 'condition', column: 'qty', operator: 'greaterThan', value: 99 }] };
      const draft = applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, [...(empty ? [none] : []), ...countRecordsCommands(s)]);
      const state = openedState(draft.snapshot, draft.config, TreeState.fromPaths([], false));
      const doc = writeCube({ name: 'box', source: TRADES, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
      return { _type: 'rows', name: 'counted', columns: [],
        input: { name: 'count', source: TRADES, query: doc.query, omit: boxOmitted(doc.query, doc.configuration) } };
    };
    for (const [empty, n] of [[true, 0], [false, 5]] as const) {
      const opened = await openRows(box(empty), opening(['trades']));
      const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
      assert.deepEqual(got, { Count: [n] }, empty ? 'no rows: one row, 0' : 'five rows');
    }
  });

  it("a Join's L and R (cube-document.ts JoinInput.unmatched): each side's rows the other's pair with none", async () => {
    const on = { rows: [{ left: 'cust', op: '==' as const, right: 'id' }] };
    const unmatched = (mine: JoinInput, by: JoinInput, condition: typeof on): RowsSource =>
      ({ _type: 'rows', name: mine.name, input: { ...mine, unmatched: [{ by, on: condition }] }, columns: [] });
    // its Left: trade 4 has no key, trade 5 no customer
    const l = unmatched(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), on);
    const left = await openRows(l, opening(['trades', 'customers']));
    assert.deepEqual(left.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty'], 'its own columns');
    const ls = await rows(left.model, left.runtime, { _type: 'lambda', parameters: [], body: [left.relation] });
    assert.deepEqual(ls['id']!.map(Number).sort(), [4, 5]);
    assert.deepEqual(leavesOf(l), [TRADES, CUSTOMERS], 'both tables, its own first');
    // its Right: the condition turned round -- customer 30 has no trade
    const r = unmatched(input(CUSTOMERS, 'customers'), input(TRADES, 'trades'), { rows: [{ left: 'id', op: '==', right: 'cust' }] });
    const right = await openRows(r, opening(['customers', 'trades']));
    const rs = await rows(right.model, right.runtime, { _type: 'lambda', parameters: [], body: [right.relation] });
    assert.deepEqual(rs['id']!.map(Number), [30]);
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

  it('append of more than two (a Union of any number, JoinSource.more): each in turn, columns by name, a table read twice once', async () => {
    const three = { ...join(input(TRADES, 'trades'), input(MORE, 'more'), { form: 'append' }), more: [input(TAGGED, 'tagged'), input(TRADES, 'again')] };
    const opened = await openJoin(three, opening(['trades', 'more', 'tagged', 'trades']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'venue', 'tags']);
    assert.deepEqual(opened.more?.map((s) => s.name), ['tagged', 'again']);
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual(got['id']!.map(Number).sort((a, b) => a - b), [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7]);
    const at = (id: number): number => got['id']!.map(Number).indexOf(id);
    assert.deepEqual([got['venue']![at(6)], got['tags']![at(1)], got['venue']![at(7)]], ['X', null, null]);
    assert.notEqual(got['tags']![at(7)], null);
    // a join of two takes no more
    await assert.rejects(openJoin({ ...three, how: LEFT_ON_CUST }, opening(['trades', 'more', 'tagged', 'trades'])), /joins two inputs, not 4/);
  });

  it('append picking one (a Dynamic Input): each input filtered by its name, the database keeping the one picked', async () => {
    // its pick as its parameter's value is bound into it where the source is resolved (page-parameters.ts boundIn)
    const picking = (name: string): JoinSource => ({ ...join(input(TRADES, 'trades'), input(MORE, 'more'), { form: 'append', pick: lambda([], lit.string(name)) }),
      more: [input(TAGGED, 'tagged')] });
    const opened = await openJoin(picking('more'), opening(['trades', 'more', 'tagged']));
    assert.deepEqual(opened.columns.map((c) => c.name), ['id', 'cust', 'sym', 'at', 'qty', 'venue', 'tags'], 'its columns all its inputs\'');
    const got = await rows(opened.model, opened.runtime, { _type: 'lambda', parameters: [], body: [opened.relation] });
    assert.deepEqual(got['id']!.map(Number), [6], "more's rows alone");
    const tagged = await openJoin(picking('tagged'), opening(['trades', 'more', 'tagged']));
    const t = await rows(tagged.model, tagged.runtime, { _type: 'lambda', parameters: [], body: [tagged.relation] });
    assert.deepEqual(t['id']!.map(Number), [7]);
    await assert.rejects(openJoin(picking('nope'), opening(['trades', 'more', 'tagged'])), /it picks "nope", none of its inputs: trades, more, tagged/);
    await assert.rejects(openJoin(join(input(TRADES, 'same'), input(MORE, 'same'), { form: 'append', pick: lambda([], lit.string('same')) }),
      opening(['trades', 'more'])), /two of its inputs are named same/);
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

  it('leaves to the compiler what it judges: a column an input lacks, a name taken twice; an append of two types refused', async () => {
    const refusedBy = async (source: JoinSource, tables: string[], words: RegExp): Promise<void> => {
      await assert.rejects(openJoin(source, opening(tables)), (e: unknown) => e instanceof Error && words.test(e.message), String(words));
    };
    const noKey: JoinHow = { ...LEFT_ON_CUST, on: { rows: [{ left: 'cust', op: '==', right: 'customer_id' }] } };
    await refusedBy(join(input(TRADES, 'trades'), input(CUSTOMERS, 'customers'), noKey), ['trades', 'customers'], /no column 'customer_id'/);
    // their id prefixed onto the c_id they have already: a name taken twice
    await refusedBy(join(input(TRADES, 'trades'), input(TWINS, 'twins'), { ...LEFT_ON_CUST, on: { rows: [] } }),
      ['trades', 'twins'], /already exists/);
    // labels' id is text, trades' a number: one name, two types -- the append's own refusal (join.ts appended), as the
    // compiler would unite an Integer and a Float
    await refusedBy(join(input(TRADES, 'trades'), input(LABELS, 'labels'), { form: 'append' }), ['trades', 'labels'],
      /'id' is Integer in trades and String in labels/);
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

// the timeseries review's G7 (docs/TIMESERIES_REVIEW_2026_10_25.md): a combination read each grid's relation by its
// place, unchecked -- one short, a grid read with no relation
describe("each source's relation, by its place: one each, checked", () => {
  it('each item paired with its relation in order; a count that differs refused in words', () => {
    const relations = [{ _type: 'var', name: 'a' }, { _type: 'var', name: 'b' }] as unknown as Parameters<typeof eachRelation>[0]['relations'];
    assert.deepEqual(eachRelation({ relations }, ['x', 'y']), [['x', relations[0]], ['y', relations[1]]]);
    assert.throws(() => eachRelation({ relations }, ['x', 'y', 'z']), /the model opened 2 relations for 3 sources/);
  });
});
