// The Pure a page's model is made of (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §3.3, §4.2, §11's first proof): two
// classes, each over its own store and relation function, with its mapping (upstream's newest form, a class mapped to a
// relation function, `~func`), related by an Association that a ModelJoin maps, in a page mapping that includes both;
// the runtime binds both stores to the tab's one connection. Class queries walking the relationship -- a to-one end as a
// column and in a filter, a to-many end in a filter (`exists`), as a column (a row per trade) and as an aggregate -- are
// planned by legend-lite's own compiler (the WASM module the tab loads) and run on DuckDB, rows checked.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { classRelation } from '../src/class-source.ts';
import { classTree, type ClassElement } from '../src/class-tree.ts';
import { applyToSnapshot, DEFAULT_CONFIGURATION } from '../src/config.ts';
import { applyGridCommands } from '../src/grid-commands.ts';
import { modelMeasureCommands } from '../src/model-measure-commands.ts';
import { modelMeasures } from '../src/model-measures.ts';
import { levelLambda, sourceWithDerived } from '../src/query.ts';
import type { CubeSnapshot, QuerySnapshot } from '../src/snapshot.ts';
import { plannerFor } from './catalog-builder.ts';

const MODEL = `
###Relational
Database files::trades::Store ( Table trades ( tradeId INTEGER PRIMARY KEY, customerId INTEGER, notional DOUBLE ) )
Database files::customers::Store ( Table customers ( id INTEGER PRIMARY KEY, name VARCHAR(64), region VARCHAR(16) ) )

###Pure
function files::trades::read(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
{
  #>{files::trades::Store.trades}#
}

function files::customers::read(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
{
  #>{files::customers::Store.customers}#
}

Class files::trades::Trade
{
  tradeId: Integer[1];
  customerId: Integer[0..1];
  notional: Float[0..1];
}

Class files::customers::Customer
{
  id: Integer[1];
  name: String[0..1];
  region: String[0..1];
}

Association page::TradeCustomer
{
  customer: files::customers::Customer[0..1];
  trades: files::trades::Trade[*];
}

###Mapping
import meta::pure::metamodel::relation::*;
import meta::pure::metamodel::type::*;

Mapping files::trades::Mapping
(
  *files::trades::Trade[trade]: Relation
  {
    ~func files::trades::read():Relation<Any>[1]
    tradeId: tradeId,
    customerId: customerId,
    notional: notional
  }
)

Mapping files::customers::Mapping
(
  *files::customers::Customer[customer]: Relation
  {
    ~func files::customers::read():Relation<Any>[1]
    id: id,
    name: name,
    region: region
  }
)

Mapping page::Mapping
(
  include files::trades::Mapping
  include files::customers::Mapping

  page::TradeCustomer: ModelJoin
  {
    {customer: files::customers::Customer[1], trades: files::trades::Trade[1] | $customer.id == $trades.customerId}
  }
)

###Connection
RelationalDatabaseConnection page::Connection
{
  store: files::trades::Store;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}

###Runtime
Runtime page::Runtime
{
  mappings: [ page::Mapping ];
  connections:
  [
    files::trades::Store: [ trades: page::Connection ],
    files::customers::Store: [ customers: page::Connection ]
  ];
}
`;

const SEED = [
  'CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region VARCHAR)',
  // Gamma has no trades
  "INSERT INTO customers VALUES (1, 'Acme', 'EMEA'), (2, 'Beta', 'AMER'), (3, 'Gamma', 'EMEA')",
  'CREATE TABLE trades (tradeId INTEGER PRIMARY KEY, customerId INTEGER, notional DOUBLE)',
  // trade 13 has no customer
  'INSERT INTO trades VALUES (10, 1, 100.0), (11, 1, 250.0), (12, 2, 50.0), (13, NULL, 75.0)',
];

let engine: DuckDbEngine;
const planner = plannerFor(MODEL, 'page::Runtime', { mapping: 'page::Mapping' });

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  for (const sql of SEED) await engine.run(sql, 0);
});

after(async () => {
  await engine?.close();
});

/** The query's SQL, as the compiler plans it, and its rows, as DuckDB answers. */
async function run(query: string): Promise<{ sql: string; rows: unknown[][] }> {
  const plan = await planner.plan(await planner.parse(`|${query}`));
  const table = await engine.run(plan.sql, 0);
  return { sql: plan.sql, rows: Array.from({ length: table.rowCount }, (_, i) => table.columns.map((c) => c.values[i] ?? null)) };
}

describe('a page\'s model: two sources as classes over relation functions, related by a ModelJoin', () => {
  it('each class reads its own source, as its relation function does', async () => {
    const { rows } = await run('files::customers::Customer.all()->project(~[id: x|$x.id, name: x|$x.name])->sort(~id->ascending())');
    assert.deepEqual(rows, [[1, 'Acme'], [2, 'Beta'], [3, 'Gamma']]);
  });

  it('a to-one end as a column: each trade with its customer\'s region, none for a trade with no customer', async () => {
    const { rows, sql } = await run(
      'files::trades::Trade.all()->project(~[trade: x|$x.tradeId, region: x|$x.customer.region])->sort(~trade->ascending())');
    assert.deepEqual(rows, [[10, 'EMEA'], [11, 'EMEA'], [12, 'AMER'], [13, null]]);
    assert.match(sql, /LEFT (OUTER )?JOIN/i, 'a to-one end is a left join');
  });

  it('a to-one end in a filter: the trades of EMEA\'s customers', async () => {
    const { rows } = await run(
      "files::trades::Trade.all()->filter(t|$t.customer.region == 'EMEA')->project(~[trade: x|$x.tradeId])->sort(~trade->ascending())");
    assert.deepEqual(rows, [[10], [11]]);
  });

  it('a to-many end in a filter: the customers with a trade over 60, each once however many it has', async () => {
    // Acme's two trades both pass: Acme once (the compiler writes the semi-join as a join to the distinct keys)
    const { rows } = await run(
      'files::customers::Customer.all()->filter(c|$c.trades->exists(t|$t.notional > 60))->project(~[name: x|$x.name])');
    assert.deepEqual(rows, [['Acme']]);
  });

  it('a to-many end as a column: a row per trade, the customer\'s own values repeated, one for a customer with none', async () => {
    const { rows } = await run(
      'files::customers::Customer.all()->project(~[name: x|$x.name, notional: x|$x.trades.notional])'
      + '->sort([~name->ascending(), ~notional->ascending()])');
    assert.deepEqual(rows, [['Acme', 100], ['Acme', 250], ['Beta', 50], ['Gamma', null]]);
  });

  it('a to-many end as an aggregate: one row per customer', async () => {
    const { rows } = await run(
      'files::customers::Customer.all()->project(~[name: x|$x.name, notional: x|$x.trades.notional])'
      + '->groupBy(~name, ~[count: x|$x.notional: y|$y->count(), total: x|$x.notional: y|$y->sum()])->sort(~name->ascending())');
    assert.deepEqual(rows.map(([name, count, total]) => [name, Number(count), total]),
      [['Acme', 2, 350], ['Beta', 1, 50], ['Gamma', 0, null]]);
  });
});

describe("a class's grid with related columns (the class tree: query.ts relatedSource), planned and run", () => {
  /** A level's rows, each by its columns' names. */
  const level = async (s: QuerySnapshot, at: number): Promise<Record<string, unknown>[]> => {
    const t = await engine.run((await planner.plan(levelLambda(s, { level: at, parent: [] }))).sql, 0);
    return Array.from({ length: t.rowCount }, (_, i) => Object.fromEntries(t.columns.map((c) => [c.name, c.values[i] ?? null])));
  };

  it("each trade's customer's region beside it, and the trades grouped by it: a to-one end's join", async () => {
    const s: QuerySnapshot = {
      source: { query: classRelation('files::trades::Trade', ['tradeId', 'notional']) },
      columns: [{ name: 'tradeId', type: 'Integer', kind: 'dimension' }, { name: 'notional', type: 'Float', kind: 'measure' }],
      derived: [{ name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension' }],
      rows: ['customer_region'], pivotOn: [], sorts: [], epoch: 1,
    };
    const by = new Map((await level(s, 1)).map((r) => [r['customer_region'], Number(r['notional'])]));
    // EMEA: Acme's 100 and 250; AMER: Beta's 50; no customer: trade 13's 75
    assert.deepEqual([by.get('EMEA'), by.get('AMER'), by.get(null), by.size], [350, 50, 75, 3]);
  });

  it("each customer's trades counted and summed for it -- never a row per trade -- so a region's total of them is exact", async () => {
    const s: QuerySnapshot = {
      source: { query: classRelation('files::customers::Customer', ['id', 'region']) },
      columns: [{ name: 'id', type: 'Integer', kind: 'dimension' }, { name: 'region', type: 'String', kind: 'dimension' }],
      derived: [
        { name: 'trades_count', related: { path: ['trades'], aggregate: 'count' }, kind: 'measure' },
        { name: 'trades_notional_sum', related: { path: ['trades', 'notional'], aggregate: 'sum' }, kind: 'measure' },
      ],
      rows: ['region'], pivotOn: [], sorts: [], epoch: 1,
    };
    // typed by the compiler as every row-stage column is (step 0, plan.ts typeColumns): numbers, so each region sums them
    const types = new Map((await planner.relationType(sourceWithDerived(s).lambda())).map((c) => [c.name, c.type]));
    assert.deepEqual([types.get('trades_count'), types.get('trades_notional_sum')], ['Integer', 'Float']);
    const typed: QuerySnapshot = { ...s, derived: s.derived.map((d) => ({ ...d, type: types.get(d.name)! })) };
    const by = new Map((await level(typed, 1)).map((r) => [r['region'], [Number(r['trades_count']), Number(r['trades_notional_sum'])]]));
    // EMEA: Acme's two trades (350) and Gamma's none; AMER: Beta's one (50) -- trade 13, no customer's, in neither
    assert.deepEqual(by.get('EMEA'), [2, 350]);
    assert.deepEqual(by.get('AMER'), [1, 50]);
  });
});

// The model's own measures (src/model-measures.ts; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.4): functions of the
// trades stereotyped BI.measure, read from the elements as this compiler gives them, added to a grid of the trades as
// the steps a person would make, and run: each region's figures what the functions say of its trades.
const MEASURED = `${MODEL}
###Pure
Profile meta::legend::bi::BI
{
  stereotypes: [measure, dimension];
  tags: [format, goodDirection];
}

function <<meta::legend::bi::BI.measure>> {meta::legend::bi::BI.format = '$#,##0', meta::pure::profiles::doc.doc = 'Gross notional booked'}
page::grossNotional(t: files::trades::Trade[*]): Float[1]
{
  $t.notional->sum()
}

function <<meta::legend::bi::BI.measure>>
page::bigNotional(t: files::trades::Trade[*]): Float[1]
{
  $t->filter(x|$x.notional > 60).notional->sum()
}

function <<meta::legend::bi::BI.measure>> {meta::legend::bi::BI.format = '#,##0.00'}
page::averageNotional(t: files::trades::Trade[*]): Float[1]
{
  page::grossNotional($t) / $t->count()
}

function <<meta::legend::bi::BI.measure>>
page::regionsKnown(t: files::trades::Trade[*]): Integer[1]
{
  $t.customer.region->count()
}

function <<meta::legend::bi::BI.measure>>
page::bigTrades(t: files::trades::Trade[*]): Integer[1]
{
  $t->filter(x|$x.notional > 60)->count()
}
`;

describe("the model's measures added to a class's grid, planned and run", () => {
  const measured = plannerFor(MEASURED, 'page::Runtime', { mapping: 'page::Mapping' });

  it("each region of trades: its sum, its filtered sum, an average of two aggregates, a count through the customer, a filter's count", async () => {
    const elements = await measured.modelElements(MEASURED) as ClassElement[];
    const measures = modelMeasures(elements);
    assert.deepEqual(measures.map((m) => [m.path, m.of, m.format ?? null]), [
      ['page::grossNotional', 'files::trades::Trade', '$#,##0'],
      ['page::bigNotional', 'files::trades::Trade', null],
      ['page::averageNotional', 'files::trades::Trade', '#,##0.00'],
      ['page::regionsKnown', 'files::trades::Trade', null],
      ['page::bigTrades', 'files::trades::Trade', null],
    ]);
    assert.equal(measures[0]!.doc, 'Gross notional booked');
    const tree = classTree(elements);
    const s0: CubeSnapshot = {
      source: { query: classRelation('files::trades::Trade', ['tradeId', 'notional']) },
      columns: [{ name: 'tradeId', type: 'Integer', kind: 'dimension' }, { name: 'notional', type: 'Float', kind: 'measure' }],
      derived: [{ name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension' }],
      rows: ['customer_region'], pivotOn: [], sorts: [], epoch: 1,
    };
    let draft = { snapshot: s0, config: DEFAULT_CONFIGURATION };
    for (const m of measures) draft = applyGridCommands(draft, modelMeasureCommands(draft.snapshot, tree, 'files::trades::Trade', measures, m));
    // the related column the count reads is the grid's own already: read, not added again
    assert.equal(draft.snapshot.derived.filter((d) => d.related !== undefined).length, 1);
    const folded = applyToSnapshot(draft.snapshot, draft.config);
    // typed by the compiler as every row-stage column is (plan.ts typeColumns)
    const types = new Map((await measured.relationType(sourceWithDerived(folded).lambda())).map((c) => [c.name, c.type]));
    const s: CubeSnapshot = { ...folded, derived: folded.derived.map((d) => ({ ...d, type: types.get(d.name)! })) };
    const t = await engine.run((await measured.plan(levelLambda(s, { level: 1, parent: [] }))).sql, 0);
    const rows = Array.from({ length: t.rowCount }, (_, i) => Object.fromEntries(t.columns.map((c) => [c.name, c.values[i] ?? null])));
    const figures = (r: Record<string, unknown>): (number | null)[] =>
      ['grossNotional', 'bigNotional', 'averageNotional', 'regionsKnown', 'bigTrades'].map((n) => (r[n] === null ? null : Number(r[n])));
    const by = new Map(rows.map((r) => [r['customer_region'], figures(r)]));
    // EMEA: Acme's 100 and 250, both over 60; AMER: Beta's 50, none over 60; no customer: trade 13's 75, its region none
    // to count (Pure's count of values, never the grid's of rows)
    assert.deepEqual(by.get('EMEA'), [350, 350, 175, 2, 2]);
    assert.deepEqual(by.get('AMER'), [50, null, 50, 1, 0]);
    assert.deepEqual(by.get(null), [75, 75, 75, 0, 1]);
    // the average's two aggregates its machinery, hidden; the measures' formats as their tags say
    assert.deepEqual(['averageNotional__1', 'averageNotional__2'].map((n) => draft.config.columns?.[n]?.hidden), [true, true]);
    assert.equal(draft.config.columns?.['grossNotional']?.format?.kind, 'currency');
  });
});

// A column's values gathered by hand into named groups (snapshot.ts ValueGroups; query.ts groupsLambda): planned and run,
// a grid grouped by the groups as by any column -- a number column's rest named, a text column's left as itself.
describe("a column's values gathered into named groups, planned and run", () => {
  const level = async (s: QuerySnapshot): Promise<Map<unknown, number>> => {
    const t = await engine.run((await planner.plan(levelLambda(s, { level: 1, parent: [] }))).sql, 0);
    const at = (name: string): readonly unknown[] => t.columns.find((c) => c.name === name)!.values;
    const keys = at(s.rows[0]!);
    return new Map(keys.map((k, i) => [k ?? null, Number(at('notional')[i])]));
  };
  const trades = (derived: QuerySnapshot['derived'], rows: string[]): QuerySnapshot => ({
    source: { query: classRelation('files::trades::Trade', ['tradeId', 'notional']) },
    columns: [{ name: 'tradeId', type: 'Integer', kind: 'dimension' }, { name: 'notional', type: 'Float', kind: 'measure' }],
    derived, rows, pivotOn: [], sorts: [], epoch: 1,
  });

  it("a number column's values: the big trades a group, the rest under its name", async () => {
    const by = await level(trades([{ name: 'size', groups: { of: 'tradeId', groups: [{ name: 'Big', values: [10, 11] }], other: 'Rest' }, kind: 'dimension', type: 'String' }], ['size']));
    // trades 10 and 11: 100 + 250; 12 and 13: 50 + 75
    assert.deepEqual([by.get('Big'), by.get('Rest'), by.size], [350, 125, 2]);
  });

  it("a related column's text: EMEA named Europe, AMER as itself, a trade with no customer blank", async () => {
    const by = await level(trades([
      { name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension', type: 'String' },
      { name: 'zone', groups: { of: 'customer_region', groups: [{ name: 'Europe', values: ['EMEA'] }] }, kind: 'dimension', type: 'String' },
    ], ['zone']));
    assert.deepEqual([by.get('Europe'), by.get('AMER'), by.get(null), by.size], [350, 50, 75, 3]);
  });
});
