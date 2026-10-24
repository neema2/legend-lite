// AGGREGATES OF TWO COLUMNS AND THE COUNTS, planned and run (snapshot.ts SECOND_COLUMN): a correlation, the covariances,
// max by and min by -- Column Properties' Aggregation with its second column -- and a column's counts as a calculated
// column and its Aggregation make them (the user, 2026-10-24: no shortcut of their own), a text column's distinct count
// among them; each group's figure as DuckDB answers the query the grid sends, its grand total too.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { accessor, col, fn, lambda, lit } from '../../pure-protocol/src/index.ts';
import { applyToSnapshot, DEFAULT_CONFIGURATION } from '../src/config.ts';
import { applyGridCommands, type GridCommand } from '../src/grid-commands.ts';
import { levelLambda, sourceWithDerived } from '../src/query.ts';
import type { CubeSnapshot, DerivedColumn } from '../src/snapshot.ts';
import { plannerFor } from './catalog-builder.ts';

const MODEL = `
###Relational
Database t::Store ( Table trades ( id INTEGER PRIMARY KEY, desk VARCHAR(16), customer VARCHAR(16), qty DOUBLE, px DOUBLE ) )
###Connection
RelationalDatabaseConnection t::Connection { store: t::Store; type: DuckDB; specification: DuckDB { }; auth: Test; }
###Runtime
Runtime t::Runtime { mappings: []; connections: [ t::Store: [ c: t::Connection ] ]; }
`;

// desk A: qty 1, 2, 3 against px 2, 4, 7, customers c1, c1, c2; desk B: qty 5, 6 against px 1, 3, customers c3 and none
const SEED = [
  'CREATE TABLE trades (id INTEGER PRIMARY KEY, desk VARCHAR, customer VARCHAR, qty DOUBLE, px DOUBLE)',
  "INSERT INTO trades VALUES (1, 'A', 'c1', 1, 2), (2, 'A', 'c1', 2, 4), (3, 'A', 'c2', 3, 7), (4, 'B', 'c3', 5, 1), (5, 'B', NULL, 6, 3)",
];

let engine: DuckDbEngine;
const planner = plannerFor(MODEL, 't::Runtime');

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  for (const sql of SEED) await engine.run(sql, 0);
});

after(async () => {
  await engine?.close();
});

const START: CubeSnapshot = {
  source: { query: accessor('t::Store', 'trades') },
  columns: [
    { name: 'id', type: 'Integer', kind: 'dimension' },
    { name: 'desk', type: 'String' },
    { name: 'customer', type: 'String' },
    { name: 'qty', type: 'Float' },
    { name: 'px', type: 'Float' },
  ],
  derived: [], rows: ['desk'], pivotOn: [], sorts: [], epoch: 1,
};

/** The grid after `commands`, its calculated columns typed by the compiler (plan.ts typeColumns), each desk's row --
 *  and the grand total's, under '' -- as DuckDB answers the level's query. */
async function figures(commands: readonly GridCommand[], names: readonly string[]): Promise<Map<string, (number | string | null)[]>> {
  const draft = applyGridCommands({ snapshot: START, config: DEFAULT_CONFIGURATION }, commands);
  const folded = applyToSnapshot(draft.snapshot, draft.config);
  const types = new Map((await planner.relationType(sourceWithDerived(folded).lambda())).map((c) => [c.name, c.type]));
  const s: CubeSnapshot = { ...folded, derived: folded.derived.map((d) => ({ ...d, type: types.get(d.name)! })) };
  const out = new Map<string, (number | string | null)[]>();
  for (const level of [0, 1]) {
    const t = await engine.run((await planner.plan(levelLambda(s, { level, parent: [] }))).sql, 0);
    const at = (name: string): readonly unknown[] => t.columns.find((c) => c.name === name)!.values;
    for (let i = 0; i < t.rowCount; i++) {
      out.set(level === 0 ? '' : String(at('desk')[i]), names.map((n) => {
        const v = at(n)[i];
        return v === null || v === undefined ? null : typeof v === 'string' ? v : Number(v);
      }));
    }
  }
  return out;
}

const round = (v: number | string | null): number | string | null => (typeof v === 'number' ? Math.round(v * 1e4) / 1e4 : v);

describe('aggregates of two columns, planned and run', () => {
  it("a correlation and the covariances of qty and px, each desk's and the whole grid's", async () => {
    const by = await figures([
      { kind: 'column', name: 'qty', change: { aggregate: 'corr', weight: 'px' } },
      { kind: 'column', name: 'px', change: { aggregate: 'covarSample', weight: 'qty' } },
    ], ['qty', 'px']);
    // A: deviations -1, 0, 1 against -7/3, -1/3, 8/3 -- a co-moment of 5 over Sxx 2 and Syy 38/3; B: two points, a line
    assert.deepEqual(by.get('A')!.map(round), [round(5 / Math.sqrt(2 * 38 / 3)), 2.5]);
    assert.deepEqual(by.get('B')!.map(round), [1, 1]);
    assert.equal(by.size, 3, 'two desks and the grand total');
  });

  it('max by and min by: the value on the row whose second column is greatest, or least', async () => {
    const by = await figures([
      { kind: 'column', name: 'qty', change: { aggregate: 'maxBy', weight: 'px' } },
      { kind: 'column', name: 'customer', change: { kind: 'measure' } },
      { kind: 'column', name: 'customer', change: { aggregate: 'minBy', weight: 'px' } },
    ], ['qty', 'customer']);
    // A: the most px (7) was qty 3, the least (2) customer c1; B: the most (3) qty 6, the least (1) c3; all: 3 and c3
    assert.deepEqual(by.get('A'), [3, 'c1']);
    assert.deepEqual(by.get('B'), [6, 'c3']);
    assert.deepEqual(by.get(''), [3, 'c3']);
  });
});

describe("a column's counts, as a calculated column and its Aggregation make them, planned and run", () => {
  /** A Leaf Level Measure: `x|<body>`, summed when the grid groups (a number's default) unless its Aggregation says. */
  const measure = (name: string, body: Parameters<typeof lambda>[1]): GridCommand =>
    ({ kind: 'calculate', level: 'row', column: { name, kind: 'measure', lambda: lambda(['x'], body) } satisfies DerivedColumn });
  /** 1 on a row with a customer and 0 on one without -- or the other way, counting blanks: COUNTIF. */
  const has = (blank: boolean) => fn('if', fn('isEmpty', col('x', 'customer')),
    lambda([], lit.integer(blank ? 1 : 0)), lambda([], lit.integer(blank ? 0 : 1)));

  it("each desk's customers: rows with one, different ones, rows with none -- a text column's, in number columns", async () => {
    const by = await figures([
      measure('withCustomer', has(false)),
      measure('blankCustomer', has(true)),
      // a number column of its own: 1 or 0 on a row, at a group the counted column's different values
      measure('customers', has(false)),
      { kind: 'column', name: 'customers', change: { aggregate: 'distinctCount', weight: 'customer' } },
    ], ['withCustomer', 'customers', 'blankCustomer']);
    // A: c1, c1, c2 -- three with one, two different; B: c3 and a blank; the whole grid: c1, c2, c3
    assert.deepEqual(by.get('A'), [3, 2, 0]);
    assert.deepEqual(by.get('B'), [1, 1, 1]);
    assert.deepEqual(by.get(''), [4, 3, 1]);
  });
});
