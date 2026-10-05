// PIVOTS, JUDGED BY ROWS (docs/DATACUBE_CUBE_PLAN_DESIGN_2026_09_27.md).
//
// The real app (jsdom) over the real WASM planner and DuckDB-WASM. Every
// pivot cell is compared with a truth query that does not pivot:
// `GROUP BY region, year` over the same rows, run on the same database. A
// pivot cell is its measure over exactly the rows of its value, so the two
// must agree for every aggregate -- average, count and median included.
//
// Columns are found by their HEADER PATH (value, then measure), never by the
// generated name, so this judges any way of producing them.

import assert from 'node:assert/strict';
import { engineClientRequire } from '../../../engine-client/src/node-require.ts';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../../src/config.ts';
import { DuckDbEngine, type ArrowishConnection } from '../../../engine-client/src/duckdb.ts';
import type { QueryEngine, RawTable } from '../../../engine-client/src/engine.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import type { CubeView } from '../../src/cube.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { MODEL, RUNTIME } from '../wasm-differential/cases.ts';
import { accessor } from '../../../pure-protocol/src/index.ts';
import { runfileDirUrl } from '../../../tools/js/runfiles.mts';

const MODULE_DIR = runfileDirUrl('WASM_PLANNER');

// Several desks per (region, year), so a pivot over a finer intermediate
// would show. A NULL year (its own column) and a NULL region (its own group).
// APAC has no 2021 rows: filtering to APAC removes a pivot value.
const ROWS = `
  ('EMEA','Rates','B1',2021,'Q1',10,1,1),  ('EMEA','Rates','B2',2021,'Q2',20,2,2),
  ('EMEA','FX','B3',2021,'Q1',30,3,3),     ('EMEA','FX','B4',2022,'Q3',100,4,4),
  ('EMEA','Credit','B5',NULL,'Q4',7,1,5),  ('AMER','Rates','B6',2021,'Q1',40,5,6),
  ('AMER','Rates','B7',2022,'Q2',50,6,NULL),('AMER','FX','B8',2022,'Q2',60,7,8),
  ('APAC','Rates','B9',2022,'Q1',70,8,9),  ('APAC','FX','B10',2022,'Q3',80,9,10),
  (NULL,'Rates','B11',2021,'Q4',5,1,11)`;

const COLUMNS: CubeSnapshot['columns'] = [
  { name: 'region', type: 'String', kind: 'dimension' },
  { name: 'desk', type: 'String', kind: 'dimension' },
  { name: 'book', type: 'String', kind: 'dimension' },
  { name: 'year', type: 'Integer', kind: 'dimension' },
  { name: 'qtr', type: 'String', kind: 'dimension' },
  { name: 'notional', type: 'Float', kind: 'measure' },
  { name: 'pnl', type: 'Float', kind: 'measure' },
  { name: 'qty', type: 'Integer', kind: 'measure' },
];

/** One measure per column: average, median, count and sum. */
const MEASURES: CubeSnapshot['measures'] = [
  { name: 'avg_n', column: 'notional', fn: 'average' },
  { name: 'med_p', column: 'pnl', fn: 'median' },
  { name: 'cnt_q', column: 'qty', fn: 'count' },
];
const TRUTH_SQL: Record<string, string> = {
  avg_n: 'AVG(notional)',
  med_p: 'MEDIAN(pnl)',
  cnt_q: 'COUNT(*)',
};

const CUBE: CubeSnapshot = {
  source: { query: accessor('trades::DB', 'TRADES') },
  columns: COLUMNS,
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: ['year'],
  measures: MEASURES,
  sorts: [],
  pivotTotal: { placement: 'right' },
  epoch: 1,
};

/** The label a NULL pivot value's column carries. */
const EMPTY = '(empty)';

let conn: ArrowishConnection;
let planner: WasmPlanner;

/** The engine, counting what is in flight, so a test can wait for quiet. */
class Watched implements QueryEngine {
  readonly name = 'duckdb';
  inFlight = 0;
  readonly #inner: DuckDbEngine;
  constructor(inner: DuckDbEngine) {
    this.#inner = inner;
  }
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return this.#watch(() => this.#inner.execute(plan, epoch, signal));
  }
  async run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return this.#watch(() => this.#inner.run(sql, epoch, signal));
  }
  async stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    return this.#watch(() => this.#inner.stream(plan, epoch, onChunk, signal));
  }
  async #watch<T>(go: () => Promise<T>): Promise<T> {
    this.inFlight += 1;
    try {
      return await go();
    } finally {
      this.inFlight -= 1;
    }
  }
  async close(): Promise<void> {}
}

interface Opened {
  readonly app: CubeApp;
  readonly engine: Watched;
  readonly errors: string[];
}

async function openCube(snapshot: CubeSnapshot, connection: ArrowishConnection = conn): Promise<Opened> {
  const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
  (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
    (fn: () => void) => { fn(); return 0; };
  const root = dom.window.document.getElementById('r') as HTMLElement;
  const engine = new Watched(new DuckDbEngine(connection));
  const errors: string[] = [];
  const app = new CubeApp(root, snapshot, {
    engine,
    planner,
    configuration: DEFAULT_CONFIGURATION,
    onStatus: (text, kind) => { if (kind === 'error') errors.push(text); },
  });
  // A refused first query is reported on the status line AND rejects
  // open(), the host's signal that the page did not open; the checks
  // below read the status.
  await app.open().catch(() => undefined);
  await quiet(engine);
  return { app, engine, errors };
}

/**
 * Wait until nothing has been in flight for a while: an app may follow a
 * result with more queries of its own (a learned cast, a totals query).
 */
async function quiet(engine: Watched): Promise<void> {
  let calm = 0;
  for (let i = 0; i < 2000 && calm < 10; i++) {
    await new Promise((r) => setTimeout(r, 10));
    calm = engine.inFlight === 0 ? calm + 1 : 0;
  }
}

function view(o: Opened): CubeView {
  const v = o.app.view;
  assert.ok(v, 'the cube has a view');
  return v;
}

/** A cell by its row's group path and its header path. */
function cell(v: CubeView, group: readonly (string | null)[], header: readonly string[]):
unknown {
  const leaf = v.columns.all.find((l) => l.path.length === header.length
    && l.path.every((s, i) => s === header[i]));
  if (!leaf) return 'NO COLUMN';
  // a null key is a null group (the tree's keys are values, not text)
  const i = v.treeRows.findIndex((r) => r.path.length === group.length
    && r.path.every((k, j) => k === group[j]));
  if (i < 0) return 'NO ROW';
  return v.rows.columns[leaf.index]?.values[i] ?? null;
}

async function truth(sql: string): Promise<Record<string, unknown>[]> {
  const t = await new DuckDbEngine(conn).run(sql, 0);
  const out: Record<string, unknown>[] = [];
  for (let i = 0; i < t.rowCount; i++) {
    const row: Record<string, unknown> = {};
    for (const c of t.columns) row[c.name] = c.values[i] ?? null;
    out.push(row);
  }
  return out;
}

function near(actual: unknown, expected: unknown, what: string): void {
  if (expected === null || expected === undefined) {
    assert.equal(actual ?? null, null, what);
    return;
  }
  assert.equal(typeof actual === 'number' || typeof actual === 'bigint'
    || typeof actual === 'string', true, `${what}: got ${String(actual)}`);
  assert.ok(Math.abs(Number(actual) - Number(expected)) < 1e-9,
    `${what}: got ${String(actual)}, truth ${String(expected)}`);
}

const yearLabel = (y: unknown): string => (y === null ? EMPTY : String(y));

/** Every (group, year, measure) cell against the truth, plus the Totals. */
async function checkLevel(v: CubeView, keys: readonly string[], parent: Record<string, string>):
Promise<void> {
  const where = Object.entries(parent).map(([k, x]) => `${k} = '${x}'`);
  const w = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
  const by = [...keys, 'year'].join(', ');
  const aggs = Object.entries(TRUTH_SQL).map(([m, e]) => `${e} AS ${m}`).join(', ');
  for (const r of await truth(`SELECT ${by}, ${aggs} FROM TRADES ${w} GROUP BY ${by}`)) {
    const group = keys.map((k) => (r[k] === null ? null : String(r[k])));
    for (const m of Object.keys(TRUTH_SQL)) {
      near(cell(v, group, [yearLabel(r['year']), m]), r[m],
        `${group.join('/')} ${yearLabel(r['year'])} ${m}`);
    }
  }
  const totals = await truth(`SELECT ${keys.join(', ')}, ${aggs} FROM TRADES ${w} GROUP BY ${keys.join(', ')}`);
  for (const r of totals) {
    const group = keys.map((k) => (r[k] === null ? null : String(r[k])));
    for (const m of Object.keys(TRUTH_SQL)) {
      near(cell(v, group, ['Total', m]), r[m], `${group.join('/')} Total ${m}`);
    }
  }
}

/** A fresh in-memory DuckDB holding the seed TRADES: the suite's, and a test's own when it writes (Bazel workplan
 *  P3-10: no test changes rows another test reads). */
async function seededConnection(): Promise<ArrowishConnection> {
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
    eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  const connection = db.connect() as ArrowishConnection;
  const local = new DuckDbEngine(connection);
  await local.run(`CREATE TABLE TRADES (region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
    year INTEGER, qtr VARCHAR(8), notional DOUBLE, pnl DOUBLE, qty INTEGER)`, 0);
  await local.run(`INSERT INTO TRADES VALUES ${ROWS}`, 0);
  return connection;
}

before(async () => {
  conn = await seededConnection();
  planner = new WasmPlanner({ model: MODEL, runtime: RUNTIME, assetBaseUrl: MODULE_DIR, cache: false });
});

describe('a grouped pivot, judged by rows', () => {
  it('R1-R2, R4-R5: every level-1 cell and Total agrees with GROUP BY region, year', async () => {
    const o = await openCube({ ...CUBE, rows: ['region'] });
    assert.deepEqual(o.errors, []);
    await checkLevel(view(o), ['region'], {});
  });

  it('R3: an expanded group\'s cells (level 2) agree with GROUP BY region, desk, year', async () => {
    const o = await openCube(CUBE);
    await o.app.change((s) => ({ ...s, tree: s.tree.setOpen(['EMEA'], true) }));
    await quiet(o.engine);
    assert.deepEqual(o.errors, []);
    await checkLevel(view(o), ['region', 'desk'], { region: 'EMEA' });
  });

  it('R4: the NULL year has a column, and a sum\'s cells add up to its Total', async () => {
    const o = await openCube({ ...CUBE, rows: ['region'],
      measures: [{ name: 'sum_q', column: 'qty', fn: 'sum' }] });
    const v = view(o);
    for (const region of ['EMEA', 'AMER', 'APAC', null]) {
      const cells = ['2021', '2022', EMPTY].map((y) => cell(v, [region], [y, 'sum_q']));
      const sum = cells.reduce<number>((a, c) => a + (typeof c === 'number' ? c : 0), 0);
      assert.equal(sum, cell(v, [region], ['Total', 'sum_q']), `${String(region)}: ${cells.join(', ')}`);
    }
    near(cell(v, ['EMEA'], [EMPTY, 'sum_q']), 5, 'EMEA (empty) sum_q');
  });

  it('R6: sort by a pivot cell, then expand a group', async () => {
    const o = await openCube(CUBE);
    const sortOn = view(o).columns.all.find((l) => l.path.join('/') === '2021/avg_n');
    assert.ok(sortOn, 'a 2021 avg_n column');
    await o.app.change((s) => ({ ...s, snapshot: { ...s.snapshot,
      sorts: [{ column: sortOn.name, direction: 'desc' }] } }));
    await quiet(o.engine);
    await o.app.change((s) => ({ ...s, tree: s.tree.setOpen(['EMEA'], true) }));
    await quiet(o.engine);
    assert.deepEqual(o.errors, []);
    const v = view(o);
    assert.ok(v.treeRows.some((r) => r.path.join('/') === 'EMEA/FX'), 'EMEA opened');
    await checkLevel(v, ['region', 'desk'], { region: 'EMEA' });
  });

  it('R10: sort by a pivot Total, top-N under the row cap at both levels', async () => {
    // Total > cnt_q descending, two rows a level: EMEA (5), AMER (3) of four regions; under
    // EMEA, FX and Rates (2 each, the tie broken by desk) of three desks
    const o = await openCube({ ...CUBE, maxRows: 2 });
    const total = view(o).columns.all.find((l) => l.path.join('/') === 'Total/cnt_q');
    assert.ok(total, 'a Total/cnt_q column');
    await o.app.change((s) => ({ ...s, snapshot: { ...s.snapshot,
      sorts: [{ column: total.name, direction: 'desc' }] } }));
    await quiet(o.engine);
    await o.app.change((s) => ({ ...s, tree: s.tree.setOpen(['EMEA'], true) }));
    await quiet(o.engine);
    assert.deepEqual(o.errors, []);
    const v = view(o);
    assert.deepEqual(v.treeRows.filter((r) => r.path.length === 1).map((r) => r.path[0]), ['EMEA', 'AMER']);
    assert.deepEqual(v.treeRows.filter((r) => r.path.length === 2).map((r) => r.path[1]), ['FX', 'Rates']);
    for (const r of await truth(`SELECT desk, COUNT(*) AS n FROM TRADES WHERE region = 'EMEA' AND desk IN ('FX', 'Rates') GROUP BY desk`)) {
      near(cell(v, ['EMEA', String(r['desk'])], ['Total', 'cnt_q']), r['n'], `EMEA/${String(r['desk'])} Total cnt_q`);
    }
  });

  it('R7: a filter that removes a pivot value', async () => {
    const o = await openCube({ ...CUBE, rows: ['region'] });
    await o.app.change((s) => ({ ...s, snapshot: { ...s.snapshot,
      filter: { kind: 'condition', column: 'region', operator: 'equal', value: 'APAC' } } }));
    await quiet(o.engine);
    assert.deepEqual(o.errors, []);
    const v = view(o);
    assert.equal(v.columns.all.some((l) => l.path[0] === '2021'), false, 'no 2021 column for APAC');
    near(cell(v, ['APAC'], ['2022', 'avg_n']), 75, 'APAC 2022 avg_n');
  });

  it('R8: a flat pivot is one row of cells and Totals', async () => {
    const o = await openCube({ ...CUBE, rows: [] });
    assert.deepEqual(o.errors, []);
    const v = view(o);
    assert.equal(v.rows.rowCount, 1);
    const at = (header: readonly string[]): unknown => {
      const leaf = v.columns.all.find((l) => l.path.join('/') === header.join('/'));
      return leaf ? v.rows.columns[leaf.index]?.values[0] ?? null : 'NO COLUMN';
    };
    const aggs = Object.entries(TRUTH_SQL).map(([m, e]) => `${e} AS ${m}`).join(', ');
    for (const r of await truth(`SELECT year, ${aggs} FROM TRADES GROUP BY year`)) {
      for (const m of Object.keys(TRUTH_SQL)) near(at([yearLabel(r['year']), m]), r[m], `${yearLabel(r['year'])} ${m}`);
    }
    for (const r of await truth(`SELECT ${aggs} FROM TRADES`)) {
      for (const m of Object.keys(TRUTH_SQL)) near(at(['Total', m]), r[m], `Total ${m}`);
    }
  });

  // its own database: the 600 rows it inserts reach no other test
  it('R9: more pivot values than the cap is refused, with a message', async () => {
    const own = await seededConnection();
    await new DuckDbEngine(own).run(`INSERT INTO TRADES
      SELECT 'ZZ', 'Rates', 'Bx' || i, 2023, 'Q1', 1, 1, 1000 + i FROM range(600) t(i)`, 0);
    const o = await openCube({ ...CUBE, rows: ['region'], pivotOn: ['book'],
      measures: [{ name: 'cnt_q', column: 'qty', fn: 'count' }] }, own);
    assert.equal(o.errors.some((e) => /book/.test(e) && /values/.test(e)), true,
      `refused naming the column: ${o.errors.join(' | ')}`);
    assert.equal(o.app.view, null, 'no 600-column grid was drawn');
  });
});
