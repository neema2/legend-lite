// TYPES COME FROM THE COMPILER (docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md).
//
// The real app (jsdom) over the real WASM planner and DuckDB-WASM. A column's
// type is what the compiler says the query returns -- never what an engine's
// wire format happens to carry, and never learned by running a query and
// looking at the answer.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../../src/config.ts';
import { DuckDbEngine, type ArrowishConnection } from '../../../engine-client/src/duckdb.ts';
import type { QueryEngine, RawTable } from '../../../engine-client/src/engine.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { inferModel } from '../../src/infer.ts';
import { catalogColumns } from '../../src/upload.ts';
import { CATALOG_RULES } from '../../src/generated/catalog-facts.ts';
import { sourceColumns } from '../../src/source-columns.ts';
import { familyOf } from '../../../engine-client/src/types.ts';
import { toCsv } from '../../src/export.ts';
import { selectionStats } from '../../src/selection.ts';
import { accessor, col, fn, lambda, lit, times, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { runfileDirUrl } from '../../../tools/js/runfiles.mts';

const MODULE_DIR = runfileDirUrl('WASM_PLANNER');

const MODEL = `###Relational
Database typed::DB
(
    Table T
    (
        region VARCHAR(32), qty INTEGER, big BIGINT, amount DECIMAL(10,2),
        price DOUBLE, day DATE, ts TIMESTAMP, flag BIT
    )
    Table BIG
    (
        id INTEGER, amount DECIMAL(38,2)
    )
    Table P
    (
        region VARCHAR(32), qty INTEGER, pnl DECIMAL(10,2), big BIGINT
    )
)

###Connection
RelationalDatabaseConnection typed::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime typed::RT
{
    mappings: [];
    connections:
    [
        typed::DB: [ c1: typed::Conn ]
    ];
}
`;

let conn: ArrowishConnection;
let planner: WasmPlanner;

/** The engine, recording what it ran and counting what is in flight. */
class Watched implements QueryEngine {
  readonly name = 'duckdb';
  inFlight = 0;
  readonly sql: string[] = [];
  readonly #inner: DuckDbEngine;
  constructor(inner: DuckDbEngine) {
    this.#inner = inner;
  }
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return this.#watch(plan.sql, () => this.#inner.execute(plan, epoch, signal));
  }
  async run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return this.#watch(sql, () => this.#inner.run(sql, epoch, signal));
  }
  async stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    return this.#watch(plan.sql, () => this.#inner.stream(plan, epoch, onChunk, signal));
  }
  async #watch<T>(sql: string, go: () => Promise<T>): Promise<T> {
    this.inFlight += 1;
    this.sql.push(sql);
    try {
      return await go();
    } finally {
      this.inFlight -= 1;
    }
  }
  async close(): Promise<void> {}
}

async function quiet(engine: Watched): Promise<void> {
  let calm = 0;
  for (let i = 0; i < 2000 && calm < 10; i++) {
    await new Promise((r) => setTimeout(r, 10));
    calm = engine.inFlight === 0 ? calm + 1 : 0;
  }
}

async function openCube(snapshot: CubeSnapshot, configuration = DEFAULT_CONFIGURATION): Promise<{
  app: CubeApp; engine: Watched; errors: string[]; root: HTMLElement; dom: JSDOM;
  downloads: [string, string, string | Uint8Array][];
}> {
  const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
  (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
    (fn: () => void) => { fn(); return 0; };
  const root = dom.window.document.getElementById('r') as HTMLElement;
  const engine = new Watched(new DuckDbEngine(conn));
  const errors: string[] = [];
  const downloads: [string, string, string | Uint8Array][] = [];
  const app = new CubeApp(root, snapshot, {
    engine,
    planner,
    configuration,
    onStatus: (text, kind) => { if (kind === 'error') errors.push(text); },
    download: (n, m, t) => downloads.push([n, m, t]),
  });
  await app.open().catch(() => undefined);
  await quiet(engine);
  return { app, engine, errors, root, dom, downloads };
}

const SOURCE = accessor('typed::DB', 'T');
const COLUMNS: CubeSnapshot['columns'] = [
  { name: 'region', type: 'String', kind: 'dimension' },
  { name: 'qty', type: 'Integer', kind: 'measure' },
  { name: 'big', type: 'Integer', kind: 'measure' },
  { name: 'amount', type: 'Decimal', kind: 'measure' },
  { name: 'price', type: 'Float', kind: 'measure' },
  { name: 'day', type: 'StrictDate', kind: 'dimension' },
  { name: 'ts', type: 'DateTime', kind: 'dimension' },
  { name: 'flag', type: 'Boolean', kind: 'dimension' },
];

before(async () => {
  const require = createRequire(import.meta.url);
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
    eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  conn = db.connect() as ArrowishConnection;
  const local = new DuckDbEngine(conn);
  await local.run(`CREATE TABLE T (region VARCHAR(32), qty INTEGER, big BIGINT, amount DECIMAL(10,2),
    price DOUBLE, day DATE, ts TIMESTAMP, flag BOOLEAN)`, 0);
  await local.run(`INSERT INTO T VALUES
    ('EMEA', 1, 9007199254740993, 10.25, 1.5, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.123456', true),
    ('EMEA', 2, 1, 20.50, 2.5, DATE '2024-01-03', TIMESTAMP '2024-01-03 00:00:00', false),
    ('AMER', 3, 2, 30.75, 3.5, DATE '2024-02-01', TIMESTAMP '2024-02-01 12:00:00', true)`, 0);
  await local.run('CREATE TABLE BIG (id INTEGER, amount DECIMAL(38,2))', 0);
  await local.run(`INSERT INTO BIG VALUES (1, 12345678901234567.89), (2, 1.01)`, 0);
  await local.run('CREATE TABLE P (region VARCHAR(32), qty INTEGER, pnl DECIMAL(10,2), big BIGINT)', 0);
  await local.run(`INSERT INTO P VALUES ('EMEA', 1, -12.50, -9007199254740993), ('EMEA', 2, 0.00, 1),
    ('AMER', 3, 1234.56, 2)`, 0);
  planner = new WasmPlanner({ model: MODEL, runtime: 'typed::RT', assetBaseUrl: MODULE_DIR, cache: false });
});

describe('step 1: types from the compiler', () => {
  it('S1a: a calculated column is typed BEFORE its first query -- one level query, not a learned re-run', async () => {
    // A numeric calculated column with no declared kind: the default
    // aggregate reads its type (a number sums). The type used to be learned
    // from the first result and the query run again.
    const o = await openCube({
      source: { query: SOURCE },
      columns: COLUMNS,
      derived: [{ name: 'double_qty', lambda: lambda(['x'], times(fn('toOne', col('x', 'qty')), lit.integer(2))) }],
      rows: ['region'],
      pivotOn: [],
      measures: [],
      sorts: [],
      epoch: 1,
    });
    assert.deepEqual(o.errors, []);
    const levels = o.engine.sql.filter((s) => /GROUP BY/.test(s));
    assert.equal(levels.length, 1, `level queries sent:\n${levels.join('\n---\n')}`);
    assert.match(levels[0]!, /SUM\([^)]*qty \* 2/i, 'the calculated column is summed on the first query');
  });

  it('S1b: every result column carries the type the COMPILER gives it, not the engine\'s wire type', async () => {
    const o = await openCube({
      source: { query: SOURCE },
      columns: COLUMNS,
      derived: [],
      rows: ['region'],
      pivotOn: [],
      // One measure per column (several on one column is audit P2-9, a
      // different leg).
      measures: [
        { name: 'sum_qty', column: 'qty', fn: 'sum' },
        { name: 'sum_amount', column: 'amount', fn: 'sum' },
        { name: 'avg_price', column: 'price', fn: 'average' },
        { name: 'n', column: 'flag', fn: 'count' },
        { name: 'last_day', column: 'day', fn: 'max' },
      ],
      sorts: [],
      epoch: 1,
    });
    assert.deepEqual(o.errors, []);
    const view = o.app.view;
    assert.ok(view);
    const typeOf = (name: string): string | undefined =>
      view.rows.columns.find((c) => c.name === name)?.type;
    // DuckDB answers SUM of an integer as a 128-bit integer, which reaches the
    // wire as a decimal; the compiler says Integer.
    const shown = view.rows.columns.map((c) => `${c.name}:${c.type}`).join(', ');
    assert.equal(typeOf('sum_qty'), 'Integer', shown);
    assert.equal(typeOf('big'), 'Integer', `the carried BIGINT sum: ${shown}`);
    assert.equal(typeOf('sum_amount'), 'Decimal', shown);
    assert.equal(typeOf('avg_price'), 'Float', shown);
    assert.equal(typeOf('n'), 'Integer', shown);
    assert.equal(typeOf('last_day'), 'StrictDate', shown);
  });
});

describe('a source\'s columns come from the compiler', () => {
  it('an inferred model of every DuckDB type compiles, and the compiler types each column', async () => {
    // A REAL table of DuckDB-WASM (the browser's DuckDB), read through the catalog question as the
    // page reads an opened file (catalog-model.ts); this compiles its model for real and reads
    // the types the compiler gives back.
    const declared = [
      ['s', 'VARCHAR'], ['big', 'BIGINT'], ['huge', 'HUGEINT'], ['ubig', 'UBIGINT'], ['i', 'INTEGER'],
      ['ti', 'TINYINT'], ['si', 'SMALLINT'], ['d', 'DOUBLE'], ['f', 'FLOAT'], ['r', 'REAL'],
      ['b', 'BOOLEAN'], ['day', 'DATE'], ['ts', 'TIMESTAMP'], ['tstz', 'TIMESTAMPTZ'],
      ['dec', 'DECIMAL(9,2)'], ['num', 'NUMERIC(18,4)'], ['uuid', 'UUID'],
      ['iv', 'INTERVAL'], ['nested', 'STRUCT(a INTEGER)'], ['j', 'JSON'],
    ];
    const duck = new DuckDbEngine(conn);
    await duck.run(`CREATE TABLE every_type (${declared.map(([n, t]) => `${n} ${t}`).join(', ')})`, 0);
    const m = inferModel(await catalogColumns(duck, 'every_type'), { table: 'every_type', convertible: true, databaseType: 'DuckDB' });
    const own = new WasmPlanner({ model: m.model, runtime: m.runtime, assetBaseUrl: MODULE_DIR, cache: false });
    const columns = await sourceColumns(own, m.source, [{ name: 'big', kind: 'dimension' }]);
    const family = Object.fromEntries(columns.map((c) => [c.name, familyOf(c.type)]));
    assert.deepEqual(columns.map((c) => c.name), declared.map(([n]) => n), 'every column, in order');
    for (const n of ['big', 'huge', 'ubig', 'i', 'ti', 'si', 'd', 'f', 'r', 'dec', 'num']) {
      assert.equal(family[n], 'numeric', n);
    }
    assert.equal(family.b, 'boolean');
    assert.equal(family.day, 'temporal');
    assert.equal(family.ts, 'temporal');
    assert.equal(family.tstz, 'temporal');
    assert.equal(family.nested, 'variant');
    assert.equal(family.j, 'variant');
    assert.equal(family.s, 'text');
    assert.equal(columns.find((c) => c.name === 'big')?.kind, 'dimension', 'the declared kind is kept');
  });

  it('every canonical type of the browser\'s DuckDB has a decision (declared, DECIMAL, or refused)', async () => {
    // the browser runs its own DuckDB, a version apart from legend-lite's: a type it adds must be
    // decided in legend-lite (DuckDb.CATALOG_RULES), not met by a person first
    const types = await new DuckDbEngine(conn).run(
      'SELECT DISTINCT logical_type FROM duckdb_types() WHERE internal AND type_oid IS NOT NULL', 0);
    const undecided = (types.columns[0]?.values ?? []).map(String)
      .filter((t) => t !== 'DECIMAL' && !(t in CATALOG_RULES.DuckDB!.types) && !(t in CATALOG_RULES.DuckDB!.refused));
    assert.deepEqual(undecided, []);
  });

  it('a type no Database holds is left out, naming the column', async () => {
    const duck = new DuckDbEngine(conn);
    await duck.run('CREATE TABLE blobs (id INTEGER, payload BLOB)', 0);
    const blobs = await catalogColumns(duck, 'blobs');
    assert.deepEqual(inferModel(blobs, { table: 'blobs', convertible: true, databaseType: 'DuckDB' }).excluded, ['payload']);
    await duck.run('CREATE TABLE only_blobs (payload BLOB)', 0);
    const onlyBlobs = await catalogColumns(duck, 'only_blobs');
    assert.throws(() => inferModel(onlyBlobs, { table: 'only_blobs', convertible: true, databaseType: 'DuckDB' }),
      /no column of 'only_blobs' can be read from its source: payload/);
  });

  it('refuses a declared column the source does not have', async () => {
    await assert.rejects(() => sourceColumns(planner, SOURCE, [{ name: 'nope', kind: 'dimension' }]),
      /the source has no column 'nope'/);
  });
});

/** A flat cube over one source, its columns typed by the compiler, opened for real. */
async function flat(source: ValueSpecification, filter?: CubeSnapshot['filter']) {
  return openCube({
    source: { query: source },
    columns: await sourceColumns(planner, source),
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    ...(filter ? { filter } : {}),
    epoch: 1,
  });
}

/** One column's exported text, row by row: what a CSV carries, unformatted. */
function exported(view: { rows: ResultTable }, column: string): string[] {
  const i = view.rows.columns.findIndex((c) => c.name === column);
  const lines = toCsv({ ...view.rows, columns: [view.rows.columns[i]!] }, { bom: false })
    .trim().split(/\r\n/).slice(1);
  return lines.sort();
}

describe('step 2: exact cells, whatever the time zone (run under TZ lanes)', () => {
  it('S2a: a DATE exports as its own calendar day, east and west of UTC', async () => {
    const o = await flat(SOURCE);
    assert.deepEqual(o.errors, []);
    assert.deepEqual(exported(o.app.view!, 'day'), ['2024-01-02', '2024-01-03', '2024-02-01']);
  });

  it('S2b: a TIMESTAMP keeps its microseconds, and a filter from its cell finds exactly its row', async () => {
    const o = await flat(SOURCE);
    const view = o.app.view!;
    assert.ok(exported(view, 'ts').includes('2024-01-02T03:04:05.123456'), exported(view, 'ts').join(' | '));
    // the right-click menu's "filter by this value": the cell's own value
    const ts = view.rows.columns.find((c) => c.name === 'ts')!;
    const at = ts.values.findIndex((v) => v !== null && String(v).includes('03:04:05'));
    const filtered = await flat(SOURCE, { kind: 'condition', column: 'ts', operator: 'equal',
      value: ts.values[at] as never });
    assert.deepEqual(filtered.errors, []);
    assert.equal(filtered.app.view?.rows.rowCount, 1, 'exactly the row the cell came from');
  });

  it('S2c: a DECIMAL(38,2) beyond 2^53 exports and sums exactly', async () => {
    const o = await flat(accessor('typed::DB', 'BIG'));
    const view = o.app.view!;
    assert.deepEqual(exported(view, 'amount'), ['1.01', '12345678901234567.89']);
    const c = view.columns.leaves.findIndex((l) => l.name === 'amount');
    const stats = selectionStats(view.rows, view.columns.leaves, {
      anchor: { row: 0, col: c }, focus: { row: 1, col: c } });
    assert.equal(String(stats.sum), '12345678901234568.90');
  });
});

// T7 (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md): everything the screen, an export, a
// chart and the selection statistics SAY about a value is decided by its column's
// compiler type -- a decimal's exact text and a big integer are numbers because their
// column is numeric, never because of the JavaScript type of the value.
describe('T7: one formatter on compiler types', () => {
  const P = accessor('typed::DB', 'P');
  const rgb = (hex: string): string =>
    `rgb(${[1, 3, 5].map((i) => Number.parseInt(hex.slice(i, i + 2), 16)).join(', ')})`;
  const NEGATIVE = rgb(DEFAULT_CONFIGURATION.appearance.negativeForeground as string);
  const ZERO = rgb(DEFAULT_CONFIGURATION.appearance.zeroForeground as string);
  const NORMAL = rgb(DEFAULT_CONFIGURATION.appearance.normalForeground as string);
  /** A column's cells on screen: their text and colour, in row order. */
  const cells = (root: HTMLElement, column: string): [string, string][] =>
    [...root.querySelectorAll<HTMLElement>(`.dc-cell[data-column="${column}"]`)]
      .map((c) => [c.textContent?.trim() ?? '', painted(c)]);
  /**
   * The colour the LIGHT theme paints a cell: a default colour reaches the DOM as its theme token
   * with the default as fallback (grid/screen-colours.ts), which jsdom does not resolve -- so the
   * fallback, normalised as jsdom normalises any colour.
   */
  const painted = (c: HTMLElement): string => {
    const token = /^var\(--[\w-]+,\s*([^)]+)\)$/.exec(c.style.color);
    if (!token) return c.style.color;
    const probe = c.ownerDocument.createElement('span');
    probe.style.color = token[1]!.trim();
    return probe.style.color;
  };
  const grouped = (measures: CubeSnapshot['measures'], only?: string) => openCube({
    source: { query: P },
    columns: ([
      { name: 'region', type: 'String', kind: 'dimension' },
      { name: 'qty', type: 'Integer', kind: 'measure' },
      { name: 'pnl', type: 'Decimal', kind: 'measure' },
      { name: 'big', type: 'Integer', kind: 'measure' },
    ] as CubeSnapshot['columns']).filter((c) => only === undefined || c.name === 'region' || c.name === only),
    derived: [], rows: ['region'], pivotOn: [], measures, sorts: [], epoch: 1,
  });

  it('T7a: a negative DECIMAL and a big integer are coloured negative, a zero one zero', async () => {
    const o = await flat(P);
    assert.deepEqual(o.errors, []);
    assert.deepEqual(cells(o.root, 'pnl'), [
      ['(12.50)', NEGATIVE], ['0.00', ZERO], ['1,234.56', NORMAL]]);
    assert.deepEqual(cells(o.root, 'big').map(([, colour]) => colour), [NEGATIVE, NORMAL, NORMAL]);
  });

  it('T7b: a measure\'s default format is ITS type\'s: the average of an Integer shows decimals', async () => {
    const o = await grouped([{ name: 'qty', column: 'qty', fn: 'average' }]);
    assert.deepEqual(o.errors, []);
    // EMEA's average of 1 and 2: 1.5, a Float -- not rounded to the source Integer's 0 places
    assert.ok(cells(o.root, 'qty').some(([text]) => text === '1.50'), JSON.stringify(cells(o.root, 'qty')));
  });

  it('T7c: the HTML export says what the screen says', async () => {
    const o = await flat(P);
    const menu = (label: string): void => {
      const item = [...o.dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role="menuitem"]')]
        .find((i) => (i.querySelector('.dc-menu-label')?.textContent ?? i.textContent) === label);
      assert.ok(item, `no menu entry ${label}`);
      item.click();
    };
    o.root.querySelector('.dc-cell')!.dispatchEvent(new o.dom.window.MouseEvent('contextmenu', { bubbles: true }));
    menu('HTML');
    ([...o.root.ownerDocument.querySelectorAll<HTMLButtonElement>('button')]
      .find((b) => b.textContent === 'Accept'))?.click();
    const found = o.downloads.find(([name]) => name.endsWith('.html'))?.[2];
    const html = typeof found === 'string' ? found : '';
    // the text the screen shows, in a cell styled as the grid styles it (inline, 2026-09-30)
    assert.match(html, /<td[^>]*>\(12\.50\)<\/td>/);
    assert.match(html, /<td[^>]*>1,234\.56<\/td>/);
  });

  it('T7d: the selection statistics read the column\'s type and format', async () => {
    const o = await openCube({
      source: { query: P }, columns: await sourceColumns(planner, P),
      derived: [], rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1,
    }, { ...DEFAULT_CONFIGURATION, showSelectionStats: true });
    o.root.querySelector<HTMLElement>('.dc-th[data-column="pnl"]')!
      .dispatchEvent(new o.dom.window.MouseEvent('click', { bubbles: true }));
    const stats = o.root.querySelector('.dc-status-stats')?.textContent ?? '';
    assert.equal(stats, 'sum 1,222.06 · avg 407.35 · min (12.50) · max 1,234.56 · 3 of 3 numeric');
  });

  it('T7f: an Integer made a DIMENSION reads as written: no thousands separators, no parentheses', async () => {
    const o = await flat(P);
    assert.deepEqual(cells(o.root, 'big').map(([t]) => t).sort(),
      ['(9,007,199,254,740,993)', '1', '2'].sort(), 'a measure, by its type: grouped digits, negatives in parentheses');
    await o.app.applyConfiguration({ columns: { big: { kind: 'dimension' } } });
    await quiet(o.engine);
    assert.deepEqual(cells(o.root, 'big').map(([t]) => t).sort(), ['-9007199254740993', '1', '2'].sort(),
      'a dimension is a label: the value as written');
  });

  it('an Integer that STARTED as a measure, made a dimension, pivots', async () => {
    // The user's path (2026-09-28): `qty` opens as a measure (every Integer does), Column
    // Properties makes it a dimension -- which also marks it excluded from pivot, as upstream --
    // and it is dragged to the column pivot. A pivot key carrying that mark was dropped from
    // the query, so nothing pivoted.
    const o = await openCube({
      source: { query: P }, columns: await sourceColumns(planner, P), derived: [],
      rows: ['region'], pivotOn: [], measures: [{ name: 'pnl', column: 'pnl', fn: 'sum' }],
      sorts: [], epoch: 1,
    });
    assert.equal(o.app.snapshot.columns.find((c) => c.name === 'qty')?.kind ?? 'measure', 'measure');
    // exactly what Column Properties > Column Kind writes (panel-column.ts)
    await o.app.applyConfiguration({ columns: { qty: { kind: 'dimension', excludedFromPivot: true } } });
    await quiet(o.engine);
    await o.app.change((s) => ({ ...s, snapshot: { ...s.snapshot, pivotOn: ['qty'] } }));
    await quiet(o.engine);
    assert.deepEqual(o.errors, []);
    const pivot = (o.app.view?.pivot?.columns ?? [])
      .filter((c) => c.tuple !== null).map((c) => c.name).sort();
    assert.deepEqual(pivot, ['1__|__pnl', '2__|__pnl', '3__|__pnl'], 'the pivot happened');
  });

  it('an Integer that STARTED as a measure, made a dimension, GROUPS -- alone, and beside a pivot', async () => {
    // The same path as the pivot case: opens as a measure, Column Properties makes it a
    // dimension (marking it excluded from pivot), then it goes to Row Groups.
    const o = await openCube({
      source: { query: P }, columns: await sourceColumns(planner, P), derived: [],
      rows: [], pivotOn: [], measures: [{ name: 'pnl', column: 'pnl', fn: 'sum' }], sorts: [], epoch: 1,
    });
    await o.app.applyConfiguration({ columns: {
      qty: { kind: 'dimension', excludedFromPivot: true },
      big: { kind: 'dimension', excludedFromPivot: true },
    } });
    await quiet(o.engine);
    const group = async (rows: string[], pivotOn: string[] = []) => {
      await o.app.change((s) => ({ ...s, snapshot: { ...s.snapshot, rows, pivotOn } }));
      await quiet(o.engine);
      assert.deepEqual(o.errors, []);
      const view = o.app.view ?? assert.fail('no view');
      const level1 = view.treeRows.map((r, i) => ({ r, i })).filter(({ r }) => r.level === 1);
      return { view, keys: level1.map(({ r }) => r.path[0]), at: level1.map(({ i }) => i) };
    };

    // grouped by it alone: one group per value, each summing its own row
    const byQty = await group(['qty']);
    assert.deepEqual([...byQty.keys].sort(), ['1', '2', '3']);
    const pnl = byQty.view.rows.columns.find((c) => c.name === 'pnl') ?? assert.fail('no pnl');
    const sums = Object.fromEntries(byQty.keys.map((k, n) => [k, String(pnl.values[byQty.at[n] as number])]));
    assert.deepEqual(sums, { 1: '-12.50', 2: '0.00', 3: '1234.56' });

    // grouped by it AND pivoted on another column
    const beside = await group(['qty'], ['region']);
    assert.deepEqual([...beside.keys].sort(), ['1', '2', '3']);
    const cells = (beside.view.pivot?.columns ?? []).filter((c) => c.tuple !== null).map((c) => c.name).sort();
    assert.deepEqual(cells, ['AMER__|__pnl', 'EMEA__|__pnl']);

    // a big Integer grouped: its label is the value as written
    await group(['big']);
    const labels = [...o.root.querySelectorAll<HTMLElement>('.dc-row .dc-cell.dc-tree')]
      .map((c) => c.textContent?.replace(/^[▸▾]\s*/, '').replace(/\s*\(\d+\)$/, '').trim());
    assert.ok(labels.includes('-9007199254740993'), labels.join(' | '));
  });

  // T7e (a treemap's tooltip says what the grid says) moved to chart-echarts.test.ts with the
  // treemap itself: it is a chart now (the user, 2026-09-30), drawn by ECharts.

});
