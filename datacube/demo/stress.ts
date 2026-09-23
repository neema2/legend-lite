// A stress matrix: many CSVs, every cube operation, real stack.
//
// Runs in a page because that is where the product runs: real
// duckdb-wasm, the real WASM planner on its worker, the real
// serialiser and the real schema inference. Nothing is stubbed, so a
// failure here is a failure a user could hit.
//
// THREE OUTCOMES, and only one of them is a bug:
//
//   ok       planned, and DuckDB ran the SQL
//   refused  the planner declined -- a legitimate answer for, say,
//            startsWith on a number, and the message is the product
//   broke    anything else: the planner emitted SQL DuckDB rejected,
//            something threw where it should have refused, or a
//            result came back malformed
//
// "broke" is what this exists to find. A large "refused" count is
// expected and its messages are worth reading.

import * as duckdb from '@duckdb/duckdb-wasm';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { serialize, type LevelScope } from '../src/serialize.ts';
import type {
  AggregateFn, CubeSnapshot, FilterNode, FilterOperator,
} from '../src/snapshot.ts';
import { ingestFile } from '../src/upload.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';
import { CORPUS } from './stress-corpus.ts';

export interface Outcome {
  readonly csv: string;
  readonly op: string;
  readonly verdict: 'ok' | 'refused' | 'broke';
  readonly detail?: string;
  readonly sql?: string;
  readonly pure?: string;
}

const AGGREGATES: AggregateFn[] = [
  'sum', 'count', 'average', 'min', 'max', 'median',
  'stdDevSample', 'stdDevPopulation', 'varianceSample',
  'variancePopulation', 'joinStrings', 'wavg',
];

const VALUE_OPS: FilterOperator[] = [
  'equal', 'notEqual', 'lessThan', 'lessThanEqual', 'greaterThan',
  'greaterThanEqual', 'contains', 'notContains', 'startsWith',
  'notStartsWith', 'endsWith', 'notEndsWith', 'equalCaseInsensitive',
  'notEqualCaseInsensitive', 'containsCaseInsensitive',
  'startsWithCaseInsensitive', 'endsWithCaseInsensitive',
];
const NULLARY_OPS: FilterOperator[] = ['isEmpty', 'isNotEmpty'];
const LIST_OPS: FilterOperator[] = [
  'in', 'notIn', 'inCaseInsensitive', 'notInCaseInsensitive',
];
const COLUMN_OPS: FilterOperator[] = [
  'equalColumn', 'notEqualColumn', 'lessThanColumn',
  'lessThanEqualColumn', 'greaterThanColumn', 'greaterThanEqualColumn',
  'equalCaseInsensitiveColumn', 'notEqualCaseInsensitiveColumn',
];

async function boot() {
  const asset = (f: string) => new URL(`./vendor/${f}`, location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: asset('duckdb-mvp.wasm'),
      mainWorker: asset('duckdb-browser-mvp.worker.js'),
    },
    eh: {
      mainModule: asset('duckdb-eh.wasm'),
      mainWorker: asset('duckdb-browser-eh.worker.js'),
    },
  });
  const db = new duckdb.AsyncDuckDB(
    new duckdb.VoidLogger(), new Worker(bundle.mainWorker!));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  const engine = new DuckDbEngine(conn as unknown as ArrowishConnection);
  const planner = new WasmPlanner({
    model: '', runtime: '',
    workerUrl: new URL('./planner-worker.js', location.href).href,
  });
  return { db, engine, planner };
}

/** Plan and run one snapshot, classifying whatever happens. */
async function attempt(
  out: Outcome[],
  deps: { engine: DuckDbEngine; planner: WasmPlanner },
  csv: string,
  op: string,
  snapshot: CubeSnapshot,
  scope?: LevelScope,
): Promise<void> {
  let pure = '';
  let sql = '';
  try {
    pure = serialize(snapshot, scope);
  } catch (e) {
    // The serialiser refusing is still a refusal, not a crash: it
    // declines shapes the cube cannot express.
    out.push({ csv, op, verdict: 'refused', pure,
      detail: `serialize: ${(e as Error).message}` });
    return;
  }
  try {
    sql = await deps.planner.plan(pure, snapshot, scope);
  } catch (e) {
    out.push({ csv, op, verdict: 'refused', pure,
      detail: `plan: ${(e as Error).message}` });
    return;
  }
  try {
    const r = await deps.engine.execute(sql, snapshot.epoch);
    // SQL the planner emitted that DuckDB accepted, but whose result
    // is nonsense, is still a bug -- check the shape rather than
    // trusting the absence of a throw.
    if (!Array.isArray(r.columns)) {
      out.push({ csv, op, verdict: 'broke', pure, sql,
        detail: 'result had no columns array' });
      return;
    }
    out.push({ csv, op, verdict: 'ok', pure, sql });
  } catch (e) {
    // THE INTERESTING CASE: the planner produced SQL and the database
    // rejected it. That is legend-lite emitting invalid SQL.
    out.push({ csv, op, verdict: 'broke', pure, sql,
      detail: `duckdb: ${(e as Error).message}` });
  }
}

function base(source: string, columns: CubeSnapshot['columns']): CubeSnapshot {
  return {
    source: { expression: source },
    columns,
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };
}

export async function runStress(): Promise<Outcome[]> {
  const { db, engine, planner } = await boot();
  const out: Outcome[] = [];

  for (const entry of CORPUS) {
    const file = new File([entry.text], entry.name, { type: 'text/csv' });
    let opened;
    try {
      opened = await ingestFile(engine, db, file);
    } catch (e) {
      // Refusing a malformed file is fine; the message must name the
      // problem. Crashing is not.
      out.push({ csv: entry.name, op: 'ingest', verdict: 'refused',
        detail: (e as Error).message });
      continue;
    }
    planner.useModel(opened.model, opened.runtime);
    const cols = opened.columns;
    const snap = (over: Partial<CubeSnapshot>): CubeSnapshot =>
      ({ ...base(opened.source, cols), ...over });

    const dims = cols.filter((c) => c.kind === 'dimension').map((c) => c.name);
    const nums = cols
      .filter((c) => c.type === 'Integer' || c.type === 'Float')
      .map((c) => c.name);
    const strs = cols.filter((c) => c.type === 'String').map((c) => c.name);
    const all = cols.map((c) => c.name);

    out.push({ csv: entry.name, op: `ingest (${cols.length} cols, `
      + `${opened.rowCount} rows)`, verdict: 'ok' });

    // -- flat, and every column selected -----------------------------
    await attempt(out, { engine, planner }, entry.name, 'flat', snap({}));

    // -- group by each dimension -------------------------------------
    for (const d of dims.slice(0, 4)) {
      await attempt(out, { engine, planner }, entry.name, `groupBy(${d})`,
        snap({ rows: [d], measures: nums[0]
          ? [{ name: 'm', column: nums[0], fn: 'sum' }] : [] }));
    }

    // -- every aggregate over the first numeric column ---------------
    for (const fn of AGGREGATES) {
      if (!nums[0] || !dims[0]) break;
      const measure = fn === 'wavg'
        ? { name: 'm', column: nums[0], fn, weight: nums[1] ?? nums[0] }
        : { name: 'm', column: nums[0], fn };
      await attempt(out, { engine, planner }, entry.name, `agg:${fn}`,
        snap({ rows: [dims[0]], measures: [measure] }));
    }
    // joinStrings over a STRING column, which is what it is for.
    if (strs[0] && dims[0]) {
      await attempt(out, { engine, planner }, entry.name, 'agg:joinStrings/str',
        snap({ rows: [dims[0]],
          measures: [{ name: 'm', column: strs[0], fn: 'joinStrings' }] }));
    }

    // -- pivot --------------------------------------------------------
    if (dims[0] && dims[1] && nums[0]) {
      await attempt(out, { engine, planner }, entry.name, 'pivot',
        snap({ rows: [dims[0]], pivotOn: [dims[1]],
          measures: [{ name: 'm', column: nums[0], fn: 'sum' }] }));
    }
    if (dims[0] && dims[1] && dims[2] && nums[0]) {
      await attempt(out, { engine, planner }, entry.name, 'pivot:two',
        snap({ rows: [dims[0]], pivotOn: [dims[1], dims[2]],
          measures: [{ name: 'm', column: nums[0], fn: 'sum' }] }));
    }

    // -- every filter operator, against a column of each type --------
    const target = strs[0] ?? all[0]!;
    for (const operator of VALUE_OPS) {
      const f: FilterNode = { kind: 'condition', column: target, operator,
        value: 'a' };
      await attempt(out, { engine, planner }, entry.name,
        `filter:${operator}`, snap({ filter: f, rows: dims[0] ? [dims[0]] : [] }));
    }
    for (const operator of NULLARY_OPS) {
      await attempt(out, { engine, planner }, entry.name,
        `filter:${operator}`,
        snap({ filter: { kind: 'condition', column: target, operator } }));
    }
    for (const operator of LIST_OPS) {
      await attempt(out, { engine, planner }, entry.name,
        `filter:${operator}`,
        snap({ filter: { kind: 'condition', column: target, operator,
          value: ['a', 'b'] } }));
    }
    for (const operator of COLUMN_OPS) {
      if (!all[1]) break;
      await attempt(out, { engine, planner }, entry.name,
        `filter:${operator}`,
        snap({ filter: { kind: 'condition', column: all[0]!, operator,
          rightColumn: all[1] } }));
    }
    // A numeric comparison, which is where coercion goes wrong.
    if (nums[0]) {
      await attempt(out, { engine, planner }, entry.name, 'filter:numeric',
        snap({ filter: { kind: 'condition', column: nums[0],
          operator: 'greaterThan', value: 0 } }));
    }
    // Nested boolean structure.
    if (all[0] && all[1]) {
      await attempt(out, { engine, planner }, entry.name, 'filter:nested',
        snap({ filter: { kind: 'and', children: [
          { kind: 'condition', column: all[0], operator: 'isNotEmpty' },
          { kind: 'not', child: { kind: 'or', children: [
            { kind: 'condition', column: all[1], operator: 'isEmpty' },
          ] } },
        ] } }));
    }

    // -- sorts ---------------------------------------------------------
    for (const direction of ['asc', 'desc'] as const) {
      if (!all[0]) break;
      await attempt(out, { engine, planner }, entry.name, `sort:${direction}`,
        snap({ sorts: [{ column: all[0], direction }] }));
    }

    // -- derived --------------------------------------------------------
    if (nums[0]) {
      await attempt(out, { engine, planner }, entry.name, 'derived:arith',
        snap({ derived: [{ name: 'dbl',
          // Quote it the way the serialiser would. Writing
          // `$x.${name}` raw emitted `$x.مبلغ * 2`, which is not
          // valid Pure -- a harness bug that looked like a product
          // one until the message was read.
          expression: `$x.${/^[A-Za-z_][A-Za-z0-9_]*$/.test(nums[0])
            ? nums[0] : `'${nums[0].replace(/\\/g, '\\\\')
              .replace(/'/g, "\\'")}'`} * 2` }] }));
    }

    // -- tree levels -----------------------------------------------------
    if (dims.length >= 2 && nums[0]) {
      const rows = dims.slice(0, 3);
      for (let level = 1; level <= rows.length; level++) {
        await attempt(out, { engine, planner }, entry.name, `tree:L${level}`,
          snap({ rows, measures: [{ name: 'm', column: nums[0], fn: 'sum' }] }),
          { level, parent: [], limit: 501 });
      }
    }
  }

  return out;
}

declare global {
  interface Window {
    __stress?: Outcome[];
    __stressDone?: boolean;
    /** The names the PICKER offers, so the runner can hold them to a
     *  higher bar than the known-broken shapes beside them. */
    __stressOffered?: string[];
  }
}

window.__stressOffered = CORPUS.filter((c) => !c.knownBroken)
  .map((c) => c.name);

void runStress().then((r) => {
  window.__stress = r;
  window.__stressDone = true;
}).catch((e) => {
  window.__stress = [{ csv: '-', op: 'harness', verdict: 'broke',
    detail: String(e && (e as Error).stack || e) }];
  window.__stressDone = true;
});
