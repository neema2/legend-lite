// DATACUBE AGAINST PYTHON'S ENGINE, IN THE BROWSER (//datacube:python_engine_test;
// docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md). The harness (python-engine.mjs) starts Python's engine
// (engine_fixture.py, beside this page: the cube corpus's rows as a Live frame, DataCube's site beside it) and opens this
// page at the engine's origin; every check runs here, in the pinned Chromium, through DataCube's own remote-run path
// (RemoteRun over LegendEngineExecutor, declared ARROW_IPC, carrying the engine's token). Nothing is checked in Node.
//
// One cube, two engines, one compiler: every case of the cube corpus runs REMOTELY on Python's engine (the compiler
// plans in Python, duckdb-python runs the SQL over the frame, the rows come back in upstream's Arrow format), then in
// the TAB (the same compiler in its worker, DuckDB-WASM over the same rows). The answers -- their columns, the types
// the compiler gave them, their values -- must agree: a case whose SQL ends in ORDER BY row for row, one without as
// a multiset. The corpus reads `trades::DB.TRADES`; each query is pointed at the frame's own table instead (its
// source, as the frame's model writes it), the one change made to it.
//
// Also: upstream's own Arrow answer (legend-engine 4.145.0's, recorded) read by the same reader; the compiler's other
// answers (a query's types, parse, print) the same on both sides; an engine read as JSON, and a call without the
// token, refused by name.

import { startDuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import type { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import { arrowResultTable, LegendEngineExecutor } from '../../../engine-client/src/engine-remote.ts';
import { arrowIpcStream } from '../../../engine-client/src/pure-v1.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { PlanThenRun, RemoteRun } from '../../src/runner.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { CASES, queries } from '../wasm-differential/cases.ts';

/** What the harness hands the page: the engine, and the frame it serves. */
interface Served {
  readonly authorization: string;
  readonly model: string;
  readonly runtime: string;
  readonly source: unknown;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly (string | number | null)[])[];
}

interface Outcome {
  readonly name: string;
  readonly ok: boolean;
  readonly message?: string;
  readonly ms: number;
}

declare global {
  interface Window {
    __pythonEngineServed?: Served;
    __pythonEngine?: { readonly done: boolean; readonly outcomes: readonly Outcome[]; readonly setupError?: string };
    /** Where the page is: the harness says it when the page does not finish. */
    __pythonEngineStage?: string;
  }
}

// --- checks -------------------------------------------------------------------------------------------------------

class CheckFailed extends Error {}

function equal(actual: unknown, expected: unknown, message = ''): void {
  if (actual !== expected) throw new CheckFailed(`${message} expected ${String(expected)}, got ${String(actual)}`.trim());
}

function deepEqual(actual: unknown, expected: unknown, message = ''): void {
  const a = JSON.stringify(actual, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
  const e = JSON.stringify(expected, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
  if (a !== e) throw new CheckFailed(`${message} expected ${e}, got ${a}`.trim());
}

async function rejects(p: Promise<unknown>, pattern: RegExp): Promise<void> {
  const outcome = await p.then(() => undefined, (e: unknown) => e);
  if (outcome === undefined) throw new CheckFailed(`expected a refusal matching ${pattern}; it answered`);
  const text = outcome instanceof Error ? outcome.message : String(outcome);
  if (!pattern.test(text)) throw new CheckFailed(`${JSON.stringify(text.slice(0, 300))} does not match ${pattern}`);
}

/** A result as comparable text: its columns and their types, then its rows (sorted unless ordered). */
function shape(r: ResultTable, ordered: boolean): string {
  const rows: string[] = [];
  for (let i = 0; i < r.rowCount; i++) {
    rows.push(JSON.stringify(r.columns.map((c) => {
      const v = c.values[i];
      return typeof v === 'bigint' ? v.toString() : v;
    })));
  }
  if (!ordered) rows.sort();
  return `${r.columns.map((c) => `${c.name}:${c.type}`).join(',')}\n${rows.join('\n')}`;
}

function ordered(sql: string): boolean {
  const lines = sql.trim().split('\n');
  return lines.some((l, i) => i >= lines.length - 2 && /^ORDER BY /.test(l.trim()));
}

/** Whether a node reads the corpus's table: `#>{trades::DB.TRADES}#`, as its queries are built. */
function readsTheCorpus(n: Record<string, unknown>): boolean {
  const path = (n['value'] as { path?: unknown } | undefined)?.path;
  return n['_type'] === 'classInstance' && n['type'] === '>' && Array.isArray(path)
    && path.length === 2 && path[0] === 'trades::DB' && path[1] === 'TRADES';
}

/**
 * A tree with every read of the corpus's table pointed at the frame's: the one change made to a query. Only plain
 * objects and arrays are rebuilt; a value of the protocol's own (an exact number) is kept as it is.
 */
function onTheFrame<T>(tree: T, source: unknown): T {
  const walk = (n: unknown): unknown => {
    if (Array.isArray(n)) return n.map(walk);
    if (n === null || typeof n !== 'object' || Object.getPrototypeOf(n) !== Object.prototype) return n;
    const node = n as Record<string, unknown>;
    if (readsTheCorpus(node)) return source;
    return Object.fromEntries(Object.entries(node).map(([k, v]) => [k, walk(v)]));
  };
  return walk(tree) as T;
}

function sqlValue(v: string | number | null): string {
  if (v === null) return 'NULL';
  return typeof v === 'number' ? String(v) : `'${v.replace(/'/g, "''")}'`;
}

// --- the setup and the cases ------------------------------------------------------------------------------------------

const outcomes: Outcome[] = [];

async function check(name: string, body: () => Promise<void>): Promise<void> {
  window.__pythonEngineStage = name;
  const t0 = performance.now();
  try {
    await body();
    outcomes.push({ name, ok: true, ms: performance.now() - t0 });
  } catch (e: unknown) {
    outcomes.push({ name, ok: false, message: e instanceof Error ? e.stack ?? e.message : String(e), ms: performance.now() - t0 });
  }
}

async function run(): Promise<void> {
  const served = window.__pythonEngineServed;
  if (served === undefined) throw new Error('the harness handed the page no engine');
  const engine = { baseUrl: location.origin, model: served.model, runtime: served.runtime };
  const remote = new RemoteRun(new LegendEngineExecutor({
    ...engine, serializationFormat: 'ARROW_IPC', authorization: served.authorization,
  }));

  // the tab: the same compiler in its worker, DuckDB-WASM over the same rows, in a table the frame's model reads
  window.__pythonEngineStage = 'starting the tab\'s DuckDB and the compiler';
  const local: DuckDbEngine = (await startDuckDbInTab('vendor/')).engine;
  await local.run(`CREATE TABLE trades (region VARCHAR, desk VARCHAR, book VARCHAR, year INTEGER, qtr VARCHAR,
    notional DOUBLE, pnl DOUBLE, qty INTEGER)`, 0);
  await local.run(`INSERT INTO trades VALUES ${served.rows.map((r) => `(${r.map(sqlValue).join(', ')})`).join(', ')}`, 0);
  const planner = new WasmPlanner({ ...engine, workerUrl: new URL('planner-worker.js', location.href).href });
  const tab = new PlanThenRun(planner, local);

  await check('every cube case answers the same on Python\'s engine and in the tab', async () => {
    const differ: string[] = [];
    let compared = 0;
    for (const [i, { name, query }] of queries().entries()) {
      const snapshot: CubeSnapshot = onTheFrame(CASES[i]!.snapshot, served.source);
      const q: Lambda = onTheFrame(query, served.source);
      const there = await remote.run(q, snapshot, CASES[i]!.scope);
      const here = await tab.run(q, snapshot, CASES[i]!.scope);
      equal(there.sql, here.sql, `${name}: the SQL Python's engine reports is the tab's plan:`);
      const inOrder = ordered(here.sql);
      const a = shape(there.rows, inOrder);
      const b = shape(here.rows, inOrder);
      if (a !== b) differ.push(`${name}\n  python: ${a.slice(0, 400)}\n  tab:    ${b.slice(0, 400)}`);
      compared += 1;
    }
    deepEqual(differ, [], 'Python\'s engine and the tab disagree:');
    equal(CASES.length > 0 && compared === CASES.length, true, `cases compared: ${compared} of ${CASES.length}`);
  });

  await check('a query\'s types, parse and print: the same compiler\'s answers', async () => {
    const source = { _type: 'lambda', parameters: [], body: [served.source] } as unknown as Lambda;
    deepEqual(await remote.relationType(source), await tab.relationType(source), 'the source\'s columns:');
    const text = '|#>{trades::DB.trades}#->filter(x|$x.qty > 5)->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])';
    const parsed = await remote.parse(text);
    deepEqual(parsed, await tab.parse(text), 'the parse:');
    equal(await remote.print(parsed, 'STANDARD'), await tab.print(parsed, 'STANDARD'), 'the print:');
  });

  await check('upstream\'s own Arrow answer reads the same way (legend-engine 4.145.0, recorded)', async () => {
    const bytes = new Uint8Array(await (await fetch('upstream.arrows.zst')).arrayBuffer());
    const { rows, activities } = arrowResultTable(await arrowIpcStream(bytes), 1, 0);
    deepEqual(rows.columns.map((c) => `${c.name}:${c.type}`), ['region:String', 'total:Float']);
    deepEqual(rows.columns.map((c) => c.values), [['AMER', 'APAC', 'EMEA', 'LATAM'], [528, 3088, 2712, 1928]]);
    equal(activities.length, 1, 'its activities:');
  });

  await check('an engine read as JSON, a JSON answer to an Arrow request, and a call without the token, are refused by name', async () => {
    const asJson = new RemoteRun(new LegendEngineExecutor({ ...engine, authorization: served.authorization }));
    const first = { name: CASES[0]!.name, snapshot: onTheFrame(CASES[0]!.snapshot, served.source) };
    const q = onTheFrame(queries()[0]!.query, served.source);
    await rejects(asJson.run(q, first.snapshot), /ARROW_IPC/);
    const without = new RemoteRun(new LegendEngineExecutor({ ...engine, serializationFormat: 'ARROW_IPC' }));
    await rejects(without.run(q, first.snapshot), /token/);
    // a server declared ARROW_IPC that does not serve it answers its JSON, 200: refused, never read as rows
    const json = '{"builder": {"_type":"tdsBuilder","columns":[]}, "activities": [], "result" : {"columns" : [], "rows" : []}}';
    const answersJson = new RemoteRun(new LegendEngineExecutor({
      ...engine, serializationFormat: 'ARROW_IPC', authorization: served.authorization,
      fetch: () => Promise.resolve(new Response(json, { status: 200, headers: { 'Content-Type': 'application/json' } })),
    }));
    await rejects(answersJson.run(q, first.snapshot), /not ARROW_IPC/);
  });
}

run().then(
  () => { window.__pythonEngine = { done: true, outcomes }; },
  (e: unknown) => {
    window.__pythonEngine = { done: true, outcomes, setupError: e instanceof Error ? e.stack ?? e.message : String(e) };
  },
);
