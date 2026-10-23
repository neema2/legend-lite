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
// answers (a query's types, parse, print) the same on both sides; an engine read as JSON answering the rows it answers
// in Arrow (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1: Python answers legend-engine's JSON too), and a call without the
// token refused by name.
//
// And A PAGE FROM PYTHON'S ENGINE (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md, step 1): `__pythonPageDocument`
// writes a page of two frames on two sheets with DataCube's own writers, for the harness to have Python serve it and a
// tab open it (demo/engine.html?page=); `__readPythonPage` reads a page Python wrote with DataCube's own reader.

import { startDuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import { arrowResultTable, LegendEngineExecutor } from '../../../engine-client/src/engine-remote.ts';
import { arrowIpcStream } from '../../../engine-client/src/pure-v1.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import { from, type Lambda } from '../../../pure-protocol/src/index.ts';
import { PlanThenRun, RemoteRun, type PlanAndRun } from '../../src/runner.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import { defaultChart } from '../../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION } from '../../src/config.ts';
import { writeCube, type FrameSource } from '../../src/cube-document.ts';
import { pageToJson, readPage, writePageOf } from '../../src/page-document.ts';
import { sourceColumns } from '../../src/source-columns.ts';
import { TreeState } from '../../src/tree.ts';
import { LitePlanner } from '../../src/lite-planner.ts';
import { CASES, queries } from '../wasm-differential/cases.ts';
import { askedOffers, askedSettings, pythonDatabase, pythonRunner, pythonServices, type PythonLink } from '../../src/services/python-host.ts';
import { browserBasics } from '../../src/services/browser-basics.ts';
import type { Services } from '../../src/app/services.ts';
import { dropFileTable, ingestFile, modelLanded, mountRemoteView } from '../../src/upload.ts';
import { SnapManager } from '../../../engine-client/src/snap.ts';

/** What the harness hands the page: the engine, and the frame it serves. */
interface Served {
  readonly authorization: string;
  readonly model: string;
  readonly runtime: string;
  readonly source: unknown;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly (string | number | null)[])[];
  /** A warehouse the harness started and seeded (python-engine.mjs): the page reads it through Python (C3). */
  readonly warehouse: string;
  /** A Parquet file a server on this machine serves (engine_fixture.py): read by Python's DuckDB and by the tab's. */
  readonly remote: string;
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
    /** A page of the engine's two frames on two sheets, as DataCube writes one (page-document.ts), as JSON. */
    __pythonPageDocument?: () => Promise<unknown>;
    /** A page as Python wrote it (its JSON text) read by DataCube's own reader: why it refuses it, or null. */
    __readPythonPage?: (text: string) => string | null;
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
  const planner = new LitePlanner({ ...engine, workerUrl: new URL('planner-worker.js', location.href).href });
  // the one start of a tab's DuckDB (engine-client's startDuckDbInTab), set up by the planner's rules before anything is
  // written to it (this browser is in New York: python-engine.mjs); the database kept too, which a person's file is
  // read into (upload.ts)
  const { db, engine: local } = await startDuckDbInTab('./vendor/', planner);
  await local.run(`CREATE TABLE trades (region VARCHAR, desk VARCHAR, book VARCHAR, year INTEGER, qtr VARCHAR,
    notional DOUBLE, pnl DOUBLE, qty INTEGER)`, 0);
  await local.run(`INSERT INTO trades VALUES ${served.rows.map((r) => `(${r.map(sqlValue).join(', ')})`).join(', ')}`, 0);
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

  // PLANANDRUN ON PYTHON (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): the runner Python's pages use -- planned
  // against the model Python holds, run on its DuckDB, in one call -- and the tab's, over the same rows: the same plan
  // function, the same reading, so the same answers, SQL and all
  await check('every cube case answers the same through PlanAndRun on Python and in the tab', async () => {
    const python = pythonRunner({ baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization },
      served.model, served.runtime);
    const differ: string[] = [];
    let compared = 0;
    for (const [i, { name, query }] of queries().entries()) {
      const snapshot: CubeSnapshot = onTheFrame(CASES[i]!.snapshot, served.source);
      const q: Lambda = onTheFrame(query, served.source);
      const there = await python.run(q, snapshot, CASES[i]!.scope);
      const here = await tab.run(q, snapshot, CASES[i]!.scope);
      equal(there.sql, here.sql, `${name}: the SQL Python planned is the tab's plan:`);
      const inOrder = ordered(here.sql);
      const a = shape(there.rows, inOrder);
      const b = shape(here.rows, inOrder);
      if (a !== b) differ.push(`${name}\n  python: ${a.slice(0, 400)}\n  tab:    ${b.slice(0, 400)}`);
      compared += 1;
    }
    deepEqual(differ, [], 'PlanAndRun on Python and the tab disagree:');
    equal(CASES.length > 0 && compared === CASES.length, true, `cases compared: ${compared} of ${CASES.length}`);
  });

  // THE READER'S FILES (§15.6): each read by DataCube's own reader (upload.ts) into the tab's DuckDB and into Python's
  // (the page hands Python the bytes the tab's DuckDB is given, fileBytes), then every row planned and run on each
  // host -- the same columns, types and values. A text file is read as the page reads it: a byte-order mark
  // dropped, CRLF lines, a Latin-1 file read as Latin-1 (G4); and a file larger than one part.
  await check('every reader file reads the same on Python\'s page and in the tab', async () => {
    const link: PythonLink = { baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization };
    const python = pythonDatabase(link, await askedOffers(link));
    if (python.ingestFile === undefined || python.forgetFile === undefined) throw new CheckFailed('Python\'s engine reads no page file');
    const utf8 = (text: string): Uint8Array => new TextEncoder().encode(text);
    const files: readonly (readonly [string, Uint8Array])[] = [
      ['bom.csv', utf8('\uFEFFid,name\r\n1,a\r\n2,b\r\n')],
      ['quoted.csv', utf8('id,name,amount\r\n1,"x, y",1.50\r\n2,z,\r\n3,"say ""hi""",-0.25\r\n')],
      ['latin1.csv', new Uint8Array([...utf8('id,name\n1,caf'), 0xe9, ...utf8('\n2,plain\n')])],
      ['dates.csv', utf8('day,at,flag\n2024-01-02,2024-01-02 03:04:05.123456,true\n0001-01-01,1970-01-01 00:00:00,false\n,,\n')],
      ['lines.jsonl', utf8('{"id":1,"tags":["a","b"],"at":"2024-01-02T03:04:05.123456"}\n{"id":2,"tags":[],"at":null}\n')],
      ['nested.json', utf8('\uFEFF[{"id":1,"customer":{"name":"A","tier":2},"amount":12.30},{"id":2,"customer":null,"amount":null}]')],
      // over 4 MiB: sent to Python in two parts (upload.ts PART_BYTES)
      ['parts.csv', utf8(`id,name\n${Array.from({ length: 400_000 }, (_, i) => `${i},José ${i}`).join('\n')}\n`)],
    ];
    // the runner reads only the snapshot's epoch
    const snapshot = CASES[0]!.snapshot;
    const all = (source: unknown): Lambda => ({ _type: 'lambda', parameters: [], body: [source] }) as unknown as Lambda;
    const differ: string[] = [];
    for (const [i, [name, bytes]] of files.entries()) {
      const file = new File([bytes as BlobPart], name);
      const table = `reader_${i + 1}`;
      const here = await ingestFile(local, db, file, planner, { table });
      const there = await python.ingestFile(file, { table });
      const tabRows = await new PlanThenRun(planner.withModel(here.model, here.runtime), local).run(all(here.source), snapshot);
      const pythonRows = await pythonRunner(link, there.model, there.runtime).run(all(there.source), snapshot);
      const a = shape(pythonRows.rows, false);
      const b = shape(tabRows.rows, false);
      if (a !== b) differ.push(`${name}\n  python: ${a.slice(0, 400)}\n  tab:    ${b.slice(0, 400)}`);
      if (there.rowCount !== here.rowCount) differ.push(`${name}: ${there.rowCount} rows on Python, ${here.rowCount} in the tab`);
      // and right, not only alike: a nested value is its JSON, its numbers numbers
      if (name === 'nested.json') {
        const customer = pythonRows.rows.columns.find((c) => c.name === 'customer')?.values.find((v) => v !== null);
        equal(customer, '{"name":"A","tier":2}', 'a nested value\'s JSON:');
      }
      await python.forgetFile(name, there.table);
    }
    deepEqual(differ, [], 'Python\'s page and the tab read a file differently:');
  });

  // A REMOTE FILE, BOTH WAYS (L12: an ability the two hosts do by a DuckDB each): the same Parquet file, served by a
  // server on this machine, mounted by the one implementation (upload.ts mountRemoteView) -- in the tab's DuckDB-WASM,
  // and in Python's DuckDB through the httpfs it carries -- its model written by the one writer, every row planned and
  // run on each host: the same columns, types and values
  await check('a remote file reads the same on Python\'s page and in the tab', async () => {
    const link: PythonLink = { baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization };
    const python = pythonDatabase(link, await askedOffers(link));
    if (python.mountRemote === undefined) throw new CheckFailed('Python\'s engine reads no remote file');
    const all = (source: unknown): Lambda => ({ _type: 'lambda', parameters: [], body: [source] }) as unknown as Lambda;
    const snapshot = CASES[0]!.snapshot;
    const here = await mountRemoteView(local, planner, { url: served.remote, name: 'remote_books' });
    const there = await python.mountRemote(served.remote, undefined, 'remote_books');
    const tabRows = await new PlanThenRun(planner.withModel(here.model, here.runtime), local).run(all(here.source), snapshot);
    const pythonRows = await pythonRunner(link, there.model, there.runtime).run(all(there.source), snapshot);
    equal(shape(tabRows.rows, false), shape(pythonRows.rows, false), 'the tab and Python read the remote file differently:');
    // and right, not only alike
    equal(shape(tabRows.rows, false), 'book:String,pages:Integer\n["Dune",412]\n["Emma",474]\n["Ulysses",730]', 'its rows:');
    // the tab's view dropped; Python keeps a remote file's view by its URL, as for its pages (a later mount of the same
    // URL takes its place), and offers a page no way to drop one
    await dropFileTable(local, planner, 'remote_books', true);
  });

  // A SNAP, BOTH WAYS (L12: the tab's copy and Python's, an implementation each): the frame's every column copied by
  // each host's own Snap -- the tab's SnapManager into its DuckDB, Python's snap.json into its own -- each copy made to
  // hold what its model declares by the one writer (upload.ts modelLanded) and read through that model: the same
  // columns, types and rows
  await check('a grid snaps the same copy on Python and in the tab', async () => {
    const link: PythonLink = { baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization };
    const python = pythonDatabase(link, await askedOffers(link));
    if (python.snap === undefined || python.release === undefined) throw new CheckFailed('Python\'s engine snaps no grid');
    const snapshot = CASES[0]!.snapshot;
    const columns = (await sourceColumns(tab, served.source as never)).map((c) => c.name);
    const pull = from(served.source as never).select(columns).lambda();
    // Python's: the pull sent as Pure, run and landed there
    const theirs = await python.snap(served.model, served.runtime, pull);
    const fromTheirs = await pythonRunner(link, theirs.model, theirs.runtime).run(from(theirs.source).select(columns).lambda(), snapshot);
    // the tab's: the live plan run into a table of its own, modelled once the rows land
    let landed: Awaited<ReturnType<typeof modelLanded>> | undefined;
    const snaps = new SnapManager(local, null, planner);
    await snaps.snap((await planner.plan(pull)).sql, 0, { target: { table: 'snap_parity', landed: async () => {
      landed = await modelLanded(local, planner, 'snap_parity');
      return landed.source;
    } } });
    const ours = new PlanThenRun(planner.withModel(landed!.model, landed!.runtime), local);
    const fromOurs = await ours.run(from(landed!.source).select(columns).lambda(), snapshot);
    equal(shape(fromOurs.rows, false), shape(fromTheirs.rows, false), 'the tab\'s copy and Python\'s differ:');
    equal(fromOurs.rows.rowCount, served.rows.length, 'the copy holds every row:');
    await snaps.release();
    await python.release(theirs.table);
  });

  // THE SESSION'S ZONE: this browser is in New York, Python's process in UTC; the tab's DuckDB set up as Python's is, a
  // query of the time now answers the same instant on both (planned `CAST(now() AS TIMESTAMP)`: the session's wall time)
  await check('the time now answers alike on Python and in a tab in New York: both sessions in UTC', async () => {
    equal(Intl.DateTimeFormat().resolvedOptions().timeZone, 'America/New_York', 'the browser\'s zone:');
    const zone = await local.run("SELECT current_setting('TimeZone') AS zone", 0);
    equal(zone.columns[0]!.values[0], 'UTC', 'the tab\'s session zone:');
    const python = pythonRunner({ baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization },
      served.model, served.runtime);
    const q = onTheFrame((await tab.parse('|#>{trades::DB.TRADES}#->limit(1)->extend(~[n: x|now()])->select(~[n])')) as Lambda,
      served.source);
    const snapshot = CASES[0]!.snapshot;
    const at = async (r: PlanAndRun): Promise<number> =>
      Date.parse(`${String((await r.run(q, snapshot)).rows.columns[0]!.values[0])}Z`);
    const [here, there] = [await at(tab), await at(python)];
    equal(Math.abs(here - there) < 60_000, true, `now() in the tab ${new Date(here).toISOString()}, on Python ${new Date(there).toISOString()}:`);
  });

  // THE PAGE'S SETTINGS, KEPT BY PYTHON (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B2): written by the page's browser
  // service to Python, which keeps them in this user's folder; the next page Python opens reads them -- as another run
  // or another notebook would, where the browser's storage is the address's, and Python's address is new each run
  await check('the page\'s settings are kept by Python, for the next page it opens', async () => {
    const link: PythonLink = { baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization };
    const offers = await askedOffers(link);
    const open = async (): Promise<Services> => {
      const kept = await askedSettings(link);
      return pythonServices({ link, shows: { page: 'trades' }, offers, ...(kept ? { settings: kept } : {}),
        watch: () => () => undefined, browser: browserBasics });
    };
    const first = await open();
    first.browser.writeSettings({ 'dataCube.grid.rowBuffer': 30 });
    equal(first.browser.readSettings()?.['dataCube.grid.rowBuffer'], 30, 'this page\'s reading of what it wrote:');
    const until = performance.now() + 5000;
    while ((await askedSettings(link))?.['dataCube.grid.rowBuffer'] !== 30) {
      if (performance.now() > until) throw new CheckFailed('Python did not keep the page\'s settings');
      await new Promise((r) => setTimeout(r, 50));
    }
    equal((await open()).browser.readSettings()?.['dataCube.grid.rowBuffer'], 30, 'the next page\'s:');
  });

  // A WAREHOUSE TABLE ON PYTHON'S PAGE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): Python signs in as the person
  // (rita), lists what she may read, writes and holds the table's model with the tab's own writer, and plans and runs
  // the grid's queries Live there -- the page sends Pure, and never holds the warehouse's token
  await check('a warehouse table read on Python\'s page: signed in there, its model Python\'s, its rows Live from the warehouse, and snapped into Python\'s DuckDB', async () => {
    const link: PythonLink = { baseUrl: location.origin, fetch: (i, init) => fetch(i, init), authorization: served.authorization };
    const offers = await askedOffers(link);
    equal(offers.warehouse, true, 'Python offers a warehouse:');
    const services = pythonServices({ link, shows: { page: 'trades' }, offers, watch: () => () => undefined, browser: browserBasics });
    const warehouse = services.warehouse!;
    await rejects(warehouse.connect(served.warehouse, 'rita', 'wrong'), /sign-in failed — AUTH_INVALID: wrong user or password/);
    const { session, objects } = await warehouse.connect(served.warehouse, 'rita', 'rita-pw');
    deepEqual([session.principal, session.token], ['rita', ''], 'the session, without its token:');
    deepEqual(objects.map((o) => o.name), ['POSITIONS'], 'what rita may read, and only that:');
    const m = await warehouse.table!(session, objects[0]!);
    const rows = services.engine.rows(m.model, m.runtime, { bitColumns: m.bitColumns });
    const columns = await sourceColumns(rows.runner!, m.source);
    deepEqual(columns.map((c) => `${c.name}:${c.type}`), ['desk:String', 'qty:Integer', 'px:Decimal'], 'its columns, as Python\'s model types them:');
    const read = await rows.runner!.run(from(m.source).select(['desk', 'qty']).lambda(), { epoch: 0 } as CubeSnapshot);
    equal(shape(read.rows, false), 'desk:String,qty:Integer\n["EQ",5]\n["FX",10]\n["FX",3]', 'its rows, read Live through Python:');
    // a table it did not list is not one Python opens
    await rejects(warehouse.table!(session, { ...objects[0]!, name: 'secret' }), /not a table rita may read/);
    // SNAP, on Python (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): the pull sent as Pure, counted and landed there
    // in a table of its own, modelled by DataCube's code as the tab models its copy; read through that model, by
    // Python's DuckDB; released, the copy is gone, and reading it is refused
    const frozen = await services.engine.database.snap!(m.model, m.runtime, from(m.source).select(['desk', 'qty', 'px']).lambda());
    deepEqual([frozen.rowCount, frozen.pulledBy?.as, frozen.pulledBy?.serverRows], [3, 'rita', 3], 'what Python froze:');
    const copy = services.engine.rows(frozen.model, frozen.runtime, { bitColumns: frozen.bitColumns }).runner!;
    const two = from(frozen.source).select(['desk', 'qty']).lambda();
    equal(shape((await copy.run(two, { epoch: 0 } as CubeSnapshot)).rows, false), 'desk:String,qty:Integer\n["EQ",5]\n["FX",10]\n["FX",3]',
      'the copy\'s rows, read in Python\'s DuckDB:');
    await services.engine.database.release!(frozen.table);
    await rejects(copy.run(two, { epoch: 0 } as CubeSnapshot), /reads only the tables it serves.*which it does not serve now/);
    // and a frame's grid snaps the same way (one Snap), its copy read through Python's own model of it
    const fm = await services.engine.database.frame!('trades');
    const frameRows = services.engine.rows(fm.model, fm.runtime).runner!;
    const all = (await sourceColumns(frameRows, fm.source)).map((c) => c.name);
    const frameCopy = await services.engine.database.snap!(fm.model, fm.runtime, from(fm.source).select(all).lambda());
    const fromCopy = await services.engine.rows(frameCopy.model, frameCopy.runtime).runner!.run(
      from(frameCopy.source).select(all).lambda(), { epoch: 0 } as CubeSnapshot);
    const fromFrame = await frameRows.run(from(fm.source).select(all).lambda(), { epoch: 0 } as CubeSnapshot);
    equal(shape(fromCopy.rows, false), shape(fromFrame.rows, false), 'a frame\'s copy holds its rows, read in Python\'s DuckDB:');
    await services.engine.database.release!(frameCopy.table);
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

  await check('an engine read as JSON answers the rows it answers in Arrow; a JSON answer to an Arrow request, and a call without the token, are refused by name', async () => {
    const asJson = new RemoteRun(new LegendEngineExecutor({ ...engine, authorization: served.authorization }));
    const asArrow = new RemoteRun(new LegendEngineExecutor({ ...engine, serializationFormat: 'ARROW_IPC', authorization: served.authorization }));
    const first = { name: CASES[0]!.name, snapshot: onTheFrame(CASES[0]!.snapshot, served.source) };
    const q = onTheFrame(queries()[0]!.query, served.source);
    const viaJson = await asJson.run(q, first.snapshot);
    const viaArrow = await asArrow.run(q, first.snapshot);
    deepEqual(viaJson.rows.columns.map((c) => [c.name, c.type, c.values]), viaArrow.rows.columns.map((c) => [c.name, c.type, c.values]),
      'the rows, as JSON and as Arrow:');
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

/** A page as Python wrote it, read by DataCube's own reader (page-document.ts): why it refuses it, or null. */
window.__readPythonPage = (text: string): string | null => {
  try {
    readPage(text);
    return null;
  } catch (e) {
    return e instanceof Error ? e.message : String(e);
  }
};

/**
 * A PAGE OVER THE ENGINE'S FRAMES, written as DataCube writes one: on its first sheet the trades grid, grouped by
 * region, and its chart beside it; on a second, named Desks, a grid over the desks frame. Each grid's cube over its
 * frame by name, its columns as the engine's model types them.
 */
window.__pythonPageDocument = async () => {
  const served = window.__pythonEngineServed!;
  const cubeOf = async (frame: string, rows: readonly string[]) => {
    const said = await (await fetch(`cube.json?table=${frame}`, { headers: { Authorization: served.authorization } })).json() as
      { model: string; runtime: string; source: never };
    const run = new RemoteRun(new LegendEngineExecutor({
      baseUrl: location.origin, model: said.model, runtime: said.runtime, serializationFormat: 'ARROW_IPC',
      authorization: served.authorization,
    }));
    const columns = await sourceColumns(run, said.source);
    const snapshot: CubeSnapshot = { source: { query: said.source }, columns, derived: [], rows, pivotOn: [], sorts: [], epoch: 1 };
    const source: FrameSource = { _type: 'frame', name: frame, columns: columns.map((c) => ({ name: c.name, type: c.type })) };
    return { snapshot, cube: writeCube({ name: frame, source, snapshot, configuration: { ...DEFAULT_CONFIGURATION, reportTitle: frame }, tree: TreeState.empty() }) };
  };
  // grouped, its notional summed: a number's Aggregation by default
  const trades = await cubeOf('trades', ['region']);
  const desks = await cubeOf('desks', []);
  const doc = writePageOf({
    name: 'Q3 from Python',
    cubes: [{ id: 'grid-1', cube: trades.cube }, { id: 'grid-2', cube: desks.cube }],
    views: {
      views: [
        { id: 'grid-1', kind: 'grid', cube: 'grid-1' },
        { id: 'grid-2', kind: 'grid', cube: 'grid-2' },
        { id: 'chart-1', kind: 'chart', cube: 'grid-1', title: 'Notional by region', spec: defaultChart(trades.snapshot)! },
      ],
      sheets: [
        { id: 'sheet-1', layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { split: 'row', parts: [
          { node: { tile: 'grid-1' }, size: 0.5 }, { node: { tile: 'chart-1' }, size: 0.5 }] } }] } },
        { id: 'sheet-2', name: 'Desks', layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid-2' } }] } },
      ],
    },
  });
  return JSON.parse(pageToJson(doc));
};

run().then(
  () => { window.__pythonEngine = { done: true, outcomes }; },
  (e: unknown) => {
    window.__pythonEngine = { done: true, outcomes, setupError: e instanceof Error ? e.stack ?? e.message : String(e) };
  },
);
