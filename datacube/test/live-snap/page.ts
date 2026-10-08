// LIVE VERSUS SNAP, over a real warehouse (docs/WAREHOUSE_D1_DESIGN_2026_09_26.md), IN THE BROWSER: this page runs
// every check where the product runs -- the pinned Chromium, DuckDB-WASM in its worker, legend-lite's compiler in its
// worker, the browser's own fetch -- served by the Bazel-built native warehouse itself (`--site`, one origin). The
// harness (live-snap.mjs) only starts the warehouse, opens this page and reads what it found: nothing is checked in
// Node. (It ran in Node until 2026-10-08, DuckDB's blocking build on Node's thread: under load that held the thread
// past the warehouse's idle close of a pooled connection, and the next request went out on the dead socket.)
//
// One cube, two engines, one planner: every case of the cube corpus runs LIVE on the warehouse, as a READER granted
// only the trades table, through the real WarehouseEngine; then the rows that reader may read are SNAPPED into the
// tab's DuckDB through the real SnapManager (the server's Arrow chunks, loaded unconverted) and every case runs again,
// locally. The answers must agree:
//
//   - a case whose SQL ends in ORDER BY is compared row for row; one without is compared as a multiset (SQL promises
//     no order, and the product must not add one it did not ask for);
//   - the rows are unique on (book, year, qtr), the key every order-sensitive window in the corpus orders through, so
//     no answer depends on how an engine orders ties;
//   - the pivots answer live too: each is two single SELECTs (its values, then one groupBy), which a reader may run.
//     None is refused; a new refusal fails this test rather than passing unnoticed.
//
// Plus the source path a person takes: the catalog lists what the reader may read, legend-lite's writer turns it into
// a model, and a query over it agrees on both engines; a table the reader was not granted is refused; receipts, an
// expired session, and the token's refresh.

import { startDuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import type { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import { SnapManager } from '../../../engine-client/src/snap.ts';
import { listObjects, SessionExpired, sessionExpired, signIn, WarehouseEngine } from '../../../engine-client/src/warehouse.ts';
import { accessor, from } from '../../../pure-protocol/src/index.ts';
import { levelLambda } from '../../src/query.ts';
import { PlanThenRun } from '../../src/runner.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { CASES, MODEL, queries, RUNTIME } from '../wasm-differential/cases.ts';

const SOURCE = accessor('trades::DB', 'TRADES');
const COLUMNS = ['region', 'desk', 'book', 'year', 'qtr', 'notional', 'pnl', 'qty'];

/** The cases a READER cannot run live: none (they were the dynamic pivots). */
const REFUSED_LIVE: string[] = [];

// Unique on (book, year, qtr); NULLs in the measures and in a dimension.
const ROWS = `
  ('EMEA','Rates','B1',2023,'Q1',100.5,1.25,10), ('EMEA','Rates','B1',2024,'Q2',200.25,-3.5,20),
  ('EMEA','FX','B3',2023,'Q1',50.0,0.5,5),       ('EMEA','FX','B3',2023,'Q3',75.0,2.0,6),
  ('AMER','Rates','B2',2024,'Q3',300.75,7.0,30), ('AMER','FX','B4',2023,'Q4',NULL,NULL,7),
  ('AMER','Credit','B5',2022,'Q1',12.5,0.25,NULL),('APAC','Credit','B6',2024,'Q1',75.0,2.0,3),
  ('APAC','Rates','B7',2022,'Q2',10.0,0.0,1),     (NULL,'Rates','B8',2024,'Q4',42.0,-1.0,4)`;
const DDL = [
  `CREATE TABLE TRADES (region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32), year INTEGER,
     qtr VARCHAR(8), notional DOUBLE, pnl DOUBLE, qty INTEGER)`,
  `INSERT INTO TRADES VALUES ${ROWS}`,
  `CREATE TABLE secret (s VARCHAR)`,
  `INSERT INTO secret VALUES ('the secret')`,
  `GRANT SELECT ON TABLE TRADES TO rita`,
];

// --- checks, as node:assert words them -------------------------------------------------------------------------------

class CheckFailed extends Error {}

function ok(value: unknown, message: string): asserts value {
  if (!value) throw new CheckFailed(message);
}

function equal(actual: unknown, expected: unknown, message = ''): void {
  if (actual !== expected) throw new CheckFailed(`${message} expected ${String(expected)}, got ${String(actual)}`.trim());
}

function deepEqual(actual: unknown, expected: unknown, message = ''): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new CheckFailed(`${message} expected ${e}, got ${a}`.trim());
}

function match(text: string, pattern: RegExp, message = ''): void {
  if (!pattern.test(text)) throw new CheckFailed(`${message} ${JSON.stringify(text.slice(0, 300))} does not match ${pattern}`.trim());
}

async function rejects(p: Promise<unknown>, pattern: RegExp): Promise<void> {
  const outcome = await p.then(() => undefined, (e: unknown) => e);
  ok(outcome !== undefined, `expected a refusal matching ${pattern}, but it succeeded`);
  match(outcome instanceof Error ? outcome.message : String(outcome), pattern);
}

/** A result as comparable text: its columns, then its rows (sorted unless ordered). */
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

// --- the setup and the cases -------------------------------------------------------------------------------------------

interface Outcome {
  readonly name: string;
  readonly ok: boolean;
  readonly message?: string;
  readonly ms: number;
}

declare global {
  interface Window {
    __liveSnap?: { readonly done: boolean; readonly outcomes: readonly Outcome[]; readonly setupError?: string };
    /** Where the page is: the harness says it when the page does not finish. */
    __liveSnapStage?: string;
  }
}

const base = location.origin;
const outcomes: Outcome[] = [];

async function asOwner(sql: string, token: string): Promise<void> {
  const r = await fetch(`${base}/sql/v1/statements`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, catalog: 'main', waitMs: 30_000 }),
  });
  const s = await r.json() as { state: string; error?: { message: string } };
  equal(s.state, 'succeeded', `${sql}: ${s.error?.message ?? ''}`);
}

async function check(name: string, body: () => Promise<void>): Promise<void> {
  window.__liveSnapStage = name;
  const t0 = performance.now();
  try {
    await body();
    outcomes.push({ name, ok: true, ms: performance.now() - t0 });
  } catch (e: unknown) {
    outcomes.push({ name, ok: false, message: e instanceof Error ? e.stack ?? e.message : String(e), ms: performance.now() - t0 });
  }
}

async function run(): Promise<void> {
  window.__liveSnapStage = 'signing in, writing the tables';
  const alice = await signIn(base, 'alice', 'alice-pw');
  for (const sql of DDL) await asOwner(sql, alice.token);
  const live = new WarehouseEngine(await signIn(base, 'rita', 'rita-pw'));
  // the tab's own DuckDB, in its worker, from the site's vendor/ (as Query and Studio start theirs)
  window.__liveSnapStage = 'starting the tab\'s DuckDB';
  const local: DuckDbEngine = (await startDuckDbInTab('vendor/')).engine;
  const worker = new URL('planner-worker.js', location.href).href;
  const planner = new WasmPlanner({ model: MODEL, runtime: RUNTIME, workerUrl: worker });

  await check('every cube case answers the same live on the warehouse and snapped in the tab', async () => {
    const liveRunner = new PlanThenRun(planner, live);
    const localRunner = new PlanThenRun(planner, local);
    const liveAnswers = new Map<string, { sql: string; rows: ResultTable } | { refused: string }>();
    for (const [i, { name, query }] of queries().entries()) {
      try {
        liveAnswers.set(name, await liveRunner.run(query, CASES[i]!.snapshot, CASES[i]!.scope));
      } catch (e: unknown) {
        liveAnswers.set(name, { refused: e instanceof Error ? e.message : String(e) });
      }
    }
    // SNAP: exactly the rows the reader may read, through the real SnapManager
    const sourceSql = (await planner.plan(from(SOURCE).select(COLUMNS).lambda())).sql;
    const snaps = new SnapManager(local, live);
    const info = await snaps.snap(sourceSql, 0, { target: { table: 'TRADES', source: SOURCE, conversions: [] } });
    equal(info.rowCount, 10, 'the snap holds every row the reader may read:');
    const refused: string[] = [];
    const differ: string[] = [];
    let compared = 0;
    for (const [i, { name, query }] of queries().entries()) {
      const snapped = await localRunner.run(query, CASES[i]!.snapshot, CASES[i]!.scope);
      const l = liveAnswers.get(name)!;
      if ('refused' in l) {
        refused.push(name);
        match(l.refused, /FORBIDDEN/, `${name} failed live for another reason:`);
        continue;
      }
      const inOrder = ordered(l.sql);
      const a = shape(l.rows, inOrder);
      const b = shape(snapped.rows, inOrder);
      if (a !== b) differ.push(`${name}\n  live:    ${a.slice(0, 400)}\n  snapped: ${b.slice(0, 400)}`);
      compared += 1;
    }
    deepEqual(refused, REFUSED_LIVE, 'the cases refused live changed:');
    deepEqual(differ, [], 'live and snapped disagree:');
    equal(compared, CASES.length - REFUSED_LIVE.length, 'cases compared:');
    await snaps.release();
  });

  await check('a person\'s path: the catalog, a model from it, the same answer on both engines', async () => {
    const objects = await listObjects({ baseUrl: base, token: (await signIn(base, 'rita', 'rita-pw')).token,
      principal: 'rita', expiresAt: '' });
    deepEqual(objects.map((o) => `${o.schema}.${o.name}`), ['main.TRADES'], 'the reader sees exactly what it was granted:');
    const trades = objects[0]!;
    // legend-lite's one model writer: the planner's own module (infer.ts `TableModels`)
    const m = await planner.tableModel(trades.columns.map((c) => ({ ...c, dataType: c.type })),
      { table: trades.name, schema: trades.schema, convertible: false, databaseType: 'DuckDB' });
    deepEqual(m.excluded, []);
    const own = planner.withModel(m.model, m.runtime);
    const snapshot = { ...CASES[0]!.snapshot, source: { query: m.source }, rows: ['region'],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' as const }], sorts: [{ column: 'region', direction: 'asc' as const }] };
    const pure = levelLambda(snapshot, { level: 1, parent: [] });
    const liveOut = await new PlanThenRun(own, live).run(pure, snapshot);
    const snaps = new SnapManager(local, live);
    await snaps.snap((await own.plan(from(m.source).select(COLUMNS).lambda())).sql, 0,
      { target: { schema: trades.schema, table: trades.name, source: m.source, conversions: m.conversions } });
    const localOut = await new PlanThenRun(own, local).run(pure, snapshot);
    equal(shape(localOut.rows, true), shape(liveOut.rows, true), 'live and snapped:');
    equal(liveOut.rows.rowCount, 4, 'groups:');
    await snaps.release();
  });

  await check('a table the reader was not granted is refused, live', async () => {
    await rejects(live.run('SELECT * FROM secret', 0), /FORBIDDEN/);
  });

  await check('every live answer carries the server\'s receipt, and the server confirms it; a snap names its pull', async () => {
    // Receipts come from what the server issued (its statement id, its count), and `check` asks the server's own
    // history -- which lists only the caller's statements -- apart from the query.
    const out = await new PlanThenRun(planner, live).run(queries()[0]!.query, CASES[0]!.snapshot, CASES[0]!.scope);
    const r = out.rows.receipt;
    ok(r, 'a live answer has a receipt');
    equal(r.plane, 'warehouse');
    equal(r.as, 'rita');
    match(r.statementId ?? '', /^[0-9a-f-]{36}$/);
    equal(r.serverRows, out.rows.rowCount);
    match(await r.check!(), new RegExp(`^On the warehouse's record for rita: statement ${r.statementId}, succeeded`));
    // an id the server never issued is not on its record
    match(await live.check('00000000-0000-0000-0000-000000000000'), /^Not on the warehouse's record for rita/);
    const sourceSql = (await planner.plan(from(SOURCE).select(COLUMNS).lambda())).sql;
    const snaps = new SnapManager(local, live);
    const info = await snaps.snap(sourceSql, 0, { target: { table: 'TRADES', source: SOURCE, conversions: [] } });
    equal(info.pulledBy?.as, 'rita');
    match(await info.pulledBy!.check!(), /^On the warehouse's record for rita/);
    // snapped, the tab's own engine answers, and its receipt says so
    const here = await new PlanThenRun(planner, local).run(queries()[0]!.query, CASES[0]!.snapshot, CASES[0]!.scope);
    equal(here.rows.receipt?.plane, 'tab');
    equal(here.rows.receipt?.statementId, undefined);
    await snaps.release();
  });

  await check('a token the warehouse no longer honours is SessionExpired, and signing in again goes on as the same user', async () => {
    // What a restarted warehouse does to an open cube: its token is refused.
    const stale = new WarehouseEngine({ baseUrl: base, token: 'cml0YXwx.bm90LWEtcmVhbC1zaWduYXR1cmU', principal: 'rita', expiresAt: '' });
    const refused = await stale.run('SELECT 1 AS one', 0).catch((e: unknown) => e);
    const expired = sessionExpired(refused);
    ok(expired instanceof SessionExpired, `not a SessionExpired: ${String(refused)}`);
    equal(expired.principal, 'rita');
    equal(expired.baseUrl, base);
    await rejects(stale.signInAgain('wrong'), /sign-in failed/);
    await stale.signInAgain('rita-pw');
    const again = await stale.run('SELECT 1 AS one', 0);
    equal(again.rowCount, 1);
    match(await again.receipt!.check!(), /^On the warehouse's record for rita/);
  });

  await check('the token refreshes: on asking, and by itself before it expires', async () => {
    const s = await signIn(base, 'rita', 'rita-pw');
    const engine = new WarehouseEngine(s);
    await engine.refreshToken();
    ok(Date.parse(engine.expiresAt) >= Date.parse(s.expiresAt), 'a refreshed token lives at least as long');
    const out = await engine.run('SELECT 1 AS one', 0);
    match(await out.receipt!.check!(), /^On the warehouse's record for rita/, 'the fresh token is rita, on the server:');
    await engine.close();
    // BY ITSELF: a session the page believes ends in 1.5s is refreshed at 80% of that, unasked. The engine's clock is
    // the test's (Bazel workplan P3-16): the timer is seen scheduled at 1.2s and fired, nobody sleeps
    const t0 = Date.now();
    const timers: { fn: () => void; ms: number }[] = [];
    const clock = { now: () => t0, setTimeout: (fn: () => void, ms: number) => timers.push({ fn, ms }), clearTimeout: () => {} };
    const soon = new WarehouseEngine({ ...s, expiresAt: new Date(t0 + 1_500).toISOString() }, 'main', clock);
    const before = soon.expiresAt;
    equal(timers.at(-1)?.ms, 1_200, 'the refresh is scheduled at 80% of the token\'s life:');
    timers.at(-1)!.fn();
    // the refresh is a request to the server: its answer, not a clock, ends this wait
    for (let i = 0; i < 1_000 && soon.expiresAt === before; i++) await new Promise((r) => setTimeout(r, 10));
    ok(soon.expiresAt !== before, 'the timer swapped the token');
    ok(Date.parse(soon.expiresAt) > Date.now() + 30 * 60_000, 'for one with the server\'s full life');
    equal((await soon.run('SELECT 1 AS one', 0)).rowCount, 1);
    await soon.close();
  });
}

run().then(
  () => { window.__liveSnap = { done: true, outcomes }; },
  (e: unknown) => {
    window.__liveSnap = { done: true, outcomes, setupError: e instanceof Error ? e.stack ?? e.message : String(e) };
  },
);
