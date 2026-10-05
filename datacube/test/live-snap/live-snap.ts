// LIVE VERSUS SNAP, over a real warehouse (docs/WAREHOUSE_D1_DESIGN_2026_09_26.md).
//
// One cube, two engines, one planner: every case of the cube corpus runs LIVE on
// the warehouse (the Bazel-built native binary), as a READER granted only the
// trades table, through the real WarehouseEngine; then the rows that reader may
// read are SNAPPED into DuckDB-WASM through the real SnapManager (the server's
// Arrow chunks, loaded unconverted) and every case runs again, locally. The
// answers must agree:
//
//   - a case whose SQL ends in ORDER BY is compared row for row; one without is
//     compared as a multiset (SQL promises no order, and the product must not
//     add one it did not ask for);
//   - the rows are unique on (book, year, qtr), the key every order-sensitive
//     window in the corpus orders through, so no answer depends on how an
//     engine orders ties;
//   - the pivots answer live too: each is two single SELECTs (its values, then
//     one groupBy), which a reader may run. None is refused; a new refusal
//     fails this test rather than passing unnoticed.
//
// Plus the source path a person takes: the catalog lists what the reader may
// read, inferModel turns it into a model, and a query over it agrees on both
// engines; a table the reader was not granted is refused.

import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { engineClientRequire } from '../../../engine-client/src/node-require.ts';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, before, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../../../engine-client/src/duckdb.ts';
import { inferModel } from '../../src/infer.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import { PlanThenRun } from '../../src/runner.ts';
import { SnapManager } from '../../../engine-client/src/snap.ts';
import { listObjects, SessionExpired, sessionExpired, signIn, WarehouseEngine } from '../../../engine-client/src/warehouse.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { levelLambda } from '../../src/query.ts';
import { accessor, from } from '../../../pure-protocol/src/index.ts';
import { CASES, MODEL, queries, RUNTIME } from '../wasm-differential/cases.ts';
import { runfileDirUrl, runfileFromEnv } from '../../../tools/js/runfiles.mts';

const MODULE_DIR = runfileDirUrl('WASM_PLANNER');
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

let server: ChildProcess;
let base = '';
let live: WarehouseEngine;
let local: DuckDbEngine;
let planner: WasmPlanner;

async function asOwner(sql: string, token: string): Promise<void> {
  const r = await fetch(`${base}/sql/v1/statements`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, catalog: 'main', waitMs: 30_000 }),
  });
  const s = await r.json() as { state: string; error?: { message: string } };
  assert.equal(s.state, 'succeeded', `${sql}: ${s.error?.message ?? ''}`);
}

before(async () => {
  const binary = runfileFromEnv('WAREHOUSE_BINARY');
  const library = runfileFromEnv('WAREHOUSE_DUCKDB_LIBRARY');
  // the test's own temp directory (Bazel's TEST_TMPDIR), never the host's
  const data = mkdtempSync(path.join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'live-snap-'));
  server = spawn(binary, ['--port', '0', '--data', data, '--user', 'alice:alice-pw', '--user', 'rita:rita-pw',
    '--owner', 'alice', '--duckdb-library', library], { stdio: ['ignore', 'ignore', 'pipe'] });
  const port = await new Promise<number>((ok, fail) => {
    let err = '';
    server.stderr!.on('data', (b: Buffer) => {
      err += String(b);
      // the warehouse's own account, in this test's log: a server-side reason is then never invisible
      for (const line of String(b).split('\n')) if (line) process.stderr.write(`[warehouse] ${line}\n`);
      const m = /listening on 127\.0\.0\.1:(\d+)/.exec(err);
      if (m) ok(Number(m[1]));
    });
    server.on('exit', (code) => fail(new Error(`the warehouse exited (${code}): ${err}`)));
  });
  base = `http://127.0.0.1:${port}`;

  const alice = await signIn(base, 'alice', 'alice-pw');
  for (const sql of DDL) await asOwner(sql, alice.token);
  live = new WarehouseEngine(await signIn(base, 'rita', 'rita-pw'));

  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
    eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  local = new DuckDbEngine(db.connect() as ArrowishConnection);
  planner = new WasmPlanner({ model: MODEL, runtime: RUNTIME, assetBaseUrl: MODULE_DIR, cache: false });
});

// stopped and WAITED for: the process is gone before the test reports (Bazel workplan P3-10)
after(async () => {
  if (server === undefined || server.exitCode !== null || server.signalCode !== null) return;
  const exited = new Promise<void>((ok) => server.once('exit', () => ok()));
  server.kill();
  await exited;
});

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

it('every cube case answers the same live on the warehouse and snapped in the tab', async () => {
  const liveRunner = new PlanThenRun(planner, live);
  const localRunner = new PlanThenRun(planner, local);

  const liveAnswers = new Map<string, { sql: string; rows: ResultTable } | { refused: string }>();
  for (const [i, { name, query }] of queries().entries()) {
    try {
      const out = await liveRunner.run(query, CASES[i]!.snapshot, CASES[i]!.scope);
      liveAnswers.set(name, out);
    } catch (e: unknown) {
      liveAnswers.set(name, { refused: e instanceof Error ? e.message : String(e) });
    }
  }

  // SNAP: exactly the rows the reader may read, through the real SnapManager
  const sourceSql = (await planner.plan(from(SOURCE).select(COLUMNS).lambda())).sql;
  const snaps = new SnapManager(local, live);
  const info = await snaps.snap(sourceSql, 0, { target: { table: 'TRADES', source: SOURCE, conversions: [] } });
  assert.equal(info.rowCount, 10, 'the snap holds every row the reader may read');

  const refused: string[] = [];
  const differ: string[] = [];
  let compared = 0;
  for (const [i, { name, query }] of queries().entries()) {
    const snapped = await localRunner.run(query, CASES[i]!.snapshot, CASES[i]!.scope);
    const l = liveAnswers.get(name)!;
    if ('refused' in l) {
      refused.push(name);
      assert.match(l.refused, /FORBIDDEN/, `${name} failed live for another reason: ${l.refused}`);
      continue;
    }
    const inOrder = ordered(l.sql);
    const a = shape(l.rows, inOrder);
    const b = shape(snapped.rows, inOrder);
    if (a !== b) differ.push(`${name}\n  live:    ${a.slice(0, 400)}\n  snapped: ${b.slice(0, 400)}`);
    compared += 1;
  }
  assert.deepEqual(refused, REFUSED_LIVE, 'the cases refused live changed');
  assert.deepEqual(differ, [], `live and snapped disagree:\n${differ.join('\n')}`);
  assert.equal(compared, CASES.length - REFUSED_LIVE.length);
  await snaps.release();
});

it('a person\'s path: the catalog, a model from it, the same answer on both engines', async () => {
  const objects = await listObjects({ baseUrl: base, token: (await signIn(base, 'rita', 'rita-pw')).token,
    principal: 'rita', expiresAt: '' });
  assert.deepEqual(objects.map((o) => `${o.schema}.${o.name}`), ['main.TRADES'],
    'the reader sees exactly what it was granted');
  const trades = objects[0]!;
  const m = inferModel(trades.columns.map((c) => ({ ...c, dataType: c.type })),
    { table: trades.name, schema: trades.schema, convertible: false, databaseType: 'DuckDB' });
  assert.deepEqual(m.excluded, []);
  const own = new WasmPlanner({ model: m.model, runtime: m.runtime, assetBaseUrl: MODULE_DIR, cache: false });
  const snapshot = { ...CASES[0]!.snapshot, source: { query: m.source }, rows: ['region'],
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' as const }], sorts: [{ column: 'region', direction: 'asc' as const }] };
  const pure = levelLambda(snapshot, { level: 1, parent: [] });
  const liveOut = await new PlanThenRun(own, live).run(pure, snapshot);
  const snaps = new SnapManager(local, live);
  await snaps.snap((await own.plan(from(m.source).select(COLUMNS).lambda())).sql, 0,
    { target: { schema: trades.schema, table: trades.name, source: m.source, conversions: m.conversions } });
  const localOut = await new PlanThenRun(own, local).run(pure, snapshot);
  assert.equal(shape(localOut.rows, true), shape(liveOut.rows, true));
  assert.equal(liveOut.rows.rowCount, 4);
  await snaps.release();
});

it('a table the reader was not granted is refused, live', async () => {
  await assert.rejects(live.run('SELECT * FROM secret', 0), /FORBIDDEN/);
});

it('every live answer carries the server\'s receipt, and the server confirms it; a snap names its pull', async () => {
  // Receipts come from what the server issued (its statement id, its count), and `check` asks
  // the server's own history -- which lists only the caller's statements -- apart from the query.
  const out = await new PlanThenRun(planner, live).run(queries()[0]!.query, CASES[0]!.snapshot, CASES[0]!.scope);
  const r = out.rows.receipt;
  assert.ok(r, 'a live answer has a receipt');
  assert.equal(r.plane, 'warehouse');
  assert.equal(r.as, 'rita');
  assert.match(r.statementId ?? '', /^[0-9a-f-]{36}$/);
  assert.equal(r.serverRows, out.rows.rowCount);
  assert.match(await r.check!(), new RegExp(`^On the warehouse's record for rita: statement ${r.statementId}, succeeded`));
  // an id the server never issued is not on its record
  assert.match(await live.check('00000000-0000-0000-0000-000000000000'), /^Not on the warehouse's record for rita/);

  const sourceSql = (await planner.plan(from(SOURCE).select(COLUMNS).lambda())).sql;
  const snaps = new SnapManager(local, live);
  const info = await snaps.snap(sourceSql, 0, { target: { table: 'TRADES', source: SOURCE, conversions: [] } });
  assert.equal(info.pulledBy?.as, 'rita');
  assert.match(await info.pulledBy!.check!(), /^On the warehouse's record for rita/);
  // snapped, the tab's own engine answers, and its receipt says so
  const here = await new PlanThenRun(planner, local).run(queries()[0]!.query, CASES[0]!.snapshot, CASES[0]!.scope);
  assert.equal(here.rows.receipt?.plane, 'tab');
  assert.equal(here.rows.receipt?.statementId, undefined);
  await snaps.release();
});

it('a token the warehouse no longer honours is SessionExpired, and signing in again goes on as the same user', async () => {
  // What a restarted warehouse does to an open cube: its token is refused.
  const stale = new WarehouseEngine({ baseUrl: base, token: 'cml0YXwx.bm90LWEtcmVhbC1zaWduYXR1cmU', principal: 'rita', expiresAt: '' });
  const refused = await stale.run('SELECT 1 AS one', 0).catch((e: unknown) => e);
  const expired = sessionExpired(refused);
  assert.ok(expired instanceof SessionExpired, `not a SessionExpired: ${String(refused)}`);
  assert.equal(expired.principal, 'rita');
  assert.equal(expired.baseUrl, base);

  await assert.rejects(stale.signInAgain('wrong'), /sign-in failed/);
  await stale.signInAgain('rita-pw');
  const again = await stale.run('SELECT 1 AS one', 0);
  assert.equal(again.rowCount, 1);
  assert.match(await again.receipt!.check!(), /^On the warehouse's record for rita/);
});

it('the token refreshes: on asking, and by itself before it expires', async () => {
  const s = await signIn(base, 'rita', 'rita-pw');
  const engine = new WarehouseEngine(s);
  await engine.refreshToken();
  assert.ok(Date.parse(engine.expiresAt) >= Date.parse(s.expiresAt), 'a refreshed token lives at least as long');
  const out = await engine.run('SELECT 1 AS one', 0);
  assert.match(await out.receipt!.check!(), /^On the warehouse's record for rita/, 'the fresh token is rita, on the server');
  await engine.close();

  // BY ITSELF: a session the page believes ends in 1.5s is refreshed at 80% of that, unasked. The engine's clock is
  // the test's (Bazel workplan P3-16): the timer is seen scheduled at 1.2s and fired, nobody sleeps
  const t0 = Date.now();
  const timers: { fn: () => void; ms: number }[] = [];
  const clock = { now: () => t0, setTimeout: (fn: () => void, ms: number) => timers.push({ fn, ms }), clearTimeout: () => {} };
  const soon = new WarehouseEngine({ ...s, expiresAt: new Date(t0 + 1_500).toISOString() }, 'main', clock);
  const before = soon.expiresAt;
  assert.equal(timers.at(-1)?.ms, 1_200, 'the refresh is scheduled at 80% of the token\'s life');
  timers.at(-1)!.fn();
  // the refresh is a request to the server: its answer, not a clock, ends this wait
  for (let i = 0; i < 1_000 && soon.expiresAt === before; i++) await new Promise((r) => setTimeout(r, 10));
  assert.notEqual(soon.expiresAt, before, 'the timer swapped the token');
  assert.ok(Date.parse(soon.expiresAt) > Date.now() + 30 * 60_000, 'for one with the server\'s full life');
  assert.equal((await soon.run('SELECT 1 AS one', 0)).rowCount, 1);
  await soon.close();
});
