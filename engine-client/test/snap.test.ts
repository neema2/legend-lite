// Snap mode against a real DuckDB-WASM, because the interesting
// behaviour is what actually materialises.

import assert from 'node:assert/strict';
import { startNodeDuckDb } from '../src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import type { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import { loadPlanner } from './planner.ts';
import { element } from '../../pure-protocol/src/index.ts';

/** The live source, and where a snap goes: a table of its own, read (once landed) by what the app's model of it says. */
const LIVE = element('trades');
const target = (table: string) => ({ table, landed: async () => element(table) });
import {
  MAX_SNAP_ROWS,
  SnapManager,
  SnapRefusal,
  snapRefusal,
} from '../src/snap.ts';

let engine: DuckDbEngine;

/** The planner, which writes each copy's statements (D5). */
let planner: WasmGrammar;

before(async () => {
  planner = await loadPlanner();
  const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  await engine.run(
    `CREATE TABLE trades AS
       SELECT (i%5) AS region, (i%20) AS book,
              CAST(2020 + (i%4) AS INTEGER) AS year,
              (i*7.5) AS notional
       FROM range(1000) t(i)`,
    0,
  );
});

after(async () => {
  await engine?.close();
});

describe('SnapManager', () => {
  it('starts live and reads from the live source', () => {
    const s = new SnapManager(engine, null, planner);
    assert.equal(s.state.mode, 'live');
    assert.equal(s.isSnapped, false);
    assert.equal(s.sourceFor(LIVE), LIVE);
  });

  it('freezes rows and reports what was taken, and when', async () => {
    const s = new SnapManager(engine, null, planner);
    const before = Date.now();
    const info = await s.snap('SELECT * FROM trades', 1, { target: target('snap_a') });

    assert.equal(info.rowCount, 1000);
    assert.ok(info.takenAt.getTime() >= before);
    assert.match(info.label, /^Snap \d\d:\d\d$/);
    assert.equal(s.isSnapped, true);
    // Queries now read the frozen table, not the live source.
    assert.deepEqual(s.sourceFor(LIVE), element('snap_a'));
  });

  it('is genuinely frozen: live changes do not reach the snap', async () => {
    // its own live table, so the row it adds reaches no other test (Bazel workplan P3-10)
    await engine.run('CREATE TABLE trades_frozen AS SELECT * FROM trades', 0);
    const s = new SnapManager(engine, null, planner);
    const { table: snapped } = await s.snap('SELECT * FROM trades_frozen', 1, { target: target('snap_frozen') });

    // The world moves on underneath.
    await engine.run(
      'INSERT INTO trades_frozen SELECT 9, 9, 2024, 1.0',
      2,
    );

    const fromSnap = await engine.run(
      `SELECT count(*) AS n FROM ${snapped}`,
      3,
    );
    const fromLive = await engine.run(
      'SELECT count(*) AS n FROM trades_frozen',
      4,
    );
    assert.equal(fromSnap.columns[0]?.values[0], 1000, 'snap is stable');
    assert.equal(fromLive.columns[0]?.values[0], 1001, 'live moved');
  });

  it('returns to live on release', async () => {
    const s = new SnapManager(engine, null, planner);
    await s.snap('SELECT * FROM trades', 1, { target: target('snap_released') });
    await s.release();
    assert.equal(s.state.mode, 'live');
    assert.equal(s.sourceFor(LIVE), LIVE);
  });

  it('refuses a snap beyond the row ceiling, before materialising', async () => {
    const s = new SnapManager(engine, null, planner);
    const estimate = await s.preflight(
      'SELECT * FROM range(20000000)',
      1,
    );
    assert.equal(estimate.withinLimit, false);
    // the one sentence (snapRefusal), which Python's engine says too with its machine's limit (headless snapRefused)
    assert.equal(estimate.refusal, snapRefusal(20_000_000, MAX_SNAP_ROWS));
    assert.equal(snapRefusal(20_000_000, 10_000_000),
      '20,000,000 rows exceeds the 10,000,000 row snap limit. Narrow the filter, or stay live.');

    await assert.rejects(
      () => s.snap('SELECT * FROM range(20000000)', 2, { target: target('snap_huge') }),
      (e: unknown) => {
        assert.ok(e instanceof SnapRefusal);
        return true;
      },
    );
    // Refused means nothing was created and we are still live.
    assert.equal(s.isSnapped, false);
  });

  it('gives each snap its own table so two can coexist', async () => {
    const s = new SnapManager(engine, null, planner);
    const a = await s.snap('SELECT * FROM trades WHERE region = 0', 1, { target: target('snap_region_0') });
    const b = await s.snap('SELECT * FROM trades WHERE region = 1', 2, { target: target('snap_region_1') });
    assert.notEqual(a.table, b.table);
    // Snap A survives taking snap B, which is what makes an
    // as-of-A vs as-of-B comparison possible.
    const stillThere = await engine.run(
      `SELECT count(*) AS n FROM "${a.table}"`,
      3,
    );
    assert.equal(stillThere.columns[0]?.values[0], 200);
  });

  it('drops the copy, live still, when the app cannot model what landed (§18.3)', async () => {
    const s = new SnapManager(engine, null, planner);
    await assert.rejects(s.snap('SELECT * FROM trades WHERE region = 2', 4, {
      target: { table: 'snap_refused', landed: async () => { throw new Error('no model of it'); } },
    }), /no model of it/);
    assert.equal(s.isSnapped, false);
    const left = await engine.run("SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'snap_refused'", 5);
    assert.equal(Number(left.columns[0]?.values[0]), 0, 'the copy is gone');
  });
});
