// Snap mode against a real DuckDB-WASM, because the interesting
// behaviour is what actually materialises.

import assert from 'node:assert/strict';
import { engineClientRequire } from '../../engine-client/src/node-require.ts';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import { element } from '../../pure-protocol/src/index.ts';

/** The live source, and where a snap goes: the model would declare both. */
const LIVE = element('trades');
const target = (table: string) => ({ table, source: element(table), conversions: [] });
import {
  SnapManager,
  SnapRefusal,
} from '../src/snap.ts';

let engine: DuckDbEngine;

before(async () => {
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB(
    {
      mvp: {
        mainModule: path.join(dist, 'duckdb-mvp.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs'),
      },
      eh: {
        mainModule: path.join(dist, 'duckdb-eh.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs'),
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
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
    const s = new SnapManager(engine);
    assert.equal(s.state.mode, 'live');
    assert.equal(s.isSnapped, false);
    assert.equal(s.sourceFor(LIVE), LIVE);
  });

  it('freezes rows and reports what was taken, and when', async () => {
    const s = new SnapManager(engine);
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
    const s = new SnapManager(engine);
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
    const s = new SnapManager(engine);
    await s.snap('SELECT * FROM trades', 1, { target: target('snap_released') });
    await s.release();
    assert.equal(s.state.mode, 'live');
    assert.equal(s.sourceFor(LIVE), LIVE);
  });

  it('refuses a snap beyond the row ceiling, before materialising', async () => {
    const s = new SnapManager(engine);
    const estimate = await s.preflight(
      'SELECT * FROM range(20000000)',
      1,
    );
    assert.equal(estimate.withinLimit, false);
    assert.match(estimate.refusal ?? '', /exceeds the .* row snap limit/);

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
    const s = new SnapManager(engine);
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
});
