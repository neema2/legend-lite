// Snap mode against a real DuckDB-WASM, because the interesting
// behaviour is what actually materialises.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import {
  MAX_PIVOT_CELLS,
  SnapManager,
  SnapRefusal,
} from '../src/snap.ts';

const require = createRequire(import.meta.url);
let engine: DuckDbEngine;

before(async () => {
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
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
  await engine.execute(
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
    assert.equal(s.sourceFor('trades'), 'trades');
  });

  it('freezes rows and reports what was taken, and when', async () => {
    const s = new SnapManager(engine);
    const before = Date.now();
    const info = await s.snap('SELECT * FROM trades', 1, {
      pivotCandidates: ['year'],
    });

    assert.equal(info.rowCount, 1000);
    assert.ok(info.takenAt.getTime() >= before);
    assert.match(info.label, /^Snap \d\d:\d\d$/);
    assert.equal(s.isSnapped, true);
    // Queries now read the frozen table, not the live source.
    assert.notEqual(s.sourceFor('trades'), 'trades');
  });

  it('captures pivot values once, so no discovery pass per query', async () => {
    const s = new SnapManager(engine);
    await s.snap('SELECT * FROM trades', 1, { pivotCandidates: ['year'] });
    assert.deepEqual(s.valuesFor('year'), ['2020', '2021', '2022', '2023']);
    assert.deepEqual(s.literalsFor('year'), [
      "'2020'",
      "'2021'",
      "'2022'",
      "'2023'",
    ]);
  });

  it('is genuinely frozen: live changes do not reach the snap', async () => {
    const s = new SnapManager(engine);
    await s.snap('SELECT * FROM trades', 1);
    const snapped = s.sourceFor('trades');

    // The world moves on underneath.
    await engine.execute(
      'INSERT INTO trades SELECT 9, 9, 2024, 1.0',
      2,
    );

    const fromSnap = await engine.execute(
      `SELECT count(*) AS n FROM ${snapped}`,
      3,
    );
    const fromLive = await engine.execute(
      'SELECT count(*) AS n FROM trades',
      4,
    );
    assert.equal(fromSnap.columns[0]?.values[0], 1000, 'snap is stable');
    assert.equal(fromLive.columns[0]?.values[0], 1001, 'live moved');

    // Clean up so later tests see the original table.
    await engine.execute('DELETE FROM trades WHERE year = 2024', 5);
  });

  it('returns to live on release', async () => {
    const s = new SnapManager(engine);
    await s.snap('SELECT * FROM trades', 1);
    await s.release();
    assert.equal(s.state.mode, 'live');
    assert.equal(s.sourceFor('trades'), 'trades');
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
      () => s.snap('SELECT * FROM range(20000000)', 2),
      (e: unknown) => {
        assert.ok(e instanceof SnapRefusal);
        return true;
      },
    );
    // Refused means nothing was created and we are still live.
    assert.equal(s.isSnapped, false);
  });

  it('refuses a pivot past the measured cell budget, before running it', () => {
    const s = new SnapManager(engine);
    // Both numbers are known in advance, so this is a pre-flight check
    // rather than a timeout after the user has already waited.
    assert.throws(
      () => s.checkCellBudget(4000, 1000),
      (e: unknown) => {
        assert.ok(e instanceof SnapRefusal);
        assert.match((e as Error).message, /4,000,000 cells/);
        assert.match((e as Error).message, /Remove a dimension/);
        return true;
      },
    );
    // Just inside the budget is allowed.
    assert.doesNotThrow(() => s.checkCellBudget(2000, 500));
    assert.equal(2000 * 500, MAX_PIVOT_CELLS);
  });

  it('gives each snap its own table so two can coexist', async () => {
    const s = new SnapManager(engine);
    const a = await s.snap('SELECT * FROM trades WHERE region = 0', 1);
    const b = await s.snap('SELECT * FROM trades WHERE region = 1', 2);
    assert.notEqual(a.table, b.table);
    // Snap A survives taking snap B, which is what makes an
    // as-of-A vs as-of-B comparison possible.
    const stillThere = await engine.execute(
      `SELECT count(*) AS n FROM "${a.table}"`,
      3,
    );
    assert.equal(stillThere.columns[0]?.values[0], 200);
  });
});
