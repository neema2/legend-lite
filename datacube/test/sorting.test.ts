// Sorting, including the case that needed checking: ordering rows by
// a PIVOTED column, which only exists after the pivot has run.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { serialize } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { totalOrderSorts } from '../src/snapshot.ts';

let engine: DuckDbEngine;

before(async () => {
  const require = createRequire(import.meta.url);
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
    `CREATE TABLE t AS SELECT * FROM (VALUES
       ('AMER', 2023,  10.0), ('AMER', 2024, 900.0),
       ('EMEA', 2023, 500.0), ('EMEA', 2024,  20.0),
       ('APAC', 2023, 100.0), ('APAC', 2024, 100.0)
     ) v(region, yr, amt)`,
    0,
  );
});

after(async () => {
  await engine?.close();
});

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [],
  derived: [],
  rows: ['region'],
  pivotOn: ['yr'],
  measures: [{ name: 'total', column: 'amt', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('sorting by a pivoted column', () => {
  it('keeps the pivoted column in the sort keys', () => {
    // It is not a row dimension, so the total-order pass must let it
    // through rather than filtering it out as "not of this level".
    const s: CubeSnapshot = {
      ...CUBE,
      sorts: [{ column: '2023__|__total', direction: 'desc' }],
    };
    assert.deepEqual(totalOrderSorts(s, ['region']), [
      { column: '2023__|__total', direction: 'desc' },
      { column: 'region', direction: 'asc' },
    ]);
  });

  it('emits the sort AFTER the pivot, where the column exists', () => {
    const out = serialize({
      ...CUBE,
      sorts: [{ column: '2023__|__total', direction: 'desc' }],
    });
    assert.ok(out.indexOf('pivot(') < out.indexOf('sort('));
    assert.match(out, /sort\(\[~'2023__\|__total'->descending\(\)/);
  });

  it('actually orders rows by a pivot cell, against the engine', async () => {
    // 2023: EMEA 500, APAC 100, AMER 10 -- an order no row dimension
    // would produce, so this proves the sort is on the cell value.
    const r = await engine.execute(
      `SELECT * FROM (PIVOT t ON yr IN (2023, 2024) USING sum(amt) AS v
         GROUP BY region) ORDER BY "2023_v" DESC`,
      1,
    );
    assert.deepEqual(r.columns[0]?.values, ['EMEA', 'APAC', 'AMER']);
  });

  it('a different pivot column gives a different order', async () => {
    // 2024: AMER 900, APAC 100, EMEA 20 -- the reverse of 2023's.
    const r = await engine.execute(
      `SELECT * FROM (PIVOT t ON yr IN (2023, 2024) USING sum(amt) AS v
         GROUP BY region) ORDER BY "2024_v" DESC`,
      1,
    );
    assert.deepEqual(r.columns[0]?.values, ['AMER', 'APAC', 'EMEA']);
  });

  it('still appends the row dimension, so ties are deterministic', () => {
    // Without it, two groups tying on the pivot cell can come back in
    // either order and a windowed read can repeat or skip one.
    const out = serialize({
      ...CUBE,
      sorts: [{ column: '2024__|__total', direction: 'desc' }],
    });
    assert.match(out, /~region->ascending\(\)\]\)/);
  });
});

describe('sorting by a derived measure', () => {
  it('sorts on a post-aggregation column', () => {
    const out = serialize({
      ...CUBE,
      pivotOn: [],
      groupDerived: [{ name: 'share', expression: '$x.total / 100' }],
      sorts: [{ column: 'share', direction: 'desc' }],
    });
    // The column must exist before it is ordered by.
    assert.ok(out.indexOf('extend(~[share') < out.indexOf('sort('));
    assert.match(out, /sort\(\[~share->descending\(\)/);
  });
});
