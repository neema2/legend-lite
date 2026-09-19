// Why there are two extend stages.
//
// A ratio computed per source row and then averaged is not the same
// number as a ratio computed from the aggregates, and the second is
// almost always the one the user meant. This proves the difference
// against a real engine rather than asserting it.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { serialize } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

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

  // One region, two deliberately lopsided rows: a tiny deal at a
  // great margin and a large deal at a poor one.
  await engine.execute(
    `CREATE TABLE deals AS SELECT * FROM (VALUES
       ('EMEA', 100.0, 90.0),
       ('EMEA', 9900.0, 990.0)
     ) v(region, revenue, profit)`,
    0,
  );
});

after(async () => {
  await engine?.close();
});

const BASE: CubeSnapshot = {
  source: { expression: 'deals' },
  columns: [],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [
    { name: 'revenue', column: 'revenue', fn: 'sum' },
    { name: 'profit', column: 'profit', fn: 'sum' },
  ],
  sorts: [],
  epoch: 1,
};

describe('the two extend stages are not interchangeable', () => {
  it('per-row margin then averaged gives the WRONG answer', async () => {
    // 90/100 = 0.9 and 990/9900 = 0.1, averaged = 0.5.
    const r = await engine.execute(
      `SELECT region, avg(profit / revenue) AS margin
         FROM deals GROUP BY region`,
      1,
    );
    assert.equal(Number(r.columns[1]?.values[0]).toFixed(3), '0.500');
  });

  it('margin from the aggregates gives the RIGHT answer', async () => {
    // (90 + 990) / (100 + 9900) = 1080/10000 = 0.108.
    const r = await engine.execute(
      `SELECT region, sum(profit) / sum(revenue) AS margin
         FROM deals GROUP BY region`,
      1,
    );
    assert.equal(Number(r.columns[1]?.values[0]).toFixed(3), '0.108');
  });

  it('so they are different by a factor of nearly five here', () => {
    // Not a rounding difference: a weighted average and an unweighted
    // one answer different questions, and a pivot that offers only
    // the first cannot express the second.
    assert.ok(Math.abs(0.5 - 0.108) > 0.39);
  });
});

describe('serialize places each stage correctly', () => {
  it('puts a leaf-derived column BEFORE the aggregation', () => {
    const out = serialize({
      ...BASE,
      derived: [{ name: 'net', expression: '$x.revenue - $x.profit' }],
    });
    const extendAt = out.indexOf('extend(~[net');
    const groupAt = out.indexOf('groupBy(');
    assert.ok(extendAt >= 0 && groupAt >= 0);
    assert.ok(extendAt < groupAt, 'leaf extend precedes the grouping');
  });

  it('puts a group-derived column AFTER the aggregation', () => {
    const out = serialize({
      ...BASE,
      groupDerived: [
        { name: 'margin', expression: '$x.profit / $x.revenue' },
      ],
    });
    const groupAt = out.indexOf('groupBy(');
    const extendAt = out.indexOf('extend(~[margin');
    assert.ok(groupAt >= 0 && extendAt >= 0);
    assert.ok(groupAt < extendAt, 'group extend follows the grouping');
  });

  it('places it after a PIVOT too, not just a groupBy', () => {
    const out = serialize({
      ...BASE,
      pivotOn: ['region'],
      groupDerived: [{ name: 'margin', expression: '$x.profit' }],
    });
    assert.ok(out.indexOf('pivot(') < out.indexOf('extend(~[margin'));
  });

  it('emits both stages in one pipeline, in order', () => {
    const out = serialize({
      ...BASE,
      derived: [{ name: 'net', expression: '$x.revenue - $x.profit' }],
      groupDerived: [{ name: 'margin', expression: '$x.profit / $x.revenue' }],
    });
    assert.ok(
      out.indexOf('extend(~[net') <
        out.indexOf('groupBy(') &&
        out.indexOf('groupBy(') < out.indexOf('extend(~[margin'),
    );
  });

  it('and the group stage lands before the sort and the cap', () => {
    // Otherwise a cube could not sort by a derived measure, which is
    // the commonest thing to want to do with one.
    const out = serialize(
      {
        ...BASE,
        groupDerived: [{ name: 'margin', expression: '$x.profit' }],
        sorts: [{ column: 'margin', direction: 'desc' }],
      },
      { level: 1, parent: [], limit: 11 },
    );
    assert.ok(out.indexOf('extend(~[margin') < out.indexOf('sort('));
    assert.ok(out.indexOf('sort(') < out.indexOf('limit('));
  });
});
