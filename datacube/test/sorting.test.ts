// Sorting, including the case that needed checking: ordering rows by
// a PIVOTED column, which only exists after the pivot has run.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';

import type { CubeSnapshot } from '../src/snapshot.ts';
import { totalOrderSorts } from '../src/snapshot.ts';
import { element } from '../../pure-protocol/src/index.ts';
import { row } from './lite-compiler.ts';
import { printLevel } from './lite-compiler.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

let engine: DuckDbEngine;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  await engine.run(
    unplanned(`CREATE TABLE t AS SELECT * FROM (VALUES
       ('AMER', 2023,  10.0), ('AMER', 2024, 900.0),
       ('EMEA', 2023, 500.0), ('EMEA', 2024,  20.0),
       ('APAC', 2023, 100.0), ('APAC', 2024, 100.0)
     ) v(region, yr, amt)`),
    0,
  );
});

after(async () => {
  await engine?.close();
});

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  // as the compiler types the table above
  columns: [
    { name: 'region', type: 'String' },
    { name: 'yr', type: 'Integer' },
    { name: 'amt', type: 'Decimal' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: ['yr'],
  sorts: [],
  epoch: 1,
};

describe('sorting by a pivoted column', () => {
  it('keeps the pivoted column in the sort keys', () => {
    // It is not a row dimension, so the total-order pass must let it
    // through rather than filtering it out as "not of this level".
    const s: CubeSnapshot = {
      ...CUBE,
      sorts: [{ column: '2023__|__amt', direction: 'desc' }],
    };
    assert.deepEqual(totalOrderSorts(s, ['region']), [
      { column: '2023__|__amt', direction: 'desc' },
      { column: 'region', direction: 'asc' },
    ]);
  });

  it('emits the sort AFTER the pivot\'s groupBy, where the column exists', async () => {
    const out = (await printLevel({
      ...CUBE,
      sorts: [{ column: '2023__|__amt', direction: 'desc' }],
    }, undefined, { tuples: [['2023'], ['2024']] }));
    assert.ok(out.indexOf('groupBy(') < out.indexOf('sort('));
    assert.match(out, /sort\(\[~'2023__\|__amt'->descending\(\)/);
  });

  it('actually orders rows by a pivot cell, against the engine', async () => {
    // 2023: EMEA 500, APAC 100, AMER 10 -- an order no row dimension
    // would produce, so this proves the sort is on the cell value.
    const r = await engine.run(
      unplanned(`SELECT * FROM (PIVOT t ON yr IN (2023, 2024) USING sum(amt) AS v
         GROUP BY region) ORDER BY "2023_v" DESC`),
      1,
    );
    assert.deepEqual(r.columns[0]?.values, ['EMEA', 'APAC', 'AMER']);
  });

  it('a different pivot column gives a different order', async () => {
    // 2024: AMER 900, APAC 100, EMEA 20 -- the reverse of 2023's.
    const r = await engine.run(
      unplanned(`SELECT * FROM (PIVOT t ON yr IN (2023, 2024) USING sum(amt) AS v
         GROUP BY region) ORDER BY "2024_v" DESC`),
      1,
    );
    assert.deepEqual(r.columns[0]?.values, ['AMER', 'APAC', 'EMEA']);
  });

  it('still appends the row dimension, so ties are deterministic', async () => {
    // Without it, two groups tying on the pivot cell can come back in
    // either order and a windowed read can repeat or skip one.
    const out = (await printLevel({
      ...CUBE,
      sorts: [{ column: '2024__|__amt', direction: 'desc' }],
    }, undefined, { tuples: [['2023'], ['2024']] }));
    assert.match(out, /~region->ascending\(\)\]\)/);
  });
});

describe('sorting by a derived measure', () => {
  it('sorts on a post-aggregation column', async () => {
    const out = (await printLevel({
      ...CUBE,
      pivotOn: [],
      groupDerived: [{ name: 'share', lambda: (await row('$x.amt / 100')) }],
      sorts: [{ column: 'share', direction: 'desc' }],
    }));
    // The column must exist before it is ordered by.
    assert.ok(out.indexOf('extend(~[share') < out.indexOf('sort('));
    assert.match(out, /sort\(\[~share->descending\(\)/);
  });
});
