// End-to-end against a real DuckDB-WASM, not a mock.
//
// Mocks would happily agree with whatever the conversion code believes
// about Arrow. The interesting failures here -- BigInt, nulls,
// pivot-generated column names -- only appear against the real engine.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase, type TestDatabase } from '#test-host';

import {
  DuckDbEngine,
  decimalToScalar,
  toScalar,
} from '../../engine-client/src/duckdb.ts';
import type { ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import { EpochGuard, STALE } from '../src/epoch.ts';
import { columnIndex } from '../../engine-client/src/result.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';


let engine: DuckDbEngine;
let conn: ArrowishConnection;
let db: TestDatabase;

before(async () => {
  db = await openDatabase();
  conn = db.connection;
  engine = new DuckDbEngine(conn);

  await engine.run(
    unplanned(`CREATE TABLE trades AS SELECT * FROM (VALUES
       ('EMEA', 'GB', 2023, 100.5,  10),
       ('EMEA', 'GB', 2024, 200.25, 20),
       ('EMEA', 'DE', 2023,  50.0,   5),
       ('AMER', 'US', 2024, 300.75, 30),
       ('AMER', 'US', 2023, NULL,    7)
     ) v(region, country, year, notional, qty)`),
    0,
  );
});

after(async () => {
  await engine?.close();
});

describe('DuckDbEngine end-to-end', () => {
  it('returns columnar results with names and row count', async () => {
    const r = await engine.run(
      unplanned('SELECT region, country, notional FROM trades ORDER BY region, country, year'),
      7,
    );
    assert.equal(r.rowCount, 5);
    assert.equal(r.epoch, 7, 'epoch travels with the result');
    assert.deepEqual(
      r.columns.map((c) => c.name),
      ['region', 'country', 'notional'],
    );
    assert.equal(r.columns[0]?.values[0], 'AMER');
    assert.ok(r.elapsedMs >= 0);
  });

  it('preserves SQL NULL as null, not as a string', async () => {
    const r = await engine.run(
      unplanned("SELECT notional FROM trades WHERE region = 'AMER' AND year = 2023"),
      1,
    );
    assert.equal(r.rowCount, 1);
    assert.equal(r.columns[0]?.values[0], null);
  });

  it('narrows BIGINT to number when it is exactly representable', async () => {
    const r = await engine.run(unplanned('SELECT 42::BIGINT AS n'), 1);
    assert.equal(r.columns[0]?.values[0], 42);
    assert.equal(typeof r.columns[0]?.values[0], 'number');
  });

  it('keeps an out-of-range BIGINT lossless, as a bigint', async () => {
    // 2^63-1 cannot be a JS number without losing its last digits, and
    // a trade id that silently rounds is worse than any other answer.
    const r = await engine.run(unplanned('SELECT 9223372036854775807::BIGINT AS n'), 1);
    assert.equal(r.columns[0]?.values[0], 9223372036854775807n);
  });

  it('executes a real pivot and names the generated columns', async () => {
    const r = await engine.run(
      unplanned(`PIVOT trades ON year IN (2023, 2024)
       USING sum(notional) GROUP BY region ORDER BY region`),
      2,
    );
    const names = r.columns.map((c) => c.name);
    assert.deepEqual(names, ['region', '2023', '2024']);
    const i2024 = columnIndex(r, '2024');
    const amer = r.columns[0]?.values.indexOf('AMER') ?? -1;
    assert.equal(r.columns[i2024]?.values[amer], '300.75', 'a DECIMAL is its exact text');
  });

  it('applies DECIMAL scale instead of returning the unscaled integer', async () => {
    // The regression this file was written to catch: sum() over a
    // DECIMAL column arrived as "30075" for 300.75 -- 100x too large,
    // as a string, silently.
    const r = await engine.run(
      unplanned('SELECT sum(notional) AS total FROM trades'),
      1,
    );
    assert.equal(r.columns[0]?.values[0], '651.50');
  });

  it('keeps DECIMAL exact through a grouped pivot', async () => {
    const r = await engine.run(
      unplanned(`PIVOT trades ON year IN (2023, 2024)
       USING sum(notional) GROUP BY region ORDER BY region`),
      1,
    );
    const emea = r.columns[0]?.values.indexOf('EMEA') ?? -1;
    const i2023 = columnIndex(r, '2023');
    // 100.5 + 50.0, not 15050 -- exact, at the DECIMAL's scale
    assert.equal(r.columns[i2023]?.values[emea], '150.50');
  });

  it('handles negative decimals', async () => {
    const r = await engine.run(unplanned('SELECT (-12.5)::DECIMAL(9,3) AS d'), 1);
    assert.equal(r.columns[0]?.values[0], '-12.500', 'exact, at its declared scale');
  });

  it('reports a bad query as QueryError carrying the SQL', async () => {
    await assert.rejects(
      () => engine.run(unplanned('SELECT * FROM no_such_table'), 1),
      (e: unknown) => {
        assert.equal((e as Error).name, 'QueryError');
        assert.match(
          (e as { sql: string }).sql,
          /no_such_table/,
          'the failing SQL is attached for triage',
        );
        return true;
      },
    );
  });

  it('discards a superseded query against the live engine', async () => {
    const guard = new EpochGuard();

    const slow = guard.issue((epoch) =>
      engine.run(
        unplanned('SELECT count(*) AS n FROM range(3000000) t(i)'),
        epoch,
      ),
    );
    // A second interaction arrives before the first finishes.
    const fast = guard.issue((epoch) =>
      engine.run(unplanned('SELECT 1 AS n'), epoch),
    );

    assert.equal(await slow, STALE);
    const winner = await fast;
    assert.notEqual(winner, STALE);
    assert.equal(
      typeof winner === 'object' && winner !== null && 'rowCount' in winner
        ? winner.rowCount
        : -1,
      1,
    );
  });
});

describe('decimalToScalar', () => {
  it('reinserts the decimal point from the unscaled integer', () => {
    assert.equal(decimalToScalar('30075', 2), '300.75');
    assert.equal(decimalToScalar('-12500', 3), '-12.500');
    assert.equal(decimalToScalar('5', 2), '0.05');
    assert.equal(decimalToScalar('0', 2), '0.00');
  });

  it('treats scale 0 as an integer', () => {
    assert.equal(decimalToScalar('42', 0), 42);
  });

  it('keeps a value beyond double precision exact, as text', () => {
    assert.equal(
      decimalToScalar('123456789012345678901', 2),
      '1234567890123456789.01',
    );
  });
});

describe('toScalar', () => {
  it('passes primitives through and maps nullish to null', () => {
    assert.equal(toScalar('a'), 'a');
    assert.equal(toScalar(1.5), 1.5);
    assert.equal(toScalar(true), true);
    assert.equal(toScalar(null), null);
    assert.equal(toScalar(undefined), null);
  });

  it('renders a nested object as JSON, a bigint the number it is', () => {
    // a nested BIGINT is a JSON number, past 2^53 too: {"tier":2}, never {"tier":"2"}
    assert.equal(toScalar({ a: 1n, b: 'x' }), '{"a":1,"b":"x"}');
    assert.equal(toScalar({ big: 2n ** 63n - 1n, list: [1n, null, 2.5], deep: { s: 'q"' } }),
      '{"big":9223372036854775807,"list":[1,null,2.5],"deep":{"s":"q\\""}}');
    assert.equal(toScalar([1n, undefined]), '[1,null]');
    assert.equal(toScalar({ gone: undefined, d: new Date(0) }), '{"d":"1970-01-01T00:00:00.000Z"}');
    assert.equal(toScalar({ xs: new BigInt64Array([3n, 4n]) }), '{"xs":[3,4]}');
    // JSON.stringify's own rules otherwise: a boxed value its value, a symbol or a toJSON's undefined left out
    assert.equal(toScalar({ n: Object(1n), s: new String('a'), sym: Symbol('x'), none: { toJSON: () => undefined } }),
      '{"n":1,"s":"a"}');
    assert.equal(toScalar([Symbol('x'), -0, Number.NaN]), '[null,0,null]');
  });
});
