// End-to-end against a real DuckDB-WASM, not a mock.
//
// Mocks would happily agree with whatever the conversion code believes
// about Arrow. The interesting failures here -- BigInt, nulls,
// pivot-generated column names -- only appear against the real engine.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import {
  DuckDbEngine,
  decimalToScalar,
  temporalKindOf,
  toScalar,
} from '../src/duckdb.ts';
import type { ArrowishConnection } from '../src/duckdb.ts';
import { EpochGuard, STALE } from '../src/epoch.ts';
import { columnIndex } from '../src/result.ts';

const require = createRequire(import.meta.url);

let engine: DuckDbEngine;
let conn: ArrowishConnection;
let db: { connect(): unknown; instantiate(): Promise<unknown> };

before(async () => {
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  db = await duckdb.createDuckDB(
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
  conn = db.connect() as ArrowishConnection;
  engine = new DuckDbEngine(conn);

  await engine.execute(
    `CREATE TABLE trades AS SELECT * FROM (VALUES
       ('EMEA', 'GB', 2023, 100.5,  10),
       ('EMEA', 'GB', 2024, 200.25, 20),
       ('EMEA', 'DE', 2023,  50.0,   5),
       ('AMER', 'US', 2024, 300.75, 30),
       ('AMER', 'US', 2023, NULL,    7)
     ) v(region, country, year, notional, qty)`,
    0,
  );
});

after(async () => {
  await engine?.close();
});

describe('DuckDbEngine end-to-end', () => {
  it('returns columnar results with names and row count', async () => {
    const r = await engine.execute(
      'SELECT region, country, notional FROM trades ORDER BY region, country, year',
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
    const r = await engine.execute(
      "SELECT notional FROM trades WHERE region = 'AMER' AND year = 2023",
      1,
    );
    assert.equal(r.rowCount, 1);
    assert.equal(r.columns[0]?.values[0], null);
  });

  it('narrows BIGINT to number when it is exactly representable', async () => {
    const r = await engine.execute('SELECT 42::BIGINT AS n', 1);
    assert.equal(r.columns[0]?.values[0], 42);
    assert.equal(typeof r.columns[0]?.values[0], 'number');
  });

  it('keeps an out-of-range BIGINT lossless as text', async () => {
    // 2^63-1 cannot be a JS number without losing its last digits, and
    // a trade id that silently rounds is worse than one shown as text.
    const r = await engine.execute('SELECT 9223372036854775807::BIGINT AS n', 1);
    assert.equal(r.columns[0]?.values[0], '9223372036854775807');
  });

  it('executes a real pivot and names the generated columns', async () => {
    const r = await engine.execute(
      `PIVOT trades ON year IN (2023, 2024)
       USING sum(notional) GROUP BY region ORDER BY region`,
      2,
    );
    const names = r.columns.map((c) => c.name);
    assert.deepEqual(names, ['region', '2023', '2024']);
    const i2024 = columnIndex(r, '2024');
    const amer = r.columns[0]?.values.indexOf('AMER') ?? -1;
    assert.equal(r.columns[i2024]?.values[amer], 300.75);
  });

  it('applies DECIMAL scale instead of returning the unscaled integer', async () => {
    // The regression this file was written to catch: sum() over a
    // DECIMAL column arrived as "30075" for 300.75 -- 100x too large,
    // as a string, silently.
    const r = await engine.execute(
      'SELECT sum(notional) AS total FROM trades',
      1,
    );
    assert.equal(r.columns[0]?.values[0], 651.5);
  });

  it('keeps DECIMAL exact through a grouped pivot', async () => {
    const r = await engine.execute(
      `PIVOT trades ON year IN (2023, 2024)
       USING sum(notional) GROUP BY region ORDER BY region`,
      1,
    );
    const emea = r.columns[0]?.values.indexOf('EMEA') ?? -1;
    const i2023 = columnIndex(r, '2023');
    // 100.5 + 50.0, not 15050
    assert.equal(r.columns[i2023]?.values[emea], 150.5);
  });

  it('handles negative decimals', async () => {
    const r = await engine.execute('SELECT (-12.5)::DECIMAL(9,3) AS d', 1);
    assert.equal(r.columns[0]?.values[0], -12.5);
  });

  it('reports a bad query as QueryError carrying the SQL', async () => {
    await assert.rejects(
      () => engine.execute('SELECT * FROM no_such_table', 1),
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
      engine.execute(
        'SELECT count(*) AS n FROM range(3000000) t(i)',
        epoch,
      ),
    );
    // A second interaction arrives before the first finishes.
    const fast = guard.issue((epoch) =>
      engine.execute('SELECT 1 AS n', epoch),
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
    assert.equal(decimalToScalar('30075', 2), 300.75);
    assert.equal(decimalToScalar('-12500', 3), -12.5);
    assert.equal(decimalToScalar('5', 2), 0.05);
    assert.equal(decimalToScalar('0', 2), 0);
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

  it('renders a nested object as JSON, bigints included', () => {
    assert.equal(
      toScalar({ a: 1n, b: 'x' }),
      '{"a":"1","b":"x"}',
    );
  });
});

describe('temporalKindOf', () => {
  // Arrow JS normalises BOTH date and timestamp vectors to epoch
  // milliseconds on get(), so the declared unit describes storage,
  // not what reaches this code. Scaling by it produced "Invalid
  // Date" for dates and "Jan 19, 1970" for microsecond timestamps.
  it('recognises a date, whatever unit it declares', () => {
    assert.equal(temporalKindOf('Date32<DAY>'), 'date');
    assert.equal(temporalKindOf('Date64<MILLISECOND>'), 'date');
  });

  it('recognises a timestamp, zoned or not', () => {
    assert.equal(temporalKindOf('Timestamp<MICROSECOND>'), 'timestamp');
    assert.equal(temporalKindOf('Timestamp<NANOSECOND>'), 'timestamp');
    assert.equal(temporalKindOf('Timestamp<MICROSECOND, UTC>'), 'timestamp');
  });

  it('leaves every non-temporal type alone', () => {
    // A multiplier here would turn a plain number into a Date.
    for (const t of ['Int64', 'Float64', 'Utf8', 'Bool',
      'Decimal<18,2>', 'Null', '']) {
      assert.equal(temporalKindOf(t), undefined, t);
    }
  });
});
