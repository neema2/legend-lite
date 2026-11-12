// Rows a page makes, as Arrow (arrow-ipc.ts arrowIpcStream): each kind of column's type and every value, as DuckDB's
// own Arrow reader takes them (DuckDB-WASM's, the tab's) -- the rows put in as data, no SQL spelled to make them
// (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5) -- and a value outside its kind refused by its row.

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { arrowIpcStream } from '../src/arrow-ipc.ts';
import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { startNodeDuckDb } from '../src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

let engine: DuckDbEngine;

before(async () => {
  const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
});

const once = async function* (bytes: Uint8Array): AsyncIterable<Uint8Array> { yield bytes; };

describe('rows a page makes, as Arrow', () => {
  it('doubles, whole numbers and text: DOUBLE, BIGINT and VARCHAR in DuckDB, every value as made', async () => {
    const whole = [0, -1, 2147483647, -2147483648];
    await engine.loadArrow({ table: 'made' }, once(arrowIpcStream(4, {
      d: { doubles: (i) => i / 4 - 0.5 },
      n: { integers: (i) => whole[i]! },
      t: { text: ['é', 'two words', ''], pick: (i) => i % 3 },
    })));
    const types = await engine.run(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'made' ORDER BY ordinal_position", 0);
    assert.deepEqual(types.columns.map((c) => c.values), [['d', 'n', 't'], ['DOUBLE', 'BIGINT', 'VARCHAR']]);
    const rows = await engine.run('SELECT d, CAST(n AS VARCHAR) AS n, t FROM made', 0);
    assert.deepEqual(rows.columns.map((c) => c.values),
      [[-0.5, -0.25, 0, 0.25], ['0', '-1', '2147483647', '-2147483648'], ['é', 'two words', '', 'é']]);
  });

  it('a value outside its kind is refused, by its row', () => {
    assert.throws(() => arrowIpcStream(1, { n: { integers: () => 2 ** 31 } }), /row 0: 2147483648 is not a 32-bit whole number/);
    assert.throws(() => arrowIpcStream(2, { n: { integers: (i) => i + 0.5 } }), /row 0: 0.5 is not a 32-bit whole number/);
    assert.throws(() => arrowIpcStream(1, { t: { text: ['a'], pick: () => 1 } }), /row 0: no value 1 of 1/);
  });
});
