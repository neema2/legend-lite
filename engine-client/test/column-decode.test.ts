// A COLUMN'S DECODE IS ITS CELLS' DECODE (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, H1a): columnDecoder's fast readers --
// each chunk read from its own buffers -- answer exactly what the per-cell decoder answers (cellByCellDecoder, the one
// rule), value for value (Object.is: a -0, a NaN, a bigint), for every type DuckDB-WASM writes, its nulls, its chunks,
// a sliced vector; and the number spellings the readers use are the BigInt spellings for every safe integer tried.

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { cellByCellDecoder, columnDecoder, toRawTable, type ArrowishTable } from '../src/duckdb.ts';
import { startNodeDuckDb } from '../src/node-require.ts';
import type { Scalar } from '../src/result.ts';
import {
  decimalText, decimalTextOfSafe, timeMicrosText, timeText, timestampMicrosText, timestampText,
} from '../src/values.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

interface Raw { query(sql: string): ArrowishTable & { slice?(begin: number, end: number): ArrowishTable } }

let conn: Raw;
const ROWS = 5000;

before(async () => {
  conn = (await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'))).connect() as unknown as Raw;
  conn.query("SET TimeZone = 'UTC'");
  conn.query("CREATE TYPE mood AS ENUM ('calm', 'busy', 'odd')");
  // every column a function of i (hash: a spread of values), a null at every eleventh row
  const n = (expr: string): string => `CASE WHEN i % 11 = 0 THEN NULL ELSE ${expr} END`;
  conn.query(`CREATE TABLE every AS
    WITH base AS (SELECT i, hash(i) AS h, CAST(CAST(hash(i) AS HUGEINT) - 9223372036854775808 AS BIGINT) AS s64
                  FROM range(${ROWS}) t(i))
    SELECT i,
      ${n('CAST(CAST(h % 256 AS INTEGER) - 128 AS TINYINT)')} AS ti,
      ${n('CAST(CAST(h % 65536 AS INTEGER) - 32768 AS SMALLINT)')} AS si,
      ${n('CAST(CAST(h % 4294967296 AS BIGINT) - 2147483648 AS INTEGER)')} AS ii,
      ${n('CASE i % 3 WHEN 0 THEN s64 WHEN 1 THEN s64 % 9007199254740993 ELSE s64 % 1000 END')} AS bi,
      ${n('CAST(h % 256 AS UTINYINT)')} AS uti,
      ${n('CAST(h % 65536 AS USMALLINT)')} AS usi,
      ${n('CAST(h % 4294967296 AS UINTEGER)')} AS ui,
      ${n('CASE WHEN i % 2 = 0 THEN h ELSE h % 9007199254740993 END')} AS ubi,
      ${n('CAST(s64 AS HUGEINT) * 1000003')} AS hi,
      ${n("CAST(CASE i % 13 WHEN 0 THEN 'nan' WHEN 1 THEN 'inf' WHEN 2 THEN '-inf' WHEN 3 THEN '-0.0' ELSE CAST((s64 % 100000) / 7.0 AS VARCHAR) END AS FLOAT)")} AS f,
      ${n("CASE i % 13 WHEN 0 THEN CAST('nan' AS DOUBLE) WHEN 1 THEN CAST('inf' AS DOUBLE) WHEN 2 THEN CAST('-inf' AS DOUBLE) WHEN 3 THEN CAST('-0.0' AS DOUBLE) ELSE s64 / 3.0 END")} AS d,
      ${n('h % 2 = 0')} AS b,
      ${n("CASE i % 5 WHEN 0 THEN '' WHEN 1 THEN 'é漢字🙂' ELSE 'v' || CAST(i AS VARCHAR) END")} AS v,
      ${n("CASE i % 5 WHEN 0 THEN '' ELSE 'a' || CAST(i AS VARCHAR) END")} AS va,
      ${n('CAST((CAST(h % 19999 AS INTEGER) - 9999) / 10.0 AS DECIMAL(4,1))')} AS dec41,
      ${n('CAST((CAST(h % 1999999999 AS BIGINT) - 999999999) / 1000.0 AS DECIMAL(9,3))')} AS dec93,
      ${n('CAST(CAST(s64 % 999999999999999999 AS DECIMAL(18,0)) / 100 AS DECIMAL(18,2))')} AS dec182,
      ${n('CAST(CAST(s64 AS DECIMAL(38,0)) * 1000003 / 10000000000 AS DECIMAL(38,10))')} AS dec3810,
      ${n("DATE '1970-01-01' + CAST(CAST(h % 2000000 AS INTEGER) - 1000000 AS INTEGER)")} AS dt,
      ${n("TIME '00:00:00' + INTERVAL (CAST(h % 86400000000 AS BIGINT)) MICROSECOND")} AS tm,
      ${n("TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (CAST(h % 120000000000000000 AS BIGINT) - 60000000000000000) MICROSECOND")} AS ts,
      ${n("CAST(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (CAST(h % 120000000000000000 AS BIGINT) - 60000000000000000) MICROSECOND AS TIMESTAMP_S)")} AS ts_s,
      ${n("CAST(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (CAST(h % 120000000000000000 AS BIGINT) - 60000000000000000) MICROSECOND AS TIMESTAMP_MS)")} AS ts_ms,
      ${n("CAST(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (CAST(h % 18000000000000000 AS BIGINT) - 9000000000000000) MICROSECOND AS TIMESTAMP_NS)")} AS ts_ns,
      ${n("CAST(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (CAST(h % 120000000000000000 AS BIGINT) - 60000000000000000) MICROSECOND AS TIMESTAMPTZ)")} AS tstz,
      ${n("CAST(['calm', 'busy', 'odd'][CAST(h % 3 AS INTEGER) + 1] AS mood)")} AS e,
      ${n('[i, NULL, i + 1]')} AS l,
      ${n("{'a': i, 'b': 'x' || CAST(i AS VARCHAR)}")} AS st,
      ${n("CAST('ab' AS BLOB)")} AS bl,
      ${n('CAST(md5(CAST(i AS VARCHAR)) AS UUID)')} AS uu
    FROM base`);
});

function decoded(table: ArrowishTable, how: typeof columnDecoder): Map<string, Scalar[]> {
  const out = new Map<string, Scalar[]>();
  table.schema.fields.forEach((field, c) => {
    const values: Scalar[] = new Array(table.numRows);
    how(field.type)(table.getChildAt(c)!, values, 0);
    out.set(`${field.name} (${String(field.type)})`, values);
  });
  return out;
}

function sameValues(table: ArrowishTable): number {
  const fast = decoded(table, columnDecoder);
  const oracle = decoded(table, cellByCellDecoder);
  let compared = 0;
  for (const [column, expected] of oracle) {
    const got = fast.get(column)!;
    assert.equal(got.length, expected.length, column);
    for (let r = 0; r < expected.length; r++) {
      assert.ok(Object.is(got[r], expected[r]), `${column} row ${r}: ${String(got[r])} (${typeof got[r]}) != ${String(expected[r])} (${typeof expected[r]})`);
      compared++;
    }
  }
  return compared;
}

describe('a column decoded whole is its cells decoded one by one', () => {
  it('every type DuckDB-WASM writes, its nulls and its chunks', () => {
    const table = conn.query('SELECT * FROM every ORDER BY i');
    assert.equal(table.numRows, ROWS);
    const chunks = (table.getChildAt(0) as unknown as { data: readonly unknown[] }).data.length;
    assert.ok(chunks > 1, `the result spans chunks (${chunks})`);
    const compared = sameValues(table);
    assert.equal(compared, ROWS * table.schema.fields.length, `every column compared: ${table.schema.fields.map((f) => `${f.name}:${String(f.type)}`).join(', ')}`);
    assert.equal(table.schema.fields.length, 31, `every column there`);
    console.log(`[types] ${table.schema.fields.map((f) => `${f.name}:${String(f.type)}`).join(", ")}`);
  });

  // A chunk at an offset is read cell by cell, so it answers what the per-cell decoder answers -- including the per-cell
  // decoder's own failure: its raw reads (rawAt) add the chunk's offset to a value buffer Arrow-JS has sliced already, so a
  // sliced date, time or timestamp vector reads past its values (no product path decodes a sliced vector: every chunk
  // DuckDB-WASM and the warehouse hand over is at offset 0; the program's section 7)
  it('a vector sliced to an offset is read cell by cell: the same values, or the same failure', () => {
    const table = conn.query('SELECT * FROM every ORDER BY i');
    const sliced = (table as { slice?(b: number, e: number): ArrowishTable }).slice?.(1000, 3500);
    assert.ok(sliced, 'Arrow tables slice');
    sliced.schema.fields.forEach((field, c) => {
      const outcome = (how: typeof columnDecoder): string => {
        const values: Scalar[] = new Array(sliced.numRows);
        try {
          how(field.type)(sliced.getChildAt(c)!, values, 0);
        } catch (e) {
          return `threw ${(e as Error).message}`;
        }
        return values.map((v) => (typeof v === 'bigint' ? `${v}n` : Object.is(v, -0) ? '-0' : JSON.stringify(v))).join('|');
      };
      assert.equal(outcome(columnDecoder), outcome(cellByCellDecoder), field.name);
    });
  });

  // the fast readers are what decode these columns: their vectors' get() is never called (a vector whose get throws
  // decodes all the same); the rest -- a 38-digit integer (scale 0), a dictionary, a list, a struct, a binary -- go cell
  // by cell
  it('reads every column of a fast type from its buffers alone, never cell by cell', () => {
    const table = conn.query('SELECT * FROM every ORDER BY i');
    const cellByCell = new Set(['hi', 'e', 'l', 'st', 'bl']);
    const fast = table.schema.fields.filter((f) => !cellByCell.has(f.name));
    assert.equal(fast.length, 26);
    for (const field of fast) {
      const c = table.schema.fields.indexOf(field);
      const vector = table.getChildAt(c)!;
      const data = vector.data;
      assert.ok(data !== undefined, field.name);
      const noGet = { length: vector.length, data, get(): never { throw new Error(`get() called on ${field.name}`); } };
      const values: Scalar[] = new Array(table.numRows);
      columnDecoder(field.type)(noGet, values, 0);
      const expected: Scalar[] = new Array(table.numRows);
      cellByCellDecoder(field.type)(vector, expected, 0);
      values.forEach((v, r) => assert.ok(Object.is(v, expected[r]), `${field.name} row ${r}`));
    }
  });

  it('toRawTable decodes as each column decodes', () => {
    const table = conn.query('SELECT * FROM every ORDER BY i');
    const raw = toRawTable(table, 0, 0);
    const oracle = decoded(table, cellByCellDecoder);
    raw.columns.forEach((column, c) => {
      const expected = [...oracle.values()][c]!;
      column.values.forEach((v, r) => assert.ok(Object.is(v, expected[r]), `${column.name} row ${r}`));
    });
  });
});

describe('the number spellings are the BigInt spellings', () => {
  const samples = (): number[] => {
    const out = [0, 1, -1, 999_999, -999_999, 1_000_000, -1_000_000, 1_000_001, -1_000_001, Number.MAX_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER, 86_399_999_999, 1_704_067_200_000_000, -62_135_596_800_000_000, 253_402_300_799_999_999];
    let x = 0x2545f491;
    for (let k = 0; k < 20_000; k++) {
      x = (x * 1103515245 + 12345) % 2147483648;
      const magnitude = 10 ** (k % 16);
      out.push(Math.trunc(((x / 2147483648) * 2 - 1) * magnitude * 9));
    }
    return out.filter((n) => Number.isSafeInteger(n));
  };

  it('timestampMicrosText is timestampText', () => {
    for (const n of samples()) {
      // within the dates JavaScript's Date holds (±8.64e15 ms): beyond them neither spelling is a date
      if (Math.abs(n) > 8.64e18) continue;
      assert.equal(timestampMicrosText(n), timestampText(BigInt(n)), String(n));
    }
  });

  it('timestampMicrosText is timestampText on every day a safe number of microseconds reaches', () => {
    let compared = 0;
    for (let day = -104_250; day <= 104_250; day++) {
      for (const inDay of [0, 1, 43_200_000_001, 86_399_999_999]) {
        const n = day * 86_400_000_000 + inDay;
        if (!Number.isSafeInteger(n)) continue;
        const fast = timestampMicrosText(n);
        if (fast !== timestampText(BigInt(n))) assert.equal(fast, timestampText(BigInt(n)), String(n));
        compared++;
      }
    }
    assert.ok(compared > 800_000, String(compared));
  });

  it('timeMicrosText is timeText', () => {
    for (const n of samples()) assert.equal(timeMicrosText(n), timeText(BigInt(n)), String(n));
  });

  it('decimalTextOfSafe is decimalText, at every positive scale', () => {
    for (const n of samples()) {
      for (const scale of [1, 2, 3, 6, 10, 18]) {
        assert.equal(decimalTextOfSafe(n, scale), decimalText(BigInt(n), scale), `${n} scale ${scale}`);
      }
    }
  });
});
