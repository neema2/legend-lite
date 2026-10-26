// An Arrow IPC stream read as a DuckDB connection's answer: apache-arrow, the reader DuckDB-WASM's own answers come
// from. For a connection whose database answers in IPC -- Python's DuckDB under DataCube's headless host
// (datacube/src/headless/host.ts; docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). A module of its own, so a
// page that never reads IPC this way imports nothing more. And the other way: rows a page makes, written as a stream
// for a connection's Arrow reader (DuckDbEngine.loadArrow) -- data, never SQL (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md
// D5).

import {
  Bool, DateDay, Float64, Int64, makeData, makeVector, Table, tableFromIPC, tableToIPC, TimestampMillisecond, Utf8, type Vector,
} from 'apache-arrow';

import type { ArrowishTable } from './duckdb.ts';

/** One Arrow IPC stream (the stream format) as the table a connection's `query` answers. */
export function arrowIpcTable(stream: Uint8Array): ArrowishTable {
  return tableFromIPC(stream) as unknown as ArrowishTable;
}

/** One Arrow IPC stream as a host's answer: its table, and what its schema's metadata says beside the rows (a host that
 *  plans and runs in one step carries its plan there: Python's plan-and-run). */
export function arrowIpcAnswer(stream: Uint8Array): { readonly table: ArrowishTable; readonly metadata: ReadonlyMap<string, string> } {
  const table = tableFromIPC(stream);
  return { table: table as unknown as ArrowishTable, metadata: table.schema.metadata };
}

/**
 * A column a page makes, row by row: doubles (DOUBLE); whole numbers, each in 32 bits, held as 64-bit integers
 * (BIGINT); text, each row one of a few values (VARCHAR); instants, each its milliseconds since the epoch, UTC
 * (TIMESTAMP_MS: what a table's TIMESTAMP takes); truths (BOOLEAN); or days, each its days since the epoch (DATE). Any
 * row's value may be null (the value null; text's pick null), the column's validity then written. The last three for
 * the live market's tables (docs/DUCKDB_LIVE_2026_10_25.md L2): a print's time, a story's flag, an order's date.
 */
export type MadeColumn =
  | { readonly doubles: (row: number) => number | null }
  | { readonly integers: (row: number) => number | null }
  | { readonly text: readonly string[]; readonly pick: (row: number) => number | null }
  | { readonly instants: (row: number) => number | null }
  | { readonly truths: (row: number) => boolean | null }
  | { readonly days: (row: number) => number | null };

/** `rows` rows of columns a page made, in order, as one Arrow IPC stream (the stream format). */
export function arrowIpcStream(rows: number, columns: Readonly<Record<string, MadeColumn>>): Uint8Array {
  const vectors: Record<string, Vector> = {};
  for (const [name, column] of Object.entries(columns)) {
    vectors[name] = 'doubles' in column ? doubles(rows, column.doubles)
      : 'integers' in column ? integers(rows, column.integers)
      : 'instants' in column ? instants(rows, column.instants)
      : 'truths' in column ? truths(rows, column.truths)
      : 'days' in column ? days(rows, column.days) : text(rows, column.text, column.pick);
  }
  return tableToIPC(new Table(vectors), 'stream');
}

/** Which rows hold a value: a bit a row, set where it is not null (Arrow's validity), and how many are null -- none
 *  written while every row holds one. */
class Validity {
  readonly #bits: Uint8Array;
  nulls = 0;

  constructor(rows: number) {
    this.#bits = new Uint8Array(Math.ceil(rows / 8)).fill(0xff);
  }

  none(row: number): void {
    this.#bits[row >> 3]! &= ~(1 << (row & 7));
    this.nulls++;
  }

  /** What a column's data takes of it: its null count, and its bitmap when one is null. */
  get data(): { readonly nullCount: number; readonly nullBitmap?: Uint8Array } {
    return this.nulls === 0 ? { nullCount: 0 } : { nullCount: this.nulls, nullBitmap: this.#bits };
  }
}

function doubles(rows: number, value: (row: number) => number | null): Vector {
  const out = new Float64Array(rows);
  const valid = new Validity(rows);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (v === null) valid.none(i);
    else out[i] = v;
  }
  return makeVector(makeData({ type: new Float64(), length: rows, ...valid.data, data: out }));
}

/** Each a 64-bit integer written as its two 32-bit words (little-endian, as Arrow is): no BigInt made per row. */
function integers(rows: number, value: (row: number) => number | null): Vector {
  const out = new BigInt64Array(rows);
  const words = new Int32Array(out.buffer);
  const valid = new Validity(rows);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (v === null) {
      valid.none(i);
      continue;
    }
    if (!Number.isInteger(v) || v < -0x80000000 || v > 0x7fffffff) throw new RangeError(`row ${i}: ${v} is not a 32-bit whole number`);
    words[2 * i] = v;
    words[2 * i + 1] = v < 0 ? -1 : 0;
  }
  return makeVector(makeData({ type: new Int64(), length: rows, ...valid.data, data: out }));
}

/** Each instant's milliseconds as a 64-bit integer, written as its two 32-bit words (as `integers`'): no BigInt made per
 *  row; a whole number of milliseconds within 2^53 of the epoch, either side. */
function instants(rows: number, value: (row: number) => number | null): Vector {
  const out = new BigInt64Array(rows);
  const words = new Uint32Array(out.buffer);
  const valid = new Validity(rows);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (v === null) {
      valid.none(i);
      continue;
    }
    if (!Number.isSafeInteger(v)) throw new RangeError(`row ${i}: ${v} is not a whole number of milliseconds`);
    const high = Math.floor(v / 0x100000000);
    words[2 * i] = v - high * 0x100000000;
    words[2 * i + 1] = high >>> 0;
  }
  return makeVector(makeData({ type: new TimestampMillisecond(), length: rows, ...valid.data, data: out }));
}

/** Truths as Arrow holds them: a bit a row, the first row the lowest bit of the first byte. */
function truths(rows: number, value: (row: number) => boolean | null): Vector {
  const data = new Uint8Array(Math.ceil(rows / 8));
  const valid = new Validity(rows);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (v === null) valid.none(i);
    else if (v) data[i >> 3]! |= 1 << (i & 7);
  }
  return makeVector(makeData({ type: new Bool(), length: rows, ...valid.data, data }));
}

/** Days since the epoch, each a 32-bit whole number (Arrow's DATE in days). */
function days(rows: number, value: (row: number) => number | null): Vector {
  const out = new Int32Array(rows);
  const valid = new Validity(rows);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (v === null) {
      valid.none(i);
      continue;
    }
    if (!Number.isInteger(v) || v < -0x80000000 || v > 0x7fffffff) throw new RangeError(`row ${i}: ${v} is not a whole number of days`);
    out[i] = v;
  }
  return makeVector(makeData({ type: new DateDay(), length: rows, ...valid.data, data: out }));
}

/** Text as Arrow holds it -- each row's UTF-8 bytes end to end, and where each ends -- made directly, each value
 *  encoded once (Arrow's builders are not in a page's startup bundle; its writer is, DuckDB-WASM's). */
function text(rows: number, values: readonly string[], pick: (row: number) => number | null): Vector {
  const encoder = new TextEncoder();
  const bytes = values.map((v) => encoder.encode(v));
  const none = new Uint8Array(0);
  const valid = new Validity(rows);
  const picked: Uint8Array[] = new Array<Uint8Array>(rows);
  for (let i = 0; i < rows; i++) {
    const at = pick(i);
    if (at === null) {
      valid.none(i);
      picked[i] = none;
      continue;
    }
    const b = bytes[at];
    if (!b) throw new RangeError(`row ${i}: no value ${at} of ${values.length}`);
    picked[i] = b;
  }
  const valueOffsets = new Int32Array(rows + 1);
  for (let i = 0; i < rows; i++) valueOffsets[i + 1] = valueOffsets[i]! + picked[i]!.length;
  const data = new Uint8Array(valueOffsets[rows]!);
  for (let i = 0; i < rows; i++) data.set(picked[i]!, valueOffsets[i]!);
  return makeVector(makeData({ type: new Utf8(), length: rows, ...valid.data, valueOffsets, data }));
}
