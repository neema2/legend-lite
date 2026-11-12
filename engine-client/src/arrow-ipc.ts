// An Arrow IPC stream read as a DuckDB connection's answer: apache-arrow, the reader DuckDB-WASM's own answers come
// from. For a connection whose database answers in IPC -- Python's DuckDB under DataCube's headless host
// (datacube/src/headless/host.ts; docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). A module of its own, so a
// page that never reads IPC this way imports nothing more. And the other way: rows a page makes, written as a stream
// for a connection's Arrow reader (DuckDbEngine.loadArrow) -- data, never SQL (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md
// D5).

import { makeData, makeVector, Table, tableFromIPC, tableToIPC, Utf8, type Vector } from 'apache-arrow';

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
 * (BIGINT); or text, each row one of a few values (VARCHAR).
 */
export type MadeColumn =
  | { readonly doubles: (row: number) => number }
  | { readonly integers: (row: number) => number }
  | { readonly text: readonly string[]; readonly pick: (row: number) => number };

/** `rows` rows of columns a page made, in order, as one Arrow IPC stream (the stream format). */
export function arrowIpcStream(rows: number, columns: Readonly<Record<string, MadeColumn>>): Uint8Array {
  const vectors: Record<string, Vector> = {};
  for (const [name, column] of Object.entries(columns)) {
    vectors[name] = 'doubles' in column ? doubles(rows, column.doubles)
      : 'integers' in column ? integers(rows, column.integers) : text(rows, column.text, column.pick);
  }
  return tableToIPC(new Table(vectors), 'stream');
}

function doubles(rows: number, value: (row: number) => number): Vector {
  const out = new Float64Array(rows);
  for (let i = 0; i < rows; i++) out[i] = value(i);
  return makeVector(out);
}

/** Each a 64-bit integer written as its two 32-bit words (little-endian, as Arrow is): no BigInt made per row. */
function integers(rows: number, value: (row: number) => number): Vector {
  const out = new BigInt64Array(rows);
  const words = new Int32Array(out.buffer);
  for (let i = 0; i < rows; i++) {
    const v = value(i);
    if (!Number.isInteger(v) || v < -0x80000000 || v > 0x7fffffff) throw new RangeError(`row ${i}: ${v} is not a 32-bit whole number`);
    words[2 * i] = v;
    words[2 * i + 1] = v < 0 ? -1 : 0;
  }
  return makeVector(out);
}

/** Text as Arrow holds it -- each row's UTF-8 bytes end to end, and where each ends -- made directly, each value
 *  encoded once (Arrow's builders are not in a page's startup bundle; its writer is, DuckDB-WASM's). */
function text(rows: number, values: readonly string[], pick: (row: number) => number): Vector {
  const encoder = new TextEncoder();
  const bytes = values.map((v) => encoder.encode(v));
  const of = (i: number): Uint8Array => {
    const b = bytes[pick(i)];
    if (!b) throw new RangeError(`row ${i}: no value ${pick(i)} of ${values.length}`);
    return b;
  };
  const valueOffsets = new Int32Array(rows + 1);
  for (let i = 0; i < rows; i++) valueOffsets[i + 1] = valueOffsets[i]! + of(i).length;
  const data = new Uint8Array(valueOffsets[rows]!);
  for (let i = 0; i < rows; i++) data.set(of(i), valueOffsets[i]!);
  return makeVector(makeData({ type: new Utf8(), length: rows, nullCount: 0, valueOffsets, data }));
}
