// DuckDB, via an Arrow result, converted to our columnar scalars.
//
// The conversion is the interesting part. duckdb-wasm hands back an
// Arrow Table, and we do NOT pass that on: an Arrow Table cannot cross
// a worker boundary (structuredClone throws on it), and Arrow's own
// per-cell accessors are slower than they look -- a dictionary-encoded
// Utf8 column reads at ~84ns per cell, SLOWER than a plain Utf8 column
// at ~71ns, because the indirection costs more than direct
// materialisation through the generic JS path. Dictionary encoding is a
// payload win, not an access-speed one.
//
// BigInt is the trap worth naming. DuckDB returns BIGINT as JS BigInt,
// which throws inside JSON.stringify and refuses to mix with numbers in
// arithmetic. Left alone it produces a crash far from its cause, so it
// is narrowed here, once, at the boundary.

import type { QueryEngine } from './engine.ts';
import { QueryError } from './engine.ts';
import type { ResultColumn, ResultTable, Scalar } from './result.ts';

/**
 * The slice of duckdb-wasm's connection we actually use. Declaring it
 * structurally keeps this file testable and keeps duckdb-wasm's types
 * (which differ between its browser and node entry points) out of our
 * public surface.
 */
export interface ArrowishConnection {
  query(sql: string): ArrowishTable | Promise<ArrowishTable>;
  close?(): void | Promise<void>;
}

export interface ArrowishTable {
  readonly numRows: number;
  readonly schema: { fields: readonly { name: string; type: unknown }[] };
  getChildAt(i: number): ArrowishVector | null;
}

export interface ArrowishVector {
  readonly length: number;
  get(i: number): unknown;
}

/**
 * Narrow one Arrow value to a Scalar.
 *
 * Exported because it is the highest-risk function in the file and
 * deserves direct tests rather than only being exercised through a
 * live database.
 */
export function toScalar(v: unknown): Scalar {
  if (v === null || v === undefined) return null;

  if (typeof v === 'bigint') {
    // Narrow to number when it is exactly representable; otherwise keep
    // full precision as text rather than silently rounding. A trade id
    // that loses its last digits is worse than one rendered as a string.
    return v >= BigInt(Number.MIN_SAFE_INTEGER) &&
      v <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(v)
      : v.toString();
  }

  if (typeof v === 'number' || typeof v === 'string' ||
      typeof v === 'boolean') {
    return v;
  }

  if (v instanceof Date) return v;

  // A raw Arrow buffer with no column type to interpret it. Decimals
  // reach `decimalToScalar` instead, via the per-column converter;
  // anything landing here is genuinely opaque, so stringify it rather
  // than invent a reading.
  if (ArrayBuffer.isView(v)) return String(v);

  if (typeof v === 'object') {
    // Struct, list, map: render as JSON rather than "[object Object]",
    // so a nested column is at least legible in the grid.
    try {
      return JSON.stringify(v, (_k, x) =>
        typeof x === 'bigint' ? x.toString() : x,
      );
    } catch {
      return String(v);
    }
  }

  return String(v);
}

/**
 * Convert an Arrow DECIMAL to a Scalar, applying its scale.
 *
 * This existed as a bug first: DECIMAL reaches JS as a DecimalBigNum
 * (a Uint32Array of 128-bit two's-complement words) whose toString
 * yields the UNSCALED integer, so `sum(notional)` of 300.75 stringified
 * to "30075" -- a value 100x too large, silently, in a financial grid.
 * A live-engine test caught it; a mocked Arrow table would have agreed
 * with whatever this file believed.
 *
 * DecimalBigNum.toString already resolves the sign, so the two's
 * complement does not need unpicking here; only the decimal point has
 * to be reinserted. The result is a number when the unscaled value is
 * exactly representable, and the exact decimal string otherwise --
 * the same policy as BIGINT, for the same reason.
 */
export function decimalToScalar(raw: unknown, scale: number): Scalar {
  const unscaled = BigInt(String(raw));
  const safe =
    unscaled >= BigInt(Number.MIN_SAFE_INTEGER) &&
    unscaled <= BigInt(Number.MAX_SAFE_INTEGER);

  if (scale <= 0) return safe ? Number(unscaled) : unscaled.toString();

  const negative = unscaled < 0n;
  const digits = (negative ? -unscaled : unscaled)
    .toString()
    .padStart(scale + 1, '0');
  const text =
    `${negative ? '-' : ''}${digits.slice(0, -scale)}.${digits.slice(-scale)}`;

  return safe ? Number(text) : text;
}

/**
 * A converter for one column, chosen once from its Arrow type rather
 * than re-derived per cell.
 */
function converterFor(type: unknown): (v: unknown) => Scalar {
  if (
    type &&
    typeof type === 'object' &&
    'scale' in type &&
    typeof (type as { scale: unknown }).scale === 'number'
  ) {
    const scale = (type as { scale: number }).scale;
    return (v) => (v === null || v === undefined ? null : decimalToScalar(v, scale));
  }
  return toScalar;
}

/** Arrow's type object stringifies to a usable name; keep it simple. */
function typeName(t: unknown): string {
  if (t && typeof t === 'object' && 'toString' in t) {
    return String(t);
  }
  return 'Unknown';
}

export function toResultTable(
  table: ArrowishTable,
  epoch: number,
  elapsedMs: number,
): ResultTable {
  const columns: ResultColumn[] = [];
  const fields = table.schema.fields;

  for (let c = 0; c < fields.length; c++) {
    const field = fields[c];
    if (!field) continue;
    const vector = table.getChildAt(c);
    const values: Scalar[] = new Array(table.numRows);
    if (vector) {
      const convert = converterFor(field.type);
      for (let r = 0; r < table.numRows; r++) {
        values[r] = convert(vector.get(r));
      }
    } else {
      values.fill(null);
    }
    columns.push({ name: field.name, type: typeName(field.type), values });
  }

  return { columns, rowCount: table.numRows, epoch, elapsedMs };
}

export class DuckDbEngine implements QueryEngine {
  readonly name = 'duckdb';
  readonly #conn: ArrowishConnection;

  constructor(connection: ArrowishConnection) {
    this.#conn = connection;
  }

  async execute(sql: string, epoch: number): Promise<ResultTable> {
    const started = performance.now();
    let table: ArrowishTable;
    try {
      table = await this.#conn.query(sql);
    } catch (cause) {
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }
    return toResultTable(table, epoch, performance.now() - started);
  }

  async close(): Promise<void> {
    await this.#conn.close?.();
  }
}
