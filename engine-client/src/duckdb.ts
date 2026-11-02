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

import type { Receipt } from './receipt.ts';
import type { QueryEngine, RawColumn, RawTable } from './engine.ts';
import { QueryError, typedByPlan } from './engine.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable, Scalar } from './result.ts';
import {
  dayText, decimalText, decimalTextOfSafe, exactInteger, timeMicrosText, timeText, timestampMicrosText, timestampText,
} from './values.ts';

/**
 * The slice of duckdb-wasm's connection we actually use. Declaring it
 * structurally keeps this file testable and keeps duckdb-wasm's types
 * (which differ between its browser and node entry points) out of our
 * public surface.
 */
export interface ArrowishConnection {
  query(sql: string): ArrowishTable | Promise<ArrowishTable>;
  /**
   * The STREAMING form, and the only real cancellation DuckDB-WASM
   * offers. `query()` runs to completion inside one call and cannot be
   * interrupted; `send()` hands back batches as they are produced, so
   * a superseded query can be stopped between them via `cancelSent()`.
   *
   * Optional only because the interface is structural and some test
   * fakes implement `query()` alone. Both real duckdb-wasm builds
   * provide it, so in the product this is THE path -- `query()` is
   * not a fallback anyone falls back to, and a guardrail test pins
   * that so it cannot quietly become one.
   */
  send?(sql: string): Promise<AsyncIterable<ArrowishTable>>;
  /** Cancel a query started with `send()`. True if it was still pending. */
  cancelSent?(): Promise<boolean>;
  /**
   * Load one Arrow IPC stream into a table: `create` makes it, otherwise the
   * rows are appended. How a snap of a REMOTE plane lands here: the server's
   * Arrow chunks, unconverted.
   */
  insertArrowFromIPCStream?(
    buffer: Uint8Array,
    options: { name: string; schema?: string; create?: boolean },
  ): void | Promise<void>;
  /**
   * A statement prepared, to run with its `?`s bound in order: how a plan's step binds the values it is given (a write's
   * count into the query that answers it: plan-runner.ts). DuckDB-WASM's bindings send the values as JSON, so a value
   * bound is text, a number, true or false, or null.
   */
  prepare?(sql: string): ArrowishPrepared | Promise<ArrowishPrepared>;
  close?(): void | Promise<void>;
}

/** The connection as a plan holds it (DuckDbEngine.exclusive): no other query between its statements. */
export interface HeldConnection {
  /** A statement's rows, each its values by column; one with values to bind, prepared and run with them (refused by
   *  name where the connection prepares none). */
  rows(sql: string, params: readonly (string | number | boolean | null)[]): Promise<unknown[][]>;
  /** A statement's answer decoded as the engine's queries are (a catalog read inside the hold). */
  table(sql: string): Promise<RawTable>;
}

/** A prepared statement, as DuckDB-WASM's connections give one. */
export interface ArrowishPrepared {
  query(...params: unknown[]): ArrowishTable | Promise<ArrowishTable>;
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
  /** Arrow's chunks: the raw storage `get()` rounds (a timestamp to milliseconds). */
  readonly data?: readonly ArrowishData[];
}

/** One chunk of a vector, as Arrow-JS holds it: what a column's decode reads (columnDecoder). */
export interface ArrowishData {
  readonly values: ArrayLike<unknown>;
  readonly offset: number;
  readonly length: number;
  /** Optional: a fake of a vector may give none, and is then read cell by cell (columnDecoder). */
  readonly nullCount?: number;
  readonly nullBitmap?: Uint8Array | null;
  readonly valueOffsets?: ArrayLike<number> | null;
  readonly stride?: number;
  readonly dictionary?: unknown;
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

  // An integer exactly: a number while it is one, a bigint beyond 2^53 (values.ts).
  if (typeof v === 'bigint') return exactInteger(v);

  if (typeof v === 'number' || typeof v === 'string' ||
      typeof v === 'boolean') {
    return v;
  }

  // A raw Arrow buffer with no column type to interpret it. Decimals
  // reach `decimalToScalar` instead, via the per-column converter;
  // anything landing here is genuinely opaque, so stringify it rather
  // than invent a reading.
  if (ArrayBuffer.isView(v)) return String(v);

  if (typeof v === 'object') {
    // Struct, list, map: render as JSON rather than "[object Object]",
    // so a nested column is at least legible in the grid.
    try {
      return exactJson(v) ?? String(v);
    } catch {
      return String(v);
    }
  }

  return String(v);
}

/**
 * A nested value as JSON text, every number as the number it is: a 64-bit integer (a bigint
 * here) its digits, never a string of them -- `{"tier":2}` stays `{"tier":2}`, where
 * JSON.stringify, which refuses a bigint, had to be told to quote it. Otherwise
 * JSON.stringify's own rules: a value's `toJSON` first (Arrow's rows and vectors, a Date), a
 * boxed primitive its value, and what JSON cannot hold (undefined, a function, a symbol) left
 * out of an object and null in an array; a typed array is the array of its values. Undefined:
 * nothing to write.
 */
function exactJson(value: unknown): string | undefined {
  let v = value;
  if (v !== null && typeof v === 'object' && typeof (v as { toJSON?: unknown }).toJSON === 'function') {
    v = (v as { toJSON(): unknown }).toJSON();
  }
  if (v instanceof Number || v instanceof String || v instanceof Boolean || v instanceof BigInt) v = v.valueOf();
  if (typeof v === 'bigint') return v.toString();
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return JSON.stringify(v);
  if (typeof v !== 'object') return undefined;
  if (Array.isArray(v)) return `[${v.map((x) => exactJson(x) ?? 'null').join(',')}]`;
  if (ArrayBuffer.isView(v) && !(v instanceof DataView)) return exactJson(Array.from(v as unknown as ArrayLike<unknown>));
  const members = Object.entries(v).flatMap(([k, x]) => {
    const json = exactJson(x);
    return json === undefined ? [] : [`${JSON.stringify(k)}:${json}`];
  });
  return `{${members.join(',')}}`;
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
  // scale 0 holds an integer (DuckDB returns a SUM of integers as one): an exact integer.
  // Otherwise ALWAYS the exact text: a DECIMAL column is never half numbers, half strings.
  return scale <= 0 ? exactInteger(unscaled * 10n ** BigInt(-scale)) : decimalText(unscaled, scale);
}

/** One column's decoder: a vector and a row to the value, exactly as stored (values.ts). */
type Decode = (vector: ArrowishVector, row: number) => Scalar;

/** The raw stored value of a row, across Arrow's chunks: what `get()` would round. */
function rawAt(vector: ArrowishVector, row: number): unknown {
  let i = row;
  for (const chunk of vector.data ?? []) {
    if (i < chunk.length) return chunk.values[chunk.offset + i];
    i -= chunk.length;
  }
  return undefined;
}

/**
 * A decoder for one column, chosen once from its Arrow STORAGE type -- what the database
 * wrote -- and exact: a DATE is its calendar day (never a local-midnight instant), a
 * TIMESTAMP keeps its microseconds, a DECIMAL its digits. What the value MEANS is the
 * plan's compiler type; this only keeps it exact.
 */
function decoderFor(type: unknown): Decode {
  const name = String(type ?? '');
  const nonNull = (d: Decode): Decode => (v, r) => (v.get(r) === null || v.get(r) === undefined ? null : d(v, r));
  if (type && typeof type === 'object' && 'scale' in type
      && typeof (type as { scale: unknown }).scale === 'number' && /^Decimal/.test(name)) {
    const scale = (type as { scale: number }).scale;
    return nonNull((v, r) => decimalToScalar(v.get(r), scale));
  }
  if (/^Date32</.test(name)) return nonNull((v, r) => dayText(Number(rawAt(v, r))));
  if (/^Date64</.test(name)) return nonNull((v, r) => dayText(Math.floor(Number(v.get(r)) / 86_400_000)));
  const unit = /<(SECOND|MILLISECOND|MICROSECOND|NANOSECOND)/.exec(name)?.[1];
  const toMicros = (raw: unknown): bigint => {
    const n = BigInt(raw as bigint | number);
    return unit === 'SECOND' ? n * 1_000_000n : unit === 'MILLISECOND' ? n * 1000n
      : unit === 'NANOSECOND' ? n / 1000n : n;
  };
  if (/^Timestamp</.test(name)) return nonNull((v, r) => timestampText(toMicros(rawAt(v, r))));
  if (/^Time(32|64)</.test(name)) return nonNull((v, r) => timeText(toMicros(rawAt(v, r))));
  if (/^(Struct|List|FixedSizeList|Map)</.test(name)) return nonNull((v, r) => nestedJson(v.get(r), type));
  return (v, r) => toScalar(v.get(r));
}

/** A column's values written into `out` from `at`, a whole vector at a time (columnDecoder). */
export type ColumnDecode = (vector: ArrowishVector, out: Scalar[], at: number) => void;

/** One chunk's reader: its cell `i` (a non-null one) as the column's per-cell decoder answers it -- or CELL, for a value
 *  the reader does not hold exactly, which the per-cell decoder then decodes (or refuses) as it always has. */
type ChunkRead = (i: number) => Scalar | typeof CELL;

/** A fast reader's answer for a cell it leaves to the per-cell decoder (a timestamp no safe number holds: Arrow-JS's
 *  own get refuses an infinite one, and that refusal stays the decoder's, never a text made of it). */
const CELL: unique symbol = Symbol('decoded cell by cell');

const LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
const TWO_32 = 4_294_967_296;
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const utf8 = new TextDecoder('utf-8');
/** The most bytes of text a chunk is decoded whole for (the ASCII path): the most one kept cell keeps alive. */
const ASCII_WHOLE = 1 << 18;

/** The signed 64-bit integers of a chunk's BigInt64Array as their two 32-bit halves, read without a BigInt. */
function halves(values: ArrayLike<unknown>): Int32Array | undefined {
  if (!LITTLE_ENDIAN || !(values instanceof BigInt64Array || values instanceof BigUint64Array)) return undefined;
  return new Int32Array(values.buffer, values.byteOffset, values.length * 2);
}

/**
 * THE COLUMN'S DECODE (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, H1a): the same values `decoderFor` answers cell by cell
 * (it stays the one rule, and the decode of any chunk this reads no faster), read here from each chunk's own buffers --
 * no `get()` per cell (Arrow-JS finds the cell's chunk and dispatches on its type for each one), no second read for its
 * null check, no BigInt for an integer, a decimal or a timestamp a number holds exactly. Each fast reader repeats
 * Arrow-JS's own `get` for its type (visitor/get.mjs: a value buffer indexed by the chunk's own index, the validity
 * bitmap and a boolean's bits by the chunk's offset plus it) and the per-cell decoder's conversion after it; it reads a
 * chunk at offset 0 only, where the per-cell decoder's raw reads (rawAt) and `get` agree. The differential test holds
 * every fast reader to the per-cell decoder (engine-client/test/column-decode.test.ts).
 */
export function columnDecoder(type: unknown): ColumnDecode {
  const cellByCell = cellByCellDecoder(type);
  const perCell = decoderFor(type);
  const reader = chunkReader(type);
  if (reader === undefined) return cellByCell;
  return (v, out, at) => {
    const chunks = v.data;
    // a chunk as Arrow-JS makes one: at offset 0, no dictionary, its null count known (a fake without one is read cell
    // by cell), its buffers its type's and long enough (each reader's own check)
    const reads = chunks?.map((c) => (c.offset === 0 && c.dictionary === undefined && typeof c.nullCount === 'number'
      ? reader(c) : undefined));
    if (chunks === undefined || reads === undefined || reads.some((r) => r === undefined)
        || chunks.reduce((n, c) => n + c.length, 0) !== v.length) {
      cellByCell(v, out, at);
      return;
    }
    let o = at;
    for (const [k, c] of chunks.entries()) {
      const read = reads[k]!;
      // Arrow-JS's getValid: a bitmap only when the chunk has nulls and a bitmap of bytes
      const bitmap = (c.nullCount ?? 0) > 0 && c.nullBitmap && c.nullBitmap.byteLength > 0 ? c.nullBitmap : null;
      for (let i = 0; i < c.length; i++, o++) {
        const j = c.offset + i;
        if (bitmap !== null && (bitmap[j >> 3]! & (1 << (j % 8))) === 0) {
          out[o] = null;
          continue;
        }
        const x = read(i);
        out[o] = x === CELL ? perCell(v, o - at) : x;
      }
    }
  };
}

/** A column of `type` decoded cell by cell (decoderFor): the one rule, the decode of every type and chunk no fast reader
 *  reads, and the oracle columnDecoder's readers are held to (engine-client/test/column-decode.test.ts). */
export function cellByCellDecoder(type: unknown): ColumnDecode {
  const perCell = decoderFor(type);
  return (v, out, at) => {
    for (let r = 0; r < v.length; r++) out[at + r] = perCell(v, r);
  };
}

/** A chunk reader for a column of `type`, or none: a type read only cell by cell (a nested value, a dictionary, a
 *  binary, a 256-bit decimal, a date in milliseconds...). A reader answers none for a chunk whose buffers are not the
 *  ones its type stores. */
function chunkReader(type: unknown): ((c: ArrowishData) => ChunkRead | undefined) | undefined {
  const name = String(type ?? '');
  if (/^(Int8|Int16|Int32|Uint8|Uint16|Uint32|Float32|Float64)$/.test(name)) {
    // getNumeric: values[stride * i], stride 1; the per-cell decoder's toScalar keeps a number as it is
    return (c) => (ArrayBuffer.isView(c.values) && !(c.values instanceof BigInt64Array) && !(c.values instanceof BigUint64Array)
      && (c.stride ?? 1) === 1 && c.values.length >= c.length
      ? ((values) => (i: number) => values[i] as number)(c.values as ArrayLike<number>) : undefined);
  }
  if (name === 'Int64' || name === 'Uint64') {
    // getBigInts: values[i]; toScalar's exactInteger: a number while one holds it exactly, else the bigint
    const signed = name === 'Int64';
    return (c) => {
      const h = halves(c.values);
      if (h === undefined || c.values.length < c.length) return undefined;
      const values = c.values as ArrayLike<bigint>;
      return (i) => {
        const hi = h[2 * i + 1]!;
        const n = (signed ? hi : hi >>> 0) * TWO_32 + (h[2 * i]! >>> 0);
        return Number.isSafeInteger(n) ? n : exactInteger(values[i]!);
      };
    };
  }
  if (name === 'Bool') {
    // getBool: the bit at offset + i
    return (c) => (c.values instanceof Uint8Array && c.values.length * 8 >= c.length
      ? ((values) => (i: number) => (values[i >> 3]! & (1 << (i % 8))) !== 0)(c.values) : undefined);
  }
  if (name === 'Utf8') {
    // getUtf8: values from valueOffsets[i] to valueOffsets[i + 1], decoded as UTF-8 (Arrow-JS's own TextDecoder)
    return (c) => {
      const values = c.values;
      const offsets = c.valueOffsets;
      if (!(values instanceof Uint8Array) || !(offsets instanceof Int32Array)) return undefined;
      // getVariableWidthBytes: no value past the offsets it has
      const past = (i: number): boolean => i + 1 >= offsets.length;
      // a chunk whose bytes are all ASCII is decoded once: an ASCII byte is the one UTF-16 unit TextDecoder makes of it,
      // so a cell is the slice of the whole at its byte offsets (and no BOM, which TextDecoder drops, is ASCII). Only a
      // chunk of well-formed offsets (each no less than the one before), and of at most ASCII_WHOLE bytes: a cell kept
      // alone keeps its chunk's whole text alive, so the whole is bounded
      const start = offsets[0] ?? 0;
      const last = Math.min(c.length, offsets.length - 1);
      const end = offsets[last] ?? start;
      let ascii = start <= end && end <= values.length && end - start <= ASCII_WHOLE;
      for (let k = 0; ascii && k < last; k++) ascii = offsets[k]! <= offsets[k + 1]!;
      for (let b = start; ascii && b < end; b++) ascii = values[b]! < 0x80;
      if (ascii) {
        const whole = utf8.decode(values.subarray(start, end));
        return (i) => (past(i) ? null : whole.substring(offsets[i]! - start, offsets[i + 1]! - start));
      }
      return (i) => (past(i) ? null : utf8.decode(values.subarray(offsets[i]!, offsets[i + 1]!)));
    };
  }
  const t = type as { scale?: unknown; bitWidth?: unknown };
  if (/^Decimal/.test(name) && typeof t.scale === 'number' && t.scale > 0 && (t.bitWidth ?? 128) === 128) {
    // getDecimal: four 32-bit words, two's complement; decimalToScalar's exact text of the unscaled integer
    const scale = t.scale;
    return (c) => {
      const w = c.values;
      if (!LITTLE_ENDIAN || !(w instanceof Uint32Array) || (c.stride ?? 4) !== 4 || w.length < 4 * c.length) return undefined;
      return (i) => {
        const lo = w[4 * i]!;
        const mid = w[4 * i + 1]! | 0;
        const top = w[4 * i + 2]! | 0;
        const top2 = w[4 * i + 3]! | 0;
        // the upper 64 bits the sign of the lower 64's: the value is the lower 64, a number when it holds it exactly
        if (top === mid >> 31 && top2 === mid >> 31) {
          const n = mid * TWO_32 + lo;
          if (Number.isSafeInteger(n)) return decimalTextOfSafe(n, scale);
        }
        return decimalToScalar(bigDecimalWords(w, i), scale);
      };
    };
  }
  if (/^Date32</.test(name)) {
    // the per-cell decoder: dayText(Number(rawAt)), the raw days
    return (c) => (c.values instanceof Int32Array && c.values.length >= c.length
      ? ((values) => (i: number) => dayText(values[i]!))(c.values) : undefined);
  }
  const unit = /<(SECOND|MILLISECOND|MICROSECOND|NANOSECOND)/.exec(name)?.[1];
  const timestamp = /^Timestamp</.test(name);
  if ((timestamp || /^Time64</.test(name)) && unit !== undefined) {
    // the per-cell decoder: toMicros(rawAt) -- BigInt's truncating arithmetic -- then timestampText or timeText; a
    // value no safe number holds, by the same BigInt steps. A timestamp the per-cell decoder's own get refuses
    // (Arrow-JS's bigIntToNumber: a second's or a millisecond's raw value, a microsecond's thousandth, not a safe
    // integer -- an infinite one) is left to it (CELL), so the refusal stays its own, never a text made of an invalid
    // date (the H1a audit, 2026-11-02). A nanosecond's get, and a time's, refuse none.
    const scaleUp = unit === 'SECOND' ? 1_000_000 : unit === 'MILLISECOND' ? 1000 : 1;
    const text = timestamp ? timestampMicrosText : timeMicrosText;
    const unsafe = (b: bigint): boolean => b < MIN_SAFE || b > MAX_SAFE;
    const refusedByGet = (raw: bigint): boolean => timestamp && (unit === 'SECOND' || unit === 'MILLISECOND'
      ? unsafe(raw) : unit === 'MICROSECOND' ? unsafe(raw / 1000n) : false);
    const slow = (raw: bigint): string => {
      const n = unit === 'SECOND' ? raw * 1_000_000n : unit === 'MILLISECOND' ? raw * 1000n
        : unit === 'NANOSECOND' ? raw / 1000n : raw;
      return timestamp ? timestampText(n) : timeText(n);
    };
    return (c) => {
      const h = halves(c.values);
      if (h === undefined || !(c.values instanceof BigInt64Array) || c.values.length < c.length) return undefined;
      const values = c.values;
      return (i) => {
        const raw = h[2 * i + 1]! * TWO_32 + (h[2 * i]! >>> 0);
        if (Number.isSafeInteger(raw)) {
          const micros = unit === 'NANOSECOND' ? (raw - (raw % 1000)) / 1000 : raw * scaleUp;
          if (Number.isSafeInteger(micros)) return text(micros);
        }
        const big = values[i]!;
        return refusedByGet(big) ? CELL : slow(big);
      };
    };
  }
  return undefined;
}

/** Cell `i`'s four 32-bit words as the signed 128-bit integer they hold (what Arrow-JS's decimal's text spells). */
function bigDecimalWords(w: Uint32Array, i: number): bigint {
  const unsigned = (BigInt(w[4 * i + 3]!) << 96n) | (BigInt(w[4 * i + 2]!) << 64n) | (BigInt(w[4 * i + 1]!) << 32n)
    | BigInt(w[4 * i]!);
  return unsigned >= 1n << 127n ? unsigned - (1n << 128n) : unsigned;
}

/**
 * A nested value (a struct's, a list's, a map's) as JSON text, each leaf by its Arrow STORAGE type exactly as a
 * top-level cell of that type is read (decoderFor): a decimal its exact text, never the unscaled integer its row view
 * hands out; a date its day and a timestamp its text, never the epoch milliseconds the row view hands out; a 64-bit
 * integer its digits. A nested timestamp keeps the milliseconds the row view keeps (its raw storage is the child
 * vector's). The phase 3 audit, 2026-10-27: a nested decimal crossed mis-scaled and a nested date failed to parse.
 */
function nestedJson(value: unknown, type: unknown): string {
  if (value === null || value === undefined) return 'null';
  const name = String(type ?? '');
  const t = type as { children?: readonly { name: string; type: unknown }[]; scale?: unknown; unit?: unknown };
  if (/^Decimal/.test(name) && typeof t.scale === 'number') {
    const exact = decimalToScalar(value, t.scale);
    return typeof exact === 'string' ? JSON.stringify(exact) : String(exact);
  }
  if (/^Date(32|64)</.test(name)) return JSON.stringify(dayText(Math.floor(Number(value) / 86_400_000)));
  if (/^Timestamp</.test(name)) return JSON.stringify(timestampText(BigInt(Math.trunc(Number(value))) * 1000n));
  const children = t.children ?? [];
  if (/^Struct</.test(name)) {
    const row = typeof (value as { toJSON?: unknown }).toJSON === 'function'
      ? (value as { toJSON(): Record<string, unknown> }).toJSON() : value as Record<string, unknown>;
    return `{${children.map((f) => `${JSON.stringify(f.name)}:${nestedJson(row[f.name], f.type)}`).join(',')}}`;
  }
  if (/^(List|FixedSizeList)</.test(name)) {
    const element = children[0]?.type;
    return `[${Array.from(value as Iterable<unknown>, (x) => nestedJson(x, element)).join(',')}]`;
  }
  if (/^Map</.test(name)) {
    const entry = children[0]?.type as { children?: readonly { name: string; type: unknown }[] } | undefined;
    const valueType = entry?.children?.[1]?.type;
    const entries = typeof (value as { toJSON?: unknown }).toJSON === 'function'
      ? (value as { toJSON(): Record<string, unknown> }).toJSON() : value as Record<string, unknown>;
    return `{${Object.entries(entries).map(([k, x]) => `${JSON.stringify(k)}:${nestedJson(x, valueType)}`).join(',')}}`;
  }
  return exactJson(value) ?? JSON.stringify(String(value));
}

/** An Arrow table's columns and values; the PLAN types them (engine.ts typedByPlan). */
export function toRawTable(
  table: ArrowishTable,
  epoch: number,
  elapsedMs: number,
): RawTable {
  const columns: RawColumn[] = [];
  const fields = table.schema.fields;

  for (let c = 0; c < fields.length; c++) {
    const field = fields[c];
    if (!field) continue;
    const vector = table.getChildAt(c);
    const values: Scalar[] = new Array(table.numRows);
    if (vector) {
      columnDecoder(field.type)(vector, values, 0);
    } else {
      values.fill(null);
    }
    // no Pure type: the plan types a planned query's columns (engine.ts typedByPlan)
    columns.push({ name: field.name, values });
  }

  return { columns, rowCount: table.numRows, epoch, elapsedMs };
}

/**
 * Stitches streamed record batches into one columnar result.
 *
 * A batch carries the same shape as a table -- schema plus
 * `getChildAt` -- so the per-column converter is chosen ONCE, from the
 * first batch's field types, and reused. Re-deriving it per batch
 * would be the obvious way to make streaming slower than the
 * non-streaming path it replaced.
 *
 * An empty result still has to produce its COLUMNS: a query that
 * matches no rows must render an empty grid with headers, not a grid
 * with no columns at all.
 */
class BatchAccumulator {
  #names: string[] = [];
  #decode: ColumnDecode[] = [];
  #values: Scalar[][] = [];
  #rows = 0;
  #started = false;

  add(batch: ArrowishTable): void {
    if (!this.#started) {
      this.#started = true;
      for (const field of batch.schema.fields) {
        this.#names.push(field.name);
        this.#decode.push(columnDecoder(field.type));
        this.#values.push([]);
      }
    }
    for (let c = 0; c < this.#names.length; c++) {
      const vector = batch.getChildAt(c);
      const out = this.#values[c] as Scalar[];
      if (vector) {
        (this.#decode[c] as ColumnDecode)(vector, out, this.#rows);
      } else {
        for (let r = 0; r < batch.numRows; r++) out.push(null);
      }
    }
    this.#rows += batch.numRows;
  }

  build(epoch: number, elapsedMs: number): RawTable {
    return {
      columns: this.#names.map((name, i) => ({
        name,
        values: this.#values[i] as Scalar[],
      })),
      rowCount: this.#rows,
      epoch,
      elapsedMs,
    };
  }
}

export class DuckDbEngine implements QueryEngine {
  readonly name = 'duckdb';
  /** The database type of what this tab holds, as a Pure connection names it: what a model of its tables declares. */
  readonly databaseType = 'DuckDB';
  readonly #conn: ArrowishConnection;
  /** Tail of the queue of queries on this connection. See #serialised. */
  #chain: Promise<void> = Promise.resolve();
  /** Remote files a mounted source reads over HTTP (`readsRemote`), for the receipt. */
  readonly #reads: string[] = [];

  constructor(connection: ArrowishConnection) {
    this.#conn = connection;
  }

  /**
   * The host mounted a remote file (remote.ts `mountRemote`): queries here read it over HTTP.
   * Said on every receipt, so "ran in this tab" never hides that bytes came from elsewhere.
   */
  readsRemote(url: string): void {
    if (!this.#reads.includes(url)) this.#reads.push(url);
  }

  /** What this engine can say for a query it ran: here, and what it read from elsewhere. */
  receipt(): Receipt {
    return {
      plane: 'tab',
      where: "this tab's DuckDB",
      ...(this.#reads.length > 0 ? { reading: [...this.#reads] } : {}),
    };
  }

  /**
   * WHAT CANCELLATION MEANS HERE, precisely.
   *
   * Two paths, and the difference is real rather than cosmetic.
   *
   * With `send()` the query streams back in batches, so a superseded
   * query is CANCELLED -- `cancelSent()` stops it between batches and
   * DuckDB abandons the rest of the work. That is the path a real
   * duckdb-wasm connection takes.
   *
   * Without it, `query()` runs to completion inside one C++ call with
   * no interrupt, and the only honest win is the query that never
   * STARTS: levels are fetched in sequence, so during a burst every
   * request behind the one in flight is still avoidable.
   *
   * The distinction is worth keeping straight, because a UI that says
   * "cancelled" while a worker is still pegged is worse than one that
   * admits it is busy -- the next interaction is then slow for no
   * reason the user can see.
   *
   * THE BRANCH IS ON CAPABILITY, NOT ON THE SIGNAL, and that took a
   * correction. Streaming was first gated on a signal being passed,
   * which quietly left two live paths in the product: snapshotting and
   * drill-through pass no signal, so they took the non-streaming route
   * for no better reason than that nobody had threaded an argument to
   * them. Two materialisation paths that can drift apart is exactly
   * the shape this codebase refuses elsewhere. A connection that can
   * stream now always streams; the signal only decides whether the
   * stream can be cut short.
   */
  /**
   * THE CONNECTION HELD FOR A PLAN (plan-runner.ts: an Output into a table, docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): its
   * steps run one after another with no query of any grid between them -- the queue's (#serialised) for the whole plan,
   * so its transaction holds its own statements alone (HeldConnection).
   */
  exclusive<T>(work: (held: HeldConnection) => Promise<T>): Promise<T> {
    const conn = this.#conn;
    const rowsOf = (t: ArrowishTable): unknown[][] => Array.from({ length: t.numRows }, (_, r) =>
      t.schema.fields.map((_f, c) => t.getChildAt(c)?.get(r)));
    return this.#serialised(() => work({
      rows: async (sql, params) => {
        if (params.length === 0) return rowsOf(await conn.query(sql));
        if (conn.prepare === undefined) throw new Error("this tab's database binds no statement's values");
        const prepared = await conn.prepare(sql);
        try {
          return rowsOf(await prepared.query(...params));
        } finally {
          await prepared.close?.();
        }
      },
      table: (sql) => this.#whole(sql, 0, undefined, performance.now()),
    }));
  }

  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return { ...typedByPlan(await this.run(plan.sql, epoch, signal), plan), receipt: this.receipt() };
  }

  async run(
    sql: string,
    epoch: number,
    signal?: AbortSignal,
  ): Promise<RawTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return this.#serialised(async () => {
      // Checked AGAIN after waiting for the connection. This is where
      // the promise of "queries behind the one in flight never start"
      // is actually kept: by the time the connection frees up, a
      // superseded query simply never runs.
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const started = performance.now();
      if (typeof this.#conn.send === 'function') {
        const acc = new BatchAccumulator();
        await this.#batches(sql, signal, (batch) => acc.add(batch));
        return acc.build(epoch, performance.now() - started);
      }
      return this.#whole(sql, epoch, signal, started);
    });
  }

  /**
   * A planned query's rows a batch at a time, as DuckDB produces them: each handed over
   * typed by the plan, none kept. A connection that cannot stream hands over its whole
   * result as one chunk (the same capability branch as `run`).
   */
  async stream(
    plan: Plan,
    epoch: number,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return this.#serialised(async () => {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const started = performance.now();
      if (typeof this.#conn.send === 'function') {
        await this.#batches(plan.sql, signal, (batch) =>
          onChunk(typedByPlan(toRawTable(batch, epoch, performance.now() - started), plan)));
        return;
      }
      onChunk(typedByPlan(await this.#whole(plan.sql, epoch, signal, started), plan));
    });
  }

  /**
   * Replace `target` with the rows of `chunks`, each a whole Arrow IPC stream
   * (the warehouse's chunks): the first creates the table, the rest append.
   * On the query queue, so no query reads a half-loaded table.
   */
  async loadArrow(
    target: { readonly schema?: string; readonly table: string },
    chunks: AsyncIterable<Uint8Array>,
    /** The planner's statements run first, in the same turn (its schema made, the table there dropped: snap.ts). */
    prepare: readonly string[] = [],
  ): Promise<void> {
    const insert = this.#conn.insertArrowFromIPCStream;
    if (typeof insert !== 'function') {
      throw new Error('this DuckDB connection cannot load Arrow data');
    }
    const where = target.schema ? `${target.schema}.${target.table}` : target.table;
    return this.#serialised(async () => {
      for (const sql of prepare) await this.#conn.query(sql);
      let first = true;
      for await (const bytes of chunks) {
        await insert.call(this.#conn, bytes, {
          name: target.table,
          ...(target.schema ? { schema: target.schema } : {}),
          create: first,
        });
        first = false;
      }
      if (first) throw new Error(`no data came back to load into ${where}`);
    });
  }

  /**
   * One query on the connection at a time.
   *
   * A DuckDB connection holds ONE pending query. Overlapping them
   * corrupts both -- a live-engine test caught the winner coming back
   * with zero rows when a second query was sent while the first was
   * still streaming.
   *
   * It also makes cancellation safe, which matters more. `cancelSent()`
   * cancels whatever is pending on the CONNECTION, not a particular
   * query, so cancelling a superseded query after the next one had
   * started would have killed the query the user is actually waiting
   * for. Serialising means the only cancellable query is ours.
   */
  async #serialised<T>(run: () => Promise<T>): Promise<T> {
    const previous = this.#chain;
    let release!: () => void;
    this.#chain = new Promise<void>((r) => {
      release = r;
    });
    // Never inherit a failure from the query ahead of us in the queue.
    await previous.catch(() => {});
    try {
      return await run();
    } finally {
      release();
    }
  }

  async #whole(
    sql: string,
    epoch: number,
    signal: AbortSignal | undefined,
    started: number,
  ): Promise<RawTable> {
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
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return toRawTable(table, epoch, performance.now() - started);
  }

  /**
   * The streaming path -- the ONLY path for a connection that can
   * stream: each batch of `sql`'s result, in order, to `onBatch`
   * (`run` accumulates them, `stream` hands them on). `signal` is
   * optional: without one the batches are simply consumed to the end,
   * which is what a snap or a drill-through wants, and the result is
   * identical either way; with one, the query is cancelled between batches.
   */
  async #batches(
    sql: string,
    signal: AbortSignal | undefined,
    onBatch: (batch: ArrowishTable) => void,
  ): Promise<void> {
    const send = this.#conn.send;
    if (!send) throw new Error('streaming unavailable');

    const cancel = async (): Promise<void> => {
      // Best effort by design: cancelSent reports whether the query
      // was still pending, and a query that already finished is not a
      // failure to cancel -- there was simply nothing left to stop.
      try {
        await this.#conn.cancelSent?.();
      } catch {
        /* the query had already finished */
      }
    };

    let batches: AsyncIterable<ArrowishTable>;
    try {
      batches = await send.call(this.#conn, sql);
    } catch (cause) {
      if (signal?.aborted) throw signal.reason ?? cause;
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }

    try {
      for await (const batch of batches) {
        if (signal?.aborted) {
          await cancel();
          throw signal.reason ?? new Error('aborted');
        }
        onBatch(batch);
      }
    } catch (cause) {
      if (signal?.aborted) {
        await cancel();
        throw signal.reason ?? cause;
      }
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }

    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
  }

  async close(): Promise<void> {
    await this.#conn.close?.();
  }
}
