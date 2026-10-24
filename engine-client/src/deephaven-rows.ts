// A DEEPHAVEN SERVER'S ROWS (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23): the Arrow IPC stream a session reads a result's
// rows as (deephaven.ts `rows`), as a planned query's result. Each column's values are read by the tab's own Arrow
// decoders (duckdb.ts `decoderFor`), so a value reads the same whichever database answered (values.ts) -- but for what
// Deephaven writes in a form of its own, named by the column's `deephaven:type`: a BigDecimal (its scale, a 32-bit
// little-endian integer, then its unscaled value, big-endian two's complement) and a BigInteger (big-endian two's
// complement). The plan names and types the columns BY POSITION: the target writes each result column under its own
// name where Deephaven holds that name, else `__c<i>` (com.legend.sql.dialect.DeephavenBatch).

import { tableFromIPC } from 'apache-arrow';

import { decoderFor, type ArrowishVector, type Decode } from './duckdb.ts';
import { QueryError, type RawTable } from './engine.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable, Scalar } from './result.ts';
import { familyOf, hasTimeOfDay, isBoolean, isDay, isEitherDate, isFractional, isNumeric, isTimeOfDay } from './types.ts';
import { decimalText, exactInteger, timeText, timestampText } from './values.ts';
import type { FollowedRows } from './deephaven-live.ts';

/** An Arrow IPC stream of a Deephaven result as its columns and values, untyped: the plan types them (`byPosition`). */
export function deephavenRows(stream: Uint8Array, epoch: number, elapsedMs: number): RawTable {
  const table = tableFromIPC(stream);
  const columns = table.schema.fields.map((field, c) => {
    const vector = table.getChildAt(c) as unknown as ArrowishVector | null;
    const decode = decoderOf(field.name, String(field.type), field.metadata.get('deephaven:type'), field.type);
    const values: Scalar[] = new Array<Scalar>(table.numRows);
    for (let r = 0; r < table.numRows; r++) values[r] = vector ? decode(vector, r) : null;
    return { name: field.name, values };
  });
  return { columns, rowCount: table.numRows, epoch, elapsedMs };
}

/** A column's decoder: Deephaven's own forms by their Java type, every other by its Arrow type. */
function decoderOf(name: string, arrowType: string, javaType: string | undefined, type: unknown): Decode {
  const bytes = (v: ArrowishVector, r: number): Uint8Array | null => {
    const b = v.get(r);
    return b === null || b === undefined ? null : b as Uint8Array;
  };
  if (javaType === 'java.math.BigDecimal') {
    return (v, r) => {
      const b = bytes(v, r);
      if (b === null) return null;
      if (b.length < 5) throw new Error(`a Deephaven BigDecimal in '${name}' is ${b.length} bytes`);
      const scale = new DataView(b.buffer, b.byteOffset, 4).getInt32(0, true);
      return decimalText(twosComplement(b.subarray(4)), scale);
    };
  }
  if (javaType === 'java.math.BigInteger') {
    return (v, r) => {
      const b = bytes(v, r);
      return b === null ? null : exactInteger(twosComplement(b));
    };
  }
  // bytes in a form this does not read are refused, never shown as their numbers
  if (/^(Binary|LargeBinary|FixedSizeBinary)/.test(arrowType)) {
    throw new Error(`the Deephaven column '${name}' is ${javaType ?? 'binary'}, which the tab does not read`);
  }
  return decoderFor(type);
}

/** Big-endian two's complement bytes as the integer they hold. */
function twosComplement(b: Uint8Array): bigint {
  let n = 0n;
  for (const byte of b) n = (n << 8n) | BigInt(byte);
  return b.length > 0 && ((b[0] ?? 0) & 0x80) ? n - (1n << BigInt(8 * b.length)) : n;
}

/**
 * A Deephaven result typed by its plan, column by column in order: the plan's name and type for each. A column count
 * that differs, or a column under neither the plan's name nor its place's (`__c<i>`), is refused: the result is not
 * the plan's, a bug to surface, never a gap to fill.
 */
export function byPosition(raw: RawTable, plan: Plan): ResultTable {
  if (raw.columns.length !== plan.columns.length) {
    throw new QueryError(`the Deephaven result has ${raw.columns.length} columns, the plan ${plan.columns.length}`,
      plan.sql);
  }
  return {
    ...raw,
    columns: raw.columns.map((c, i) => {
      const p = plan.columns[i];
      if (p === undefined || (c.name !== p.name && c.name !== `__c${i}`)) {
        throw new QueryError(`the Deephaven result's column ${i} is '${c.name}', where the plan has '${p?.name}'`,
          plan.sql);
      }
      return { name: p.name, type: p.type, values: c.values };
    }),
  };
}

/**
 * A followed table's rows (deephaven-live.ts: Deephaven's JS library's values) as a planned query's result, each value
 * exactly as values.ts writes it -- the same as the Arrow rows of the same table (`deephavenRows`) -- by the plan's type
 * of its column: a 64-bit integer, an instant's nanoseconds, a decimal and a big integer from the library's own exact
 * text, a date its calendar day, a time of day from its parts. Named and typed by the plan by position (`byPosition`).
 */
export function followedTable(rows: FollowedRows, plan: Plan, epoch: number, elapsedMs: number): ResultTable {
  const columns = rows.columns.map((name, c) => {
    const type = plan.columns[c]?.type;
    const values = rows.values[c] ?? [];
    return { name, values: type === undefined ? values.map(() => null) : values.map((v) => followedValue(v, type, name)) };
  });
  const rowCount = rows.values[0]?.length ?? 0;
  return byPosition({ columns, rowCount, epoch, elapsedMs }, plan);
}

/**
 * One of the library's values as the Scalar values.ts writes for a column of the plan's `type` -- by the type's family
 * (types.ts, the one reader) and the value's own form: a whole number from its exact digits (an int a number, a long the
 * library's wrapper); a fractional one a number where Deephaven holds a double and exact text where it holds a
 * BigDecimal (the wrapper), as the Arrow rows of the same column are; an instant from its nanoseconds; a day its text; a
 * time of day from its parts. A value the type does not hold is refused, never shown as something else.
 */
export function followedValue(v: unknown, type: string, column: string): Scalar {
  if (v === null || v === undefined) return null;
  const refused = (): Error =>
    new Error(`the Deephaven column '${column}' (${type}) holds ${typeof v} ${String(v)}, which the tab does not read`);
  if (familyOf(type) === 'text') {
    if (typeof v === 'string') return v;
    throw refused();
  }
  if (isBoolean(type)) {
    if (typeof v === 'boolean') return v;
    throw refused();
  }
  if (isNumeric(type)) {
    if (!isFractional(type)) return exactInteger(BigInt(String(v)));
    return typeof v === 'number' ? v : javaDecimal(String(v));
  }
  if (hasTimeOfDay(type)) return timestampText(BigInt(String(v)) / 1000n);
  if (isDay(type)) {
    if (/^-?\d{4,}-\d{2}-\d{2}$/.test(String(v))) return String(v);
    throw refused();
  }
  // a Date -- Pure's either, what datePart and adjust answer -- by the value's own form: the library's day is its
  // text, its instant its nanoseconds (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K1)
  if (isEitherDate(type)) {
    const text = String(v);
    if (/^-?\d{4,}-\d{2}-\d{2}$/.test(text)) return text;
    if (/^-?\d+$/.test(text)) return timestampText(BigInt(text) / 1000n);
    throw refused();
  }
  if (isTimeOfDay(type)) {
    const t = v as { getHour?(): number; getMinute?(): number; getSecond?(): number; getNano?(): number };
    if (!t.getHour || !t.getMinute || !t.getSecond || !t.getNano) throw refused();
    const seconds = (BigInt(t.getHour()) * 60n + BigInt(t.getMinute())) * 60n + BigInt(t.getSecond());
    return timeText(seconds * 1_000_000n + BigInt(t.getNano()) / 1000n);
  }
  throw refused();
}

/** Java's BigDecimal text (`12.345`, `-1E+3`, `1.5E-7`) as exact decimal text. */
function javaDecimal(text: string): string {
  const m = /^(-?)(\d+)(?:\.(\d+))?(?:E([+-]?\d+))?$/.exec(text);
  if (!m) throw new Error(`'${text}' is not a decimal`);
  const fraction = m[3] ?? '';
  const unscaled = BigInt(`${m[1]}${m[2]}${fraction}`);
  return decimalText(unscaled, fraction.length - Number(m[4] ?? 0));
}
