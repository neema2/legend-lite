// A cell's value, EXACTLY as the database stored it
// (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T3).
//
// JavaScript has no calendar date, no microsecond timestamp and no exact decimal, and
// every stand-in it offers is lossy: a `Date` is an instant read in the viewer's time
// zone (a DATE drifted a day east or west of UTC), `get()` rounds a timestamp to
// milliseconds, a double drops a DECIMAL's digits past 2^53. So a value keeps the
// database's own exact form, as text where JavaScript has no type for it:
//
//   StrictDate  'YYYY-MM-DD'                   a calendar day, no instant, no zone
//   DateTime    'YYYY-MM-DDTHH:MM:SS[.ffffff]' as stored (UTC), to the microsecond
//   StrictTime  'HH:MM:SS[.ffffff]'
//   Decimal     exact decimal text             '12345678901234567.89'
//   Integer     a number when exactly one, a bigint beyond 2^53
//
// What a value MEANS is its column's compiler type; this module only keeps it exact.

import { isNumeric } from './types.ts';

const MS_PER_DAY = 86_400_000;
const pad = (n: number | bigint, w = 2): string => String(n).padStart(w, '0');

/** A year as the calendar writes it: four digits, a sign before the common era. */
function yearText(y: number): string {
  return y < 0 ? `-${pad(-y, 4)}` : pad(y, 4);
}

/** Days since 1970-01-01 as its calendar day. UTC arithmetic: no zone, no 19xx for year 45. */
export function dayText(days: number): string {
  const d = new Date(days * MS_PER_DAY);
  return `${yearText(d.getUTCFullYear())}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

/** Microseconds since the epoch as the stored timestamp, to the microsecond. */
export function timestampText(micros: bigint): string {
  let secs = micros / 1_000_000n;
  let frac = micros % 1_000_000n;
  if (frac < 0n) {
    frac += 1_000_000n;
    secs -= 1n;
  }
  const d = new Date(Number(secs) * 1000);
  const day = `${yearText(d.getUTCFullYear())}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
  const time = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  return `${day}T${time}${fraction(frac)}`;
}

/** Microseconds since midnight as a time of day. */
export function timeText(micros: bigint): string {
  const secs = micros / 1_000_000n;
  const h = secs / 3600n;
  const m = (secs / 60n) % 60n;
  const s = secs % 60n;
  return `${pad(h)}:${pad(m)}:${pad(s)}${fraction(micros % 1_000_000n)}`;
}

/** `.ffffff` with its trailing zeros dropped; nothing for a whole second. */
function fraction(micros: bigint | number): string {
  if (micros === 0n || micros === 0) return '';
  return `.${pad(micros, 6).replace(/0+$/, '')}`;
}

/**
 * `timestampText` of a safe integer, without a BigInt (a column's decode, duckdb.ts columnDecoder: one cell of a million
 * need not allocate one): the same text for the same microseconds, the same steps in a number's exact integer
 * arithmetic -- `%` keeps the dividend's sign as BigInt's does, and the quotient of a difference that is a multiple of
 * the divisor is exact. `micros` must be a safe integer (Number.isSafeInteger); a larger one is BigInt's.
 */
export function timestampMicrosText(micros: number): string {
  let frac = micros % 1_000_000;
  let secs = (micros - frac) / 1_000_000;
  if (frac < 0) {
    frac += 1_000_000;
    secs -= 1;
  }
  // the UTC calendar day and time of `secs`, as Date's getUTC* read them (the proleptic Gregorian calendar), computed
  // without one: days since the epoch by floor division, then the civil date of the day (civilOf)
  const days = Math.floor(secs / 86_400);
  const inDay = secs - days * 86_400;
  const civil = civilOf(days);
  const h = Math.floor(inDay / 3600);
  const m = Math.floor((inDay - h * 3600) / 60);
  const s = inDay - h * 3600 - m * 60;
  return `${yearText(civil.year)}-${TWO[civil.month]}-${TWO[civil.day]}T${TWO[h]}:${TWO[m]}:${TWO[s]}${fractionOf(frac)}`;
}

/** 00 to 99, as pad writes them. */
const TWO = Array.from({ length: 100 }, (_, n) => String(n).padStart(2, '0'));

/**
 * The proleptic Gregorian date of a day counted from 1970-01-01 (Howard Hinnant's civil_from_days): what Date's
 * getUTCFullYear, getUTCMonth + 1 and getUTCDate answer for that day, held to them day by day over the range a safe
 * number of microseconds reaches (engine-client/test/column-decode.test.ts).
 */
function civilOf(days: number): { readonly year: number; readonly month: number; readonly day: number } {
  const z = days + 719_468;
  const era = Math.floor(z / 146_097);
  const doe = z - era * 146_097;
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36_524) - Math.floor(doe / 146_096)) / 365);
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const day = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const month = mp < 10 ? mp + 3 : mp - 9;
  return { year: yoe + era * 400 + (month <= 2 ? 1 : 0), month, day };
}

/** `fraction` of a number, its trailing zeros dropped by hand (no expression per cell). */
function fractionOf(micros: number): string {
  if (micros === 0) return '';
  const digits = String(micros).padStart(6, '0');
  let end = digits.length;
  while (end > 0 && digits.charCodeAt(end - 1) === 48) end--;
  return `.${digits.slice(0, end)}`;
}

/** `timeText` of a safe integer, without a BigInt, as timestampMicrosText is timestampText's: BigInt's truncating
 *  division, step by step. */
export function timeMicrosText(micros: number): string {
  const rest = micros % 1_000_000;
  const secs = (micros - rest) / 1_000_000;
  const h = (secs - (secs % 3600)) / 3600;
  const m = ((secs - (secs % 60)) / 60) % 60;
  const s = secs % 60;
  return `${pad(h)}:${pad(m)}:${pad(s)}${fraction(rest)}`;
}

/** `decimalText` of a safe integer, without a BigInt: its digits are a number's own (`String` of a safe integer is
 *  its exact digits), split at the scale as decimalText splits them. `scale` must be positive. */
export function decimalTextOfSafe(unscaled: number, scale: number): string {
  const negative = unscaled < 0;
  const digits = String(negative ? -unscaled : unscaled).padStart(scale + 1, '0');
  return `${negative ? '-' : ''}${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
}

/** An unscaled integer and its scale as exact decimal text: (1234567, 2) is '12345.67'. */
export function decimalText(unscaled: bigint, scale: number): string {
  if (scale <= 0) return (unscaled * 10n ** BigInt(-scale)).toString();
  const negative = unscaled < 0n;
  const digits = (negative ? -unscaled : unscaled).toString().padStart(scale + 1, '0');
  return `${negative ? '-' : ''}${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
}

/** An integer exactly: a number while it is one, a bigint beyond 2^53. */
export function exactInteger(v: bigint): number | bigint {
  return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
}

/** A numeric cell as an exact decimal: its unscaled integer and scale. Null when it is not one. */
export function asDecimal(v: unknown): { unscaled: bigint; scale: number } | null {
  if (typeof v === 'bigint') return { unscaled: v, scale: 0 };
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return null;
    return asDecimal(Number.isInteger(v) ? BigInt(v) : String(v));
  }
  if (typeof v !== 'string') return null;
  const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(v.trim());
  if (!m) return null;
  const frac = m[3] ?? '';
  const unscaled = BigInt(`${m[2]}${frac}`);
  return { unscaled: m[1] === '-' ? -unscaled : unscaled, scale: frac.length };
}

/** The exact sum of numeric cells, as decimal text at the widest scale among them. */
export function exactSum(values: readonly unknown[]): string {
  let scale = 0;
  const parts: { unscaled: bigint; scale: number }[] = [];
  for (const v of values) {
    const d = asDecimal(v);
    if (d) {
      parts.push(d);
      if (d.scale > scale) scale = d.scale;
    }
  }
  let total = 0n;
  for (const d of parts) total += d.unscaled * 10n ** BigInt(scale - d.scale);
  return decimalText(total, scale);
}

/**
 * A timestamp as a server wrote it (legend-engine: `2024-01-02T03:04:05.123456000+0000`)
 * as the stored text: the zone dropped (UTC, as stored), the fraction to the microsecond
 * with its trailing zeros dropped. Anything else is returned as it came.
 */
export function timestampFromText(text: string): string {
  const m = /^(-?\d{4,}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(?:Z|[+-]00:?00)?$/.exec(text);
  if (!m) return text;
  const frac = (m[3] ?? '').slice(0, 6).replace(/0+$/, '');
  return `${m[1]}T${m[2]}${frac ? `.${frac}` : ''}`;
}

/**
 * JSON with its numbers EXACT: an integer past 2^53 becomes a bigint and a number whose
 * digits a double would drop keeps its text; every other number is the number it was.
 * JSON.parse hands the reviver each number's source text (a current browser's feature);
 * one that cannot is refused rather than read lossily.
 */
export function parseExact(text: string): unknown {
  return JSON.parse(text, function reviver(_key: string, value: unknown, context?: { source?: string }) {
    if (typeof value !== 'number') return value;
    const source = context?.source;
    if (source === undefined) {
      throw new Error('this browser cannot read numbers exactly (JSON.parse source text access); '
        + 'DataCube needs a current Chrome, Edge, Firefox or Safari');
    }
    if (/^-?\d+$/.test(source)) {
      return Number.isSafeInteger(value) ? value : exactInteger(BigInt(source));
    }
    const plain = source.includes('e') || source.includes('E') ? null
      : source.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
    return plain === null || String(value) === plain ? value : source;
  } as (this: unknown, key: string, value: unknown) => unknown);
}

/**
 * A JSON number with a fraction or an exponent, kept with the digits it was written in (`parseKeepingDigits`): what it
 * means is its column's type -- a Decimal's exact text, any other number the number `parseExact` makes of it.
 */
export class WrittenNumber {
  readonly value: number;
  readonly source: string;

  constructor(value: number, source: string) {
    this.value = value;
    this.source = source;
  }

  /** As `parseExact` reads it: the number, or its text where a double would drop its digits. */
  exact(): number | string {
    const plain = this.source.includes('e') || this.source.includes('E') ? null
      : this.source.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
    return plain === null || String(this.value) === plain ? this.value : this.source;
  }
}

/**
 * JSON as `parseExact` reads it, but a number with a fraction or an exponent kept as a `WrittenNumber` (its value and its
 * digits as written), for a reader that types each value by its column (the legend-engine adapter: a Decimal keeps its
 * scale, `12.30` as written, docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D21). An integer reads as `parseExact` reads it.
 */
export function parseKeepingDigits(text: string): unknown {
  return JSON.parse(text, function reviver(_key: string, value: unknown, context?: { source?: string }) {
    if (typeof value !== 'number') return value;
    const source = context?.source;
    if (source === undefined) {
      throw new Error('this browser cannot read numbers exactly (JSON.parse source text access); '
        + 'DataCube needs a current Chrome, Edge, Firefox or Safari');
    }
    if (/^-?\d+$/.test(source)) {
      return Number.isSafeInteger(value) ? value : exactInteger(BigInt(source));
    }
    return new WrittenNumber(value, source);
  } as (this: unknown, key: string, value: unknown) => unknown);
}

/**
 * A cell of a NUMERIC column as a double, for PRESENTATION only -- a heatmap's colour, a
 * chart's pixel, a cell's sign: a number, a bigint, or a decimal's exact text. Numeric by
 * the column's COMPILER type, never by the value's own: a String column of '00501' holds no
 * numbers. Null for anything else. Never for a value that is shown, exported, summed or
 * written back: those stay exact.
 */
export function numberOf(v: unknown, type: string | undefined): number | null {
  if (!isNumeric(type)) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'string' && /^-?\d+(\.\d+)?$/.test(v)) return Number(v);
  return null;
}
