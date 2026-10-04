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
function fraction(micros: bigint): string {
  if (micros === 0n) return '';
  return `.${pad(micros, 6).replace(/0+$/, '')}`;
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
