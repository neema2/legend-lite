// Cell formatting, on the main thread, against a cache.
//
// Measured on this machine, per call:
//
//   new Intl.NumberFormat(...)      4,579ns     .format()    244ns
//   new Intl.DateTimeFormat(...)   22,697ns     .format()    964ns
//
// A viewport is ~1,200 cells. Constructing a formatter per cell costs
// ~5.5ms for numbers and ~27ms for dates -- the date case alone blows a
// 16ms frame budget on formatting alone. Reusing cached formatters puts
// the whole viewport at ~0.3-1.2ms. So the cache is a correctness
// requirement for smooth scrolling, not an optimisation.
//
// Formats are attached to a COLUMN, not to rendered cells. That is what
// makes formatting survive a refresh: re-running the query cannot lose
// it, because it was never stored on the results. It also gives us
// Power BI's "dynamic format string" behaviour for free -- switching a
// cube between currency, percent and basis points changes the column's
// format, while the measure underneath stays one numeric value.

import type { Scalar } from '../../engine-client/src/result.ts';
import { prettyJson } from './json-shape.ts';
import { hasTimeOfDay, isNumeric, isTemporal, isTimeOfDay, isVariant } from '../../engine-client/src/types.ts';

export type FormatKind =
  | 'auto'
  | 'number'
  | 'percent'
  | 'currency'
  | 'date'
  | 'datetime'
  | 'text';

/**
 * Number scales, with DataCube's own names and suffixes.
 *
 * A scale is a divisor AND a suffix: the point of "$1.2m" is that it
 * is readable, not that it is 1,200,000 divided by a million.
 */
export type NumberScale =
  | 'basisPoints'
  | 'percent'
  | 'thousands'
  | 'millions'
  | 'billions'
  | 'trillions'
  | 'auto';

export const SCALES: Readonly<
  Record<Exclude<NumberScale, 'auto'>, { divisor: number; suffix: string }>
> = {
  basisPoints: { divisor: 1e-4, suffix: 'bp' },
  percent: { divisor: 1e-2, suffix: '%' },
  thousands: { divisor: 1e3, suffix: 'k' },
  millions: { divisor: 1e6, suffix: 'm' },
  billions: { divisor: 1e9, suffix: 'b' },
  trillions: { divisor: 1e12, suffix: 't' },
};

/** Text case, applied after formatting. */
export type FontCase = 'lowercase' | 'uppercase' | 'capitalize';

export interface ColumnFormat {
  readonly kind: FormatKind;
  readonly locale?: string;
  readonly minimumFractionDigits?: number;
  readonly maximumFractionDigits?: number;
  /** Fixed decimal places, DataCube's `decimals`: sets both bounds. */
  readonly decimals?: number;
  /** Thousands separators. On by default; off for an id-like number. */
  readonly displayCommas?: boolean;
  /** ISO 4217 code; required when kind is 'currency'. */
  readonly currency?: string;
  /**
   * Named scale, applied before formatting and suffixed after it.
   * Takes precedence over the raw `scale` divisor below.
   */
  readonly numberScale?: NumberScale;
  /** Raw divisor, for a scale the named set does not cover. */
  readonly scale?: number;
  /**
   * A unit glued after the value ('kg' → "12.5kg"), or before it when
   * it starts with `_` ('_$' → "$12.5"). Distinct from a scale.
   */
  readonly unit?: string;
  /** Rendered for null. Empty string by default, never "null". */
  readonly nullText?: string;
  /** Wrap negatives in parentheses, as finance expects. */
  readonly negativeParens?: boolean;
  /** Text case, applied last so it cannot disturb the number. */
  readonly fontCase?: FontCase;
}

/**
 * Pick a scale from the magnitude, for `auto`.
 *
 * Chosen per VALUE rather than per column: a column holding both 900
 * and 4,000,000,000 is unreadable at either fixed scale, and the
 * suffix says which one each cell used.
 */
export function autoScale(n: number): {
  divisor: number;
  suffix: string;
} {
  const abs = Math.abs(n);
  if (abs >= 1e12) return SCALES.trillions;
  if (abs >= 1e9) return SCALES.billions;
  if (abs >= 1e6) return SCALES.millions;
  if (abs >= 1e3) return SCALES.thousands;
  return { divisor: 1, suffix: '' };
}

function applyCase(text: string, c: FontCase | undefined): string {
  switch (c) {
    case 'lowercase':
      return text.toLowerCase();
    case 'uppercase':
      return text.toUpperCase();
    case 'capitalize':
      return text.replace(/\b\p{L}/gu, (ch) => ch.toUpperCase());
    default:
      return text;
  }
}

export const DEFAULT_FORMAT: ColumnFormat = { kind: 'auto' };

/**
 * THE compact number: a magnitude scale per value and one place ("1.2m", "450k", "12.5") --
 * a chart's axis, where room is short. Rendered by `FormatterCache.format` like every other
 * number, never by hand.
 */
export const COMPACT_FORMAT: ColumnFormat = {
  kind: 'number', numberScale: 'auto', maximumFractionDigits: 1,
};

/**
 * Cache key. Only the fields that affect the Intl object belong here --
 * nullText and negativeParens are applied by us afterwards, so folding
 * them into the key would split the cache for no reason.
 */
function intlKey(f: ColumnFormat): string {
  return [
    f.kind,
    f.locale ?? '',
    f.minimumFractionDigits ?? '',
    f.maximumFractionDigits ?? '',
    f.decimals ?? '',
    f.displayCommas === false ? 'nogroup' : '',
    f.currency ?? '',
    // NUL joins, so no field's value can forge another key.
  ].join('\u0000');
}

/**
 * Resolve fraction-digit bounds so they can never cross.
 *
 * Intl THROWS ("maximumFractionDigits value is out of range") when the
 * minimum exceeds the maximum, and the easy way to cause that is to set
 * only a maximum and let the minimum keep a larger default -- exactly
 * what "currency with 0 decimals" does against a default minimum of 2.
 * A display preference must never be able to throw, so an explicit
 * maximum pulls the default minimum down with it.
 */
function fractionDigits(
  f: ColumnFormat,
  defaultMin: number,
  defaultMax: number,
): { minimumFractionDigits: number; maximumFractionDigits: number } {
  // `decimals` is DataCube's fixed-places setting, so it pins BOTH
  // bounds: "2 decimals" means 1.5 renders as 1.50, not as 1.5.
  if (f.decimals !== undefined) {
    return {
      minimumFractionDigits: f.decimals,
      maximumFractionDigits: f.decimals,
    };
  }
  const max = f.maximumFractionDigits ?? defaultMax;
  const min = f.minimumFractionDigits ?? Math.min(defaultMin, max);
  return {
    minimumFractionDigits: Math.min(min, max),
    maximumFractionDigits: max,
  };
}

type AnyIntlFormat = Intl.NumberFormat | Intl.DateTimeFormat;

/**
 * A formatter cache. One per grid instance rather than a module-level
 * singleton, so tearing down a view releases its formatters and tests
 * cannot leak state into each other.
 */
export class FormatterCache {
  readonly #cache = new Map<string, AnyIntlFormat>();
  #hits = 0;
  #misses = 0;

  /** Cache statistics, for asserting in tests and perf work. */
  get stats(): { hits: number; misses: number; size: number } {
    return { hits: this.#hits, misses: this.#misses, size: this.#cache.size };
  }

  #get(format: ColumnFormat): AnyIntlFormat | undefined {
    const kind = format.kind;
    if (kind === 'text' || kind === 'auto') return undefined;

    const key = intlKey(format);
    const hit = this.#cache.get(key);
    if (hit) {
      this.#hits += 1;
      return hit;
    }
    this.#misses += 1;

    const locale = format.locale;
    let made: AnyIntlFormat;
    switch (kind) {
      case 'date':
        made = new Intl.DateTimeFormat(locale, {
          year: 'numeric',
          month: 'short',
          day: '2-digit',
          timeZone: 'UTC',
        });
        break;
      case 'datetime':
        made = new Intl.DateTimeFormat(locale, {
          year: 'numeric',
          month: 'short',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          timeZone: 'UTC',
        });
        break;
      case 'currency':
        made = new Intl.NumberFormat(locale, {
          style: 'currency',
          currency: format.currency ?? 'USD',
          useGrouping: format.displayCommas !== false,
          signDisplay: 'negative',
          ...fractionDigits(format, 2, 2),
        });
        break;
      case 'percent':
        made = new Intl.NumberFormat(locale, {
          style: 'percent',
          useGrouping: format.displayCommas !== false,
          signDisplay: 'negative',
          ...fractionDigits(format, 1, 1),
        });
        break;
      case 'number':
        made = new Intl.NumberFormat(locale, {
          useGrouping: format.displayCommas !== false,
          // A minus only on a value that is STILL negative once rounded:
          // -0.0012 at two places read "-0" (2026-09-25 probe), which
          // says negative about a number the grid is showing as zero.
          signDisplay: 'negative',
          ...fractionDigits(format, 0, 2),
        });
        break;
    }
    this.#cache.set(key, made);
    return made;
  }

  /**
   * A cell as text. `type` is the column's COMPILER type: 'auto' renders a date, a
   * timestamp or a number by it (a date's value is its calendar text, so its own JS type
   * says nothing). Dates and timestamps render in UTC -- as stored, never shifted into the
   * viewer's zone (the user's ruling on what a DateTime means).
   */
  format(value: Scalar, format: ColumnFormat = DEFAULT_FORMAT, type?: string): string {
    if (value === null || value === undefined) {
      return format.nullText ?? '';
    }

    if (format.kind === 'auto' || format.kind === 'text') {
      if (format.kind === 'auto' && isTemporal(type) && !isTimeOfDay(type)) {
        return this.format(value, { ...format, kind: hasTimeOfDay(type) ? 'datetime' : 'date' }, type);
      }
      // a number by its column's type, never its own: a decimal is exact text, and a
      // String column of '00501' is text
      if (format.kind === 'auto' && isNumeric(type)) {
        return this.format(value, { ...format, kind: 'number' }, type);
      }
      // a JSON document, shown for the eye (`{kind: billing, city: Paris}`); its value stays JSON
      if (format.kind === 'auto' && isVariant(type)) return prettyJson(String(value));
      return applyCase(String(value), format.fontCase);
    }

    const intl = this.#get(format);
    if (!intl) return String(value);

    if (intl instanceof Intl.DateTimeFormat) {
      const at = instantOf(value);
      return at === null ? String(value) : applyCase(intl.format(at), format.fontCase);
    }

    // exact numbers stay exact to Intl (it formats a bigint, and a decimal's text)
    const exact = typeof value === 'bigint' || (typeof value === 'string' && /^-?\d+(\.\d+)?$/.test(value));
    let n = typeof value === 'number' ? value : Number(value);
    if (Number.isNaN(n)) return String(value);

    // A named scale divides AND suffixes; a bare divisor only divides.
    let suffix = '';
    let scaled = false;
    if (format.numberScale) {
      scaled = true;
      const s =
        format.numberScale === 'auto'
          ? autoScale(n)
          : SCALES[format.numberScale];
      n = n / s.divisor;
      suffix = s.suffix;
    } else if (format.scale !== undefined && format.scale !== 0) {
      scaled = true;
      n = n / format.scale;
    }
    // unscaled, an exact value reaches Intl as it is: its digits, not a double's
    type IntlNumber = Parameters<Intl.NumberFormat['format']>[0];
    const negative = exact ? String(value).trim().startsWith('-') : n < 0;
    const shown = (abs: boolean): IntlNumber => {
      if (!exact || scaled) return abs ? Math.abs(n) : n;
      const v = value as bigint | string;
      if (!abs) return v as IntlNumber;
      return (typeof v === 'bigint' ? (v < 0n ? -v : v) : v.replace(/^-/, '')) as IntlNumber;
    };
    // DataCube's unit: glued on after the number ("12.5kg"), or --
    // when it starts with `_` -- before it, without the `_` ("_$" is
    // "$12.5"): the one field that spells a currency sign in a cube
    // saved upstream.
    let prefix = '';
    if (format.unit?.startsWith('_')) prefix = format.unit.slice(1);
    else if (format.unit) suffix += format.unit;

    // Parentheses wrap the WHOLE rendering, units included: "($1.2m)"
    // rather than "($1.2)m", which reads as a different number.
    const body =
      format.negativeParens && negative
        ? `(${prefix}${intl.format(shown(true))}${suffix})`
        : `${prefix}${intl.format(shown(false))}${suffix}`;
    return applyCase(body, format.fontCase);
  }

  /** Drop everything. Call when a view is torn down. */
  clear(): void {
    this.#cache.clear();
    this.#hits = 0;
    this.#misses = 0;
  }
}

/**
 * A date's or a timestamp's text as the instant Intl formats IN UTC: its calendar day at
 * UTC midnight, or its time as stored. Null for text that is not one.
 */
function instantOf(value: Scalar): Date | null {
  if (typeof value !== 'string') return null;
  const m = /^(-?\d{4,})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?$/.exec(value);
  if (!m) return null;
  const at = new Date(0);
  at.setUTCFullYear(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  at.setUTCHours(Number(m[4] ?? 0), Number(m[5] ?? 0), Number(m[6] ?? 0),
    Number((m[7] ?? '0').slice(0, 3).padEnd(3, '0')));
  return at;
}
