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

import type { Scalar } from './result.ts';

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
  /** A unit shown after the value, e.g. 'kg'. Distinct from a scale. */
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
        });
        break;
      case 'datetime':
        made = new Intl.DateTimeFormat(locale, {
          year: 'numeric',
          month: 'short',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
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

  format(value: Scalar, format: ColumnFormat = DEFAULT_FORMAT): string {
    if (value === null || value === undefined) {
      return format.nullText ?? '';
    }

    if (format.kind === 'auto' || format.kind === 'text') {
      // 'auto' renders by the value's own type, so a cube with no
      // configured formats still looks sane.
      if (value instanceof Date) {
        return this.format(value, { ...format, kind: 'date' });
      }
      if (typeof value === 'number') {
        return this.format(value, { ...format, kind: 'number' });
      }
      return applyCase(String(value), format.fontCase);
    }

    const intl = this.#get(format);
    if (!intl) return String(value);

    if (intl instanceof Intl.DateTimeFormat) {
      const d = value instanceof Date ? value : new Date(String(value));
      return Number.isNaN(d.getTime())
        ? String(value)
        : applyCase(intl.format(d), format.fontCase);
    }

    let n = typeof value === 'number' ? value : Number(value);
    if (Number.isNaN(n)) return String(value);

    // A named scale divides AND suffixes; a bare divisor only divides.
    let suffix = '';
    if (format.numberScale) {
      const s =
        format.numberScale === 'auto'
          ? autoScale(n)
          : SCALES[format.numberScale];
      n = n / s.divisor;
      suffix = s.suffix;
    } else if (format.scale !== undefined && format.scale !== 0) {
      n = n / format.scale;
    }
    if (format.unit) suffix += ` ${format.unit}`;

    // Parentheses wrap the WHOLE rendering, suffix included: "($1.2m)"
    // rather than "($1.2)m", which reads as a different number.
    const body =
      format.negativeParens && n < 0
        ? `(${intl.format(Math.abs(n))}${suffix})`
        : `${intl.format(n)}${suffix}`;
    return applyCase(body, format.fontCase);
  }

  /** Drop everything. Call when a view is torn down. */
  clear(): void {
    this.#cache.clear();
    this.#hits = 0;
    this.#misses = 0;
  }
}
