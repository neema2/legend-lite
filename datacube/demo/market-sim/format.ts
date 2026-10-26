// The few of Java's String.format spellings the market's text uses (market.groovy's headlines and ids), as the tab
// writes them: a fixed number of places, digits grouped by thousands, a number padded with zeros, and a double as Java
// prints one (150.0, never 150).

/** `v` to `places` decimals, as Java's `%.nf`. */
export function fixed(v: number, places: number): string {
  return v.toFixed(places);
}

const GROUPED = new Map<number, Intl.NumberFormat>();

/** `v` to `places` decimals, its digits grouped by thousands, as Java's `%,.nf`. */
export function grouped(v: number, places: number): string {
  let f = GROUPED.get(places);
  if (f === undefined) {
    f = new Intl.NumberFormat('en-US', { minimumFractionDigits: places, maximumFractionDigits: places, useGrouping: true });
    GROUPED.set(places, f);
  }
  return f.format(v);
}

/** A whole number padded with zeros to `width` digits, as Java's `%0nd`. */
export function padded(v: number, width: number): string {
  const s = String(Math.abs(Math.trunc(v)));
  return (v < 0 ? '-' : '') + s.padStart(width - (v < 0 ? 1 : 0), '0');
}

/** A double as Java's Double.toString writes one in the plain range: a whole number with its `.0`. */
export function javaDouble(v: number): string {
  return Number.isInteger(v) ? `${v}.0` : String(v);
}

/** A day (days since the epoch) as Java's LocalDate writes it: 2026-10-26. */
export function isoDay(day: number): string {
  return new Date(day * 86_400_000).toISOString().slice(0, 10);
}

/** Today's day in UTC, of an instant in ms. */
export function dayOf(ms: number): number {
  return Math.floor(ms / 86_400_000);
}

/** A calendar date's day (days since the epoch), its month 1 to 12. */
export function dayAt(year: number, month: number, date: number): number {
  return Math.floor(Date.UTC(year, month - 1, date) / 86_400_000);
}
