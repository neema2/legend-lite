// A sample CSV, for having something to open.
//
// Deliberately not the demo's own eight columns. The point of a
// sample is to exercise the thing you just opened it with, so the
// shape here covers every branch the schema inference has: a string,
// an integer that is a MEASURE, an integer that is a KEY (and so
// must stay a dimension and lose its thousands separators), a
// floating-point measure that goes negative, a date, and a boolean.
//
// It also carries a book name with a comma, a quoted phrase and an
// apostrophe, because that is where CSV readers and SQL-literal
// escaping actually fail, and a sample that avoids the hard case
// teaches the wrong lesson. (No embedded newline: legal in CSV, but
// it makes the file unpleasant to eyeball in a text editor, which is
// half the point of handing someone a sample.)

/** Deterministic, so a regenerated sample diffs cleanly. */
function rng(seed: number): () => number {
  // mulberry32: small, and good enough for made-up trades.
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** RFC 4180: quote when the value holds a comma, quote or newline. */
export function csvCell(value: string): string {
  return /[",\n\r]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}

export const SAMPLE_COLUMNS = [
  'trade_id', 'trade_date', 'booked_at', 'region', 'desk', 'book',
  'year', 'quarter', 'notional', 'pnl', 'quantity', 'settled',
] as const;

const REGIONS = ['EMEA', 'APAC', 'AMER'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equities', 'Commodities'];
// One book name is hostile on purpose: a comma, a quoted phrase and
// an apostrophe, so the sample proves the CSV path and the Pure
// literal escaping both hold.
const BOOKS = [
  'Alpha Fund',
  'Smith, "Big" Fund',
  "O'Brien Holdings",
  'Beta Fund',
];

export interface SampleOptions {
  readonly rows?: number;
  readonly seed?: number;
}

/** The sample as CSV text, header included. */
export function sampleCsv(options: SampleOptions = {}): string {
  const rows = Math.max(1, options.rows ?? 5000);
  const rand = rng(options.seed ?? 20260920);
  const out: string[] = [SAMPLE_COLUMNS.join(',')];

  for (let i = 0; i < rows; i++) {
    const year = 2021 + (i % 5);
    const month = 1 + Math.floor(rand() * 12);
    const day = 1 + Math.floor(rand() * 28);
    const quarter = `Q${1 + Math.floor((month - 1) / 3)}`;
    const cells = [
      String(100000 + i),
      `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`,
      // A TIMESTAMP as well as a DATE: they reach the grid through
      // different Arrow types and only one of them was ever right.
      `${year}-${String(month).padStart(2, '0')}-`
        + `${String(day).padStart(2, '0')} `
        + `${String(Math.floor(rand() * 24)).padStart(2, '0')}:`
        + `${String(Math.floor(rand() * 60)).padStart(2, '0')}:00`,
      REGIONS[i % REGIONS.length]!,
      DESKS[Math.floor(rand() * DESKS.length)]!,
      BOOKS[Math.floor(rand() * BOOKS.length)]!,
      String(year),
      quarter,
      (rand() * 1_000_000).toFixed(2),
      (rand() * 200_000 - 100_000).toFixed(2),
      String(1 + Math.floor(rand() * 500)),
      rand() < 0.8 ? 'true' : 'false',
    ];
    out.push(cells.map(csvCell).join(','));
  }
  // A trailing newline: a file without one is legal and every text
  // tool complains about it.
  return `${out.join('\n')}\n`;
}
