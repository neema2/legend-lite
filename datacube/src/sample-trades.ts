// The demo's sample trades (demo/boot.ts, a page that opens with no source): rows the tab's DuckDB MAKES into the table
// the demo's model declares (demo/trades.pure's TRADES), by a plan -- legend-lite's SYNTHESIZE (register S303: the
// table made as declared, then ONE statement over range(N), each column a formula of the row's number; the user's
// rulings of 2026-11-12, "Demo has to go through platform" and the simple synthesize). No row is in the plan:
// SAMPLE_KNOBS says what each column takes, the planner writes how, the database computes every value.
//
// The same rows as a CSV file are the sample a page saved over them reopens (samples.ts DESK_TRADES): the start's grid
// is written down as that file (DESK_TRADES_FILE), so the page it starts can be saved, shared and joined as any
// example's can (the user, 2026-10-23: the start page could not be saved, and its Join… refused). That file is written
// here, by sampleTradesCsv, with the formulas the planner writes for SAMPLE_KNOBS (HostOperation.Formula), row for row:
// test/sample-trades.test.ts holds the file to the table the plan made.

import type { SynthesizeKnob, SynthesizeRequest } from '../../engine-client/src/host-operations.ts';

/** How many trades the demo makes. */
export const SAMPLE_TRADES = 200_000;

const REGIONS = ['EMEA', 'AMER', 'APAC'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equity', 'Commodities'];
const QTRS = ['Q1', 'Q2', 'Q3', 'Q4'];
const BOOKS = ['Book 1', 'Book 2', 'Book 3', 'Book 4'];

/** What each column of TRADES takes (the synthesize request's knobs): its values in turn, or a range at a scale. */
export const SAMPLE_KNOBS: Readonly<Record<string, SynthesizeKnob>> = {
  region: { values: REGIONS },
  desk: { values: DESKS },
  year: { range: [2021, 2025] },
  qtr: { values: QTRS },
  book: { values: BOOKS },
  notional: { range: [0, 9999.99], scale: 2 },
  pnl: { range: [-1000, 999.99], scale: 2 },
  qty: { range: [1, 97] },
};

/** The demo's request for its trades: TRADES of its model, SAMPLE_TRADES rows, the knobs above, on `databaseType`. */
export function sampleTradesRequest(model: string, databaseType: string, count = SAMPLE_TRADES): SynthesizeRequest {
  return { databaseType, model, database: 'trades::DB', schema: 'default', table: 'TRADES', count, columns: SAMPLE_KNOBS };
}

/** The prime 2^31 - 1 a range's offset is taken modulo (HostOperation.Formula.MODULUS). */
const MODULUS = 2_147_483_647n;
/** The primitive root whose powers are the ranges' multipliers (Synthesize's ROOT). */
const ROOT = 742_938_285n;

/** The multiplier of the range at `position` among the table's ranges, in its declaration order (Synthesize.multiplier):
 *  ROOT to the power position + 1, modulo MODULUS. */
function multiplier(position: number): number {
  let out = 1n;
  for (let p = 0; p <= position; p++) out = (out * ROOT) % MODULUS;
  return Number(out);
}

/** A list's value of row i (HostOperation.Formula.Cycle): its digit of the row number in mixed radix, worth `every` --
 *  the product of the sizes of the lists the model declares before it. */
const turn = (values: readonly string[], every: number) => (i: number): string =>
  values[Math.floor(i / every) % values.length]!;

/** A range's value of row i (HostOperation.Formula.Spread): lo + (((i * m) mod MODULUS) mod n) steps of 10^-scale, n the
 *  range's values, m its own multiplier. Exact for the rows here (i * m below 2^53), each a decimal's nearest double, as
 *  DuckDB casts it. */
const spread = (position: number, lo: number, hi: number, scale = 0) => {
  const m = multiplier(position);
  const unit = 10 ** scale;
  const n = Math.round((hi - lo) * unit) + 1;
  return (i: number): number => (Math.round(lo * unit) + ((i * m) % Number(MODULUS)) % n) / unit;
};

/** Each column of the CSV file, in order, as the planner's formula makes row i's value. The model declares TRADES as
 *  region, desk, book, year, qtr, notional, pnl, qty: the lists are digits in that order (region the lowest, then desk,
 *  book and qtr), the ranges take the multipliers in that order (year, notional, pnl, qty). */
const COLUMNS: readonly (readonly [string, (i: number) => string | number])[] = [
  ['region', turn(REGIONS, 1)],
  ['desk', turn(DESKS, REGIONS.length)],
  ['year', spread(0, 2021, 2025)],
  ['qtr', turn(QTRS, REGIONS.length * DESKS.length * BOOKS.length)],
  ['book', turn(BOOKS, REGIONS.length * DESKS.length)],
  ['notional', spread(1, 0, 9999.99, 2)],
  ['pnl', spread(2, -1000, 999.99, 2)],
  ['qty', spread(3, 1, 97)],
];

/** `rows` sample trades as a CSV file's text: the rows the plan makes, a header first (no value needs quoting). */
export function sampleTradesCsv(rows = SAMPLE_TRADES): string {
  const out = [COLUMNS.map(([name]) => name).join(',')];
  for (let i = 0; i < rows; i++) {
    out.push(COLUMNS.map(([, value]) => String(value(i))).join(','));
  }
  return `${out.join('\n')}\n`;
}

/** The sample these trades are as a file (samples.ts): what a page saved over the start's grid rebuilds. */
export const DESK_TRADES = 'desk-trades';

/**
 * The start's trades as the CSV file of the sample, SAMPLE_TRADES rows: its name, size and SHA-256 -- what its grid is
 * written down as, known here rather than made and hashed as the page starts (test/sample-trades.test.ts holds them to
 * the file sampleTradesCsv makes).
 */
export const DESK_TRADES_FILE = {
  name: `sample-${DESK_TRADES}.csv`,
  size: 8_989_263,
  sha256: '0f8d84b5b4c793c1763acfdddee3cfef070661da62f0f4eb5853327642edaa27',
} as const;
