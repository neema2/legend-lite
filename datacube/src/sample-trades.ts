// The demo's sample trades (demo/boot.ts, a page that opens with no source): rows made in the tab as Arrow data, the
// table the demo's model reads (demo/trades.pure's TRADES), put in by DuckDB's Arrow reader -- no SQL statement makes
// them (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5). Row i's values are those DuckDB's own `range()` made of it before,
// value for value (test/sample-trades.test.ts holds them to that statement).
//
// The same rows as a CSV file are the sample a page saved over them reopens (samples.ts DESK_TRADES): the start's grid
// is written down as that file (DESK_TRADES_FILE), so the page it starts can be saved, shared and joined as any
// example's can (the user, 2026-10-23: the start page could not be saved, and its Join… refused).

import { arrowIpcStream, type MadeColumn } from '../../engine-client/src/arrow-ipc.ts';

/** How many trades the demo makes. */
export const SAMPLE_TRADES = 200_000;

const REGIONS = ['EMEA', 'AMER', 'APAC'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equity', 'Commodities'];

/** Each column, in order, as row i makes it: the Arrow data and the CSV file alike. */
const COLUMNS: Readonly<Record<string, MadeColumn>> = {
  region: { text: REGIONS, pick: (i) => i % 3 },
  desk: { text: DESKS, pick: (i) => Math.floor(i / 3) % 5 },
  year: { integers: (i) => 2021 + (Math.floor(i / 15) % 5) },
  qtr: { text: ['Q1', 'Q2', 'Q3', 'Q4'], pick: (i) => Math.floor(i / 75) % 4 },
  book: { text: ['Book 1', 'Book 2', 'Book 3', 'Book 4'], pick: (i) => Math.floor(i / 300) % 4 },
  notional: { doubles: (i) => ((i * 7919) % 1000000) / 100.0 },
  pnl: { doubles: (i) => ((i * 104729) % 200000) / 100.0 - 1000.0 },
  qty: { integers: (i) => ((i * 31) % 97) + 1 },
};

/** `rows` sample trades as one Arrow IPC stream: region, desk, year, qtr, book, notional, pnl, qty. */
export function sampleTrades(rows = SAMPLE_TRADES): Uint8Array {
  return arrowIpcStream(rows, COLUMNS);
}

/** `rows` sample trades as a CSV file's text: the same rows, a header first (no value needs quoting). */
export function sampleTradesCsv(rows = SAMPLE_TRADES): string {
  const columns = Object.values(COLUMNS);
  const out = [Object.keys(COLUMNS).join(',')];
  for (let i = 0; i < rows; i++) {
    out.push(columns.map((c) => ('text' in c ? c.text[c.pick(i)]! : String('doubles' in c ? c.doubles(i) : c.integers(i)))).join(','));
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
  size: 10_217_418,
  sha256: '5a06fa298e750ba41c133ac854eecd1d5c6f59d27a48992055ff8d0c8e97712a',
} as const;
