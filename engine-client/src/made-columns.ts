// A COLUMN A PAGE MAKES, ROW BY ROW: a shape and nothing else (records), so an app that makes rows -- the market in the
// tab, a test -- names it without reaching the run side that writes them (tools/deps/app-runs.test.ts: no app module
// reaches a run module; 2026-11-17, out of arrow-ipc.ts, which writes them as Arrow).

/**
 * A column a page makes, row by row: doubles (DOUBLE); whole numbers, each in 32 bits, held as 64-bit integers
 * (BIGINT); text, each row one of a few values (VARCHAR); instants, each its milliseconds since the epoch, UTC
 * (TIMESTAMP_MS: what a table's TIMESTAMP takes); truths (BOOLEAN); or days, each its days since the epoch (DATE). Any
 * row's value may be null (the value null; text's pick null), the column's validity then written. The last three for
 * the live market's tables (docs/DUCKDB_LIVE_2026_10_25.md L2): a print's time, a story's flag, an order's date.
 */
export type MadeColumn =
  | { readonly doubles: (row: number) => number | null }
  | { readonly integers: (row: number) => number | null }
  | { readonly text: readonly string[]; readonly pick: (row: number) => number | null }
  | { readonly instants: (row: number) => number | null }
  | { readonly truths: (row: number) => boolean | null }
  | { readonly days: (row: number) => number | null };
