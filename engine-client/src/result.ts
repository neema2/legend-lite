// What comes back from a query, and the shape it travels in.
//
// Columnar, and carrying RAW scalars -- never formatted strings. Three
// measured reasons:
//
//  - JSON beats Arrow at viewport scale. Parsing a ~20KB viewport is
//    ~44us; Arrow IPC serialise+deserialise for a SMALLER viewport is
//    ~0.6ms, because Arrow's per-message overhead (schema, padding,
//    footer) is fixed and only amortises over large batches. An Arrow
//    Table also cannot cross a worker boundary at all -- structuredClone
//    throws on it -- so "Arrow all the way to the DOM" was never on the
//    table.
//  - Formatting belongs on the main thread, against a cache. Building an
//    Intl.DateTimeFormat costs ~22.7us; reusing one costs ~0.96us. At
//    1,200 visible cells that is ~27ms of pure formatter construction per
//    frame versus ~1ms. See format.ts.
//  - Raw scalars keep sorting, comparison and export correct. A column
//    formatted to strings in the worker cannot be re-sorted numerically
//    without parsing back, and parsing back is where locale bugs live.

/** Scalar types a cell can hold. Null means SQL NULL, not "missing". */
/**
 * A cell's value, exact (values.ts): a date, a timestamp, a time and a decimal are the
 * database's own text; an integer past 2^53 a bigint. Never a JS `Date`: an instant read
 * in the viewer's time zone is not a calendar day. What it means is the column's type.
 */
import type { Receipt } from './receipt.ts';

export type Scalar = string | number | bigint | boolean | null;

export interface ResultColumn {
  readonly name: string;
  /**
   * Pure type name, e.g. 'Float'.
   *
   * THE COMPILER'S, whichever plane answered: a planned query's result is
   * typed by its plan (`PlanThenRun`, the engine page's builder), both
   * vocabularies read by the one reader, `pureType` in relation-type.ts.
   * Raw, unplanned SQL carries no Pure type at all (engine.ts `RawTable`).
   *
   * This was documented as the contract before it was true. The local
   * plane passed Arrow's own `Float64` straight through, and the first
   * consumer to depend on the contract -- a calculated column learning
   * its type from the result -- got a name that matched no Pure type,
   * so `isNumericType` said false and a numeric column aggregated as
   * `unique`: a blank column rather than an error.
   */
  readonly type: string;
  /** One entry per row, in row order. */
  readonly values: readonly Scalar[];
}

export interface ResultTable {
  readonly columns: readonly ResultColumn[];
  readonly rowCount: number;
  /** The snapshot epoch this result answers. */
  readonly epoch: number;
  /** Wall time the engine spent, for the perf budget. */
  readonly elapsedMs: number;
  /** What ran it, where, as whom: issued by the engine that did (receipt.ts). */
  readonly receipt?: Receipt;
}

export function emptyResult(epoch: number): ResultTable {
  return { columns: [], rowCount: 0, epoch, elapsedMs: 0 };
}

/** Read one cell without materialising rows. */
export function cell(
  table: ResultTable,
  rowIndex: number,
  columnIndex: number,
): Scalar {
  const col = table.columns[columnIndex];
  if (!col) return null;
  return col.values[rowIndex] ?? null;
}

export function columnIndex(table: { readonly columns: readonly { readonly name: string }[] }, name: string): number {
  return table.columns.findIndex((c) => c.name === name);
}
