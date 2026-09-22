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
export type Scalar = string | number | boolean | Date | null;

export interface ResultColumn {
  readonly name: string;
  /**
   * Pure type name, e.g. 'Float'.
   *
   * ONE VOCABULARY, whichever plane answered. Two backends report a
   * column's type in their own words -- legend-engine says
   * `meta::pure::precisePrimitives::Varchar`, an Arrow batch says
   * `Utf8` -- and a consumer that has to know which is which cannot
   * be written once. So each driver normalises on the way in:
   * `pureTypeName` in engine-remote.ts for the engine's spelling and
   * {@link pureTypeOfArrow} here for Arrow's.
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

export function columnIndex(table: ResultTable, name: string): number {
  return table.columns.findIndex((c) => c.name === name);
}

/**
 * An Arrow type name as the Pure type it carries.
 *
 * Arrow spells a type with its width and its unit -- `Int64`,
 * `Float64`, `Decimal<38,6>`, `Timestamp<MICROSECOND>` -- and Pure
 * does not care about either. The match is on the leading token so a
 * parameterised spelling needs no separate arm.
 *
 * Unknown is NOT String: a column whose type we failed to read must
 * not silently become groupable text. It stays 'Unknown', which
 * matches no numeric test and no temporal one, so callers fall back
 * rather than assert.
 */
export function pureTypeOfArrow(name: string): string {
  const head = /^[A-Za-z]+/.exec(name.trim())?.[0] ?? '';
  switch (head) {
    case 'Utf8':
    case 'LargeUtf8':
      return 'String';
    case 'Bool':
      return 'Boolean';
    case 'Int':
    case 'Int8':
    case 'Int16':
    case 'Int32':
    case 'Int64':
    case 'Uint8':
    case 'Uint16':
    case 'Uint32':
    case 'Uint64':
      return 'Integer';
    case 'Float':
    case 'Float16':
    case 'Float32':
    case 'Float64':
      return 'Float';
    // A decimal is its own Pure type, and lite spells it Decimal
    // (RelationalKinds.pureKindOf) -- not Float. Collapsing it here
    // would lose the distinction the generated lite-facts table
    // exists to preserve.
    case 'Decimal':
      return 'Decimal';
    case 'Date':
      return 'StrictDate';
    case 'Timestamp':
      return 'DateTime';
    default:
      return 'Unknown';
  }
}
