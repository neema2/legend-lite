// The cube's configuration: one immutable object that is the single
// source of truth for what the user is looking at.
//
// Everything downstream is derived from a snapshot -- the query sent to
// legend-lite, the grid's column model, the saved-view payload. Nothing
// else holds state that could disagree with it. That is the property
// that makes the whole design predictable: there is no second place for
// a filter or a sort to live and drift out of sync.
//
// Snapshots are treated as immutable. A user interaction produces a NEW
// snapshot; it never mutates one in place. That is what lets a pending
// query be matched against the snapshot that asked for it and discarded
// if it is stale (see `epoch` below).

/** A column available from the source, with the type the engine reports. */
export interface ColumnSpec {
  readonly name: string;
  /** Pure type name, e.g. 'String' | 'Integer' | 'Float' | 'Date'. */
  readonly type: string;
}

/**
 * Aggregate functions, each mapped to the reduce lambda legend-lite
 * actually accepts. The names on the right were taken from legend-lite's
 * own test corpus rather than invented -- `$y->sum()`, `$y->average()`
 * and friends appear there with the counts to prove it.
 *
 * `wavg` is included deliberately: a weighted average is the single
 * most-complained-about gap in spreadsheet pivots (calculated fields
 * operate on already-aggregated totals, so SUM(a*b)/SUM(b) is
 * inexpressible). legend-lite has it natively.
 */
export type AggregateFn =
  | 'sum'
  | 'count'
  | 'average'
  | 'min'
  | 'max'
  | 'median'
  | 'stdDevSample'
  | 'stdDevPopulation'
  | 'varianceSample'
  | 'variancePopulation'
  | 'joinStrings'
  | 'wavg';

/** One value column in the cube: an aggregate over a source column. */
export interface Measure {
  /** Output column name. */
  readonly name: string;
  /** Source column being aggregated. Ignored by `count`. */
  readonly column: string;
  readonly fn: AggregateFn;
  /** Weight column, required by and only used for `wavg`. */
  readonly weight?: string;
}

/** A column computed before aggregation, via `extend`. */
export interface DerivedColumn {
  readonly name: string;
  /** Pure expression body, with `$x` bound to the row, e.g. '$x.a * 2'. */
  readonly expression: string;
}

export type SortDirection = 'asc' | 'desc';

export interface SortSpec {
  readonly column: string;
  readonly direction: SortDirection;
}

/**
 * The filter vocabulary.
 *
 * Matches the operator set DataCube exposes, so a cube saved there can
 * be opened here without losing a condition. The case-insensitive
 * forms lower BOTH sides rather than relying on collation, which
 * varies by backend and would make the same cube answer differently on
 * two engines.
 *
 * The `*Column` operators compare two columns instead of a column and
 * a literal; they read `rightColumn` rather than `value`.
 */
export type FilterOperator =
  | 'equal'
  | 'notEqual'
  | 'lessThan'
  | 'lessThanEqual'
  | 'greaterThan'
  | 'greaterThanEqual'
  | 'isEmpty'
  | 'isNotEmpty'
  | 'contains'
  | 'notContains'
  | 'startsWith'
  | 'notStartsWith'
  | 'endsWith'
  | 'notEndsWith'
  | 'in'
  | 'notIn'
  | 'equalCaseInsensitive'
  | 'notEqualCaseInsensitive'
  | 'containsCaseInsensitive'
  | 'startsWithCaseInsensitive'
  | 'endsWithCaseInsensitive'
  | 'inCaseInsensitive'
  | 'notInCaseInsensitive'
  | 'equalColumn'
  | 'notEqualColumn'
  | 'lessThanColumn'
  | 'lessThanEqualColumn'
  | 'greaterThanColumn'
  | 'greaterThanEqualColumn';

/** A leaf comparison against a column. */
export interface FilterCondition {
  readonly kind: 'condition';
  readonly column: string;
  readonly operator: FilterOperator;
  /** Absent for isEmpty / isNotEmpty and the *Column forms; an array for `in`. */
  readonly value?: FilterValue | readonly FilterValue[];
  /** The other column, for the `*Column` operators. */
  readonly rightColumn?: string;
}

export type FilterValue = string | number | boolean | Date;

export interface FilterGroup {
  readonly kind: 'and' | 'or';
  readonly children: readonly FilterNode[];
}

export interface FilterNot {
  readonly kind: 'not';
  readonly child: FilterNode;
}

export type FilterNode = FilterCondition | FilterGroup | FilterNot;

/** Where the rows come from. */
export interface SourceRef {
  /** Pure expression yielding a relation, e.g. a table or a function call. */
  readonly expression: string;
}

/**
 * The window of rows the grid currently wants. Bounding this bounds the
 * payload, which is why row windowing is kept even though column
 * windowing was dropped: payload = row window x columns.
 */
export interface RowWindow {
  readonly offset: number;
  readonly limit: number;
}

export interface CubeSnapshot {
  readonly source: SourceRef;
  readonly columns: readonly ColumnSpec[];
  readonly derived: readonly DerivedColumn[];
  readonly filter?: FilterNode;
  /** Row dimensions, outermost first. These become the group-by. */
  readonly rows: readonly string[];
  /** Column dimensions. Empty means a flat (non-pivoted) result. */
  readonly pivotOn: readonly string[];
  /**
   * Escape hatch, normally absent. Supplying values pins the pivot's
   * output columns -- but legend-lite then PRE-FILTERS the source to
   * those values for engine parity, which drops row groups whose keys
   * all fall outside the list. Measured: four groups became one across
   * adjacent windows. So this must never be driven from a scroll
   * position; it is only for a deliberately narrowed cube.
   * See research/bench/windowinvariant.py.
   */
  readonly pivotValues?: readonly FilterValue[];
  readonly measures: readonly Measure[];
  readonly sorts: readonly SortSpec[];
  readonly window?: RowWindow;
  /**
   * Monotonic stamp, incremented for every new snapshot. A query result
   * carries the epoch it was issued under and is discarded unless it
   * still matches. This is the ONLY cancellation mechanism available in
   * the browser: DuckDB-WASM's `connection.query()` cannot be cancelled
   * once it starts.
   */
  readonly epoch: number;
}

/**
 * Columns a snapshot actually needs from the source, in emit order.
 *
 * `groupCols` defaults to every row dimension. Passing fewer is how a
 * subtotal is expressed: the same measures over a shorter grouping.
 */
export function referencedColumns(
  s: CubeSnapshot,
  groupCols: readonly string[] = s.rows,
): string[] {
  const out: string[] = [];
  const push = (n: string) => {
    if (!out.includes(n)) out.push(n);
  };
  groupCols.forEach(push);
  s.pivotOn.forEach(push);
  for (const m of s.measures) {
    if (m.fn !== 'count') push(m.column);
    if (m.weight) push(m.weight);
  }
  return out;
}

/** The type reported for a column, or undefined if the source lacks it. */
export function columnType(
  s: CubeSnapshot,
  name: string,
): string | undefined {
  return s.columns.find((c) => c.name === name)?.type;
}

/**
 * A snapshot's sort keys, extended so the ordering is a TOTAL order.
 *
 * Without this, two rows that tie on every sort key may come back in
 * different relative positions on different queries, and a windowed
 * read can then show a row twice or skip it entirely. Appending the row
 * dimensions guarantees a deterministic order, because the row
 * dimensions are exactly what makes a group unique.
 */
export function totalOrderSorts(
  s: CubeSnapshot,
  groupCols: readonly string[] = s.rows,
): SortSpec[] {
  // Only this level's grouping columns exist in its result, so a
  // deeper dimension must not be named in the ORDER BY.
  const present = new Set(groupCols);
  const out: SortSpec[] = s.sorts.filter(
    (x) => present.has(x.column) || !s.rows.includes(x.column),
  );
  for (const r of groupCols) {
    if (!out.some((x) => x.column === r)) {
      out.push({ column: r, direction: 'asc' });
    }
  }
  return out;
}
