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

/**
 * What a column may be used for.
 *
 * DataCube carries this per column, and it is load-bearing rather
 * than cosmetic: without it a UI offers to sum a region name, or to
 * group by a notional, and the resulting query fails somewhere far
 * from the mistake.
 */
/**
 * A cube this product refuses to run, and why.
 *
 * Distinct from any other error on purpose. These are DELIBERATE
 * walls -- a weighted average with no weight, a pivot with nothing
 * to aggregate -- where producing a query anyway would mean quietly
 * answering a different question. Typing them separates "your cube
 * is not a question" from "we have a bug", which matters twice: a
 * host can show the first to a user and report the second, and a
 * fuzzer can assert that a random cube only ever produces the
 * first.
 */
export class CubeRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CubeRefusal';
  }
}

export type ColumnKind = 'dimension' | 'measure';

/** A column available from the source, with the type the engine reports. */
export interface ColumnSpec {
  readonly name: string;
  /** Pure type name, e.g. 'String' | 'Integer' | 'Float' | 'Date'. */
  readonly type: string;
  /** Defaults by type: numeric columns measure, everything else groups. */
  readonly kind?: ColumnKind;
  /**
   * Keep this column out of the horizontal pivot even when it would
   * otherwise be carried into it.
   */
  readonly excludedFromPivot?: boolean;
}

/**
 * The kind a column should be treated as.
 *
 * Numeric columns default to measures and everything else to
 * dimensions, which is right far more often than not; an explicit
 * kind always wins, because a year is numeric and is almost always a
 * dimension.
 */
export function kindOf(column: ColumnSpec): ColumnKind {
  if (column.kind) return column.kind;
  return isNumericType(column.type) ? 'measure' : 'dimension';
}

/**
 * Whether a Pure type name is one a cube can sum.
 *
 * THE predicate, because it was four. This one knew Decimal and
 * Number; the two in `serialize.ts` and the one in `infer.ts` tested
 * `Integer || Float` only, so a DECIMAL column was neither defaulted
 * to a measure nor given a SUM -- while the comment above the
 * groupBy path said "SUM for Integer/Decimal/Float". The comment was
 * right and the code was not.
 *
 * `Number` is Pure's abstract numeric; lite spells decimals
 * `Decimal` (RelationalKinds.pureKindOf), which is what
 * `src/generated/lite-facts.ts` now carries.
 */
export function isNumericType(type: string | undefined): boolean {
  return type === 'Integer' || isFractionalType(type);
}

/**
 * Whether a Pure type name is a type that carries a FRACTION.
 *
 * The distinction matters for one decision: whether a column with no
 * declared kind DEFAULTS to a measure. Summing an id, a year or a
 * postcode gives a plausible number that is meaningless and says
 * nothing about being wrong; leaving a quantity un-summed gives a
 * blank, which reads as "no aggregate chosen". So integers default to
 * dimensions and fractions to measures -- a deliberate divergence
 * from DataCube, which sums every numeric.
 *
 * Money and rates arrive as DOUBLE or DECIMAL and must land here.
 * Until `pureTypeOf` read its table out of legend-lite, DECIMAL was
 * mislabelled 'Float' and reached this decision by accident; lite
 * calls it 'Decimal', so the predicate has to name it.
 */
export function isFractionalType(type: string | undefined): boolean {
  return type === 'Float' || type === 'Number' || type === 'Decimal';
}

/** Columns a cube may group or pivot by. */
export function dimensionColumns(s: CubeSnapshot): ColumnSpec[] {
  return s.columns.filter((c) => kindOf(c) === 'dimension');
}

/** Columns a cube may aggregate. */
export function measureColumns(s: CubeSnapshot): ColumnSpec[] {
  return s.columns.filter((c) => kindOf(c) === 'measure');
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
  | 'wavg'
  /**
   * The value when a group has exactly one, otherwise null.
   *
   * DataCube's default aggregate for every non-numeric column
   * (DataCubeConfigurationBuilder: numbers default to SUM, everything
   * else to UNIQUE), and the reason grouping there keeps all the
   * columns instead of dropping them. Lowers to
   * `CASE WHEN COUNT(DISTINCT x) = 1 THEN MAX(x) ELSE NULL END`.
   */
  | 'unique';

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

/**
 * A column computed with `extend`.
 *
 * Which STAGE it belongs to decides what it can see, and the two are
 * not interchangeable -- see `derived` and `groupDerived` on the
 * snapshot.
 */
export interface DerivedColumn {
  readonly name: string;
  /** Pure expression body, with `$x` bound to the row, e.g. '$x.a * 2'. */
  readonly expression: string;
  /**
   * The Pure type the expression turned out to have.
   *
   * LEARNED, not declared. Nothing here can infer it -- `$x.a * 2` is
   * a Float and `$x.a->toUpper()` a String, and deciding which by
   * reading the expression would be writing a type checker the
   * planner already is. So it arrives from a landed result
   * (`ResultColumn.type`) and is recorded here, the same way the
   * snapshot already learns the pivot's generated column names.
   *
   * It matters because the aggregate DEFAULT reads it: without a type
   * a numeric calculated column groups as `unique` rather than `sum`,
   * which looks like a blank cell rather than an error. Undefined
   * until the first result lands.
   */
  readonly type?: string;
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
  | 'equalCaseInsensitiveColumn'
  | 'notEqualColumn'
  | 'notEqualCaseInsensitiveColumn'
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
  /**
   * Columns computed per SOURCE ROW, before aggregation. They can see
   * the source columns and are then aggregated like any other.
   */
  readonly derived: readonly DerivedColumn[];
  /**
   * Columns computed from the AGGREGATES, after grouping and pivoting.
   *
   * This is how a margin percentage, a ratio of two measures or a
   * contribution-to-total is expressed: sum(profit) / sum(revenue)
   * over the group. Computing that per row and averaging gives a
   * different and wrong answer -- the classic weighted-average defect
   * -- so the two extend stages cannot substitute for each other.
   *
   * The expressions see MEASURE names, not source columns, because by
   * this point the source rows are gone.
   */
  readonly groupDerived?: readonly DerivedColumn[];
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
  /**
   * The columns a pivot PRODUCED, learnt from a result.
   *
   * A pivot spreads each measure across the values it finds, and
   * those column names (`2021__|__notional`) do not exist until it
   * has run. An outer `groupBy` has to name every aggregate it
   * keeps, so a cube that is both grouped and pivoted cannot be
   * written in one pass: the first query supplies the names and the
   * second uses them. This is the same device as DataCube's
   * `pivot.castColumns`.
   *
   * Each entry carries the MEASURE it came from, so the aggregate
   * can be the one that measure is configured with -- and so nothing
   * here has to know how a pivot name is spelled.
   */
  readonly pivotCast?: readonly { readonly name: string;
    readonly measure: string }[];
  /**
   * Keep a row dimension as a column of its own, beside the tree.
   *
   * Here rather than in a grid option because the TREE is what has
   * to produce it: at each level the query groups BY that dimension
   * and its value never comes back as a column -- it becomes the
   * tree's label. So "keep the grouped columns" cannot be honoured
   * by un-hiding anything; the column has to be built from the row
   * paths, which is `assemble`'s job, and `assemble` is handed the
   * snapshot. `treeColumnSort` reaches it the same way.
   */
  readonly keepGroupedColumns?: boolean;
  readonly measures: readonly Measure[];
  readonly sorts: readonly SortSpec[];
  readonly window?: RowWindow;
  /**
   * Sort direction for the tree column itself, which orders the
   * GROUPS rather than any measure. Separate from `sorts` because it
   * applies at every level, including ones the user has not opened.
   */
  readonly treeColumnSort?: SortDirection;
  /**
   * Cap on the rows fetched for ONE level of the tree.
   *
   * Per level, not per grid: each open branch is its own query, so
   * three open branches can fetch three times this. That matches how
   * DataCube caps a block rather than a whole view, and it is the
   * behaviour that keeps an expanded branch usable instead of
   * starving it because its siblings were opened first.
   *
   * Lives on the snapshot because it changes the emitted query, so it
   * travels with a saved view and a colleague opening the cube sees
   * the same rows. Absent means DEFAULT_MAX_ROWS.
   */
  readonly maxRows?: number;
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
  // A column marked excludedFromPivot stays out of the pivot key even
  // if it was listed there, so the exclusion cannot be defeated by
  // the order the user configured things in.
  const excluded = new Set(
    s.columns.filter((c) => c.excludedFromPivot).map((c) => c.name),
  );
  s.pivotOn.filter((c) => !excluded.has(c)).forEach(push);
  for (const m of s.measures) {
    if (m.fn !== 'count') push(m.column);
    if (m.weight) push(m.weight);
  }
  return out;
}

/**
 * The type reported for a column, or undefined if nothing knows it.
 *
 * A CALCULATED column counts. Its type is learned from a landed result
 * (see `DerivedColumn.type`) and the pivot's `cast(@Relation<...>)`
 * needs it: with only `s.columns` consulted, a calculated column was
 * declared `String` in the cast, so the aggregate default read String
 * and gave it `uniqueValueOnly()` -- a blank column on a grouped cube
 * rather than the sum it should have had.
 *
 * Source columns win a name collision, which the editor refuses to
 * create anyway (`nameProblem`).
 */
export function columnType(
  s: CubeSnapshot,
  name: string,
): string | undefined {
  const source = s.columns.find((c) => c.name === name)?.type;
  if (source !== undefined) return source;
  for (const d of [...s.derived, ...(s.groupDerived ?? [])]) {
    if (d.name === name && d.type !== undefined) return d.type;
  }
  return undefined;
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
  // The tree-column sort orders the GROUPS, so it applies to this
  // level's dimensions rather than to any measure.
  const treeDirection = s.treeColumnSort;
  // Only this level's grouping columns exist in its result, so a
  // deeper dimension must not be named in the ORDER BY.
  const present = new Set(groupCols);
  const applicable = s.sorts.filter(
    (x) => present.has(x.column) || !s.rows.includes(x.column),
  );

  // FIRST WINS, and each column appears once. A column sorted twice
  // put the same key in the ORDER BY twice: the second is dead, and
  // more importantly it means the sort list was never normalised --
  // so "ascending then descending on the same column" silently
  // resolved to whichever the engine read first.
  const out: SortSpec[] = [];
  const seen = new Set<string>();
  for (const x of applicable) {
    if (seen.has(x.column)) continue;
    seen.add(x.column);
    out.push(x);
  }
  for (const r of groupCols) {
    if (!seen.has(r)) {
      seen.add(r);
      out.push({ column: r, direction: treeDirection ?? 'asc' });
    }
  }
  return out;
}
