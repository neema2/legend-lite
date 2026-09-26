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

/**
 * The leaf count's column in a group query's result ("Show leaf
 * count"). Internal, like the tree column's `__tree`: `assemble` folds
 * it into the group's label and never shows it as a column.
 */
export const LEAF_COUNT_COLUMN = '__leafCount';

/**
 * The pivot total's own key, where a pivot VALUE would sit in a
 * generated name: `__pivot_total____|__notional` is the total of
 * `notional` across every value of the pivot. Its header reads the
 * configured name (upstream's `pivotStatisticColumnName`, "Total"),
 * never this. A key rather than the name itself, so a pivot value that
 * happens to be spelled "Total" cannot be mistaken for the total.
 */
export const PIVOT_TOTAL_KEY = '__pivot_total__';

/**
 * The pivot total column (upstream's pivot statistic column).
 *
 * Upstream carries the settings -- `pivotStatisticColumnPlacement`
 * and a per-measure `pivotStatisticColumnFunction` -- and renders
 * nothing for them; the user ruled that a bug (2026-09-25). Here it is
 * computed in the DATABASE: each row's total is its measure over the
 * row's whole slice with the pivot key dropped -- the same query as
 * the row subtotals -- so it is right for an average or a count, where
 * adding up the pivot's cells is not.
 */
export interface PivotTotal {
  readonly placement: 'left' | 'right';
  /** Per measure COLUMN: the aggregate its total uses. Absent = its own. */
  readonly functions?: Readonly<Record<string, AggregateFn>>;
}

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
  /**
   * The aggregate this column takes when the cube groups or pivots
   * it -- Column Properties > Aggregation. Absent: the kind decides
   * (a measure sums, a dimension takes its unique value), as before.
   */
  readonly aggregate?: AggregateFn;
  /** The weight column, for `wavg` only. */
  readonly aggregateWeight?: string;
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

/** A column that exists BEFORE aggregation, source or calculated. */
export interface RowColumn {
  readonly name: string;
  /** Absent for a calculated column whose first result has not landed. */
  readonly type?: string;
  readonly kind: ColumnKind;
  /** True for a row-stage calculated column. */
  readonly derived: boolean;
  readonly excludedFromPivot?: boolean;
}

/**
 * Every column a cube can group, filter or pivot by: the source's,
 * then the row-stage calculated ones.
 *
 * THE lookup, because reading `snapshot.columns` alone was the one
 * defect behind four: a calculated column was never a dimension (so
 * it could not be grouped), had no type (so the filter menu offered
 * `contains` on a Boolean), and was never a measure (so a column
 * pivot dropped it). `groupDerived` is absent on purpose -- those
 * exist only after the groupBy, so nothing before it can use them.
 *
 * A calculated column's kind is its own declared one; the type
 * default applies only to a snapshot saved before kinds were
 * declared, exactly as `kindOf` does for a source column.
 */
export function rowColumns(s: CubeSnapshot): RowColumn[] {
  return [
    ...s.columns.map((c): RowColumn => ({
      name: c.name,
      type: c.type,
      kind: kindOf(c),
      derived: false,
      ...(c.excludedFromPivot ? { excludedFromPivot: true } : {}),
    })),
    ...s.derived.map((d): RowColumn => ({
      name: d.name,
      ...(d.type === undefined ? {} : { type: d.type }),
      kind: d.kind ?? (isNumericType(d.type) ? 'measure' : 'dimension'),
      derived: true,
    })),
  ];
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

/**
 * Whether a Pure type name is Variant: semi-structured data (a JSON
 * object, array or scalar) in one column.
 *
 * Both spellings, because both arrive: the generated SQL-to-Pure
 * table and a remote engine say `Variant`, while a relation type the
 * compiler reports carries the full path. Nested data of every other
 * shape -- a STRUCT, a LIST, a MAP -- is converted to JSON when it is
 * loaded (`upload.ts`), so this is the one nested type the cube meets.
 */
export function isVariantType(type: string | undefined): boolean {
  return type === 'Variant'
    || type === 'meta::pure::metamodel::variant::Variant';
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
/**
 * A window function: a value computed over the rows AROUND a row --
 * a running total, a rank, the previous period's figure. Pure's
 * `extend(over(...), ~col:{p,w,r|...})`, which the planner turns into
 * SQL's `... OVER (PARTITION BY ... ORDER BY ... ROWS ...)`.
 */
export type WindowFunction =
  | 'sum' | 'average' | 'min' | 'max' | 'count'
  | 'rank' | 'denseRank' | 'rowNumber' | 'percentRank' | 'cumeDist' | 'ntile'
  | 'lag' | 'lead' | 'first' | 'last';

/**
 * Which rows around the current one an aggregate sees: from the start
 * of the partition to this row (a running figure), the whole partition,
 * or the last N rows (this one included) -- a moving figure.
 */
export type WindowFrame = 'running' | 'partition' | { readonly lastRows: number };

export interface WindowSpec {
  readonly fn: WindowFunction;
  /** The column the function reads: aggregates, lag/lead, first/last. */
  readonly column?: string;
  /** Restart for each distinct value of these; empty = one partition. */
  readonly partition: readonly string[];
  /**
   * The order the rows are taken in. At the GROUP level, empty means
   * the order the grid shows that level in, so a running total runs
   * down the rows as they are seen.
   */
  readonly order: readonly SortSpec[];
  /** Aggregates and first/last only; ranking functions take none. */
  readonly frame?: WindowFrame;
  /** lag / lead: how many rows back or ahead (default 1). */
  readonly offset?: number;
  /** ntile: how many buckets. */
  readonly buckets?: number;
}

/** What each window function needs from the form, and what it is called. */
export const WINDOW_FUNCTIONS: readonly {
  readonly fn: WindowFunction;
  readonly label: string;
  /** Reads a column's values. */
  readonly column: boolean;
  /** Means nothing without an order. */
  readonly ordered: boolean;
  /** Takes a frame. */
  readonly framed: boolean;
}[] = [
  { fn: 'sum', label: 'Sum (running / moving)', column: true, ordered: false, framed: true },
  { fn: 'average', label: 'Average (running / moving)', column: true, ordered: false, framed: true },
  { fn: 'min', label: 'Minimum', column: true, ordered: false, framed: true },
  { fn: 'max', label: 'Maximum', column: true, ordered: false, framed: true },
  { fn: 'count', label: 'Count', column: true, ordered: false, framed: true },
  { fn: 'rank', label: 'Rank', column: false, ordered: true, framed: false },
  { fn: 'denseRank', label: 'Dense rank', column: false, ordered: true, framed: false },
  { fn: 'rowNumber', label: 'Row number', column: false, ordered: true, framed: false },
  { fn: 'percentRank', label: 'Percent rank', column: false, ordered: true, framed: false },
  { fn: 'cumeDist', label: 'Cumulative distribution', column: false, ordered: true, framed: false },
  { fn: 'ntile', label: 'Bucket (ntile)', column: false, ordered: true, framed: false },
  { fn: 'lag', label: 'Previous value (lag)', column: true, ordered: true, framed: false },
  { fn: 'lead', label: 'Next value (lead)', column: true, ordered: true, framed: false },
  { fn: 'first', label: 'First value', column: true, ordered: true, framed: true },
  { fn: 'last', label: 'Last value', column: true, ordered: true, framed: true },
];

/**
 * An aggregate of the CHILD GROUPS' figures, shown on their parent: a
 * region row shows the smallest of its desks' totals. Each level asks
 * the database for its children's figures (one level deeper) and
 * aggregates them per group; the deepest group's children are its
 * source rows. Group level only; not on a pivoted cube.
 */
export type ChildAggregateFn = 'min' | 'max' | 'average' | 'median' | 'sum' | 'count';

export interface ChildAggregate {
  readonly fn: ChildAggregateFn;
  /** The group-level column whose child figures are aggregated: a measure. */
  readonly of: string;
}

export interface DerivedColumn {
  readonly name: string;
  /**
   * Pure expression body, with `$x` bound to the row, e.g. '$x.a * 2'.
   * Unused (empty) for a window column.
   */
  readonly expression: string;
  /** A window column instead of an expression: see `WindowSpec`. */
  readonly window?: WindowSpec;
  /** An aggregate of the child groups instead: see `ChildAggregate`. */
  readonly childAggregate?: ChildAggregate;
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
  /**
   * Measure or dimension, as the user DECLARED it.
   *
   * Asked, not inferred. Upstream asks too
   * (DataCubeExtendedColumnKind), and the alternative here was
   * inferring it from a type learned off a landed result -- which
   * meant the answer to "should this column sum?" came from Arrow's
   * wire vocabulary, via a converter, one query late. A declared kind
   * is right on the first query and cannot be wrong about what the
   * user wanted.
   *
   * Only meaningful at the ROW stage: a group-stage column is already
   * post-aggregation, which is why upstream has no
   * measure-or-dimension variant of GROUP_LEVEL.
   *
   * Undefined falls back to the type-based default, so a snapshot
   * saved before this field existed still behaves as it did.
   */
  readonly kind?: ColumnKind;
  /** As on `ColumnSpec`: Column Properties > Aggregation. */
  readonly aggregate?: AggregateFn;
  readonly aggregateWeight?: string;
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

/**
 * A date relative to when the query RUNS: upstream's TODAY and NOW
 * advanced values (DataCubeOperationAdvancedValueType). Rendered as
 * `today()` / `now()`, so a saved view filtered to "before today" means
 * today whenever it is opened, not the day it was saved.
 */
export interface RelativeDate {
  readonly relative: 'today' | 'now';
}

/**
 * A JSON document, compared as one: `fromJson('{"a":1}')`. What a group
 * key on a Variant column turns back into when its group is drilled --
 * the key IS the document's JSON text, and comparing a Variant to a
 * plain string compares it to a JSON STRING (`to_json('{...}')`), which
 * matches nothing.
 */
export interface JsonValue {
  readonly json: string;
}

export function isJsonValue(v: unknown): v is JsonValue {
  return typeof v === 'object' && v !== null && !(v instanceof Date)
    && typeof (v as { json?: unknown }).json === 'string';
}

export type FilterValue = string | number | boolean | Date | RelativeDate
  | JsonValue;

export function isRelativeDate(v: unknown): v is RelativeDate {
  return typeof v === 'object' && v !== null && !(v instanceof Date)
    && ((v as { relative?: unknown }).relative === 'today'
      || (v as { relative?: unknown }).relative === 'now');
}

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
   * See bench/model/windowinvariant.py.
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
  /** The pivot total column; absent means none. See `PivotTotal`. */
  readonly pivotTotal?: PivotTotal;
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
  /**
   * Count the rows under each group and show it beside the group's
   * label -- General Properties > "Show leaf count", upstream's
   * `suppressCount: !showLeafCount` over a count aggregate its group
   * queries carry. It changes the query, which is why it lives here.
   */
  readonly leafCount?: boolean;
  /**
   * Show, beside an OPENED group's label, how many rows sit directly
   * beneath it (the next level, or its detail rows) -- read off the
   * rows that opening it fetched, so no query changes. "N+" when the
   * row cap cut them short. The "next level" count mode (config
   * `leafCountMode`), the default.
   */
  readonly childCount?: boolean;
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
  // A child-group aggregate arrives from its own query, beside the
  // level's; the level's query has no such column to order by.
  const apart = new Set((s.groupDerived ?? []).filter((d) => d.childAggregate).map((d) => d.name));
  const applicable = s.sorts.filter(
    (x) => (present.has(x.column) || !s.rows.includes(x.column)) && !apart.has(x.column),
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

/**
 * Every reference to a column, renamed: rows, pivot keys, sorts,
 * measures and filters.
 *
 * For renaming a CALCULATED column that is in use. Upstream moves the
 * column's configuration to the new name and then refuses the change
 * if anything else still names the old one (its compile check fails);
 * this carries the rename through instead, so renaming a column you
 * have grouped by is a rename rather than an error. Expressions of
 * other calculated columns are NOT rewritten -- that would be editing
 * Pure text -- so one that refers to the old name is refused by the
 * planner, and the editor says so.
 */
function renamedWindow(d: DerivedColumn, one: (n: string) => string): DerivedColumn {
  if (d.childAggregate) return { ...d, childAggregate: { ...d.childAggregate, of: one(d.childAggregate.of) } };
  const w = d.window;
  if (!w) return d;
  return {
    ...d,
    window: {
      ...w,
      ...(w.column !== undefined ? { column: one(w.column) } : {}),
      partition: w.partition.map(one),
      order: w.order.map((o) => ({ ...o, column: one(o.column) })),
    },
  };
}

export function renameColumnReferences(
  s: CubeSnapshot,
  from: string,
  to: string,
): CubeSnapshot {
  const one = (n: string): string => (n === from ? to : n);
  const inFilter = (node: FilterNode): FilterNode => {
    switch (node.kind) {
      case 'condition':
        return {
          ...node,
          column: one(node.column),
          ...(node.rightColumn !== undefined
            ? { rightColumn: one(node.rightColumn) }
            : {}),
        };
      case 'and':
      case 'or':
        return { ...node, children: node.children.map(inFilter) };
      case 'not':
        return { ...node, child: inFilter(node.child) };
    }
  };
  const { pivotCast, ...rest } = s;
  // A cast built on the old name describes a relation that no longer
  // exists; dropping it makes the pivot learn its columns again.
  const keepCast = pivotCast !== undefined
    && !pivotCast.some((c) => c.measure === from);
  return {
    ...rest,
    ...(keepCast ? { pivotCast } : {}),
    rows: s.rows.map(one),
    pivotOn: s.pivotOn.map(one),
    sorts: s.sorts.map((x) => ({ ...x, column: one(x.column) })),
    measures: s.measures.map((m) => ({
      ...m,
      column: one(m.column),
      ...(m.weight !== undefined ? { weight: one(m.weight) } : {}),
    })),
    ...(s.filter ? { filter: inFilter(s.filter) } : {}),
    // A window names its columns outright, so a rename reaches them.
    derived: s.derived.map((d) => renamedWindow(d, one)),
    ...(s.groupDerived ? { groupDerived: s.groupDerived.map((d) => renamedWindow(d, one)) } : {}),
  };
}
