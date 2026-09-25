// Fetching the tree and assembling it into one ordered table.
//
// Each open branch is a separate query -- the same measures with a
// different number of grouping columns -- and this stitches the
// results into the single row-ordered table the grid renders.
//
// Two things are deliberately NOT done here. Rows are never re-sorted:
// the engine already ordered each level, and re-sorting client-side is
// how a grid ends up disagreeing with the ORDER BY that produced its
// pagination. And values are never re-aggregated: a subtotal row shows
// the row its own query returned, so it cannot drift from the detail.


import type { EpochGuard } from './epoch.ts';
import type { QueryRunner } from './runner.ts';
import {
  NULL_GROUP,
  pivotTotalQuery,
  serialize,
  type LevelScope,
} from './serialize.ts';
import { PIVOT_SEPARATOR } from './generated/lite-facts.ts';
import type { ResultColumn, ResultTable, Scalar } from './result.ts';
import {
  LEAF_COUNT_COLUMN,
  PIVOT_TOTAL_KEY,
  type CubeSnapshot,
} from './snapshot.ts';
import {
  type LevelRequest,
  type RowPath,
  type TreeRow,
  TreeState,
  flattenTree,
  pathKey,
  requestKey,
  requiredLevels,
} from './tree.ts';

/**
 * The default row cap.
 *
 * DataCube uses 1000 as its maximum cache block size (500 with
 * pagination on); the same number for the same reason -- it is the
 * point past which a level costs real main-thread time to
 * materialise. Measured on a 50,000-group level: 49ms to read into JS
 * and 1.7MB across the boundary, against ~1ms and nothing at 100 rows.
 * The QUERY cost barely moves, because the aggregation scans
 * everything either way; the cap is about payload, not engine time.
 */
export const DEFAULT_MAX_ROWS = 1000;

/** One level's result, plus the group path of each of its rows. */
export interface LevelData {
  readonly request: LevelRequest;
  readonly table: ResultTable;
  readonly paths: readonly RowPath[];
  /** True when the engine had more rows than the cap allowed. */
  readonly truncated: boolean;
  /**
   * The Pure that was planned, and the SQL that came back.
   *
   * Kept because "show me the query" has to show the query that
   * RAN. The view's `sql` field carried Pure for the whole life of
   * this project -- a panel labelled SQL that had never shown any
   * -- and nothing noticed until a real planner started returning
   * SQL worth reading.
   *
   * Optional because a level assembled by hand in a test has no
   * query behind it, and `assemble` does not read them.
   */
  readonly pure?: string;
  readonly sql?: string;
}

export interface TreeView {
  /** Rows in display order, parallel to `table`. */
  readonly rows: readonly TreeRow[];
  /**
   * Levels that hit the row cap. Non-empty means the grid is showing
   * a prefix, which the user must be told rather than left to infer
   * from a suspiciously round row count.
   */
  readonly truncated: readonly LevelRequest[];
  /** One combined table whose row i belongs to rows[i]. */
  readonly table: ResultTable;
  /** Every level fetched, for diagnostics and incremental reuse. */
  readonly levels: ReadonlyMap<string, LevelData>;
}

/**
 * A group key rendered as text, with SQL NULL kept distinguishable.
 *
 * A temporal key is written as ISO 8601 rather than by `String()`,
 * and that is not cosmetic: the path segment is fed BACK into the
 * next level's query as a filter value, so it has to round-trip.
 * `String(date)` gives the locale form -- "Fri Jan 01 2021 03:58:00
 * GMT-0500 (Eastern Standard Time)" -- which went into the SQL as a
 * string and came back as `Conversion Error: invalid timestamp field
 * format`. Grouping by any date or timestamp column simply failed,
 * and the grid kept the previous answer.
 */
function groupValue(v: Scalar): string {
  if (v === null) return NULL_GROUP;
  // LOCAL components, with no zone suffix, so `new Date(text)` reads
  // it back as the same instant. `toISOString()` would round-trip
  // through UTC and land the key on a different day for any zone
  // behind or ahead far enough -- the same mistake, one layer up.
  if (v instanceof Date) {
    const p = (n: number): string => String(n).padStart(2, '0');
    return `${v.getFullYear()}-${p(v.getMonth() + 1)}-${p(v.getDate())}`
      + `T${p(v.getHours())}:${p(v.getMinutes())}:${p(v.getSeconds())}`;
  }
  return String(v);
}

/**
 * Derive each row's group path from a level result.
 *
 * The dimension column is the LAST grouping column of that level, and
 * the engine returns grouping columns first, so its index is
 * `level - 1`. Reading it by name would break on a pivot whose
 * generated columns happen to share a dimension's name.
 */
function pathsOf(
  request: LevelRequest,
  table: ResultTable,
): RowPath[] {
  if (request.level === 0) return [[]];
  const col = table.columns[request.level - 1];
  if (!col) return [];
  return col.values.map((v) => [...request.parent, groupValue(v)]);
}

/**
 * Fetch every level the current expansion needs.
 *
 * Levels are fetched in sequence rather than in parallel: each level's
 * results decide which branches beneath it exist, so the next round of
 * requests cannot be known until this one lands.
 */
export async function fetchTree(
  snapshot: CubeSnapshot,
  state: TreeState,
  deps: {
    /**
     * Pure in, rows out -- however that happens.
     *
     * A level does not care whether a planner and a local engine did
     * it in two steps or a remote engine did it in one; it cares
     * that its rows answer its query.
     */
    readonly runner: QueryRunner;
    readonly guard: EpochGuard;
    readonly epoch: number;
    readonly assemble?: AssembleOptions;
    readonly maxRows?: number;
    /**
     * Abort for work a newer interaction replaced. Levels are fetched
     * in SEQUENCE, so a burst that is already obsolete stops at the
     * next level rather than grinding through the whole tree.
     */
    readonly signal?: AbortSignal;
  },
): Promise<TreeView> {
  // The snapshot wins, so a saved view keeps its own cap; the deps
  // override exists for tests and for a host that wants a hard lid.
  const maxRows = deps.maxRows ?? snapshot.maxRows ?? DEFAULT_MAX_ROWS;
  const depth = snapshot.rows.length;
  const levels = new Map<string, LevelData>();

  const childrenOf = (parent: RowPath): readonly RowPath[] | undefined =>
    levels.get(requestKey({ level: parent.length + 1, parent }))?.paths;

  // Requests are recomputed after each round, because opening a branch
  // only becomes fetchable once its parent's rows are known.
  for (let round = 0; round <= depth; round++) {
    const wanted = requiredLevels(state, depth, childrenOf).filter(
      (r) => !levels.has(requestKey(r)),
    );
    if (wanted.length === 0) break;

    for (const request of wanted) {
      // Ask for one more than the cap: if it comes back there is more
      // data, which is cheaper than a second counting query.
      const scoped = { ...request, limit: maxRows + 1 };
      const grammar = serialize(snapshot, scoped);
      const { rows: full, sql } = await deps.runner.run(
        grammar, { ...snapshot, epoch: deps.epoch }, scoped, deps.signal,
      );
      const truncated = full.rowCount > maxRows;
      const capped = truncated ? takeRows(full, maxRows) : full;
      const table = await withPivotTotals(
        snapshot, request, capped, pathsOf(request, capped), truncated,
        { ...deps, snapshot: { ...snapshot, epoch: deps.epoch } },
      );
      levels.set(requestKey(request), {
        request,
        table,
        paths: pathsOf(request, table),
        truncated,
        pure: grammar,
        sql,
      });
      // A superseded interaction should stop fetching the rest of the
      // tree rather than finish work nobody will look at.
      if (!deps.guard.isCurrent(deps.epoch)) {
        return {
          rows: [],
          table: emptyTable(deps.epoch),
          levels,
          truncated: [],
        };
      }
    }
  }

  const rows = flattenTree(state, depth, childrenOf);
  return {
    rows,
    table: assemble(snapshot, rows, levels, deps.assemble ?? {}),
    levels,
    truncated: [...levels.values()]
      .filter((d) => d.truncated)
      .map((d) => d.request),
  };
}

/** The name of one measure's pivot total column. */
export function pivotTotalColumn(measure: string): string {
  return `${PIVOT_TOTAL_KEY}${PIVOT_SEPARATOR}${measure}`;
}

/** Whether a column is a pivot total (see `PivotTotal`). */
export function isPivotTotalColumn(name: string): boolean {
  return name.startsWith(`${PIVOT_TOTAL_KEY}${PIVOT_SEPARATOR}`);
}

/**
 * Add the PIVOT TOTAL columns to one level's result.
 *
 * A second query -- the level with the pivot key dropped, see
 * `pivotTotalQuery` -- joined back by GROUP KEY rather than by
 * position, since the two queries need not order alike. A row the
 * totals did not return gets null, never a neighbour's figure.
 *
 * `request` undefined is a flat cube: one pivoted row, one total row.
 */
export async function withPivotTotals(
  snapshot: CubeSnapshot,
  request: LevelRequest | undefined,
  table: ResultTable,
  paths: readonly RowPath[],
  truncated: boolean,
  deps: {
    readonly runner: QueryRunner;
    readonly snapshot: CubeSnapshot;
    readonly signal?: AbortSignal;
  },
): Promise<ResultTable> {
  const level = request?.level ?? 0;
  const keys = truncated && level > 0
    ? paths.map((p) => p[p.length - 1] ?? NULL_GROUP)
    : undefined;
  const scope: LevelScope | undefined = request === undefined
    ? undefined
    : { level: request.level, parent: request.parent };
  const query = pivotTotalQuery(snapshot, scope, keys);
  if (query === null || table.rowCount === 0) return table;
  const { rows: totals } = await deps.runner.run(
    query.pure, deps.snapshot, scope, deps.signal,
  );

  // Where each of the level's rows finds its total.
  const at: number[] = [];
  if (level === 0) {
    for (let i = 0; i < table.rowCount; i++) at.push(0);
  } else {
    const keyColumn = totals.columns[level - 1];
    const byKey = new Map<string, number>();
    keyColumn?.values.forEach((v, i) => {
      byKey.set(pathKey([...(request?.parent ?? []), groupValue(v)]), i);
    });
    for (const p of paths) at.push(byKey.get(pathKey(p)) ?? -1);
  }

  const added: ResultColumn[] = [];
  for (const measure of query.measures) {
    const source = totals.columns.find((c) => c.name === measure);
    if (!source) continue;
    added.push({
      name: pivotTotalColumn(measure),
      type: source.type,
      values: at.map((i) => (i < 0 ? null : (source.values[i] ?? null))),
    });
  }
  return { ...table, columns: [...table.columns, ...added] };
}

/** The first `n` rows of a result, columns preserved. */
export function takeRows(table: ResultTable, n: number): ResultTable {
  return {
    ...table,
    columns: table.columns.map((c) => ({ ...c, values: c.values.slice(0, n) })),
    rowCount: Math.min(table.rowCount, n),
  };
}

function emptyTable(epoch: number): ResultTable {
  return { columns: [], rowCount: 0, epoch, elapsedMs: 0 };
}

/**
 * Stitch the level results into one table in display order.
 *
 * Value columns are UNIONED across levels, because a dynamic pivot
 * discovers its own values per level: a branch that happens to contain
 * no 2021 rows produces no 2021 column, and without the union its
 * neighbours' values would shift left into the wrong columns.
 */
/**
 * How the row hierarchy is presented.
 *
 * `single` is what real DataCube does: one synthetic column carrying
 * whichever dimension's value belongs to that row, with depth shown by
 * indentation. It is heterogeneous by construction -- DataCube marks
 * the column `cellDataType: false` for exactly this reason -- and it
 * stays one column no matter how deep the cube goes.
 *
 * `perDimension` gives each row dimension its own column and steps the
 * labels diagonally. Easier to scan on a shallow cube, and it puts a
 * column header on every level, but it widens without bound.
 */
export type TreeColumnMode = 'single' | 'perDimension';

/** Name of the synthetic tree column. Its header renders empty. */
export const TREE_COLUMN = '__tree';

export interface AssembleOptions {
  readonly treeColumn?: TreeColumnMode;
  readonly totalsLabel?: string;
  /**
   * Append the leaf count to a group label, as DataCube's
   * `showLeafCount` does. Only rendered when the cube actually has a
   * count measure to render -- inventing one would change the query.
   */
  readonly showLeafCount?: boolean;
}

export function assemble(
  snapshot: CubeSnapshot,
  rows: readonly TreeRow[],
  levels: ReadonlyMap<string, LevelData>,
  options: AssembleOptions = {},
): ResultTable {
  const mode: TreeColumnMode = options.treeColumn ?? 'single';
  const totalsLabel = options.totalsLabel ?? 'Total';
  const dims = snapshot.rows;
  const valueNames: string[] = [];
  const valueTypes = new Map<string, string>();

  for (const data of levels.values()) {
    // Grouping columns come first, so anything past them is a value.
    const skip = Math.max(0, data.request.level);
    data.table.columns.slice(skip).forEach((c) => {
      if (!valueTypes.has(c.name)) {
        valueNames.push(c.name);
        valueTypes.set(c.name, c.type);
      }
    });
  }

  // Where each display row's data lives.
  const source = new Map<number, { data: LevelData; index: number }>();
  rows.forEach((row, i) => {
    const key = requestKey({
      level: row.level,
      parent: row.path.slice(0, -1),
    });
    const data = levels.get(key);
    if (!data) return;
    const index = data.paths.findIndex((p) => pathKey(p) === pathKey(row.path));
    if (index >= 0) source.set(i, { data, index });
  });

  // "Show leaf count": the count each group query carried, read by
  // display row, and rendered after the label as upstream's group cell
  // does -- `EMEA (1234)`.
  const countOf = (i: number): Scalar => {
    const hit = source.get(i);
    const col = hit?.data.table.columns.find((c) => c.name === LEAF_COUNT_COLUMN);
    return hit && col ? (col.values[hit.index] ?? null) : null;
  };
  const counted = (label: Scalar, i: number): Scalar => {
    const n = countOf(i);
    return n === null || label === null ? label : `${String(label)} (${String(n)})`;
  };

  const labelOf = (row: TreeRow, i: number): Scalar => {
    if (row.level === 0) return totalsLabel;
    const own = row.path[row.path.length - 1];
    // A group whose key is SQL NULL has no label of its own; showing
    // the sentinel would leak an internal string into the grid.
    return counted(own === undefined || own === NULL_GROUP ? null : own, i);
  };

  /**
   * The row dimensions as columns of their own, beside the tree.
   *
   * Built from the row PATHS, because that is the only place the
   * value exists: the level's query groups by that dimension, so it
   * comes back as the group key and becomes the tree's label rather
   * than a column. Un-hiding cannot conjure it -- which is why
   * turning "keep grouped columns" on restored `desk` and `book`,
   * whose aggregated versions the query does return, and never
   * `region`.
   *
   * ANCESTORS ARE FILLED: a row under AMER / Equities reads AMER in
   * `region` and Equities in `desk`, rather than only its own level.
   * A column that is blank on every row but one says less than the
   * tree it sits beside.
   */
  const keptDims: ResultColumn[] =
    mode === 'single' && snapshot.keepGroupedColumns === true
      ? dims.map((name, d) => ({
          name,
          type: 'String',
          values: rows.map((row) => (row.level > d
            ? (row.path[d] ?? null)
            : null)),
        }))
      : [];

  const dimColumns: ResultColumn[] =
    mode === 'single'
      ? [
          {
            name: TREE_COLUMN,
            // Heterogeneous on purpose: this column holds a different
            // dimension's value at every level, so it has no single
            // type. DataCube marks its own tree column the same way.
            type: 'Any',
            values: rows.map((row, i) => labelOf(row, i)),
          },
        ]
      : dims.map((name, d) => ({
          name,
          type: 'String',
          // A row shows a label only in its OWN level's column; deeper
          // columns stay empty, which is what gives the stepped look
          // instead of repeating the parent on every child.
          //
          // The grand total has no dimension value at all, so it takes
          // its label in the first column rather than rendering as a
          // blank row of numbers.
          values: rows.map((row, i) => {
            if (row.level === 0) return d === 0 ? totalsLabel : null;
            return row.level === d + 1 ? counted(row.path[d] ?? null, i) : null;
          }),
        }));

  // A dimension rebuilt from the paths REPLACES the query's own
  // aggregated copy of it -- at this level that copy is a
  // uniqueValueOnly over the whole group, which is blank whenever
  // the group holds more than one value. Two columns of the same
  // name, one blank, is worse than either alone.
  const kept = new Set(keptDims.map((c) => c.name));
  const valueColumns: ResultColumn[] = valueNames
    // The leaf count is part of the label, not a column of its own.
    .filter((name) => !kept.has(name) && name !== LEAF_COUNT_COLUMN)
    .map((name) => ({
    name,
    type: valueTypes.get(name) ?? 'Unknown',
    values: rows.map((_row, i) => {
      const hit = source.get(i);
      if (!hit) return null;
      const col = hit.data.table.columns.find((c) => c.name === name);
      return col ? (col.values[hit.index] ?? null) : null;
    }),
  }));

  const elapsedMs = [...levels.values()].reduce(
    (sum, d) => sum + d.table.elapsedMs,
    0,
  );
  const epoch = [...levels.values()][0]?.table.epoch ?? 0;

  return {
    columns: [...dimColumns, ...keptDims, ...valueColumns],
    rowCount: rows.length,
    epoch,
    elapsedMs,
  };
}
