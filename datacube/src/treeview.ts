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

import type { QueryEngine } from './engine.ts';
import type { EpochGuard } from './epoch.ts';
import type { Planner } from './cube.ts';
import { NULL_GROUP, serialize } from './serialize.ts';
import type { ResultColumn, ResultTable, Scalar } from './result.ts';
import type { CubeSnapshot } from './snapshot.ts';
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
    readonly planner: Planner;
    readonly engine: QueryEngine;
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
      const sql = await deps.planner.plan(grammar, snapshot, scoped, deps.signal);
      const full = await deps.engine.execute(sql, deps.epoch, deps.signal);
      const truncated = full.rowCount > maxRows;
      const table = truncated ? takeRows(full, maxRows) : full;
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

/** The first `n` rows of a result, columns preserved. */
function takeRows(table: ResultTable, n: number): ResultTable {
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

  const labelOf = (row: TreeRow): Scalar => {
    if (row.level === 0) return totalsLabel;
    const own = row.path[row.path.length - 1];
    // A group whose key is SQL NULL has no label of its own; showing
    // the sentinel would leak an internal string into the grid.
    return own === undefined || own === NULL_GROUP ? null : own;
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
            values: rows.map(labelOf),
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
          values: rows.map((row) => {
            if (row.level === 0) return d === 0 ? totalsLabel : null;
            return row.level === d + 1 ? (row.path[d] ?? null) : null;
          }),
        }));

  // A dimension rebuilt from the paths REPLACES the query's own
  // aggregated copy of it -- at this level that copy is a
  // uniqueValueOnly over the whole group, which is blank whenever
  // the group holds more than one value. Two columns of the same
  // name, one blank, is worse than either alone.
  const kept = new Set(keptDims.map((c) => c.name));
  const valueColumns: ResultColumn[] = valueNames
    .filter((name) => !kept.has(name))
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
