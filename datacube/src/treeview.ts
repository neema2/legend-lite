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

/** One level's result, plus the group path of each of its rows. */
export interface LevelData {
  readonly request: LevelRequest;
  readonly table: ResultTable;
  readonly paths: readonly RowPath[];
}

export interface TreeView {
  /** Rows in display order, parallel to `table`. */
  readonly rows: readonly TreeRow[];
  /** One combined table whose row i belongs to rows[i]. */
  readonly table: ResultTable;
  /** Every level fetched, for diagnostics and incremental reuse. */
  readonly levels: ReadonlyMap<string, LevelData>;
}

/** A group key rendered as text, with SQL NULL kept distinguishable. */
function groupValue(v: Scalar): string {
  return v === null ? NULL_GROUP : String(v);
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
  },
): Promise<TreeView> {
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
      const grammar = serialize(snapshot, request);
      const sql = await deps.planner.plan(grammar, snapshot, request);
      const table = await deps.engine.execute(sql, deps.epoch);
      levels.set(requestKey(request), {
        request,
        table,
        paths: pathsOf(request, table),
      });
      // A superseded interaction should stop fetching the rest of the
      // tree rather than finish work nobody will look at.
      if (!deps.guard.isCurrent(deps.epoch)) {
        return { rows: [], table: emptyTable(deps.epoch), levels };
      }
    }
  }

  const rows = flattenTree(state, depth, childrenOf);
  return { rows, table: assemble(snapshot, rows, levels), levels };
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
export function assemble(
  snapshot: CubeSnapshot,
  rows: readonly TreeRow[],
  levels: ReadonlyMap<string, LevelData>,
  totalsLabel = 'Total',
): ResultTable {
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

  const dimColumns: ResultColumn[] = dims.map((name, d) => ({
    name,
    type: 'String',
    // A row shows a label only in its OWN level's column; deeper
    // columns stay empty, which is what gives a pivot its stepped
    // look instead of repeating the parent on every child.
    //
    // The grand total has no dimension value at all, so it would
    // otherwise render as a blank row of numbers with no indication
    // of what it totals. It takes a label in the first column.
    values: rows.map((row) => {
      if (row.level === 0) return d === 0 ? totalsLabel : null;
      return row.level === d + 1 ? (row.path[d] ?? null) : null;
    }),
  }));

  const valueColumns: ResultColumn[] = valueNames.map((name) => ({
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
    columns: [...dimColumns, ...valueColumns],
    rowCount: rows.length,
    epoch,
    elapsedMs,
  };
}
