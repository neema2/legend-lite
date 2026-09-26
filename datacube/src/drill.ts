// Drill-through: the rows behind a number.
//
// Ranked the highest-value interaction in the research, and the reason
// is trust rather than convenience: it turns every subtotal from an
// assertion into an auditable claim. An analyst who can click a figure
// and see the trades that produced it can defend it; one who cannot,
// cannot.
//
// The query is the cube's own filter, plus the clicked row's group
// path, plus the clicked column's pivot value -- and then NO
// aggregation at all. That is what makes the result provably the same
// population the aggregate was computed over: same filter, same
// grouping keys, one fewer step.
//
// Against a snap it reads the frozen rows, so the drill audits the
// same data the aggregate came from rather than a live table that has
// moved on underneath it. That is why a snap captures source rows at
// drillable grain instead of storing aggregated results.

import { NULL_GROUP, derivedExtend, filterExpression } from './serialize.ts';
import type { CubeSnapshot, FilterNode } from './snapshot.ts';
import type { RowPath } from './tree.ts';

export interface DrillRequest {
  /** The clicked row's group path; [] for the grand total. */
  readonly path: RowPath;
  /**
   * The clicked column's pivot values, outermost first. Absent for a
   * row-dimension cell or an unpivoted cube.
   */
  readonly pivotPath?: readonly string[];
  /** Safety valve: drill-through is a peek, not an export. */
  readonly limit?: number;
}

export const DEFAULT_DRILL_LIMIT = 500;

/**
 * Conditions pinning the clicked cell.
 *
 * Exported because the set of conditions is the auditable part: a
 * reviewer should be able to read exactly which rows were counted.
 */
export function drillConditions(
  snapshot: CubeSnapshot,
  request: DrillRequest,
): FilterNode[] {
  const out: FilterNode[] = [];
  if (snapshot.filter) out.push(snapshot.filter);

  const pin = (column: string, value: string): FilterNode =>
    value === NULL_GROUP
      ? { kind: 'condition', column, operator: 'isEmpty' }
      : { kind: 'condition', column, operator: 'equal', value };

  request.path.forEach((value, i) => {
    const column = snapshot.rows[i];
    if (column !== undefined) out.push(pin(column, value));
  });

  (request.pivotPath ?? []).forEach((value, i) => {
    const column = snapshot.pivotOn[i];
    if (column !== undefined) out.push(pin(column, value));
  });

  return out;
}

/**
 * Pure for the rows behind a cell.
 *
 * Every source column is selected, not just the cube's: the point of
 * drilling is to see the record, and a column the cube happens not to
 * group by is often exactly the one that explains the number.
 */
export function drillQuery(
  snapshot: CubeSnapshot,
  request: DrillRequest,
): string {
  const parts: string[] = [snapshot.source.expression];

  for (const d of snapshot.derived) {
    parts.push(derivedExtend(d));
  }

  const conditions = drillConditions(snapshot, request);
  if (conditions.length === 1) {
    parts.push(`filter(x|${filterExpression(conditions[0]!)})`);
  } else if (conditions.length > 1) {
    parts.push(
      `filter(x|${filterExpression({ kind: 'and', children: conditions })})`,
    );
  }

  // Deliberately no groupBy and no pivot: the population, not its
  // aggregate.
  const limit = request.limit ?? DEFAULT_DRILL_LIMIT;
  parts.push(`limit(${limit})`);
  return parts.join('->');
}

/**
 * Split a generated pivot column name back into its values.
 *
 * The measure name is stripped by the caller (the grid's column model
 * already did that work), so this receives only the value path.
 */
export function pivotPathOf(
  leafPath: readonly string[],
  measureNames: readonly string[],
): string[] {
  const last = leafPath[leafPath.length - 1];
  return last !== undefined && measureNames.includes(last)
    ? leafPath.slice(0, -1)
    : [...leafPath];
}
