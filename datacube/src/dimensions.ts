// Named dimensions: a hierarchy drilled as a unit.
//
// DataCube's DataCubeDimensionConfiguration is a name plus an ORDERED
// list of columns -- Geography being region, country, city. The point
// is that a user drills "Geography" rather than remembering that
// country comes after region and before city, and that the order is
// defined once by whoever knows the model rather than rediscovered by
// each analyst.
//
// It composes with the row-group tree rather than replacing it: a
// dimension is a preset for `rows`, and drilling one level deeper
// appends its next column. That keeps one tree implementation instead
// of a second "dimensional mode" with its own bugs.

import { rowColumns, type CubeSnapshot } from './snapshot.ts';

export interface Dimension {
  readonly name: string;
  /** Columns from coarsest to finest. Order is the hierarchy. */
  readonly columns: readonly string[];
}

/** How far a cube is currently drilled into a dimension. */
export function drilledDepth(s: CubeSnapshot, d: Dimension): number {
  let depth = 0;
  while (
    depth < d.columns.length &&
    s.rows[depth] !== undefined &&
    s.rows[depth] === d.columns[depth]
  ) {
    depth += 1;
  }
  return depth;
}

/** Whether the cube's rows are exactly a prefix of this dimension. */
export function isDrilling(s: CubeSnapshot, d: Dimension): boolean {
  const depth = drilledDepth(s, d);
  return depth > 0 && depth === s.rows.length;
}

/**
 * Drill one level deeper, or to an explicit depth.
 *
 * Depth is clamped to the dimension rather than throwing: a UI that
 * offers "drill down" on the finest level should get a no-op, not an
 * exception, and clamping makes the caller's guard optional rather
 * than mandatory.
 */
export function drillTo(
  s: CubeSnapshot,
  d: Dimension,
  depth: number,
): CubeSnapshot {
  const target = Math.max(0, Math.min(depth, d.columns.length));
  const rows = d.columns.slice(0, target);
  // Same rows means the same query; returning the same object lets a
  // caller skip the refresh.
  if (
    rows.length === s.rows.length &&
    rows.every((c, i) => c === s.rows[i])
  ) {
    return s;
  }
  return { ...s, rows };
}

export function drillDown(s: CubeSnapshot, d: Dimension): CubeSnapshot {
  return drillTo(s, d, drilledDepth(s, d) + 1);
}

export function drillUp(s: CubeSnapshot, d: Dimension): CubeSnapshot {
  return drillTo(s, d, drilledDepth(s, d) - 1);
}

/**
 * Apply a dimension from scratch, at one level.
 *
 * Starting at one level rather than all of them on purpose: opening
 * Geography fully would fetch city-level groups for every country
 * before the user has asked for any of them.
 */
export function useDimension(
  s: CubeSnapshot,
  d: Dimension,
  depth = 1,
): CubeSnapshot {
  return drillTo(s, d, depth);
}

/**
 * The dimensions a cube could drill, given the columns it has.
 *
 * A dimension naming a column the source does not have is skipped
 * rather than offered and then failing -- model configuration and
 * source schema drift apart, and the grid is the wrong place to
 * discover that with an error.
 */
export function availableDimensions(
  s: CubeSnapshot,
  dimensions: readonly Dimension[],
): Dimension[] {
  const have = new Set(rowColumns(s).map((c) => c.name));
  return dimensions.filter(
    (d) => d.columns.length > 0 && d.columns.every((c) => have.has(c)),
  );
}

/**
 * A breadcrumb of the current drill path, for a header.
 *
 * Returns the dimension's name followed by each level entered, so a
 * user can see where they are and click back to any level.
 */
export function drillPath(
  s: CubeSnapshot,
  d: Dimension,
): { readonly label: string; readonly depth: number }[] {
  const depth = drilledDepth(s, d);
  const out = [{ label: d.name, depth: 0 }];
  for (let i = 0; i < depth; i++) {
    out.push({ label: d.columns[i] as string, depth: i + 1 });
  }
  return out;
}
