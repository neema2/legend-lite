// Turning a flat result into the grid's column model, including the
// nested header a pivot needs.
//
// A pivot's generated columns encode their path in the name, joined by
// a separator. legend-lite builds multi-column pivot keys by
// concatenating with '__|__', and DataCube uses the identical
// separator (PIVOT_COLUMN_NAME_VALUE_SEPARATOR) -- the two sides
// already agree, which is why this parses rather than negotiates.
//
//   region                     a row dimension, depth 1
//   2023__|__total             pivot value + measure, depth 2
//   USA__|__NYC__|__total      two pivot dimensions + measure, depth 3
//
// The result is RAGGED: row dimensions sit at depth 1 while pivot
// columns run deeper, so a row dimension's header has to span the full
// header height instead of leaving holes above the data. Getting that
// wrong is the classic misaligned-pivot-header bug.

import type { ResultTable } from '../result.ts';

/** legend-lite's and DataCube's shared pivot path separator. */
export const PIVOT_SEPARATOR = '__|__';

export interface LeafColumn {
  /** Index into the result's columns. */
  readonly index: number;
  /** Full generated name, e.g. '2023__|__total'. */
  readonly name: string;
  /** Path segments, e.g. ['2023', 'total']. */
  readonly path: readonly string[];
  /** Engine-reported type. */
  readonly type: string;
  /** True when this column is a row dimension rather than a value. */
  readonly isDimension: boolean;
}

export interface HeaderCell {
  readonly label: string;
  /** Columns spanned horizontally. */
  readonly colSpan: number;
  /** Header levels spanned vertically; >1 only for ragged dimensions. */
  readonly rowSpan: number;
  /** Leaf column index when this cell sits directly over one. */
  readonly leafIndex?: number;
}

export interface ColumnModel {
  readonly leaves: readonly LeafColumn[];
  /** One row of header cells per level, top to bottom. */
  readonly headerRows: readonly (readonly HeaderCell[])[];
  /** Number of header levels. At least 1. */
  readonly depth: number;
}

export function splitPath(name: string): string[] {
  return name.split(PIVOT_SEPARATOR);
}

/**
 * Build the column model.
 *
 * `dimensions` names the row-dimension columns (from the snapshot's
 * `rows`), because a result alone cannot distinguish a dimension from a
 * single-level pivot column -- both are plain names. Passing them in is
 * what keeps this honest rather than heuristic.
 */
export function buildColumnModel(
  table: ResultTable,
  dimensions: readonly string[] = [],
): ColumnModel {
  const leaves: LeafColumn[] = table.columns.map((c, index) => {
    const path = splitPath(c.name);
    return {
      index,
      name: c.name,
      path,
      type: c.type,
      isDimension: dimensions.includes(c.name),
    };
  });

  const depth = Math.max(1, ...leaves.map((l) => l.path.length));

  // Level by level, merge runs of adjacent leaves that share a prefix.
  const headerRows: HeaderCell[][] = [];
  for (let level = 0; level < depth; level++) {
    const row: HeaderCell[] = [];
    let i = 0;
    while (i < leaves.length) {
      const leaf = leaves[i]!;

      // A leaf shallower than this level has already been covered by a
      // rowSpan emitted at its own level -- skip it rather than
      // emitting a blank, which is what misaligns the header.
      if (leaf.path.length <= level) {
        i += 1;
        continue;
      }

      const label = leaf.path[level]!;
      // Merge while the whole prefix matches, not just this segment:
      // two different years can both have a 'total' beneath them, and
      // merging on the segment alone would fuse unrelated columns.
      let j = i + 1;
      while (j < leaves.length) {
        const next = leaves[j]!;
        if (next.path.length <= level) break;
        if (!samePrefix(leaf.path, next.path, level)) break;
        j += 1;
      }

      const isLeafHere = leaf.path.length === level + 1;
      row.push({
        label,
        colSpan: j - i,
        // A short path spans the remaining header levels, so a row
        // dimension's header fills the header block.
        rowSpan: isLeafHere ? depth - level : 1,
        ...(isLeafHere && j - i === 1 ? { leafIndex: leaf.index } : {}),
      });
      i = j;
    }
    headerRows.push(row);
  }

  return { leaves, headerRows, depth };
}

function samePrefix(
  a: readonly string[],
  b: readonly string[],
  level: number,
): boolean {
  for (let k = 0; k <= level; k++) {
    if (a[k] !== b[k]) return false;
  }
  return true;
}

/** Leaf columns that carry values rather than row dimensions. */
export function valueColumns(model: ColumnModel): LeafColumn[] {
  return model.leaves.filter((l) => !l.isDimension);
}
