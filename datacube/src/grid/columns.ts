// Turning a flat result into the grid's column model, including the
// nested header a pivot needs.
//
// A pivot's generated columns encode a path in their name, but the
// exact spelling is the ENGINE's, not ours, and assuming otherwise was
// a real bug here: DuckDB names a pivoted column `2021_notional`,
// joining value to measure with an underscore, while '__|__' is what
// legend-lite uses to join multiple pivot DIMENSIONS into one
// composite key. Two different joins, two different separators.
//
// So the split is driven by what we ASKED for rather than by guessing
// at a delimiter: the measure names are known, so a column ending in a
// measure name splits there, and only the remaining prefix is parsed
// for the multi-dimension separator. That works for either engine's
// naming and degrades to a flat header rather than a wrong one.
//
//   region                     a row dimension, depth 1
//   2023_total                 pivot value + measure, depth 2
//   USA__|__NYC_total          two pivot dimensions + measure, depth 3
//
// The result is RAGGED: row dimensions sit at depth 1 while pivot
// columns run deeper, so a row dimension's header has to span the full
// header height instead of leaving holes above the data. Getting that
// wrong is the classic misaligned-pivot-header bug.

import type { ResultTable } from '../result.ts';
import { TREE_COLUMN } from '../treeview.ts';

export { TREE_COLUMN };

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
  /**
   * Zero-based leaf column this cell starts at.
   *
   * Required because a ragged header cannot be laid out by document
   * order alone: once a dimension spans several header rows, later
   * rows have fewer cells than columns, and anything that just lays
   * them out left to right puts them under the wrong columns.
   */
  readonly colStart: number;
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

/**
 * Split a generated column name into its header path.
 *
 * `measures` are the measure names the snapshot asked for. When a name
 * ends in one of them, that becomes the last path segment and the
 * prefix (minus any trailing separator character) is the pivot value
 * path. The longest matching measure wins, so 'pnl' cannot shadow
 * 'pnl_net'.
 */
export function splitPath(
  name: string,
  measures: readonly string[] = [],
): string[] {
  const matched = measures
    .filter((m) => name === m || name.endsWith(m))
    .sort((a, b) => b.length - a.length)[0];

  if (matched && name !== matched) {
    const prefix = name.slice(0, name.length - matched.length);
    // Tolerate whatever single separator the engine used between the
    // value and the measure -- '_' for DuckDB, '__|__' elsewhere.
    const cleaned = prefix.endsWith(PIVOT_SEPARATOR)
      ? prefix.slice(0, -PIVOT_SEPARATOR.length)
      : prefix.replace(/[_\-.|]+$/, '');
    return cleaned.length > 0
      ? [...cleaned.split(PIVOT_SEPARATOR), matched]
      : [matched];
  }

  return name.split(PIVOT_SEPARATOR);
}

/**
 * Build the column model.
 *
 * `dimensions` names the row-dimension columns and `measures` the
 * measure names, both from the snapshot. A result alone cannot
 * distinguish a dimension from a single-level pivot column, nor tell
 * where a generated name stops being a pivot value and starts being a
 * measure. Passing both in is what keeps this exact rather than
 * heuristic.
 */
export function buildColumnModel(
  table: ResultTable,
  dimensions: readonly string[] = [],
  measures: readonly string[] = [],
): ColumnModel {
  const leaves: LeafColumn[] = table.columns.map((c, index) => {
    // The tree column's header is deliberately blank: it holds a
    // different dimension at every level, so no single name is
    // truthful. Real DataCube sets headerName: '' for the same reason.
    const path = c.name === TREE_COLUMN
      ? ['']
      : dimensions.includes(c.name)
        ? [c.name]
        : splitPath(c.name, measures);
    return {
      index,
      name: c.name,
      path,
      type: c.type,
      isDimension: c.name === TREE_COLUMN || dimensions.includes(c.name),
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
        colStart: i,
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
