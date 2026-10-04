// Cell selection, and the statistics DataCube shows for it.
//
// A range rather than a set of rows: in a pivot the interesting
// selection is usually a rectangle of measures -- one quarter across
// three desks -- and a row-based model cannot express that. DataCube
// uses cell selection rather than row selection for the same reason,
// which is also why its "copy" actions are cell-shaped.
//
// Kept pure so the arithmetic can be tested without a DOM, and so the
// grid renders a selection rather than owning one.

import type { LeafColumn } from './grid/columns.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { asDecimal, exactSum, numberOf } from '../../engine-client/src/values.ts';

/**
 * The columns a selection's positions name: the grid's VISIBLE leaves, in
 * display order. A selection is in grid coordinates, and the result's
 * own column order is something else -- hidden columns, the configured
 * order, a pivot and the tree all come between them. Every reader of a
 * selection goes through this, never through `table.columns[position]`
 * (which copied a hidden column's values while another was highlighted,
 * P2-36).
 */
export type SelectionColumns = readonly Pick<LeafColumn, 'index' | 'name' | 'label'>[];

export interface CellRef {
  readonly row: number;
  readonly col: number;
}

/**
 * A rectangle, stored by its ANCHOR and its focus rather than
 * normalised bounds.
 *
 * Dragging up-and-left is as ordinary as down-and-right, and a
 * normalised rectangle forgets which corner the user started from --
 * which is the corner that must stay put when the selection is
 * extended with shift.
 */
export interface CellRange {
  readonly anchor: CellRef;
  readonly focus: CellRef;
}

export interface Bounds {
  readonly top: number;
  readonly left: number;
  readonly bottom: number;
  readonly right: number;
}

export function bounds(range: CellRange): Bounds {
  return {
    top: Math.min(range.anchor.row, range.focus.row),
    bottom: Math.max(range.anchor.row, range.focus.row),
    left: Math.min(range.anchor.col, range.focus.col),
    right: Math.max(range.anchor.col, range.focus.col),
  };
}

export function contains(range: CellRange, row: number, col: number): boolean {
  const b = bounds(range);
  return row >= b.top && row <= b.bottom && col >= b.left && col <= b.right;
}

export function cellCount(range: CellRange): number {
  const b = bounds(range);
  return (b.bottom - b.top + 1) * (b.right - b.left + 1);
}

/** A single cell, as a degenerate range. */
export function single(row: number, col: number): CellRange {
  return { anchor: { row, col }, focus: { row, col } };
}

/**
 * Extend a selection to a new focus, keeping the anchor.
 *
 * This is what shift-click and shift-arrow do, and keeping the anchor
 * is the whole behaviour: extending should grow from where the user
 * started, not from wherever they last were.
 */
export function extend(range: CellRange, to: CellRef): CellRange {
  return { anchor: range.anchor, focus: to };
}

export interface SelectionStats {
  /** Cells in the rectangle, including blanks. */
  readonly cells: number;
  /** Cells holding a number. */
  readonly numeric: number;
  /** Cells holding nothing. */
  readonly blank: number;
  /** EXACT: decimal text (values.ts `exactSum`), so a DECIMAL beyond 2^53 sums to its digits. */
  readonly sum: string;
  readonly average: number;
  /** The smallest and largest numeric cells, as their own exact values; null when none. */
  readonly min: Scalar;
  readonly max: Scalar;
  /** The compiler type the numeric cells share, when they share one. */
  readonly type: string | undefined;
  /** The leaves the numeric cells came from, by name: whose format they are read in. */
  readonly columns: readonly string[];
}

/**
 * Statistics over a selection.
 *
 * Blanks are counted but excluded from the arithmetic: an average
 * over a pivot region that silently treated empty combinations as
 * zero would be wrong, and wrong in the direction of looking
 * plausible.
 *
 * Non-numeric cells are ignored rather than coerced, so selecting a
 * label column alongside a measure does not turn the sum into NaN.
 */
export function selectionStats(
  table: ResultTable,
  leaves: SelectionColumns,
  range: CellRange,
): SelectionStats {
  const b = bounds(range);
  let cells = 0;
  let numeric = 0;
  let blank = 0;
  let sum = 0;
  const exact: unknown[] = [];
  let min: { n: number; v: Scalar } | null = null;
  let max: { n: number; v: Scalar } | null = null;
  const types = new Set<string>();
  const names = new Set<string>();

  for (let c = b.left; c <= b.right; c++) {
    const leaf = leaves[c];
    const column = leaf === undefined ? undefined : table.columns[leaf.index];
    for (let r = b.top; r <= b.bottom; r++) {
      cells += 1;
      const v: Scalar = column?.values[r] ?? null;
      if (v === null) {
        blank += 1;
        continue;
      }
      // numeric by the column's COMPILER type: a decimal's value is its exact text and a
      // big integer a bigint; min and max keep the cell's own exact value
      const n = numberOf(v, column?.type);
      if (n !== null && asDecimal(v) !== null) {
        numeric += 1;
        sum += n;
        exact.push(v);
        types.add(column?.type as string);
        if (leaf !== undefined) names.add(leaf.name);
        if (min === null || n < min.n) min = { n, v };
        if (max === null || n > max.n) max = { n, v };
      }
    }
  }

  return {
    cells,
    numeric,
    blank,
    sum: exactSum(exact),
    average: numeric > 0 ? sum / numeric : 0,
    min: min?.v ?? null,
    max: max?.v ?? null,
    type: types.size === 1 ? [...types][0] : undefined,
    columns: [...names],
  };
}

/**
 * The selected rectangle as a table, for copying.
 *
 * Returns a ResultTable so the existing exporters handle it: a
 * clipboard payload and a CSV file differ in their delimiter, not in
 * how they escape, and duplicating the escaping is how the two drift.
 */
export function selectionTable(
  table: ResultTable,
  leaves: SelectionColumns,
  range: CellRange,
): ResultTable {
  const b = bounds(range);
  const columns = leaves
    .slice(b.left, b.right + 1)
    .flatMap((leaf) => {
      const c = table.columns[leaf.index];
      // Headed as the screen heads it: a renamed column copies under its
      // label, not its internal name (P2-41).
      return c === undefined ? [] : [{
        ...c,
        name: leaf.label ?? c.name,
        values: c.values.slice(b.top, b.bottom + 1),
      }];
    });
  return {
    columns,
    rowCount: Math.max(0, b.bottom - b.top + 1),
    epoch: table.epoch,
    elapsedMs: 0,
  };
}
