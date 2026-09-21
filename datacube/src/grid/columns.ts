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
  /** Index into the result's columns. Survives reordering and hiding. */
  readonly index: number;
  /** Pixel width, when the user has set one, within its bounds. */
  readonly width?: number;
  /** Frozen edge, if any. */
  readonly pinned?: PinPlacement;
  /** Header text, which may differ from the column's name. */
  readonly label?: string;
  /** Rendered obscured until hovered. */
  readonly blurred?: boolean;
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
 * Force a parsed value path to the arity the cube actually pivoted on.
 *
 * The separator is OUR delimiter appearing in THEIR data, so counting
 * separators is not a reliable way to count dimensions: a product code
 * containing '__|__' parses as two values, and an empty cell parses as
 * none. Both produce a column whose header is a different DEPTH from
 * its neighbours, which is precisely what leaves a pivot header
 * misaligned -- with correct numbers underneath it, so nothing looks
 * broken.
 *
 * The cube knows how many dimensions it pivoted on, so the count is a
 * fact rather than an inference. Too few segments pad; too many fold
 * back into the FIRST, on the grounds that a value carrying a
 * separator is still one value.
 *
 * Which value absorbed the separator is genuinely ambiguous without
 * escaping the data, so a rare header can name the wrong split point.
 * It can no longer be the wrong DEPTH, which is the failure that
 * misaligns a grid rather than mislabelling one cell.
 */
function toArity(parts: readonly string[], arity: number): string[] {
  if (parts.length === arity) {
    return [...parts];
  }
  if (parts.length < arity) {
    return [...parts, ...Array<string>(arity - parts.length).fill('')];
  }
  const fold = parts.length - arity + 1;
  return [parts.slice(0, fold).join(PIVOT_SEPARATOR), ...parts.slice(fold)];
}

/**
 * Split a generated column name into its header path.
 *
 * `measures` are the measure names the snapshot asked for. When a name
 * ends in one of them, that becomes the last path segment and the
 * prefix (minus any trailing separator character) is the pivot value
 * path. The longest matching measure wins, so 'pnl' cannot shadow
 * 'pnl_net'.
 *
 * `pivotArity` is how many columns the cube pivoted on. Pass it and
 * the value path is forced to that many segments; omit it and the
 * separator count decides, which is only safe when the data cannot
 * contain the separator.
 */
export function splitPath(
  name: string,
  measures: readonly string[] = [],
  pivotArity?: number,
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
    const parts = cleaned.length > 0 ? cleaned.split(PIVOT_SEPARATOR) : [];
    return pivotArity === undefined
      ? parts.length > 0
        ? [...parts, matched]
        : [matched]
      : [...toArity(parts, pivotArity), matched];
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
/** Which edge a column is frozen to, if any. */
export type PinPlacement = 'left' | 'right';

export interface ColumnLayout {
  /** Display order. Columns not listed keep engine order, after these. */
  readonly order?: readonly string[];
  /** Hidden from the grid. Still queried, so totals stay correct. */
  readonly hidden?: readonly string[];
  /** Keep a row dimension as a data column as well as in the tree. */
  readonly keepGrouped?: boolean;
  /** Pixel widths by column name. */
  readonly widths?: Readonly<Record<string, number>>;
  /** Lower and upper bounds, applied to the width above. */
  readonly minWidths?: Readonly<Record<string, number>>;
  readonly maxWidths?: Readonly<Record<string, number>>;
  /** Frozen columns, by edge. */
  readonly pinned?: Readonly<Record<string, PinPlacement>>;
  /** Header text, where it should differ from the column name. */
  readonly displayNames?: Readonly<Record<string, string>>;
  /**
   * Columns rendered obscured until hovered.
   *
   * DataCube's `blur`. For a figure that should not be readable over
   * a shoulder or in a screen share, which in this domain is a real
   * requirement rather than a novelty.
   */
  readonly blurred?: readonly string[];
}

export function buildColumnModel(
  table: ResultTable,
  dimensions: readonly string[] = [],
  measures: readonly string[] = [],
  layout: ColumnLayout = {},
  /**
   * How many columns the cube pivoted on. Given, the header depth is
   * a fact about the CUBE; omitted, it is inferred from the generated
   * names, which the data can distort.
   */
  pivotArity?: number,
): ColumnModel {
  // The tree column's header is deliberately blank: it holds a
  // different dimension at every level, so no single name is
  // truthful, and real DataCube sets headerName: '' for the same
  // reason (DataCubeGridConfigurationBuilder, autoGroupColumnDef).
  //
  // That reads correctly while OTHER columns sit beside it. Alone it
  // does not: grouping a cube that has no measures leaves the tree
  // column as the only column, and a grid whose entire header row is
  // one empty cell is indistinguishable from a broken one -- it was
  // reported as "all the column headers disappear". So when it would
  // be the only header, it says what it is grouping by.
  const treeIsAlone = table.columns.length === 1
    && table.columns[0]?.name === TREE_COLUMN;
  const treeLabel = treeIsAlone && dimensions.length > 0
    ? dimensions.join(' / ')
    : '';

  const leaves: LeafColumn[] = table.columns.map((c, index) => {
    const display = layout.displayNames?.[c.name];
    const path = c.name === TREE_COLUMN
      ? [treeLabel]
      : display !== undefined
        ? [display]
        : dimensions.includes(c.name)
          ? [c.name]
          : splitPath(c.name, measures, pivotArity);
    return {
      index,
      name: c.name,
      path,
      type: c.type,
      isDimension: c.name === TREE_COLUMN || dimensions.includes(c.name),
    };
  });

  // Hiding happens BEFORE the header is built, or a hidden column
  // still contributes a colSpan and pushes its neighbours sideways.
  // The tree column is never hidden: without it a grouped cube has no
  // row labels at all.
  const hidden = new Set(layout.hidden ?? []);
  // A ROW DIMENSION IS SHOWN IN THE TREE, NOT TWICE.
  //
  // The query aggregates every column that is not the group key of
  // the level being fetched, so a cube grouped by region, desk and
  // book still returns desk and book at level one -- and they were
  // rendered as ordinary columns beside the tree. The first row
  // dimension vanished (its values are the tree's) while the rest
  // stayed, which is the inconsistency a user sees: "region
  // disappears but the next ones I group by stay in the grid".
  //
  // ag-grid hides a column once it is row-grouped, and DataCube
  // leans on that (`rowGroup: Boolean(groupByCol)` in
  // DataCubeGridConfigurationBuilder). Only when the tree is
  // actually present, though: without it these columns are all a
  // flat cube has.
  const treeShown = table.columns.some((c) => c.name === TREE_COLUMN);
  const inTree = new Set(
    treeShown && layout.keepGrouped !== true ? dimensions : [],
  );
  const visible = leaves.filter(
    (l) => l.name === TREE_COLUMN
      || (!hidden.has(l.name) && !inTree.has(l.name)),
  );

  // Ordering is applied to the VISIBLE leaves; anything unlisted keeps
  // engine order behind the listed ones, so adding a measure does not
  // silently vanish from a saved view that predates it.
  const order = layout.order;
  /**
   * What a leaf is ORDERED BY.
   *
   * Its own name when it is a plain column, and its MEASURE when it
   * is pivoted: `2021__|__notional` is a value crossed with a
   * measure, and the order a configuration holds is a list of source
   * columns. Matching on the full leaf name found nothing for a
   * pivoted cube, so its columns could not be reordered at all.
   */
  const rankOf = (l: LeafColumn): string =>
    l.path[l.path.length - 1] ?? l.name;

  /**
   * Which pivot VALUE block a leaf belongs to, by first appearance.
   *
   * Reordering the measures must happen INSIDE each block. A single
   * ordering across every leaf would interleave the blocks --
   * 2021's notional, 2022's notional, 2021's pnl -- which is not a
   * pivot table any more.
   */
  const blocks = new Map<string, number>();
  const blockOf = (l: LeafColumn): number => {
    const key = l.path.slice(0, -1).join('\u0000');
    const known = blocks.get(key);
    if (known !== undefined) return known;
    blocks.set(key, blocks.size);
    return blocks.size - 1;
  };
  for (const l of visible) blockOf(l);

  const ordered = order
    ? [...visible].sort((a, b) => {
        const ba = blockOf(a);
        const bb = blockOf(b);
        if (ba !== bb) return ba - bb;
        const ia = order.indexOf(rankOf(a));
        const ib = order.indexOf(rankOf(b));
        if (ia === -1 && ib === -1) return a.index - b.index;
        if (ia === -1) return 1;
        if (ib === -1) return -1;
        return ia - ib;
      })
    : visible;

  const widths = layout.widths ?? {};
  const minWidths = layout.minWidths ?? {};
  const maxWidths = layout.maxWidths ?? {};
  const pinned = layout.pinned ?? {};
  const displayNames = layout.displayNames ?? {};
  const blurred = new Set(layout.blurred ?? []);

  const sized: LeafColumn[] = ordered.map((l) => {
    // A width is clamped by its own bounds rather than applied raw,
    // so a saved width from a wider screen cannot squeeze a column
    // past the minimum that made it readable.
    const raw = widths[l.name];
    const lo = minWidths[l.name];
    const hi = maxWidths[l.name];
    let width = raw ?? lo ?? hi;
    if (width !== undefined) {
      if (lo !== undefined) width = Math.max(width, lo);
      if (hi !== undefined) width = Math.min(width, hi);
    }
    const label = displayNames[l.name];
    const pin = pinned[l.name];
    return {
      ...l,
      ...(width !== undefined ? { width } : {}),
      ...(pin ? { pinned: pin } : {}),
      ...(label !== undefined ? { label } : {}),
      ...(blurred.has(l.name) ? { blurred: true } : {}),
    };
  });

  const depth = Math.max(1, ...sized.map((l) => l.path.length));

  // Level by level, merge runs of adjacent leaves that share a prefix.
  const headerRows: HeaderCell[][] = [];
  for (let level = 0; level < depth; level++) {
    const row: HeaderCell[] = [];
    let i = 0;
    while (i < sized.length) {
      const leaf = sized[i]!;

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
      while (j < sized.length) {
        const next = sized[j]!;
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
        // THE POSITION IN `leaves`, not the column's original index.
        //
        // Its one consumer does `model.leaves[cell.leafIndex]`, and
        // `leaves` here is the list AFTER hiding and reordering --
        // whereas `leaf.index` is where the column sat in the source.
        // They agree only for a cube with nothing hidden and no
        // custom order, which is why this survived.
        //
        // Hide one column and every header to its right claimed the
        // identity of its right-hand NEIGHBOUR: the label read
        // `booked_at` and its `data-column` said `region`, so sorting
        // that header sorted region, dragging it grouped by region,
        // and hiding it hid region. The last header resolved to
        // nothing at all, leaving its whole column menu disabled.
        // Nothing looked wrong -- `colStart` uses the position, so
        // the layout stayed perfect while every action went one
        // column across.
        ...(isLeafHere && j - i === 1 ? { leafIndex: i } : {}),
      });
      i = j;
    }
    headerRows.push(row);
  }

  return { leaves: sized, headerRows, depth };
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
