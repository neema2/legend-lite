// Turning a flat result into the grid's column model, including the
// nested header a pivot needs.
//
// A pivot's generated column encodes a path in its name, and the
// spelling belongs to legend-lite: PIVOT_SEPARATOR ('__|__',
// Type.java:467) joins pivot DIMENSIONS to each other and the last
// dimension to the MEASURE. Every planner spells it that way --
// including DuckDB's native PIVOT, which legend-lite normalises by
// naming the aggregate alias `_|__agg` so DuckDB's own
// value + '_' + alias concatenation lands on `value__|__agg`
// (DuckDb.java:186; DuckDBIntegrationTest:5515 asserts it).
//
// This once also accepted a single '_', on the belief that DuckDB
// emitted `2021_notional`. Nothing emits that, and the tolerance was
// not harmless: it made the separator optional, so ANY column whose
// name merely ENDS in a measure name became a pivot column. A plain
// `forecast_pnl`, with a measure called `pnl`, was torn into
// ['forecast', 'pnl'] and rendered under a two-level header it had no
// business in -- silently, since a plausible header is not an error.
//
// So the separator is required, not sniffed. Measure names are still
// the anchor, but the prefix must end in the separator or the column
// is not a generated pivot column and stays flat.
//
//   region                          a row dimension, depth 1
//   2023__|__total                  pivot value + measure, depth 2
//   USA__|__NYC__|__total           two pivot dimensions + measure, depth 3
//   forecast_pnl                    NOT a pivot column: stays flat
//
// The result is RAGGED: row dimensions sit at depth 1 while pivot
// columns run deeper, so a row dimension's header has to span the full
// header height instead of leaving holes above the data. Getting that
// wrong is the classic misaligned-pivot-header bug.

import { PIVOT_SEPARATOR } from '../generated/lite-facts.ts';
import type { ResultTable } from '../result.ts';
import { PIVOT_TOTAL_KEY } from '../snapshot.ts';
import { TREE_COLUMN } from '../treeview.ts';

export { TREE_COLUMN };

/** legend-lite's and DataCube's shared pivot path separator. */
// Re-exported, not declared: the spelling is legend-lite's
// (Type.java's PIVOT_SEPARATOR) and `src/generated/lite-facts.ts`
// reads it out of there, so a change in lite fails
// `bazel test //datacube:update_generated_test` instead of silently flattening a
// header. Every existing importer of this name keeps working.
export { PIVOT_SEPARATOR };

/**
 * The grand total's synthetic group key.
 *
 * A total is one group over everything, and the obvious way to say
 * that -- `groupBy(~[], ~[...])` -- crashes the real engine
 * ("NullPointerException ... because resO is null"). Upstream never
 * writes it: it extends a constant column and groups by THAT
 * (`_extendRootAggregation`, value `[ROOT]`), which is one group by
 * construction. The column is an artefact of saying so, and is never
 * shown.
 */
export const ROOT_COLUMN = '__root__';

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
  /**
   * Rendered as a link when the value is an http(s) URL; the label is
   * this parameter of the URL, else the URL. DataCube's displayAsLink.
   */
  readonly linkLabelParameter?: string;
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
  /**
   * EVERY column the result carried, hidden ones included.
   *
   * What the grid shows and what the engine returned are different
   * questions, and one caller needs the second: the pivot's cast
   * describes the shape of the ANSWER. Reading it off the visible
   * leaves meant that unticking `2022__|__notional` in the columns
   * panel narrowed the next query's `cast(...)`, so the column left
   * the data as well as the screen -- and nothing could bring it
   * back, because the panel lists the cast.
   */
  readonly all: readonly LeafColumn[];
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
 * ends in one of them AND the prefix ends in `PIVOT_SEPARATOR`, that
 * measure becomes the last path segment and the rest of the prefix is
 * the pivot value path. The longest matching measure wins, so 'pnl'
 * cannot shadow 'pnl_net'; a shorter candidate is tried when the
 * longer one leaves a prefix that does not end in the separator.
 *
 * A name that ends in a measure without the separator is a plain
 * column that happens to share a suffix, and stays flat.
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
  // Longest first, but not longest-only: a longer measure that leaves
  // an unseparated prefix is the wrong anchor, and a shorter one may
  // be the right one.
  const candidates = measures
    .filter((m) => name === m || name.endsWith(m))
    .sort((a, b) => b.length - a.length);

  for (const matched of candidates) {
    if (name === matched) break;
    const prefix = name.slice(0, name.length - matched.length);
    // THE SEPARATOR IS REQUIRED. Without it this is not a column the
    // pivot generated, whatever its suffix looks like.
    if (!prefix.endsWith(PIVOT_SEPARATOR)) continue;
    const cleaned = prefix.slice(0, -PIVOT_SEPARATOR.length);
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
  /** Columns shown as links: name -> the URL parameter naming the label. */
  readonly links?: Readonly<Record<string, string>>;
  /**
   * Per pivot key, in `pivotOn` order: the direction its VALUES run
   * across the header. Horizontal Pivots > sort direction.
   */
  readonly pivotDirections?: readonly ('asc' | 'desc')[];
  /**
   * The pivot total columns' header and edge: upstream's
   * `pivotStatisticColumnName` and `pivotStatisticColumnPlacement`.
   */
  readonly pivotTotal?: {
    readonly label: string;
    readonly placement: 'left' | 'right';
  };
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

  const totalPrefix = `${PIVOT_TOTAL_KEY}${PIVOT_SEPARATOR}`;
  const leaves: LeafColumn[] = table.columns.map((c, index) => {
    const display = layout.displayNames?.[c.name];
    const path = c.name === TREE_COLUMN
      ? [treeLabel]
      // A pivot total spans ONE header level whatever the pivot's
      // depth: its name over its measure, the name cell reaching down.
      : c.name.startsWith(totalPrefix)
        ? [layout.pivotTotal?.label ?? 'Total', c.name.slice(totalPrefix.length)]
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
      // The grand total's synthetic key is machinery, not a column.
      || (l.name !== ROOT_COLUMN
        && !hidden.has(l.name)
        && !inTree.has(l.name)),
  );

  // Ordering is applied to the VISIBLE leaves; anything unlisted keeps
  // engine order behind the listed ones, so adding a measure does not
  // silently vanish from a saved view that predates it.
  const order = layout.order;
  /**
   * WHERE A LEAF SITS, given the order the cube declares.
   *
   * Three parts, because a pivoted cube has two kinds of column and
   * the answer's own order must not decide either.
   *
   * ITS MEASURE, not its name. `2021__|__notional` is a value
   * crossed with a measure, and the order a configuration holds is a
   * list of source columns; matching the whole leaf name found
   * nothing and fell back to the result's order.
   *
   * THE BLOCKS GO WHERE THE MEASURES WERE. Every pivoted leaf shares
   * one position -- the earliest measure the pivot replaced -- so the
   * spread measures occupy the seat `notional` and `pnl` used to
   * have, and a column that came after them still comes after. Put
   * plainly: a cube ending `..., quarter, notional, pnl, quantity,
   * settled` pivots to `..., quarter, <the year blocks>, quantity,
   * settled`, not to `..., quantity, settled, <the year blocks>`.
   *
   * AND EACH VALUE BLOCK STAYS WHOLE, in the order the values came
   * back in. Value-major, not measure-major: ordering primarily by
   * measure would give notional for every year and then pnl for
   * every year, which is a different table from the one a pivot is
   * for.
   *
   * The blocks are numbered among the pivoted leaves, though nothing
   * rests on that -- `block` is only consulted for pivoted leaves,
   * so numbering across all of them would shift every index by the
   * same one. What DID rest on appearance order was an earlier
   * version that used the block as the PRIMARY key for every leaf:
   * the plain columns' position then depended on where the engine
   * happened to put the pivot's output, which is the fault this
   * whole comparator exists to remove.
   */
  const pivoted = (l: LeafColumn): boolean => l.path.length > 1;
  const measureRank = (l: LeafColumn): number =>
    order ? order.indexOf(l.path[l.path.length - 1] ?? l.name) : -1;

  let home = -1;
  for (const l of visible) {
    if (!pivoted(l)) continue;
    const r = measureRank(l);
    if (r !== -1 && (home === -1 || r < home)) home = r;
  }

  const blockKey = (l: LeafColumn): string =>
    l.path.slice(0, -1).join('\u0000');
  const blocks = new Map<string, number>();
  for (const l of visible) {
    if (!pivoted(l)) continue;
    const key = blockKey(l);
    if (!blocks.has(key)) blocks.set(key, blocks.size);
  }

  const seat = (l: LeafColumn): number =>
    pivoted(l) ? home : measureRank(l);
  const block = (l: LeafColumn): number =>
    pivoted(l) ? (blocks.get(blockKey(l)) ?? 0) : -1;

  const ordered = order
    ? [...visible].sort((a, b) => {
        // The tree column is the row dimensions and belongs at the
        // left, whatever the order says -- it names none of the
        // source columns, so a rank lookup would send it to the end.
        if (a.name === TREE_COLUMN) return b.name === TREE_COLUMN ? 0 : -1;
        if (b.name === TREE_COLUMN) return 1;
        const sa = seat(a);
        const sb = seat(b);
        // A column the order does not mention keeps engine order,
        // behind everything it does mention: adding a measure must
        // not make it vanish from a saved view that predates it.
        if (sa === -1 && sb === -1) return a.index - b.index;
        if (sa === -1) return 1;
        if (sb === -1) return -1;
        if (sa !== sb) return sa - sb;
        const ba = block(a);
        const bb = block(b);
        if (ba !== bb) return ba - bb;
        const ma = measureRank(a);
        const mb = measureRank(b);
        if (ma === mb) return a.index - b.index;
        if (ma === -1) return 1;
        if (mb === -1) return -1;
        return ma - mb;
      })
    : visible;

  // THE PIVOT'S VALUES in the configured direction, per key. Upstream's
  // rule exactly (DataCubeGridConfigurationBuilder): a stable sort from
  // the LAST pivot key to the first, localeCompare, reversed for desc.
  // Only the pivoted leaves move, and only among the seats they already
  // hold, so each value block stays whole and the plain columns stay
  // where the order above put them.
  const directions = layout.pivotDirections ?? [];
  if (directions.length > 0) {
    const values = (l: LeafColumn): string[] => l.path.slice(0, -1);
    const seats = ordered.flatMap((l, i) => (pivoted(l) ? [i] : []));
    const moving = seats.map((i) => ordered[i] as LeafColumn);
    for (let k = directions.length - 1; k >= 0; k--) {
      const desc = directions[k] === 'desc';
      moving.sort((a, b) => {
        const va = values(a)[k] ?? '';
        const vb = values(b)[k] ?? '';
        return desc ? vb.localeCompare(va) : va.localeCompare(vb);
      });
    }
    const reseated = [...ordered];
    seats.forEach((seatAt, i) => { reseated[seatAt] = moving[i] as LeafColumn; });
    ordered.splice(0, ordered.length, ...reseated);
  }

  // THE PIVOT TOTAL at its edge of the pivot: before every value block
  // or after the last, keeping the measures in the order they had.
  const isTotal = (l: LeafColumn): boolean => l.name.startsWith(totalPrefix);
  if (ordered.some(isTotal)) {
    const seats = ordered.flatMap((l, i) => (pivoted(l) ? [i] : []));
    const inSeats = seats.map((i) => ordered[i] as LeafColumn);
    const totals = inSeats.filter(isTotal);
    const values = inSeats.filter((l) => !isTotal(l));
    const moving = layout.pivotTotal?.placement === 'left'
      ? [...totals, ...values]
      : [...values, ...totals];
    const reseated = [...ordered];
    seats.forEach((seatAt, i) => { reseated[seatAt] = moving[i] as LeafColumn; });
    ordered.splice(0, ordered.length, ...reseated);
  }

  const widths = layout.widths ?? {};
  const minWidths = layout.minWidths ?? {};
  const maxWidths = layout.maxWidths ?? {};
  const pinned = layout.pinned ?? {};
  const displayNames = layout.displayNames ?? {};
  const blurred = new Set(layout.blurred ?? []);
  const links = layout.links ?? {};

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
      ...(links[l.name] !== undefined
        ? { linkLabelParameter: links[l.name] }
        : {}),
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

  return { leaves: sized, all: leaves, headerRows, depth };
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

/**
 * What a "display as link" cell shows, or null for plain text.
 *
 * Upstream's LinkRenderer: a value that is a URL becomes a link that
 * opens in a new tab, labelled by the URL's own `labelParameter` query
 * parameter when it has one. Only http and https: a `javascript:` or
 * `data:` value in the data must never become something to click.
 */
export function linkFor(
  value: unknown,
  labelParameter: string,
): { href: string; label: string } | null {
  if (typeof value !== 'string') return null;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return null;
  return { href: value, label: url.searchParams.get(labelParameter) ?? value };
}
