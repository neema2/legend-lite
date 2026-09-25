// The configuration: one object for everything the editor edits.
//
// This existed already, four times over -- ColumnLayout held order,
// widths and pinning, GridOptions.formats held number formats,
// GridOptions.appearance and columnAppearance held colours and fonts,
// and the snapshot held kind, maxRows and treeColumnSort. Each was
// right for its consumer and wrong for an editor, which needs ONE
// draft to show, revert and apply atomically.
//
// So the four become projections of this, never the other way round.
// A column's settings live together under its name, which is what
// makes them survive the column being hidden, reordered, pivoted and
// brought back: a name is stable, a position is not.
//
// DataCube reached the same shape (DataCubeConfiguration with a list
// of DataCubeColumnConfiguration), and matching it is what lets a
// cube saved there open here without dropping a setting.

import type { ColumnFormat, NumberScale } from './format.ts';
import type { PinPlacement } from './grid/columns.ts';
import type {
  AggregateFn,
  ColumnKind,
  ColumnSpec,
  CubeSnapshot,
  PivotTotal,
  SortDirection,
} from './snapshot.ts';
import type { CellAppearance, GridAppearance, HeatmapSpec } from './style.ts';

/**
 * A change to some settings.
 *
 * Distinct from the settings themselves because
 * `exactOptionalPropertyTypes` makes "absent" and "explicitly
 * undefined" different types, and a patch needs the second: it is
 * how a control says CLEAR this rather than leave it alone. Applying
 * a patch prunes, so the cleared key leaves no trace -- which also
 * matters through JSON, where `{x: undefined}` writes `"x": null`
 * and a reader cannot tell that from a deliberate null.
 */
export type Patch<T> = { [K in keyof T]?: T[K] | undefined };

export type ColumnPatch = Patch<ColumnConfiguration>;

/** How a column's width is decided. */
export type WidthMode = 'any' | 'fixed' | 'range';

/**
 * Everything configurable about one column.
 *
 * Every field optional and absent-means-default, because a column the
 * user has never opened must serialise to nothing rather than to a
 * copy of the defaults -- otherwise a saved view records opinions
 * nobody held, and a later change to a default cannot reach the
 * cubes that predate it.
 */
export interface ColumnConfiguration {
  readonly displayName?: string;
  readonly kind?: ColumnKind;
  /** The aggregate applied when this column is a measure. */
  readonly aggregateFn?: AggregateFn;
  /** Extra arguments for aggregates that take them, e.g. wavg's weight. */
  readonly aggregationParameters?: readonly string[];

  readonly hidden?: boolean;
  readonly blurred?: boolean;
  readonly pinned?: PinPlacement;

  readonly widthMode?: WidthMode;
  readonly width?: number;
  readonly minWidth?: number;
  readonly maxWidth?: number;

  readonly format?: ColumnFormat;
  readonly appearance?: CellAppearance;
  readonly heatmap?: HeatmapSpec;

  readonly excludedFromPivot?: boolean;
  /** Direction this column's values take as pivot headers. */
  readonly pivotSortDirection?: SortDirection;
  /**
   * The aggregate this measure's PIVOT TOTAL uses -- upstream's
   * `pivotStatisticColumnFunction`. Absent: the measure's own.
   */
  readonly pivotStatisticColumnFunction?: AggregateFn;

  readonly displayAsLink?: boolean;
  /** Query parameter whose value labels the link instead of the URL. */
  readonly linkLabelParameter?: string;
}

export interface CubeConfiguration {
  readonly reportTitle?: string;

  // --- tree column ---
  readonly showRootAggregation: boolean;
  /**
   * Keep a row dimension as a DATA COLUMN as well as in the tree.
   *
   * OFF, which is what DataCube shows: it sets `groupDisplayType:
   * 'singleColumn'` and leaves ag-grid's `suppressRowGroupHidesColumns`
   * alone, so grouping a column hides it -- its values are the tree's
   * now. The query still returns it, because every column that is
   * not the group key of the level being fetched gets aggregated.
   *
   * It is an ag-grid OPTION rather than a law, and upstream exposes
   * no setting for it, so this is a small deliberate divergence: the
   * default matches theirs and the choice is available.
   */
  readonly showGroupedColumns: boolean;
  /**
   * Whether the row/column drag bar is on screen.
   *
   * ON, because a drop target you cannot see is a feature you cannot
   * find -- upstream always shows it. Off is for the person who has
   * finished arranging the cube and wants the rows: the bar is 34px
   * of a 900px viewport, and grouping stays reachable from the
   * right-click menu, from the columns panel, and from the bar
   * itself, which comes back for the length of a column drag.
   */
  readonly showDragZones: boolean;
  /**
   * Whether the title bar is on screen.
   *
   * ON. Off leaves a lip carrying the one control that brings it
   * back -- never nothing, because the hamburger lives in that bar
   * and a person who hid it would otherwise have lost the menu. The
   * grid's own right-click menu carries the same toggle, so there
   * are always two ways back.
   */
  readonly showTitleBar: boolean;
  readonly showLeafCount: boolean;
  readonly treeColumnSort: SortDirection;
  /**
   * Levels opened when the cube first loads. Absent means none.
   *
   * A number rather than a boolean because "expand everything" on a
   * deep hierarchy issues a query per branch per level, and a user
   * who wanted two levels should not pay for five.
   */
  readonly initialExpandToLevel?: number;

  // --- rows ---
  /**
   * General Properties > Row Limit. UNSET means no limit, as upstream
   * (`DataCubeSnapshot.limit` starts undefined). A tree still caps each
   * level at DEFAULT_MAX_ROWS without it -- that is a guard on how
   * many groups one query may return, not the user's limit.
   */
  readonly maxRows?: number;
  readonly showTruncationWarning: boolean;

  // --- appearance ---
  readonly appearance: GridAppearance;
  readonly showSelectionStats: boolean;

  /**
   * The pivot total column (upstream's pivot statistic column): its
   * header, and which edge of the pivot it sits on. Placement UNSET
   * means no total, which is how upstream reads a specification that
   * does not mention one; a new cube shows it on the right.
   */
  readonly pivotStatisticColumnName?: string;
  readonly pivotStatisticColumnPlacement?: 'left' | 'right';

  // No grid mode: "Dimensional" was offered and read by nothing
  // (census §2). It returns with Essbase mode, which gives it a meaning.

  /** Per column, by column name. */
  readonly columns: Readonly<Record<string, ColumnConfiguration>>;
  /** Display order, outermost first. Names not listed keep source order. */
  readonly columnOrder?: readonly string[];
}

/**
 * Fold a reordering of SOME columns into the order of all of them.
 *
 * The grid can only report the columns it is showing, and a cube
 * hides plenty: the row dimensions are the tree's, a pivot key is
 * spent on the header, an unticked column is off. Writing the grid's
 * report straight into `columnOrder` therefore dropped every one of
 * them out of the order -- and the columns panel, which sorts by
 * that order and puts anything unlisted last, sent them to the end
 * of the list. One drag and the grouped columns jumped.
 *
 * So the moved columns take the SLOTS they already occupy, in their
 * new relative order, and every other column keeps its place. A
 * column that is not in the order at all -- one dragged in from the
 * panel -- is placed beside whichever of its new neighbours is.
 */
export function mergeColumnOrder(
  full: readonly string[],
  moved: readonly string[],
): string[] {
  const base = [...full];
  for (const [i, name] of moved.entries()) {
    if (base.includes(name)) continue;
    // Its neighbours in the NEW order say where it belongs: after the
    // nearest one before it, or before the nearest one after.
    const before = moved.slice(0, i).reverse()
      .find((n) => base.includes(n));
    const after = moved.slice(i + 1).find((n) => base.includes(n));
    const at = before !== undefined
      ? base.indexOf(before) + 1
      : after !== undefined
        ? base.indexOf(after)
        : base.length;
    base.splice(at, 0, name);
  }
  const slots = new Set(moved);
  const queue = moved.filter((n) => base.includes(n));
  let next = 0;
  return base.map((n) => (slots.has(n) ? (queue[next++] ?? n) : n));
}

/** DataCube's own default row cap. */
export const DEFAULT_MAX_ROWS = 1000;

/**
 * DataCube's own defaults, taken from its DataCubeQueryEngine
 * constants rather than chosen here.
 *
 * The colours matter more than they look. A cube that renders
 * negatives in the same colour as positives is readable but wrong
 * at a glance, and every one of these had been left unset -- so the
 * value-state colouring machinery existed and did nothing by
 * default. Their set:
 *
 *   foreground        black          DEFAULT_FOREGROUND_COLOR
 *   negative          red-500        DEFAULT_NEGATIVE_FOREGROUND_COLOR
 *   zero              neutral-400    DEFAULT_ZERO_FOREGROUND_COLOR
 *   error             blue-600       DEFAULT_ERROR_FOREGROUND_COLOR
 *   alternate row     #d7e0eb        DEFAULT_ROW_HIGHLIGHT_BACKGROUND_COLOR
 *   grid line         neutral-300    DEFAULT_GRID_LINE_COLOR
 *
 * Note the grid LINE colour (neutral-300) is a shade darker than
 * the grid's structural --ag-border-color (neutral-200): the lines
 * a user can switch on are meant to read more strongly than the
 * frame.
 *
 * The font is Roboto at 11px -- which is NOT the grid's 12px
 * --ag-font-size. The cell font comes from the column's own font
 * configuration and is a point smaller than the chrome around it.
 */
export const DEFAULT_CONFIGURATION: CubeConfiguration = {
  /*
   * OFF, as theirs is (DataCubeConfiguration.showRootAggregation =
   * false). This was set to true here by assumption rather than by
   * reading their source -- the same mistake caught for
   * alternateRows, made twice. A grand total is a query of its own
   * at every refresh, and a cube that opens with one has decided
   * for the user that they wanted it.
   */
  showRootAggregation: false,
  showGroupedColumns: false,
  showDragZones: true,
  showTitleBar: true,
  showLeafCount: false,
  treeColumnSort: 'asc',
  showTruncationWarning: true,
  showSelectionStats: false,
  // Upstream leaves this unset, and renders no total whatever it says
  // -- a bug by the user's ruling (2026-09-25). A pivot reads across a
  // row, and the first question about a row is its total.
  pivotStatisticColumnName: 'Total',
  pivotStatisticColumnPlacement: 'right',
  appearance: {
    showHorizontalGridLines: false,
    showVerticalGridLines: true,
    gridLineColor: '#d4d4d4',
    /* DataCube's own default here is OFF; this is a deliberate
       departure, asked for directly. On a dense 20px grid the
       banding is what lets the eye track a row across a wide pivot,
       which is worth more than matching the default exactly. */
    alternateRows: true,
    alternateRowsCount: 1,
    alternateRowsColor: '#d7e0eb',
    fontFamily: 'Roboto, ui-sans-serif, system-ui, sans-serif',
    fontSize: 11,
    textAlign: 'left',
    normalForeground: '#000000',
    negativeForeground: '#ef4444',
    zeroForeground: '#a3a3a3',
    errorForeground: '#2563eb',
  },
  columns: {},
};

/** Their DEFAULT_COLUMN_WIDTH / MIN_WIDTH, for a column with none. */
export const DEFAULT_COLUMN_WIDTH = 300;
export const DEFAULT_COLUMN_MIN_WIDTH = 50;

/** The settings for one column, or the empty set. */
export function columnConfig(
  config: CubeConfiguration,
  name: string,
): ColumnConfiguration {
  return config.columns[name] ?? {};
}

/**
 * Replace one column's settings, leaving the rest untouched.
 *
 * Returns the SAME configuration when nothing changed, so a caller
 * can skip a refresh rather than re-running an identical query --
 * the same identity contract `drillTo` keeps.
 */
export function withColumn(
  config: CubeConfiguration,
  name: string,
  patch: ColumnPatch,
): CubeConfiguration {
  const current = columnConfig(config, name);
  const next = prune<ColumnConfiguration>({ ...current, ...patch });
  if (sameColumn(current, next)) return config;
  const columns = { ...config.columns };
  if (Object.keys(next).length === 0) {
    delete columns[name];
  } else {
    columns[name] = next;
  }
  return { ...config, columns };
}

/** Drop keys set to undefined. See `Patch`. */
export function prune<T extends object>(value: Patch<T>): T {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value)) {
    if (v !== undefined) out[k] = v;
  }
  return out as T;
}

/**
 * Apply a patch to the cube-wide settings.
 *
 * Goes through `prune` for the same reason `withColumn` does: a
 * cleared report title must vanish rather than become an explicit
 * null in the saved view.
 */
export function withSettings(
  config: CubeConfiguration,
  patch: Patch<CubeConfiguration>,
): CubeConfiguration {
  return prune<CubeConfiguration>({ ...config, ...patch });
}

/** Apply a patch to the grid-wide appearance. */
export function withAppearance(
  config: CubeConfiguration,
  patch: Patch<GridAppearance>,
): CubeConfiguration {
  return {
    ...config,
    appearance: prune<GridAppearance>({ ...config.appearance, ...patch }),
  };
}

function sameColumn(a: ColumnConfiguration, b: ColumnConfiguration): boolean {
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  return ak.every(
    (k) =>
      JSON.stringify((a as Record<string, unknown>)[k]) ===
      JSON.stringify((b as Record<string, unknown>)[k]),
  );
}

// --------------------------------------------------------------------
// Projections. Derived, never authoritative.
// --------------------------------------------------------------------

/**
 * The width a column resolves to, given its mode.
 *
 * The mode is explicit rather than inferred from which of width /
 * minWidth / maxWidth are set, because "fixed at 120" and "in range
 * 120-120" are the same numbers and different intentions, and a user
 * switching back to (Any) must not lose the bounds they typed.
 */
export function resolvedWidths(c: ColumnConfiguration): {
  width?: number;
  minWidth?: number;
  maxWidth?: number;
} {
  const mode = c.widthMode ?? (c.width !== undefined ? 'fixed' : 'any');
  if (mode === 'fixed') {
    return c.width !== undefined ? { width: c.width } : {};
  }
  if (mode === 'range') {
    const out: { minWidth?: number; maxWidth?: number } = {};
    if (c.minWidth !== undefined) out.minWidth = c.minWidth;
    if (c.maxWidth !== undefined) out.maxWidth = c.maxWidth;
    return out;
  }
  return {};
}

export interface ColumnLayoutProjection {
  order?: readonly string[];
  hidden?: readonly string[];
  widths?: Record<string, number>;
  minWidths?: Record<string, number>;
  maxWidths?: Record<string, number>;
  pinned?: Record<string, PinPlacement>;
  displayNames?: Record<string, string>;
  blurred?: readonly string[];
  /** Columns shown as links: name -> the URL parameter naming the label. */
  links?: Record<string, string>;
  /** Keep a row dimension as a data column as well as in the tree. */
  keepGrouped?: boolean;
  /** The pivot total columns' header and edge. */
  pivotTotal?: { label: string; placement: 'left' | 'right' };
  /** Columns whose width is fixed, so not drag-resizable. */
  fixed?: readonly string[];
}

/**
 * The URL parameter a link's label is read from when a column does not
 * name one: upstream's DEFAULT_URL_LABEL_QUERY_PARAM.
 */
export const DEFAULT_LINK_LABEL_PARAMETER = 'dataCube.linkLabel';

/** The `ColumnLayout` the grid's column model wants. */
export function toColumnLayout(
  config: CubeConfiguration,
): ColumnLayoutProjection {
  const hidden: string[] = [];
  const blurred: string[] = [];
  const fixed: string[] = [];
  const widths: Record<string, number> = {};
  const minWidths: Record<string, number> = {};
  const maxWidths: Record<string, number> = {};
  const pinned: Record<string, PinPlacement> = {};
  const displayNames: Record<string, string> = {};
  const links: Record<string, string> = {};
  const keepGrouped = config.showGroupedColumns;

  for (const [name, c] of Object.entries(config.columns)) {
    if (c.hidden) hidden.push(name);
    if (c.blurred) blurred.push(name);
    // Upstream's DEFAULT_URL_LABEL_QUERY_PARAM when none is given.
    if (c.displayAsLink) {
      links[name] = c.linkLabelParameter ?? DEFAULT_LINK_LABEL_PARAMETER;
    }
    if (c.pinned) pinned[name] = c.pinned;
    if (c.widthMode === 'fixed') fixed.push(name);
    if (c.displayName !== undefined) displayNames[name] = c.displayName;
    const w = resolvedWidths(c);
    if (w.width !== undefined) widths[name] = w.width;
    if (w.minWidth !== undefined) minWidths[name] = w.minWidth;
    if (w.maxWidth !== undefined) maxWidths[name] = w.maxWidth;
  }

  const out: ColumnLayoutProjection = {};
  if (keepGrouped) out.keepGrouped = true;
  if (config.columnOrder) out.order = config.columnOrder;
  if (hidden.length) out.hidden = hidden;
  if (blurred.length) out.blurred = blurred;
  if (Object.keys(links).length) out.links = links;
  if (Object.keys(widths).length) out.widths = widths;
  if (Object.keys(minWidths).length) out.minWidths = minWidths;
  if (Object.keys(maxWidths).length) out.maxWidths = maxWidths;
  if (Object.keys(pinned).length) out.pinned = pinned;
  if (fixed.length) out.fixed = fixed;
  if (Object.keys(displayNames).length) out.displayNames = displayNames;
  if (config.pivotStatisticColumnPlacement !== undefined) {
    out.pivotTotal = {
      label: config.pivotStatisticColumnName ?? 'Total',
      placement: config.pivotStatisticColumnPlacement,
    };
  }
  return out;
}

/** Per-column number/date formats, for the grid. */
export function toFormats(
  config: CubeConfiguration,
): Record<string, ColumnFormat> {
  const out: Record<string, ColumnFormat> = {};
  for (const [name, c] of Object.entries(config.columns)) {
    if (c.format) out[name] = c.format;
  }
  return out;
}

/** Per-column fonts and colours, merged over the grid's by the grid. */
export function toColumnAppearance(
  config: CubeConfiguration,
): Record<string, CellAppearance> {
  const out: Record<string, CellAppearance> = {};
  for (const [name, c] of Object.entries(config.columns)) {
    if (c.appearance) out[name] = c.appearance;
  }
  return out;
}

/**
 * Fold the configuration's query-shaping settings into a snapshot.
 *
 * Only the settings that change the SQL: kind, exclusion from the
 * pivot, the row cap and the tree sort. Colours cannot reach this
 * function, and that separation is the point -- restyling a cube must
 * never re-run a query.
 *
 * The epoch is NOT bumped here. A caller that means to refetch bumps
 * it; one that is only reconciling does not.
 */
export function applyToSnapshot(
  snapshot: CubeSnapshot,
  config: CubeConfiguration,
): CubeSnapshot {
  const columns: ColumnSpec[] = snapshot.columns.map((spec) => {
    const c = columnConfig(config, spec.name);
    const next: ColumnSpec = {
      ...withoutAggregate(spec),
      ...(c.kind !== undefined ? { kind: c.kind } : {}),
      ...(c.excludedFromPivot !== undefined
        ? { excludedFromPivot: c.excludedFromPivot }
        : {}),
      ...aggregateOf(c),
    };
    return next;
  });
  // A calculated column aggregates like any other: the same Column
  // Properties setting, carried onto the column the query reads.
  const derived = snapshot.derived.map((d) => ({
    ...withoutAggregate(d),
    ...aggregateOf(columnConfig(config, d.name)),
  }));
  const { maxRows: _previousLimit, pivotTotal: _previousTotal, ...unlimited }
    = snapshot;
  return {
    ...unlimited,
    ...(config.maxRows !== undefined ? { maxRows: config.maxRows } : {}),
    ...(config.showGroupedColumns ? { keepGroupedColumns: true } : {}),
    // Clear it as well as set it, so unticking the box takes the count
    // back out of the query.
    ...(config.showLeafCount ? { leafCount: true } : { leafCount: false }),
    ...pivotTotalOf(config),
    columns,
    derived,
    treeColumnSort: config.treeColumnSort,
  };
}

/**
 * Read a snapshot's query-shaping settings back into a configuration.
 *
 * The inverse of `applyToSnapshot`, and it has to exist: a cube can
 * arrive as a snapshot alone -- from a saved view, a colleague's
 * link, or a `pivotOn` set by dragging a header -- and the editor
 * must open showing what is actually running rather than the
 * defaults.
 */
export function fromSnapshot(
  snapshot: CubeSnapshot,
  base: CubeConfiguration = DEFAULT_CONFIGURATION,
): CubeConfiguration {
  const limit = snapshot.maxRows ?? base.maxRows;
  let config: CubeConfiguration = {
    ...base,
    ...(limit !== undefined ? { maxRows: limit } : {}),
    treeColumnSort: snapshot.treeColumnSort ?? base.treeColumnSort,
  };
  // The pivot total, when the snapshot carries one. A snapshot without
  // it leaves the base's setting alone rather than switching it off:
  // most snapshots predate the field.
  const total = snapshot.pivotTotal;
  if (total) {
    config = { ...config, pivotStatisticColumnPlacement: total.placement };
    for (const [name, fn] of Object.entries(total.functions ?? {})) {
      config = withColumn(config, name, { pivotStatisticColumnFunction: fn });
    }
  }
  for (const spec of snapshot.columns) {
    const patch: ColumnConfiguration = {
      ...(spec.kind !== undefined ? { kind: spec.kind } : {}),
      ...(spec.excludedFromPivot !== undefined
        ? { excludedFromPivot: spec.excludedFromPivot }
        : {}),
      ...aggregateBack(spec),
    };
    if (Object.keys(patch).length > 0) {
      config = withColumn(config, spec.name, patch);
    }
  }
  for (const d of snapshot.derived) {
    const patch = aggregateBack(d);
    if (Object.keys(patch).length > 0) config = withColumn(config, d.name, patch);
  }
  return config;
}

/**
 * The pivot total, as the query reads it: where it sits and, per
 * measure column, the aggregate its total takes.
 */
function pivotTotalOf(config: CubeConfiguration): { pivotTotal?: PivotTotal } {
  const placement = config.pivotStatisticColumnPlacement;
  if (placement === undefined) return {};
  const functions: Record<string, AggregateFn> = {};
  for (const [name, c] of Object.entries(config.columns)) {
    if (c.pivotStatisticColumnFunction !== undefined) {
      functions[name] = c.pivotStatisticColumnFunction;
    }
  }
  return {
    pivotTotal: {
      placement,
      ...(Object.keys(functions).length > 0 ? { functions } : {}),
    },
  };
}

/**
 * Column Properties > Aggregation, as the query reads it.
 *
 * The weight rides only with `wavg`, the one aggregate that takes a
 * parameter; kept on any other it would be a setting the query
 * silently ignores.
 */
function aggregateOf(
  c: ColumnConfiguration,
): { aggregate?: AggregateFn; aggregateWeight?: string } {
  if (c.aggregateFn === undefined) return {};
  const weight = c.aggregationParameters?.[0];
  return {
    aggregate: c.aggregateFn,
    ...(c.aggregateFn === 'wavg' && weight !== undefined
      ? { aggregateWeight: weight }
      : {}),
  };
}

/** The inverse of `aggregateOf`, for `fromSnapshot`. */
function aggregateBack(
  c: { aggregate?: AggregateFn; aggregateWeight?: string },
): ColumnConfiguration {
  if (c.aggregate === undefined) return {};
  return {
    aggregateFn: c.aggregate,
    ...(c.aggregateWeight !== undefined
      ? { aggregationParameters: [c.aggregateWeight] }
      : {}),
  };
}

/**
 * A column with its aggregate taken OFF, so the configuration is the
 * only source of it: clearing Aggregation in the panel must clear it
 * on the snapshot too, not leave the last choice behind.
 */
function withoutAggregate<T extends { aggregate?: AggregateFn;
  aggregateWeight?: string }>(c: T): T {
  const { aggregate: _a, aggregateWeight: _w, ...rest } = c;
  return rest as T;
}

/** The header text for a column: its display name, or its name. */
export function labelFor(config: CubeConfiguration, name: string): string {
  return columnConfig(config, name).displayName ?? name;
}

/**
 * The scale suffix a user picked, for the Column Properties preview.
 *
 * Here rather than in format.ts because it is a label for a control,
 * not a formatting rule.
 */
export const SCALE_LABELS: Readonly<Record<NumberScale, string>> = {
  auto: 'Auto',
  basisPoints: 'Basis Points (bp)',
  percent: 'Percent (%)',
  thousands: 'Thousands (k)',
  millions: 'Millions (m)',
  billions: 'Billions (b)',
  trillions: 'Trillions (t)',
};

/**
 * A column's settings, moved to its new name: its entry in `columns`
 * and its place in `columnOrder`. Upstream's `updateColumn` does the
 * same for a renamed calculated column.
 */
export function renameColumnConfig(
  config: CubeConfiguration,
  from: string,
  to: string,
): CubeConfiguration {
  const { [from]: moved, ...others } = config.columns;
  return {
    ...config,
    columns: moved === undefined ? config.columns : { ...others, [to]: moved },
    ...(config.columnOrder
      ? { columnOrder: config.columnOrder.map((n) => (n === from ? to : n)) }
      : {}),
  };
}
