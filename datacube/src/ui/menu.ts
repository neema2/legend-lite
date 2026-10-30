// The grid's context menu.
//
// DataCube's has 66 entries, and the thing that makes that tractable
// is that almost all of them are the same few verbs applied to
// whatever was right-clicked. So the menu is DATA -- built from the
// context, tested as data -- and the renderer is thin. A menu built
// by conditional DOM is a menu nobody can test.
//
// It NESTS. Reading their DataCubeGridMenuBuilder properly rather
// than grepping its labels turned up three levels: Export, Copy,
// Sort, Filter, Pivot, Resize, Pin and Heatmap are all submenus,
// and Filter's own "More Filters on X..." is a submenu inside a
// submenu. A flat list of the same entries is a different product:
// sixteen top-level items scan as a wall, where eight verbs with
// their variants tucked underneath scan as a sentence.
//
// The rules the shape enforces:
//
//  - An inapplicable action is DISABLED, not omitted. This reverses
//    an earlier decision here, and DataCube is right: a menu whose
//    entries move depending on context cannot be learned, and the
//    whole value of a context menu is that the third time you use
//    it you no longer read it. "Remove Vertical Pivot", greyed,
//    still says the column is not pivoted.
//  - Every entry names its target. "Vertical Pivot on desk" rather
//    than "Vertical Pivot", because a context menu is read after the
//    right-click, when which column was clicked is already fading.
//  - The FILTER entries know the value that was clicked. Right-click
//    a cell reading EMEA and the menu offers "Add Filter: region =
//    EMEA" directly, with every other operator for that column's
//    type one level down. This is the single most useful thing in
//    their menu and it is the part a labels-only reading misses.

import type { MemberPath } from '../adhoc/state.ts';
import type {
  CubeSnapshot,
  FilterOperator,
  FilterValue,
  RelatedPath,
  SortDirection,
} from '../snapshot.ts';
import { className, relatedLabel, type RelatedOffer } from '../class-tree.ts';
import type { CalcStage } from '../calc.ts';
import { GRAIN_LABELS, GRID_SHOW_AS_KINDS, GRID_SHOW_AS_LABELS, type GridShowAsKind } from '../calc-presets.ts';
import { TIME_GRAINS, type TimeGrain } from '../chart-spec.ts';
import { DATE_RANGES, DATE_RANGE_LABELS, type DateRange } from '../date-ranges.ts';
import { isNumeric, isTemporal, isTimeOfDay } from '../../../engine-client/src/types.ts';
import { PIVOT_TOTAL_KEY, isJsonValue, isRelativeDate, relativeDateWords, rowColumns } from '../snapshot.ts';
import { PIVOT_SEPARATOR } from '../grid/columns.ts';
import { TREE_COLUMN } from '../treeview.ts';
import { takesOperator } from '../offers.ts';
import { gestureTakes, isGridGesture, type GestureArgs, type GridGestureName } from '../grid-gestures.ts';
import { COLOR_SCALES, ICON_SET_LABELS, ICON_SET_NAMES, RULE_PRESETS, type FormatRule, type IconSetName } from '../conditional.ts';
import type { HeatmapSpec } from '../style.ts';

/**
 * A pivoted column's name is a PATH -- `2021__|__notional` -- and a
 * menu that shows it raw asks the user to read the separator. It
 * reads back as "2021 / notional", the same presentation the sorts
 * panel uses; a pivot total's as its header names it, `total`
 * (Properties' Pivot Statistic Column Name, Total unless renamed).
 */
export function columnLabel(name: string, total?: string): string {
  return name.split(PIVOT_SEPARATOR).map((part) => (part === PIVOT_TOTAL_KEY ? total ?? 'Total' : part)).join(' / ');
}

export interface MenuContext {
  readonly snapshot: CubeSnapshot;
  /** The column under the pointer, if any. */
  readonly column?: string;
  /** A flow's box (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL9): New Box After offers a step made in a box of its own. */
  readonly stepAfter?: boolean;
  /** Whether that column is a row dimension already. */
  readonly isRowDimension?: boolean;
  /** Whether a selection exists, which gates the copy entries. */
  readonly hasSelection?: boolean;
  /** Whether the grid has a step to undo, and one to redo: Edit ▸ Undo and Redo, disabled without. */
  readonly canUndo?: boolean;
  readonly canRedo?: boolean;
  /** Whether any group is currently open, which gates Collapse All. */
  readonly hasExpanded?: boolean;
  /** Whether the column under the pointer already has a heatmap. */
  readonly hasHeatmap?: boolean;
  /** The column under the pointer is one of the grid's rows' columns, whose values Group Values gathers. */
  readonly groupable?: boolean;
  /** The column under the pointer gathers another's values into groups (snapshot.ts ValueGroups): Edit Groups. */
  readonly groupsColumn?: boolean;
  /** The column under the pointer's conditional formats (Conditional Formatting ▸). */
  readonly conditional?: ConditionalFacts;
  /** What a pivot total's header is named (Properties' Pivot Statistic Column Name), when it is renamed. */
  readonly totalLabel?: string;
  /**
   * Whether the column may be grouped or pivoted by at all.
   *
   * A pivoted measure cannot: "Vertical Pivot on 2021 / notional"
   * is an action with no meaning, and offering it is worse than
   * omitting it -- the menu already omits actions that cannot
   * apply rather than disabling them.
   */
  readonly canGroup?: boolean;
  /**
   * Whether the host can send mail.
   *
   * A browser cannot attach a file to a mailto: link, so emailing an
   * export is something only the embedding application can do. The
   * entries are shown DISABLED when it cannot, rather than hidden:
   * "this build cannot email" is information, and a menu that
   * silently lacks an action a colleague's build has is confusing.
   */
  readonly canEmail?: boolean;
  /** Whether the host knows where the cube's rows come from, so the cube can be written down. */
  readonly canSaveCube?: boolean;
  /** A grid over a class: the related columns one hop away it has not yet (class-tree.ts relatedOffers). */
  readonly related?: readonly RelatedOffer[];
  /** The column under the pointer is a related column: the class tree's, never a formula to edit. */
  readonly relatedColumn?: boolean;
  /** A grid over a class: the model's measures of it the grid has not yet (model-measures.ts measuresOf). */
  readonly modelMeasures?: readonly { readonly name: string; readonly path: string; readonly doc?: string }[];
  /** From a header: Properties... opens Column Properties on this. */
  readonly propertiesColumn?: string;
  /** The column pivot's headers are measure-first (a checked entry). */
  readonly measuresFirst?: boolean;
  /** Where the column is pinned, if it is: the Pin entries say so. */
  readonly pinned?: 'left' | 'right';
  /**
   * The value in the cell that was right-clicked, if any.
   *
   * This is what turns the Filter submenu from a door to a dialog
   * into a one-click action, and it is the part a labels-only
   * reading of their menu misses entirely. Absent when the menu
   * came from a header, where there is no value to filter on;
   * null means the cell was blank.
   */
  readonly value?: FilterValue | null;
  /** The type of the column that value belongs to. */
  readonly columnType?: string;
  /**
   * Whether a new calculated column can be seeded from this one:
   * true for a column that exists before aggregation, source or
   * calculated. A pivot's generated column or a group-stage one is
   * not something a row-stage expression can see.
   */
  readonly extendable?: boolean;
  /** The stage, when the column is itself a calculated one. */
  readonly calcStage?: CalcStage;
  /** Set on a pivot result column: the measure it came from. */
  readonly pivotBase?: string;
  /** The column (or, on a pivot result, its measure) is a measure. */
  readonly isMeasure?: boolean;
  /** The column is a measure kept OUT of the horizontal pivot. */
  readonly excludedFromPivot?: boolean;
  /** The column has a fixed width, which Minimize must not override. */
  readonly fixedWidth?: boolean;
  /** The cube's controls are hidden (the grid alone): the last entry brings them back. */
  readonly controlsHidden?: boolean;
  /**
   * A grid on a page of sheets: View ▸ Move to Sheet's entries (each other sheet, then a new one), or `locked` -- the
   * page locked, the entry offered disabled. Absent off such a page.
   */
  readonly sheets?: readonly MenuItem[] | 'locked';
  /** A grid on a page of its own: View ▸ Remove from Page. */
  readonly canRemove?: boolean;
  /**
   * The host's Join ▸ on the column clicked (the user, 2026-10-23): a quick join to each grid sharing a column, the
   * other ways, the window, a data source not on the page yet -- or its Join… disabled, saying why. Absent (a cube
   * on no page): Join ▸ is still there, its Join… disabled, saying a join is made on a page.
   */
  readonly join?: readonly MenuItem[];
  /** The host's own Create ▸ entries (Class…, Association…, Data Space…), after the grid's. */
  readonly create?: readonly MenuItem[];
  /** Whether a data source can be opened beside the grid: Create ▸ Data Source…. */
  readonly canOpenSource?: boolean;
}

/**
 * CREATE ▸, the same in every menu -- the right-click, a grid's own menu, the title bar's (the user, 2026-10-23: "Insert"
 * became "Create", one word for all three): a grid over another source, a copy of this one, a chart of it; then, a line
 * above them, the host's (Class…, Association…, Data Space…).
 */
export function createItems(o: { readonly source: boolean; readonly host: readonly MenuItem[]; readonly disabled?: boolean }): MenuItem[] {
  const off = o.disabled === true ? { disabled: true } : {};
  return [
    ...(o.source ? [{ id: 'source.new' as const, label: 'Data Source\u2026', ...off }] : []),
    { id: 'grid.new', label: 'Copy of This Grid', ...off },
    { id: 'chart.plot', label: 'Visualization', ...off },
    // a timeseries tile (ui/timeseries-view.ts): its series along the grid's dates, gs-quant's functions over them
    { id: 'timeseries.new', label: 'Timeseries', ...off },
    // a text tile (page-text.ts): a page's title, a note beside a chart
    { id: 'text.new', label: 'Text Box', ...off },
    ...o.host.map((h, i) => (i === 0 ? { ...h, separated: true } : h)),
  ];
}

/**
 * Whether a column is a calculated one, and at which stage.
 *
 * Spread into the menu context, so an ordinary column adds nothing.
 */
export function calcStageOf(
  s: CubeSnapshot,
  column: string,
): { calcStage?: CalcStage } {
  if (s.derived.some((d) => d.name === column)) return { calcStage: 'row' };
  if ((s.groupDerived ?? []).some((d) => d.name === column)) {
    return { calcStage: 'group' };
  }
  return {};
}

export type MenuActionId =
  /**
   * A HOST's own entry, which must be prefixed.
   *
   * The cube's ids stay a closed union, so the dispatch in app.ts -- a
   * grid's gesture by its id (menuGesture), the rest by their cases --
   * covers them, and `menu-ids.test.ts` can still tell a dead entry
   * from a live one.
   * A host adds items to the title bar menu and gets them back
   * through `onHostMenu`; the prefix is what keeps the two sets from
   * ever being confused for one another.
   */
  | `host.${string}`
  /** A tile's own entries (a chart's right-click menu, page/cube-page.ts). */
  | `tile.${string}`
  /** A page's sheets (page/page-app.ts, ui/sheet-tabs.ts): a tab's own entries, and a tile's Move to Sheet (`sheet.to.<id>`). */
  | `sheet.${string}`
  | 'sort.asc'
  | 'sort.desc'
  | 'sort.addAsc'
  | 'sort.addDesc'
  | 'sort.clearColumn'
  | 'sort.clearAll'
  | 'filter.add'
  | 'filter.column'
  | 'filter.clearAll'
  | 'pivot.vertical'
  | 'pivot.addVertical'
  | 'pivot.removeVertical'
  | 'pivot.clearVertical'
  | 'pivot.horizontal'
  | 'pivot.addHorizontal'
  | 'pivot.removeHorizontal'
  | 'pivot.clearHorizontal'
  | 'pivot.measuresFirst'
  | 'column.hide'
  | 'column.autoSize'
  | 'column.autoSizeAll'
  | 'column.pinLeft'
  | 'column.pinRight'
  | 'column.unpin'
  | 'column.unpinAll'
  | 'tree.collapseAll'
  | 'tree.expandAll'
  | 'copy.selection'
  | 'copy.column'
  | 'export.csv'
  | 'export.excel'
  | 'export.html'
  | 'export.text'
  | 'export.pdf'
  | 'export.specification'
  | 'email.html'
  | 'email.excel'
  | 'email.csv'
  | 'email.text'
  | 'email.pdf'
  | 'heatmap.add'
  | 'heatmap.remove'
  | 'format.dataBar'
  | 'format.colorScale'
  | 'format.iconSet'
  | 'format.addRule'
  | 'format.clear'
  | 'chart.plot'
  | 'timeseries.new'
  | 'text.new'
  // a parameter of the page (page-parameters.ts): the parameters bar's Add form
  | 'parameter.new'
  | 'grid.new'
  | 'source.new'
  | 'page.blank'
  // the page's layouts (ui/layout-picker.ts), and edit or view mode (nothing moves)
  | 'page.arrange'
  // a grid on a page of its own, taken off it from its right-click's Tile ▸
  | 'grid.remove'
  // a page of its own as a file (page/page-app.ts)
  | 'page.export'
  | 'page.undoLayout'
  | 'page.redoLayout'
  | 'page.undo'
  | 'page.redo'
  | 'page.editLayout'
  // the page's bar folded or shown (its View ▸ Hide the Bar)
  | 'page.bar'
  | 'view.properties'
  | 'view.shortcuts'
  // a grid's plane, from its tile's state (app.ts #movePlane): live again, or a copy snapped (afresh)
  | 'plane.live'
  | 'plane.snap'
  | 'filter.valueList'
  | 'filter.dateRange'
  | 'filter.topN'
  | 'filter.topNOff'
  // the cube's controls hidden or shown, the grid alone (setControlsHidden)
  | 'view.controls'
  // Upstream's Copy > Selected Rows, Resize > Minimize and Size to Fit,
  // and Pivot > Exclude / Include.
  | 'copy.rows'
  | 'column.minimize'
  | 'column.minimizeAll'
  | 'grid.sizeToFit'
  | 'pivot.exclude'
  | 'pivot.include'
  // Upstream's Extended Columns submenu.
  | 'calc.add'
  | 'calc.extend'
  // the chart's presets, in the grid (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2): a date's period, a number's
  // bins, a measure shown as something computed from it
  | 'calc.grain'
  | 'calc.bins'
  | 'calc.showAs'
  | 'calc.related'
  | 'calc.groupValues'
  | 'calc.editGroups'
  | 'calc.modelMeasure'
  | 'calc.edit'
  | 'calc.delete'
  // The chrome, toggled from the title bar's menu. Not a snapshot
  // change and not a column operation: what is on SCREEN. It left the
  // grid's menu by the user's direction (2026-09-25); a folded title
  // bar leaves a lip that restores it.
  // The grid's history, in the title bar's menu and the right-click's Edit ▸, and the
  // title bar's Settings.
  | 'view.undo'
  | 'view.redo'
  | 'view.settings'
  | 'adhoc.zoomNext'
  | 'adhoc.zoomAll'
  | 'adhoc.zoomBottom'
  | 'adhoc.zoomOut'
  | 'adhoc.keepOnly'
  | 'adhoc.removeOnly'
  | 'adhoc.pivot'
  | 'adhoc.pivotToPov'
  | 'adhoc.povToRows'
  | 'adhoc.povToColumns'
  | 'adhoc.members'
  | 'adhoc.refresh'
  | 'adhoc.undo'
  | 'adhoc.redo'
  | 'adhoc.options'
  | 'adhoc.exit';

export interface MenuItem {
  /** Absent on a pure submenu parent, which does nothing itself. */
  readonly id?: MenuActionId;
  readonly label: string;
  /**
   * A host's entry: where it goes -- the main menu's file group (the default), its View, its
   * top-level data entries, its Create ▸ (Class…, Association…, Data Space…), or the status bar's planner readout
   * (`plane`: where the planner runs).
   */
  readonly section?: 'file' | 'view' | 'data' | 'create' | 'plane';
  /** Column the action applies to, when it is column-specific. */
  readonly column?: string;
  readonly direction?: SortDirection;
  /** For a value-aware filter entry. */
  readonly operator?: FilterOperator;
  readonly value?: FilterValue;
  /**
   * Shown but not actionable. DataCube disables rather than omits,
   * so the menu keeps its shape and can be learned.
   */
  readonly disabled?: boolean;
  /** A state the entry reports: upstream's check mark (Pin Left / Right). */
  readonly checked?: boolean;
  /** Ad Hoc Analysis mode: the member (or dimension) the entry acts on. */
  readonly adhoc?: {
    readonly dimension: string;
    readonly member?: MemberPath;
    /** Members selected with it, for Keep Only / Remove Only. */
    readonly selected?: readonly MemberPath[];
  };
  readonly submenu?: readonly MenuItem[];
  /** A preset's choice: the period a date is taken to (`calc.grain`). */
  readonly grain?: TimeGrain;
  /** A preset's choice: what a measure is shown as (`calc.showAs`). */
  readonly showAs?: GridShowAsKind;
  /** A related class's property to add (`calc.related`: class-tree.ts relatedOffers). */
  readonly related?: RelatedPath;
  /** A model measure to add, by its path (`calc.modelMeasure`: model-measures.ts). */
  readonly measure?: string;
  /** A date range relative to today (Filter ▸ Date Range ▸, date-ranges.ts). */
  readonly range?: DateRange;
  /** A top N's N, and whether it is the bottom's (Filter ▸ Top 10). */
  readonly n?: number;
  readonly bottom?: boolean;
  /** Conditional Formatting ▸'s choices: data bars on or off, a colour scale, an icon set (null: none), a rule. */
  readonly on?: boolean;
  readonly scale?: HeatmapSpec;
  readonly set?: IconSetName | null;
  readonly rule?: FormatRule;
  /** A line above it: what follows is apart from what precedes (the host's model entries under Create ▸). */
  readonly separated?: boolean;
  /** Said on hover: why a disabled entry is off (Show Grid on a cube on its own). */
  readonly hint?: string;
  /** A flow's box's New Box After (FL9): its gesture made in a new box after this one, never on this one. */
  readonly after?: true;
  /**
   * Its key, shown right-aligned (docs/DATACUBE_UI_REVIEW_2026_10_24.md, the menus): only where the key does what the
   * entry does (shortcuts.ts lists them), spelled as that list spells it.
   */
  readonly shortcut?: string;
}

/** What Conditional Formatting ▸ says of a column: its data bars, its colour scale, its icons, how many rules. */
export interface ConditionalFacts {
  readonly dataBar: boolean;
  readonly scale: boolean;
  readonly iconSet?: IconSetName;
  readonly rules: number;
}

export interface MenuGroup {
  readonly label: string;
  readonly items: readonly MenuItem[];
}

/** Their label for each operator, for the value-aware filter items. */
const OPERATOR_LABEL: Readonly<Partial<Record<FilterOperator, string>>> = {
  equal: '=',
  notEqual: '!=',
  lessThan: '<',
  lessThanEqual: '<=',
  greaterThan: '>',
  greaterThanEqual: '>=',
  contains: 'contains',
  notContains: 'does not contain',
  startsWith: 'starts with',
  notStartsWith: 'does not start with',
  endsWith: 'ends with',
  notEndsWith: 'does not end with',
  isEmpty: 'is null',
  isNotEmpty: 'is not null',
};

/** The operators the menu can apply with the value under the pointer, in its order (theirs). */
const VALUE_OPERATORS: readonly FilterOperator[] = [
  'equal', 'notEqual', 'lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual',
  'contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith',
];

/**
 * The value-aware filters the menu offers on a cell of this type: those of its value
 * operators the compiler accepts for the type (`takesOperator`, src/offers.ts).
 */
export function filterOperatorsFor(type: string): FilterOperator[] {
  return VALUE_OPERATORS.filter((op) => takesOperator(op, type));
}

/**
 * "Add Filter: region = EMEA" -- their wording exactly.
 *
 * The value is in the label because the menu is read after the
 * click, and by then which cell was under the pointer has gone.
 */
/**
 * A filter value as the menu names it.
 *
 * A Date through `String()` read "Tue Feb 09 2021 00:00:00 GMT-0500
 * (Eastern Standard Time)" beside a cell showing "Feb 09, 2021"
 * (2026-09-25 probe). The day, and the time only when there is one --
 * the same shape as the Pure literal the filter sends.
 */
export function valueLabel(value: FilterValue): string {
  if (isRelativeDate(value)) {
    return value.startOf === undefined && value.offset === undefined ? (value.relative === 'today' ? 'TODAY' : 'NOW')
      : relativeDateWords(value);
  }
  if (isJsonValue(value)) return value.json;
  // Text QUOTED, as upstream's: "region = 'EMEA'", so a value that
  // looks like a number or is blank still reads as the text it is.
  if (typeof value === 'string') return `'${value}'`;
  return String(value);
}

function filterItem(
  column: string,
  operator: FilterOperator,
  value: FilterValue | undefined,
): MenuItem {
  const shown = value === undefined ? '' : ` ${valueLabel(value)}`;
  return {
    id: 'filter.add',
    label: `Add Filter: ${columnLabel(column)} ${OPERATOR_LABEL[operator] ?? operator}${shown}`,
    column,
    operator,
    ...(value !== undefined ? { value } : {}),
  };
}

/**
 * NEW BOX AFTER (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL9: a gesture in the results pane adds a box after the box shown,
 * so every box keeps doing one thing): the cell's value filtered by -- kept, or left out -- its column sorted by, or
 * grouped by, each the grid's own gesture, made in a box after this one (`after`). Disabled off a column.
 */
function newBoxItems(ctx: MenuContext): MenuItem[] {
  const { column, value } = ctx;
  if (column === undefined || column === TREE_COLUMN) return [{ label: 'Filter, sort or group in a box of its own', disabled: true, hint: 'Right-click a cell or a header' }];
  const named = columnLabel(column, ctx.totalLabel);
  const items: MenuItem[] = [];
  const filter = (operator: FilterOperator, v: FilterValue | undefined): void => {
    items.push({ ...filterItem(column, operator, v), after: true,
      label: `Filter: ${named} ${OPERATOR_LABEL[operator] ?? operator}${v === undefined ? '' : ` ${valueLabel(v)}`}` });
  };
  // a value to filter by: a row's, typed by the compiler (a pivot's cell has none of its own)
  if (value !== undefined && ctx.pivotBase === undefined) {
    if (value === null) {
      filter('isEmpty', undefined);
      filter('isNotEmpty', undefined);
    } else if (ctx.columnType !== undefined && filterOperatorsFor(ctx.columnType).includes('equal')) {
      filter('equal', value);
      if (filterOperatorsFor(ctx.columnType).includes('notEqual')) filter('notEqual', value);
    }
  }
  items.push(
    { id: 'sort.asc', label: `Sort: ${named} ascending`, column, direction: 'asc', after: true, separated: items.length > 0 },
    { id: 'sort.desc', label: `Sort: ${named} descending`, column, direction: 'desc', after: true },
  );
  if (ctx.canGroup !== false && ctx.pivotBase === undefined) items.push({ id: 'pivot.vertical', label: `Summarize: by ${named}`, column, after: true });
  return items;
}

/**
 * Build the menu for a context.
 *
 * One list of top-level verbs, most of them carrying a submenu --
 * their structure, not a flattening of it. Groups here are the
 * separated blocks their menu draws rules between.
 */
/** Export's entries: the right-click menu's and the menu bar's File, the same. */
export function exportItems(canSaveCube: boolean): MenuItem[] {
  return [
    { id: 'export.html', label: 'HTML' },
    { id: 'export.excel', label: 'Excel (Grid)' },
    { id: 'export.csv', label: 'CSV (Grid)' },
    { id: 'export.text', label: 'Plain Text' },
    { id: 'export.pdf', label: 'PDF' },
    // the cube itself, as a saved-cube document: offered when the host knows its source
    { id: 'export.specification', label: 'Cube File (JSON)', ...(canSaveCube ? {} : { disabled: true }) },
  ];
}

/** Email's entries: the right-click menu's and the menu bar's File, the same. */
export function emailItems(canEmail: boolean): MenuItem[] {
  const items: MenuItem[] = [
    { id: 'email.html', label: 'HTML' },
    { id: 'email.excel', label: 'Excel (Grid)' },
    { id: 'email.csv', label: 'CSV (Grid)' },
    { id: 'email.text', label: 'Plain Text' },
    { id: 'email.pdf', label: 'PDF' },
  ];
  return items.map((i) => ({ ...i, ...(canEmail ? {} : { disabled: true }) }));
}

/**
 * The presets for the column under the pointer (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2), as the chart offers
 * them: a source date by its period, a source number by its bins (the column editor, seeded: the width is the
 * person's), and -- the rows grouped, nothing pivoted -- a measure shown as a running total, a share, a change, a
 * moving average or a rank. Each adds ordinary calculated columns, edited afterwards as any other.
 */
function presetItems(ctx: MenuContext, column: string | undefined, named: string): MenuItem[] {
  if (column === undefined) return [];
  const s = ctx.snapshot;
  const type = rowColumns(s).find((c) => c.name === column)?.type;
  const out: MenuItem[] = [];
  if (ctx.extendable && isTemporal(type)) {
    out.push({ label: `${named} by Period`, submenu: TIME_GRAINS.map((g) => ({
      id: 'calc.grain' as const, label: GRAIN_LABELS[g], column, grain: g,
    })) });
  }
  if (ctx.extendable && isNumeric(type)) out.push({ id: 'calc.bins', label: `Bins of ${named}…`, column });
  if (ctx.isMeasure && ctx.pivotBase === undefined && s.rows.length > 0 && s.pivotOn.length === 0) {
    out.push({ label: `Show ${named} As`, submenu: GRID_SHOW_AS_KINDS.map((k) => ({
      id: 'calc.showAs' as const, label: GRID_SHOW_AS_LABELS[k], column, showAs: k,
    })) });
  }
  return out;
}

/** Extended Columns ▸ Add Related Column ▸ (a grid over a class): each class one hop away, and its columns -- a to-one
 *  end's values, a to-many end's count and aggregates (class-tree.ts relatedOffers). */
function relatedItems(ctx: MenuContext): MenuItem[] {
  if (!ctx.related || ctx.related.length === 0) return [];
  return [{
    label: 'Add Related Column',
    submenu: ctx.related.map((o) => ({
      label: `${o.property} (${o.many ? `each ${className(o.target)}` : className(o.target)})`,
      submenu: o.columns.map((related) => ({ id: 'calc.related' as const, label: relatedLabel(related), related })),
    })),
  }];
}

/** Extended Columns ▸ Add Model Measure ▸ (a grid over a class whose model declares measures of it): each by its name,
 *  its words its hint (model-measures.ts). */
function modelMeasureItems(ctx: MenuContext): MenuItem[] {
  if (!ctx.modelMeasures || ctx.modelMeasures.length === 0) return [];
  return [{
    label: 'Add Model Measure',
    submenu: ctx.modelMeasures.map((m) => ({ id: 'calc.modelMeasure' as const, label: m.name, measure: m.path, ...(m.doc !== undefined ? { hint: m.doc } : {}) })),
  }];
}

/**
 * Conditional Formatting ▸ (Excel's; conditional.ts): the heatmap and Data Bars on or off, a colour scale, an icon set,
 * the one-click rules -- a rule of a value of one's own is the column's Properties' -- and the column's formats cleared.
 * Each on what a heatmap is on (a pivoted column's measure). The heatmap is the grid's colour scale, one setting with the
 * presets: its entry comes first, one click, and says what it will do -- Heatmap, or Remove Heatmap while a scale is on
 * (the user, 2026-10-25: it was lost two levels down, under Color Scales ▸; a toggle names its action and flips).
 */
function conditionalItems(ctx: MenuContext, column: string | undefined): MenuItem[] {
  const c = ctx.conditional;
  const on: Pick<MenuItem, 'column'> = column !== undefined ? { column } : {};
  return [{
    label: 'Conditional Formatting',
    submenu: [
      ctx.hasHeatmap
        ? { id: 'heatmap.remove' as const, label: 'Remove Heatmap', disabled: !column, ...on }
        : { id: 'heatmap.add' as const, label: 'Add Heatmap', disabled: !column, ...on },
      { id: 'format.dataBar', label: c?.dataBar ? 'Remove Data Bars' : 'Add Data Bars', disabled: !column, on: !c?.dataBar, ...on },
      {
        label: 'Color Scales',
        submenu: COLOR_SCALES.map((s) => ({ id: 'format.colorScale' as const, label: s.label, disabled: !column, scale: s.scale, ...on })),
      },
      {
        label: 'Icon Sets',
        submenu: [
          ...ICON_SET_NAMES.map((set) => ({ id: 'format.iconSet' as const, label: ICON_SET_LABELS[set], disabled: !column,
            checked: c?.iconSet === set, set, ...on })),
          { id: 'format.iconSet' as const, label: 'No Icons', disabled: !column || c?.iconSet === undefined, set: null, separated: true, ...on },
        ],
      },
      {
        label: 'Highlight Rules',
        submenu: RULE_PRESETS.map((r) => ({ id: 'format.addRule' as const, label: r.label, disabled: !column, rule: r.rule, ...on })),
      },
      {
        id: 'format.clear', label: 'Clear Formatting from Column', separated: true,
        disabled: !column || !(c && (c.dataBar || c.scale || c.iconSet !== undefined || c.rules > 0)), ...on,
      },
    ],
  }];
}

export function buildMenu(ctx: MenuContext): MenuGroup[] {
  const { snapshot: s } = ctx;
  // A pivot total is a column of the level's query like any other (an aggregate over the group's every pivot value),
  // so it is offered what a pivot's cell is: a sort, a heatmap of its own, a width, hiding, copying. What it is not
  // offered it is not, as a cell is not: a dimension to group or pivot by (canGroup), a value to filter by (a filter
  // runs on source rows, before any total), a pin (a pivot's result, pivotBase).
  const column = ctx.column;
  const sortColumn = ctx.column;
  const groups: MenuGroup[] = [];
  const push = (label: string, items: (MenuItem | null)[]): void => {
    const present = items.filter((i): i is MenuItem => i !== null);
    if (present.length > 0) groups.push({ label, items: present });
  };

  const sorted = sortColumn
    ? s.sorts.find((x) => x.column === sortColumn)
    : undefined;
  const isVertical = column ? s.rows.includes(column) : false;
  const isHorizontal = column ? s.pivotOn.includes(column) : false;
  const groupable = column !== undefined && ctx.canGroup !== false;

  // ---- Export / Edit ----------------------------------------------
  // EDIT ▸ where Copy ▸ was (the user, 2026-10-23: "change it to Edit with undo/redo and copy"): the grid's own Undo
  // and Redo -- its menu's, before the grid's menu button went -- then its copies, as plain text
  push('', [
    { label: 'Export', submenu: exportItems(ctx.canSaveCube === true) },
    { label: 'Email', submenu: emailItems(ctx.canEmail === true) },
    {
      label: 'Edit',
      submenu: [
        { id: 'view.undo', label: 'Undo', disabled: ctx.canUndo !== true, shortcut: 'Ctrl+Z' },
        { id: 'view.redo', label: 'Redo', disabled: ctx.canRedo !== true, shortcut: 'Ctrl+Shift+Z' },
        {
          id: 'copy.selection',
          label: 'Copy Selected Cells',
          disabled: !ctx.hasSelection,
          separated: true,
          shortcut: 'Ctrl+C',
        },
        {
          id: 'copy.rows',
          label: 'Copy Selected Rows',
          disabled: !ctx.hasSelection,
        },
        {
          id: 'copy.column',
          label: column
            ? `Copy Column ${columnLabel(column, ctx.totalLabel)}`
            : 'Copy Column',
          disabled: !column,
          ...(column ? { column } : {}),
        },
      ],
    },
  ]);

  // ---- Sort / Filter / Pivot ----------------------------------------
  push('', [
    // a flow's box: a step after it, in a box of its own (FL9)
    ctx.stepAfter === true ? { label: 'New Box After', submenu: newBoxItems(ctx) } : null,
    {
      label: 'Sort',
      submenu: [
        {
          id: 'sort.asc',
          label: 'Ascending',
          disabled: !sortColumn,
          ...(sortColumn ? { column: sortColumn, direction: 'asc' as const } : {}),
        },
        {
          id: 'sort.desc',
          label: 'Descending',
          disabled: !sortColumn,
          ...(sortColumn ? { column: sortColumn, direction: 'desc' as const } : {}),
        },
        // Upstream disables an Add that would change nothing.
        {
          id: 'sort.addAsc',
          label: 'Add Ascending',
          disabled: !sortColumn || sorted?.direction === 'asc',
          ...(sortColumn ? { column: sortColumn, direction: 'asc' as const } : {}),
        },
        {
          id: 'sort.addDesc',
          label: 'Add Descending',
          disabled: !sortColumn || sorted?.direction === 'desc',
          ...(sortColumn ? { column: sortColumn, direction: 'desc' as const } : {}),
        },
        {
          id: 'sort.clearColumn',
          label: 'Clear Sort',
          disabled: !sorted,
          ...(sortColumn ? { column: sortColumn } : {}),
        },
        {
          id: 'sort.clearAll',
          label: 'Clear All Sorts',
          disabled: s.sorts.length === 0,
        },
      ],
    },
    { label: 'Filter', submenu: filterSubmenu(ctx) },
    // JOIN, beside Filter (the user, 2026-10-23): the host's -- a quick join to each grid sharing a column, the window, a
    // data source -- and always there (the user, 2026-10-24: an entry is disabled, never missing): with nothing to join
    // from here, Join… disabled, saying why
    {
      label: 'Join',
      submenu: ctx.join !== undefined && ctx.join.length > 0 ? ctx.join
        : [{ label: 'Join…', disabled: true, hint: 'A join is made on a page: between its grids, or with a data source' }],
    },
    {
      label: 'Pivot',
      submenu: [
        {
          id: 'pivot.vertical',
          label: groupable
            ? `Vertical Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Vertical Pivot',
          disabled: !groupable,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.addVertical',
          label: groupable
            ? `Add Vertical Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Add Vertical Pivot',
          disabled: !groupable || isVertical,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.removeVertical',
          label: groupable
            ? `Remove Vertical Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Remove Vertical Pivot',
          disabled: !isVertical,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.clearVertical',
          label: 'Clear All Vertical Pivots',
          disabled: s.rows.length === 0,
        },
        {
          id: 'pivot.horizontal',
          label: groupable
            ? `Horizontal Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Horizontal Pivot',
          disabled: !groupable,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.addHorizontal',
          label: groupable
            ? `Add Horizontal Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Add Horizontal Pivot',
          disabled: !groupable || isHorizontal,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.removeHorizontal',
          label: groupable
            ? `Remove Horizontal Pivot on ${columnLabel(column, ctx.totalLabel)}`
            : 'Remove Horizontal Pivot',
          disabled: !isHorizontal,
          ...(column ? { column } : {}),
        },
        // Upstream's pair, only while a column pivot is active: take a
        // measure out of the pivot from one of its pivoted columns, or
        // put an excluded measure back from the measure itself.
        ...(ctx.pivotBase !== undefined && ctx.isMeasure
          && !ctx.excludedFromPivot && s.pivotOn.length > 0
          ? [{
            id: 'pivot.exclude' as const,
            label: `Exclude Column ${columnLabel(ctx.pivotBase, ctx.totalLabel)} from Horizontal Pivot`,
            column: ctx.pivotBase,
          }]
          : []),
        ...(column !== undefined && ctx.pivotBase === undefined && ctx.isMeasure
          && ctx.excludedFromPivot && s.pivotOn.length > 0
          ? [{
            id: 'pivot.include' as const,
            label: `Include Column ${columnLabel(column, ctx.totalLabel)} in Horizontal Pivot`,
            column,
          }]
          : []),
        {
          id: 'pivot.clearHorizontal',
          label: 'Clear All Horizontal Pivots',
          disabled: s.pivotOn.length === 0,
        },
        // Ours: the header measure-first (notional > 2021, 2022) or
        // value-first (2021 > notional, pnl, upstream's). A switch says
        // what it will do, and flips (the menus, docs/DATACUBE_UI_REVIEW_2026_10_24.md).
        {
          id: 'pivot.measuresFirst',
          label: ctx.measuresFirst === true ? 'Values First in Column Headers' : 'Measures First in Column Headers',
          disabled: s.pivotOn.length === 0,
        },
      ],
    },
  ]);

  // ---- Extended Columns -----------------------------------------------
  //
  // Upstream's entries and wording (DataCubeGridMenuBuilder): add one,
  // seed one from the column under the pointer, and edit or delete
  // the one under the pointer when it is itself calculated.
  const named = column === undefined ? '' : columnLabel(column, ctx.totalLabel);
  push('', [
    {
      label: 'Extended Columns',
      submenu: [
        { id: 'calc.add', label: 'Add New Column…' },
        ...(column !== undefined && ctx.extendable
          ? [{ id: 'calc.extend' as const, label: `Extend Column ${named}…`,
            column }]
          : []),
        ...presetItems(ctx, column, named),
        ...relatedItems(ctx),
        // Group Values: a column's values gathered by hand into named groups, a column of its own; its groups edited again
        ...(column !== undefined && ctx.groupsColumn
          ? [{ id: 'calc.editGroups' as const, label: `Edit Groups of ${named}…`, column }]
          : column !== undefined && ctx.groupable ? [{ id: 'calc.groupValues' as const, label: `Group Values of ${named}…`, column }] : []),
        ...modelMeasureItems(ctx),
        ...(column !== undefined && ctx.calcStage !== undefined
          ? [
            // a related column is the class tree's (Add Related Column), never a formula to edit
            ...(ctx.relatedColumn || ctx.groupsColumn ? [] : [{ id: 'calc.edit' as const, label: `Edit Column ${named}…`, column }]),
            { id: 'calc.delete' as const, label: `Delete Column ${named}`,
              column },
          ]
          : []),
      ],
    },
  ]);

  // ---- Resize / Pin / Hide / Collapse / Heatmap -----------------------
  push('', [
    {
      label: 'Resize',
      submenu: [
        {
          id: 'column.autoSize',
          label: 'Auto-size to Fit Content',
          disabled: !column,
          ...(column ? { column } : {}),
        },
        {
          id: 'column.minimize',
          label: 'Minimize Column',
          disabled: !column || ctx.fixedWidth === true,
          ...(column ? { column } : {}),
        },
        { id: 'column.autoSizeAll', label: 'Auto-size All Columns' },
        { id: 'column.minimizeAll', label: 'Minimize All Columns' },
        { id: 'grid.sizeToFit', label: 'Size Grid to Fit Screen' },
      ],
    },
    {
      label: 'Pin',
      submenu: [
        // Upstream: each placement CHECKED when it holds, and disabled
        // then -- there is nothing to do -- as Unpin is when unpinned.
        // A PIVOT RESULT column cannot be pinned, as upstream's
        // (lockPinned): it is one cell of a value block, and pinning it
        // would tear it out of the block it belongs to.
        {
          id: 'column.pinLeft',
          label: 'Pin Left',
          disabled: !column || ctx.pinned === 'left' || ctx.pivotBase !== undefined,
          checked: column !== undefined && ctx.pinned === 'left',
          ...(column ? { column } : {}),
        },
        {
          id: 'column.pinRight',
          label: 'Pin Right',
          disabled: !column || ctx.pinned === 'right' || ctx.pivotBase !== undefined,
          checked: column !== undefined && ctx.pinned === 'right',
          ...(column ? { column } : {}),
        },
        {
          id: 'column.unpin',
          label: 'Unpin',
          disabled: !column || ctx.pinned === undefined,
          ...(column ? { column } : {}),
        },
        { id: 'column.unpinAll', label: 'Remove All Pinnings' },
      ],
    },
    {
      id: 'column.hide',
      label: column ? `Hide ${columnLabel(column, ctx.totalLabel)}` : 'Hide',
      disabled: !column,
      ...(column ? { column } : {}),
    },
    ...conditionalItems(ctx, column),
  ]);

  // CREATE sits beside Properties, as Insert did: a chart, another grid, a class is made OF the cube rather than an
  // operation ON a column (createItems: the same in every menu). A treemap is one of the chart's types (the user,
  // 2026-09-30); a copy starts as this grid is (page/cube-page.ts).
  push('', [{ label: 'Create', submenu: createItems({ source: ctx.canOpenSource === true, host: ctx.create ?? [] }) }]);

  push('', [{
    id: 'view.properties',
    label: 'Properties…',
    shortcut: 'Ctrl+E',
    ...(ctx.propertiesColumn !== undefined ? { column: ctx.propertiesColumn } : {}),
  }]);

  // LAST, after every entry people know by its place (the user: the right-click order stays as it is): VIEW ▸, how the
  // grid is shown and where (the user, 2026-10-24: one bucket, the menu shorter) -- its groups all open or all closed;
  // the one switch for the cube's controls (title bar, drag zones, columns panel, status bar), hidden together for the
  // grid alone, and the way back: the right-click menu is the only way back (the user, 2026-10-01: "right click only");
  // the keyboard's shortcuts; and on a page, what is done to the grid's tile -- onto another sheet, off the page (once
  // Tile ▸, the user, 2026-10-23: the grid's menu button gone).
  const tile: MenuItem[] = [
    ...(ctx.sheets === undefined ? []
      : [{ label: 'Move to Sheet', ...(ctx.sheets === 'locked' ? { disabled: true, hint: 'Edit Layout first' } : { submenu: ctx.sheets }) }]),
    // off while the page is locked, as its tile's x is (nothing moves by accident: the 2026-10-25 audit)
    ...(ctx.canRemove === true ? [{ id: 'grid.remove' as const, label: 'Remove from Page',
      ...(ctx.sheets === 'locked' ? { disabled: true, hint: 'Edit Layout first' } : {}) }] : []),
  ];
  push('', [{
    label: 'View',
    submenu: [
      // groups under groups to open: one level of groups has only its rows under it
      { id: 'tree.expandAll', label: 'Expand All', disabled: s.rows.length < 2 },
      { id: 'tree.collapseAll', label: 'Collapse All', disabled: !ctx.hasExpanded },
      { id: 'view.controls', label: ctx.controlsHidden ? 'Show Controls' : 'Hide Controls', separated: true },
      { id: 'view.shortcuts', label: 'Keyboard Shortcuts…' },
      ...tile.map((item, i) => (i === 0 ? { ...item, separated: true } : item)),
    ],
  }]);

  return groups;
}

/**
 * The Filter submenu, which is the value-aware one.
 *
 * With a value under the pointer it leads with the equality filter
 * on that value and tucks every other operator for the column's
 * type into a submenu of its own -- their three-level shape. A
 * blank cell gets the two null predicates instead, because "= "
 * against nothing is not a filter anyone means.
 */
function filterSubmenu(ctx: MenuContext): MenuItem[] {
  const { column, value } = ctx;
  const items: MenuItem[] = [];

  if (column !== undefined && value !== undefined) {
    if (value === null) {
      items.push(
        filterItem(column, 'isEmpty', undefined),
        filterItem(column, 'isNotEmpty', undefined),
      );
    } else {
      const operators = ctx.columnType === undefined ? [] : filterOperatorsFor(ctx.columnType);
      if (operators.includes('equal')) {
        items.push(filterItem(column, 'equal', value));
        const more = operators.filter((op) => op !== 'equal');
        if (more.length > 0) {
          items.push({
            label: `More Filters on ${columnLabel(column, ctx.totalLabel)}…`,
            submenu: more.map((op) => filterItem(column, op, value)),
          });
        }
      }
    }
  }

  // a row's column the compiler has typed (the app's columnType: a source column or a row-stage calculated one) --
  // what a filter can run on, its values' literals written by its type -- offers its values to tick, and a date (not a
  // time of day) its ranges relative to today; a pivot's cell offers neither
  const type = column === undefined || ctx.pivotBase !== undefined ? undefined : ctx.columnType;
  if (column !== undefined && type !== undefined) {
    items.push({ id: 'filter.valueList', label: `Filter Values of ${columnLabel(column, ctx.totalLabel)}…`, column });
    if (isTemporal(type) && !isTimeOfDay(type)) {
      items.push({ label: 'Date Range', submenu: DATE_RANGES.map((range) => ({
        id: 'filter.dateRange' as const, label: DATE_RANGE_LABELS[range], column, range,
      })) });
    }
  }
  // TOP OR BOTTOM N by a measure -- a grouped grid's groups, each parent's own -- or by a flat grid's number; never a
  // pivot's (its figures are its values' columns)
  const s = ctx.snapshot;
  const rankable = column !== undefined && ctx.pivotBase === undefined && s.pivotOn.length === 0
    && (s.rows.length > 0 ? ctx.isMeasure === true : isNumeric(type));
  if (rankable) {
    const named = columnLabel(column, ctx.totalLabel);
    items.push(
      { id: 'filter.topN', label: `Top 10 by ${named}`, column, n: 10 },
      { id: 'filter.topN', label: `Bottom 10 by ${named}`, column, n: 10, bottom: true },
    );
  }
  if (s.topN !== undefined) {
    items.push({ id: 'filter.topNOff', label: `Clear ${s.topN.bottom ? 'Bottom' : 'Top'} ${s.topN.n}` });
  }
  items.push(
    { id: 'filter.column', label: 'Filters…' },
    {
      id: 'filter.clearAll',
      label: 'Clear All Filters',
      disabled: !ctx.snapshot.filter,
    },
  );
  return items;
}

/** Every item in a menu, submenus included -- for tests and search. */
export function menuItems(groups: readonly MenuGroup[]): MenuItem[] {
  const walk = (items: readonly MenuItem[]): MenuItem[] =>
    items.flatMap((i) => [i, ...(i.submenu ? walk(i.submenu) : [])]);
  return groups.flatMap((g) => walk(g.items));
}

// -- an entry as its gesture -------------------------------------------

/**
 * The grid's gesture a menu entry makes (grid-gestures.ts: its id the gesture's name, the one Python's grid.gesture
 * takes) and what the entry carries of what the gesture takes -- the column under it, an operator and its value -- or
 * null: an entry that is the screen's own (a column measured, a window opened), layout, clipboard or export. What the
 * gesture means is grid-gestures.ts's: an entry that would change nothing makes no command there, one its offers refuse
 * is refused when applied, in their words (CubeRefusal).
 */
export function menuGesture(item: MenuItem): { readonly name: GridGestureName; readonly args: GestureArgs } | null {
  if (!isGridGesture(item.id)) return null;
  const carried: Record<string, unknown> = {
    column: item.column, operator: item.operator, value: item.value, range: item.range, n: item.n, bottom: item.bottom,
    on: item.on, scale: item.scale, set: item.set, rule: item.rule,
  };
  return {
    name: item.id,
    args: Object.fromEntries(gestureTakes(item.id).flatMap((k) => (carried[k] !== undefined ? [[k, carried[k]]] : []))),
  };
}
