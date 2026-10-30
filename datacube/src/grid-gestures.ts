// A GRID'S GESTURES AS THEIR COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8; the user, 2026-10-22: "find all
// [places the UI is coupled to the implementation] and guard against this"). What each gesture a person makes on a grid
// means -- a column added to the row groups, a header clicked, every column unpinned -- written once: the screen's
// control calls it, Python's grid.gesture asks it (headless gridGesture), so a gesture means one thing wherever it is
// made. Named by the menu's own ids where the menu has the gesture. The screen keeps what is its alone -- pixels: a
// column measured, the grid's width, where a drag let go -- and passes what it measured; the rest is here.
//
// Two ways in, one table: gridGesture, the screen's controls', each gesture's arguments typed (GridGestureArgs: a
// misnamed or mistyped one does not compile); gridGestureGiven, the menu's and Python's, the gesture named by text and
// its arguments checked as they come. What a gesture carries into its commands unjudged (a filter, a group's path, a
// calculated column) is typed where the screen gives it; given from JSON (Python's, headless gridGesture) the commands
// are read by the commands' own reader, as every command Python builds is (grid-commands.ts readGridCommand) -- the
// reader kept out of the page's first download.

import { columnConfig, mergeColumnOrder, withColumn, type CubeConfiguration } from './config.ts';
import { availableDimensions, drillDown, drillUp, useDimension, type Dimension } from './dimensions.ts';
import { familyOf, isDecimal } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { headerSortCommand, type SortedHeader } from './header-sort.ts';
import {
  CubeRefusal, isPivotTotalColumn, readTopN, rowColumns, sameValue, type CubeSnapshot, type DerivedColumn, type FilterNode, type FilterOperator, type FilterValue,
} from './snapshot.ts';
import type { RowPath } from './tree.ts';
import { DATA_BAR, readColorScale, readIconSet, readRule, type FormatRuleGiven, type IconSetName } from './conditional.ts';
import type { HeatmapSpec } from './style.ts';
import { readFormatCode } from './format-code.ts';
import { dateRangeConditions, datesBetweenConditions, type DateRange } from './date-ranges.ts';
import { keptCondition, ownConditions } from './value-filter.ts';
import { TREE_COLUMN } from './treeview.ts';

/** What a gesture reads of the grid: its state, and the dimensions it offers (its own, or its host's). */
export interface GestureGrid {
  readonly snapshot: CubeSnapshot;
  readonly config: CubeConfiguration;
  readonly dimensions: readonly Dimension[];
  /** What only the grid's view has, for the gestures that read it (GRID_GESTURES' `view`): the headers it paints
   *  (shown-columns.ts shownColumns) and its pivot's columns, each with the measure it spreads. */
  readonly view: GestureView | null;
}

export interface GestureView {
  readonly headers: readonly SortedHeader[];
  readonly pivot: readonly { readonly name: string; readonly measure: { readonly name: string } }[];
}

/** A gesture's arguments by name, as a menu entry carries them or Python's JSON does: checked by the gesture. */
export type GestureArgs = Readonly<Record<string, unknown>>;

/** A gesture that takes nothing. */
type Nothing = Readonly<Record<string, never>>;
type Column = { readonly column: string };
type Widths = Readonly<Record<string, number>>;
type Level = 'row' | 'group';

/** Each gesture's arguments, by its name: what the screen's controls give (gridGesture), checked by the compiler. */
export type GridGestureArgs = {
  readonly 'sort.asc': Column; readonly 'sort.desc': Column; readonly 'sort.addAsc': Column; readonly 'sort.addDesc': Column;
  readonly 'sort.clearColumn': Column; readonly 'sort.clearAll': Nothing;
  /** `column` null: the tree column's */
  readonly 'header.sort': { readonly column: string | null; readonly chained: boolean };
  readonly 'filter.add': { readonly column: string; readonly operator: FilterOperator; readonly value?: FilterValue };
  readonly 'filter.clearAll': Nothing;
  readonly 'filter.set': { readonly filter: FilterNode | null };
  readonly 'filter.values': { readonly column: string; readonly values: readonly (FilterValue | null)[] | null };
  readonly 'filter.dateRange': { readonly column: string; readonly range: DateRange };
  readonly 'filter.dates': { readonly column: string; readonly range?: DateRange; readonly from?: string; readonly to?: string };
  readonly 'filter.topN': { readonly column: string; readonly n: number; readonly bottom?: boolean };
  readonly 'filter.topNOff': Nothing;
  readonly 'chart.refilter': { readonly remove: readonly FilterNode[]; readonly add: readonly FilterNode[] };
  readonly 'pivot.vertical': Column; readonly 'pivot.addVertical': Column; readonly 'pivot.removeVertical': Column;
  readonly 'pivot.clearVertical': Nothing;
  readonly 'pivot.horizontal': Column; readonly 'pivot.addHorizontal': Column; readonly 'pivot.removeHorizontal': Column;
  readonly 'pivot.clearHorizontal': Nothing;
  readonly 'pivot.pin': { readonly values: readonly (FilterValue | null)[] | null };
  readonly 'pivot.addValues': { readonly values: readonly (FilterValue | null)[] };
  readonly 'zones.set': { readonly rows: readonly string[]; readonly columns: readonly string[] };
  readonly 'pivot.measuresFirst': Nothing; readonly 'pivot.exclude': Column; readonly 'pivot.include': Column;
  readonly 'column.hide': Column; readonly 'column.show': { readonly column: string; readonly visible: boolean };
  readonly 'column.pinLeft': Column; readonly 'column.pinRight': Column; readonly 'column.unpin': Column;
  readonly 'column.unpinAll': Nothing;
  readonly 'column.resize': { readonly column: string; readonly width: number };
  readonly 'columns.reorder': { readonly order: readonly string[]; readonly added?: string };
  readonly 'columns.autoSize': { readonly widths: Widths };
  readonly 'column.minimize': Column; readonly 'column.minimizeAll': Nothing;
  readonly 'columns.sizeToFit': { readonly widths: Widths; readonly viewport: number };
  readonly 'heatmap.add': Column; readonly 'heatmap.remove': Column;
  readonly 'format.dataBar': Column & { readonly on: boolean };
  readonly 'format.colorScale': Column & { readonly scale: HeatmapSpec };
  readonly 'format.iconSet': Column & { readonly set: IconSetName | null; readonly reverse?: boolean };
  readonly 'format.addRule': Column & { readonly rule: FormatRuleGiven };
  readonly 'format.clear': Column;
  readonly 'format.code': Column & { readonly code: string | null };
  readonly 'view.chrome': { readonly showDragZones?: boolean; readonly showTitleBar?: boolean };
  readonly 'tree.open': { readonly path: RowPath; readonly open: boolean };
  readonly 'tree.collapseAll': Nothing;
  readonly 'tree.expandAll': Nothing;
  readonly 'view.dimension': { readonly name: string; readonly move: 'use' | 'down' | 'up' };
  readonly 'column.calculate': { readonly level: Level; readonly column: DerivedColumn };
  readonly 'column.editCalculated': { readonly name: string; readonly level: Level; readonly column: DerivedColumn };
  readonly 'calc.delete': Column;
};

/** Room for a cell's own padding and its border (Auto-size). */
const AUTO_SIZE_PAD = 8;
/** Narrow enough to be honest, wide enough to still be grabbable. */
const AUTO_SIZE_MIN = 48;
/** One long free-text column must not push the rest off screen. */
const AUTO_SIZE_MAX = 480;
/** Upstream's DEFAULT_COLUMN_MIN_WIDTH: what Minimize shrinks to, and the narrowest Size to Fit makes a column. */
const MINIMIZED_WIDTH = 50;
/** A heatmap as the menu's Add Heatmap paints it. */
const HEATMAP = { from: '#ffffff', to: '#ff8a65' };

interface Gesture {
  /** Its arguments by name; `name?` one it may go without. */
  readonly args: readonly string[];
  /** It reads the grid's view (GestureGrid.view): always, or where its arguments say it must on this grid. */
  readonly view?: true | ((s: CubeSnapshot, a: GestureArgs) => boolean);
  /** Its commands; none, a gesture that would change nothing. */
  readonly make: (g: GestureGrid, a: GestureArgs) => GridCommand[];
}

const text = (a: GestureArgs, name: string, what: string): string => {
  const v = a[name];
  if (typeof v !== 'string') throw new CubeRefusal(`${what} is text, not ${JSON.stringify(v) ?? 'nothing'}`);
  return v;
};
const names = (a: GestureArgs, name: string, what: string): string[] => {
  const v = a[name];
  if (!Array.isArray(v) || !v.every((n) => typeof n === 'string')) {
    throw new CubeRefusal(`${what} is a list of the grid's columns, not ${JSON.stringify(v) ?? 'nothing'}`);
  }
  return v;
};
const flag = (a: GestureArgs, name: string, what: string): boolean => {
  const v = a[name];
  if (typeof v !== 'boolean') throw new CubeRefusal(`${what} is true or false, not ${JSON.stringify(v) ?? 'nothing'}`);
  return v;
};
/** A width in pixels (what the screen measured). */
const pixels = (a: GestureArgs, name: string, what: string): number => {
  const v = a[name];
  if (typeof v !== 'number' || !Number.isFinite(v)) throw new CubeRefusal(`${what} is a number of pixels, not ${JSON.stringify(v) ?? 'nothing'}`);
  return v;
};
/** Widths by column, in pixels (what the screen measured). */
const widthsOf = (a: GestureArgs, name: string, what: string): Record<string, number> => {
  const v = a[name];
  if (typeof v !== 'object' || v === null || Array.isArray(v)
    || !Object.values(v).every((w) => typeof w === 'number' && Number.isFinite(w))) {
    throw new CubeRefusal(`${what} are widths by column, in pixels, not ${JSON.stringify(v) ?? 'nothing'}`);
  }
  return v as Record<string, number>;
};
const columnOf = (a: GestureArgs): string => text(a, 'column', "the gesture's column");
const viewOf = (g: GestureGrid): GestureView => {
  // a gesture reading the view is flagged so (GRID_GESTURES' `view`): whoever asks it gives the view
  if (g.view === null) throw new Error('a gesture that reads the grid\'s view was given none');
  return g.view;
};

/** Columns' presentation patched, each by a configure's own keys (null clears a setting), with settings beside them. */
const configure = (columns: Readonly<Record<string, object>>, settings: object = {}): GridCommand => ({
  kind: 'configure', patch: { ...settings, ...(Object.keys(columns).length > 0 ? { columns } : {}) },
});
const column = (name: string, patch: object): GridCommand => configure({ [name]: patch });
/** A column's settings patched, or none when it has each already (null: a setting it has not) -- a pin it has, a width
 *  it is, a heatmap it has not: nothing to tell, no step of the grid's history, nothing to Python's page (the S8
 *  audit's 6). */
const patched = (g: GestureGrid, name: string, patch: Readonly<Record<string, unknown>>): GridCommand[] => {
  const has = columnConfig(g.config, name) as Readonly<Record<string, unknown>>;
  const same = Object.entries(patch).every(([k, v]) => (v === null ? has[k] === undefined : JSON.stringify(has[k]) === JSON.stringify(v)));
  return same ? [] : [column(name, patch)];
};

/** The columns' order as the grid shows it: the configured order, then any declared column it leaves out. */
export function columnOrderOf(s: CubeSnapshot, config: CubeConfiguration): string[] {
  const declared = s.columns.map((c) => c.name);
  const configured = config.columnOrder ?? [];
  return [...configured.filter((n) => declared.includes(n)), ...declared.filter((n) => !configured.includes(n))];
}

/** The row groups, or the pivot, with `name` added last -- none when it is there already -- or taken out. */
/** `column`'s own conditions in the filter (value-filter.ts ownConditions) replaced by `add`: one refilter, or nothing
 *  when nothing changes. */
const replaced = (s: CubeSnapshot, column: string, add: readonly FilterNode[]): GridCommand[] => {
  const remove = ownConditions(s.filter, column);
  return remove.length === 0 && add.length === 0 ? [] : [{ kind: 'refilter', remove, add }];
};

const zone = (kind: 'group' | 'pivot', s: CubeSnapshot, change: (has: readonly string[]) => string[] | null): GridCommand[] => {
  const has = kind === 'group' ? s.rows : s.pivotOn;
  const next = change(has);
  if (next === null) return [];
  return [kind === 'group' ? { kind, rows: next } : { kind, columns: next }];
};
const added = (name: string) => (has: readonly string[]): string[] | null => (has.includes(name) ? null : [...has, name]);
const removed = (name: string) => (has: readonly string[]): string[] | null => (has.includes(name) ? has.filter((x) => x !== name) : null);

/** Whether `column` can be a pivoted leaf, which only the view names the measure of: on a pivoted grid, no total and no
 *  column of the grid's own (the heatmap asks the view only then -- headless runs the grid for it). */
function pivotedLeaf(s: CubeSnapshot, a: GestureArgs): boolean {
  const leaf = a['column'];
  return s.pivotOn.length > 0 && typeof leaf === 'string' && !isPivotTotalColumn(leaf) && !rowColumns(s).some((c) => c.name === leaf);
}

/** A heatmap on or off for the column a leaf is of: a pivoted leaf's measure, so every value's leaf has it (a heatmap
 *  on one pivoted leaf and not its siblings is worse than none); a pivot total is a column of its own (the user,
 *  2026-10-18), its measure's leaves left alone (export-render.ts heatmapSetting). */
const heatmap = (g: GestureGrid, leaf: string, on: boolean): GridCommand[] =>
  // null takes it off, as JSON cannot write undefined
  patched(g, presentedOn(g, leaf), { heatmap: on ? HEATMAP : null });

/** The column a leaf's colouring is set on: a pivoted leaf's measure (heatmap's rule), else the leaf. */
const presentedOn = (g: GestureGrid, leaf: string): string =>
  (pivotedLeaf(g.snapshot, { column: leaf }) ? viewOf(g).pivot.find((c) => c.name === leaf)?.measure.name ?? leaf : leaf);

/** A rule added to a column's (conditional.ts), after those it has; one it has already, nothing. */
const addRule = (g: GestureGrid, leaf: string, given: unknown): GridCommand[] => {
  const name = presentedOn(g, leaf);
  const rule = readRule(given);
  const has = columnConfig(g.config, name).rules ?? [];
  return has.some((r) => JSON.stringify(r) === JSON.stringify(rule)) ? [] : [column(name, { rules: [...has, rule] })];
};

/** The values a Cross Tab's pin holds (FL2), or null: its columns its rows' values. */
const pinnedValues = (g: GestureGrid): readonly FilterValue[] | null =>
  (g.snapshot.pivotValues !== undefined && g.snapshot.pivotValues.length > 0 ? g.snapshot.pivotValues : null);

/** A pivot's values as given, each typed as its column is (a number answered as text, as a database's big integer is, a
 *  number; a Decimal's, its text). An empty value is refused in words: it names no column. */
function pivotValuesTyped(g: GestureGrid, given: unknown): FilterValue[] {
  if (!Array.isArray(given)) throw new CubeRefusal(`a pivot's values are a list, or null, not ${JSON.stringify(given)}`);
  const on = g.snapshot.pivotOn[0];
  const type = rowColumns(g.snapshot).find((c) => c.name === on)?.type;
  return (given as readonly unknown[]).map((v): FilterValue => {
    if (v === null) throw new CubeRefusal(`its rows hold an empty '${on ?? ''}', which names no column: fill or filter its empty values first`);
    // a number past what a double holds exactly kept as its text: its condition reads it as its column's type; a
    // Decimal's text kept whole, every digit it has (a double rounds 12345678901234.567 and drops 12.10's zero: the
    // review of 2026-10-26's B1)
    const n = typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
    const exact = familyOf(type) === 'numeric' && !isDecimal(type);
    if (exact && Number.isFinite(n) && (!Number.isInteger(n) || Number.isSafeInteger(n))) return n;
    return v as FilterValue;
  });
}

const GESTURES = {
  // -- sorts (the header menu's Sort, the header's click)
  'sort.asc': { args: ['column'], make: (_g, a) => [{ kind: 'sorts', sorts: [{ column: columnOf(a), direction: 'asc' }] }] },
  'sort.desc': { args: ['column'], make: (_g, a) => [{ kind: 'sorts', sorts: [{ column: columnOf(a), direction: 'desc' }] }] },
  'sort.addAsc': { args: ['column'], make: (_g, a) => [{ kind: 'addSort', column: columnOf(a), direction: 'asc' }] },
  'sort.addDesc': { args: ['column'], make: (_g, a) => [{ kind: 'addSort', column: columnOf(a), direction: 'desc' }] },
  // the column's sort taken off by the one rule (grid-edits.ts clearSort)
  'sort.clearColumn': { args: ['column'], make: (_g, a) => [{ kind: 'clearSort', column: columnOf(a) }] },
  'sort.clearAll': { args: [], make: () => [{ kind: 'sorts', sorts: [] }] },
  // a header clicked (header-sort.ts headerSortCommand): `column` null, the tree column's; its first direction the type
  // its header has, which only the view has
  'header.sort': {
    args: ['column', 'chained'], view: true,
    make: (g, a) => [headerSortCommand(viewOf(g).headers,
      a['column'] === null ? TREE_COLUMN : text(a, 'column', 'the header clicked'), flag(a, 'chained', 'a chained click'))],
  },

  // -- the filter
  // ADDS to the filter rather than replacing it (a cell's Add Filter), so the entry is usable twice in a row: and-ed
  // with the tree it has (grid-commands.ts addFilter); an OR is the filter editor's job
  'filter.add': {
    args: ['column', 'operator', 'value?'],
    make: (_g, a) => [{ kind: 'addFilter', condition: {
      kind: 'condition', column: columnOf(a), operator: a['operator'] as FilterOperator,
      ...(a['value'] !== undefined ? { value: a['value'] as FilterValue } : {}),
    } }],
  },
  'filter.clearAll': { args: [], make: () => [{ kind: 'filter', filter: null }] },
  // the filter editor's filter, whole; null takes it off
  'filter.set': { args: ['filter'], make: (_g, a) => [{ kind: 'filter', filter: a['filter'] as FilterNode | null }] },
  // THE VALUE LIST (value-filter.ts, Excel's autofilter): the values of the column kept -- null among them the blanks --
  // in place of the column's own conditions; null for every value (its own conditions taken off)
  'filter.values': {
    args: ['column', 'values'],
    make: (g, a) => {
      const column = columnOf(a);
      const values = a['values'];
      if (values !== null && !Array.isArray(values)) {
        throw new CubeRefusal(`the values kept are a list (null among them the blanks), or null for every one, not ${JSON.stringify(values)}`);
      }
      return replaced(g.snapshot, column, values === null ? [] : [keptCondition(column, values as (FilterValue | null)[])]);
    },
  },
  // TOP OR BOTTOM N by the column (snapshot.ts TopN): the menu's Top 10 and Bottom 10, Python's grid.top
  'filter.topN': {
    args: ['column', 'n', 'bottom?'],
    make: (_g, a) => [{ kind: 'topN', topN: readTopN({ by: columnOf(a), n: a['n'], ...(a['bottom'] === true ? { bottom: true } : {}) }) }],
  },
  'filter.topNOff': { args: [], make: (g) => (g.snapshot.topN === undefined ? [] : [{ kind: 'topN', topN: null }]) },
  // A DATE RANGE relative to when the query runs (date-ranges.ts): from its start to its end, in place of the column's
  // own conditions
  'filter.dateRange': {
    args: ['column', 'range'],
    make: (g, a) => {
      const column = columnOf(a);
      return replaced(g.snapshot, column, dateRangeConditions(column, text(a, 'range', 'the date range') as DateRange));
    },
  },
  // A DATE FILTER'S DATES (date-ranges.ts; a flow's Date Filter box, Python's grid.date_filter): its filter made the
  // column's dates in a range relative to today, or the days from one to another (both kept), alone -- the box's one
  // step, in place of every condition it had
  'filter.dates': {
    args: ['column', 'range?', 'from?', 'to?'],
    make: (g, a) => {
      const column = columnOf(a);
      const days = a['from'] !== undefined || a['to'] !== undefined;
      if ((a['range'] !== undefined) === days) {
        throw new CubeRefusal('a Date Filter keeps a range of dates, or the days from one to another: one of them');
      }
      // its column's type: a date-time's every moment of both days (date-ranges.ts; the review of 2026-10-26's C5)
      const type = rowColumns(g.snapshot).find((c) => c.name === column)?.type;
      const add = days ? datesBetweenConditions(column, text(a, 'from', 'its first day'), text(a, 'to', 'its last day'), type)
        : dateRangeConditions(column, text(a, 'range', 'the date range') as DateRange);
      const had = g.snapshot.filter;
      const remove = had === undefined ? [] : had.kind === 'and' ? had.children : [had];
      return sameValue(remove, add) ? [] : [{ kind: 'refilter', remove, add }];
    },
  },
  // a chart's mark on the grid's filter -- the selection it had taken off, the one it has now on -- or a selection off
  'chart.refilter': {
    args: ['remove', 'add'],
    make: (_g, a) => [{ kind: 'refilter', remove: a['remove'] as readonly FilterNode[], add: a['add'] as readonly FilterNode[] }],
  },

  // -- the row groups and the pivot (the menu's Vertical and Horizontal Pivot, the zones, the columns panel)
  'pivot.vertical': { args: ['column'], make: (_g, a) => [{ kind: 'group', rows: [columnOf(a)] }] },
  'pivot.addVertical': { args: ['column'], make: (g, a) => zone('group', g.snapshot, added(columnOf(a))) },
  'pivot.removeVertical': { args: ['column'], make: (g, a) => zone('group', g.snapshot, removed(columnOf(a))) },
  'pivot.clearVertical': { args: [], make: () => [{ kind: 'group', rows: [] }] },
  'pivot.horizontal': { args: ['column'], make: (_g, a) => [{ kind: 'pivot', columns: [columnOf(a)] }] },
  'pivot.addHorizontal': { args: ['column'], make: (g, a) => zone('pivot', g.snapshot, added(columnOf(a))) },
  'pivot.removeHorizontal': { args: ['column'], make: (g, a) => zone('pivot', g.snapshot, removed(columnOf(a))) },
  'pivot.clearHorizontal': { args: [], make: () => [{ kind: 'pivot', columns: [] }] },
  // A CROSS TAB'S LOAD VALUES (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL2; Python's grid.pin_pivot): the values its rows
  // hold now, as the database answered them, pinned -- its columns those from then on (grid-edits.ts pinPivot), each
  // typed as its column is (a number answered as text, as a database's big integer is, a number; a Decimal's, its
  // text); null, its values its
  // rows' again. An empty value is refused in words: it names no column. The same values pinned again: nothing
  'pivot.pin': {
    args: ['values'],
    make: (g, a) => {
      const had = pinnedValues(g);
      const given = a['values'];
      if (given === null) return had === null ? [] : [{ kind: 'pinPivot', values: null }];
      const values = pivotValuesTyped(g, given);
      return sameValue(had, values) ? [] : [{ kind: 'pinPivot', values }];
    },
  },
  // A CROSS TAB'S LACKING VALUES ADDED TO ITS COLUMNS (FL8's fix, "Add to the cast"; Python's grid.add_pivot_values):
  // each value given its pin lacks a column of its own, after the columns it has, in the order given, typed as Load
  // values types them -- the pin made longer (pinPivot). An empty value is skipped: it names no column. Refused in words:
  // a pin it has not (its columns are its rows' already), or a column for each of them already
  'pivot.addValues': {
    args: ['values'],
    make: (g, a) => {
      const had = pinnedValues(g);
      if (had === null) throw new CubeRefusal('its columns are not fixed: Load values fixes them');
      const given = a['values'];
      if (!Array.isArray(given)) throw new CubeRefusal(`a pivot's values are a list, not ${JSON.stringify(given)}`);
      const added: FilterValue[] = [];
      for (const v of pivotValuesTyped(g, (given as readonly unknown[]).filter((v) => v !== null))) {
        if (![...had, ...added].some((p) => sameValue(p, v))) added.push(v);
      }
      if (added.length === 0) throw new CubeRefusal('it has a column for each of them already');
      return [{ kind: 'pinPivot', values: [...had, ...added] }];
    },
  },
  // a drag in the zones: the whole layout the drop leaves, one change (P2-103)
  'zones.set': {
    args: ['rows', 'columns'],
    make: (_g, a) => [{ kind: 'group', rows: names(a, 'rows', 'the row groups') },
      { kind: 'pivot', columns: names(a, 'columns', 'the pivot') }],
  },
  'pivot.measuresFirst': {
    args: [], make: (g) => [configure({}, { pivotMeasuresFirst: g.config.pivotMeasuresFirst ? null : true })],
  },
  'pivot.exclude': { args: ['column'], make: (_g, a) => [column(columnOf(a), { excludedFromPivot: true })] },
  'pivot.include': { args: ['column'], make: (_g, a) => [column(columnOf(a), { excludedFromPivot: false })] },

  // -- the columns' presentation
  // a pivot's header or the tree column too
  'column.hide': { args: ['column'], make: (g, a) => patched(g, columnOf(a), { hidden: true }) },
  // the columns panel's tick
  'column.show': {
    args: ['column', 'visible'], make: (g, a) => patched(g, columnOf(a), { hidden: !flag(a, 'visible', 'shown') }),
  },
  'column.pinLeft': { args: ['column'], make: (g, a) => patched(g, columnOf(a), { pinned: 'left' }) },
  'column.pinRight': { args: ['column'], make: (g, a) => patched(g, columnOf(a), { pinned: 'right' }) },
  'column.unpin': { args: ['column'], make: (g, a) => patched(g, columnOf(a), { pinned: null }) },
  'column.unpinAll': {
    args: [],
    make: (g) => {
      const pinned = Object.keys(g.config.columns).filter((name) => g.config.columns[name]?.pinned !== undefined);
      return pinned.length === 0 ? [] : [configure(Object.fromEntries(pinned.map((n) => [n, { pinned: null }])))];
    },
  },
  // a dragged edge: a width like Auto-size's, resizable again (unlike an explicit Fixed one)
  'column.resize': {
    args: ['column', 'width'],
    make: (g, a) => patched(g, columnOf(a), { width: pixels(a, 'width', "a column's width") }),
  },
  // REORDERED where the list is (the columns panel, the grid's header drag) -- MERGED, never written over: the list
  // reports only what it shows, so writing its report straight in dropped every grouped, pivoted and hidden column out
  // of the order (config.ts mergeColumnOrder). A column dragged out of the panel into the grid (`added`) is shown, in
  // the same change.
  'columns.reorder': {
    args: ['order', 'added?'],
    make: (g, a) => [configure(a['added'] !== undefined ? { [text(a, 'added', 'the column dragged in')]: { hidden: false } } : {},
      { columnOrder: mergeColumnOrder(columnOrderOf(g.snapshot, g.config), names(a, 'order', "the columns' order")) })],
  },
  // Auto-size: each column measured (`widths`: its content's width, the screen's) to that width, between a floor and a
  // ceiling -- a column of long free text would push every other off the screen, an empty one collapse to nothing --
  // none when no width changes
  'columns.autoSize': {
    args: ['widths'],
    make: (g, a) => {
      const sized = Object.entries(widthsOf(a, 'widths', "the columns' content"))
        .map(([name, content]) => [name, Math.min(AUTO_SIZE_MAX, Math.max(AUTO_SIZE_MIN, content + AUTO_SIZE_PAD))] as const);
      if (sized.every(([name, width]) => withColumn(g.config, name, { width }) === g.config)) return [];
      return [configure(Object.fromEntries(sized.map(([name, width]) => [name, { width }])))];
    },
  },
  // Resize > Minimize: a column to its own minimum width, or upstream's DEFAULT_COLUMN_MIN_WIDTH; a fixed width kept
  'column.minimize': { args: ['column'], make: (g, a) => minimized(g, [columnOf(a)]) },
  // every column the grid shows
  'column.minimizeAll': {
    args: [], view: true, make: (g) => minimized(g, viewOf(g).headers.map((h) => h.name).filter((n) => n !== TREE_COLUMN)),
  },
  // Resize > Size Grid to Fit Screen: every column shown (`widths`, as drawn) scaled by one factor so together they
  // fill the grid's width (`viewport`) -- ag-Grid's sizeColumnsToFit, which upstream calls; a fixed width keeps its
  // size, the rest share what is left
  'columns.sizeToFit': {
    args: ['widths', 'viewport'],
    make: (g, a) => {
      const widths = widthsOf(a, 'widths', 'the columns drawn');
      const viewport = pixels(a, 'viewport', "the grid's width");
      const fixed = Object.keys(widths).filter((n) => columnConfig(g.config, n).widthMode === 'fixed');
      const flexible = Object.keys(widths).filter((n) => !fixed.includes(n));
      const taken = fixed.reduce((sum, n) => sum + (widths[n] ?? 0), 0);
      const current = flexible.reduce((sum, n) => sum + (widths[n] ?? 0), 0);
      if (viewport <= 0 || current <= 0 || flexible.length === 0) return [];
      const factor = Math.max(0, viewport - taken) / current;
      return [configure(Object.fromEntries(flexible.map((name) => [name, {
        width: Math.max(MINIMIZED_WIDTH, Math.floor((widths[name] ?? 0) * factor)),
      }])))];
    },
  },
  'heatmap.add': { args: ['column'], view: pivotedLeaf, make: (g, a) => heatmap(g, columnOf(a), true) },
  'heatmap.remove': { args: ['column'], view: pivotedLeaf, make: (g, a) => heatmap(g, columnOf(a), false) },
  // conditional formats (conditional.ts), each on what a heatmap is on: data bars, a colour scale, icons, a rule, none
  'format.dataBar': {
    args: ['column', 'on'], view: pivotedLeaf,
    make: (g, a) => patched(g, presentedOn(g, columnOf(a)), { dataBar: flag(a, 'on', 'the data bars shown') ? DATA_BAR : null }),
  },
  'format.colorScale': {
    args: ['column', 'scale'], view: pivotedLeaf,
    make: (g, a) => patched(g, presentedOn(g, columnOf(a)), { heatmap: readColorScale(a['scale']) }),
  },
  'format.iconSet': {
    args: ['column', 'set', 'reverse?'], view: pivotedLeaf,
    make: (g, a) => patched(g, presentedOn(g, columnOf(a)), {
      iconSet: a['set'] === null ? null : readIconSet({ set: a['set'], ...(a['reverse'] !== undefined ? { reverse: a['reverse'] } : {}) }),
    }),
  },
  'format.addRule': { args: ['column', 'rule'], view: pivotedLeaf, make: (g, a) => addRule(g, columnOf(a), a['rule']) },
  // an Excel number format code on the column (format-code.ts), judged here; null takes it off
  'format.code': {
    args: ['column', 'code'], view: pivotedLeaf,
    make: (g, a) => {
      const name = presentedOn(g, columnOf(a));
      const given = a['code'];
      if (given !== null && typeof given !== 'string') throw new CubeRefusal(`a format code is text, not ${JSON.stringify(given)}`);
      if (given !== null) readFormatCode(given);
      const { code: _old, ...rest } = columnConfig(g.config, name).format ?? { kind: 'auto' as const };
      return patched(g, name, { format: given === null ? (Object.keys(rest).length > 1 || rest.kind !== 'auto' ? rest : null) : { ...rest, code: given } });
    },
  },
  'format.clear': {
    args: ['column'], view: pivotedLeaf,
    make: (g, a) => patched(g, presentedOn(g, columnOf(a)), { heatmap: null, dataBar: null, iconSet: null, rules: null }),
  },
  // the zones or the title bar folded away or brought back (a page's bar says so too)
  'view.chrome': {
    args: ['showDragZones?', 'showTitleBar?'],
    make: (_g, a) => [configure({}, {
      ...(a['showDragZones'] !== undefined ? { showDragZones: flag(a, 'showDragZones', 'the drag zones shown') } : {}),
      ...(a['showTitleBar'] !== undefined ? { showTitleBar: flag(a, 'showTitleBar', 'the title bar shown') } : {}),
    })],
  },

  // -- the groups' tree
  // SET, never toggled: the grid says which way the person asked, and asking for what already is changes nothing
  // (P2-127)
  'tree.open': { args: ['path', 'open'], make: (_g, a) => [{ kind: 'open', path: a['path'] as RowPath, open: flag(a, 'open', 'opened') }] },
  'tree.collapseAll': { args: [], make: () => [{ kind: 'collapseAll' }] },
  'tree.expandAll': { args: [], make: () => [{ kind: 'expandAll' }] },
  // a named dimension (dimensions.ts) used -- the grid grouped by its first level -- or drilled a level down or up, by
  // the group the Rows zone builds
  'view.dimension': {
    args: ['name', 'move'],
    make: (g, a) => {
      const name = text(a, 'name', "a dimension's name");
      const move = a['move'];
      if (move !== 'use' && move !== 'down' && move !== 'up') {
        throw new CubeRefusal(`a dimension is used, drilled down or drilled up, not ${JSON.stringify(move) ?? 'nothing'}`);
      }
      const found = g.dimensions.find((d) => d.name === name);
      if (found === undefined) {
        throw new CubeRefusal(`the grid has no dimension '${name}'` + (g.dimensions.length > 0
          ? ` (its dimensions: ${g.dimensions.map((d) => d.name).join(', ')})` : ' (it has none: configure its dimensions first)'));
      }
      if (availableDimensions(g.snapshot, [found]).length === 0) {
        const have = new Set(rowColumns(g.snapshot).map((c) => c.name));
        throw new CubeRefusal(`the dimension '${name}' names ${found.columns.filter((c) => !have.has(c)).join(', ')
          || 'no column'}, which the grid does not have`);
      }
      const s = g.snapshot;
      return [{ kind: 'group', rows: (move === 'use' ? useDimension(s, found) : move === 'down' ? drillDown(s, found) : drillUp(s, found)).rows }];
    },
  },

  // -- calculated columns (the column editor's Add, OK and Delete; the header menu's Delete)
  'column.calculate': {
    args: ['level', 'column'],
    make: (_g, a) => [{ kind: 'calculate', level: a['level'] as Level, column: a['column'] as DerivedColumn }],
  },
  'column.editCalculated': {
    args: ['name', 'level', 'column'],
    make: (_g, a) => [{
      kind: 'editCalculated', name: text(a, 'name', "the calculated column's name"), level: a['level'] as Level,
      column: a['column'] as DerivedColumn,
    }],
  },
  'calc.delete': { args: ['column'], make: (_g, a) => [{ kind: 'removeCalculated', name: columnOf(a) }] },
} satisfies { readonly [N in keyof GridGestureArgs]: Gesture };

/** Minimize's widths: each column not fixed, to its own minimum or MINIMIZED_WIDTH; none when every one is fixed. */
function minimized(g: GestureGrid, columns: readonly string[]): GridCommand[] {
  const sized = columns.flatMap((name) => {
    const c = columnConfig(g.config, name);
    return c.widthMode === 'fixed' ? [] : [[name, { width: c.minWidth ?? MINIMIZED_WIDTH }] as const];
  });
  return sized.length === 0 ? [] : [configure(Object.fromEntries(sized))];
}

export type GridGestureName = keyof GridGestureArgs;
/** Every grid gesture's name: what Python's grid.gesture takes. */
export const GRID_GESTURES: readonly GridGestureName[] = Object.keys(GESTURES) as GridGestureName[];

export function isGridGesture(name: unknown): name is GridGestureName {
  return typeof name === 'string' && Object.hasOwn(GESTURES, name);
}

/** The arguments a gesture takes, by name (what a menu entry carries of them it is given: its column, its operator). */
export function gestureTakes(name: GridGestureName): readonly string[] {
  const gesture: Gesture = GESTURES[name];
  return gesture.args.map((n) => n.replace(/\?$/, ''));
}

/** Whether a gesture, given `args` on a grid of `snapshot`, reads the grid's view: whoever asks it gives GestureGrid.view
 *  then (headless runs the grid for it). */
export function gestureReadsView(name: GridGestureName, snapshot: CubeSnapshot, args: GestureArgs): boolean {
  const gesture: Gesture = GESTURES[name];
  return typeof gesture.view === 'function' ? gesture.view(snapshot, args) : gesture.view === true;
}

/**
 * The commands gesture `name` makes on the grid as it is -- none when it would change nothing (a column added to the row
 * groups it is in already) -- or refused in words, as the rules it asks refuse (a header the grid does not show, a
 * dimension it has not). A screen control's way in: its arguments as GridGestureArgs types them.
 */
export function gridGesture<N extends GridGestureName>(name: N, grid: GestureGrid, args: GridGestureArgs[N]): GridCommand[] {
  return made(name, grid, args);
}

/**
 * A gesture named by text, its arguments as they come -- a menu entry's (ui/menu.ts menuGesture), Python's JSON (headless
 * gridGesture) -- made as gridGesture makes it, or refused in words: a gesture a grid has not, arguments not given by
 * name, one it does not take, one it needs and is not given, one that is not what it takes. Given from JSON, read its
 * commands (readGridCommand) before applying them.
 */
export function gridGestureGiven(name: string, grid: GestureGrid, given: unknown): GridCommand[] {
  return made(gestureNamed(name), grid, argumentsGiven(given));
}

/** A gesture named by text, or refused in words, naming every gesture a grid has. */
export function gestureNamed(name: string): GridGestureName {
  if (!isGridGesture(name)) throw new CubeRefusal(`'${name}' is no gesture of a grid's: ${GRID_GESTURES.join(', ')}`);
  return name;
}

/** A gesture's arguments as they come: by name, or refused in words. */
export function argumentsGiven(given: unknown): GestureArgs {
  if (typeof given !== 'object' || given === null || Array.isArray(given)) {
    throw new CubeRefusal(`a gesture's arguments are given by name, not ${JSON.stringify(given) ?? 'nothing'}`);
  }
  return given as GestureArgs;
}

function made(name: GridGestureName, grid: GestureGrid, args: GestureArgs): GridCommand[] {
  const gesture: Gesture = GESTURES[name];
  const takes = gestureTakes(name);
  const odd = Object.keys(args).filter((k) => !takes.includes(k));
  if (odd.length > 0) throw new CubeRefusal(`'${name}' takes ${takes.join(', ') || 'nothing'}, not ${odd.join(', ')}`);
  const missing = gesture.args.filter((n) => !n.endsWith('?') && args[n] === undefined);
  if (missing.length > 0) throw new CubeRefusal(`'${name}' needs ${missing.join(', ')}`);
  return gesture.make(grid, args);
}
