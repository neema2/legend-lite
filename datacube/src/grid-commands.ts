// A GRID EDIT AS A VALUE (docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md; the command program's stage 1): each edit of a
// grid a person or a program makes is a GridCommand -- plain JSON, so Python sends it and a live channel can carry it --
// and one pure function applies it, by grid-edits.ts and config.ts, the screen's own rules. The screen builds the
// command from a click, Python from a call; neither holds a rule of its own, so the two cannot make one edit two ways.
// No DOM, no planner: what needs the compiler (a calculated column's parse) is done before the command is built.

import { familyOf } from '../../engine-client/src/types.ts';
import { nameProblem } from './calc.ts';
import { readColorScale, readDataBar, readIconSet, readRule } from './conditional.ts';
import { readFormatCode } from './format-code.ts';
import { fieldNames, type CommandFields, type MissingFields } from './command-fields.ts';
import {
  APPEARANCE_SETTINGS, COLUMN_SETTINGS, CUBE_SETTINGS, columnConfig, renameColumnConfig, withColumn, withConfiguration,
  type CubeConfiguration, type Patch,
} from './config.ts';
import {
  addSort, allColumns, clearSort, clickSort, clickTreeSort, columnKind, pinPivot, pivotHeaderMeasure, setColumn, setGrouping, setPivot,
  setSorts, spreadMeasureNames, type ColumnChange, type GridDraft,
} from './grid-edits.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { defaultKind } from '../../engine-client/src/types.ts';
import { operandKind } from './filter-operators.ts';
import {
  AGGREGATE_FNS, CHILD_AGGREGATE_FNS, COLUMN_KINDS, CubeRefusal, FILTER_OPERATORS, GRAIN_ATS, PATH_AGGREGATES, SORT_DIRECTIONS,
  WINDOW_FUNCTIONS, isJsonValue,
  isPivotTotalColumn, isRelativeDate, rowColumns, pivotTotalColumn, readTopN, withCalculated, withoutConditions,
  type AggregateFn, type ChildAggregate, type ColumnKind, type DerivedColumn, type Grain, type RelatedPath, type FilterNode, type FilterOperator,
  type FilterValue, type Measure, type SortDirection, type SortSpec, type TopN, type WindowFrame, type WindowFunction,
  type WindowSpec,
  type CubeSnapshot,
  type ValueGroups,
} from './snapshot.ts';
import { TREE_COLUMN } from './treeview.ts';
import { isDetailPath, TreeState, type RowPath } from './tree.ts';

/** Settings by their document's own keys, each cleared by null (a value JSON can carry, as undefined is not). */
export type SettingsPatch = Readonly<Record<string, unknown>>;

/** One edit of a grid. */
export type GridCommand =
  /** The row groups, whole (the Rows zone, the menu's Vertical Pivot, Python's group). */
  | { readonly kind: 'group'; readonly rows: readonly string[] }
  /** The pivot, whole (the Columns zone, the menu's Horizontal Pivot, Python's pivot). */
  | { readonly kind: 'pivot'; readonly columns: readonly string[] }
  /** The pivot's values pinned, its columns those (a Cross Tab's Load values, FL2: grid-edits.ts pinPivot); null, its
   *  values its rows' each time it runs. */
  | { readonly kind: 'pinPivot'; readonly values: readonly FilterValue[] | null }
  /** The sorts, whole (the Sorts panel, the menu's Ascending, Descending and Clear). */
  | { readonly kind: 'sorts'; readonly sorts: readonly SortSpec[] }
  /** A sort added last (the menu's Add Ascending and Add Descending, Python's sort). */
  | { readonly kind: 'addSort'; readonly column: string; readonly direction: SortDirection }
  /** A column's sort taken off, the others kept (the menu's Clear Sort, Python's clear_sort). */
  | { readonly kind: 'clearSort'; readonly column: string }
  /**
   * A header click: alone, or chained after the other sorts. `first` is the direction a click on it starts with, which
   * the type the compiler gave the header decides (grid-edits.ts firstSortDirection): a pivot's header, or a count of
   * text, is a number though no column of the query is -- so the screen, which has the view, says it.
   */
  | { readonly kind: 'clickSort'; readonly column: string; readonly first: SortDirection; readonly chained: boolean }
  /** A click on the tree column's header. */
  | { readonly kind: 'clickTreeSort'; readonly chained: boolean }
  /** Column Properties: a column's kind, Aggregation, weighted average's weight (null clears the last two). */
  | { readonly kind: 'column'; readonly name: string; readonly change: ColumnChange }
  /** Settings, an appearance, a column's presentation, in the saved document's keys (null clears one). */
  | { readonly kind: 'configure'; readonly patch: SettingsPatch }
  /** The filter, whole (the filter editor, Python's filter); null takes it off. */
  | { readonly kind: 'filter'; readonly filter: FilterNode | null }
  /** One condition added to the filter (a cell's menu): and-ed with what it had. */
  | { readonly kind: 'addFilter'; readonly condition: FilterNode }
  /** A calculated column added at its level, its lambda already parsed (the column editor, Python's calculate). */
  /** Conditions taken off the filter and others ANDed on, as one change (a chart's mark clicked: the selection it had
   *  for the one it has now) -- each condition taken off where it stands, the rest of the filter kept as it is. */
  | { readonly kind: 'refilter'; readonly remove: readonly FilterNode[]; readonly add: readonly FilterNode[] }
  /** `before`: the calculated column of its level the new one goes before (an undo putting a column back in its place:
   *  grid-between.ts); left out, last of its level, as the editor adds one (D19, 2026-10-24). */
  | { readonly kind: 'calculate'; readonly level: 'row' | 'group'; readonly column: DerivedColumn; readonly before?: string }
  /** A calculated column edited (Edit Column's OK): the column `name` replaced by `column`, computed at `level` -- in its
   *  place when it stays at its level, else last of its new one, or before `before` (D19); renamed, what read it (the
   *  groups, the pivot, the sorts, the filter, the columns computed from it, its settings) reads it by its new name. */
  | {
    readonly kind: 'editCalculated'; readonly name: string; readonly level: 'row' | 'group'; readonly column: DerivedColumn;
    readonly before?: string;
  }
  /** A calculated column taken out, whichever level it is at (Edit Column's Delete, the header menu's), its settings with
   *  it: nothing is kept where nothing reads it (D19, 2026-10-24). */
  | { readonly kind: 'removeCalculated'; readonly name: string }
  /** A group opened or closed (its chevron): its path, the group keys from the top down -- SET, never toggled, so a
   *  click repeated while a query runs asks the same thing twice (P2-127). Saved with the page, so a page shared or
   *  reopened shows the groups its maker had open. */
  | { readonly kind: 'open'; readonly path: RowPath; readonly open: boolean }
  /** Every group closed, the expand level's too (the menu's Collapse All). */
  | { readonly kind: 'collapseAll' }
  /** Every group above the detail rows open, as an Excel pivot's Expand Entire Field (the menu's Expand All). */
  | { readonly kind: 'expandAll' }
  /** The top or bottom N rows or groups by a column (snapshot.ts TopN: the menu's Top 10, Python's grid.top); null
   *  takes it off. */
  | { readonly kind: 'topN'; readonly topN: TopN | null }
  /**
   * The open groups set whole, in the saved tree's own shape (cube-document.ts CubeDocument.tree): the groups open, the
   * grand total shown, the level the groups open to and the groups closed under it. What puts a collapsed tree back --
   * an undo of Collapse All (grid-between.ts), Python's grid.tree -- which no other command can: `collapseAll` forgets
   * the level and the closes, and a `configure` of the same level moves nothing (D19, 2026-10-24).
   */
  | {
    readonly kind: 'tree'; readonly open: readonly RowPath[]; readonly showTotals: boolean; readonly expandTo?: number;
    readonly closed?: readonly RowPath[];
  };

export const GRID_COMMAND_KINDS = [
  'group', 'pivot', 'pinPivot', 'sorts', 'addSort', 'clearSort', 'clickSort', 'clickTreeSort', 'column', 'configure', 'filter', 'addFilter',
  'refilter', 'calculate', 'editCalculated', 'removeCalculated', 'open', 'collapseAll', 'expandAll', 'topN', 'tree',
] as const satisfies readonly GridCommand['kind'][];

/** Each grid command's fields, by kind: what readGridCommand takes, and what Python's generated builders write (D17). */
export const GRID_COMMAND_FIELDS = {
  group: ['rows'], pivot: ['columns'], pinPivot: ['values'], sorts: ['sorts'], addSort: ['column', 'direction'], clearSort: ['column'],
  clickSort: ['column', 'first', 'chained'], clickTreeSort: ['chained'], column: ['name', 'change'], configure: ['patch'],
  filter: ['filter'], addFilter: ['condition'], refilter: ['remove', 'add'], calculate: ['level', 'column', 'before?'],
  editCalculated: ['name', 'level', 'column', 'before?'], removeCalculated: ['name'], open: ['path', 'open'],
  collapseAll: [], expandAll: [], topN: ['topN'], tree: ['open', 'showTotals', 'expandTo?', 'closed?'],
} as const satisfies CommandFields<GridCommand>;
// every field of every kind listed: a field the list leaves out is a type error naming it
const gridFieldsWhole: [MissingFields<GridCommand, typeof GRID_COMMAND_FIELDS>] extends [never] ? true
  : MissingFields<GridCommand, typeof GRID_COMMAND_FIELDS> = true;
void gridFieldsWhole;

/** A command applied: the grid as the screen's rules leave it, or a refusal in their words (CubeRefusal). */
export function applyGridCommand<D extends GridDraft>(draft: D, command: GridCommand): D {
  switch (command.kind) {
    case 'group':
      return setGrouping(draft, command.rows);
    case 'pivot':
      return setPivot(draft, command.columns);
    case 'pinPivot':
      return pinPivot(draft, command.values);
    case 'sorts':
      return setSorts(draft, command.sorts);
    case 'addSort':
      return addSort(draft, command.column, command.direction);
    case 'clearSort':
      return clearSort(draft, command.column);
    case 'clickSort':
      return clickSort(draft, command.column, command.first, command.chained);
    case 'clickTreeSort':
      return clickTreeSort(draft, command.chained);
    case 'column':
      return setColumn(draft, command.name, command.change);
    case 'configure':
      return configured(draft, command.patch);
    case 'filter': {
      const { filter: _was, ...rest } = draft.snapshot;
      return { ...draft, snapshot: command.filter === null ? rest : { ...rest, filter: command.filter } };
    }
    case 'addFilter': {
      // ADDS to the filter rather than replacing it, which is what "Add Filter" says and what makes the entry usable
      // twice in a row: the new condition joins the existing tree with AND; an OR is the filter editor's job
      const was = draft.snapshot.filter;
      const filter: FilterNode = was
        ? was.kind === 'and' ? { kind: 'and', children: [...was.children, command.condition] }
          : { kind: 'and', children: [was, command.condition] }
        : command.condition;
      return { ...draft, snapshot: { ...draft.snapshot, filter } };
    }
    case 'refilter': {
      const kept = withoutConditions(draft.snapshot.filter, command.remove) ?? undefined;
      const children = [...(kept === undefined ? [] : kept.kind === 'and' ? kept.children : [kept]), ...command.add];
      const { filter: _was, ...rest } = draft.snapshot;
      return { ...draft, snapshot: children.length === 0 ? rest
        : { ...rest, filter: children.length === 1 ? children[0]! : { kind: 'and', children } } };
    }
    case 'calculate': {
      const problem = nameProblem(draft.snapshot, command.level, command.column.name);
      if (problem !== null) throw new CubeRefusal(problem);
      if (command.column.related !== undefined && command.level !== 'row') {
        throw new CubeRefusal(`'${command.column.name}' is a related class's property: a row-level column, projected with the class's own`);
      }
      groupsProblem(draft.snapshot, command.level, command.column);
      return { ...draft, snapshot: withCalculated(draft.snapshot, undefined, command.level, command.column, command.before) };
    }
    case 'editCalculated': {
      const was = calculatedNamed(draft, command.name);
      groupsProblem(draft.snapshot, command.level, command.column);
      const problem = nameProblem(draft.snapshot, command.level, command.column.name, command.name);
      if (problem !== null) throw new CubeRefusal(problem);
      if (command.before === command.name) throw new CubeRefusal(`'${command.name}' cannot go before itself`);
      // one grouped or pivoted on keeps its kind and its level, as Column Properties locks a source column's (grid-edits.ts
      // setColumn): a grid grouped by a measure, or by a column computed after the grouping, is no grid (D19, 2026-10-24)
      if (draft.snapshot.rows.includes(command.name) || draft.snapshot.pivotOn.includes(command.name)) {
        const level = draft.snapshot.derived.includes(was) ? 'row' : 'group';
        const kind = (d: DerivedColumn): ColumnKind => d.kind ?? defaultKind(d.type);
        if (level !== command.level || kind(was) !== kind(command.column)) {
          throw new CubeRefusal(`'${command.name}' is grouped or pivoted on: its kind and level cannot change while it is`);
        }
      }
      const renamed = command.column.name !== command.name;
      return {
        ...draft,
        snapshot: withCalculated(draft.snapshot, command.name, command.level, command.column, command.before),
        // its settings (a format, a width, a display name) follow it
        ...(renamed ? { config: renameColumnConfig(draft.config, command.name, command.column.name) } : {}),
      };
    }
    case 'removeCalculated': {
      calculatedNamed(draft, command.name);
      const s = draft.snapshot;
      // another column reading it: the compiler refuses the grid, in its words, when the change is planned. Its settings
      // go with it: a column made again under its name starts as a new column does (D19)
      const { [command.name]: _gone, ...columns } = draft.config.columns;
      return { ...draft, snapshot: { ...s, derived: s.derived.filter((d) => d.name !== command.name),
        groupDerived: (s.groupDerived ?? []).filter((d) => d.name !== command.name) }, config: { ...draft.config, columns } };
    }
    case 'open': {
      if (command.path.length === 0) throw new CubeRefusal('the grand total is no group to open: it is shown or not (Show Root Aggregation)');
      if (isDetailPath(command.path)) throw new CubeRefusal('a detail row is a source row, not a group: it does not open');
      // a path deeper than the row groups names no group of the grid now (another editor regrouped it since the click):
      // nothing to open, and no refusal of an edit that was right when it was made
      if (command.path.length > draft.snapshot.rows.length) return draft;
      return { ...draft, tree: shownTree(draft).setOpen(command.path, command.open) };
    }
    case 'collapseAll':
      return { ...draft, tree: shownTree(draft).collapseAll() };
    case 'expandAll':
      return { ...draft, tree: shownTree(draft).expandAll(draft.snapshot.rows.length) };
    case 'topN': {
      const { topN: _was, ...rest } = draft.snapshot;
      if (command.topN === null) return { ...draft, snapshot: rest };
      if (draft.snapshot.pivotOn.length > 0) {
        throw new CubeRefusal("a top N ranks the grid's rows or groups; a pivot's figures are its values' columns: take the pivot off first");
      }
      return { ...draft, snapshot: { ...rest, topN: command.topN } };
    }
    case 'tree': {
      shownTree(draft);
      for (const path of [...command.open, ...(command.closed ?? [])]) {
        if (path.length === 0) throw new CubeRefusal('the grand total is no group to open: it is shown or not (showTotals)');
        if (isDetailPath(path)) throw new CubeRefusal('a detail row is a source row, not a group: it does not open');
      }
      const expandTo = command.expandTo ?? 0;
      if (!Number.isInteger(expandTo) || expandTo < 0) throw new CubeRefusal(`the level the groups open to is a whole number, 0 or more, not ${JSON.stringify(expandTo)}`);
      return { ...draft, tree: TreeState.saved(command.open, command.showTotals, expandTo, command.closed ?? []) };
    }
  }
}

/** The calculated column `name` of the grid, or refused in words (a source's column is edited in Column Properties, not
 *  in the column editor). */
function calculatedNamed(draft: GridDraft, name: string): DerivedColumn {
  const calculated = [...draft.snapshot.derived, ...(draft.snapshot.groupDerived ?? [])];
  const column = calculated.find((d) => d.name === name);
  if (column === undefined) {
    throw new CubeRefusal(`'${name}' is not a calculated column of the grid (${calculated.map((d) => d.name).join(', ') || 'it has none'})`);
  }
  return column;
}

/** The open groups of a grid as it shows: an editor's draft has none to open (no screen builds it such a command). */
function shownTree(draft: GridDraft): TreeState {
  if (draft.tree === undefined) throw new Error("a group opened in an editor's draft, which holds no open groups");
  return draft.tree;
}

/** Commands applied in order: the first refusal stops them, in its words. */
export function applyGridCommands<D extends GridDraft>(draft: D, commands: readonly GridCommand[]): D {
  return commands.reduce<D>((d, c) => applyGridCommand(d, c), draft);
}

// ---------------------------------------------------------------- an editor's draft, as commands

/**
 * THE COMMANDS AN EDITOR'S DRAFT MAKES (the Properties window's Apply, app.ts #applyDraft): what changed between `base`,
 * the grid the window opened on, and `edited`, its draft -- so, applied to the grid as it is NOW, they land only what the
 * window changed, and an edit another window made meanwhile stays. In the order a person makes them: a column made a
 * dimension (it can then be grouped); the row groups and the pivot; the other kinds, Aggregations and weights (a column
 * ungrouped can then be a measure); the settings the draft changed beyond what those set (null clears one); the sorts
 * last (one may name a pivot's header the settings decide). A list is set whole; the settings key by key -- the grid's,
 * its appearance's, each column's. Refused, in the screen's words, when the draft is not one its rules reach.
 */
export function draftCommands(base: GridDraft, edited: GridDraft): GridCommand[] {
  const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);
  // Column Properties: a kind, an Aggregation, a weight -- by the column command, its rules setColumn's
  const early: GridCommand[] = [];
  const late: GridCommand[] = [];
  for (const name of new Set([...Object.keys(base.config.columns), ...Object.keys(edited.config.columns)])) {
    const was = columnConfig(base.config, name);
    const now = columnConfig(edited.config, name);
    const aggregation: ColumnChange = {
      ...(!same(now.aggregateFn, was.aggregateFn) ? { aggregate: now.aggregateFn ?? null } : {}),
      ...(!same(now.aggregationParameters, was.aggregationParameters) ? { weight: now.aggregationParameters?.[0] ?? null } : {}),
    };
    // a dimension keeps the Aggregation it had as a measure (setColumn), but only a measure is given one: set as a
    // measure, then made the dimension it ends as (2026-10-24, found by the undo's random walks: Aggregation, then
    // dimension, was a draft no command list reached)
    if (now.kind === 'dimension' && Object.keys(aggregation).length > 0) {
      early.push({ kind: 'column', name, change: { ...(columnKind(base, name) === 'dimension' ? { kind: 'measure' } : {}), ...aggregation } });
      early.push({ kind: 'column', name, change: { kind: 'dimension' } });
      continue;
    }
    const change: ColumnChange = {
      // a kind is set, never cleared: one cleared is left to configure, which refuses it in words
      ...(now.kind !== undefined && now.kind !== was.kind ? { kind: now.kind } : {}),
      ...aggregation,
    };
    if (Object.keys(change).length === 0) continue;
    (change.kind === 'dimension' ? early : late).push({ kind: 'column', name, change });
  }
  const structure: GridCommand[] = [
    ...early,
    ...(same(base.snapshot.rows, edited.snapshot.rows) ? [] : [{ kind: 'group', rows: [...edited.snapshot.rows] } as const]),
    ...(same(base.snapshot.pivotOn, edited.snapshot.pivotOn) ? [] : [{ kind: 'pivot', columns: [...edited.snapshot.pivotOn] } as const]),
    ...late,
  ];
  // the settings beyond those: what the draft has that the grid it opened on, so edited, has not
  const patch = settingsPatch(applyGridCommands(base, structure).config, edited.config);
  return [
    ...structure,
    ...(patch === undefined ? [] : [{ kind: 'configure', patch } as const]),
    ...(same(base.snapshot.sorts, edited.snapshot.sorts) ? []
      : [{ kind: 'sorts', sorts: edited.snapshot.sorts.map((x) => ({ column: x.column, direction: x.direction })) } as const]),
  ];
}

/** What `to` sets that `from` has not, as a configure command's patch (null clears a setting), or undefined: nothing. */
export function settingsPatch(from: CubeConfiguration, to: CubeConfiguration): SettingsPatch | undefined {
  const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);
  const changed = (a: object, b: object): Record<string, unknown> | undefined => {
    const was = a as Record<string, unknown>;
    const now = b as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const key of new Set([...Object.keys(was), ...Object.keys(now)])) {
      if (!same(was[key], now[key])) out[key] = now[key] === undefined ? null : now[key];
    }
    return Object.keys(out).length > 0 ? out : undefined;
  };
  const { columns: fromColumns, appearance: fromAppearance, ...fromSettings } = from;
  const { columns: toColumns, appearance: toAppearance, ...toSettings } = to;
  const settings = changed(fromSettings, toSettings);
  const appearance = changed(fromAppearance, toAppearance);
  const columns: Record<string, unknown> = {};
  for (const name of new Set([...Object.keys(fromColumns), ...Object.keys(toColumns)])) {
    const one = changed(columnConfig(from, name), columnConfig(to, name));
    if (one !== undefined) columns[name] = one;
  }
  const patch = {
    ...settings,
    ...(appearance !== undefined ? { appearance } : {}),
    ...(Object.keys(columns).length > 0 ? { columns } : {}),
  };
  return Object.keys(patch).length > 0 ? patch : undefined;
}

// ---------------------------------------------------------------- configure

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/**
 * Settings patched as a host patches them (config.ts withConfiguration), in the
 * saved document's own keys: null clears a setting, among the settings, in the appearance and in a column's. A column's
 * kind, Aggregation and weight go by Column Properties' own rules (setColumn), so what the panel refuses is refused here
 * too: a kind is set, never cleared, and whether a measure is kept out of the pivot is true or false. A name DataCube
 * does not have (a setting misspelt, a column the grid has not) is refused, never kept where nothing reads it.
 */
function configured<D extends GridDraft>(draft: D, given: SettingsPatch): D {
  if (!isObject(given)) throw new CubeRefusal("a grid's configuration is its settings by name, in its document's own keys");
  const cleared = (o: Record<string, unknown>): Record<string, unknown> =>
    Object.fromEntries(Object.entries(o).map(([k, v]) => [k, v === null ? undefined : v]));
  const patched = (d: D, patch: Record<string, unknown>): D =>
    ({ ...d, config: withConfiguration(d.config, patch as Patch<CubeConfiguration>) });
  const { columns, appearance, ...settings } = given;
  const known = (keys: readonly string[], settings: object, what: string): void => {
    const odd = keys.find((k) => !Object.hasOwn(settings, k));
    if (odd !== undefined) throw new CubeRefusal(`'${odd}' is not ${what} (${Object.keys(settings).join(', ')})`);
  };
  known(Object.keys(settings), CUBE_SETTINGS, "a grid's setting");
  if (appearance !== undefined && !isObject(appearance)) {
    throw new CubeRefusal("a grid's appearance is its settings by name, each cleared by null, not cleared whole");
  }
  if (isObject(appearance)) known(Object.keys(appearance), APPEARANCE_SETTINGS, 'an appearance setting');
  let d = treed(patched(draft, { ...cleared(settings), ...(isObject(appearance) ? { appearance: cleared(appearance) } : {}) }),
    draft.config);
  if (columns === undefined) return d;
  if (!isObject(columns)) throw new CubeRefusal("a grid's columns' settings are by name: {name: {setting: value}}");
  for (const [name, c] of Object.entries(columns)) {
    const named = allColumns(d);
    // a pivot total is a column of its own (its heatmap, width, hiding: the screen's menu offers them), of a measure
    // column the grid has or of the count a pivot of no measure fills with; so is that count (its heatmap, which its
    // pivot's columns show), a pivot value's column while the grid pivots (its hiding), and the tree column (its width,
    // hiding) -- each its presentation alone: a header's kind and Aggregation are its measure's
    const own = named.includes(name);
    const spread = spreadMeasureNames(d);
    const filled = !own && spread.includes(name);
    const total = !own && isPivotTotalColumn(name) ? name.slice(pivotTotalColumn('').length) : undefined;
    const header = !own && !filled && total === undefined ? pivotHeaderMeasure(d, name) : undefined;
    const tree = !own && name === TREE_COLUMN;
    const totalled = total !== undefined && (named.includes(total) || spread.includes(total));
    if (!own && !filled && !totalled && header === undefined && !tree) {
      // settings of a column the grid has not, each CLEARED (every value null): taken off, as nothing reads them -- a
      // page saved before 2026-10-24 kept a calculated column's settings past its removal (D19); anything set is refused
      if (isObject(c) && Object.keys(c).length > 0 && Object.values(c).every((v) => v === null)) {
        d = patched(d, { columns: { [name]: cleared(c) } });
        continue;
      }
      throw new CubeRefusal(`'${name}' is not a column of the grid (${named.join(', ')})`);
    }
    if (!isObject(c)) throw new CubeRefusal(`a column's settings are an object, not ${JSON.stringify(c)}`);
    known(Object.keys(c), COLUMN_SETTINGS, "a column's setting");
    const { kind, aggregateFn, aggregationParameters, excludedFromPivot, ...presentation } = c;
    if (!own && [kind, aggregateFn, aggregationParameters, excludedFromPivot].some((x) => x !== undefined)) {
      const of = total ?? header;
      throw new CubeRefusal(of !== undefined
        ? `${name} is ${total !== undefined ? 'the pivot total' : 'a pivot column'} of '${of}': its kind, Aggregation `
          + `and pivot exclusion are '${of}''s, set on '${of}'`
        : `${name} is ${tree ? "the grid's tree column" : 'the count a pivot of no measure fills with'}: it has no kind, `
          + 'Aggregation or pivot exclusion');
    }
    if (kind === null) {
      // a kind CLEARED (D19, 2026-10-24: what an undo puts back for a column that had none): the column's as its source
      // gives it, from its configuration and its query both -- the query keeps a kind the configuration has none for
      // (config.ts applyToSnapshot), so the one cleared leaves both. A calculated column's kind is its editor's; one
      // grouped or pivoted on keeps its kind, as Column Properties locks it (grid-edits.ts setColumn)
      if (!d.snapshot.columns.some((x) => x.name === name)) {
        throw new CubeRefusal(`'${name}' is a calculated column: its kind is set in its editor (Edit Column)`);
      }
      if (d.snapshot.rows.includes(name) || d.snapshot.pivotOn.includes(name)) {
        throw new CubeRefusal(`'${name}' is grouped or pivoted on: its kind cannot change while it is`);
      }
      const columns = d.snapshot.columns.map((x) => {
        if (x.name !== name) return x;
        const { kind: _was, excludedFromPivot: _kept, ...rest } = x;
        return rest;
      });
      d = { ...d, snapshot: { ...d.snapshot, columns }, config: withColumn(d.config, name, { kind: undefined, excludedFromPivot: undefined }) };
    }
    if (excludedFromPivot !== undefined && excludedFromPivot !== null && typeof excludedFromPivot !== 'boolean') {
      throw new CubeRefusal(`${name}'s excludedFromPivot is true or false, not ${JSON.stringify(excludedFromPivot)}`);
    }
    // conditional formats (conditional.ts): each as its reader takes it, or refused in its words
    const { heatmap, dataBar, iconSet, rules, format } = presentation as Record<string, unknown>;
    try {
      // as the saved cube's reader takes one (cube-document.ts): null is no code, refused -- a format without one has none
      const code = isObject(format) ? format['code'] : undefined;
      if (code !== undefined) {
        if (typeof code !== 'string') throw new CubeRefusal(`a format code is text, not ${JSON.stringify(code)}`);
        readFormatCode(code);
      }
      if (heatmap !== undefined && heatmap !== null) readColorScale(heatmap);
      if (dataBar !== undefined && dataBar !== null) readDataBar(dataBar);
      if (iconSet !== undefined && iconSet !== null) readIconSet(iconSet);
      if (rules !== undefined && rules !== null) {
        if (!Array.isArray(rules)) throw new CubeRefusal(`rules are a list, each {when, style}, not ${JSON.stringify(rules)}`);
        rules.forEach(readRule);
      }
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      throw new CubeRefusal(`${name}: ${e.message}`);
    }
    if ((kind !== undefined && kind !== null) || aggregateFn !== undefined || aggregationParameters !== undefined) {
      d = setColumn(d, name, {
        ...(kind !== undefined && kind !== null ? { kind: kindFrom(kind) } : {}),
        ...(aggregateFn !== undefined ? { aggregate: aggregateFn === null ? null : aggregateFrom(aggregateFn) } : {}),
        ...(aggregationParameters !== undefined ? { weight: weightFrom(aggregationParameters, name) } : {}),
      });
    }
    // an exclusion cleared (null) is one taken off: the configuration's alone, which the fold reads (D19)
    d = patched(d, { columns: { [name]: { ...cleared(presentation), ...(excludedFromPivot !== undefined ? { excludedFromPivot: excludedFromPivot ?? undefined } : {}) } } });
  }
  return d;
}

/**
 * The tree moved by the settings that decide it, as they change from `was` (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
 * G3): "Show root aggregation" shows the grand total or not, "Initially expand to level" opens the groups to that depth
 * as they load (upstream's isServerSideGroupOpenByDefault) -- in the same change as the settings, so the screen's
 * Properties (which sends only what changed) and a host's configure move it alike: a setting given again as it is moves
 * nothing, the groups closed under its level staying closed (the S2 audit's 4, F3). A draft of no open groups (an
 * editor's) has no tree to move.
 */
function treed<D extends GridDraft>(draft: D, was: CubeConfiguration): D {
  if (draft.tree === undefined) return draft;
  let tree = draft.tree;
  if (draft.config.showRootAggregation !== was.showRootAggregation) tree = tree.withTotals(draft.config.showRootAggregation);
  const level = draft.config.initialExpandToLevel ?? 0;
  if (level !== (was.initialExpandToLevel ?? 0)) tree = tree.withExpandTo(level);
  return tree === draft.tree ? draft : { ...draft, tree };
}

// ---------------------------------------------------------------- reading a command from JSON

/** A field as text, or why not. */
function textFrom(v: unknown, what: string): string {
  if (typeof v !== 'string') throw new CubeRefusal(`${what} is text, not ${JSON.stringify(v)}`);
  return v;
}

/** A list of the grid's columns (a part named `what`), or why not: the edit judges the names. */
function namesFrom(given: unknown, what: string): string[] {
  if (!Array.isArray(given) || !given.every((n) => typeof n === 'string')) {
    throw new CubeRefusal(`${what} is a list of the grid's columns, not ${JSON.stringify(given)}`);
  }
  return given;
}

/** One of DataCube's aggregates, or why not. */
function aggregateFrom(fn: unknown): AggregateFn {
  if (typeof fn !== 'string' || !AGGREGATE_FNS.includes(fn as AggregateFn)) {
    throw new CubeRefusal(`${JSON.stringify(fn)} is not one of DataCube's aggregates: ${AGGREGATE_FNS.join(', ')}`);
  }
  return fn as AggregateFn;
}

/** A column's kind, or why not. */
function kindFrom(kind: unknown): ColumnKind {
  if (typeof kind !== 'string' || !COLUMN_KINDS.includes(kind as ColumnKind)) {
    throw new CubeRefusal(`a column's kind is ${COLUMN_KINDS.map((k) => `'${k}'`).join(' or ')}, not ${JSON.stringify(kind)}`);
  }
  return kind as ColumnKind;
}

/** A weighted average's parameters as the document keeps them, `[weight column]` -- or null, cleared -- or why not. */
function weightFrom(given: unknown, name: string): string | null {
  if (given === null) return null;
  if (!Array.isArray(given) || given.length !== 1 || typeof given[0] !== 'string') {
    throw new CubeRefusal(`${name}'s aggregationParameters is [its weight column], not ${JSON.stringify(given)}`);
  }
  return given[0];
}

/** A sort direction, or why not. */
function directionFrom(v: unknown): SortDirection {
  if (typeof v !== 'string' || !SORT_DIRECTIONS.includes(v as SortDirection)) {
    // in setSorts' own words, as Python has always been told
    throw new CubeRefusal(`a sort is ${SORT_DIRECTIONS.map((d) => `'${d}'`).join(' or ')}, not '${String(v)}'`);
  }
  return v as SortDirection;
}

/** A command's fields, exactly: a field it does not take is refused, never passed over. */
function fields(o: Record<string, unknown>, kind: string, listed: readonly string[], what = `a '${kind}' command`): void {
  const allowed = fieldNames(listed);
  const odd = Object.keys(o).filter((k) => k !== 'kind' && !allowed.includes(k));
  if (odd.length > 0) throw new CubeRefusal(`${what} takes ${allowed.join(', ') || 'nothing more'}, not ${odd.join(', ')}`);
}

/** A filter value, or why not: text, a number, true or false, a date relative to today or now, or a JSON document. */
function filterValueFrom(v: unknown): FilterValue {
  if (typeof v === 'string' || typeof v === 'boolean' || (typeof v === 'number' && Number.isFinite(v))
    || isRelativeDate(v) || isJsonValue(v)) return v;
  throw new CubeRefusal(`a filter's value is text, a number, true or false, {relative} or {json}, not ${JSON.stringify(v)}`);
}

/**
 * A filter read field by field -- a condition (its operator one of DataCube's, its operand the one the operator takes:
 * none, a value, a list, another column), a group of them, a not -- or why not. What it names (a column the grid has)
 * is the apply's to judge.
 */
export function readFilterNode(v: unknown): FilterNode {
  if (!isObject(v)) throw new CubeRefusal(`a filter is a condition, a group or a not, not ${JSON.stringify(v)}`);
  switch (v['kind']) {
    case 'condition': {
      fields(v, 'condition', ['column', 'operator', 'value', 'rightColumn'], 'a condition');
      const operator = v['operator'];
      if (typeof operator !== 'string' || !FILTER_OPERATORS.includes(operator as FilterOperator)) {
        throw new CubeRefusal(`a condition's operator is one of ${FILTER_OPERATORS.join(', ')}, not ${JSON.stringify(operator)}`);
      }
      const op = operator as FilterOperator;
      const takes = operandKind(op);
      const { value, rightColumn } = v;
      const fits = takes === 'none' ? value === undefined && rightColumn === undefined
        : takes === 'single' ? value !== undefined && !Array.isArray(value) && rightColumn === undefined
          : takes === 'list' ? Array.isArray(value) && rightColumn === undefined
            : rightColumn !== undefined && value === undefined;
      if (!fits) {
        const operand = { none: 'no operand', single: 'a value', list: 'a list of values', column: 'another column (rightColumn)' };
        throw new CubeRefusal(`'${op}' takes ${operand[takes]}`);
      }
      return {
        kind: 'condition', column: textFrom(v['column'], "a condition's column"), operator: op,
        ...(value !== undefined ? { value: Array.isArray(value) ? value.map(filterValueFrom) : filterValueFrom(value) } : {}),
        ...(rightColumn !== undefined ? { rightColumn: textFrom(rightColumn, "a condition's other column") } : {}),
      };
    }
    case 'and':
    case 'or': {
      fields(v, v['kind'], ['children'], 'a group');
      if (!Array.isArray(v['children'])) throw new CubeRefusal("a group's children are a list of filters");
      return { kind: v['kind'], children: v['children'].map(readFilterNode) };
    }
    case 'not':
      fields(v, 'not', ['child'], 'a not');
      return { kind: 'not', child: readFilterNode(v['child']) };
    default:
      throw new CubeRefusal(`a filter is a condition, an and, an or or a not, not ${JSON.stringify(v['kind'])}`);
  }
}

/** A group's path -- its keys from the top down, each text or null -- or why not. */
function pathFrom(v: unknown): RowPath {
  if (!Array.isArray(v) || !v.every((k) => k === null || typeof k === 'string')) {
    throw new CubeRefusal(`a group's path is its keys from the top down, each text or null, not ${JSON.stringify(v)}`);
  }
  return v as RowPath;
}

/** The calculated column a new or edited one goes before (`before`), as given, or nothing. */
function beforeFrom(v: unknown): { before?: string } {
  return v === undefined ? {} : { before: textFrom(v, 'the calculated column it goes before') };
}

/** A whole number of 1 or more (a window's offset, buckets, rows), or why not. */
function countFrom(v: unknown, what: string): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 1) throw new CubeRefusal(`${what} is a whole number, 1 or more, not ${JSON.stringify(v)}`);
  return v;
}

/** A window frame -- 'running', 'partition', or the last so many rows -- or why not. */
function frameFrom(v: unknown): WindowFrame {
  if (v === 'running' || v === 'partition') return v;
  if (isObject(v)) {
    fields(v, 'frame', ['lastRows'], "a window's frame");
    if (typeof v['lastRows'] !== 'number' || !Number.isInteger(v['lastRows']) || v['lastRows'] < 1) {
      throw new CubeRefusal(`a window's lastRows is a whole number of rows, 1 or more, not ${JSON.stringify(v['lastRows'])}`);
    }
    return { lastRows: v['lastRows'] };
  }
  throw new CubeRefusal(`a window's frame is 'running', 'partition' or {lastRows}, not ${JSON.stringify(v)}`);
}

/** A window column's spec, field by field: what it names, the column editor's rules judge when it is applied. */
function windowFrom(v: unknown): WindowSpec {
  if (!isObject(v)) throw new CubeRefusal(`a window is {fn, column, partition, order, ...}, not ${JSON.stringify(v)}`);
  fields(v, 'window', ['fn', 'column', 'partition', 'order', 'frame', 'offset', 'buckets'], 'a window');
  const fn = v['fn'];
  if (typeof fn !== 'string' || !WINDOW_FUNCTIONS.some((w) => w.fn === fn)) {
    throw new CubeRefusal(`a window's function is one of ${WINDOW_FUNCTIONS.map((w) => w.fn).join(', ')}, not ${JSON.stringify(fn)}`);
  }
  if (!Array.isArray(v['order'])) throw new CubeRefusal(`a window's order is a list of sorts, not ${JSON.stringify(v['order'])}`);
  const order = v['order'].map((x): SortSpec => {
    if (!isObject(x)) throw new CubeRefusal(`a sort is {column, direction}, not ${JSON.stringify(x)}`);
    fields(x, 'sort', ['column', 'direction'], 'a sort');
    return { column: textFrom(x['column'], 'a sort'), direction: directionFrom(x['direction']) };
  });
  return {
    fn: fn as WindowFunction,
    ...(v['column'] !== undefined ? { column: textFrom(v['column'], "a window's column") } : {}),
    partition: namesFrom(v['partition'], "a window's partition"),
    order,
    ...(v['frame'] !== undefined ? { frame: frameFrom(v['frame']) } : {}),
    ...(v['offset'] !== undefined ? { offset: countFrom(v['offset'], "a window's offset") } : {}),
    ...(v['buckets'] !== undefined ? { buckets: countFrom(v['buckets'], "a window's buckets") } : {}),
  };
}

/** A child aggregate -- its function, the group-level column it aggregates -- or why not. */
function childAggregateFrom(v: unknown): ChildAggregate {
  if (!isObject(v)) throw new CubeRefusal(`a child aggregate is {fn, of}, not ${JSON.stringify(v)}`);
  fields(v, 'childAggregate', ['fn', 'of'], 'a child aggregate');
  const fn = v['fn'];
  if (typeof fn !== 'string' || !(CHILD_AGGREGATE_FNS as readonly string[]).includes(fn)) {
    throw new CubeRefusal(`a child aggregate's function is one of ${CHILD_AGGREGATE_FNS.join(', ')}, not ${JSON.stringify(fn)}`);
  }
  return { fn: fn as ChildAggregate['fn'], of: textFrom(v['of'], "a child aggregate's column") };
}

/** A measure at a grain -- the measure, its parent's level or the grand total's -- or why not. */
function grainFrom(v: unknown): Grain {
  if (!isObject(v)) throw new CubeRefusal(`a measure at a grain is {of, at}, not ${JSON.stringify(v)}`);
  fields(v, 'grain', ['of', 'at'], 'a measure at a grain');
  const at = v['at'];
  if (typeof at !== 'string' || !(GRAIN_ATS as readonly string[]).includes(at)) {
    throw new CubeRefusal(`a measure's grain is its parent's ('parent') or the grand total's ('all'), not ${JSON.stringify(at)}`);
  }
  return { of: textFrom(v['of'], "a grain's measure"), at: at as Grain['at'] };
}

/** A related class's property -- its path from the class, a to-many end's aggregate -- or why not. What the path names
 *  (a property the class has, to one or to many) is the model's to judge: class-source.ts relatedProblem. */
function relatedFrom(v: unknown): RelatedPath {
  if (!isObject(v)) throw new CubeRefusal(`a related column is {path[, aggregate]}, not ${JSON.stringify(v)}`);
  fields(v, 'related', ['path', 'aggregate'], 'a related column');
  const path = namesFrom(v['path'], "a related column's path");
  if (path.length === 0) throw new CubeRefusal("a related column's path is the properties from the class, at least one");
  const aggregate = v['aggregate'];
  if (aggregate !== undefined && (typeof aggregate !== 'string' || !(PATH_AGGREGATES as readonly string[]).includes(aggregate))) {
    throw new CubeRefusal(`a related column's values are aggregated by ${PATH_AGGREGATES.join(', ')}, not ${JSON.stringify(aggregate)}`);
  }
  return { path, ...(aggregate !== undefined ? { aggregate: aggregate as RelatedPath['aggregate'] & string } : {}) };
}

/** Value groups judged against the grid (snapshot.ts ValueGroups): a row-level column, of a column of its rows, its
 *  values in no group named where they are no text (an if's branches one type). */
function groupsProblem(s: CubeSnapshot, level: string, column: DerivedColumn): void {
  const g = column.groups;
  if (g === undefined) return;
  if (level !== 'row') throw new CubeRefusal(`'${column.name}' groups a column's values: a row-level column, before the grouping`);
  const of = rowColumns(s).find((c) => c.name === g.of);
  if (of === undefined) throw new CubeRefusal(`'${g.of}' is no column of the grid's rows (${rowColumns(s).map((c) => c.name).join(', ')})`);
  if (g.other === undefined && of.type !== undefined && familyOf(of.type) !== 'text') {
    throw new CubeRefusal(`${g.of} is ${of.type}: its values in no group need a name (other), a group's name being text`);
  }
}

/** A column's values gathered into named groups (snapshot.ts ValueGroups) read from JSON, or why not: a group of no
 *  values, a name twice, a value in two groups. Which column it gathers is the grid's to judge (the calculate command). */
function groupsFrom(v: unknown): ValueGroups {
  if (!isObject(v)) throw new CubeRefusal(`value groups are {of, groups: [{name, values}], other?}, not ${JSON.stringify(v)}`);
  fields(v, 'groups', ['of', 'groups', 'other'], 'value groups');
  const of = textFrom(v['of'], 'the column the groups gather');
  const given = v['groups'];
  if (!Array.isArray(given) || given.length === 0) throw new CubeRefusal(`value groups have at least one group, each {name, values}, not ${JSON.stringify(given)}`);
  const seen = new Map<string, string>();
  const names = new Set<string>();
  const groups = given.map((g) => {
    if (!isObject(g)) throw new CubeRefusal(`a group is {name, values}, not ${JSON.stringify(g)}`);
    fields(g, 'group', ['name', 'values'], 'a group');
    const name = textFrom(g['name'], "a group's name").trim();
    if (name === '') throw new CubeRefusal("a group's name is some text, not blank");
    if (names.has(name)) throw new CubeRefusal(`two groups are named ${name}: a name is one group's`);
    names.add(name);
    const values = g['values'];
    if (!Array.isArray(values) || values.length === 0) throw new CubeRefusal(`the group ${name} holds at least one value`);
    return {
      name,
      values: values.map((x) => {
        const value = x === null ? null : filterValueFrom(x);
        const key = JSON.stringify(value);
        const had = seen.get(key);
        if (had !== undefined) throw new CubeRefusal(`${value === null ? 'the blanks' : JSON.stringify(value)} are in ${had} and ${name}: a value is in one group`);
        seen.set(key, name);
        return value;
      }),
    };
  });
  const other = v['other'];
  if (other !== undefined && (typeof other !== 'string' || other.trim() === '')) {
    throw new CubeRefusal(`the name of the values in no group is some text, not ${JSON.stringify(other)}`);
  }
  return { of, groups, ...(other !== undefined ? { other: (other as string).trim() } : {}) };
}

/**
 * A calculated column read from JSON, as the screen's column editor and its presets make one: a formula (its lambda the
 * protocol's, its body the planner's to judge; exploded, each row repeated per element), a window column (Show As: a
 * running total, a rank), an aggregate of the child groups, a measure at its parent's or the grand total's grain or a
 * related class's property -- exactly one of them -- with its name (trimmed, as the editor takes one), its kind and its
 * type; or why not.
 */
function calculatedFrom(v: unknown): DerivedColumn {
  if (!isObject(v)) throw new CubeRefusal(`a calculated column is {name, lambda | window | childAggregate | grain | related | groups, ...}, not ${JSON.stringify(v)}`);
  fields(v, 'column', ['name', 'lambda', 'window', 'childAggregate', 'grain', 'related', 'groups', 'unnest', 'kind', 'type'], 'a calculated column from JSON');
  const shapes = (['lambda', 'window', 'childAggregate', 'grain', 'related', 'groups'] as const).filter((k) => v[k] !== undefined);
  if (shapes.length !== 1) {
    throw new CubeRefusal(`a calculated column is one of a lambda, a window, a child aggregate, a grain, a related property or value groups, not ${shapes.join(' and ') || 'none'}`);
  }
  if (v['unnest'] !== undefined && typeof v['unnest'] !== 'boolean') throw new CubeRefusal(`unnest is true or false, not ${JSON.stringify(v['unnest'])}`);
  if (v['unnest'] === true && shapes[0] !== 'lambda') throw new CubeRefusal("explode is a formula's: a window, a child aggregate, a grain, a related property or value groups are not exploded");
  const lambda = v['lambda'];
  if (lambda !== undefined && (!isObject(lambda) || lambda['_type'] !== 'lambda' || !Array.isArray(lambda['body']))) {
    throw new CubeRefusal(`a calculated column's lambda is the protocol's {_type: 'lambda', body, parameters}, not ${JSON.stringify(lambda)}`);
  }
  return {
    name: textFrom(v['name'], "a calculated column's name").trim(),
    ...(lambda !== undefined ? { lambda: lambda as unknown as Lambda } : {}),
    ...(v['window'] !== undefined ? { window: windowFrom(v['window']) } : {}),
    ...(v['childAggregate'] !== undefined ? { childAggregate: childAggregateFrom(v['childAggregate']) } : {}),
    ...(v['grain'] !== undefined ? { grain: grainFrom(v['grain']) } : {}),
    ...(v['related'] !== undefined ? { related: relatedFrom(v['related']) } : {}),
    ...(v['groups'] !== undefined ? { groups: groupsFrom(v['groups']) } : {}),
    ...(v['unnest'] !== undefined ? { unnest: v['unnest'] as boolean } : {}),
    ...(v['kind'] !== undefined ? { kind: kindFrom(v['kind']) } : {}),
    ...(v['type'] !== undefined ? { type: textFrom(v['type'], "a calculated column's type") } : {}),
  };
}

/** A measure as JSON carries one (`{column, fn[, name][, weight]}`: Python's grid.measure, page.grid's measures) read,
 *  or refused in words -- unnamed, named as its column: that column's own Aggregation (measurePlan). */
export function readMeasure(m: unknown): Measure {
  if (!isObject(m)) throw new CubeRefusal(`a measure is {column, fn[, name][, weight]}, not ${JSON.stringify(m)}`);
  const column = textFrom(m['column'], "a measure's column");
  return {
    name: m['name'] === undefined ? column : textFrom(m['name'], "a measure's name").trim(),
    column,
    fn: aggregateFrom(m['fn']),
    ...(m['weight'] !== undefined ? { weight: textFrom(m['weight'], "a measure's weight") } : {}),
  };
}

/**
 * A command read from JSON (Python's, a live channel's): its kind one this module applies, its fields exactly those
 * the kind takes, each of its type -- or refused in words. What the fields NAME (a column the grid has, a setting
 * DataCube knows) is the apply's to judge, as it judges the screen's.
 */
export function readGridCommand(given: unknown): GridCommand {
  if (!isObject(given) || typeof given['kind'] !== 'string') {
    throw new CubeRefusal(`a grid's command is {kind, ...}, its kind one of ${GRID_COMMAND_KINDS.join(', ')}, not ${JSON.stringify(given)}`);
  }
  const o = given;
  const kind = o['kind'] as string;
  switch (kind) {
    case 'group':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, rows: namesFrom(o['rows'], 'the row groups') };
    case 'pivot':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, columns: namesFrom(o['columns'], 'the pivot') };
    case 'pinPivot': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      const values = o['values'];
      if (values === null) return { kind, values: null };
      if (!Array.isArray(values) || !values.every((v) => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean')) {
        throw new CubeRefusal(`a pivot's pinned values are a list of texts, numbers or true and false, or null, not ${JSON.stringify(values)}`);
      }
      return { kind, values: values as FilterValue[] };
    }
    case 'sorts': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (!Array.isArray(o['sorts'])) throw new CubeRefusal('the sorts are a list');
      return { kind, sorts: o['sorts'].map((x) => {
        if (!isObject(x)) throw new CubeRefusal(`a sort is {column, direction}, not ${JSON.stringify(x)}`);
        return { column: textFrom(x['column'], 'a sort'), direction: directionFrom(x['direction']) };
      }) };
    }
    case 'addSort':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, column: textFrom(o['column'], 'a sort'), direction: directionFrom(o['direction']) };
    case 'clearSort':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, column: textFrom(o['column'], 'a sort cleared') };
    case 'clickSort':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (typeof o['chained'] !== 'boolean') throw new CubeRefusal('a header click is chained or not: true or false');
      if (o['first'] === undefined) throw new CubeRefusal("a header click says the direction it starts with: first, 'asc' or 'desc'");
      return { kind, column: textFrom(o['column'], 'a header click'), first: directionFrom(o['first']), chained: o['chained'] };
    case 'clickTreeSort':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (typeof o['chained'] !== 'boolean') throw new CubeRefusal('a tree header click is chained or not: true or false');
      return { kind, chained: o['chained'] };
    case 'column': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      const change = o['change'];
      if (!isObject(change)) throw new CubeRefusal(`a column's properties are {kind, aggregate, weight}, not ${JSON.stringify(change)}`);
      const { kind: k, aggregate, weight, ...other } = change;
      const unknown = Object.keys(other);
      if (unknown.length > 0) throw new CubeRefusal(`a column's properties are kind, aggregate and weight, not ${unknown.join(', ')}`);
      // null clears an Aggregation or a weight, as the panel's "none" does
      return { kind, name: textFrom(o['name'], "a column's name"), change: {
        ...(k !== undefined ? { kind: kindFrom(k) } : {}),
        ...(aggregate !== undefined ? { aggregate: aggregate === null ? null : aggregateFrom(aggregate) } : {}),
        ...(weight !== undefined ? { weight: weight === null ? null : textFrom(weight, "a column's weight") } : {}),
      } };
    }
    case 'configure':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (!isObject(o['patch'])) throw new CubeRefusal("a grid's configuration is its settings by name, in its document's own keys");
      return { kind, patch: o['patch'] };
    case 'filter':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, filter: o['filter'] === null ? null : readFilterNode(o['filter']) };
    case 'addFilter':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (!isObject(o['condition']) || o['condition']['kind'] !== 'condition') {
        throw new CubeRefusal(`a filter added is one condition, not ${JSON.stringify(o['condition'])}`);
      }
      return { kind, condition: readFilterNode(o['condition']) };
    case 'refilter': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      const conditions = (v: unknown, what: string): FilterNode[] => {
        if (!Array.isArray(v)) throw new CubeRefusal(`${what} is a list of conditions, not ${JSON.stringify(v)}`);
        return v.map(readFilterNode);
      };
      return { kind, remove: conditions(o['remove'], 'what a refilter takes off'), add: conditions(o['add'], 'what a refilter adds') };
    }
    case 'calculate':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (o['level'] !== 'row' && o['level'] !== 'group') throw new CubeRefusal(`a calculated column's level is 'row' or 'group', not ${JSON.stringify(o['level'])}`);
      return { kind, level: o['level'], column: calculatedFrom(o['column']), ...beforeFrom(o['before']) };
    case 'editCalculated':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (o['level'] !== 'row' && o['level'] !== 'group') throw new CubeRefusal(`a calculated column's level is 'row' or 'group', not ${JSON.stringify(o['level'])}`);
      return { kind, name: textFrom(o['name'], "the calculated column's name"), level: o['level'], column: calculatedFrom(o['column']), ...beforeFrom(o['before']) };
    case 'removeCalculated':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, name: textFrom(o['name'], "the calculated column's name") };
    case 'open': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      if (typeof o['open'] !== 'boolean') throw new CubeRefusal('a group is opened or closed: open is true or false');
      return { kind, path: pathFrom(o['path']), open: o['open'] };
    }
    case 'collapseAll':
    case 'expandAll':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind };
    case 'topN':
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      return { kind, topN: o['topN'] === null ? null : readTopN(o['topN']) };
    case 'tree': {
      fields(o, kind, GRID_COMMAND_FIELDS[kind]);
      const paths = (v: unknown, what: string): RowPath[] => {
        if (!Array.isArray(v)) throw new CubeRefusal(`${what} is a list of groups' paths, not ${JSON.stringify(v)}`);
        return v.map(pathFrom);
      };
      if (typeof o['showTotals'] !== 'boolean') throw new CubeRefusal('the grand total is shown or not: showTotals is true or false');
      const expandTo = o['expandTo'];
      if (expandTo !== undefined && (typeof expandTo !== 'number' || !Number.isInteger(expandTo) || expandTo < 0)) {
        throw new CubeRefusal(`the level the groups open to is a whole number, 0 or more, not ${JSON.stringify(expandTo)}`);
      }
      return {
        kind, open: paths(o['open'], 'the groups open'), showTotals: o['showTotals'],
        ...(expandTo !== undefined ? { expandTo } : {}),
        ...(o['closed'] !== undefined ? { closed: paths(o['closed'], 'the groups closed under the level') } : {}),
      };
    }
    default:
      throw new CubeRefusal(`'${kind}' is not a grid's command: ${GRID_COMMAND_KINDS.join(', ')}`);
  }
}
