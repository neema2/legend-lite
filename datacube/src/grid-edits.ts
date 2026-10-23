// ONE WAY TO EDIT A GRID (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md): its row groups, its pivot, its sorts and a
// column's kind and Aggregation, by the screen's own offers -- what the Pivots and Sorts panels, the header menu, the
// drag zones and Column Properties offer a person -- so the screen and Python (headless gridApply) edit a grid alike.
// No DOM. An edit the offers do not allow is refused in words (CubeRefusal): the screen's controls never ask one,
// Python's calls may.

import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { isNumeric, isTemporal } from '../../engine-client/src/types.ts';
import { lambda, property, variable } from '../../pure-protocol/src/index.ts';
import { nameProblem } from './calc.ts';
import { draftColumn, newDraft } from './column-draft.ts';
import { applyToSnapshot, columnConfig, withColumn, type ColumnPatch, type CubeConfiguration } from './config.ts';
import { spreadMeasures } from './query.ts';
import { takesAggregate } from './offers.ts';
import {
  AGGREGATE_FNS, COLUMN_KINDS, CubeRefusal, SORT_DIRECTIONS, rowColumns, totalOrderSorts, type AggregateFn,
  type ColumnKind, type CubeSnapshot, type DerivedColumn, type Measure, type SortDirection, type SortSpec,
} from './snapshot.ts';
import type { TreeState } from './tree.ts';

/** What an edit changes: the grid's query and its configuration (the editor's draft carries both). */
export interface GridDraft {
  readonly snapshot: CubeSnapshot;
  readonly config: CubeConfiguration;
  /** Which groups are open, where the edit is of a grid as it shows (the screen, headless gridApply): an editor's
   *  draft (the Properties window's) holds none, and opens none. */
  readonly tree?: TreeState;
}

const listed = (names: readonly string[]): string => (names.length === 0 ? 'none' : [...names].sort().join(', '));

/**
 * A column's kind as the grid groups by it: a calculated column's as its editor declared it, a source column's as
 * Column Properties sets it, else its type's default. Undefined for a column that does not exist before aggregation --
 * a group-level column, a pivot's generated one -- which is therefore neither groupable nor extendable.
 */
export function columnKind(draft: GridDraft, name: string): ColumnKind | undefined {
  const c = rowColumns(draft.snapshot).find((x) => x.name === name);
  if (!c) return undefined;
  return c.derived ? c.kind : columnConfig(draft.config, name).kind ?? c.kind;
}

/**
 * The columns a row group or a pivot may use: the dimensions, as DataCube's `enableRowGroup: kind === DIMENSION`.
 * Grouping by a notional makes one group per distinct amount, which is never what anyone meant.
 */
export function groupableColumns(draft: GridDraft): string[] {
  return rowColumns(draft.snapshot).map((c) => c.name).filter((n) => columnKind(draft, n) === 'dimension');
}

/** Every column the cube knows, its calculated ones too: what the Sorts panel offers and Column Properties lists. */
export function allColumns(draft: GridDraft): string[] {
  const s = draft.snapshot;
  return [...s.columns.map((c) => c.name), ...s.derived.map((d) => d.name), ...(s.groupDerived ?? []).map((d) => d.name)];
}

/**
 * The aggregates Aggregation offers a column of this type, as the compiler answers them (offers.ts takesAggregate):
 * each whose result is of the column's own family. An untyped column (a calculated one not yet typed): all, the
 * compiler judging it on Apply.
 */
export function offeredAggregates(type: string | undefined): AggregateFn[] {
  return type === undefined ? [...AGGREGATE_FNS] : AGGREGATE_FNS.filter((fn) => takesAggregate(fn, type));
}

/** `rows` as the grid's row groups, outermost first (the Vertical Pivots panel, the menu's Vertical Pivot, the Rows
 *  zone): each once, and each it did not have a dimension of the grid -- the screen offers dimensions to add, and a
 *  group it has is taken away whatever it is. */
export function setGrouping<D extends GridDraft>(draft: D, rows: readonly string[]): D {
  checkGroupable(draft, rows, draft.snapshot.rows, 'the row groups');
  return { ...draft, snapshot: { ...draft.snapshot, rows: [...rows] } };
}

/** `pivotOn` as the grid's pivot (the Horizontal Pivots panel, the menu's Horizontal Pivot, the Columns zone): each
 *  once, and each it did not have a dimension of the grid. Unpivoted, the grid has no pivot headers: a sort of one (the
 *  header menu's) goes with them, as the grid's rows would be sorted by a column they do not have. */
export function setPivot<D extends GridDraft>(draft: D, pivotOn: readonly string[]): D {
  checkGroupable(draft, pivotOn, draft.snapshot.pivotOn, 'the pivot');
  const columns = allColumns(draft);
  const sorts = pivotOn.length > 0 ? draft.snapshot.sorts : draft.snapshot.sorts.filter((x) => columns.includes(x.column));
  return { ...draft, snapshot: { ...draft.snapshot, pivotOn: [...pivotOn], sorts } };
}

function checkGroupable(draft: GridDraft, names: readonly string[], had: readonly string[], what: string): void {
  const offered = groupableColumns(draft);
  const seen = new Set<string>();
  for (const name of names) {
    if (seen.has(name)) throw new CubeRefusal(`${what} name '${name}' twice`);
    seen.add(name);
    if (had.includes(name) || offered.includes(name)) continue;
    throw new CubeRefusal(columnKind(draft, name) === 'measure'
      ? `'${name}' is a measure, and ${what} take dimensions: Column Properties > Column Kind makes it one`
      : `'${name}' is not a column ${what} can take: the grid's dimensions are ${listed(offered)}`);
  }
}

/**
 * The measure a pivot's header is of, or undefined (the grid does not pivot, or `column` is no header of its pivot): a
 * pivot value's column or a pivot total's, each ending with a measure the pivot spreads (query.ts spreadMeasures: a
 * measure column, or the count a pivot of none fills with), made unique by `~N` when its name is taken (pivotColumns).
 * What a header sorts by (setSorts) and what a header's own presentation is set on (grid-commands.ts configure).
 */
export function pivotHeaderMeasure(draft: GridDraft, column: string): string | undefined {
  const at = column.lastIndexOf(PIVOT_SEPARATOR);
  if (at <= 0) return undefined;
  const measure = column.slice(at + PIVOT_SEPARATOR.length).replace(/~\d+$/, '');
  return spreadMeasureNames(draft).includes(measure) ? measure : undefined;
}

/** The measures the grid's pivot spreads (query.ts spreadMeasures: its measure columns, or the count a pivot of none
 *  fills with), by name; none when it does not pivot. */
export function spreadMeasureNames(draft: GridDraft): string[] {
  if (draft.snapshot.pivotOn.length === 0) return [];
  return spreadMeasures(applyToSnapshot(draft.snapshot, draft.config)).map((m) => m.name);
}

/**
 * `sorts` as the grid's sorts, in order, each once, ascending or descending: each it did not sort by a column the grid
 * has (the Sorts panel's), or, pivoted, a pivot's header (pivotHeaderMeasure) -- as the header menu sorts by the header
 * clicked; a sort it has is kept or taken away as it is. A header of values the level does not have is left out of its
 * query. A child group's aggregate is refused: no query of the level has it to order by (P2-15).
 */
export function setSorts<D extends GridDraft>(draft: D, sorts: readonly SortSpec[]): D {
  const columns = allColumns(draft);
  const pivoted = draft.snapshot.pivotOn.length > 0;
  // a child group's aggregate is read by its own query, beside the level's: the level's rows cannot be ordered by it
  const apart = new Set((draft.snapshot.groupDerived ?? []).filter((d) => d.childAggregate).map((d) => d.name));
  const seen = new Set<string>();
  for (const { column, direction } of sorts) {
    if (!SORT_DIRECTIONS.includes(direction)) {
      throw new CubeRefusal(`a sort is ${SORT_DIRECTIONS.map((d) => `'${d}'`).join(' or ')}, not '${String(direction)}'`);
    }
    if (seen.has(column)) throw new CubeRefusal(`the sorts name '${column}' twice`);
    seen.add(column);
    if (draft.snapshot.sorts.some((x) => x.column === column)) continue;
    if (apart.has(column)) {
      throw new CubeRefusal(`'${column}' is worked out from each group's child rows after the groups are read, `
        + 'so the groups cannot be sorted by it');
    }
    if (!columns.includes(column) && pivotHeaderMeasure(draft, column) === undefined) {
      throw new CubeRefusal(`the sort names '${column}', which the grid does not have: one of its columns `
        + `(${listed(columns)})${pivoted ? ", or a pivot's header" : ''}`);
    }
  }
  return { ...draft, snapshot: { ...draft.snapshot, sorts: sorts.map((s) => ({ column: s.column, direction: s.direction })) } };
}

/**
 * `column` sorted last, `direction`, its sort before taken away -- the header menu's Add Ascending and Add Descending.
 * An add that would change nothing (the column sorted so already) changes nothing, as the menu offers none.
 */
export function addSort<D extends GridDraft>(draft: D, column: string, direction: SortDirection): D {
  const sorts = draft.snapshot.sorts;
  if (sorts.some((x) => x.column === column && x.direction === direction)) return draft;
  return setSorts(draft, [...sorts.filter((x) => x.column !== column), { column, direction }]);
}

/**
 * `column`'s sort taken off, the others kept as they are -- the header menu's Clear Sort (offered only for a column the
 * grid is sorted by), Python's `clear_sort`. A column it is not sorted by is refused, naming its sorts.
 */
export function clearSort<D extends GridDraft>(draft: D, column: string): D {
  const sorts = draft.snapshot.sorts;
  if (!sorts.some((x) => x.column === column)) {
    throw new CubeRefusal(`the grid is not sorted by '${column}'`
      + (sorts.length > 0 ? ` (it is sorted by ${sorts.map((x) => x.column).join(', ')})` : ' (it has no sorts)'));
  }
  return setSorts(draft, sorts.filter((x) => x.column !== column));
}

/**
 * The direction a header click sorts a column in first (the user, 2026-10-18): a number or a date from the largest,
 * the latest; anything else from A. `type` is the column's as the result reports it (a pivot's cell is its aggregate's).
 */
export function firstSortDirection(type: string | undefined): SortDirection {
  return isNumeric(type) || isTemporal(type) ? 'desc' : 'asc';
}

/**
 * A HEADER CLICK'S SORT (the user, 2026-10-18): the column's first direction, then the other, then none. A plain
 * click sorts by the column alone, every other sort taken off; a chained click (Ctrl, or ⌘ on a Mac) keeps the others
 * and the column's place among them, and puts a column with no sort last.
 */
export function clickSort<D extends GridDraft>(draft: D, column: string, first: SortDirection, chained: boolean): D {
  const sorts = draft.snapshot.sorts;
  const now = sorts.find((x) => x.column === column)?.direction;
  const next = now === undefined ? first : now === first ? (first === 'asc' ? 'desc' : 'asc') : undefined;
  if (!chained) return setSorts(draft, next === undefined ? [] : [{ column, direction: next }]);
  if (next === undefined) return setSorts(draft, sorts.filter((x) => x.column !== column));
  return setSorts(draft, now === undefined
    ? [...sorts, { column, direction: next }]
    : sorts.map((x) => (x.column === column ? { column, direction: next } : x)));
}

/**
 * THE HEADERS' SORT MARKS: the order the grid's query gives its rows (snapshot.ts totalOrderSorts), as its headers can
 * show it -- so no header claims an order the rows do not have. Each sort the query applies, in its order, on the
 * header it has among `headed` (the grid's leaves); a sort of the first row group, which has no header of its own in
 * the tree, on the tree column's (`tree`); and, with no sort applied, the tree column's own direction, which then
 * orders the groups. A sort the query leaves out -- a pivot total's with the totals off, a child group's aggregate
 * (P2-15) -- is neither marked nor counted, so the positions a person reads have no gaps. `snapshot` is folded (a view's).
 */
export function headerSorts(snapshot: CubeSnapshot, headed: readonly string[], tree: string): SortSpec[] {
  const marks: SortSpec[] = [];
  const applied = appliedSorts(snapshot);
  for (const x of applied) {
    if (headed.includes(x.column)) marks.push(x);
    else if (x.column === snapshot.rows[0] && headed.includes(tree)) marks.push({ column: tree, direction: x.direction });
  }
  if (applied.length === 0 && snapshot.rows.length > 0 && headed.includes(tree)) {
    marks.push({ column: tree, direction: snapshot.treeColumnSort ?? 'asc' });
  }
  return marks;
}

/** The sorts a folded snapshot's query applies, in its order: its ORDER BY without the row groups no sort names. */
function appliedSorts(snapshot: CubeSnapshot): SortSpec[] {
  const sorted = new Set(snapshot.sorts.map((x) => x.column));
  return totalOrderSorts(snapshot).filter((x) => sorted.has(x.column));
}

/**
 * A click on the TREE column's header (the user, 2026-10-18). The groups are always ordered, so it has no "none". A
 * plain click orders the rows by the groups alone, every other sort taken off: the other way from the order the tree's
 * header shows (headerSorts), or, when it shows none, its first group column's first direction (firstSortDirection). A
 * chained click with sorts applied chains that first group column, as a click chains any column (clickSort); with none
 * applied there is nothing to chain after, and it is a plain click.
 */
export function clickTreeSort<D extends GridDraft>(draft: D, chained: boolean): D {
  const folded = applyToSnapshot(draft.snapshot, draft.config);
  const group = folded.rows[0];
  if (group === undefined) throw new CubeRefusal('the grid has no row groups to order');
  const first = firstSortDirection(rowColumns(folded).find((c) => c.name === group)?.type);
  const applied = appliedSorts(folded);
  if (chained && applied.length > 0) return clickSort(draft, group, first, true);
  const shown = applied.length === 0 ? folded.treeColumnSort ?? 'asc' : applied.find((x) => x.column === group)?.direction;
  const alone = setSorts(draft, []);
  const direction = shown === undefined ? first : shown === 'asc' ? 'desc' : 'asc';
  return { ...alone, config: { ...alone.config, treeColumnSort: direction } };
}

/** What Column Properties sets of a column through this module: its kind, its Aggregation, and a weighted average's
 *  weight. A field told as null is cleared, as the panel's "none". */
export interface ColumnChange {
  readonly kind?: ColumnKind;
  readonly aggregate?: AggregateFn | null;
  readonly weight?: string | null;
}

/**
 * A column's kind, Aggregation or weight set as Column Properties sets them: the kind of a source column not grouped
 * or pivoted (the panel locks it then; a calculated column's is its editor's, which every query reads), a new kind
 * taking the pivot's exclusion with it (a dimension out, a measure in: upstream's setExcludedFromPivot(kind ===
 * DIMENSION)); the Aggregation of a measure the grouped level aggregates -- a source or a row-level calculated column,
 * never a group-level one, computed after grouping -- one offered for its type (or the one it has); a weight a column
 * of the grid's, for a weighted average only.
 */
export function setColumn<D extends GridDraft>(draft: D, name: string, change: ColumnChange): D {
  const columns = allColumns(draft);
  if (!columns.includes(name)) throw new CubeRefusal(`'${name}' is not a column of the grid (${listed(columns)})`);
  const s = draft.snapshot;
  let patch: ColumnPatch = {};
  // a kind it has already is no change (the panel sets none)
  if (change.kind !== undefined && change.kind !== columnKind(draft, name)) {
    if (!COLUMN_KINDS.includes(change.kind)) {
      throw new CubeRefusal(`a column's kind is ${COLUMN_KINDS.map((k) => `'${k}'`).join(' or ')}, not '${String(change.kind)}'`);
    }
    if (!s.columns.some((c) => c.name === name)) {
      throw new CubeRefusal(`'${name}' is a calculated column: its kind is set in its editor (Edit Column)`);
    }
    if (s.rows.includes(name) || s.pivotOn.includes(name)) {
      throw new CubeRefusal(`'${name}' is grouped or pivoted on: its kind cannot change while it is`);
    }
    patch = { kind: change.kind, excludedFromPivot: change.kind === 'dimension' };
  }
  const config = withColumn(draft.config, name, patch);
  const now = { ...draft, config };
  const c = columnConfig(config, name);
  if (change.aggregate !== undefined && change.aggregate !== null) {
    const kind = columnKind(now, name);
    if (kind === undefined) {
      throw new CubeRefusal(`'${name}' is a group-level column, computed after grouping: it takes no Aggregation`);
    }
    if (kind !== 'measure') {
      throw new CubeRefusal(`'${name}' is a dimension, and only a measure takes an Aggregation: Column Properties > `
        + 'Column Kind makes it one');
    }
    const type = typeOf(s, name);
    if (change.aggregate !== c.aggregateFn && !offeredAggregates(type).includes(change.aggregate)) {
      throw new CubeRefusal(`'${change.aggregate}' is not offered on ${name}, a ${type} column: `
        + `${offeredAggregates(type).join(', ') || 'none'}`);
    }
  }
  const aggregate = change.aggregate === undefined ? c.aggregateFn : change.aggregate ?? undefined;
  if (change.weight !== undefined && change.weight !== null) {
    if (aggregate !== 'wavg') {
      throw new CubeRefusal(`only a weighted average (wavg) takes a weight: ${name}'s Aggregation is `
        + `${aggregate ?? 'its default'}`);
    }
    const rows = rowColumns(s).map((x) => x.name);
    if (!rows.includes(change.weight)) {
      throw new CubeRefusal(`${name}'s weight names '${change.weight}', not a column the rows have (${listed(rows)})`);
    }
  }
  const value: ColumnPatch = {
    ...(change.aggregate !== undefined ? { aggregateFn: change.aggregate ?? undefined } : {}),
    ...(change.weight !== undefined
      ? { aggregationParameters: change.weight === null ? undefined : [change.weight] } : {}),
  };
  return { ...now, config: withColumn(config, name, value) };
}

/** A source or row-level calculated column's type as the compiler typed it; undefined while not yet typed. */
function typeOf(s: CubeSnapshot, name: string): string | undefined {
  return rowColumns(s).find((x) => x.name === name)?.type;
}

/**
 * The column a calculated column copies as it is -- `x|$x.c`, as measureAsColumn makes one for a second aggregate of a
 * column -- else undefined.
 */
export function copiedColumn(d: DerivedColumn): string | undefined {
  const l = d.lambda;
  if (!l || d.window || d.childAggregate || d.unnest || l.parameters.length !== 1 || l.body.length !== 1) return undefined;
  const body = l.body[0] as { readonly _type?: string; readonly property?: unknown; readonly parameters?: readonly unknown[] };
  const owner = body.parameters?.[0] as { readonly _type?: string; readonly name?: unknown } | undefined;
  return body._type === 'property' && typeof body.property === 'string' && body.parameters?.length === 1
    && owner?._type === 'var' && owner.name === l.parameters[0]?.name ? body.property : undefined;
}

/** A measure made a column of the grid's: `column` carries it (its own, or a calculated column of its name). */
export interface MeasureMade {
  readonly measure: Measure;
  readonly column: string;
}

/**
 * ONE MEASURE AS THE SCREEN'S MODEL (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.3-3.4): named as its own column,
 * that column not a key and not in `taken`, that column's Aggregation (a dimension made a measure first, as Column
 * Properties would); any other a calculated column of its own name reading its column (the column editor's own steps:
 * column-draft.ts newDraft, draftColumn; its name by calc.ts nameProblem), with that Aggregation -- each by setColumn's
 * rules. Refused in their words (CubeRefusal): a column the grid has not, a name taken, an aggregate its column's type
 * is not offered (a count of text: "Show leaf count" counts a group's rows). Python's grid.measure() and a chart's
 * values shown in a grid of their own (Show Grid) are made the grid's by this one rule. Only `converting` (a chart's values:
 * what was there already) makes a dimension a measure; asked of a grid (Python's measure()), a dimension's
 * Aggregation is refused, as Column Properties refuses it.
 */
export function measureAsColumn<D extends GridDraft>(draft: D, m: Measure, taken: ReadonlySet<string> = new Set(),
  converting = false): {
  readonly draft: D;
  readonly made: MeasureMade;
  readonly note: string;
} {
  const plan = measurePlan(draft, m, taken);
  if (plan.copy === undefined) {
    const dimension = converting && columnKind(draft, m.column) === 'dimension';
    const d = setColumn(dimension ? asMeasure(draft, m.column) : draft, m.column, plan.change);
    return {
      draft: d, made: { measure: m, column: m.column },
      note: `measure ${m.name} is now ${m.column}'s Aggregation, ${m.fn}${dimension ? ` (${m.column} is now a measure)` : ''}`,
    };
  }
  const copy = plan.copy;
  const d = setColumn({ ...draft, snapshot: { ...draft.snapshot, derived: [...draft.snapshot.derived, copy] } },
    copy.name, plan.change);
  // a column kept out of the pivot carries its exclusion; a calculated column has none (its editor sets none): a pivot
  // spreads the copy
  const column = rowColumns(draft.snapshot).find((c) => c.name === m.column)!;
  const keptOut = converting && (columnConfig(draft.config, m.column).excludedFromPivot ?? column.excludedFromPivot) === true;
  return {
    draft: d, made: { measure: m, column: copy.name },
    note: `measure ${m.name} is now a calculated column of ${m.column}, its Aggregation ${m.fn}`
      + (keptOut ? ` (a pivot spreads it: ${m.column} is kept out of one, a calculated column cannot be)` : ''),
  };
}

/**
 * WHAT ONE MEASURE IS MADE AS (measureAsColumn's rule, and grid-measures.ts measureCommands' -- the screen's own steps,
 * as commands, for Python's grid.measure): its Aggregation (`change`) set on its own column, or (`copy`) on a calculated
 * column of its name reading its column. Refused in their words, as measureAsColumn says.
 */
export function measurePlan(draft: GridDraft, m: Measure, taken: ReadonlySet<string> = new Set()):
  { readonly change: ColumnChange; readonly copy?: DerivedColumn } {
  const change: ColumnChange = { aggregate: m.fn, ...(m.weight !== undefined ? { weight: m.weight } : {}) };
  const column = rowColumns(draft.snapshot).find((c) => c.name === m.column);
  if (!column) {
    throw new CubeRefusal(allColumns(draft).includes(m.column)
      ? `'${m.column}' is a group-level column, computed after grouping: it takes no Aggregation`
      : `'${m.column}' is not a column of the grid (${listed(allColumns(draft))})`);
  }
  // Aggregation's offer, judged by the column it reads (the calculated column a measure may become is of its type)
  if (column.type !== undefined && !offeredAggregates(column.type).includes(m.fn)) {
    throw new CubeRefusal(`'${m.fn}' is not offered on ${m.column}, a ${column.type} column: `
      + `${offeredAggregates(column.type).join(', ') || 'none'}${m.fn === 'count' ? " (Show leaf count counts a group's rows)" : ''}`);
  }
  const isKey = draft.snapshot.rows.includes(m.column) || draft.snapshot.pivotOn.includes(m.column);
  if (m.name === m.column && !isKey && !taken.has(m.column)) return { change };
  const problem = nameProblem(draft.snapshot, 'row', m.name);
  if (problem !== null) throw new CubeRefusal(problem);
  const copy: DerivedColumn = {
    ...draftColumn(newDraft(m.name, '', 'measure'), lambda(['x'], property(variable('x'), m.column))),
    // typed as its column is, so its Aggregation is offered by its type now (the compiler types it again on refresh)
    ...(column.type !== undefined ? { type: column.type } : {}),
  };
  return { change, copy };
}

/**
 * `name` made a measure: a source column as Column Properties makes one (setColumn), a calculated column on its own
 * entry, as its editor makes one (Column Properties sets no calculated column's kind): a chart's value of a
 * calculated column not yet typed, which reads as a dimension.
 */
function asMeasure<D extends GridDraft>(draft: D, name: string): D {
  if (!draft.snapshot.derived.some((d) => d.name === name)) return setColumn(draft, name, { kind: 'measure' });
  const derived = draft.snapshot.derived.map((d) => (d.name === name ? { ...d, kind: 'measure' as const } : d));
  return { ...draft, snapshot: { ...draft.snapshot, derived } };
}

/**
 * A LIST OF MEASURES AS THE SCREEN'S MODEL: each by measureAsColumn, in order, against what the ones before it made (a
 * column given an Aggregation once is taken: another measure of it is a calculated column). One it cannot be is left
 * out, said, the rest made: Show Grid makes what it can of a chart's values.
 */
export function measuresAsColumns<D extends GridDraft>(draft: D, measures: readonly Measure[]): {
  readonly draft: D;
  readonly made: readonly MeasureMade[];
  readonly notes: readonly string[];
} {
  let d = draft;
  const made: MeasureMade[] = [];
  const notes: string[] = [];
  const taken = new Set<string>();
  for (const m of measures) {
    try {
      const one = measureAsColumn(d, m, taken, true);
      d = one.draft;
      if (one.made.column === m.column) taken.add(m.column);
      made.push(one.made);
      notes.push(one.note);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      notes.push(`left out measure ${m.name}: ${e.message}`);
    }
  }
  return { draft: d, made, notes };
}
