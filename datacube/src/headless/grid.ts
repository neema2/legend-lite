// A GRID, HEADLESS (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §4): a page's cube over its source, opened and
// run by DataCube's own code, with no screen -- openCube over the source as the compiler types it now (as a page opens
// a saved cube), its configuration folded into its query as the tab's state owner folds it (cube-state.ts `fold`: its
// row cap, each column's aggregate and kind, pivot totals and sorts), CubeController.run for its view (its levels,
// totals, pivot and truncation) laid out as the screen lays it out (shownColumns: hidden columns out, its order), and
// classLambda for its rows at their finest level (Create a Class's relation), which Python plans and runs itself for
// all of them (Grid.to_pandas, typed by its plan: docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15.3a). Its planner is
// legend-lite's over the host's worker, its engine DuckDB over the host's connection: the tab's own pair, in Python.
//
// Each call opens the grid afresh from the cube document it is given: the document is the grid's state, Python's
// page keeps it, and nothing here holds a grid between calls.

import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import type { Scalar } from '../../../engine-client/src/result.ts';
import { fromJson, toJson, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { nameProblem } from '../calc.ts';
import { chartFrom, chartProblems, followCube, followRefusal, readChart } from '../chart-spec.ts';
import { draftCandidate, draftColumn, draftStage, newDraft } from '../column-draft.ts';
import { CubeController, type CubeView } from '../cube.ts';
import { fold, type CubeState } from '../cube-state.ts';
import {
  cubeToJson, frameSource, gridCapped, openCube, readCube, rebuilt, writeCube,
  type OpenedCube,
} from '../cube-document.ts';
import {
  OPERATORS, isCompatible, newCondition, newGroup, toFilterNode, type DraftCondition, type DraftNode,
} from '../filter-operators.ts';
import { STALE } from '../epoch.ts';
import {
  EXPORT_KINDS, columnFormats, heatmapColour, heatmapSetting, isExportKind, measureHeatmaps, renderExport,
} from '../export-render.ts';
import { FormatterCache } from '../format.ts';
import { LitePlanner } from '../lite-planner.ts';
import {
  APPEARANCE_SETTINGS, COLUMN_SETTINGS, CUBE_SETTINGS, applyToSnapshot, fromSnapshot, withConfiguration, type CubeConfiguration,
  type Patch,
} from '../config.ts';
import {
  addSort, allColumns, measureAsColumn, setColumn, setGrouping, setPivot, setSorts, type GridDraft,
} from '../grid-edits.ts';
import { suggestedClassName, suggestedDataSpaceName, suggestedPackage } from '../model-names.ts';
import { typeColumns } from '../plan.ts';
import { PlanError } from '../planner.ts';
import { classLambda, sampleLambda, storeTableOf } from '../query.ts';
import { PlanThenRun } from '../runner.ts';
import { SampleRefusal, sampleOf, type SampleRows } from '../sample-rows.ts';
import { shownColumns } from '../shown-columns.ts';
import {
  AGGREGATE_FNS, COLUMN_KINDS, CubeRefusal, isJsonValue, isPivotTotalColumn, isRelativeDate, pivotTotalColumn, rowColumns,
  type AggregateFn,
  type ColumnKind, type ColumnSpec, type CubeSnapshot, type FilterValue, type Measure, type SortDirection,
} from '../snapshot.ts';
import { sourceColumns } from '../source-columns.ts';
import { TreeState } from '../tree.ts';
import { hostCalls, hostDatabase, hostWorker } from './host.ts';
import { base64 } from './web.ts';

/** A grid as Python hands it in: its cube document (page-document's, as JSON text), and its source's model. */
export interface GridSpec {
  /** The cube document, as JSON text: read as the protocol's exact JSON (a `12.30` stays `12.30`). */
  readonly cube: string;
  /** The model its source is declared in, and the runtime its queries run on. */
  readonly model: string;
  readonly runtime: string;
  /** Its source relation, as Pure text: `#>{frames::DB.trades}#`. */
  readonly source: string;
}

interface Opened {
  readonly planner: LitePlanner;
  readonly cube: OpenedCube;
  /** Its state as the query sees it: the configuration folded into the snapshot. */
  readonly state: CubeState;
}

/** A source as the compiler types it now: its planner over the host's worker, its relation, its columns. */
async function typed(spec: Omit<GridSpec, 'cube'>): Promise<{
  readonly planner: LitePlanner; readonly relation: ValueSpecification; readonly columns: ColumnSpec[];
}> {
  const calls = hostCalls();
  const planner = new LitePlanner({ model: spec.model, runtime: spec.runtime, worker: () => hostWorker(calls) });
  try {
    const relation = (await planner.parse(`|${spec.source}`)).body[0] as ValueSpecification | undefined;
    if (relation === undefined) throw new Error(`the source is not a relation: ${spec.source}`);
    return { planner, relation, columns: await sourceColumns(planner, relation) };
  } catch (e) {
    planner.dispose();
    throw e;
  }
}

/** The grid opened: its planner over the host's worker, its cube over the source as the compiler types it now. */
async function opened(spec: GridSpec): Promise<Opened> {
  const { planner, relation, columns } = await typed(spec);
  try {
    const was = openCube(readCube(fromJson(spec.cube)), { query: relation }, columns);
    // the configuration the screen opens on: the saved one, with what the query carries read back (app.ts's
    // constructor) -- a column's kind or Aggregation the document holds only in its query is kept, not folded away
    const cube = { ...was, configuration: fromSnapshot(was.snapshot, was.configuration) };
    return { planner, cube, state: fold({ snapshot: cube.snapshot, configuration: cube.configuration, tree: cube.tree }) };
  } catch (e) {
    planner.dispose();
    throw e;
  }
}

/** A frame as Python hands one in for a grid's cube: its source's model and relation (as GridSpec's), and its name. */
export interface FrameSpec extends Omit<GridSpec, 'cube'> {
  /** The frame's name on the host: the cube's source, `{_type: 'frame', name}`. */
  readonly frame: string;
}

/**
 * A GRID'S CUBE OVER A FRAME, as DataCube makes one (cube-document.ts rebuilt and writeCube, the frame's source as
 * frameSource records it) -- so Python's pages make no cube document of their own
 * (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, items 2 and 1). With no `saved` cube: a new grid's, its rows as they are,
 * titled `label`, capped at the row limit when it is not its page's only grid (cube-document.ts gridCapped, the app's
 * own rule, of `gridsOnPage`: the page's grids counting this one; rebuilt reads it only for a new cube). With one (a
 * cube document, JSON): that cube rebuilt over the frame as it is now -- a new column hidden, a gone one left out of
 * everything that read it, the open groups dropped when the grouping changed -- said in `notes`, and `changed` when a
 * part was left out (openCube's). `name` is its page's (a page's cubes carry the page's name). JSON: `{cube, notes, changed}`, the
 * cube document as its text (its numbers exact).
 */
export async function gridCube(spec: FrameSpec, saved: string | null, name: string, label: string,
  gridsOnPage: number): Promise<string> {
  const { planner, relation, columns } = await typed(spec);
  try {
    const was = saved === null ? undefined : readCube(fromJson(saved));
    const made = rebuilt(was, relation, columns, label, gridCapped(gridsOnPage));
    const cube = writeCube({
      name, source: frameSource(spec.frame, columns), snapshot: made.snapshot, configuration: made.configuration,
      tree: made.tree ?? TreeState.empty(), ...(was?.unknown ? { unknown: was.unknown } : {}),
    });
    return JSON.stringify({ cube: cubeToJson(cube), notes: made.notes, changed: made.changed });
  } finally {
    planner.dispose();
  }
}

async function using<T>(spec: GridSpec, use: (o: Opened) => Promise<T>): Promise<T> {
  const o = await opened(spec);
  try {
    return await use(o);
  } finally {
    o.planner.dispose();
  }
}

/** A cell as JSON Python reads exactly: an integer past 2^53 and a float JSON cannot write are tagged. */
function cell(v: Scalar): unknown {
  if (typeof v === 'bigint') return { $int: v.toString() };
  if (typeof v === 'number' && (!Number.isFinite(v) || Object.is(v, -0))) return { $float: Object.is(v, -0) ? '-0.0' : String(v) };
  return v;
}

/** The view as the grid shows it: its columns (the leaves the screen shows, in its order), its rows, their tree, what
 *  was cut. */
function viewJson(view: CubeView, configuration: CubeState['configuration'], query: string): string {
  const leaves = shownColumns(view, configuration).leaves;
  const result = view.rows;
  const rows = Array.from({ length: result.rowCount }, (_, r) => leaves.map((l) => cell(result.columns[l.index]?.values[r] ?? null)));
  return JSON.stringify({
    columns: leaves.map((l) => ({
      name: l.name, path: l.path, type: l.type, dimension: l.isDimension, ...(l.label !== undefined ? { label: l.label } : {}),
    })),
    rows,
    tree: view.treeRows.map((t) => ({
      path: t.path, level: t.level, group: t.isGroup, expanded: t.expanded, total: t.isTotal, ...(t.isDetail ? { detail: true } : {}),
    })),
    truncated: view.truncated.map((l) => ({ level: l.level, parent: l.parent })),
    ...(view.totalRows !== undefined ? { totalRows: view.totalRows } : {}),
    sql: view.sql,
    query,
  });
}

/**
 * A CALCULATED COLUMN ADDED as DataCube's column editor adds one (ui/column-editor.ts), by its own steps
 * (column-draft.ts): a new row-stage column's draft, of the `kind` declared -- a measure unless told (null), as the
 * editor starts one (newDraft) -- named as calc.ts nameProblem allows, its expression (the whole lambda,
 * `x|$x.qty * 2`) parsed by the planner, the cube with it (draftCandidate) compiled as a refresh would send it
 * (CubeController.compile: its columns typed, then every query, window and child columns too) -- so Python checks no
 * calculated column by rules of its own (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 5). JSON: `{derived}`, the column
 * as the cube document holds it (its text: its lambda's numbers exact), or `{problems}`, why it was refused, in
 * DataCube's words.
 */
export async function gridCalculate(spec: GridSpec, name: string, expression: string,
  kind: ColumnKind | null): Promise<string> {
  return using(spec, async ({ planner, state }) => {
    if (kind !== null && !COLUMN_KINDS.includes(kind)) {
      const kinds = COLUMN_KINDS.map((k) => `'${k}'`).join(' or ');
      return JSON.stringify({ problems: [`a column's kind is ${kinds}, not '${String(kind)}'`] });
    }
    const draft = newDraft(name, expression, kind ?? undefined);
    const problem = nameProblem(state.snapshot, draftStage(draft), draft.name);
    if (problem !== null) return JSON.stringify({ problems: [problem] });
    let lambda: Lambda;
    try {
      lambda = await planner.parse(draft.expression.trim());
    } catch (e) {
      // the parse refused (the editor's #parsed): its message is the compiler's. Only the planner's refusal: a planner
      // that cannot answer (PlannerUnavailableError), or any other failure, is no mistake of the expression's
      if (!(e instanceof PlanError)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    const snapshot = draftCandidate(state.snapshot, undefined, draft, lambda);
    const outcome = await compiled(planner, { snapshot, tree: state.tree });
    if (outcome.refusal !== null) return JSON.stringify({ problems: [outcome.refusal] });
    return JSON.stringify({ derived: toJson(draftColumn(draft, lambda)) });
  });
}

/** The grid's columns as the compiler types them now, its calculated ones too: the controller's step 0 (plan.ts
 *  typeColumns), compile-only -- the snapshot the screen's editors work on. */
async function typedColumns(planner: LitePlanner, snapshot: CubeSnapshot): Promise<CubeSnapshot> {
  const runner = new PlanThenRun(planner, new DuckDbEngine(hostDatabase(hostCalls()).connection));
  return (await typeColumns(snapshot, runner)).snapshot;
}

/** The cube compiled as a refresh would send it (CubeController.compile: its columns typed, every query), never run. */
async function compiled(planner: LitePlanner, state: { snapshot: CubeSnapshot; tree: CubeState['tree'] }):
  Promise<{ readonly refusal: string | null }> {
  const controller = new CubeController(new DuckDbEngine(hostDatabase(hostCalls()).connection), planner);
  return controller.compile(state, null);
}

/** A filter as Python hands one in: its own conditions' operands, not yet the editor's values or columns. */
type GivenFilter =
  | { readonly kind: 'and' | 'or'; readonly children: readonly GivenFilter[] }
  | { readonly kind: 'not'; readonly child: GivenFilter }
  | { readonly kind: 'condition'; readonly column: unknown; readonly operator: unknown; readonly operand?: unknown };

const isValue = (v: unknown): v is FilterValue => typeof v === 'string' || typeof v === 'boolean'
  || (typeof v === 'number' && Number.isFinite(v)) || isRelativeDate(v) || isJsonValue(v);

const VALUES = 'text, a number, a boolean, a date as its text, {"relative": "today" | "now"} or {"json": text}';

/** What each operand is, in DataCube's words: what a condition missing it is told. */
const OPERAND_TOLD: Readonly<Record<'single' | 'list' | 'column', (op: string) => string>> = {
  single: (op) => `'${op}' takes one value`,
  list: (op) => `'${op}' takes a list of values, one at least, in their order`,
  column: (op) => `'${op}' compares to another column of the grid's`,
};

/**
 * `given` as the filter editor's draft tree (filter-operators.ts DraftNode): each condition's operand where its
 * operator's takes one (none, one value, a list in its order, another column) -- a value carried on the editor's
 * `stored` path, exactly, as a reopened filter's is -- its operator one a column of its type is offered (offers.ts),
 * over the grid's columns (`types`); a negation a negated group of one, so it keeps its shape -- or why not, in the
 * editor's own terms. An empty group is no filter at the top (`nested` false: the editor's normal form, `filter([])`)
 * and refused inside one, where the editor's conversion would leave it out: an API call has no half-written row.
 */
function draftFrom(given: GivenFilter, types: ReadonlyMap<string, string | undefined>, nested = false): DraftNode | string {
  switch (given.kind) {
    case 'and':
    case 'or': {
      if (nested && given.children.length === 0) return `an '${given.kind}' needs one condition at least`;
      const children: DraftNode[] = [];
      for (const child of given.children) {
        const made = draftFrom(child, types, true);
        if (typeof made === 'string') return made;
        children.push(made);
      }
      return { ...newGroup(children), join: given.kind };
    }
    case 'not': {
      const child = draftFrom(given.child, types, true);
      return typeof child === 'string' ? child : { ...newGroup([child]), not: true };
    }
    case 'condition':
      return conditionFrom(given, types);
  }
}

function conditionFrom(
  given: Extract<GivenFilter, { kind: 'condition' }>, types: ReadonlyMap<string, string | undefined>,
): DraftCondition | string {
  const { column, operator } = given;
  if (typeof column !== 'string' || !types.has(column)) {
    const names = [...types.keys()].sort().join(', ');
    return `the filter names ${JSON.stringify(column)}, not a column of the grid (${names})`;
  }
  const meta = OPERATORS.find((o) => o.op === operator);
  if (meta === undefined) {
    const all = OPERATORS.map((o) => o.op).sort().join(', ');
    return `${JSON.stringify(operator)} is not one of DataCube's filter operators: ${all}`;
  }
  const op = meta.op;
  if (!isCompatible(op, types.get(column))) {
    return `'${op}' is not offered on ${column}, a ${String(types.get(column))} column`;
  }
  const has = 'operand' in given;
  const operand = given.operand;
  // the editor's new row on the column, its operator chosen and its operand filled as the editor holds one
  const draft = (filled: Partial<DraftCondition>): DraftCondition => ({ ...newCondition(column), operator: op, ...filled });
  let made: DraftCondition;
  switch (meta.operand) {
    case 'none':
      if (has) return `'${op}' takes no value`;
      made = draft({});
      break;
    case 'column':
      if (!has || typeof operand !== 'string' || !types.has(operand)) {
        return `${OPERAND_TOLD.column(op)}: ${JSON.stringify(operand ?? null)} is not one of them`;
      }
      made = draft({ rightColumn: operand });
      break;
    case 'list':
      if (!has || !Array.isArray(operand) || operand.length === 0) return OPERAND_TOLD.list(op);
      if (!operand.every(isValue)) return `'${op}' takes ${VALUES} in its list`;
      made = draft({ stored: { column, text: '', value: operand } });
      break;
    case 'single':
      if (!has) return OPERAND_TOLD.single(op);
      if (Array.isArray(operand)) return `'${op}' takes one value, not several (several are for "in")`;
      if (!isValue(operand)) return `'${op}' takes ${VALUES}`;
      made = draft({ stored: { column, text: '', value: operand } });
      break;
  }
  // an API call has no half-written row to drop: a condition the editor would leave out is refused, never dropped
  if (toFilterNode(made, (c) => types.get(c)) === null) {
    return meta.operand === 'none' ? `'${op}' on ${column} makes no condition` : OPERAND_TOLD[meta.operand](op);
  }
  return made;
}

/**
 * A FILTER SET as DataCube's filter editor sets one (ui/filter-editor.ts): `given` (JSON, GivenFilter) built into the
 * editor's draft tree by its operators' own operands and offers (draftFrom), converted as the editor converts its tree
 * (filter-operators.ts toFilterNode: its normal form, a group of one that one, an empty group none), and the cube with
 * it compiled as a refresh would send it -- so Python checks no filter by rules of its own
 * (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 6). JSON: `{filter}` (null: no filter at all) or `{problems}`, why it
 * was refused.
 */
export async function gridFilter(spec: GridSpec, given: string): Promise<string> {
  return using(spec, async ({ planner, state }) => {
    const typed = await typedColumns(planner, state.snapshot);
    const types = new Map(rowColumns(typed).map((c) => [c.name, c.type] as const));
    const made = draftFrom(JSON.parse(given) as GivenFilter, types);
    if (typeof made === 'string') return JSON.stringify({ problems: [made] });
    const filter = toFilterNode(made, (c) => types.get(c)) ?? undefined;
    const { filter: _was, ...rest } = typed;
    const snapshot: CubeSnapshot = filter === undefined ? rest : { ...rest, filter };
    const outcome = await compiled(planner, { snapshot, tree: state.tree });
    if (outcome.refusal !== null) return JSON.stringify({ problems: [outcome.refusal] });
    return JSON.stringify({ filter: filter ?? null });
  });
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** A list of the grid's columns as Python sends one (a part named `what`), or why not: the edit judges the names. */
function namesFrom(given: unknown, what: string): string[] {
  if (!Array.isArray(given) || !given.every((n) => typeof n === 'string')) {
    throw new CubeRefusal(`${what} is a list of the grid's columns, not ${JSON.stringify(given)}`);
  }
  return given;
}

/** A field of Python's call as text, or why not. */
function textFrom(v: unknown, what: string): string {
  if (typeof v !== 'string') throw new CubeRefusal(`${what} is text, not ${JSON.stringify(v)}`);
  return v;
}

/** Python's edit, in order -- the settings, then Column Properties (a column made a dimension can then be grouped), the
 *  row groups and the pivot, the measures (a measure of a grouped column is one of its own), the sorts last (one may
 *  name a column a measure made) -- each by grid-edits.ts and config.ts, the screen's own rules. A part it does not
 *  know is refused. */
function edited(draft: GridDraft, asked: Record<string, unknown>): GridDraft {
  const unknown = Object.keys(asked).filter((k) => !EDIT_PARTS.includes(k));
  if (unknown.length > 0) throw new CubeRefusal(`a grid's edit is ${EDIT_PARTS.join(', ')}, any of them: not ${unknown.join(', ')}`);
  let d = draft;
  // the settings first, as a host patches them (CubeApp.applyConfiguration): one rule, config.ts withConfiguration
  if (asked['configuration'] !== undefined) d = configured(d, asked['configuration']);
  if (asked['columns'] !== undefined) {
    if (!isObject(asked['columns'])) throw new CubeRefusal("a grid's columns are set by name: {name: {kind, aggregate, weight}}");
    for (const [name, change] of Object.entries(asked['columns'])) {
      if (!isObject(change)) throw new CubeRefusal(`a column's properties are {kind, aggregate, weight}, not ${JSON.stringify(change)}`);
      const { kind, aggregate, weight, ...other } = change;
      const unknown = Object.keys(other);
      if (unknown.length > 0) throw new CubeRefusal(`a column's properties are kind, aggregate and weight, not ${unknown.join(', ')}`);
      // null clears an Aggregation or a weight, as the panel's "none" does
      d = setColumn(d, name, {
        ...(kind !== undefined ? { kind: textFrom(kind, "a column's kind") as ColumnKind } : {}),
        ...(aggregate !== undefined ? { aggregate: aggregate === null ? null : aggregateFrom(aggregate) } : {}),
        ...(weight !== undefined ? { weight: weight === null ? null : textFrom(weight, "a column's weight") } : {}),
      });
    }
  }
  if (asked['rows'] !== undefined) d = setGrouping(d, namesFrom(asked['rows'], 'the row groups'));
  if (asked['pivotOn'] !== undefined) d = setPivot(d, namesFrom(asked['pivotOn'], 'the pivot'));
  if (asked['measures'] !== undefined) {
    if (!Array.isArray(asked['measures'])) throw new CubeRefusal('the measures are a list');
    const taken = new Set<string>();
    for (const m of asked['measures']) {
      if (!isObject(m)) throw new CubeRefusal(`a measure is {name, column, fn[, weight]}, not ${JSON.stringify(m)}`);
      const measure: Measure = {
        name: textFrom(m['name'], "a measure's name").trim(),
        column: textFrom(m['column'], "a measure's column"),
        fn: aggregateFrom(m['fn']),
        ...(m['weight'] !== undefined ? { weight: textFrom(m['weight'], "a measure's weight") } : {}),
      };
      const one = measureAsColumn(d, measure, taken);
      if (one.made.column === measure.column) taken.add(measure.column);
      d = one.draft;
    }
  }
  if (asked['sorts'] !== undefined) {
    if (!Array.isArray(asked['sorts'])) throw new CubeRefusal('the sorts are a list');
    d = setSorts(d, asked['sorts'].map((x) => {
      if (!isObject(x)) throw new CubeRefusal(`a sort is {column, direction}, not ${JSON.stringify(x)}`);
      return { column: textFrom(x['column'], 'a sort'), direction: textFrom(x['direction'], "a sort's direction") as SortDirection };
    }));
  }
  // a sort added last, as the header menu's Add Ascending and Add Descending add one (grid-edits.ts addSort)
  if (asked['addSort'] !== undefined) {
    const x = asked['addSort'];
    if (!isObject(x)) throw new CubeRefusal(`a sort is {column, direction}, not ${JSON.stringify(x)}`);
    d = addSort(d, textFrom(x['column'], 'a sort'), textFrom(x['direction'], "a sort's direction") as SortDirection);
  }
  return d;
}

/** The parts of a grid's edit, in the order they are applied. */
const EDIT_PARTS = ['configuration', 'columns', 'rows', 'pivotOn', 'measures', 'sorts', 'addSort'];

/**
 * A grid's settings patched as a host patches them (config.ts withConfiguration, CubeApp.applyConfiguration's rule),
 * from JSON in the saved document's own keys: null clears a setting -- the patch's undefined, which JSON cannot write
 * -- among the settings, in the appearance and in a column's. A column's kind, Aggregation and weight go by Column
 * Properties' own rules (setColumn), so what the panel refuses is refused here too: a kind is set, never cleared, and
 * whether a measure is kept out of the pivot is true or false.
 */
function configured(draft: GridDraft, given: unknown): GridDraft {
  if (!isObject(given)) throw new CubeRefusal("a grid's configuration is its settings by name, in its document's own keys");
  const cleared = (o: Record<string, unknown>): Record<string, unknown> =>
    Object.fromEntries(Object.entries(o).map(([k, v]) => [k, v === null ? undefined : v]));
  const patched = (d: GridDraft, patch: Record<string, unknown>): GridDraft =>
    ({ ...d, config: withConfiguration(d.config, patch as Patch<CubeConfiguration>) });
  const { columns, appearance, ...settings } = given;
  // a setting DataCube has not (a name misspelt) is refused, never kept where nothing reads it (config.ts's lists)
  const known = (keys: readonly string[], settings: object, what: string): void => {
    const odd = keys.find((k) => !Object.hasOwn(settings, k));
    if (odd !== undefined) throw new CubeRefusal(`'${odd}' is not ${what} (${Object.keys(settings).join(', ')})`);
  };
  known(Object.keys(settings), CUBE_SETTINGS, "a grid's setting");
  if (appearance !== undefined && !isObject(appearance)) {
    throw new CubeRefusal("a grid's appearance is its settings by name, each cleared by null, not cleared whole");
  }
  if (isObject(appearance)) known(Object.keys(appearance), APPEARANCE_SETTINGS, 'an appearance setting');
  let d = patched(draft, { ...cleared(settings), ...(isObject(appearance) ? { appearance: cleared(appearance) } : {}) });
  if (columns === undefined) return d;
  if (!isObject(columns)) throw new CubeRefusal("a grid's columns' settings are by name: {name: {setting: value}}");
  for (const [name, c] of Object.entries(columns)) {
    const named = allColumns(d);
    // a pivot total is a column of its own (its heatmap, width, hiding: the screen's menu offers them), of a measure
    // column the grid has; its kind and Aggregation are its measure's
    const total = isPivotTotalColumn(name) ? name.slice(pivotTotalColumn('').length) : undefined;
    if (!named.includes(name) && (total === undefined || !named.includes(total))) {
      throw new CubeRefusal(`'${name}' is not a column of the grid (${named.join(', ')})`);
    }
    if (!isObject(c)) throw new CubeRefusal(`a column's settings are an object, not ${JSON.stringify(c)}`);
    known(Object.keys(c), COLUMN_SETTINGS, "a column's setting");
    const { kind, aggregateFn, aggregationParameters, excludedFromPivot, ...presentation } = c;
    if (total !== undefined && [kind, aggregateFn, aggregationParameters, excludedFromPivot].some((x) => x !== undefined)) {
      throw new CubeRefusal(`${name} is the pivot total of '${total}': its kind, Aggregation and pivot exclusion are `
        + `'${total}''s, set on '${total}'`);
    }
    if (kind === null) {
      throw new CubeRefusal(`${name}'s kind is set, never cleared: 'dimension' or 'measure' (Column Properties > Column Kind)`);
    }
    if (excludedFromPivot !== undefined && typeof excludedFromPivot !== 'boolean') {
      throw new CubeRefusal(`${name}'s excludedFromPivot is true or false, not ${JSON.stringify(excludedFromPivot)}`);
    }
    if (kind !== undefined || aggregateFn !== undefined || aggregationParameters !== undefined) {
      d = setColumn(d, name, {
        ...(kind !== undefined ? { kind: textFrom(kind, "a column's kind") as ColumnKind } : {}),
        ...(aggregateFn !== undefined ? { aggregate: aggregateFn === null ? null : aggregateFrom(aggregateFn) } : {}),
        ...(aggregationParameters !== undefined ? { weight: weightFrom(aggregationParameters, name) } : {}),
      });
    }
    d = patched(d, { columns: { [name]: { ...cleared(presentation), ...(excludedFromPivot !== undefined ? { excludedFromPivot } : {}) } } });
  }
  return d;
}

/** A weighted average's parameters as the document keeps them, `[weight column]` -- or null, cleared -- or why not. */
function weightFrom(given: unknown, name: string): string | null {
  if (given === null) return null;
  if (!Array.isArray(given) || given.length !== 1 || typeof given[0] !== 'string') {
    throw new CubeRefusal(`${name}'s aggregationParameters is [its weight column], not ${JSON.stringify(given)}`);
  }
  return given[0];
}

/** One of DataCube's aggregates, or why not. */
function aggregateFrom(fn: unknown): AggregateFn {
  if (typeof fn !== 'string' || !AGGREGATE_FNS.includes(fn as AggregateFn)) {
    throw new CubeRefusal(`${JSON.stringify(fn)} is not one of DataCube's aggregates: ${AGGREGATE_FNS.join(', ')}`);
  }
  return fn as AggregateFn;
}

/**
 * A GRID EDITED AS THE SCREEN EDITS ONE (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.3): `given` (JSON) any of
 * `configuration` (its settings, in the saved document's own keys, patched as a host patches them: config.ts
 * withConfiguration; null clears one; a column's kind, Aggregation and weight by Column Properties' rules), `columns`
 * ({name: {kind, aggregate, weight}}: Column Properties; null clears an Aggregation or a weight), `rows`, `pivotOn`,
 * `measures` ([{name, column, fn, weight?}]: each a column's Aggregation, or a calculated column of its name), `sorts`
 * ([{column, direction}], whole) and `addSort` ({column, direction}: the header menu's Add), in that order, by grid-edits.ts
 * -- the module the screen's panels, its header menu and its drag zones call -- then the cube compiled as Apply
 * compiles the screen's draft (app.ts #applyDraft: the configuration folded in, every query planned, none run). The
 * open groups stay, as the screen keeps them. JSON: `{cube, notes}`, the cube document as DataCube writes it (its
 * numbers exact) and what opening it over its source changed (openCube's notes), or `{problems}`, why it was refused,
 * in the screen's words. Python keeps no rule of a grid's query of its own.
 */
export async function gridEdit(spec: GridSpec, given: string): Promise<string> {
  return using(spec, async ({ planner, cube }) => {
    const asked: unknown = JSON.parse(given);
    if (!isObject(asked)) {
      return JSON.stringify({ problems: ["a grid's edit is {configuration, columns, rows, pivotOn, measures, sorts, addSort}, any of them"] });
    }
    const draft: GridDraft = { snapshot: await typedColumns(planner, cube.snapshot), config: cube.configuration };
    let d: GridDraft;
    try {
      d = edited(draft, asked);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    const snapshot = applyToSnapshot(d.snapshot, d.config);
    // the open groups stay, as the screen's edits keep them (app.ts #edit): a path the new grouping has not is shown closed
    const tree = cube.tree;
    const outcome = await compiled(planner, { snapshot, tree });
    if (outcome.refusal !== null) return JSON.stringify({ problems: [outcome.refusal] });
    const doc = readCube(fromJson(spec.cube));
    const written = writeCube({
      name: doc.name, source: doc.source, snapshot, configuration: d.config, tree,
      ...(doc.unknown ? { unknown: doc.unknown } : {}),
    });
    // what opening it over its source changed (openCube's notes), written with the edit: Python says them
    return JSON.stringify({ cube: cubeToJson(written), notes: cube.notes });
  });
}

/** What the grid shows, as DataCube computes it: JSON text (viewJson). */
export async function gridView(spec: GridSpec): Promise<string> {
  return using(spec, async ({ planner, state }) => {
    const controller = new CubeController(new DuckDbEngine(hostDatabase(hostCalls()).connection), planner);
    const view = await controller.run({ snapshot: state.snapshot, tree: state.tree });
    if (view === STALE) throw new Error('the grid was asked again before its view was ready');
    return viewJson(view, state.configuration, await planner.compose(view.query, 'STANDARD'));
  });
}

/** The grid's rows at their finest level as Pure, as Create a Class makes its class of them (classLambda). */
export async function gridRelation(spec: GridSpec): Promise<string> {
  return using(spec, async ({ planner, state }) => planner.compose(classLambda(state.snapshot), 'STANDARD'));
}

/**
 * A grid's rows as a class's, as Create a Class… asks for one (model-actions.ts `pageGrids`): its rows at their finest
 * level (classLambda), the table its source reads (storeTableOf), that table's first rows as the window writes them
 * into the project (sampleLambda, run on the host's database by sample-rows.ts sampleOf: item 7 of
 * docs/PYTHON_ASKS_DATACUBE_2026_10_15.md), the source they are read from, and the names the window offers (`named`:
 * the grid's source's and the page's) -- DataCube's own
 * semantics, which Python's `grid.create_class()` asks of DataCube's code before it calls the generic library
 * (legend_lite/model.py; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4). JSON, each query as the protocol writes it; a
 * source that is no table of its own refused in words (`{"refused"}`: the window leaves such a grid out of a data
 * space), a sample it cannot carry in its own (`{"problems"}`: the window refuses the whole action).
 */
export async function gridClass(spec: GridSpec, named: { readonly source: string; readonly page: string }):
  Promise<string> {
  return using(spec, async ({ planner, state }) => {
    const table = storeTableOf(state.snapshot.source.query);
    // refused in words, as the window leaves such a grid out (model-actions.ts pageGrids)
    if (!table) return JSON.stringify({ refused: "its source is not a table of its own: a class is made of a table's rows" });
    // the names the window offers before a person changes them (model-names.ts)
    const names = {
      className: suggestedClassName(named.source), package: suggestedPackage(named.page),
      dataSpace: suggestedDataSpaceName(named.page),
    };
    // its first rows as the window writes them into the project (sample-rows.ts sampleOf), read on the host's database
    const runner = new PlanThenRun(planner, new DuckDbEngine(hostDatabase(hostCalls()).connection));
    let sample: SampleRows;
    try {
      sample = await sampleOf(runner.host, sampleLambda(state.snapshot.source.query));
    } catch (e) {
      // refused in words, as the window says it (a NaN or an infinity the sample cannot carry): not a grid the window
      // leaves out, an action it refuses
      if (!(e instanceof SampleRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return `{"query":${toJson(classLambda(state.snapshot))},"sample":${JSON.stringify(sample)},`
      + `"table":${JSON.stringify(table)},"source":${JSON.stringify(planner.source())},"names":${JSON.stringify(names)}}`;
  });
}


/**
 * The grid exported, as its Export menu makes the file (export-render.ts, the screen's own core): what it shows, its
 * formats, appearance and heatmaps, as `kind`, one its Export menu offers (export-render.ts EXPORT_KINDS). JSON: its
 * name and type, and its content (text, or base64 for a workbook or a PDF); or `{problems}`, a kind it does not export.
 * No chart rides along: a page's board with its charts' pictures is the screen's (a chart drawn headless is not yet:
 * PARK-32).
 */
export async function gridExport(spec: GridSpec, kind: string): Promise<string> {
  if (!isExportKind(kind)) {
    const kinds = EXPORT_KINDS.map((k) => `'${k}'`).join(', ');
    return JSON.stringify({ problems: [`a grid exports as one of ${kinds}, not ${JSON.stringify(kind)}`] });
  }
  return using(spec, async ({ planner, state }) => {
    const controller = new CubeController(new DuckDbEngine(hostDatabase(hostCalls()).connection), planner);
    const view = await controller.run({ snapshot: state.snapshot, tree: state.tree });
    if (view === STALE) throw new Error('the grid was asked again before its view was ready');
    const config = state.configuration;
    const heatmaps = measureHeatmaps(view, view.treeRows, (name) => heatmapSetting(config, view, name));
    const file = renderExport(kind, {
      view, model: shownColumns(view, config), config,
      formats: columnFormats(config, state.snapshot, state.snapshot, view), formatters: new FormatterCache(),
      cellBackground: (leaf, row, value) => heatmapColour(heatmaps, view.treeRows, leaf, row, value), at: new Date(),
    });
    const { content } = file;
    return JSON.stringify({ name: file.name, mime: file.mime, text: typeof content === 'string',
      content: typeof content === 'string' ? content : base64(content) });
  });
}

/**
 * A chart of the grid as DataCube makes and judges it: made of what Python was told as the panel's form makes a change
 * by hand (chart-spec.ts chartFrom, editChart: of DataCube's own new chart, or of `before`), read as a page reads a
 * chart, and judged and kept as the charts' panel draws and keeps it -- a live chart regrouped the way the grid is now
 * (followCube) -- against the grid as opened: its columns as the compiler types them now, its calculated columns too
 * (chartProblems). The one place a chart's rules are written: Python's grid.chart(), Chart.plot() and Chart.options()
 * keep the chart this makes, or refuse in these words (legend_lite/page.py _made). JSON: `{"spec", "problems"}`
 * (`problems` empty when it draws).
 */
export async function gridChart(spec: GridSpec, given: string, before: string | null): Promise<string> {
  return using(spec, async ({ planner, state }) => {
    const was = before === null ? undefined : readChart(JSON.parse(before));
    if (typeof was === 'string') return JSON.stringify({ problems: [`DataCube cannot read the chart as it was: ${was}.`] });
    // the grid's columns as the compiler types them now, its calculated ones too: the controller's step 0 (plan.ts
    // typeColumns), compile-only -- what the grid's run would type them as before its chart is drawn
    const runner = new PlanThenRun(planner, new DuckDbEngine(hostDatabase(hostCalls()).connection));
    const { snapshot } = await typeColumns(state.snapshot, runner);
    const told = JSON.parse(given) as Record<string, unknown>;
    const made = chartFrom(told, snapshot, was);
    if (typeof made === 'string') return JSON.stringify({ problems: [made] });
    // kept as the panel keeps it: a live chart regrouped as the grid is now (ui/chart-panel.ts refresh) -- never with
    // what it was told replaced without a word
    const drawn = followCube(made, snapshot);
    const replaced = followRefusal(Object.keys(told), made, drawn);
    if (replaced !== null) return JSON.stringify({ problems: [replaced] });
    return JSON.stringify({ spec: drawn, problems: chartProblems(drawn, snapshot) });
  });
}
