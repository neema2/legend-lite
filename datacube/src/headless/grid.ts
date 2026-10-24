// A GRID, HEADLESS (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §4): a page's cube over its source, opened and
// run by DataCube's own code, with no screen -- openCube over the source as the compiler types it now (as a page opens
// a saved cube), its configuration folded into its query as the tab's state owner folds it (cube-state.ts `fold`: its
// row cap, each column's aggregate and kind, pivot totals and sorts), CubeController.run for its view (its levels,
// totals, pivot and truncation) laid out as the screen lays it out (shownColumns: hidden columns out, its order), and
// classLambda for its rows at their finest level (Create a Class's relation), which Python's plan-and-run reads for
// all of them (Grid.to_pandas, typed by its plan: docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15.3a). Its planner is
// legend-lite's over the host's worker; its rows the host's plan-and-run's, as a tab on the host reads them (D16).
//
// Each call opens the grid afresh from the cube document it is given: the document is the grid's state, Python's
// page keeps it, and nothing here holds a grid between calls.

import { snapRefusal } from '../../../engine-client/src/snap.ts';
import type { Scalar } from '../../../engine-client/src/result.ts';
import { fromJson, toJson, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { nameProblem } from '../calc.ts';
import { chartFrom, chartGrid, chartProblems, followCube, followRefusal, readChart, showGridWay } from '../chart-spec.ts';
import { draftColumn, draftStage, existingDraft, newDraft, type ColumnDraft } from '../column-draft.ts';
import { presetCommands, readGridPreset, type GridPreset } from '../grid-presets.ts';
import {
  argumentsGiven, gestureNamed, gestureReadsView, gridGestureGiven, type GestureArgs, type GestureView, type GridGestureName,
} from '../grid-gestures.ts';
import { CubeController, type CubeView } from '../cube.ts';
import { fold, openedState, type CubeState } from '../cube-state.ts';
import {
  CubeDocumentError, cubeOpenedAgain, cubeTaken, cubeToJson, fileChanged, fileSourceOf, frameSource, gridCapped, leavesOf, newGridTree, openCube,
  readCube, readCubeSource, rebuilt, remoteSourceOf, writeCube, type ClassSource, type CubeDocument, type CubeSource, type JoinSource,
  type OpenedCube, type QuerySource, type WarehouseSource,
} from '../cube-document.ts';
import {
  OPERATORS, isCompatible, newCondition, newGroup, toFilterNode, type DraftCondition, type DraftNode,
} from '../filter-operators.ts';
import { STALE } from '../epoch.ts';
import {
  EXPORT_KINDS, cellDecoration, columnFormats, conditionalSetting, heatmapColour, heatmapSetting, isExportKind, measureConditionals,
  measureHeatmaps, renderExport,
} from '../export-render.ts';
import { FormatterCache } from '../format.ts';
import { LitePlanner } from '../lite-planner.ts';
import { applyToSnapshot } from '../config.ts';
import { applyGridCommands, readGridCommand, readMeasure, type GridCommand } from '../grid-commands.ts';
import { measureCommands } from '../grid-measures.ts';
import type { GridDraft } from '../grid-edits.ts';
import { leftOutNote, leftOutWhenUnopened, plainNumbers, readGridCopy, readPage, type GridCopy, type PageView } from '../page-document.ts';
import { gridLens, readPageFilters } from '../page-filters.ts';
import { typeColumns } from '../plan.ts';
import { PlanError, type ModelOptions } from '../planner.ts';
import { classBasis, classLambda, snapPull, type ClassBasis } from '../query.ts';
import type { PlanAndRun, QueryLens } from '../runner.ts';
import { classSamples } from '../model-plans.ts';
import { SampleRefusal, type SampleRows } from '../sample-rows.ts';
import { formatOf } from '../upload.ts';
import { shownColumns } from '../shown-columns.ts';
import {
  COLUMN_KINDS, CubeRefusal, isJsonValue, isRelativeDate, rowColumns,
  type AggregateFn, type ChildAggregate, type Grain, type ColumnKind, type ColumnSpec, type CubeSnapshot, type DerivedColumn, type FilterNode,
  type FilterValue,
  type SortDirection, type WindowSpec,
} from '../snapshot.ts';
import { sourceColumns } from '../source-columns.ts';
import { pythonRunner } from '../services/python-host.ts';
import { hostCalls, hostEngine, hostWorker } from './host.ts';
import { leafOnHost, onHost, type HostBase, type HostLeaf, type HostTables } from './host-tables.ts';
import { classGiven, className, type ClassElement } from '../class-source.ts';
import { classTree, measuresOf, relatedName, relatedProblem, supertypesOf } from '../class-tree.ts';
import { modelMeasureCommands } from '../model-measure-commands.ts';
import { modelMeasures } from '../model-measures.ts';
import type { PageModelRef } from '../page-model.ts';
import { sharedPart, type SharedQuery } from '../../../query-store/src/share.ts';
import { joinOpenedOn } from './join.ts';
import { base64 } from './web.ts';

/** A grid as Python hands it in: its cube document (page-document's, as JSON text), and its base (gridBase's). */
export interface GridSpec {
  /** The cube document, as JSON text: read as the protocol's exact JSON (a `12.30` stays `12.30`). */
  readonly cube: string;
  /** The model its base is declared in, and the runtime its queries run on. */
  readonly model: string;
  readonly runtime: string;
  /** Its base relation, as Pure text: `#>{frames::DB.trades}#` -- or, a base no one table is (a join's), `relation`. */
  readonly source?: string;
  /** Its base relation as a lambda, as the protocol's JSON text (gridBase's, a join's: its numbers exact). */
  readonly relation?: string;
  /** Read through a mapping (a class, a saved query): the mapping, and the model's enumerations (planner.ts ModelOptions). */
  readonly mapping?: string;
  readonly enumerations?: readonly string[];
  /** Its rows its project's own (a saved query's: host-tables.ts HostBase, F28). */
  readonly project?: true;
  /** What the page's filters are to it (gridBaseOn's, its page given): the page's filters and the grid's charts, as the
   *  protocol's JSON text -- what a page's grid reads through, as the screen's grid does (page-filters.ts gridLens). */
  readonly lens?: string;
}

interface Opened {
  readonly planner: LitePlanner;
  /** What answers its queries: the host's plan-and-run (runnerOf). */
  readonly runner: PlanAndRun;
  /** The page's filters' rewrite of its queries, as the screen's grid takes it (page-filters.ts gridLens); none, none. */
  readonly lens?: QueryLens;
  readonly cube: OpenedCube;
  /** Its state as the query sees it: the configuration folded into the snapshot. */
  readonly state: CubeState;
}

/**
 * What answers a base's queries, ONE WAY WHATEVER THE BASE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D16): the host's
 * plan-and-run, which reads them where they are -- the tables the host serves (a frame, a file, a copy, a class's), a
 * warehouse table on its warehouse, a saved query's project's own (`project`: F28) -- asked by the tab's own runner over
 * the host's engine (services/python-host.ts pythonRunner), as a tab on the host reads every grid; its planner, over the
 * host's worker, typing what the grid shows. Through its mapping, when it has one.
 */
function runnerOf(spec: Omit<GridSpec, 'cube'>): { readonly planner: LitePlanner; readonly runner: PlanAndRun } {
  const calls = hostCalls();
  const how: ModelOptions = {
    ...(spec.mapping !== undefined ? { mapping: spec.mapping } : {}),
    ...(spec.enumerations !== undefined ? { enumerations: spec.enumerations } : {}),
    ...(spec.project === true ? { project: true as const } : {}),
  };
  const runner = pythonRunner(hostEngine(calls), spec.model, spec.runtime, how, () => hostWorker(calls));
  return { planner: runner.host.planner as LitePlanner, runner };
}

/** A source as the compiler types it now: its planner and runner (runnerOf), its relation, its columns. */
async function typed(spec: Omit<GridSpec, 'cube'>): Promise<{
  readonly planner: LitePlanner; readonly runner: PlanAndRun; readonly relation: ValueSpecification; readonly columns: ColumnSpec[];
}> {
  const { planner, runner } = runnerOf(spec);
  try {
    if ((spec.source === undefined) === (spec.relation === undefined)) {
      throw new Error('a grid is given its base as Pure text (source) or as a lambda (relation), one of the two');
    }
    const relation = (spec.relation !== undefined
      ? (fromJson(spec.relation) as unknown as Lambda).body[0]
      : (await planner.parse(`|${spec.source}`)).body[0]) as ValueSpecification | undefined;
    if (relation === undefined) throw new Error(`the source is not a relation: ${spec.source ?? spec.relation}`);
    return { planner, runner, relation, columns: await sourceColumns(planner, relation) };
  } catch (e) {
    planner.dispose();
    throw e;
  }
}

/** The grid opened: its planner over the host's worker, its cube over the source as the compiler types it now. */
async function opened(spec: GridSpec): Promise<Opened> {
  const { planner, runner, relation, columns } = await typed(spec);
  try {
    const doc = readCube(fromJson(spec.cube));
    const was = openCube(doc, { query: relation }, columns);
    // the cube as the screen opens it (cube-state.ts openedState): what the query carries read back, then folded
    const state = openedState(was.snapshot, was.configuration, was.tree);
    // what the page's filters do to it, as the screen's page gives its grid (page-app.ts #lensOf: one rule)
    const lens = spec.lens === undefined ? undefined : lensOf(spec.lens, doc.source);
    return { planner, runner, ...(lens ? { lens } : {}), cube: { ...was, configuration: state.configuration }, state };
  } catch (e) {
    planner.dispose();
    throw e;
  }
}

/** A frame as Python hands one in for a grid's cube: its source's model and relation (as GridSpec's), and its name -- or
 *  the file or remote file its host read in as a table (`leaf`, HostLeaf), which the cube's source records; or a source
 *  read where the host's engine reads it, as saved (`as`: a saved cube opened again, gridOpened). */
export interface FrameSpec extends Omit<GridSpec, 'cube'> {
  /** The frame's name on the host: the cube's source, `{_type: 'frame', name}`. */
  readonly frame: string;
  /** Not a frame: the file or remote file the host read in, the cube's source then (fileSourceOf, remoteSourceOf). */
  readonly leaf?: HostLeaf;
  /** Neither: a class, a saved query or a warehouse table, as the cube saved it -- written again with its columns now. */
  readonly as?: ClassSource | QuerySource | WarehouseSource;
}

/** The source a cube over a host's table records (gridCube): a frame's, else the file's or remote file's it is -- or one
 *  read where the host's engine reads it, as saved, its columns as the compiler types them now. */
function sourceOfSpec(spec: FrameSpec, columns: readonly ColumnSpec[]): CubeSource {
  if (spec.as !== undefined) return { ...spec.as, columns: columns.map((c) => ({ name: c.name, type: c.type })) };
  if (spec.leaf === undefined) return frameSource(spec.frame, columns);
  return spec.leaf._type === 'file'
    ? fileSourceOf(spec.leaf, formatOf(spec.leaf.name), columns) : remoteSourceOf(spec.leaf.url, columns);
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
    // written as the screen holds the grid it opens (cube-state.ts openedState): what the query carries read back, then
    // folded -- a new grid's raw rows written unfolded were another document than the screen's (parity.json's "a new
    // grid"), and a saved cube folded without the read-back lost what only its query carried
    const state = openedState(made.snapshot, made.configuration, made.tree ?? newGridTree(made.configuration));
    const cube = writeCube({
      name, source: sourceOfSpec(spec, columns), snapshot: state.snapshot, configuration: state.configuration,
      tree: state.tree, ...(was?.unknown ? { unknown: was.unknown } : {}),
    });
    return JSON.stringify({ cube: cubeToJson(cube), notes: made.notes, changed: made.changed });
  } finally {
    planner.dispose();
  }
}

/**
 * A NEW GRID'S CUBE OVER A FILE OR A REMOTE FILE THE HOST READ IN (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X4: Python's
 * `page.grid(file=...)`, `page.grid(remote=...)`), as gridCube makes one: over its base where the host reads it, the one
 * rule (host-tables.ts leafOnHost), its source the file's as the tab records it, titled after the file. `leaf`: the file
 * as the host read it in (headless ingest's and mount's `leaf`). JSON: as gridCube's.
 */
export async function leafCube(leaf: HostLeaf, host: HostTables, name: string, gridsOnPage: number): Promise<string> {
  const on = await leafOnHost(leaf, host);
  return gridCube({ ...on.base, frame: leaf.name, ...(on.leaf !== undefined ? { leaf: on.leaf } : {}) }, null, name, leaf.name, gridsOnPage);
}

/**
 * WHAT PYTHON NAMES A NEW GRID'S SOURCE BY (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X4: page.grid's `cls=`, `query=`,
 * `warehouse=`), as the source the tab records when it adds a grid over it, its columns still to be typed -- read by the
 * cube's own reader (readCubeSource), refused in its words:
 * - a class (`{class, project?, revision?}`), by the one rule the tab's Classes and Add a Grid over a class name it by
 *   too (class-source.ts classGiven: K16, K17);
 * - a saved query by its store's record (`{record}`): what the tab writes down of one (query-store sharedPart);
 * - a warehouse table (`{warehouse, catalog, table}`, its table `schema.name`, as the cube names it).
 */
export function givenSource(model: PageModelRef | null, given: Readonly<Record<string, unknown>>, place: string):
  ClassSource | QuerySource | WarehouseSource | string {
  try {
    if (given['class'] !== undefined) {
      const at = classGiven(model, given, place);
      if (typeof at === 'string') return at;
      return readCubeSource({ _type: 'class', name: className(at.class), ...at, columns: [] }) as ClassSource;
    }
    if (given['record'] !== undefined) {
      const q = given['record'] as SharedQuery | null;
      if (q === null || typeof q !== 'object' || typeof q.name !== 'string') return 'not a saved query: the store\'s record has no name';
      return readCubeSource({ _type: 'savedQuery', name: q.name, query: sharedPart(q), columns: [] }) as QuerySource;
    }
    if (given['warehouse'] !== undefined) {
      const { warehouse, catalog, table } = given;
      if (typeof warehouse !== 'string' || warehouse === '') return `a warehouse is named by its address, not ${JSON.stringify(warehouse)}`;
      if (typeof catalog !== 'string' || catalog === '') return `a warehouse table is in a catalog: name it (catalog=...), not ${JSON.stringify(catalog ?? null)}`;
      if (typeof table !== 'string' || !/^[^.]+\.[^.]+$/.test(table)) return `a warehouse table is named schema.table, not ${JSON.stringify(table ?? null)}`;
      const [schema, name] = table.split('.') as [string, string];
      return readCubeSource({ _type: 'warehouseTable', name: table, warehouse, catalog, schema, table: name, columns: [] }) as WarehouseSource;
    }
    return 'a grid is over a class, a saved query or a warehouse table';
  } catch (e) {
    if (e instanceof CubeDocumentError || e instanceof CubeRefusal) return e.message;
    throw e;
  }
}

/**
 * A NEW GRID'S CUBE OVER A CLASS, A SAVED QUERY OR A WAREHOUSE TABLE, AS PYTHON ADDS ONE (X4), as the tab adds one: the
 * source Python names (givenSource) read where the host reads it, the one rule (host-tables.ts leafOnHost: a class
 * through its project at its revision in the host's model home, a saved query through its project's version in the
 * host's depot, a warehouse table under the session the host holds for it), its columns as the compiler types them now
 * -- gridCube's cube, titled after the source. `model`: the page's (its `model`), or null. JSON: as gridCube's, or
 * `{problems}`, in DataCube's words.
 */
export async function sourceCube(model: string | null, given: string, host: HostTables, name: string, gridsOnPage: number,
  place: string): Promise<string> {
  const source = givenSource(model === null ? null : JSON.parse(model) as PageModelRef, JSON.parse(given) as Record<string, unknown>, place);
  if (typeof source === 'string') return JSON.stringify({ problems: [source] });
  try {
    const on = await leafOnHost(source, host);
    return await gridCube({ ...on.base, frame: source.name, as: source }, null, name, source.name, gridsOnPage);
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * WHAT A CUBE READS, as its host names it (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): its source's leaves
 * (cube-document.ts leavesOf, a join's in its order), each its kind and its name -- a frame's name its table in the
 * host's database -- and what a host reads it by (a file's bytes' hash, a remote file's URL). JSON: `[{_type, name,
 * sha256?, url?}]`.
 */
export function cubeLeaves(cube: string): string {
  return JSON.stringify(leavesOf(readCube(fromJson(cube)).source).map((l) => ({
    _type: l._type, name: l.name, ...(l._type === 'file' ? { sha256: l.sha256 } : {}), ...(l._type === 'remoteFile' ? { url: l.url } : {}),
  })));
}

/**
 * A CUBE'S BASE OVER THE HOST'S TABLES (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): what every grid call is given
 * (GridSpec) -- a leaf's, its own model and relation (a frame's accessor; a file's table's, a remote file's view's, as
 * DataCube read it in: host-tables.ts leafOnHost); a join's,
 * one model of its leaves' tables and the join over them (headless join.ts joinOpenedOn). A host asks this, never which
 * kind of source a cube has. JSON: `{model, runtime, source}` (Pure text) or `{model, runtime, relation}` (a lambda as
 * the protocol writes it, as text).
 */
export async function gridBase(cube: string, host: HostTables): Promise<string> {
  const on = await onHost(readCube(fromJson(cube)).source, host);
  if ('leaf' in on) return JSON.stringify(on.leaf.base);
  const opened = await joinOpenedOn(on.join, on.tables);
  return `{"model":${JSON.stringify(opened.model)},"runtime":${JSON.stringify(opened.runtime)},`
    + `"relation":${JSON.stringify(toJson({ _type: 'lambda', parameters: [], body: [opened.relation] }))}}`;
}

/** A copy its host holds, as the host modelled it (Python's engine: Engine.held_copies): what a snapped grid reads. */
export interface HeldCopy {
  readonly model: string;
  readonly runtime: string;
  /** The relation that reads it, as the protocol writes one. */
  readonly source: unknown;
}


/**
 * A GRID'S BASE ON THE HOST, the one rule (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z15; DATACUBE_MODEL_DESIGN §18.8):
 * a snapped grid reads its copy (`snapped`, the page's copy for it) as the host modelled it (`copies`, the copies the host
 * holds, by table) -- one the host no longer holds refused in words, never read live in its place; any other grid, or a
 * snapped one's live base (what a snap pulls, what a class is made of: no `snapped`), its base over the host's tables
 * (gridBase). The keeper's rebase asks it of each grid it applies a write to (headless page.ts pageRebase), and Python of
 * each grid it asks DataCube about (gridBaseOn).
 */
export async function baseOnHost(cube: string, snapped: GridCopy | undefined, host: HostTables,
  copies: Readonly<Record<string, HeldCopy>>): Promise<HostBase> {
  if (snapped === undefined) return JSON.parse(await gridBase(cube, host)) as HostBase;
  const copy = copies[snapped.table];
  if (copy === undefined) {
    throw new CubeRefusal(`${readCube(fromJson(cube)).name}: its snap (${snapped.label}) is no longer held -- go live to `
      + 'read its rows live again, or snap it anew');
  }
  return { model: copy.model, runtime: copy.runtime, relation: JSON.stringify({ _type: 'lambda', parameters: [], body: [copy.source] }) };
}

/**
 * A GRID'S BASE ON THE HOST, HEADLESS (baseOnHost): `cube` as the page holds it, `snapped` the page's copy for its grid
 * (null: it reads live, or its live base is asked), `host` and `copies` what the host serves and holds. JSON: `{model,
 * runtime, source}` or `{model, runtime, relation}`, as gridBase says it -- or `{problems}`, in DataCube's words.
 */
export async function gridBaseOn(cube: string, snapped: unknown, host: HostTables,
  copies: Readonly<Record<string, HeldCopy>>, page: string | null = null, id: string | null = null): Promise<string> {
  try {
    const base = await baseOnHost(cube, snapped === null ? undefined : readGridCopy(snapped), host, copies);
    if (page === null || id === null) return JSON.stringify(base);
    // its page given: what its filters are to it, the grid's charts with them (a click's filter is not its own chart's
    // grid's), carried as the protocol's JSON (their values exact) for each grid call to read it through (GridSpec.lens)
    const doc = readPage(page, { empty: true });
    const charts = doc.views.filter((v) => v.kind === 'chart' && v.cube === id).map((v) => v.id);
    return JSON.stringify({ ...base, lens: toJson({ filters: doc.filters ?? [], charts }) });
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A SAVED CUBE OPENED OVER THE HOST'S TABLES as it is now (§18.3): a leaf's (gridCube) -- a file's over the file given
 * for it as the page opens (`given`: a file's name, the hash of the file given for it), told when it is not the file it
 * was saved over (cube-document.ts fileChanged, as the tab tells a file picked for it) -- or a join's of the host's
 * tables, opened as the tab opens a saved join (join-open.ts openJoin: a kept column gone is left out, said), then
 * rebuilt over it (cube-document.ts rebuilt) and written as the screen holds it. `name` is its page's. JSON: `{cube,
 * notes, changed}`, as gridCube's, and its base -- `model`, `runtime`, and `source` (a frame's accessor) or `relation`
 * (a file's, a remote file's or a join's, a lambda), as gridBase says it -- which a tab on the host's page reads its rows
 * over (the host's runner: it has no compiler of its own there).
 */
// `taken`: whether the cube opened is not the one held -- its frames' columns moved, a part left out, its source written
// otherwise -- by the rule a page takes it by (cube-document.ts cubeTaken); `opened`, the source as it is now, which the
// page's `rebuilt` command carries (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P11, Z10); `how`, its base's ModelOptions.
// `leaveOut`, as its page opens: `{leftOut}`, the page's note, for a source the page leaves out (leftOutWhenUnopened)
export async function gridOpened(cube: string, host: HostTables, name: string, gridsOnPage: number,
  given: Readonly<Record<string, string>> = {}, leaveOut = false): Promise<string> {
  const was = readCube(fromJson(cube));
  try {
    return await openedOn(was, cube, host, name, gridsOnPage, given);
  } catch (e) {
    // `leaveOut`, as a page opens: a source the page leaves out when it cannot be opened, for any reason, said in the
    // page's words, the cube kept as saved (page-document.ts leftOutWhenUnopened, the tab's Open…'s rule; the S7 audit's
    // H-6) -- a join that no longer joins fails as its plan does, no refusal (the S7 confirmation audit's Y2-1)
    if (!leaveOut || !leftOutWhenUnopened(was.source)) throw e;
    return JSON.stringify({ leftOut: leftOutNote(was.source.name, e instanceof Error ? e.message : String(e)) });
  }
}

/** gridOpened's cube opened over the host's tables, as it is now. */
async function openedOn(was: CubeDocument, cube: string, host: HostTables, name: string, gridsOnPage: number,
  given: Readonly<Record<string, string>>): Promise<string> {
  const on = await onHost(was.source, host, given);
  if ('leaf' in on) {
    const { base, leaf } = on.leaf;
    // the leaf's columns as the compiler types them now (gridCube): the source as it is now
    const read = was.source;
    const as = read._type === 'class' || read._type === 'savedQuery' || read._type === 'warehouseTable' ? read : undefined;
    const made = JSON.parse(await gridCube({ ...base, frame: read.name, ...(leaf !== undefined ? { leaf } : {}),
      ...(as !== undefined ? { as } : {}) }, cube, name, read.name, gridsOnPage)) as { cube: string };
    const now = readCube(fromJson(made.cube)).source;
    return openedAgain(was, now, now._type === 'file' ? fileChanged(was.source, now) : [], base);
  }
  const opened = await joinOpenedOn(on.join, on.tables);
  const now: JoinSource = { ...on.join, how: opened.how, columns: opened.columns.map((c) => ({ name: c.name, type: c.type })) };
  return openedAgain(was, now, opened.notes, { model: opened.model, runtime: opened.runtime,
    relation: toJson({ _type: 'lambda', parameters: [], body: [opened.relation] }) });
}

/** A saved cube opened again over its source as it is now (`now`), by the page's own rule (cube-document.ts
 *  cubeOpenedAgain), as gridOpened answers it: what opening said (`said` first), and `base`, where its rows are read --
 *  its relation, when it is a lambda, written as one. */
function openedAgain(was: CubeDocument, now: CubeSource, said: readonly string[], base: HostBase): string {
  const again = cubeOpenedAgain(was, now);
  const changed = again.changed || said.length > 0;
  const reads = 'source' in base ? `"source":${JSON.stringify(base.source)}` : `"relation":${base.relation}`;
  // how its rows are planned over its model besides (a class's mapping, a model's enumerations, a saved query's own
  // project): what a tab on the host's page reads its rows by (the S7 audit's H-2)
  const how = { ...(base.mapping !== undefined ? { mapping: base.mapping } : {}),
    ...(base.enumerations !== undefined ? { enumerations: base.enumerations } : {}), ...(base.project ? { project: true } : {}) };
  return `{"cube":${JSON.stringify(cubeToJson(again.document))},"notes":${JSON.stringify([...said, ...again.notes])},`
    + `"changed":${JSON.stringify(changed)},"taken":${JSON.stringify(cubeTaken(changed, was.source, now))},`
    + `"opened":${JSON.stringify(toJson(now))},"model":${JSON.stringify(base.model)},"runtime":${JSON.stringify(base.runtime)},`
    + `"how":${JSON.stringify(how)},${reads}}`;
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
 * A CALCULATED COLUMN ADDED -- or, `replacing` one, EDITED -- AS DATACUBE'S COLUMN EDITOR MAKES ONE (ui/column-editor.ts),
 * by its own steps (column-draft.ts), so Python checks no calculated column by rules of its own
 * (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 5): `given`, the editor's form, `{name, replacing?, level?, kind?,
 * mode?, expression?, explode?, window?, child?}` -- before the grouping (`level` 'row', of the `kind` declared: a measure
 * unless told, as the editor starts one) or after it ('group', no kind); an expression (the whole lambda, `x|$x.qty * 2`,
 * parsed by the planner; `explode`: each row repeated once per element of what it gives, row level only), a window over
 * the rows around it, or (group level) an aggregate of the child groups' figures -- each read as a calculate command is
 * read (readGridCommand), named as calc.ts nameProblem allows, the cube with it compiled as a refresh would send it. An
 * edit starts from the column as it is (Edit Column opens on it). JSON: as gridApply's -- the cube document as the screen
 * writes the editor's OK, the `calculate` (or `editCalculated`) command among `commands` -- or `{problems}`, in
 * DataCube's words.
 */
export async function gridColumn(spec: GridSpec, given: string): Promise<string> {
  const asked = JSON.parse(given) as {
    name?: unknown; replacing?: unknown; level?: unknown; kind?: ColumnKind | null; mode?: unknown;
    expression?: unknown; explode?: unknown; window?: unknown; child?: unknown; grain?: unknown;
  };
  const kind = asked.kind ?? null;
  if (kind !== null && !COLUMN_KINDS.includes(kind)) {
    return JSON.stringify({ problems: [`a column's kind is ${COLUMN_KINDS.map((k) => `'${k}'`).join(' or ')}, not '${String(kind)}'`] });
  }
  if (asked.level !== undefined && asked.level !== 'row' && asked.level !== 'group') {
    return JSON.stringify({ problems: [`a calculated column's level is 'row' or 'group', not ${JSON.stringify(asked.level)}`] });
  }
  if (asked.level === 'group' && kind !== null) {
    return JSON.stringify({ problems: ['a group-level column has no kind: it is computed after the grouping'] });
  }
  const mode = asked.mode ?? 'expression';
  if (!isDraftMode(mode)) {
    return JSON.stringify({ problems: [`a calculated column is an expression, a window, a child aggregate or a grain, not ${JSON.stringify(mode)}`] });
  }
  if (mode === 'children' && asked.level !== 'group') {
    return JSON.stringify({ problems: ["a child aggregate is a group-level column: its level is 'group'"] });
  }
  if (mode === 'grain' && asked.level !== 'group') {
    return JSON.stringify({ problems: ["a measure at a grain is a group-level column: its level is 'group'"] });
  }
  if (asked.explode === true && (mode !== 'expression' || asked.level === 'group')) {
    return JSON.stringify({ problems: ['explode repeats each row once per element of an expression: a row-level expression only'] });
  }
  if (typeof asked.name !== 'string') return JSON.stringify({ problems: [`a calculated column's name is text, not ${JSON.stringify(asked.name)}`] });
  const name = asked.name;
  if (mode !== 'expression') {
    // a window's, a child aggregate's or a grain's fields read as a calculate command's are, refused in its words
    try {
      readGridCommand({ kind: 'calculate', level: asked.level === 'group' ? 'group' : 'row',
        column: { name, ...(mode === 'window' ? { window: asked.window } : mode === 'grain' ? { grain: asked.grain }
          : { childAggregate: asked.child }) } });
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
  }
  // the column an edit replaces, named -- an edit naming none is refused, never taken for a new column (the S7 audit's
  // fourth round's 3: Python's edit_calculated(None, ...) added one)
  if ('replacing' in asked && typeof asked.replacing !== 'string') {
    return JSON.stringify({ problems: [`the calculated column edited is named by text, not ${JSON.stringify(asked.replacing)}`] });
  }
  const replacing = typeof asked.replacing === 'string' ? asked.replacing : null;
  return using(spec, async (o) => {
    const { planner, state } = o;
    // an edit starts from the column as it is (Edit Column opens on it: its level, its explode), as told otherwise
    // a related column is the class tree's (gridRelated), never a formula to edit
    if (replacing !== null && state.snapshot.derived.some((d) => d.name === replacing && d.related !== undefined)) {
      return JSON.stringify({ problems: [`'${replacing}' is a related column: remove it and add another (grid.related)`] });
    }
    const was = replacing === null ? undefined : existingDraft(state.snapshot, replacing)?.draft;
    if (replacing !== null && was === undefined) {
      return JSON.stringify({ problems: [`'${replacing}' is not a calculated column of the grid`] });
    }
    const expression = typeof asked.expression === 'string' ? asked.expression : '';
    const start = was ?? newDraft(name, expression, kind ?? undefined);
    const level = asked.level === 'group' ? 'group' : kind ?? (asked.level === 'row' && start.level === 'group' ? 'measure' : start.level);
    const draft: ColumnDraft = {
      ...start, name, expression, mode, level,
      ...(asked.explode !== undefined ? { unnest: asked.explode === true } : {}),
      ...(mode === 'window' ? { window: asked.window as WindowSpec } : {}),
      ...(mode === 'children' ? { child: asked.child as ChildAggregate } : {}),
      ...(mode === 'grain' ? { grain: asked.grain as Grain } : {}),
    };
    const problem = nameProblem(state.snapshot, draftStage(draft), draft.name, replacing ?? undefined);
    if (problem !== null) return JSON.stringify({ problems: [problem] });
    let lambda: Lambda | undefined;
    if (mode === 'expression') {
      try {
        lambda = await planner.parse(draft.expression.trim());
      } catch (e) {
        // the parse refused (the editor's #parsed): its message is the compiler's. Only the planner's refusal: a planner
        // that cannot answer (PlannerUnavailableError), or any other failure, is no mistake of the expression's
        if (!(e instanceof PlanError)) throw e;
        return JSON.stringify({ problems: [e.message] });
      }
    }
    // the column as the editor makes it (column-draft.ts draftColumn), by the command the editor's OK builds
    const column = draftColumn(draft, lambda);
    return applied(spec, o, [replacing === null
      ? { kind: 'calculate', level: draftStage(draft), column }
      : { kind: 'editCalculated', name: replacing, level: draftStage(draft), column }]);
  });
}

const isDraftMode = (m: unknown): m is ColumnDraft['mode'] => m === 'expression' || m === 'window' || m === 'children' || m === 'grain';

/**
 * A RELATED COLUMN ADDED AS THE HEADER MENU'S ADD RELATED COLUMN ADDS ONE (snapshot.ts RelatedPath; Python's grid.related):
 * `given` `{path, aggregate?, name?}` -- read as a calculate command's related column is, its path judged against the
 * class tree of the grid's model (class-tree.ts relatedProblem: a property the class reaches, a to-many end aggregated)
 * -- added by the column editor's command, named after its path unless named, a measure when it aggregates. JSON: as
 * gridApply's, or `{problems}`, in DataCube's words.
 */
export async function gridRelated(spec: GridSpec, given: string): Promise<string> {
  const asked = JSON.parse(given) as { path?: unknown; aggregate?: unknown; name?: unknown };
  const source = readCube(fromJson(spec.cube)).source;
  if (source._type !== 'class') {
    return JSON.stringify({ problems: [`a related column reads a class's relationships, and this grid is over ${source.name}, no class`] });
  }
  if (asked.name !== undefined && typeof asked.name !== 'string') {
    return JSON.stringify({ problems: [`a related column's name is text, not ${JSON.stringify(asked.name)}`] });
  }
  let column: DerivedColumn;
  try {
    const read = readGridCommand({ kind: 'calculate', level: 'row', column: { name: asked.name ?? 'related', related: { path: asked.path,
      ...(asked.aggregate !== undefined ? { aggregate: asked.aggregate } : {}) } } });
    column = (read as { readonly column: DerivedColumn }).column;
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
  const related = column.related!;
  return using(spec, async (o) => {
    const problem = relatedProblem(classTree(await o.planner.modelElements(spec.model) as ClassElement[]), source.class, related);
    if (problem !== null) return JSON.stringify({ problems: [problem] });
    const base = relatedName(related);
    let name = asked.name as string | undefined;
    if (name === undefined) {
      name = base;
      for (let i = 2; nameProblem(o.state.snapshot, 'row', name) !== null; i += 1) name = `${base}_${i}`;
    }
    return applied(spec, o, [{ kind: 'calculate', level: 'row',
      column: { name, related, kind: related.aggregate !== undefined ? 'measure' : 'dimension' } }]);
  });
}

/**
 * A MODEL MEASURE ADDED AS THE HEADER MENU'S ADD MODEL MEASURE ADDS ONE (model-measures.ts; Python's grid.model_measure):
 * `given` `{measure}`, by its name or its path, of the grid's class or a supertype of it in the grid's model -- added as the
 * steps a person would make (modelMeasureCommands). JSON: as gridApply's, or `{problems}`, in DataCube's words.
 */
export async function gridModelMeasure(spec: GridSpec, given: string): Promise<string> {
  const asked = JSON.parse(given) as { measure?: unknown };
  const source = readCube(fromJson(spec.cube)).source;
  if (source._type !== 'class') {
    return JSON.stringify({ problems: [`a model measure is of a class's instances, and this grid is over ${source.name}, no class`] });
  }
  if (typeof asked.measure !== 'string' || asked.measure === '') {
    return JSON.stringify({ problems: [`a model measure is named by its name or its path, not ${JSON.stringify(asked.measure)}`] });
  }
  return using(spec, async (o) => {
    const elements = await o.planner.modelElements(spec.model) as ClassElement[];
    const all = modelMeasures(elements);
    const mine = measuresOf(all, (c) => supertypesOf(elements, c), source.class);
    const measure = mine.find((m) => m.path === asked.measure || m.name === asked.measure);
    if (measure === undefined) {
      return JSON.stringify({ problems: [`${className(source.class)} has no measure ${asked.measure as string} in its model`
        + ` (${mine.map((m) => m.name).sort().join(', ') || 'none'})`] });
    }
    let commands: GridCommand[];
    try {
      commands = modelMeasureCommands(o.state.snapshot, classTree(elements), source.class, all, measure);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return applied(spec, o, commands);
  });
}

/**
 * A PRESET ADDED AS THE HEADER MENU ADDS ONE (grid-presets.ts presetCommands: a date to its period, a measure shown as,
 * a number to its bin -- Python's grid.grain, grid.show_as, grid.bins): `given`, the preset as JSON (readGridPreset),
 * over the grid's columns as the compiler types them now. JSON: as gridApply's, or `{problems}`, in DataCube's words.
 */
export async function gridPreset(spec: GridSpec, given: string): Promise<string> {
  let preset: GridPreset;
  try {
    preset = readGridPreset(JSON.parse(given));
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
  return using(spec, async (o) => {
    let commands: GridCommand[];
    try {
      commands = presetCommands(await typedColumns(o.runner, o.state.snapshot), preset);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return applied(spec, o, commands);
  });
}

/**
 * A GRID'S GESTURE AS PYTHON MAKES IT (grid.gesture, and the named ways in that are gestures: toggle_sort,
 * toggle_group_sort, dimension, drill, drill_up, add_group ...; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8): gesture
 * `name` (grid-gestures.ts GRID_GESTURES: the menu's own ids where the menu has it) made on the grid as it is, given its
 * arguments by name (`given`, JSON), by the one function the screen's control calls (grid-gestures.ts gridGesture) --
 * a gesture that reads the grid's view given it as the screen paints it (its headers, shown-columns.ts shownColumns;
 * its pivot's columns), the grid run for it -- its commands read as gridApply reads Python's, then applied as gridApply
 * applies them. JSON: as gridApply's;
 * `{commands: []}` alone, a gesture that changes nothing; or `{problems}`, in DataCube's words.
 */
export async function gridGesture(spec: GridSpec, name: string, given: string): Promise<string> {
  // named and given by name, judged before the grid is opened for it
  let gesture: GridGestureName;
  let args: GestureArgs;
  try {
    gesture = gestureNamed(name);
    args = argumentsGiven(untaggedDeep(JSON.parse(given)));
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
  return using(spec, async (o) => {
    let view: GestureView | null = null;
    if (gestureReadsView(gesture, o.state.snapshot, args)) {
      const shown = await controllerOf(o).run({ snapshot: o.state.snapshot, tree: o.state.tree });
      if (shown === STALE) throw new Error('the grid was asked again before its view was ready');
      view = { headers: shownColumns(shown, o.state.configuration).leaves, pivot: shown.pivot?.columns ?? [] };
    }
    let commands: GridCommand[];
    try {
      // its commands read by the commands' own reader, as every command Python builds is: what it carries from Python's
      // JSON unjudged (a filter, a group's path, a calculated column) judged there
      commands = gridGestureGiven(gesture, {
        snapshot: o.state.snapshot, config: o.state.configuration, dimensions: o.cube.configuration.dimensions ?? [], view,
      }, args).map(readGridCommand);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return commands.length === 0 ? JSON.stringify({ commands: [] }) : applied(spec, o, commands);
  });
}

/** What a page's filters (GridSpec.lens) do to a grid over `source`: the screen's own rule (page-filters.ts gridLens). */
function lensOf(text: string, source: CubeSource): QueryLens | undefined {
  const said = fromJson(text) as unknown as { readonly filters: unknown; readonly charts: readonly string[] };
  const charts = new Set(said.charts);
  // its numbers plain, as a page's filters are read (page-document.ts readPage): a filter's value is a plain number
  return gridLens(readPageFilters(plainNumbers(said.filters), charts), source, charts).lens;
}

/** The grid's columns as the compiler types them now, its calculated ones too: the controller's step 0 (plan.ts
 *  typeColumns), compile-only -- the snapshot the screen's editors work on. */
async function typedColumns(runner: PlanAndRun, snapshot: CubeSnapshot): Promise<CubeSnapshot> {
  return (await typeColumns(snapshot, runner)).snapshot;
}

/** The cube compiled as a refresh would send it (CubeController.compile: its columns typed, every query), never run. */
async function compiled(o: Opened, state: { snapshot: CubeSnapshot; tree: CubeState['tree'] }):
  Promise<{ readonly refusal: string | null }> {
  return controllerOf(o).compile(state, null);
}

/** The grid's controller as the screen's grid has it: its runner, the page's filters' lens on it. */
function controllerOf(o: Opened): CubeController {
  const controller = new CubeController(o.runner);
  controller.setLens(o.lens);
  return controller;
}

/** A filter as Python hands one in: its own conditions' operands, not yet the editor's values or columns. */
export type GivenFilter =
  | { readonly kind: 'and' | 'or'; readonly children: readonly GivenFilter[] }
  | { readonly kind: 'not'; readonly child: GivenFilter }
  | { readonly kind: 'condition'; readonly column: unknown; readonly operator: unknown; readonly operand?: unknown };

const isValue = (v: unknown): v is FilterValue => typeof v === 'string' || typeof v === 'boolean'
  || (typeof v === 'number' && Number.isFinite(v)) || isRelativeDate(v) || isJsonValue(v);

const VALUES = 'text, a number, a boolean, a date or a timestamp, {"relative": "today" | "now"} or {"json": text}';

/** A host's form of a filter, said in its words (readGivenFilter). */
const FILTER_FORM = 'a filter is a condition (column, operator, value), a group ("and", [...]) or ("not", ...)';
/** What gridFilter's refusals add: how a host writes a condition. */
const CONDITION_FORM = 'a condition is (column, operator) or (column, operator, value), its value a list where the '
  + 'operator takes several';

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/**
 * A VALUE AS A HOST TAGS IT where JSON has no form for it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D12; a grid's cells
 * come back tagged the same way): a date, a timestamp and a finite decimal (`{"$date": text}`, `{"$timestamp": text}`,
 * `{"$decimal": text}`) are their exact text, as the filter editor keeps one; anything else as it came -- a number JSON
 * cannot write (`{"$float": "NaN"}`) is then no value a filter takes, refused by its operand's check in its words.
 */
export function untagged(v: unknown): unknown {
  if (!isRecord(v) || Object.keys(v).length !== 1) return v;
  const text = v['$date'] ?? v['$timestamp'] ?? v['$decimal'];
  if (typeof text !== 'string') return v;
  return v['$decimal'] !== undefined && !Number.isFinite(Number(text)) ? v : text;
}

/** A gesture's arguments as Python's JSON carries them (its `_tagged`), each tagged value untagged as a filter's is,
 *  however deep it sits (a filter's value, a condition's in a refilter). */
function untaggedDeep(v: unknown): unknown {
  const flat = untagged(v);
  if (flat !== v) return flat;
  if (Array.isArray(v)) return v.map(untaggedDeep);
  if (isRecord(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, untaggedDeep(x)]));
  return v;
}

/**
 * A FILTER AS A HOST WRITES ONE (D12, Z12: Python's `grid.filter`), read into the filter editor's tree (GivenFilter):
 * a condition `[column, operator]` or `[column, operator, operand]`, a group `["and" | "or", [nodes]]`, a negation
 * `["not", node]`, or a list of nodes -- all of them -- each value as JSON carries it, tagged where JSON has no form for
 * it (untagged); a set (`{"$set": [...]}`) is refused, a list of values being in its order. Why not, in words.
 */
export function readGivenFilter(raw: unknown): GivenFilter | string {
  if (!Array.isArray(raw)) return `${FILTER_FORM}, not ${JSON.stringify(raw)}`;
  const head: unknown = raw[0];
  const nodes = (list: readonly unknown[]): GivenFilter[] | string => {
    const made: GivenFilter[] = [];
    for (const n of list) {
      const read = readGivenFilter(n);
      if (typeof read === 'string') return read;
      made.push(read);
    }
    return made;
  };
  if (raw.length === 0 || typeof head !== 'string') {
    // a list of nodes: all of them (none: no filter)
    const children = nodes(raw);
    return typeof children === 'string' ? children : { kind: 'and', children };
  }
  if ((head === 'and' || head === 'or') && raw.length === 2 && Array.isArray(raw[1])) {
    const children = nodes(raw[1] as unknown[]);
    return typeof children === 'string' ? children : { kind: head, children };
  }
  if (head === 'not' && raw.length === 2) {
    const child = readGivenFilter(raw[1]);
    return typeof child === 'string' ? child : { kind: 'not', child };
  }
  if (raw.length < 2) return `${FILTER_FORM}, not ${JSON.stringify(raw)}`;
  if (raw.length > 3) return `a condition is (column, operator) or (column, operator, value), not ${JSON.stringify(raw)}`;
  if (raw.length === 2) return { kind: 'condition', column: head, operator: raw[1] };
  const given: unknown = raw[2];
  if (isRecord(given) && Object.keys(given).length === 1 && given['$set'] !== undefined) {
    return 'a filter takes a list of values, in their order (a list or a tuple), not a set';
  }
  return { kind: 'condition', column: head, operator: raw[1], operand: Array.isArray(given) ? given.map(untagged) : untagged(given) };
}

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
export function draftFrom(given: GivenFilter, types: ReadonlyMap<string, string | undefined>, nested = false): DraftNode | string {
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
 * A FILTER SET as DataCube's filter editor sets one (ui/filter-editor.ts): `given` (JSON, as a host writes a filter:
 * readGivenFilter, D12) built into the
 * editor's draft tree by its operators' own operands and offers (draftFrom), converted as the editor converts its tree
 * (filter-operators.ts toFilterNode: its normal form, a group of one that one, an empty group none), and the cube with
 * it compiled as a refresh would send it -- so Python checks no filter by rules of its own
 * (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 6). JSON: as gridApply's, the `filter` command applied among
 * `commands` (its filter null: no filter at all), or `{problems}`, why it was refused.
 */
export async function gridFilter(spec: GridSpec, given: string): Promise<string> {
  const read = readGivenFilter(JSON.parse(given));
  if (typeof read === 'string') return JSON.stringify({ problems: [read] });
  return using(spec, async (o) => {
    const made = await editorFilter(o, read);
    if ('problem' in made) return JSON.stringify({ problems: [made.problem] });
    // the editor's normal form, set by the command the filter editor's Apply builds
    return applied(spec, o, [{ kind: 'filter', filter: made.filter }]);
  });
}

/**
 * ONE CONDITION ADDED TO THE FILTER, as a cell's Add Filter adds one (grid-commands.ts addFilter: AND-ed with the filter
 * the grid has): `given` (JSON, as gridFilter takes it) built and converted as the filter editor builds and converts one
 * (editorFilter) -- one condition, as the command takes. JSON: as gridApply's, or `{problems}`, in DataCube's words.
 */
export async function gridAddFilter(spec: GridSpec, given: string): Promise<string> {
  const read = readGivenFilter(JSON.parse(given));
  if (typeof read === 'string') return JSON.stringify({ problems: [read] });
  return using(spec, async (o) => {
    const made = await editorFilter(o, read);
    if ('problem' in made) return JSON.stringify({ problems: [made.problem] });
    if (made.filter === null || made.filter.kind !== 'condition') {
      return JSON.stringify({ problems: [`a filter added is one condition, not ${given} -- ${CONDITION_FORM}`] });
    }
    return applied(spec, o, [{ kind: 'addFilter', condition: made.filter }]);
  });
}

/** `read` built into the filter editor's draft over the grid's columns as the compiler types them now (draftFrom: its
 *  operators' own operands and offers) and converted as the editor converts its tree (toFilterNode: its normal form, a
 *  group of one that one, an empty group none) -- or why not, in its words. */
async function editorFilter(o: Opened, read: GivenFilter): Promise<{ filter: FilterNode | null } | { problem: string }> {
  const typed = await typedColumns(o.runner, o.state.snapshot);
  const types = new Map(rowColumns(typed).map((c) => [c.name, c.type] as const));
  const made = draftFrom(read, types);
  if (typeof made === 'string') return { problem: `${made} -- ${CONDITION_FORM}` };
  return { filter: toFilterNode(made, (c) => types.get(c)) };
}

/**
 * COMMANDS APPLIED AS THE SCREEN APPLIES AN EDIT (grid-commands.ts: the one module the screen's handlers and Python's
 * calls build commands for -- docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md): to the grid as opened, its columns typed as
 * the compiler types them now; then the cube compiled as Apply compiles the screen's draft (the configuration folded
 * in, every query planned, none run) and written as the screen's state lands (its columns typed by the run, folded).
 * The open groups stay, as the screen keeps them. JSON: `{cube, notes, commands}` -- the cube document as DataCube
 * writes it (its numbers exact), what opening it over its source changed (openCube's notes), and the commands applied
 * -- or `{problems}`, why it was refused, in the screen's words.
 */
async function applied(spec: GridSpec, o: Opened, commands: readonly GridCommand[]): Promise<string> {
  const { runner, cube } = o;
  let d: Required<GridDraft>;
  try {
    d = applyGridCommands({ snapshot: await typedColumns(runner, cube.snapshot), config: cube.configuration, tree: cube.tree },
      commands);
  } catch (e) {
    if (!(e instanceof CubeRefusal)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
  const tree = d.tree;
  const snapshot = applyToSnapshot(d.snapshot, d.config);
  const outcome = await compiled(o, { snapshot, tree });
  if (outcome.refusal !== null) return JSON.stringify({ problems: [outcome.refusal] });
  // as the screen's state lands after the edit: its columns typed by the run (the controller's step 0), folded
  const landed = fold({ snapshot: await typedColumns(runner, snapshot), configuration: d.config, tree });
  const doc = readCube(fromJson(spec.cube));
  const written = writeCube({
    name: doc.name, source: doc.source, snapshot: landed.snapshot, configuration: landed.configuration, tree,
    ...(doc.unknown ? { unknown: doc.unknown } : {}),
  });
  return `{"cube":${JSON.stringify(cubeToJson(written))},"notes":${JSON.stringify(cube.notes)},`
    + `"commands":[${commands.map((c) => toJson(c)).join(',')}]}`;
}

/** A new grid's query as Python gives it in one call: each part a list, a measure and a sort a pair. */
interface GridSetUp {
  readonly dimensions: readonly string[];
  readonly rows: readonly string[];
  readonly columns: readonly string[];
  /** Each `[column, aggregate]`: a measure of the column, named as it is. */
  readonly measures: readonly (readonly [string, AggregateFn])[];
  /** Each `[column, direction]`. */
  readonly sorts: readonly (readonly [string, SortDirection])[];
}

/** `given` read as a GridSetUp -- or why not, in words: each part a list, of names or of pairs. */
function readSetUp(given: unknown): GridSetUp | string {
  if (typeof given !== 'object' || given === null || Array.isArray(given)) {
    return 'a grid set up is its dimensions, rows, columns, measures and sorts';
  }
  const o = given as Record<string, unknown>;
  const names = (key: 'dimensions' | 'rows' | 'columns'): readonly string[] | string => {
    const v = o[key];
    return Array.isArray(v) && v.every((x) => typeof x === 'string') ? v : `its ${key} are a list of column names`;
  };
  const pairs = (key: 'measures' | 'sorts', what: string): readonly (readonly [string, string])[] | string => {
    const v = o[key];
    if (!Array.isArray(v)) return `its ${key} are a list of pairs`;
    const odd = v.find((x) => !Array.isArray(x) || x.length !== 2 || !x.every((y) => typeof y === 'string'));
    return odd === undefined ? v : `a ${what}, not ${JSON.stringify(odd)}`;
  };
  const dimensions = names('dimensions');
  const rows = names('rows');
  const columns = names('columns');
  const measures = pairs('measures', 'measure is a pair: its column and its aggregate');
  const sorts = pairs('sorts', 'sort is a pair: its column and its direction');
  for (const read of [dimensions, rows, columns, measures, sorts]) if (typeof read === 'string') return read;
  // each aggregate and direction is the command's reader's to judge (readGridCommand, as gridApply reads it)
  return { dimensions, rows, columns, measures, sorts } as GridSetUp;
}

/**
 * A NEW GRID'S QUERY AS THE COMMANDS THAT MAKE IT (`given`: JSON, a GridSetUp), in the order a person makes them, as
 * draftCommands orders an editor's: the columns made dimensions first (a column made one can then be grouped), the row
 * groups and the pivot, the measures (a measure of a grouped column is one of its own) -- each by the steps a person
 * makes one by (grid-measures.ts measureCommands, judged against the grid the steps before it made) -- the sorts last
 * (one may name a column a measure made); a part none is given of, left out -- what Python's `page.grid(...)` applies
 * in one call (gridApply, which judges each; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z8). JSON: `{commands}`, or
 * `{problems}`, why it was refused, in words.
 */
export async function gridSetUp(spec: GridSpec | null, given: string): Promise<string> {
  const read = readSetUp(JSON.parse(given));
  if (typeof read === 'string') return JSON.stringify({ problems: [read] });
  const shaped: GridCommand[] = [
    ...read.dimensions.map((name): GridCommand => ({ kind: 'column', name, change: { kind: 'dimension' } })),
    ...(read.rows.length > 0 ? [{ kind: 'group', rows: [...read.rows] } as const] : []),
    ...(read.columns.length > 0 ? [{ kind: 'pivot', columns: [...read.columns] } as const] : []),
  ];
  const sorts: GridCommand[] = read.sorts.length > 0
    ? [{ kind: 'sorts', sorts: read.sorts.map(([column, direction]) => ({ column, direction })) }] : [];
  if (read.measures.length === 0) return JSON.stringify({ commands: [...shaped, ...sorts] });
  // the measures judged against the grid the steps before them make: its spec, which only they need
  if (spec === null) throw new Error("a grid's measures are judged against the grid: give its spec");
  return using(spec, async ({ runner, cube }) => {
    try {
      const d = applyGridCommands({ snapshot: await typedColumns(runner, cube.snapshot), config: cube.configuration, tree: cube.tree },
        shaped);
      const measures = measureCommands(d, read.measures.map(([column, fn]) => readMeasure({ column, fn })));
      return JSON.stringify({ commands: [...shaped, ...measures, ...sorts] });
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
  });
}

/**
 * MEASURES MADE AS A PERSON MAKES THEM (grid-measures.ts measureCommands: Column Properties' Aggregation, or the column
 * editor's calculated column of its own name and then its Aggregation) -- Python's grid.measure: `given`, a JSON list of
 * `{column, fn[, name][, weight]}`. JSON: as gridApply's, or `{problems}`, in DataCube's words.
 */
export async function gridMeasure(spec: GridSpec, given: string): Promise<string> {
  return using(spec, async (o) => {
    let commands: GridCommand[];
    try {
      const asked: unknown = JSON.parse(given);
      if (!Array.isArray(asked)) throw new CubeRefusal('the measures are a list');
      commands = measureCommands({ snapshot: await typedColumns(o.runner, o.cube.snapshot), config: o.cube.configuration, tree: o.cube.tree },
        asked.map(readMeasure));
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return applied(spec, o, commands);
  });
}

/**
 * A GRID EDITED BY COMMANDS (`given`: a JSON list of grid-commands.ts commands, each read by readGridCommand -- an
 * unknown kind or field refused in words), applied in order as the screen applies them (applied above): what Python's
 * grid methods send, each a command the screen builds for the same edit. Python keeps no rule of a grid of its own.
 */
export async function gridApply(spec: GridSpec, given: string): Promise<string> {
  return using(spec, async (o) => {
    const asked: unknown = JSON.parse(given);
    if (!Array.isArray(asked)) return JSON.stringify({ problems: ["a grid's edit is a list of commands"] });
    let commands: GridCommand[];
    try {
      commands = asked.map(readGridCommand);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return JSON.stringify({ problems: [e.message] });
    }
    return applied(spec, o, commands);
  });
}

/** What the grid shows, as DataCube computes it: JSON text (viewJson). */
export async function gridView(spec: GridSpec): Promise<string> {
  return using(spec, async (o) => {
    const { planner, state } = o;
    const controller = controllerOf(o);
    const view = await controller.run({ snapshot: state.snapshot, tree: state.tree });
    if (view === STALE) throw new Error('the grid was asked again before its view was ready');
    return viewJson(view, state.configuration, await planner.compose(view.query, 'STANDARD'));
  });
}

/** The grid's rows at their finest level as Pure, as Create a Class makes its class of them (classLambda). */
export async function gridRelation(spec: GridSpec): Promise<string> {
  // through the page's filters, as it is sent to be planned (over its mapping: `->from(mapping, runtime)`), so its host
  // plans it as it is
  return using(spec, async ({ planner, lens, state }) => {
    const rows = classLambda(state.snapshot);
    return planner.compose(planner.sent(lens ? lens(rows) : rows), 'STANDARD');
  });
}

/**
 * WHAT A SNAP OF THE GRID COPIES (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): its base query, its every column, as
 * the screen's Snap copies it (query.ts snapPull) -- the protocol's JSON, for its host to copy where the rows are (Python:
 * engine.py snap_copy). `spec` is the grid over its live base.
 */
export async function gridSnapPull(spec: GridSpec): Promise<string> {
  return using(spec, async ({ state }) => toJson(snapPull(state.snapshot)));
}

/**
 * WHY A SNAP OF `rows` ROWS IS REFUSED past its host's `limit` (each a whole number, as text): the tab's own sentence
 * (snap.ts snapRefusal), which Python's engine says when a copy it counted is past its machine's limit (Z16).
 */
export function snapRefused(rows: string, limit: string): string {
  return snapRefusal(Number(rows), Number(limit));
}

/** A grid's rows as a class's (gridClassed): what a class of it is made over, where its rows are read from, and each
 *  table's sample. */
export interface GridClassed {
  readonly basis: ClassBasis;
  readonly source: { readonly model: string; readonly runtime: string };
  readonly samples: Readonly<Record<string, SampleRows>>;
}

/**
 * A GRID'S ROWS AS A CLASS'S, HEADLESS (the model actions': headless model.ts modelClass, modelDataSpace): what a class
 * of it is made over (query.ts classBasis: its rows at their finest level, the Database and every table they read -- a
 * join's each, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3), where they are read from, and each table's first rows as
 * the window writes them into the project, read on the host's database (model-plans.ts classSamples). Rows read from no table
 * refused in words (`{refused}`: the window leaves such a grid out of a data space); a sample it cannot carry
 * (`{problems}`: the window refuses the whole action).
 */
export async function gridClassed(spec: GridSpec): Promise<GridClassed | { readonly refused: string } | { readonly problems: string[] }> {
  return using(spec, async ({ planner, runner, state }) => {
    let basis: ClassBasis;
    try {
      basis = classBasis(state.snapshot);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return { refused: e.message };
    }
    try {
      return { basis, source: planner.source(), samples: await classSamples(runner, basis) };
    } catch (e) {
      // refused in words, as the window says it (a NaN or an infinity the sample cannot carry): not a grid the window
      // leaves out, an action it refuses
      if (!(e instanceof SampleRefusal)) throw e;
      return { problems: [e.message] };
    }
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
  return using(spec, async (o) => {
    const { state } = o;
    const controller = controllerOf(o);
    const view = await controller.run({ snapshot: state.snapshot, tree: state.tree });
    if (view === STALE) throw new Error('the grid was asked again before its view was ready');
    const config = state.configuration;
    const heatmaps = measureHeatmaps(view, view.treeRows, (name) => heatmapSetting(config, view, name));
    const conditionals = measureConditionals(view, view.treeRows, (name) => conditionalSetting(config, view, name));
    const file = renderExport(kind, {
      view, model: shownColumns(view, config), config,
      formats: columnFormats(config, state.snapshot, state.snapshot, view), formatters: new FormatterCache(),
      cellBackground: (leaf, row, value) => heatmapColour(heatmaps, view.treeRows, leaf, row, value),
      cellDecoration: (leaf, row, value) => cellDecoration(conditionals, view.treeRows, leaf, row, value), at: new Date(),
    });
    const { content } = file;
    return JSON.stringify({ name: file.name, mime: file.mime, text: typeof content === 'string',
      content: typeof content === 'string' ? content : base64(content) });
  });
}

/**
 * A CHART'S GRID AS PYTHON ASKS FOR IT (chart.grid(); docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D7, the screen's Show
 * Grid): `page`, the page as Python holds it, and its chart `chart`; `spec`, the chart's cube over its base, as gridChart
 * takes it. A chart whose grid holds what it draws answers that grid (`{grid}`: shown, the page as it is; chart-spec.ts
 * showGridWay); any other the grid of its own query (chart-spec.ts chartGrid, the screen's rule) -- `{cube, spec, notes}`, the
 * new grid's document as the screen's new grid writes itself (opened over the source as the compiler types it now,
 * folded, no group open) and the chart following it, for Python's addGrid, followGrid and setChart -- or `{problems}`,
 * in DataCube's words.
 */
export async function chartOwnGrid(spec: GridSpec, page: string, chart: string): Promise<string> {
  const views = (JSON.parse(page) as { views: readonly PageView[] }).views;
  const view = views.find((v) => v.id === chart);
  if (view?.kind !== 'chart') return JSON.stringify({ problems: [`'${chart}' is no chart of the page`] });
  // the grid it reads shown, or one of its own made: the screen's Show Grid's rule (chart-spec.ts showGridWay)
  const grid = views.find((v) => v.kind === 'grid' && v.cube === view.cube);
  if (grid !== undefined && showGridWay(view.spec, true) === 'reveal') return JSON.stringify({ grid: grid.id });
  return using(spec, async ({ runner, state }) => {
    // the chart's cube as the screen's chart reads it: its grid's columns as the compiler types them now
    const snapshot = await typedColumns(runner, state.snapshot);
    const made = chartGrid({ snapshot, config: state.configuration }, view.spec, view.selection ?? []);
    // the tree a new grid starts with, as the screen's Show Grid's grid opens (the S7 audit's G-2)
    const tree = newGridTree(made.configuration);
    const landed = fold({ snapshot: await typedColumns(runner, made.snapshot), configuration: made.configuration, tree });
    const doc = readCube(fromJson(spec.cube));
    const written = writeCube({ name: doc.name, source: doc.source, snapshot: landed.snapshot, configuration: landed.configuration, tree });
    return `{"cube":${JSON.stringify(cubeToJson(written))},"spec":${JSON.stringify(made.spec)},"notes":${JSON.stringify(made.notes)}}`;
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
  return using(spec, async ({ runner, state }) => {
    const was = before === null ? undefined : readChart(JSON.parse(before));
    if (typeof was === 'string') return JSON.stringify({ problems: [`DataCube cannot read the chart as it was: ${was}.`] });
    // the grid's columns as the compiler types them now, its calculated ones too: the controller's step 0 (plan.ts
    // typeColumns), compile-only -- what the grid's run would type them as before its chart is drawn
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
