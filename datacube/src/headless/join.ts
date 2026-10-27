// A JOIN, HEADLESS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.4): two of a page's cubes joined as the tab's Join…
// joins them -- each cube's result over the table it reads in the host's database (a frame, a file read in, a remote
// file's view), one model of those tables written by legend-lite's writer over the host's worker, the join typed by its
// compiler -- so a host's page adds a join's grid by the page command every grid is added by (addGrid with the cube
// document this gives), and asks what a join will do before it makes it, with no screen. Nothing is held between calls.

import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import { fromJson, toJson } from '../../../pure-protocol/src/index.ts';
import { openedState } from '../cube-state.ts';
import { cubeToJson, gridCapped, newGridTree, rebuilt, writeCube, type JoinInput, type JoinSource, type RowsSource } from '../cube-document.ts';
import {
  HOST_TABLES, openInputs, openJoin, openRows, openSources, summaryCounts, type JoinOpening, type OpenedJoin, type OpenedRows, type OpenedSource,
  type OpenedSources,
} from '../join-open.ts';
import { joinSummary, joinTitle, readJoinHow, summaryWords, type JoinHow } from '../join.ts';
import { joinInputOf } from '../page-commands.ts';
import { pageBoxName } from '../flow-boxes.ts';
import { resolvedSource } from '../live-inputs.ts';
import { layoutTiles, readPage, type PageDocument } from '../page-document.ts';
import { CubeRefusal } from '../snapshot.ts';
import { LitePlanner } from '../lite-planner.ts';
import { catalogColumns } from '../upload.ts';
import { pythonRunner } from '../services/python-host.ts';
import { hostCalls, hostDatabase, hostEngine, hostWorker } from './host.ts';
import { leafTables, type HostTables } from './host-tables.ts';

/** What a host hands in: its page, the two grids joined, how they join, and the table of its database each one reads. */
export interface JoinSpec {
  /** The page as the host has it (page-document.ts), read strictly. */
  readonly page: unknown;
  /** The grids joined, by their tiles' ids: each read as the page has it, under the name it shows (joinInputOf). */
  readonly mine: string;
  readonly theirs: string;
  /** How they combine (join-how.ts JoinHow), read strictly. */
  readonly how: unknown;
  /** What the host serves (host-tables.ts HostTables): the table each of their sources' leaves reads is DataCube's to say
   *  (leafTables), mine's then theirs'. */
  readonly host: HostTables;
}

/** A join's grid added (joinCube): on which sheet -- null, its first input's, beside it, as the tab's Join… places it --
 *  and the title a person gave it (none: the join's own). */
export interface JoinAdded {
  readonly sheet: string | null;
  readonly title: string | null;
  /** Its inputs read live, each the grid of the page as it is now (FL6, a flow's Join box: docs/DATACUBE_FLOW_MODE_2026_10_24.md
   *  M2); else -- absent, false -- each a copy of what its grid is now. */
  readonly live?: boolean;
}

/** A join's two inputs as a host hands them in (JoinSpec, its `how` aside): each grid as the page has it, and the tables
 *  they read. */
async function inputsOf(spec: string): Promise<{
  readonly s: JoinSpec & Partial<JoinAdded>; readonly page: PageDocument; readonly mine: string; readonly theirs: string;
  readonly left: JoinInput; readonly right: JoinInput; readonly tables: readonly string[];
}> {
  // the protocol's exact JSON: a cube's numbers, and a condition's in Pure, kept as they are
  const s = fromJson(spec) as unknown as JoinSpec & Partial<JoinAdded>;
  const page = readPage(s.page, { empty: true });
  const left = joinInputOf(page, s.mine);
  const right = joinInputOf(page, s.theirs);
  // each input's leaves' tables, as the host reads them (the one rule: host-tables.ts leafTables), mine's then theirs'
  const tables = [...await leafTables(left.source, s.host), ...await leafTables(right.source, s.host)];
  return { s, page, mine: s.mine, theirs: s.theirs, left, right, tables };
}

async function read(spec: string): Promise<{
  readonly page: PageDocument; readonly mine: string; readonly theirs: string; readonly left: JoinInput; readonly right: JoinInput;
  readonly how: JoinHow; readonly tables: readonly string[]; readonly added?: JoinAdded;
}> {
  const { s, ...inputs } = await inputsOf(spec);
  return {
    ...inputs, how: readJoinHow(s.how),
    ...(s.sheet !== undefined ? { added: { sheet: s.sheet, title: s.title ?? null, live: s.live === true } } : {}),
  };
}

/** Legend-lite's writer and the planners a call makes over the host's worker, let go after; the host's database read
 *  for its tables' catalog alone (every row a join reads is the host's plan-and-run's: D16). */
async function using<T>(use: (o: JoinOpening & { readonly planners: LitePlanner[] }) => Promise<T>,
  tables: readonly string[]): Promise<T> {
  const calls = hostCalls();
  // the model writer needs no model of its own: it writes one (the worker's tableModel)
  const models = new LitePlanner({ model: '', runtime: '', worker: () => hostWorker(calls) });
  const engine = new DuckDbEngine(hostDatabase(calls).connection);
  const planners: LitePlanner[] = [];
  try {
    return await use({
      here: HOST_TABLES, tables, models, planners, databaseType: 'DuckDB',
      catalog: (table) => catalogColumns(engine, models, table),
      typer: (model, runtime) => {
        const p = new LitePlanner({ model, runtime, worker: () => hostWorker(calls) });
        planners.push(p);
        return p;
      },
    });
  } finally {
    models.dispose();
    for (const p of planners) p.dispose();
  }
}

/**
 * A JOIN'S GRID (the Join window's Apply): the join of two grids of the page (JoinSpec, with JoinAdded) as the page
 * command every grid is added by -- addGrid, a new grid's cube document, its rows as they are (capped at the row limit
 * when it is not the page's only grid), titled as the tab titles it (join.ts joinTitle), placed beside its first input
 * as the tab's Join… places it (join-flow.ts). JSON: `{command, notes, model, runtime, relation}`: the command, what
 * drifted in an input, and the join's model, runtime and relation (a lambda) a host plans its rows over.
 */
export async function joinCube(spec: string): Promise<string> {
  const s = await read(spec);
  if (!s.added) throw new Error('a join\'s grid is added on a sheet: {"sheet", "title"}');
  const { title } = s.added;
  // beside its first input, on that input's sheet unless another is named (the S7 audit's Y-3: Python's went on the
  // first sheet, where the input was not)
  const sheet = s.added.sheet ?? s.page.sheets.find((sh) => layoutTiles(sh.layout).includes(s.mine))?.id ?? s.page.sheets[0]!.id;
  const name = s.page.name;
  const gridsOnPage = s.page.cubes.length + 1;
  // a flow's boxes named as its canvas names them (flow-boxes.ts pageBoxName), as the tab's Join window titles the box
  // (the review of 2026-10-26's B3); a board's grids, as their inputs are named
  const onFlow = (id: string): boolean => s.page.sheets.some((sh) => sh.layout.kind === 'flow' && sh.layout.boxes.some((b) => b.tile === id));
  const named = (id: string, input: JoinInput): string => (onFlow(id) ? pageBoxName(s.page, id) : input.name);
  return using(async (o) => {
    const source: JoinSource = {
      _type: 'join', name: joinTitle(s.how, named(s.mine, s.left), named(s.theirs, s.right)), left: s.left, right: s.right, how: s.how, columns: [],
    };
    const opened = await openJoin(source, o);
    const made = rebuilt(undefined, opened.relation, opened.columns, source.name, gridCapped(gridsOnPage));
    // written as the screen holds the grid it opens (cube-state.ts openedState), as headless gridCube writes one
    const state = openedState(made.snapshot, made.configuration, made.tree ?? newGridTree(made.configuration));
    // live: its inputs written as links to the two grids, opened over what they are now
    const links = s.added!.live === true ? { left: { grid: s.mine }, right: { grid: s.theirs } } : {};
    const cube = writeCube({
      name, source: { ...source, ...links, how: opened.how, columns: opened.columns.map((c) => ({ name: c.name, type: c.type })) },
      snapshot: state.snapshot, configuration: state.configuration, tree: state.tree,
    });
    const command = `{"kind":"addGrid","document":${cubeToJson(cube)},"sheet":${JSON.stringify(sheet)},`
      + `"near":${JSON.stringify(s.mine)},"title":${JSON.stringify(title)},"columns":null}`;
    return `{"command":${command},"notes":${JSON.stringify(opened.notes)},`
      + `"model":${JSON.stringify(opened.model)},"runtime":${JSON.stringify(opened.runtime)},`
      + `"relation":${toJson({ _type: 'lambda', parameters: [], body: [opened.relation] })}}`;
  }, s.tables);
}

/** What a host hands in for grids read together (joinTogether): its page, the grids by their tiles' ids, and what it
 *  serves (host-tables.ts HostTables). */
export interface TogetherSpec {
  readonly page: unknown;
  readonly grids: readonly string[];
  readonly host: HostTables;
  /** The copies the host holds, by table (Python's held_copies): a snapped grid is read over its copy. */
  readonly copies?: Readonly<Record<string, unknown>>;
}

/**
 * EACH GRID AS A COMBINATION READS IT ON THE HOST: a snapped grid (the page's record, `copies`) its copy's table -- one
 * the host no longer holds refused in the words a grid's base is (grid.ts baseOnHost), never read live in its place --
 * any other its leaves' tables (host-tables.ts leafTables). The review's P3: the host read every grid live.
 */
export async function sourcesOnHost(page: PageDocument, cubes: readonly string[], host: HostTables,
  held: Readonly<Record<string, unknown>>): Promise<{ readonly sources: OpenedSource[]; readonly tables: string[] }> {
  const sources: OpenedSource[] = [];
  const tables: string[] = [];
  for (const id of cubes) {
    const cube = page.cubes.find((c) => c.id === id);
    if (!cube) throw new CubeRefusal(`'${id}' is no cube of the page`);
    const copy = page.copies?.[id];
    if (copy) {
      if (!Object.hasOwn(held, copy.table)) {
        throw new CubeRefusal(`${cube.cube.name}: its snap (${copy.label}) is no longer held -- go live to read its rows live again, or snap it anew`);
      }
      sources.push({ _type: 'copy', table: copy.table });
      tables.push(copy.table);
      continue;
    }
    // a flow's box reads another's rows: its input resolved against the page first, as the box itself is read (the
    // review of 2026-10-26: a timeseries of several grids, one a box, was refused)
    const source = resolvedSource(page, cube.cube.source);
    sources.push(source);
    tables.push(...await leafTables(source, host));
  }
  return { sources, tables };
}

/**
 * GRIDS READ TOGETHER (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2): a page's grids over one model of the tables
 * they read in the host's database (join-open.ts openSources, the rule the tab opens them by over its own) -- for a
 * timeseries series that combines them, on a tab on the host's page, which has no database of its own there: the series'
 * one query is written over these relations by the tab and planned and run by the host's plan-and-run. JSON: `{model,
 * runtime, relations, notes}`, each relation as the protocol writes it, in the grids' order.
 */
export async function joinTogether(spec: string): Promise<string> {
  const s = fromJson(spec) as unknown as TogetherSpec;
  const page = readPage(s.page, { empty: true });
  if (!Array.isArray(s.grids) || s.grids.length === 0) throw new Error('grids read together are a list of the page\'s grids');
  // each a cube of the page -- a grid's, or one kept for a tile that reads it, its grid removed (the timeseries' kept
  // cube: the second audit's P-4, refused as "no grid of the page" on a host's page, read on the tab) -- or a grid by
  // its tile's id
  const cubes = s.grids.map((id) => {
    if (page.cubes.some((c) => c.id === id)) return id;
    const view = page.views.find((v) => v.id === id);
    if (!view || view.kind !== 'grid') throw new Error(`'${id}' is no grid nor cube of the page`);
    return view.cube;
  });
  // each grid's tables as the host reads them, in the grids' order: a snapped one's copy (sourcesOnHost)
  const { sources, tables } = await sourcesOnHost(page, cubes, s.host, s.copies ?? {});
  const opened = await sourcesOpenedOn(sources, tables);
  return `{"model":${JSON.stringify(opened.model)},"runtime":${JSON.stringify(opened.runtime)},`
    + `"relations":[${opened.relations.map((r) => toJson(r)).join(',')}],"notes":${JSON.stringify(opened.notes)}}`;
}

/** Sources opened over one model of the host's tables they read (`tables`: their leaves', leafTables, in their order) --
 *  join-open.ts openSources, as the tab opens them over its own (joinTogether; headless timeseries.ts timeseriesQuery). */
export async function sourcesOpenedOn(sources: readonly OpenedSource[], tables: readonly string[]): Promise<OpenedSources> {
  return using((o) => openSources(sources, o), tables);
}

/**
 * A SAVED JOIN OPENED OVER THE HOST'S TABLES (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): its leaves' tables
 * (cube-document.ts leavesOf, in its order), one model of them, its inputs and the join typed now (join-open.ts openJoin,
 * as the tab opens a saved join) -- for a host that opens a page's join grid over its own tables (headless grid.ts
 * gridBase, gridOpened).
 */
export async function joinOpenedOn(source: JoinSource, tables: readonly string[]): Promise<OpenedJoin> {
  return using((o) => openJoin(source, o), tables);
}

/** Another grid's rows opened over the host's tables, each read once, as a join is (join-open.ts openRows): a box over a
 *  Join's L or R, which reads both of the join's inputs (cube-document.ts JoinInput.unmatched). */
export async function rowsOpenedOn(source: RowsSource, tables: readonly string[]): Promise<OpenedRows> {
  return using((o) => openRows(source, o), tables);
}

/**
 * A JOIN'S TWO SIDES, BEFORE IT IS MADE (what the Join window shows and suggests keys from: join.ts suggestedKeys): each
 * grid's result as a join reads it, its columns as the compiler types them, and what drifted in an input since -- for a
 * tab on the host's page, which has no database of its own there (its Join…: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
 * B11). JoinSpec, its `how` not asked. JSON: `{left, right, notes}`, each side `{name, relation, columns}` (join-how.ts
 * JoinSide), as the protocol writes it.
 */
export async function joinSides(spec: string): Promise<string> {
  const s = await inputsOf(spec);
  return using(async (o) => {
    const inputs = await openInputs(s.left, s.right, o);
    return toJson({ left: inputs.left, right: inputs.right, notes: inputs.notes });
  }, s.tables);
}

/**
 * WHAT A JOIN WILL DO (the Join window's questions, before it is applied): my rows, those that find a match, the rows
 * after it, their keys that repeat -- each a query over the two cubes' model, planned and run by the host's plan-and-run
 * -- and in the window's words. JSON: `{counts, words}`.
 */
export async function joinQuestions(spec: string): Promise<string> {
  const s = await read(spec);
  return using(async (o) => {
    const inputs = await openInputs(s.left, s.right, o);
    // each question's rows read by the host's plan-and-run, the one way every row is read on the host (D16)
    const calls = hostCalls();
    const rows = pythonRunner(hostEngine(calls), inputs.model, inputs.runtime, {}, () => hostWorker(calls));
    try {
      const counts = await summaryCounts(joinSummary(inputs.left, inputs.right, s.how),
        async (query) => (await rows.host.planAndRun(query, 0)).rows);
      return JSON.stringify({ counts, words: summaryWords(s.how, counts) });
    } finally {
      (rows.host.planner as LitePlanner).dispose();
    }
  }, s.tables);
}
