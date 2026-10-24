// THE PAGE'S SOURCES (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.1, §4.4; step 1b): where a grid's rows come from,
// and opening each kind -- a file from the computer or an example, read into the host's database under a table of its
// own; a warehouse table, Live there as the person and Snapped into the host's database; a remote Parquet, CSV or
// Iceberg file, mounted as a view; a saved query, over its project's model and rows -- for a new grid, or to rebuild a
// saved one over what its source holds now. Every one is opened through the host's services (src/app/services.ts),
// so the app opens sources the same way on every host; the source picker (src/ui/source-picker.ts) is offered the
// sections the services can open.
//
// Each grid has ITS OWN rows over ITS OWN model (the engine service's `rows`, §4.5): a page's grids share no model. The
// tables a page's files are read into are its own and named apart (no grid's table is ever replaced by another's), and
// a file's table goes with its last grid. A Python host's frame is a source too, its model Python's.

import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import type { QueryEngine } from '../../../engine-client/src/engine.ts';
import type { CatalogObject, WarehouseEngine, WarehouseSession } from '../../../engine-client/src/warehouse.ts';
import type { Query, QueryReader } from '../../../query-store/src/index.ts';
import { from, fromJson, lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { ClassAt, ClassElement, ClassGiven } from '../class-source.ts';
import type { ClassOf } from '../class-tree.ts';
import type { HeldCopy } from '../app.ts';
import type { CubeConfiguration } from '../config.ts';
import type { HostSnap, Planner, SnapTarget } from '../cube.ts';
import {
  cubeTaken,
  cubeToJson,
  fileChanged,
  fileSource,
  frameSource,
  lastSegment,
  leavesOf,
  readCube,
  rebuilt,
  remoteSourceOf,
  type ClassSource,
  type CubeDocument,
  type CubeSource,
  type FileSource,
  type JoinSource,
  type QuerySource,
  type RemoteSource,
  type WarehouseSource,
} from '../cube-document.ts';
import { canKeepHandles, handleKey, pickDataFile, readHandle, type FileHandle } from '../file-handles.ts';
import type { InferredModel } from '../infer.ts';
import type { GridMaker } from '../page/page-app.ts';
import type { PageModelRef } from '../page-model.ts';
import type { ModelOptions } from '../planner.ts';
import { inferFormat, type S3Credentials } from '../remote.ts';
import type { PlanAndRun, QueryRunner } from '../runner.ts';
import { SAMPLES, sampleById, sampleFileName, type Sample } from '../samples.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import { sourceColumns } from '../source-columns.ts';
import type { TreeState } from '../tree.ts';
import { pickSource, type DatabaseSession, type PickerSections, type RemoteCredentials, type SectionId } from '../ui/source-picker.ts';
import { formatOf, modelLanded, tableNameOf } from '../upload.ts';
import type { JoinRead } from './join-sources.ts';
import type { ConfiguredProject, GridRows, LocalEngine, QueryService, Services, WarehouseService } from './services.ts';
import { lazily } from '../lazily.ts';

// saved queries' code, fetched the first time it is needed -- the Saved queries section, a query link -- as charts
// are: a grid-only page never downloads it (test/bundle-budget.test.ts)
const savedQueries = () => lazily(() => import('../saved-queries.ts'));
const queryStores = () => lazily(() => import('../../../query-store/src/index.ts'));
// a join's opener, fetched the first time a grid joins: a page with no join never downloads it (test/bundle-budget.test.ts)
const joinSources = () => lazily(() => import('./join-app.ts'));

/** A grid's place, as a maker takes it: where its rows are and how it is written down. */
export type GridPlace = {
  // A warehouse source: Live runs on it, Snap copies into the host's database, and the
  // snap lands under the source's own name so one model reads both.
  readonly live?: WarehouseEngine;
  readonly snapTarget?: SnapTarget;
  /** A warehouse table its host reads (Python's): Snap copies into the host's database, planned and run there (C3). */
  readonly hostSnap?: HostSnap;
  /** Signing in again as the same person when the warehouse's session ends, where the grid has no live engine of its
   *  own to ask (a table its host reads). */
  readonly signInAgain?: (password: string) => Promise<void>;
  /** A grid can be saved: its source, by identity (never its data). */
  readonly cubeSource?: CubeSource;
  /** A grid over a class: its class and the model's class tree, what its related columns may name (class-tree.ts). */
  readonly classTree?: ClassOf;
  /** The groups a saved cube had open. */
  readonly tree?: TreeState;
  /** The rows are a copy in the host's database already: an opened file, generated rows. */
  readonly heldCopy?: HeldCopy;
  /** Where its queries run when not on the host's database as it is: a saved query's version, which brings its own rows. */
  readonly engine?: QueryEngine;
  /** Its own planner, over its own model (a page's grids share no model); the page's planner when none. */
  readonly planner?: Planner;
  /** Planned and run by the host instead (a Python host's frame: R4): no planner or engine in the page. */
  readonly runner?: PlanAndRun;
  /** What its header says it reads. */
  readonly label?: string;
  /**
   * The file it reads, the handle it was picked through, and the table it was read into: the page keeps them by the
   * grid's id, and drops the table once no grid on the page reads it.
   */
  readonly file?: { readonly file: File; readonly handle?: FileHandle; readonly table: string };
  /**
   * The tables of the host's database its rows read (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3): a file's, a
   * remote file's view, a join's inputs' -- what a join of this grid reads, held while a grid on the page reads them.
   */
  readonly tables?: readonly string[];
  /** Its columns panel folded at the start: a grid added beside others; open, one opened alone. */
  readonly foldPanel?: boolean;
};

/**
 * A SOURCE OPENED FOR A GRID: what makes the grid -- its own planner over its own model, its source written down so
 * the page can be saved -- and, a saved cube rebuilt over it, what opening it left out.
 */
export type Opened = {
  readonly make: GridMaker;
  readonly label: string;
  readonly notes: readonly string[];
  /** A saved cube of a host's page opened again over the host's frames: whether it is another now (cube-document.ts
   *  cubeTaken), which the page is told as its `rebuilt` (P11). */
  readonly taken?: boolean;
  /** An overtaken open (a newer one went ahead): what it read into the host's database, let go. */
  readonly discard?: () => Promise<void>;
};

/**
 * A SAVED QUERY (the picker's Saved queries; the user, 2026-10-01: "load from a saved Query"): read from the query store
 * as upstream serves it, by the rules Query writes it by (src/saved-queries.ts). Its project gives the model it compiles
 * against and the rows it reads; the planner reads it `->from(mapping, runtime)`.
 */
export type OpenedQuery = {
  readonly label: string;
  readonly model: string;
  readonly runtime: string;
  readonly how: ModelOptions;
  readonly source: ValueSpecification;
  readonly columns: CubeSnapshot['columns'];
  /** Its rows over its model, in the host's shape (§4.5): the tab's planner, or the host's runner (Python's). */
  readonly rows: GridRows;
  /** The cube's source, as Save and Share write it down: the query itself (cube-document.ts). */
  readonly cubeSource: QuerySource;
  /** A project opened by name: its version's own database in the host's (C5, engine-client's project-data.ts). */
  readonly engine?: QueryEngine;
};

/** What the source picker gave: where the rows come from. */
export type Chosen =
  | { readonly kind: 'file'; readonly file: File; readonly handle?: FileHandle; readonly sample?: { readonly id: string; readonly rows: number } }
  | { readonly kind: 'table'; readonly session: WarehouseSession; readonly object: CatalogObject }
  | { readonly kind: 'remote'; readonly url: string; readonly s3?: S3Credentials }
  | { readonly kind: 'saved'; readonly query: OpenedQuery }
  | { readonly kind: 'class'; readonly given: ClassGiven & { readonly class: string } };

/** A file a grid reads, as the page keeps it by the grid's id. */
export interface GridFile {
  readonly grid: string;
  readonly file: File;
  readonly handle?: FileHandle;
  readonly table: string;
  readonly sha256: string;
  readonly sample: boolean;
}

/** The warehouse the picker is signed in to, and its tables. */
export interface SignedIn {
  readonly session: WarehouseSession;
  readonly objects: readonly CatalogObject[];
}

export interface SourcesOf {
  readonly services: Services;
  /**
   * The page: the cubes it holds now (a file's table goes with its last grid), and the grids it let go that an undo can
   * bring back (theirs are kept while it can).
   */
  readonly page: {
    readonly cubes: readonly string[]; readonly recoverable?: readonly string[];
    /** Its name ('' before it has one) and its model, where it has one: what a class it names is read by (K16, K17). */
    readonly title: string; readonly model: PageModelRef | undefined;
  };
  /** How a grid is made over a source: the app's grid maker. */
  readonly gridMaker: (snapshot: CubeSnapshot, configuration: CubeConfiguration, dimensions: { name: string; columns: string[] }[],
    place: GridPlace) => GridMaker;
  /** The tables the page's start holds already (the tab's sample: `trades`). */
  readonly taken: readonly string[];
  /** Ask the person in the saved pages' window (a file to choose, a click to allow); undefined: nothing to ask now. */
  readonly ask: (content: readonly (string | HTMLElement)[] | undefined) => void;
}

/** The page's sources: what each grid reads, and opening one. */
export interface PageSources {
  /** A grid made on the page, by its tile's id: what the page keeps of it (its planners, its file). */
  madeGrid(id: string, place: GridPlace): void;
  /** Told of every change on the page: a table no grid reads any more is dropped, a planner none plans through let go. */
  dropUnread(): void;
  /** A grid's place, by its id. */
  place(id: string): GridPlace | undefined;
  /** The files on the page now: each grid's, one per table it reads from a file (a join's leaves' among them). */
  filesNow(): GridFile[];
  /** Where a grid whose rows are not in the tab already snaps: copies of its own in the tab's database (§18.3); none
   *  where the page has no database of its own. */
  snapTarget(): SnapTarget | undefined;
  /** While a saved page opens, the tables its model keeps: no fresh name takes one of them (the audit's S3). */
  reserve(tables: Iterable<string>): void;
  release(): void;
  /** The chosen source opened for a grid: fresh, or -- `capped` -- added beside the others. */
  opened(chosen: Chosen, capped: boolean): Promise<Opened>;
  /** What a chosen source is called, as asking to open it in place says it. */
  named(chosen: Chosen): string;
  /**
   * A saved cube's source opened again, the least intrusive way that works; undefined when the person gave up. `page`:
   * the saved page it is opened from, whose kept handles its files are read from (file-handles.ts handleKey); `tables`:
   * the names its page's model keeps for its leaves' tables (a join's each, in its leaves' order); `served`: a grid of
   * the page its other editor (Python) serves, whose every source the host holds where it reads it.
   */
  reopened(doc: CubeDocument, page: string | undefined, several: boolean, tables?: readonly string[],
    served?: boolean): Promise<Opened | undefined>;
  /** A saved query's record -- from a store, or as a share link carries it -- as a cube's source. */
  openedRecord(q: SavedQueryRecord): Promise<OpenedQuery>;
  /** A frame of the host's (Python's dataframe) for a grid: fresh, or a saved cube rebuilt over it. */
  openedFrame(name: string, saved?: CubeDocument, capped?: boolean): Promise<Opened>;
  /** A class of a project in the model home, at a revision, for a grid (a dashboard's tile): fresh, or a saved cube
   *  rebuilt over it. */
  openedClass(at: ClassAt, saved?: CubeDocument, capped?: boolean): Promise<Opened>;
  /**
   * A join of two grids' results for a grid (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): over `tables`, the tables
   * its leaves read (the joined grids' own: GridPlace.tables), or -- none given, a saved join reopened -- each leaf's
   * file read again (a sample rebuilt, a file open on the page taken, else asked for) and each remote file mounted.
   * Refused where the page has no database of its own (§17.3).
   */
  openedJoin(source: JoinSource, how?: { readonly saved?: CubeDocument; readonly capped?: boolean; readonly tables?: readonly string[];
    readonly page?: string; readonly kept?: readonly string[] }): Promise<Opened | undefined>;
  /** The frames a grid reads, by its id -- its leaves' (cube-document.ts leavesOf): what following the host's frames
   *  refreshes. */
  framesOf(id: string): string[];
  /** A frame's version as the page last read it; undefined when no grid has read it. */
  frameVersion(name: string): number | undefined;
  /** A frame read again, its host having moved it: whether its model is the one its grids read (its new version noted,
   *  and their views re-run will do), or changed (its columns: its grids open again over the new model). */
  frameAgain(name: string): Promise<'same' | 'changed'>;
  /** The source picker, offering every section the services can open; `act` does what the person chose. */
  pick<T>(purpose: 'add' | 'open', act: (chosen: Chosen) => Promise<T | null>, start?: SectionId): Promise<T | undefined>;
  /** The single-user app's launch key: signed in at the warehouse that served the page, its tables listed. */
  signInWithKey(key: string): Promise<SignedIn>;
  /** What leaving would lose, said: the warehouse's session, when signed in. */
  sessionHeld(): string | undefined;
  /** What leaving would lose, said: the files opened on the page (not an example's). */
  filesHeld(): string | undefined;
}

type SavedQueryRecord = Pick<Query, 'name' | 'groupId' | 'artifactId' | 'versionId' | 'content' | 'executionContext' | 'defaultParameterValues'>;

/** A size in words: 2.1 MB. */
function bytes(n: number): string {
  if (n < 1024) return `${n} bytes`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/** A saved cube over a file: what reopening one needs is that file back. */
type FileCube = CubeDocument & { readonly source: FileSource };
const isFileCube = (doc: CubeDocument): doc is FileCube => doc.source._type === 'file';


const detectFormat = (url: string): string | undefined => ({ parquet: 'Parquet', csv: 'CSV', iceberg: 'Iceberg' })[inferFormat(url)];

/** What the Remote section's form gave, as DuckDB's S3 settings: only what was filled in. */
const s3Of = (credentials: RemoteCredentials): S3Credentials => ({
  ...(credentials.region ? { region: credentials.region } : {}),
  ...(credentials.keyId ? { accessKeyId: credentials.keyId } : {}),
  ...(credentials.secret ? { secretAccessKey: credentials.secret } : {}),
  ...(credentials.endpoint ? { endpoint: credentials.endpoint } : {}),
});

/** One warehouse, however its address is written. */
const sameWarehouse = (a: string, b: string): boolean => {
  try {
    return new URL(a).origin === new URL(b).origin;
  } catch {
    return a.replace(/\/+$/, '') === b.replace(/\/+$/, '');
  }
};

// A sample is built without a row cap: the one hard limit is the browser's longest string (about 512M characters, some
// millions of rows), and past it the build throws a RangeError -- said in plain words rather than guessed at with a
// ceiling here.
const tooBig = (e: unknown, rows: number): string =>
  e instanceof RangeError
    ? `${rows.toLocaleString(UI_LOCALE)} rows is more than this tab can hold as one file -- try fewer`
    : e instanceof Error ? e.message : String(e);
const mimeOf = (s: Sample): string => (s.format === 'jsonl' ? 'application/x-ndjson' : 'text/csv');

/** An example's file, built: `rows` of it. */
function sampleFile(id: string, rows: number): { file: File; text: string; sample: { id: string; rows: number } } {
  const s = sampleById(id);
  if (!s) throw new Error(`no example '${id}'`);
  let text: string;
  try {
    text = s.build(rows);
  } catch (e) {
    throw new Error(tooBig(e, rows));
  }
  return { file: new File([text], sampleFileName(s), { type: mimeOf(s) }), text, sample: { id: s.id, rows } };
}

/** The warehouse's sign-in, as the picker's Database section shows it. */
function asSession(s: SignedIn): DatabaseSession {
  // the catalog is named only when the warehouse offers more than one
  const several = new Set(s.objects.map((o) => o.catalog)).size > 1;
  return {
    principal: s.session.principal,
    where: new URL(s.session.baseUrl).host,
    objects: s.objects.map((o) => ({
      ...(several ? { catalog: o.catalog } : {}), schema: o.schema, name: o.name, kind: o.kind, columns: o.columns.length,
    })),
  };
}

/** Whether the source picker has a section to offer over these services: New ▸ Data Source… and the blank page's button
 *  are offered only then. Saved queries are read through the page's own pair (`local`); a warehouse through it, or by
 *  a host that reads its tables itself. */
export function picksSources(services: Services): boolean {
  const { database, local } = services.engine;
  return database.ingestFile !== undefined || database.mountRemote !== undefined
    || services.warehouse?.table !== undefined
    || (local !== undefined && (services.queries !== undefined || services.warehouse !== undefined));
}

/** The page's sources, over the host's services. */
export function pageSources(d: SourcesOf): PageSources {
  const { services, page, gridMaker } = d;
  const { database } = services.engine;
  /** The page's own planner and database, where it has them (the tab): what only that shape opens needs it. */
  const local = services.engine.local;
  /** A warehouse is read through the page's own pair (its model writer, its Snap), or by a host that reads its tables
   *  itself (Python's, C3): offered where either is. */
  const warehouse = local || services.warehouse?.table ? services.warehouse : undefined;
  const modelHome = services.modelHome;
  /** Who reads a remote file: the page's own database (the tab's, which the file's server must let this page read), or
   *  its host's (Python's). */
  const remoteReader: 'page' | 'host' = local ? 'page' : 'host';
  /** Saved queries, offered on every host: read by its compiler, run where its rows are (C5). */
  const queries = services.queries;
  /** A grid's place from its rows: a planner and the engine it runs on, or the host's runner. */
  const rowsPlace = (rows: GridRows): Pick<GridPlace, 'planner' | 'engine' | 'runner'> =>
    (rows.runner ? { runner: rows.runner } : { planner: rows.planner, engine: rows.engine });
  /** What types a source's columns: the rows' planner, or the host's runner (both answer lambdaRelationType). */
  const typer = (rows: GridRows): Planner | QueryRunner => rows.runner ?? rows.planner;
  /** The rows' own planner let go (a runner holds no model in the page). */
  const letGo = (rows: GridRows): void => { rows.planner?.dispose?.(); };

  // -- each grid's file and planners, kept by its id ---------------------------------------------------------------

  /** The tables this page's grids read, each its own: none may replace another's. */
  const taken = new Set<string>(d.taken);
  /** The names a saved page's model keeps for its grids, while that page opens. */
  const reserved = new Set<string>();
  const freshTable = (base: string): string => {
    const busy = (t: string): boolean => taken.has(t) || reserved.has(t);
    let name = base;
    for (let n = 2; busy(name); n += 1) name = `${base}_${n}`;
    taken.add(name);
    return name;
  };
  /**
   * The file each table of the host's database was read from, by the table, whoever read it -- a file's grid, or a join
   * that read its leaf's file itself (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3: what a grid reads is its leaves,
   * never one file of its own). A grid's files are those of the tables it reads (GridPlace.tables).
   */
  const tableFiles = new Map<string, Omit<GridFile, 'grid' | 'table'>>();
  /** The files of the grids on the page now (`page.cubes`): each grid's, one per table it reads from a file. */
  const filesNow = (): GridFile[] => page.cubes.flatMap((grid) => (gridTables.get(grid) ?? []).flatMap((table) => {
    const f = tableFiles.get(table);
    return f ? [{ grid, table, ...f }] : [];
  }));
  /** The tables files were read into for grids, by table: the file's name, to drop it by. */
  const held = new Map<string, string>();
  /** Each grid's own planners (by its tile's id), which hold its model: let go with the last grid using them. */
  const gridPlanners = new Map<string, readonly Planner[]>();
  /** Each grid's place, by its id: what the page's model actions read of it (its planner, engine, label). */
  const gridPlaces = new Map<string, GridPlace>();
  /** The tables each grid reads (GridPlace.tables), by its id: a file's table stays while a join reads it. */
  const gridTables = new Map<string, readonly string[]>();
  /** The files joins not made yet read in, by table (app/join-sources.ts JoinRead): held once a grid of one is made. */
  const joinRead = new Map<string, JoinRead>();
  /**
   * The files read in for grids not made yet, by their hash: a saved page makes its grids once all its sources are open,
   * and a join of them opens before -- it reads the table a grid beside it read, never the file again.
   */
  const readForGrids = new Map<string, { readonly table: string; readonly file: File; readonly handle?: FileHandle }>();
  /** When each table a file was read into was read, by the table: what a grid of copies held here says of them. */
  const readAt = new Map<string, Date>();

  const madeGrid = (id: string, place: GridPlace): void => {
    // its own planner: let go with the last grid planning through it (a snap's copy lets its own go with it)
    gridPlanners.set(id, place.planner ? [place.planner] : []);
    gridPlaces.set(id, place);
    if (place.tables) gridTables.set(id, place.tables);
    for (const t of place.tables ?? []) {
      const read = joinRead.get(t);
      if (read === undefined) continue;
      joinRead.delete(t);
      held.set(t, read.file.name);
      tableFiles.set(t, { file: read.file, ...(read.handle ? { handle: read.handle } : {}), sha256: read.sha256, sample: read.sample });
    }
    const src = place.cubeSource;
    if (src?._type === 'file') readForGrids.delete(src.sha256);
    if (place.file && src?._type === 'file') {
      const { table, ...file } = place.file;
      tableFiles.set(table, { ...file, sha256: src.sha256, sample: src.sample !== undefined });
      held.set(table, place.file.file.name);
    }
  };

  /**
   * A FILE'S TABLE GOES WITH ITS LAST GRID: once no grid on the page (or kept off it for a detached chart) reads it, it
   * is dropped from the host's database and its name is free again -- a file opened three times in place holds one
   * table, not three. Told of every change on the page.
   */
  const dropUnread = (): void => {
    // the grids on the page, and those an undo can bring back: what they read stays
    const live = new Set([...page.cubes, ...(page.recoverable ?? [])]);
    const read = new Set([...live].flatMap((grid) => gridTables.get(grid) ?? []));
    for (const grid of [...gridTables.keys()]) if (!live.has(grid)) gridTables.delete(grid);
    for (const [table, fileName] of [...held]) {
      if (read.has(table)) continue;
      held.delete(table);
      tableFiles.delete(table);
      readAt.delete(table);
      void database.forgetFile?.(fileName, table).catch(() => {}).finally(() => taken.delete(table));
    }
    // a planner no grid plans through any more lets its model go (a grid and its copies share one)
    for (const [grid, owned] of [...gridPlanners]) {
      if (live.has(grid)) continue;
      gridPlanners.delete(grid);
      gridPlaces.delete(grid);
      const still = new Set([...gridPlanners.values()].flat());
      for (const p of owned) if (!still.has(p)) p.dispose?.();
    }
  };

  // -- a warehouse table ---------------------------------------------------------------------------------------------

  /** The cubes live on a warehouse: renewed when the same user signs in again (P2-297). */
  const liveEngines = new Set<WarehouseEngine>();
  const track = (live: WarehouseEngine): WarehouseEngine => {
    liveEngines.add(live);
    return live;
  };
  /** The picker's warehouse sign-in, kept between openings of the window. */
  let signedIn: SignedIn | undefined;

  /**
   * A GRID'S SNAP WHERE ITS HOST RUNS ITS ROWS (Python's page: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3) -- a
   * frame's grid, a warehouse table's the host opened: each snap a copy of the base query in a table of the host's own,
   * modelled by the host as it holds the rows, and read through a runner over that model. None where the host keeps no
   * copy.
   */
  const hostSnapOf = (m: { readonly model: string; readonly runtime: string }): HostSnap | undefined => {
    const { snap, release, held } = database;
    if (!snap || !release) return undefined;
    const store = database.place ?? 'the host';
    return {
      store,
      snap: async (pull) => {
        const made = await snap(m.model, m.runtime, pull);
        const runner = services.engine.rows(made.model, made.runtime, { bitColumns: made.bitColumns }).runner;
        if (!runner) {
          await release(made.table);
          throw new Error(`${store} made a copy whose rows it does not run`);
        }
        return { ...made, runner };
      },
      release: (table) => release(table),
      // a copy the page's other editor made (Python's grid.snap(), §18.8), read through the host's runner as one made here
      ...(held ? {
        held: async (table: string) => {
          const made = await held(table);
          const runner = services.engine.rows(made.model, made.runtime, { bitColumns: made.bitColumns }).runner;
          if (!runner) throw new Error(`${store} holds a copy whose rows it does not run`);
          return { ...made, runner };
        },
      } : {}),
    };
  };

  const hostSnapPlace = (m: { readonly model: string; readonly runtime: string }): Pick<GridPlace, 'hostSnap'> => {
    const hostSnap = hostSnapOf(m);
    return hostSnap ? { hostSnap } : {};
  };

  /** A warehouse table's model: its own runtime, of the catalog's database type. */
  const warehouseModel = (l: LocalEngine, o: CatalogObject): Promise<InferredModel> => l.tables.tableModel(
    o.columns.map((c) => ({ ...c, dataType: c.type })),
    { table: o.name, schema: o.schema, convertible: false, databaseType: o.databaseType });
  /**
   * WHERE A GRID SNAPS IN THE TAB (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3), for a grid whose rows are not in the
   * tab already (a warehouse table's, read live there; a remote file's, read afresh by every query): each snap a copy of
   * the base query's rows in a table of the tab's own, modelled once the rows land as a file read in is (upload.ts
   * modelLanded), and planned by a planner over that model; its planner let go and its name free again with it.
   */
  const snapInto = (l: LocalEngine): SnapTarget => ({
    host: l.tables,
    copy: () => {
      const table = freshTable('snap');
      let planner: Planner | undefined;
      return {
        table,
        landed: async () => {
          const m = await modelLanded(l.engine, l.tables, table);
          planner = l.another(m.model, m.runtime, { bitColumns: m.bitColumns });
          return { source: m.source, planner };
        },
        dispose: () => {
          planner?.dispose?.();
          taken.delete(table);
        },
      };
    },
  });

  /** A warehouse table for a grid: Live there as the user, Snap into the host's database; its own planner over its model. */
  async function openedTable(on: WarehouseService, session: WarehouseSession, chosen: CatalogObject, saved?: CubeDocument,
    capped = false): Promise<Opened> {
    if (on.table) return openedHostTable(on, session, chosen, saved, capped);
    // read through the page's own pair: a warehouse the host does not read is offered only where the page has one
    if (!local || !on.live) throw new Error('this page reads no warehouse');
    // A warehouse table is read-only: a column the compiler says must be converted to be declared cannot be, so it is
    // left out, and named.
    const m = await warehouseModel(local, chosen);
    const own = local.another(m.model, m.runtime, { bitColumns: m.bitColumns });
    const columns = await sourceColumns(own, m.source);
    const live = track(on.live(session, chosen.catalog, chosen.databaseType));
    const name = `${chosen.schema}.${chosen.name}`;
    // where it is, never the sign-in: whoever reopens it signs in as themselves
    const cubeSource: WarehouseSource = {
      _type: 'warehouseTable', name, warehouse: session.baseUrl, catalog: chosen.catalog, schema: chosen.schema, table: chosen.name,
      columns: columns.map((c) => ({ name: c.name, type: c.type })),
    };
    const r = rebuilt(saved, m.source, columns, name, capped);
    return {
      label: name,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], {
        planner: own, live, snapTarget: snapInto(local), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
      }),
      // never made into a grid: its planner's model let go
      discard: async () => own.dispose?.(),
    };
  }

  /**
   * A warehouse table its HOST reads (Python's, C3): the host writes its model with the tab's own writer and holds it,
   * and the grid's rows are the host's -- planned and run there, Live on the warehouse as the person signed in there
   * (R4: the page sends Pure).
   */
  async function openedHostTable(on: WarehouseService, session: WarehouseSession, chosen: CatalogObject,
    saved?: CubeDocument, capped = false): Promise<Opened> {
    const m = await on.table!(session, chosen);
    const rows = services.engine.rows(m.model, m.runtime, { bitColumns: m.bitColumns });
    const hostSnap = hostSnapOf(m);
    const columns = await sourceColumns(typer(rows), m.source);
    const name = `${chosen.schema}.${chosen.name}`;
    // where it is, never the sign-in: whoever reopens it signs in as themselves
    const cubeSource: WarehouseSource = {
      _type: 'warehouseTable', name, warehouse: session.baseUrl, catalog: chosen.catalog, schema: chosen.schema, table: chosen.name,
      columns: columns.map((c) => ({ name: c.name, type: c.type })),
    };
    const r = rebuilt(saved, m.source, columns, name, capped);
    return {
      label: name,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], {
        ...rowsPlace(rows), ...(hostSnap ? { hostSnap } : {}), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}),
        foldPanel: capped,
        // an expired session signed in again as the same person, by the host, as the tab's live engine does (P2-297)
        signInAgain: async (password) => { await on.connect(session.baseUrl, session.principal, password); },
      }),
      discard: async () => letGo(rows),
    };
  }

  /**
   * The Database section: the sign-in kept between windows, and what opening a listed table does. `want` (reopening a
   * cube saved over a table): its warehouse's address offered, the session of another warehouse not, and the table
   * opened as soon as it is listed.
   */
  function databaseSection<T>(
    on: WarehouseService,
    open: (session: WarehouseSession, object: CatalogObject) => Promise<T>,
    want?: { readonly warehouse: string; readonly schema: string; readonly name: string },
  ): NonNullable<PickerSections<T>['database']> {
    const url = want?.warehouse ?? on.offered();
    const keep = signedIn && (!want || sameWarehouse(signedIn.session.baseUrl, want.warehouse)) ? signedIn : undefined;
    return {
      ...(url ? { url } : {}),
      ...(keep ? { session: asSession(keep) } : {}),
      ...(on.table && database.place !== undefined ? { copiesInto: database.place } : {}),
      ...(want ? { want: { schema: want.schema, name: want.name } } : {}),
      signIn: async (url, user, password) => {
        // nothing of a previous sign-in stays on offer until this one has listed its tables (P2-334)
        signedIn = undefined;
        const connected = await on.connect(url, user, password);
        signedIn = { session: connected.session, objects: connected.objects };
        on.remember(connected.session.baseUrl);
        // THE CUBES LIVE THERE go on with the new token when it is the same user (P2-297)
        for (const live of liveEngines) {
          try {
            live.renew(connected.session);
          } catch {
            // another user: that cube keeps its own sign-in
          }
        }
        return asSession(signedIn);
      },
      open: (object) => {
        const s = signedIn;
        const found = s?.objects.find((o) => (object.catalog === undefined || o.catalog === object.catalog)
          && o.schema === object.schema && o.name === object.name);
        if (!s || !found) return Promise.reject(new Error(`${object.schema}.${object.name} is no longer offered: sign in again`));
        return open(s.session, found);
      },
    };
  }

  /**
   * A saved (or shared) cube over a WAREHOUSE TABLE, reopened: straight away when this page is signed in to that
   * warehouse; otherwise the Database section asks for a sign-in there and opens the table once it is listed. No
   * credential was ever written down.
   */
  async function reopenTable(src: WarehouseSource, saved: CubeDocument, several = false): Promise<Opened | undefined> {
    const host = (() => { try { return new URL(src.warehouse).host; } catch { return src.warehouse; } })();
    if (!warehouse) throw new Error(`“${saved.name}” reads ${src.name} on ${host}: this page reaches no warehouse`);
    const on = signedIn && sameWarehouse(signedIn.session.baseUrl, src.warehouse) ? signedIn : undefined;
    const found = on?.objects.find((o) => o.catalog === src.catalog && o.schema === src.schema && o.name === src.table);
    if (on && found) return openedTable(warehouse, on.session, found, saved, several);
    let got: Opened | undefined;
    const done = await pickSource<boolean>(document, {
      purpose: 'open',
      start: 'database',
      reason: `“${saved.name}” reads ${src.name} on ${host}: sign in there to open it.`,
      sections: {
        database: databaseSection(warehouse, async (session, object) => {
          got = await openedTable(warehouse, session, object, saved, several);
          return true;
        }, { warehouse: src.warehouse, schema: src.schema, name: src.table }),
      },
    });
    return done ? got : undefined;
  }

  // -- a file, an example --------------------------------------------------------------------------------------------

  /**
   * A FILE FOR A GRID, read into the host's database under a table of its own (no grid's table is ever replaced by
   * another's): a fresh grid, or -- `saved` -- a saved cube reconciled with what the file holds NOW.
   */
  async function openedFile(
    file: File,
    how: {
      readonly handle?: FileHandle;
      readonly sample?: { readonly id: string; readonly rows: number };
      readonly saved?: CubeDocument;
      readonly capped?: boolean;
      /** The table its page's model keeps for it (its project's store reads it by that name). */
      readonly table?: string;
    } = {},
  ): Promise<Opened> {
    /** The name taken for it while it is read, and -- read -- its table: let go again when no grid comes of it. */
    let reserved: string | undefined;
    let read: string | undefined;
    try {
      const { ingestFile, forgetFile } = database;
      if (!ingestFile || !forgetFile) throw new Error('this page takes no file');
      // the name the page's model keeps for it, when no grid on screen holds that name; else a fresh one, and said
      const kept = how.table !== undefined && !taken.has(how.table) ? how.table : undefined;
      if (kept !== undefined) taken.add(kept);
      const asked = kept ?? freshTable(tableNameOf(file.name));
      reserved = asked;
      const opened = await ingestFile(file, { table: asked });
      reserved = undefined;
      // the table it was read into: the one asked for, or -- the host's database holding that name already (another
      // page on the same Python) -- the free one the host chose instead
      const table = opened.table;
      read = table;
      if (table !== asked) {
        taken.delete(asked);
        taken.add(table);
      }
      const loadedAt = new Date();
      readAt.set(table, loadedAt);
      const rows = services.engine.rows(opened.model, opened.runtime, { bitColumns: opened.bitColumns });
      const columns = await sourceColumns(typer(rows), opened.source);
      const source = await fileSource(file, formatOf(file.name), columns, how.sample);
      readForGrids.set(source.sha256, { table, file, ...(how.handle ? { handle: how.handle } : {}) });
      const saved = how.saved;
      const r = rebuilt(saved, opened.source, columns, opened.fileName, how.capped === true);
      const notes = saved
        ? [
          ...fileChanged(saved.source, source),
          ...(how.table !== undefined && table !== how.table
            ? [`${file.name} is read as ${table} in this tab, not ${how.table} as the page's classes read it: `
              + (kept === undefined
                ? 'a grid on screen holds that name (open the page in a tab of its own to read its classes)'
                : 'the database it is read into holds that name already (another page on it)')]
            : []),
          ...r.notes,
        ]
        : [];
      return {
        label: opened.fileName,
        notes,
        // never made into a grid: its planner's model let go, its table gone, and its name free again
        discard: () => {
          letGo(rows);
          if (readForGrids.get(source.sha256)?.table === table) readForGrids.delete(source.sha256);
          readAt.delete(table);
          return forgetFile(file.name, table).catch(() => {}).finally(() => taken.delete(table));
        },
        make: gridMaker(r.snapshot, r.configuration, [], {
          ...rowsPlace(rows),
          cubeSource: source,
          heldCopy: { label: opened.fileName, takenAt: loadedAt, rowCount: opened.rowCount },
          label: opened.fileName,
          ...(r.tree ? { tree: r.tree } : {}),
          file: { file, table, ...(how.handle ? { handle: how.handle } : {}) },
          tables: [table],
          foldPanel: how.capped === true,
        }),
      };
    } catch (e) {
      if (reserved !== undefined) taken.delete(reserved);
      // read, then refused further on (its columns not typed, say): its table goes, and its name is free again
      if (read !== undefined) {
        const gone = read;
        readAt.delete(gone);
        await database.forgetFile?.(file.name, gone).catch(() => {});
        taken.delete(gone);
      }
      // Say what failed and about which file -- the one input the user can actually fix. Shown where the open was
      // asked: the source picker's window, or the saved pages'.
      throw new Error(`could not open ${file.name}: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
    }
  }

  /**
   * The file a saved cube needs, got the least intrusive way that works: a sample is rebuilt; the file already open is
   * reused when it IS that file; a kept handle is read (asking for the browser's permission takes a click, so the window
   * offers one); otherwise the user is asked for it.
   */
  async function fileFor(src: FileSource, cube: string, pageId: string | undefined): Promise<{ file: File; handle?: FileHandle } | undefined> {
    if (src.sample) {
      const s = sampleById(src.sample.id);
      if (s) {
        return { file: new File([s.build(src.sample.rows)], src.name, { type: mimeOf(s) }) };
      }
    }
    const open = [...tableFiles.values()].find((f) => f.sha256 === src.sha256);
    if (open) return { file: open.file, ...(open.handle ? { handle: open.handle } : {}) };
    const kept = pageId === undefined ? undefined : await (await services.pages)?.handles?.get(handleKey(pageId, src.sha256));
    if (kept) {
      const read = await readHandle(kept, false);
      if (read.state === 'file') return { file: read.file, handle: kept };
      if (read.state === 'needs-click') {
        return new Promise((resolve) => {
          const button = document.createElement('button');
          button.type = 'button';
          button.textContent = `Open ${kept.name}`;
          button.addEventListener('click', () => {
            void readHandle(kept, true).then(async (again) => {
              d.ask(undefined);
              resolve(again.state === 'file' ? { file: again.file, handle: kept } : await chooseFile(src, cube));
            });
          });
          d.ask([`"${cube}" reads ${kept.name} from where you picked it. The browser wants you to allow it:`, button]);
        });
      }
    }
    return chooseFile(src, cube);
  }

  /** Ask the user for the file, naming the one the cube was saved over. */
  function chooseFile(src: FileSource, cube: string): Promise<{ file: File; handle?: FileHandle } | undefined> {
    return new Promise((resolve) => {
      const choose = document.createElement('button');
      choose.type = 'button';
      choose.textContent = 'Choose file…';
      const input = document.createElement('input');
      input.type = 'file';
      input.accept = '.csv,.parquet,.json,.jsonl,.ndjson,.gz';
      input.hidden = true;
      input.className = 'dc-lib-choose';
      input.addEventListener('change', () => {
        const file = input.files?.[0];
        d.ask(undefined);
        resolve(file ? { file } : undefined);
      });
      choose.addEventListener('click', () => {
        if (!canKeepHandles()) {
          input.click();
          return;
        }
        void pickDataFile().then((picked) => {
          d.ask(undefined);
          resolve(picked);
        });
      });
      const cancel = document.createElement('button');
      cancel.type = 'button';
      cancel.textContent = 'Cancel';
      cancel.addEventListener('click', () => {
        d.ask(undefined);
        resolve(undefined);
      });
      d.ask([
        `"${cube}" was built over ${src.name} (${bytes(src.size)}). Choose that file:`,
        choose, cancel, input,
      ]);
    });
  }

  // -- a remote file -------------------------------------------------------------------------------------------------

  /** A remote file for a grid, mounted as a view of its own; its keys are never written down. */
  async function openedRemote(url: string, s3: S3Credentials | undefined, saved?: CubeDocument, capped = false): Promise<Opened> {
    if (!database.mountRemote) throw new Error('this page reads no remote file');
    const view = freshTable('remote');
    const m = await database.mountRemote(url, s3, view);
    const rows = services.engine.rows(m.model, m.runtime, { bitColumns: m.bitColumns });
    const columns = await sourceColumns(typer(rows), m.source);
    const cubeSource = remoteSourceOf(url, columns);
    const { name } = cubeSource;
    const r = rebuilt(saved, m.source, columns, name, capped);
    return {
      label: name,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], {
        ...rowsPlace(rows), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), tables: [view], foldPanel: capped,
        // read afresh by every query, so live: snapped into a copy of its own where the tab runs its rows
        ...(local && !rows.runner ? { snapTarget: snapInto(local) } : {}),
      }),
      discard: async () => letGo(rows),
    };
  }

  // -- a join of two grids' results (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): app/join-sources.ts, fetched with it

  async function openedJoin(source: JoinSource, how: { readonly saved?: CubeDocument; readonly capped?: boolean;
    readonly tables?: readonly string[]; readonly page?: string; readonly kept?: readonly string[] } = {}): Promise<Opened | undefined> {
    if (!local) throw new Error(`${source.name} is a join, read in this tab's own database: this page has none`);
    const { openedJoin: opened } = await joinSources();
    return opened({
      local, database, services, fileFor, freshTable, joinRead, readAt, typer, letGo, gridMaker, rowsPlace,
      snapTarget: () => snapInto(local),
      readFile: (sha) => {
        const open = [...tableFiles].find(([, f]) => f.sha256 === sha) ?? [...joinRead].find(([, r]) => r.sha256 === sha);
        return open ? { table: open[0], file: open[1].file, ...(open[1].handle ? { handle: open[1].handle } : {}) } : readForGrids.get(sha);
      },
      take: (t) => { taken.add(t); },
      untake: (t) => { taken.delete(t); },
      isTaken: (t) => taken.has(t),
    }, source, how);
  }

  // -- a frame (Python's dataframe) --------------------------------------------------------------------------------

  /** Each frame as its grids last read it -- its version, its model and its source -- a later version a change to follow. */
  const framesRead = new Map<string, { readonly version: number; readonly model: string; readonly source: string }>();

  /**
   * A FRAME FOR A GRID: the model its host wrote when it was registered, the grid's rows in the host's shape (on a
   * Python host, planned and run by Python: R4) -- a fresh grid, or a saved cube rebuilt over what the frame holds now.
   */
  async function openedFrame(name: string, saved?: CubeDocument, capped = false): Promise<Opened> {
    if (!database.frame) throw new Error(`${saved ? `"${saved.name}" reads` : 'this is'} the dataframe ${name}: open it from Python, where it is (ll.show)`);
    const m = await database.frame(name);
    const rows = services.engine.rows(m.model, m.runtime);
    const columns = await sourceColumns(typer(rows), m.source);
    framesRead.set(name, { version: m.version, model: m.model, source: JSON.stringify(m.source) });
    const cubeSource = frameSource(name, columns);
    const r = rebuilt(saved, m.source, columns, name, capped);
    return {
      label: name,
      notes: r.notes,
      ...(saved ? { taken: cubeTaken(r.changed, saved.source, cubeSource) } : {}),
      make: gridMaker(r.snapshot, r.configuration, [], {
        ...rowsPlace(rows), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
        // a frame is read afresh by every query: its grid snaps into a copy of its own, the host's (§18.3)
        ...(rows.runner ? hostSnapPlace(m) : {}),
      }),
      discard: async () => letGo(rows),
    };
  }

  /**
   * A SAVED CUBE ON A HOST'S PAGE (Python's: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3), every kind but a frame's --
   * a file's, a remote file's, a class's, a saved query's, a warehouse table's, a join's -- opened by the host where it
   * reads it, as its source is now (DatabaseService.opened: DataCube's own code, headless gridOpened, Python's own rule
   * for it; a file's over the table the host holds by its bytes, never asked for again) -- this page has no database or
   * compiler of its own on the host -- its rows read through the host's runner over its model and typed by it, as a
   * frame's grid's are, what opening it changed said as Python says it (`taken`: the page's `rebuilt`), and each frame it
   * reads followed as that frame's own grid follows it (the S7 audit's H-2, H-3).
   */
  async function openedOnHost(src: CubeSource, doc: CubeDocument, capped: boolean): Promise<Opened> {
    const opened = await database.opened!(cubeToJson(doc), doc.name, capped ? 2 : 1);
    const was = readCube(fromJson(opened.cube));
    if (was.source._type !== src._type) throw new Error(`the host opened ${src.name} as another kind of source (${was.source._type})`);
    const rows = services.engine.rows(opened.model, opened.runtime, opened.how);
    try {
      const columns = await sourceColumns(typer(rows), opened.relation);
      for (const [name, version] of Object.entries(opened.frames)) {
        const m = await database.frame!(name);
        framesRead.set(name, { version, model: m.model, source: JSON.stringify(m.source) });
      }
      const r = rebuilt(was, opened.relation, columns, src.name, capped);
      return {
        label: src.name,
        notes: [...new Set([...opened.notes, ...r.notes])],
        taken: opened.taken,
        make: gridMaker(r.snapshot, r.configuration, [], {
          ...rowsPlace(rows), cubeSource: was.source, label: src.name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
          // its rows read afresh where the host reads them by every query: it snaps into a copy of its own, the host's --
          // but a class's or a saved query's, planned through a mapping or its project's own, which the host's copy is not
          ...(rows.runner && opened.how.mapping === undefined && !opened.how.project ? hostSnapPlace(opened) : {}),
        }),
        discard: async () => letGo(rows),
      };
    } catch (e) {
      letGo(rows);
      throw e;
    }
  }

  /**
   * A saved (or shared) cube over a REMOTE FILE, reopened: read straight away when the file is public; refused (a
   * private bucket), the Remote section asks for its keys, the URL filled in.
   */
  async function reopenRemote(src: RemoteSource, saved: CubeDocument, several = false): Promise<Opened | undefined> {
    let refusal: string;
    try {
      return await openedRemote(src.url, undefined, saved, several);
    } catch (e) {
      refusal = e instanceof Error ? e.message : String(e);
    }
    let got: Opened | undefined;
    const done = await pickSource<boolean>(document, {
      purpose: 'open',
      start: 'remote',
      reason: `“${saved.name}” reads ${src.name}, which did not open without its keys (${refusal.slice(0, 160)}).`,
      sections: {
        remote: {
          detect: detectFormat,
          url: src.url,
          keys: true,
          reader: remoteReader,
          open: async (url, credentials) => {
            got = await openedRemote(url, credentials ? s3Of(credentials) : undefined, saved, several);
            return true;
          },
        },
      },
    });
    return done ? got : undefined;
  }

  // -- a saved query -------------------------------------------------------------------------------------------------

  /** Each project's model text, fetched once. */
  const projectModels = new Map<string, Promise<string>>();
  const once = <T,>(cache: Map<string, Promise<T>>, key: string, make: () => Promise<T>): Promise<T> => {
    let p = cache.get(key);
    if (!p) {
      p = make();
      cache.set(key, p);
      p.catch(() => cache.delete(key));
    }
    return p;
  };
  const projectFor = (q: Pick<Query, 'groupId' | 'artifactId' | 'versionId'>): ConfiguredProject | undefined =>
    queries?.projects.find((p) => p.groupId === q.groupId && p.artifactId === q.artifactId && p.versionId === q.versionId);

  async function openedQuery(store: QueryReader, id: string): Promise<OpenedQuery> {
    return openedRecord(await store.get(id));
  }

  /** A saved query's record -- from a store, or as a share link carries it -- as a cube's source. */
  async function openedRecord(q: SavedQueryRecord): Promise<OpenedQuery> {
    if (!queries) throw new Error(`“${q.name}” is a saved query: this page reads none`);
    // read by the host's compiler (the tab's planner, Python's native library: C5), planned over its project's
    // mapping, its rows the project's own Data rows where the host's runs its project databases (C5)
    const { reading } = services.engine;
    const { projectOf, savedQueryOpened } = await savedQueries();
    const project = projectFor(q);
    if (!project && !modelHome) throw new Error(`“${q.name}” belongs to ${projectOf(q)}, which this page has no model for (config.json projects[], or a depot)`);
    const key = projectOf(q);
    // the page's own project (config.json projects[]), else the version opened by name from Depot (design Phase 3)
    const model = await once(projectModels, key, async () => (project
      ? (await Promise.all(project.models.map((url) => queries.fetchText(url)))).join('\n')
      : (await lazily(() => import('../../../depot-client/src/model-text.ts'))).modelText((await modelHome!.connect()).depot, q.groupId, q.artifactId, q.versionId)));
    // in the tab, a project brings its own rows: its Data elements' tables, in a database of the version's own inside
    // the tab's DuckDB (C5, engine-client's project-data.ts) -- another version may be on the page beside it, in its
    // own. On Python, Python runs a project's model in its own (the host's runner).
    const versioned: QueryEngine | undefined = local
      ? (await (await local.projects()).engineFor(model, local.engine)).engine : undefined;
    // rules 1-4, the one rule (saved-queries.ts savedQueryOpened), typed over its rows where the host reads them
    let rows: GridRows | undefined;
    const { runtime, how, source } = await savedQueryOpened(q, model, reading, (at, options) => {
      rows = services.engine.rows(model, at, options);
      return typer(rows);
    });
    if (rows === undefined) throw new Error('unreachable: a saved query is typed over its rows');
    const columns = await sourceColumns(typer(rows), source);
    const { sharedPart } = await queryStores();
    const cubeSource: QuerySource = {
      _type: 'savedQuery', name: q.name, query: sharedPart(q), columns: columns.map((c) => ({ name: c.name, type: c.type })),
    };
    return { label: q.name, model, runtime, how, source, columns, rows, cubeSource, ...(versioned ? { engine: versioned } : {}) };
  }

  /** A saved query for a grid: its own planner over its project's model, its version's rows; the query written down. */
  function openedSavedQuery(o: OpenedQuery, saved?: CubeDocument, capped = false): Opened {
    const r = rebuilt(saved, o.source, o.columns, o.label, capped);
    return {
      label: o.label,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], {
        ...rowsPlace(o.rows), cubeSource: o.cubeSource, label: o.label, ...(o.engine ? { engine: o.engine } : {}),
        ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
      }),
      discard: async () => letGo(o.rows),
    };
  }

  // -- a class of a project in the model home (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.1) ----------------------

  /**
   * A CLASS for a grid: the project's files at the revision as one model text, planned by the host's compiler through
   * the class's mapping and runtime (a saved query's way, C5); its properties the grid's columns, each under its own
   * name. Its rows are where the host's database has them -- the tables the page that made it read in -- so one not
   * open here is refused when it opens, in words, never its sample read in its place.
   */
  async function openedClass(at: ClassAt, saved?: CubeDocument, capped = false): Promise<Opened> {
    const name = at.class.slice(at.class.lastIndexOf(':') + 1);
    if (!modelHome) throw new Error(`“${name}” is a class of ${at.project}: this page reaches no model home`);
    // fetched the first time a class is opened: a page with no class grid never downloads them
    const [{ classOpened }, home] = await Promise.all([lazily(() => import('../class-source.ts')), modelHome.connect()]);
    const { model, left, source, tree, measures, supertypes } = await classOpened(at,
      await home.client.pure({ project: at.project, workspace: at.workspace, revision: at.revision }),
      async (text) => await services.engine.reading.elements(text) as ClassElement[]);
    const rows = services.engine.rows(model, at.runtime, { mapping: at.mapping });
    try {
      const columns = await sourceColumns(typer(rows), source);
      // its rows, asked for once: a table its store names that is not open in this tab is said now, by the database
      const first = lambda([], from(source).limit(1).node);
      try {
        if (rows.runner) await rows.runner.host.planAndRun(first, 0);
        else await rows.engine.run((await rows.planner.plan(first)).sql, 0);
      } catch (e) {
        throw new Error(`“${name}” cannot read its rows here (${e instanceof Error ? e.message : String(e)}): a class reads `
          + 'the tables the page that made it opened -- open that page, or its file, first');
      }
      const cubeSource: ClassSource = {
        _type: 'class', name, class: at.class, project: at.project, workspace: at.workspace, revision: at.revision,
        mapping: at.mapping, runtime: at.runtime, columns: columns.map((c) => ({ name: c.name, type: c.type })),
      };
      const r = rebuilt(saved, source, columns, name, capped);
      return {
        label: name,
        // what it leaves out, said when it is first opened: a saved grid's reopening is no change of it
        notes: [...(left.length > 0 && !saved ? [`${name}: not shown, ${left.join(', ')}`] : []), ...r.notes],
        make: gridMaker(r.snapshot, r.configuration, [], {
          ...rowsPlace(rows), cubeSource, classTree: { class: at.class, tree, measures, supertypes }, label: name,
          ...(r.tree ? { tree: r.tree } : {}),
          foldPanel: capped,
        }),
        discard: async () => letGo(rows),
      };
    } catch (e) {
      letGo(rows);
      throw e;
    }
  }

  /**
   * A CLASS A PERSON NAMED for a new grid (the picker's Classes, Add a Grid over a class: K16, K17), by the one rule
   * Python's page.grid(cls=...) names it by (class-source.ts classGiven): opened here where the page plans and runs its
   * own SQL; on a page its other editor serves (Python's), which has no compiler or model home of its own there, its cube
   * made by the host where it reads the class (DatabaseService.made: headless sourceCube, page.grid's own) and opened as
   * every grid of that page is (openedOnHost).
   */
  async function openedGivenClass(given: ClassGiven, capped: boolean): Promise<Opened> {
    const model = page.model ?? null;
    if (local) {
      const writing = services.engine.modelWriting;
      if (!writing) throw new Error('this page writes no model: a class is read through the runtime its place writes');
      const { classGiven } = await lazily(() => import('../class-source.ts'));
      const at = classGiven(model, given, writing.place);
      if (typeof at === 'string') throw new Error(at);
      return openedClass(at, undefined, capped);
    }
    if (!database.made) throw new Error('this page reaches no host that reads a class');
    const doc = readCube(fromJson(await database.made(model === null ? null : JSON.stringify(model), JSON.stringify(given),
      page.title, capped ? 2 : 1)));
    return openedOnHost(doc.source, doc, capped);
  }

  // -- every kind --------------------------------------------------------------------------------------------------

  function opened(chosen: Chosen, capped: boolean): Promise<Opened> {
    if (chosen.kind === 'saved') return Promise.resolve(openedSavedQuery(chosen.query, undefined, capped));
    if (chosen.kind === 'class') return openedGivenClass(chosen.given, capped);
    if (chosen.kind === 'file') {
      return openedFile(chosen.file, {
        ...(chosen.sample ? { sample: chosen.sample } : {}),
        ...(chosen.handle ? { handle: chosen.handle } : {}),
        capped,
      });
    }
    if (chosen.kind === 'table') {
      if (!warehouse) return Promise.reject(new Error('this page reaches no warehouse'));
      return openedTable(warehouse, chosen.session, chosen.object, undefined, capped);
    }
    return openedRemote(chosen.url, chosen.s3, undefined, capped);
  }

  const named = (chosen: Chosen): string => (chosen.kind === 'saved' ? chosen.query.label : chosen.kind === 'file' ? chosen.file.name
    : chosen.kind === 'table' ? `${chosen.object.schema}.${chosen.object.name}` : chosen.kind === 'class' ? chosen.given.class
      : lastSegment(chosen.url));

  /**
   * A saved cube's source opened again, the least intrusive way that works (a file asked for, a saved query rerun, a
   * warehouse signed in to, a remote file's keys asked for): its grid's maker, or undefined when the person gave up.
   */
  async function reopened(doc: CubeDocument, page: string | undefined, several: boolean, tables?: readonly string[],
    served = false): Promise<Opened | undefined> {
    const src = doc.source;
    // a grid of the page its other editor serves: every kind but a frame's opened by the host, where it reads it (H-2,
    // H-3). A page of this tab's own on a host opens its sources the tab's way -- the host holds only what was read
    // there this session, and refuses a file it does not (the S7 confirmation audit's H2-1)
    if (served && src._type !== 'frame') {
      if (!database.opened) throw new Error(`${src.name} is the host's, and this page reaches no host`);
      return openedOnHost(src, doc, several);
    }
    // `several`: one of a page's grids, its columns panel folded as a grid added beside others is
    if (src._type === 'savedQuery') return openedSavedQuery(await openedRecord(src.query), doc, several);
    if (src._type === 'warehouseTable') return reopenTable(src, doc, several);
    if (src._type === 'remoteFile') return reopenRemote(src, doc, several);
    // a dataframe's rows are the Python's that serves it: opened where the host has its frames, refused elsewhere
    if (src._type === 'frame') return openedFrame(src.name, doc, several);
    // a class at the revision its grid was read at: its pin, kept by its source (§16.1)
    if (src._type === 'class') return openedClass(src, doc, several);
    // a join's files are its leaves': each read from where the page kept it, as a file's grid's is, under the name its
    // page's model keeps for it
    if (src._type === 'join') {
      // on a host's page, the host opens it over the tables it holds -- this page has no database or compiler of its own
      if (!local && database.opened) return openedOnHost(src, doc, several);
      return openedJoin(src, { saved: doc, capped: several, ...(page !== undefined ? { page } : {}), ...(tables ? { kept: tables } : {}) });
    }
    if (!isFileCube(doc)) throw new Error(`"${doc.name}" reads a source this page cannot open`);
    const got = await fileFor(doc.source, doc.name, page);
    if (!got) return undefined;
    return openedFile(got.file, {
      ...(got.handle ? { handle: got.handle } : {}),
      ...(doc.source.sample ? { sample: doc.source.sample } : {}),
      saved: doc,
      capped: several,
      ...(tables?.[0] !== undefined ? { table: tables[0] } : {}),
    });
  }

  /**
   * THE SOURCE PICKER (src/ui/source-picker.ts; the user, 2026-10-01): where the rows come from, one window for every
   * kind the services can open -- a file, an example, a saved query, a warehouse table, a remote file. Each choice is
   * read INSIDE the window, so a refusal is shown there.
   */
  async function pick<T>(purpose: 'add' | 'open', act: (chosen: Chosen) => Promise<T | null>, start?: SectionId): Promise<T | undefined> {
    const address = services.browser.address;
    const files = database.ingestFile !== undefined;
    const sections: PickerSections<T | null> = {
      ...(files ? { files: {
        accept: '.csv,.parquet,.json,.jsonl,.ndjson,.gz,text/csv,application/json,application/gzip',
        formats: ['CSV', 'Parquet', 'JSON', 'JSON Lines'],
        // where the browser can keep a handle to the file, pick THROUGH it: a saved cube then reopens its file from
        // where it was picked (file-handles.ts)
        ...(canKeepHandles() ? { pick: () => pickDataFile() } : {}),
        open: (file: File, handle?: unknown) => act({ kind: 'file', file, ...(handle ? { handle: handle as FileHandle } : {}) }),
      } } : {}),
      ...(files ? { examples: {
        list: SAMPLES.map((s) => ({
          id: s.id, name: s.label, description: s.about, rows: s.defaultRows,
          tags: [s.format === 'jsonl' ? 'JSON Lines' : 'CSV'],
        })),
        open: (id, rows) => act({ kind: 'file', ...sampleFile(id, rows) }),
        // the generated file itself, for sharing or reopening
        download: (id: string, rows: number) => {
          const { file, text } = sampleFile(id, rows);
          services.browser.download(file.name, file.type, text);
        },
      } } : {}),
      ...(queries ? { saved: savedSection(queries, address, act) } : {}),
      ...(warehouse ? { database: databaseSection(warehouse, (session, object) => act({ kind: 'table', session, object })) } : {}),
      ...(database.mountRemote ? { remote: {
        detect: detectFormat,
        reader: remoteReader,
        open: (url: string, credentials?: RemoteCredentials) => act({ kind: 'remote', url, ...(credentials ? { s3: s3Of(credentials) } : {}) }),
      } } : {}),
      // a class of a project at a revision (K17): where the page reads a model home and writes a place's runtime, or its
      // host makes the class's cube
      ...((local && modelHome && services.engine.modelWriting) || database.made ? { classes: {
        open: (given: { readonly class: string; readonly project?: string; readonly revision?: string }) => act({ kind: 'class', given }),
      } } : {}),
    };
    const made = await pickSource(document, { purpose, sections, ...(start ? { start } : {}) });
    return made ?? undefined;
  }

  /** The Saved queries section: the store's queries, searched; one opened as a grid's source, or its link copied. */
  function savedSection<T>(queries: QueryService, address: (() => string) | undefined,
    act: (chosen: Chosen) => Promise<T | null>): NonNullable<PickerSections<T | null>['saved']> {
    return {
        where: queries.where,
        // the browser's store: a query saved in another tab (Legend Query) appears without reopening
        ...(queries.watch ? { watch: queries.watch } : {}),
        search: async (text, mineOnly) => (await (await queries.store()).search({
          ...(text ? { searchTermSpecification: { searchTerm: text, includeOwner: true } } : {}),
          showCurrentUserQueriesOnly: mineOnly,
          sortByOption: 'SORT_BY_UPDATE',
          limit: 50,
        })).map((q) => {
          const project = projectFor(q);
          return {
            id: q.id,
            name: q.name,
            ...(q.owner ? { owner: q.owner } : {}),
            ...(q.lastUpdatedAt ? { modified: new Date(q.lastUpdatedAt).toLocaleDateString(UI_LOCALE) } : {}),
            project: project?.title ?? `${q.groupId}:${q.artifactId}:${q.versionId}`,
            // with a depot, a project not in config.json opens by name (openedRecord)
            ...(project || modelHome ? {} : { unusable: 'This page has no model for its project' }),
          };
        }),
        open: async (id) => act({ kind: 'saved', query: await openedQuery(await queries.store(), id) }),
        // a link to this page that opens the query as its source (query-store/src/share.ts), where the page has an
        // address others can open
        ...(address ? { copyLink: async (id: string) => {
          const [store, { queryFragment }] = await Promise.all([queries.store(), queryStores()]);
          const at = new URL(address!());
          at.hash = await queryFragment(await store.get(id));
          const link = at.href;
          await services.browser.writeClipboard(link);
          return `Link copied (${link.length.toLocaleString(UI_LOCALE)} characters): it opens the query as the cube’s source, and holds the query, never its rows.`;
        } } : {}),
    };
  }

  return {
    madeGrid,
    dropUnread,
    place: (id) => gridPlaces.get(id),
    filesNow,
    snapTarget: () => (local ? snapInto(local) : undefined),
    reserve: (names) => { for (const t of names) reserved.add(t); },
    // the files read for grids not made yet go when each is made or let go (madeGrid, discard), not with an opening: a
    // second page opening over the first still reads them
    release: () => reserved.clear(),
    opened,
    named,
    reopened,
    openedRecord,
    openedFrame,
    openedClass,
    openedJoin,
    framesOf: (id) => {
      const src = gridPlaces.get(id)?.cubeSource;
      return src ? leavesOf(src).flatMap((l) => (l._type === 'frame' ? [l.name] : [])) : [];
    },
    frameVersion: (name) => framesRead.get(name)?.version,
    frameAgain: async (name) => {
      if (!database.frame) throw new Error('this page reads no frame');
      const was = framesRead.get(name);
      const m = await database.frame(name);
      if (!was || was.model !== m.model || was.source !== JSON.stringify(m.source)) return 'changed';
      framesRead.set(name, { ...was, version: m.version });
      return 'same';
    },
    pick,
    signInWithKey: async (key) => {
      if (!warehouse) throw new Error('this page reaches no warehouse');
      signedIn = await warehouse.signInWithKey(key);
      return signedIn;
    },
    sessionHeld: () => (signedIn ? `the warehouse session (${signedIn.session.principal})` : undefined),
    filesHeld: () => {
      const opened = [...new Set(filesNow().filter((f) => !f.sample).map((f) => f.file.name))];
      return opened.length > 0 ? `the file${opened.length === 1 ? '' : 's'} opened in this tab (${opened.join(', ')})` : undefined;
    },
  };
}
