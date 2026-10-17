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
import { lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { HeldCopy } from '../app.ts';
import type { CubeConfiguration } from '../config.ts';
import type { HostSnap, Planner, SnapTarget } from '../cube.ts';
import {
  fileSource,
  frameSource,
  rebuilt,
  type CubeDocument,
  type CubeSource,
  type FileSource,
  type QuerySource,
  type RemoteSource,
  type WarehouseSource,
} from '../cube-document.ts';
import { canKeepHandles, pickDataFile, readHandle, type FileHandle } from '../file-handles.ts';
import type { InferredModel } from '../infer.ts';
import type { GridMaker } from '../page/page-app.ts';
import type { ModelOptions } from '../planner.ts';
import { inferFormat, type S3Credentials } from '../remote.ts';
import type { PlanAndRun, QueryRunner } from '../runner.ts';
import type { ModelElement } from '../saved-queries.ts';
import { SAMPLES, sampleById, sampleFileName, type Sample } from '../samples.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import { sourceColumns } from '../source-columns.ts';
import type { TreeState } from '../tree.ts';
import { pickSource, type DatabaseSession, type PickerSections, type RemoteCredentials, type SectionId } from '../ui/source-picker.ts';
import { formatOf, tableNameOf } from '../upload.ts';
import type { ConfiguredProject, GridRows, LocalEngine, QueryService, Services, WarehouseService } from './services.ts';

// saved queries' code, fetched the first time it is needed -- the Saved queries section, a query link -- as charts
// are: a grid-only page never downloads it (test/bundle-budget.test.ts)
const savedQueries = () => import('../saved-queries.ts');
const queryStores = () => import('../../../query-store/src/index.ts');

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
  | { readonly kind: 'saved'; readonly query: OpenedQuery };

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
  /** The page: the cubes it holds now (a file's table goes with its last grid). */
  readonly page: { readonly cubes: readonly string[] };
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
  /** The files on the page now: one per grid that reads one. */
  filesNow(): GridFile[];
  /** While a saved page opens, the tables its model keeps: no fresh name takes one of them (the audit's S3). */
  reserve(tables: Iterable<string>): void;
  release(): void;
  /** The chosen source opened for a grid: fresh, or -- `capped` -- added beside the others. */
  opened(chosen: Chosen, capped: boolean): Promise<Opened>;
  /** What a chosen source is called, as asking to open it in place says it. */
  named(chosen: Chosen): string;
  /** A saved cube's source opened again, the least intrusive way that works; undefined when the person gave up. */
  reopened(doc: CubeDocument, keys: readonly string[], several: boolean, table?: string): Promise<Opened | undefined>;
  /** A saved query's record -- from a store, or as a share link carries it -- as a cube's source. */
  openedRecord(q: SavedQueryRecord): Promise<OpenedQuery>;
  /** A frame of the host's (Python's dataframe) for a grid: fresh, or a saved cube rebuilt over it. */
  openedFrame(name: string, saved?: CubeDocument, capped?: boolean): Promise<Opened>;
  /** The frame a grid reads, by its id: what following the host's frames refreshes. */
  frameOf(id: string): string | undefined;
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

const lastSegment = (url: string): string => url.replace(/[?#].*$/, '').replace(/\/+$/, '').split('/').pop() || url;

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
  /** Each grid's file, by the grid's id: read only for the grids on the page now (`page.cubes`). */
  const files = new Map<string, Omit<GridFile, 'grid'>>();
  const filesNow = (): GridFile[] => page.cubes.flatMap((grid) => {
    const f = files.get(grid);
    return f ? [{ grid, ...f }] : [];
  });
  /** The tables files were read into for grids, by table: the file's name, to drop it by. */
  const held = new Map<string, string>();
  /** Each grid's own planners (by its tile's id), which hold its model: let go with the last grid using them. */
  const gridPlanners = new Map<string, readonly Planner[]>();
  /** Each grid's place, by its id: what the page's model actions read of it (its planner, engine, label). */
  const gridPlaces = new Map<string, GridPlace>();

  const madeGrid = (id: string, place: GridPlace): void => {
    // its own planners (its source's, and a snapped copy's): let go with the last grid planning through them
    gridPlanners.set(id, [place.planner, place.snapTarget?.planner].filter((p): p is Planner => p !== undefined));
    gridPlaces.set(id, place);
    const src = place.cubeSource;
    if (place.file && src?._type === 'file') {
      files.set(id, { ...place.file, sha256: src.sha256, sample: src.sample !== undefined });
      held.set(place.file.table, place.file.file.name);
    }
  };

  /**
   * A FILE'S TABLE GOES WITH ITS LAST GRID: once no grid on the page (or kept off it for a detached chart) reads it, it
   * is dropped from the host's database and its name is free again -- a file opened three times in place holds one
   * table, not three. Told of every change on the page.
   */
  const dropUnread = (): void => {
    const read = new Set(filesNow().map((f) => f.table));
    const live = new Set(page.cubes);
    for (const grid of [...files.keys()]) if (!live.has(grid)) files.delete(grid);
    for (const [table, fileName] of [...held]) {
      if (read.has(table)) continue;
      held.delete(table);
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
   * A warehouse table's model: its own runtime (the catalog's database type), and the runtime its Snap -- a copy into
   * the host's database -- is planned against (the host database's type).
   */
  const warehouseModel = (l: LocalEngine, o: CatalogObject): Promise<InferredModel & { readonly snapRuntime: string }> => l.tables.tableModel(
    o.columns.map((c) => ({ ...c, dataType: c.type })),
    { table: o.name, schema: o.schema, convertible: false, databaseType: o.databaseType, snapDatabaseType: l.engine.databaseType });
  /**
   * A warehouse table's Snap: the rows pulled with the live plan, into a table of the same name in the host's database,
   * and every query on the copy planned by the SAME model against the snap runtime.
   */
  const snapOf = (l: LocalEngine, o: CatalogObject, m: InferredModel & { readonly snapRuntime: string }): { snapTarget: SnapTarget } => ({
    snapTarget: {
      schema: o.schema, table: o.name, source: m.source, conversions: m.conversions,
      planner: l.another(m.model, m.snapRuntime, { bitColumns: m.bitColumns }),
      host: l.tables,
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
    const snap = snapOf(local, chosen, m);
    return {
      label: name,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], {
        planner: own, live, ...snap, cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
      }),
      // never made into a grid: its planners' models let go
      discard: async () => {
        own.dispose?.();
        snap.snapTarget.planner.dispose?.();
      },
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
    // Snap there: the copy under the table's own name in the host's database, read through the same model's snap
    // runtime -- planned and run by the host as the live rows are
    const copy = on.snap && on.release ? services.engine.rows(m.model, m.snapRuntime, { bitColumns: m.bitColumns }) : undefined;
    const hostSnap: HostSnap | undefined = copy?.runner && on.snap && on.release ? {
      table: chosen.name, schema: chosen.schema, source: m.source, runner: copy.runner, store: on.place ?? 'the host',
      snap: (pull) => on.snap!(m.model, pull),
      release: () => on.release!(m.model),
    } : undefined;
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
      discard: async () => {
        letGo(rows);
        if (copy) letGo(copy);
      },
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
      ...(on.table && on.place !== undefined ? { copiesInto: on.place } : {}),
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
      const rows = services.engine.rows(opened.model, opened.runtime, { bitColumns: opened.bitColumns });
      const columns = await sourceColumns(typer(rows), opened.source);
      const source = await fileSource(file, formatOf(file.name), columns, how.sample);
      const saved = how.saved;
      const r = rebuilt(saved, opened.source, columns, opened.fileName, how.capped === true);
      const notes = saved
        ? [
          ...(saved.source._type !== 'file' || source.sha256 !== saved.source.sha256
            ? [`${file.name} is not the file this cube was saved over (its contents differ)`]
            : []),
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
          return forgetFile(file.name, table).catch(() => {}).finally(() => taken.delete(table));
        },
        make: gridMaker(r.snapshot, r.configuration, [], {
          ...rowsPlace(rows),
          cubeSource: source,
          heldCopy: { label: opened.fileName, takenAt: loadedAt, rowCount: opened.rowCount },
          label: opened.fileName,
          ...(r.tree ? { tree: r.tree } : {}),
          file: { file, table, ...(how.handle ? { handle: how.handle } : {}) },
          foldPanel: how.capped === true,
        }),
      };
    } catch (e) {
      if (reserved !== undefined) taken.delete(reserved);
      // read, then refused further on (its columns not typed, say): its table goes, and its name is free again
      if (read !== undefined) {
        const gone = read;
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
  async function fileFor(doc: FileCube, keys: readonly string[]): Promise<{ file: File; handle?: FileHandle } | undefined> {
    const src = doc.source;
    if (src.sample) {
      const s = sampleById(src.sample.id);
      if (s) {
        return { file: new File([s.build(src.sample.rows)], src.name, { type: mimeOf(s) }) };
      }
    }
    const open = [...files.values()].find((f) => f.sha256 === src.sha256);
    if (open) return { file: open.file, ...(open.handle ? { handle: open.handle } : {}) };
    let kept: FileHandle | undefined;
    const handles = (await services.pages)?.handles;
    for (const key of keys) kept ??= await handles?.get(key);
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
              resolve(again.state === 'file' ? { file: again.file, handle: kept } : await chooseFile(doc));
            });
          });
          d.ask([`"${doc.name}" reads ${kept.name} from where you picked it. The browser wants you to allow it:`, button]);
        });
      }
    }
    return chooseFile(doc);
  }

  /** Ask the user for the file, naming the one the cube was saved over. */
  function chooseFile(doc: FileCube): Promise<{ file: File; handle?: FileHandle } | undefined> {
    return new Promise((resolve) => {
      const src = doc.source;
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
        `"${doc.name}" was built over ${src.name} (${bytes(src.size)}). Choose that file:`,
        choose, cancel, input,
      ]);
    });
  }

  // -- a remote file -------------------------------------------------------------------------------------------------

  /** A remote file for a grid, mounted as a view of its own; its keys are never written down. */
  async function openedRemote(url: string, s3: S3Credentials | undefined, saved?: CubeDocument, capped = false): Promise<Opened> {
    if (!database.mountRemote) throw new Error('this page reads no remote file');
    const m = await database.mountRemote(url, s3, freshTable('remote'));
    const rows = services.engine.rows(m.model, m.runtime, { bitColumns: m.bitColumns });
    const columns = await sourceColumns(typer(rows), m.source);
    const name = lastSegment(url);
    const cubeSource: RemoteSource = { _type: 'remoteFile', name, url, columns: columns.map((c) => ({ name: c.name, type: c.type })) };
    const r = rebuilt(saved, m.source, columns, name, capped);
    return {
      label: name,
      notes: r.notes,
      make: gridMaker(r.snapshot, r.configuration, [], { ...rowsPlace(rows), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped }),
      discard: async () => letGo(rows),
    };
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
      make: gridMaker(r.snapshot, r.configuration, [], { ...rowsPlace(rows), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped }),
      discard: async () => letGo(rows),
    };
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
    const { contextOf, enumerationsOf, enumsAsStrings, projectOf, sourceOf } = await savedQueries();
    const project = projectFor(q);
    if (!project && !modelHome) throw new Error(`“${q.name}” belongs to ${projectOf(q)}, which this page has no model for (config.json projects[], or a depot)`);
    const key = projectOf(q);
    // the page's own project (config.json projects[]), else the version opened by name from Depot (design Phase 3)
    const model = await once(projectModels, key, async () => (project
      ? (await Promise.all(project.models.map((url) => queries.fetchText(url)))).join('\n')
      : (await import('../../../depot-client/src/model-text.ts')).modelText((await modelHome!.connect()).depot, q.groupId, q.artifactId, q.versionId)));
    const elements = await reading.elements(model) as ModelElement[];
    const context = contextOf(q, elements);
    const lambdaOf = await reading.parse(q.content);
    const values = new Map<string, ValueSpecification>();
    for (const v of q.defaultParameterValues ?? []) {
      const parsed = await reading.parse(`|${v.content}`);
      if (parsed.body[0]) values.set(v.name, parsed.body[0]);
    }
    let source = sourceOf(lambdaOf, values);
    // in the tab, a project brings its own rows: its Data elements' tables, in a database of the version's own inside
    // the tab's DuckDB (C5, engine-client's project-data.ts) -- another version may be on the page beside it, in its
    // own. On Python, Python runs a project's model in its own (the host's runner).
    const versioned: QueryEngine | undefined = local
      ? (await (await local.projects()).engineFor(model, local.engine)).engine : undefined;
    const how: ModelOptions = { mapping: context.mapping, enumerations: [...enumerationsOf(elements)] };
    const rows = services.engine.rows(model, context.runtime, how);
    // rule 4: an enumeration column is read as its value's name
    const named = new Set((await typer(rows).relationType(lambda([], source))).filter((c) => c.enumeration).map((c) => c.name));
    if (named.size > 0) source = enumsAsStrings(source, named);
    const columns = await sourceColumns(typer(rows), source);
    const { sharedPart } = await queryStores();
    const cubeSource: QuerySource = {
      _type: 'savedQuery', name: q.name, query: sharedPart(q), columns: columns.map((c) => ({ name: c.name, type: c.type })),
    };
    return { label: q.name, model, runtime: context.runtime, how, source, columns, rows, cubeSource, ...(versioned ? { engine: versioned } : {}) };
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

  // -- every kind --------------------------------------------------------------------------------------------------

  function opened(chosen: Chosen, capped: boolean): Promise<Opened> {
    if (chosen.kind === 'saved') return Promise.resolve(openedSavedQuery(chosen.query, undefined, capped));
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
    : chosen.kind === 'table' ? `${chosen.object.schema}.${chosen.object.name}` : lastSegment(chosen.url));

  /**
   * A saved cube's source opened again, the least intrusive way that works (a file asked for, a saved query rerun, a
   * warehouse signed in to, a remote file's keys asked for): its grid's maker, or undefined when the person gave up.
   */
  async function reopened(doc: CubeDocument, keys: readonly string[], several: boolean, table?: string): Promise<Opened | undefined> {
    const src = doc.source;
    // `several`: one of a page's grids, its columns panel folded as a grid added beside others is
    if (src._type === 'savedQuery') return openedSavedQuery(await openedRecord(src.query), doc, several);
    if (src._type === 'warehouseTable') return reopenTable(src, doc, several);
    if (src._type === 'remoteFile') return reopenRemote(src, doc, several);
    // a dataframe's rows are the Python's that serves it: opened where the host has its frames, refused elsewhere
    if (src._type === 'frame') return openedFrame(src.name, doc, several);
    if (!isFileCube(doc)) throw new Error(`"${doc.name}" reads a source this page cannot open`);
    const got = await fileFor(doc, keys);
    if (!got) return undefined;
    return openedFile(got.file, {
      ...(got.handle ? { handle: got.handle } : {}),
      ...(doc.source.sample ? { sample: doc.source.sample } : {}),
      saved: doc,
      capped: several,
      ...(table !== undefined ? { table } : {}),
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
    reserve: (names) => { for (const t of names) reserved.add(t); },
    release: () => reserved.clear(),
    opened,
    named,
    reopened,
    openedRecord,
    openedFrame,
    frameOf: (id) => {
      const src = gridPlaces.get(id)?.cubeSource;
      return src?._type === 'frame' ? src.name : undefined;
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
