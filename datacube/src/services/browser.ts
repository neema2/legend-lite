// THE BROWSER TAB'S SERVICES (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.2): what DataCube's app runs on in a tab --
// the planner the page chose (the tab's WebAssembly module, or a server), DuckDB-WASM as the database every file,
// remote file and Snap goes into, the saved pages in this browser's IndexedDB (memory when it refuses), the saved
// queries in the query store the deployment names (this browser's own when none), the model home a depot names, the
// warehouse the picker offers, and the browser's downloads, clipboard and kept settings. Each is the client the tab
// already used, built here once rather than at each use; the tab's entry hands in what only it knows (its plane, its
// planner, its DuckDB, its configuration).

import type { TsItemAnswer } from '../lite-planner.ts';
import type { openDuckDbInTab } from '../../../engine-client/src/doors/tab-duckdb.ts';
import type { ProjectDoor } from '../../../engine-client/src/doors/project-data.ts';
import type { ProjectDataSource } from '../../../engine-client/src/planner-sources.ts';
import type { QueriesModule, QueryReader } from '../../../query-store/src/index.ts';
import type { Planner } from '../cube.ts';
import {
  BrowserRecords, CubeStoreClient, LOCAL_CUBES_API, MemoryRecords, openCubeDatabase, persistStorage, wasmCubeServer, type CubeRecords,
} from '../cube-store.ts';
import { FileHandles } from '../file-handles.ts';
import type { TableModels } from '../infer.ts';
import { writesModels, type ModelWriter } from '../model-writer.ts';
import type { ModelOptions } from '../planner.ts';
import { PlanThenRun } from '../runner.ts';
import { forgetUpload, ingestFile, mountRemoteView, type DuckDbFiles } from '../upload.ts';
import { browserBasics, browserBasicsFor, tabTitle } from './browser-basics.ts';
import type {
  ConfiguredProject,
  DatabaseService,
  ModelHomeService,
  ModelWriting,
  PageService,
  PlaneService,
  QueryService,
  Services,
  WarehouseService,
} from '../app/services.ts';
import { lazily } from '../lazily.ts';

/** The tab's DuckDB engine, as its door opens it (engine-client's doors/tab-duckdb.ts: the C5-T last round's T1, an app
 *  reaches the run side through its doors alone). */
type DuckDbEngine = Awaited<ReturnType<typeof openDuckDbInTab>>['engine'];

// the saved queries' code, fetched the first time it is needed -- the Saved queries section, a query link -- as charts
// are: a grid-only page never downloads it (test/bundle-budget.test.ts)
const queryStores = () => lazily(() => import('../../../query-store/src/index.ts'));

/** The tab's planner, as its plane builds it (demo/planners.ts). */
export interface TabPlanner {
  readonly planner: Planner;
  readonly label: string;
  readonly tables: TableModels;
  /** legend-lite's planner for a project's own database (C5, engine-client's project-data.ts). */
  readonly projects: ProjectDataSource;
  /** A timeseries series' #TS text, read by legend-lite's module (A3 step 3): the same module as `tables`. */
  tsItem(text: string): Promise<TsItemAnswer>;
  readonly models: {
    another(model: string, runtime: string, how?: ModelOptions): Planner;
    elements(text: string): Promise<unknown[]>;
  };
}

/** What the tab's deployment says (demo/page-config.ts `PageConfig`): its servers, its store, its projects, its depot. */
export interface TabConfig {
  readonly warehouse: string;
  readonly queryStore: string;
  readonly user: string;
  readonly projects: readonly ConfiguredProject[];
  readonly depot?: { readonly sdlc: string };
  /** Where the page's WebAssembly is served (page-config.ts `vendor`). */
  readonly vendor: string;
}

export interface BrowserServicesOptions {
  readonly plane: PlaneService;
  readonly planner: TabPlanner;
  /** This tab's DuckDB: its connection's engine, the database files are registered on, and its other connections. */
  readonly duckdb: { readonly engine: DuckDbEngine; readonly db: DuckDbFiles; connect(): Promise<DuckDbEngine> };
  readonly config: TabConfig;
  /** Where the page is, for the links it makes (Studio on a project). */
  readonly location: { readonly href: string };
  /** A component in another app: local services only, private storage names, no ownership of its title or URL. */
  readonly embedding?: { readonly storageNamespace: string };
}

export interface BrowserServices extends Services {
  /** Close this instance's browser storage connections after its UI has been disposed. */
  dispose(): Promise<void>;
}

/** The tab's services, built once as the page starts, over the tab's DuckDB as engine-client's `openDuckDbInTab`
 *  started it (its session set up, its extensions the site's). */
export async function browserServices(o: BrowserServicesOptions): Promise<BrowserServices> {
  const { config } = o;
  if (o.embedding && (config.warehouse || config.queryStore || config.depot)) {
    throw new Error('An embedded browser DataCube uses only local services');
  }
  const namespace = o.embedding?.storageNamespace;
  const releases: (() => Promise<void>)[] = [];
  const lifetime = new AbortController();
  let disposed = false;
  const checkOpen = (): void => { if (disposed) throw new Error('DataCube browser services have been disposed'); };
  const modelHome = o.config.depot ? depotHome(o.config.depot.sdlc, o.config.vendor, o.location) : undefined;
  // THE STORES' RULES: the saved queries' and the saved pages', one WebAssembly module (`vendor/queries/`), fetched
  // once, at the first call of either store (a load that failed is tried again at the next)
  let stores: Promise<QueriesModule> | undefined;
  const storeRules = (): Promise<QueriesModule> => (stores ??= queryStores()
    .then((m) => m.loadQueriesModule(new URL(`${config.vendor}queries/`, o.location.href).href))
    .catch((e: unknown) => {
      stores = undefined;
      throw e;
    }));
  let projectKeeper: Promise<ProjectDoor> | undefined;
  const pages = browserPages(storeRules, namespace, releases, lifetime.signal);
  // The owner can be cancelled before its app observes this promise. Preserve the refusal for that app without
  // reporting an unhandled rejection when disposal is all that ever consumes this service.
  void pages.catch(() => undefined);
  return {
    dispose: async () => {
      if (disposed) return;
      disposed = true;
      lifetime.abort();
      const closed = await Promise.allSettled(releases.splice(0).map((release) => release()));
      const errors = closed.filter((r): r is PromiseRejectedResult => r.status === 'rejected').map((r) => r.reason as unknown);
      if (errors.length > 0) throw new AggregateError(errors, 'Could not close DataCube browser storage');
    },
    engine: {
      plane: o.plane,
      label: o.planner.label,
      // the page's own planner reads a saved query and its project's model
      reading: {
        parse: (text) => o.planner.planner.parse(text), elements: (text) => o.planner.models.elements(text),
        // a series' #TS text: legend-lite's module, beside whichever planner the page chose (the one reader, A3)
        tsItem: (text) => o.planner.tsItem(text),
      },
      // planned here and run here: a planner over the source's own model, on this tab's DuckDB
      rows: (model, runtime, how) => ({ planner: o.planner.models.another(model, runtime, how), engine: o.duckdb.engine }),
      database: tabDatabase(o.duckdb, o.planner.tables),
      // the model actions: the tab's module writes (beside whichever planner the page chose), the tab's DuckDB answers
      ...(writesModels(o.planner.tables) ? { modelWriting: tabModelWriting(o.planner.tables, o.duckdb.engine) } : {}),
      local: {
        planner: o.planner.planner,
        another: (model, runtime, how) => o.planner.models.another(model, runtime, how),
        elements: (text) => o.planner.models.elements(text),
        tables: o.planner.tables,
        // a project's own rows, in a database of their own inside this tab's DuckDB (C5): the keeper fetched with the
        // first saved query a page opens by name, never with the grid's start
        projects: () => (projectKeeper ??= lazily(() => import('../../../engine-client/src/doors/project-data.ts')).then(({ ProjectDoor: Keeper }) =>
          new Keeper({
            databaseType: o.duckdb.engine.databaseType,
            engine: o.duckdb.engine,
            connect: () => o.duckdb.connect(),
          }, o.planner.projects))),
        engine: o.duckdb.engine,
      },
    },
    // opened now and waited for where a page is saved or opened: the start's first grid does not wait on IndexedDB
    pages,
    queries: queryService(config, storeRules, namespace, releases, checkOpen),
    ...(modelHome ? { modelHome } : {}),
    ...(o.embedding ? {} : { warehouse: warehouseService(config, o.location) }),
    browser: {
      ...(namespace === undefined ? browserBasics : browserBasicsFor(`${namespace}.settings`)),
      ...(o.embedding ? {} : { address: () => o.location.href, title: tabTitle() }),
    },
  };
}

/** The model actions in this tab: the module's writer, a project's facts planned by it and run on the tab's DuckDB. */
function tabModelWriting(writer: ModelWriter, engine: DuckDbEngine): ModelWriting {
  return { place: 'Browser', writer, rows: (model, runtime) => new PlanThenRun(writer.withModel(model, runtime), engine) };
}

/** DuckDB in this tab: every grid's rows, and where a file, a remote file or a Snap goes. */
function tabDatabase(duck: { readonly engine: DuckDbEngine; readonly db: DuckDbFiles }, tables: TableModels): DatabaseService {
  const { engine, db } = duck;
  return {
    ingestFile: (file, options) => ingestFile(engine, db, file, tables, options),
    forgetFile: (fileName, table) => forgetUpload(engine, tables, db, fileName, table),
    mountRemote: (url, s3, name) => mountRemoteView(engine, tables, { url, name, ...(s3 ? { s3 } : {}) }),
  };
}

/** SAVED PAGES, in this browser: IndexedDB when the browser gives it, else memory (this visit only, and said so), by
 *  legend-lite's own rules (core's SavedCubes, compiled to WebAssembly; the program's B6). One database holds the pages
 *  and their files' handles. */
async function browserPages(load: () => Promise<QueriesModule>, namespace: string | undefined,
  releases: (() => Promise<void>)[], signal: AbortSignal): Promise<PageService> {
  let records: CubeRecords;
  let handles: FileHandles | undefined;
  let memoryOnly = false;
  try {
    const database = openCubeDatabase(globalThis.indexedDB, namespace === undefined ? 'datacube' : `${namespace}.pages`, signal);
    // A refused or cancelled open is handled below; a late successful open closes itself after cancellation.
    releases.push(() => database.then((db) => db.close(), () => undefined));
    await database;
    records = new BrowserRecords(database);
    handles = new FileHandles(database);
  } catch {
    signal.throwIfAborted();
    records = new MemoryRecords();
    memoryOnly = true;
  }
  const user = 'this browser';
  return {
    store: new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user }).fetch, user),
    where: memoryOnly ? 'for this visit only (this browser refused storage)' : 'in this browser',
    memoryOnly,
    ...(handles ? { handles } : {}),
    persist: persistStorage,
  };
}

/**
 * THE QUERY STORE, through the one client (query-store/README.md): the server the deployment names, or -- none named --
 * the same API answered in this page from this origin's browser store, the one Legend Query keeps its saved queries in
 * when it runs without a server, by legend-lite's own rules (the server's, compiled to WebAssembly: the stores' module,
 * fetched the first time a store is asked; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5).
 */
function queryService(config: TabConfig, load: () => Promise<QueriesModule>, namespace: string | undefined,
  releases: (() => Promise<void>)[], checkOpen: () => void): QueryService {
  const url = config.queryStore;
  let store: Promise<QueryReader> | undefined;
  return {
    store: () => {
      checkOpen();
      return (store ??= queryStores().then((m) => {
        checkOpen();
        if (url) return new m.QueryStoreClient(url);
        const records = new m.BrowserRecords(globalThis.indexedDB,
          namespace === undefined ? m.DATABASE : `${namespace}.queries`,
          namespace === undefined ? m.CHANNEL : `${namespace}.queries.changed`);
        releases.push(() => records.close());
        return new m.QueryStoreClient(m.LOCAL_API, m.wasmQueryServer({ load, records, user: config.user }).fetch);
      }));
    },
    where: url ? `on ${hostOf(url)}` : 'in this browser',
    // the browser's store: a query saved in another tab (Legend Query) appears without reopening
    ...(url ? {} : {
      watch: (changed: () => void) => {
        let stop = (): void => undefined;
        let stopped = false;
        void queryStores().then((m) => {
          if (!stopped) stop = m.watchBrowserStore(changed, namespace === undefined ? m.CHANNEL : `${namespace}.queries.changed`);
        });
        return () => { stopped = true; stop(); };
      },
    }),
    projects: config.projects,
    fetchText: async (url) => {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`${url} answered ${r.status}`);
      return r.text();
    },
  };
}

/** A server's host as the page names it; an address that does not parse is said as written (it is refused when used). */
function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/** The model home a depot names (the page's own SDLC and Depot, or servers), opened once -- loaded only when first
 *  asked for: the grid-only page's budget does not carry it. */
function depotHome(sdlc: string, vendor: string, location: { readonly href: string }): ModelHomeService {
  let home: ReturnType<ModelHomeService['connect']> | undefined;
  return {
    connect: () => (home ??= lazily(() => import('../../../depot-client/src/model-home.ts'))
      .then(({ connectModelHome }) => connectModelHome({ sdlc, vendor }))),
    // beside this page, as the one site lays the apps out (<site>/datacube/demo/, <site>/studio/demo/), wherever the site
    // is served -- under a path on GitHub Pages; at the root, where `..` stops, as before
    studio: (project, workspace) => new URL(
      `../../studio/demo/index.html#/edit/${encodeURIComponent(project)}/${encodeURIComponent(workspace)}`, location.href).href,
  };
}

/** Which warehouse to offer: the deployment's (config.json, ?warehouse=), else the last one this browser signed in
 *  to -- a convenience kept in this browser only; storage may be refused. */
const REMEMBERED = 'datacube.warehouse.url';

function warehouseService(config: TabConfig, location: { readonly href: string }): WarehouseService {
  // THE WAREHOUSE'S DRIVER AND ITS API: the driver (engine-client's warehouse.ts, through its door, doors/warehouse.ts:
  // the C5-T last round's T1) and the one binding (vendor/warehouse/, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3), both
  // fetched at the first sign-in (a load that failed is tried again at the next): a page that never signs in to a
  // warehouse loads neither. A table is opened only once signed in, so its live engine finds them loaded
  type Warehouse = Awaited<ReturnType<typeof import('../../../engine-client/src/doors/warehouse.ts')['warehouseAt']>>;
  let ready: Warehouse | undefined;
  let loading: Promise<Warehouse> | undefined;
  const loaded = (): Promise<Warehouse> => (loading ??=
    lazily(() => import('../../../engine-client/src/doors/warehouse.ts'))
      .then(async (door) => (ready = await door.warehouseAt(new URL(`${config.vendor}warehouse/`, location.href).href)))
      .catch((e: unknown) => {
        loading = undefined;
        throw e;
      }));
  return {
    offered: () => {
      if (config.warehouse) return config.warehouse;
      try {
        return window.localStorage.getItem(REMEMBERED) ?? '';
      } catch {
        return '';
      }
    },
    remember: (url) => {
      try {
        window.localStorage.setItem(REMEMBERED, url);
      } catch {
        // storage refused: not remembered, nothing else changes
      }
    },
    connect: async (url, user, password) => (await loaded()).connect(url, user, password),
    signInWithKey: async (key) => {
      if (!config.warehouse) throw new Error('this page was not served by a warehouse: its config.json names none');
      return (await loaded()).signInWithKey(config.warehouse, key);
    },
    live: (session, catalog, databaseType) => {
      if (ready === undefined) throw new Error('the warehouse is read once signed in: sign in first');
      return ready.live(session, catalog, databaseType);
    },
  };
}
