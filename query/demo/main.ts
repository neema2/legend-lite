// The page: read the config (`?config=<file>`, default config.json), start legend-lite's planner
// in a worker, load each project's model, and connect where queries run:
//
//  - duckdb-wasm: DuckDB in this tab, each project's rows its Data elements' (C5) -- no server at all;
//  - warehouse:   the warehouse's DuckDB, signed in as a user (DataCube's Live plane);
//  - server:      legend-lite's server (or legend-engine), which executes and keeps saved queries.
//
// In the browser planes the tab's planner writes the SQL and one of DataCube's engines runs it,
// and saved queries stay in this browser (the same records and rules as a server's store).

import { startDuckDbInTab } from '../../engine-client/src/duckdb-tab.ts';
import { ProjectDatabases, ProjectRunner } from '../../engine-client/src/project-data.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import {
  listObjects, loadWarehouseBinding, signIn, WarehouseEngine, type WarehouseBinding, type WarehouseSession,
} from '../../engine-client/src/warehouse.ts';
import { connectModelHome } from '../../depot-client/src/model-home.ts';
import { loadByName, versionsOf } from '../src/app/by-name.ts';
import { AppContext, type AppConfig, type CubeRows, type LoadedProject } from '../src/app/context.ts';
import { App } from '../src/app/app.ts';
import { BrowserEngine } from '../../engine-client/src/legend/browser-engine.ts';
import { HttpEngine, RoutedEngine, type Engine, type Grammar, type QueryStore } from '../src/backend/engine.ts';
import { BrowserRecords, LOCAL_API, loadQueriesModule, QueryStoreClient, wasmQueryServer } from '../../query-store/src/index.ts';
import { WasmGrammar, WorkerPort } from '../../engine-client/src/legend/wasm-grammar.ts';
import { h, mount } from '../src/ui/dom.ts';

async function text(url: string): Promise<string> {
  const res = await fetch(url, { cache: 'no-cache' });
  if (!res.ok) throw new Error(`could not load ${url}: ${res.status}`);
  return res.text();
}

/**
 * The warehouse's fixture (config-warehouse.json `seed`): each SQL file's statements (one per line; `--` comments
 * skipped) run on the warehouse, a real server's data. In this tab a project's rows are its Data elements' (C5).
 */
async function seed(engine: QueryEngine, files: readonly string[]): Promise<void> {
  for (const f of files) {
    for (const line of (await text(f)).split('\n')) {
      const stmt = line.trim();
      if (stmt && !stmt.startsWith('--')) await engine.run(stmt, 0);
    }
  }
}

/**
 * A warehouse catalog's database type, what a statement run on it is written for (Snap's count): as the warehouse
 * lists it -- or, for its own catalog (`main`) with nothing in it yet (the fixture loads after sign-in), its DuckDB.
 */
async function catalogType(binding: WarehouseBinding, session: WarehouseSession, catalog: string): Promise<string> {
  const listed = (await listObjects(binding, session)).find((o) => o.catalog === catalog)?.databaseType;
  if (listed) return listed;
  if (catalog === 'main') return 'DuckDB';
  throw new Error(`the warehouse lists nothing this user may read in its catalog ${catalog}: its database type is not known`);
}

/** Ask for the warehouse user and password (nothing is stored). */
function askSignIn(root: HTMLElement, url: string): Promise<{ user: string; password: string }> {
  return new Promise((resolve) => {
    const user = h('input', { class: 'q-input', placeholder: 'user', autocomplete: 'username' });
    const password = h('input', { class: 'q-input', type: 'password', placeholder: 'password', autocomplete: 'current-password' });
    const form = h('form', {
      style: 'display:flex; flex-direction:column; gap:8px; width:320px',
      onsubmit: (e: Event) => { e.preventDefault(); resolve({ user: user.value, password: password.value }); },
    }, user, password, h('button', { class: 'q-btn primary', type: 'submit' }, 'Sign in'));
    mount(root, h('div', { style: 'padding:40px' }, h('h2', null, 'Sign in to the warehouse'), h('p', { class: 'q-muted mono' }, url), form));
    user.focus();
  });
}

async function boot(): Promise<void> {
  const root = document.getElementById('app')!;
  const starting = (what: string): void => mount(root, h('div', { style: 'padding:40px; color:var(--text-2)' }, h('span', { class: 'q-spinner' }), ` ${what}…`));
  starting('Starting Legend Query');
  const configFile = new URLSearchParams(location.search).get('config') ?? 'config.json';
  const config = JSON.parse(await text(`./${configFile}`)) as AppConfig;
  // the page's WebAssembly: beside it, or the one copy a site serves for every app (config.json `vendor`)
  const vendor = config.vendor ?? './vendor/';
  const exec = config.execution;
  const planner = config.planner
    ? new WasmGrammar(new WorkerPort(new URL(config.planner.worker, location.href), vendor))
    : undefined;
  const http = exec.kind === 'server' ? new HttpEngine(exec.engine) : undefined;
  if (!planner && !http) throw new Error('queries run in the browser, which needs the planner (config.planner)');
  const grammar: Grammar = planner ?? http!;

  // THE MODEL HOME (docs/ONE_CONNECTION_DESK_2026_10_28.md D5): every project is found there -- the site's released
  // versions (the demo's trading model) opened now, read through its Depot as any version is; the rest listed by name
  // (what Studio publishes), each loaded when opened
  if (!config.depot) throw new Error('Legend Query finds its projects in a model home: config.json names none (`depot`)');
  starting('Opening the model home');
  const home = await connectModelHome({ ...config.depot, vendor });
  const { depot } = home;
  starting('Loading the models');
  const projects: LoadedProject[] = await Promise.all(home.released.map(async (r) => {
    const loaded = await loadByName(depot, grammar, r.groupId, r.artifactId, r.versionId);
    return { ...loaded, config: { ...loaded.config, title: r.title } };
  }));
  const byName: { load: NonNullable<AppContext["byName"]>; versions: NonNullable<AppContext["versions"]> } = {
    load: (g: string, a: string, v: string) => loadByName(depot, grammar, g, a, v),
    versions: (g: string, a: string) => versionsOf(depot, g, a),
  };
  const depotProjects: AppContext["depotProjects"] = (await depot.projects()).map((p) => ({ groupId: p.groupId, artifactId: p.artifactId }));
  /** Where a project's own rows go: its database in this tab's DuckDB, once started (set below; none on the warehouse). */
  let tabProjects: AppContext['tabProjects'];

  let engine: Engine;
  let cubeRows: CubeRows;
  let store: QueryStore;
  let user: string;
  if (exec.kind === 'server') {
    engine = planner ? new RoutedEngine(planner, http!) : http!;
    // the server answers pure/v1 at its root; the config names its /api
    cubeRows = { kind: 'server', baseUrl: exec.engine.replace(/\/api\/?$/, '') };
    store = new QueryStoreClient(exec.engine);
    user = await http!.currentUser();
  } else {
    let runner: QueryEngine;
    if (exec.kind === 'duckdb-wasm') {
      starting('Starting DuckDB in this tab');
      const tab = await startDuckDbInTab(vendor, planner!);
      // a project opened by name brings its own rows, in a database of its own here (C5): the screens run on `runner`,
      // pointed there as it opens (AppContext.activate)
      const projectRunner = new ProjectRunner(tab.engine);
      runner = projectRunner;
      tabProjects = {
        databases: new ProjectDatabases({
          databaseType: tab.engine.databaseType, run: (sql) => tab.engine.run(sql, 0), connect: () => tab.connect(),
        }, planner!),
        runner: projectRunner,
        host: tab.engine,
      };
      user = exec.user;
    } else {
      // the warehouse's API as the one binding speaks it (vendor/warehouse/: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3),
      // fetched while the person types
      const loading = loadWarehouseBinding(new URL(`${vendor}warehouse/`, location.href).href);
      const creds = await askSignIn(root, exec.url);
      starting('Signing in');
      const binding = await loading;
      const session = await signIn(binding, exec.url, creds.user, creds.password);
      const catalog = exec.catalog ?? 'main';
      runner = new WarehouseEngine(binding, session, catalog, await catalogType(binding, session, catalog));
      user = session.principal;
      if (exec.seed?.length) {
        starting('Loading the demo data');
        await seed(runner, exec.seed);
      }
    }
    engine = new BrowserEngine(planner!, runner, user);
    cubeRows = { kind: 'sql', engine: runner };
    // no server: the same API, answered in this page from this origin's IndexedDB by legend-lite's own rules (the
    // server's, compiled to WebAssembly: query-store's module at vendor/queries/, fetched at the store's first call)
    store = new QueryStoreClient(LOCAL_API, wasmQueryServer({
      load: () => loadQueriesModule(new URL(`${vendor}queries/`, location.href).href), records: new BrowserRecords(), user,
    }).fetch);
  }
  const ctx = new AppContext(config, engine, store, planner, projects, user, cubeRows);
  ctx.byName = byName.load;
  ctx.versions = byName.versions;
  ctx.depotProjects = depotProjects;
  ctx.tabProjects = tabProjects;
  // warm the planner on the first model while the person looks at the landing page
  if (planner && projects[0]) void planner.warm(projects[0].context).catch(() => undefined);
  new App(ctx, root).start();
}

boot().catch((e: unknown) => {
  const root = document.getElementById('app')!;
  mount(root, h('div', { style: 'padding:40px' },
    h('h2', null, 'Legend Query could not start'),
    h('div', { class: 'q-error-box' }, e instanceof Error ? e.message : String(e))));
});
