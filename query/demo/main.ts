// The page: read the config (`?config=<file>`, default config.json), start legend-lite's planner
// in a worker, load each project's model, and connect where queries run:
//
//  - duckdb-wasm: DuckDB in this tab, seeded from SQL files -- no server at all;
//  - warehouse:   the warehouse's DuckDB, signed in as a user (DataCube's Live plane);
//  - server:      legend-lite's server (or legend-engine), which executes and keeps saved queries.
//
// In the browser planes the tab's planner writes the SQL and one of DataCube's engines runs it,
// and saved queries stay in this browser (the same records and rules as a server's store).

import * as duckdb from '../../engine-client/src/duckdb-wasm.ts';
import { DuckDbEngine, type ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { signIn, WarehouseEngine } from '../../engine-client/src/warehouse.ts';
import { AppContext, gavOf, type AppConfig, type CubeRows, type LoadedProject } from '../src/app/context.ts';
import { App } from '../src/app/app.ts';
import { BrowserEngine } from '../src/backend/browser-engine.ts';
import { HttpEngine, RoutedEngine, type Engine, type Grammar, type QueryStore } from '../src/backend/engine.ts';
import { BrowserRecords, LOCAL_API, localQueryServer, QueryStoreClient } from '../../query-store/src/index.ts';
import { WasmGrammar, WorkerPort } from '../src/backend/wasm-grammar.ts';
import { ModelGraph } from '../src/model/graph.ts';
import { h, mount } from '../src/ui/dom.ts';

async function text(url: string): Promise<string> {
  const res = await fetch(url, { cache: 'no-cache' });
  if (!res.ok) throw new Error(`could not load ${url}: ${res.status}`);
  return res.text();
}

/** DuckDB in this tab, from the files `//query:vendor` copies next to the page. */
async function startDuckDb(): Promise<QueryEngine> {
  const asset = (f: string): string => new URL(`./vendor/${f}`, location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: { mainModule: asset('duckdb-mvp.wasm'), mainWorker: asset('duckdb-browser-mvp.worker.js') },
    eh: { mainModule: asset('duckdb-eh.wasm'), mainWorker: asset('duckdb-browser-eh.worker.js') },
  });
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), new Worker(bundle.mainWorker!));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return new DuckDbEngine((await db.connect()) as unknown as ArrowishConnection);
}

/** Run each SQL file's statements (one per line; `--` comments skipped) on the engine. */
async function seed(engine: QueryEngine, files: readonly string[]): Promise<void> {
  for (const f of files) {
    for (const line of (await text(f)).split('\n')) {
      const stmt = line.trim();
      if (stmt && !stmt.startsWith('--')) await engine.run(stmt, 0);
    }
  }
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
  const exec = config.execution;
  const planner = config.planner
    ? new WasmGrammar(new WorkerPort(new URL(config.planner.worker, location.href), config.planner.vendor))
    : undefined;
  const http = exec.kind === 'server' ? new HttpEngine(exec.engine) : undefined;
  if (!planner && !http) throw new Error('queries run in the browser, which needs the planner (config.planner)');
  const grammar: Grammar = planner ?? http!;

  starting('Loading the models');
  const projects: LoadedProject[] = await Promise.all(config.projects.map(async (p) => {
    const code = (await Promise.all(p.models.map(text))).join('\n');
    return { config: p, gav: gavOf(p), context: { _type: 'text', code }, graph: new ModelGraph(await grammar.modelJson(code)) };
  }));
  const isEnumeration = (t: string): boolean => projects.some((p) => p.graph.enumerations.has(t));

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
      runner = await startDuckDb();
      user = exec.user;
    } else {
      const creds = await askSignIn(root, exec.url);
      starting('Signing in');
      const session = await signIn(exec.url, creds.user, creds.password);
      runner = new WarehouseEngine(session, exec.catalog ?? 'main');
      user = session.principal;
    }
    if (exec.seed?.length) {
      starting('Loading the demo data');
      await seed(runner, exec.seed);
    }
    engine = new BrowserEngine(planner!, runner, isEnumeration, user);
    cubeRows = { kind: 'sql', engine: runner };
    // no server: the same API, answered in this page from this origin's IndexedDB (query-store)
    store = new QueryStoreClient(LOCAL_API, localQueryServer({ records: new BrowserRecords(), user }).fetch);
  }
  const ctx = new AppContext(config, engine, store, planner, projects, user, cubeRows);
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
