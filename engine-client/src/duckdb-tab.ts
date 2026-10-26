// DuckDB in this tab, THE one start of it for every app that runs queries here (DataCube, Query, Studio, and the test
// pages that stand in for them): its bundle from the app's own vendor/ (never a CDN: a cross-origin worker is
// forbidden, and a deployment behind a firewall has none), and its extensions from there too (ownExtensions), one
// connection -- the query engine, its session set up as the planner's SQL assumes before anything uses it (engine.ts
// setUpSession) -- and the database itself, into which a model's test data is loaded (model-data.ts).

import * as duckdb from './duckdb-wasm.ts';
import { DuckDbEngine, type ArrowishConnection } from './duckdb.ts';
import { setUpSession } from './engine.ts';
import type { FileSink } from './model-data.ts';
import { runHostOperations, type HostOperationsSource } from './host-operations.ts';

export interface DuckDbInTab {
  readonly engine: DuckDbEngine;
  /** Where a model's test data and a person's files go: a file by name (text or bytes), and SQL on the engine's connection. */
  readonly data: FileSink;
  /** The database itself: what a host registers a person's file on, and lets go of it (DataCube's upload.ts). */
  readonly db: duckdb.AsyncDuckDB;
  /**
   * Another connection to the same database, set up as the first (its extensions from the site, its session the
   * planner's): what a project's own database is queried on (project-data.ts), its names pointed there alone.
   * `whole`: one that reads each result in one round trip (duckdb.ts DuckDbEngine's constructor: the live engine's).
   */
  connect(options?: { readonly whole?: boolean }): Promise<DuckDbEngine>;
}

/** What the tab's DuckDB is set up by: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface TabDuckDbPlanner extends HostOperationsSource {
  /** The planner's session setup for a database's type (Boundary.sessionSetup: a DuckDB session in UTC). */
  sessionSetup(databaseType: string): Promise<readonly string[]>;
}

/**
 * The tab's DuckDB loads its extensions from `repository` -- the site's own (vendor/duckdb-extensions/, beside its
 * DuckDB files: //engine-client:duckdb_extensions, pinned in MODULE.bazel), in the layout DuckDB asks a repository for
 * -- never from extensions.duckdb.org; the statements the planner's (a host operation, D5). Before anything loads one:
 * the session's time zone loads ICU (setUpSession).
 */
async function ownExtensions(engine: DuckDbEngine, planner: HostOperationsSource, repository: string): Promise<void> {
  await runHostOperations(planner, engine, [{ op: 'extensionRepository', url: repository }]);
}

/**
 * `vendor`: the URL of the folder holding duckdb-eh.wasm and its worker. `planner`: what sets up each
 * connection before it is handed out -- the extension folder, then the session (WasmGrammar's sessionSetup).
 */
export async function startDuckDbInTab(vendor: string, planner: TabDuckDbPlanner): Promise<DuckDbInTab> {
  // absolute URLs: the worker resolves its module against its own location, not the page's
  const asset = (f: string): string => new URL(f, new URL(vendor, globalThis.location.href)).href;
  // ONE BUILD, DuckDB's with WebAssembly exception handling (eh): every browser the apps support has had it since
  // 2021-22, and the build without it (mvp) doubled what a site ships. A browser without it is refused by name, never
  // handed a build it cannot run
  if (!(await duckdb.getPlatformFeatures()).wasmExceptions) {
    throw new Error('This browser cannot run DuckDB in the page: it lacks WebAssembly exception handling. Use a current Chrome, Edge, Firefox or Safari.');
  }
  const bundle = { mainModule: asset('duckdb-eh.wasm'), mainWorker: asset('duckdb-browser-eh.worker.js'), pthreadWorker: null };
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), new Worker(bundle.mainWorker!));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  // every connection set up alike: a setting of DuckDB's may be a session's, so none is left to the first's
  const connect = async (options: { readonly whole?: boolean } = {}): Promise<DuckDbEngine> => {
    const connection = new DuckDbEngine((await db.connect()) as unknown as ArrowishConnection, options);
    await ownExtensions(connection, planner, asset('duckdb-extensions'));
    await setUpSession(connection, (databaseType) => planner.sessionSetup(databaseType));
    return connection;
  };
  const engine = await connect();
  return {
    engine,
    data: {
      registerFileBuffer: (name, bytes) => db.registerFileBuffer(name, bytes),
      dropFile: async (name) => { await db.dropFile(name); },
      run: (sql) => engine.run(sql, 0),
    },
    db,
    connect,
  };
}
