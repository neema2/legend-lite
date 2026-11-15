// DuckDB in this tab, THE one start of it for every app that runs queries here (DataCube, Query, Studio, and the test
// pages that stand in for them): its bundle from the app's own vendor/ (never a CDN: a cross-origin worker is
// forbidden, and a deployment behind a firewall has none), and its extensions from there too, one connection -- the
// query engine, its session set up as the planner's SQL assumes before anything uses it (engine.ts setUpSession) -- and
// the database itself, into which a model's test data is loaded (model-data.ts). The setup is the planner's answer,
// asked by the start's door (doors/tab-duckdb.ts, C5-T, 2026-11-15): this module runs it and asks nothing.

import * as duckdb from './duckdb-wasm.ts';
import { DuckDbEngine, type ArrowishConnection } from './duckdb.ts';
import { setUpSession } from './engine.ts';
import type { FileSink } from './model-data.ts';
import { ConnectionSettings } from './connection-settings.ts';
import type { PlannedSql } from './planned-sql.ts';

/**
 * A tab's file registry: a file's text or bytes registered by name, and let go of -- and nothing else of the database,
 * whose connections run any text. A class with a private field, so its type is its own (the C final audit's H11,
 * 2026-11-12: as an interface, the raw AsyncDuckDB satisfied it, and `db: db` handed the database out again with every
 * check green); test/planned-sql.test.ts holds the database itself to be no TabFiles.
 */
export class TabFiles {
  readonly #db: Pick<duckdb.AsyncDuckDB, 'registerFileText' | 'registerFileBuffer' | 'dropFile'>;

  constructor(db: Pick<duckdb.AsyncDuckDB, 'registerFileText' | 'registerFileBuffer' | 'dropFile'>) {
    this.#db = db;
  }

  registerFileText(name: string, text: string): Promise<void> {
    return this.#db.registerFileText(name, text);
  }

  registerFileBuffer(name: string, buffer: Uint8Array): Promise<void> {
    return this.#db.registerFileBuffer(name, buffer);
  }

  async dropFile(name: string): Promise<void> {
    await this.#db.dropFile(name);
  }
}

export interface DuckDbInTab {
  readonly engine: DuckDbEngine;
  /** Where a person's files go: a file's bytes registered by name, and let go of (a put's statements are its planned
   *  load's, on the connection that holds them: model-data.ts putFile). */
  readonly data: Pick<FileSink, 'registerFileBuffer' | 'dropFile'>;
  /** Where a host registers a person's file, and lets go of it (DataCube's upload.ts, its DuckDbFiles): the database's
   *  file registry alone -- never the database itself, whose connections run any text (2026-11-12, the C checkpoint's
   *  second audit: the raw AsyncDuckDB was handed out, its `connect().query(string)` outside the PlannedSql brand). */
  readonly db: TabFiles;
  /**
   * Another connection to the same database, set up as the first (its extensions from the site, its session the
   * planner's): what a project's own database is queried on (project-data.ts), its names pointed there alone.
   */
  connect(): Promise<DuckDbEngine>;
  /** The settings a remote file's mount made on a connection (a bearer URL's session variable: B3's Q3), kept for the
   *  tab's database and replayed on every connection `connect` opens. */
  readonly settings: ConnectionSettings;
}

/**
 * A TAB'S DUCKDB'S SETUP, as its planner answered it (C5-T, 2026-11-15: the door asks, doors/tab-duckdb.ts; until then
 * this module asked the planner itself, on every connection): run on every connection before it is handed out.
 * `extensions`: where it loads its extensions from -- the site's own (vendor/duckdb-extensions/, beside its DuckDB
 * files: //engine-client:duckdb_extensions, pinned in MODULE.bazel; `extensionRepository`), in the layout DuckDB asks a
 * repository for, never extensions.duckdb.org -- the planner's statements for that host operation (D5). `session`: the
 * session the planner's SQL assumes (Boundary.sessionSetup: a DuckDB session in UTC, which loads ICU), after them.
 */
export interface TabSetup {
  readonly extensions: readonly PlannedSql[];
  readonly session: readonly PlannedSql[];
}

/** The tab's DuckDB's database type, as the planner names it (duckdb.ts DuckDbEngine's). */
export const TAB_DATABASE_TYPE = 'DuckDB';

/** A file of the vendor folder `vendor` names, as an absolute URL: the worker resolves its module against its own
 *  location, not the page's. */
function vendorAsset(vendor: string, f: string): string {
  return new URL(f, new URL(vendor, globalThis.location.href)).href;
}

/** Where the tab's DuckDB loads its extensions from: the vendor folder's own (the door plans its host operation). */
export function extensionRepository(vendor: string): string {
  return vendorAsset(vendor, 'duckdb-extensions');
}

/**
 * `vendor`: the URL of the folder holding duckdb-eh.wasm and its worker. `setup`: what each connection runs before it
 * is handed out, as the planner answered it -- the extension folder, then the session.
 */
export async function startDuckDbInTab(vendor: string, setup: TabSetup): Promise<DuckDbInTab> {
  const asset = (f: string): string => vendorAsset(vendor, f);
  // ONE BUILD, DuckDB's with WebAssembly exception handling (eh): every browser the apps support has had it since
  // 2021-22, and the build without it (mvp) doubled what a site ships. A browser without it is refused by name, never
  // handed a build it cannot run
  if (!(await duckdb.getPlatformFeatures()).wasmExceptions) {
    throw new Error('This browser cannot run DuckDB in the page: it lacks WebAssembly exception handling. Use a current Chrome, Edge, Firefox or Safari.');
  }
  const bundle = { mainModule: asset('duckdb-eh.wasm'), mainWorker: asset('duckdb-browser-eh.worker.js'), pthreadWorker: null };
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), new Worker(bundle.mainWorker!));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  // every connection set up alike: a setting of DuckDB's may be a session's, so none is left to the first's -- a remote
  // file's session variable included, replayed from the tab's settings (B3's Q3: a variable is one connection's)
  const settings = new ConnectionSettings();
  const connect = async (): Promise<DuckDbEngine> => {
    const connection = new DuckDbEngine((await db.connect()) as unknown as ArrowishConnection, { settings });
    for (const statement of setup.extensions) await connection.run(statement, 0);
    await setUpSession(connection, setup.session);
    await settings.replay((sql, params) => connection.exclusive((held) => held.rows(sql, params)));
    return connection;
  };
  const engine = await connect();
  return {
    engine,
    data: {
      registerFileBuffer: (name, bytes) => db.registerFileBuffer(name, bytes),
      dropFile: async (name) => { await db.dropFile(name); },
    },
    db: new TabFiles(db),
    connect,
    settings,
  };
}
