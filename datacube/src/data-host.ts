// THE HOST'S DATABASE AS DATACUBE'S DATA FLOWS ASK OF IT (the #6 bridge, the user's go of 2026-11-12, "why don't we
// start fixing the python right now"): INTENT IN, ANSWERS OUT. A file read into a table, a remote file mounted, a table
// dropped, rewritten or counted, a table's catalog read -- each asked for as what it is (the planner's request, a host
// operation by name, a table by name), planned by the host's planner and run by the host's runner where its database
// is, the answer handed back. DataCube's code decides (which file, which table, which operations, in what order: upload.ts,
// remote.ts, table-output.ts); it never hands a host a statement or a plan.
//
// Two hosts implement it: the tab's (heldDataHost: its own planner plans, its DuckDB-WASM runs, as before) and
// Python's (headless/host.ts hostData: each ask one call of the host's, planned and run in Python -- until then DataCube's
// code planned in V8 and handed Python each statement, which Python admitted by the statements its planner had answered).

import type { RawTable } from '../../engine-client/src/engine.ts';
import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import { SecretStore, type HostOperation, type RemoteMountRequest, type SecretField, type SecretValue } from '../../engine-client/src/host-operations.ts';
import type { FileLoadRequest } from '../../engine-client/src/model-data.ts';
import type { ConnectionSettings, PlanValue } from '../../engine-client/src/plan-runner.ts';
import type { PlannedSql } from '../../engine-client/src/planned-sql.ts';
import type { CatalogColumn, TableModels } from './infer.ts';
import { catalogColumnsRead, columnOf } from './upload.ts';

/** What a host is asked to load, as its planner is asked for the plan: a file it holds read into a table of its own
 *  (`fileLoad`), or a remote file's access or its view (`remoteMount`). */
export type LoadRequest = { readonly fileLoad: FileLoadRequest } | { readonly remoteMount: RemoteMountRequest };

/** A file the host holds, as a load's plan names it (its name, size and SHA-256), and the HANDLE the host issued for it
 *  (the orchestrator's ruling of 2026-11-12, the path gap closed structurally): the host's own name for the file, which
 *  only the host resolves to what its database reads -- the tab's, the name DuckDB-WASM registered it under; Python's, a
 *  name in its registry of the files it handed out, never a path DataCube's code sends back. */
export interface HeldFile {
  readonly name: string;
  readonly digest: string;
  readonly size: number;
  readonly handle: string;
}

/** What a load binds by name, never written into its plan: the files it reads, and the keys and bearer URLs it is
 *  given (each by the name the plan's held binds name, its fields). */
export interface LoadHolds {
  readonly files?: readonly HeldFile[];
  readonly secrets?: Readonly<Record<string, Readonly<Partial<Record<SecretField, SecretValue>>>>>;
}

/** The rows a statement answered, each its values by column. */
export type Rows = readonly (readonly unknown[])[];

/** The host's database as DataCube's data flows ask of it (above). */
export interface DataHost {
  /** The database's type, as the planner names it (`DuckDB`): what a model of its tables declares. */
  readonly databaseType: string;
  /** `request` planned and run on the host, binding what `holds` names. A mount's settings of the connection (a bearer
   *  URL's session variable) are the host's to keep, by its view, and replay on every connection it opens. */
  load(request: LoadRequest, holds?: LoadHolds): Promise<void>;
  /** `operations` planned and run in order: each one's last statement's rows. */
  operations(operations: readonly HostOperation[]): Promise<readonly Rows[]>;
  /** A table's columns as the host's catalog says them. */
  catalog(schema: string, table: string): Promise<CatalogColumn[]>;
  /** A mount's kept settings let go of (its view dropped): no connection opened after replays them. */
  forget(view: string): void;
}

/** The host's store of `holds`' keys and bearer URLs, as the hosts' runner binds a plan's held binds from one. */
export function secretStore(secrets: NonNullable<LoadHolds['secrets']>): SecretStore {
  const store = new SecretStore();
  for (const [name, fields] of Object.entries(secrets)) {
    store.hold(name, new Map(Object.entries(fields) as [SecretField, SecretValue][]));
  }
  return store;
}

/** The tab's files, as the hosts' runner locates the one a plan names: by its name, size and digest, else refused by
 *  name (Python's runner says the same: projects.py `located`) -- its handle, the name DuckDB-WASM reads it by. */
export function heldFiles(files: readonly HeldFile[]): { located(ref: { readonly name: string; readonly digest: string; readonly size: number }): Promise<string> } {
  return {
    located: async (ref) => {
      const f = files.find((h) => h.name === ref.name && h.digest === ref.digest && h.size === ref.size);
      if (f === undefined) throw new Error(`the file '${ref.name}' (${ref.size} bytes of digest ${ref.digest}) is not one this load holds`);
      return f.handle;
    },
  };
}

/** A RawTable's rows, each its values by column. */
export function rawRows(t: RawTable): Rows {
  return Array.from({ length: t.rowCount }, (_, r) => t.columns.map((c) => c.values[r]));
}

/** What a database run in this process gives the tab's host: its type, its queue's run, its connection held for a
 *  statement that binds (DuckDbEngine.exclusive: a load's, which one without it cannot run), and where it keeps a
 *  mount's settings, if anywhere. */
export interface HeldDatabase {
  readonly databaseType: string;
  run(sql: PlannedSql, epoch: number): Promise<RawTable>;
  exclusive?<T>(work: (held: Pick<HeldConnection, 'rows'>) => Promise<T>): Promise<T>;
  readonly settings?: ConnectionSettings | undefined;
}

/**
 * THE TAB'S HOST (and any whose database runs in this process): its planner plans each ask, its database runs it -- the
 * hosts' runner for a load (runLoad: a file's on the connection held for the whole load, a mount's statement by
 * statement, one that binds on the connection held), each operation's statements in order, the catalog's question.
 */
export function heldDataHost(database: HeldDatabase,
  planner: Pick<TableModels, 'fileLoad' | 'remoteMount' | 'hostOperations' | 'catalogColumnsSql'>): DataHost {
  return {
    databaseType: database.databaseType,
    async load(request, holds = {}) {
      const exclusive = database.exclusive?.bind(database);
      if (exclusive === undefined) throw new Error(`this ${database.databaseType} database holds no connection for a load`);
      const { runLoad } = await import('../../engine-client/src/plan-runner.ts');
      const files = holds.files === undefined ? undefined : heldFiles(holds.files);
      const secrets = holds.secrets === undefined ? undefined : secretStore(holds.secrets);
      if ('fileLoad' in request) {
        const plan = await planner.fileLoad(request.fileLoad);
        await exclusive((held) => runLoad(plan, (sql, params) => held.rows(sql, params), { ...(files ? { files } : {}), ...(secrets ? { secrets } : {}) }));
        return;
      }
      const plan = await planner.remoteMount(request.remoteMount);
      // a statement that binds nothing runs as every query runs (DuckDbEngine.run: B3.1's fix, the demo's cube over a
      // remote Parquet); one that binds, on the held connection
      const run = (sql: PlannedSql, params: readonly PlanValue[]): Promise<unknown> =>
        (params.length === 0 ? database.run(sql, 0) : exclusive((held) => held.rows(sql, params)));
      const { settings } = await runLoad(plan, run, { ...(files ? { files } : {}), ...(secrets ? { secrets } : {}) });
      const view = request.remoteMount.view;
      if (view !== undefined) database.settings?.keep(view, settings, secrets ?? new SecretStore());
    },
    async operations(operations) {
      const out: Rows[] = [];
      for (const statements of await planner.hostOperations(database.databaseType, operations)) {
        let last: Rows = [];
        for (const sql of statements) last = rawRows(await database.run(sql, 0));
        out.push(last);
      }
      return out;
    },
    async catalog(schema, table) {
      const answer = await database.run(await planner.catalogColumnsSql(schema, table), 0);
      return catalogColumnsRead((name) => columnOf(answer, name));
    },
    forget(view) {
      database.settings?.forget(view);
    },
  };
}
