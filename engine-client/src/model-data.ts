// A model's tables in the tab's DuckDB (plan A2): which tables a model's Databases declare and which of them its Data
// elements fill (their rows are the project's database's: project-data.ts, the plan's ONCE load), and a person's own
// file put into a declared table -- a planned load (B3, 2026-11-11: LoadPlanner.hostFile, the file a pointer the tab's
// store binds), run by the hosts' runner (runLoad) in one transaction. Until then the server's seed statements were
// handed over as text (testDataSql) and the file named by a host's operation, run outside any plan (census M14).

import type { HostOperationsSource } from './host-operations.ts';
import { runLoad, sha256Hex, type PlanValue } from './plan-runner.ts';
import type { PlannedSql } from './planned-sql.ts';

/** The model JSON parts read here (grammarToJson/model). */
interface ModelElement {
  readonly _type: string;
  readonly name?: string;
  readonly package?: string;
  readonly schemas?: readonly {
    readonly name: string;
    readonly tables: readonly { readonly name: string; readonly columns: readonly { readonly name: string }[] }[];
  }[];
  readonly data?: { readonly _type: string; readonly tables?: readonly { readonly schema: string; readonly table: string; readonly values: string }[] };
}

/** A table a model's Database declares: where it is, and its columns' names. */
export interface DeclaredTable {
  /** The Database's path. */
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string }[];
}

/** One table of test data: the Database that declares it, where it goes, its CSV (header first), the Data element. */
export interface DataTable {
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly csv: string;
  readonly element: string;
}

/**
 * A host's own file read into its database, as the planner is asked for its plan (Boundary.fileLoad): the file by the
 * name the host keeps it under, its bytes' SHA-256 and size and its format; a table of its own, or a table the model's
 * Database declares.
 */
export interface FileLoadRequest {
  readonly databaseType: string;
  readonly file: { readonly name: string; readonly digest: string; readonly size: number; readonly format: 'csv' | 'parquet' | 'json' };
  readonly into: { readonly table: string } | { readonly model: string; readonly database: string; readonly schema: string; readonly table: string };
}

/** Where a host's file's plan comes from: legend-lite's planner (WasmGrammar.fileLoad, DataCube's LitePlanner). */
export interface FileLoadSource {
  /** The plan, PlanJson's text: one HOST_SESSION load the hosts' runner runs (runLoad). */
  fileLoad(request: FileLoadRequest): Promise<string>;
}

/** Every table the model's Databases declare (its JSON's elements), in the order declared. */
export function databaseTables(elements: readonly ModelElement[]): DeclaredTable[] {
  const out: DeclaredTable[] = [];
  for (const e of elements) {
    if (e._type !== 'relational') continue;
    for (const s of e.schemas ?? []) {
      for (const t of s.tables) {
        out.push({ database: `${e.package}::${e.name}`, schema: s.name, table: t.name, columns: t.columns.map((c) => ({ name: c.name })) });
      }
    }
  }
  return out;
}

/**
 * The one Database of the model that declares `schema.table` (its JSON's elements). None, or more than one, is refused:
 * which one would be a guess (the server is told, by the test that names its store; the tab has no test to ask).
 * `what` names whose table it is, for the refusal.
 */
export function databaseOf(elements: readonly ModelElement[], schema: string, table: string, what: string): string {
  const dbs = databaseTables(elements).filter((t) => t.schema === schema && t.table === table).map((t) => t.database);
  if (dbs.length === 0) throw new Error(`${what} ${schema}.${table}, which no Database of the model declares`);
  if (dbs.length > 1) throw new Error(`${what} ${schema}.${table}, which more than one Database declares (${dbs.join(', ')})`);
  return dbs[0]!;
}

/** The test data of a model (its JSON's elements): one entry per table of every relational Data element (databaseOf). */
export function dataTables(elements: readonly ModelElement[]): DataTable[] {
  const out: DataTable[] = [];
  for (const e of elements) {
    if (e._type !== 'dataElement' || e.data?._type !== 'relationalCSVData') continue;
    const element = `${e.package}::${e.name}`;
    for (const t of e.data.tables ?? []) {
      const database = databaseOf(elements, t.schema, t.table, `the Data element ${element} fills`);
      out.push({ database, schema: t.schema, table: t.table, csv: t.values, element });
    }
  }
  return out;
}

/**
 * Where a person's own file goes in (the tab's DuckDB): its bytes registered under a name (DuckDB-WASM takes the buffer
 * it is given -- it hands it to its worker: the view is empty after -- so the caller's bytes are copied before they go
 * in), let go of after (`dropFile`, where it can), and the connection held for the put's statements, one transaction.
 */
export interface FileSink {
  registerFileBuffer(name: string, bytes: Uint8Array): Promise<void>;
  dropFile?(name: string): Promise<void>;
  /** The connection held for `work`: each statement run with its bound values, no other query between them. */
  hold<T>(work: (run: (sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>) => Promise<T>): Promise<T>;
}

/**
 * A table made from a person's own file (plan A2's second source), as a PLANNED LOAD (B3, 2026-11-11): the planner's
 * plan (fileLoad) makes the table as the model declares it, empty, then puts the file's rows in -- each declared column
 * from the file's column of that name, a CSV's read as text and a Parquet's as typed, DuckDB converting each value to its
 * column's type -- and the hosts' runner runs it in one transaction, so a refused file (a column missing, a value its type
 * cannot take) leaves the table as it was. The file's name says which kind it is. Its bytes are registered as `<as>.csv`
 * or `<as>.parquet`, never by the person's own name (read_csv and read_parquet read a name as a glob: `sales[2024].csv`
 * matches nothing), and the plan names them by that name, their SHA-256 and size: the statement's ? is bound to them.
 */
export async function loadFileTable(sink: FileSink, source: FileLoadSource & HostOperationsSource, databaseType: string, model: string,
  target: { readonly database: string; readonly schema: string; readonly table: string },
  file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<void> {
  const format = /\.csv$/i.test(file.name) ? 'csv' : /\.parquet$/i.test(file.name) ? 'parquet' : undefined;
  if (!format) throw new Error(`${file.name}: a table is filled from a .csv or a .parquet file`);
  const registered = `${as}.${format}`;
  const digest = await sha256Hex(file.bytes);
  const plan = await source.fileLoad({
    databaseType, file: { name: registered, digest, size: file.bytes.length, format },
    into: { model, database: target.database, schema: target.schema, table: target.table },
  });
  const [begin, commit, rollback] = await source.hostOperations(databaseType,
    (['begin', 'commit', 'rollback'] as const).map((step) => ({ op: 'transaction' as const, step })));
  // a copy: the database takes the buffer it is given, and the caller keeps its bytes (Studio puts them in again)
  await sink.registerFileBuffer(registered, file.bytes.slice());
  // the tab's store of its own files: the one registered for this put, by its name, its bytes the plan's
  const files = {
    located: async (ref: { readonly name: string; readonly digest: string; readonly size: number }): Promise<string> => {
      if (ref.name !== registered || ref.digest !== digest || ref.size !== file.bytes.length) {
        throw new Error(`the file '${ref.name}' is not the one this put registered (${registered}, ${file.bytes.length} bytes of digest ${digest})`);
      }
      return registered;
    },
  };
  try {
    await sink.hold(async (run) => {
      for (const sql of begin!) await run(sql, []);
      try {
        await runLoad(plan, run, { files });
        for (const sql of commit!) await run(sql, []);
      } catch (e) {
        try {
          for (const sql of rollback!) await run(sql, []);
        } catch (r) {
          throw new Error(`${e instanceof Error ? e.message : String(e)} -- and putting the table back failed: ${r instanceof Error ? r.message : String(r)}`);
        }
        throw e;
      }
    });
  } finally {
    // the rows are the table's now (or the table is as it was): the file is not read again -- and a drop that fails
    // neither fails a put that worked nor hides why a read did not (at worst the file stays registered)
    await sink.dropFile?.(registered).catch(() => undefined);
  }
}
