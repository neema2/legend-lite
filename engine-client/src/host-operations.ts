// A HOST'S NAMED OPERATIONS on its own database (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5): what the tab used to
// spell as SQL itself -- a file read into a table, a remote file mounted, a copy made, rows counted, an extension, a
// secret -- named here, with its data, and answered by legend-lite's planner (Boundary.hostOperations: the platform's
// HostOperations in Java, by the database's type) as the statements the database runs. The page spells no SQL; a
// database without an operation refuses it by name. Python's host writes the same JSON (compiler.host_operations).

/** A relation's name. `declared`: a model's own table, spelled as its seed made it; any other name is quoted. */
export interface HostName {
  readonly database?: string;
  readonly schema?: string;
  readonly name: string;
  readonly declared?: boolean;
}

/** Where a relation's rows come from. */
export type HostSource =
  | { readonly relation: HostName }
  /** A file the database reads by a registered name or a path: sniffed CSV with a header, CSV as text (a declared
   *  table's file), Parquet, or JSON (each record's keys its columns). */
  | { readonly file: { readonly at: string; readonly format: 'csv' | 'csvText' | 'parquet' | 'json' } }
  | { readonly remote: { readonly url: string; readonly format: 'csv' | 'parquet' | 'iceberg' } }
  /** A query a planner wrote, for this database. */
  | { readonly query: string };

export type HostOperation =
  /** A relation made: replacing one of its name or not; its columns the planner's copy select list, or every column
   *  with `replacing`'s replaced (the planner's expressions), or every column. */
  | {
    readonly op: 'relation'; readonly kind: 'table' | 'view'; readonly target: HostName; readonly replace?: boolean;
    readonly selectList?: string; readonly replacing?: readonly { readonly column: string; readonly sql: string }[];
    readonly from: HostSource;
  }
  /** Rows put into a declared table, each declared column from the source's column of its name. */
  | { readonly op: 'insert'; readonly target: HostName; readonly columns: readonly { readonly target: string; readonly from: string }[]; readonly from: HostSource }
  /** Rows put into a keyed table from a table of the database, each the latest of its key: a row of the key there
   *  replaced, and of the source's rows of one key its last (the live DuckDB's upsert: duckdb-live.ts). */
  | {
    readonly op: 'upsert'; readonly target: HostName; readonly columns: readonly { readonly target: string; readonly from: string }[];
    readonly key: readonly string[]; readonly from: { readonly relation: HostName };
  }
  /** A stream's rows past its bound deleted: older, by `column`, than its latest `rows`, or than its latest less `millis`. */
  | { readonly op: 'bound'; readonly target: HostName; readonly column: string; readonly rows: number }
  | { readonly op: 'bound'; readonly target: HostName; readonly column: string; readonly millis: number }
  | { readonly op: 'drop'; readonly kind: 'table' | 'view'; readonly target: HostName }
  | { readonly op: 'schema'; readonly database?: string; readonly name: string }
  | { readonly op: 'dropSchema'; readonly database?: string; readonly name: string }
  /** How many rows: one row, one column `n`. */
  | { readonly op: 'count'; readonly from: HostSource }
  /** A secret for S3 (KEY_ID, SECRET, SESSION_TOKEN, REGION, ENDPOINT, URL_STYLE, USE_SSL), scoped to a path. */
  | { readonly op: 'secret'; readonly name: string; readonly scope: string; readonly credentials: Readonly<Record<string, string>> }
  | { readonly op: 'extensions'; readonly names: readonly string[] }
  /** Where the database installs extensions from, on demand (the site's folder). */
  | { readonly op: 'extensionRepository'; readonly url: string }
  | { readonly op: 'transaction'; readonly step: 'begin' | 'commit' | 'rollback' };

/** What answers a host's operations: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface HostOperationsSource {
  /** Each operation's statements, in order, for a database of `databaseType`. */
  hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<string[][]>;
}

/** Each of `operations`' statements run on `engine`, in order: the operations' whole step. */
export async function runHostOperations(source: HostOperationsSource,
  engine: { readonly databaseType: string; run(sql: string, epoch: number): Promise<unknown> },
  operations: readonly HostOperation[], epoch = 0): Promise<void> {
  for (const statements of await source.hostOperations(engine.databaseType, operations)) {
    for (const sql of statements) await engine.run(sql, epoch);
  }
}
