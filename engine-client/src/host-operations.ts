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
  // a person's own file is read by a planned load (B3: the planner's fileLoad, run by plan-runner.ts runLoad), never by
  // an operation naming it
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
  | { readonly op: 'drop'; readonly kind: 'table' | 'view'; readonly target: HostName }
  | { readonly op: 'schema'; readonly database?: string; readonly name: string }
  | { readonly op: 'dropSchema'; readonly database?: string; readonly name: string }
  /** How many rows: one row, one column `n`. */
  | { readonly op: 'count'; readonly from: HostSource }
  /** A secret for S3, scoped to a path, BY NAME (B3, 2026-11-11: credentials by name): which of its fields the host
   *  holds, never their values -- its one statement binds each field's value, then the scope (`secretBinds`), from the
   *  host's own store (`SecretStore`). */
  | { readonly op: 'secret'; readonly name: string; readonly scope: string; readonly fields: readonly SecretField[] }
  /** A secret dropped, when it is there. */
  | { readonly op: 'dropSecret'; readonly name: string }
  | { readonly op: 'extensions'; readonly names: readonly string[] }
  /** Where the database installs extensions from, on demand (the site's folder). */
  | { readonly op: 'extensionRepository'; readonly url: string }
  | { readonly op: 'transaction'; readonly step: 'begin' | 'commit' | 'rollback' };

/** An S3 secret's fields, as DuckDB's CREATE SECRET names them (the platform's DuckDbHostOperations.CREDENTIALS). */
export type SecretField = 'KEY_ID' | 'SECRET' | 'SESSION_TOKEN' | 'REGION' | 'ENDPOINT' | 'URL_STYLE' | 'USE_SSL';

/** A secret's value: a text, or USE_SSL's boolean. */
export type SecretValue = string | boolean;

/** What a secret operation's statement binds, in order (HostOperation.Secret.binds in Java): each field's value, then
 *  the scope. */
export function secretBinds(op: Extract<HostOperation, { op: 'secret' }>): readonly string[] {
  return [...op.fields, 'SCOPE'];
}

/**
 * THE HOST'S SECRETS, BY NAME (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "credentials by name ...
 * a guard keeps any secret value out of the plan"): the values a person gave a source, held in this host's memory under
 * the secret's name and bound into its statement -- never written into an operation or a statement's text. A secret
 * this store does not hold, or a field it holds no value for, is refused by name, the values never named.
 */
export class SecretStore {
  readonly #held = new Map<string, ReadonlyMap<SecretField, SecretValue>>();

  /** `values` held under `name`, replacing what it held. */
  hold(name: string, values: ReadonlyMap<SecretField, SecretValue>): void {
    this.#held.set(name, new Map(values));
  }

  /** What `op`'s statement binds (secretBinds' order): each field's value from this store, then the scope. */
  bound(op: Extract<HostOperation, { op: 'secret' }>): SecretValue[] {
    const values = this.#held.get(op.name);
    if (values === undefined) throw new Error(`the secret '${op.name}' is not one this host holds`);
    return [...op.fields.map((field) => {
      const value = values.get(field);
      if (value === undefined) throw new Error(`the secret '${op.name}' holds no ${field}`);
      return value;
    }), op.scope];
  }
}

/** What answers a host's operations: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface HostOperationsSource {
  /** Each operation's statements, in order, for a database of `databaseType`. */
  hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<string[][]>;
}

/** Each of `operations`' statements run on `engine`, in order: the operations' whole step. */
export async function runHostOperations(source: HostOperationsSource,
  engine: { readonly databaseType: string; run(sql: string, epoch: number): Promise<unknown> },
  operations: readonly HostOperation[], epoch = 0): Promise<void> {
  // a secret's statement binds its values (SecretStore): never run here as text
  const secret = operations.find((o) => o.op === 'secret');
  if (secret !== undefined) throw new Error(`the secret '${(secret as { readonly name: string }).name}' binds its values: run it with the host's store, not as text`);
  for (const statements of await source.hostOperations(engine.databaseType, operations)) {
    for (const sql of statements) await engine.run(sql, epoch);
  }
}
