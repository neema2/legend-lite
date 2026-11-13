// A HOST'S NAMED OPERATIONS on its own database (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5): what the tab used to
// spell as SQL itself -- a file read into a table, a remote file mounted, a copy made, rows counted, an extension, a
// secret -- named here, with its data, and answered by legend-lite's planner (Boundary.hostOperations: the platform's
// HostOperations in Java, by the database's type) as the statements the database runs. The page spells no SQL; a
// database without an operation refuses it by name. Python's host writes the same JSON (compiler.host_operations).

import type { PlannedSql, PlanText } from './planned-sql.ts';

/** A relation's name. `declared`: a model's own table, spelled as its seed made it; any other name is quoted. */
export interface HostName {
  readonly database?: string;
  readonly schema?: string;
  readonly name: string;
  readonly declared?: boolean;
}

/**
 * Where a relation's rows come from: a relation of the database. A host operation takes NO SQL (the final audit's item
 * 1, 2026-11-12: the user's ruling, no planner door takes SQL but the playground's): a query's rows are counted and
 * copied by the planner's plan of its pull (a snapshot's count, its snapCopy), never by its statement handed back; a
 * person's own file is read by a planned load (the planner's fileLoad), a remote file likewise (its remoteMount).
 */
export type HostSource = { readonly relation: HostName };

/** A column as the database's catalog holds it (Boundary.tableModel's columns): what a copy's conversions are written
 *  from by the platform (`convert`), never a select list a host hands back. */
export interface CatalogColumn {
  readonly name: string;
  readonly dataType: string;
  readonly logicalType?: string | null;
  readonly precision?: number | null;
  readonly scale?: number | null;
  readonly notNull?: boolean;
}

export type HostOperation =
  /** A relation made: replacing one of its name or not; every column, or -- `convert`, the source's columns as its
   *  catalog holds them -- each the platform converts to hold what its model declares (CatalogModel's conversions,
   *  written by the database's own select list: the final audit's item 1(d), 2026-11-12). */
  | {
    readonly op: 'relation'; readonly kind: 'table' | 'view'; readonly target: HostName; readonly replace?: boolean;
    readonly convert?: readonly CatalogColumn[];
    readonly from: HostSource;
  }
  | { readonly op: 'drop'; readonly kind: 'table' | 'view'; readonly target: HostName }
  | { readonly op: 'schema'; readonly database?: string; readonly name: string }
  | { readonly op: 'dropSchema'; readonly database?: string; readonly name: string }
  /** How many rows: one row, one column `n`. */
  | { readonly op: 'count'; readonly from: HostSource }
  // a remote file's secret is a step of its planned mount, its fields bound from the host's store (B3's Q3: the
  // planner's remoteMount), never an operation
  /** A secret dropped, when it is there. */
  | { readonly op: 'dropSecret'; readonly name: string }
  /** The session variable for the value held under `held` reset, set or not: the connection's own (a remote file's
   *  bearer URL, let go of; the dialect names the variable). */
  | { readonly op: 'resetVariable'; readonly held: string }
  | { readonly op: 'extensions'; readonly names: readonly string[] }
  /** Where the database installs extensions from, on demand (the site's folder). */
  | { readonly op: 'extensionRepository'; readonly url: string }
  | { readonly op: 'transaction'; readonly step: 'begin' | 'commit' | 'rollback' };

/** An S3 secret's fields, as DuckDB's CREATE SECRET names them (the platform's DuckDbHostOperations.CREDENTIALS), and
 *  URL, a bearer URL's (a presigned link, its query string its token: B3's Q3). */
export type SecretField = 'KEY_ID' | 'SECRET' | 'SESSION_TOKEN' | 'REGION' | 'ENDPOINT' | 'URL_STYLE' | 'USE_SSL' | 'URL';

/** A secret's value: a text, or USE_SSL's boolean. */
export type SecretValue = string | boolean;

/**
 * THE HOST'S SECRETS, BY NAME (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "credentials by name ...
 * a guard keeps any secret value out of the plan"): the values a person gave a source -- its keys, a bearer URL --
 * held in this host's memory under a name and bound into a planned statement where the plan names them (plan-runner.ts
 * HostSecrets) -- never written into an operation, a statement's text or a plan. A name this store does not hold, or a
 * field it holds no value for, is refused by name, the values never named.
 */
export class SecretStore {
  readonly #held = new Map<string, ReadonlyMap<SecretField, SecretValue>>();

  /** `values` held under `name`, replacing what it held. */
  hold(name: string, values: ReadonlyMap<SecretField, SecretValue>): void {
    this.#held.set(name, new Map(values));
  }

  /** The value held under `name`'s `field`: what a plan's held bind names (plan-runner.ts HostSecrets). */
  value(name: string, field: string): SecretValue {
    const values = this.#held.get(name);
    if (values === undefined) throw new Error(`'${name}' is not a secret this host holds`);
    const value = values.get(field as SecretField);
    if (value === undefined) throw new Error(`the secret '${name}' holds no ${field}`);
    return value;
  }
}

/**
 * A REMOTE FILE'S MOUNT, as the planner is asked for its plan (Boundary.remoteMount, B3's Q3): NAMES AND FIELDS ONLY --
 * an address that carries no token, or a bearer URL by the name this host holds it under and its address without the
 * query string; an S3 secret by the name this host holds it under and the fields it holds. The planner refuses any
 * other key, so no value rides in one.
 */
export interface RemoteMountRequest {
  readonly databaseType: string;
  /** The view it is read through; none for its access alone (its extensions and keys, before a check of its files). */
  readonly view?: string;
  readonly location: { readonly url: string } | { readonly bearer: string; readonly shown: string };
  readonly format: 'csv' | 'parquet' | 'iceberg';
  readonly credentials?: { readonly name: string; readonly fields: readonly SecretField[] };
}

/** Where a remote file's mount's plan comes from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface RemoteMountSource {
  /** The plan, PlanJson's text: one HOST_SESSION load the hosts' runner runs (runLoad), binding what it names. */
  remoteMount(request: RemoteMountRequest): Promise<PlanText>;
}

/**
 * A SNAPSHOT'S STREAM, as the planner is asked for its plan (Boundary.snapStream, the user's ruling of 2026-11-11: a
 * snapshot from a remote plane is a planned load whose source is a stream): the host's database's type, the name this
 * host holds the remote plane under, the PULL -- the grid's base query as its model, runtime and lambda, which the
 * planner plans in the same call for the plane's own database (the user's ruling of 2026-11-12, "that is a real off
 * platform problem": a request carries no statement) -- and the table of this host's database it lands in. Never a
 * row, never SQL: the planner refuses any other key by name.
 */
export interface SnapStreamRequest extends SnapPull {
  readonly databaseType: string;
  readonly plane: string;
  readonly schema?: string;
  readonly table: string;
}

/** A pull, as a planner that plans it where its rows are is asked (its planRequest): the model's text, the runtime it
 *  runs through, and the query as a lambda's protocol JSON. */
export interface SnapPull {
  readonly model: string;
  readonly runtime: string;
  readonly lambda: string;
}

/**
 * A SNAPSHOT'S LOCAL COPY, as the planner is asked for its plan (Boundary.snapCopy, the final audit's item 1(b),
 * 2026-11-12): the host's database's type, the PULL, which the planner plans in the call, and the table it is copied
 * into. Never SQL: the planner refuses any other key by name.
 */
export interface SnapCopyRequest extends SnapPull {
  readonly databaseType: string;
  readonly schema?: string;
  readonly table: string;
}

/** Where a snapshot's stream's and copy's plans come from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface SnapStreamSource {
  /** The plan, PlanJson's text: one HOST_SESSION load whose stream step the hosts' runner runs (runLoad). */
  snapStream(request: SnapStreamRequest): Promise<PlanText>;
  /** The plan, PlanJson's text: one write -- its schema, the table there before dropped, the table made from the pull
   *  (its data: payloads bound by digest) -- in its target's transaction, which the hosts' runner runs (runPlan). */
  snapCopy?(request: SnapCopyRequest): Promise<PlanText>;
}

/**
 * ROWS THE DATABASE GENERATES INTO A DECLARED TABLE, as the planner is asked for their plan (Boundary.synthesize: the
 * test-data generator's SYNTHESIZE, its simple slice, the user's ruling of 2026-11-12; DuckDB alone): the host's
 * database's type, the model's text, the table its Database declares, how many rows, and a knob per column by name --
 * its values in turn, or a range of numbers (at a scale) or of ISO dates. Never a row: the planner refuses any other key.
 */
export interface SynthesizeRequest {
  readonly databaseType: string;
  readonly model: string;
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly count: number;
  readonly columns?: Readonly<Record<string, SynthesizeKnob>>;
}

/** A column's knob: its values in turn, or a range `[lo, hi]` (numbers at `scale` places, or ISO dates). */
export type SynthesizeKnob =
  | { readonly values: readonly (string | number)[] }
  | { readonly range: readonly [string | number, string | number]; readonly scale?: number };

/** Where a synthesized table's plan comes from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface SynthesizeSource {
  /** The plan, PlanJson's text: one HOST_SESSION load (the table made as declared, then one statement filling it). */
  synthesize(request: SynthesizeRequest): Promise<PlanText>;
}

/** What answers a host's operations: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface HostOperationsSource {
  /** Each operation's statements, in order, for a database of `databaseType`. */
  hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<PlannedSql[][]>;
}

/** Each of `operations`' statements run on `engine`, in order: the operations' whole step. */
export async function runHostOperations(source: HostOperationsSource,
  engine: { readonly databaseType: string; run(sql: PlannedSql, epoch: number): Promise<unknown> },
  operations: readonly HostOperation[], epoch = 0): Promise<void> {
  for (const statements of await source.hostOperations(engine.databaseType, operations)) {
    for (const sql of statements) await engine.run(sql, epoch);
  }
}
