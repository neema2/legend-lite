// THE REQUESTS AND ANSWERS BETWEEN A HOST, ITS PLANNER AND ITS RUNNER (C5-T, 2026-11-15: the TypeScript split of
// plan and run, docs/TOTAL_WALL_START_HERE_2026_11_11.md section 6.1): records, the shapes a host asks legend-lite's
// planner with and the shapes the planner answers in, read by both sides -- the planner, which plans from them, and the
// runner and the doors, which run what it answers. A record holds no behaviour: it plans nothing and sends nothing.
// Until C5 they lived in the run modules that use them (host-operations.ts, model-data.ts, project-data.ts), which
// re-export them, so the planner imported its own requests from the run side; now both read them here
// (//engine-client:records, tools/deps/ts-layers.txt). The questions they are asked in -- fileLoad, projectData, the
// host's operations -- are the planner's (planner-sources.ts), which the run side never names.

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

/** The planner's answer for one project's database: its name, and the statements of each step, in order. */
export interface ProjectDataStatements {
  /** Its name, from what it holds: the model's Databases and Data rows. */
  readonly name: string;
  /** Make it, empty (on any connection). */
  readonly open: readonly PlannedSql[];
  /** Point a connection of its own at it. */
  readonly use: readonly PlannedSql[];
  /** The plan whose root is the ONCE load that makes and fills every table a relational Data element fills (on that
   *  connection), PlanJson's text as the planner answered it; absent for a model with no Data rows. */
  readonly load?: PlanText;
  /** Drop it, with everything in it (on any connection). */
  readonly close: readonly PlannedSql[];
}

/**
 * HOW A DATASET'S ROWS LAND, as the planner is asked for their plans (Boundary.ingestPlans: an ingest's plans, the desk
 * roadmap's C0, the user's go of 2026-11-14): the host's database, the model's text, a table an ingest's dataset makes
 * (reached from `database` through its includes), and the name the host holds each batch under. Never a row.
 */
export interface IngestPlansRequest {
  readonly databaseType: string;
  readonly model: string;
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly held: string;
  /** The deployment whose live target's `keep` decides what the table keeps (docs/DEPLOYMENT_DESIGN_2026_11_14.md step
   *  5); absent, the ingest's own retention. */
  readonly deployment?: string;
}

/** A dataset's three plans, each one HOST_SESSION load the hosts' runner runs (runLoad): its table made as declared,
 *  one batch landed (the value held under the request's `held` attached by name, put in by the write mode), and its
 *  retention kept, run once `every` rows have arrived since the last -- null for a dataset that keeps every row. */
export interface IngestPlans {
  readonly make: PlanText;
  readonly land: PlanText;
  readonly keep: { readonly plan: PlanText; readonly every: number } | null;
}
