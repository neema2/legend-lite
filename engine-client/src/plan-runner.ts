// THE PLAN RUNNER, a host's (docs/WRITES_DESIGN_2026_10_18.md leg 7; docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, an Output
// into a table): an execution plan -- PlanJson's text, version 22, as legend-lite's planner writes it -- run on a host's own
// database, for a host that runs its SQL itself: the tab (DuckDB-WASM) and Python's headless DataCube (its DuckDB). The
// rules are Java's PlanRunner's (core/.../exec/PlanRunner.java), for the plans a host's program makes: a SEQUENCE of
// steps in one transaction -- each `update` run for the change it makes, its count kept where a later step binds it
// (`#count`), each `check` run for what its database may raise and its rows dropped, the one `tdsResult` read once the
// transaction has committed -- or a `tdsResult` alone. Every value is the database's: its counts, its rows. The answer
// is TYPED rows (the result boundary program, 2026-10-27: a plan names no output format), and this runner writes them
// as lite's JSON rows, one object per row keyed by the plan's column names, for the cells a host's write answers (a
// count, a text, a boolean); a richer answer is the planner's to write (executeAnswer), refused here by name. What a
// host's program never plans is refused by name, never run some other way: a plan that takes parameters, a type hole,
// a list or a key's cell bound, a statement of per-key regions, a step of another database or of several, a database set
// up first.
//
// THE LOADS (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "the hosts run the plan's loads"): a plan's
// target may name loads -- a connection's declared test data, a project's Data -- in the plan's load section, by
// identity. The host makes a database for such a target (its session source: never its own tables) and `establish`es it
// there once, its ONCE and PER_ACQUISITION loads run in one transaction, each content blob's bytes checked against the
// digest its load names; runPlan then runs the plan on that session (`established`). (A plan's release and a target's
// AT_RELEASE loads went with the data: session tables they dropped, the JSON pass, 2026-11-11; this runner's release
// with them, B3's follow-up.) A file's load names a HOST's own file (a person's upload, Studio's put): the host's store
// (HostFiles) says what its database reads it by and that its bytes are the reference's, and the statement's ? is
// bound to it -- never a path in a statement's text (B3: uploads and file puts as planned loads). A remote file's mount
// (B3's Q3) is a load whose `bound` steps bind what the host holds BY NAME (HostSecrets: a key's field, a bearer URL),
// the plan holding no value; its CONNECTION-reach steps (a session variable) are handed back for the host to replay on
// every connection it opens (ConnectionSettings).
//
// PURE: no DOM, no planner; a host gives its session.

import { boundValues, type Bind, type HostSecrets, type KeptSetting } from './connection-settings.ts';
import { familyOf, isBoolean } from './types.ts';
import { payloadValue } from './plan-params.ts';
import type { Receipt } from './receipt.ts';
import { fromPlan, type PlannedSql, type PlanText } from './planned-sql.ts';

/** A value bound into a statement's `?`. */
export type PlanValue = string | number | boolean | null;

// a host's held values and the settings a load makes on a connection: a module of their own, so a page that keeps them
// (the tab's duckdb-tab.ts) carries this runner only once it runs a plan (B3's Q3)
export { ConnectionSettings, replaySettings, type Bind, type HostSecrets, type KeptSetting } from './connection-settings.ts';

/**
 * Where a plan runs: a host's own database, one statement at a time. Its transactions are the plan's: the statements
 * its target carries (`transaction`), sent here as every other statement -- never asked of the planner at run (the
 * re-audit's S8, 2026-11-12: until then a host's session asked for them, and a run never plans, G4).
 */
export interface PlanSession {
  /** The database's type, as the planner names it (`DuckDB`): a plan's steps must be of it. */
  readonly databaseType: string;
  /** A statement run with its `?`s bound in order: its rows, each its values by column. */
  run(sql: PlannedSql, params: readonly PlanValue[]): Promise<readonly (readonly unknown[])[]>;
}

interface Slot { readonly parameter: string; readonly binding: { readonly _type: string; readonly asText?: unknown; readonly digest?: string } }
interface Sql {
  readonly statement: string; readonly slots: readonly Slot[]; readonly regions?: readonly unknown[]; readonly target: unknown; readonly stamp?: string;
  /** The payloads its slots bind, by digest (the JSON pass): the plan's own data. */
  readonly content?: Readonly<Record<string, string>>;
}

/** The execution plan's format this runner reads (core/.../executionplan/PlanJson.java VERSION): 18 since 2026-10-29
 *  (the result boundary program: a plan's answer is a `tdsResult` of typed columns, never a text result of a format;
 *  the written-precision fact on a column since S1.4) -- `update`, `check` and `sequence` as version 5 wrote them; a
 *  statement's per-key regions and a key's cell (a dynamic pivot's probe plan) are no write's. */
// 19 since 2026-10-29: the LITERAL slot word retired (track L's L2; core's PlanJson.VERSION is the one owner)
// 20 since 2026-10-30: the verdicts node (a run of asserts as one step: the planner-driver seam program's S1b) -- a
// program's step, which this runner refuses by name as any step that is not an update, a check or a typed result
// 21 since 2026-10-30: a verdict's constant side says whether the compiler folded its cells (S2f) -- inside the verdicts
// node this runner refuses, so nothing here reads it
// 22 since 2026-10-31: a keyed result's key source and a verdict's keyed wrapper (the seam program's S3c)
// 23 since 2026-10-31: the constant node (a value the program wrote, answered with nothing sent: the test-is-the-product
// plan's M2) -- its one column's cells as text, a String's or a Boolean's, written as the typed result's would be
// 24 since 2026-10-31: an effect's script, the setup node (a program's seed step) and a verdict group's savepoint (the
// test-is-the-product plan's M3) -- an effect and a seed step are refused by name, as any step that is not an update, a
// check, a typed result or a constant
// 25 since 2026-11-01: a verdict carries no wantEqual -- inside the verdicts node this runner refuses
// 26 since 2026-11-01: a target's database may be the graph's system database -- a database this runner does not hold
// 27 since 2026-11-08: the verdict group and the branch leave for the test plan's own format -- nodes this runner refused
// 28 since 2026-11-08: the system database names its store by its content's key -- as 26
// 29 since 2026-11-08: a declared or platform database carries its project's identity (`project`) -- a server's
// project database, which only the server's plans name; a host runs on its own session and reads none of it
// 30 since 2026-11-09: whether the plan writes (`writes`: where Java's run sends it) and a project's refusal
// (`refused`) -- this runner runs every step on the host's one session, a write in its transaction, so it reads neither
// 31 since 2026-11-09: the setup node is the load node (identity, cadence) and a load step may be a file by its data
// reference (docs/DATA_IN_THE_PLAN_2026_11_09.md L1) -- a load node is refused by name, as the setup node was, and a
// target's setup steps are refused whatever their kind (below)
// 32 since 2026-11-09: a data reference's format is the declared format, an object, and a file's one delivery is the
// streamed one (L2) -- read by none of this runner's steps: a load node is refused by name, as at 31; a target's
// setup is its `loads` (identities into the plan's `loads` section): a target naming any is refused, as its setup was
// 33 since 2026-11-09: a load may be PER_TRANSACTION (a model's registration in the system catalog, L4b) and a system
// database's target names the catalog's ONCE load -- read by none of this runner's steps: a load node, and a target
// naming any load, are refused by name, as at 32
// 34 since 2026-11-10: a file's delivery is the database's reading of it (reads) -- a load is refused by name, as at 33
// 35 since 2026-11-10: a file's delivery may be the runner's streamed cells (streamed) -- a load is refused by name, as at 34
// 36 since 2026-11-10: a slot may bind a live source's location (source), the runner binding the path its admission
// allows -- refused by name here: a host's runner admits no source
// 37 since 2026-11-10: a load may read the model's text from its bundle (content; a rows reference) -- a load is
// refused by name here, as at 36
// 38 since 2026-11-10: the model's data is a load step of statements (a CONTENT blob of SQL statements); the rows
// step and format are gone -- a load is refused by name here, as at 37 (until B3)
// 39 since 2026-11-10: a file's load is the database's reading of it alone (the streamed delivery gone) -- a load is
// refused by name here, as at 38
// 40 since 2026-11-10: a load's statements step names the data it loads (of) -- a load is refused by name here, as at 39
// 41 since 2026-11-10: a date bound as its text carries no digit count (asText is {}) -- still refused by name here
// 42 since 2026-11-10: a column's store type is an object (plain/sized/scaled), which this runner does not read
// 43 since 2026-11-11: a list's array binding of dates carries the digits its database keeps of an element
// (fraction) -- a slot this runner binds no list for
// 44 since 2026-11-11: a sourceUrl source names its form and a load may be SOURCE (a live source's session table) --
// a load is refused by name here, as at 42
// 45 since 2026-11-11: a plan may carry its release (on, statements: its data: tables' drops, sent once the run is
// over whatever its outcome) -- refused by name here, as a load is
// 46 the same day: a slot may bind a data: payload's JSON array (payload: Snowflake's and BigQuery's delivery) -- not a
// host database's plan. B3 (2026-11-11, no new version): a target's loads and a plan's release run here where the host
// established a session for the target (establish, release); a load node of a program and a file's load stay refused
// 47 the same day (B3): a data reference may name a HOST's own file (detectedCsv, parquet, json) and a load may be
// HOST_SESSION -- a person's file read into the host's database, kept until reset (runLoad, with the host's files);
// and (B3's Q3) such a load's `bound` step, its binds the plan's own value or a value the host holds by name
// 48 since 2026-11-11 (the JSON pass, over 47 when the two merged): a payload slot names its text by its SHA-256
// (payload's digest), the text in its statement's content -- refused by name here, as at 46; and no plan carries a
// release and no load is SOURCE or AT_RELEASE (every payload bound or inline: no session table): this runner's release,
// which sent them, deleted with them (B3's follow-up, the same day)
// 49 since 2026-11-12 (the C checkpoint's SHOULD-FIX 3): a target may carry its database's settings (H2's) -- no host
// database's
// 50 since 2026-11-12 (the re-audit's S8): a target carries its database's transaction (begin, commit, rollback: DuckDB's)
// -- what this runner opens one with, around a plan's writes, a target's loads and a keyed snapshot; a plan whose
// target has none is refused by name where it needs one
// 51 since 2026-11-12 (the user's ruling: "that is a real off platform problem"): a stream step's sql is the planner's
// own plan of the snapshot's pull -- its request carries the pull (model, runtime, lambda), never the statement
const PLAN_VERSION = 51;
interface Column { readonly name: string; readonly type: string }
type Step =
  | { readonly _type: 'update'; readonly sql: Sql; readonly count?: string | null }
  | { readonly _type: 'effect'; readonly sql: Sql; readonly script?: unknown }
  | { readonly _type: 'check'; readonly sql: Sql }
  | { readonly _type: 'tdsResult'; readonly shape: string; readonly columns: readonly Column[]; readonly sql: Sql }
  | { readonly _type: 'constant'; readonly columns: readonly Column[]; readonly cells: readonly (readonly (string | null)[])[] }
  | { readonly _type: 'sequence'; readonly steps: readonly Step[] }
  | { readonly _type: string };

/** A load as the plan's load section writes it (PlanJson): its identity, cadence and steps, and its content blobs by
 *  digest (inline: the plan the host's planner writes holds them). */
interface Load {
  readonly identity: string;
  readonly cadence: string;
  readonly steps: readonly LoadStep[];
  readonly content?: Readonly<Record<string, string>>;
  /** Where it loads: its database's own target (PlanJson writes it on every load). */
  readonly target?: Target;
}
type LoadStep =
  | { readonly _type: 'statement'; readonly sql: string }
  | { readonly _type: 'statements'; readonly ref: DataRef; readonly of: string }
  | { readonly _type: 'file'; readonly ref: DataRef }
  | BoundStep
  | StreamStep
  | { readonly _type: string };
/** A remote plane's answer streamed into a table (PlanJson's `stream`): the plane by name, never a row. */
interface StreamStep {
  readonly _type: 'stream'; readonly plane: string; readonly sql: string; readonly schema: string | null;
  readonly table: string; readonly prepare: readonly string[]; readonly count: string;
  readonly transaction: Transaction;
}
/** A statement whose `?`s the host binds (PlanJson's `bound`): its effect the database's, or the connection's. */
interface BoundStep { readonly _type: 'bound'; readonly sql: string; readonly reach: 'DATABASE' | 'CONNECTION'; readonly binds: readonly Bind[] }

interface DataRef { readonly namespace: string; readonly name: string; readonly digest: string; readonly size: number; readonly format: { readonly _type: string } }
/** A database's transaction as a plan writes it (PlanJson's `transaction`): each step's statements, in order. */
interface Transaction { readonly begin: readonly string[]; readonly commit: readonly string[]; readonly rollback: readonly string[] }
/** A target as a plan's statement writes it: its loads by identity, into the plan's load section; its database's
 *  transaction where a host opens one there (version 50: DuckDB's). */
interface Target {
  readonly database?: unknown; readonly session?: readonly string[]; readonly loads?: readonly string[];
  readonly transaction?: Transaction;
}
/** A plan read: its JSON, and its load section by identity. */
interface ReadPlan {
  readonly format?: unknown; readonly version?: unknown; readonly parameters?: unknown; readonly root?: Step;
  readonly loads?: readonly Load[];
}

/** `plan`'s JSON read, refused by name unless it is a legend-lite plan of the version this runner reads. */
function read(plan: string): ReadPlan {
  const p = JSON.parse(plan) as ReadPlan;
  if (p.format !== 'legend-lite-plan' || p.version !== PLAN_VERSION) {
    throw new PlanRefusal(`not a legend-lite execution plan of version ${PLAN_VERSION}: ${JSON.stringify({ format: p.format, version: p.version })}`);
  }
  return p;
}

/** The statements of `plan`'s steps that a session runs (a constant sends none). */
function sentSqls(plan: ReadPlan): Sql[] {
  const root = plan.root;
  if (root === undefined) throw new PlanRefusal('a plan with no root');
  if (root._type === 'keyed') return [(root as Keyed).keys, (root as Keyed).body];
  const steps = root._type === 'sequence' ? (root as { readonly steps: readonly Step[] }).steps : [root];
  return steps.filter((s) => s._type !== 'constant').map((s) => stepSql(s));
}

/** The one target `plan`'s statements run on (none for a constant alone), refused by name where they name several. */
function targetOf(plan: ReadPlan): Target | undefined {
  const sqls = sentSqls(plan);
  if (sqls.length === 0) return undefined;
  const target = JSON.stringify(sqls[0]!.target);
  if (sqls.some((s) => JSON.stringify(s.target) !== target)) throw new PlanRefusal("a plan's steps run on one database: these name several");
  return sqls[0]!.target as Target;
}

/**
 * A field a plan of this version always carries (Java's PlanJson writes it, and reads it as required: a target's
 * `session` and `loads`, the plan's `loads` section, a payload slot's `digest`, a statement's `content` where a slot
 * binds a payload), refused by name where it is missing -- never read as empty (G7: no defaulting; the C final audit's
 * H10, 2026-11-12).
 */
function required<T>(value: T | undefined | null, what: string): T {
  if (value === undefined || value === null) throw new PlanRefusal(`a plan of version ${PLAN_VERSION} carries ${what}, and this one has none`);
  return value;
}

/**
 * `work` in one transaction on `run`, its statements `target`'s (the plan's: version 50, the re-audit's S8) -- committed
 * where it completes, rolled back where it fails and its failure thrown (a rollback that fails too is said with it).
 */
async function inTransaction<T>(target: Target, run: (sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>,
  work: () => Promise<T>): Promise<T> {
  const t = required(target.transaction, "its target's transaction");
  return transacted({ begin: t.begin.map((s) => fromPlan(s)), commit: t.commit.map((s) => fromPlan(s)), rollback: t.rollback.map((s) => fromPlan(s)) },
    run, work);
}

/**
 * `work` between a transaction's statements on `run` (each a plan's or a planner's: inTransaction, DataCube's table
 * writer) -- committed where it completes, rolled back where it fails and its failure thrown, a rollback that fails too
 * said with it.
 */
export async function transacted<T>(t: { readonly begin: readonly PlannedSql[]; readonly commit: readonly PlannedSql[]; readonly rollback: readonly PlannedSql[] },
  run: (sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>, work: () => Promise<T>): Promise<T> {
  for (const statement of t.begin) await run(statement, []);
  let done: T;
  try {
    done = await work();
  } catch (e) {
    try {
      for (const statement of t.rollback) await run(statement, []);
    } catch (r) {
      throw new Error(`${e instanceof Error ? e.message : String(e)} -- and rolling its transaction back failed: ${r instanceof Error ? r.message : String(r)}`, { cause: e });
    }
    throw e;
  }
  for (const statement of t.commit) await run(statement, []);
  return done;
}

/**
 * A LOAD'S PLAN RUN IN ONE TRANSACTION (a person's file put into a declared table: model-data.ts loadFileTable) --
 * `work` (the host's run of it: runLoad) between the transaction statements the load's target carries, so a refused
 * file leaves the table as it was. The statements are the plan's (the re-audit's S8, 2026-11-12: until then the host
 * asked the planner for them at run).
 */
export async function inLoadsTransaction<T>(plan: PlanText, run: (sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>,
  work: () => Promise<T>): Promise<T> {
  const root = read(plan).root as (Load & { readonly _type: string }) | undefined;
  if (root === undefined || root._type !== 'load') throw new PlanRefusal(`a load's plan has a load at its root, not a ${String(root?._type)}`);
  return inTransaction(required(root.target, "its load's target"), run, work);
}

/** `target`'s loads from `plan`'s load section, in the target's order: one the section lacks refused by name. */
function loadsOf(plan: ReadPlan, target: Target): Load[] {
  const section = new Map(required(plan.loads, 'its loads section').map((l) => [l.identity, l]));
  return required(target.loads, "its target's loads").map((identity) => {
    const load = section.get(identity);
    if (load === undefined) throw new PlanRefusal(`a target names the load ${identity}, which its plan does not hold`);
    return load;
  });
}

/**
 * The identity of what a plan's target holds once established: its database, its session statements and its loads'
 * identities (each the digest of its content) -- the key a host keeps the database it made for the target by, as
 * Java's shared sessions key theirs (PlanJson.writeTarget). Null for a plan whose target loads nothing (the host's own
 * session serves it).
 */
export function targetKey(plan: PlanText): string | null {
  const target = targetOf(read(plan));
  if (target === undefined || required(target.loads, "its target's loads").length === 0) return null;
  return JSON.stringify({ database: target.database, session: required(target.session, "its target's session"), loads: target.loads });
}

/** `text`'s SHA-256, lowercase hex, and its length in UTF-8 bytes: WebCrypto's in every host (the tab, Node, and
 *  Python's V8, where the headless bundle installs it over the host's hashlib: install-web.ts). */
async function digestOf(text: string): Promise<{ readonly digest: string; readonly size: number }> {
  const bytes = new TextEncoder().encode(text);
  const hash = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return { digest: [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, '0')).join(''), size: bytes.length };
}

/**
 * A HOST'S OWN FILES (the plan's HOST namespace, B3): what its database reads a file by -- a registered buffer's name,
 * a path -- once the host has checked the bytes it keeps under the reference's name are the reference's (its digest and
 * size), refused by name otherwise.
 */
export interface HostFiles {
  located(ref: { readonly name: string; readonly digest: string; readonly size: number }): Promise<string>;
}

/** `bytes`' SHA-256, lowercase hex: a host's file's digest, as its plan names it (WebCrypto in every host). */
export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const hash = await globalThis.crypto.subtle.digest('SHA-256', bytes as Uint8Array<ArrayBuffer>);
  return [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/**
 * A REMOTE PLANE'S ANSWER, AS A HOST STREAMS IT INTO ITS DATABASE (the user's ruling of 2026-11-11: a snapshot from a
 * remote plane is a planned load whose source is a stream): the planes this host holds, by the name a plan's stream step
 * names one by (a warehouse a grid reads live), and its held connection -- a statement's rows, and one Arrow IPC
 * stream's rows put into a table by the database's own Arrow import (duckdb.ts HeldConnection).
 */
export interface HostStreams {
  readonly planes: Readonly<Record<string, HostPlane>>;
  readonly held: {
    rows(sql: PlannedSql, params: readonly PlanValue[]): Promise<unknown[][]>;
    insertArrow(bytes: Uint8Array, target: { readonly schema?: string; readonly table: string }, create: boolean): Promise<void>;
  };
}

/** A remote plane: its answer to a statement as Arrow IPC streams, one per chunk, and the receipt its done step issues
 *  (its count of the answer's rows) before the first chunk (warehouse.ts arrowChunks). */
export interface HostPlane {
  arrowChunks(sql: PlannedSql, signal?: AbortSignal, onReceipt?: (receipt: Receipt) => void): AsyncIterable<Uint8Array>;
}

/** What a stream step landed: its table, the rows the database counted there, and the plane's receipt. */
export interface Landed {
  readonly plane: string;
  readonly schema?: string;
  readonly table: string;
  readonly rows: number;
  readonly receipt: Receipt;
}

/**
 * One stream step run (PlanJson's `stream`): in ONE transaction, its statements the plan's -- the step's `prepare` (its
 * schema made, the table there before dropped), then each chunk of the plane's answer put in by the database's Arrow
 * import, the first making the table, then the database's count of what landed -- committed only when the stream
 * completed (the plane's done step heard, every chunk it names read) and the count is the plane's own for its answer;
 * else rolled back, nothing left, and refused by name. No hash (the orchestrator, 2026-11-12): a snap has no content
 * known at planning; the transport is TLS's, a stream cut short the count's and the done step's to tell.
 */
async function stream(load: Load, step: StreamStep, streams: HostStreams | undefined): Promise<Landed> {
  const into = step.schema === null ? step.table : `${step.schema}.${step.table}`;
  if (streams === undefined) {
    throw new PlanRefusal(`the load ${load.identity} streams the remote plane '${step.plane}' into ${into}, and this host streams no plane's rows into its database`);
  }
  const plane = Object.hasOwn(streams.planes, step.plane) ? streams.planes[step.plane] : undefined;
  if (plane === undefined) throw new PlanRefusal(`'${step.plane}' is not a remote plane this host holds: nothing is streamed into ${into}`);
  const run = async (statements: readonly string[]): Promise<void> => {
    for (const sql of statements) await streams.held.rows(fromPlan(sql), []);
  };
  const target = { ...(step.schema === null ? {} : { schema: step.schema }), table: step.table };
  await run(step.transaction.begin);
  try {
    await run(step.prepare);
    let receipt: Receipt | undefined;
    let chunks = 0;
    for await (const bytes of plane.arrowChunks(fromPlan(step.sql), undefined, (r) => { receipt = r; })) {
      await streams.held.insertArrow(bytes, target, chunks === 0);
      chunks += 1;
    }
    if (receipt === undefined) throw new PlanRefusal(`the stream from '${step.plane}' ended before its plane said it was done: nothing of it is kept in ${into}`);
    if (chunks === 0) throw new PlanRefusal(`no data came back from '${step.plane}' to load into ${into}, not even its columns`);
    const expected = receipt.serverRows;
    if (expected === undefined) throw new PlanRefusal(`'${step.plane}' did not count its answer, so a stream cut short could not be told: nothing of it is kept in ${into}`);
    const counted = Number((await streams.held.rows(fromPlan(step.count), []))[0]?.[0]);
    if (counted !== expected) {
      throw new PlanRefusal(`the stream from '${step.plane}' landed ${counted} rows in ${into}, where its plane counted ${expected}: nothing of it is kept`);
    }
    await run(step.transaction.commit);
    return { plane: step.plane, ...target, rows: counted, receipt };
  } catch (e) {
    // the database's own undo: the table it made in the transaction is gone with it (measured, DuckDB-WASM)
    // a rollback that fails too is said with the failure, never swallowed (G7; the final audit, 2026-11-12)
    await run(step.transaction.rollback).catch((r: unknown) => {
      throw new Error(`${e instanceof Error ? e.message : String(e)} -- and rolling its transaction back failed: ${r instanceof Error ? r.message : String(r)}`, { cause: e });
    });
    throw e;
  }
}

/** One load's steps sent in order on `session`: a statement as it is; a content blob of statements, its bytes checked
 *  against the digest and size its reference names first, each statement as written, a refusal naming the data; a
 *  host's own file through `files`, each delivery statement's ? bound to what its database reads it by, the statements
 *  that undo the delivery run whatever its outcome; a remote plane's answer streamed through `streams` in one
 *  transaction (`stream`). */
async function deliver(load: Load, session: Pick<PlanSession, 'run'>, files?: HostFiles,
  secrets?: HostSecrets, streams?: HostStreams, landed: Landed[] = []): Promise<KeptSetting[]> {
  const settings: KeptSetting[] = [];
  for (const step of load.steps) {
    if (step._type === 'statement') {
      await session.run(fromPlan((step as { readonly sql: string }).sql), []);
    } else if (step._type === 'statements') {
      const { ref, of } = step as Extract<LoadStep, { _type: 'statements' }>;
      const text = load.content?.[ref.digest];
      if (text === undefined) throw new PlanRefusal(`the load ${load.identity} reads the content ${ref.digest}, which its plan does not carry`);
      const held = await digestOf(text);
      if (held.digest !== ref.digest || held.size !== ref.size) {
        throw new PlanRefusal(`${of} is not the data it was planned against: ${held.size} bytes of digest ${held.digest}, where the plan names ${ref.size} bytes of digest ${ref.digest}`);
      }
      const statements = JSON.parse(text) as unknown;
      if (!Array.isArray(statements) || statements.length === 0 || statements.some((x) => typeof x !== 'string')) {
        throw new PlanRefusal(`${of}: a statements blob is a non-empty JSON array of strings`);
      }
      for (const statement of statements as string[]) {
        try {
          await session.run(fromPlan(statement), []);
        } catch (e) {
          // the database's own words, the data named (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 5)
          throw new Error(`${of}: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
        }
      }
    } else if (step._type === 'file') {
      const { ref, delivery } = step as Extract<LoadStep, { _type: 'file' }> & { readonly delivery: { readonly _type: string; readonly statements: readonly string[]; readonly after: readonly string[] } };
      if (ref.namespace !== 'HOST') throw new PlanRefusal(`the file '${ref.name}' (${ref.namespace}) is not a host's own: this host reads its own files alone`);
      if (files === undefined) throw new PlanRefusal(`the file '${ref.name}' is a host's own, and this run was given no host's files`);
      if (delivery._type !== 'reads') throw new PlanRefusal(`a file's delivery is its database's reading of it, not ${delivery._type}`);
      const at = await files.located(ref);
      try {
        for (const statement of delivery.statements) {
          try {
            await session.run(fromPlan(statement), [at]);
          } catch (e) {
            // the database's own words, the file named (its reader names the bound path)
            throw new Error(`the file '${ref.name}': ${e instanceof Error ? e.message : String(e)}`, { cause: e });
          }
        }
      } finally {
        for (const statement of delivery.after) await session.run(fromPlan(statement), []);
      }
    } else if (step._type === 'bound') {
      // what the host holds bound by name, never written (B3's Q3); a connection's setting kept for the host to replay
      const bound = step as BoundStep;
      await session.run(fromPlan(bound.sql), boundValues(bound.binds, secrets, `the load ${load.identity}'s bound statement`));
      if (bound.reach === 'CONNECTION') settings.push({ sql: fromPlan(bound.sql), binds: bound.binds });
      else if (bound.reach !== 'DATABASE') throw new PlanRefusal(`a bound statement's reach is DATABASE or CONNECTION, not ${String(bound.reach)}`);
    } else if (step._type === 'stream') {
      landed.push(await stream(load, step as StreamStep, streams));
    } else if (step._type === 'attach') {
      // a value the host holds attached by name (a Python frame): Python's host alone attaches one
      throw new PlanRefusal(`the load ${load.identity} attaches the host's value '${(step as unknown as { readonly held: string }).held}', and this host attaches none`);
    } else {
      throw new PlanRefusal(`a load's step is a statement, a statements blob, a file, a bound statement or a stream, not a ${step._type}`);
    }
  }
  return settings;
}

/**
 * A PLAN THAT IS ONE LOAD, run by `run` (each statement with its bound values): a project's Data rows as the ONCE load
 * its own database is made with (the planner's LoadPlanner.projectData: the load a server's project plan carries), or a
 * person's own file read into the host's database, kept until reset (HOST_SESSION: LoadPlanner.hostFile, the file a
 * reference `files` resolves) -- its steps in order, each content blob checked against its digest, a refusal naming its
 * data (B3, 2026-11-11: until then the host was handed the fill's statements and the file's path as text and ran them
 * outside any plan, census M14), or a snapshot from a remote plane, its answer streamed in through `streams` in the
 * step's own transaction (HOST_SESSION: LoadPlanner.snapStream; the user's ruling of 2026-11-11: until then
 * DuckDbEngine.loadArrow took the chunks outside any plan, in no transaction, census M14-snap) -- what it landed answered.
 * A load of any other cadence is refused by name; the host undoes a failure its own way (a project's database dropped
 * whole, a file put in one transaction, a stream rolled back).
 */
export async function runLoad(plan: PlanText, run: (sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>,
  host: { readonly files?: HostFiles; readonly secrets?: HostSecrets; readonly streams?: HostStreams } = {}):
  Promise<{ readonly settings: readonly KeptSetting[]; readonly landed: readonly Landed[] }> {
  const root = read(plan).root as (Load & { readonly _type: string }) | undefined;
  if (root === undefined || root._type !== 'load') throw new PlanRefusal(`a load's plan has a load at its root, not a ${String(root?._type)}`);
  // a project's Data, made with its database (ONCE); a person's file or a remote file's mount, kept until reset
  // (HOST_SESSION)
  if (root.cadence !== 'ONCE' && root.cadence !== 'HOST_SESSION') throw new PlanRefusal(`a load's plan is ONCE or HOST_SESSION, not ${root.cadence}`);
  const landed: Landed[] = [];
  const settings = await deliver(root, { run: (sql, params) => run(sql, params).then(() => []) }, host.files, host.secrets,
    host.streams, landed);
  return { settings, landed };
}

/**
 * THE TARGET ESTABLISHED on `session`, a session of a database the host made for it now (B3): its session statements,
 * then its ONCE loads (a project's Data) and its PER_ACQUISITION loads (a connection's declared test data) in its order,
 * in one transaction -- a failure rolls every load back and is thrown, naming
 * its data. Run once per database the host makes (`targetKey`); runPlan then runs the plan there (`established`).
 */
export async function establish(plan: PlanText, session: PlanSession): Promise<void> {
  const p = read(plan);
  const target = targetOf(p);
  if (target === undefined) return;
  for (const statement of required(target.session, "its target's session")) await session.run(fromPlan(statement), []);
  const loads = loadsOf(p, target);
  for (const l of loads) {
    if (l.cadence !== 'ONCE' && l.cadence !== 'PER_ACQUISITION') throw new PlanRefusal(`a target's load is ONCE or PER_ACQUISITION, not ${l.cadence}`);
  }
  if (loads.length === 0) return;
  await inTransaction(target, session.run, async () => {
    for (const l of loads) await deliver(l, session);
  });
}

/** A plan's refusal, in words: nothing of it ran. */
export class PlanRefusal extends Error {
  override readonly name = 'PlanRefusal';
}

/**
 * A plan run on `session` (`plan`: PlanJson's text): its answer as lite's JSON rows, written here from the typed rows
 * its `tdsResult` answers (one object per row, the plan's column names its keys) -- once its writes have committed, all
 * of them or none (a failure rolls back and is thrown, the database's words in it). `joined`: the caller's transaction
 * is open already (a table made before it is written, one change: an Output's create), so the plan runs inside it and
 * neither begins nor ends it -- as Java's runner joins a caller's.
 */
export async function runPlan(plan: PlanText, session: PlanSession,
  o: { readonly joined?: boolean; readonly established?: boolean } = {}): Promise<string> {
  const read = JSON.parse(plan) as ReadPlan;
  if (read.format !== 'legend-lite-plan' || read.version !== PLAN_VERSION) {
    throw new PlanRefusal(`not a legend-lite execution plan of version ${PLAN_VERSION}: ${JSON.stringify({ format: read.format, version: read.version })}`);
  }
  if (!Array.isArray(read.parameters) || read.parameters.length > 0) throw new PlanRefusal('a plan that takes parameters is not run here: its program binds none');
  const root = read.root;
  if (root === undefined) throw new PlanRefusal('a plan with no root');
  if (root._type === 'keyed') {
    // a pivot whose columns the data names: its keys read first, then its body per key (B3: the hosts run it)
    const keyed = root as Keyed;
    checkTarget(keyed.keys, session, o);
    if (o.established !== true) for (const statement of required((keyed.keys.target as Target).session, "its target's session")) await session.run(fromPlan(statement), []);
    return keyedRows(keyed, session);
  }
  const steps = root._type === 'sequence' ? (root as { readonly steps: readonly Step[] }).steps
    : root._type === 'tdsResult' || root._type === 'constant' ? [root] : undefined;
  if (steps === undefined) throw new PlanRefusal(`a plan's root is a sequence, a typed result or a constant, not a ${root._type}`);
  const results = steps.filter((s) => s._type === 'tdsResult' || s._type === 'constant');
  // a plan answers with one typed result, or -- every step a write (a snapshot's copy: Boundary.snapCopy) -- none
  const writes = steps.every((s) => s._type === 'update' || s._type === 'effect');
  if (results.length !== 1 && !(results.length === 0 && writes)) throw new PlanRefusal(`a plan answers with one typed result, not ${results.length}`);
  // a constant answer is the plan's own value: nothing to send for it (the test-is-the-product plan's M2)
  const sent = steps.filter((s) => s._type !== 'constant');
  if (sent.length === 0) return constantRows(results[0] as Extract<Step, { _type: 'constant' }>);
  const sqls = sent.map((s) => stepSql(s));
  // one database, as it is: every step's target the same, of this session's type, set up by nothing first
  const [first] = sqls;
  const target = JSON.stringify(first!.target);
  if (sqls.some((s) => JSON.stringify(s.target) !== target)) throw new PlanRefusal("a plan's steps run on one database: these name several");
  const t = checkTarget(first!, session, o);
  for (const s of sent) {
    // a relation of no declared column is a raw grid's: the columns its result reports, read from the real run by
    // the planner's runner (the seam program's S3c, 2026-10-30); this runner writes a host's program, never one
    if (s._type === 'tdsResult' && (s as { readonly shape?: string }).shape === 'RELATION'
        && (s as { readonly columns: readonly Column[] }).columns.length === 0) {
      throw new PlanRefusal("a relation of the columns its result reports (a raw grid's) is not run here: the planner runs it");
    }
  }
  for (const s of sqls) {
    if ((s.regions ?? []).length > 0) throw new PlanRefusal('a statement of per-key regions (a dynamic pivot\'s) is not run here: a write\'s has none');
    // a statement's own execution-trace stamp (an activity's comment, the test-is-the-product plan's M3f): a host's
    // session sends no trace comment, so a plan that reads one is not run here
    if (s.stamp !== undefined) throw new PlanRefusal(`a statement binding its own execution-trace stamp ('${s.stamp}') is not run here: a host's session sends no trace comment`);
    for (const slot of s.slots) {
      // a data: payload is the plan's own value, bound from the statement's content (plan-params.ts, M3)
      if (slot.binding._type === 'payload') continue;
      if (slot.binding._type !== 'one') {
        throw new PlanRefusal(`'${slot.parameter}' binds ${slot.binding._type === 'key' ? "a key's cell" : slot.binding._type === 'source' ? 'a file source' : slot.binding._type === 'payload' ? "a data: payload" : 'a list'}, which is not run here`);
      }
      // a date or date-time bound as its text into its declared type's cast (P4): a host binds none
      if (slot.binding.asText !== undefined && slot.binding.asText !== null) throw new PlanRefusal(`'${slot.parameter}' is a date bound as its text, which is not run here`);
    }
  }
  const changes = steps.some((s) => s._type === 'update' || s._type === 'effect') && o.joined !== true;
  // the target's session statements: run by `establish` on a session established for it, else here
  if (o.established !== true) for (const statement of required(t.session, "its target's session")) await session.run(fromPlan(statement), []);
  const values = new Map<string, PlanValue>();
  const bound = (s: Sql): PlanValue[] => s.slots.map((slot) => {
    // its payload's text from the statement's content, checked against its digest
    if (slot.binding._type === 'payload') return payloadValue(required(s.content, "a payload slot's statement content"), required(slot.binding.digest, "a payload slot's digest"));
    if (!values.has(slot.parameter)) throw new PlanRefusal(`'${slot.parameter}' is bound before any step gives it`);
    return values.get(slot.parameter)!;
  });
  let text: string | undefined;
  const all = async (): Promise<void> => {
    for (const step of steps) {
      if (step._type === 'constant') {
        text = constantRows(step as Extract<Step, { _type: 'constant' }>);
        continue;
      }
      const sql = stepSql(step);
      const rows = await session.run(fromPlan(sql.statement), bound(sql));
      if (step._type === 'update') {
        // the database's count, a value it produced, bound where a later step reads it
        const count = (step as { readonly count?: string | null }).count;
        if (count !== undefined && count !== null) values.set(count, countOf(rows));
      } else if (step._type === 'tdsResult') {
        text = rowsOf((step as { readonly columns: readonly Column[] }).columns, rows);
      }
    }
  };
  // a plan's writes in one transaction, its statements the plan's target's (version 50); a joined run is inside its
  // caller's
  if (changes) await inTransaction(t, session.run, all);
  else await all();
  // a plan of writes alone answers no row
  return text ?? '[]';
}

/**
 * A STATEMENT A PERSON WROTE, run (Studio's SQL playground: Boundary.authored, the user's ruling of 2026-11-12): the
 * plan's one statement -- the person's text as the planner carried it, never parsed, rewritten or split -- sent as
 * written by `on.run`, its answer the database's own (the relation it reports, its columns as it says them). A text the
 * database cannot take is its own error, passed on. Run as every plan is (the C final audit's H9, 2026-11-12): its
 * target's database of `on`'s type, its target's session statements first (unless `established`), a target that
 * loads data refused, its columns read as the plan states them (none: the relation the database reports). Any other
 * plan is refused by name.
 */
export async function runAuthored<T>(plan: PlanText, on: { readonly databaseType: string; run(sql: PlannedSql): Promise<T> },
  o: { readonly established?: boolean } = {}): Promise<T> {
  const root = read(plan).root;
  const result = root as { readonly _type?: string; readonly shape?: string; readonly columns?: readonly Column[]; readonly sql?: Sql } | undefined;
  if (result?._type !== 'tdsResult' || result.shape !== 'RELATION' || result.sql === undefined) {
    throw new PlanRefusal(`an authored statement's plan answers the relation its database reports, not a ${String(result?._type)}`);
  }
  if (required(result.columns, "its result's columns").length > 0) {
    throw new PlanRefusal("an authored statement's plan answers the relation its database reports, not declared columns");
  }
  if (result.sql.slots.length > 0) throw new PlanRefusal("an authored statement binds no value: it is the person's text, as written");
  const t = checkTarget(result.sql, { databaseType: on.databaseType, run: async () => [] }, o);
  if (o.established !== true) for (const statement of required(t.session, "its target's session")) await on.run(fromPlan(statement));
  return on.run(fromPlan(result.sql.statement));
}

/** A per-key region of a keyed body (ExecutionPlan.Region): the text [start, end) written once per key. */
interface Region {
  readonly start: number; readonly end: number; readonly lead: string; readonly separator: string;
  readonly empty: string | null; readonly firstSlot: number; readonly slotCount: number; readonly counters: readonly number[];
}
/** A keyed result (ExecutionPlan.Keyed): a dynamic pivot's keys statement, its outputs and its body. */
interface Keyed {
  readonly _type: 'keyed'; readonly keys: Sql; readonly keysOf: string; readonly keyFields: readonly string[];
  readonly outputs: readonly { readonly _type: string; readonly name?: number; readonly field: string; readonly column: Column }[];
  readonly perKeyAt: number; readonly body: Sql & { readonly regions?: readonly Region[] }; readonly snapshot: string;
  readonly maxColumns: number | null;
}

/**
 * A keyed body written for `keys` (Java's PlanRunner.expand, the plan's one allowed edit at run, the charter's A3(a)):
 * the text outside its regions as it is; each region once per key (`lead` before the first copy, `separator` between
 * copies, the key's position at each counter; with no key `empty`, or the statement refused where it has none); each
 * slot inside a region bound in every copy, a key's cell to that key's text. A slot outside a region binds a declared
 * parameter, which a host's plan takes none of: refused by name.
 */
export function expandKeyed(body: Sql & { readonly regions?: readonly Region[] }, keys: readonly (readonly string[])[]): { readonly statement: string; readonly params: PlanValue[] } {
  const text = body.statement;
  let out = '';
  const params: PlanValue[] = [];
  let from = 0;
  let slot = 0;
  for (const r of body.regions ?? []) {
    out += text.slice(from, r.start);
    if (slot < r.firstSlot) throw new PlanRefusal(`'${body.slots[slot]!.parameter}' is bound outside a per-key region: a host's plan binds no parameter`);
    if (keys.length === 0) {
      if (r.empty === null) throw new PlanRefusal('a dynamic pivot with no fixed column found no key: its relation has no column');
      out += r.empty;
    } else {
      out += r.lead;
      keys.forEach((key, k) => {
        if (k > 0) out += r.separator;
        let at = r.start;
        for (const counter of r.counters) {
          out += text.slice(at, counter) + String(k);
          at = counter;
        }
        out += text.slice(at, r.end);
        for (let s = r.firstSlot; s < r.firstSlot + r.slotCount; s++) {
          const binding = body.slots[s]!.binding as { readonly _type: string; readonly cell?: number };
          if (binding._type !== 'key') throw new PlanRefusal(`'${body.slots[s]!.parameter}' binds a ${binding._type} inside a per-key region: a host's plan binds a key's cell there alone`);
          const cell = key[binding.cell!];
          if (typeof cell !== 'string') throw new PlanRefusal(`a keys row's cell ${binding.cell} is ${String(cell)}: the probe writes each key's text and names`);
          params.push(cell);
        }
      });
    }
    slot = r.firstSlot + r.slotCount;
    from = r.end;
  }
  if (slot < body.slots.length) throw new PlanRefusal(`'${body.slots[slot]!.parameter}' is bound outside a per-key region: a host's plan binds no parameter`);
  return { statement: out + text.slice(from), params };
}

/**
 * A KEYED RESULT run (Java's PlanRunner.keyedOn): the keys statement read (each row the key's text and each per-key
 * output's name), the body written for those keys and run, in one transaction where the plan asks a snapshot; its
 * answer the relation's columns in their own order -- the first `perKeyAt` fixed ones, every key's per-key ones, the
 * rest of the fixed ones -- as lite's JSON rows. A raw grid's keys (the columns its result reports) are a test body's,
 * refused by name.
 */
async function keyedRows(keyed: Keyed, session: PlanSession): Promise<string> {
  if (keyed.keysOf !== 'ROWS') throw new PlanRefusal("a raw grid's keyed result (its keys the columns its result reports) is not run here: the planner runs it");
  const perKey = keyed.outputs.filter((o) => o._type === 'perKey').length;
  const snapshot = keyed.snapshot !== 'NONE';
  const read = async (): Promise<string> => {
    if (keyed.keys.slots.length > 0) throw new PlanRefusal("a keys statement binding a value is not run here: a host's plan binds none");
    const keys = (await session.run(fromPlan(keyed.keys.statement), [])).map((row) => row.slice(0, perKey + 1).map((cell) => {
      if (typeof cell !== 'string') throw new PlanRefusal(`a keys row holds ${String(cell)}: the probe writes each key's text and names`);
      return cell;
    }));
    const fixed = keyed.outputs.length - perKey;
    const width = fixed + keys.length * perKey;
    if (keyed.maxColumns !== null && width > keyed.maxColumns) {
      throw new PlanRefusal(`a dynamic pivot of ${keys.length} keys answers ${width} columns, more than the database's ${keyed.maxColumns}`);
    }
    // the body's columns in its own order (its fixed ones first), then placed in the relation's
    const answered: Column[] = keyed.outputs.filter((o) => o._type === 'fixed').map((o) => o.column);
    keys.forEach((key) => {
      for (const o of keyed.outputs) {
        if (o._type === 'perKey') answered.push({ ...o.column, name: key[o.name!]! });
      }
    });
    const at = keyed.perKeyAt;
    const place = answered.map((_c, i) => (i < at ? i : i < fixed ? i + (width - fixed) : i - (fixed - at)));
    const inOrder: Column[] = new Array<Column>(width);
    answered.forEach((c, i) => { inOrder[place[i]!] = c; });
    const body = expandKeyed(keyed.body, keys);
    const rows = (await session.run(fromPlan(body.statement), body.params)).map((row) => {
      const placed = new Array<unknown>(width);
      row.forEach((cell, i) => { placed[place[i]!] = cell; });
      return placed;
    });
    return rowsOf(inOrder, rows);
  };
  // the keys and the body read in one snapshot where the plan asks one: the target's transaction (version 50)
  return snapshot ? inTransaction(keyed.keys.target as Target, session.run, read) : read();
}

/**
 * `sql`'s target, of this session's database type: its loads (its declared test data, a project's Data, a data:
 * source's session table) run where the host made a database for the target and established it there (B3:
 * `establish`); on the host's own session a target that loads data is refused by name -- never run against tables no
 * load made. A target whose list is absent is no plan of version 32.
 */
function checkTarget(sql: Sql, session: PlanSession, o: { readonly established?: boolean }): Target {
  const t = sql.target as Target & { database?: { _type?: string; connection?: { databaseType?: string }; databaseType?: string } };
  const type = t.database?.connection?.databaseType ?? t.database?.databaseType;
  if (type !== session.databaseType) throw new PlanRefusal(`a plan of a ${String(type)} database is not run on this ${session.databaseType} one`);
  if (!Array.isArray(t.loads)) throw new PlanRefusal('a plan whose target names no loads list is not of this version');
  if (t.loads.length > 0 && o.established !== true) throw new PlanRefusal('a plan that loads data into its database first runs on a database the host made for it and established (establish): not on the host\'s own');
  return t;
}

/** A step's SQL; refused by name for a step a host's program does not plan. */
function stepSql(s: Step): Sql {
  // an effect: one statement sent for what it does (a snapshot's copy: Boundary.snapCopy); a script of several is not run
  if (s._type === 'effect' && (s as { readonly script?: unknown }).script != null) throw new PlanRefusal("an effect's script is not run here");
  if (s._type === 'update' || s._type === 'effect' || s._type === 'check' || s._type === 'tdsResult') return (s as { readonly sql: Sql }).sql;
  throw new PlanRefusal(s._type === 'sequence' ? 'a sequence inside a sequence' : `a plan's step is an update, an effect, a check or a typed result, not a ${s._type}`);
}

/** A constant answer's rows (PlanRunner.constantRows' rule): a String's cell its text, a Boolean's `true` or `false`,
 *  an absent value null; any other column the planner never writes, refused by name. */
function constantRows(c: Extract<Step, { _type: 'constant' }>): string {
  const [column] = c.columns;
  if (c.columns.length !== 1 || column === undefined) throw new PlanRefusal(`a constant answer is one column, not ${c.columns.length}`);
  const typed = c.cells.map((row) => row.map((text) => {
    if (text === null) return null;
    // the cell's kind by the column type's family (the one reader, types.ts), never by its name
    if (familyOf(column.type) === 'text') return text;
    if (isBoolean(column.type) && (text === 'true' || text === 'false')) return text === 'true';
    throw new PlanRefusal(`a constant answer of ${column.type} ('${text}'): the planner writes a String or a Boolean`);
  }));
  return rowsOf(c.columns, typed);
}

/** The count a statement that changes the database answers: its one row's first value. */
function countOf(rows: readonly (readonly unknown[])[]): number {
  const v = rows[0]?.[0];
  const n = typeof v === 'bigint' ? Number(v) : typeof v === 'number' ? v : typeof v === 'string' ? Number(v) : NaN;
  if (!Number.isSafeInteger(n)) throw new Error(`the database answered a change with no count: ${JSON.stringify(rows[0] ?? null)}`);
  return n;
}

/** A typed result's rows as lite's JSON rows (`[{"value":2}]`: what the planner's JsonRowsWriter writes for the same
 *  rows), each cell as the host's driver read it: an integer (a bigint within a safe integer, a number), a text, a
 *  boolean, a null -- a write's answer is its count; any other cell (a date, a decimal, a float, a structure) is the
 *  planner's to spell by the plan's column facts (executeAnswer), not this runner's: refused by name, never guessed. */
function rowsOf(columns: readonly Column[], rows: readonly (readonly unknown[])[]): string {
  const cell = (c: Column, v: unknown): string => {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'boolean') return String(v);
    if (typeof v === 'string') return JSON.stringify(v);
    if (typeof v === 'bigint' && Number.isSafeInteger(Number(v))) return String(v);
    if (typeof v === 'number' && Number.isSafeInteger(v)) return String(v);
    throw new PlanRefusal(`the host's runner writes an integer, a text or a boolean cell, and '${c.name}' (${c.type}) holds ${typeof v}: the planner writes it (executeAnswer)`);
  };
  return `[${rows.map((r) => `{${columns.map((c, i) => `${JSON.stringify(c.name)}:${cell(c, r[i])}`).join(',')}}`).join(',')}]`;
}
