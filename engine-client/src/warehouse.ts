// The warehouse as a QueryEngine: SQL runs on the server, as the signed-in
// user, and comes back as the same ResultTable the local plane produces.
//
// The LIVE plane of a warehouse source (docs/WAREHOUSE_D1_DESIGN_2026_09_26.md).
// Snapping copies what the user may read into DuckDB-WASM and runs there, so
// Live and Snap are one cube on two engines, one planner above both.
//
// The client below is a DRIVER of the one binding (warehouse/.../sqlapi/SqlApiCalls.java, docs/ONE_PLATFORM_PROGRAM
// _2026_10_12.md C3): the binding, compiled to WebAssembly (//warehouse:sqlapi_page, the page's vendor/warehouse/),
// names every call and reads every answer -- the very ones Python's driver (python/legend_lite/warehouse.py, through
// the native library) makes, by the binding the JVM's client uses too -- and this file only performs them: the HTTP, the clock, aborting, and the
// Arrow bytes, which stay here. One recorded corpus holds every driver to the same calls
// (warehouse/src/test/resources/sqlapi-corpus.json; engine-client/test/warehouse-corpus.test.ts).
//
// Results are Arrow IPC streams, one per chunk, read by Arrow JS -- the
// library DuckDB-WASM's own results come from -- and handed to the same
// `toRawTable` the local plane uses. Measured: for every type the warehouse
// returns, the cells are identical to DuckDB-WASM's (the D1 homework, H2).

import { UI_LOCALE } from './locale.ts';
import { Table, tableFromIPC, type RecordBatch } from 'apache-arrow';

import { QueryError, typedByPlan, type QueryEngine, type RawTable } from './engine.ts';
import type { Plan } from './relation-type.ts';
import { toRawTable, type ArrowishTable } from './duckdb.ts';
import { hostOf, type Receipt } from './receipt.ts';
import type { ResultTable } from './result.ts';
import { SessionExpired } from './warehouse-session.ts';

/** A signed-in session: where the warehouse is, and the bearer token. */
export interface WarehouseSession {
  /** e.g. `https://warehouse.example.com` (no trailing slash needed). */
  readonly baseUrl: string;
  readonly token: string;
  /** Who the server says the token is. */
  readonly principal: string;
  readonly expiresAt: string;
}

/** One table or view the user may read (the catalog call; W2 filters it). */
export interface CatalogObject {
  readonly schema: string;
  readonly name: string;
  readonly kind: string;
  /** The warehouse catalog it is in. */
  readonly catalog: string;
  /** That catalog's database type, as a Pure connection names it (`DuckDB`, `Postgres`): what its model declares. */
  readonly databaseType: string;
  /**
   * Its columns, as the warehouse's DuckDB catalog reports them: `type` its own name, and
   * STRUCTURED (as the catalog question reads one, DuckDb.CATALOG_COLUMNS_SQL) its canonical type and a
   * DECIMAL's precision and scale.
   */
  readonly columns: readonly {
    readonly name: string;
    readonly type: string;
    readonly logicalType: string | null;
    readonly precision: number | null;
    readonly scale: number | null;
    /** The catalog says it holds no NULL. */
    readonly notNull: boolean;
  }[];
}

interface ApiError {
  readonly code: string;
  readonly message: string;
}

/** One statement on the warehouse's record (`GET /sql/v1/history`), as the binding reads it. */
interface HistoryEntry {
  readonly statementId: string;
  readonly state: string;
  readonly submittedAt: string;
  readonly finishedAt?: string;
  readonly rowCount: number | null;
}

/** A statement's next step, as the binding reads a submit's or a poll's answer. */
type Step =
  | { readonly step: 'poll'; readonly statementId: string }
  | { readonly step: 'done'; readonly statementId: string; readonly rowCount?: number; readonly chunkCount: number }
  | { readonly step: 'failed'; readonly statementId?: string; readonly error: ApiError };

/** One HTTP call the binding names, relative to the warehouse's base URL. */
interface HttpCall {
  readonly method: string;
  readonly path: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: string | null;
}

/** A call the binding read as refused: the API's error, and whether the session it carried is gone. */
interface Refusal {
  readonly error: ApiError;
  readonly expired?: true;
}

/** The binding's module (//warehouse:sqlapi_page): its two exports, JSON in and out. */
export interface SqlApiModule {
  readonly exports: {
    call(op: string, args: string): string;
    read(op: string, status: number, body: string): string;
  };
}

/** THE ONE BINDING, loaded: every call this driver makes is named by it, and every answer read by it. */
export class WarehouseBinding {
  readonly #module: SqlApiModule;

  constructor(module: SqlApiModule) {
    this.#module = module;
  }

  /** The call `op` makes with `args`; a refusal of its arguments is this driver's bug, thrown. */
  call(op: string, args: Readonly<Record<string, unknown>>): HttpCall {
    const said = JSON.parse(this.#module.exports.call(op, JSON.stringify(args))) as HttpCall | Refusal;
    if ('error' in said) throw new Error(`the warehouse binding refused ${op}: ${said.error.code}: ${said.error.message}`);
    return said;
  }

  /** What the answer to `op`'s call says, or its refusal. */
  read<T>(op: string, status: number, body: string): T | Refusal {
    return JSON.parse(this.#module.exports.read(op, status, body)) as T | Refusal;
  }
}

/** The binding's module, from the directory holding its `classes.wasm` and TeaVM's runtime (`<vendor>warehouse/`). */
export async function loadWarehouseBinding(base: string, load?: (runtimeUrl: string) => Promise<unknown>): Promise<WarehouseBinding> {
  const dir = base.endsWith('/') ? base : `${base}/`;
  const runtimeUrl = `${dir}wasm-gc-module-runtime.js`;
  const runtime = (load ? await load(runtimeUrl) : await import(/* @vite-ignore */ runtimeUrl)) as {
    load(src: string, options?: unknown): Promise<SqlApiModule>;
  };
  return new WarehouseBinding(await runtime.load(`${dir}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  }));
}

/** A refusal: an error and no step (a statement's failed step carries its error too, and is an answer). */
const refused = (said: unknown): said is Refusal => typeof said === 'object' && said !== null && 'error' in said
  && !('step' in said);
const words = (r: Refusal): string => `${r.error.code}: ${r.error.message}`;

/** `fetch`, with a refused connection said as what it is: the browser's own words ("Failed to fetch") name neither
 *  the server nor the fact it could not be reached. */
async function reach(baseUrl: string, call: HttpCall, signal?: AbortSignal): Promise<Response> {
  try {
    return await fetch(baseUrl.replace(/\/+$/, '') + call.path, {
      method: call.method,
      headers: call.headers,
      ...(call.body === null ? {} : { body: call.body }),
      ...(signal ? { signal } : {}),
    });
  } catch (error: unknown) {
    if (signal?.aborted) throw error;
    throw new Error(`cannot reach the warehouse at ${baseUrl} — ${error instanceof Error ? error.message : String(error)}`,
      { cause: error });
  }
}

/** One call the binding names, made, and its answer as the binding reads it. */
async function exchange<T>(binding: WarehouseBinding, baseUrl: string, op: string, args: Readonly<Record<string, unknown>>,
  signal?: AbortSignal): Promise<T | Refusal> {
  const r = await reach(baseUrl, binding.call(op, args), signal);
  return binding.read<T>(op, r.status, await r.text());
}

interface Token { readonly token: string; readonly expiresAt: string; readonly principal: string }

/**
 * Sign in and get a token. The DEVELOPMENT sign-in: the warehouse's own
 * user list (`--user name:password`). The password is sent once and not kept;
 * only the token is. Production replaces this with the company's single
 * sign-on, whose token the warehouse verifies -- nothing else here changes.
 */
export async function signIn(binding: WarehouseBinding, baseUrl: string, user: string, password: string): Promise<WarehouseSession> {
  const t = await exchange<Token>(binding, baseUrl, 'login', { user, password });
  if (refused(t)) throw new Error(`sign-in failed — ${words(t)}`);
  return { baseUrl, token: t.token, principal: t.principal, expiresAt: t.expiresAt };
}

/**
 * The single-user app's sign-in (docs/DATACUBE_APP_PLAN_2026_10_02.md): the launch key the warehouse
 * printed and put in this page's address, for a token like a password's -- refreshed, and renewed, the same.
 */
export async function signInWithKey(binding: WarehouseBinding, baseUrl: string, key: string): Promise<WarehouseSession> {
  const t = await exchange<Token>(binding, baseUrl, 'loginKey', { key });
  if (refused(t)) throw new Error(`sign-in failed — ${words(t)}`);
  return { baseUrl, token: t.token, principal: t.principal, expiresAt: t.expiresAt };
}

/**
 * What the user may read in EVERY catalog of the warehouse, in one call: each table or view with its
 * columns, its catalog, and that catalog's database type -- the SQL its statements are written in,
 * as a Pure connection names it (`DuckDB`, `Postgres`). An attached catalog the user holds no USAGE of
 * is not listed.
 */
export async function listObjects(binding: WarehouseBinding, session: WarehouseSession): Promise<CatalogObject[]> {
  const objects = await exchange<CatalogObject[]>(binding, session.baseUrl, 'objects', { token: session.token });
  if (refused(objects)) throw new Error(`could not list the tables — ${words(objects)}`);
  return objects;
}

/**
 * Sign in AND list what the user may read, as one step: the session comes back only with its
 * own tables. Signing in, then listing separately, left one user's table list beside another
 * user's session when the listing failed (P2-334).
 */
export async function connect(binding: WarehouseBinding, baseUrl: string, user: string, password: string):
Promise<{ readonly session: WarehouseSession; readonly objects: CatalogObject[] }> {
  const session = await signIn(binding, baseUrl, user, password);
  const objects = await listObjects(binding, session);
  return { session, objects };
}

// the session's end, as its own error (warehouse-session.ts: a page's code reads it without loading this driver)
export { SessionExpired, sessionExpired } from './warehouse-session.ts';

/** The time a WarehouseEngine reads and the timer it schedules its token refresh on: the system's, or a test's own
 *  (Bazel workplan P3-16: a test proves the refresh by advancing a clock, never by sleeping). */
export interface Clock {
  now(): number;
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
}

export const SYSTEM_CLOCK: Clock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => {
    const handle = setTimeout(fn, ms);
    // a timer must not keep a process alive (node: a test, a CLI) for a page's convenience
    (handle as { unref?: () => void }).unref?.();
    return handle;
  },
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/** A statement that binds values (a plan's data: payloads, M3) is not sent to the warehouse, which runs SQL text alone:
 *  refused by name, never sent without them. */
function refuseParams(sql: string, params: readonly string[] | undefined): void {
  if (params !== undefined && params.length > 0) {
    throw new QueryError(`this statement binds ${params.length} value(s) (a data: source's payload), which the warehouse does not take: `
      + 'it runs on the tab\'s own database or legend-lite\'s server', sql);
  }
}

export class WarehouseEngine implements QueryEngine {
  readonly name = 'warehouse';
  /** Its catalog's database type (`DuckDB`, `Postgres`): what a statement run on it is written for (Snap's count). */
  readonly databaseType: string;
  readonly #binding: WarehouseBinding;
  /** Renewed in place when the same user signs in again (`renew`) or the token is refreshed. */
  #session: WarehouseSession;
  readonly #catalog: string;
  /** The next refresh of the token, before it expires (`#schedule`). */
  #refreshTimer: unknown;
  readonly #clock: Clock;

  /** `databaseType`: the catalog's, as the warehouse lists it (CatalogObject.databaseType) -- never assumed. */
  constructor(binding: WarehouseBinding, session: WarehouseSession, catalog: string, databaseType: string,
    clock: Clock = SYSTEM_CLOCK) {
    this.databaseType = databaseType;
    this.#binding = binding;
    this.#session = session;
    this.#catalog = catalog;
    this.#clock = clock;
    this.#schedule();
  }

  /**
   * KEEP THE SIGN-IN ALIVE while the page is open: swap the token for a fresh one at 80% of its
   * life (`POST /sql/v1/token/refresh`), so nobody is asked for a password mid-work. The server
   * stops refreshing at its session limit; after that, and after a sleep that outlasted the
   * token, the next query says the session expired and the cube offers to sign in again.
   */
  #schedule(): void {
    this.#clock.clearTimeout(this.#refreshTimer);
    const left = Date.parse(this.#session.expiresAt) - this.#clock.now();
    if (!Number.isFinite(left) || left <= 0) return;
    this.#refreshTimer = this.#clock.setTimeout(() => {
      this.refreshToken().catch(() => {
        // not refreshed (the session's limit, the server gone): the next query says so, and
        // the cube offers to sign in -- a background timer has no one to tell
      });
    }, Math.max(1_000, left * 0.8));
  }

  /** Swap the token for a fresh one now: same user, a new expiry. */
  async refreshToken(): Promise<void> {
    const t = await this.#call<Token>('refresh', {});
    this.renew({ baseUrl: this.#session.baseUrl, token: t.token, principal: t.principal, expiresAt: t.expiresAt });
  }

  /** When the token in use expires (the server's word), for a host to show. */
  get expiresAt(): string {
    return this.#session.expiresAt;
  }

  /** The signed-in user, for the plane badge. */
  get principal(): string {
    return this.#session.principal;
  }

  /**
   * A fresh token for the SAME user at the same warehouse: the open cube goes on with it. The
   * engine kept the token it was built with for its whole life, so signing in again after
   * expiry -- as the error asked -- never reached the cube (P2-297). Anyone else's session is
   * refused: what a cube reads is its user's, and it never changes hands quietly.
   */
  /** Where this engine's warehouse is. */
  get baseUrl(): string {
    return this.#session.baseUrl;
  }

  /**
   * Sign in again as THIS engine's user at THIS engine's warehouse, and go on with the new
   * token: the one thing a person needs after the warehouse restarted. The password is sent
   * once and not kept.
   */
  async signInAgain(password: string): Promise<void> {
    this.renew(await signIn(this.#binding, this.#session.baseUrl, this.#session.principal, password));
  }

  renew(session: WarehouseSession): void {
    if (session.principal !== this.#session.principal
      || session.baseUrl.replace(/\/+$/, '') !== this.#session.baseUrl.replace(/\/+$/, '')) {
      throw new Error(`this cube reads the warehouse as ${this.#session.principal}; `
        + `signed in as ${session.principal}, open a table to work as ${session.principal}`);
    }
    this.#session = session;
    this.#schedule();
  }

  /**
   * Run `sql` on the warehouse and return its rows as the local plane would.
   *
   * `signal` aborts at once: the in-flight request stops and the statement is
   * cancelled on the server.
   */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return typedByPlan(await this.run(plan.sql, epoch, signal, plan.params), plan);
  }

  /** A planned query's rows chunk by chunk, as the server wrote them (its Arrow chunks). */
  async stream(
    plan: Plan,
    epoch: number,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    refuseParams(plan.sql, plan.params);
    const started = performance.now();
    try {
      for await (const bytes of this.arrowChunks(plan.sql, signal)) {
        const table = tableFromIPC(bytes) as unknown as ArrowishTable;
        onChunk(typedByPlan(toRawTable(table, epoch, performance.now() - started), plan));
      }
    } catch (error: unknown) {
      if (signal?.aborted || error instanceof QueryError) throw error;
      throw new QueryError(error instanceof Error ? error.message : String(error), plan.sql, { cause: error });
    }
  }

  async run(sql: string, epoch: number, signal?: AbortSignal, params?: readonly string[]): Promise<RawTable> {
    refuseParams(sql, params);
    const started = performance.now();
    const batches: RecordBatch[] = [];
    let receipt: Receipt | undefined;
    try {
      for await (const bytes of this.arrowChunks(sql, signal, (r) => { receipt = r; })) {
        batches.push(...tableFromIPC(bytes).batches);
      }
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryError(error instanceof Error ? error.message : String(error), sql, { cause: error });
    }
    const table = new Table(batches) as unknown as ArrowishTable;
    const raw = toRawTable(table, epoch, performance.now() - started);
    return receipt ? { ...raw, receipt } : raw;
  }

  /**
   * Ask the warehouse, apart from the query, whether statement `id` is on its record for this
   * user: its history (`GET /sql/v1/history`), which lists only the caller's own statements.
   */
  async check(id: string): Promise<string> {
    const history = await this.#call<readonly HistoryEntry[]>('history', { limit: 1000 });
    const found = history.find((h) => h.statementId === id);
    const who = this.#session.principal;
    if (!found) return `Not on the warehouse's record for ${who}: no statement ${id} in its last ${history.length}.`;
    const rows = found.rowCount === null ? 'its rows uncounted' : `${found.rowCount.toLocaleString(UI_LOCALE)} rows`;
    return `On the warehouse's record for ${who}: statement ${id}, ${found.state}, `
      + `${rows}, submitted ${new Date(found.submittedAt).toLocaleTimeString(UI_LOCALE)}`
      + (found.finishedAt ? `, finished ${new Date(found.finishedAt).toLocaleTimeString(UI_LOCALE)}` : '') + '.';
  }

  /**
   * The result of `sql` as the server wrote it: one Arrow IPC stream per chunk,
   * in order. What a snap loads into DuckDB-WASM as it is, unconverted.
   * The statement is closed once every chunk is read, so the server frees it.
   */
  async *arrowChunks(
    sql: string,
    signal?: AbortSignal,
    onReceipt?: (receipt: Receipt) => void,
  ): AsyncGenerator<Uint8Array> {
    let step = await this.#call<Step>('submit', { sql, catalog: this.#catalog }, signal);
    const id = step.statementId;
    // where a read that ends early leaves its statement: still running, cancelled; done, its result closed; read to the
    // end (or failed there), nothing
    let ending: 'cancel' | 'closeStatement' | undefined = 'cancel';
    try {
      while (step.step === 'poll') step = await this.#call<Step>('poll', { statementId: step.statementId }, signal);
      if (step.step === 'failed') {
        ending = undefined;
        throw new Error(`${step.error.code}: ${step.error.message}`);
      }
      const done = step;
      ending = 'closeStatement';
      // What the SERVER issued for it: its id and its count, never the tab's.
      onReceipt?.({
        plane: 'warehouse',
        where: `the warehouse at ${hostOf(this.#session.baseUrl)}`,
        as: this.#session.principal,
        statementId: done.statementId,
        ...(done.rowCount !== undefined ? { serverRows: done.rowCount } : {}),
        check: () => this.check(done.statementId),
      });
      for (let index = 0; index < done.chunkCount; index++) {
        // a chunk's bytes are this driver's: only a failed chunk's answer is the binding's to read
        const r = await reach(this.#session.baseUrl, this.#binding.call('fetchChunk', { statementId: done.statementId, index, ...this.#token() }), signal);
        if (r.status !== 200) this.#refuse(this.#binding.read('fetchChunk', r.status, await r.text()));
        yield new Uint8Array(await r.arrayBuffer());
      }
    } finally {
      // every chunk read, or the read ended early: the server may free the result now, not at expiry, and stop a
      // statement still running; what it answers changes nothing here (the rows were read, or the read has failed)
      if (ending !== undefined && id !== undefined) await this.#call(ending, { statementId: id }).catch(() => undefined);
    }
  }

  async close(): Promise<void> {
    // nothing held open: every statement is closed as it finishes; the refresh stops
    this.#clock.clearTimeout(this.#refreshTimer);
  }

  #token(): { readonly token: string } {
    return { token: this.#session.token };
  }

  /** A refused answer, raised: an expired session as its own error, so the cube can offer to sign in again. */
  #refuse(said: unknown): void {
    if (!refused(said)) return;
    if (said.expired) throw new SessionExpired(this.#session.baseUrl, this.#session.principal);
    throw new Error(words(said));
  }

  /** One call the binding names, with this engine's token, and its answer as the binding reads it. */
  async #call<T>(op: string, args: Readonly<Record<string, unknown>>, signal?: AbortSignal): Promise<T> {
    const said = await exchange<T>(this.#binding, this.#session.baseUrl, op, { ...args, ...this.#token() }, signal);
    this.#refuse(said);
    return said as T;
  }
}
