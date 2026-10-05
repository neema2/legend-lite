// The warehouse as a QueryEngine: SQL runs on the server, as the signed-in
// user, and comes back as the same ResultTable the local plane produces.
//
// The LIVE plane of a warehouse source (docs/WAREHOUSE_D1_DESIGN_2026_09_26.md).
// Snapping copies what the user may read into DuckDB-WASM and runs there, so
// Live and Snap are one cube on two engines, one planner above both.
//
// The client below speaks the warehouse's HTTP SQL API. Its reference is the
// Java binding every JVM client uses (warehouse/.../sqlapi/NativeBinding.java):
// the same requests, the same states, the same close-after-reading. The API is
// ours and two clients speak it; both are held to the real server in the chain
// (the warehouse suite; the Live-versus-Snap test), and a spec replaces this
// hand spelling the day a third client or an outside user appears.
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
   * STRUCTURED (catalog-model.ts) its canonical type and a DECIMAL's precision and scale.
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

/** One statement on the warehouse's record (`GET /sql/v1/history`), as it writes it. */
interface HistoryEntry {
  readonly statementId: string;
  readonly state: string;
  readonly submittedAt: string;
  readonly finishedAt?: string;
  readonly rowCount: string | number;
}

interface StatementStatus {
  readonly statementId: string;
  readonly state: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  readonly result?: { readonly rowCount: number; readonly chunkCount: number };
  readonly error?: ApiError;
}

/** How long one poll may wait on the server (NativeBinding's pollWaitMs). */
const POLL_WAIT_MS = 10_000;
/** Rows per Arrow chunk: whole DuckDB batches of 2,048 each. */
const ROWS_PER_CHUNK = 100_000;
const TIMEOUT_MS = 300_000;

function url(base: string, path: string): string {
  return base.replace(/\/+$/, '') + path;
}

/** The API's own error, when a failed reply carries one; else the status line. */
async function failure(r: Response): Promise<string> {
  const text = await r.text();
  try {
    const e = (JSON.parse(text) as { error?: ApiError }).error;
    if (e) return `${e.code}: ${e.message}`;
  } catch {
    // not one of ours (a proxy's page, say): fall through
  }
  return `HTTP ${r.status}${text ? `: ${text.slice(0, 200)}` : ''}`;
}

/**
 * Sign in and get a token. The DEVELOPMENT sign-in: the warehouse's own
 * user list (`--user name:password`). The password is sent once and not kept;
 * only the token is. Production replaces this with the company's single
 * sign-on, whose token the warehouse verifies -- nothing else here changes.
 */
export async function signIn(baseUrl: string, user: string, password: string): Promise<WarehouseSession> {
  const r = await fetch(url(baseUrl, '/sql/v1/login'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user, password }),
  });
  if (!r.ok) throw new Error(`sign-in failed — ${await failure(r)}`);
  const t = await r.json() as { token: string; expiresAt: string; principal: string };
  return { baseUrl, token: t.token, principal: t.principal, expiresAt: t.expiresAt };
}

/**
 * The single-user app's sign-in (docs/DATACUBE_APP_PLAN_2026_10_02.md): the launch key the warehouse
 * printed and put in this page's address, for a token like a password's -- refreshed, and renewed, the same.
 */
export async function signInWithKey(baseUrl: string, key: string): Promise<WarehouseSession> {
  const r = await fetch(url(baseUrl, '/sql/v1/login'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key }),
  });
  if (!r.ok) throw new Error(`sign-in failed — ${await failure(r)}`);
  const t = await r.json() as { token: string; expiresAt: string; principal: string };
  return { baseUrl, token: t.token, principal: t.principal, expiresAt: t.expiresAt };
}

/**
 * What the user may read in EVERY catalog of the warehouse, in one call: each table or view with its
 * columns, its catalog, and that catalog's database type -- the SQL its statements are written in,
 * as a Pure connection names it (`DuckDB`, `Postgres`). An attached catalog the user holds no USAGE of
 * is not listed.
 */
export async function listObjects(session: WarehouseSession): Promise<CatalogObject[]> {
  const r = await fetch(url(session.baseUrl, '/sql/v1/objects'), {
    headers: { Authorization: `Bearer ${session.token}` },
  });
  if (!r.ok) throw new Error(`could not list the tables — ${await failure(r)}`);
  return await r.json() as CatalogObject[];
}

/**
 * Sign in AND list what the user may read, as one step: the session comes back only with its
 * own tables. Signing in, then listing separately, left one user's table list beside another
 * user's session when the listing failed (P2-334).
 */
export async function connect(baseUrl: string, user: string, password: string):
Promise<{ readonly session: WarehouseSession; readonly objects: CatalogObject[] }> {
  const session = await signIn(baseUrl, user, password);
  const objects = await listObjects(session);
  return { session, objects };
}

/**
 * The warehouse refused the token: expired, or issued by a warehouse that has since restarted.
 * Its own type, so the cube can offer to sign in again right where it says so.
 */
export class SessionExpired extends Error {
  readonly baseUrl: string;
  readonly principal: string;
  constructor(baseUrl: string, principal: string) {
    super(`the warehouse session for ${principal} has expired — sign in again`);
    this.name = 'SessionExpired';
    this.baseUrl = baseUrl;
    this.principal = principal;
  }
}

/** Whether `error`, or anything in its cause chain, is a `SessionExpired`. */
export function sessionExpired(error: unknown): SessionExpired | null {
  for (let e = error, depth = 0; e && depth < 10; e = (e as { cause?: unknown }).cause, depth++) {
    if (e instanceof SessionExpired) return e;
  }
  return null;
}

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

export class WarehouseEngine implements QueryEngine {
  readonly name = 'warehouse';
  /** Renewed in place when the same user signs in again (`renew`) or the token is refreshed. */
  #session: WarehouseSession;
  readonly #catalog: string;
  /** The next refresh of the token, before it expires (`#schedule`). */
  #refreshTimer: unknown;
  readonly #clock: Clock;

  constructor(session: WarehouseSession, catalog = 'main', clock: Clock = SYSTEM_CLOCK) {
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
    const t = await this.#call<{ token: string; expiresAt: string; principal: string }>('POST', '/sql/v1/token/refresh');
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
    this.renew(await signIn(this.#session.baseUrl, this.#session.principal, password));
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
    return typedByPlan(await this.run(plan.sql, epoch, signal), plan);
  }

  /** A planned query's rows chunk by chunk, as the server wrote them (its Arrow chunks). */
  async stream(
    plan: Plan,
    epoch: number,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
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

  async run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
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
    const history = await this.#call<readonly HistoryEntry[]>('GET', '/sql/v1/history?limit=1000');
    const found = history.find((h) => h.statementId === id);
    const who = this.#session.principal;
    if (!found) return `Not on the warehouse's record for ${who}: no statement ${id} in its last ${history.length}.`;
    return `On the warehouse's record for ${who}: statement ${id}, ${found.state}, `
      + `${Number(found.rowCount).toLocaleString(UI_LOCALE)} rows, submitted ${new Date(found.submittedAt).toLocaleTimeString(UI_LOCALE)}`
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
    let status = await this.#call<StatementStatus>('POST', '/sql/v1/statements', signal, {
      sql,
      catalog: this.#catalog,
      timeoutMs: TIMEOUT_MS,
      waitMs: POLL_WAIT_MS,
      rowsPerChunk: ROWS_PER_CHUNK,
      resultFormat: 'arrow',
    });
    const id = status.statementId;
    try {
      while (status.state === 'queued' || status.state === 'running') {
        status = await this.#call<StatementStatus>('GET', `/sql/v1/statements/${id}?waitMs=${POLL_WAIT_MS}`, signal);
      }
      if (status.state !== 'succeeded') {
        const e = status.error;
        throw new Error(e ? `${e.code}: ${e.message}` : `the statement ended ${status.state}`);
      }
      // What the SERVER issued for it: its id and its count, never the tab's.
      onReceipt?.({
        plane: 'warehouse',
        where: `the warehouse at ${hostOf(this.#session.baseUrl)}`,
        as: this.#session.principal,
        statementId: id,
        ...(status.result ? { serverRows: Number(status.result.rowCount) } : {}),
        check: () => this.check(id),
      });
      const chunks = status.result?.chunkCount ?? 0;
      for (let i = 0; i < chunks; i++) {
        const r = await this.#fetch(url(this.#session.baseUrl, `/sql/v1/statements/${id}/chunks/${i}`), {
          headers: this.#auth(),
          ...(signal ? { signal } : {}),
        });
        if (r.status === 401) throw new SessionExpired(this.#session.baseUrl, this.#session.principal);
        if (!r.ok) throw new Error(await failure(r));
        yield new Uint8Array(await r.arrayBuffer());
      }
      // every chunk read: the server may free the result now, not at expiry
      await fetch(url(this.#session.baseUrl, `/sql/v1/statements/${id}`), { method: 'DELETE', headers: this.#auth() });
    } catch (error: unknown) {
      if (signal?.aborted) {
        // stop the server's work too; the page has already moved on
        void fetch(url(this.#session.baseUrl, `/sql/v1/statements/${id}/cancel`), {
          method: 'POST', headers: this.#auth(),
        }).catch(() => undefined);
      }
      throw error;
    }
  }

  async close(): Promise<void> {
    // nothing held open: every statement is closed as it finishes; the refresh stops
    this.#clock.clearTimeout(this.#refreshTimer);
  }

  #auth(): Record<string, string> {
    return { Authorization: `Bearer ${this.#session.token}` };
  }

  /**
   * `fetch`, with a refused connection said as what it is. The browser's own words are
   * "Failed to fetch", which names neither the server nor the fact it could not be reached.
   */
  async #fetch(target: string, init: RequestInit): Promise<Response> {
    try {
      return await fetch(target, init);
    } catch (error: unknown) {
      if (init.signal?.aborted) throw error;
      throw new Error(`cannot reach the warehouse at ${this.#session.baseUrl} — `
        + `${error instanceof Error ? error.message : String(error)}`, { cause: error });
    }
  }

  async #call<T>(method: string, path: string, signal?: AbortSignal, body?: unknown): Promise<T> {
    const r = await this.#fetch(url(this.#session.baseUrl, path), {
      method,
      headers: body === undefined ? this.#auth() : { ...this.#auth(), 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      ...(signal ? { signal } : {}),
    });
    if (r.status === 401) throw new SessionExpired(this.#session.baseUrl, this.#session.principal);
    if (!r.ok && r.status !== 202) throw new Error(await failure(r));
    return await r.json() as T;
  }
}
