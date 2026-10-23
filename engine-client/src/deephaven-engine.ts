// A DEEPHAVEN SERVER AS THE TAB'S ENGINE (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23): a planned query's plan sent straight
// from the tab (deephaven.ts) and its rows read back typed by the plan, so a grid runs on Deephaven as it runs on the
// tab's DuckDB -- `new CubeController(engine, planner)`, the planner planning on a Deephaven runtime. What it adds is
// Deephaven's own: the session holds the tables each query made, so the grid's next query over the same source reads
// them rather than making them again (a regroup, a repivot, a filter sends only what differs); and the server says
// when a result the grid read has changed (`onTick`), so the grid can read it again. A server that restarts keeps no
// session: `onTick` says the session ended, and `reconnect` signs in again.

import type { DeephavenPlanner, DeephavenServer, DeephavenSessionOptions, HeldResult } from './deephaven.ts';
import { DeephavenSession } from './deephaven.ts';
import { byPosition, deephavenRows } from './deephaven-rows.ts';
import { QueryError, type QueryEngine, type RawTable } from './engine.ts';
import { hostOf, type Receipt } from './receipt.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable } from './result.ts';

/**
 * What the server said since the last tick: the epochs whose results changed, the results that failed (a to-one join
 * that met a second match, DH20), and -- when the session's update stream ended (the server went away, or restarted:
 * Deephaven keeps no session across one) -- why; `reconnect` then signs in again.
 */
export interface Ticked {
  readonly epochs: ReadonlySet<number>;
  readonly failures: readonly string[];
  readonly ended: string | null;
}

/** The engine's measure of itself, for a page that shows it and a test that checks it. */
export interface DeephavenCounts {
  /** Plans executed, and the operations in them. */
  readonly queries: number;
  readonly planned: number;
  /** Operations sent to the server to answer them: a query over a source the session holds sends only its own. */
  readonly sent: number;
  /** Plans whose result the session held already: nothing sent, the rows read again. */
  readonly held: number;
  /** Tables the session made again after letting them go (deephaven.ts `remade`). */
  readonly remade: number;
  /** Sessions signed in again (`reconnect`). */
  readonly reconnects: number;
}

export class DeephavenEngine implements QueryEngine {
  readonly name = 'deephaven';
  readonly #server: DeephavenServer;
  readonly #planner: DeephavenPlanner;
  readonly #options: DeephavenSessionOptions;
  #session: DeephavenSession;
  /** The epoch each result was last read for, by its export (in the session now). */
  readonly #reads = new Map<number, number>();
  readonly #tickers = new Set<(ticked: Ticked) => void>();
  #stopUpdates: (() => void) | null = null;
  #counts = { queries: 0, planned: 0, sent: 0, held: 0, remade: 0, reconnects: 0 };

  private constructor(server: DeephavenServer, planner: DeephavenPlanner, options: DeephavenSessionOptions,
    session: DeephavenSession) {
    this.#server = server;
    this.#planner = planner;
    this.#options = options;
    this.#session = session;
  }

  /** Signs in to `server` (deephaven.ts `DeephavenSession.open`). */
  static async open(server: DeephavenServer, planner: DeephavenPlanner,
    options: DeephavenSessionOptions = {}): Promise<DeephavenEngine> {
    return new DeephavenEngine(server, planner, options, await DeephavenSession.open(server, planner, options));
  }

  /** The session queries go to now. */
  get session(): DeephavenSession {
    return this.#session;
  }

  get counts(): DeephavenCounts {
    return { ...this.#counts, remade: this.#counts.remade + this.#session.remade };
  }

  /**
   * The plan's result made (or read from what the session holds), its rows read as they are now, typed by the plan.
   * A Deephaven server cannot stop a batch it has started; `signal` is checked before the plan is sent and before the
   * rows are read.
   */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const started = performance.now();
    const session = this.#session;
    let held: HeldResult;
    try {
      held = await session.hold(plan.sql);
    } catch (e) {
      throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
    }
    try {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const stream = await session.rows(held.exportId);
      if (session === this.#session) this.#reads.set(held.exportId, epoch);
      this.#counts.queries++;
      this.#counts.planned += held.ops;
      this.#counts.sent += held.sent;
      if (held.sent === 0) this.#counts.held++;
      let raw: RawTable;
      try {
        raw = deephavenRows(stream, epoch, performance.now() - started);
      } catch (e) {
        throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
      }
      return { ...byPosition(raw, plan), receipt: this.#receipt() };
    } finally {
      held.release();
    }
  }

  /** A Deephaven result is read whole: handed over as one chunk. */
  async stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    onChunk(await this.execute(plan, epoch, signal));
  }

  /** A Deephaven server runs plans, not SQL text: nothing reaches it but a plan. */
  async run(sql: string): Promise<RawTable> {
    throw new QueryError('a Deephaven server runs a planned query, not SQL text: it has no statement to run', sql);
  }

  /**
   * Calls `listener` when a result the engine read has changed on the server -- with the epochs it was read for, the
   * latest read of each -- or failed, or when the session's update stream ended. At most once each update stream read
   * (`updatesMs`). Returns what stops it.
   */
  onTick(listener: (ticked: Ticked) => void): () => void {
    this.#tickers.add(listener);
    if (this.#stopUpdates === null) this.#listen();
    return () => {
      this.#tickers.delete(listener);
      if (this.#tickers.size === 0 && this.#stopUpdates !== null) {
        this.#stopUpdates();
        this.#stopUpdates = null;
      }
    };
  }

  #listen(): void {
    const session = this.#session;
    this.#stopUpdates = session.onUpdates((updates) => {
      const epochs = new Set<number>();
      const failures: string[] = [];
      for (const u of updates) {
        const epoch = this.#reads.get(u.exportId);
        if (epoch === undefined) continue;
        if (u.failure) failures.push(u.failure);
        else epochs.add(epoch);
      }
      // a result the session no longer holds ticks no more
      for (const id of this.#reads.keys()) if (!session.holds(id)) this.#reads.delete(id);
      if (epochs.size === 0 && failures.length === 0) return;
      for (const t of this.#tickers) t({ epochs, failures, ended: null });
    }, (why) => {
      const ended = why instanceof Error ? why.message : String(why);
      for (const t of this.#tickers) t({ epochs: new Set(), failures: [], ended });
    });
  }

  /**
   * Signs in again: a new session, holding nothing (the old one's tables went with it), its update stream reopened
   * while anything listens. Throws while the server does not answer; the old session is let go either way.
   */
  async reconnect(): Promise<void> {
    const session = await DeephavenSession.open(this.#server, this.#planner, this.#options);
    const old = this.#session;
    this.#stopUpdates?.();
    this.#stopUpdates = null;
    this.#counts.remade += old.remade;
    this.#session = session;
    this.#reads.clear();
    this.#counts.reconnects++;
    old.close().catch(() => { /* the server that held it is gone */ });
    if (this.#tickers.size > 0) this.#listen();
  }

  async close(): Promise<void> {
    this.#stopUpdates?.();
    this.#stopUpdates = null;
    await this.#session.close();
  }

  #receipt(): Receipt {
    return { plane: 'warehouse', where: `the Deephaven server at ${hostOf(this.#server.url)}` };
  }
}
