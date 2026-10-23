// A DEEPHAVEN SERVER AS THE TAB'S ENGINE (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23, DH24): a planned query's plan sent
// straight from the tab (deephaven.ts) and its rows read back typed by the plan, so a grid runs on Deephaven as it runs
// on the tab's DuckDB -- `new CubeController(engine, planner)`, the planner planning on a Deephaven runtime. The session
// keeps alive the tables each query made, so the grid's next query over the same source builds on them (a regroup, a
// repivot, a filter sends only what differs). And it TICKS: `follow` keeps every result a view read live in the page
// too -- Deephaven's JS library pushes each one's rows as the server's update cycle changes them (deephaven-live.ts)
// -- so the grid runs again with no query sent and no row fetched: `execute` answers a followed plan from its live
// rows, and `onLive` says at once that one changed.

import type { DeephavenPlanner, DeephavenServer, DeephavenSessionOptions, KeptResult } from './deephaven.ts';
import { DeephavenSession } from './deephaven.ts';
import { LiveClient, type DhModule, type Followed } from './deephaven-live.ts';
import { byPosition, deephavenRows, followedTable } from './deephaven-rows.ts';
import { QueryError, type QueryEngine, type RawTable } from './engine.ts';
import { hostOf, type Receipt } from './receipt.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable } from './result.ts';

/** What the live side says: a followed result changed (`since`: when the first change since the last word arrived,
 *  `performance.now()`), or the live connection ended (`ended`: why) -- the server went away; `reconnect`. */
export interface LiveEvent {
  readonly since: number;
  readonly ended: string | null;
}

/** The engine's measure of itself, for a page that shows it and a test that checks it. */
export interface DeephavenCounts {
  /** Plans executed, and the operations in them. */
  readonly queries: number;
  readonly planned: number;
  /** Operations sent to the server to answer them: a query over a source the session keeps alive sends only its own. */
  readonly sent: number;
  /** Results read from the server (Arrow Flight's DoGet), and results answered from their live rows in the page. */
  readonly fetched: number;
  readonly live: number;
  /** Results followed now. */
  readonly following: number;
  /** Tables the session made again after letting them go (deephaven.ts `remade`). */
  readonly remade: number;
  /** Sessions signed in again (`reconnect`). */
  readonly reconnects: number;
}

/** A plan followed: its live rows once the first update lands, and how to stop. */
interface Following {
  table: ResultTable | null;
  stop: (() => void) | null;
  stopped: boolean;
}

export interface DeephavenEngineOptions extends DeephavenSessionOptions {
  /** How Deephaven's JS library is loaded (a test's own); from the server by default. */
  readonly loadLive?: (url: string) => Promise<DhModule>;
}

export class DeephavenEngine implements QueryEngine {
  readonly name = 'deephaven';
  readonly #server: DeephavenServer;
  readonly #planner: DeephavenPlanner;
  readonly #options: DeephavenEngineOptions;
  #session: DeephavenSession;
  #live: Promise<LiveClient> | null = null;
  /** The plans each recent epoch executed, by their text: what `follow` follows. */
  readonly #executed = new Map<number, Map<string, Plan>>();
  readonly #following = new Map<string, Following>();
  readonly #listeners = new Set<(e: LiveEvent) => void>();
  /** The grids' bases kept on the server (B1), by the name a query reads each by. */
  readonly #bases = new Map<string, KeptResult>();
  #changedSince: number | null = null;
  #counts = { queries: 0, planned: 0, sent: 0, fetched: 0, live: 0, remade: 0, reconnects: 0 };

  private constructor(server: DeephavenServer, planner: DeephavenPlanner, options: DeephavenEngineOptions,
    session: DeephavenSession) {
    this.#server = server;
    this.#planner = planner;
    this.#options = options;
    this.#session = session;
  }

  /** Signs in to `server` (deephaven.ts `DeephavenSession.open`). */
  static async open(server: DeephavenServer, planner: DeephavenPlanner,
    options: DeephavenEngineOptions = {}): Promise<DeephavenEngine> {
    return new DeephavenEngine(server, planner, options, await DeephavenSession.open(server, planner, options));
  }

  /** The session queries go to now. */
  get session(): DeephavenSession {
    return this.#session;
  }

  get counts(): DeephavenCounts {
    return { ...this.#counts, following: this.#following.size, remade: this.#counts.remade + this.#session.remade };
  }

  /**
   * The plan's result: from its live rows when it is followed, else made (or found alive in the session) and its rows
   * read from the server, typed by the plan. A Deephaven server cannot stop a batch it has started; `signal` is
   * checked before the plan is sent and before the rows are read.
   */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    this.#record(plan, epoch);
    const live = this.#following.get(plan.sql)?.table;
    if (live) {
      this.#counts.live++;
      return { ...live, epoch, elapsedMs: 0, receipt: this.#receipt() };
    }
    const started = performance.now();
    const session = this.#session;
    let kept: KeptResult;
    try {
      kept = await session.keepAlive(plan.sql, this.#supplied());
    } catch (e) {
      throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
    }
    try {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const stream = await session.rows(kept.exportId);
      this.#counts.queries++;
      this.#counts.planned += kept.ops;
      this.#counts.sent += kept.sent;
      this.#counts.fetched++;
      let raw: RawTable;
      try {
        raw = deephavenRows(stream, epoch, performance.now() - started);
      } catch (e) {
        throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
      }
      return { ...byPosition(raw, plan), receipt: this.#receipt() };
    } finally {
      kept.release();
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
   * FOLLOWS THE VIEW of `epoch`: every result its queries read kept live -- on the server (the session keeps its table
   * alive) and in the page (Deephaven's JS library pushes its rows at every change) -- and every result no longer read
   * let go. Resolves once each is followed; its rows arrive by `onLive`.
   */
  async follow(epoch: number): Promise<void> {
    const plans = this.#executed.get(epoch);
    if (plans === undefined) return;
    for (const [sql, f] of [...this.#following]) {
      if (!plans.has(sql)) {
        this.#following.delete(sql);
        stopFollowing(f);
      }
    }
    await Promise.all([...plans.values()].filter((p) => !this.#following.has(p.sql)).map((p) => this.#start(p)));
  }

  /** Calls `listener` when a followed result changed or the live side ended. */
  onLive(listener: (e: LiveEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  async #start(plan: Plan): Promise<void> {
    const f: Following = { table: null, stop: null, stopped: false };
    this.#following.set(plan.sql, f);
    const session = this.#session;
    const kept = await session.keepAlive(plan.sql, this.#supplied());
    try {
      const name = await session.publishKept(kept.exportId);
      const followed: Followed = await (await this.#liveClient()).follow(name, (rows) => {
        if (f.stopped) return;
        f.table = followedTable(rows, plan, 0, 0);
        this.#changed();
      });
      f.stop = () => {
        followed.close();
        kept.release();
      };
    } catch (e) {
      kept.release();
      this.#following.delete(plan.sql);
      throw e;
    }
    if (f.stopped) stopFollowing(f);
  }

  /**
   * A GRID'S BASE KEPT ON THE SERVER (docs/DEEPHAVEN_PLAN_2026_10_22.md B1): `plan` -- the grid's base query and its
   * leaf-level calculated columns -- made once with its result stored, so the server computes the calculations once a
   * change and every query the grid sends, and every result it follows, reads them; kept until `letBaseGo`. Answers
   * the name a query reads it by (a table of that name, which this engine supplies on every send) and the stored
   * result's columns as the plan names them. A base kept already is answered at once.
   */
  async keepBase(plan: string): Promise<{ readonly name: string; readonly columns: readonly string[] }> {
    const session = this.#session;
    const stored = await session.stored(plan);
    if (!this.#bases.has(stored.name)) {
      const kept = await session.keepAlive(stored.plan, this.#supplied());
      if (session !== this.#session) {
        kept.release();
        throw new QueryError('the Deephaven session changed while the base was made', plan);
      }
      this.#bases.get(stored.name)?.release();
      this.#bases.set(stored.name, kept);
    }
    return { name: stored.name, columns: stored.columns };
  }

  /** Whether the base named `name` is kept now: a reconnect lets every one go. */
  keepsBase(name: string): boolean {
    return this.#bases.has(name);
  }

  /** The base named `name` let go: idle in the session, then gone past its keep (deephaven.ts). */
  letBaseGo(name: string): void {
    this.#bases.get(name)?.release();
    this.#bases.delete(name);
  }

  /** The tables this engine supplies on a send: each base, by its name. */
  #supplied(): Record<string, number> {
    return Object.fromEntries([...this.#bases].map(([name, kept]) => [name, kept.exportId]));
  }

  #liveClient(): Promise<LiveClient> {
    this.#live ??= LiveClient.open(this.#server, (why) => this.#ended(why), this.#options.loadLive);
    return this.#live;
  }

  /** A followed result changed: said at once (the next microtask: the changes one message carried, said once) --
   *  waiting for an animation frame cost a tick half a frame, and its listener runs one run at a time anyway. */
  #changed(): void {
    if (this.#changedSince !== null) return;
    this.#changedSince = performance.now();
    queueMicrotask(() => {
      const since = this.#changedSince ?? performance.now();
      this.#changedSince = null;
      for (const l of this.#listeners) l({ since, ended: null });
    });
  }

  #ended(why: string): void {
    for (const l of this.#listeners) l({ since: performance.now(), ended: why });
  }

  /** Each epoch's plans, the last few kept: `follow` follows the view's, which is the latest one or near it. */
  #record(plan: Plan, epoch: number): void {
    let plans = this.#executed.get(epoch);
    if (plans === undefined) {
      plans = new Map();
      this.#executed.set(epoch, plans);
      for (const old of [...this.#executed.keys()].sort((a, b) => a - b).slice(0, -8)) this.#executed.delete(old);
    }
    plans.set(plan.sql, plan);
  }

  /**
   * Signs in again: a new session, keeping nothing alive (the old one's tables went with it), and a new live reader;
   * nothing is followed until the next `follow`. Throws while the server does not answer.
   */
  async reconnect(): Promise<void> {
    const session = await DeephavenSession.open(this.#server, this.#planner, this.#options);
    const old = this.#session;
    for (const f of this.#following.values()) stopFollowing(f);
    this.#following.clear();
    this.#executed.clear();
    // the old session's bases went with it: each grid makes its own again on its next query
    this.#bases.clear();
    const live = this.#live;
    this.#live = null;
    live?.then((l) => l.close(), () => { /* it never opened */ });
    this.#counts.remade += old.remade;
    this.#session = session;
    this.#counts.reconnects++;
    old.close().catch(() => { /* the server it was on is gone */ });
  }

  async close(): Promise<void> {
    for (const f of this.#following.values()) stopFollowing(f);
    this.#following.clear();
    const live = this.#live;
    this.#live = null;
    (await live?.catch(() => null))?.close();
    await this.#session.close();
  }

  #receipt(): Receipt {
    return { plane: 'warehouse', where: `the Deephaven server at ${hostOf(this.#server.url)}` };
  }
}

function stopFollowing(f: Following): void {
  f.stopped = true;
  f.stop?.();
  f.stop = null;
}
