// A DEEPHAVEN SERVER AS THE TAB'S ENGINE (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23, DH24): a planned query's plan sent
// straight from the tab (deephaven.ts) and its rows read back typed by the plan, so a grid runs on Deephaven as it runs
// on the tab's DuckDB -- `new CubeController(engine, planner)`, the planner planning on a Deephaven runtime. The session
// keeps alive the tables each query made, so the grid's next query over the same source builds on them (a regroup, a
// repivot, a filter sends only what differs). And it TICKS: `follow` keeps every result a view read live in the page
// too -- Deephaven's JS library pushes each one's rows as the server's update cycle changes them (deephaven-live.ts)
// -- so the grid runs again with no query sent and no row fetched: `execute` answers a followed plan from its live
// rows, and `onLive` says at once that one changed.
//
// SEVERAL GRIDS, ONE ENGINE (V2): the session, the live reader and the bases are the page's, one each; what a grid
// follows is its own -- `reader()`, a QueryEngine for each grid, follows its views and says its own changes, and a
// grid following its view lets go of nothing another grid reads. A whole result two grids read is one subscription.
// The engine is itself the reader of one grid (its own `execute`, `follow`, `onLive`), for a page of one.

import type {
  DeephavenColumn, DeephavenPlanner, DeephavenServer, DeephavenSessionOptions, KeptResult,
} from './deephaven.ts';
import { DeephavenSession } from './deephaven.ts';
import { LiveClient, type DhModule, type Followed } from './deephaven-live.ts';
import { byPosition, deephavenRows, followedTable } from './deephaven-rows.ts';
import { QueryError, type QueryEngine, type RawTable, type ResultWindow, type WindowedResult } from './engine.ts';
import { hostOf, type Receipt } from './receipt.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable } from './result.ts';
import { measured } from './timeline.ts';

// what the live side says, the same of every live engine (live.ts): re-exported, Deephaven's readers' word
import type { LiveEvent } from './live.ts';
export type { LiveEvent } from './live.ts';

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
  /** Tables the session made again after letting them go (deephaven.ts `remade`), and keeps alive now (`kept`). */
  readonly remade: number;
  readonly kept: number;
  /** Sessions signed in again (`reconnect`). */
  readonly reconnects: number;
}

/** A plan followed: its live rows once the first update lands, and how to stop. */
interface Following {
  table: ResultTable | null;
  /** Followed by a window (V1), not whole: the window's place and the result's size; null, every row followed. */
  window: FollowingWindow | null;
  stop: (() => void) | null;
  stopped: boolean;
}

/** A followed window: the one asked for last, where the rows followed now are, the result's size, how to move it, and
 *  the runs waiting for the rows of the window asked. */
interface FollowingWindow {
  asked: ResultWindow;
  offset: number;
  total: number;
  move: ((window: ResultWindow) => void) | null;
  waiting: { readonly resolve: () => void; readonly reject: (error: unknown) => void }[];
}

/** A whole result followed for every reader that reads it: one subscription, its changes said to each; `ready`
 *  settles once it is followed (or could not be), for every reader that joins it meanwhile. */
interface SharedFollowing extends Following {
  readonly readers: Set<DeephavenReader>;
  ready: Promise<void>;
}

/** A plan an epoch executed, and the window it asked of the result (null: the whole result). */
interface Executed {
  readonly plan: Plan;
  readonly window: ResultWindow | null;
}

export interface DeephavenEngineOptions extends DeephavenSessionOptions {
  /** How Deephaven's JS library is loaded (a test's own); from the server by default. */
  readonly loadLive?: (url: string) => Promise<DhModule>;
}

/** The engine's counts as they are kept: its readers add to them. */
interface Counts {
  queries: number;
  planned: number;
  sent: number;
  fetched: number;
  live: number;
  remade: number;
  reconnects: number;
}

/** What a grid's reader reads through: the engine's session, its live reader, its bases and its counts -- the page's. */
interface EngineCore {
  session(): DeephavenSession;
  supplied(): Record<string, number>;
  liveClient(): Promise<LiveClient>;
  readonly counts: Counts;
  receipt(): Receipt;
  /** A whole result's live rows, whichever reader follows it; null while none does or before its first update. */
  wholeRows(sql: string): ResultTable | null;
  holdWhole(plan: Plan, reader: DeephavenReader): Promise<void>;
  letGoWhole(sql: string, reader: DeephavenReader): void;
  forget(reader: DeephavenReader): void;
}

export class DeephavenEngine implements QueryEngine {
  readonly name = 'deephaven';
  readonly #server: DeephavenServer;
  readonly #planner: DeephavenPlanner;
  readonly #options: DeephavenEngineOptions;
  #session: DeephavenSession;
  #live: Promise<LiveClient> | null = null;
  /** The whole results followed, by their text, each for the readers that read it. */
  readonly #whole = new Map<string, SharedFollowing>();
  readonly #readers = new Set<DeephavenReader>();
  /** The grids' bases kept on the server (B1), by the name a query reads each by. */
  readonly #bases = new Map<string, KeptResult>();
  readonly #counts: Counts = { queries: 0, planned: 0, sent: 0, fetched: 0, live: 0, remade: 0, reconnects: 0 };
  readonly #core: EngineCore;
  /** The reader of a page of one grid: the engine's own `execute`, `follow` and `onLive`. */
  readonly #own: DeephavenReader;

  private constructor(server: DeephavenServer, planner: DeephavenPlanner, options: DeephavenEngineOptions,
    session: DeephavenSession) {
    this.#server = server;
    this.#planner = planner;
    this.#options = options;
    this.#session = session;
    this.#core = {
      session: () => this.#session,
      supplied: () => this.#supplied(),
      liveClient: () => this.#liveClient(),
      counts: this.#counts,
      receipt: () => this.#receipt(),
      wholeRows: (sql) => this.#whole.get(sql)?.table ?? null,
      holdWhole: (plan, reader) => this.#holdWhole(plan, reader),
      letGoWhole: (sql, reader) => this.#letGoWhole(sql, reader),
      forget: (reader) => { this.#readers.delete(reader); },
    };
    this.#own = this.reader();
  }

  /**
   * Signs in to `server` (deephaven.ts `DeephavenSession.open`), and starts loading Deephaven's JS library for the live
   * reader at once (O1): it is 8 MB to parse on the page's thread, which a first follow paid in the middle of the
   * person's first gesture (measured: a group's opening 390 ms slower); loaded now, it is parsed while the first view
   * waits on the server. A library that fails to load is said by the first follow that needs it.
   */
  static async open(server: DeephavenServer, planner: DeephavenPlanner,
    options: DeephavenEngineOptions = {}): Promise<DeephavenEngine> {
    // a session the server forgot ends the live side as a server gone does: each reader's host signs in again -- while
    // it is the engine's session (one a reconnect let go of is forgotten as it closes, and is no server gone)
    let ended: (why: string) => void = () => { /* no engine yet */ };
    const session = await DeephavenSession.open(server, planner, { ...options, onForgotten: (why) => ended(why) });
    const engine = new DeephavenEngine(server, planner, options, session);
    ended = (why) => { if (engine.#session === session) engine.#ended(why); };
    engine.#liveClient().catch(() => { /* said by the follow that needs it */ });
    return engine;
  }

  /** The session queries go to now. */
  get session(): DeephavenSession {
    return this.#session;
  }

  get counts(): DeephavenCounts {
    let windows = 0;
    for (const r of this.#readers) windows += r.windowsFollowed;
    return {
      ...this.#counts, following: this.#whole.size + windows, remade: this.#counts.remade + this.#session.remade,
      kept: this.#session.kept,
    };
  }

  /** A grid's own reader over this engine's session, live reader and bases: what the grid follows is its own. */
  reader(): DeephavenReader {
    const reader = new DeephavenReader(this.#core);
    this.#readers.add(reader);
    return reader;
  }

  execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return this.#own.execute(plan, epoch, signal);
  }

  executeWindow(plan: Plan, epoch: number, window: ResultWindow, signal?: AbortSignal): Promise<WindowedResult> {
    return this.#own.executeWindow(plan, epoch, window, signal);
  }

  stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    return this.#own.stream(plan, epoch, onChunk, signal);
  }

  run(sql: string): Promise<RawTable> {
    return this.#own.run(sql);
  }

  /** The view of `epoch` followed (`DeephavenReader.follow`), for a page of one grid. */
  follow(epoch: number): Promise<void> {
    return this.#own.follow(epoch);
  }

  /** Calls `listener` when a result the engine's own reader follows changed, or the live side ended. */
  onLive(listener: (e: LiveEvent) => void): () => void {
    return this.#own.onLive(listener);
  }

  /** `plan`'s whole result followed for `reader`: one subscription for every reader that reads it, resolved once its
   *  rows are live in the page -- a reader that joins a following under way waits for it too. */
  #holdWhole(plan: Plan, reader: DeephavenReader): Promise<void> {
    const held = this.#whole.get(plan.sql);
    if (held !== undefined) {
      held.readers.add(reader);
      return held.ready;
    }
    const f: SharedFollowing = { table: null, window: null, stop: null, stopped: false, readers: new Set([reader]),
      ready: Promise.resolve() };
    this.#whole.set(plan.sql, f);
    f.ready = this.#startWhole(plan, f);
    return f.ready;
  }

  /** Resolved once the result's first rows are in the page (N1: a read the moment it resolves is answered live), or
   *  once it is let go before they came; refused when it could not be followed or its rows could not be read. */
  async #startWhole(plan: Plan, f: SharedFollowing): Promise<void> {
    const session = this.#session;
    let kept: KeptResult;
    try {
      kept = await session.keepAlive(plan.sql, this.#supplied());
    } catch (e) {
      // never a following left behind that no one makes: the next read follows it again
      if (this.#whole.get(plan.sql) === f) this.#whole.delete(plan.sql);
      throw e;
    }
    let arrived!: () => void;
    let unread!: (error: unknown) => void;
    const first = new Promise<void>((done, refuse) => { arrived = done; unread = refuse; });
    let firstSeen = false;
    try {
      const name = await session.publishKept(kept.exportId);
      const followed: Followed = await (await this.#liveClient()).follow(name, (rows) => {
        if (f.stopped) return;
        let table: ResultTable;
        try {
          table = followedTable(rows, plan, 0, 0);
        } catch (error) {
          // rows the tab does not read (deephaven-rows.ts refuses them by name): said, and the result let go -- never a
          // following that stopped ticking unsaid, nor a view waiting on rows that will not come
          if (this.#whole.get(plan.sql) === f) this.#whole.delete(plan.sql);
          unread(error);
          const why = error instanceof Error ? error.message : String(error);
          if (firstSeen) for (const r of f.readers) r.unfollowed(why);
          stopFollowing(f);
          return;
        }
        f.table = table;
        firstSeen = true;
        arrived();
        for (const r of f.readers) r.changed();
      }, (error) => {
        // the server failed the result (a row its formula fails on): said as rows the tab cannot read are
        if (f.stopped) return;
        if (this.#whole.get(plan.sql) === f) this.#whole.delete(plan.sql);
        unread(error);
        if (firstSeen) for (const r of f.readers) r.unfollowed(error.message);
        stopFollowing(f);
      });
      f.stop = () => {
        followed.close();
        kept.release();
        arrived();
      };
    } catch (e) {
      kept.release();
      if (this.#whole.get(plan.sql) === f) this.#whole.delete(plan.sql);
      throw e;
    }
    if (f.stopped) stopFollowing(f);
    await first;
  }

  /** `reader` no longer reads `sql`'s whole result: let go when no reader does. */
  #letGoWhole(sql: string, reader: DeephavenReader): void {
    const f = this.#whole.get(sql);
    if (f === undefined) return;
    f.readers.delete(reader);
    if (f.readers.size > 0) return;
    this.#whole.delete(sql);
    stopFollowing(f);
  }

  /**
   * A GRID'S BASE KEPT ON THE SERVER (docs/DEEPHAVEN_PLAN_2026_10_22.md B1): `plan` -- the grid's base query and its
   * leaf-level calculated columns -- made once with its result stored, so the server computes the calculations once a
   * change and every query the grid sends, and every result it follows, reads them; kept until `letBaseGo`. Answers
   * the name a query reads it by (a table of that name, which this engine supplies on every send), the stored
   * result's columns as the plan names them, and its columns as the server holds them (`held`: the base is declared as
   * it is held, DH16). A base kept already is answered at once.
   */
  async keepBase(plan: string): Promise<{
    readonly name: string;
    readonly columns: readonly string[];
    readonly held: readonly DeephavenColumn[];
  }> {
    const session = this.#session;
    const stored = await session.stored(plan);
    let kept = this.#bases.get(stored.name);
    if (kept === undefined) {
      kept = await session.keepAlive(stored.plan, this.#supplied());
      if (session !== this.#session) {
        kept.release();
        throw new QueryError('the Deephaven session changed while the base was made', plan);
      }
      this.#bases.get(stored.name)?.release();
      this.#bases.set(stored.name, kept);
    }
    return { name: stored.name, columns: stored.columns, held: kept.columns };
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
    if (this.#live === null) {
      // its end is the server gone while it is the engine's live reader: one a reconnect let go of ends as it closes
      const opening: Promise<LiveClient> = LiveClient.open(this.#server, (why) => {
        if (this.#live === opening) this.#ended(why);
      }, this.#options.loadLive);
      this.#live = opening;
      // one that failed to open is opened again by the next follow that needs it, never kept failing
      opening.catch(() => { if (this.#live === opening) this.#live = null; });
    }
    return this.#live;
  }

  #ended(why: string): void {
    for (const r of this.#readers) r.ended(why);
  }

  /**
   * Signs in again: a new session, keeping nothing alive (the old one's tables went with it), and a new live reader;
   * nothing is followed until each reader's next `follow`. Throws while the server does not answer.
   */
  async reconnect(): Promise<void> {
    // its end is the server gone while it is the engine's session (as `open`'s)
    let ended: (why: string) => void = () => { /* not the engine's yet */ };
    const session = await DeephavenSession.open(this.#server, this.#planner,
      { ...this.#options, onForgotten: (why) => ended(why) });
    ended = (why) => { if (this.#session === session) this.#ended(why); };
    const old = this.#session;
    for (const f of this.#whole.values()) stopFollowing(f);
    this.#whole.clear();
    for (const r of this.#readers) r.reset();
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

  /** Done: the session ended first (its request outlives a page that is going: deephaven.ts `close`), then every
   *  following stopped and the live reader closed. */
  async close(): Promise<void> {
    const ending = this.#session.close();
    for (const f of this.#whole.values()) stopFollowing(f);
    this.#whole.clear();
    for (const r of this.#readers) r.reset();
    const live = this.#live;
    this.#live = null;
    (await live?.catch(() => null))?.close();
    await ending;
  }

  #receipt(): Receipt {
    return { plane: 'warehouse', where: `the Deephaven server at ${hostOf(this.#server.url)}` };
  }
}

/**
 * ONE GRID'S READER (V2): a QueryEngine over the engine's session, live reader and bases (`DeephavenEngine.reader`).
 * What it follows is its grid's: `follow` keeps its view's results live and lets go of the ones its grid no longer
 * reads -- never another grid's -- and `onLive` says the changes to what it follows. A whole result is followed once
 * for every reader that reads it; a window (a flat grid's viewport) is the reader's own.
 */
export class DeephavenReader implements QueryEngine {
  readonly name = 'deephaven';
  readonly #core: EngineCore;
  /** The plans each recent epoch executed, by their text: what `follow` follows. */
  readonly #executed = new Map<number, Map<string, Executed>>();
  /** The whole results this reader follows (held by the engine), by their text: each settled once it is followed. */
  readonly #whole = new Map<string, Promise<void>>();
  /** The results this reader follows by a window, by their text. */
  readonly #windows = new Map<string, Following>();
  readonly #listeners = new Set<(e: LiveEvent) => void>();
  #changedSince: number | null = null;
  /**
   * The epoch of the view last followed (`follow`): a result read in it or later is followed as it is read -- a chart
   * drawn after its grid's view, a tick's re-read -- and one last read before it is let go at the next `follow`.
   * Null before the first view: nothing is followed until a view is.
   */
  #floor: number | null = null;
  /** The epoch each result was last read in, by its text. */
  readonly #readAt = new Map<string, number>();

  constructor(core: EngineCore) {
    this.#core = core;
  }

  /** How many results it follows by a window now (the engine counts its whole ones). */
  get windowsFollowed(): number {
    return this.#windows.size;
  }

  /**
   * The plan's result: from its live rows when it is followed whole (by any grid), else made (or found alive in the
   * session) and its rows read from the server, typed by the plan. A Deephaven server cannot stop a batch it has
   * started; `signal` is checked before the plan is sent and before the rows are read.
   */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    this.#record(plan, epoch, null);
    const counts = this.#core.counts;
    const live = this.#core.wholeRows(plan.sql);
    if (live) {
      counts.live++;
      this.#keepWhole(plan, epoch);
      return { ...live, epoch, elapsedMs: 0, receipt: this.#core.receipt() };
    }
    const started = performance.now();
    const session = this.#core.session();
    let kept: KeptResult;
    // the rows asked for as soon as the result's export is named: the server answers once it has made it (G1)
    let reading: Promise<Uint8Array> | null = null;
    try {
      kept = await session.keepAlive(plan.sql, this.#core.supplied(), (exportId) => {
        if (signal?.aborted) return;
        reading = session.rows(exportId);
        reading.catch(() => { /* a refused batch is said by keepAlive */ });
        // published as its rows are read, when the view that reads it will follow it: its following then starts a
        // round trip on (N1)
        if (this.#follows(plan, epoch)) session.publishKept(exportId).catch(() => { /* its following asks again */ });
      });
    } catch (e) {
      throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
    }
    try {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const stream = await (reading ?? session.rows(kept.exportId));
      counts.queries++;
      counts.planned += kept.ops;
      counts.sent += kept.sent;
      counts.fetched++;
      let raw: RawTable;
      const decoding = performance.now();
      try {
        raw = deephavenRows(stream, epoch, performance.now() - started);
        measured('rows', decoding);
      } catch (e) {
        throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
      }
      const rows = { ...byPosition(raw, plan), receipt: this.#core.receipt() };
      this.#keepWhole(plan, epoch);
      return rows;
    } finally {
      kept.release();
    }
  }

  /** Whether `plan`'s whole result read in `epoch` will be followed: read in a view not older than the one followed
   *  (the first view's, before any is), and not read by a window. */
  #follows(plan: Plan, epoch: number): boolean {
    return (this.#floor === null || epoch >= this.#floor) && !this.#windows.has(plan.sql);
  }

  /** `plan`'s whole result read in `epoch`: followed from now when that is the view's or later (`#floor`). */
  #keepWhole(plan: Plan, epoch: number): void {
    if (this.#floor === null || epoch < this.#floor || this.#whole.has(plan.sql) || this.#windows.has(plan.sql)) return;
    // a failure is said to the listeners, and the next read follows it again: `follow` waits for this, never throws it
    const holding: Promise<void> = this.#core.holdWhole(plan, this).catch((error: unknown) => {
      if (this.#whole.get(plan.sql) === holding) this.#whole.delete(plan.sql);
      const failed = error instanceof Error ? error.message : String(error);
      for (const l of this.#listeners) l({ since: performance.now(), ended: null, failed });
    });
    this.#whole.set(plan.sql, holding);
  }

  /**
   * A WINDOW of the plan's result (docs/DEEPHAVEN_PLAN_2026_10_22.md V1): the result made on the server and kept there,
   * and followed in the page by a viewport -- the window's rows only, whatever the result's size. A window the followed
   * rows show is answered at once, from them; another moves the viewport, and is answered when the server has sent its
   * rows. Every update the window's rows take is said by `onLive`, as a whole result's is.
   */
  async executeWindow(plan: Plan, epoch: number, window: ResultWindow, signal?: AbortSignal): Promise<WindowedResult> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    this.#record(plan, epoch, window);
    const started = performance.now();
    let f = this.#windows.get(plan.sql);
    try {
      if (f === undefined) {
        f = await this.#startWindow(plan, window);
      } else if (!shows(f, window)) {
        const w = f.window as FollowingWindow;
        if (w.asked.offset !== window.offset || w.asked.count !== window.count) {
          w.asked = window;
          w.move?.(window);
        }
        await waitFor(w);
      }
    } catch (e) {
      throw new QueryError(e instanceof Error ? e.message : String(e), plan.sql, { cause: e });
    }
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const w = f.window as FollowingWindow;
    const table = f.table as ResultTable;
    this.#core.counts.live++;
    return {
      rows: { ...table, epoch, elapsedMs: performance.now() - started, receipt: this.#core.receipt() },
      offset: w.offset,
      total: w.total,
    };
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
   * FOLLOWS THE VIEW of `epoch` (one that LANDED, never a tick's): every result read in it or since kept live -- on the
   * server (the session keeps its table alive) and in the page (Deephaven's JS library pushes its rows at every
   * change) -- and every result last read before it let go. A result read later, in it or after it (a chart drawn from
   * the view, a tick's re-read), is followed as it is read. Resolves once each is followed and its rows are in the page
   * (a read then is answered from them); every change after arrives by `onLive`.
   */
  async follow(epoch: number): Promise<void> {
    if (this.#floor === null || epoch > this.#floor) this.#floor = epoch;
    const floor = this.#floor;
    const read = (sql: string): number => this.#readAt.get(sql) ?? Number.NEGATIVE_INFINITY;
    for (const sql of [...this.#whole.keys()]) {
      if (read(sql) < floor) {
        this.#whole.delete(sql);
        this.#core.letGoWhole(sql, this);
      }
    }
    for (const [sql, f] of [...this.#windows]) {
      if (read(sql) < floor) {
        this.#windows.delete(sql);
        stopFollowing(f);
      }
    }
    for (const [sql, at] of [...this.#readAt]) if (at < floor) this.#readAt.delete(sql);
    // what the view and everything since read, each by how it was read last
    const plans = new Map<string, Executed>();
    for (const e of [...this.#executed.keys()].filter((e) => e >= floor).sort((a, b) => a - b)) {
      for (const [sql, x] of this.#executed.get(e) ?? []) plans.set(sql, x);
    }
    await Promise.all([...plans.values()].map((e) => {
      if (e.window !== null) return this.#windows.has(e.plan.sql) ? undefined : this.#startWindow(e.plan, e.window);
      // one under way since its read (`#keepWhole`) waited for: resolved, the view is live
      const under = this.#whole.get(e.plan.sql);
      if (under !== undefined) return under;
      const holding: Promise<void> = this.#core.holdWhole(e.plan, this).catch((error: unknown) => {
        if (this.#whole.get(e.plan.sql) === holding) this.#whole.delete(e.plan.sql);
        throw error;
      });
      this.#whole.set(e.plan.sql, holding);
      return holding;
    }));
  }

  /** Calls `listener` when a result this reader follows changed or the live side ended. */
  onLive(listener: (e: LiveEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  /**
   * A result this reader follows changed (the engine's word, for a whole one): said once the messages already arrived
   * are read -- the next task, so the changes a server's cycle sent its results (one message a result: a tree's levels,
   * a window and its count) are said once. Said at the next microtask, each message was a tick of its own: the trading
   * desk's market map ran each grid about four times a cycle (2026-10-26); waiting for an animation frame cost a tick
   * half a frame.
   */
  changed(): void {
    if (this.#changedSince !== null) return;
    this.#changedSince = performance.now();
    setTimeout(() => {
      const since = this.#changedSince ?? performance.now();
      this.#changedSince = null;
      for (const l of this.#listeners) l({ since, ended: null });
    }, 0);
  }

  /** The live side ended (the engine's word): said to this reader's listeners. */
  ended(why: string): void {
    for (const l of this.#listeners) l({ since: performance.now(), ended: why });
  }

  /** A result it follows could no longer be followed (the engine's word: its rows could not be read): said to its
   *  listeners, as one that could not be followed at its read is (`failed`). */
  unfollowed(why: string): void {
    for (const l of this.#listeners) l({ since: performance.now(), ended: null, failed: why });
  }

  /** Everything it followed let go, as of a session gone (the engine's reconnect): nothing until its next `follow`. */
  reset(): void {
    for (const f of this.#windows.values()) stopFollowing(f);
    this.#windows.clear();
    this.#whole.clear();
    this.#executed.clear();
    this.#readAt.clear();
    this.#floor = null;
  }

  /** Done: every result it follows let go, and the engine no longer counts it. */
  async close(): Promise<void> {
    for (const sql of this.#whole.keys()) this.#core.letGoWhole(sql, this);
    this.#whole.clear();
    for (const f of this.#windows.values()) stopFollowing(f);
    this.#windows.clear();
    this.#listeners.clear();
    this.#core.forget(this);
  }

  /** `plan`'s result followed by `window`, resolved once the server has sent the window's rows. */
  async #startWindow(plan: Plan, window: ResultWindow): Promise<Following> {
    const w: FollowingWindow = { asked: window, offset: 0, total: 0, move: null, waiting: [] };
    const f: Following = { table: null, window: w, stop: null, stopped: false };
    this.#windows.set(plan.sql, f);
    const shown = waitFor(w);
    // refused before it is awaited (its result not made, or a reconnect let it go): said by the throw below, and by
    // the await at the end otherwise -- never a rejection no one handles
    shown.catch(() => undefined);
    const session = this.#core.session();
    const counts = this.#core.counts;
    let kept: KeptResult;
    try {
      // published as soon as it is named, while the server makes it: the window's rows a round trip sooner (N1)
      kept = await session.keepAlive(plan.sql, this.#core.supplied(), (exportId) => {
        session.publishKept(exportId).catch(() => { /* asked again below, and said there */ });
      });
    } catch (e) {
      if (this.#windows.get(plan.sql) === f) this.#windows.delete(plan.sql);
      for (const r of w.waiting.splice(0)) r.reject(e);
      throw e;
    }
    counts.queries++;
    counts.planned += kept.ops;
    counts.sent += kept.sent;
    try {
      const name = await session.publishKept(kept.exportId);
      const followed = await (await this.#core.liveClient()).followWindow(name, window, (rows) => {
        if (f.stopped) return;
        let table: ResultTable;
        try {
          table = followedTable(rows, plan, 0, 0);
        } catch (error) {
          // rows the tab does not read: the runs waiting for them refused, else said; the window let go
          if (this.#windows.get(plan.sql) === f) this.#windows.delete(plan.sql);
          const waiting = w.waiting.splice(0);
          for (const r of waiting) r.reject(error);
          if (waiting.length === 0) this.unfollowed(error instanceof Error ? error.message : String(error));
          stopFollowing(f);
          return;
        }
        f.table = table;
        w.offset = rows.offset;
        w.total = rows.total;
        // an update a run waits for is that run's to read; any other is a tick
        if (shows(f, w.asked) && w.waiting.length > 0) {
          for (const r of w.waiting.splice(0)) r.resolve();
        } else {
          this.changed();
        }
      }, (error) => {
        const waiting = w.waiting.splice(0);
        for (const r of waiting) r.reject(error);
        // a tick's update the library could not place: said, never a window that stopped moving unsaid
        if (waiting.length === 0) this.unfollowed(error.message);
      });
      w.move = (moved) => { followed.move(moved); };
      f.stop = () => {
        followed.close();
        kept.release();
      };
    } catch (e) {
      kept.release();
      if (this.#windows.get(plan.sql) === f) this.#windows.delete(plan.sql);
      for (const r of w.waiting.splice(0)) r.reject(e);
      throw e;
    }
    if (f.stopped) stopFollowing(f);
    await shown;
    return f;
  }

  /** Each epoch's plans, the last few kept: `follow` follows the view's, which is the latest one or near it. A whole
   *  read of a result the epoch read by a window (an export, a copy) leaves it read by its window. */
  #record(plan: Plan, epoch: number, window: ResultWindow | null): void {
    let plans = this.#executed.get(epoch);
    if (plans === undefined) {
      plans = new Map();
      this.#executed.set(epoch, plans);
      for (const old of [...this.#executed.keys()].sort((a, b) => a - b).slice(0, -8)) this.#executed.delete(old);
    }
    this.#readAt.set(plan.sql, Math.max(epoch, this.#readAt.get(plan.sql) ?? epoch));
    if (window === null && plans.get(plan.sql)?.window) return;
    plans.set(plan.sql, { plan, window });
  }
}

function stopFollowing(f: Following): void {
  f.stopped = true;
  f.stop?.();
  f.stop = null;
  for (const r of f.window?.waiting.splice(0) ?? []) r.reject(new Error('the window is no longer followed'));
}

/** Whether `f`'s followed rows are `window`'s: its first row's, and as many as the result has from there. */
function shows(f: Following, window: ResultWindow): boolean {
  const w = f.window;
  if (w === null || f.table === null || w.offset !== window.offset) return false;
  return f.table.rowCount >= Math.min(window.count, Math.max(0, w.total - window.offset));
}

/** Resolves when the window's followed rows are the window asked; fails with the follow. */
function waitFor(w: FollowingWindow): Promise<void> {
  return new Promise((resolve, reject) => { w.waiting.push({ resolve, reject }); });
}
