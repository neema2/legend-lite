// GRIDS ON A DEEPHAVEN SERVER (docs/ONE_CONNECTION_DESK_2026_10_28.md step 5; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md,
// the trading desk): where a runtime's connections are a Deephaven server's (runtime-connections.ts `engineOf`), its
// grids are planned in the tab -- by the tab's module, each project's model held once -- and run on the server through
// their bases (server-base.ts B1), each grid its own reader, following what its views read and ticking when those rows
// change (services.ts LiveRows). One engine for each server, signed in to when a grid first reads its rows (lazy about
// work: docs/TENETS.md), its key the connection's vault reference read from the page's vault. This code is a chunk of
// its own: fetched once the page reads a model that names a Deephaven server (services/browser.ts), never with the page.
//
// The server and its key are the deployment's: a test server's key is a test key, never a production browser's.

import { DeephavenEngine } from '../../engine-client/src/deephaven-engine.ts';
import type { QueryEngine, RawTable, ResultWindow, WindowedResult } from '../../engine-client/src/engine.ts';
import type { Plan } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { measured } from '../../engine-client/src/timeline.ts';
import type { GridRows, LiveGrid, LiveRows } from './app/services.ts';
import type { LitePlanner } from './lite-planner.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { PlanAndRun, PlanThenRun, QueryFailure, localHost, type PlanAndRunHost, type QueryRunner, type RunWindow } from './runner.ts';
import type { DeephavenServerOf } from './runtime-connections.ts';
import { ServerBases, type ServerBaseOptions, type ServerBaseRunner } from './server-base.ts';

export interface DeephavenGridsOptions {
  /** legend-lite's module in the tab: each model held by a planner of its own over it; the server's wire is its (DH11). */
  readonly module: LitePlanner;
  /** The page's vault: a server's key by its connection's vault reference (config.json `vault`). */
  readonly vault: Readonly<Record<string, string>>;
  /** A grid's base as its snapshot writes it (server-base.ts `ServerBaseOptions.baseOf`): the page's query writer. */
  readonly baseOf: ServerBaseOptions['baseOf'];
}

/** The address a server is reached at. */
const urlOf = (s: DeephavenServerOf): string => `${s.useTls ? 'https' : 'http'}://${s.host}:${s.port}`;

/**
 * The bases' own connection to `server`, its store `store` (server-base.ts `connectionOf`: their Database in a model of
 * their own, never the project's): the project's connection to the server, as its runtime's connections say it.
 */
const basesConnection = (server: DeephavenServerOf) => (store: string): string => `RelationalDatabaseConnection bases::Deephaven
{
  store: ${store};
  type: Deephaven;
  specification: Deephaven { host: '${server.host}'; port: ${server.port}; useTls: ${server.useTls}; };
  auth: PreSharedKey { keyVaultReference: '${server.keyVaultReference}'; };
}`;

/** The page's Deephaven servers: one engine each, and each grid's rows on the server its runtime names. */
export class DeephavenGrids {
  readonly #o: DeephavenGridsOptions;
  readonly #servers = new Map<string, Server>();

  constructor(o: DeephavenGridsOptions) {
    this.#o = o;
    // For the browser harness ONLY (test/deephaven/desk.mjs, beside the app's own `__dataPage`): the server's counts --
    // what was sent to it and read from it, which no page shows. Not product code.
    (window as unknown as { __deephaven?: unknown }).__deephaven = { counts: () => this.#counts() };
  }

  /** A source's rows on `server`: planned over `model` against `runtime`, whose `stores` the server holds. */
  rows(server: DeephavenServerOf, model: string, runtime: string, stores: readonly string[]): GridRows {
    const key = `${urlOf(server)} ${server.keyVaultReference}`;
    let at = this.#servers.get(key);
    if (at === undefined) {
      at = new Server(server, this.#o);
      this.#servers.set(key, at);
    }
    return at.rows(model, runtime, stores);
  }

  /** The counts of the one server signed in to (the harness's). */
  async #counts(): Promise<unknown> {
    const [only, ...more] = [...this.#servers.values()];
    if (only === undefined || more.length > 0) throw new Error(`${this.#servers.size} Deephaven servers: the harness reads one`);
    return (await only.engine()).counts;
  }
}

/** A project's model held on a server: the planner holding it, and the bases its grids read. */
interface Held {
  readonly planner: LitePlanner;
  readonly bases: Promise<ServerBases>;
}

/** One server: its engine, signed in to once; the models held for it; the sheets not shown warmed; its reconnection. */
class Server {
  readonly #at: DeephavenServerOf;
  readonly #o: DeephavenGridsOptions;
  #engine: Promise<DeephavenEngine> | undefined;
  /** The models held, by runtime then model text (a 2.3 MB key is never joined into another string). */
  readonly #held = new Map<string, Map<string, Held>>();
  /** The connection back, after the server went (a restart): tried each second until it answers, once for every grid. */
  #reconnecting: Promise<void> | null = null;
  /** The grids made and not asked for rows yet, and how to warm each (`warmAhead`). */
  readonly #ahead = new Map<object, () => Promise<void>>();
  /** The grids asked for rows whose first view has not landed. */
  readonly #reading = new Set<object>();
  #warming = false;

  constructor(at: DeephavenServerOf, o: DeephavenGridsOptions) {
    this.#at = at;
    this.#o = o;
  }

  /** The engine, signed in to the first time a grid reads its rows; a sign-in refused is said, and tried again at the
   *  next read. */
  engine(): Promise<DeephavenEngine> {
    const opening = this.#engine ??= (async () => {
      const url = urlOf(this.#at);
      // the key first: a page whose vault holds none says so, and nothing is sent to the server (a sign-in prompt: later,
      // docs/ONE_CONNECTION_DESK_2026_10_28.md step 5)
      const token = this.#o.vault[this.#at.keyVaultReference];
      if (token === undefined || token === '') {
        throw new Error(`the Deephaven server at ${this.#at.host}:${this.#at.port} signs in with the key `
          + `'${this.#at.keyVaultReference}', which this page's vault does not hold (config.json \`vault\`)`);
      }
      let engine: DeephavenEngine;
      try {
        engine = await DeephavenEngine.open({
          url,
          signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token },
        }, (op, args) => this.#o.module.deephaven(op, args));
      } catch (e) {
        throw new Error(`the Deephaven server at ${url} is not answering: ${e instanceof Error ? e.message : String(e)}`);
      }
      addEventListener('pagehide', () => { engine.close().catch(() => { /* the page is gone */ }); });
      return engine;
    })();
    opening.catch(() => { if (this.#engine === opening) this.#engine = undefined; });
    return opening;
  }

  /** A source's rows on the server: typed by the model's planner; each grid made over them (a copy too) its own. */
  rows(model: string, runtime: string, stores: readonly string[]): GridRows {
    const held = this.#hold(model, runtime, stores);
    return { runner: new PlanThenRun(held.planner, new OnReader(() => this.engine())), live: () => this.#grid(held) };
  }

  /** `model` held for `runtime`'s grids: a planner of the module's over it, and the bases its grids read. */
  #hold(model: string, runtime: string, stores: readonly string[]): Held {
    let models = this.#held.get(runtime);
    if (models === undefined) {
      models = new Map();
      this.#held.set(runtime, models);
    }
    let held = models.get(model);
    if (held === undefined) {
      const planner = this.#o.module.withModel(model, runtime);
      // the model held now, beside the sign-in: every question of it after -- a source's columns typed (pure/v1, which
      // answers from a model held and compiles one it does not hold for each call: 2.3 MB, the trading desk's), a query
      // planned -- asks of it compiled once (as the market's host held it at the page's start, before step 5)
      planner.warmUp().catch(() => { /* said by the first question of it */ });
      const bases = this.engine().then((engine) => new ServerBases(planner, engine, {
        model, connection: 'bases::Deephaven', stores, connectionOf: basesConnection(this.#at), baseOf: this.#o.baseOf,
      }));
      // a sign-in refused is said by the grid that reads; tried again for the next model held
      bases.catch(() => { if (models!.get(model) === held) models!.delete(model); });
      held = { planner, bases };
      models.set(model, held);
    }
    return held;
  }

  #reconnected(): Promise<void> {
    return (this.#reconnecting ??= (async () => {
      const engine = await this.engine();
      for (;;) {
        await new Promise((r) => setTimeout(r, 1000));
        try {
          await engine.reconnect();
          return;
        } catch {
          // not back yet
        }
      }
    })().finally(() => { this.#reconnecting = null; }));
  }

  /**
   * THE SHEETS NOT SHOWN, WARMED (the user, 2026-10-26: "first tab first and then rest"): once every grid asked for rows
   * has its first view -- the sheet shown drawn -- each grid not asked yet has its base made on the server, its formulas
   * compiled, no row read and nothing followed, four at a time in the order the page made them; a sheet shown later
   * reads what is made (the options desk waited seconds on the server's compiler when first shown). Four at a time
   * (measured 2026-10-26 on a server just started, the options desk shown 10 s after the first tab): one at a time had
   * not reached it, 9-16 s to fill; four filled it in 4.6-6.8 s, eight no sooner.
   */
  #warmAhead(): void {
    if (this.#warming || this.#ahead.size === 0 || this.#reading.size > 0) return;
    this.#warming = true;
    const pause = (ms: number): Promise<unknown> => new Promise((resolve) => setTimeout(resolve, ms));
    // each warmer takes the next grid none has
    const warmer = async (): Promise<void> => {
      for (const [grid, warm] of this.#ahead) {
        // a sheet a person has just shown comes first: its grids drawn before the next is warmed
        while (this.#reading.size > 0) await pause(200);
        // a refusal is the grid's to say, at its first view when its sheet is shown
        if (this.#ahead.delete(grid)) await warm().catch(() => undefined);
      }
    };
    // the sheet shown's charts drawn first: its grids' views landed, what draws from them has its moment
    void pause(2000).then(() => Promise.all([warmer(), warmer(), warmer(), warmer()]))
      .finally(() => { this.#warming = false; this.#warmAhead(); });
  }

  /** One grid's rows on the server: its own reader, its queries read from its bases, its views followed. */
  #grid(held: Held): LiveGrid {
    const reader = this.engine().then((engine) => engine.reader());
    const based = Promise.all([held.bases, reader]).then(([bases, r]) => bases.runner(r));
    // a sign-in refused is said by the grid's first run
    based.catch(() => undefined);
    let stop: (() => void) | undefined;
    let closed = false;
    const grid = {};
    let drawn = false;
    // its first view landed, or refused: the sheet shown waits for it no more
    const drew = (): void => {
      if (drawn) return;
      drawn = true;
      this.#reading.delete(grid);
      this.#warmAhead();
    };
    // its rows asked for: made now, not ahead, and the sheet shown not drawn until its first view lands
    const asked = (): void => {
      this.#ahead.delete(grid);
      if (!drawn) this.#reading.add(grid);
    };
    const rows: LiveRows = {
      viewed: (epoch) => {
        reader.then((r) => r.follow(epoch)).catch((e: unknown) => console.warn('a grid on Deephaven could not follow its view', e));
        drew();
      },
      onChange: (grid) => {
        void reader.then((r) => {
          if (closed) return;
          stop?.();
          stop = r.onLive((e) => {
            if (e.ended !== null) {
              void this.#reconnected().then(() => grid.refresh());
              return;
            }
            if (e.failed !== undefined) {
              console.warn('a grid on Deephaven will not tick', e.failed);
              return;
            }
            // a tick on the browser's timeline (dh:tick): from its rows to the grid painted again with them
            void grid.tick()?.then(() => requestAnimationFrame(() => measured('tick', e.since)));
          });
        }, () => undefined);
      },
      close: () => {
        closed = true;
        stop?.();
        this.#ahead.delete(grid);
        this.#reading.delete(grid);
        based.then((b) => b.close(), () => undefined);
        reader.then((r) => r.close(), () => undefined);
      },
    };
    return {
      runner: new OnServer(based, localHost(held.planner, new OnReader(() => reader)), asked, drew),
      rows,
      warm: (snapshot) => { this.#ahead.set(grid, () => based.then((b) => b.warm(snapshot))); },
    };
  }
}

/** The engine's own reader, or a grid's, had once the server is signed in to: each run waits for it. Closing a grid's
 *  reader is its grid's (`close`): a runner's engine is never closed by the runner. */
class OnReader implements QueryEngine {
  readonly name = 'deephaven';
  readonly #reader: () => Promise<QueryEngine & Required<Pick<QueryEngine, 'executeWindow'>>>;

  constructor(reader: () => Promise<QueryEngine & Required<Pick<QueryEngine, 'executeWindow'>>>) {
    this.#reader = reader;
  }

  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return (await this.#reader()).execute(plan, epoch, signal);
  }

  async executeWindow(plan: Plan, epoch: number, window: ResultWindow, signal?: AbortSignal): Promise<WindowedResult> {
    return (await this.#reader()).executeWindow(plan, epoch, window, signal);
  }

  async stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    return (await this.#reader()).stream(plan, epoch, onChunk, signal);
  }

  async run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return (await this.#reader()).run(sql, epoch, signal);
  }

  async close(): Promise<void> {
    // the reader's owner closes it
  }
}

/**
 * A grid's rows on the server, as the app's places take a runner (sources.ts GridPlace.runner): its queries run through
 * its bases' runner (server-base.ts), had once the server is signed in to; the model's planner and the grid's reader its
 * host's -- what a model action reads of a grid (its planner) is the model's.
 */
class OnServer extends PlanAndRun {
  readonly #based: Promise<ServerBaseRunner>;
  /** Said at each query run, and at a query refused: the grid's rows asked for, and its view refused (the sheets not
   *  shown warmed once the sheet shown is drawn, `Server.#grid`). */
  readonly #asked: () => void;
  readonly #refused: () => void;
  override readonly window: RunWindow;

  constructor(based: Promise<ServerBaseRunner>, host: PlanAndRunHost, asked: () => void, refused: () => void) {
    super(host);
    this.#based = based;
    this.#asked = asked;
    this.#refused = refused;
    this.window = async (...args) => (await this.#on(args[0])).window(...args);
  }

  override run(...args: Parameters<QueryRunner['run']>): ReturnType<QueryRunner['run']> {
    this.#asked();
    const answered = this.#on(args[0]).then((b) => b.run(...args));
    answered.catch(this.#refused);
    return answered;
  }

  override stream(...args: Parameters<QueryRunner['stream']>): ReturnType<QueryRunner['stream']> {
    this.#asked();
    const answered = this.#on(args[0]).then((b) => b.stream(...args));
    answered.catch(this.#refused);
    return answered;
  }

  override compile(...args: Parameters<QueryRunner['compile']>): ReturnType<QueryRunner['compile']> {
    return this.#on(args[0]).then((b) => b.compile(...args));
  }

  /** The bases' runner; a server not signed in to (no key, not answering) is the query's failure, in its words. */
  #on(query: Lambda): Promise<ServerBaseRunner> {
    return this.#based.catch((e: unknown) => { throw new QueryFailure(e, query); });
  }
}
