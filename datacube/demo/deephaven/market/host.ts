// THE MARKET ON A DEEPHAVEN SERVER, A HOST OF DATACUBE'S APP (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading
// desk; the user, 2026-10-25: "a real saved page just like charts in real datacube"): the desk (desk.ts: its project,
// its elements, the one branch, its example) where the page's config.json names a market world's Deephaven server
// (page-config.ts `market`). DataCube's own page -- its Open…, Save, sheets, grids and charts -- is the app as the tab
// opens it (src/app/datacube-app.ts); this is the desk's engine: a grid over the market's runtime planned in the tab --
// by the tab's own module, the model held once -- and run on the server through its bases (src/server-base.ts B1), each
// grid its own reader, following what its views read and ticking when those rows change (services.ts LiveRows).
//
// The server is the test server's, its key a test key (page-config.ts): never a production browser's.

import { DeephavenEngine } from '../../../../engine-client/src/deephaven-engine.ts';
import { measured } from '../../../../engine-client/src/timeline.ts';
import type { GridRows, LiveGrid, LiveRows } from '../../../src/app/services.ts';
import type { LitePlanner } from '../../../src/lite-planner.ts';
import { PlanAndRun, PlanThenRun, localHost, type PlanAndRunHost, type QueryRunner, type RunWindow } from '../../../src/runner.ts';
import { ServerBases, type ServerBaseRunner } from '../../../src/server-base.ts';
import type { MarketConfig } from '../../page-config.ts';
import { fetched, openDesk, type Market } from './desk.ts';
import { DESK_MODEL, ON_DEEPHAVEN } from './model.ts';

/**
 * The bases' own connection at `host`:`port` over TLS, its store `store` (src/server-base.ts `connectionOf`: their
 * Database in a model of their own, never the page's). Step 5 of docs/ONE_CONNECTION_DESK_2026_10_28.md reads the
 * server from the desk's Deephaven connection; until then the config's address, as the engine's.
 */
const basesConnection = (host: string, port: number) => (store: string): string => `RelationalDatabaseConnection bases::Deephaven
{
  store: ${store};
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}`;

/**
 * The market `config` names, opened: its model fetched and held by the page's module (`planner`, the tab's own when it
 * plans in the tab: its worker), the server signed in to. Refused, in words, when the server does not answer.
 */
export async function openMarket(config: MarketConfig, planner: LitePlanner): Promise<Market> {
  const at = new URL(config.url);
  const host = at.hostname;
  const port = Number(at.port);
  const [corpus, desk, environments] = await Promise.all([fetched('the trading model', config.model),
    fetched("the desk's model", config.deskModel), fetched("the desk's environments", config.environments)]);
  // the desk on its Deephaven context (environments.pure: the server's connections), the model the project's
  const model = DESK_MODEL(corpus, desk, environments);
  const market = planner.withModel(model, ON_DEEPHAVEN.runtime);
  // the server signed in to while the module holds the model: neither waits for the other
  const holding = market.warmUp();
  let engine: DeephavenEngine;
  try {
    engine = await DeephavenEngine.open({
      url: config.url,
      signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
    }, (op, args) => market.deephaven(op, args));
  } catch (e) {
    throw new Error(`the market's Deephaven (${config.url}) is not answering: ${e instanceof Error ? e.message : String(e)}`);
  }
  await holding;
  addEventListener('pagehide', () => { engine.close().catch(() => { /* the page is gone */ }); });
  // For the browser harness ONLY (test/deephaven/desk.mjs, beside the app's own `__dataPage`): the market's counts --
  // what was sent to the server and read from it, which no page shows. Not product code.
  (window as unknown as { __market?: unknown }).__market = { counts: () => engine.counts };
  // each source's base on the server, in a model of its own: the page's module holds the trading model once
  const bases = new ServerBases(market, engine, {
    model, connection: 'bases::Deephaven', stores: ['store::DB'], connectionOf: basesConnection(host, port),
  });

  /** The connection back, after the server went (a restart): tried each second until it answers, once for every grid. */
  let reconnecting: Promise<void> | null = null;
  const reconnected = (): Promise<void> => (reconnecting ??= (async () => {
    for (;;) {
      await new Promise((r) => setTimeout(r, 1000));
      try {
        await engine.reconnect();
        return;
      } catch {
        // not back yet
      }
    }
  })().finally(() => { reconnecting = null; }));

  /** A source's rows on the server: typed by the module's planner; each grid made over them (a copy too) its own. */
  const rowsOnServer = (): GridRows => ({ runner: new PlanThenRun(market, engine), live: gridOnServer });

  /**
   * THE SHEETS NOT SHOWN, WARMED (the user, 2026-10-26: "first tab first and then rest"): once every grid asked for rows
   * has its first view -- the sheet shown drawn -- each grid not asked yet has its base made on the server, its formulas
   * compiled, no row read and nothing followed, four at a time in the order the page made them; a sheet shown later
   * reads what is made (the options desk waited seconds on the server's compiler when first shown). Four at a time
   * (measured 2026-10-26 on a server just started, the options desk shown 10 s after the first tab): one at a time had
   * not reached it, 9-16 s to fill; four filled it in 4.6-6.8 s, eight no sooner.
   */
  const ahead = new Map<object, () => Promise<void>>();
  /** The grids asked for rows whose first view has not landed. */
  const reading = new Set<object>();
  let warming = false;
  const warmAhead = (): void => {
    if (warming || ahead.size === 0 || reading.size > 0) return;
    warming = true;
    const pause = (ms: number): Promise<unknown> => new Promise((resolve) => setTimeout(resolve, ms));
    // each warmer takes the next grid none has
    const warmer = async (): Promise<void> => {
      for (const [grid, warm] of ahead) {
        // a sheet a person has just shown comes first: its grids drawn before the next is warmed
        while (reading.size > 0) await pause(200);
        // a refusal is the grid's to say, at its first view when its sheet is shown
        if (ahead.delete(grid)) await warm().catch(() => undefined);
      }
    };
    // the sheet shown's charts drawn first: its grids' views landed, what draws from them has its moment
    void pause(2000).then(() => Promise.all([warmer(), warmer(), warmer(), warmer()]))
      .finally(() => { warming = false; warmAhead(); });
  };

  /** One grid's rows on the server: its own reader, its queries read from its bases, its views followed. */
  const gridOnServer = (): LiveGrid => {
    const reader = engine.reader();
    const based = bases.runner(reader);
    let stop: (() => void) | undefined;
    const grid = {};
    let drawn = false;
    // its first view landed, or refused: the sheet shown waits for it no more
    const drew = (): void => {
      if (drawn) return;
      drawn = true;
      reading.delete(grid);
      warmAhead();
    };
    // its rows asked for: made now, not ahead, and the sheet shown not drawn until its first view lands
    const asked = (): void => {
      ahead.delete(grid);
      if (!drawn) reading.add(grid);
    };
    const followed: LiveRows = {
      viewed: (epoch) => {
        reader.follow(epoch).catch((e: unknown) => console.warn('a market grid could not follow its view', e));
        drew();
      },
      onChange: (grid) => {
        stop?.();
        stop = reader.onLive((e) => {
          if (e.ended !== null) {
            void reconnected().then(() => grid.refresh());
            return;
          }
          if (e.failed !== undefined) {
            console.warn('a market grid will not tick', e.failed);
            return;
          }
          // a tick on the browser's timeline (dh:tick): from its rows to the grid painted again with them
          void grid.tick()?.then(() => requestAnimationFrame(() => measured('tick', e.since)));
        });
      },
      close: () => {
        stop?.();
        ahead.delete(grid);
        reading.delete(grid);
        based.close();
        void reader.close();
      },
    };
    return {
      runner: new OnServer(based, localHost(market, reader), asked, drew),
      rows: followed,
      warm: (snapshot) => { ahead.set(grid, () => based.warm(snapshot)); },
    };
  };

  return openDesk({
    modelUrl: config.model,
    deskModelUrl: config.deskModel,
    environmentsUrl: config.environments,
    context: ON_DEEPHAVEN,
    where: 'the market world',
    start: () => Promise.resolve({ model, planner: market }),
    ready: true,
    // a model this host does not hold is refused here, never run on the tab's database
    rows: (text) => {
      if (text !== model) throw new Error(`${ON_DEEPHAVEN.runtime} runs on the market's server over the trading model this page holds; this query's project names another model`);
      return rowsOnServer();
    },
    once: async (plan) => {
      const reader = engine.reader();
      try {
        return await reader.execute(plan, 1);
      } finally {
        void reader.close();
      }
    },
  });
}

/**
 * A grid's rows on the server, as the app's places take a runner (sources.ts GridPlace.runner): its queries run through
 * its bases' runner (src/server-base.ts), the page's planner and the grid's reader its host's -- what a model action
 * reads of a grid (its planner) is the market's.
 */
class OnServer extends PlanAndRun {
  readonly #based: ServerBaseRunner;
  /** Said at each query run, and at a query refused: the grid's rows asked for, and its view refused (the sheets not
   *  shown warmed once the sheet shown is drawn, gridOnServer). */
  readonly #asked: () => void;
  readonly #refused: () => void;
  override readonly window: RunWindow;

  constructor(based: ServerBaseRunner, host: PlanAndRunHost, asked: () => void, refused: () => void) {
    super(host);
    this.#based = based;
    this.#asked = asked;
    this.#refused = refused;
    this.window = based.window;
  }

  override run(...args: Parameters<QueryRunner['run']>): ReturnType<QueryRunner['run']> {
    this.#asked();
    const answered = this.#based.run(...args);
    answered.catch(this.#refused);
    return answered;
  }

  override stream(...args: Parameters<QueryRunner['stream']>): ReturnType<QueryRunner['stream']> {
    this.#asked();
    const answered = this.#based.stream(...args);
    answered.catch(this.#refused);
    return answered;
  }

  override compile(...args: Parameters<QueryRunner['compile']>): ReturnType<QueryRunner['compile']> {
    return this.#based.compile(...args);
  }
}
