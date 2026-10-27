// THE MARKET, A HOST OF DATACUBE'S APP (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk; the user,
// 2026-10-25: "a real saved page just like charts in real datacube"): what the tab's services become where the page's
// config.json names a market world's Deephaven server (page-config.ts `market`). DataCube's own page -- its Open…, Save,
// sheets, grids and charts -- is the app as the tab opens it (src/app/datacube-app.ts); this adds to its services only
// what the market needs, as the one-app design has a host add a service (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.2):
//
//   * THE PROJECT the trader screens' saved queries belong to (screens.ts `marketSource`): the one trading model and the
//     market's own part (model.ts), so a saved query of it opens as any saved query does (sources.ts openedRecord);
//   * ITS ROWS ON THE SERVER: a grid over the market's runtime is planned in the tab -- by the tab's own module, the
//     model held once -- and run on the server through its bases (src/server-base.ts B1), each grid its own reader,
//     following what its views read and ticking when those rows change (services.ts LiveRows); every other grid's rows
//     stay the tab's;
//   * THE TRADING DESK, first under Open…'s Examples: one page, a tab (a sheet) per screen of the desk (screens.ts).
//
// The server is the test server's, its key a test key (page-config.ts): never a production browser's.

import { DeephavenEngine } from '../../../../engine-client/src/deephaven-engine.ts';
import { measured } from '../../../../engine-client/src/timeline.ts';
import type { GridRows, LiveGrid, LiveRows, Services } from '../../../src/app/services.ts';
import type { ExamplePage } from '../../../src/examples/catalog.ts';
import type { LitePlanner } from '../../../src/lite-planner.ts';
import { PlanAndRun, PlanThenRun, localHost, type PlanAndRunHost, type QueryRunner, type RunWindow } from '../../../src/runner.ts';
import { ServerBases, type ServerBaseRunner } from '../../../src/server-base.ts';
import type { MarketConfig } from '../../page-config.ts';
import { MARKET_CONNECTION, MARKET_LIVE, MARKET_MODEL, MARKET_RUNTIME } from './model.ts';
import { DESK, MARKET_PROJECT, type MarketValues, type SourceTypes } from './screens.ts';

/** The market, opened: what it adds to the tab's services, and its example pages. */
export interface Market {
  /** The tab's services with the market's added. */
  over(services: Services): Services;
  /** Open…'s Examples of its own: the trading desk. */
  readonly examples: readonly ExamplePage[];
}

/**
 * The market `config` names, opened: its model fetched and held by the page's module (`planner`, the tab's own when it
 * plans in the tab: its worker), the server signed in to. Refused, in words, when the server does not answer.
 */
export async function openMarket(config: MarketConfig, planner: LitePlanner): Promise<Market> {
  const at = new URL(config.url);
  const host = at.hostname;
  const port = Number(at.port);
  const fetched = async (what: string, url: string): Promise<string> => {
    const answered = await fetch(url);
    if (!answered.ok) throw new Error(`${what} (${url}) answered ${answered.status}`);
    return answered.text();
  };
  const [corpus, desk] = await Promise.all([fetched('the trading model', config.model), fetched("the desk's model", config.deskModel)]);
  const model = MARKET_MODEL(corpus, desk, host, port);
  // the model's connections, a file of the project's own: the page's text, served by the browser from memory
  const live = URL.createObjectURL(new Blob([MARKET_LIVE(host, port)], { type: 'text/plain' }));
  const market = planner.withModel(model, MARKET_RUNTIME);
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
    model, connection: 'market::Live', stores: ['store::DB'], connectionOf: MARKET_CONNECTION(host, port),
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

  /** A source's columns as the compiler types them, by its text: over the model the module holds. */
  const types: SourceTypes = async (text) => (await market.relationTypeText(text)).map((c) => ({ name: c.name, type: c.type }));
  /** The values of a query's first column now, read once from the server: what a page starts on. */
  const values: MarketValues = async (text) => {
    const plan = await market.planText(text);
    const reader = engine.reader();
    try {
      const column = (await reader.execute(plan, 1)).columns[0];
      return column === undefined ? [] : [...column.values].filter((v) => v !== null && v !== undefined).map(String);
    } finally {
      void reader.close();
    }
  };

  /**
   * THE MODEL'S ELEMENTS, read once: every saved query of the project reads them (saved-queries.ts savedQueryOpened) --
   * for its data spaces and its enumerations, which are the trading corpus's alone. They are read through the drop-in
   * surface (pure/v1's grammarToJson/model), which refuses lite's own Pure by design, and the desk's own model and its
   * connections are lite's own (a Deephaven connection: ConnectionSectionGrammar's LITE_DATABASE_TYPES); they declare no
   * data space and no enumeration, so the elements read are the corpus's.
   */
  let elements: Promise<unknown[]> | undefined;
  return {
    over: (services) => {
      const { engine: tab, queries } = services;
      if (queries === undefined) throw new Error('the market\'s screens are saved queries: this page reads none');
      // read now, as the tab starts (eager about knowledge: docs/TENETS.md) -- the first saved query opened waited for it
      elements = tab.reading.elements(corpus);
      elements.catch(() => { elements = undefined; });
      return {
        ...services,
        engine: {
          ...tab,
          reading: {
            parse: (text) => tab.reading.parse(text),
            elements: (text) => (text === model ? (elements ??= tab.reading.elements(corpus)) : tab.reading.elements(text)),
          },
          // THE ONE BRANCH: the market's runtime runs on its server; a model this host does not hold is refused there,
          // never run on the tab's database
          rows: (text, runtime, how) => {
            if (runtime !== MARKET_RUNTIME) return tab.rows(text, runtime, how);
            if (text !== model) throw new Error(`${MARKET_RUNTIME} runs on the market's server over the trading model this page holds; this query's project names another model`);
            return rowsOnServer();
          },
        },
        queries: {
          ...queries,
          // its files in the order MARKET_MODEL joins them: the corpus, the desk's own, the connections
          projects: [...queries.projects, { ...MARKET_PROJECT, title: 'The trader screens (the market world)', models: [config.model, config.deskModel, live] }],
        },
      };
    },
    examples: [{ ...DESK, build: () => DESK.build(types, values) }],
  };
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
