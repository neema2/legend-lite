// THE TRADING DESK LIVE IN THE TAB (docs/DUCKDB_LIVE_2026_10_25.md L3): where the page names no market world's server
// (page-config.ts `market` absent -- DataCube's static site, no config at all), the same desk opens over the market world
// run in this tab: the market's tables in the tab's own DuckDB, the world (L2) writing them, every grid of the desk its
// own live reader (L1), following what its views read and ticking when those rows change -- the Deephaven host's shape
// (deephaven/market/host.ts), the server's place taken by the tab. The same saved page, the same model and screens: only
// the connections differ (model.ts MARKET_TAB: the runtime's two connections on DuckDB).
//
// LAZY: nothing is fetched or started with the page. The market starts when the desk is opened from Open…'s Examples, or
// when a saved page of it first asks for its rows: the trading model fetched and held by a module of its own, its tables
// made by the planner's statements, the world begun. Until then this host adds an example and a project, nothing else.

import type { DuckDbLiveReader } from '../../../engine-client/src/duckdb-live.ts';
import type { DuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import type { QueryEngine, RawTable } from '../../../engine-client/src/engine.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import { measured } from '../../../engine-client/src/timeline.ts';
import type { GridRows, LiveGrid, LiveRows, Services } from '../../src/app/services.ts';
import type { Planner } from '../../src/cube.ts';
import type { ExamplePage } from '../../src/examples/catalog.ts';
import type { LitePlanner } from '../../src/lite-planner.ts';
import { PlanThenRun } from '../../src/runner.ts';
import type { Market } from '../deephaven/market/host.ts';
import { MARKET_RUNTIME, MARKET_TAB, MARKET_TAB_MODEL } from '../deephaven/market/model.ts';
import { DESK, MARKET_PROJECT, type MarketValues, type SourceTypes } from '../deephaven/market/screens.ts';
import type { LiveMarket } from './live-market.ts';
import type { MarketSettings } from './market.ts';

export interface LocalMarketOptions {
  /** The tab's planner: the market's model is held by a module of its own over it (`withModel`). */
  readonly planner: LitePlanner;
  /** The tab's DuckDB: the market's tables are made in it, the writer and each reader a connection of it. */
  readonly duck: DuckDbInTab;
  /** Where the page serves the trading model and the desk's own (the site's trading-model.pure and market.pure). */
  readonly model: string;
  readonly deskModel: string;
  /** The world's size and pace (market.ts MarketSettings). */
  readonly settings?: MarketSettings;
  /**
   * The prints the tape keeps (25,000). Measured (docs/DUCKDB_LIVE_2026_10_25.md stage A): at 25,000 every tab ticks in
   * under a second; at 100,000 the Stock and News tabs take 4-11 s a tick, DuckDB's moving average of a price that may
   * be null folding each print's whole history (DuckDb.windowCall).
   */
  readonly tape?: number;
}

/** The market once started: the trading model's text and the whole model's, the module holding it, the live engine and
 *  the world. */
interface Started {
  readonly corpus: string;
  readonly model: string;
  readonly planner: LitePlanner;
  readonly market: LiveMarket;
}

/** The desk, as Open… lists it here: the same page, its figures this tab's own. */
const LABEL = 'Trading desk: ten screens, live in this tab';
const ABOUT = 'A trading floor on one page, a tab a screen, every figure computed by DuckDB in this tab from a market world '
  + 'simulated in it, ticking: the market map and movers; a stock from the open, its candles, quote, prints and news; a '
  + 'news terminal; the order flow; a crypto desk; the execution blotter; TCA and markouts; P&L against limits; the options '
  + 'chain and its risk; FX rates, crosses and RFQs. Nothing runs on a server.';

/**
 * The desk over the market in this tab: what it adds to the tab's services and its example. Nothing starts until the desk
 * is opened or a saved page of it reads its rows (above).
 */
export function openLocalMarket(o: LocalMarketOptions): Market {
  let started: Promise<Started> | undefined;
  const start = (): Promise<Started> => (started ??= (async () => {
    const fetched = async (what: string, url: string): Promise<string> => {
      const answered = await fetch(url);
      if (!answered.ok) throw new Error(`${what} (${url}) answered ${answered.status}`);
      return answered.text();
    };
    const [corpus, desk] = await Promise.all([fetched('the trading model', o.model), fetched("the desk's model", o.deskModel)]);
    const model = MARKET_TAB_MODEL(corpus, desk);
    const planner = o.planner.withModel(model, MARKET_RUNTIME);
    await planner.warmUp();
    // the world's code a chunk of its own, fetched with the market, not with the page
    const { openLiveMarket } = await import('./live-market.ts');
    const market = await openLiveMarket({
      connect: () => o.duck.connect({ whole: true }), planner, model, tape: o.tape ?? 25_000,
      settings: { ...o.settings, morningPrints: o.settings?.morningPrints ?? o.tape ?? 25_000 },
    });
    addEventListener('pagehide', () => { market.close().catch(() => { /* the page is gone */ }); });
    // For the browser harness ONLY (test/market/live-desk.mjs): the market's counts -- the world's rows written and the
    // engine's emits -- which no page shows. Not product code.
    (window as unknown as { __market?: unknown }).__market = { counts: () => ({ world: market.world.counts, live: market.live.counts }) };
    return { corpus, model, planner, market };
  })());

  /** The market's module, for a query of `text`: a model this host does not hold is refused, never run elsewhere. */
  const held = async (text: string): Promise<LitePlanner> => {
    const s = await start();
    if (text !== s.model) throw new Error(`${MARKET_RUNTIME} runs on the market in this tab over the trading model it holds; this query's project names another model`);
    return s.planner;
  };

  /** The reader a source's one-off runs share (a grid's own rows have a reader each, gridInTab). */
  let scratch: Promise<DuckDbLiveReader> | undefined;
  const shared = (): Promise<DuckDbLiveReader> => (scratch ??= start().then((s) => s.market.live.reader()));

  /** A source's rows in the tab's market: planned by the market's module, run by a reader of the live engine. */
  const rowsInTab = (text: string): GridRows => ({
    runner: new PlanThenRun(later(() => held(text)), new Reading(shared)),
    live: () => gridInTab(text),
  });

  /** One grid's rows: its own reader, its views followed, a tick each time what they read changes. */
  const gridInTab = (text: string): LiveGrid => {
    // its reader opened when it is first asked for: a grid never read opens none
    let opened: Promise<DuckDbLiveReader> | undefined;
    const own = (): Promise<DuckDbLiveReader> => (opened ??= start().then((s) => s.market.live.reader()));
    let stop: (() => void) | undefined;
    let closed = false;
    const rows: LiveRows = {
      viewed: (epoch) => {
        void own().then((r) => r.follow(epoch)).catch((e: unknown) => console.warn('a market grid could not follow its view', e));
      },
      onChange: (grid) => {
        void own().then((r) => {
          if (closed) return;
          stop?.();
          stop = r.onLive((e) => {
            if (e.failed !== undefined) {
              console.warn('a market grid will not tick', e.failed);
              return;
            }
            const ticked = grid.tick();
            // its sheet not shown: nothing followed until it is -- the app runs the grid when a sheet shows it, and its
            // view followed then (the tab's one DuckDB runs no hidden sheet's grids at an emit)
            if (ticked === undefined) {
              r.rest();
              return;
            }
            // a tick on the browser's timeline: from its rows to the grid painted again with them
            void ticked.then(() => requestAnimationFrame(() => measured('tick', e.since)));
          });
        });
      },
      close: () => {
        closed = true;
        stop?.();
        void opened?.then((r) => r.close());
      },
    };
    return { runner: new PlanThenRun(later(() => held(text)), new Reading(own)), rows };
  };

  /** What the desk's page is written from: its sources' columns, as the module types them, and its starting values. */
  const types = (s: Started): SourceTypes => async (text) => (await s.planner.relationTypeText(text)).map((c) => ({ name: c.name, type: c.type }));
  const values = (s: Started): MarketValues => async (text) => {
    const plan = await s.planner.planText(text);
    const reader = await s.market.live.reader();
    try {
      const column = (await reader.execute(plan, 1)).columns[0];
      return column === undefined ? [] : [...column.values].filter((v) => v !== null && v !== undefined).map(String);
    } finally {
      void reader.close();
    }
  };

  // the project's connections, a file of its own: the page's text, served by the browser from memory
  const connections = URL.createObjectURL(new Blob([MARKET_TAB()], { type: 'text/plain' }));
  return {
    over: (services) => {
      const { engine: tab, queries } = services;
      if (queries === undefined) throw new Error('the market\'s screens are saved queries: this page reads none');
      /** The model's elements, read once: the trading corpus's (deephaven/market/host.ts says why not the desk's own). */
      let elements: Promise<unknown[]> | undefined;
      const corpusElements = async (): Promise<unknown[]> => tab.reading.elements((await start()).corpus);
      return {
        ...services,
        engine: {
          ...tab,
          reading: {
            parse: (text) => tab.reading.parse(text),
            // the market's project's model ends in its connections, this host's own file: any other is the tab's
            elements: (text) => (text.endsWith(MARKET_TAB()) ? (elements ??= corpusElements()) : tab.reading.elements(text)),
          },
          // THE ONE BRANCH: the market's runtime runs on the market in this tab
          rows: (text, runtime, how) => (runtime === MARKET_RUNTIME ? rowsInTab(text) : tab.rows(text, runtime, how)),
        },
        queries: {
          ...queries,
          // its files in the order MARKET_TAB_MODEL joins them: the corpus, the desk's own, the connections
          projects: [...queries.projects, { ...MARKET_PROJECT, title: 'The trader screens (the market in this tab)', models: [o.model, o.deskModel, connections] }],
        },
      } satisfies Services;
    },
    examples: [{ ...DESK, label: LABEL, about: ABOUT, build: async () => { const s = await start(); return DESK.build(types(s), values(s)); } }] satisfies ExamplePage[],
  };
}

/**
 * The desk over the market in this tab, as DataCube's page opens it where the deployment names no market world's server
 * and the page plans in the tab (demo/boot.ts): its module the tab's own planner, its models the site's own files beside
 * the page, the world sized by the page's address where it says (`market-names`, `market-tape`, `market-feed=off`: a
 * test's).
 */
export function openLocalMarketHere(planner: LitePlanner, duck: DuckDbInTab): Market {
  const address = new URLSearchParams(location.search);
  const count = (name: string): number | undefined => (address.has(name) ? Number(address.get(name)) : undefined);
  const names = count('market-names');
  const tape = count('market-tape');
  return openLocalMarket({
    planner,
    duck,
    model: new URL('./trading-model.pure', location.href).href,
    deskModel: new URL('./market.pure', location.href).href,
    settings: { ...(names === undefined ? {} : { names }), ...(address.get('market-feed') === 'off' ? { feed: 'off' } : {}) },
    ...(tape === undefined ? {} : { tape }),
  });
}

/** A planner whose module is had later: each question waits for it. */
function later(planner: () => Promise<Planner>): Planner {
  return {
    plan: async (query, signal) => (await planner()).plan(query, signal),
    relationType: async (query, signal) => (await planner()).relationType(query, signal),
    parse: async (text, signal) => (await planner()).parse(text, signal),
    print: async (query, style, signal) => (await planner()).print(query, style, signal),
  };
}

/** A reader of the live engine, opened when first asked to run: each run waits for it. Closing it is its owner's (the
 *  grid's rows, or the page's end): a runner's engine is never closed by the runner. */
class Reading implements QueryEngine {
  readonly name = 'duckdb-live';
  readonly #reader: () => Promise<DuckDbLiveReader>;

  constructor(reader: () => Promise<DuckDbLiveReader>) {
    this.#reader = reader;
  }

  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return (await this.#reader()).execute(plan, epoch, signal);
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
