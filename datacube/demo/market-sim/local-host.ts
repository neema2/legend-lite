// THE TRADING DESK LIVE IN THE TAB (docs/DUCKDB_LIVE_2026_10_25.md L3): where the page names no market world's server
// (page-config.ts `market` absent -- DataCube's static site, no config at all), the same desk (deephaven/market/desk.ts)
// opens over the market world run in this tab: the market's tables in the tab's own DuckDB, the world (L2) writing them,
// every grid of the desk its own live reader (L1), following what its views read and ticking when those rows change --
// the desk's engine the tab where the Deephaven host's is the server (deephaven/market/host.ts). The same saved page,
// the same model and screens, on the data space's other context: "In this tab" (environments.pure: the runtime's two
// connections on DuckDB).
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
import type { GridRows, LiveGrid, LiveRows } from '../../src/app/services.ts';
import type { Planner } from '../../src/cube.ts';
import type { LitePlanner } from '../../src/lite-planner.ts';
import { PlanThenRun } from '../../src/runner.ts';
import { fetched, openDesk, type Market, type Started } from '../deephaven/market/desk.ts';
import { DESK_MODEL, IN_TAB } from '../deephaven/market/model.ts';
import type { LiveMarket } from './live-market.ts';
import type { MarketSettings } from './market.ts';

export interface LocalMarketOptions {
  /** The tab's planner: the market's model is held by a module of its own over it (`withModel`). */
  readonly planner: LitePlanner;
  /** The tab's DuckDB: the market's tables are made in it, the writer and each reader a connection of it. */
  readonly duck: DuckDbInTab;
  /** Where the page serves the trading model, the desk's own and its environments (the site's trading-model.pure,
   *  market.pure and environments.pure). */
  readonly model: string;
  readonly deskModel: string;
  readonly environments: string;
  /** The world's size and pace (market.ts MarketSettings). */
  readonly settings?: MarketSettings;
  /**
   * The prints the tape keeps (25,000). Measured (docs/DUCKDB_LIVE_2026_10_25.md stage A): at 25,000 every tab ticks in
   * under a second; at 100,000 the Stock and News tabs take 4-11 s a tick, DuckDB's moving average of a price that may
   * be null folding each print's whole history (DuckDb.windowCall).
   */
  readonly tape?: number;
}

/** The market once started (desk.ts Started): with the live engine and the world. */
interface Running extends Started {
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
  let started: Promise<Running> | undefined;
  const start = (): Promise<Running> => (started ??= (async () => {
    const [corpus, desk, environments] = await Promise.all([fetched('the trading model', o.model),
      fetched("the desk's model", o.deskModel), fetched("the desk's environments", o.environments)]);
    const model = DESK_MODEL(corpus, desk, environments);
    const planner = o.planner.withModel(model, IN_TAB.runtime);
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
    return { model, planner, market };
  })());

  /** The market's module, for a query of `text`: a model this host does not hold is refused, never run elsewhere. */
  const held = async (text: string): Promise<LitePlanner> => {
    const s = await start();
    if (text !== s.model) throw new Error(`${IN_TAB.runtime} runs on the market in this tab over the trading model it holds; this query's project names another model`);
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

  return openDesk({
    modelUrl: o.model,
    deskModelUrl: o.deskModel,
    environmentsUrl: o.environments,
    context: IN_TAB,
    where: 'the market in this tab',
    example: { label: LABEL, about: ABOUT },
    start,
    ready: false,
    rows: rowsInTab,
    once: async (plan) => {
      const reader = await (await start()).market.live.reader();
      try {
        return await reader.execute(plan, 1);
      } finally {
        void reader.close();
      }
    },
  });
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
    environments: new URL('./environments.pure', location.href).href,
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
