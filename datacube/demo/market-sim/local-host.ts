// THE MARKET IN THIS TAB (docs/DUCKDB_LIVE_2026_10_25.md L3; docs/ONE_CONNECTION_DESK_2026_10_28.md step 5): until the
// Market data source (step 6), the trading desk's simulator -- the market's tables made in the tab's own DuckDB by the
// planner's statements over the desk's model, a world simulated in the tab writing them (L2), the live engine (L1)
// following -- served to DataCube as a source that makes the stores of the data space's tab context ("In this tab":
// environments.pure, the runtime's two connections on DuckDB) live (src/live-in-tab.ts). DataCube's grids on that
// context then follow it, each its own reader; the desk (deephaven/market/desk.ts) serves it.
//
// LAZY: nothing is fetched or started with the page. The market starts when a grid on the tab's context first reads its
// rows (the desk opened from Open…'s Examples, or a saved page of it): the desk's model held by a module of its own, its
// tables made by the planner's statements, the world begun.

import type { DuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import type { Services } from '../../src/app/services.ts';
import type { LitePlanner } from '../../src/lite-planner.ts';
import type { LiveSource } from '../../src/live-in-tab.ts';
import { openDesk, type Desk } from '../deephaven/market/desk.ts';
import { IN_TAB } from '../deephaven/market/model.ts';
import type { MarketSettings } from './market.ts';

export interface MarketInTabOptions {
  /** The tab's module: the market's model is held by a planner of its own over it (`withModel`). */
  readonly planner: LitePlanner;
  /** The tab's DuckDB: the market's tables are made in it, the writer and each reader a connection of it. */
  readonly duck: DuckDbInTab;
  /** The world's size and pace (market.ts MarketSettings). */
  readonly settings?: MarketSettings;
  /**
   * The prints the tape keeps (25,000). Measured (docs/DUCKDB_LIVE_2026_10_25.md stage A): at 25,000 every tab ticks in
   * under a second; at 100,000 the Stock and News tabs take 4-11 s a tick, DuckDB's moving average of a price that may
   * be null folding each print's whole history (DuckDb.windowCall).
   */
  readonly tape?: number;
}

/** The market in this tab over the desk's model (`model`: its text, fetched when the market starts), as a live source of
 *  the tab context's runtime. */
export function marketInTab(o: MarketInTabOptions): (model: () => Promise<string>) => LiveSource {
  return (model) => {
    let started: ReturnType<LiveSource['start']> | undefined;
    return {
      runtime: IN_TAB.runtime,
      start: () => (started ??= (async () => {
        const text = await model();
        const planner = o.planner.withModel(text, IN_TAB.runtime);
        await planner.warmUp();
        // the world's code a chunk of its own, fetched with the market, not with the page
        const { openLiveMarket } = await import('./live-market.ts');
        const market = await openLiveMarket({
          connect: () => o.duck.connect({ whole: true }), planner, model: text, tape: o.tape ?? 25_000,
          settings: { ...o.settings, morningPrints: o.settings?.morningPrints ?? o.tape ?? 25_000 },
        });
        addEventListener('pagehide', () => { market.close().catch(() => { /* the page is gone */ }); });
        // For the browser harness ONLY (test/market/live-desk.mjs): the market's counts -- the world's rows written and
        // the engine's emits -- which no page shows. Not product code.
        (window as unknown as { __market?: unknown }).__market = { counts: () => ({ world: market.world.counts, live: market.live.counts }) };
        return { model: text, planner, reader: () => market.live.reader() };
      })()),
    };
  };
}

/**
 * The trading desk as DataCube's page opens it where it plans in the tab (demo/boot.ts; one chunk, fetched beside
 * DuckDB's start): its project from the model home, written on the context the deployment names (`contexts`:
 * config.json), its market in this tab.
 */
export function openDeskHere(services: Services, planner: LitePlanner, duck: DuckDbInTab,
  contexts: Readonly<Record<string, string>>): Desk {
  return openDesk(services, { contexts, inTab: marketHere(planner, duck) });
}

/** The market in this tab as DataCube's page serves it: the world sized by the page's address where it says
 *  (`market-names`, `market-tape`, `market-feed=off`: a test's). */
function marketHere(planner: LitePlanner, duck: DuckDbInTab): (model: () => Promise<string>) => LiveSource {
  const address = new URLSearchParams(location.search);
  const count = (name: string): number | undefined => (address.has(name) ? Number(address.get(name)) : undefined);
  const names = count('market-names');
  const tape = count('market-tape');
  return marketInTab({
    planner,
    duck,
    settings: { ...(names === undefined ? {} : { names }), ...(address.get('market-feed') === 'off' ? { feed: 'off' } : {}) },
    ...(tape === undefined ? {} : { tape }),
  });
}
