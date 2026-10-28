// STORES LIVE IN THE TAB (docs/ONE_CONNECTION_DESK_2026_10_28.md step 5; docs/DUCKDB_LIVE_2026_10_25.md L1, L3): a
// runtime whose stores a source has made live in the tab's own DuckDB -- their tables written as rows arrive (the Market
// data source, step 6; until it lands, the trading desk's simulator) -- has its grids follow the live engine: each grid
// its own reader, following what its views read, ticking when those rows change, resting while no sheet shows it. Any
// other runtime on the tab's DuckDB is planned and run as every grid is. The page's services keep which runtimes are
// live (services/browser.ts `live.serve`); this code is fetched when a source is served, never with the page.
//
// LAZY: a source served starts when a grid first reads its rows (or its own owner starts it): its model held, its tables
// made, its writer begun. Until then it is a name and a way to start.

import type { DuckDbLiveReader } from '../../engine-client/src/duckdb-live.ts';
import type { QueryEngine, RawTable } from '../../engine-client/src/engine.ts';
import type { Plan } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { measured } from '../../engine-client/src/timeline.ts';
import type { GridRows, LiveGrid, LiveRows } from './app/services.ts';
import type { Planner } from './cube.ts';
import { PlanThenRun } from './runner.ts';

/** A source's stores live in the tab, started: the model they are declared by, its planner, and a reader of each. */
export interface LiveStarted {
  readonly model: string;
  readonly planner: Planner;
  /** A reader of the live engine (engine-client's duckdb-live.ts): its own connection, what it follows its own. */
  reader(): Promise<DuckDbLiveReader>;
}

/** A source that makes `runtime`'s stores live in the tab, and how it starts (once: its owner keeps it started). */
export interface LiveSource {
  readonly runtime: string;
  start(): Promise<LiveStarted>;
}

/** A source's rows on `source`'s live stores: planned by its planner, run by a reader of the live engine. */
export function liveRows(source: LiveSource, model: string): GridRows {
  /** Its planner, for a query over `model`: a model other than the one its stores are declared by is refused, by name,
   *  never run on them. */
  const held = async (): Promise<Planner> => {
    const s = await source.start();
    if (s.model !== model) throw new Error(`${source.runtime} is live in this tab over the model its source holds; this query's project names another model`);
    return s.planner;
  };
  /** The reader a source's one-off runs share (a grid's own rows have a reader each, `grid`). */
  let scratch: Promise<DuckDbLiveReader> | undefined;
  const shared = (): Promise<DuckDbLiveReader> => (scratch ??= source.start().then((s) => s.reader()));

  /** One grid's rows: its own reader, its views followed, a tick each time what they read changes. */
  const grid = (): LiveGrid => {
    // its reader opened when it is first asked for: a grid never read opens none
    let opened: Promise<DuckDbLiveReader> | undefined;
    const own = (): Promise<DuckDbLiveReader> => (opened ??= source.start().then((s) => s.reader()));
    let stop: (() => void) | undefined;
    let closed = false;
    const rows: LiveRows = {
      viewed: (epoch) => {
        void own().then((r) => r.follow(epoch)).catch((e: unknown) => console.warn('a live grid could not follow its view', e));
      },
      onChange: (grid) => {
        void own().then((r) => {
          if (closed) return;
          stop?.();
          stop = r.onLive((e) => {
            if (e.failed !== undefined) {
              console.warn('a live grid will not tick', e.failed);
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
        }, () => undefined);
      },
      close: () => {
        closed = true;
        stop?.();
        void opened?.then((r) => r.close(), () => undefined);
      },
    };
    return { runner: new PlanThenRun(later(held), new Reading(own)), rows };
  };

  return { runner: new PlanThenRun(later(held), new Reading(shared)), live: grid };
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
