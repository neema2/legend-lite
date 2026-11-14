// THE MARKET LIVE IN THE TAB'S DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md L2-L3): every table of the market's an ingest's
// dataset (core's stress/29-desk-sources.pure, the desk's market.pure), landed by the planner's plans for it (an
// ingest's plans, docs/DESK_DATA_ROADMAP_2026_10_29.md C0: made empty as declared, each batch put in by the dataset's
// write mode, its retention kept at the plan's cadence) -- and the world started, its rows written into them. Spells no
// SQL and loads no data outside a plan: every statement is the planner's, each batch a value the land plan attaches.

import { LiveDuckDb, type LiveTable } from '../../../engine-client/src/duckdb-live.ts';
import type { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import type { HostOperationsSource, IngestPlansSource } from '../../../engine-client/src/host-operations.ts';
import { MarketWorld, type MarketSettings } from './market.ts';
import { MARKET_TABLES, WORLD, type MarketTable } from './tables.ts';

/** The store the market's tables are the model's of, and the world's own status store's. */
const STORE = 'store::DB';
const STATUS = 'market::World';

/** The name each batch is held under while its land plan runs (the plans are asked for with it). */
const HELD = 'batch';

export interface LiveMarketOptions {
  /** A connection of the tab's DuckDB, set up (duckdb-tab.ts DuckDbInTab.connect): the writer's and each reader's. */
  readonly connect: () => Promise<DuckDbEngine>;
  /** The planner, over `model`: each table's plans, and a reader's snapshot statements. */
  readonly planner: HostOperationsSource & IngestPlansSource;
  /** The model the tables are declared by: the corpus, the desk's own and its environments (model.ts DESK_MODEL). */
  readonly model: string;
  /** The world's size and pace. */
  readonly settings?: MarketSettings;
  /** How often the market's rows are written and its followed results run again, in one cycle (250 ms); `timer` false:
   *  the caller beats, writes and emits. */
  readonly cycleMs?: number;
  readonly timer?: boolean;
}

/** The market live: its engine, its world, and how long each part of its start took (ms): the planner writing the
 *  tables' plans, DuckDB making the tables, the world's reference and morning written. */
export interface LiveMarket {
  readonly live: LiveDuckDb;
  readonly world: MarketWorld;
  readonly started: { readonly ddl: number; readonly tables: number; readonly world: number };
  /** The cycle stopped, the world and the engine closed. */
  close(): Promise<void>;
}

/** The Database a market table is declared by: the corpus's store, or the world's own status store. */
function databaseOf(table: MarketTable): string {
  return table === WORLD ? STATUS : STORE;
}

export async function openLiveMarket(o: LiveMarketOptions): Promise<LiveMarket> {
  const t0 = performance.now();
  // the engine's own timer off: the market's cycle emits, right after it writes
  const live = await LiveDuckDb.open({ connect: o.connect, host: o.planner, timer: false });
  // each table's plans, the planner's from its ingest's declaration
  const tables: LiveTable[] = await Promise.all(MARKET_TABLES.map(async (t) => ({
    name: t.name,
    held: HELD,
    plans: await o.planner.ingestPlans({
      databaseType: live.databaseType, model: o.model, database: databaseOf(t), schema: 'default', table: t.name, held: HELD,
    }),
  })));
  const tDdl = performance.now();
  await live.declare(tables);
  const t1 = performance.now();
  const timer = o.timer !== false;
  const world = await MarketWorld.open({ ...o.settings, timer, flushes: false },
    (table, count, rows) => live.write(table.name, count, rows));
  // ONE CYCLE: the world's rows since the last written, then every reader's stale results run again over them -- a tick
  // waits one cycle, never a write's timer and then a refresh's
  let cycling = false;
  const cycle = timer ? setInterval(() => {
    if (cycling) return;
    cycling = true;
    void world.flush().then(() => live.emit()).catch(() => { /* said in each one's counts */ }).finally(() => { cycling = false; });
  }, o.cycleMs ?? 250) : undefined;
  return {
    live,
    world,
    started: { ddl: tDdl - t0, tables: t1 - tDdl, world: performance.now() - t1 },
    close: async () => {
      if (cycle !== undefined) clearInterval(cycle);
      world.close();
      await live.close();
    },
  };
}
