// THE MARKET LIVE IN THE TAB'S DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md L2-L3): every table of the market's made by the
// planner's own statements (model-data.ts SeedSource: the store's DDL, its column types and keys the model's), declared
// to the live engine as the world writes it -- the tape, the wire, the fills and routes streams, each bounded; the
// quotes, orders, rates and requests keyed; the reference written once -- and the world started, its rows written into
// them. Spells no SQL: every statement is the planner's.

import { LiveDuckDb, type Keep, type LiveTable } from '../../../engine-client/src/duckdb-live.ts';
import type { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import type { HostOperationsSource } from '../../../engine-client/src/host-operations.ts';
import type { SeedSource } from '../../../engine-client/src/model-data.ts';
import { MarketWorld, type MarketSettings } from './market.ts';
import { MARKET_TABLES, WORLD, type MarketTable } from './tables.ts';

/** The store the market's tables are the model's of, and the world's own status table's. */
const STORE = 'store::DB';
const STATUS = 'market::World';

export interface LiveMarketOptions {
  /** A connection of the tab's DuckDB, set up (duckdb-tab.ts DuckDbInTab.connect): the writer's and each reader's. */
  readonly connect: () => Promise<DuckDbEngine>;
  /** The planner, over `model`: each table's statements, and every statement the live engine runs. */
  readonly planner: HostOperationsSource & SeedSource;
  /** The model the tables are declared by: the corpus, the desk's own and its environments (model.ts DESK_MODEL). */
  readonly model: string;
  /** The world's size and pace. */
  readonly settings?: MarketSettings;
  /** The prints the tape keeps (100000): its latest, by sequence. */
  readonly tape?: number;
  /** How often the market's rows are written and its followed results run again, in one cycle (250 ms); `timer` false:
   *  the caller beats, writes and emits. */
  readonly cycleMs?: number;
  readonly timer?: boolean;
  /** False: every append through a staging table, none straight in (the measurement's comparison, §2). */
  readonly directAppends?: boolean;
}

/** The market live: its engine, its world, and how long each part of its start took (ms): the planner writing the
 *  tables' statements, DuckDB running them, the world's reference and morning written. */
export interface LiveMarket {
  readonly live: LiveDuckDb;
  readonly world: MarketWorld;
  readonly started: { readonly ddl: number; readonly tables: number; readonly world: number };
  /** The cycle stopped, the world and the engine closed. */
  close(): Promise<void>;
}

/** What each stream keeps of its history: the tape its latest `tape` prints, the rest a working day's worth. */
function keepOf(table: MarketTable, tape: number): Keep | undefined {
  switch (table.name) {
    case 'MARKET_TRADE': return { column: 'SEQUENCE_NUMBER', rows: tape };
    case 'NEWS_STORY': return { column: 'PUBLISH_TIME', rows: 5_000 };
    case 'EXECUTION': return { column: 'SEQUENCE_NUMBER', rows: 50_000 };
    case 'ORDER_ROUTE': return { column: 'ROUTE_DATE', rows: 50_000 };
    default: return undefined;
  }
}

/** The live table each market table is, as the world writes it: a stream's batches its whole rows, in its order
 *  (tables.ts TableRows), so they go straight in. */
function liveTable(table: MarketTable, tape: number, direct: boolean): LiveTable {
  if (table.write.kind === 'keyed') return { name: table.name, kind: 'keyed', key: [table.write.key] };
  const keep = keepOf(table, tape);
  return {
    name: table.name, kind: 'stream', ...(keep === undefined ? {} : { keep }),
    ...(direct ? { columns: Object.keys(table.shape) } : {}),
  };
}

export async function openLiveMarket(o: LiveMarketOptions): Promise<LiveMarket> {
  const tape = o.tape ?? 100_000;
  const t0 = performance.now();
  // the engine's own timer off: the market's cycle emits, right after it writes
  const live = await LiveDuckDb.open({ connect: o.connect, host: o.planner, timer: false });
  // each table made empty by the planner's statements (a header alone), the store's and the world's own
  const header = (t: MarketTable): string => Object.keys(t.shape).join(',');
  const store = MARKET_TABLES.filter((t) => t !== WORLD);
  const seeds = [
    ...await o.planner.testDataSql(o.model, STORE, store.map((t) => ({ schema: 'default', table: t.name, csv: header(t) }))),
    ...await o.planner.testDataSql(o.model, STATUS, [{ schema: 'default', table: WORLD.name, csv: header(WORLD) }]),
  ];
  const tDdl = performance.now();
  for (const seed of seeds) for (const sql of seed.sql) await live.run(sql);
  live.declare(MARKET_TABLES.map((t) => liveTable(t, tape, o.directAppends !== false)));
  const t1 = performance.now();
  const timer = o.timer !== false;
  const world = await MarketWorld.open({ ...o.settings, timer, flushes: false, morningPrints: o.settings?.morningPrints ?? tape },
    (table, count, rows) => (table.write.kind === 'keyed' ? live.upsert(table.name, count, rows) : live.append(table.name, count, rows)));
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
