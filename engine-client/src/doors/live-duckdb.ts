// THE DOOR TO THE TAB'S LIVE DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md L1; main's app rule, 2026-11-17: an app reaches the
// run side through its doors alone, //tools/deps:app_runs_test). The planner asked first, outside any run -- a reader's
// snapshot statements (its transaction) and each table's plans from its ingest's declaration -- then the live engine
// opened over the tab's connections and each table made empty by its make plan, every statement in a run
// (duckdb-live.ts). An app names the engine's type as this door hands it (`Awaited<ReturnType<typeof
// openLiveDuckDb>>['live']`), as main's apps name the tab DuckDB's.

import type { DuckDbEngine } from '../duckdb.ts';
import { LiveDuckDb } from '../duckdb-live.ts';
import type { HostOperationsSource, IngestPlansSource } from '../planner-sources.ts';
import type { IngestPlansRequest } from '../requests.ts';

/** The database a live table's plans are asked for: the tab's DuckDB (the planner's name for it). */
const DATABASE = 'DuckDB';

/** A table the live engine writes: its name, the name a batch is held under while its land plan runs, and the rest of
 *  what its plans are asked for with (its model, database, schema and deployment). */
export interface LiveTableAsked {
  readonly name: string;
  readonly held: string;
  readonly request: Omit<IngestPlansRequest, 'databaseType' | 'table' | 'held'>;
}

export interface LiveDuckDbDoorOptions {
  /** A connection of the tab's DuckDB, set up (duckdb-tab.ts DuckDbInTab.connect): the writer's and each reader's. */
  readonly connect: () => DuckDbEngine | Promise<DuckDbEngine>;
  readonly planner: HostOperationsSource & IngestPlansSource;
  readonly tables: readonly LiveTableAsked[];
  /** False: no timer, the caller emits. */
  readonly timer?: boolean;
}

/** The live engine with its tables made, and how long each part took (ms): the planner's answers, the tables made. */
export async function openLiveDuckDb(o: LiveDuckDbDoorOptions): Promise<{
  readonly live: LiveDuckDb;
  readonly askedMs: number;
  readonly madeMs: number;
}> {
  const t0 = performance.now();
  const [[begin, commit], plans] = await Promise.all([
    o.planner.hostOperations(DATABASE, [{ op: 'transaction', step: 'begin' }, { op: 'transaction', step: 'commit' }]),
    Promise.all(o.tables.map((t) => o.planner.ingestPlans({ ...t.request, databaseType: DATABASE, table: t.name, held: t.held }))),
  ]);
  const t1 = performance.now();
  const live = await LiveDuckDb.open({ connect: o.connect, transaction: { begin: begin ?? [], commit: commit ?? [] },
    timer: o.timer ?? true });
  if (live.databaseType !== DATABASE) throw new Error(`the live engine runs on ${live.databaseType}, its plans were asked for ${DATABASE}`);
  await live.declare(o.tables.map((t, i) => ({ name: t.name, held: t.held, plans: plans[i]! })));
  return { live, askedMs: t1 - t0, madeMs: performance.now() - t1 };
}
