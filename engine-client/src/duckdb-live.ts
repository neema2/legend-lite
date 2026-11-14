// A LIVE DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md): a streaming engine's concepts on a DuckDB the page holds -- the tab's
// DuckDB-WASM, no server -- at a demo's scale. A dev, demo and simulation tool, not a streaming database.
//
//   TABLES, AS THEIR INGESTS LAND THEM (an ingest's plans, docs/DESK_DATA_ROADMAP_2026_10_29.md C0): each table the
//   engine writes is a dataset of an ingest, and lands by its three plans, the planner's (Boundary.ingestPlans) --
//   made empty as declared; each batch attached by name and put in by the dataset's write mode (appended, or latest by
//   key: Proton's versioned_kv, a name's quote); its retention kept at the plan's cadence, so a day's tape never
//   outgrows a tab. The engine knows no write mode. Every write marks its table changed: a version, one a write.
//
//   READERS. A grid's reader (live.ts LiveReader) runs its plans on a connection of its own; the results its landed view
//   read are FOLLOWED, as the Deephaven reader's are (deephaven-engine.ts): those read in the view followed last, or
//   since. Each result knows the tables its SQL names (read off the SQL: a word that is a declared table's name,
//   over-approximated -- a column of the same name runs it again more often, never less) and their versions then.
//
//   THE EMIT (Proton's EMIT PERIODIC). Every `emitEveryMs`, each reader's followed results whose tables changed -- or
//   that read the clock (`now()`: a live time window) -- run again, together, in one transaction (one snapshot); each
//   new result is kept, and the reader is told only when one differs from what it was. The grid's run at that tick is
//   answered from the kept results: nothing runs twice.
//
// It spells no SQL and loads no data outside a plan (D5, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md; main's total wall,
// docs/TOTAL_WALL_START_HERE_2026_11_11.md): a batch's rows are a value it holds, which the land plan's attach step
// puts in (plan-runner.ts runLoad, HostValues), and every statement it runs -- a plan's, a snapshot's transaction (the
// planner's, host-operations.ts) -- is the planner's. Every followed result runs again whole (an incremental view,
// ClickHouse's lesson, only where measured: §3).

import { arrowIpcStream, type MadeColumn } from './arrow-ipc.ts';
import type { DuckDbEngine, HeldConnection } from './duckdb.ts';
import type { RawTable } from './engine.ts';
import type { HostOperationsSource, IngestPlans } from './host-operations.ts';
import { runLoad, type PlanValue } from './plan-runner.ts';
import type { PlannedSql, PlanText } from './planned-sql.ts';
import type { LiveEvent, LiveReader } from './live.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable } from './result.ts';

/** A table the live engine writes and follows: as the model declares it (MARKET_TRADE), and the plans its ingest lands
 *  its rows by (host-operations.ts IngestPlans, the planner's: Boundary.ingestPlans), each batch held under `held`. */
export interface LiveTable {
  readonly name: string;
  readonly plans: IngestPlans;
  /** The name the plans' land step attaches a batch by: the one the plans were asked for with. */
  readonly held: string;
}

/** Rows a page made, a column at a time (arrow-ipc.ts MadeColumn), each named as the table's column it goes in. */
export type LiveRows = Readonly<Record<string, MadeColumn>>;

export interface LiveDuckDbOptions {
  /** A connection of the database's own: the writer's, and each reader's (each reads its own snapshot) -- the tab's set
   *  up as every connection of its is (duckdb-tab.ts DuckDbInTab.connect). */
  readonly connect: () => DuckDbEngine | Promise<DuckDbEngine>;
  /** The planner, which writes the statements of a reader's snapshot (its transaction: D5); a table's are its plans'. */
  readonly host: HostOperationsSource;
  /** The session every connection is set up with first (the planner's: engine.ts setUpSession). */
  readonly setup?: readonly PlannedSql[];
  /** How often followed results are run again when what they read changed (Proton's EMIT PERIODIC): 250 ms. */
  readonly emitEveryMs?: number;
  /** False: no timer, the caller emits (`emit()`) -- a test, a page that paces its own emits. */
  readonly timer?: boolean;
}

/** What the live engine did, for a page that shows it and a test that checks it. */
export interface LiveCounts {
  /** Emits run. */
  emits: number;
  /** Followed results run again, and how many of them came back the same. */
  reruns: number;
  unchanged: number;
  /** Times a reader was told its results changed. */
  told: number;
  /** Writes (batches landed), the rows they wrote, and the keeps that trimmed a table. */
  writes: number;
  rows: number;
  trims: number;
  /** The last emit's time, ms: its bounds and every reader's runs. */
  lastEmitMs: number;
  /** Why the last emit the timer ran failed, if it did; null since one that did not. */
  lastFailure: string | null;
}

/** The database the planner writes the live engine's statements for. */
const DATABASE = 'DuckDB';

/** A word of SQL that reads the clock: a result of it runs again at every emit (a live time window). */
const CLOCK = /\b(now|current_timestamp|current_date|current_time|today|get_current_timestamp|get_current_time|localtimestamp|localtime)\b/i;

/** A transaction's statements, the planner's: what a reader's runs at an emit read one snapshot in. */
interface Snapshot {
  readonly begin: readonly PlannedSql[];
  readonly commit: readonly PlannedSql[];
}

/** What a reader keeps of a result: its plan, its rows, the tables it read and their versions when it ran. */
interface Kept {
  readonly plan: Plan;
  result: ResultTable;
  /** Its values as one string: what tells a changed result from the same one run again. */
  digest: string;
  readonly tables: readonly string[];
  versions: readonly number[];
  readonly clock: boolean;
  /** The latest epoch it was read in: followed when that is the view's followed last, or later. */
  epoch: number;
}

/** A table's state: its version (one a write), and the rows arrived since its keep last ran. */
interface TableState {
  readonly table: LiveTable;
  version: number;
  arrived: number;
}

export class LiveDuckDb {
  /** The database its tables' plans are asked for (the planner's name for it). */
  readonly databaseType = DATABASE;
  readonly #options: LiveDuckDbOptions;
  readonly #writer: DuckDbEngine;
  readonly #snapshot: Snapshot;
  readonly #tables = new Map<string, TableState>();
  readonly #readers = new Set<DuckDbLiveReader>();
  #timer: ReturnType<typeof setInterval> | undefined;
  /** Writes one at a time: each a land plan's load on the writer's connection, held for it alone. */
  #writing: Promise<unknown> = Promise.resolve();
  #emitting: Promise<void> | null = null;
  /** The time (performance.now) of the first write since the last emit; null: none. */
  #firstChange: number | null = null;
  #closed = false;
  readonly counts: LiveCounts = {
    emits: 0, reruns: 0, unchanged: 0, told: 0, writes: 0, rows: 0, trims: 0, lastEmitMs: 0, lastFailure: null,
  };

  private constructor(options: LiveDuckDbOptions, writer: DuckDbEngine, snapshot: Snapshot) {
    this.#options = options;
    this.#writer = writer;
    this.#snapshot = snapshot;
    if (options.timer !== false) {
      this.#timer = setInterval(() => {
        this.emit().then(() => { this.counts.lastFailure = null; }, (e: unknown) => {
          this.counts.lastFailure = e instanceof Error ? e.message : String(e);
        });
      }, options.emitEveryMs ?? 250);
    }
  }

  /** The live engine over the database `options.connect` reaches: its writer's connection opened and set up. */
  static async open(options: LiveDuckDbOptions): Promise<LiveDuckDb> {
    const [begin, commit] = await options.host.hostOperations(DATABASE, [
      { op: 'transaction', step: 'begin' },
      { op: 'transaction', step: 'commit' },
    ]);
    return new LiveDuckDb(options, await connected(options), { begin: begin ?? [], commit: commit ?? [] });
  }

  /** The tables written through this engine, each made as its ingest declares it, empty (its make plan). */
  async declare(tables: readonly LiveTable[]): Promise<void> {
    for (const table of tables) {
      if (this.#tables.has(table.name)) throw new Error(`the live table ${table.name} is declared already`);
      this.#tables.set(table.name, { table, version: 0, arrived: 0 });
    }
    for (const table of tables) await this.#write(() => this.#run(table.plans.make));
  }

  /** `count` rows of `table` landed by its land plan -- appended, or each key's latest kept, as its ingest says: the
   *  batch held under the plan's name while the plan's load runs, never written into a statement. */
  async write(table: string, count: number, rows: LiveRows): Promise<number> {
    const state = this.#tables.get(table);
    if (state === undefined) throw new Error(`${table} is not a live table: declare it first`);
    const ipc = arrowIpcStream(count, rows);
    return this.#write(async () => {
      await this.#run(state.table.plans.land, { [state.table.held]: ipc });
      state.arrived += count;
      this.#changed(state, count);
      return count;
    });
  }

  /** A grid's reader: its own connection, its followed results told as they change. */
  async reader(): Promise<DuckDbLiveReader> {
    if (this.#closed) throw new Error('the live DuckDB is closed');
    const reader: DuckDbLiveReader = new DuckDbLiveReader(this, await connected(this.#options), this.#snapshot,
      () => this.#readers.delete(reader));
    this.#readers.add(reader);
    return reader;
  }

  /** The declared tables `sql` reads, by name, and whether it reads the clock. */
  reads(sql: string): { readonly tables: readonly string[]; readonly clock: boolean } {
    const tables = [...this.#tables.keys()].filter((name) => new RegExp(`(?<![\\w$])${escaped(name)}(?![\\w$])`, 'i').test(sql));
    return { tables, clock: CLOCK.test(sql) };
  }

  /** Each table's version now, in the order given. */
  versions(tables: readonly string[]): number[] {
    return tables.map((t) => this.#tables.get(t)?.version ?? 0);
  }

  /**
   * ONE EMIT: each stream bounded (trimmed back to its `keep` once it has grown past it by a tenth), then each reader's
   * stale followed results run again, and the reader told when one changed. One at a time: an emit asked for while one
   * runs is that one.
   */
  emit(): Promise<void> {
    if (this.#closed) return Promise.resolve();
    this.#emitting ??= (async () => {
      const started = performance.now();
      this.counts.emits++;
      await this.#write(() => this.#bound());
      const since = this.#firstChange ?? started;
      this.#firstChange = null;
      for (const reader of [...this.#readers]) await reader.refresh(since, this.counts);
      this.counts.lastEmitMs = performance.now() - started;
    })().finally(() => { this.#emitting = null; });
    return this.#emitting;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer !== undefined) clearInterval(this.#timer);
    await this.#emitting?.catch(() => { /* said to whoever asked for it */ });
    for (const reader of [...this.#readers]) await reader.close();
    await this.#writing.catch(() => { /* said to its writer */ });
    await this.#writer.close();
  }

  #write<T>(write: () => Promise<T>): Promise<T> {
    if (this.#closed) return Promise.reject(new Error('the live DuckDB is closed'));
    const next = this.#writing.then(write, write);
    this.#writing = next.catch(() => { /* said to its writer */ });
    return next;
  }

  /** `plan`'s load on the writer's connection, held for it alone; an attach step puts in the value `held` names (the
   *  batch, as Arrow IPC, by the database's own Arrow import). Its last statement's first cell answered: a keep's rows
   *  deleted. */
  #run(plan: PlanText, held: Readonly<Record<string, Uint8Array>> = {}): Promise<number> {
    return this.#writer.exclusive(async (conn: HeldConnection) => {
      let last: unknown[][] = [];
      await runLoad(plan, async (sql: PlannedSql, params: readonly PlanValue[]) => { last = await conn.rows(sql, params); }, {
        values: {
          attach: async (name, as) => {
            const bytes = Object.hasOwn(held, name) ? held[name] : undefined;
            if (bytes === undefined) throw new Error(`the plan attaches '${name}', which this write does not hold`);
            await conn.insertArrow(bytes, { table: as }, true);
          },
        },
      });
      return Number(last[0]?.[0] ?? 0);
    });
  }

  #changed(state: TableState, rows: number): void {
    state.version++;
    this.#firstChange ??= performance.now();
    this.counts.writes++;
    this.counts.rows += rows;
  }

  /** Each table whose keep's cadence has come round (its plan's `every` rows arrived since it last ran) kept: its
   *  rows past its retention deleted, a change of its own where any were. */
  async #bound(): Promise<void> {
    for (const state of this.#tables.values()) {
      const keep = state.table.plans.keep;
      if (keep === null || state.arrived < keep.every) continue;
      const deleted = await this.#run(keep.plan);
      state.arrived = 0;
      if (deleted > 0) {
        this.counts.trims++;
        state.version++;
      }
    }
  }
}

/** A grid's reader of the live DuckDB: its plans run on its own connection, the results its view read followed. */
export class DuckDbLiveReader implements LiveReader {
  readonly name = 'duckdb-live';
  readonly databaseType = DATABASE;
  readonly #live: LiveDuckDb;
  readonly #conn: DuckDbEngine;
  readonly #snapshot: Snapshot;
  readonly #release: () => void;
  /** The results read lately, by their SQL: each followed once read in the view followed last, or later. */
  readonly #kept = new Map<string, Kept>();
  /** The epoch of the view followed last; null before the first, and while it rests: nothing is followed then. */
  #floor: number | null = null;
  /** The view's epoch it followed when it rested (`rest`): followed again once its grid runs again (`execute`). */
  #rested: number | null = null;
  readonly #listeners = new Set<(e: LiveEvent) => void>();
  #closed = false;

  constructor(live: LiveDuckDb, conn: DuckDbEngine, snapshot: Snapshot, release: () => void) {
    this.#live = live;
    this.#conn = conn;
    this.#snapshot = snapshot;
    this.#release = release;
  }

  /** A planned query's rows: a kept result still fresh answers it (no run: elapsed 0), else it runs and is kept. */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    // ITS GRID RUNNING AGAIN, its sheet shown: what it followed when it rested is followed again. The grid's run is a
    // tick of the view it has -- no view lands, so no `follow` is told (the user, 2026-10-29: the desk in the tab
    // "stops ticking if you change tabs and come back": every grid left resting for good)
    if (this.#floor === null && this.#rested !== null) {
      this.#floor = this.#rested;
      this.#rested = null;
    }
    const kept = this.#kept.get(plan.sql);
    if (kept !== undefined && this.#fresh(kept)) {
      kept.epoch = Math.max(kept.epoch, epoch);
      return { ...kept.result, epoch, elapsedMs: 0 };
    }
    const { tables, clock } = this.#live.reads(plan.sql);
    // the versions read BEFORE it runs: a write landing meanwhile runs it again at the next emit, never goes unseen
    const versions = this.#live.versions(tables);
    const result = await this.#conn.execute(plan, epoch, signal);
    this.#kept.set(plan.sql, { plan, result, digest: digest(result), tables, versions, clock, epoch: Math.max(epoch, kept?.epoch ?? 0) });
    return result;
  }

  stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    return this.#conn.stream(plan, epoch, onChunk, signal);
  }

  run(sql: PlannedSql, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return this.#conn.run(sql, epoch, signal);
  }

  /** The view of `epoch` landed: what it read, and anything read since, followed; what only older views read let go. */
  async follow(epoch: number): Promise<void> {
    this.#floor = epoch;
    this.#rested = null;
    for (const [sql, kept] of this.#kept) if (kept.epoch < epoch) this.#kept.delete(sql);
  }

  onLive(listener: (e: LiveEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  /**
   * Its grid not shown (a sheet hidden): nothing followed until its grid runs again (`execute`, its sheet shown) or its
   * next view (`follow`), so an emit runs none of its results -- on the tab's one DuckDB a hidden sheet's grids would
   * otherwise run at every emit (measured 2026-10-27, the desk in the tab: every tab visited, an emit 7.1 s). Its kept
   * results stay; a run of one gone stale runs it again.
   */
  rest(): void {
    if (this.#floor !== null) this.#rested = this.#floor;
    this.#floor = null;
  }

  /** How many results it follows now. */
  get following(): number {
    const floor = this.#floor;
    return floor === null ? 0 : [...this.#kept.values()].filter((k) => k.epoch >= floor).length;
  }

  /**
   * THE EMIT, for this reader: its followed results that are stale run again in one transaction (one snapshot), each
   * kept; the reader told, with `since`, when any came back different. One that fails is let go (the grid's next run
   * runs it), and said.
   */
  async refresh(since: number, counts: LiveCounts): Promise<void> {
    const floor = this.#floor;
    if (this.#closed || floor === null) return;
    const stale = [...this.#kept.values()].filter((k) => k.epoch >= floor && !this.#fresh(k));
    if (stale.length === 0) return;
    // each one's versions read before the snapshot is: a write after them is seen again at the next emit
    const versions = stale.map((k) => this.#live.versions(k.tables));
    let changed = false;
    const failed: string[] = [];
    for (const sql of this.#snapshot.begin) await this.#conn.run(sql, 0);
    try {
      for (const [i, kept] of stale.entries()) {
        let result: ResultTable;
        try {
          result = await this.#conn.execute(kept.plan, kept.epoch);
        } catch (e) {
          this.#kept.delete(kept.plan.sql);
          failed.push(e instanceof Error ? e.message : String(e));
          continue;
        }
        const next = digest(result);
        counts.reruns++;
        if (next === kept.digest) counts.unchanged++;
        else changed = true;
        kept.result = result;
        kept.digest = next;
        kept.versions = versions[i] ?? [];
      }
    } finally {
      for (const sql of this.#snapshot.commit) await this.#conn.run(sql, 0);
    }
    if (this.#closed) return;
    for (const why of failed) for (const listener of [...this.#listeners]) listener({ since, ended: null, failed: why });
    if (!changed) return;
    counts.told++;
    for (const listener of [...this.#listeners]) listener({ since, ended: null });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#listeners.clear();
    this.#kept.clear();
    this.#release();
    await this.#conn.close();
  }

  /** A kept result is fresh while every table it read is at the version it read, and it reads no clock. */
  #fresh(kept: Kept): boolean {
    if (kept.clock) return false;
    const now = this.#live.versions(kept.tables);
    return now.every((v, i) => v === kept.versions[i]);
  }
}

/** A connection of the database's, its session set up. */
async function connected(options: LiveDuckDbOptions): Promise<DuckDbEngine> {
  const engine = await options.connect();
  for (const statement of options.setup ?? []) await engine.run(statement, 0);
  return engine;
}

function escaped(name: string): string {
  return name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** A result's values as one string to compare (a bigint as its text, a typed array as its bytes). */
function digest(result: ResultTable): string {
  return JSON.stringify([result.rowCount, result.columns.map((c) => c.values)], (_k, v: unknown) =>
    (typeof v === 'bigint' ? v.toString() : ArrayBuffer.isView(v) ? Array.from(new Uint8Array(v.buffer, v.byteOffset, v.byteLength)) : v));
}
