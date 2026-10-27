// A LIVE DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md): a streaming engine's concepts on a DuckDB the page holds -- the tab's
// DuckDB-WASM, no server -- at a demo's scale. A dev, demo and simulation tool, not a streaming database.
//
//   STREAMS AND KEYED TABLES. A stream is a table written only by appends, its history bounded (`keep`: the latest rows
//   by a column, or those within some seconds of its latest), so a day's tape never outgrows a tab; a keyed table holds
//   the latest row a key, written by upserts (Proton's versioned_kv: a name's quote). Every write marks its table
//   changed: a version, one a write.
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
// It spells no SQL (D5, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md): a write's rows go in as Arrow, through a staging table,
// and every statement it runs -- a write's, a bound's, a snapshot's transaction -- is the planner's, for an operation it
// names (host-operations.ts), asked once a shape. Every followed result runs again whole (an incremental view,
// ClickHouse's lesson, only where measured: §3).

import { arrowIpcStream, type MadeColumn } from './arrow-ipc.ts';
import type { DuckDbEngine } from './duckdb.ts';
import type { RawTable } from './engine.ts';
import type { HostName, HostOperation, HostOperationsSource } from './host-operations.ts';
import type { LiveEvent, LiveReader } from './live.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable } from './result.ts';

/** How a stream's history is bounded: its latest `rows` by `column`, or its rows within `seconds` of `column`'s latest
 *  (a time column: the stream's own clock, which a simulated market runs at its own speed). */
export type Keep =
  | { readonly column: string; readonly rows: number }
  | { readonly column: string; readonly seconds: number };

/** A table the live engine follows writes to: a stream (appends, bounded by `keep`) or a keyed table (upserts). */
export interface LiveTable {
  /** As the model declares it, and the planner's SQL names it (MARKET_TRADE). */
  readonly name: string;
  /** Its schema, where it is not the database's default one. */
  readonly schema?: string;
  readonly kind: 'stream' | 'keyed';
  /** A keyed table's key: its primary key's columns (`INSERT OR REPLACE` replaces by it). */
  readonly key?: readonly string[];
  readonly keep?: Keep;
  /**
   * A stream's columns, in the table's order: a batch of exactly these, in this order, goes straight into the table
   * (Arrow appended by position: one round trip); any other batch through a staging table, by name. Measured
   * (docs/DUCKDB_LIVE_2026_10_25.md §2): the staging path's four statements were most of a write's time.
   */
  readonly columns?: readonly string[];
}

/** Rows a page made, a column at a time (arrow-ipc.ts MadeColumn), each named as the table's column it goes in. */
export type LiveRows = Readonly<Record<string, MadeColumn>>;

export interface LiveDuckDbOptions {
  /** A connection of the database's own: the writer's, and each reader's (each reads its own snapshot) -- the tab's set
   *  up as every connection of its is (duckdb-tab.ts DuckDbInTab.connect). */
  readonly connect: () => DuckDbEngine | Promise<DuckDbEngine>;
  /** The planner, which writes each statement the live engine runs (D5): a write's, a bound's, a transaction's. */
  readonly host: HostOperationsSource;
  /** The session every connection is set up with first (the planner's: engine.ts setUpSession). */
  readonly setup?: readonly string[];
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
  /** Writes (appends, upserts), the rows they wrote, and the bounds that trimmed a stream. */
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
  readonly begin: readonly string[];
  readonly commit: readonly string[];
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

/** A table's state: its version (one a write), and the rows appended since its bound last trimmed it. */
interface TableState {
  readonly table: LiveTable;
  /** As the planner names it: the model's declared table. */
  readonly target: HostName;
  /** The staging table a write's rows land in first: a name of the engine's own. */
  readonly staging: HostName;
  version: number;
  appended: number;
  /** A staging table a failed write left: dropped before the next is loaded. */
  stagingLeft: boolean;
}

export class LiveDuckDb {
  readonly #options: LiveDuckDbOptions;
  readonly #writer: DuckDbEngine;
  readonly #snapshot: Snapshot;
  readonly #tables = new Map<string, TableState>();
  readonly #readers = new Set<DuckDbLiveReader>();
  /** The planner's statements, by the operations' shape: asked once each. */
  readonly #written = new Map<string, Promise<string[][]>>();
  #timer: ReturnType<typeof setInterval> | undefined;
  /** Writes one at a time: each through a staging table of its table's. */
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

  /** The tables written through this engine, each made beforehand (the planner's DDL, `run`): a stream or a keyed table. */
  declare(tables: readonly LiveTable[]): void {
    for (const table of tables) {
      if (this.#tables.has(table.name)) throw new Error(`the live table ${table.name} is declared already`);
      if (table.kind === 'keyed' && (table.key === undefined || table.key.length === 0)) throw new Error(`${table.name}: a keyed table names its key`);
      if (table.kind === 'keyed' && table.keep !== undefined) throw new Error(`${table.name}: a keyed table keeps a row a key, not a history`);
      if (table.kind === 'stream' && table.key !== undefined) throw new Error(`${table.name}: a stream is appended to, never keyed`);
      this.#tables.set(table.name, {
        table,
        target: { ...(table.schema === undefined ? {} : { schema: table.schema }), name: table.name, declared: true },
        staging: { name: `__live_${table.schema === undefined ? '' : `${table.schema}_`}${table.name}` },
        version: 0,
        appended: 0,
        stagingLeft: true,
      });
    }
  }

  /** A statement the planner wrote, on the writer's connection -- a table made, its first rows: no write a reader is told of. */
  run(sql: string): Promise<RawTable> {
    return this.#write(() => this.#writer.run(sql, 0));
  }

  /** `count` rows appended to the stream `table`, each column the table's of its name (any it lacks, null): straight in
   *  when they are the table's declared columns in its order (LiveTable.columns), else through its staging table. */
  async append(table: string, count: number, rows: LiveRows): Promise<number> {
    const state = this.#state(table, 'stream');
    const names = Object.keys(rows);
    const declared = state.table.columns;
    if (declared !== undefined && declared.length === names.length && declared.every((c, i) => c === names[i])) {
      return this.#write(async () => {
        await this.#writer.loadArrow({ ...(state.table.schema === undefined ? {} : { schema: state.table.schema }), table: table, append: true },
          once(arrowIpcStream(count, rows)));
        state.appended += count;
        this.#changed(state, count);
        return count;
      });
    }
    const columns = names.map((name) => ({ target: name, from: name }));
    return this.#write(async () => {
      const written = await this.#staged(state, arrowIpcStream(count, rows), `append ${table} ${names.join(',')}`,
        { op: 'insert', target: state.target, columns, from: { relation: state.staging } });
      state.appended += written;
      this.#changed(state, written);
      return written;
    });
  }

  /** `count` rows upserted into the keyed table `table`: each the latest of its key, an older row of the key replaced --
   *  the batch's last row of a key where it holds more than one. */
  async upsert(table: string, count: number, rows: LiveRows): Promise<number> {
    const state = this.#state(table, 'keyed');
    const columns = Object.keys(rows).map((name) => ({ target: name, from: name }));
    return this.#write(async () => {
      const written = await this.#staged(state, arrowIpcStream(count, rows), `upsert ${table} ${Object.keys(rows).join(',')}`,
        { op: 'upsert', target: state.target, columns, key: state.table.key ?? [], from: { relation: state.staging } });
      this.#changed(state, written);
      return written;
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

  /** The planner's statements for `operations`, asked once for each `shape` (a write's table and columns, a bound's). */
  #statements(shape: string, operations: readonly HostOperation[]): Promise<string[][]> {
    let asked = this.#written.get(shape);
    if (asked === undefined) {
      asked = this.#options.host.hostOperations(DATABASE, operations);
      this.#written.set(shape, asked);
      asked.catch(() => { this.#written.delete(shape); });
    }
    return asked;
  }

  /** `ipc` loaded into the table's staging table, then written from it into the table by `write`: the rows written. The
   *  staging table is dropped after, and before only where a failed write left one. */
  async #staged(state: TableState, ipc: Uint8Array, shape: string, write: HostOperation): Promise<number> {
    const [drop, into] = await this.#statements(shape, [{ op: 'drop', kind: 'table', target: state.staging }, write]);
    await this.#writer.loadArrow({ table: state.staging.name }, once(ipc), state.stagingLeft ? drop ?? [] : []);
    state.stagingLeft = true;
    try {
      let written: RawTable | undefined;
      for (const sql of into ?? []) written = await this.#writer.run(sql, 0);
      return Number(written?.columns[0]?.values[0] ?? 0);
    } finally {
      for (const sql of drop ?? []) await this.#writer.run(sql, 0);
      state.stagingLeft = false;
    }
  }

  #state(table: string, kind: LiveTable['kind']): TableState {
    const state = this.#tables.get(table);
    if (state === undefined) throw new Error(`${table} is not a live table: declare it first`);
    if (state.table.kind !== kind) throw new Error(`${table} is ${state.table.kind === 'stream' ? 'a stream: it is appended to' : 'keyed: it is upserted into'}`);
    return state;
  }

  #changed(state: TableState, rows: number): void {
    state.version++;
    this.#firstChange ??= performance.now();
    this.counts.writes++;
    this.counts.rows += rows;
  }

  /** Each stream past its bound by a tenth of it (a span's: by any append) trimmed back to it: a change of its own. */
  async #bound(): Promise<void> {
    for (const state of this.#tables.values()) {
      const keep = state.table.keep;
      if (keep === undefined || state.appended === 0) continue;
      if ('rows' in keep && state.appended < Math.max(1, keep.rows / 10)) continue;
      const operation: HostOperation = 'rows' in keep
        ? { op: 'bound', target: state.target, column: keep.column, rows: keep.rows }
        : { op: 'bound', target: state.target, column: keep.column, millis: Math.round(keep.seconds * 1000) };
      const [bound] = await this.#statements(`bound ${state.table.name}`, [operation]);
      let deleted = 0;
      for (const sql of bound ?? []) deleted += Number((await this.#writer.run(sql, 0)).columns[0]?.values[0] ?? 0);
      state.appended = 0;
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
  /** The epoch of the view followed last; null before the first: nothing is followed until a view is. */
  #floor: number | null = null;
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

  run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return this.#conn.run(sql, epoch, signal);
  }

  /** The view of `epoch` landed: what it read, and anything read since, followed; what only older views read let go. */
  async follow(epoch: number): Promise<void> {
    this.#floor = epoch;
    for (const [sql, kept] of this.#kept) if (kept.epoch < epoch) this.#kept.delete(sql);
  }

  onLive(listener: (e: LiveEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  /**
   * Its grid not shown (a sheet hidden): nothing followed until its next view (`follow`), so an emit runs none of its
   * results -- on the tab's one DuckDB a hidden sheet's grids would otherwise run at every emit (measured 2026-10-27, the
   * desk in the tab: every tab visited, an emit 7.1 s). Its kept results stay; a run of one gone stale runs it again.
   */
  rest(): void {
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

/** One chunk, as the async iterable an Arrow load reads. */
async function* once(bytes: Uint8Array): AsyncIterable<Uint8Array> {
  yield bytes;
}

function escaped(name: string): string {
  return name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** A result's values as one string to compare (a bigint as its text, a typed array as its values). */
function digest(result: ResultTable): string {
  return JSON.stringify([result.rowCount, result.columns.map((c) => c.values)], (_k, v: unknown) =>
    (typeof v === 'bigint' ? v.toString() : ArrayBuffer.isView(v) ? Array.from(v as unknown as ArrayLike<number>) : v));
}
