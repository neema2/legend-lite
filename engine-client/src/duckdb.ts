// DuckDB, via an Arrow result, converted to our columnar scalars.
//
// The conversion is the interesting part. duckdb-wasm hands back an
// Arrow Table, and we do NOT pass that on: an Arrow Table cannot cross
// a worker boundary (structuredClone throws on it), and Arrow's own
// per-cell accessors are slower than they look -- a dictionary-encoded
// Utf8 column reads at ~84ns per cell, SLOWER than a plain Utf8 column
// at ~71ns, because the indirection costs more than direct
// materialisation through the generic JS path. Dictionary encoding is a
// payload win, not an access-speed one.
//
// BigInt is the trap worth naming. DuckDB returns BIGINT as JS BigInt,
// which throws inside JSON.stringify and refuses to mix with numbers in
// arithmetic. Left alone it produces a crash far from its cause, so it
// is narrowed here, once, at the boundary.

import type { Receipt } from './receipt.ts';
import type { QueryEngine, RawColumn, RawTable } from './engine.ts';
import { QueryError, typedByPlan } from './engine.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable, Scalar } from './result.ts';
import { dayText, decimalText, exactInteger, timeText, timestampText } from './values.ts';

/**
 * The slice of duckdb-wasm's connection we actually use. Declaring it
 * structurally keeps this file testable and keeps duckdb-wasm's types
 * (which differ between its browser and node entry points) out of our
 * public surface.
 */
export interface ArrowishConnection {
  query(sql: string): ArrowishTable | Promise<ArrowishTable>;
  /**
   * The STREAMING form, and the only real cancellation DuckDB-WASM
   * offers. `query()` runs to completion inside one call and cannot be
   * interrupted; `send()` hands back batches as they are produced, so
   * a superseded query can be stopped between them via `cancelSent()`.
   *
   * Optional only because the interface is structural and some test
   * fakes implement `query()` alone. Both real duckdb-wasm builds
   * provide it, so in the product this is THE path -- `query()` is
   * not a fallback anyone falls back to, and a guardrail test pins
   * that so it cannot quietly become one.
   */
  send?(sql: string): Promise<AsyncIterable<ArrowishTable>>;
  /** Cancel a query started with `send()`. True if it was still pending. */
  cancelSent?(): Promise<boolean>;
  /**
   * Load one Arrow IPC stream into a table: `create` makes it, otherwise the
   * rows are appended. How a snap of a REMOTE plane lands here: the server's
   * Arrow chunks, unconverted.
   */
  insertArrowFromIPCStream?(
    buffer: Uint8Array,
    options: { name: string; schema?: string; create?: boolean },
  ): void | Promise<void>;
  close?(): void | Promise<void>;
}

export interface ArrowishTable {
  readonly numRows: number;
  readonly schema: { fields: readonly { name: string; type: unknown }[] };
  getChildAt(i: number): ArrowishVector | null;
}

export interface ArrowishVector {
  readonly length: number;
  get(i: number): unknown;
  /** Arrow's chunks: the raw storage `get()` rounds (a timestamp to milliseconds). */
  readonly data?: readonly { readonly values: ArrayLike<unknown>; readonly offset: number; readonly length: number }[];
}

/**
 * Narrow one Arrow value to a Scalar.
 *
 * Exported because it is the highest-risk function in the file and
 * deserves direct tests rather than only being exercised through a
 * live database.
 */
export function toScalar(v: unknown): Scalar {
  if (v === null || v === undefined) return null;

  // An integer exactly: a number while it is one, a bigint beyond 2^53 (values.ts).
  if (typeof v === 'bigint') return exactInteger(v);

  if (typeof v === 'number' || typeof v === 'string' ||
      typeof v === 'boolean') {
    return v;
  }

  // A raw Arrow buffer with no column type to interpret it. Decimals
  // reach `decimalToScalar` instead, via the per-column converter;
  // anything landing here is genuinely opaque, so stringify it rather
  // than invent a reading.
  if (ArrayBuffer.isView(v)) return String(v);

  if (typeof v === 'object') {
    // Struct, list, map: render as JSON rather than "[object Object]",
    // so a nested column is at least legible in the grid.
    try {
      return JSON.stringify(v, (_k, x) =>
        typeof x === 'bigint' ? x.toString() : x,
      );
    } catch {
      return String(v);
    }
  }

  return String(v);
}

/**
 * Convert an Arrow DECIMAL to a Scalar, applying its scale.
 *
 * This existed as a bug first: DECIMAL reaches JS as a DecimalBigNum
 * (a Uint32Array of 128-bit two's-complement words) whose toString
 * yields the UNSCALED integer, so `sum(notional)` of 300.75 stringified
 * to "30075" -- a value 100x too large, silently, in a financial grid.
 * A live-engine test caught it; a mocked Arrow table would have agreed
 * with whatever this file believed.
 *
 * DecimalBigNum.toString already resolves the sign, so the two's
 * complement does not need unpicking here; only the decimal point has
 * to be reinserted. The result is a number when the unscaled value is
 * exactly representable, and the exact decimal string otherwise --
 * the same policy as BIGINT, for the same reason.
 */
export function decimalToScalar(raw: unknown, scale: number): Scalar {
  const unscaled = BigInt(String(raw));
  // scale 0 holds an integer (DuckDB returns a SUM of integers as one): an exact integer.
  // Otherwise ALWAYS the exact text: a DECIMAL column is never half numbers, half strings.
  return scale <= 0 ? exactInteger(unscaled * 10n ** BigInt(-scale)) : decimalText(unscaled, scale);
}

/** One column's decoder: a vector and a row to the value, exactly as stored (values.ts). */
type Decode = (vector: ArrowishVector, row: number) => Scalar;

/** The raw stored value of a row, across Arrow's chunks: what `get()` would round. */
function rawAt(vector: ArrowishVector, row: number): unknown {
  let i = row;
  for (const chunk of vector.data ?? []) {
    if (i < chunk.length) return chunk.values[chunk.offset + i];
    i -= chunk.length;
  }
  return undefined;
}

/**
 * A decoder for one column, chosen once from its Arrow STORAGE type -- what the database
 * wrote -- and exact: a DATE is its calendar day (never a local-midnight instant), a
 * TIMESTAMP keeps its microseconds, a DECIMAL its digits. What the value MEANS is the
 * plan's compiler type; this only keeps it exact.
 */
function decoderFor(type: unknown): Decode {
  const name = String(type ?? '');
  const nonNull = (d: Decode): Decode => (v, r) => (v.get(r) === null || v.get(r) === undefined ? null : d(v, r));
  if (type && typeof type === 'object' && 'scale' in type
      && typeof (type as { scale: unknown }).scale === 'number' && /^Decimal/.test(name)) {
    const scale = (type as { scale: number }).scale;
    return nonNull((v, r) => decimalToScalar(v.get(r), scale));
  }
  if (/^Date32</.test(name)) return nonNull((v, r) => dayText(Number(rawAt(v, r))));
  if (/^Date64</.test(name)) return nonNull((v, r) => dayText(Math.floor(Number(v.get(r)) / 86_400_000)));
  const unit = /<(SECOND|MILLISECOND|MICROSECOND|NANOSECOND)/.exec(name)?.[1];
  const toMicros = (raw: unknown): bigint => {
    const n = BigInt(raw as bigint | number);
    return unit === 'SECOND' ? n * 1_000_000n : unit === 'MILLISECOND' ? n * 1000n
      : unit === 'NANOSECOND' ? n / 1000n : n;
  };
  if (/^Timestamp</.test(name)) return nonNull((v, r) => timestampText(toMicros(rawAt(v, r))));
  if (/^Time(32|64)</.test(name)) return nonNull((v, r) => timeText(toMicros(rawAt(v, r))));
  return (v, r) => toScalar(v.get(r));
}

/** An Arrow table's columns and values; the PLAN types them (engine.ts typedByPlan). */
export function toRawTable(
  table: ArrowishTable,
  epoch: number,
  elapsedMs: number,
): RawTable {
  const columns: RawColumn[] = [];
  const fields = table.schema.fields;

  for (let c = 0; c < fields.length; c++) {
    const field = fields[c];
    if (!field) continue;
    const vector = table.getChildAt(c);
    const values: Scalar[] = new Array(table.numRows);
    if (vector) {
      const decode = decoderFor(field.type);
      for (let r = 0; r < table.numRows; r++) {
        values[r] = decode(vector, r);
      }
    } else {
      values.fill(null);
    }
    // no Pure type: the plan types a planned query's columns (engine.ts typedByPlan)
    columns.push({ name: field.name, values });
  }

  return { columns, rowCount: table.numRows, epoch, elapsedMs };
}

/**
 * Stitches streamed record batches into one columnar result.
 *
 * A batch carries the same shape as a table -- schema plus
 * `getChildAt` -- so the per-column converter is chosen ONCE, from the
 * first batch's field types, and reused. Re-deriving it per batch
 * would be the obvious way to make streaming slower than the
 * non-streaming path it replaced.
 *
 * An empty result still has to produce its COLUMNS: a query that
 * matches no rows must render an empty grid with headers, not a grid
 * with no columns at all.
 */
class BatchAccumulator {
  #names: string[] = [];
  #decode: Decode[] = [];
  #values: Scalar[][] = [];
  #rows = 0;
  #started = false;

  add(batch: ArrowishTable): void {
    if (!this.#started) {
      this.#started = true;
      for (const field of batch.schema.fields) {
        this.#names.push(field.name);
        this.#decode.push(decoderFor(field.type));
        this.#values.push([]);
      }
    }
    for (let c = 0; c < this.#names.length; c++) {
      const vector = batch.getChildAt(c);
      const out = this.#values[c] as Scalar[];
      const decode = this.#decode[c] as Decode;
      if (vector) {
        for (let r = 0; r < batch.numRows; r++) out.push(decode(vector, r));
      } else {
        for (let r = 0; r < batch.numRows; r++) out.push(null);
      }
    }
    this.#rows += batch.numRows;
  }

  build(epoch: number, elapsedMs: number): RawTable {
    return {
      columns: this.#names.map((name, i) => ({
        name,
        values: this.#values[i] as Scalar[],
      })),
      rowCount: this.#rows,
      epoch,
      elapsedMs,
    };
  }
}

export class DuckDbEngine implements QueryEngine {
  readonly name = 'duckdb';
  /** The database type of what this tab holds, as a Pure connection names it: what a model of its tables declares. */
  readonly databaseType = 'DuckDB';
  readonly #conn: ArrowishConnection;
  /** Tail of the queue of queries on this connection. See #serialised. */
  #chain: Promise<void> = Promise.resolve();
  /** Remote files a mounted source reads over HTTP (`readsRemote`), for the receipt. */
  readonly #reads: string[] = [];

  constructor(connection: ArrowishConnection) {
    this.#conn = connection;
  }

  /**
   * The host mounted a remote file (remote.ts `mountRemote`): queries here read it over HTTP.
   * Said on every receipt, so "ran in this tab" never hides that bytes came from elsewhere.
   */
  readsRemote(url: string): void {
    if (!this.#reads.includes(url)) this.#reads.push(url);
  }

  /** What this engine can say for a query it ran: here, and what it read from elsewhere. */
  receipt(): Receipt {
    return {
      plane: 'tab',
      where: "this tab's DuckDB",
      ...(this.#reads.length > 0 ? { reading: [...this.#reads] } : {}),
    };
  }

  /**
   * WHAT CANCELLATION MEANS HERE, precisely.
   *
   * Two paths, and the difference is real rather than cosmetic.
   *
   * With `send()` the query streams back in batches, so a superseded
   * query is CANCELLED -- `cancelSent()` stops it between batches and
   * DuckDB abandons the rest of the work. That is the path a real
   * duckdb-wasm connection takes.
   *
   * Without it, `query()` runs to completion inside one C++ call with
   * no interrupt, and the only honest win is the query that never
   * STARTS: levels are fetched in sequence, so during a burst every
   * request behind the one in flight is still avoidable.
   *
   * The distinction is worth keeping straight, because a UI that says
   * "cancelled" while a worker is still pegged is worse than one that
   * admits it is busy -- the next interaction is then slow for no
   * reason the user can see.
   *
   * THE BRANCH IS ON CAPABILITY, NOT ON THE SIGNAL, and that took a
   * correction. Streaming was first gated on a signal being passed,
   * which quietly left two live paths in the product: snapshotting and
   * drill-through pass no signal, so they took the non-streaming route
   * for no better reason than that nobody had threaded an argument to
   * them. Two materialisation paths that can drift apart is exactly
   * the shape this codebase refuses elsewhere. A connection that can
   * stream now always streams; the signal only decides whether the
   * stream can be cut short.
   */
  async execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return { ...typedByPlan(await this.run(plan.sql, epoch, signal), plan), receipt: this.receipt() };
  }

  async run(
    sql: string,
    epoch: number,
    signal?: AbortSignal,
  ): Promise<RawTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return this.#serialised(async () => {
      // Checked AGAIN after waiting for the connection. This is where
      // the promise of "queries behind the one in flight never start"
      // is actually kept: by the time the connection frees up, a
      // superseded query simply never runs.
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const started = performance.now();
      if (typeof this.#conn.send === 'function') {
        const acc = new BatchAccumulator();
        await this.#batches(sql, signal, (batch) => acc.add(batch));
        return acc.build(epoch, performance.now() - started);
      }
      return this.#whole(sql, epoch, signal, started);
    });
  }

  /**
   * A planned query's rows a batch at a time, as DuckDB produces them: each handed over
   * typed by the plan, none kept. A connection that cannot stream hands over its whole
   * result as one chunk (the same capability branch as `run`).
   */
  async stream(
    plan: Plan,
    epoch: number,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return this.#serialised(async () => {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const started = performance.now();
      if (typeof this.#conn.send === 'function') {
        await this.#batches(plan.sql, signal, (batch) =>
          onChunk(typedByPlan(toRawTable(batch, epoch, performance.now() - started), plan)));
        return;
      }
      onChunk(typedByPlan(await this.#whole(plan.sql, epoch, signal, started), plan));
    });
  }

  /**
   * Replace `target` with the rows of `chunks`, each a whole Arrow IPC stream
   * (the warehouse's chunks): the first creates the table, the rest append.
   * On the query queue, so no query reads a half-loaded table.
   */
  async loadArrow(
    target: { readonly schema?: string; readonly table: string },
    chunks: AsyncIterable<Uint8Array>,
  ): Promise<void> {
    const insert = this.#conn.insertArrowFromIPCStream;
    if (typeof insert !== 'function') {
      throw new Error('this DuckDB connection cannot load Arrow data');
    }
    const qi = (n: string) => `"${n.replace(/"/g, '""')}"`;
    const qualified = target.schema ? `${qi(target.schema)}.${qi(target.table)}` : qi(target.table);
    return this.#serialised(async () => {
      if (target.schema) await this.#conn.query(`CREATE SCHEMA IF NOT EXISTS ${qi(target.schema)}`);
      await this.#conn.query(`DROP TABLE IF EXISTS ${qualified}`);
      let first = true;
      for await (const bytes of chunks) {
        await insert.call(this.#conn, bytes, {
          name: target.table,
          ...(target.schema ? { schema: target.schema } : {}),
          create: first,
        });
        first = false;
      }
      if (first) throw new Error(`no data came back to load into ${qualified}`);
    });
  }

  /**
   * One query on the connection at a time.
   *
   * A DuckDB connection holds ONE pending query. Overlapping them
   * corrupts both -- a live-engine test caught the winner coming back
   * with zero rows when a second query was sent while the first was
   * still streaming.
   *
   * It also makes cancellation safe, which matters more. `cancelSent()`
   * cancels whatever is pending on the CONNECTION, not a particular
   * query, so cancelling a superseded query after the next one had
   * started would have killed the query the user is actually waiting
   * for. Serialising means the only cancellable query is ours.
   */
  async #serialised<T>(run: () => Promise<T>): Promise<T> {
    const previous = this.#chain;
    let release!: () => void;
    this.#chain = new Promise<void>((r) => {
      release = r;
    });
    // Never inherit a failure from the query ahead of us in the queue.
    await previous.catch(() => {});
    try {
      return await run();
    } finally {
      release();
    }
  }

  async #whole(
    sql: string,
    epoch: number,
    signal: AbortSignal | undefined,
    started: number,
  ): Promise<RawTable> {
    let table: ArrowishTable;
    try {
      table = await this.#conn.query(sql);
    } catch (cause) {
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return toRawTable(table, epoch, performance.now() - started);
  }

  /**
   * The streaming path -- the ONLY path for a connection that can
   * stream: each batch of `sql`'s result, in order, to `onBatch`
   * (`run` accumulates them, `stream` hands them on). `signal` is
   * optional: without one the batches are simply consumed to the end,
   * which is what a snap or a drill-through wants, and the result is
   * identical either way; with one, the query is cancelled between batches.
   */
  async #batches(
    sql: string,
    signal: AbortSignal | undefined,
    onBatch: (batch: ArrowishTable) => void,
  ): Promise<void> {
    const send = this.#conn.send;
    if (!send) throw new Error('streaming unavailable');

    const cancel = async (): Promise<void> => {
      // Best effort by design: cancelSent reports whether the query
      // was still pending, and a query that already finished is not a
      // failure to cancel -- there was simply nothing left to stop.
      try {
        await this.#conn.cancelSent?.();
      } catch {
        /* the query had already finished */
      }
    };

    let batches: AsyncIterable<ArrowishTable>;
    try {
      batches = await send.call(this.#conn, sql);
    } catch (cause) {
      if (signal?.aborted) throw signal.reason ?? cause;
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }

    try {
      for await (const batch of batches) {
        if (signal?.aborted) {
          await cancel();
          throw signal.reason ?? new Error('aborted');
        }
        onBatch(batch);
      }
    } catch (cause) {
      if (signal?.aborted) {
        await cancel();
        throw signal.reason ?? cause;
      }
      throw new QueryError(
        cause instanceof Error ? cause.message : String(cause),
        sql,
        { cause },
      );
    }

    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
  }

  async close(): Promise<void> {
    await this.#conn.close?.();
  }
}
