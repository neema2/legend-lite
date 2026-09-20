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

import type { QueryEngine } from './engine.ts';
import { QueryError } from './engine.ts';
import type { ResultColumn, ResultTable, Scalar } from './result.ts';

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

  if (typeof v === 'bigint') {
    // Narrow to number when it is exactly representable; otherwise keep
    // full precision as text rather than silently rounding. A trade id
    // that loses its last digits is worse than one rendered as a string.
    return v >= BigInt(Number.MIN_SAFE_INTEGER) &&
      v <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(v)
      : v.toString();
  }

  if (typeof v === 'number' || typeof v === 'string' ||
      typeof v === 'boolean') {
    return v;
  }

  if (v instanceof Date) return v;

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
  const safe =
    unscaled >= BigInt(Number.MIN_SAFE_INTEGER) &&
    unscaled <= BigInt(Number.MAX_SAFE_INTEGER);

  if (scale <= 0) return safe ? Number(unscaled) : unscaled.toString();

  const negative = unscaled < 0n;
  const digits = (negative ? -unscaled : unscaled)
    .toString()
    .padStart(scale + 1, '0');
  const text =
    `${negative ? '-' : ''}${digits.slice(0, -scale)}.${digits.slice(-scale)}`;

  return safe ? Number(text) : text;
}

/**
 * A converter for one column, chosen once from its Arrow type rather
 * than re-derived per cell.
 */
function converterFor(type: unknown): (v: unknown) => Scalar {
  if (
    type &&
    typeof type === 'object' &&
    'scale' in type &&
    typeof (type as { scale: unknown }).scale === 'number'
  ) {
    const scale = (type as { scale: number }).scale;
    return (v) => (v === null || v === undefined ? null : decimalToScalar(v, scale));
  }
  return toScalar;
}

/** Arrow's type object stringifies to a usable name; keep it simple. */
function typeName(t: unknown): string {
  if (t && typeof t === 'object' && 'toString' in t) {
    return String(t);
  }
  return 'Unknown';
}

export function toResultTable(
  table: ArrowishTable,
  epoch: number,
  elapsedMs: number,
): ResultTable {
  const columns: ResultColumn[] = [];
  const fields = table.schema.fields;

  for (let c = 0; c < fields.length; c++) {
    const field = fields[c];
    if (!field) continue;
    const vector = table.getChildAt(c);
    const values: Scalar[] = new Array(table.numRows);
    if (vector) {
      const convert = converterFor(field.type);
      for (let r = 0; r < table.numRows; r++) {
        values[r] = convert(vector.get(r));
      }
    } else {
      values.fill(null);
    }
    columns.push({ name: field.name, type: typeName(field.type), values });
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
  #types: string[] = [];
  #convert: ((v: unknown) => Scalar)[] = [];
  #values: Scalar[][] = [];
  #rows = 0;
  #started = false;

  add(batch: ArrowishTable): void {
    if (!this.#started) {
      this.#started = true;
      for (const field of batch.schema.fields) {
        this.#names.push(field.name);
        this.#types.push(typeName(field.type));
        this.#convert.push(converterFor(field.type));
        this.#values.push([]);
      }
    }
    for (let c = 0; c < this.#names.length; c++) {
      const vector = batch.getChildAt(c);
      const out = this.#values[c] as Scalar[];
      const convert = this.#convert[c] as (v: unknown) => Scalar;
      if (vector) {
        for (let r = 0; r < batch.numRows; r++) out.push(convert(vector.get(r)));
      } else {
        for (let r = 0; r < batch.numRows; r++) out.push(null);
      }
    }
    this.#rows += batch.numRows;
  }

  build(epoch: number, elapsedMs: number): ResultTable {
    return {
      columns: this.#names.map((name, i) => ({
        name,
        type: this.#types[i] as string,
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
  readonly #conn: ArrowishConnection;
  /** Tail of the queue of queries on this connection. See #serialised. */
  #chain: Promise<void> = Promise.resolve();

  constructor(connection: ArrowishConnection) {
    this.#conn = connection;
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
  async execute(
    sql: string,
    epoch: number,
    signal?: AbortSignal,
  ): Promise<ResultTable> {
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return this.#serialised(async () => {
      // Checked AGAIN after waiting for the connection. This is where
      // the promise of "queries behind the one in flight never start"
      // is actually kept: by the time the connection frees up, a
      // superseded query simply never runs.
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      const started = performance.now();
      if (typeof this.#conn.send === 'function') {
        return this.#stream(sql, epoch, signal, started);
      }
      return this.#whole(sql, epoch, signal, started);
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
  ): Promise<ResultTable> {
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
    return toResultTable(table, epoch, performance.now() - started);
  }

  /**
   * The streaming path -- the ONLY path for a connection that can
   * stream. `signal` is optional: without one the batches are simply
   * consumed to the end, which is what a snap or a drill-through
   * wants, and the result is identical either way.
   */
  async #stream(
    sql: string,
    epoch: number,
    signal: AbortSignal | undefined,
    started: number,
  ): Promise<ResultTable> {
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

    const acc = new BatchAccumulator();
    try {
      for await (const batch of batches) {
        if (signal?.aborted) {
          await cancel();
          throw signal.reason ?? new Error('aborted');
        }
        acc.add(batch);
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
    return acc.build(epoch, performance.now() - started);
  }

  async close(): Promise<void> {
    await this.#conn.close?.();
  }
}
