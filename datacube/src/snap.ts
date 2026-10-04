// Snap mode: freeze the rows behind the current view and explore them
// locally.
//
// This is the primary mental model, not an option beside an automatic
// heuristic. Two reasons, one of them the important one:
//
//  - Legibility. With automatic plane selection the user cannot tell
//    why a query took 20ms or 3 seconds. Snap-versus-live is two states
//    a person can reason about, and it is LESS machinery than
//    auto-selection, not more.
//  - Correctness of the work. An analyst tying out numbers needs a
//    stable denominator. If the underlying data moves mid-analysis the
//    figures shift under them and the work is unfalsifiable. "I
//    reconciled against the 09:00 snap" is auditable; "against live
//    data this morning" is not.
//
// The historical objection to caching is stale data, and it is taken
// seriously here. The defect was never that data was old -- it was that
// data was old and you could not tell. So three rules are enforced
// rather than documented:
//
//   1. A snap always carries its timestamp and row count, so the UI can
//      never fail to show what is being looked at.
//   2. An operation the embedded engine cannot express is REFUSED with
//      a reason. Silently escalating to live would break the
//      point-in-time guarantee invisibly, which is worse than an error.
//   3. The snap captures source rows at drillable grain, never
//      aggregated results, so drill-through audits the same frozen data
//      it is drilling into.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { Planner } from './cube.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import type { Receipt } from '../../engine-client/src/receipt.ts';

/**
 * A LIVE plane on another machine (the warehouse): where a snap's rows come
 * from when they are not already in the tab. Counted there, then streamed as
 * Arrow into the local store.
 */
export interface RemoteSource {
  /** Raw SQL on the remote plane: the preflight count. */
  run: QueryEngine['run'];
  /** `onReceipt` hears, once the statement has succeeded, what the server issued for it. */
  arrowChunks(sql: string, signal?: AbortSignal, onReceipt?: (receipt: Receipt) => void): AsyncIterable<Uint8Array>;
}

/** A local store that can take Arrow chunks (DuckDbEngine). */
interface ArrowLoader {
  loadArrow(
    target: { readonly schema?: string; readonly table: string },
    chunks: AsyncIterable<Uint8Array>,
  ): Promise<void>;
}

/**
 * Memory ceiling. A ~500MB tab
 * budget holds about 10M rows across every data shape measured --
 * 9.3M for a UUID-bearing worst case, 33.8M for realistic shapes.
 */
export const MAX_SNAP_ROWS = 10_000_000;

export interface SnapInfo {
  readonly label: string;
  readonly takenAt: Date;
  readonly rowCount: number;
  /** Table name the snap materialised into. */
  readonly table: string;
  /** Its schema, when the live source's table has one (a warehouse's `sales.v_orders`). */
  readonly schema?: string;
  /**
   * What a query should read from while this snap holds: a relation the
   * planner can resolve, as protocol (`#>{db.TABLE_SNAP}#`), never a bare
   * SQL identifier -- the query is Pure before it is SQL.
   */
  readonly source: ValueSpecification;
  /** The pull from a remote live plane that made it: the server's receipt. */
  readonly pulledBy?: Receipt;
}

/**
 * Where a snap materialises: a table the cube's MODEL also declares, and the
 * relation that reads it (`#>{db.TABLE_SNAP}#`), so a snapped cube's queries
 * are planned exactly as live ones are.
 */
export interface SnapTarget {
  readonly schema?: string;
  readonly table: string;
  readonly source: ValueSpecification;
  /**
   * What the COPY applies so it holds the model's declared types (the catalog model's
   * `conversions`, CatalogModel.database): a zoned timestamp read in place under the source's
   * UTC session is copied as its UTC wall time, so the tab's store, in whatever zone, reads the
   * same values (docs/DATACUBE_APP_PLAN_2026_10_02.md, leg B). SQL in the catalog's own terms --
   * DuckDB's, whose catalog describes every column (catalog-model.ts) -- so it is applied by the
   * store the copy is written to, after the pull: the pull runs where the rows are, in that
   * database's SQL (a Postgres catalog's is Postgres), and a rewrite there would have to be
   * spelled in it.
   */
  readonly conversions: readonly { readonly column: string; readonly sql: string }[];
  /**
   * How a query on the copy is planned: the SAME model against the runtime of the store the copy
   * is in (`InferredModel.snapRuntime`). The rows are pulled with the live plan, which runs where
   * they are; every query on the copy runs here, in the tab's engine, so it is planned for that
   * engine's database type (docs/DATACUBE_APP_PLAN_2026_10_02.md, leg C).
   */
  readonly planner: Planner;
}

export type PlaneState =
  | { readonly mode: 'live' }
  | { readonly mode: 'snapped'; readonly snap: SnapInfo };

/** Refusal rather than silent escalation. See rule 2 above. */
export class SnapRefusal extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.name = 'SnapRefusal';
    this.reason = reason;
  }
}

export interface PreflightEstimate {
  readonly rowCount: number;
  readonly withinLimit: boolean;
  /** Present when the snap would be refused. */
  readonly refusal?: string;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function qualified(schema: string | undefined, table: string): string {
  return schema ? `${quoteIdent(schema)}.${quoteIdent(table)}` : quoteIdent(table);
}

export class SnapManager {
  /**
   * The local store a snapshot freezes INTO.
   *
   * Null on a plane where a remote engine answers the queries: there
   * is no local table to materialise into and nothing here can
   * invent one. Every method that needs it refuses by name rather
   * than half-working -- the cache mode is what will fill this in.
   */
  readonly #engine: QueryEngine | null;
  /** The live plane, when it is remote; absent when live is the local store. */
  readonly #remote: RemoteSource | null;
  #state: PlaneState = { mode: 'live' };
  #counter = 0;

  constructor(engine: QueryEngine | null, remote: RemoteSource | null = null) {
    this.#engine = engine;
    this.#remote = remote;
  }

  /**
   * The local store, or a refusal naming what is missing.
   *
   * "Cannot read properties of null" is not an answer anyone can
   * act on; "this plane has no local store to freeze into" is.
   */
  #localStore(): QueryEngine {
    if (!this.#engine) {
      throw new SnapRefusal(
        'this cube\u2019s queries run on a remote engine, which has no'
        + ' local store to freeze a snapshot into. Caching a source'
        + ' locally is a mode of its own, and not this one.',
      );
    }
    return this.#engine;
  }

  get state(): PlaneState {
    return this.#state;
  }

  get isSnapped(): boolean {
    return this.#state.mode === 'snapped';
  }

  /**
   * The relation a query should read from, given the current plane.
   *
   * This existed and NOTHING CALLED IT, so snapping materialised a
   * table and then went on querying the live source: the badge
   * changed and the data did not. The controller calls it on every
   * refresh now, and a test pins that the snapped plane reads the
   * snap.
   */
  sourceFor(liveSource: ValueSpecification): ValueSpecification {
    return this.#state.mode === 'snapped'
      ? this.#state.snap.source
      : liveSource;
  }

  /**
   * Check size before materialising anything, so the user is told
   * "this is 40 million rows" instead of watching a tab die.
   */
  async preflight(sourceSql: string, epoch: number): Promise<PreflightEstimate> {
    // counted where the rows are: the remote live plane when there is one
    const engine = this.#remote ?? this.#localStore();
    const r = await engine.run(
      `SELECT count(*) AS n FROM (${sourceSql})`,
      epoch,
    );
    const raw = r.columns[0]?.values[0] ?? 0;
    const rowCount = typeof raw === 'number' ? raw : Number(raw);

    if (rowCount > MAX_SNAP_ROWS) {
      return {
        rowCount,
        withinLimit: false,
        refusal:
          `${rowCount.toLocaleString()} rows exceeds the ` +
          `${MAX_SNAP_ROWS.toLocaleString()} row snap limit. ` +
          `Narrow the filter, or stay live.`,
      };
    }
    return { rowCount, withinLimit: true };
  }

  /**
   * Freeze `sourceSql` into a local table and switch to snapped. A
   * pivot's values are found by the cube's own query (plan.ts) on every
   * refresh, snapped or not, so nothing about them is captured here.
   */
  async snap(
    sourceSql: string,
    epoch: number,
    options: {
      readonly label?: string;
      /**
       * Where to materialise, and what to call it in a query afterwards:
       * a table the cube's model also declares. Supplied by the caller,
       * which owns the model.
       */
      readonly target: Omit<SnapTarget, 'planner'>;
    },
  ): Promise<SnapInfo> {
    const estimate = await this.preflight(sourceSql, epoch);
    if (!estimate.withinLimit) {
      throw new SnapRefusal(estimate.refusal ?? 'snap refused');
    }

    this.#counter += 1;
    let pulledBy: Receipt | undefined;
    const schema = options.target.schema;
    const bare = options.target.table;
    const table = qualified(schema, bare);
    const engine = this.#localStore();
    // the copy's conversions, applied by the store the copy is written to (SnapTarget.conversions):
    // the same rewrite an upload applies at ingest (upload.ts), in that store's SQL
    const conversions = options.target.conversions;
    const converted = (from: string): string => (conversions.length === 0 ? from
      : `SELECT * REPLACE (${conversions.map((c) => `${c.sql} AS ${quoteIdent(c.column)}`).join(', ')}) FROM ${from}`);
    if (this.#remote) {
      // The rows live on the server: the live plan runs there, and its Arrow
      // chunks stream into a local table of the same name, so the model -- and
      // the planned SQL -- read it unchanged. Exactly the rows the server let
      // this user read. A copy that needs converting lands beside its name
      // first, and is rewritten into it here.
      const loader = engine as unknown as Partial<ArrowLoader>;
      if (typeof loader.loadArrow !== 'function') {
        throw new SnapRefusal('the local store cannot load Arrow data, so a remote live plane cannot be snapped');
      }
      const landing = conversions.length === 0 ? bare : `${bare}__pulled`;
      await loader.loadArrow({ ...(schema ? { schema } : {}), table: landing },
        this.#remote.arrowChunks(sourceSql, undefined, (r) => { pulledBy = r; }));
      if (landing !== bare) {
        const pulled = qualified(schema, landing);
        try {
          await engine.run(`CREATE OR REPLACE TABLE ${table} AS ${converted(pulled)}`, epoch);
        } finally {
          await engine.run(`DROP TABLE IF EXISTS ${pulled}`, epoch);
        }
      }
    } else {
      await engine.run(`CREATE OR REPLACE TABLE ${table} AS ${conversions.length === 0 ? sourceSql
        : converted(`(${sourceSql}) AS ${quoteIdent('snap_source')}`)}`, epoch);
    }

    const takenAt = new Date();
    const snap: SnapInfo = {
      ...(pulledBy ? { pulledBy } : {}),
      label: options.label ?? defaultLabel(takenAt),
      takenAt,
      rowCount: estimate.rowCount,
      table: bare,
      ...(schema ? { schema } : {}),
      source: options.target.source,
    };
    this.#state = { mode: 'snapped', snap };
    return snap;
  }

  /** Drop the snap and return to live. */
  async release(): Promise<void> {
    const held = this.detach();
    if (held) await this.discard(held);
  }

  /**
   * Back to live, KEEPING the snap's table: going live is only done once a live
   * query has answered. Dropping first left a warehouse that could not be
   * reached showing the dropped copy's rows under "Live" (2026-09-29).
   */
  detach(): SnapInfo | null {
    if (this.#state.mode !== 'snapped') return null;
    const held = this.#state.snap;
    this.#state = { mode: 'live' };
    return held;
  }

  /** Live did not answer: back on the snap `detach` kept. */
  reattach(snap: SnapInfo): void {
    this.#state = { mode: 'snapped', snap };
  }

  /** Live answered: the kept snap's table goes. */
  async discard(snap: SnapInfo): Promise<void> {
    await this.#localStore()
      .run(`DROP TABLE IF EXISTS ${qualified(snap.schema, snap.table)}`, 0);
  }
}

function defaultLabel(at: Date): string {
  const hh = String(at.getHours()).padStart(2, '0');
  const mm = String(at.getMinutes()).padStart(2, '0');
  return `Snap ${hh}:${mm}`;
}
