// A warehouse session's end, as its own error: what the warehouse's driver (warehouse.ts) raises when the warehouse
// refuses the token, and what a page's code reads to offer signing in again -- apart from the driver, so a page loads
// the driver only when it signs in to a warehouse (datacube/src/services/browser.ts).

import type { PlannedSql } from './planned-sql.ts';
import type { QueryEngine } from './query-engine.ts';
import type { Receipt } from './receipt.ts';

/**
 * The warehouse refused the token: expired, or issued by a warehouse that has since restarted.
 * Its own type, so the cube can offer to sign in again right where it says so.
 */
export class SessionExpired extends Error {
  readonly baseUrl: string;
  readonly principal: string;
  constructor(baseUrl: string, principal: string) {
    super(`the warehouse session for ${principal} has expired — sign in again`);
    this.name = 'SessionExpired';
    this.baseUrl = baseUrl;
    this.principal = principal;
  }
}

/** Whether `error`, or anything in its cause chain, is a `SessionExpired`. */
export function sessionExpired(error: unknown): SessionExpired | null {
  for (let e = error, depth = 0; e && depth < 10; e = (e as { cause?: unknown }).cause, depth++) {
    if (e instanceof SessionExpired) return e;
  }
  return null;
}

// ---- the warehouse's shapes (the C5-T last round's T1, 2026-11-15: out of warehouse.ts, the run side's driver, so an
// app takes them as records and reaches the driver through its door alone, engine-client's doors/warehouse.ts) ----

/** A signed-in session: where the warehouse is, and the bearer token. */
export interface WarehouseSession {
  /** e.g. `https://warehouse.example.com` (no trailing slash needed). */
  readonly baseUrl: string;
  readonly token: string;
  /** Who the server says the token is. */
  readonly principal: string;
  readonly expiresAt: string;
}

/** One table or view the user may read (the catalog call; W2 filters it). */
export interface CatalogObject {
  readonly schema: string;
  readonly name: string;
  readonly kind: string;
  /** The warehouse catalog it is in. */
  readonly catalog: string;
  /** That catalog's database type, as a Pure connection names it (`DuckDB`, `Postgres`): what its model declares. */
  readonly databaseType: string;
  /**
   * Its columns, as the warehouse's DuckDB catalog reports them: `type` its own name, and
   * STRUCTURED (as the catalog question reads one, DuckDb.CATALOG_COLUMNS_SQL) its canonical type and a
   * DECIMAL's precision and scale.
   */
  readonly columns: readonly {
    readonly name: string;
    readonly type: string;
    readonly logicalType: string | null;
    readonly precision: number | null;
    readonly scale: number | null;
    /** The catalog says it holds no NULL. */
    readonly notNull: boolean;
  }[];
}

/** A warehouse table read live, as an app holds one (the driver's WarehouseEngine, opened by its door): an engine, and
 *  its session renewed or signed in again where the warehouse's ends. */
export interface LiveWarehouse extends QueryEngine {
  /** Its catalog's database type (`DuckDB`, `Postgres`): what a statement run on it is written for. */
  readonly databaseType: string;
  /** A statement's rows as Arrow IPC chunks (a snap's copy streamed into the tab); `onReceipt` hears what was issued. */
  arrowChunks(sql: PlannedSql, signal?: AbortSignal, onReceipt?: (receipt: Receipt) => void): AsyncIterable<Uint8Array>;
  /** The same person signed in again, by their password: the session renewed in place. */
  signInAgain(password: string): Promise<void>;
  /** A session signed in elsewhere (the same person's, P2-297): queries read under it from now on. */
  renew(session: WarehouseSession): void;
}
