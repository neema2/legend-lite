// THE TAB'S LIVE READER (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, DH24): the rows of a table the tab published
// (deephaven.ts), opened by its shared name with the JS library the Deephaven server itself serves (`/jsapi/dh-core.js`,
// loaded at run time from the person's own server, never bundled: it is under the Deephaven Community License, DH11),
// every row of it, as each update cycle changes them. The library's session is its own: it signs in with the same
// credential, and opens what the sender's session published.

import type { DeephavenServer } from './deephaven.ts';
import type { ResultWindow } from './engine.ts';

/** The part of Deephaven's JS library the live reader uses, as its own API names it. */
export interface DhModule {
  readonly CoreClient: new (url: string) => DhClient;
  readonly Table: { readonly EVENT_UPDATED: string };
  readonly RangeSet: { ofRange(first: number, last: number): DhRangeSet };
}

interface DhRangeSet {
  readonly size: number;
}

interface DhClient {
  login(credentials: { readonly type: string; readonly token: string }): Promise<void>;
  getAsIdeConnection(): Promise<DhIde>;
  disconnect(): void;
}

interface DhColumn {
  readonly name: string;
}

/** A followed table's rows, as one update left them: every row's values, column by column, in the table's order. */
export interface FollowedRows {
  readonly columns: readonly string[];
  readonly values: readonly (readonly unknown[])[];
}

/** A followed table: closed, it stops the updates (the library's session stays, for the engine's other tables). */
export interface Followed {
  close(): void;
}

/** A followed window's rows, as one update left them, where the first of them is in the table, and the table's size. */
export interface WindowRows extends FollowedRows {
  readonly offset: number;
  readonly total: number;
}

/** A followed window of a table: moved, the next update carries the rows the new window shows. */
export interface FollowedWindow extends Followed {
  move(window: ResultWindow): void;
}

interface DhIde {
  getSharedObject(name: string, type: 'Table'): Promise<DhTable>;
  addEventListener(name: string, listener: (e: unknown) => void): () => void;
}

interface DhTable {
  readonly columns: readonly DhColumn[];
  /** How many rows the table has now: kept current while a subscription follows it. */
  readonly size: number;
  createSubscription(options: { readonly columns: readonly DhColumn[]; readonly updateIntervalMs: number }): DhSubscription;
  createViewportSubscription(options: {
    readonly columns: readonly DhColumn[];
    readonly rows: DhRangeSet;
    readonly updateIntervalMs: number;
  }): DhViewportSubscription;
  close(): void;
}

interface DhSubscription {
  addEventListener(name: string, listener: (e: { readonly detail: { readonly rows: readonly DhRow[] } }) => void): () => void;
  close(): void;
}

/** The library's viewport subscription: an update's rows are the window's, `offset` the first one's place. */
interface DhViewportSubscription {
  addEventListener(name: string,
    listener: (e: { readonly detail: { readonly offset: number; readonly rows: readonly DhRow[] } }) => void): () => void;
  update(options: { readonly rows: DhRangeSet; readonly columns: readonly DhColumn[] }): void;
  close(): void;
}

interface DhRow {
  get(column: DhColumn): unknown;
}

/**
 * THE ENGINE'S LIVE READER (docs/DEEPHAVEN_PLAN_2026_10_22.md DH24): Deephaven's JS library, loaded from the server and
 * signed in once, following the tables the engine's session published -- each by a subscription to every row (a grid
 * level's result is small: the row cap bounds it), its rows handed over on every update, as soon as the server has one
 * (`intervalMs`, 10: measured, a viewport or full subscription asked every 10 ms sees an update cycle's change 7 to 10
 * ms after it, where one asked at the 100 ms cycle waits up to the cycle again). `onEnded` is told when the library's
 * connection ends (the server went away).
 */
export class LiveClient {
  readonly #dh: DhModule & { readonly IdeConnection?: { readonly EVENT_DISCONNECT?: string } };
  readonly #client: DhClient;
  readonly #ide: DhIde;

  private constructor(dh: DhModule, client: DhClient, ide: DhIde) {
    this.#dh = dh;
    this.#client = client;
    this.#ide = ide;
  }

  /** Loads the library from `server` (never bundled: DH11) and signs in with the person's own credential. */
  static async open(server: DeephavenServer, onEnded: (why: string) => void,
    load?: (url: string) => Promise<DhModule>): Promise<LiveClient> {
    const loaded = load ?? (async (url: string): Promise<DhModule> =>
      ((await import(/* @vite-ignore */ url)) as { default: DhModule }).default);
    const dh = await loaded(`${server.url}/jsapi/dh-core.js`);
    const client = new dh.CoreClient(server.url);
    try {
      await client.login({ type: server.signIn.type, token: server.signIn.token });
      const ide = await client.getAsIdeConnection();
      const live = new LiveClient(dh, client, ide);
      ide.addEventListener(live.#dh.IdeConnection?.EVENT_DISCONNECT ?? 'disconnect',
        () => onEnded('the Deephaven server closed the live connection'));
      return live;
    } catch (e) {
      client.disconnect();
      throw e;
    }
  }

  /** Follows the published table `name`: `onRows` with every row at each update (the first: the rows as they are). */
  async follow(name: string, onRows: (rows: FollowedRows) => void, intervalMs = 10): Promise<Followed> {
    const table = await this.#ide.getSharedObject(name, 'Table');
    const columns = table.columns;
    const names = columns.map((c) => c.name);
    const subscription = table.createSubscription({ columns, updateIntervalMs: intervalMs });
    subscription.addEventListener(this.#dh.Table.EVENT_UPDATED, (e) => {
      const rows = e.detail.rows;
      onRows({ columns: names, values: columns.map((c) => rows.map((r) => r.get(c))) });
    });
    return {
      close: () => {
        subscription.close();
        table.close();
      },
    };
  }

  /**
   * Follows a WINDOW of the published table `name` (a viewport, docs/DEEPHAVEN_PLAN_2026_10_22.md V1): `onRows` with the
   * window's rows at each update, where the first of them is in the table and how many rows the table has -- the
   * server sends the window's rows only, so an update costs what a grid shows, whatever the table's size. `move`
   * changes the window; the next update carries the new one's rows. An update the library sends without its place or
   * the table's size is `onFailed`, never read as a window it is not.
   */
  async followWindow(name: string, window: ResultWindow, onRows: (rows: WindowRows) => void,
    onFailed: (error: Error) => void, intervalMs = 10): Promise<FollowedWindow> {
    const table = await this.#ide.getSharedObject(name, 'Table');
    const columns = table.columns;
    const names = columns.map((c) => c.name);
    const range = (w: ResultWindow): DhRangeSet => this.#dh.RangeSet.ofRange(w.offset, w.offset + Math.max(1, w.count) - 1);
    const subscription = table.createViewportSubscription({ columns, rows: range(window), updateIntervalMs: intervalMs });
    subscription.addEventListener(this.#dh.Table.EVENT_UPDATED, (e) => {
      const { offset, rows } = e.detail;
      const total = table.size;
      if (typeof offset !== 'number' || typeof total !== 'number') {
        onFailed(new Error(`Deephaven's viewport update said no place (${String(offset)}) or no size (${String(total)})`));
        return;
      }
      onRows({ columns: names, values: columns.map((c) => rows.map((r) => r.get(c))), offset, total });
    });
    return {
      move: (w) => { subscription.update({ rows: range(w), columns }); },
      close: () => {
        subscription.close();
        table.close();
      },
    };
  }

  close(): void {
    this.#client.disconnect();
  }
}
