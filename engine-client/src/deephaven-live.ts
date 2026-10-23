// THE TAB'S LIVE READER (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, DH24): the rows of a table the tab published
// (deephaven.ts), opened by its shared name with the JS library the Deephaven server itself serves (`/jsapi/dh-core.js`,
// loaded at run time from the person's own server, never bundled: it is under the Deephaven Community License, DH11),
// every row of it, as each update cycle changes them. The library's session is its own: it signs in with the same
// credential, and opens what the sender's session published.

import type { DeephavenServer } from './deephaven.ts';

/** The part of Deephaven's JS library the live reader uses, as its own API names it. */
export interface DhModule {
  readonly CoreClient: new (url: string) => DhClient;
  readonly Table: { readonly EVENT_UPDATED: string };
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

interface DhIde {
  getSharedObject(name: string, type: 'Table'): Promise<DhTable>;
  addEventListener(name: string, listener: (e: unknown) => void): () => void;
}

interface DhTable {
  readonly columns: readonly DhColumn[];
  createSubscription(options: { readonly columns: readonly DhColumn[]; readonly updateIntervalMs: number }): DhSubscription;
  close(): void;
}

interface DhSubscription {
  addEventListener(name: string, listener: (e: { readonly detail: { readonly rows: readonly DhRow[] } }) => void): () => void;
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

  close(): void {
    this.#client.disconnect();
  }
}
