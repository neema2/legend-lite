// A LIVE RESULT IN THE TAB (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21): the rows of a table the tab published
// (deephaven.ts), opened by its shared name with the JS library the Deephaven server itself serves (`/jsapi/dh-core.js`,
// loaded at run time from the person's own server, never bundled: it is under the Deephaven Community License, DH11),
// for the rows the grid shows, as each update cycle changes them. The library's session is its own: it signs in with
// the same credential, and opens what the sender's session published.

import type { DeephavenServer } from './deephaven.ts';

/** The visible rows, as one update left them: the table's columns, and each row's values in their order. */
export interface LiveRows {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly unknown[])[];
}

/** A watch on a live result: closed, it stops the updates and ends the library's session. */
export interface LiveWatch {
  close(): void;
}

/** What the tab asks of a watch: the rows from `first` to `last` (inclusive, by position), each update at most every
 *  `intervalMs` (the server's update cycle is the faster bound), and how it loads the library (a test's own). */
export interface LiveWatchOptions {
  readonly first: number;
  readonly last: number;
  readonly intervalMs?: number;
  readonly load?: (url: string) => Promise<DhModule>;
}

/** The part of Deephaven's JS library a watch uses, as its own API names it. */
export interface DhModule {
  readonly CoreClient: new (url: string) => DhClient;
  readonly Table: { readonly EVENT_UPDATED: string };
}

interface DhClient {
  login(credentials: { readonly type: string; readonly token: string }): Promise<void>;
  getAsIdeConnection(): Promise<{ getSharedObject(name: string, type: 'Table'): Promise<DhTable> }>;
  disconnect(): void;
}

interface DhColumn {
  readonly name: string;
}

interface DhTable {
  readonly columns: readonly DhColumn[];
  setViewport(first: number, last: number, columns: readonly DhColumn[], updateIntervalMs: number): void;
  addEventListener(name: string, listener: (e: { readonly detail: DhViewport }) => void): () => void;
  close(): void;
}

interface DhViewport {
  readonly rows: readonly { get(column: DhColumn): unknown }[];
}

/** Opens the published table `name` on `server` and calls `onRows` with the visible rows at every update. */
export async function watch(server: DeephavenServer, name: string, onRows: (rows: LiveRows) => void,
  options: LiveWatchOptions): Promise<LiveWatch> {
  const load = options.load ?? (async (url: string): Promise<DhModule> =>
    ((await import(/* @vite-ignore */ url)) as { default: DhModule }).default);
  const dh = await load(`${server.url}/jsapi/dh-core.js`);
  const client = new dh.CoreClient(server.url);
  try {
    await client.login({ type: server.signIn.type, token: server.signIn.token });
    const table = await (await client.getAsIdeConnection()).getSharedObject(name, 'Table');
    const columns = table.columns.map((c) => c.name);
    const stop = table.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
      onRows({ columns, rows: e.detail.rows.map((row) => table.columns.map((c) => row.get(c))) });
    });
    table.setViewport(options.first, options.last, table.columns, options.intervalMs ?? 200);
    return {
      close: () => {
        stop();
        table.close();
        client.disconnect();
      },
    };
  } catch (e) {
    client.disconnect();
    throw e;
  }
}
