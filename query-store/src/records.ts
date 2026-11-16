// Where the page's own store keeps its queries: every version of each, as legend-lite's
// `SavedQueries.java` keeps them in a file per query -- the current version the one whose
// `validUntil` is null. IndexedDB in a browser, a Map in a test.

import type { Query } from './wire.ts';

/**
 * Each query's versions, by its id, as the rules write them: the JSON text, kept as written -- so a number keeps its
 * spelling (`1.0`, a long beyond 2^53), as legend-lite's server keeps it in its file, where an object stored as a
 * structured clone would not.
 */
export interface Records {
  /**
   * One call of the rules: `f` is handed every query's versions by id (in id order) and answers the ones it wrote,
   * with its answer -- all in one transaction, so no other tab writes between the read and the writes (`f` runs
   * synchronously inside it).
   */
  call<T>(f: (held: ReadonlyMap<string, string>) => Called<T>): Promise<T>;
}

/** What a call answers: the queries it wrote (their versions' text, by id), and its own answer. */
export interface Called<T> {
  readonly changed: ReadonlyMap<string, string>;
  readonly answer: T;
}

export class MemoryRecords implements Records {
  readonly #data = new Map<string, string>();

  async call<T>(f: (held: ReadonlyMap<string, string>) => Called<T>): Promise<T> {
    const said = f(new Map([...this.#data].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))));
    for (const [id, text] of said.changed) this.#data.set(id, text);
    return said.answer;
  }
}

/** The database every app on this origin shares: one name, one layout, defined here only. */
export const DATABASE = 'legend-query';
const STORE = 'queries';
/**
 * 3: each query's versions as the JSON text the rules wrote. 2 kept them as objects (structured clones, which lose a
 * number's spelling), and 1 (the Query app's first layout) wrapped each version: `{query, validUntil, deletedAt}`
 * -- both moved over on upgrade, never dropped.
 */
const LAYOUT = 3;

/** A version as layout 1 kept it. */
interface Wrapped {
  readonly query: Query;
  readonly validUntil: number | null;
  readonly deletedAt: number | null;
}

/** Where a write to this origin's store is announced: every app's open tabs hear it. */
export const CHANNEL = 'legend-query-store';

/**
 * Told whenever a tab on this origin writes its browser store (a query saved in Query, say),
 * so an open list can be read again. Returns how to stop. Absent BroadcastChannel: never told.
 */
export function watchBrowserStore(onChange: () => void, name = CHANNEL): () => void {
  if (typeof BroadcastChannel === 'undefined') return () => undefined;
  const channel = new BroadcastChannel(name);
  channel.onmessage = () => onChange();
  return () => channel.close();
}

const upgraded = (e: unknown): Error =>
  // closed by a newer layout elsewhere (onversionchange)
  new Error(`the saved queries were upgraded by another tab: reload this page (${e instanceof Error ? e.message : String(e)})`);

export class BrowserRecords implements Records {
  readonly #db: Promise<IDBDatabase>;
  readonly #channel: string;
  #closed = false;
  #cancelOpen: () => void = () => undefined;

  constructor(factory: IDBFactory = globalThis.indexedDB, name = DATABASE, channel = CHANNEL) {
    this.#channel = channel;
    this.#db = new Promise((resolve, reject) => {
      const open = factory.open(name, LAYOUT);
      const refused = (error: unknown): void => {
        this.#closed = true;
        this.#cancelOpen = () => undefined;
        reject(error);
      };
      this.#cancelOpen = () => refused(new Error('the saved-query store has been closed'));
      open.onupgradeneeded = (e) => {
        if (this.#closed) { open.transaction?.abort(); return; }
        const db = open.result;
        if (e.oldVersion < 1) {
          db.createObjectStore(STORE);
          return;
        }
        const cursor = open.transaction!.objectStore(STORE).openCursor();
        cursor.onsuccess = () => {
          const c = cursor.result;
          if (!c) return;
          const versions = e.oldVersion < 2
            ? (c.value as readonly Wrapped[]).map((v) => ({ ...v.query, validUntil: v.validUntil, deletedAt: v.deletedAt }))
            : c.value as readonly Query[];
          c.update(JSON.stringify(versions));
          c.continue();
        };
      };
      // ANOTHER APP on this origin opening a newer layout: let it, rather than block it until
      // this tab closes; this tab's next call says the store must be reloaded
      open.onsuccess = () => {
        const db = open.result;
        if (this.#closed) { db.close(); return; }
        this.#cancelOpen = () => undefined;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      open.onblocked = () => refused(new Error('the saved queries are open in another tab of an older version: close it and reload'));
      open.onerror = () => refused(open.error ?? new Error('IndexedDB could not open'));
    });
    // Opening starts before the first operation; preserve a rejection for that caller without reporting it unhandled.
    void this.#db.catch(() => undefined);
  }

  /** Release this embedding's connection, including an open blocked by another tab; a late connection closes itself. */
  async close(): Promise<void> {
    this.#closed = true;
    this.#cancelOpen();
    await this.#db.then((db) => db.close(), () => undefined);
  }

  async call<T>(f: (held: ReadonlyMap<string, string>) => Called<T>): Promise<T> {
    const db = await this.#db;
    if (this.#closed) throw new Error('the saved-query store has been closed');
    return new Promise<T>((resolve, reject) => {
      let tx: IDBTransaction;
      try {
        tx = db.transaction(STORE, 'readwrite');
      } catch (e) {
        reject(upgraded(e));
        return;
      }
      let said: Called<T> | undefined;
      let failed: unknown;
      tx.oncomplete = () => {
        // every app's open tabs hear of a write
        if (said!.changed.size > 0 && typeof BroadcastChannel !== 'undefined') {
          const channel = new BroadcastChannel(this.#channel);
          channel.postMessage({ ids: [...said!.changed.keys()] });
          channel.close();
        }
        resolve(said!.answer);
      };
      tx.onabort = () => reject(failed ?? tx.error ?? new Error('IndexedDB transaction aborted'));
      const store = tx.objectStore(STORE);
      // the ids and the texts, in key order, of the one transaction: read, the rules run, their writes made
      const ids = store.getAllKeys();
      const texts = store.getAll();
      texts.onsuccess = () => {
        try {
          said = f(new Map(ids.result.map((id, i) => [String(id), texts.result[i] as string])));
          for (const [id, text] of said.changed) store.put(text, id);
        } catch (e) {
          failed = e;
          tx.abort();
        }
      };
    });
  }
}
