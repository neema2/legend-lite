// Where the page's own SDLC keeps its projects: plain keyed records -- git objects, refs and project
// records, as sdlc-server's rules write them (`com.legend.sdlc.Storage`). IndexedDB in a browser, a
// Map in a test. The layout is the rules'; this file only stores and reads it (wasm-server.ts).

/** Keyed JSON records. `list(prefix)` answers every record whose key starts with it, in key order. */
export interface Records {
  get<T>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  list<T>(prefix: string): Promise<T[]>;
  /** Writes (a value) and deletes (null) together: all of them or none (one IndexedDB transaction). */
  apply(changes: readonly (readonly [string, unknown])[]): Promise<void>;
}

export class MemoryRecords implements Records {
  readonly #data = new Map<string, string>();

  async get<T>(key: string): Promise<T | undefined> {
    const v = this.#data.get(key);
    return v === undefined ? undefined : JSON.parse(v) as T;
  }

  async put(key: string, value: unknown): Promise<void> {
    this.#data.set(key, JSON.stringify(value));
  }

  async delete(key: string): Promise<void> {
    this.#data.delete(key);
  }

  async list<T>(prefix: string): Promise<T[]> {
    return [...this.#data.keys()].filter((k) => k.startsWith(prefix)).sort().map((k) => JSON.parse(this.#data.get(k)!) as T);
  }

  async apply(changes: readonly (readonly [string, unknown])[]): Promise<void> {
    for (const [key, value] of changes) {
      if (value === null) this.#data.delete(key);
      else this.#data.set(key, JSON.stringify(value));
    }
  }
}

/** The database every app on this origin shares for its SDLC: one name, one layout, defined here only. */
export const DATABASE = 'legend-projects';
const STORE = 'records';
const LAYOUT = 1;

export class BrowserRecords implements Records {
  readonly #db: Promise<IDBDatabase>;

  constructor(factory: IDBFactory = globalThis.indexedDB) {
    this.#db = new Promise((resolve, reject) => {
      const open = factory.open(DATABASE, LAYOUT);
      open.onupgradeneeded = () => {
        open.result.createObjectStore(STORE);
      };
      // another tab opening a newer layout: let it, rather than block it until this tab closes
      open.onsuccess = () => {
        const db = open.result;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      open.onblocked = () => reject(new Error('the projects are open in another tab of an older version: close it and reload'));
      open.onerror = () => reject(open.error ?? new Error('IndexedDB could not open'));
    });
  }

  async #request<T>(mode: IDBTransactionMode, f: (s: IDBObjectStore) => IDBRequest<T>): Promise<T> {
    const db = await this.#db;
    return new Promise((resolve, reject) => {
      let r: IDBRequest<T>;
      try {
        r = f(db.transaction(STORE, mode).objectStore(STORE));
      } catch (e) {
        reject(new Error(`the projects were upgraded by another tab: reload this page (${e instanceof Error ? e.message : String(e)})`));
        return;
      }
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => reject(r.error ?? new Error('IndexedDB request failed'));
    });
  }

  async get<T>(key: string): Promise<T | undefined> {
    return (await this.#request('readonly', (s) => s.get(key))) as T | undefined;
  }

  async put(key: string, value: unknown): Promise<void> {
    await this.#request('readwrite', (s) => s.put(value, key));
  }

  async delete(key: string): Promise<void> {
    await this.#request('readwrite', (s) => s.delete(key));
  }

  async apply(changes: readonly (readonly [string, unknown])[]): Promise<void> {
    if (changes.length === 0) return;
    const db = await this.#db;
    await new Promise<void>((resolve, reject) => {
      let tx: IDBTransaction;
      try {
        tx = db.transaction(STORE, 'readwrite');
      } catch (e) {
        reject(new Error(`the projects were upgraded by another tab: reload this page (${e instanceof Error ? e.message : String(e)})`));
        return;
      }
      const store = tx.objectStore(STORE);
      for (const [key, value] of changes) {
        if (value === null) store.delete(key);
        else store.put(value, key);
      }
      tx.oncomplete = () => resolve();
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'));
      tx.onerror = () => reject(tx.error ?? new Error('IndexedDB transaction failed'));
    });
  }

  async list<T>(prefix: string): Promise<T[]> {
    // every key from `prefix` up to the last string starting with it
    return (await this.#request('readonly', (s) => s.getAll(IDBKeyRange.bound(prefix, `${prefix}￿`)))) as T[];
  }
}
