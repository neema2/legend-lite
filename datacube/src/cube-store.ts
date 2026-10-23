// Where saved cubes live: upstream's query-store contract, exactly
// (legend-engine `ApplicationQuery` / `DataCubeQueryStoreManager`, read at 4.145.0;
// docs/DATACUBE_SAVE_SHARE_2026_09_28.md step 0), at its own routes (`/api/pure/v1/query/dataCube`).
//
// ONE IMPLEMENTATION (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6): the store's rules are legend-lite's Java
// (core/.../queries/SavedCubes.java) -- the server's, Python's native library's, and the page's, compiled to WebAssembly
// on the saved queries' module (//query-store:page) -- so this file holds only the client of those routes
// (`CubeStoreClient`), the page's records (IndexedDB, memory in a test) and the adapter that hands them to the module
// (`wasmCubeServer`). The rules, kept there: the client supplies the id (creating one that exists is refused); the owner
// is the current user, and only the owner updates or deletes; reading a cube stamps `lastOpenAt`; a search answers light
// records -- the current user's first, then the chosen sort (newest first), then the limit.
//
// A page is kept as a ROW: its record with its content as the protocol library's EXACT JSON text, so a calculated
// column's `12.30` or an integer past 2^53 comes back as written (JSON.stringify and the browser's structured clone
// would turn them into doubles); the client writes and reads the wire exactly too.

import { ExactNumber, fromJson, toJson } from '../../pure-protocol/src/index.ts';
import type { QueriesModule } from '../../query-store/src/wasm-server.ts';

/** One saved cube, as upstream's store keeps it. Times are epoch milliseconds. */
export interface DataCubeQuery {
  readonly id: string;
  readonly name: string;
  readonly description?: string;
  readonly content: Readonly<Record<string, unknown>>;
  readonly owner?: string;
  readonly createdAt?: number;
  readonly lastUpdatedAt?: number;
  readonly lastOpenAt?: number;
}

/** What search answers: the record without its content. */
export type LightDataCubeQuery = Omit<DataCubeQuery, 'content'>;

export type QuerySearchSortBy = 'SORT_BY_CREATE' | 'SORT_BY_VIEW' | 'SORT_BY_UPDATE';

/** Upstream's `QuerySearchSpecification`, the parts a cube search uses. */
export interface QuerySearchSpecification {
  readonly searchTermSpecification?: {
    readonly searchTerm: string;
    readonly exactMatchName?: boolean;
    readonly includeOwner?: boolean;
  };
  readonly limit?: number;
  readonly showCurrentUserQueriesOnly?: boolean;
  readonly sortByOption?: QuerySearchSortBy;
}

/** The orders Open… lists saved pages in, as its menu names them -- its first, the one it opens with (the tab's library;
 *  Python's ll.Page.saved(), headless pageSearch: K15). */
export const SAVED_SORTS: readonly (readonly [QuerySearchSortBy, string])[] = [
  ['SORT_BY_VIEW', 'Last opened'],
  ['SORT_BY_UPDATE', 'Last saved'],
  ['SORT_BY_CREATE', 'Created'],
];

/** A search of the saved pages as Open… asks the store it (the tab's library; Python's ll.Page.saved(), headless
 *  pageSearch: K15): the pages `term` matches (trimmed; blank, every one), in `sort`'s order (null, Open…'s first), the
 *  first `limit` (null, as many as the store answers) -- which and in what order, the store's rules (SavedCubes). */
export function savedSearch(term: string, sort: QuerySearchSortBy | null, limit: number | null): QuerySearchSpecification {
  const searchTerm = term.trim();
  return {
    ...(searchTerm ? { searchTermSpecification: { searchTerm } } : {}),
    sortByOption: sort ?? SAVED_SORTS[0]![0],
    ...(limit !== null ? { limit } : {}),
  };
}

export class CubeStoreError extends Error {
  /** The HTTP status upstream's store answers the same refusal with. */
  readonly status: 400 | 403 | 404;
  constructor(message: string, status: 400 | 403 | 404) {
    super(message);
    this.name = 'CubeStoreError';
    this.status = status;
  }
}

export interface CubeStore {
  currentUser(): Promise<string>;
  search(spec: QuerySearchSpecification): Promise<LightDataCubeQuery[]>;
  get(id: string): Promise<DataCubeQuery>;
  create(query: DataCubeQuery): Promise<DataCubeQuery>;
  update(id: string, query: DataCubeQuery): Promise<DataCubeQuery>;
  delete(id: string): Promise<void>;
}

/** A saved page as a host keeps it: its record, its content the protocol's exact JSON text. */
export type CubeRow = Readonly<Record<string, unknown>> & { readonly id: string; readonly content: string };

/** What one call of the rules answers: the pages it wrote (their rows) or removed (null), by id, and its own answer. */
export interface CubeCalled<T> {
  readonly changed: ReadonlyMap<string, CubeRow | null>;
  readonly answer: T;
}

/** Where the page keeps its pages' rows: the part a backend supplies. */
export interface CubeRecords {
  /**
   * One call of the rules: `f` is handed every row by id (in id order) and answers what it wrote or removed, with its
   * answer -- all in one transaction, so no other tab writes between the read and the writes (`f` runs synchronously
   * inside it).
   */
  call<T>(f: (rows: ReadonlyMap<string, CubeRow>) => CubeCalled<T>): Promise<T>;
}

/**
 * THE ONE CLIENT of a saved-cube store: upstream's routes under `api` (`.../api`), wherever they are answered -- the
 * page's module (`wasmCubeServer`), Python's engine, legend-lite's server -- the request and its answer written and
 * read as the protocol's exact JSON. A refusal is a `CubeStoreError` with the store's status and words; `user` is who
 * the host says the caller is (there is no sign-in yet: G10).
 */
export class CubeStoreClient implements CubeStore {
  readonly #root: string;
  readonly #fetch: typeof fetch;
  readonly #user: string;

  constructor(api: string, fetcher: typeof fetch, user: string) {
    this.#root = `${api.replace(/\/+$/, '')}/pure/v1/query/dataCube`;
    this.#fetch = fetcher;
    this.#user = user;
  }

  async currentUser(): Promise<string> {
    return this.#user;
  }

  async search(spec: QuerySearchSpecification): Promise<LightDataCubeQuery[]> {
    return await this.#call('POST', '/search', spec) as LightDataCubeQuery[];
  }

  async get(id: string): Promise<DataCubeQuery> {
    return await this.#call('GET', `/${encodeURIComponent(id)}`) as DataCubeQuery;
  }

  async create(query: DataCubeQuery): Promise<DataCubeQuery> {
    return await this.#call('POST', '', query) as DataCubeQuery;
  }

  async update(id: string, query: DataCubeQuery): Promise<DataCubeQuery> {
    return await this.#call('PUT', `/${encodeURIComponent(id)}`, query) as DataCubeQuery;
  }

  async delete(id: string): Promise<void> {
    await this.#call('DELETE', `/${encodeURIComponent(id)}`);
  }

  async #call(method: string, path: string, body?: unknown): Promise<unknown> {
    const answer = await this.#fetch(`${this.#root}${path}`, {
      method,
      ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: toJson(body) }),
    });
    // every answer read whole, an empty one too: a response left unread is cancelled by the browser
    const text = await answer.text();
    if (!answer.ok) {
      let message = text || `${answer.status}`;
      try {
        const said = JSON.parse(text) as { message?: unknown };
        if (typeof said.message === 'string') message = said.message;
      } catch {
        // not JSON: the text is the message
      }
      if (answer.status === 400 || answer.status === 403 || answer.status === 404) {
        throw new CubeStoreError(message, answer.status);
      }
      throw new Error(`the saved pages' store answered ${answer.status}: ${message}`);
    }
    return text === '' ? undefined : wire(fromJson(text));
  }
}

/** The record's times, which are numbers (epoch milliseconds); the rest of it read exactly. */
const TIMES = ['createdAt', 'lastUpdatedAt', 'lastOpenAt'] as const;

/**
 * A record (or a search's records) as the store answered it, read as the protocol's exact JSON -- the content's
 * numbers exact (a calculated column's `12.30` kept) -- with its times plain numbers, as a record's are.
 */
function wire(read: unknown): unknown {
  if (Array.isArray(read)) return read.map(wire);
  if (read === null || typeof read !== 'object') return read;
  const out: Record<string, unknown> = { ...(read as Record<string, unknown>) };
  for (const t of TIMES) {
    const v = out[t];
    if (v instanceof ExactNumber) out[t] = Number(v.text);
  }
  return out;
}

/** The API root the page's own store answers at: never on the network. */
export const LOCAL_CUBES_API = 'http://this-browser.invalid/api';

/** The rows as the module reads them, `{"id": row}`. */
const rowsJson = (rows: ReadonlyMap<string, CubeRow>): string =>
  `{${[...rows].map(([id, row]) => `${JSON.stringify(id)}:${JSON.stringify(row)}`).join(',')}}`;

const answered = (status: number, body: string): Response =>
  new Response(status === 204 ? null : body, { status, headers: { 'Content-Type': 'application/json' } });

/**
 * THE STORE IN THIS PAGE: the routes answered by legend-lite's own rules compiled to WebAssembly (the saved queries'
 * module, `QueriesPage.cubes`) over the page's records. Each call hands the module every row and writes back what it
 * wrote or removed, in one transaction of the records, so the module keeps nothing and two tabs never interleave a
 * call (the later of two saves of one page is the page, as on a server). `load` fetches the module at the store's
 * first call; a load that failed is tried again at the next.
 */
export function wasmCubeServer(o: { readonly load: () => Promise<QueriesModule>; readonly records: CubeRecords; readonly user: string }):
  { readonly fetch: typeof fetch } {
  let loaded: Promise<QueriesModule> | undefined;
  const module = (): Promise<QueriesModule> => (loaded ??= o.load().catch((e: unknown) => {
    loaded = undefined;
    throw e;
  }));
  const handle = async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    const at = url.pathname.indexOf('/pure/v1/query/dataCube');
    if (at < 0) {
      return answered(404, JSON.stringify({ code: -1, message: `no such API in this page's store: ${url.pathname}`, status: 'error' }));
    }
    const rest = url.pathname.slice(at + '/pure/v1/query/dataCube'.length);
    const body = request.method === 'GET' || request.method === 'DELETE' ? '' : await request.text();
    const m = await module();
    return o.records.call((rows) => {
      let said: { status: number; body: string; changed: Record<string, CubeRow | null> };
      try {
        said = JSON.parse(m.exports.cubes(request.method, rest, url.search.replace(/^\?/, ''), body, o.user, rowsJson(rows))) as typeof said;
      } catch (e) {
        // rows the rules cannot read are an exception out of the module (and a bug in them a trap): the engine's
        // 500, naming it, as the server answers its own -- and nothing written
        const error = e instanceof Error ? e : new Error(String(e));
        return { changed: new Map(), answer: answered(500, JSON.stringify({ code: -1, message: `${error.name}: ${error.message}`, status: 'error' })) };
      }
      return { changed: new Map(Object.entries(said.changed)), answer: answered(said.status, said.body) };
    });
  };
  return { fetch: ((input: RequestInfo | URL, init?: RequestInit) => handle(new Request(input, init))) as typeof fetch };
}

// ---------------------------------------------------------------- backends

/** Rows in memory, for tests and for a page that cannot store anything. */
export class MemoryRecords implements CubeRecords {
  readonly #rows = new Map<string, CubeRow>();

  async call<T>(f: (rows: ReadonlyMap<string, CubeRow>) => CubeCalled<T>): Promise<T> {
    const said = f(new Map([...this.#rows].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))));
    for (const [id, row] of said.changed) {
      if (row === null) this.#rows.delete(id);
      else this.#rows.set(id, row);
    }
    return said.answer;
  }
}

/**
 * The browser database saved cubes live in: `cubes` (rows, keyed by id) and `handles`
 * (file handles by cube id: `file-handles.ts`).
 */
export function openCubeDatabase(factory: IDBFactory = globalThis.indexedDB): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const open = factory.open('datacube', 1);
    open.onupgradeneeded = () => {
      open.result.createObjectStore(BrowserRecords.STORE, { keyPath: 'id' });
      open.result.createObjectStore('handles');
    };
    open.onsuccess = () => resolve(open.result);
    open.onerror = () => reject(open.error ?? new Error('this browser refused its database'));
  });
}

/** Rows in the browser's IndexedDB: one object store keyed by id, each call of the rules one transaction. */
export class BrowserRecords implements CubeRecords {
  static readonly STORE = 'cubes';
  readonly #db: Promise<IDBDatabase>;

  constructor(db: Promise<IDBDatabase> = openCubeDatabase()) {
    this.#db = db;
  }

  async call<T>(f: (rows: ReadonlyMap<string, CubeRow>) => CubeCalled<T>): Promise<T> {
    const db = await this.#db;
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(BrowserRecords.STORE, 'readwrite');
      const store = tx.objectStore(BrowserRecords.STORE);
      let said: CubeCalled<T> | undefined;
      let failed: unknown;
      // resolved when the TRANSACTION commits, not when a request succeeds: a write is only saved once its transaction is
      tx.oncomplete = () => resolve(said!.answer);
      tx.onabort = () => reject(failed ?? tx.error ?? new Error('the browser aborted the write (storage full?)'));
      // every row, in key order, read in the one transaction: the rules run, their writes made in it
      const all = store.getAll();
      all.onsuccess = () => {
        try {
          said = f(new Map((all.result as CubeRow[]).map((row) => [row.id, row])));
          for (const [id, row] of said.changed) {
            if (row === null) store.delete(id);
            else store.put(row);
          }
        } catch (e) {
          failed = e;
          tx.abort();
        }
      };
    });
  }
}

/** A record as the row kept: its content the exact JSON text. */
export function toRow(q: DataCubeQuery): CubeRow {
  return { ...q, content: toJson(q.content) };
}

/**
 * Ask the browser to keep this site's data under storage pressure. Says whether it will;
 * a browser that cannot answer is "best effort".
 */
export async function persistStorage(): Promise<boolean> {
  try {
    const storage = globalThis.navigator?.storage;
    if (!storage?.persist) return false;
    return (await storage.persisted?.()) || (await storage.persist());
  } catch {
    return false;
  }
}
