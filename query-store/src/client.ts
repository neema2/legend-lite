// THE ONE CLIENT of a query store: upstream's `/api/pure/v1/query`, typed (its user lite's own `user` operation; the
// records move to lite's `/api/lite/v1/queries` with the page's store, H6). Every app here talks to saved queries
// through this and nothing else; WHERE the store is -- legend-engine, legend-lite (`--query-store`), or this page
// (wasm-server.ts) -- is only the `fetch` it is handed, never a branch in an app.

import type { Query, QuerySearchSpecification } from './wire.ts';

/** A store's refusal, as it said it: its status and its own words. */
export class QueryStoreError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'QueryStoreError';
    this.status = status;
  }
}

/** What a reader of saved queries needs (DataCube opens them; it never writes). */
export interface QueryReader {
  /** `POST query/search`: by name (or id, or owner), the store's own order and limits. */
  search(spec: QuerySearchSpecification): Promise<Query[]>;
  /** `GET query/{id}`: the current version (the store marks it opened). */
  get(id: string): Promise<Query>;
}

/** The whole store, as Query uses it. */
export interface QueryStore extends QueryReader {
  /** `GET query/batch?queryIds=...`. */
  batch(ids: readonly string[]): Promise<Query[]>;
  /** `GET query/{id}/history[?version=n]`: earlier and deleted versions, or one version. */
  history(id: string, version?: number): Promise<Query[]>;
  /** `POST query`: a new query, owned by the caller. */
  create(query: Query): Promise<Query>;
  /** `PUT query/{id}`: the owner's new version. */
  update(query: Query): Promise<Query>;
  /** `PUT query/{id}/patchQuery`: the fields set, over the current version, as a new version. */
  patch(id: string, fields: Partial<Query>): Promise<Query>;
  /** `DELETE query/{id}`: the owner's; its versions stay history. */
  delete(id: string): Promise<void>;
}

/**
 * The store at `api` -- a server's API root, `http://host:port/api` -- through `fetcher`: the
 * network, or the page's own store (`localQueryServer(...).fetch`).
 */
export class QueryStoreClient implements QueryStore {
  readonly #api: string;
  readonly #fetch: typeof fetch;

  constructor(api: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)) {
    this.#api = api.replace(/\/+$/, '');
    this.#fetch = fetcher;
  }

  search(spec: QuerySearchSpecification): Promise<Query[]> {
    return this.#json('POST', '/search', spec);
  }

  get(id: string): Promise<Query> {
    return this.#json('GET', `/${encodeURIComponent(id)}`);
  }

  batch(ids: readonly string[]): Promise<Query[]> {
    if (ids.length === 0) return Promise.resolve([]);
    return this.#json('GET', `/batch?${ids.map((id) => `queryIds=${encodeURIComponent(id)}`).join('&')}`);
  }

  history(id: string, version?: number): Promise<Query[]> {
    return this.#json('GET', `/${encodeURIComponent(id)}/history${version === undefined ? '' : `?version=${version}`}`);
  }

  create(query: Query): Promise<Query> {
    return this.#json('POST', '', query);
  }

  update(query: Query): Promise<Query> {
    return this.#json('PUT', `/${encodeURIComponent(query.id)}`, query);
  }

  patch(id: string, fields: Partial<Query>): Promise<Query> {
    return this.#json('PUT', `/${encodeURIComponent(id)}/patchQuery`, fields);
  }

  async delete(id: string): Promise<void> {
    await this.#call('DELETE', `/${encodeURIComponent(id)}`);
  }

  /**
   * Who the store keeps saved queries for: its user, the owner of what is saved -- lite's `user` operation,
   * `POST /api/lite/v1/user` (lite's API v1 spec §2, 2026-11-17; the orchestrator's Q-b ruling: the store's own user;
   * legend-engine's `/api/server/v1/currentUser` is the edge's, never asked here).
   */
  async currentUser(): Promise<string> {
    const text = await (await this.#send('POST', `${this.#api}/lite/v1/user`, {})).text();
    const { user } = JSON.parse(text) as { readonly user?: unknown };
    if (typeof user !== 'string') throw new QueryStoreError(`the store named no user: ${text.slice(0, 120)}`, 500);
    return user;
  }

  // the store's calls stay at `/api/pure/v1/query` until H6 moves them, with the page's store, to lite's own
  // `/api/lite/v1/queries` (the page's module answers legend-engine's words until its product entry exists)
  #call(method: string, path: string, body?: unknown): Promise<Response> {
    return this.#send(method, `${this.#api}/pure/v1/query${path}`, body);
  }

  /** The store's one send: `method` to `url`, a refusal its message as a QueryStoreError. */
  async #send(method: string, url: string, body?: unknown): Promise<Response> {
    const res = await this.#fetch(url, {
      method,
      ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
    });
    if (!res.ok) {
      const text = await res.text();
      let message = text || `${res.status} ${res.statusText}`;
      try {
        const e = JSON.parse(text) as { message?: unknown };
        if (typeof e.message === 'string') message = e.message;
      } catch {
        // not JSON: the text is the message
      }
      throw new QueryStoreError(message, res.status);
    }
    return res;
  }

  async #json<T>(method: string, path: string, body?: unknown): Promise<T> {
    return (await this.#call(method, path, body)).json() as Promise<T>;
  }
}
