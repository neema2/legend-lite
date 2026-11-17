// THE ONE CLIENT of a query store: upstream's `/api/pure/v1/query`, typed. Every app here talks to
// saved queries through this and nothing else; WHERE the store is -- legend-engine, legend-lite
// (`--query-store`), or this page (local-server.ts) -- is only the `fetch` it is handed, never a
// branch in an app.

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
   * Who the store keeps saved queries for: its user, the owner of what is saved -- asked of legend-lite's own store
   * mount, `GET /api/lite/v1/query/currentUser` (the edge program's H7, the orchestrator's Q-b ruling of 2026-11-17;
   * legend-engine's `/api/server/v1/currentUser` is the edge's, never asked here).
   */
  async currentUser(): Promise<string> {
    const text = await (await this.#send('GET', `${this.#api}/lite/v1/query/currentUser`)).text();
    const user: unknown = JSON.parse(text);
    if (typeof user !== 'string') throw new QueryStoreError(`the store named no user: ${text.slice(0, 120)}`, 500);
    return user;
  }

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
