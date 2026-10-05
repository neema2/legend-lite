// THE ONE CLIENT of a Depot: upstream legend-depot's read API and lite's dependency-files route, typed.
// Studio (dependencies), Query and DataCube (models by coordinates) read published versions through
// this; where the Depot is -- depot-server in the model home, a real legend-depot, or depot-server's
// rules compiled into the page (sdlc-client/src/wasm-server.ts) -- is only the `fetch` it is handed.

import type { Entity } from '../../sdlc-client/src/wire.ts';
import type {
  ArtifactDependency, ProjectVersion, ProjectVersionEntities, ProjectVersionFiles, StoreProjectData,
} from './wire.ts';

/** A Depot's refusal: its status and its words (empty for upstream's bodiless 404). */
export class DepotError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'DepotError';
    this.status = status;
  }
}

const enc = encodeURIComponent;

export class DepotClient {
  readonly #api: string;
  readonly #fetch: typeof fetch;

  constructor(api: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)) {
    this.#api = api.replace(/\/+$/, '');
    this.#fetch = fetcher;
  }

  /** Every project Depot knows. */
  projects(): Promise<StoreProjectData[]> {
    return this.#json('GET', '/project-configurations');
  }

  /** One project, or undefined (upstream's empty 404). */
  async project(groupId: string, artifactId: string): Promise<StoreProjectData | undefined> {
    return this.#maybe('GET', `/project-configurations/${enc(groupId)}/${enc(artifactId)}`);
  }

  /** A project's versions, oldest first; with `snapshots`, the line's `master-SNAPSHOT` last. */
  versions(groupId: string, artifactId: string, snapshots = true): Promise<string[]> {
    return this.#json('GET', `/projects/${enc(groupId)}/${enc(artifactId)}/versions?snapshots=${snapshots}`);
  }

  /** A version's record (aliases `latest`, `head`), or undefined. */
  async version(groupId: string, artifactId: string, versionId: string): Promise<ProjectVersion | undefined> {
    return this.#maybe('GET', `/versions/${enc(groupId)}/${enc(artifactId)}/${enc(versionId)}`);
  }

  /** A version's entities. */
  entities(groupId: string, artifactId: string, versionId: string): Promise<Entity[]> {
    return this.#json('GET', `/projects/${enc(groupId)}/${enc(artifactId)}/versions/${enc(versionId)}`);
  }

  /** Upstream's dependency answer: the nearest-wins closure of `dependencies`, each version's entities. */
  dependencyEntities(dependencies: readonly ArtifactDependency[], transitive = true, includeOrigin = true): Promise<ProjectVersionEntities[]> {
    return this.#json('POST', `/projects/dependenciesFromArtifactDependencies?transitive=${transitive}&includeOrigin=${includeOrigin}`, dependencies);
  }

  /** lite's twin: the same closure, each version's files (what the in-tab compiler reads). */
  dependencyFiles(dependencies: readonly ArtifactDependency[], transitive = true): Promise<ProjectVersionFiles[]> {
    return this.#json('POST', `/projects/dependencies/pure?transitive=${transitive}`, dependencies);
  }

  async #call(method: string, path: string, body?: unknown): Promise<Response> {
    const res = await this.#fetch(`${this.#api}${path}`, {
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
      throw new DepotError(message, res.status);
    }
    return res;
  }

  async #json<T>(method: string, path: string, body?: unknown): Promise<T> {
    return (await this.#call(method, path, body)).json() as Promise<T>;
  }

  async #maybe<T>(method: string, path: string): Promise<T | undefined> {
    try {
      return await this.#json<T>(method, path);
    } catch (e) {
      if (e instanceof DepotError && e.status === 404) return undefined;
      throw e;
    }
  }
}
