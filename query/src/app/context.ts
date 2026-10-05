// What every screen of the app shares: the engine and query store, the loaded project and its
// model graph, the current user, recently viewed things.

import type { ModelHomeConfig } from '../../../depot-client/src/model-home.ts';
import type { QueryEngine } from '../../../engine-client/src/engine.ts';
import type { Engine, QueryStore } from '../backend/engine.ts';
import type { WasmGrammar } from '../backend/wasm-grammar.ts';
import type { PureModelContextText } from '../backend/wire.ts';
import type { ModelGraph } from '../model/graph.ts';

export interface ProjectConfig {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly title?: string;
  /** Model files (Pure grammar), relative to the page. */
  readonly models: readonly string[];
}

/**
 * Where queries run. In the browser (`duckdb-wasm`: DuckDB in this tab, seeded from SQL files;
 * `warehouse`: the warehouse's DuckDB, as the signed-in user) the tab's planner writes the SQL
 * and saved queries stay in this browser. On a `server` (legend-lite's, or legend-engine) the
 * server executes and keeps saved queries.
 */
export type ExecutionConfig =
  | { readonly kind: 'duckdb-wasm'; readonly seed?: readonly string[]; readonly user: string }
  | { readonly kind: 'warehouse'; readonly url: string; readonly catalog?: string; readonly seed?: readonly string[] }
  | { readonly kind: 'server'; readonly engine: string };

/**
 * Where the results grid -- a DataCube over the query -- reads its rows: in a browser plane, the
 * SQL engine there (DuckDB-WASM, the warehouse), planned by the tab's planner; on a server, its
 * `pure/v1` execute (the server plans and runs).
 */
export type CubeRows =
  | { readonly kind: 'sql'; readonly engine: QueryEngine }
  | { readonly kind: 'server'; readonly baseUrl: string };

export interface AppConfig {
  readonly execution: ExecutionConfig;
  /** legend-lite's planner in the tab (grammar, typing, SQL); required unless a server answers them. */
  readonly planner?: { readonly worker: string; readonly vendor: string };
  /** Projects whose model is files served with the page (the demo's own trading model). */
  readonly projects: readonly ProjectConfig[];
  /**
   * Where projects are opened by name (design Phase 3): a Depot -- `{ "sdlc": "page" }` is the one this origin's
   * pages share (Studio publishes into it), or a model home's server. Each project there opens at its project line's
   * snapshot (Depot's `master-SNAPSHOT`, upstream Query's HEAD), with its dependencies.
   */
  readonly depot?: ModelHomeConfig;
}

/** A project's model, loaded: its text (what execution sends), and its graph (what the screens read). */
export interface LoadedProject {
  readonly config: ProjectConfig;
  readonly gav: string;
  readonly context: PureModelContextText;
  readonly graph: ModelGraph;
}

export function gavOf(p: ProjectConfig): string {
  return `${p.groupId}:${p.artifactId}:${p.versionId}`;
}

export interface Recent {
  readonly dataSpaces: readonly { readonly gav: string; readonly path: string; readonly context?: string }[];
  readonly queries: readonly string[];
}

const RECENT_DATA_SPACES = 'query-editor.recent-dataSpaces';
const RECENT_QUERIES = 'query-editor.recent-queries';

function readList<T>(key: string): T[] {
  try {
    const v = JSON.parse(globalThis.localStorage?.getItem(key) ?? '[]') as unknown;
    return Array.isArray(v) ? v as T[] : [];
  } catch {
    return [];
  }
}

function writeList(key: string, list: readonly unknown[]): void {
  try {
    globalThis.localStorage?.setItem(key, JSON.stringify(list));
  } catch { /* storage unavailable: recents are a convenience */ }
}

/** Recently viewed data spaces and queries (upstream's user-data keys, 10 each). */
export const recent = {
  get(): Recent {
    return { dataSpaces: readList(RECENT_DATA_SPACES), queries: readList(RECENT_QUERIES) };
  },
  dataSpace(gav: string, path: string, context?: string): void {
    const list = readList<{ gav: string; path: string }>(RECENT_DATA_SPACES).filter((d) => !(d.gav === gav && d.path === path));
    writeList(RECENT_DATA_SPACES, [{ gav, path, context }, ...list].slice(0, 10));
  },
  query(id: string): void {
    writeList(RECENT_QUERIES, [id, ...readList<string>(RECENT_QUERIES).filter((q) => q !== id)].slice(0, 10));
  },
  forgetQuery(id: string): void {
    writeList(RECENT_QUERIES, readList<string>(RECENT_QUERIES).filter((q) => q !== id));
  },
};

export class AppContext {
  readonly config: AppConfig;
  readonly engine: Engine;
  readonly store: QueryStore;
  readonly planner: WasmGrammar | undefined;
  /** The projects loaded so far: the configured ones, and each version opened by name since (ensure). */
  readonly projects: LoadedProject[];
  readonly user: string;
  /** Where the results grid (a DataCube) reads rows. */
  readonly cubeRows: CubeRows;

  constructor(config: AppConfig, engine: Engine, store: QueryStore, planner: WasmGrammar | undefined,
    projects: LoadedProject[], user: string, cubeRows: CubeRows) {
    this.config = config;
    this.engine = engine;
    this.store = store;
    this.planner = planner;
    this.projects = projects;     // the caller's own list: what loads later (ensure) is seen by whoever holds it
    this.user = user;
    this.cubeRows = cubeRows;
  }

  project(gav: string): LoadedProject {
    const p = this.projects.find((x) => x.gav === gav);
    if (!p) throw new Error(`no project ${gav} is configured`);
    return p;
  }

  /** Where a version not yet loaded is opened by name (by-name.ts), when the app has a Depot. */
  byName: ((groupId: string, artifactId: string, versionId: string) => Promise<LoadedProject>) | undefined;
  /** A project's versions in Depot, HEAD first (by-name.ts versionsOf); undefined for the demo's own projects. */
  versions: ((groupId: string, artifactId: string) => Promise<string[]>) | undefined;
  /**
   * The projects in Depot, by name only: none is loaded at start (a Depot may hold many); the start page lists them,
   * and opening one loads its HEAD (ensure).
   */
  depotProjects: readonly { readonly groupId: string; readonly artifactId: string }[] = [];
  readonly #loading = new Map<string, Promise<LoadedProject>>();

  /**
   * The project at `gav`, loading it from Depot the first time it is asked for (a release, or a snapshot not loaded
   * at start): a route, a saved query or the version picker names a version, and this makes it there.
   */
  async ensure(gav: string): Promise<LoadedProject> {
    const have = this.projects.find((x) => x.gav === gav);
    if (have) return have;
    const parts = gav.split(':');
    if (!this.byName || parts.length !== 3) return this.project(gav);
    let loading = this.#loading.get(gav);
    if (!loading) {
      loading = this.byName(parts[0]!, parts[1]!, parts[2]!).then((p) => {
        this.projects.push(p);
        return p;
      });
      this.#loading.set(gav, loading);
      loading.catch(() => this.#loading.delete(gav));
    }
    return loading;
  }

  /** The project that holds an element, by path. */
  projectOf(path: string): LoadedProject | undefined {
    return this.projects.find((p) => p.graph.elements.has(path));
  }
}
