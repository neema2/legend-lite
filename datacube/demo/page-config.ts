// Where the pages' servers are: DEPLOYMENT data, never compiled in.
//
// `config.json`, served beside the page, names them; a deployment ships its
// own. A URL parameter overrides the file for one visit (`?legendLite=`,
// `?engine=`, `?warehouse=`, `?queryStore=`), so a page can be pointed at another server
// without rebuilding or editing anything. The file in the repository names the
// local development servers (`bazel run //core:server`, a legend-engine on its
// default port); the warehouse has none by default -- its URL is typed, or
// remembered by this browser from the last sign-in.

export interface PageConfig {
  /** legend-lite's server: the server-mode page plans there. Empty: not configured. */
  readonly legendLite: string;
  /** legend-engine: the engine-mode page runs there. Empty: not configured. */
  readonly legendEngine: string;
  /** The warehouse the Data window offers first. Empty: none. */
  readonly warehouse: string;
  /**
   * Where saved queries are kept: a server's API root (`http://host:port/api`) answering upstream's
   * `/api/pure/v1/query` -- legend-lite started with `--query-store`, or legend-engine. Empty: this
   * browser's own store, the one Legend Query keeps when it runs without a server.
   */
  readonly queryStore: string;
  /** Who this page's caller is to the browser's own store ("Mine only"): Legend Query's in-browser user. */
  readonly user: string;
  /** The projects a saved query can belong to, as the Query app's config.json names them. */
  readonly projects: readonly ProjectConfig[];
  /**
   * Where a saved query's project is opened by name when `projects[]` has none (design Phase 3): a Depot --
   * `"sdlc": "page"`, the one this origin's pages share (what Studio publishes), or a model home's server. Its rows
   * are its own: the model's Data elements, loaded into this tab's DuckDB (plan A2). Absent: none.
   */
  readonly depot?: DepotConfig;
}

export interface DepotConfig {
  readonly sdlc: string;
  /** Where the page's SDLC module is served (`<vendor>sdlc/`), for `"page"`. */
  readonly vendor: string;
}

/**
 * A project: what a saved query's `groupId:artifactId:versionId` compiles against (`models`, Pure
 * text joined in order), and -- the cube runs in this tab's DuckDB -- the SQL that puts its rows
 * there (`seed`, one statement per line). URLs relative to config.json.
 */
export interface ProjectConfig {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly title: string;
  readonly models: readonly string[];
  readonly seed: readonly string[];
}

const EMPTY: PageConfig = { legendLite: '', legendEngine: '', warehouse: '', queryStore: '', user: '', projects: [] };

const texts = (v: unknown): string[] => (Array.isArray(v) ? v.map(text).filter((t) => t !== '') : []);

/** The well-formed entries of `projects[]`: a malformed one is left out, not guessed at. */
function projects(v: unknown, base: string): ProjectConfig[] {
  if (!Array.isArray(v)) return [];
  return v.flatMap((p: unknown) => {
    if (typeof p !== 'object' || p === null) return [];
    const o = p as Record<string, unknown>;
    const [groupId, artifactId, versionId] = [text(o['groupId']), text(o['artifactId']), text(o['versionId'])];
    const models = texts(o['models']);
    if (!groupId || !artifactId || !versionId || models.length === 0) return [];
    const at = (u: string): string => new URL(u, base).href;
    return [{
      groupId, artifactId, versionId,
      title: text(o['title']) || `${groupId}:${artifactId}`,
      models: models.map(at),
      seed: texts(o['seed']).map(at),
    }];
  });
}


/** `depot`, when it names an SDLC (`"page"` or a URL). */
function depot(v: unknown): DepotConfig | undefined {
  if (typeof v !== 'object' || v === null) return undefined;
  const o = v as Record<string, unknown>;
  const sdlc = text(o['sdlc']);
  if (!sdlc) return undefined;
  return { sdlc, vendor: text(o['vendor']) || './vendor/' };
}

function text(v: unknown): string {
  return typeof v === 'string' ? v.trim() : '';
}

/**
 * The page's configuration: `config.json`, then the URL's parameters over it.
 * A missing or unreadable file is an EMPTY configuration, not an error: the
 * local page needs no server at all, and a page that does says which setting
 * is missing rather than guessing an address.
 */
export async function pageConfig(location: Location = window.location): Promise<PageConfig> {
  let file: PageConfig = EMPTY;
  try {
    const url = new URL('config.json', location.href);
    const r = await fetch(url, { cache: 'no-cache' });
    if (r.ok) {
      const raw = await r.json() as Record<string, unknown>;
      const byName = depot(raw['depot']);
      file = {
        legendLite: text(raw['legendLite']), legendEngine: text(raw['legendEngine']), warehouse: text(raw['warehouse']),
        queryStore: text(raw['queryStore']), user: text(raw['user']), projects: projects(raw['projects'], url.href),
        ...(byName ? { depot: byName } : {}),
      };
    }
  } catch {
    // no configuration file: every setting is empty
  }
  const q = new URLSearchParams(location.search);
  return {
    legendLite: text(q.get('legendLite')) || file.legendLite,
    legendEngine: text(q.get('engine')) || file.legendEngine,
    warehouse: text(q.get('warehouse')) || file.warehouse,
    queryStore: text(q.get('queryStore')) || file.queryStore,
    user: file.user,
    projects: file.projects,
    ...(file.depot ? { depot: file.depot } : {}),
  };
}
