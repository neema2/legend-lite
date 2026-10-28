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
  /**
   * Where the page's WebAssembly is served -- DuckDB and its extensions, the compiler, the page's SDLC, the saved
   * stores' rules, the warehouse's binding: `./vendor/` beside the page, unless the site serves one copy for every app
   * (//site:dist). The one key: nothing else in the config names a vendor folder.
   */
  readonly vendor: string;
  /**
   * THE PAGE'S VAULT: a server's key by the vault reference its connection names (a Deephaven connection's
   * `PreSharedKey { keyVaultReference: ... }`, docs/ONE_CONNECTION_DESK_2026_10_28.md step 5) -- a test server's key,
   * never a production one (a sign-in prompt: later). Empty: none, and a grid on such a server says so.
   */
  readonly vault: Readonly<Record<string, string>>;
  /**
   * The execution context each data space runs on here, by the data space's path, where the deployment's is not the data
   * space's default (D2): what a page written here (an example: the trading desk) runs on. Empty: every default.
   */
  readonly contexts: Readonly<Record<string, string>>;
}

export interface DepotConfig {
  /** `"page"` (the page's own SDLC, its module from the page's `vendor`), or a model home's server. */
  readonly sdlc: string;
}

/**
 * A project: what a saved query's `groupId:artifactId:versionId` compiles against (`models`, Pure
 * text joined in order; its rows its Data elements', C5). URLs relative to config.json.
 */
export interface ProjectConfig {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly title: string;
  readonly models: readonly string[];
}

const EMPTY: PageConfig = {
  legendLite: '', legendEngine: '', warehouse: '', queryStore: '', user: '', projects: [], vendor: './vendor/', vault: {}, contexts: {},
};

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
    }];
  });
}


/** A JSON object of texts (`vault`, `contexts`): an entry whose value is not a text is left out, not guessed at. */
function textsByName(v: unknown): Record<string, string> {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return {};
  return Object.fromEntries(Object.entries(v).map(([k, t]) => [k, text(t)] as const).filter(([, t]) => t !== ''));
}

/** `depot`, when it names an SDLC (`"page"` or a URL). */
function depot(v: unknown): DepotConfig | undefined {
  if (typeof v !== 'object' || v === null) return undefined;
  const o = v as Record<string, unknown>;
  const sdlc = text(o['sdlc']);
  if (!sdlc) return undefined;
  return { sdlc };
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
        vendor: text(raw['vendor']) || EMPTY.vendor,
        vault: textsByName(raw['vault']), contexts: textsByName(raw['contexts']),
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
    vendor: file.vendor,
    vault: file.vault,
    contexts: file.contexts,
    ...(file.depot ? { depot: file.depot } : {}),
  };
}
