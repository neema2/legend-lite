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
  /**
   * THE MODEL HOME, where every project is found (docs/ONE_CONNECTION_DESK_2026_10_28.md D5): `"sdlc": "page"`, the one
   * this origin's pages share (what Studio publishes), or a model home's server; and `"releases"`, the folder of the
   * versions the site releases (its `index.json` and their files, relative to config.json), loaded at every open and
   * never kept in the browser's storage. A saved query's project opens by name from it; its rows are its own: the
   * model's Data elements, loaded into this tab's DuckDB (plan A2). Absent: none, and no saved query opens.
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
  /** The site's releases folder (D5), as a URL resolved against config.json. */
  readonly releases?: string;
}

const EMPTY: PageConfig = {
  legendLite: '', legendEngine: '', warehouse: '', queryStore: '', user: '', vendor: './vendor/', vault: {}, contexts: {},
};


/** A JSON object of texts (`vault`, `contexts`): an entry whose value is not a text is left out, not guessed at. */
function textsByName(v: unknown): Record<string, string> {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return {};
  return Object.fromEntries(Object.entries(v).map(([k, t]) => [k, text(t)] as const).filter(([, t]) => t !== ''));
}

/** `depot`, when it names an SDLC (`"page"` or a URL), and its releases folder resolved against config.json. */
function depot(v: unknown, base: string): DepotConfig | undefined {
  if (typeof v !== 'object' || v === null) return undefined;
  const o = v as Record<string, unknown>;
  const sdlc = text(o['sdlc']);
  if (!sdlc) return undefined;
  const releases = text(o['releases']);
  return { sdlc, ...(releases ? { releases: new URL(releases, base).href } : {}) };
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
      const byName = depot(raw['depot'], url.href);
      file = {
        legendLite: text(raw['legendLite']), legendEngine: text(raw['legendEngine']), warehouse: text(raw['warehouse']),
        queryStore: text(raw['queryStore']), user: text(raw['user']),
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
    vendor: file.vendor,
    vault: file.vault,
    contexts: file.contexts,
    ...(file.depot ? { depot: file.depot } : {}),
  };
}
