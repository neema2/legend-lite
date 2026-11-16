// DATACUBE IN ANOTHER APPLICATION'S PAGE: the npm package's entry (npm/README.md; its types are library-api.ts, which
// the package ships as its declarations). The application fetches its data and hands it over; DataCube's app
// (src/app/datacube-app.ts) takes it from there, over the tab's own services (src/services/browser.ts) -- planned by
// legend-lite's compiler as WebAssembly and run by DuckDB-WASM, both in the page, as the site's page is (boot.ts). So
// this is one more host of the same app, beside the tab (boot.ts), Python's tab (engine.ts) and a notebook's cell
// (widget.ts), and it differs from the tab only in what a page that is not DataCube's own must not do:
//
//   - it reads nothing of the page's address and no config.json: what it is told, it is told in `options`;
//   - its WebAssembly and workers come from where `options.assets` says (the package's assets/ folder), never from
//     beside the page or the bundle;
//   - it sets no title and offers no share link (the address is the application's), and asks nothing of the browser
//     as the page is left unless told to (`askBeforeLeaving`);
//   - it gives the services a page of its own can stand behind there: the engine and the saved pages (this browser's).
//     No warehouse, no saved-query store and no model home: each needs a server or a site the application is not.
//
// Data comes in as a file does: each table handed over is a File the app opens as its picker opens one it is given
// (DataCubeApp.open), so it is read, modelled, saved and joined by the one code that does those for a person's file.

import { openDuckDbInTab } from '../../engine-client/src/doors/tab-duckdb.ts';
import { openDataCube, type DataCubeApp } from '../src/app/datacube-app.ts';
import { LitePlanner } from '../src/lite-planner.ts';
import { browserServices } from '../src/services/browser.ts';
import { browserBasics } from '../src/services/browser-basics.ts';
import type { DataCube, DataCubeData, DataCubeRows, mountDataCube as Declared } from './library-api.ts';

export type { DataCube, DataCubeContent, DataCubeData, DataCubeOptions, DataCubeRows } from './library-api.ts';

/** Each format's media type, as a picked file of it has. */
const MEDIA = { csv: 'text/csv', json: 'application/json', parquet: 'application/vnd.apache.parquet' } as const;

/** How many records are written as one part of a table's JSON: no part is a string too long for the page. */
const ROWS_A_PART = 5000;

/** `JSON.rawJSON`, where the browser has it: a bigint written as the digits it is, never rounded to a double. */
const rawJson = (JSON as { readonly rawJSON?: (text: string) => unknown }).rawJSON;

function exact(_key: string, value: unknown): unknown {
  if (typeof value !== 'bigint') return value;
  if (!rawJson) throw new Error('this browser cannot write a bigint as JSON exactly: hand it over as a number, or as text');
  return rawJson(String(value));
}

/** Records as one JSON document, an array of them, in parts: what the database's JSON reader reads a table from. */
function jsonParts(table: DataCubeRows): string[] {
  const { name, rows } = table;
  if (!Array.isArray(rows)) throw new Error(`${name}: its rows are not an array of records`);
  if (rows.length === 0) throw new Error(`${name} has no rows: a table's columns are read from its rows`);
  const parts = ['['];
  for (let at = 0; at < rows.length; at += ROWS_A_PART) {
    const part = rows.slice(at, at + ROWS_A_PART).map((row, i) => {
      if (typeof row !== 'object' || row === null || Array.isArray(row)) throw new Error(`${name}: row ${at + i + 1} is not a record`);
      return JSON.stringify(row, exact);
    });
    parts.push(`${at > 0 ? ',\n' : ''}${part.join(',\n')}`);
  }
  parts.push(']');
  return parts;
}

/** A table handed over, as the file the app opens: a File as it is, records as JSON, content under its format's name. */
function fileOf(data: DataCubeData): File {
  if (data instanceof File) return data;
  if (typeof data !== 'object' || data === null || typeof data.name !== 'string' || data.name.trim() === '') {
    throw new Error('a table handed to DataCube needs a name');
  }
  if ('rows' in data) return new File(jsonParts(data), `${data.name}.json`, { type: MEDIA.json });
  if (!Object.hasOwn(MEDIA, data.format)) {
    throw new Error(`${data.name}: ${String(data.format)} is not a format DataCube reads (${Object.keys(MEDIA).join(', ')})`);
  }
  return new File([data.content as BlobPart], `${data.name}.${data.format}`, { type: MEDIA[data.format] });
}

/** The package's assets/ folder as an absolute URL ending in a slash, on the page's own origin. */
function assetsUrl(given: string): string {
  if (typeof given !== 'string' || given === '') {
    throw new Error("mountDataCube needs `assets`: where the package's assets/ folder is served");
  }
  const url = new URL(given.endsWith('/') ? given : `${given}/`, document.baseURI);
  if (url.origin !== location.origin) {
    throw new Error(`DataCube's assets must be served from the page's own origin (${location.origin}), and ${url.origin} is another: a browser starts a worker from no other`);
  }
  return url.href;
}

/** Whether a DataCube is mounted in this page: its windows have ids, so a page holds one at a time. */
let mounted = false;

export const mountDataCube: typeof Declared = async (element, options) => {
  if (!(element instanceof HTMLElement)) throw new Error('mountDataCube needs the element DataCube fills');
  if (typeof options !== 'object' || options === null) throw new Error('mountDataCube needs its options: `assets` at least');
  if (mounted) throw new Error('DataCube is already mounted in this page: dispose it before mounting another');
  // what the application handed over, read before anything starts: a table that cannot be a file is refused here
  const assets = assetsUrl(options.assets);
  const files = (options.data ?? []).map(fileOf);
  mounted = true;

  // the page fills `host`; the app's windows float in `windows` (the element itself unless told another), which must
  // be positioned to keep them
  const windows = options.windows ?? element;
  const position = element.style.position;
  if (getComputedStyle(element).position === 'static') element.style.position = 'relative';
  const host = document.createElement('div');
  host.style.cssText = 'position:relative;box-sizing:border-box;height:100%';
  // with data to open on, nothing is shown until it is on the page: never the blank page's card first
  if (files.length > 0) host.style.visibility = 'hidden';
  element.replaceChildren(host);
  const status = document.createElement('span');
  status.textContent = 'starting DuckDB and the planner…';

  // the compiler's module and its worker from the assets folder: no model of its own (a page with no sample), each
  // table's model the planner's own writer's (infer.ts)
  const planner = new LitePlanner({
    assetBaseUrl: assets, model: '', runtime: '', workerUrl: new URL('planner-worker.js', assets).href,
  });
  let duck: Awaited<ReturnType<typeof openDuckDbInTab>> | undefined;
  let app: DataCubeApp | undefined;
  let down = false;
  const takeDown = (): void => {
    if (down) return;
    down = true;
    app?.dispose();
    planner.dispose();
    void duck?.close().catch(() => {});
    host.remove();
    element.style.position = position;
    mounted = false;
  };

  try {
    // the planner's boot layer built while DuckDB downloads and starts: the slower of the two is the floor (boot.ts)
    const warmed = planner.warmUp();
    warmed.catch(() => {});
    duck = await openDuckDbInTab(assets, planner);
    await warmed;
    const services = await browserServices({
      // one planner, the page's own: no other to choose
      plane: { word: 'local', planes: [], choose: () => {} },
      planner: {
        planner,
        label: 'local',
        tables: planner,
        projects: planner,
        tsItem: (text) => planner.tsItem(text),
        models: {
          another: (model, runtime, how) => planner.withModel(model, runtime, how),
          elements: (text) => planner.modelElements(text),
        },
      },
      duckdb: duck,
      config: { warehouse: '', queryStore: '', user: '', projects: [], vendor: assets },
      location: { href: document.baseURI },
    });
    status.textContent = services.engine.label;
    app = await openDataCube({
      // the engine and this browser's saved pages; the browser's own things without an address or a title, which are
      // the application's
      services: { engine: services.engine, ...(services.pages ? { pages: services.pages } : {}), browser: browserBasics },
      host,
      status,
      windows,
      start: { kind: 'blank' },
      taken: [],
      settled: { show: () => {} },
      ...(options.askBeforeLeaving ? {} : { embedded: true as const }),
    });
    for (const file of files) await app.open(file);
  } catch (e) {
    takeDown();
    throw e;
  }
  host.style.visibility = '';

  const opened = app;
  return {
    add: async (data) => {
      if (down) throw new Error('DataCube was taken down: it takes no data');
      await opened.open(fileOf(data));
    },
    dispose: takeDown,
  } satisfies DataCube;
};
