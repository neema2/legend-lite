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
//     No warehouse, no saved-query store and no model home: each needs a server or a site the application is not;
//   - the page lives on after it: a mount can be stopped (`signal`), and taking it down ends its workers and takes off
//     the page everything it put there, its menus and dialogs on the document's body included.
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

/** A file's name the app reads by: the endings its picker takes (file-handles.ts DATA_FILES), each also gzipped. A
 *  name with another ending is refused here -- the app would read it as CSV, which its picker never lets happen. */
const READ_BY_NAME = /\.(csv|json|jsonl|ndjson|parquet)(\.gz)?$/i;

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
    const part = rows.slice(at, at + ROWS_A_PART).map((row: unknown, i) => {
      if (typeof row !== 'object' || row === null || Array.isArray(row)) throw new Error(`${name}: row ${at + i + 1} is not a record`);
      return JSON.stringify(row, exact);
    });
    parts.push(`${at > 0 ? ',\n' : ''}${part.join(',\n')}`);
  }
  parts.push(']');
  return parts;
}

/** The file a table called `name` is read as: its name under its format's ending, once. */
function fileName(name: string, format: keyof typeof MEDIA): string {
  return name.toLowerCase().endsWith(`.${format}`) ? name : `${name}.${format}`;
}

/** A table handed over, as the file the app opens: a File as it is, records as JSON, content under its format's name. */
function fileOf(data: DataCubeData): File {
  if (data instanceof File) {
    if (!READ_BY_NAME.test(data.name)) {
      throw new Error(`${data.name || 'a file with no name'}: a file is read by its name's ending, one of .csv, .json, .jsonl, .ndjson and .parquet (each also .gz)`);
    }
    return data;
  }
  if (typeof data !== 'object' || data === null || typeof data.name !== 'string' || data.name.trim() === '') {
    throw new Error('a table handed to DataCube needs a name');
  }
  if ('rows' in data) return new File(jsonParts(data), fileName(data.name, 'json'), { type: MEDIA.json });
  if (!Object.hasOwn(MEDIA, data.format)) {
    throw new Error(`${data.name}: ${String(data.format)} is not a format DataCube reads (${Object.keys(MEDIA).join(', ')})`);
  }
  const content: unknown = data.content;
  if (!(typeof content === 'string' || content instanceof Blob || content instanceof ArrayBuffer || ArrayBuffer.isView(content))) {
    throw new Error(`${data.name}: its content is not text, bytes or a Blob`);
  }
  return new File([content as BlobPart], fileName(data.name, data.format), { type: MEDIA[data.format] });
}

/** The package's assets/ folder as an absolute URL ending in a slash, on the page's own origin. */
function assetsUrl(given: unknown): string {
  if (typeof given !== 'string' || given === '') {
    throw new Error("mountDataCube needs `assets`: where the package's assets/ folder is served");
  }
  let url: URL;
  try {
    url = new URL(given, document.baseURI);
  } catch {
    throw new Error(`mountDataCube's \`assets\` is no address: ${given}`);
  }
  if (url.search !== '' || url.hash !== '') {
    throw new Error(`mountDataCube's \`assets\` names a folder, and ${given} carries a query or a fragment: each file is fetched by its name under it`);
  }
  if (url.origin !== location.origin) {
    throw new Error(`DataCube's assets must be served from the page's own origin (${location.origin}), and ${url.origin} is another: a browser starts a worker from no other`);
  }
  if (!url.pathname.endsWith('/')) url.pathname += '/';
  return url.href;
}

/** The two workers' scripts, which a browser says nothing useful about when it cannot load one (the compiler's worker
 *  dies with no message; DuckDB's start then waits for ever). */
const WORKERS = ['planner-worker.js', 'duckdb-browser-eh.worker.js'] as const;

/**
 * THE ASSETS FOLDER IS THERE, asked before anything starts: each worker's script by a HEAD, so a folder that is not
 * where `assets` says is refused in words that name the file -- the commonest mistake an application makes, and one
 * the workers themselves report as nothing at all. A site that answers every address with its own page (an
 * application's router) answers a missing file with 200 and a page: that is refused too. Any other answer is the
 * load's own to judge.
 */
async function assetsThere(assets: string, signal: AbortSignal | undefined): Promise<void> {
  await Promise.all(WORKERS.map(async (file) => {
    const missed = (how: string): Error => new Error(`DataCube's assets are not at ${assets}: ${file} ${how}. Serve the package's assets/ folder there, or say in \`assets\` where it is served`);
    let answer: Response;
    try {
      answer = await fetch(new URL(file, assets), { method: 'HEAD', ...(signal ? { signal } : {}) });
    } catch (e) {
      if (signal?.aborted) throw e;
      throw missed(`could not be fetched (${e instanceof Error ? e.message : String(e)})`);
    }
    if (answer.status === 404 || answer.status === 410) throw missed('is not there');
    if ((answer.headers.get('content-type') ?? '').includes('text/html')) throw missed('is answered with a page, not the file');
  }));
}

/** The names DataCube's app leaves on `window` for its browser harnesses (datacube-app.ts): taken off with it, so
 *  nothing of a page taken down is still held. */
const HARNESS_NAMES = ['__dataPage', '__dataCube', '__dataPageHash', '__dataCubeSignal', '__dataCubeViews'] as const;

/** Whether a DataCube is mounted in this page: its windows have ids, so a page holds one at a time. */
let mounted = false;

export const mountDataCube: typeof Declared = async (element, options) => {
  if (!(element instanceof HTMLElement)) throw new Error('mountDataCube needs the element DataCube fills');
  if (typeof options !== 'object' || options === null) throw new Error('mountDataCube needs its options: `assets` at least');
  const { signal } = options;
  signal?.throwIfAborted();
  if (mounted) throw new Error('DataCube is already mounted in this page: dispose it before mounting another');
  // what the application handed over, read before anything starts: a table that cannot be a file is refused here
  const assets = assetsUrl(options.assets);
  const files = (options.data ?? []).map(fileOf);
  mounted = true;

  // the page fills `host`, put in the element once the assets are known to be there (below): until then the element is
  // as the application has it. The app's own windows float in `windows` (the element itself unless told another),
  // which must be positioned to keep them
  const windows = options.windows ?? element;
  const position = element.style.position;
  const host = document.createElement('div');
  host.style.cssText = 'position:relative;box-sizing:border-box;height:100%';
  // with data to open on, nothing is shown until it is on the page: never the blank page's card first
  if (files.length > 0) host.style.visibility = 'hidden';
  const status = document.createElement('span');
  status.textContent = 'starting DuckDB and the planner…';
  // where the application's focus was: the blank page takes it as it opens, which a page of its own may and a part of
  // another's may not
  const focused = document.activeElement;

  // WHAT DATACUBE LAYS OVER THE PAGE: its menus and its dialogs go on the document's body (ui/menu-view.ts, the
  // pickers' backdrops), where the app's own taking down does not reach one left open. Each is noted as it is put
  // there -- an element of DataCube's classes, never the application's own -- and taken off with DataCube.
  const laid = new Set<Element>();
  const note = (changes: readonly MutationRecord[]): void => {
    for (const change of changes) {
      for (const node of change.removedNodes) if (node instanceof Element) laid.delete(node);
      for (const node of change.addedNodes) {
        if (node instanceof Element && [...node.classList].some((name) => name.startsWith('dc-'))) laid.add(node);
      }
    }
  };
  const watcher = new MutationObserver(note);
  watcher.observe(document.body, { childList: true });

  // the compiler's module and its worker from the assets folder: no model of its own (a page with no sample), each
  // table's model the planner's own writer's (infer.ts)
  const planner = new LitePlanner({
    assetBaseUrl: assets, model: '', runtime: '', workerUrl: new URL('planner-worker.js', assets).href,
  });
  let duck: Awaited<ReturnType<typeof openDuckDbInTab>> | undefined;
  /** The database's start, while it is under way: it asks the planner for its setup, so the planner outlives it. */
  let starting: Promise<unknown> | undefined;
  let app: DataCubeApp | undefined;

  // TAKEN DOWN, once: by dispose, by the signal, or by a start that failed. `gone` is rejected with why, so whatever
  // waits on DataCube then (the mount itself, a table being added) is told at once -- the database's and the
  // compiler's own calls are dropped unanswered when their workers end. Every step runs whatever the one before did.
  let down = false;
  let tellGone = (_why: unknown): void => {};
  const gone = new Promise<never>((_resolve, reject) => { tellGone = reject; });
  gone.catch(() => {});
  const until = <T>(work: Promise<T>): Promise<T> => Promise.race([work, gone]);
  const takeDown = (why: unknown): void => {
    if (down) return;
    down = true;
    mounted = false;
    tellGone(why);
    const steps = [
      () => app?.dispose(),
      // the planner ended -- for good: work still under way on the page asks it afterwards, and it starts no worker
      // then (LitePlanner.end) -- once the database's start, if one is under way, is over: a planner ended under it
      // leaves its asks unanswered, the start never over and its worker never ended (each found by the test's count
      // of workers left)
      () => void (starting ?? Promise.resolve()).catch(() => {}).then(() => planner.end()),
      () => duck?.close(),
      () => {
        // (what was put there in this very task is not yet told: read, then told no more)
        note(watcher.takeRecords());
        watcher.disconnect();
        for (const node of laid) node.remove();
        laid.clear();
      },
      () => { for (const name of HARNESS_NAMES) Reflect.deleteProperty(window, name); },
      () => {
        host.remove();
        element.style.position = position;
      },
    ];
    let failed: { readonly error: unknown } | undefined;
    for (const step of steps) {
      try {
        step();
      } catch (error) {
        failed ??= { error };
      }
    }
    if (failed) throw failed.error;
  };
  const aborted = (): void => {
    try {
      takeDown(signal?.reason);
    } catch {
      // the mount is rejected with the signal's reason: what taking it down met is not the application's to hear
    }
  };
  signal?.addEventListener('abort', aborted, { once: true });

  /** A file on the page, or why not: refused as the app refuses it, or not landed. */
  const open = async (file: File): Promise<void> => {
    if (!app) throw new Error('DataCube is not open');
    if (!(await until(app.open(file)))) {
      throw new Error(`${file.name} was not added: another page was put in DataCube while it was read, or the page it would have replaced was kept`);
    }
  };

  try {
    await until(assetsThere(assets, signal));
    if (getComputedStyle(element).position === 'static') element.style.position = 'relative';
    element.replaceChildren(host);
    // the planner's boot layer built while DuckDB downloads and starts: the slower of the two is the floor (boot.ts)
    const warmed = planner.warmUp();
    warmed.catch(() => {});
    const start = openDuckDbInTab(assets, planner);
    // (one that starts after the mount was taken down is ended as it arrives; the planner after it, above)
    starting = start.then((started) => {
      duck = started;
      if (down) started.close();
    }, () => {}).finally(() => { starting = undefined; });
    const started = await until(start);
    await until(warmed);
    const services = await until(browserServices({
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
      duckdb: started,
      config: { warehouse: '', queryStore: '', user: '', projects: [], vendor: assets },
      location: { href: document.baseURI },
    }));
    status.textContent = services.engine.label;
    const opening = openDataCube({
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
    // (as the database: an app that opens after the mount was taken down is taken down as it arrives)
    void opening.then((opened) => {
      app = opened;
      if (down) opened.dispose();
    }, () => {});
    await until(opening);
    for (const file of files) await open(file);
    // the blank page took the focus as it opened: given back where the application had it
    if (host.contains(document.activeElement)) {
      if (focused instanceof HTMLElement && focused.isConnected && focused !== document.body) focused.focus();
      else (document.activeElement as HTMLElement | null)?.blur();
    }
  } catch (e) {
    try {
      takeDown(e);
    } catch {
      // the start's own failure is the one to tell
    }
    throw e;
  } finally {
    signal?.removeEventListener('abort', aborted);
  }
  host.style.visibility = '';

  /** The tables added, one after another in the order asked: two read at once would each open in place of the other. */
  let last: Promise<unknown> = Promise.resolve();
  return {
    add: async (data) => {
      if (down) throw new Error('DataCube was taken down: it takes no data');
      const file = fileOf(data);
      const added = last.then(() => {
        if (down) throw new Error('DataCube was taken down: it takes no data');
        return open(file);
      });
      last = added.catch(() => {});
      return added;
    },
    dispose: () => takeDown(new Error('DataCube was taken down')),
  } satisfies DataCube;
};
