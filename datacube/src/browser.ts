// The npm browser entry: one DataCube application, its existing planner and DuckDB in this browser.
// The host supplies a place and the package's static assets. No demo configuration or server participates.

import { openDuckDbInTab } from '../../engine-client/src/doors/tab-duckdb.ts';
import { openDataCube, type DataCubeApp } from './app/datacube-app.ts';
import { LitePlanner } from './lite-planner.ts';
import { browserServices, type BrowserServices } from './services/browser.ts';

export interface BrowserDataCubeStatus {
  readonly phase: 'starting' | 'ready' | 'opening' | 'disposed';
  readonly message: string;
}

export interface BrowserDataCubeOptions {
  /** The component fills this element, which must have a height supplied by the application. */
  readonly host: HTMLElement;
  /** The package's copied assets directory, served by this application's origin. */
  readonly assetsUrl: string | URL;
  /** Saved pages, queries and settings share this namespace. Choose one per application or workspace. */
  readonly storageNamespace?: string;
  /** Startup and host-requested file operations. A callback that throws rejects that operation. */
  readonly onStatus?: (status: BrowserDataCubeStatus) => void;
  /** Aborting disposes this instance. */
  readonly signal?: AbortSignal;
}

export interface BrowserDataCube {
  /** Add a CSV, JSON or Parquet file using the same path as the app's file picker. Resolves when its grid is ready. */
  openFile(file: File): Promise<void>;
  /** Remove the UI and close its database, browser storage connections and planner worker. Idempotent. */
  dispose(): Promise<void>;
}

const mounted = new WeakSet<HTMLElement>();

/** Mount the complete DataCube app, with planning and query execution entirely in this browser. */
export async function createBrowserDataCube(options: BrowserDataCubeOptions): Promise<BrowserDataCube> {
  const { host, signal } = options;
  signal?.throwIfAborted();
  if (typeof document === 'undefined' || host.ownerDocument !== document) {
    throw new Error('DataCube must be mounted in an element of the current browser document');
  }
  if (mounted.has(host)) throw new Error('This element already holds a DataCube instance');
  const assets = new URL(options.assetsUrl, document.baseURI);
  if (assets.origin !== window.location.origin || !['https:', 'http:'].includes(assets.protocol)) {
    throw new Error('DataCube assets must be served over HTTP or HTTPS from this application\'s origin');
  }
  if (assets.search || assets.hash) throw new Error('DataCube assetsUrl must name a directory without a query or fragment');
  if (!assets.pathname.endsWith('/')) assets.pathname += '/';
  const namespace = options.storageNamespace ?? 'legend-lite.datacube';
  if (namespace.trim().length === 0) throw new Error('DataCube storageNamespace must not be empty');

  const root = document.createElement('div');
  root.className = 'dc-browser';
  Object.assign(root.style, { position: 'relative', height: '100%', minHeight: '240px', display: 'flex', flexDirection: 'column', isolation: 'isolate', contain: 'layout paint' });
  const pageHost = document.createElement('div');
  Object.assign(pageHost.style, { position: 'relative', flex: '1', minHeight: '0' });
  const status = document.createElement('div');
  status.setAttribute('role', 'status');
  root.append(status, pageHost);
  host.append(root);
  mounted.add(host);

  const planner = new LitePlanner({
    model: '', runtime: '', assetBaseUrl: assets.href, workerUrl: new URL('planner-worker.js', assets).href,
  });
  const startup = new AbortController();
  let duck: Awaited<ReturnType<typeof openDuckDbInTab>> | undefined;
  let services: BrowserServices | undefined;
  let app: DataCubeApp | undefined;
  let disposed = false;
  let closing: Promise<void> | undefined;
  let queued: Promise<void> = Promise.resolve();
  let initialized!: () => void;
  const initialization = new Promise<void>((resolve) => { initialized = resolve; });
  // Both startup operations belong to this instance; a failed one must cancel the other.
  let databaseReady: Promise<Awaited<ReturnType<typeof openDuckDbInTab>>> | undefined;
  const say = (phase: BrowserDataCubeStatus['phase'], message: string): void => {
    status.textContent = message;
    options.onStatus?.({ phase, message });
  };
  const checkOpen = (): void => {
    signal?.throwIfAborted();
    if (disposed) throw new Error('DataCube has been disposed');
  };
  const dispose = (): Promise<void> => {
    if (closing) return closing;
    disposed = true;
    startup.abort();
    // This facade owns the browser worker and every model in it. Termination also cancels a compiler request that
    // cannot answer a graceful release until its current synchronous compilation finishes.
    planner.terminate();
    signal?.removeEventListener('abort', onAbort);
    root.remove();
    mounted.delete(host);
    closing = (async () => {
      // Storage may be blocked behind another tab. Stop waiting for it and release a ready database immediately,
      // so startup can unwind its unfinished app and listeners before initialization is declared settled.
      const early = Promise.allSettled([duck?.dispose(), services?.dispose()]);
      // An abort can arrive while openDataCube is awaiting its storage. Its eventual app is ours to dispose too.
      await initialization;
      const errors: unknown[] = [];
      try { app?.dispose(); } catch (error) { errors.push(error); }
      // A database still starting belongs to this instance even if the planner or app failed first.
      if (databaseReady) await databaseReady.then((ready) => { duck = ready; }, () => undefined);
      const ended = await Promise.allSettled([
        // Cancel native work before waiting on callers that may be waiting for that work.
        duck?.dispose(),
        queued,
        services?.dispose(),
        planner.close(),
      ]);
      errors.push(...[...await early, ...ended]
        .filter((r): r is PromiseRejectedResult => r.status === 'rejected').map((r) => r.reason as unknown));
      app = undefined;
      services = undefined;
      duck = undefined;
      databaseReady = undefined;
      if (errors.length > 0) throw new AggregateError(errors, 'Could not dispose DataCube');
    })();
    return closing;
  };
  // The creator and every file operation observe the signal too; abort initiates cleanup even while idle.
  const onAbort = (): void => {
    startup.abort(signal?.reason);
    void dispose().catch((error: unknown) => console.error('DataCube cleanup failed', error));
  };
  signal?.addEventListener('abort', onAbort, { once: true });
  try {
    say('starting', 'Starting DataCube in this browser…');
    checkOpen();
    const warmed = planner.warmUp();
    databaseReady = openDuckDbInTab(assets.href, planner, startup.signal);
    const [, ready] = await Promise.all([warmed, databaseReady]);
    duck = ready;
    checkOpen();
    services = await browserServices({
      plane: {
        word: 'local', planes: [{ id: 'host.plane.wasm', word: 'local', label: 'In this browser' }],
        choose: () => { throw new Error('This DataCube runs in the browser'); },
      },
      planner: {
        planner, tables: planner, projects: planner, label: 'local', tsItem: (text) => planner.tsItem(text),
        models: {
          another: (model, runtime, how) => planner.withModel(model, runtime, how),
          elements: (text) => planner.modelElements(text),
        },
      },
      duckdb: duck,
      config: { warehouse: '', queryStore: '', user: 'this browser', projects: [], vendor: assets.href },
      location: { href: window.location.href },
      embedding: { storageNamespace: namespace },
    });
    checkOpen();
    app = await openDataCube({
      host: pageHost, windows: root, status, services, start: { kind: 'empty' }, taken: [],
      settled: { show: (reason) => { status.textContent = reason; } }, publishDebugState: false,
    });
    checkOpen();
    say('ready', 'Ready');
  } catch (error) {
    // Cancelling terminates both workers. Their shutdown rejections may win the startup race, but the caller's
    // abort remains the reason this creation ended.
    const failure: unknown = signal?.aborted ? signal.reason : error;
    startup.abort(failure);
    initialized();
    try { await dispose(); } catch (cleanup) {
      throw new AggregateError([failure, cleanup], 'DataCube failed to start and could not finish cleanup', { cause: failure });
    }
    throw failure;
  } finally {
    initialized();
  }

  let disposedStatus = false;
  return {
    openFile: (file) => {
      const operation = queued.then(async () => {
        checkOpen();
        say('opening', `Opening ${file.name}…`);
        await app!.openFile(file);
        checkOpen();
        say('ready', 'Ready');
      });
      // File reads are supplied by the host too: a stream or arrayBuffer may never finish. Disposal rejects its
      // caller immediately; app.openFile checks its own lifetime before adding a grid if that read answers later.
      const opened = new Promise<void>((resolve, reject) => {
        const aborted = (): void => {
          startup.signal.removeEventListener('abort', aborted);
          reject(startup.signal.reason as unknown);
        };
        startup.signal.addEventListener('abort', aborted, { once: true });
        operation.then(() => {
          startup.signal.removeEventListener('abort', aborted);
          resolve();
        }, (error: unknown) => {
          startup.signal.removeEventListener('abort', aborted);
          reject(error);
        });
        if (startup.signal.aborted) aborted();
      });
      // A failed file is reported to its caller without preventing the next file from opening.
      queued = opened.then(() => undefined, () => undefined);
      return opened;
    },
    dispose: async () => {
      await dispose();
      if (!disposedStatus) {
        disposedStatus = true;
        say('disposed', 'DataCube closed');
      }
    },
  };
}
