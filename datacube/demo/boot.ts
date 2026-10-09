// A runnable demo: real DuckDB-WASM in the browser, real snap mode, the real grid.
//
// ONE PAGE, its planner a setting (`chosenPlane`, `?planner=`): in this tab, legend-lite or
// legend-engine -- three addresses of the same service, built by planners.ts. The planner writes
// the SQL; DuckDB in this tab runs it. There is no fallback: a planner that does not answer is
// said (`refusePlanner`) and the page stops.

import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import * as duckdb from '../../engine-client/src/duckdb-wasm.ts';

import { CubeApp, type HeldCopy } from '../src/app.ts';
import { PageApp, type GridMaker } from '../src/page/page-app.ts';
import {
  DEFAULT_CONFIGURATION,
  DEFAULT_MAX_ROWS,
  type CubeConfiguration,
} from '../src/config.ts';
import type { Planner } from '../src/cube.ts';
import type { ModelOptions } from '../src/planner.ts';
import { DuckDbEngine, type ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import type { SeedSource, TestData } from '../../engine-client/src/model-data.ts';
import { inferFormat, mountRemote, type S3Credentials } from '../src/remote.ts';
import { catalogColumns, forgetUpload, formatOf, ingestFile, tableNameOf } from '../src/upload.ts';
import { pickSource, type DatabaseSession, type PickerSections, type RemoteCredentials, type SectionId } from '../src/ui/source-picker.ts';
import { saveDialog } from '../src/ui/save-dialog.ts';
import { Latest, TabWork, mayLeave } from '../src/host.ts';
import { isPageFragment, readPageFragment, shareLink } from '../src/share/link.ts';
import {
  fileSource,
  openCube,
  type CubeDocument,
  type CubeSource,
  type FileSource,
  type QuerySource,
  type RemoteSource,
  type WarehouseSource,
} from '../src/cube-document.ts';

/** A saved cube over a file: what reopening one needs is that file back. */
type FileCube = CubeDocument & { readonly source: FileSource };
const isFileCube = (doc: CubeDocument): doc is FileCube => doc.source._type === 'file';
import {
  BrowserRecords,
  MemoryRecords,
  RuleStore,
  openCubeDatabase,
  persistStorage,
  type CubeStore,
} from '../src/cube-store.ts';
import {
  FileHandles,
  canKeepHandles,
  pickDataFile,
  readHandle,
  type FileHandle,
} from '../src/file-handles.ts';
import { CubeLibrary } from '../src/ui/cube-library.ts';
import {
  pageContent,
  pageDefinitionText,
  readSaved,
  type PageDocument,
  type SavedDocument,
} from '../src/page-document.ts';
import type { InferredModel, TableModels } from '../src/infer.ts';
import type { ModelHome } from '../../depot-client/src/model-home.ts';
import { pageConfig, type DepotConfig, type PageConfig, type ProjectConfig } from './page-config.ts';
import type { ModelElement } from '../src/saved-queries.ts';
import type { Query, QueryReader } from '../../query-store/src/index.ts';

// SAVED QUERIES' CODE, fetched the first time it is needed -- the Saved queries section, a query
// link -- as charts are: a grid-only page never downloads it (test/bundle-budget.test.ts).
const savedQueries = () => import('../src/saved-queries.ts');
const queryStores = () => import('../../query-store/src/index.ts');
import {
  connect,
  listObjects,
  signInWithKey,
  WarehouseEngine,
  type CatalogObject,
  type WarehouseSession,
} from '../../engine-client/src/warehouse.ts';
import { makeWindow, type WindowSpec } from '../src/ui/window.ts';
import type { MenuItem } from '../src/ui/menu.ts';
import {
  SAMPLES,
  sampleById,
  sampleFileName,
  type Sample,
} from '../src/samples.ts';
import type { ColumnFormat } from '../src/format.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { TreeState } from '../src/tree.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { accessor, lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { SnapTarget } from '../src/cube.ts';

const ROWS = 200_000;

/**
 * THE THREE PLANES, as data rather than as three copies of a menu.
 *
 * Each is one page loading one arrangement, statically: a bundle
 * decides who compiles the Pure and who runs it, and nothing at
 * runtime can change that (`test/guardrails.test.ts` -- the one time
 * shipped code could pick a planner by health check, the fallback hid
 * three real bugs for the life of the project). The menu navigates;
 * it does not switch.
 */
export const PLANES: readonly {
  readonly id: `host.plane.${string}`;
  /** Its `?planner=` word, and the status bar's. */
  readonly word: PlaneWord;
  readonly label: string;
}[] = [
  { id: 'host.plane.wasm', word: 'local', label: 'Plan local (in this tab)' },
  { id: 'host.plane.server', word: 'remote', label: 'Plan remote (legend-lite on :8080)' },
  { id: 'host.plane.engine', word: 'engine', label: 'Plan on the engine (legend-engine on :6300)' },
];

/** Where the page plans: in this tab, on legend-lite, or on legend-engine -- the same API. */
export type PlaneWord = 'local' | 'remote' | 'engine';

/**
 * THE ONE PLACE A PLANNER IS CHOSEN (the user, 2026-09-30): the page's `?planner=`, read once
 * as the page loads -- absent, the planner in this tab. ONE PAGE, three planners at three
 * addresses of the same API: the in-tab one by WebAssembly calls, legend-lite and legend-engine
 * over HTTP. The choice is explicit (the address, the status bar's picker), always shown (the
 * status bar names it), and never changed behind the user's back: an unknown word is refused,
 * and a planner that does not answer is SAID, never substituted (test/guardrails.test.ts).
 */
/**
 * WHAT THE PAGE WAS ASKED TO OPEN, decided before anything is generated or mounted
 * (docs/DATACUBE_APP_PLAN_2026_10_02.md, A3). Only `sample` builds the sample cube -- over the
 * generated trades, or over a `?remote=` file read through the same model. A share link or a saved
 * page in the address opens in place of nothing; so does the single-user app's launch key
 * (`#key=…&table=schema.name`, a fragment: never sent to a server).
 */
type Start =
  | { readonly kind: 'sample' }
  | { readonly kind: 'link' }
  | { readonly kind: 'warehouse'; readonly key: string; readonly table?: string };

async function startOf(where: Location): Promise<Start> {
  if (where.hash.startsWith('#key=')) {
    const p = new URLSearchParams(where.hash.slice(1));
    const table = p.get('table');
    return { kind: 'warehouse', key: p.get('key') ?? '', ...(table ? { table } : {}) };
  }
  if (isPageFragment(where.hash)) return { kind: 'link' };
  const queryLink = where.hash.replace(/^#\/shared\//, '#');
  if (queryLink.length > 1 && (await queryStores()).isQueryFragment(queryLink)) return { kind: 'link' };
  return { kind: 'sample' };
}

export function chosenPlane(): PlaneWord {
  const word = new URLSearchParams(location.search).get('planner') ?? 'local';
  const known = PLANES.find((plane) => plane.word === word);
  if (!known) {
    throw new Error(`?planner=${word} is not a planner: one of ${PLANES.map((p) => p.word).join(', ')}`);
  }
  return known.word;
}

/**
 * THE REFUSAL, when the chosen planner does not answer: said on the page, in words that name what
 * was wanted and where, with the way to another -- and the page stops there. It never switches
 * planners for the user (test/guardrails.test.ts).
 */
export function refusePlanner(what: string, where: string, start: string): never {
  const box = must('plannermissing');
  box.replaceChildren();
  const strong = document.createElement('strong');
  strong.textContent = `${what} is not answering on ${where || 'no configured address'}.`;
  const rest = document.createElement('span');
  rest.textContent = ` This page does not switch planners on its own, and will not show numbers it made up: ${start}, and reload -- or plan in this tab: `;
  const local = document.createElement('a');
  const url = new URL(location.href);
  url.searchParams.delete('planner');
  local.href = url.href;
  local.textContent = 'index.html';
  box.append(strong, rest, local, document.createTextNode('.'));
  box.hidden = false;
  throw new Error(`${what} is not answering on ${where || 'no configured address'}`);
}

/** Which planner the page plans on. */
export function currentPlane(): string {
  return chosenPlane();
}

/** The plane entries, with the one you are ON disabled, not hidden. */
export function planeMenu(): MenuItem[] {
  const now = currentPlane();
  return PLANES.map((plane) => ({
    id: plane.id,
    label: plane.label,
    // where the planner runs: the status bar's readout opens these
    section: 'plane' as const,
    ...(plane.word === now ? { disabled: true as const } : {}),
  }));
}

/** Navigate to a plane, if that is what was chosen. */
export function goToPlane(id: string | undefined): boolean {
  const found = PLANES.find((plane) => plane.id === id);
  if (!found) return false;
  // THE SAME PAGE, its planner named in the address; the page's other settings go with it
  // (?remote=, ?warehouse=, ...). A NAVIGATION: a cube never changes planner while it runs.
  const url = new URL(location.href);
  url.pathname = url.pathname.replace(/[^/]*$/, 'index.html');
  url.searchParams.set('planner', found.word);
  location.href = url.href;
  return true;
}

/**
 * The formats the HOST knows and the snapshot cannot: notional and
 * pnl are money, qty is a count. Rendering a trade count as $10,005
 * is the kind of wrong that looks plausible.
 */
export const MONEY: ColumnFormat = {
  kind: 'currency',
  currency: 'USD',
  locale: 'en-US',
  maximumFractionDigits: 0,
  negativeParens: true,
};

/** The demo's own configuration, shared by every plane. */
export function demoConfiguration(title: string): CubeConfiguration {
  return {
    ...DEFAULT_CONFIGURATION,
    reportTitle: title,
    showSelectionStats: true,
    columns: {
      notional: { format: MONEY },
      pnl: { format: MONEY },
      qty: {
        format: { kind: 'number', locale: 'en-US', maximumFractionDigits: 0 },
      },
    },
  };
}

/** The named hierarchies the demo offers, shared by every plane. */
export const DEMO_DIMENSIONS: readonly {
  readonly name: string;
  readonly columns: readonly string[];
}[] = [
  { name: 'Geography', columns: ['region', 'desk', 'book'] },
  { name: 'Calendar', columns: ['year', 'qtr'] },
];

/** What the page hands `boot`: its planner and what it reads. */
export interface Engine {
  readonly planner: Planner;
  readonly source: ValueSpecification;
  readonly snapTarget: SnapTarget;
  /**
   * legend-lite's model writer for a table opened on the page -- a file, a remote file, a warehouse
   * table -- whichever planner plans it (infer.ts): the planner itself in this tab; legend-lite's
   * module, loaded when first asked, beside a planner on a server (planners.ts).
   */
  readonly tables: TableModels;
  /**
   * legend-lite's planner in this tab for a project's test data (a saved query opened by name): the server's statements
   * that make and fill its tables (engine-client's model-data.ts) -- the same module as `tables`.
   */
  readonly seeds: SeedSource;
  /**
   * A file opened in this tab: the planner repointed at the model written for it from DuckDB's
   * catalog (`tables`, infer.ts). Every planner takes one (planners.ts); absent, the page
   * offers no file to open -- the capability and the affordance are the same fact.
   */
  readonly models?: {
    use(model: string, runtime: string, how?: ModelOptions): void;
    /** ANOTHER source on the page: a planner over its model, on the same worker or server. */
    another(model: string, runtime: string, how?: ModelOptions): Planner;
    /** A model's elements as the compiler reads them: a saved query's project (data spaces, enumerations). */
    elements(text: string): Promise<unknown[]>;
  };
  /**
   * What the status line should say about this planner.
   *
   * Returned rather than written, because `boot` starts the planner
   * CONCURRENTLY with DuckDB: two writers racing on one status line
   * produce flicker and, worse, a final message that depends on
   * which finished last. `boot` owns the line and writes this when
   * both are ready.
   */
  /**
   * WHICH BACKEND ANSWERED, in one word.
   *
   * `local` plans in this tab, `remote` on legend-lite over HTTP,
   * `engine` on the real legend-engine. It sits in a 20px strip
   * beside the row count, where "planner: legend-lite (wasm, no
   * server)" was most of the bar -- and three planes want three
   * words a person can tell apart at a glance, not three sentences.
   */
  readonly label: string;
}

/**
 * How the page supplies its planner: the one `chosenPlane` names, built by planners.ts (main.ts).
 * Passed in so `boot` starts it concurrently with DuckDB.
 */
export type MakePlanner = (status: HTMLElement) => Promise<Engine>;

/** Where the demo's shared model and tables live: `#>{trades::DB.TRADES}#`. */
export const SOURCE = accessor('trades::DB', 'TRADES');
/** Where the demo cube snaps; its copy is planned by the page's own planner (the model is DuckDB's). */
export const SNAP_TARGET: Omit<SnapTarget, 'planner'> = {
  table: 'TRADES_SNAP',
  source: accessor('trades::DB', 'TRADES_SNAP'),
  conversions: [],
};
export const RUNTIME = 'trades::RT';

/** One copy of the model text, fetched so the file is the source. */
export async function loadModel(): Promise<string> {
  return (await fetch('./trades.pure')).text();
}

// -- sample data ----------------------------------------------------

const REGIONS = ['EMEA', 'AMER', 'APAC'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equity', 'Commodities'];

/**
 * DuckDB in this tab: its bundle from our own origin, its worker, one connection -- the engine,
 * and the database itself (files are registered on it). Shared by the demo page and the page of
 * several cubes (page.ts).
 */
export async function startDuckDb(): Promise<{ readonly engine: DuckDbEngine; readonly db: duckdb.AsyncDuckDB }> {
  // Bundles are served from OUR origin, copied out of node_modules by
  // `bazel build //datacube:vendor`. Loading them from a CDN instead forces a
  // cross-origin Worker, which the platform forbids outright and which
  // is then usually worked around with a blob that importScripts the
  // CDN url. That workaround exists to solve a problem worth not
  // having: a deployment behind a firewall is not fetching its query
  // engine from a CDN anyway.
  // Absolute URLs, not relative ones. The WORKER resolves mainModule
  // against its own location, so './vendor/x.wasm' becomes
  // '/demo/vendor/vendor/x.wasm' and fails as an opaque
  // "WebAssembly.compile: HTTP status code is not ok".
  const asset = (f: string) => new URL(`./vendor/${f}`, location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: asset('duckdb-mvp.wasm'),
      mainWorker: asset('duckdb-browser-mvp.worker.js'),
    },
    eh: {
      mainModule: asset('duckdb-eh.wasm'),
      mainWorker: asset('duckdb-browser-eh.worker.js'),
    },
  });
  const worker = new Worker(bundle.mainWorker!);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  return { engine: new DuckDbEngine(conn as unknown as ArrowishConnection), db };
}

/** The demo's trades, generated in this tab's DuckDB as the table the model reads. */
export async function generateTrades(engine: DuckDbEngine): Promise<void> {
  await engine.run(
    `CREATE OR REPLACE TABLE trades AS
     SELECT
       ${sqlPick(REGIONS, 'i % 3')}            AS region,
       ${sqlPick(DESKS, '(i // 3) % 5')}       AS desk,
       (2021 + ((i // 15) % 5))                AS year,
       ('Q' || (1 + ((i // 75) % 4)))          AS qtr,
       ('Book ' || (1 + ((i // 300) % 4)))     AS book,
       ((i * 7919) % 1000000) / 100.0  AS notional,
       ((i * 104729) % 200000) / 100.0 - 1000.0 AS pnl,
       ((i * 31) % 97) + 1             AS qty
     FROM range(${ROWS}) t(i)`,
    0,
  );
}

export async function boot(makePlanner: MakePlanner): Promise<void> {
  const status = must('status');

  // Start the planner NOW, and await it further down where it is
  // first needed.
  //
  // It needs nothing from DuckDB and DuckDB needs nothing from it,
  // but boot used to run them in series, so ~1.3s of boot-layer
  // construction (prelude parse, system metamodel, resolve and
  // normalize) waited for a 36 MB WASM instantiate that had already
  // finished nothing useful for it. Overlapped, the slower of the
  // two sets the floor instead of their sum.
  performance.mark('dc:boot-start');
  const engineReady = makePlanner(status);
  void engineReady.then(() => performance.mark('dc:planner-ready'));
  // Await happens below; this only stops an early rejection being
  // reported as unhandled in the window before that.
  engineReady.catch(() => {});

  const start = await startOf(location);
  status.textContent = 'starting DuckDB…';

  const { engine, db } = await startDuckDb();
  performance.mark('dc:duckdb-ready');

  // A REMOTE SOURCE, when one is named.
  //
  //   ?remote=https://host/trades.parquet
  //   ?remote=s3://bucket/table&format=iceberg
  //
  // The data stays where it is: DuckDB reads it over HTTP range
  // requests, so the cube pulls the bytes a query needs rather than
  // the file. Everything downstream is unchanged, because the remote
  // file is mounted as a VIEW called `trades` -- the same name the
  // generated table would have had, and the name the model already
  // refers to.
  const params = new URLSearchParams(location.search);
  const remote = params.get('remote');
  /** Generated rows are a copy in this tab from the start; a mounted remote file is live. */
  let generated: HeldCopy | undefined;
  if (start.kind === 'sample' && remote) {
    status.textContent = `mounting ${remote}…`;
    const format = params.get('format');
    await mountRemote(engine, {
      sources: [{
        name: 'trades',
        url: remote,
        ...(format === 'parquet' || format === 'csv' || format === 'iceberg'
          ? { format }
          : {}),
      }],
      // Credentials come from the host, never from the URL bar: a
      // query string lands in history, logs and shoulder-surfing
      // range. A bucket that needs them is configured by the
      // embedding application.
    });
    // said on every receipt: this tab's DuckDB answers, reading the file over HTTP
    engine.readsRemote(remote);
    status.textContent = `reading ${remote}`;
  } else if (start.kind === 'sample') {

  status.textContent = `generating ${ROWS.toLocaleString(UI_LOCALE)} rows…`;
  await generateTrades(engine);
  generated = { label: 'trades (generated in this tab)', takenAt: new Date(), rowCount: ROWS };
  }

  // -- the cube ------------------------------------------------------

  performance.mark('dc:data-ready');
  status.textContent = 'starting planner…';
  const { planner, source, snapTarget, label, models, tables, seeds } = await engineReady;
  status.textContent = label;

  /** The sample cube's opening view. */
  const sampleSnapshot = async (): Promise<CubeSnapshot> => ({
    source: { query: source },
    // the compiler types every column; the page declares only that year is a dimension
    columns: await sourceColumns(planner, source, [{ name: 'year', kind: 'dimension' }]),
    derived: [],
    rows: ['region', 'desk', 'book'],
    pivotOn: ['year'],
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
  });


  // The page builds the APP, not a grid and a pile of checkboxes.
  // Those checkboxes were the demo standing in for a product; what
  // they reached is now reachable from the toolbar, the drag zones,
  // the context menu and the properties editor -- which is the whole
  // reason src/app.ts exists.
  // The host knows what the snapshot cannot: that notional and pnl
  // are money and qty is a count. Rendering a trade count as $10,005
  // is the kind of wrong that looks plausible.
  const configuration: CubeConfiguration = demoConfiguration('Trades');

  // The close button on each host window. Wired once, by delegation,
  // so a window can be added to the markup without another listener.
  document.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const close = target.closest('.hostwin-close');
    if (!(close instanceof HTMLElement)) return;
    const id = close.dataset['win'];
    if (id) must(id).hidden = true;
  });

  // The cube, built so it can be built AGAIN.
  //
  // Opening a file replaces the model, and therefore the columns and
  // the source relation, so the app is recreated rather than mutated
  // -- a cube whose snapshot no longer matches its model is not a
  // state worth supporting. Everything the construction needs is a
  // parameter so there is only one copy of it.
  const host = must('app');
  const DEMO_DIMENSIONS = [
    { name: 'Geography', columns: ['region', 'desk', 'book'] },
    { name: 'Calendar', columns: ['year', 'qtr'] },
  ];

  /**
   * WORK THIS TAB HOLDS that leaving would lose: each part of the page adds what it knows (an
   * opened file lives only in this tab's database; a warehouse session only in memory; unsaved
   * changes). Asked before leaving the page and before switching plane (P2-337).
   */
  const work = new TabWork();
  /** Set once the person has agreed to leave: the browser's own question is not asked twice. */
  let leaving = false;

  /**
   * FOR THE BROWSER HARNESSES: when the page has finished what an action started, as a fact
   * rather than a guess. `changes` counts every change the cube reports (a view landing, a
   * presentation change, a refusal, the Pure pane written); `printing` is the Pure prints still
   * out. With the cube's own `busy`, a harness waits until nothing is in flight and nothing has
   * changed for a moment -- the time the action took, not a fixed sleep (a 4s fall-through on
   * every change that runs no query made verify-features take 8 minutes, and a 150ms sleep read
   * the page before a slower machine had re-queried). Not product code: the demo page.
   */
  const harnessSignal = { changes: 0, printing: 0 };
  (window as unknown as { __dataCubeSignal?: typeof harnessSignal }).__dataCubeSignal = harnessSignal;

  /** A source's rows as they are: grouped by nothing, up to the row cap (a person builds the cube up). */
  function rawRows(query: ValueSpecification, columns: CubeSnapshot['columns']): CubeSnapshot {
    return { source: { query }, columns, derived: [], rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1 };
  }

  /** A grid's place, as a maker takes it: where its rows are and how it is written down. */
  type GridPlace = {
    // A warehouse source: Live runs on it, Snap copies into `engine`, and the
    // snap lands under the source's own name so one model reads both.
    readonly live?: WarehouseEngine;
    readonly snapTarget?: SnapTarget;
    /** A grid can be saved: its source, by identity (never its data). */
    readonly cubeSource?: CubeSource;
    /** The groups a saved cube had open. */
    readonly tree?: TreeState;
    /** The rows are a copy in this tab already: an opened file, generated rows. */
    readonly heldCopy?: HeldCopy;
    /** Where its queries run when not on the tab's engine as it is: a saved query's version, which brings its own rows. */
    readonly engine?: QueryEngine;
    /** Its own planner, over its own model (a page's grids share no model); the page's planner when none. */
    readonly planner?: Planner;
    /** What its header says it reads. */
    readonly label?: string;
    /**
     * The file it reads, the handle it was picked through, and the table it was read into in this tab: the page keeps
     * them by the grid's id, and drops the table once no grid on the page reads it.
     */
    readonly file?: { readonly file: File; readonly handle?: FileHandle; readonly table: string };
    /** Its columns panel folded at the start: a grid added beside others; open, one opened alone. */
    readonly foldPanel?: boolean;
  };

  /**
   * A GRID ON THE PAGE, as DataCube's app makes one (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §6): a compact cube in a
   * tile, its windows floating over the whole page, wired to the page by `spawned` (its charts, copies, removal,
   * export, settings). Every grid is made here, the first as the fifth.
   */
  function gridMaker(
    snap: CubeSnapshot,
    config: CubeConfiguration,
    dims: { name: string; columns: string[] }[],
    place: GridPlace = {},
  ): GridMaker {
    // `start`: a copy of a grid of this source (Copy of Grid), starting where that grid is now
    return (gridHost, spawned, start) => {
      let printed = 0;
      const created: CubeApp = new CubeApp(gridHost, start?.snapshot ?? snap, {
        engine: place.engine ?? engine,
        planner: place.planner ?? planner,
        // Where the rows are (Live on a warehouse, or Snapped in this tab) is the grid's plane
        // button, and who read them the status bar's receipt.
        ...(place.live ? { live: place.live } : {}),
        configuration: start?.configuration ?? config,
        // Snap only where the place says what to copy: no other source's target stands in for it
        ...(place.snapTarget ? { snapTarget: place.snapTarget } : {}),
        ...(place.cubeSource ? { cubeSource: place.cubeSource } : {}),
        // the groups a saved cube had open: its own grid's, not a copy's (a copy starts collapsed, as a new grid)
        ...(place.tree && !start ? { tree: place.tree } : {}),
        ...(place.heldCopy ? { heldCopy: place.heldCopy } : {}),
        ...(place.label !== undefined ? { sourceLabel: place.label } : {}),
        compact: true,
        // a copy is a grid added beside others
        foldPanel: start !== undefined || place.foldPanel === true,
        // WHERE THE PLANNER RUNS, changed where it is read: the status bar's readout offers the planes
        hostMenu: () => planeMenu(),
        onHostMenu: (item) => onHostItem(item),
        // its windows float over the page, not squeezed into its tile
        windowHost: page.root,
        // "Changed since saved" is re-read on every change of the cube's state,
        // a presentation change (a width, a colour) included: it runs no query.
        onChange: () => {
          harnessSignal.changes += 1;
          onCubeView?.();
        },
        showColumnZone: true,
        dimensions: dims,
        writeClipboard: (text) => navigator.clipboard?.writeText(text),
        // Settings kept between visits, as upstream's hosts keep them
        // (settingsData.values / onSettingsChanged); the page keeps them for every grid.
        ...(storedSettings() ? { settings: storedSettings() as Record<string, unknown> } : {}),
        download,
        onStatus: (text, kind) => {
          // ERRORS ONLY. The cube's own status bar carries the result
          // and the timing; a host that echoed the same line beside it
          // said one fact twice in a 20px strip. What a host is for is
          // saying what the cube cannot -- a planner that failed.
          if (kind !== 'error') return;
          harnessSignal.changes += 1;
          status.textContent = text;
          status.classList.add('bad');
          status.classList.remove('warn-text');
        },
        onView: (view) => {
          // For the browser harness: how many views have landed.
          const w = window as unknown as { __dataCubeViews?: number };
          w.__dataCubeViews = (w.__dataCubeViews ?? 0) + 1;
          harnessSignal.changes += 1;
          // A VIEW LANDED, SO THE LAST ERROR IS OVER: a status line says what is true NOW.
          if (status.classList.contains('bad')) {
            status.classList.remove('bad');
            status.textContent = label;
          }
          // The query this product built, as the compiler prints it, and
          // the SQL the planner made of it. Both, because they answer
          // different questions -- the last view of any grid on the page.
          // The print is asked for; a later view's print wins.
          const printing = (printed += 1);
          harnessSignal.printing += 1;
          void created.controller.print(view.query, 'STANDARD').then(
            (text) => { if (printing === printed) must('pure').textContent = text; },
            (error: unknown) => { if (printing === printed) must('pure').textContent = String(error); },
          ).finally(() => {
            harnessSignal.printing -= 1;
            harnessSignal.changes += 1;
          });
          must('sql').textContent =
            view.sql || '(no SQL for this view)';
        },
        ...spawned,
      });
      if (spawned.id !== undefined) madeGrid?.(spawned.id, place);
      // a grid on the page: the start's reason for a blank one is over
      sayWhyBlank(undefined);
      return created;
    };
  }

  /** The host's own entries, from the page's menu or a grid's status bar. */
  function onHostItem(item: MenuItem): void {
    if (item.id === 'host.open') showCubes?.();
    if (item.id === 'host.save') saveCube?.(false);
    if (item.id === 'host.saveAs') saveCube?.(true);
    if (item.id === 'host.share') void copyShareLink?.();
    if (item.id === 'host.query') toggleHostWindow('querywin');
    // A NAVIGATION, not a switch: the same page loads again with the chosen ?planner= (a cube
    // never changes planner while it runs). Asked first when leaving would lose work here.
    if (PLANES.some((plane) => plane.id === item.id)) {
      if (!mayLeave(work, (q) => window.confirm(q), 'Choosing another planner reloads the page')) return;
      leaving = true;
      goToPlane(item.id);
    }
  }

  /** Hand a file to the user. */
  const download = (name: string, mime: string, text: string | Uint8Array): void => {
    const url = URL.createObjectURL(new Blob([typeof text === 'string' ? text : text.slice()], { type: mime }));
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  };

  /** Opens the saved cubes' window; set once the page can open files. */
  let showCubes: (() => void) | undefined;
  /** Close the Open… window, when it is open. */
  let closeCubes: (() => void) | undefined;
  /** Save the page (over the one it was opened from), or Save As a new one. */
  let saveCube: ((asNew: boolean) => void) | undefined;
  /** Copies the page's share link; set once the page can open files. */
  let copyShareLink: (() => Promise<void>) | undefined;
  /** Told when a view lands: "changed since saved" is re-read then. */
  let onCubeView: (() => void) | undefined;
  /** Told when a grid is made on the page, by its tile's id: what the page keeps of it (its file). */
  let madeGrid: ((id: string, place: GridPlace) => void) | undefined;
  /** Told of every change on the page: the tables no grid reads any more are dropped. */
  let dropUnread: (() => void) | undefined;
  /** The source picker, once the page can open sources: New ▸ Data Source… adds a grid over one; a blank page opens one. */
  let picker: ((purpose: 'add' | 'open', start?: SectionId) => Promise<GridMaker | undefined>) | undefined;
  /** New ▸ Blank Page: everything goes, for a first data source. */
  let blankPage: ((reason?: string) => void) | undefined;
  /** Why the page's start opened nothing (a link that failed, a key refused): the empty page says it. */
  let startProblem: string | undefined;
  /** The empty page's card: hidden until the start has settled -- a page still signing in, or opening a link, is not blank. */
  let blankCard: HTMLElement | undefined;
  /** Where the card says why the start opened nothing: after its lead. */
  let blankLead: HTMLElement | undefined;
  /** The card's line saying why, while it says it. */
  let blankWhy: HTMLElement | undefined;
  /** The empty page's card says why the start opened nothing, or -- undefined -- says nothing of it. */
  const sayWhyBlank = (reason: string | undefined): void => {
    blankWhy?.remove();
    blankWhy = undefined;
    if (!reason || !blankLead) return;
    const why = document.createElement('p');
    why.className = 'dc-blank-reason';
    why.setAttribute('role', 'alert');
    why.textContent = reason;
    blankLead.after(why);
    blankWhy = why;
  };

  /**
   * THE PAGE (src/page/page-app.ts): its bar -- the page's menu, its name, its fold -- and its grids.
   * The page's menu carries the host's entries: Save, Open, Share, the generated Pure and SQL, the planner planes.
   */
  const page = new PageApp({
    host,
    hostMenu: () => [
      // ONLY IF THE PAGE CAN OPEN FILES. Without `models` the
      // bar's controls are inert -- this page's planner compiles a
      // fixed model -- and an entry that opens a panel of dead
      // controls is the dead-button fault one layer up.
      ...(models
        ? [
          // saved pages: in this browser, over the sources they were built on
          { id: 'host.save' as const, label: 'Save', section: 'file' as const },
          { id: 'host.saveAs' as const, label: 'Save As\u2026', section: 'file' as const },
          { id: 'host.open' as const, label: 'Open\u2026', section: 'file' as const },
          // the page's settings in a link: never a row of data (src/share/link.ts)
          { id: 'host.share' as const, label: 'Share\u2026', section: 'file' as const },
        ]
        : []),
      { id: 'host.query', label: 'Generated Pure & SQL\u2026', section: 'view' as const },
      // The planes, as entries rather than a control: the bar is
      // for what you watch, the menu for what you do occasionally.
      // The one you are ON is disabled rather than hidden, so the
      // menu still says where the work happens.
      ...planeMenu(),
    ],
    onHostMenu: (item) => onHostItem(item),
    // THE HOST'S TEXT, IN THE STATUS BAR (planner progress, errors, the planner's word): the page's first grid's, at the
    // right of its status bar -- a readout belongs there, not in the bar that says what is on screen. MOVED rather than
    // copied, on each render of that bar: `status` is the same node the planner writes to.
    hostStatus: (slot) => slot.append(status),
    // THE EMPTY PAGE (New ▸ Blank Page, the last grid removed, a start that opened nothing): what to do first -- a
    // data source (the picker, opening in place) or a saved page
    empty: (slot) => {
      const doc = document;
      const blank = doc.createElement('div');
      blank.className = 'dc-blank';
      const card = doc.createElement('div');
      card.className = 'dc-blank-card';
      const title = doc.createElement('h2');
      title.className = 'dc-blank-title';
      title.textContent = 'A blank page';
      const lead = doc.createElement('p');
      lead.className = 'dc-blank-lead';
      lead.textContent = models
        ? 'Start with a data source: a file from your computer, an example, a table in a warehouse, or a remote Parquet, CSV or Iceberg file.'
        : 'This planner opens only the sample.';
      blankLead = lead;
      card.append(title, lead);
      if (models) {
        const actions = doc.createElement('div');
        actions.className = 'dc-blank-actions';
        const add = doc.createElement('button');
        add.type = 'button';
        add.className = 'dc-picker-button dc-primary';
        add.textContent = 'Add a data source';
        add.addEventListener('click', () => void picker?.('open'));
        const saved = doc.createElement('button');
        saved.type = 'button';
        saved.className = 'dc-picker-button dc-quiet';
        saved.textContent = 'Open a saved cube';
        saved.addEventListener('click', () => showCubes?.());
        actions.append(add, saved);
        card.append(actions);
      }
      blank.append(card);
      blank.hidden = true;
      blankCard = blank;
      slot.append(blank);
    },
    ...(models ? {
      // New ▸ Data Source…: a grid over another source, through the picker, beside the others
      openSource: () => picker?.('add') ?? Promise.resolve(undefined),
      onBlankPage: () => blankPage?.(),
    } : {}),
    onChange: () => {
      harnessSignal.changes += 1;
      dropUnread?.();
      onCubeView?.();
    },
    onSettingsChanged: (values) => {
      try {
        window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(values));
      } catch {
        // storage refused (private window, quota): the settings hold
        // for this visit
      }
    },
    download: (name, mime, text) => download(name, mime, text),
  });
  // For the browser harness ONLY: the page, its views and its layout; and its first grid (a CubeApp), so a check can
  // read that grid's own configuration and snapshot when what it sees on screen disagrees. Not product code.
  (window as unknown as { __dataPage?: PageApp }).__dataPage = page;
  Object.defineProperty(window, '__dataCube', {
    configurable: true,
    get: () => (page.grids[0] !== undefined ? page.grid(page.grids[0]) : undefined),
  });

  if (start.kind === 'sample') {
    page.addGrid(gridMaker(await sampleSnapshot(), configuration, DEMO_DIMENSIONS,
      { snapTarget, ...(generated ? { heldCopy: generated } : {}) }));
    await page.ready();
  }

  // OPENING A FILE.
  //
  // DuckDB reads it and sniffs the schema, legend-lite's writer (`tables.tableModel`) writes the
  // Pure model from what its catalog found, and the cube is rebuilt against that.
  // Nothing downstream learns the data was uploaded: the planner
  // compiles an ordinary model over an ordinary table, which is why
  // the SQL panel, the tree and the snap plane all keep working
  // without a second code path.
  if (models) {

    // Narrowed once: the check above does not reach into a function.
    const local = models;

    // A WAREHOUSE (the picker's Database section): the development sign-in, the warehouse's own users.
    /** The cubes live on a warehouse: renewed when the same user signs in again (P2-297). */
    const liveEngines = new Set<WarehouseEngine>();
    const track = (live: WarehouseEngine): WarehouseEngine => {
      liveEngines.add(live);
      return live;
    };
    work.add(() => (signedIn ? `the warehouse session (${signedIn.session.principal})` : undefined));
    // Which warehouse to offer: the deployment's (config.json, ?warehouse=), else the last one this
    // browser signed in to -- a convenience kept in this browser only; storage may be refused.
    const REMEMBERED = 'datacube.warehouse.url';
    const rememberedWarehouse = (): string => {
      try {
        return window.localStorage.getItem(REMEMBERED) ?? '';
      } catch {
        return '';
      }
    };

    /**
     * A warehouse table's model: its own runtime (the catalog's database type), and the runtime its
     * Snap -- a copy into this tab's engine -- is planned against (the tab engine's type).
     */
    const warehouseModel = (o: CatalogObject): Promise<InferredModel & { readonly snapRuntime: string }> => tables.tableModel(o.columns.map((c) => ({ ...c, dataType: c.type })),
      { table: o.name, schema: o.schema, convertible: false, databaseType: o.databaseType, snapDatabaseType: engine.databaseType });
    /**
     * A warehouse table's Snap: the rows pulled with the live plan, into a table of the same name in
     * this tab, and every query on the copy planned by the SAME model against the snap runtime.
     */
    const snapOf = (o: CatalogObject, m: InferredModel & { readonly snapRuntime: string }): { snapTarget: SnapTarget } => ({
      snapTarget: {
        schema: o.schema, table: o.name, source: m.source, conversions: m.conversions,
        planner: local.another(m.model, m.snapRuntime, { bitColumns: m.bitColumns }),
      },
    });

    /**
     * A SOURCE OPENED FOR A GRID: what makes the grid -- its own planner over its own model, its source written down so
     * the page can be saved -- and, a saved cube rebuilt over it, what opening it left out.
     */
    type Opened = {
      readonly make: GridMaker;
      readonly label: string;
      readonly notes: readonly string[];
      /** An overtaken open (a newer one went ahead): what it read into this tab, let go. */
      readonly discard?: () => Promise<void>;
    };

    /**
     * A saved cube rebuilt over a source (`relation`, its `columns` as the compiler types them now) -- its view, its
     * configuration, the groups it had open, what it had that the source no longer has -- or a fresh one: its rows as
     * they are, grouped by nothing; `capped` (a source added beside others), up to the row limit (the user,
     * 2026-10-01: "showing the max rows configured (1000 default)").
     */
    const rebuilt = (saved: CubeDocument | undefined, relation: ValueSpecification, columns: CubeSnapshot['columns'],
      label: string, capped: boolean): { snapshot: CubeSnapshot; configuration: CubeConfiguration; tree?: TreeState; notes: readonly string[] } => {
      if (!saved) {
        return {
          snapshot: rawRows(relation, columns),
          configuration: { ...DEFAULT_CONFIGURATION, reportTitle: label, ...(capped ? { maxRows: DEFAULT_MAX_ROWS } : {}) },
          notes: [],
        };
      }
      const cube = openCube(saved, { query: relation }, columns);
      return { snapshot: cube.snapshot, configuration: cube.configuration, ...(cube.tree ? { tree: cube.tree } : {}), notes: cube.notes };
    };

    /** A warehouse table for a grid: Live there as the user, Snap into this tab; its own planner over its model. */
    async function openedTable(signedIn: WarehouseSession, chosen: CatalogObject, saved?: CubeDocument, capped = false): Promise<Opened> {
      // A warehouse table is read-only: a column the compiler says must be
      // converted to be declared cannot be, so it is left out, and named.
      const m = await warehouseModel(chosen);
      const own = local.another(m.model, m.runtime, { bitColumns: m.bitColumns });
      const columns = await sourceColumns(own, m.source);
      const live = track(new WarehouseEngine(signedIn, chosen.catalog));
      const name = `${chosen.schema}.${chosen.name}`;
      // where it is, never the sign-in: whoever reopens it signs in as themselves
      const cubeSource: WarehouseSource = {
        _type: 'warehouseTable', name, warehouse: signedIn.baseUrl, catalog: chosen.catalog, schema: chosen.schema, table: chosen.name,
        columns: columns.map((c) => ({ name: c.name, type: c.type })),
      };
      const r = rebuilt(saved, m.source, columns, name, capped);
      return {
        label: name,
        notes: r.notes,
        make: gridMaker(r.snapshot, r.configuration, [], {
          planner: own, live, ...snapOf(chosen, m), cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
        }),
      };
    }

    // THE PAGE ON SCREEN, as a saved page sees it: the saved page it came from (a Save then saves over it), its name,
    // what a newer writer put in it (written back as it was), the definition it had when saved or first opened
    // ("changed since saved" compares to it), and what opening left out of it. And each grid's file (by the grid's
    // id): the File itself, so a page saved over the same file reopens without asking, and the handle the browser gave
    // for it, to reopen it from where it was picked.
    let library: CubeLibrary | undefined;
    let current: {
      cubeId?: string;
      name?: string;
      /** Fields of the saved PAGE this reader does not know, and of each of its cubes, written back as they were. */
      pageUnknown?: Readonly<Record<string, unknown>>;
      cubeUnknown?: ReadonlyMap<string, Readonly<Record<string, unknown>>>;
      /** The definition as saved, or as first opened: "changed since saved" compares to it. */
      baseline?: string;
      /** What opening left out of the saved page (a file changed, a source not opened): saving over it loses them. */
      lost?: readonly string[];
    } = {};
    /**
     * The tables this page's grids read, each its own: none may replace another's. The demo's generated (or mounted)
     * `trades` is the sample grid's from the start.
     */
    const taken = new Set<string>(['trades']);
    const freshTable = (base: string): string => {
      const busy = (t: string): boolean => taken.has(t);
      let name = base;
      for (let n = 2; busy(name); n += 1) name = `${base}_${n}`;
      taken.add(name);
      return name;
    };
    /**
     * Each grid's file, by the grid's id: the File, the handle it was picked through, the table it was read into, its
     * fingerprint, a sample's. Read only for the grids on the page now (`page.cubes`).
     */
    const files = new Map<string, {
      readonly file: File;
      readonly handle?: FileHandle;
      readonly table: string;
      readonly sha256: string;
      readonly sample: boolean;
    }>();
    /** The files on the page now: one per grid that reads one (copies of a grid read the same). */
    const filesNow = (): { readonly grid: string; readonly file: File; readonly handle?: FileHandle; readonly table: string; readonly sha256: string; readonly sample: boolean }[] =>
      page.cubes.flatMap((grid) => {
        const f = files.get(grid);
        return f ? [{ grid, ...f }] : [];
      });
    /** The tables files were read into for grids, by table: the file's name, to drop it by. */
    const held = new Map<string, string>();
    madeGrid = (id, place) => {
      const src = place.cubeSource;
      if (place.file && src?._type === 'file') {
        files.set(id, { ...place.file, sha256: src.sha256, sample: src.sample !== undefined });
        held.set(place.file.table, place.file.file.name);
      }
    };
    /**
     * A FILE'S TABLE GOES WITH ITS LAST GRID: once no grid on the page (or kept off it for a detached chart) reads it,
     * it is dropped from the tab's database and its name is free again -- a file opened three times in place holds one
     * table, not three. Told of every change on the page.
     */
    dropUnread = () => {
      const read = new Set(filesNow().map((f) => f.table));
      const live = new Set(page.cubes);
      for (const grid of [...files.keys()]) if (!live.has(grid)) files.delete(grid);
      for (const [table, fileName] of [...held]) {
        if (read.has(table)) continue;
        held.delete(table);
        void forgetUpload(engine, db, fileName, table).catch(() => {}).finally(() => taken.delete(table));
      }
    };

    /**
     * What saving now would write -- always ONE kind of thing, the page (page-document.ts): one cube per grid, its
     * charts and layout -- and its definition, for "changed since saved".
     */
    const savedForm = (name: string): { content: Record<string, unknown>; definition: string } | undefined => {
      const doc = page.document(name, {
        ...(current.pageUnknown ? { page: current.pageUnknown } : {}),
        ...(current.cubeUnknown ? { cubes: current.cubeUnknown } : {}),
      });
      return doc && { content: pageContent(doc), definition: pageDefinitionText(doc) };
    };

    /** The page on screen differs from what was saved (or first opened). */
    const dirty = (): boolean => {
      if (current.baseline === undefined) return false;
      if ((current.lost?.length ?? 0) > 0) return true;
      const now = savedForm(current.name ?? (page.title || 'page'));
      return now !== undefined && now.definition !== current.baseline;
    };
    const baseTitle = document.title;
    onCubeView = () => {
      const changed = dirty();
      document.title = page.empty
        ? baseTitle
        : `${changed ? '\u2022 ' : ''}${current.name ?? (page.title || 'page')} \u2013 ${baseTitle}`;
      library?.sync();
    };
    // Leaving the page with unsaved changes asks, the browser's way.
    work.add(() => (dirty() ? 'unsaved changes' : undefined));
    work.add(() => {
      const opened = [...new Set(filesNow().filter((f) => !f.sample).map((f) => f.file.name))];
      return opened.length > 0 ? `the file${opened.length === 1 ? '' : 's'} opened in this tab (${opened.join(', ')})` : undefined;
    });
    window.addEventListener('beforeunload', (event) => {
      if (leaving || work.what() === undefined) return;
      event.preventDefault();
      event.returnValue = '';
    });

    /** LATEST WINS: each open in place takes a number, and one overtaken by a newer open lands nothing (P2-330). */
    const opens = new Latest();

    /**
     * A FILE FOR A GRID, read into this tab under a table of its own (no grid's table is ever replaced by another's):
     * a fresh grid, or -- `saved` -- a saved cube reconciled with what the file holds NOW.
     */
    async function openedFile(
      file: File,
      how: {
        readonly handle?: FileHandle;
        readonly sample?: { readonly id: string; readonly rows: number };
        readonly saved?: CubeDocument;
        readonly capped?: boolean;
      } = {},
    ): Promise<Opened> {
      try {
        const table = freshTable(tableNameOf(file.name));
        const opened = await ingestFile(engine, db, file, tables, { table });
        const loadedAt = new Date();
        const own = local.another(opened.model, opened.runtime, { bitColumns: opened.bitColumns });
        const columns = await sourceColumns(own, opened.source);
        const source = await fileSource(file, formatOf(file.name), columns, how.sample);
        const saved = how.saved;
        const r = rebuilt(saved, opened.source, columns, opened.fileName, how.capped === true);
        const notes = saved
          ? [
            ...(saved.source._type !== 'file' || source.sha256 !== saved.source.sha256
              ? [`${file.name} is not the file this cube was saved over (its contents differ)`]
              : []),
            ...r.notes,
          ]
          : [];
        return {
          label: opened.fileName,
          notes,
          // never made into a grid: its table goes, and its name is free again
          discard: () => forgetUpload(engine, db, file.name, table).catch(() => {}).finally(() => taken.delete(table)),
          make: gridMaker(r.snapshot, r.configuration, [], {
            planner: own,
            cubeSource: source,
            heldCopy: { label: opened.fileName, takenAt: loadedAt, rowCount: opened.rowCount },
            label: opened.fileName,
            ...(r.tree ? { tree: r.tree } : {}),
            file: { file, table, ...(how.handle ? { handle: how.handle } : {}) },
            foldPanel: how.capped === true,
          }),
        };
      } catch (e) {
        // Say what failed and about which file -- the one input the user can actually fix. Shown
        // where the open was asked: the source picker's window, or the saved pages'.
        throw new Error(`could not open ${file.name}: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
      }
    }

    /**
     * A SOURCE IN PLACE of the page: everything on it goes (asked first when there are unsaved changes), then one grid
     * over the source -- fresh, the page's name its. An open overtaken by a newer one lands nothing.
     */
    async function inPlace(open: () => Promise<Opened>, what: string): Promise<readonly string[]> {
      if (dirty() && !window.confirm(`The page has unsaved changes. Open ${what} anyway?`)) return [];
      const newest = opens.start();
      const o = await open();
      if (!newest()) {
        await o.discard?.();
        return [];
      }
      page.clear();
      current = {};
      // the page has no name of its own until saved: its bar says the grid's report title (its source's name)
      page.setTitle('');
      page.addGrid(o.make);
      await page.ready();
      // overtaken while it landed: the newer open's page is the one to keep a baseline of
      if (!newest()) return [];
      // The baseline is the page as it LANDED (normalized by its first refresh)
      const landed = savedForm(page.title || 'page');
      current = { ...(landed ? { baseline: landed.definition } : {}), lost: [] };
      onCubeView?.();
      library?.sync();
      return o.notes;
    }

    // SAVED CUBES, in this browser: IndexedDB when the browser gives it, else memory (this
    // visit only, and said so). One database holds the cubes and their files' handles.
    let store: CubeStore;
    let handles: FileHandles | undefined;
    let persistent = false;
    /** The browser gave no database: saved cubes last only as long as this visit. */
    let memoryOnly = false;
    try {
      const database = openCubeDatabase();
      await database;
      store = new RuleStore(new BrowserRecords(database), 'this browser');
      handles = new FileHandles(database);
    } catch {
      store = new RuleStore(new MemoryRecords(), 'this browser');
      memoryOnly = true;
    }

    /**
     * The file a saved cube needs, got the least intrusive way that works: a sample is
     * rebuilt; the file already open is reused when it IS that file; a kept handle is read
     * (asking for the browser's permission takes a click, so the window offers one);
     * otherwise the user is asked for it.
     */
    async function fileFor(
      doc: FileCube,
      keys: readonly string[],
    ): Promise<{ file: File; handle?: FileHandle } | undefined> {
      const src = doc.source;
      if (src.sample) {
        const s = sampleById(src.sample.id);
        if (s) {
          return { file: new File([s.build(src.sample.rows)], src.name, { type: mimeOf(s) }) };
        }
      }
      const open = [...files.values()].find((f) => f.sha256 === src.sha256);
      if (open) return { file: open.file, ...(open.handle ? { handle: open.handle } : {}) };
      let kept: FileHandle | undefined;
      for (const key of keys) kept ??= await handles?.get(key);
      if (kept) {
        const read = await readHandle(kept, false);
        if (read.state === 'file') return { file: read.file, handle: kept };
        if (read.state === 'needs-click') {
          return new Promise((resolve) => {
            const button = document.createElement('button');
            button.type = 'button';
            button.textContent = `Open ${kept.name}`;
            button.addEventListener('click', () => {
              void readHandle(kept, true).then(async (again) => {
                library?.ask(undefined);
                resolve(again.state === 'file' ? { file: again.file, handle: kept } : await chooseFile(doc));
              });
            });
            library?.ask([`"${doc.name}" reads ${kept.name} from where you picked it. The browser wants you to allow it:`, button]);
          });
        }
      }
      return chooseFile(doc);
    }

    /** Ask the user for the file, naming the one the cube was saved over. */
    function chooseFile(doc: FileCube): Promise<{ file: File; handle?: FileHandle } | undefined> {
      return new Promise((resolve) => {
        const src = doc.source;
        const choose = document.createElement('button');
        choose.type = 'button';
        choose.textContent = 'Choose file…';
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.csv,.parquet,.json,.jsonl,.ndjson';
        input.hidden = true;
        input.className = 'dc-lib-choose';
        input.addEventListener('change', () => {
          const file = input.files?.[0];
          library?.ask(undefined);
          resolve(file ? { file } : undefined);
        });
        choose.addEventListener('click', () => {
          if (!canKeepHandles()) {
            input.click();
            return;
          }
          void pickDataFile().then((picked) => {
            library?.ask(undefined);
            resolve(picked);
          });
        });
        const cancel = document.createElement('button');
        cancel.type = 'button';
        cancel.textContent = 'Cancel';
        cancel.addEventListener('click', () => {
          library?.ask(undefined);
          resolve(undefined);
        });
        library?.ask([
          `"${doc.name}" was built over ${src.name} (${bytes(src.size)}). Choose that file:`,
          choose, cancel, input,
        ]);
      });
    }

    /**
     * Where a saved page keeps the handle of the file a grid reads: by the page and the grid -- or, saved before a page
     * held several grids, by the page alone.
     */
    const handleKeys = (pageId: string | undefined, grid: string): string[] => (pageId === undefined ? [] : [`${pageId}#${grid}`, pageId]);

    /** A cube saved alone, before pages: a page of its one grid, the whole board. */
    const pageOf = (cube: CubeDocument): PageDocument => ({
      kind: 'datacube.page',
      version: 2,
      name: cube.name,
      cubes: [{ id: 'cube', cube }],
      views: [{ id: 'grid', kind: 'grid', cube: 'cube' }],
      layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid' } }] },
    });

    /**
     * A saved cube's source opened again, the least intrusive way that works (a file asked for, a saved query rerun, a
     * warehouse signed in to, a remote file's keys asked for): its grid's maker, or undefined when the person gave up.
     */
    async function reopened(doc: CubeDocument, keys: readonly string[], several: boolean): Promise<Opened | undefined> {
      const src = doc.source;
      // `several`: one of a page's grids, its columns panel folded as a grid added beside others is
      if (src._type === 'savedQuery') return openedSavedQuery(await openedRecord(await pageConfig(), src.query), doc, several);
      if (src._type === 'warehouseTable') return reopenTable(src, doc, several);
      if (src._type === 'remoteFile') return reopenRemote(src, doc, several);
      if (!isFileCube(doc)) throw new Error(`"${doc.name}" reads a source this page cannot open`);
      const got = await fileFor(doc, keys);
      if (!got) return undefined;
      return openedFile(got.file, {
        ...(got.handle ? { handle: got.handle } : {}),
        ...(doc.source.sample ? { sample: doc.source.sample } : {}),
        saved: doc,
        capped: several,
      });
    }

    /**
     * OPEN A SAVED PAGE (from the store, a file someone handed over, a share link): each of its cubes through its own
     * source, in turn, then the page laid out as it was saved -- its grids under their saved ids, its charts, its
     * layout. A cube whose source was not opened is left out, and said; saving over the page would lose it.
     */
    async function openSaved(saved: SavedDocument, id: string | undefined): Promise<void> {
      const doc = saved.kind === 'page' ? saved.page : pageOf(saved.cube);
      if (dirty() && !window.confirm(`The page has unsaved changes. Open "${doc.name}" anyway?`)) return;
      // LATEST WINS (P2-330), as an open in place: each source is a wait (a file asked for, a sign-in), and an open
      // overtaken meanwhile lands nothing -- what it read is dropped
      const newest = opens.start();
      const makers = new Map<string, GridMaker>();
      const read: Opened[] = [];
      const notes: string[] = [];
      for (const { id: cubeId, cube } of doc.cubes) {
        const o = await reopened(cube, handleKeys(id, gridOfCube(doc, cubeId)), doc.cubes.length > 1);
        if (!newest()) {
          for (const r of [...read, ...(o ? [o] : [])]) await r.discard?.();
          return;
        }
        if (!o) {
          notes.push(`left out: ${cube.source.name} (not opened)`);
          continue;
        }
        read.push(o);
        makers.set(cubeId, o.make);
        notes.push(...o.notes);
      }
      if (makers.size === 0) {
        library?.say(`not opened: "${doc.name}" -- none of its sources was opened`, 'warn');
        return;
      }
      page.restore(doc, makers);
      page.setTitle(doc.name);
      await page.ready();
      // overtaken while it landed: the newer open's page is the one to keep
      if (!newest()) return;
      // each file the page reads, where it was picked: kept for the next time it is opened
      if (id !== undefined) await keepHandles(id);
      current = {
        ...(id !== undefined ? { cubeId: id } : {}),
        name: doc.name,
        ...(doc.unknown ? { pageUnknown: doc.unknown } : {}),
        cubeUnknown: new Map(doc.cubes.filter((c) => c.cube.unknown).map((c) => [gridOfCube(doc, c.id), c.cube.unknown!])),
      };
      // The baseline is the page as it LANDED (normalized by its first refresh); a page opened with parts left out is
      // changed from the start.
      const landed = savedForm(doc.name);
      current = { ...current, ...(landed ? { baseline: landed.definition } : {}), lost: notes.filter((n) => n.startsWith('left out')) };
      onCubeView?.();
      library?.sync();
      library?.say(notes.length === 0
        ? `opened "${doc.name}"`
        : `opened "${doc.name}", with changes since it was saved:\n${notes.map((n) => `- ${n}`).join('\n')}`,
      notes.length === 0 ? 'ok' : 'warn');
      // opened cleanly: the window goes, the page is what the person wanted to see
      if (notes.length === 0) closeCubes?.();
    }

    /**
     * Each grid's file, where it was picked: kept by the page and the grid (a grid kept off the board for a detached
     * chart included), so opening the page again reads it from there.
     */
    async function keepHandles(pageId: string): Promise<void> {
      for (const f of filesNow()) if (f.handle) await handles?.put(`${pageId}#${f.grid}`, f.handle);
    }

    /** The grid a saved page's cube is shown in (its grid view's id), or the cube's own id (a detached chart's). */
    const gridOfCube = (doc: PageDocument, cube: string): string =>
      doc.views.find((v) => v.kind === 'grid' && v.cube === cube)?.id ?? cube;

    /** Save the page on screen: over its saved copy, or as a new one (the Save window's act). */
    async function saveTo(name: string, asNew: boolean): Promise<void> {
      const refused = page.saveRefusal();
      if (refused) throw new Error(refused);
      const form = savedForm(name);
      if (!form) throw new Error('This page cannot be saved yet: a grid on it does not know its source.');
      const id = !asNew && current.cubeId !== undefined ? current.cubeId : crypto.randomUUID();
      const record = { id, name, content: form.content };
      if (id === current.cubeId) await store.update(id, record);
      else await store.create(record);
      await keepHandles(id);
      current = { ...current, cubeId: id, name, baseline: form.definition, lost: [] };
      page.setTitle(name);
      onCubeView?.();
      if (!persistent) persistent = await persistStorage();
      library?.sync();
    }

    library = new CubeLibrary(must('cubelib'), store, {
      currentId: () => current.cubeId,
      dirty,
      open: async (id) => openSaved(readSaved((await store.get(id)).content), id),
      openText: async (text) => openSaved(readSaved(text), undefined),
      forget: async (id) => {
        // every handle the page kept, whichever page is on screen
        await handles?.removePage(id);
        if (current.cubeId === id) {
          const { cubeId: _gone, ...rest } = current;
          current = rest;
        }
      },
    });
    // OPEN… (the user, 2026-10-01: "fix the Open dialog now too"): the saved cubes in a window of
    // their own, as the source picker and Save are -- searched, sorted, opened, deleted, or a cube
    // file opened. It closes once a cube opens cleanly; what an open has to say (a file to choose,
    // changes since it was saved) keeps it open.
    showCubes = () => {
      if (!closeCubes) {
        const backdrop = document.createElement('div');
        backdrop.id = 'cubeswin';
        backdrop.className = 'dc-picker-backdrop';
        const win = document.createElement('div');
        win.className = 'dc-picker dc-open dc-app-floating';
        win.setAttribute('role', 'dialog');
        win.setAttribute('aria-modal', 'true');
        win.setAttribute('aria-labelledby', 'dc-open-title');
        const head = document.createElement('div');
        head.className = 'dc-picker-head';
        const titles = document.createElement('div');
        titles.className = 'dc-picker-titles';
        const title = document.createElement('h2');
        title.className = 'dc-picker-title';
        title.id = 'dc-open-title';
        title.textContent = 'Open a saved cube';
        const sub = document.createElement('p');
        sub.className = 'dc-picker-subtitle';
        sub.textContent = memoryOnly
          ? 'Kept for this visit only (this browser refused storage). Each reopens over its own file, or regenerates its example.'
          : 'Saved in this browser. Each reopens over its own file (asked for when needed), or regenerates its example.';
        titles.append(title, sub);
        const close = document.createElement('button');
        close.type = 'button';
        close.className = 'dc-picker-close';
        close.setAttribute('aria-label', 'Close');
        close.textContent = '\u00d7';
        head.append(titles, close);
        const body = document.createElement('div');
        body.className = 'dc-open-body';
        body.append(must('cubelib'));
        win.append(head, body);
        backdrop.append(win);
        document.body.append(backdrop);
        const onKey = (e: KeyboardEvent): void => {
          if (e.key === 'Escape') { e.preventDefault(); closeCubes?.(); }
        };
        document.addEventListener('keydown', onKey, true);
        closeCubes = () => {
          must('cubeholder').append(must('cubelib'));
          document.removeEventListener('keydown', onKey, true);
          backdrop.remove();
          closeCubes = undefined;
        };
        close.addEventListener('click', () => closeCubes?.());
        backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop) closeCubes?.(); });
      }
      library?.sync();
      void library?.refresh();
      (document.querySelector('#cubelib .dc-lib-search') as HTMLElement | null)?.focus();
    };
    // THE MENU'S SAVE AND SAVE AS: a window of their own (src/ui/save-dialog.ts) -- the name, where
    // it is kept, what is saved and what is not (the rows), and what saving over would drop
    /** What each grid's source needs to be read again, in words, once per kind of source on the page. */
    const sourcesSaid = (doc: PageDocument | undefined, how: 'save' | 'share'): string => {
      const said = new Set<string>();
      for (const { cube } of doc?.cubes ?? []) {
        const src = cube.source;
        said.add(how === 'save'
          ? src._type === 'savedQuery' ? `opening it runs the saved query “${src.name}” again`
            : src._type === 'warehouseTable' ? `opening it reads ${src.name} on the warehouse again, signed in as whoever opens it`
              : src._type === 'remoteFile' ? `opening it reads ${src.name} again from its URL`
                : src.sample ? `the example (${src.sample.rows.toLocaleString(UI_LOCALE)} rows) is generated again when it opens`
                  : `opening it reads ${src.name} again, from your computer`
          : src._type === 'savedQuery' ? `it reads the saved query “${src.name}” again, where its project (${src.query.groupId}:${src.query.artifactId}) is known`
            : src._type === 'warehouseTable' ? `whoever opens it signs in to the warehouse as themselves and needs to be granted ${src.name}; no sign-in is in the link`
              : src._type === 'remoteFile' ? `it reads ${src.url} again; no keys are in the link, a private bucket asks for them`
                : src.sample ? 'it rebuilds its sample on its own'
                  : `whoever opens it needs ${src.name}`);
      }
      return [...said].join('; ');
    };

    // THE MENU'S SAVE AND SAVE AS: a window of their own (src/ui/save-dialog.ts) -- the name, where
    // it is kept, what is saved and what is not (the rows), and what saving over would drop
    saveCube = (asNew) => {
      if (page.empty) {
        library?.say('There is nothing on this page to save: add a data source first.', 'warn');
        return;
      }
      void (async () => {
        const refused = page.saveRefusal();
        const offered = current.name ?? (page.title || 'page');
        const savedAt = current.cubeId !== undefined ? (await store.get(current.cubeId).catch(() => undefined))?.lastUpdatedAt : undefined;
        const doc = page.document(offered);
        const views = page.views().views;
        const charts = views.filter((v) => v.kind === 'chart').length;
        const grids = page.grids.map((id) => ({ title: views.find((v) => v.id === id && v.kind === 'grid')?.title ?? id, grid: page.grid(id)! }));
        const shape = (s: CubeSnapshot): string => [
          s.rows.length > 0 ? `grouped by ${s.rows.join(', ')}` : 'its rows as they are, grouped by nothing',
          ...(s.pivotOn.length > 0 ? [`pivoted on ${s.pivotOn.join(', ')}`] : []),
          ...(s.filter ? ['its filter'] : []),
          ...(s.derived.length + (s.groupDerived?.length ?? 0) > 0
            ? [`${s.derived.length + (s.groupDerived?.length ?? 0)} calculated column${s.derived.length + (s.groupDerived?.length ?? 0) === 1 ? '' : 's'}`] : []),
        ].join(', ');
        const keeps = [
          ...(grids.length === 1
            ? [shape(grids[0]!.grid.snapshot).replace(/^./, (c) => c.toUpperCase())]
            : grids.map(({ title, grid }) => `${title}: ${shape(grid.snapshot)}`)),
          ...(charts > 0 ? [`${charts} visualization${charts === 1 ? '' : 's'}, and the page's layout`] : []),
          grids.length > 1 ? 'Each grid\'s formats, widths, colours and settings' : 'Its formats, widths, colours and settings',
        ];
        const leaves = `Not the rows: ${sourcesSaid(doc, 'save') || 'opening it reads its sources again'}.`;
        await saveDialog(document, {
          purpose: asNew ? 'saveAs' : 'save',
          name: offered,
          ...(current.cubeId !== undefined
            ? { over: { name: current.name ?? offered, ...(savedAt !== undefined ? { savedAt } : {}) } } : {}),
          where: memoryOnly ? 'for this visit only (this browser refused storage)' : 'in this browser',
          keeps,
          leaves,
          ...(current.lost?.length
            ? { warning: `The saved “${current.name ?? 'page'}” has parts this page cannot show: ${current.lost.join('; ')}.` } : {}),
          save: async (name, saveAsNew) => {
            if (refused) throw new Error(refused);
            await saveTo(name, saveAsNew);
          },
        });
      })();
    };

    // THE SHARE LINK: the page's settings, never its data. It is said HOW LONG it is, and a long
    // one -- which some mail and chat tools cut -- is said so, with the file suggested instead.
    // SHARE: the link in a window of its own, shown and copied to the clipboard at once, with a
    // way to copy it again -- and what it holds (the page's settings, never its data) said there.
    copyShareLink = async () => {
      // nothing on screen is said the way any unsharable page is: in the window, not as a dead button
      const refused = page.saveRefusal();
      const name = current.name ?? (page.title || 'page');
      const shared = refused ? undefined : page.document(name, {
        ...(current.pageUnknown ? { page: current.pageUnknown } : {}),
        ...(current.cubeUnknown ? { cubes: current.cubeUnknown } : {}),
      });
      const field = must('sharelink') as HTMLTextAreaElement;
      const note = must('sharenote');
      const copy = must('sharecopy') as HTMLButtonElement;
      showHostWindow('sharewin', { width: 560, height: 200 });
      // nothing to copy: the window says why, without an empty link and a dead button
      field.hidden = !shared;
      copy.hidden = !shared;
      if (!shared) {
        field.value = '';
        note.textContent = refused ?? 'This page cannot be shared yet: a grid on it does not know its source.';
        note.className = 'sharenote bad';
        return;
      }
      const link = shareLink(location.href, shared);
      field.value = link.url;
      const about = `It holds the page's settings, filter values included, not its data; ${sourcesSaid(shared, 'share')}.`;
      const put = async (): Promise<void> => {
        field.select();
        try {
          await navigator.clipboard.writeText(link.url);
        } catch {
          note.textContent = `The browser did not allow copying: select the link and copy it. ${about}`;
          note.className = 'sharenote bad';
          return;
        }
        note.textContent = link.long
          ? `Copied, but it is long (${link.length.toLocaleString(UI_LOCALE)} characters): some mail and chat tools cut links this long. ${about}`
          : `Copied to the clipboard (${link.length.toLocaleString(UI_LOCALE)} characters). ${about}`;
        note.className = link.long ? 'sharenote warn' : 'sharenote';
      };
      copy.onclick = () => void put();
      await put();
    };

    // A sample is built without a row cap: the one hard limit is the browser's longest string
    // (about 512M characters, some millions of rows), and past it the build throws a RangeError --
    // said in plain words rather than guessed at with a ceiling here.
    const tooBig = (e: unknown, rows: number): string =>
      e instanceof RangeError
        ? `${rows.toLocaleString(UI_LOCALE)} rows is more than this tab can hold as one file -- try fewer`
        : e instanceof Error ? e.message : String(e);
    const mimeOf = (s: Sample): string =>
      s.format === 'jsonl' ? 'application/x-ndjson' : 'text/csv';

    // THE SOURCE PICKER (src/ui/source-picker.ts; the user, 2026-10-01): where the rows come from,
    // one window for every kind -- a file, an example, a warehouse table, a remote file. Each
    // choice is read INSIDE the window, so a refusal is shown there. "add" makes a grid over it
    // with ITS OWN planner over ITS OWN model (`another`), on this tab's DuckDB; "open" puts it
    // in place of the cube, as the Data window did.
    type Chosen =
      | { readonly kind: 'file'; readonly file: File; readonly handle?: FileHandle; readonly sample?: { readonly id: string; readonly rows: number } }
      | { readonly kind: 'table'; readonly session: WarehouseSession; readonly object: CatalogObject }
      | { readonly kind: 'remote'; readonly url: string; readonly s3?: S3Credentials }
      | { readonly kind: 'saved'; readonly query: OpenedQuery };
    /** The picker's warehouse sign-in, kept between openings of the window. */
    let signedIn: { readonly session: WarehouseSession; readonly objects: readonly CatalogObject[] } | undefined;
    const sampleFile = (id: string, rows: number): { file: File; sample: { id: string; rows: number } } => {
      const s = sampleById(id);
      if (!s) throw new Error(`no example '${id}'`);
      let text: string;
      try {
        text = s.build(rows);
      } catch (e) {
        throw new Error(tooBig(e, rows));
      }
      return { file: new File([text], sampleFileName(s), { type: mimeOf(s) }), sample: { id: s.id, rows } };
    };
    const asSession = (s: { readonly session: WarehouseSession; readonly objects: readonly CatalogObject[] }): DatabaseSession => {
      // the catalog is named only when the warehouse offers more than one
      const several = new Set(s.objects.map((o) => o.catalog)).size > 1;
      return {
        principal: s.session.principal,
        where: new URL(s.session.baseUrl).host,
        objects: s.objects.map((o) => ({
          ...(several ? { catalog: o.catalog } : {}), schema: o.schema, name: o.name, kind: o.kind, columns: o.columns.length,
        })),
      };
    };
    const lastSegment = (url: string): string => url.replace(/[?#].*$/, '').replace(/\/+$/, '').split('/').pop() || url;

    /** A remote file as a view of this tab's DuckDB, and the model written from its catalog. */
    async function mountedRemote(url: string, s3: S3Credentials | undefined, name: string) {
      await mountRemote(engine, { sources: [{ name, url }], ...(s3 ? { s3 } : {}) });
      // a view of a remote file cannot be rewritten: a column that needs a conversion is left out
      return tables.tableModel(await catalogColumns(engine, tables, name), { table: name, convertible: false, databaseType: engine.databaseType });
    }

    // A SAVED QUERY (the picker's Saved queries; the user, 2026-10-01: "load from a saved Query"):
    // read from the query store as upstream serves it, never from Query's browser storage, by the
    // rules Query writes it by (src/saved-queries.ts, tested against fixtures/saved-queries). Its
    // project (config.json `projects[]`) gives the model it compiles against and the rows it reads,
    // seeded into this tab's DuckDB once; the planner reads it `->from(mapping, runtime)`.
    type OpenedQuery = {
      readonly label: string;
      readonly model: string;
      readonly runtime: string;
      readonly how: ModelOptions;
      readonly source: ValueSpecification;
      readonly columns: CubeSnapshot['columns'];
      /** A planner over its model, the one that typed it. */
      readonly planner: Planner;
      /** The cube's source, as Save and Share write it down: the query itself (cube-document.ts). */
      readonly cubeSource: QuerySource;
      /** A project opened by name: the tab's engine with its version's own rows made there for each query (withTestData). */
      readonly engine?: QueryEngine;
    };
    const fetchText = async (url: string): Promise<string> => {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`${url} answered ${r.status}`);
      return r.text();
    };
    /** Each project's model text, fetched once. */
    const projectModels = new Map<string, Promise<string>>();
    /** Each configured project's rows (its seed), in this tab's DuckDB once. */
    const seeded = new Map<string, Promise<void>>();
    /** Where a project opened by name puts its test data in this tab (each query its cubes run makes its version's rows there). */
    let testData: TestData | undefined;
    /** The model home a depot names (the page's own SDLC and Depot, or servers), opened once. */
    let home: Promise<ModelHome> | undefined;
    // loaded only when a saved query's project is opened by name: the grid-only page's budget does not carry it
    const modelHome = (d: DepotConfig): Promise<ModelHome> => (home ??= import('../../depot-client/src/model-home.ts')
      .then(({ connectModelHome }) => connectModelHome({ sdlc: d.sdlc, vendor: d.vendor })));
    const once = <T,>(cache: Map<string, Promise<T>>, key: string, make: () => Promise<T>): Promise<T> => {
      let p = cache.get(key);
      if (!p) {
        p = make();
        cache.set(key, p);
        p.catch(() => cache.delete(key));
      }
      return p;
    };
    const projectFor = (config: PageConfig, q: Pick<Query, 'groupId' | 'artifactId' | 'versionId'>): ProjectConfig | undefined =>
      config.projects.find((p) => p.groupId === q.groupId && p.artifactId === q.artifactId && p.versionId === q.versionId);

    async function openedQuery(config: PageConfig, store: QueryReader, id: string): Promise<OpenedQuery> {
      return openedRecord(config, await store.get(id));
    }

    /** A saved query's record -- from a store, or as a share link carries it -- as a cube's source. */
    async function openedRecord(config: PageConfig, q: Pick<Query, 'name' | 'groupId' | 'artifactId' | 'versionId' | 'content' | 'executionContext' | 'defaultParameterValues'>): Promise<OpenedQuery> {
      const { contextOf, enumerationsOf, enumsAsStrings, projectOf, sourceOf } = await savedQueries();
      const project = projectFor(config, q);
      const byName = config.depot;
      if (!project && !byName) throw new Error(`“${q.name}” belongs to ${projectOf(q)}, which this page has no model for (config.json projects[], or a depot)`);
      const key = projectOf(q);
      // the page's own project (config.json projects[]), else the version opened by name from Depot (design Phase 3)
      const model = await once(projectModels, key, async () => (project
        ? (await Promise.all(project.models.map(fetchText))).join('\n')
        : (await import('../../depot-client/src/model-text.ts')).modelText((await modelHome(byName!)).depot, q.groupId, q.artifactId, q.versionId)));
      const elements = await local.elements(model) as ModelElement[];
      const context = contextOf(q, elements);
      const lambdaOf = await planner.parse(q.content);
      const values = new Map<string, ValueSpecification>();
      for (const v of q.defaultParameterValues ?? []) {
        const parsed = await planner.parse(`|${v.content}`);
        if (parsed.body[0]) values.set(v.name, parsed.body[0]);
      }
      let source = sourceOf(lambdaOf, values);
      // a project opened by name brings its own rows: its Data elements' tables, made and filled by the server's
      // statements (`seeds`; plan A2, model-data.ts), made the tab's for each query its cube runs -- another version,
      // filling the same tables, may be on the page beside it
      let versioned: QueryEngine | undefined;
      if (!project) {
        const { dataTables, seeded: seedTables, TestData } = await import('../../engine-client/src/model-data.ts');
        const { withTestData } = await import('../../engine-client/src/tab-data.ts');
        testData ??= new TestData({ run: (sql) => engine.run(sql, 0) });
        versioned = withTestData(engine, testData, key, await seedTables(seeds, model, dataTables(elements as Parameters<typeof dataTables>[0])));
      } else {
        await once(seeded, key, async () => {
          for (const url of project.seed) {
            for (const line of (await fetchText(url)).split('\n')) {
              const sql = line.trim();
              if (sql && !sql.startsWith('--')) await engine.run(sql, 0);
            }
          }
        });
      }
      const how: ModelOptions = { mapping: context.mapping, enumerations: [...enumerationsOf(elements)] };
      const own = local.another(model, context.runtime, how);
      // rule 4: an enumeration column is read as its value's name
      const named = new Set((await own.relationType(lambda([], source))).filter((c) => c.enumeration).map((c) => c.name));
      if (named.size > 0) source = enumsAsStrings(source, named);
      const columns = await sourceColumns(own, source);
      const { sharedPart } = await queryStores();
      const cubeSource: QuerySource = {
        _type: 'savedQuery', name: q.name, query: sharedPart(q), columns: columns.map((c) => ({ name: c.name, type: c.type })),
      };
      return { label: q.name, model, runtime: context.runtime, how, source, columns, planner: own, cubeSource, ...(versioned ? { engine: versioned } : {}) };
    }

    /** A saved query for a grid: its own planner over its project's model, its version's rows; the query written down. */
    function openedSavedQuery(o: OpenedQuery, saved?: CubeDocument, capped = false): Opened {
      const r = rebuilt(saved, o.source, o.columns, o.label, capped);
      return {
        label: o.label,
        notes: r.notes,
        make: gridMaker(r.snapshot, r.configuration, [], {
          planner: o.planner, cubeSource: o.cubeSource, label: o.label, ...(o.engine ? { engine: o.engine } : {}),
          ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped,
        }),
      };
    }

    /** A remote file for a grid, mounted as a view of its own; its keys are never written down. */
    async function openedRemote(url: string, s3: S3Credentials | undefined, saved?: CubeDocument, capped = false): Promise<Opened> {
      const m = await mountedRemote(url, s3, freshTable('remote'));
      const own = local.another(m.model, m.runtime, { bitColumns: m.bitColumns });
      const columns = await sourceColumns(own, m.source);
      const name = lastSegment(url);
      const cubeSource: RemoteSource = { _type: 'remoteFile', name, url, columns: columns.map((c) => ({ name: c.name, type: c.type })) };
      const r = rebuilt(saved, m.source, columns, name, capped);
      return {
        label: name,
        notes: r.notes,
        make: gridMaker(r.snapshot, r.configuration, [], { planner: own, cubeSource, label: name, ...(r.tree ? { tree: r.tree } : {}), foldPanel: capped }),
      };
    }

    /** The chosen source, opened for a grid: in place of the page, or -- `capped` -- added beside the others. */
    function opened(chosen: Chosen, capped: boolean): Promise<Opened> {
      if (chosen.kind === 'saved') return Promise.resolve(openedSavedQuery(chosen.query, undefined, capped));
      if (chosen.kind === 'file') {
        return openedFile(chosen.file, {
          ...(chosen.sample ? { sample: chosen.sample } : {}),
          ...(chosen.handle ? { handle: chosen.handle } : {}),
          capped,
        });
      }
      if (chosen.kind === 'table') return openedTable(chosen.session, chosen.object, undefined, capped);
      return openedRemote(chosen.url, chosen.s3, undefined, capped);
    }

    /** The chosen source IN PLACE of the page: it empties, then one grid over the source. */
    async function openInPlace(chosen: Chosen): Promise<void> {
      const what = chosen.kind === 'saved' ? chosen.query.label : chosen.kind === 'file' ? chosen.file.name
        : chosen.kind === 'table' ? `${chosen.object.schema}.${chosen.object.name}` : lastSegment(chosen.url);
      await inPlace(() => opened(chosen, false), what);
    }

    // THE QUERY STORE, through the one client (query-store/README.md): the server config.json names,
    // or -- none named -- the same API answered in this page from this origin's browser store, the
    // one Legend Query keeps its saved queries in when it runs without a server.
    let stores: { readonly url: string; readonly store: Promise<QueryReader>; readonly where: string } | undefined;
    const queryStore = (config: PageConfig): { readonly store: Promise<QueryReader>; readonly where: string } => {
      if (stores?.url !== config.queryStore) {
        const url = config.queryStore;
        stores = {
          url,
          where: url ? `on ${new URL(url).host}` : 'in this browser',
          store: queryStores().then((m) => (url
            ? new m.QueryStoreClient(url)
            : new m.QueryStoreClient(m.LOCAL_API, m.localQueryServer({ records: new m.BrowserRecords(), user: config.user }).fetch))),
        };
      }
      return stores;
    };

    picker = async (purpose: 'add' | 'open', start?: SectionId): Promise<GridMaker | undefined> => {
      const config = await pageConfig();
      const act = async (chosen: Chosen): Promise<GridMaker | null> => {
        if (purpose === 'add') return (await opened(chosen, true)).make;
        await openInPlace(chosen);
        return null;
      };
      const sections: PickerSections<GridMaker | null> = {
        files: {
          accept: '.csv,.parquet,.json,.jsonl,.ndjson,text/csv,application/json',
          formats: ['CSV', 'Parquet', 'JSON', 'JSON Lines'],
          // where the browser can keep a handle to the file, pick THROUGH it: a saved cube then
          // reopens its file from where it was picked (file-handles.ts)
          ...(canKeepHandles() ? { pick: () => pickDataFile() } : {}),
          open: (file, handle) => act({ kind: 'file', file, ...(handle ? { handle: handle as FileHandle } : {}) }),
        },
        examples: {
          list: SAMPLES.map((s) => ({
            id: s.id, name: s.label, description: s.about, rows: s.defaultRows,
            tags: [s.format === 'jsonl' ? 'JSON Lines' : 'CSV'],
          })),
          open: (id, rows) => act({ kind: 'file', ...sampleFile(id, rows) }),
          // the generated file itself, for sharing or reopening
          download: (id, rows) => {
            const { file } = sampleFile(id, rows);
            const url = URL.createObjectURL(file);
            const a = document.createElement('a');
            a.href = url;
            a.download = file.name;
            a.click();
            URL.revokeObjectURL(url);
          },
        },
        saved: {
          where: queryStore(config).where,
          // the browser's store: a query saved in another tab (Legend Query) appears without reopening
          ...(config.queryStore ? {} : {
            watch: (changed: () => void) => {
              let stop = (): void => undefined;
              let stopped = false;
              void queryStores().then((m) => { if (!stopped) stop = m.watchBrowserStore(changed); });
              return () => { stopped = true; stop(); };
            },
          }),
          search: async (text, mineOnly) => (await (await queryStore(config).store).search({
            ...(text ? { searchTermSpecification: { searchTerm: text, includeOwner: true } } : {}),
            showCurrentUserQueriesOnly: mineOnly,
            sortByOption: 'SORT_BY_UPDATE',
            limit: 50,
          })).map((q) => {
            const project = projectFor(config, q);
            return {
              id: q.id,
              name: q.name,
              ...(q.owner ? { owner: q.owner } : {}),
              ...(q.lastUpdatedAt ? { modified: new Date(q.lastUpdatedAt).toLocaleDateString(UI_LOCALE) } : {}),
              project: project?.title ?? `${q.groupId}:${q.artifactId}:${q.versionId}`,
              // with a depot, a project not in config.json opens by name (openedRecord)
              ...(project || config.depot ? {} : { unusable: 'This page has no model for its project' }),
            };
          }),
          open: async (id) => act({ kind: 'saved', query: await openedQuery(config, await queryStore(config).store, id) }),
          // a link to this page that opens the query as its source (query-store/src/share.ts)
          copyLink: async (id) => {
            const [store, { queryFragment }] = await Promise.all([queryStore(config).store, queryStores()]);
            const link = `${location.origin}${location.pathname}${location.search}#${await queryFragment(await store.get(id))}`;
            await navigator.clipboard.writeText(link);
            return `Link copied (${link.length.toLocaleString(UI_LOCALE)} characters): it opens the query as the cube’s source, and holds the query, never its rows.`;
          },
        },
        database: databaseSection(config, (session, object) => act({ kind: 'table', session, object })),
        remote: {
          detect: detectFormat,
          open: (url, credentials) => act({ kind: 'remote', url, ...(credentials ? { s3: s3Of(credentials) } : {}) }),
        },
      };
      const made = await pickSource(document, { purpose, sections, ...(start ? { start } : {}) });
      return made ?? undefined;
    };

    const detectFormat = (url: string): string | undefined => ({ parquet: 'Parquet', csv: 'CSV', iceberg: 'Iceberg' })[inferFormat(url)];
    /** What the Remote section's form gave, as DuckDB's S3 settings: only what was filled in. */
    const s3Of = (credentials: RemoteCredentials): S3Credentials => ({
      ...(credentials.region ? { region: credentials.region } : {}),
      ...(credentials.keyId ? { accessKeyId: credentials.keyId } : {}),
      ...(credentials.secret ? { secretAccessKey: credentials.secret } : {}),
      ...(credentials.endpoint ? { endpoint: credentials.endpoint } : {}),
    });
    /** One warehouse, however its address is written. */
    const sameWarehouse = (a: string, b: string): boolean => {
      try {
        return new URL(a).origin === new URL(b).origin;
      } catch {
        return a.replace(/\/+$/, '') === b.replace(/\/+$/, '');
      }
    };

    /**
     * The Database section: the sign-in kept between windows, and what opening a listed table does.
     * `want` (reopening a cube saved over a table): its warehouse's address offered, the session of
     * another warehouse not, and the table opened as soon as it is listed.
     */
    function databaseSection<T>(
      config: PageConfig,
      open: (session: WarehouseSession, object: CatalogObject) => Promise<T>,
      want?: { readonly warehouse: string; readonly schema: string; readonly name: string },
    ): NonNullable<PickerSections<T>['database']> {
      const url = want?.warehouse ?? (config.warehouse || rememberedWarehouse());
      const keep = signedIn && (!want || sameWarehouse(signedIn.session.baseUrl, want.warehouse)) ? signedIn : undefined;
      return {
        ...(url ? { url } : {}),
        ...(keep ? { session: asSession(keep) } : {}),
        ...(want ? { want: { schema: want.schema, name: want.name } } : {}),
        signIn: async (url, user, password) => {
          // nothing of a previous sign-in stays on offer until this one has listed its tables (P2-334)
          signedIn = undefined;
          const connected = await connect(url, user, password);
          signedIn = { session: connected.session, objects: connected.objects };
          try {
            window.localStorage.setItem(REMEMBERED, connected.session.baseUrl);
          } catch {
            // storage refused: not remembered, nothing else changes
          }
          // THE CUBES LIVE THERE go on with the new token when it is the same user (P2-297)
          for (const live of liveEngines) {
            try {
              live.renew(connected.session);
            } catch {
              // another user: that cube keeps its own sign-in
            }
          }
          return asSession(signedIn);
        },
        open: (object) => {
          const s = signedIn;
          const found = s?.objects.find((o) => (object.catalog === undefined || o.catalog === object.catalog)
            && o.schema === object.schema && o.name === object.name);
          if (!s || !found) return Promise.reject(new Error(`${object.schema}.${object.name} is no longer offered: sign in again`));
          return open(s.session, found);
        },
      };
    }

    /**
     * A saved (or shared) cube over a WAREHOUSE TABLE, reopened: straight away when this page is
     * signed in to that warehouse; otherwise the Database section asks for a sign-in there and
     * opens the table once it is listed. No credential was ever written down.
     */
    async function reopenTable(src: WarehouseSource, saved: CubeDocument, several = false): Promise<Opened | undefined> {
      const on = signedIn && sameWarehouse(signedIn.session.baseUrl, src.warehouse) ? signedIn : undefined;
      const found = on?.objects.find((o) => o.catalog === src.catalog && o.schema === src.schema && o.name === src.table);
      if (on && found) return openedTable(on.session, found, saved, several);
      let got: Opened | undefined;
      const host = (() => { try { return new URL(src.warehouse).host; } catch { return src.warehouse; } })();
      const done = await pickSource<boolean>(document, {
        purpose: 'open',
        start: 'database',
        reason: `“${saved.name}” reads ${src.name} on ${host}: sign in there to open it.`,
        sections: {
          database: databaseSection(await pageConfig(), async (session, object) => {
            got = await openedTable(session, object, saved, several);
            return true;
          }, { warehouse: src.warehouse, schema: src.schema, name: src.table }),
        },
      });
      return done ? got : undefined;
    }

    /**
     * A saved (or shared) cube over a REMOTE FILE, reopened: read straight away when the file is
     * public; refused (a private bucket), the Remote section asks for its keys, the URL filled in.
     */
    async function reopenRemote(src: RemoteSource, saved: CubeDocument, several = false): Promise<Opened | undefined> {
      let refusal: string;
      try {
        return await openedRemote(src.url, undefined, saved, several);
      } catch (e) {
        refusal = e instanceof Error ? e.message : String(e);
      }
      let got: Opened | undefined;
      const done = await pickSource<boolean>(document, {
        purpose: 'open',
        start: 'remote',
        reason: `“${saved.name}” reads ${src.name}, which did not open without its keys (${refusal.slice(0, 160)}).`,
        sections: {
          remote: {
            detect: detectFormat,
            url: src.url,
            keys: true,
            open: async (url, credentials) => {
              got = await openedRemote(url, credentials ? s3Of(credentials) : undefined, saved, several);
              return true;
            },
          },
        },
      });
      return done ? got : undefined;
    }

    // A BLANK PAGE (New ▸ Blank Page; the user, 2026-10-01): every grid and chart goes, and the page
    // says what to do first -- a data source (the picker, opening in place) or a saved page (PageApp's empty page,
    // `empty` above). Asked first when there are unsaved changes; the page stays until the person agrees.
    blankPage = (reason?: string) => {
      if (dirty() && !window.confirm('The page has unsaved changes. Start a blank page anyway?')) return;
      page.clear();
      current = {};
      page.setTitle('');
      sayWhyBlank(reason);
      if (blankCard) blankCard.hidden = false;
      document.title = 'New page';
      (host.querySelector('.dc-blank-actions .dc-primary') as HTMLElement | null)?.focus();
    };

    // A SHARE LINK in the address opens its page, found the way a saved one is (a sample rebuilt,
    // a file asked for). Then the link is taken out of the address: a reload never reopens it over
    // what the person has done since. Last in the setup: opening one reaches everything above
    // (a sample's rebuild included).
    // A SAVED QUERY'S SHARE LINK (query-store/src/share.ts): `#q1.<data>`, or Legend Query's own
    // `#/shared/q1.<data>` -- the query, opened as the cube's source; taken out of the address, as above
    const queryLink = location.hash.replace(/^#\/shared\//, '#');
    const links = queryLink.length > 1 ? await queryStores() : undefined;
    if (links?.isQueryFragment(queryLink)) {
      history.replaceState(null, '', location.pathname + location.search);
      try {
        const config = await pageConfig();
        await openInPlace({ kind: 'saved', query: await openedRecord(config, await links.readQueryFragment(queryLink)) });
      } catch (e) {
        failedStart(e);
      }
    }
    if (isPageFragment(location.hash)) {
      const fragment = location.hash;
      history.replaceState(null, '', location.pathname + location.search);
      showCubes();
      try {
        await openSaved({ kind: 'page', page: readPageFragment(fragment) }, undefined);
        // a page someone shared opens locked: nothing moves by accident (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md
        // §3.3, 5); its menu's Edit Layout unlocks it
        page.setLayoutEditing(false);
      } catch (e) {
        library?.say(e instanceof Error ? e.message : String(e), 'error');
      }
    }

    // THE SINGLE-USER APP'S START (docs/DATACUBE_APP_PLAN_2026_10_02.md, A3): the launch key signs in
    // to the warehouse that served this page (its config.json names it), then the table asked for is
    // opened, or the tables are offered. The key stays in the address: a reload signs in again.
    if (start.kind === 'warehouse') {
      const config = await pageConfig();
      try {
        if (!config.warehouse) throw new Error('this page was not served by a warehouse: its config.json names none');
        const session = await signInWithKey(config.warehouse, start.key);
        signedIn = { session, objects: await listObjects(session) };
      } catch (e) {
        failedStart(e);
      }
      const on = signedIn;
      const asked = start.table;
      const found = on && asked ? on.objects.filter((o) => `${o.schema}.${o.name}` === asked) : [];
      if (on && found.length === 1 && found[0]) {
        await openInPlace({ kind: 'table', session: on.session, object: found[0] });
      } else if (on) {
        if (asked) {
          failedStart(new Error(found.length === 0
            ? `${asked} is not a table you may read here`
            : `${asked} is in ${found.length} catalogs (${found.map((o) => o.catalog).join(', ')}): choose one`));
        }
        blankPage(startProblem);
        void picker('open', 'database');
      }
    }
    // A START THAT OPENED NOTHING (a link that failed, a key refused) leaves the blank page, saying
    // why: never an empty page without a word
    if (page.empty) blankPage(startProblem);
  } else if (start.kind !== 'sample') {
    status.textContent = 'this planner opens only the sample: the link or key in the address needs the in-tab planner';
    status.classList.add('bad');
  }
  // the start has settled: from now on an empty page is a blank one
  if (blankCard) blankCard.hidden = false;

  /** The reason a start opened nothing, said where the page says what is wrong. */
  function failedStart(e: unknown): void {
    harnessSignal.changes += 1;
    startProblem = e instanceof Error ? e.message : String(e);
    status.textContent = startProblem;
    status.classList.add('bad');
  }
}

/**
 * A host panel, shown as a floating window.
 *
 * The same window the cube uses for its own dialogs
 * (`src/ui/window.ts`), so a panel the host adds behaves like the
 * ones it did not: dragged by its header, resized from any edge, and
 * remembering where it was left.
 */
/** A size in words: 2.1 MB. */
function bytes(n: number): string {
  if (n < 1024) return `${n} bytes`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

const hostWindows = new Map<string, WindowSpec>();

function toggleHostWindow(id: string): void {
  const el = must(id);
  if (!el.hidden) {
    el.hidden = true;
    return;
  }
  showHostWindow(id);
}

/** Show a host window (left where it is if already shown), at `size` the first time. */
function showHostWindow(id: string, size: { width: number; height: number } = { width: 720, height: 420 }): void {
  const el = must(id);
  if (!el.hidden) return;
  el.hidden = false;
  const head = el.querySelector('.hostwin-head');
  if (!(head instanceof HTMLElement)) return;
  hostWindows.set(id, makeWindow(el, head, document.body, {
    ...size,
    ...(hostWindows.get(id) ? { spec: hostWindows.get(id) } : {}),
    onChange: (spec) => hostWindows.set(id, spec),
  }));
}


/**
 * Pick a label by an explicit index expression.
 *
 * The index is passed in rather than derived from the value count,
 * because deriving it gave every dimension the same `i % n` and made
 * them perfectly correlated: each desk then had exactly one year, so
 * four of five pivot columns were legitimately null and the grid
 * looked broken. Independent divisors make every combination occur.
 */
function sqlPick(values: readonly string[], indexExpr: string): string {
  const cases = values.map((v, i) => `WHEN ${i} THEN '${v}'`).join(' ');
  return `CASE (${indexExpr}) ${cases} END`;
}

export function must(id: string): HTMLElement {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing #${id}`);
  return el;
}

const SETTINGS_KEY = 'dataCube.settings';

/** The settings this browser kept, or none. */
function storedSettings(): Record<string, unknown> | undefined {
  try {
    const raw = window.localStorage.getItem(SETTINGS_KEY);
    const parsed: unknown = raw === null ? undefined : JSON.parse(raw);
    return parsed !== null && typeof parsed === 'object'
      ? (parsed as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}
