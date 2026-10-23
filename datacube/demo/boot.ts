// A runnable demo: real DuckDB-WASM in the browser, real snap mode, the real grid.
//
// ONE PAGE, its planner a setting (`chosenPlane`, `?planner=`): in this tab, legend-lite or
// legend-engine -- three addresses of the same service, built by planners.ts. The planner writes
// the SQL; DuckDB in this tab runs it. There is no fallback: a planner that does not answer is
// said (`refusePlanner`) and the page stops.

import { UI_LOCALE } from '../../engine-client/src/locale.ts';

import type { HeldCopy } from '../src/app.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../src/config.ts';
import type { Planner } from '../src/cube.ts';
import type { ModelOptions } from '../src/planner.ts';
import type { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { startDuckDbInTab } from '../../engine-client/src/duckdb-tab.ts';
import { SAMPLE_TRADES, sampleTrades } from '../src/sample-trades.ts';
import type { HostOperationsSource } from '../../engine-client/src/host-operations.ts';
import type { ProjectDataSource } from '../../engine-client/src/project-data.ts';
import { mountRemote } from '../src/remote.ts';
import { isPageFragment } from '../src/share/link.ts';
import type { TableModels } from '../src/infer.ts';
import { pageConfig } from './page-config.ts';
import { browserServices } from '../src/services/browser.ts';
import { openDataCube } from '../src/app/datacube-app.ts';
import type { PlaneService, PlaneWord, Services } from '../src/app/services.ts';
import type { ColumnFormat } from '../src/format.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { accessor, type ValueSpecification } from '../../pure-protocol/src/index.ts';

// the query store's code, fetched the first time it is needed -- a query link in the address -- as charts are: a
// grid-only page never downloads it (test/bundle-budget.test.ts)
const queryStores = () => import('../../query-store/src/index.ts');

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

/** Where the page plans: in this tab, on legend-lite, or on legend-engine -- the same API (src/app/services.ts). */
export type { PlaneWord };


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

/** The tab's plane, as its services take it: the one `?planner=` chose, opened again on another by navigating. */
function tabPlane(): PlaneService {
  return { word: chosenPlane(), planes: PLANES, choose: (plane) => void goToPlane(plane.id) };
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
  /**
   * legend-lite's model writer for a table opened on the page -- a file, a remote file, a warehouse
   * table -- whichever planner plans it (infer.ts): the planner itself in this tab; legend-lite's
   * module, loaded when first asked, beside a planner on a server (planners.ts).
   */
  readonly tables: TableModels;
  /**
   * legend-lite's planner in this tab for a project's own database (a saved query opened by name; C5): the statements
   * that make, fill and drop it (engine-client's project-data.ts) -- the same module as `tables`.
   */
  readonly projects: ProjectDataSource;
  /**
   * A file opened in this tab: the planner repointed at the model written for it from DuckDB's
   * catalog (`tables`, infer.ts). Every planner takes one (planners.ts).
   */
  readonly models: {
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
export const RUNTIME = 'trades::RT';

/** One copy of the model text, fetched so the file is the source. */
export async function loadModel(): Promise<string> {
  return (await fetch('./trades.pure')).text();
}

// -- sample data ----------------------------------------------------

/**
 * The demo's trades, generated in this tab as the table the model reads (src/sample-trades.ts: Arrow data, put in by
 * DuckDB's Arrow reader -- the page spells no SQL), a table of that name dropped first (the planner's operation).
 */
export async function generateTrades(engine: DuckDbEngine, host: HostOperationsSource): Promise<void> {
  const stream = sampleTrades();
  const [drop] = await host.hostOperations(engine.databaseType, [{ op: 'drop', kind: 'table', target: { name: 'trades' } }]);
  await engine.loadArrow({ table: 'trades' }, (async function* () { yield stream; })(), drop);
}

/**
 * DataCube's app, started: the page, and what the address asks it to open. A start that throws leaves the empty page
 * saying so -- never an empty page without a word (the status line is in a grid's bar, and there may be none).
 */
export async function boot(makePlanner: MakePlanner): Promise<void> {
  const settled = { show: (_reason: string): void => {} };
  try {
    await startPage(makePlanner, settled);
  } catch (e) {
    settled.show(`failed to start: ${e instanceof Error ? e.message : String(e)}`);
    throw e;
  }
}

/** The start itself; `settled.show` is set once the page has its empty card, to show it with a reason. */
async function startPage(makePlanner: MakePlanner, settled: { show: (reason: string) => void }): Promise<void> {
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
  // the deployment's settings (config.json, the address's overrides), read once: the services hold what they say
  const configured = pageConfig();
  configured.catch(() => {});
  status.textContent = 'starting DuckDB and the planner…';

  // the one start of a tab's DuckDB (engine-client's startDuckDbInTab): its files and extensions from ./vendor/, its
  // session set up by the planner's rules before anything uses it. So the database is ready once the planner is too:
  // DuckDB's start still overlaps the planner's (above), but the sample's rows and a remote mount below come after
  // both -- before a value is written, the session is the planner's (C1), not the browser's zone.
  // the planner's, as it arrives: the extension folder, the session, and the page's host operations (D5)
  const planned = {
    sessionSetup: (databaseType: string) => engineReady.then((p) => p.tables.sessionSetup(databaseType)),
    hostOperations: (databaseType: string, operations: Parameters<TableModels['hostOperations']>[1]) =>
      engineReady.then((p) => p.tables.hostOperations(databaseType, operations)),
  };
  const duck = await startDuckDbInTab((await configured).vendor, planned);
  performance.mark('dc:database-ready');

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
    await mountRemote(duck.engine, planned, {
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
    duck.engine.readsRemote(remote);
    status.textContent = `reading ${remote}`;
  } else if (start.kind === 'sample') {

  status.textContent = `generating ${SAMPLE_TRADES.toLocaleString(UI_LOCALE)} rows…`;
  await generateTrades(duck.engine, planned);
  generated = { label: 'trades (generated in this tab)', takenAt: new Date(), rowCount: SAMPLE_TRADES };
  }

  // -- the cube ------------------------------------------------------

  performance.mark('dc:data-ready');
  status.textContent = 'starting planner…';
  const chosen = await engineReady;
  const { planner, source, label } = chosen;
  status.textContent = label;
  // THE TAB'S SERVICES (src/services/browser.ts): everything below reads its planner, its database, its stores and its
  // browser through them
  const services: Services = await browserServices({
    plane: tabPlane(),
    planner: chosen,
    duckdb: duck,
    config: await configured,
    location,
  });

  /** The sample cube's opening view. */
  const sampleSnapshot = async (): Promise<CubeSnapshot> => ({
    source: { query: source },
    // the compiler types every column; the page declares only that year is a dimension
    columns: await sourceColumns(planner, source, [{ name: 'year', kind: 'dimension' }]),
    derived: [],
    rows: ['region', 'desk', 'book'],
    pivotOn: ['year'],
    sorts: [],
    epoch: 1,
  });

  // THE APP (src/app/datacube-app.ts), opened over the tab's services: what the address asks for first -- the sample
  // (the generated trades, or a `?remote=` file read through the same model), a link, or the launch key
  await openDataCube({
    services,
    host: must('app'),
    status,
    windows: document.body,
    start: start.kind === 'sample'
      ? {
        kind: 'sample',
        sample: {
          snapshot: sampleSnapshot,
          ...(generated ? { heldCopy: generated } : {}),
          // the host knows what the snapshot cannot: that notional and pnl are money and qty is a count
          configuration: demoConfiguration('Trades'),
          dimensions: DEMO_DIMENSIONS.map((d) => ({ name: d.name, columns: [...d.columns] })),
        },
      }
      : start.kind === 'link'
        ? { kind: 'link', hash: location.hash, forget: () => history.replaceState(null, '', location.pathname + location.search) }
        : start,
    taken: ['trades'],
    settled,
  });
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
export function must(id: string): HTMLElement {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing #${id}`);
  return el;
}
