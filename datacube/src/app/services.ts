// THE SERVICES: everything a host gives DataCube's app (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.2). The app --
// the page, its sources, its saved pages, its model actions -- is written against these and nothing else of its host,
// so the browser tab, Python's tab and widget, and Python headless are each a set of services for the same app. Each
// service is an existing client over its host's transport, never a second implementation; what a host cannot do is a
// service it does not give, and the app does not offer its entries (§3.4).
//
// Types only: a host's services are built by the host (src/services/browser.ts for the tab, src/services/python-host.ts
// for Python's tab and notebook widget).

import type { ModelHome } from '../../../depot-client/src/model-home.ts';
import type { QueryEngine } from '../../../engine-client/src/engine.ts';
import type { ProjectDatabases } from '../../../engine-client/src/project-data.ts';
import type { CatalogObject, WarehouseEngine, WarehouseSession } from '../../../engine-client/src/warehouse.ts';
import type { Lambda, ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { Receipt } from '../../../engine-client/src/receipt.ts';
import type { QueryReader } from '../../../query-store/src/index.ts';
import type { HeldCopy } from '../app.ts';
import type { CubeConfiguration } from '../config.ts';
import type { Planner } from '../cube.ts';
import type { CubeDocument, CubeSource } from '../cube-document.ts';
import type { CubeStore } from '../cube-store.ts';
import type { FileHandles } from '../file-handles.ts';
import type { InferredModel, TableModels } from '../infer.ts';
import type { JoinHow, JoinSide } from '../join-how.ts';
import type { JoinSummaryCounts } from '../join.ts';
import type { PageDocument } from '../page-document.ts';
import type { PageCommand } from '../page-commands.ts';
import type { GridCommand } from '../grid-commands.ts';
import type { ModelOptions } from '../planner.ts';
import type { S3Credentials } from '../remote.ts';
import type { ModelWriter } from '../model-writer.ts';
import type { PlanAndRun } from '../runner.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import type { UploadResult } from '../upload.ts';

/** Everything a host gives the app. */
export interface Services {
  /** Where queries are planned and run, and where data goes in (§4). */
  readonly engine: EngineService;
  /** Where pages are saved (§5): opened as the app starts, and waited for only where a page is saved or opened. Absent:
   *  the page offers no Save, Save As… or Open… (a Python host until its page store, step 2d). */
  readonly pages?: Promise<PageService>;
  /** Another editor of what the page shows (§5): Python, whose code changes the page and its frames, and reads the page
   *  back. Absent in the tab: the page is the person's alone. */
  readonly live?: LiveService;
  /** Where saved queries are, and the projects they compile against. Absent: the page offers no saved query. */
  readonly queries?: QueryService;
  /** Where classes go: an SDLC and its Depot (§6). Absent: the page makes no class, and opens no project by name. */
  readonly modelHome?: ModelHomeService;
  /** A warehouse the page signs in to, its tables read Live there. Absent: the page offers none. */
  readonly warehouse?: WarehouseService;
  /** The browser's own things. */
  readonly browser: BrowserService;
}

// -------------------------------------------------------------------------------------------------------- the engine

/** Where the page plans: one word, as its status bar says it (`local`, `remote`, `engine`). */
export type PlaneWord = 'local' | 'remote' | 'engine';

/** A plane the page can open on, as its menu offers it. */
export interface Plane {
  readonly id: `host.plane.${string}`;
  readonly word: PlaneWord;
  readonly label: string;
}

/**
 * THE PLANE, chosen once as the app opens (the user, 2026-09-30, and the one-app design's R3): shown in the status bar,
 * never substituted -- a planner that does not answer is said, and the page stops. Another is chosen by opening the app
 * again on it.
 */
export interface PlaneService {
  readonly word: PlaneWord;
  readonly planes: readonly Plane[];
  /** Open the app again on `plane` (the tab: the same page, its `?planner=` the plane's). */
  choose(plane: Plane): void;
}

/**
 * A grid's rows (§4.5), in the two shapes a grid takes: planned here and run here -- a planner over the source's own
 * model and the database it runs on (the tab, headless) -- or planned and run by the host, the page sending Pure and
 * never SQL (a Python host: R4).
 */
export type GridRows =
  | { readonly planner: Planner; readonly engine: QueryEngine; readonly runner?: undefined }
  | { readonly runner: PlanAndRun; readonly planner?: undefined; readonly engine?: undefined };

/**
 * WHAT THE MODEL ACTIONS NEED OF A HOST (Create a Class…, Create an Association…, Create a Data Space…;
 * docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B1): legend-lite's model writer and the host's database -- never the page's own
 * planner or engine, so every host that gives this (and a model home) gets them. A grid's own rows come from where its
 * rows always do (its PlanAndRun); a project's from `rows`.
 */
export interface ModelWriting {
  /** The name a class's model gives the host's database (its store, connection and runtime): `Browser`, `Python`. */
  readonly place: string;
  /** legend-lite's model writer: the tab's module (beside whichever planner the page chose), Python's native library. */
  readonly writer: ModelWriter;
  /** A project's queries planned by the writer over its files and run on the host's database: a class's facts. */
  rows(model: string, runtime: string): PlanAndRun;
}

/**
 * WHAT A SAVED QUERY IS READ BY on any host (C5): its text, and its project's model's elements (its data spaces,
 * enumerations), as the compiler reads them -- the tab's own planner, Python's native library.
 */
export interface QueryReading {
  parse(text: string): Promise<Lambda>;
  elements(text: string): Promise<unknown[]>;
}

/** Where queries are planned and run, and where data comes in. */
export interface EngineService {
  readonly plane: PlaneService;
  /** What the status bar says of the planner, in one word. */
  readonly label: string;
  /** What a saved query's text and its project's model are read by. */
  readonly reading: QueryReading;
  /** A grid's rows over a source's model (§4.5): its own (a page's grids share no model), in the host's shape. */
  rows(model: string, runtime: string, how?: ModelOptions): GridRows;
  /** Where data comes in. */
  readonly database: DatabaseService;
  /** What the model actions need (`ModelWriting`); absent, they are not offered (§3.4). */
  readonly modelWriting?: ModelWriting;
  /**
   * THE PAGE'S OWN PLANNER AND DATABASE, where the page plans and runs SQL itself (the tab): what only that shape has
   * (§4.5) -- the sample's planner and the one a saved query's text is read by, the model writer of a warehouse table,
   * test data, Snap. Absent on a Python host, where the page writes no SQL (R4): what needs it is not offered there.
   */
  readonly local?: LocalEngine;
}

/** The page's own planner and database (the tab's: its module or a server, and DuckDB-WASM). */
export interface LocalEngine {
  /** The page's planner: the sample's, and the one a saved query's text is read by. */
  readonly planner: Planner;
  /** A planner over another source's model (a page's grids share no model), on the same worker or server. */
  another(model: string, runtime: string, how?: ModelOptions): Planner;
  /** A model's elements as the compiler reads them: a saved query's project (data spaces, enumerations). */
  elements(text: string): Promise<unknown[]>;
  /** legend-lite's model writer for a table of the host's database, whichever planner plans it (infer.ts). */
  readonly tables: TableModels;
  /** A project's own databases in the host's (C5, engine-client's project-data.ts): a saved query's rows by name. */
  projects(): Promise<ProjectDatabases>;
  /** Where a grid over one of the database's tables runs, its database type the model's connection names. */
  readonly engine: QueryEngine & { readonly databaseType: string; run(sql: string, epoch: number): Promise<unknown> };
}

/**
 * The host's database: every way data comes in -- a file, a remote file, a Snap (§4.2), a frame -- each answered with
 * the model the host wrote for it. A way a host does not take yet is absent, and the page does not offer it (§3.4).
 */
export interface DatabaseService {
  /** A file read into a table of its own (`table`), its model written from what the catalog found (upload.ts). Given
   *  with `forgetFile`. */
  readonly ingestFile?: (file: File, options: { readonly table: string }) => Promise<UploadResult>;
  /** A file's table dropped, its name free again. */
  readonly forgetFile?: (fileName: string, table: string) => Promise<void>;
  /** A remote file mounted as a view named `name`, its model written from its catalog (read-only: no conversion). */
  readonly mountRemote?: (url: string, s3: S3Credentials | undefined, name: string) => Promise<InferredModel>;
  /** A frame (Python's dataframe) as the host says it is now: the model it wrote when the frame was registered. */
  readonly frame?: (name: string) => Promise<FrameModel>;
  /**
   * A grid's base query copied into a table of the host's database of its own (Snap: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
   * §18.3; Python's `snap.json`) -- a frame's grid, a file's, a warehouse table's the host opened: `pull` (Pure, its
   * every column) planned over `model` against `runtime` and run where the rows are, the copy modelled by the host as
   * it holds them. Its count, the pull's receipt, its table, and that model. Given with `release`.
   */
  readonly snap?: (model: string, runtime: string, pull: Lambda) => Promise<HostCopy>;
  /** A snap's copy dropped, by its table. */
  readonly release?: (table: string) => Promise<void>;
  /**
   * A copy the host holds, by its table, as it was made -- its model and the relation that reads it: what a page
   * follows when its other editor snapped a grid (Python's grid.snap(), the page's snap command naming only the copy's
   * table: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8). Refused when the host holds no such copy.
   */
  readonly held?: (table: string) => Promise<InferredModel & { readonly table: string }>;
  /** Whose database it is, as a person reads it (`Python`): where a snap's copy is said to be kept. */
  readonly place?: string;
  /**
   * A saved cube opened by the host where it reads it, as its source is now (Python's `opened.json`: headless gridOpened)
   * -- every kind but a frame's, which a page with no database or compiler of its own on the host cannot open
   * (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3; the S7 audit's H-2, H-3). `cube`: its document's text; `page`: its
   * page's name; `gridsOnPage`: how many grids the page holds.
   */
  readonly opened?: (cube: string, page: string, gridsOnPage: number) => Promise<HostOpened>;
  /**
   * A new grid's cube over a class made by the host where it reads it (Python's `source.json`: headless sourceCube, as
   * Python's page.grid(cls=...) makes one: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md K16, K17) -- for a page with no
   * compiler or model home of its own there. `model`: the page's model (its text), or null; `given`: the class as
   * class-source.ts classGiven takes it (its text); `page`: the page's name; `gridsOnPage`: how many grids the page holds
   * with it. Its cube document's text, then opened as every grid of the host's page is (`opened`); refused in DataCube's
   * words.
   */
  readonly made?: (model: string | null, given: string, page: string, gridsOnPage: number) => Promise<string>;
  /** A join of two of the page's grids made by the host over its tables (HostJoin): where the page has no database of
   *  its own there, its Join… asks the host (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11). */
  readonly join?: HostJoin;
}

/**
 * A JOIN MADE BY THE HOST (Python's `join.json`: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11), for a page with no
 * database of its own there: each answer DataCube's own code's, run by the host over its tables (headless join.ts). `page`
 * is the page as the tab holds it, its document's text; `mine` and `theirs` the grids joined, by their tiles' ids.
 */
export interface HostJoin {
  /** The two sides as the Join window shows them (headless joinSides), and what drifted in an input since. */
  sides(page: string, mine: string, theirs: string): Promise<{ readonly left: JoinSide; readonly right: JoinSide; readonly notes: readonly string[] }>;
  /** What the join will do (headless joinQuestions). */
  questions(page: string, mine: string, theirs: string, how: JoinHow): Promise<JoinSummaryCounts>;
  /** The join's grid's cube document, as the page command addGrid carries it onto `sheet` (headless joinCube). */
  cube(page: string, mine: string, theirs: string, how: JoinHow, sheet: string | null): Promise<CubeDocument>;
}

/** A saved cube the host opened (`DatabaseService.opened`): rebuilt as it opens now, and the base its rows are read over. */
export interface HostOpened {
  /** The cube document's text, rebuilt over its source as it is now. */
  readonly cube: string;
  /** What opening it said: what drifted, what was left out. */
  readonly notes: readonly string[];
  /** Whether the cube opened is another than the one asked about (cube-document.ts cubeTaken). */
  readonly taken: boolean;
  readonly model: string;
  readonly runtime: string;
  /** How its rows are planned over `model` besides: a class's mapping, a saved query's own project. */
  readonly how: ModelOptions;
  /** The relation its rows are read over: a file's, a remote file's, a class's, a saved query's, a warehouse table's,
   *  a join's of the host's tables. */
  readonly relation: ValueSpecification;
  /** Each frame it reads, by name, at the version it was read at: what following the host's frames compares. */
  readonly frames: Readonly<Record<string, number>>;
}

/** A frame's model, as its host wrote it (Python's `Frames`): its model, runtime and source, and how often it changed. */
export interface FrameModel {
  readonly model: string;
  readonly runtime: string;
  readonly source: ValueSpecification;
  /** How many times the frame changed (its rows, its columns): a later version is a change to follow. */
  readonly version: number;
}

// --------------------------------------------------------------------------------------------------------- the pages

/** Where pages are saved: upstream's DataCube query API (cube-store.ts). */
export interface PageService {
  readonly store: CubeStore;
  /** Said where a page is saved and opened: "in this browser", "for this visit only (...)". */
  readonly where: string;
  /** What the store keeps lasts only this visit (the browser refused storage). */
  readonly memoryOnly: boolean;
  /** Where a page keeps the handle each grid's file was picked through, when the host can keep one. */
  readonly handles?: FileHandles;
  /** Ask that what is saved be kept (the browser's persistent storage): whether it was promised. */
  persist(): Promise<boolean>;
}

/** What another editor says of what the page shows: the page's version (when it serves a page) and each frame's. */
export interface Versions {
  readonly version?: number;
  readonly frames: Readonly<Record<string, number>>;
}

/**
 * ANOTHER EDITOR OF WHAT THE PAGE SHOWS (§5): Python, whose code changes the page (`ll.Page`) and its frames. The page is
 * one copy, Python's keeper's (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): its protocol is the page's document, its
 * versions, and a write against the version it is at -- refused when the page has moved on since.
 */
export interface LiveService {
  /**
   * Told each time the editor moves what the page shows (its versions), or -- a string -- why it no longer says (the
   * page or the frame closed, Python gone): told nothing after that. `shown`: the version of the editor's page on
   * screen (none while it opens or follows one), which the editor is told as it is asked (its copies that version
   * reads stay until the screen moves past them: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17). Returns what stops it.
   */
  watch(moved: (now: Versions | string) => void, shown?: () => number | undefined): () => void;
  /** The page the editor serves, when it serves one (`ll.Page`; `ll.show(df)` serves a frame, a page of one grid). */
  readonly page?: LivePage;
}

/** One command the editor made (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): the page's, or a grid's, by its tile's id. */
export type LiveCommand =
  | { readonly page: PageCommand }
  | { readonly grid: string; readonly command: GridCommand };

/**
 * What a page open on screen writes to the page's keeper (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4): the edits made
 * on screen at the version it is at, each a command, which the keeper applies -- an undo or a redo among them, as the
 * commands between (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W5: no page is written whole). Each write has an id, its
 * writer's (one per channel) and its number there, from 1: the keeper applies it once however often it is sent, so a
 * write whose answer was lost is asked about, never taken for refused (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md,
 * the middle version).
 */
export interface LiveWrite {
  readonly edits: readonly LiveCommand[];
  readonly writer: string;
  readonly seq: number;
}

/** The page as its keeper says it is now: at its version, with every id it has held (none is given out again: B12), and
 *  its hash (page-hash.ts: what the page open on it must hash to, W17); asked by a writer, the number of the last of its
 *  writes the keeper took (`taken`, 0: none). */
export interface LiveRead {
  readonly version: number;
  readonly page: PageDocument;
  readonly held: readonly string[];
  readonly hash: string;
  readonly taken?: number;
}

/**
 * A write settled by the page's keeper (keeper.py Settled): the page's version now and its hash, which the screen's
 * must hash to (W17) -- and, when the keeper made another page than the screen sent (it had moved on meanwhile, the
 * edits made again on top), that page and every id it has held, which the screen is put at
 * (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version).
 */
export interface LiveSettled {
  readonly version: number;
  readonly hash: string;
  readonly page?: PageDocument;
  readonly held?: readonly string[];
}

/** A write the page's keeper refused, in its words: an edit that does not apply where the page now is, the host refusing
 *  what it makes, a write from further back than it keeps. Nothing of it was kept. Any other failure of a write is no
 *  refusal: its answer never came, and the keeper is asked whether it took it. */
export class LiveRefused extends Error {}

/** A page another editor serves: read as it is now, and told what it is now. */
export interface LivePage {
  /** The page as the editor says it is now, and -- asked by `writer` -- the last of its writes the keeper took; refused
   *  with the editor's reason. */
  read(writer?: string): Promise<LiveRead>;
  /**
   * A write against the version on screen, settled by the page's keeper (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): its
   * edits applied by DataCube's one rule to the page as the keeper has it, and what they make kept (LiveSettled).
   * Refused in the keeper's words (LiveRefused): nothing kept, and the screen is put at the page as the keeper has it.
   */
  report(version: number, write: LiveWrite): Promise<LiveSettled>;
}

// ------------------------------------------------------------------------------------------------------- the queries

/**
 * A project a saved query can belong to, as the host's configuration names it: what it compiles against (`models`,
 * Pure text joined in order). Its rows are its Data elements', in a database of its own where the host runs it (C5).
 */
export interface ConfiguredProject {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly title: string;
  readonly models: readonly string[];
}

/** Where saved queries are (upstream's query store API), and the projects the host configured for them. */
export interface QueryService {
  readonly store: () => Promise<QueryReader>;
  /** Said where they are: "in this browser", "on host:port". */
  readonly where: string;
  /** Told when another app on the host saves one (the browser's own store), when the host can say. */
  readonly watch?: (changed: () => void) => () => void;
  readonly projects: readonly ConfiguredProject[];
  /** A file of a configured project (a model, a seed), by its URL. */
  fetchText(url: string): Promise<string>;
}

// ----------------------------------------------------------------------------------------------------- the model home

/** Where classes go, and where a saved query's project is opened by name (§6). */
export interface ModelHomeService {
  /** The SDLC and its Depot, connected the first time they are asked for. */
  connect(): Promise<ModelHome>;
  /** Studio, opened on a project's workspace, where the host serves it; absent where none is (Python's, until it serves
   *  Studio: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md E). */
  studio?(project: string, workspace: string): string;
}

// ------------------------------------------------------------------------------------------------------ the warehouse

/** A warehouse the page signs in to as its user (engine-client/src/warehouse.ts). */
export interface WarehouseService {
  /** The warehouse the picker offers first: the deployment's, else the one this browser signed in to last. */
  offered(): string;
  /** Remember the warehouse signed in to, for the next visit. */
  remember(url: string): void;
  connect(url: string, user: string, password: string): Promise<{ session: WarehouseSession; objects: CatalogObject[] }>;
  /** The single-user app's launch key: signed in with it, at the warehouse that served the page. */
  signInWithKey(key: string): Promise<{ session: WarehouseSession; objects: CatalogObject[] }>;
  /** A table's rows read Live there, as the signed-in user, by the page itself (the tab's). */
  live?(session: WarehouseSession, catalog: string, databaseType: string): WarehouseEngine;
  /**
   * A table's model written and held by the host, which then plans and runs its queries itself (Python's: C3, R4) --
   * the tab's own writer's answer, read-only.
   */
  table?(session: WarehouseSession, object: CatalogObject): Promise<InferredModel>;
}

/** A copy its host made of a grid's base query (DatabaseService.snap): its own table, and the host's model of it. */
export interface HostCopy extends InferredModel {
  readonly rowCount: number;
  readonly pulledBy?: Receipt;
  readonly table: string;
}

// -------------------------------------------------------------------------------------------------------- the browser

/** The browser's own things: the same in a tab and under a notebook's cell. */
export interface BrowserService {
  download(name: string, mime: string, content: string | Uint8Array): void;
  /** Text onto the clipboard; refused, in words, where the browser does not let the page copy. */
  writeClipboard(text: string): Promise<void>;
  /** The grids' settings kept between visits (upstream's settingsData); undefined when none or not readable. */
  readSettings(): Record<string, unknown> | undefined;
  writeSettings(values: Record<string, unknown>): void;
  /** This page's address, which a share link opens; absent where no one else can open it, and no link is offered. */
  readonly address?: () => string;
  /** The page's own tab's title (its name, and whether it changed); absent where the page is not a tab of its own. */
  readonly title?: { readonly base: string; set(text: string): void };
}

// --------------------------------------------------------------------------------------------------------- the start

/** The tab's sample: generated rows (or a remote file read through the same model) under the demo's model. */
export interface Sample {
  /** Its opening view, typed by the compiler over its source. */
  readonly snapshot: () => Promise<CubeSnapshot>;
  /**
   * What its rows are, written down (a generated sample's file: sample-trades.ts DESK_TRADES_FILE), and the tables of
   * the host's database they are in: its grid is then saved, shared and joined as any file's is. Absent (a remote file
   * named in the address), the page it starts says it cannot be saved.
   */
  readonly source?: { readonly cube: CubeSource; readonly tables: readonly string[] };
  /** Generated rows are a copy in the host's database from the start; a mounted remote file is live, and snaps as any
   *  grid does (PageSources.snapTarget). */
  readonly heldCopy?: HeldCopy;
  readonly configuration: CubeConfiguration;
  readonly dimensions: { name: string; columns: string[] }[];
}
