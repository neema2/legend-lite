// PYTHON'S SERVICES (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.2, §4.3, §5; step 2a): what DataCube's app runs on
// when Python shows it -- in a tab of its own (demo/engine.ts, over HTTP to Python's engine with its token) or under a
// notebook's cell (demo/widget.ts, over the widget's channel, which the notebook authenticates). The app is the tab's;
// only these services differ.
//
// - THE ROWS ARE PYTHON'S (R4): the page sends Pure, never SQL. Each grid is one PlanAndRun (§15): the page's own
//   planner asks Python for the plan, which Python makes against the model it holds and runs on its DuckDB in the same
//   call (`plan-and-run.json`), and the page reads the rows typed by that plan, as the tab reads its own; its dry run
//   (the compile check) is that call's planning half (`plan.json`). A question about the Pure query alone -- a grid's
//   columns, a parse, a print -- is legend-engine's own route (`/api/pure/v1/...`), which Python answers as every Legend
//   server does (§15.2, as amended 2026-10-15: no worker messages carried over HTTP).
// - THE DATA IN is Python's frames: each a model Python wrote when the frame was registered (`cube.json?table=`).
// - PYTHON IS ANOTHER EDITOR of what the page shows (§5): it changes the page (`ll.Page`) and its frames, the page
//   follows their versions (`version.json`, or the widget's), and it writes the edits it made, each a command, against
//   the version it is at, to the one copy Python keeps (`page.json`): Python has DataCube apply them to the page as it
//   has it -- made again on top when it moved on, refused in words when one no longer applies -- and an undo or a redo
//   is written whole, kept only at its version (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md;
//   docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4).
//
// - A PAGE'S FILE (an upload, an example) goes into Python's DuckDB (step 2b): its bytes to Python a part at a time
//   (`file-part.json`), read there as one file by DataCube's own code (upload.ts, run headless: `file.json`), which
//   writes its model; the grid's rows are then Python's over that model, as a frame's are. Offered where Python says it
//   takes files (`services.json`: the datacube extra).
//
// - ITS MODEL HOME (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3): the SDLC and the Depot Python serves (`/sdlc/api`,
//   `/depot/api`), the same rules the browser's in-page SDLC runs, over a git repository in this user's folder; the
//   model actions write through Python's native library (ModelWriting), so Create a Class… and the rest are offered.
// - ITS SAVED QUERIES (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): the store Python keeps (`/api/pure/v1/query`, B5),
//   a query read by Python's native library and opened as a grid planned over its project's mapping, its rows the
//   project's own Data rows in that project's database inside Python's DuckDB.
//
// What Python does not answer is a service it does not give, and the app does not offer what reads it (§3.4): another
// plane (a host plans in process: Python's own planner is its one plane, the program's §1.1). A share link needs an
// address others can open; Python's is this machine's loopback.

import { arrowIpcAnswer } from '../../../engine-client/src/arrow-ipc.ts';
import { toRawTable } from '../../../engine-client/src/duckdb.ts';
import { typedByPlan } from '../../../engine-client/src/engine.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import { hostOf, type Receipt } from '../../../engine-client/src/receipt.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import { LitePlanner } from '../lite-planner.ts';
import { type ModelOptions } from '../planner.ts';
import type { PlannerWorker, Request, RequestKind, Response } from '../../../engine-client/src/legend/planner-protocol.ts';
import { ExactNumber, fromJson, readValueSpecification, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { base64 } from '../headless/web.ts';
import { CubeStoreClient } from '../cube-store.ts';
import { readPage } from '../page-document.ts';
import { readCube } from '../cube-document.ts';
import type { JoinHow, JoinSide } from '../join-how.ts';
import type { JoinSummaryCounts } from '../join.ts';
import { PlanAndRun, QueryFailure } from '../runner.ts';
import type { InferredModel } from '../infer.ts';
import type { S3Credentials } from '../remote.ts';
import { fileParts, type UploadResult } from '../upload.ts';
import type {
  BrowserService, DatabaseService, FrameModel, LivePage, LiveRead, LiveWrite, ModelHomeService, PageService, Plane, PlaneService, PlaneWord,
  QueryService, Services, Versions, WarehouseService, HostCopy, HostOpened,
} from '../app/services.ts';
import { LiveRefused } from '../app/services.ts';
import type { CatalogObject, WarehouseSession } from '../../../engine-client/src/warehouse.ts';
import type { QueryReader } from '../../../query-store/src/index.ts';
import { SessionExpired } from '../../../engine-client/src/warehouse-session.ts';
import { lazily } from '../lazily.ts';

/** How the page reaches Python's engine. */
export interface PythonLink {
  /** What every call's path goes under: a tab's own origin, or the widget channel's base (widget-loader.ts). */
  readonly baseUrl: string;
  /** What carries the calls: the page's fetch, or the widget channel's. */
  readonly fetch: typeof fetch;
  /** The `Authorization` header the engine asks for over HTTP (`Bearer <token>`); a notebook's channel needs none. */
  readonly authorization?: string;
}

/** What Python shows: a page of its own, by its key (`ll.Page`; `ll.show(df)`, a page of one grid over the frame: P12). */
export interface PythonShows {
  readonly page: string;
}

/** A plane as Python names it: its word, and what the planner menu says of it. */
export interface PythonPlane {
  readonly word: PlaneWord;
  readonly label: string;
}

/** What Python's host offers beside its frames (`services.json`): whether it reads a page's file, the plane it plans on
 *  (its own, in process: a host plans in process, the program's §1.1), and every plane it offers (that one). */
export interface PythonOffers {
  readonly files: boolean;
  /** Whether it reads a page's remote file (the httpfs extension it carries). */
  readonly remote: boolean;
  /** Where it keeps the page's saved pages (this user's folder), and whose they are. */
  readonly pages: { readonly where: string; readonly user: string };
  /** Where its model home keeps the projects (B3: the native library's SDLC over a git repository), when it has one. */
  readonly modelHome?: { readonly where: string };
  /** Whether it reads a warehouse's tables (C3: Python signs in as this user; the token never reaches the page). */
  readonly warehouse?: boolean;
  /** The apps it serves beside DataCube (E: `query`, `studio`), when it has their sites. */
  readonly apps?: readonly string[];
  readonly plane: PythonPlane;
  readonly planes: readonly PythonPlane[];
}

export interface PythonServicesOptions {
  readonly link: PythonLink;
  readonly shows: PythonShows;
  /** What Python said it offers (`askedOffers`), asked once as the page starts. */
  readonly offers: PythonOffers;
  /**
   * The page's settings as Python keeps them (`askedSettings`, asked once as the page starts), or none: Python keeps
   * them in this user's folder, where the browser's own storage, its address's, would lose them with each run's new
   * address (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B2).
   */
  readonly settings?: Record<string, unknown>;
  /**
   * How the page is told that Python moved what it shows: at once of the versions now, then as they move, or why
   * Python no longer says (a string). A tab asks (`askedVersions`, about once a second), saying where it is
   * (`follower`); a notebook's widget is told (its `versions`, or its frame's `version`). Returns what stops it.
   */
  readonly watch: (moved: (now: Versions | string) => void, follower: PageFollower) => () => void;
  /** The browser's own things where the page is: a tab's (its title too), a notebook's (no title of its own). */
  readonly browser: BrowserService;
}

// the one plane a Python host plans on: its own compiler, in process (a remote engine is a client feature over the remote
// side's own models and data, never a mode of the host: the user, 2026-10-15)
const WORDS: readonly PlaneWord[] = ['local'];

/** A plane Python said, read: its own, in process, or refused. */
function planeOf(said: unknown): PythonPlane {
  const p = said as { word?: unknown; label?: unknown } | null;
  if (p === null || typeof p !== 'object' || typeof p.word !== 'string' || typeof p.label !== 'string') {
    throw new Error(`the engine named a plane this page does not know: ${JSON.stringify(said)}`);
  }
  if (!WORDS.includes(p.word as PlaneWord)) {
    throw new Error(`a Python host plans on its own planner, in process: it named ${JSON.stringify(p.word)}`);
  }
  return { word: p.word as PlaneWord, label: p.label };
}

/**
 * WHERE THE PAGE PLANS, as Python says (§4.1, R3's "shown"): the plane Python plans on -- its own compiler, in process,
 * the one plane a Python host has -- and the planes it offers, that one, so a page asks for none.
 */
function pythonPlanes(offers: PythonOffers): PlaneService {
  // a plane offered here would be one the page could choose, which a Python host has none of: refused, never a menu
  // entry that asks to reload and then fails
  const other = offers.planes.find((p) => p.word !== offers.plane.word);
  if (other) throw new Error(`the engine offers ${other.label}, and a page on Python plans on Python's own planner only`);
  const asPlane = (p: PythonPlane): Plane => ({ id: `host.plane.${p.word}`, word: p.word, label: p.label });
  return {
    word: offers.plane.word,
    planes: offers.planes.map(asPlane),
    choose: (plane) => {
      throw new Error(`${plane.label}: a page on Python plans on Python's own planner, in process`);
    },
  };
}

/**
 * A PAGE OPEN ON ONE PYTHON KEEPS, as Python knows it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): the id it asks by,
 * and the version it shows (none while it opens or follows one). Python keeps the copies that version reads, and every
 * later version's, until the page says it moved past them -- so a page that has not followed yet still reads its copies.
 */
export interface PageFollower {
  readonly id: string;
  shown(): number | undefined;
}

/** A follower's id: random, never read by a person -- made where `crypto.randomUUID` is not too (a page on plain http). */
function followerId(): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(12)), (b) => b.toString(16).padStart(2, '0')).join('');
}

const headers = (link: PythonLink): Record<string, string> =>
  (link.authorization === undefined ? {} : { Authorization: link.authorization });

/** What the engine answered to `path`, as text, or -- refused -- why, in the page's words. */
async function asked(link: PythonLink, path: string): Promise<string> {
  const answer = await link.fetch(`${link.baseUrl}/${path}`, { headers: headers(link) })
    .catch((e: unknown) => { throw new Error(`the engine did not answer: ${String(e)}`); });
  if (!answer.ok) throw new Error(`the engine did not say what to show: ${answer.status} ${await answer.text()}`);
  return answer.text();
}

/** The tab's worker's requests a Python host is not asked, each with why: the tab's own, or no page's. */
const NOT_ASKED: Readonly<Record<Exclude<RequestKind, 'hold' | 'release' | 'pureV1' | 'planJson'>, string>> = {
  plan: "a page plans a query's protocol tree (planJson), not its text",
  relationType: "a page types a query on legend-engine's route (lambdaRelationType), not its text",
  planModel: "an unheld plan is Query's, in its tab",
  tableModel: "a table's model is the tab's own database's",
  catalogColumnsSql: "a catalog's reading is the tab's own database's",
  executePlan: "execute's halves are the tab's: a Python host plans and runs in one call (plan-and-run)",
  executeAnswer: "execute's halves are the tab's: a Python host plans and runs in one call (plan-and-run)",
  testData: "a model's test data is the tab's own database's",
  sessionSetup: "a session's setup is the tab's own database's",
  projectData: "a project's Data rows are held by Python itself (C5)",
  hostOperations: "a host's operations are the tab's own database's",
  warm: "warming the planner is the tab's",
};

/**
 * THE PLANNER'S WORKER ON A PYTHON HOST (§15.2, as amended 2026-10-15): the page's planner asks a Python host as it asks
 * any host -- a question about the Pure query on legend-engine's own route (`pure/v1`: Python answers it as every Legend
 * server does, and the tab's worker in process; a model's writing on legend-lite's, `lite/v1`, through the same kind),
 * and its dry run on the host's compute (`plan.json`, plan-and-run's planning half). Nothing else is sent: the worker's
 * other requests are the tab's own, refused here by name. No handle crosses the hosts (§15.3): a `hold` is kept here,
 * its model's text by the handle this gives, and the dry run sends that text, Python planning against the model it
 * holds of that text.
 */
function pythonWorker(link: PythonLink, project = false): PlannerWorker {
  const held = new Map<number, string>();
  let next = 1;
  let alive = true;
  const worker: PlannerWorker = {
    onmessage: null,
    onerror: null,
    postMessage(message: Request): void {
      const reply = (response: Response): void => { if (alive) worker.onmessage?.({ data: response }); };
      const { id, base: _base, ...body } = message;
      /** One call to Python, and the worker's answer made of Python's -- an answer it cannot read refused, never left
       *  unanswered; the call failing is the worker's death, as a crashed tab worker's is (the planner starts another). */
      const call = (path: string, sent: string, answered: (answer: globalThis.Response, text: string) => Response): void => {
        void link.fetch(`${link.baseUrl}${path}`, {
          method: 'POST',
          headers: { ...headers(link), 'Content-Type': 'application/json' },
          body: sent,
        }).then(async (answer) => {
          let made: Response;
          try {
            made = answered(answer, await answer.text());
          } catch (e) {
            made = { id, ok: false, error: `the engine's answer to ${path} could not be read: ${String(e)}` };
          }
          reply(made);
        }, (e: unknown) => {
          if (!alive) return;
          alive = false;
          worker.onerror?.({ message: `the engine did not answer: ${String(e)}` });
        });
      };
      switch (body.kind) {
        case 'hold': {
          const handle = next++;
          held.set(handle, body.model);
          reply({ id, ok: true, answer: `OK\n${handle}` });
          return;
        }
        case 'release':
          held.delete(body.handle);
          reply({ id, ok: true, answer: 'OK\n' });
          return;
        case 'pureV1':
          // legend-engine's own route, answered as Python answers it: its status, media type and body, as the planner's
          // route folds them
          call(`${body.path}${body.query === '' ? '' : `?${body.query}`}`, body.body, (answer, text) =>
            ({ id, ok: true, answer: `OK\n${answer.status}\n${answer.headers.get('content-type') ?? ''}\n${text}` }));
          return;
        case 'planJson': {
          const model = held.get(body.handle);
          if (model === undefined) {
            reply({ id, ok: true, answer: `ERR\nMODEL\nno model held as ${body.handle} on this page` });
            return;
          }
          // the dry run: plan-and-run's planning half, over the model Python holds of that text, never run -- a project's
          // own (a saved query's: ModelOptions.project) said so, as plan-and-run says it
          call('/plan.json', JSON.stringify({ lambda: body.lambda, model, runtime: body.runtime, ...(project ? { project } : {}) }), (answer, text) =>
            (answer.ok
              ? { id, ok: true, answer: (JSON.parse(text) as { plan: string }).plan }
              : { id, ok: false, error: `the engine answered ${answer.status}: ${text}` }));
          return;
        }
        default:
          reply({ id, ok: false, error: `${body.kind} is not asked of a Python host: ${NOT_ASKED[body.kind]}` });
      }
    },
    terminate(): void {
      alive = false;
    },
  };
  return worker;
}

/**
 * PLANANDRUN ON PYTHON (§15.2): the tab's planner over Python's worker for the compiler's questions, and one call
 * (`plan-and-run.json`) that plans the query against the model Python holds and runs its SQL on Python's DuckDB --
 * the page sends Pure, never SQL (R4). Python answers the database's rows as Arrow, the planner's answer in their
 * metadata; the page reads the plan as its planner reads its own, and the rows typed by it as the tab's executor types
 * the tab's (engine.ts typedByPlan, §15.3a). `worker`: the compiler's questions' worker -- Python's over the link, or,
 * DataCube's code run inside the host, the host's own (headless host.ts hostWorker).
 */
export function pythonRunner(link: PythonLink, model: string, runtime: string, how: ModelOptions = {},
  worker: () => PlannerWorker = () => pythonWorker(link, how.project === true)): PlanAndRun {
  const { planner, request, read } = planning(model, runtime, how, worker);
  const readPlan = (answer: string, query: Lambda): Plan => {
    try {
      return read(answer, query);
    } catch (e) {
      throw new QueryFailure(e, query);
    }
  };
  const planAndRun = async (query: Lambda, epoch: number, signal?: AbortSignal): Promise<{ plan: Plan; rows: ResultTable }> => {
    const started = performance.now();
    const answer = await link.fetch(`${link.baseUrl}/plan-and-run.json`, {
      method: 'POST',
      headers: { ...headers(link), 'Content-Type': 'application/json' },
      body: JSON.stringify(request(query)),
      ...(signal ? { signal } : {}),
    }).catch((e: unknown) => {
      if (signal?.aborted) throw e;
      throw new QueryFailure(new Error(`the engine did not answer: ${String(e)}`), query);
    });
    if (!answer.ok || (answer.headers.get('Content-Type') ?? '').startsWith('application/json')) {
      // no rows: the planner refused (its own answer, read as it reads every refusal), or its SQL did (and why)
      const text = await answer.text();
      const said = (() => {
        try {
          return JSON.parse(text) as { plan?: string; error?: string; expired?: { baseUrl: string; principal: string } };
        } catch {
          return {};
        }
      })();
      if (said.plan === undefined) throw new QueryFailure(new Error(`the engine answered ${answer.status}: ${text}`), query);
      const plan = readPlan(said.plan, query);
      // a warehouse's session ended (a table Python reads, C3): the tab's own error, so the cube offers to sign in again
      const cause = said.expired ? new SessionExpired(said.expired.baseUrl, said.expired.principal)
        : new Error(said.error ?? `the engine answered ${answer.status}`);
      throw new QueryFailure(cause, query, plan.sql);
    }
    // a body cut short or not Arrow: said as every other failure here is, never thrown raw at the cube
    const { table, metadata } = await answer.arrayBuffer().then((bytes) => arrowIpcAnswer(new Uint8Array(bytes))).catch((e: unknown) => {
      if (signal?.aborted) throw e;
      throw new QueryFailure(new Error(`the engine's rows could not be read: ${e instanceof Error ? e.message : String(e)}`), query);
    });
    const said = metadata.get('legend.plan');
    if (said === undefined) throw new QueryFailure(new Error('the engine answered rows without the plan they are of'), query);
    const plan = readPlan(said, query);
    // where the rows were read, as Python says it: a warehouse table's on the warehouse, its statement its own (C3)
    const theirs = metadata.get('legend.receipt');
    const receipt: Receipt = theirs !== undefined ? JSON.parse(theirs) as Receipt
      : { plane: 'engine', where: `the engine at ${hostOf(link.baseUrl)}` };
    try {
      const rows = typedByPlan(toRawTable(table, epoch, performance.now() - started), plan);
      return { plan, rows: { ...rows, receipt } };
    } catch (e) {
      throw new QueryFailure(e, query, plan.sql);
    }
  };
  return new PlanAndRun({
    name: 'python',
    planner,
    planAndRun,
    // the whole result as one chunk: streamed back in chunks is a follow-up (§15.7, 3)
    stream: async (query, epoch, onChunk, signal) => { onChunk((await planAndRun(query, epoch, signal)).rows); },
  });
}

/**
 * HOW A PAGE ON PYTHON PLANS: Python's own planner, over its planner protocol (the tab's planner over Python's worker,
 * read as the tab reads its module's) for a query's types, a parse and a print; the plan request Python's plan-and-run
 * takes; and the reading of the plan it answers with the rows. Over a mapping (a saved query's project, C5), the query
 * is planned as `->from(mapping, runtime)`, as Query sends it.
 */
function planning(model: string, runtime: string, how: ModelOptions, worker: () => PlannerWorker): {
  readonly planner: LitePlanner;
  readonly request: (query: Lambda) => {
    readonly lambda: string; readonly model: string; readonly runtime: string; readonly project?: true;
  };
  readonly read: (answer: string, query: Lambda) => Plan;
} {
  const planner = new LitePlanner({
    model, runtime, worker,
    ...(how.mapping === undefined ? {} : { mapping: how.mapping }),
    ...(how.enumerations === undefined ? {} : { enumerations: how.enumerations }),
  });
  return {
    planner,
    // a project's own rows (a saved query's: C5) said so, where the host reads them; any other model's, only the tables
    // the host serves (F28)
    request: (query) => ({ ...planner.planRequest(query), ...(how.project ? { project: true as const } : {}) }),
    read: (answer, query) => planner.readPlan(answer, query),
  };
}

/** A frame as Python says it is now (`cube.json`): the model Python wrote for it, and its version. */
async function frameOf(link: PythonLink, name: string): Promise<FrameModel> {
  const said = JSON.parse(await asked(link, `cube.json?table=${encodeURIComponent(name)}`)) as {
    model: string; runtime: string; source: ValueSpecification; version: number;
  };
  return { model: said.model, runtime: said.runtime, source: said.source, version: said.version };
}

/** The page's settings as Python keeps them (`settings.json`), or none kept. */
export async function askedSettings(link: PythonLink): Promise<Record<string, unknown> | undefined> {
  const said = JSON.parse(await asked(link, 'settings.json')) as unknown;
  return said !== null && typeof said === 'object' && !Array.isArray(said) ? (said as Record<string, unknown>) : undefined;
}

/**
 * The browser's own things, its settings kept by Python: read from what Python said as the page started, written to it
 * whole as they change, each write after the one before (two at once could land in either order, the older kept). A
 * write Python refuses leaves them for this visit, as a browser that refuses its storage does.
 */
function pythonBrowser(link: PythonLink, browser: BrowserService, kept: Record<string, unknown> | undefined): BrowserService {
  let settings = kept;
  let written: Promise<unknown> = Promise.resolve();
  return {
    ...browser,
    readSettings: () => settings,
    writeSettings: (values) => {
      settings = values;
      written = written.then(() => posted(link, 'settings.json', values)).catch(() => undefined);
    },
  };
}

/** What Python's host offers the page (`services.json`), or why it did not say. */
export async function askedOffers(link: PythonLink): Promise<PythonOffers> {
  const said = JSON.parse(await asked(link, 'services.json')) as {
    files?: unknown; remote?: unknown; pages?: { where?: unknown; user?: unknown }; modelHome?: { where?: unknown };
    warehouse?: unknown; apps?: unknown; plane?: unknown; planes?: unknown;
  };
  if (typeof said.files !== 'boolean' || typeof said.remote !== 'boolean') {
    throw new Error(`the engine did not say which of a page's files it reads: ${JSON.stringify(said)}`);
  }
  if (typeof said.pages?.where !== 'string' || typeof said.pages.user !== 'string') {
    throw new Error(`the engine did not say where it keeps saved pages: ${JSON.stringify(said)}`);
  }
  if (!Array.isArray(said.planes)) throw new Error(`the engine did not say which planes it offers: ${JSON.stringify(said)}`);
  return {
    files: said.files, remote: said.remote, pages: { where: said.pages.where, user: said.pages.user },
    ...(typeof said.modelHome?.where === 'string' ? { modelHome: { where: said.modelHome.where } } : {}),
    ...(said.warehouse === true ? { warehouse: true } : {}),
    ...(Array.isArray(said.apps) ? { apps: said.apps.filter((a): a is string => typeof a === 'string') } : {}),
    plane: planeOf(said.plane), planes: said.planes.map(planeOf),
  };
}

/**
 * PYTHON'S MODEL HOME (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3): the SDLC and its Depot Python serves
 * (`/sdlc/api`, `/depot/api`), the very rules the browser's in-page SDLC runs, over a git repository in this user's
 * folder; reached by the same clients, each call carrying the engine's token; and Studio's link to a project, where
 * Python serves Studio (the program's E).
 */
function pythonModelHome(link: PythonLink, where: string, apps: readonly string[]): ModelHomeService {
  const withToken = tokened(link);
  let home: ReturnType<ModelHomeService['connect']> | undefined;
  // Studio's editor on a project of this model home, where Python serves Studio (E) and the page holds the engine's
  // token (a tab's; a notebook's channel carries none): the link's first load trades the token for Studio's cookie
  const token = link.authorization?.startsWith('Bearer ') ? link.authorization.slice('Bearer '.length) : undefined;
  const studio = apps.includes('studio') && token !== undefined
    ? (project: string, workspace: string): string => `${link.baseUrl}/studio/demo/index.html?token=${encodeURIComponent(token)}`
      + `#/edit/${encodeURIComponent(project)}/${encodeURIComponent(workspace)}`
    : undefined;
  return {
    ...(studio ? { studio } : {}),
    // the clients fetched the first time a model action asks, as the tab's model home's are: the page's startup carries
    // neither
    connect: () => (home ??= Promise.all([
      lazily(() => import('../../../sdlc-client/src/client.ts')), lazily(() => import('../../../depot-client/src/client.ts')),
    ]).then(([{ SdlcClient }, { DepotClient }]) => ({
      client: new SdlcClient(`${link.baseUrl}/sdlc/api`, withToken),
      depot: new DepotClient(`${link.baseUrl}/depot/api`, withToken),
      where: `this user's folder (${where})`,
    }))),
  };
}

/** A call carrying JSON to Python: its answer's text, or -- refused -- Python's reason. */
async function posted(link: PythonLink, path: string, body: unknown, keepalive = false): Promise<string> {
  return postedText(link, path, JSON.stringify(body), keepalive);
}

/** A join.json call's page and grids, as JSON members. */
const joinOf = (page: string, mine: string, theirs: string): string =>
  `"page":${JSON.stringify(page)},"mine":${JSON.stringify(mine)},"theirs":${JSON.stringify(theirs)}`;

/** The same, its body JSON text already (a join's condition written as the protocol writes it: its numbers exact). */
async function postedText(link: PythonLink, path: string, body: string, keepalive = false): Promise<string> {
  const answer = await link.fetch(`${link.baseUrl}/${path}`, {
    method: 'POST',
    headers: { ...headers(link), 'Content-Type': 'application/json' },
    body,
    // a request that outlives its page (a copy let go as the tab closes)
    ...(keepalive ? { keepalive } : {}),
  }).catch((e: unknown) => { throw new Error(`the engine did not answer: ${String(e)}`); });
  const text = await answer.text();
  if (!answer.ok) throw new Error(text || `the engine answered ${answer.status}`);
  return text;
}

/**
 * Python's DuckDB as the page's database: its frames, and -- where Python takes them -- a page's file, read there by
 * DataCube's own code into the table the page named (the page's table names are its own: no grid's is replaced).
 */
export function pythonDatabase(link: PythonLink, offers: PythonOffers): DatabaseService {
  return {
    frame: (name) => frameOf(link, name),
    place: 'Python',
    // a saved cube opened by Python where it reads it, every kind but a frame's (§18.3; the S7 audit's H-2, H-3): this page
    // has no compiler or database of its own there
    ...(offers.files ? {
      opened: async (cube: string, page: string, gridsOnPage: number): Promise<HostOpened> => {
        // read exactly: the relation's numbers as their digits (a join condition's decimal)
        const said = fromJson(await posted(link, 'opened.json', { cube, page, gridsOnPage })) as {
          cube: string; notes: string[]; taken: boolean; model: string; runtime: string; relation?: unknown;
          how: { mapping?: string; enumerations?: string[]; project?: true }; frames: Record<string, ExactNumber>;
        };
        if (said.relation === undefined) throw new Error('Python opened the grid, and said no relation its rows are read over');
        const relation = readValueSpecification(said.relation);
        if (relation._type !== 'lambda' || relation.body.length !== 1) throw new Error('Python said a relation that is no query');
        return {
          cube: said.cube, notes: said.notes, taken: said.taken, model: said.model, runtime: said.runtime, how: said.how,
          relation: relation.body[0]!,
          frames: Object.fromEntries(Object.entries(said.frames).map(([name, v]) => [name, Number(v.text)])),
        };
      },
    } : {}),
    // a new grid's cube over a class, made by Python where it reads the class (K16, K17): this page has no compiler or
    // model home of its own there
    ...(offers.files ? {
      made: async (model: string | null, given: string, page: string, gridsOnPage: number): Promise<string> =>
        (JSON.parse(await posted(link, 'source.json', { model, given, page, gridsOnPage })) as { cube: string }).cube,
    } : {}),
    // a join of two of the page's grids, made by Python over its tables -- this page has no database of its own there
    // (B11): each answer DataCube's own code's, run there (headless join.ts), read as the protocol's exact JSON
    ...(offers.files ? {
      join: {
        sides: async (page: string, mine: string, theirs: string) => {
          const said = fromJson(await posted(link, 'join.json', { op: 'sides', page, mine, theirs })) as {
            left: { name: string; relation: unknown; columns: { name: string; type: string }[] };
            right: { name: string; relation: unknown; columns: { name: string; type: string }[] }; notes: string[];
          };
          const side = (s: typeof said.left): JoinSide => ({ name: s.name, relation: readValueSpecification(s.relation), columns: s.columns });
          return { left: side(said.left), right: side(said.right), notes: said.notes };
        },
        questions: async (page: string, mine: string, theirs: string, how: JoinHow) => (JSON.parse(await postedText(link, 'join.json',
          `{"op":"questions",${joinOf(page, mine, theirs)},"how":${toJson(how)}}`)) as { counts: JoinSummaryCounts }).counts,
        cube: async (page: string, mine: string, theirs: string, how: JoinHow, sheet: string | null) => {
          const said = fromJson(await postedText(link, 'join.json',
            `{"op":"cube",${joinOf(page, mine, theirs)},"how":${toJson(how)},"sheet":${JSON.stringify(sheet)}}`)) as {
            command: { document: unknown };
          };
          return readCube(said.command.document);
        },
      },
    } : {}),
    // a grid's Snap (§18.3): the pull sent as Pure (R4), as a plan-and-run's query is; Python plans it where the rows are
    // and keeps the copy, a table of its own it models as it holds the rows (DataCube's code, headless: where it takes
    // a page's file)
    ...(offers.files ? {
      snap: async (model: string, runtime: string, pull: Lambda) =>
        JSON.parse(await posted(link, 'snap.json', { op: 'snap', model, runtime, pull: toJson(pull) })) as HostCopy,
      // sent to outlive the tab: a grid's copy is let go as its tab closes (§18.8)
      release: async (table: string) => { await posted(link, 'snap.json', { op: 'release', table }, true); },
      // a copy Python made for its page (grid.snap(), §18.8): what the page follows the snap command with
      held: async (table: string) =>
        JSON.parse(await posted(link, 'snap.json', { op: 'held', table })) as InferredModel & { readonly table: string },
    } : {}),
    ...(offers.files ? {
      ingestFile: async (file: File, options: { readonly table: string }) => {
        // the bytes the tab's DuckDB is given (upload.ts fileParts: a text file decoded by its encoding, G4), sent a
        // part at a time as they are read (file-part.json, each saying where it starts), so no file is too big to send
        // and none is ever whole in the page; then read there, as one file (file.json)
        let upload: string | undefined;
        let at = 0;
        const send = async (part: Uint8Array): Promise<void> => {
          const said = JSON.parse(await posted(link, 'file-part.json', { ...(upload ? { upload } : {}), at, bytes: base64(part) })) as
            { upload: string };
          upload = said.upload;
          at += part.length;
        };
        for await (const part of fileParts(file)) await send(part);
        // an empty file is an upload too: Python reads it, and says what an empty file is
        if (upload === undefined) await send(new Uint8Array(0));
        return JSON.parse(await posted(link, 'file.json', { upload, name: file.name, table: options.table })) as UploadResult;
      },
      forgetFile: async (fileName: string, table: string) => { await posted(link, 'forget.json', { name: fileName, table }); },
    } : {}),
    // a remote file, read by Python's DuckDB through the httpfs extension Python carries; Python reads it only from a
    // remote URL, never from its own machine's files
    ...(offers.remote ? {
      mountRemote: async (url: string, s3: S3Credentials | undefined, name: string) =>
        JSON.parse(await posted(link, 'remote.json', { url, name, ...(s3 ? { s3 } : {}) })) as InferredModel,
    } : {}),
  };
}

/**
 * A WAREHOUSE ON PYTHON (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): Python signs in as the person running it (who is
 * the person at this page) and lists what they may read; a table opened, Python writes its model with the tab's own
 * writer and holds it, and the grid's queries are planned and run by Python (PlanAndRun, R4), Live on the warehouse.
 * The session the page holds names the warehouse and the person, never a token: Python keeps that.
 */
function pythonWarehouse(link: PythonLink): WarehouseService {
  return {
    // the address is typed each time: what this page offered last is kept by the tab's own warehouse service only
    offered: () => '',
    remember: () => undefined,
    connect: async (url, user, password) => JSON.parse(await posted(link, 'warehouse.json', { op: 'connect', url, user, password })) as
      { session: WarehouseSession; objects: CatalogObject[] },
    signInWithKey: () => Promise.reject(new Error('a page on Python signs in to a warehouse by its user and password')),
    table: async (session, object) => JSON.parse(await posted(link, 'warehouse.json', {
      op: 'table', url: session.baseUrl, catalog: object.catalog, schema: object.schema, name: object.name,
    })) as InferredModel,
  };
}

/** A fetch of Python's that carries the engine's token, for the clients of what it serves. */
function tokened(link: PythonLink): typeof fetch {
  return (input, init) =>
    link.fetch(input, { ...init, headers: { ...(init?.headers as Record<string, string> | undefined), ...headers(link) } });
}

/**
 * THE SAVED PAGES, in this user's folder (step 2d, B6): upstream's routes Python serves
 * (`/api/pure/v1/query/dataCube`), answered by legend-lite's own rules in its native library -- the ones the browser's
 * page runs compiled to WebAssembly -- over `ll.home()/pages`, through the one client. Kept on disk, so nothing more to
 * ask for to keep them.
 */
function pythonPages(link: PythonLink, offers: PythonOffers): PageService {
  return {
    store: new CubeStoreClient(`${link.baseUrl}/api`, tokened(link), offers.pages.user),
    where: `in ${offers.pages.where}`,
    memoryOnly: false,
    persist: async () => true,
  };
}

/**
 * Saved queries on Python's page (C5): the store Python keeps (`ll.home()/queries`, its `/api/pure/v1/query`, B5),
 * read as the tab reads its own -- its code fetched the first time it is asked for. A query's project opens by name
 * from Python's Depot; Python has no deployment projects of its own.
 */
function pythonQueries(link: PythonLink, offers: PythonOffers): QueryService {
  let store: Promise<QueryReader> | undefined;
  return {
    store: () => (store ??= lazily(() => import('../../../query-store/src/index.ts'))
      .then((m) => new m.QueryStoreClient(`${link.baseUrl}/api`, tokened(link)))),
    where: `in ${offers.pages.where}`,
    projects: [],
    fetchText: (url) => Promise.reject(new Error(`${url}: a page on Python reads no deployment's files`)),
  };
}

/**
 * The page Python serves (`page.json`), as the protocol's exact JSON reads it: a calculated column's `12.30` stays.
 * `follower`: the id the page open here asks by, each follow and write saying the version it shows (Z17).
 */
export function livePage(link: PythonLink, key: string, follower?: string): LivePage {
  const at = `page.json?page=${encodeURIComponent(key)}`;
  const as = follower === undefined ? '' : `&follower=${follower}`;
  const hashOf = (plain: { hash?: unknown }): string => {
    if (typeof plain.hash !== 'string') throw new Error('the engine said no hash of the page');
    return plain.hash;
  };
  /** The page as Python serves it now, read from the answer's exact JSON, every id it has held, and its hash. */
  const read = (text: string): LiveRead => {
    const said = fromJson(text) as { version: unknown; page: unknown };
    const version = said.version instanceof ExactNumber && said.version.isInteger ? Number(said.version.text) : NaN;
    if (!Number.isSafeInteger(version)) throw new Error('the engine said no version of the page');
    const plain = JSON.parse(text) as { held?: unknown; hash?: unknown };
    const { held } = plain;
    if (!Array.isArray(held) || !held.every((id) => typeof id === 'string')) throw new Error('the engine said no ids the page has held');
    // a page of no grid too: one written live is a page (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md, known limit 5)
    return { version, page: readPage(said.page, { empty: true }), held, hash: hashOf(plain) };
  };
  return {
    read: async (writer?: string) => {
      const text = await asked(link, `${at}${as}${writer === undefined ? '' : `&writer=${encodeURIComponent(writer)}`}`);
      const page = read(text);
      if (writer === undefined) return page;
      // asked by a writer: the last of its writes the keeper took (0: none), never left unsaid
      const taken = (JSON.parse(text) as { taken?: unknown }).taken;
      if (typeof taken !== 'number' || !Number.isSafeInteger(taken)) throw new Error("the engine said not which of this page's writes it took");
      return { ...page, taken };
    },
    report: async (version: number, write: LiveWrite) => {
      const answer = await link.fetch(`${link.baseUrl}/${at}&version=${version}${as}`, {
        method: 'POST',
        headers: { ...headers(link), 'Content-Type': 'application/json' },
        // its edits as DataCube writes them (a grid's cube, a calculated column's, its numbers exact), and its id
        body: `{"edits":[${write.edits.map((e) => toJson(e)).join(',')}],"writer":${JSON.stringify(write.writer)},"seq":${write.seq}}`,
      });
      // refused, in the keeper's words: nothing kept (any other failure is no refusal: the keeper is asked)
      if (answer.status === 409) throw new LiveRefused(await answer.text());
      if (!answer.ok) throw new Error(`the engine answered ${answer.status}: ${await answer.text()}`);
      const text = await answer.text();
      const said = JSON.parse(text) as { version?: unknown; hash?: unknown; page?: unknown };
      if (typeof said.version !== 'number' || !Number.isSafeInteger(said.version)) throw new Error('the engine said no version of the page');
      // the keeper made another page than this one sent: that page, which the screen is put at
      if (said.page !== undefined && said.page !== null) {
        const page = read(text);
        return { version: said.version, hash: hashOf(said), page: page.page, held: page.held };
      }
      return { version: said.version, hash: hashOf(said) };
    },
  };
}

/**
 * The versions Python says now (`version.json`) of the page it shows -- its own and each of its frames' -- or why it does
 * not say (the page closed, the engine gone). A page open here says, asking, who it is and the version it shows
 * (`follower`, Z17).
 */
export async function askedVersions(link: PythonLink, shows: PythonShows, follower?: PageFollower): Promise<Versions | string> {
  const shown = follower?.shown();
  const where = follower === undefined ? '' : `&follower=${follower.id}${shown === undefined ? '' : `&at=${shown}`}`;
  const query = `page=${encodeURIComponent(shows.page)}${where}`;
  const answer = await link.fetch(`${link.baseUrl}/version.json?${query}`, { headers: headers(link) }).catch(() => undefined);
  if (answer === undefined) return 'the engine stopped';
  if (answer.status === 404) return 'the page was closed';
  if (!answer.ok) return `the engine answered ${answer.status}`;
  const said = (await answer.json()) as { version: number; frames?: Record<string, number> };
  return { version: said.version, frames: said.frames ?? {} };
}

/** Python's services, over its link. */
export function pythonServices(o: PythonServicesOptions): Services {
  const { link } = o;
  // Python's compiler on legend-engine's and legend-lite's own routes (pure/v1, lite/v1): it writes a model, and reads a
  // query's text and a model's elements
  const compiler = new LitePlanner({ model: '', runtime: '', worker: () => pythonWorker(link) });
  // who this page is to Python, as it follows Python's page and writes it (Z17)
  const follower = followerId();
  return {
    engine: {
      plane: pythonPlanes(o.offers),
      label: o.offers.plane.word,
      // planned and run by Python: the page sends Pure, and the rows come back (R4). A mapping or enumerations would
      // change the plan, and Python plans over the model it wrote: refused rather than dropped. Bit columns are a
      // reading of legend-engine's types (relation-type.ts, S23), not of legend-lite's, which Python plans with
      rows: (model, runtime, how) => {
        // a mapping is a project's (a saved query's, C5): Python plans over a model it wrote, or a project's model
        // with Data rows in that project's own database (C5), and refuses any other.
        // PlanAndRun on Python (§15): planned against the model Python holds, by Python's own planner, and run on its
        // DuckDB, in one call
        return { runner: pythonRunner(link, model, runtime, how ?? {}) };
      },
      database: pythonDatabase(link, o.offers),
      // the model actions: Python's native library writes, a project's facts planned and run in Python (PlanAndRun)
      modelWriting: {
        place: 'Python',
        writer: compiler,
        rows: (model, runtime) => pythonRunner(link, model, runtime),
      },
      // Python's native library reads a saved query and its project's model (C5)
      reading: { parse: (text) => compiler.parse(text), elements: (text) => compiler.modelElements(text) },
    },
    pages: Promise.resolve(pythonPages(link, o.offers)),
    queries: pythonQueries(link, o.offers),
    ...(o.offers.modelHome ? { modelHome: pythonModelHome(link, o.offers.modelHome.where, o.offers.apps ?? []) } : {}),
    ...(o.offers.warehouse ? { warehouse: pythonWarehouse(link) } : {}),
    live: {
      watch: (moved, shown) => o.watch(moved, { id: follower, shown: shown ?? (() => undefined) }),
      page: livePage(link, o.shows.page, follower),
    },
    browser: pythonBrowser(link, o.browser, o.settings),
  };
}
