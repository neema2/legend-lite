# DataCube: one app, every host (design, 2026-10-11)

Status: **agreed** (the user, 2026-10-11: "agree on all 6 ruling"; §13, §14). Step 1 and step 2a are built (§12). It
supersedes parts of four agreed designs, each named in §13 with what changes.

## 0. In one paragraph

DataCube is started in four places today: the browser tab (`demo/boot.ts`, 2,384 lines), Python's tab and notebook
widget (`demo/engine-page.ts` for a page, `demo/engine-cube.ts` for one cube), and an old page fixture
(`demo/page.ts`). Python headless runs pieces of it without a screen. They share the page and the grid (`PageApp`,
`CubePage`, `CubeApp`), but everything around them -- opening sources, saving pages, sharing, Create a Class…, the
planner menu -- lives in `boot.ts` alone, so Python's pages have none of it. This design makes **one DataCube app**
that every host opens. A host supplies only **services**: where queries are planned, where they run and where data
goes in, where pages are saved, where classes go, and where saved queries are. The browser tab, Python's tab, Python's
widget and Python headless become four sets of services for the same app. A feature is built once, and every host has
it. Python also becomes a full local host for legend-lite's apps: its own SDLC over a git folder, so Studio and Query
can start from Python too.

## 1. What binds this design (the user's words, 2026-10-11)

- "why would we ever want two wrappers at all?"
- "i dont really think there should be any of those differences - for example upload a file should just use whatever
  is configured as the Snap database (duckdb wasm for browser, pyhton duckdb for python) - same with choosing a planner -
  both modes can choose whatever planner they want - local is wasm in browser, native in python but can both choose
  remote if they want"
- "we also eventually want to start studio and query from python too", so where classes go must not differ by host.
- "for 'where the data is' again that is just configuration of where planner/database come from?"
- "i dont want to land anything here that does not 'stand alone'": each step of §12 lands on its own merits.

And the rules already agreed, which this design keeps:

- **DataCube's logic is DataCube's TypeScript, wherever it runs** (headless design §1). It is not copied into Python.
- **No fallbacks** (AGENTS.md invariant 4). A planner that does not answer is said, never substituted
  (`test/guardrails.test.ts`).
- **The page document is the contract** between hosts (Python pages design §3).
- **No WebAssembly in the Python process** (headless design §1).

## 2. What exists (read 2026-10-11)

**The shared layer.**
- `PageApp` (`src/page/page-app.ts`) draws the page: its bar and menu, sheets, layout, stacks, charts, export of the
  page file.
- `CubeApp` (`src/app.ts`) is one grid.
- A host hands `PageApp` a `GridMaker` per grid, and each `CubeApp` its rows: either `{planner, engine, live?}`
  (planned here, run here) or `{runner}` (planned and run elsewhere: `RemoteRun`).

**What only the tab's wrapper does** (`demo/boot.ts`):
- the source picker, and opening every kind of source: a file uploaded into DuckDB-WASM, an example, a remote
  Parquet/CSV/Iceberg file, a warehouse table, a saved query (`boot.ts` 1158-2198);
- reopening each kind for a saved page (1368-1458);
- the saved pages: Open…, Save, Save As… in IndexedDB (1248-1631);
- share links, and the start from a link, a page fragment or a launch key (152-167, 1637-1679, 2215-2272);
- the blank page; "changed since saved"; asking before leaving;
- settings kept in this browser;
- the planner menu (116-228);
- the Generated Pure & SQL window;
- Create a Class…, Create an Association…, Create a Data Space… and the Model menu (1770-1863).

**What Python's wrappers do** (`engine-page.ts`, `engine-cube.ts`):
- grids over Python's frames;
- every query planned and run by Python (`RemoteRun` over `LegendEngineExecutor`);
- the page followed when Python changes it, and reported back for `page.read()`.

They have none of `boot.ts`'s list. The engine page loads no compiler and no DuckDB-WASM (budget 300 KB). The widget is
one file of at most 500 KB, because a module imported from a blob URL can load no chunk.

**Python headless** runs DataCube's TypeScript in V8 with no screen:
- the planner is `LitePlanner` over the host's worker (the native library);
- the database is `DuckDbEngine` over Python's DuckDB.

It has grids (`view`, `relation`, `to_pandas`), not pages.

**Already one implementation, many places** (the pattern this design extends):
- **The SDLC.** `Sdlc.java`'s rules run in the browser (compiled to WebAssembly, kept in IndexedDB) or as a server over
  a real git repository (`GitStorage`: plain Java, no git program, no JGit).
- **The saved-query store.** `SavedQueries.java` runs in legend-lite's server (`--query-store DIR`); `query-store`'s
  in-page server is its port.
- **The planner's worker protocol.** `planner-protocol.ts` is answered by the tab's module or by Python's native
  library.

**The planner and the database are already independent in the code.** Any `Planner` pairs with any `QueryEngine`. Three
things tie them today:
1. the SQL dialect follows the model's connection type;
2. `RemoteRun` fuses the two (plan and run in one call);
3. opening a table needs the compiler's table-model writer.

## 3. The shape

### 3.1 The app

`datacube/src/app/` holds **everything a DataCube page does**:
- the page and its grids (`PageApp`, unchanged);
- the source picker and the opening and reopening of each source kind;
- the saved pages (Open…, Save, Save As…), share links and the page file;
- the start (what to open first);
- the blank page, "changed since saved", and asking before leaving;
- the planner menu and the Generated Pure & SQL window;
- the model actions (Create a Class…, Create an Association…, Create a Data Space…);
- the status line.

It is written against `Services` (§3.2) and nothing else of its host. It is the code `boot.ts` has today, moved and
cut loose from the tab.

One entry, used by every host that has a screen:

```ts
const app = await openDataCube(host: HTMLElement, services: Services);
```

### 3.2 The services (the whole of a host's job)

```ts
interface Services {
  /** Where queries are planned and run, and where data goes in (§4). */
  readonly engine: EngineService;
  /** Where pages are saved, and -- when another editor has the open page -- that editor (§5). */
  readonly pages: PageService;
  /** Where classes go: an SDLC and its Depot (§6). */
  readonly modelHome: ModelHomeService;
  /** Where saved queries are (upstream's query store API). */
  readonly queries: QueryStoreService;
  /** What to open first: the tab's address, the page Python hands over. */
  readonly start: Start;
  /** The browser's own things: download, clipboard, settings kept, files picked, confirm (§3.4). Absent headless. */
  readonly browser?: BrowserService;
}
```

**Every service is an existing client over a different transport**, never a second implementation:

| Service | Its client today | In a browser tab | On a Python host (tab or widget) | Headless (in Python) |
|---|---|---|---|---|
| engine: planner | `LitePlanner` / `UpstreamPlanner` | the tab's WebAssembly module (local), or a server | Python's: the native library (local), or a server, called by Python (§4.3) | `LitePlanner` over the host's worker |
| engine: database | `DuckDbEngine` | DuckDB-WASM | Python's DuckDB, behind Python (§4.3) | Python's DuckDB, through the host's connection |
| pages | `CubeStore` (upstream's DataCube query API) | IndexedDB (`RuleStore`) | a folder, served by Python (§5) | the `ll.Page` object itself |
| model home | `SdlcClient` + `DepotClient` | the in-page SDLC (WebAssembly + IndexedDB), or a server | Python's SDLC over a git folder, served by Python (§6) | Python's SDLC, through a host call |
| queries | `QueryStoreClient` | the in-page store, or a server | Python's store over a folder (`SavedQueries.java`) | the same, through a host call |

### 3.3 The hosts become

- **The browser tab** (`demo/main.ts`): the browser's services, then `openDataCube`. `boot.ts` goes; what is left is
  building the services from `config.json` and the address, about 150 lines.
- **Python's tab and widget** (`demo/engine.ts`, `demo/widget.ts`): Python's services over the token-guarded HTTP or
  the widget's channel, then `openDataCube`. `engine-page.ts` and `engine-cube.ts` go. `ll.show(df)` becomes a page of
  one grid (§13, R1).
- **Python headless:** the in-process services and the app's actions without its windows (§8).
- **Legend Query** keeps embedding one `CubeApp` (a grid inside another app, not a DataCube page). Nothing changes for
  it.
- **`demo/page.ts`** (the F6 fixture, before `PageApp`) goes. Its test, `verify_page`, stays and runs on the app: the
  sample grid and its copy on DataCube's own page. Those two-cube checks have no other home in a real browser.

### 3.4 What differs by host is a capability, said by its services

A host offers what its services can do, and the app shows what is offered, exactly as `boot.ts` already hides Save
when the planner opens no files. Examples:
- a warehouse source while Python's host does not reach one yet;
- a share link, which works only where the page's address is reachable by others: the tab's, not Python's loopback
  (Python's page offers the page file instead);
- the browser's own keeping of settings, absent headless.

**A capability is never a second path through the app:** the same code runs, and an absent service means its
entries are not offered.

## 4. Where queries are planned and run

### 4.1 The planner: chosen on every host, the same way

The planes are the same three words everywhere:
- `local`: the compiler in this host's process. That is the tab's WebAssembly module in a browser tab, and the
  native library on a Python host.
- `remote`: legend-lite's server.
- `engine`: legend-engine.

The rule of 2026-09-30 holds on every host:
- **chosen once**, when the app opens, in one place per host: the tab's `?planner=` (a Python host has one plane, its
  own: a host plans in process, the program's §1.1, the user's ruling of 2026-10-15; Python's `ll.show(page,
  planner=...)` is removed);
- **always shown**, in the status bar's readout;
- **never substituted**: a planner that does not answer is said, and the page stops.

Choosing another plane in the menu opens the app again on it (the tab navigates; Python serves the page again on the
new plane). The guardrail that today allows exactly one reader of `?planner=` (`demo/boot.ts`) becomes one reader per
host, in that host's services module.

### 4.2 The database: where SQL runs, and where data goes in

The database is the host's: DuckDB-WASM in a browser tab, Python's DuckDB on a Python host. A warehouse table is still
read Live on its warehouse, as the source's own engine. **Every way data comes in goes into the host's database:**
- a file uploaded (`upload.ts`'s ingest);
- an example generated;
- a remote file mounted (`remote.ts`);
- a Snap (`SnapManager`): the copy goes into the configured Snap database, which is the host's.

The project's place is named after the database: `Browser` for DuckDB-WASM, `Python` for Python's DuckDB (§6). The
route already takes any place name (`CreateClass.java`, `where.as`).

### 4.3 On a Python host, the page sends Pure, never SQL

The Python show design's safety rule is kept: **only SQL that Python's configured planner wrote is ever run on Python's
DuckDB**. Python's DuckDB can read the machine's files, so a page must not be able to send it SQL.

- **Plan and run.** On a Python host the app's grids use the host's `{runner}` shape, which `CubeApp` already takes.
  The page sends Pure; Python plans with its chosen planner and runs the SQL itself:
  - `local`: the native library, as today;
  - `remote` or `engine`: Python asks that server for the plan (`generatePlan`), then runs it on its DuckDB.
- **Data in.** On a Python host it is a call to Python, with the bytes, the URL or the snap's source. **Python answers
  it with DataCube's own code**, run headless in Python (`upload.ts`, `remote.ts`, `SnapManager`) against its DuckDB,
  so the tab and Python ingest a file the same way (the headless design's rule: DataCube's logic is its TypeScript,
  wherever it runs). The page never writes the SQL.
- **The headless host gains a `fetch`,** so DataCube's `UpstreamPlanner` (the `remote` and `engine` planes) runs in
  Python too. Headless design §3 left fetch out "since nothing in Python needs DataCube's HTTP clients"; the planner
  menu now needs one.

The browser tab keeps planning and running in-page, as today. Its DuckDB is inside the browser's sandbox, so the page
may write SQL for it.

**The tenet, stated precisely** (the user asked, 2026-10-11, whether "everything goes through Python native" was already
decided: it was, as the Python show design's rule). With the planner chosen on every host (R3):
- **the SQL Python runs is always written by the planner the person chose, and asked for by Python, never by the
  page**;
- on the `local` plane that planner is Python's native library, so everything does go through it;
- on `remote` it is legend-lite's server;
- on `engine` it is legend-engine, exactly as the tab's `engine` plane runs legend-engine's SQL in the tab's DuckDB.

No Legend protocol logic is written in Python for it: the remote planner is called by DataCube's own TypeScript, run
headless in Python (R6), and Python only carries the request.

### 4.4 Sources by host

The picker's sections come from the engine service. A host offers what its database can open:

| Source | Browser tab | Python host |
|---|---|---|
| a frame (Python's dataframe) | — (only Python has it) | yes |
| a file from the computer, an example | yes, into DuckDB-WASM | yes, into Python's DuckDB (§4.3) |
| a remote Parquet/CSV/Iceberg file | yes | yes |
| a saved query | yes | yes, once Python has its query store and model home (§6, §7) |
| a warehouse table (Live, Snap into the host) | yes | later: Python reaching a warehouse is its own piece ("the shared warehouse from Python", IN_FLIGHT) |

A saved page reopens each source the way its host can. A page over a frame opens on the Python that serves the frame,
and nowhere else, as today.

### 4.5 One way to open a source on every host (the engine service's contract, step 2a)

Step 1 gave the engine service the tab's own shape: a planner and a database in the page. A Python host has neither in
the page (R4), so before Python's pages open the app, the contract says what every host answers. The rule is that **a
source is opened by the same code on every host; only the services differ**:

1. **The data comes in through the database service**, which answers with the model it wrote for it: a file (ingest),
   a remote file (mount), a frame (Python's: the model Python wrote when the frame was registered). Each answer is the
   same `InferredModel`: its model, runtime, source relation and columns. The tab writes the model in the page (its
   module's table-model writer). Python writes it in Python, with DataCube's own code run headless (§4.3).
2. **A grid's rows come from the engine service over that model**, `rows(model, runtime, how?)`, in the shape `CubeApp`
   already takes:
   - in the tab and headless, planned here and run here: `{planner, engine}`, the planner over the source's own model
     (a page's grids share no model);
   - on a Python host, planned and run by Python: `{runner}`, the page sending Pure, never SQL (R4).
3. **The page's own compiler** (a saved query's text parsed, a query printed) is the engine service's `compiler`: the
   tab's planner, or Python's compile routes. It is never a planner whose `plan` refuses.

What only one shape has stays with that shape's service, never as a branch in the app:
- **Snap into the host's database**: the tab's local pair (`SnapManager`); Python's, a call to Python (step 2b).
- **The model writer of a warehouse table, and test data**: the tab's module; on Python with steps 2b and 3.

The app branches on the shape in one place only: when a grid is made, its rows are either a planner and an engine, or a
runner (`GridPlace`).

## 5. Where pages live, and who else edits an open page

"Who owns the page" is two separate things:

1. **Where pages are saved.** That is the page service's store: upstream's DataCube query API (`CubeStore`), the one the
   tab already uses.
   - In the tab, IndexedDB.
   - On a Python host, a folder (beside the model home: §6, Q1), served by Python with the same API, so Open… and Save
     work the same. `page.save(path)` and `Page.load` stay as they are.
2. **Another editor of the open page.** That is an optional `live` part of the page service: the page as the other
   editor says it is, a signal when it changes, and the page's reports back.
   - Only Python is such an editor today: `ll.Page` in code, and `page.read()`.
   - Its protocol is today's: `page.json`, `version.json`, the 409 on a stale report.
   - The app follows and reports through it whatever the host. This is `engine-page.ts`'s `follow`, `#report` and
     `#reopen`, moved into the app. The model now goes through with the page document, which today's report drops.

## 6. Where classes go: an SDLC on every host

A page's classes are a **project**: a model with its own history. A page pins the revision it was built on, Studio
edits the project, Query reads it, and it is promoted and published as a project. The thing that keeps projects and
their revisions is the SDLC. So **every host's model home is an SDLC**, reached by the same `SdlcClient`:
- in the tab: the in-page SDLC (WebAssembly, IndexedDB) or a server, as today;
- **on a Python host and headless: Python's SDLC**, the same `Sdlc.java` rules and the Depot's, built into Python's
  native library over `GitStorage`.
  - Python serves it at its own address (`/sdlc/api`, `/depot/api`) to the browser side.
  - Headless calls it in-process, as the browser's in-page SDLC turns each `fetch` into one call (`wasm-server.ts`).

**Python's is the browser's, in Python's terms** (Q1, the user's principle: "the python analogous version of what we did
there so its all consistent"):

| The browser's in-page SDLC | Python's |
|---|---|
| this site's storage in this browser profile (IndexedDB), shared by every page and app of the site (DataCube, Studio, Query) | this user's storage on this machine: the operating system's per-user data folder (macOS `~/Library/Application Support/legend-lite`, Linux `~/.local/share/legend-lite`, Windows `%LOCALAPPDATA%\legend-lite`), shared by every notebook and script of the user; `ll.home()` says where |
| the SDLC's records (git objects, refs, projects) kept as IndexedDB records | the same records as a bare git repository there (`GitStorage`): the same keys on disk, and `git push` works on it |
| a project made quietly at the page's first class, named after the page, in DataCube's group and workspace | the same: the same app code makes it |
| a page points to its project, pinned to its revision | the same |
| leaving it is deliberate: a bundle when a page travels, a push when it is promoted to a team SDLC | the same |
| a link shared says "its classes stay in this browser" | a notebook handed over points to a project only its author has, said the same way, with the same remedies |
| IndexedDB serializes what several tabs write | a lock around each SDLC call, held across Python processes (two notebooks at once) |
| the tab's config points the model home at a server instead | `ll.home(path)` points it at a folder of one's choosing (Studio's level 1: a repository beside the notebook, kept in its git) or a server |

Saved pages (§5) and saved queries follow the same rule: Python's per-user folder, as the browser's saved pages and
query store are per site.

The page document's `model` keeps one form, the SDLC's: the project, the workspace, the package, each grid's table and
class, and (owed already: model design §6 says so, `PageModelRef` has no field for it) the revision the page is pinned
to.

**The model held in the page itself** becomes only what model design §6 calls the bundle: what a page carries when it
travels as one file. It is never a home. Python's `create_class`, `create_association` and `create_data_space` (built,
parked on `datacube/python-model-parked` with 13 tests) come back on this home, through the same code the tab's windows
use.

The model home is per origin in a browser. On a Python host it is a server's (Python's), so the notebook's origin does
not matter.

## 7. Studio and Query from Python

With §6, Python is a local legend-lite host: it answers `pure/v1` and the lite routes (as today), the SDLC and Depot
(§6), the saved-query store (`SavedQueries.java` over a folder), and the page store (§5). Then:
- Python serves the site (DataCube, Studio, Query) on its loopback address, each app pointed at Python's services;
- `ll.studio()` and `ll.query()` open them, as `ll.show` opens DataCube.

Studio and Query are the Studio line's apps. This design asks of them only what they already take: their services'
addresses (Studio design S1, S21: "what answers it is configuration").

## 8. Headless: the app without its screen

Headless needs the app's actions, not its windows. So **every window's action has a core of its own**, and the window
only asks the person:
- Create a Class… is the window plus `makeClass(request)`;
- Save… is the window plus `savePage(name)`;
- an export is its menu entry plus `exportPage(format)`.

The window calls the core, and so does headless. This is the headless design's step 3 (the page engine) done piece by
piece as each window moves into the app, rather than as one separate extraction.

`grid.create_class`, `page.create_association`, `page.create_data_space` and `page.export` are those cores, called with
Python's in-process services.

## 9. What moves where

| Today | Becomes |
|---|---|
| `demo/boot.ts`: the sources, the saved pages, share and start, the model actions, the planes, the Pure & SQL window | `src/app/` (the app), each part a module of its own |
| `demo/boot.ts`: DuckDB-WASM, the planner worker, IndexedDB, config.json, `?planner=` | `src/services/browser.ts` (the tab's services) |
| `demo/engine-page.ts`: following and reporting the page | the app's live page (§5) |
| `demo/engine-page.ts`, `demo/engine-cube.ts`: frames, `RemoteRun` to Python | `src/services/python-host.ts` (Python's services over HTTP or the widget channel) |
| `src/headless/` | the headless services (in-process) and the cores' entry points |
| `demo/main.ts`, `demo/engine.ts`, `demo/widget.ts` | each: build its services, `openDataCube` |
| `demo/page.ts` | gone; `verify_page` runs on the app (demo/index.html) |
| `create-class.ts`, `model-actions.ts` | the app's model actions, over the model home service; each a core and a window |
| Python's `engine.py` | answers the new services' routes: the page store, the data-in calls, the SDLC and Depot, the query store, the plane |
| `native/` (the native library) | adds the SDLC and Depot rules over `GitStorage`, and the folder query store |

## 10. What it costs, and the risks

- **The largest refactor left.** `boot.ts` touches everything on screen. That is why it moves in steps (§12), the tab
  first, with no visible change in each step and the browser harnesses (`verify_*.mjs`) and screen tests green after
  each.
- **The widget's one file grows.** A blob-URL module loads no chunk, so the app's windows, lazy chunks in the tab, are
  inside the widget's file.
  - The widget measured 489 KB of its 500 KB budget when last raised (2026-10-09). The app's windows will push it past
    that; measured at step 2, the budget raised with its reason.
  - If the size bites, the loader can learn to fetch chunks over the channel. That is its own piece, later, on a
    measurement.
- **The engine page's startup** (300 KB) holds: the windows stay lazy chunks there, as in the tab.
- **Python's server grows** (§7). Each new route is the existing Java (SDLC, Depot, query store) or DataCube's own
  TypeScript run headless, never new logic in Python. Each is held to the same conformance suite as its other places:
  `sdlc-client`'s and `query-store`'s suites already run against more than one implementation.
- **Three runtimes in Python** (Python, the native library, V8) were measured and accepted in the headless design.
  This adds no fourth.

## 11. How it is kept proven

- **The app is tested once:** the screen tests (jsdom) and the browser harnesses open it over the tab's services, as
  they open `boot.ts` today.
- **Each host's services are held to one suite:** the page store, the model home and the query store each run their
  existing conformance suite against every place that answers them, Python's included.
- **Python's host is tested through the app:**
  - `python_engine_test` opens the app over Python's services, in a tab and in the widget;
  - the cases it runs today stay;
  - the app's features are added to it (a file uploaded into Python's DuckDB, a class made, a page saved and
    opened).
- **The headless lane** runs the cores, as it runs the grid today.

## 12. The plan (each step lands on its own)

1. **The tab is the app's first host.** No visible change. Four landings:
   - **1a.** `Services` and the browser's services. `boot.ts` builds them and reads its DuckDB, planner and stores
     through them.
   - **1b.** The sources (picker, opening, reopening) move into the app.
   - **1c.** The saved pages, share, start, blank page, "changed since saved", and leaving.
   - **1d.** The model actions (each a core and a window), the planes, the Pure & SQL window. `boot.ts` is then the
     tab's services. `demo/page.ts` goes, and with it the dead `if (models)` branches (both planners always return
     `models`).

   **As built (2026-10-11, branch `datacube/one-app-1a`):**
   - **1a** (e6d6ec56f): `src/app/services.ts` and `src/services/browser.ts`, and `boot.ts` reads everything through
     them. The dead `if (models)` branches fell out here. `config.json` is read once as the page starts, not again at
     each use.
   - **1b** (1f3fff381): `src/app/sources.ts`.
   - **1c** (ba2876221): `src/app/datacube-app.ts`, `openDataCube`, taking 1c's and 1d's moves together (the page, its
     saved pages, share, the start, the planes' menu, the Pure & SQL window, the model actions), since they share their
     state. The app builds its own windows (their markup and styles were `index.html`'s). The share link's address and
     the tab's title are browser services. `boot.ts` is the tab's host, 465 lines; it was 2,384.
   - **1d** (eadd0e854): `demo/page.ts` goes, its checks run on the app (`verify_page_test`: the sample grid and its
     copy).
   - **Not in step 1: each model action as a core and a window.** It comes with step 4, when headless is the second
     caller; a split now would have no caller.
2. **Python's pages open the app.** Four landings:
   - **2a.** `engine-page.ts` and `engine-cube.ts` go: the app over Python's services, the live page, `ll.show(df)` a
     page of one grid. Python's pages gain every app feature that needs no new service: the Pure & SQL window,
     settings, renaming, "changed".
   - **2b.** Data in: upload, examples, remote files and Snap into Python's DuckDB, answered by DataCube's code headless.
   - **2c.** The planes on Python: the headless `fetch`; the remote planners called by Python.
   - **2d.** The page store on Python: Open… and Save into a folder.

   **2a as built (2026-10-12, branch `datacube/one-app-2a`):**
   - **The engine service is §4.5's contract.** `rows(model, runtime, how?)` gives a grid's rows in the host's shape:
     the tab's a planner over the source's model and DuckDB-WASM, Python's a `RemoteRun` (R4). What only the page's own
     pair has (the sample's planner, the model writer of a warehouse table, test data, Snap) is the engine service's
     `local` part, absent on Python. A file and a remote file now open through `rows()`, so 2b gives them to Python by
     giving the database service's `ingestFile` and `mountRemote`, with no change to the app.
   - **Not built: the page's compiler as its own service (§4.5, point 3).** Its only reader is a saved query's text,
     which needs the page's own pair for its test data, so on Python it comes with step 3, its first Python caller (as
     each model action's core waits for step 4). Until then the parser is `local.planner`'s.
   - **The data in**: the database service's ways are each optional; Python's gives `frame` (`cube.json`). A frame is a
     source of the app's (`sources.openedFrame`), reopened from a page like any other. Its answer is a `FrameModel`
     (model, runtime, source, version), not §4.5's `InferredModel`: a frame has no conversions, no columns left out and
     no bit columns to say, and its version is what following reads. The grid's columns are typed the same way either
     way, by the rows' own planner or runner.
   - **Where the shape is read**: when a grid is made (`GridPlace` and CubeApp's options), and in the three helpers that
     build a `GridPlace` from `rows()` (`sources.ts`: `rowsPlace`, `typer`, `letGo`). The model actions read only grids
     that have a planner in the page (step 3 decides Python's).
   - **The live page (§5)** is a service of its own, `services.live` (`watch`, and `page` when Python serves one: `read`,
     `report`), beside the page store, since Python in 2a has the other editor but no store yet. The app follows it (a
     page's version: opened again on the sheet shown; a frame's: its grids query again, or open again over a new model)
     and reports the page once each change settles, a grid's own changes included, and once as it opens (so
     `page.read()` has it before anything changes, as before). `services.pages` and `services.queries` are optional: no
     store, no Save, Save As… or Open…, no New ▸ Blank Page where nothing could be put on it; no Saved queries section.
   - **Following, one behavior on both of Python's hosts**: a move that fails to be followed (a frame not read, the page
     not opened) is said in the status line and the title, and tried again at the next move; a success says nothing
     more of it. Told that Python no longer serves it (the page or the frame closed, the engine gone), the page keeps
     what it shows, says so, and reports nothing more. Before, the tab stopped at the first failure and the widget
     kept trying: the widget's way is kept, for both. The status line keeps saying it between errors (an error shows
     over it, and the next view brings it back), where the widget had a note of its own.
   - **What a person sees change on Python's pages**: the page's bar, sheets and menu, as in the tab; the title is
     `<name> – DataCube` (`(not followed: …)` after the name when it applies), not the name alone; a download's address
     is let go of five seconds after its click, as Python's pages did (the tab's did so at once until now).
   - **Known, not fixed here**: two notebook widgets on one page share the app windows' element ids (`#pure`, `#sql`) and
     the harnesses' globals (`window.__dataPage`); each widget reads its own through its element. An app whose start
     throws before it returns leaves its windows and its leave-the-page listener for the page's life.
   - **`ll.show(df)` is a page of one grid** (R1): the app's `frame` start, the page named by the frame, followed by the
     frame's version. Python is unchanged: the same links and widget state.
   - **The hosts**: `src/services/python-host.ts` (the services, over HTTP or the widget's channel), `demo/engine.ts` (a
     tab: asks for the versions about once a second while shown) and `demo/widget.ts` (a notebook: told by the widget's
     state; the app's windows float in the widget's own element, beside the page's). `demo/engine-page.ts` and `demo/engine-cube.ts` are gone. The browser's own things every host has are
     `src/services/browser-basics.ts`. The status line's look moved into the app (`.dc-app-status`, app.css).
   - **Budgets** (test/bundle-budget.test.ts): the widget 500 KB to 560 KB, as §10 said (measured 536,970). The
     engine page 300 KB to 380 KB (measured 366,792). §10's "the engine page's startup holds" did not survive R1: the
     page itself, its sheets and the app's sources are at startup, where one cube's tab loaded the grid alone. Making
     the source picker, the examples and the saved pages' windows lazy would trim the tab and the engine page both; it
     is its own piece, on a measurement. The tab 389 KB to 391 KB (measured 390,035, +1,979).
   - **Proven**: `python_engine_test` runs every earlier case on the app, plus: the frame as a page of one grid whose
     Pure & SQL window has the query sent and the SQL Python ran, in a tab and in a notebook widget; a frame closed in
     Python, its tab keeping its rows and saying it no longer follows. `marimo_test` names the re-run page by its frame.
     Not covered by a test yet: a stale report's 409 asserted (it is tolerated), a frame's model changing under a page
     (covered for a frame shown alone), and the live logic in jsdom.
   - **Build** (Compiler Rewrite's review): the Depot client is declared once, on `:src_base` beside the SDLC client,
     since only `src/` files import it; the bundles get it through `:src`.

   **2b, first part, as built (2026-10-12, branch `datacube/one-app-2b`): a page's file and the examples.**
   - **The page's side**: Python's database service gives `ingestFile` and `forgetFile` where Python says it takes files
     (`services.json`, asked once as the page starts: the `datacube` extra installed, its bundle where it is read). The
     bytes go as base64 in JSON (`file.json`; a part at a time since, `file-part.json`: "Any size" below), the same
     call over HTTP and over the widget's channel, which carries text. The app is unchanged: its Files and Examples sections appear because the service is there (§3.4), and the
     grid's rows come from `rows()` as a frame's do.
   - **Python's side**: Python writes the file's bytes, once, in the headless engine's folder under a name of its own,
     and hands DataCube's own code only that path (`src/headless/data-in.ts`). `upload.ts`'s `readFileInto` -- the one
     implementation of reading a file, which the tab's `ingestFile` calls too -- reads it into a table of its own on
     the frames' DuckDB and writes its model with legend-lite's writer. The bytes never enter V8; the table is a copy,
     so the file goes at once. The SQL names the file by its absolute path, never a bare name DuckDB would look for in
     the process's working directory first. `Frames` keeps each such table and its model beside the frames'
     (`reading_file`, `keep_file`, `file_table`, `forget_file`): the models `serving()` gives are still the only ones a
     query may be over (R4).
   - **The same bytes on both hosts, as the tab always read them** (the user, 2026-10-12: "everything to work as it
     does today"): `upload.ts`'s `fileBytes` is the one place a file's bytes are made for DuckDB -- Parquet as it is, a
     text file (CSV, JSON, JSON Lines) decoded as UTF-8 in the page (a byte-order mark taken off, a byte that is not
     UTF-8 replaced) and written out again, as the tab has always registered it. The tab registers those bytes, and
     Python is sent those bytes, so both DuckDBs read what the tab's always did: a Latin-1 CSV opens (its letters as the
     decoding replaced them), a JSON array with a byte-order mark reads as its records' columns (`upload_test`). Reading
     UTF-16, a gzipped file, or Latin-1 without the replacement would change today's behaviour: follow-ups, if wanted.
   - **Whose name**: Python chooses the table. It takes the name the page asked for when it is free, else the next free
     one (`<name>_2`, ...), and the answer says which; the page adopts it. Free means no frame's, no other file's (two
     tabs or widgets on one Python each get a table of their own), no table or view in any schema of the database, and
     never `__legend_lite_`, Frames' own. A frame cannot take a file's name either, nor does `show()` choose one. A
     page drops only a table one of its files was read into, by the name Frames gave it. The drop comes before the model
     stops being served, so a drop that fails leaves the table served, for the page to let go of again; a file DuckDB
     cannot read leaves nothing under its name.
   - **How large**: first measured 2026-10-12: a 192 MB CSV (256 MB as base64) was read in 0.8 s, the process peaking
     at 1.8 GB, the measuring script's own copies included (64 MB: 0.4 s, 0.75 GB). When the bytes went through V8,
     64 MB took 4.2 s and 3.5 GB. `file.json` then took a body of up to 256 MB.
   - **Any size, a part at a time** (the user, 2026-10-12: "do the chunking"):
     - The page sends the bytes as `upload.ts`'s `fileParts` reads them, in 4 MiB parts (`file-part.json`), each saying
       where it starts. Python appends each part to one file of its own; a part sent twice or out of order is refused,
       never written twice. `file.json` then names the upload and reads that file, as before.
     - No call carries more than one part, so no body limit binds a file, and over a notebook's channel no message is
       larger than a part. No file is ever one string in the page either: the text decoding streams.
     - Both hosts take their bytes from `fileParts`. The tab registers them whole (`fileBytes`). The decoding is the
       tab's own, as a stream: the same bytes as a `File`'s `text()`, a character split between two of the stream's
       chunks included (`upload_test`).
     - Measured 2026-10-12 in the pinned Chromium: a 359 MB CSV (9,000,000 rows) was sent and read on Python's page in
       21 s. The frames are held still while a file is read.
   - **Where this goes next (proposed to the dialect program, its D5)**: `readFileInto`'s statements -- read a file into
     a table, the conversions' rewrite -- are SQL a TypeScript app writes for a database that is not only the tab's.
     They belong in the dialect, as IR it renders (a create-table-as-select, a file-reading source), beside the
     catalog question that program already moves there; `upload.ts` would then run what the planner wrote, and Python
     would call the native library directly, with no V8 for a file at all.
   - **A limit found, not this step's**: a file whose rows hold nested values (a Variant column: the JSON example) is
     read in Python, but its queries are refused by Python's execute, which plans in the compatibility mode, where
     Variant's relational spelling is still unprobed ("probe legend-engine, do not guess"). The tab's own planner plans
     it. That is the plan's to settle (Plan Gen).
   - **Not in it**: Snap reaches Python with a warehouse it can read ("the shared warehouse from Python", IN_FLIGHT).

   **2b, second part, as built (2026-10-12): a remote file on Python** (the user: httpfs "hermetic in the build";
   remote schemes only).
   - **httpfs comes with the wheel, never downloaded**: MODULE.bazel pins DuckDB's httpfs extension (1.5.5, matching the
     duckdb wheel) per platform by content, gunzipped by the warehouse's Java action and laid out as DuckDB's own
     extension folder (`legend_lite/_duckdb/v1.5.5/<platform>/`, `//python:wheel_duckdb`). Python points its DuckDB
     there, with auto-install off and that same folder as the only repository: DataCube's `INSTALL httpfs; LOAD httpfs`
     finds it installed and loads it, and an extension not there is refused, never fetched. The macOS arm64 wheel is
     25.0 MB, 5.6 MB of it httpfs. Tests are given the folder as `LEGEND_LITE_DUCKDB_EXTENSIONS`; `wheel_test` proves an
     installed wheel reads a remote file with its own.
   - **Remote schemes only**: Python reads a page's remote file from `http(s)`, `s3`, `gs`, `r2` or `hf` URLs (no Azure:
     the wheel carries no azure extension), and refuses a path, `file://`, a Windows path or any other scheme before any
     SQL runs: Python's DuckDB can read this machine's files, and the page must not make it (R4's reason). The URL must
     begin with exactly `<scheme>://`, lower case, and hold no space or control character anywhere: DuckDB matches its
     remote file systems by that very prefix and reads anything else from this machine, so Python never judges a URL by
     its own, more forgiving reading of it (the audit's blocker: `HTTP://…` and ` http://…` passed a check on the parsed
     scheme, and DuckDB read them as local paths).
   - **When Python reads none, it says why**: without the `datacube` extra; on a DuckDB of the person's own (`Frames`
     given a connection: the extension settings would change it for good, so they are set only on a database
     legend-lite made); and when the installed duckdb is not the one the carried httpfs is for (the wheel takes any
     duckdb 1.5.5 and later, and httpfs is carried for 1.5.5: the refusal names both). A slow remote server holds the
     page's other queries while it answers, as every query over a remote view does: one connection, its queries one at a
     time. A file of this machine comes from the page
     through Files (its bytes), or from Python code. An `http://` URL may still name this machine's or its network's own
     servers -- a cloud machine's metadata address too -- which Python fetches without the browser's CORS rules: R4
     forbids this machine's files, not its network. Only the engine's own page, with its token, can ask.
   - **Each remote file's keys are its own**: one S3 secret per source, named by its view and scoped to its own path
     (`remote.ts`), so where one DuckDB serves several pages (Python's) one page's keys never replace another's.
   - **One implementation**: the tab's mount (a view over the file, its model from the catalog) is `upload.ts`'s
     `mountRemoteView`, which the tab and Python (`remote.json`, `data-in.ts`'s `mount`) both call. Python keeps the
     view and its model beside the frames' (`reading_file(view=True)`), under a name it chooses as for a file. The
     Remote section says who reads the file: the page (the server must let it: CORS), or Python.
   - **Not in it, waiting on a ruling**: Iceberg on Python, which needs DuckDB's `iceberg` and `avro` extensions too, about
     13-22 MB more per wheel; until then an Iceberg URL is refused by Python's DuckDB, its extension not there.
   - **Proven**: `headless_test` (Remote) reads a Parquet and a CSV file from a server on this machine, refuses every
     local form, and offers none without the extensions; `python_engine_test` opens a remote Parquet file through the
     app's own Remote section on Python's page; `wheel_test` reads one with the installed wheel's own httpfs.
   - **Proven**: `python_engine_test`, on Python's page, chooses a CSV in the app's own picker and reads its rows (the
     SQL Python ran reading the file's table), then opens an example the same way, the CSV's table let go of as its grid
     goes. `headless_test` (Files) covers a file read, served and let go of; two pages asking one name; names held by a
     frame, another table or a temporary table; names refused; a frame not taking a file's name; a header with quotes,
     a brace and a line break compiled into its model; a drop that fails; an unreadable file; an engine whose bundle is
     missing. `engine_test` covers an engine with no headless, which offers no files. Not covered by a test: a file
     over the widget's channel, `close()` dropping file tables, a body over the limit.
   - **Build**: the Python test's fixture takes `//python:headless`, and the test carries `:headless_bundle`
     (`LEGEND_LITE_DATACUBE`), as `//python:headless_test` does. `node_test` takes `platform_env`, a select merged last
     (Compiler Rewrite's review), which gives the test Windows' `PROCESSOR_ARCHITECTURE` as `//python`'s tests have it.

   **2c's seams, as built (2026-10-12)** (the user: no remote-engine calls in this phase, but the code "pluggable even if
   we never plug in the others").
   - Python's engine answers every call the PAGE makes that the compiler answers -- the engine's pure/v1 API: a query's
     types, a parse, a print, and an execute's plan -- through the one `Planner` it was made with, chosen once (R3);
     today `NativePlanner`, the native library, on the `local` plane. One on a server is another `Planner`, with no
     other change to the engine. Python's own calls in its process (a headless grid's view, `Frames.plan`) still plan
     with the native library: routing them through the same planner is 2c's too.
   - `services.json` names the plane Python plans on and the planes it offers (today its own), and the page builds its
     planner menu and its status word from them: no plane is written into the page. A Python host has one plane, its
     own (2026-10-15: a host plans in process, the program's §1.1; the server planners 2c built are removed), and the
     page refuses an offer of any other.

   **2d, as built (2026-10-12): Save and Open… on Python** (the user's choice of the two shapes: the page keeps the
   store's rules, Python keeps the records).
   - **Python's side** (`python/legend_lite/_home.py`): `home()` is this user's legend-lite folder (Q1: the operating
     system's per-user data folder, made private to its owner; `home(path)` points this process elsewhere, and wins over
     `LEGEND_LITE_HOME`, a folder for every process started with it, such as a test's own), and
     `SavedPages` keeps the page's saved pages in `home()/pages`: the browser's records, one `<id>.json` file each, the
     page's content kept as its exact JSON text and never read in Python. Four calls (`saved-pages.json`: all, find, put,
     remove), each record written whole (beside, then moved over), under a lock file several Python processes share;
     an id that could name another place, or a Windows device, is refused; a file that is not a record is left out,
     and said, so the others still open.
   - **The page's side**: `cube-store.ts`'s `HostRecords`, a third backend beside memory and IndexedDB, carries the four
     calls to the host, and the store's rules are `RuleStore`'s, unchanged. Python's services give `pages` from it; the
     save window and Open… say where (`in <home>/pages`), and its owner is `this user`, as the browser's is `this
     browser`. Open… on Python lists Python's pages, the tab's its browser's: one store per place, as Q1 has it.
   - **Proven**: `engine_test` (the four calls; ids refused; a write whole; the lock holding a second writer off;
     `home()`), `cube_store_test` (RuleStore over host-kept records, the content's exact numbers kept), and
     `python_engine_test`: Save As… on Python's page writes a record in the test's own folder, and Open… opens it again
     over its remote file.

3. **Python's model home.**
   - The SDLC, Depot and query store in the native library over Python's per-user folders (§6), served by Python.
   - Nothing is built for Create a Class… itself: the app's model actions are already on Python's pages from step 2,
     and appear once this service answers.
   - `grid.create_class` and the rest come back from the parked branch, as the cores of those same actions (§8).

**On Python, the work is services, never features** (the user, 2026-10-11: "i thought we were doing all of this work so
python gets all of this for 'free'"). Every feature's code comes with the app. What a host must build is only the
service a feature reads or writes: a database for upload, a page store for Save, a model home for Create a Class….
Until the service answers, its entries are not offered (§3.4).
4. **Headless pages:** the cores, `page.export`.
5. **Studio and Query from Python:** the site served by Python; `ll.studio()`, `ll.query()`. Built with the Studio line.

Each landing goes through an audit, the local gate, CI on the affected lanes on every platform, then main; Bazel
changes are reviewed by the Compiler Rewrite line, and planner-boundary changes by the Plan Gen line, as always.

## 13. Ruled (the user, 2026-10-11: "agree on all 6 ruling"; each supersedes something agreed)

- **R1. One app for every host.** It supersedes:
  - the Python show design's "a page of its own … so the demo's `boot.ts` … untouched";
  - the pages design's "a cube embedded alone (… a notebook …) stays a `CubeApp`".

  `ll.show(df)` becomes a page of one grid: its lone grid sits in the page's bar, as a one-grid page's does in the tab.
  Legend Query's embedded grid is unchanged.
- **R2. Python's model home is an SDLC** over a git folder (§6). It supersedes model design §8, §14 ("Python's a
  bundle by default and a project folder on request") and §15.5 ("Python's page is its model's home"). The bundle stays
  as the travel form.
- **R3. The planner is chosen on every host** (§4.1). It supersedes `guardrails.test.ts`'s single reader of
  `?planner=` in `demo/boot.ts`, replaced by one reader per host. The rest of the 2026-09-30 rule stays: chosen once,
  shown, never substituted.
- **R4. On a Python host the page sends Pure, never SQL** (§4.3). This keeps the Python show design's rule, already a
  tenet, stated precisely for every plane in §4.3. It is why the planner on a Python host is called by Python, not by
  the page. The alternative (the page plans in the browser and sends SQL to Python's DuckDB) would let any page that has
  the token run SQL against the machine's files.
- **R5. Data in goes to the host's database**, Snap included. It supersedes the show design's "the only feature that
  mode refuses is Snap into the tab, by design": on a Python host, Snap copies into Python's DuckDB.
- **R6. The headless host gains `fetch`** (§4.3), for the remote planners. It supersedes headless design §3's "the
  fetch API is left out".

## 14. The two questions, answered

- **Q1. Where Python's model home and page folder live by default.** The user's principle (2026-10-11): "python should
  mimic as close a s posible/as much as makes sense the decisions we made for browser/indeDB SDLC - it should be the
  python analogous version of what we did there so its all consistent". Applied: **the per-user store** (§6's table),
  the analogue of the browser's per-site storage. A folder beside the notebook is the opt-in (`ll.home(path)`), the
  analogue of pointing the tab at a server. This replaces the first revision's recommendation of a `legend/` folder
  beside the notebook by default, which no browser decision mirrors.
- **Q2. The order of steps 2 and 3.** The user, 2026-10-11: "i think it should become the app before yes". Step 2
  lands before Python has a model home; the model actions appear on Python's pages when step 3's service answers.

> **The rest of the program** (steps 3 to 5 widened: one implementation per store, one engine path, legend-lite as a
> drop-in legend-engine, the model plan's remaining steps) is `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`.

## 15. PlanAndRun: two twin hosts, one path (agreed 2026-10-12)

The user, 2026-10-12: "we have to think of Python and Browser as two compute 'hosts/platforms' with same capabilities
... these platforms should be twins"; "we need both paths to go through same exact code path - but as single 'atomic'
call"; "PlanThenRun still can configure what the planner is (local, remote lite, remote engine) and Run can configure
what runs it ... against what CONNECTION ... Actually the Connection come from the User (file, remote read, example,
REMOTE DATABASE)"; and, of this section's shape, "yes, please lets do it like this!". It supersedes §4.3's and §4.5's
`{runner}` shape for a Python host, and with it the Python show design's Python-as-legend-engine for DataCube.

### 15.1 Why

Until this section, the tab planned in the page (its planner over the WebAssembly module) and ran on DuckDB-WASM,
reading the rows itself, while Python's pages talked to Python as if it were legend-engine: `RemoteRun` over pure/v1's
execute in upstream's Arrow format. The compiler and DataCube's code were the same, the path was not:

- Python's answer had to match legend-engine's format type by type, and a type whose legend-engine spelling is not yet
  probed is refused: a Variant column's queries (core's `UpstreamRelationType.relationalSpelling`), which the tab plans.
- pure/v1 compiles the model again on every request (`PureV1Api`), where the tab plans against a held model.
- Every "works in the browser, not on Python" difference found in step 2 came from that one layer.

### 15.2 The shape

A grid's rows have three parts. Two are choices, and the third follows from them:

| | Chosen by | Options |
|---|---|---|
| **planner** (who writes the SQL) | the person, once per app (R3) | local (the tab's WebAssembly, Python's native library), legend-lite's server, legend-engine |
| **connection** (where the data is) | the source the person picks | a file, an example, a remote file: the host's DuckDB; a remote database (a warehouse table): that database |
| **runner** (who executes the SQL) | the host and the connection | DuckDB-WASM: the page; Python's DuckDB: Python; a remote database: its own server |

Every grid on every host is one call:

```
PlanAndRun(query) = plan   the chosen planner's plan function       (the same Java, Boundary, on every host)
                  + run    the connection's runner executes the SQL  (rows as the database answers them: Arrow)
                  + read   the page reads the rows, typed by the plan (the same TypeScript on every host)
```

- **Each host supplies two small pieces**: its plan step (the same function, compiled to WebAssembly or native) and its
  run step (this SQL on this connection, the rows as the database gives them). Neither holds logic of DataCube's.
- **On Python, PlanAndRun is one call** over HTTP or the notebook's channel: the page sends the query and its source;
  Python plans against the model it holds for that source, runs the SQL on its DuckDB, and answers the plan and the
  rows. The page reads both with the code the tab uses.
- **Every other question the compiler answers** -- amended 2026-10-15 (the user: "i thought the whole point is that
  browser, python, remote legend-lite, and remote legend-engine use the EXACT same API across all"; §15.10): a
  question about the Pure query alone (a query's columns, a parse, a print, a whole model's compile) is legend-engine's
  own request (`pure/v1`'s shapes), on every host -- the tab's worker in process, Python at `/api/pure/v1/...`, a
  legend-lite or legend-engine server over HTTP. Running a host's own data, and its dry run (the compile check), is
  PlanAndRun and its planning half (`plan.json` on Python). A remote server's data is its own standard
  `generatePlan` / `execute`. (Until 2026-10-15 Python's page sent the tab's worker requests over HTTP, `planner.json`:
  retired, with the allowlist and the handle translation it needed.)
- **The warehouse is a connection**, its runner the warehouse's server: Live is PlanAndRun there. **Snap is data in**
  (R5): rows read from one connection and written into the host's, the grid's connection the host's from then on.
- **legend-engine's answer format** is used only on the engine plane, by either host.

### 15.3 Models: each host holds its own (model design §9, §10, kept)

`com.legend.HeldModels` over `com.legend.cache.HeldStore` is the one implementation, in core, and each host owns its
instance: the tab's in its WebAssembly module, Python's in its native library. The server holds none (a request has no
holder; the Studio line, 2026-10-10). Planning against a held model took 6 to 14 ms at 1 to 200 classes, where a whole
compile took 54 ms to 3.8 s.

- The tab's planners hold their grid's model while the grid lives. Python's sources hold theirs while registered:
  a frame does today, and a page's file table and remote view will, in the same way.
- No handle crosses hosts: the page names its source's model, which Python holds, and Python plans only over the
  models it serves (R4). A page that vanishes leaks nothing in Python.

### 15.3a Every host's executor types its results by the plan (the user, 2026-10-12: "for now lets go this way")

Typing a result by its plan is an **execution** capability, not the compiler's: the compiler decides every column's
type when it plans (the plan carries it), and the executor of the language the values become -- the one that turns the
database's answer into that language's values -- puts the plan's types on them. One contract, each host's executor
implementing it in its own language, the conformance suite (15.6) holding them alike:

- a result's columns carry exactly the plan's types; a column the plan does not type is refused;
- each Pure type's values follow one rule on every host (an exact decimal stays exact; a timestamp is UTC to the
  microsecond; a Variant is its JSON value).

As it stands (measured 2026-10-12):

- **TypeScript's** executors keep `QueryEngine.execute`'s promise ("every column typed by the plan") through
  `engine-client`'s `typedByPlan`. Each column carries the plan's type, matched by name, and a column the plan does not
  type is refused. Each value stays exact, as stored (`values.ts`). A grid on Python reads Python's rows with that same
  reader, since its values become the page's.
- **Python's** executor, `Frames.run`, types every result through `frames.typed_by_plan`, under the same rules:
  - It covers `Table.execute`, `Grid.to_pandas()` (`gridRelation`'s Pure, planned in Python over the frame's held
    model) and PlanAndRun's rows before they go to the page. So the cube corpus's differential (15.6) runs through it
    too, then through the page's reader.
  - Each column carries the plan's type as its Arrow field's `legend.type`.
  - Values stay as stored, exact, with one exception: an Integer is an int64, a DuckDB SUM's decimal(38, 0) included.
    A value past 64 bits keeps that exact decimal. That is the column-wide counterpart of TypeScript's "a number while
    it is one, a bigint beyond".
  - `frames.to_pandas` (and `GridView.to_pandas`) keeps an Integer and a Boolean with nulls as pandas' nullable
    `Int64` and `boolean`, never a float or an object.
- **Java's** execution (core's `exec`, step K) is not yet the contract's reference. Its `ExecutionResult` names its
  columns by the plan by position, checking only the count. It keeps each value as the driver gives it, decoded by the
  plan's SQL type:
  - an INT32 stays an `Integer`, and a DECIMAL(38, 0) becomes a `Long`;
  - a DateTime is a zone-less `LocalDateTime`, as stored;
  - a DuckDB Variant is a `JsonNode`, its JSON text;
  - StrictTime has no case (`exec/Executor.java`).
  This was told to the Plan Gen line, 2026-10-12.
- **A Variant is one value: its JSON text (since 2026-10-12, 47349d82a).** Until then, a Variant stored as a STRUCT or
  a LIST (a nested JSON file's column) came back as JSON text only when the planner's SQL projected it
  (`CAST(t0.customer AS JSON)`). Under `SELECT *` (the bare
  source, a limit, a filter, a sort) the database answers the STRUCT. The tab's reader renders that as JSON
  (`toScalar`), which until 2026-10-12 quoted a nested integer (`{"tier":"2"}`, now `{"tier":2}`). Python's own
  results give a dict.
  - The fix is the planner's, so that every Variant is answered as the database's JSON and no executor renders JSON.
  - The Compiler Rewrite line took it (2026-10-12): the variant storage census's V2 hook (DuckDB's `columnRef`
    renders a stored Variant as `CAST(col AS JSON)`) never fires for a star frame, which names no column. So at the
    statement's egress, a star frame whose outputs include a stored Variant lists them by name. No other SQL changes.
  - Landed 2026-10-12 after PR #31 (47349d82a; GATES "A stored Variant column leaves the statement as JSON, whatever
    the shape of the query"): every shape now answers JSON (bare source, limit, filter, sort, distinct, select,
    extend, join, concatenate), and `frames_test` pins it for Python's own results.

Where this moves to (the Plan Gen line, 2026-10-12: "it's the right layer, it's already designed, and I wouldn't wait
for drift"): the plan already describes each result column as `ExecutionPlan.TdsResult`'s `TdsColumn(name, type,
sqlType)` -- its Pure type and the SQL type the database answers with, legend-engine's shape too -- and reading the
database's rows by that description is phase 3 of docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md (§8), after its step 4.
Its contract: one rule per (Pure type, sqlType) pair, kept as data all three hosts read, so Java, TypeScript and Python
apply one table rather than three decoders, Java's `ExecutionResult` the reference until then. The planners' plan answer
to DataCube (Boundary's `{"sql", "type"}`) carries no `sqlType` yet: when it does, it is `TdsColumn`'s, added by that
phase, never a second field. Until then the executors type by the plan's Pure types, under 15.3a's contract.

### 15.4 Who may send a runner SQL

- **The page's DuckDB-WASM**, in the browser's sandbox: the page.
- **Python's DuckDB**, which can read this machine's files: only SQL planned in the same PlanAndRun call. Raw SQL
  against Python, if it is ever wanted, is a capability of the planner -- a plan that passes the SQL through -- so this
  rule still holds (the user, 2026-10-12).
  - **Not yet by construction** (the audit, 2026-10-12). A planned query can name a file itself: Pure's
    `meta::legend::lite::sourceUrl('file:///…')` plans to `read_json_objects('<the local path>')`. So a page with
    Python's token can have Python read a local file through a model Python serves.
  - The same holds for Python's pure/v1 execute, which predates PlanAndRun. Python's `remote.json` already refuses a
    local URL for this reason.
  - **Accepted (the user, 2026-10-12)**: "its your datacube started from your python that could just read that file
    anyway". The person whose files they are runs that Python, and the rows show on their own screen.
  - What it would still matter for: a page someone else made, opened on your Python; or script injected into the page,
    which already holds the token and the frames.
  - So refusing `file:` when Python plans for a page is an optional hardening, not a blocker.
- **A warehouse**: the signed-in person's SQL, under the warehouse's own grants, as today.

### 15.5 What it replaces

- For DataCube's pages, Python's `RemoteRun` over pure/v1, and 2a's Python `rows()`. The engine service's two row shapes
  (`GridRows`) become one: PlanAndRun with a planner and a connection.
- 2c's seam moves into the plan step: a remote planner answers it, on either host.
- Python's pure/v1 API stays, for other clients and the engine plane. `python_engine_test` keeps its check that every
  cube case answers the same through it as in the tab.

### 15.6 How it is proven

One conformance suite: the cube corpus's cases, and files that test a reader (a byte-order mark, CRLF, Latin-1, gzip,
JSON Lines, nested values), run over every host, planner and connection the build can reach. The answers must be
identical in their columns, types and values, with the tab's local pair the reference.

### 15.7 Known differences, and their follow-ups (the user, 2026-10-12)

1. **The two DuckDB versions**: accepted for now. Follow-up: every DuckDB, the corpus tests' included, bumped to one 1.5
   release.
   - The tab's engine is DuckDB v1.5.4; `1.33.1-dev57` is only the npm package's own version (measured
     2026-10-12, `SELECT version()` in the pinned Chromium).
   - Python's is `duckdb 1.5.5`: one patch release apart.
2. **Python's one DuckDB connection is shared** by every page on its process, so their queries run one at a time:
   sharing is a feature. Follow-up option: a connection per page, to the same database.
3. **Large results from Python come back whole.** Follow-up: streamed back in chunks, as the tab's export reads them.
4. **Python's pure/v1 API loses DataCube as its daily user**: accepted, and still tested.
5. **A warehouse on Python holds the person's warehouse session**: acceptable, since the credentials are those of the
   person running that Python.
6. **No SQL from the page to Python**: if it is ever wanted, it is a planner capability (15.4).

### 15.8 The plan

1. **The runner**: PlanAndRun in `runner.ts`, from `PlanThenRun`. Each host's two pieces sit behind it. `DuckDbEngine`'s
   execute splits into the run (the database's rows) and the read (typed by the plan), and the read is shared.
2. **Python**:
   - one call (`plan-and-run.json`): plan over the held model, run, and answer the plan and the rows;
   - Python's own results (`Frames.execute`, `to_pandas`) typed by the plan (15.3a);
   - the planner protocol's requests carried over HTTP and the channel;
   - a page's file tables and remote views holding their models.
3. **The app**: every grid's rows through PlanAndRun. The warehouse's Live and Snap become a connection and data in.
4. **The conformance suite** (15.6).

The plan step's crossing to Python is planner protocol, so it is reviewed by the Plan Gen line and the Compiler
Rewrite line, then audited before it lands.

### 15.9 As built (2026-10-12, on datacube/one-app-2b, in PR #31)

- **The runner**: `runner.ts`'s `PlanAndRun` runs over a host's `PlanAndRunHost` (its planner, `planAndRun`, `stream`).
  The tab's host is `localHost(planner, engine)`: plan in the page, run on the connection's engine.
  - `PlanThenRun` is PlanAndRun over it. The tab's grids run through it: local files, a live warehouse, a snap's copy.
  - `lite-planner.ts`'s `planRequest` and `readPlan` are the one plan request and the one reading of a plan's answer.
    Every host uses them.
- **Python's pages**: each grid's rows come from `pythonRunner` (`services/python-host.ts`).
  - Planning: the page's own `LitePlanner` over `pythonWorker`. Hold and release are kept in the page, and a held kind
    sends the model's text, so no handle crosses hosts.
  - One `POST plan-and-run.json` per grid. Python plans against the model it holds for that frame or file
    (`Frames.held_for`), runs only that SQL (R4; a query's own `sourceUrl('file:...')` accepted: 15.4) and types the rows
    (`Frames.run`). Both new routes are private, POST-only, and always answered: what one does not answer is logged
    and said (500), never a dropped call.
  - The answer is Arrow IPC, the planner's answer in its schema's metadata (`legend.plan`). The page reads it as its
    planner reads its own. A refusal is JSON: the plan, and why.
  - Every other planner question goes over `planner.json`, answered by the native library from the planner protocol's
    table (`legend_lite/_planner.py`, moved out of `_headless.py`).
  - A page's file table and remote view hold their models while kept (`Frames.keep_file`), as a frame does.
- **Python's own results** are typed by the plan (15.3a). `Grid.to_pandas()` plans `gridRelation` in Python;
  the bundle's `gridSql` is gone.
- **Proof** (`python_engine_test`, 34 checks; `frames_test`; `headless_test`):
  - every cube case answers the same through PlanAndRun on Python and in the tab, SQL included;
  - six reader files answer the same on Python's page and in the tab, read by `upload.ts` into each host's DuckDB and
    planned and run on each: a byte-order mark, CRLF and quoting, Latin-1, dates and booleans, JSON Lines, and a
    BOM'd JSON array of nested values, its JSON pinned;
  - a nested-JSON example opens on Python's page;
  - each typing rule (each Pure type, a SUM past 64 bits, an untyped column refused, pandas' nullable types).
- **A page's file, a part at a time** (§4.3's "Any size"): `file-part.json`, then `file.json` names the upload; the
  256 MB body limit is gone.
- **One session on both hosts** (measured 2026-10-12): left alone, DuckDB-WASM took the browser's zone, and a fixed
  offset of it at that: `Etc/GMT+5` in New York, in October too.
  - So `now()` answered 16:28 in the tab and 21:28 on Python, and `today()` could differ by a date in the evening.
  - Now each host asks the compiler, as it sets up its DuckDB, what a DuckDB session runs (`Boundary.sessionSetup`:
    `SET TimeZone='UTC'`), and runs it. Nothing of it is written in either host.
    - The tab: services/browser.ts `setUpSession`, over the planner protocol's `sessionSetup` (the module's existing
      export `sessionSetupOrError`). The stress page does the same.
    - Python: `Frames`, through the native library's `lite_session_setup`.
  - `python_engine_test` now runs its browser in New York: every cube case, and the time now, answer alike.
- **Budget**: the grid page's gzipped script grew 390,817 -> 391,138 bytes (PlanAndRun and the shared plan
  reading), and its budget went 391,000 -> 392,000. Gzip-compressed files are 15.7's encoding follow-up, read by
  neither host today.

### 15.10 Plan and run where the data is (the user, 2026-10-15)

The user, stepping back over the twins PR (neema2/legend-lite#47): "can we pause and step back and think about the right
architecture and design holistically for browser, python, remote legend-lite, remote legend-engine". What was settled,
which is §15 itself stated across all four:

| A query's data lives in | Who plans it and runs it | On which contract | The page's runner |
|---|---|---|---|
| the tab's DuckDB-WASM | the tab | lite's own, in process: the plan function over a held model | `PlanThenRun` |
| Python's DuckDB | Python | lite's own, over HTTP: `plan-and-run.json`, and its dry run `plan.json` | `PlanAndRun` |
| a remote server's databases | that server (legend-lite's or legend-engine) | legend-engine's standard `generatePlan` / `execute` | `RemoteRun` |

- **A question about the Pure query alone** -- a parse, a print, a query's columns, a whole model's errors -- produces
  no SQL, so its answer does not depend on where the query runs: it is legend-engine's own `pure/v1` request on every
  host, the same API on all four.
- **Planning is for a database**, so it is the data's host's: our two twin hosts plan with lite's plan function over a
  model they hold (§15.1: legend-engine's formats, and a model compiled on every request, are why not `pure/v1`); a
  remote server plans and runs with its own standard calls. A dry run (the compile check) uses the same planner as the
  run it stands for: `plan.json` on Python, the held plan in the tab, a remote server's `generatePlan`.
- **No handle crosses hosts** (§15.3): the tab's handles are its worker's cache; Python holds models for the sources it
  serves, by their text.
- **Not settled here, and not in PR #47**: the remote case as a source in DataCube's own app (Query already embeds it:
  `query/src/app/cube.ts`), its dry run on the server's `generatePlan`, how Snap caches a remote result, and whether the
  mode where a remote planner writes SQL for the tab's DuckDB is kept -- a session of its own with the user.
