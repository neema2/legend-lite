# One connection: the trading desk the same on DuckDB and Deephaven (2026-10-28, revision 2)

## 0. The goal (the user, 2026-10-28)

"This whole demo should work with a single connection change -- and otherwise all the code is EXACTLY the same except
for market simulator code that writes the ticking data. Like everything else including even creating tables comes from
platform abstraction of duckdb and DeepHaven."

Measured the day before: the queries already are one implementation. The model (the one trading model, the desk's
`market.pure`: its 23 screens and the options' maths), the saved page (`screens.ts`), the desk's host (`desk.ts`) and the
whole DataCube app are the same on both, and every screen answers on Deephaven what DuckDB answers to the same call
(`//datacube:deephaven_corpus_test`, the desk's pass: 23 of 23).

## 1. Decisions (the user, 2026-10-28)

**D1. Lite's own connection words get a JSON form of lite's own; backward compatibility still refuses.** Three parts of
a lite connection had none -- the `Deephaven { host; port; useTls; }` datasource, the `SQLite { path }` datasource and
`PreSharedKey { keyVaultReference; }` authentication -- so a model holding one could not be written as legend's JSON:
DataCube could not read the project's elements (its saved queries, its class grids), and the model home could not make
an entity of the file. They are written as legend's extensions are (Snowflake's datasource its own `_type`):
`{"_type": "deephaven", "host", "port", "useTls"}`, `{"_type": "sqlite", "path"}`, `{"_type": "preSharedKey",
"keyVaultReference"}`, read back the same. Where legend-engine answers, lite answers as it does: legend-engine's
`grammarToJson/model` (E2) still parses at the engine's dialect and refuses the words in the engine's own message, and
`jsonToGrammar/model` still refuses to print them, by name. Lite's own `/api/lite/v1/grammar/grammarToJson/model` reads
a model at lite's dialect, and a planner of lite's reads a project's elements there. The compile, type and plan routes
read lite's words in a model's text already (their model is lite's product surface), and now read them in its JSON
alike; a client of legend-engine's never sends them. If legend-engine ships an extension with one of these `_type`s,
lite's aligns to it (as the SQLite grammar was spelled for: OWN_CORPUS_DECISIONS §9).

**D2. Environments are execution contexts of a data space; a saved page names the data space, its context a default
it can switch (the user, 2026-10-28: "option C yes with option to switch").** It is legend's own way of choosing what a
query runs against, and the compiler checks all of it. The desk's model gains one file, `environments.pure`: a
connection pair for each environment (the store `store::DB` and the world's `market::World`) -- `market::InTab` and
`market::InTabStatus` on the tab's DuckDB, `market::Deephaven` and `market::DeephavenStatus` on a Deephaven server
(`localhost:10000`, TLS, its key by vault reference: `PreSharedKey { keyVaultReference: 'deephaven.psk'; }`) -- a
runtime for each (`market::InTabRuntime`, `market::DeephavenRuntime`, both over `stress::AllMapping`), and a data space
`market::TradingDesk` whose execution contexts are "In this tab" (the default) and "Deephaven". More environments are
more contexts, each its own named connection (legend's prod and UAT). No address is typed in or substituted: another
server is another context.

- **A saved page** (its grids' saved queries) names the data space and the context it was saved on; opening it runs on
  that context where this page can reach it, and **Open With ▸** opens the saved page again, in a tab of its own, on
  another of the data space's contexts. *Revised by the user 2026-10-28, after the in-place switch (Run On ▸) crashed
  the page's renderer: "maybe it is way to complex to expect to switch in the middle of a page running - maybe it opens
  a new page instead", then "Open With is only for Saved things - its basically an Open for a saved Page that has a
  parameter".* The context is a parameter of that opening (the address's `#open=<id>&space=<path>&context=<name>`),
  never an edit: the page opened is not changed by it, and Save writes the page's own contexts. A saved page is one the
  store keeps or an example the site ships (the trading desk, `#example=<id>&…`: the user, 2026-10-29, "shouldnt that
  be a saved page?"), opened as saved or as shipped. The entry is never hidden: where it cannot open anything (a page
  never saved, no grid reading a saved query over a data space, a view that is no tab of its own) it is greyed out and
  its hover says why (the user, 2026-10-29: "menu items should not be hidden, they should be greyed out with a hover
  that says why disabled"). A context the page
  cannot reach (Deephaven on a static site with no server) is said by its grids -- never a silent fallback (AGENTS.md
  invariant 4).
- **Open With → "Deephaven at another address…"** writes a connection pair, a runtime and a context into the person's copy
  of the project, as "Create a Class…" writes a class (the model writer): versioned and checked, never a typed-in
  override.
- **The engine is the connection's**: DataCube runs a runtime on the engine its connection's type names -- the tab's
  DuckDB (live where a writer keeps the tables live) or a Deephaven server at the connection's address -- and asks the
  person to sign in where the connection's key is a vault reference this page does not hold (as the warehouse source
  asks; a key is never in a page's address). The engine wiring in the desk's hosts (`market/host.ts`,
  `market-sim/local-host.ts`) and `boot.ts`'s guess from the config move into DataCube.
- **A screen run on another context is in the cross-engine check** (`//datacube:deephaven_corpus_test`'s desk pass): the
  targets differ in recorded edge cases (docs/SEMANTICS_REGISTER.md).

**D3. Market data is a source of DataCube's, its feeds configured per part (the user, 2026-10-28).** In the source
picker, beside Files: **Market data**. It makes the market's tables in the tab's DuckDB from the model's store (the
platform's DDL) and keeps them filled live, each part from a feed the person chooses -- the prices (simulated; a live
crypto exchange, Coinbase's public feed; a market data source at a URL, a stream of rows in the model's columns; a file
replayed by its time column), the news (simulated, a URL, a file), the reference data and the bank's books (simulated,
a file). "Simulated" is every part simulated: the desk example's configuration, saved with its page. A feed refused (the
exchange unreachable) is said, the simulated one offered -- no quiet fallback. Later the same feeds write into a
Deephaven server's tables (with lite's writes to Deephaven: below).

**D4. Production: Deephaven is a remote database (the user, 2026-10-28).** A person connects to a Deephaven server as to
any remote database and reads the tables configured there; creating tables and views on that server through the
platform is a separate track (scripts using lite's executor), not DataCube's. The demo and the tests stand in for that
server: `desk.mjs` (a hand tool) and `//datacube:deephaven_desk_test` start one with the market's Groovy app; a person
with Deephaven on a laptop runs it with the demo's app and a local certificate, opens the same saved desk page, and picks
**Open With → Deephaven**.

**D5. A project is found in one place, the model home; a site's released versions are a read-only layer of it, loaded
from files (the user, 2026-10-28: "nothing is in prod yet so if that helps us delete projects[] ... that is fine
too").** DataCube and the Query app found a project two ways: a config's `projects[]` (a name and a list of files the
site serves: the trading demo, the desk) or the model home's Depot by name. `projects[]` goes. The page's model home (its
SDLC and Depot, lite's own rules in the page's WebAssembly) holds two layers: the versions the site releases, loaded
from its files at every open and never written to the browser's storage, so a site's update is never shadowed by an old
copy; and the person's own work in IndexedDB (workspaces, commits, versions of their own). Every client reads every
project through the Depot API it already speaks (`modelText`: a version's Pure files and their dependencies), so no new
API. Editing a released version (Create a Class…, Deephaven at another address…) starts a workspace from it, as Studio
does. Two rules: a released version is immutable (a workspace records the hash of the version it came from, and a
version changed under the same number is said by name: the publisher bumps it); and a person's version never takes a
released version's coordinates. The model home makes entities of a file at lite's dialect (D1), so a file holding
lite's own connections (environments.pure) is an entity like any other. The same Java serves a server's model home, so
a server loads its released versions from a folder the same way. The demos' configs and saved pages are rewritten to
it: nothing of `projects[]` is kept for compatibility.

## 2. What differs today

| Piece | DuckDB in the tab | Deephaven | After this plan |
|---|---|---|---|
| Connections (`market/model.ts`) | `MARKET_TAB` | `MARKET_LIVE` | `environments.pure`: both, a context each (D2) |
| The saved queries' context (`screens.ts`) | `market::LiveRuntime`, pinned | the same name, other connections | the data space, its saved default context (D2) |
| Engine wiring (`market-sim/local-host.ts`, `market/host.ts`, `boot.ts`) | lazy start, live readers | sign-in, bases, warming, reconnection | DataCube's, by the connection's type (D2) |
| Making the tables | the platform (the planner's DDL), started by the desk's host | the server's Groovy app | the Market data source (D3); the server as configured (D4) |
| Writing rows | the TypeScript simulator, started by the desk's host | the server's Groovy app | the Market data source's feeds (D3); the server as configured (D4) |
| Table shapes (`market-sim/tables.ts`) | the store's columns, copied | copied in the Groovy | read from the model (D3) |

## 3. The steps, in order

Each ends in a deletion where it can. Sizes are estimates, not measures.

1. **Lite's connection words as JSON (D1).** Landed.
2. **The environments and the data space (D2)** -- `environments.pure` (the connection pairs, the runtimes, the data
   space with "In this tab" and "Deephaven" on `localhost:10000`), the runtime out of `market.pure`, `model.ts`'s
   connection strings deleted. *Small.*
3. **Saved pages by the data space, with a saved default context and the switch (D2)** -- the desk's saved queries name
   the data space and its context; DataCube opens a page on its saved context and offers the others. *Small to medium.*
4. **Open With ▸ (D2, as revised)** -- after step 5 (an engine for every context first); "Deephaven at another
   address…" with the model home (D5, below). *Small to medium.*
5. **The engine by the connection's type, inside DataCube; a sign-in for Deephaven (D2)** -- the hosts' engine wiring
   moved into the app, `boot.ts`'s guess and the desk's hosts deleted (the example and the project stay the desk's).
   *Medium: the largest.* As designed (2026-10-28):
   - **Knowledge, not a question.** DataCube reads a project's elements before it asks for a grid's rows (a saved query's
     data space, a class's tree), and the elements hold every runtime (each store's connection, by name) and every
     connection (its database type, its datasource, its sign-in: lite's own words as JSON since D1). A reader of them
     (`runtimeConnections`) says what a runtime runs on; `rows(model, runtime)` stays as it is, its answer decided by
     the model's elements already read -- no new planner call.
   - **Deephaven:** the grids of a runtime whose connections are a Deephaven server's run there -- the Deephaven host's
     wiring (a reader each, the server's bases, warming the sheets not shown, reconnection) moved into DataCube, one
     engine for each server, its key by the connection's vault reference from the page's vault (the deployment's
     config; a sign-in prompt after).
   - **The tab's DuckDB:** as before -- planned and run in the tab, a project's own rows in a database of its own (C5)
     -- unless a source has made the store's tables live in the tab (the Market data source, step 6; until then the
     desk's simulator), whose grids then follow the live engine (the in-tab host's wiring, a reader each, resting while
     unseen, moved likewise).
   - **The desk's hosts** keep only what is the desk's: its project, its example (written on the deployment's context:
     the config's, else the data space's default) and, until step 6, starting the simulator. `boot.ts`'s guess (a
     market server named in the config) goes.
6. **The Market data source (D3)** -- in the source picker; its tables from the model (`tables.ts`'s copies deleted);
   simulated and the crypto exchange first, a URL and a file next; the desk example configured "simulated", the desk
   host's start of the simulator deleted. *Medium.* **As proposed (2026-10-28, waiting on the user)** -- D3 left three
   things open, each proposed here:
   - **Where a page keeps it:** a page-level list, `markets`, each one a market the page fills live in the tab: the
     project and the runtime whose stores it fills (the desk's `market::InTabRuntime`), and a feed for each part --
     prices, news, reference data, the bank's books -- `simulated`, `coinbase` (prices: the crypto names from
     Coinbase's public feed, the equities simulated, as today), later `url` and `file`. Saved with the page; the desk
     example writes every part `simulated`. A page with a market serves its runtime live in the tab (services
     `local.live.serve`, step 5), started when a grid on it first reads its rows.
   - **What the picker's Market data opens:** the projects whose model has a store a market fills (today the trader
     screens), the feeds chosen per part, Simulated by default; Open adds the market to the page and a grid over its
     tape (the store's `MARKET_TRADE` through the data space's tab context) -- the person then adds the rest as for any
     source. Until D5 the project is the page's own (the desk's files); after it, any in the model home.
   - **Where its code lives:** DataCube's own, `src/market/` (the world, its parts and the live binding moved from
     `demo/market-sim/`, about 2,300 lines), a chunk fetched with the first market a page opens. Its tables' columns are
     read from the model's store (the Database's columns and types: `tables.ts`'s column lists deleted); how each is
     written -- appended, upserted by its key, written once -- stays the feed's own knowledge.
7. **The tests** -- the in-tab desk lane on the simulated source; the Deephaven desk lane by the Deephaven context, its
   port from a test copy of `environments.pure`. *Small.*

**Then: the project in a model home (D5)** -- the page's model home loading released versions from files, `projects[]`
deleted (DataCube, the Query app, their configs and demos), the model home's entities at lite's dialect, workspaces
from a released version; "Deephaven at another address…" lands with it. *About 2-3 days.*

**Later, separate tracks:** lite's writes to Deephaven (the Market data source writing into a Deephaven server's
tables, which retires the Groovy app: a writer, input tables, measured first -- write rate, a created table's visibility
to other sessions, memory); Deephaven in the Database source (a server's tables listed, opened live); tables and views
created on a server through platform scripts (the Java executor's Deephaven sender, L3s); DH16 for tables other programs
publish (docs/DEEPHAVEN_START_HERE.md, open decision 5).

**Last: the demo runs without leaks, its bugs fixed** (the user, 2026-10-28) -- the desk left open for hours on
Deephaven without its memory growing or its cycles slowing, and the bugs a long run finds fixed: the server keeps a row
of every trade in about a hundred operations, a history cap costs the Stock tab's EMA, `restart` does not resume the
page, the Coinbase feed splices messages after a sleep. What was measured, and the unpushed feed fix of 2026-10-26:
docs/DEEPHAVEN_START_HERE.md, "the desk under load".

## 4. The licence

The later tracks stand on the licence questions' ground (docs/DEEPHAVEN_START_HERE.md, open decisions 1 and 2): making
input tables is section 4's ("no third party may define or change the schema of input tables": internal use is
unlimited), and the upload and input-table calls are more message definitions written from Deephaven's `.proto` files
(the Derivative Work question). Steps 2-7 add neither. The email to Deephaven gains a line: legend-lite would create and
write input tables through the public API.

## 5. Status

| Step | State |
|---|---|
| 1 | landed (2026-10-28): the three words written and read as JSON of lite's own; lite's `grammarToJson/model`; the tab's planner reads a project's elements there, so the desk reads its whole model (its corpus-only reading deleted, one cache of the whole model's elements kept: 2.3 MB of Pure). Checks: //core:core_tests (27 of 27), //datacube:verify_live_desk_test, //datacube:deephaven_desk_test, typecheck and bundle budget green. Left: Python's `model_elements` (`python/legend_lite/compiler.py`, still legend-engine's route: its test needs the native library built) |
| 2-3 | landed (2026-10-28): `environments.pure` (the connection pairs, `market::InTabRuntime` and `market::DeephavenRuntime`, the data space `market::TradingDesk`), the runtime out of `market.pure`, `model.ts`'s connection strings deleted (`DESK_MODEL`, `IN_TAB`, `ON_DEEPHAVEN`); the desk's saved queries name the data space, and the page the context it runs on as legend's `executionKey` on each (`src/run-on.ts`: `runOn`, `runningOn`) -- the desk example written on its host's context, a grid on the desk's other context refused by name; the corpus lane's desk cases and the market lane read the file, a test's server's copy at its port (`environments.mjs`). Every screen plans on both contexts, the same columns (`//datacube:desk_model_test`). Checks: the desk in the tab (`verify_live_desk_test`), the desk on Deephaven (`deephaven_desk_test`: its saved desk, on the Deephaven context, saved and reopened), the market lane and the corpus lane's desk pass (23 of 23) green |
| 5 | landed (2026-10-28). **Knowledge:** a model's elements read by the page's compiler once for the model read latest, and from them what each of its runtimes runs on, kept for every model read (`src/runtime-connections.ts`: `runtimeConnections`, `engineOf`, `runtimesOf` -- a runtime binding a Deephaven server beside another database, or two servers, refused by name). **The choice** (`src/runtime-engines.ts`, a chunk fetched with the first model read; `src/services/browser.ts` `tabEngines`, the stub at startup): a Deephaven server's runtime runs there (`src/deephaven-grids.ts`: one engine for each server, signed in to when a grid first reads its rows, its key the connection's vault reference read from the page's vault, config.json `vault`; each grid its own reader, the bases, the sheets not shown warmed, reconnection -- moved from the desk's host, which is deleted); a runtime a source made live in the tab follows the live engine (`src/live-in-tab.ts`, services `local.live.serve`; until step 6 the desk's simulator, `market-sim/local-host.ts`); any other is planned and run on the tab's DuckDB, as before. **The desk** keeps its project, its example (written on the context the deployment names, config.json `contexts`, else the data space's default; typed and valued through the services' rows like any source) and the market in the tab; `boot.ts`'s guess (config.json `market`) and `page-config.ts`'s `market` are gone -- the Deephaven site's config is its vault and its context. **Measured:** the startup download held under its budget (419,000; the engine choice a lazy chunk, server-base handed the page's base writer so the Deephaven chunk splits no startup file: 37 files, as before); the desk's first open, the previous tree against this one on this machine: in the tab 8.1 s then Markets drawn 1.5 s, now 9.5-10.7 s then 1.1 s; on Deephaven 6.0 s then 4.7 s, now 6.7 s then 4.9 s -- the elements read (0.7 s) is ahead of the page's writing now, where it came after it. A planner made for a server's runtime holds its model at once (pure/v1's typing compiles a model it does not hold at each call: the desk took 38 s to open before that). Checks: `//datacube:verify_live_desk_test`, `//datacube:deephaven_desk_test` (its Stock news check now waits for the cards and the grid to agree: a story crossing between the two reads failed it once), every DataCube unit test (189), typecheck and bundle budget green. Left: a sign-in prompt for a key the vault lacks; a source's options (`how`: a saved query's mapping, its enumerations) reach the tab's planner, not yet the live engines' (as before) |
| 4 | landed (2026-10-28), as the user revised D2: **Open With ▸** in a saved page's menu -- each data space the page's saved queries were read through, its contexts by their titles as the model declares them, the one this tab runs on checked (`src/app/open-with-menu.ts`, a chunk fetched with the first saved query read over a data space; the contexts recorded with the read: `saved-queries.ts` `DataSpaceRead`, `sources.ts` `dataSpaces`). Another chosen, the saved page opens in a tab of its own at `#open=<id>&space=<data space>&context=<name>` (`demo/boot.ts`, `datacube-app.ts`); there each saved query over the data space is read on that context (`sources.ts` `openedWith`), planned on its runtime and run where its connections say, and kept as saved: the page is not changed by it. Offered on an example the site ships too (the trading desk, `#example=<id>&…`, opened as shipped), and never hidden: greyed out where it cannot open anything, its hover saying why (the page menu's Open…, Save, Save As…, Share… and Create ▸ likewise since 2026-10-29). Replaced Run On ▸'s switch of the page on screen, which crashed the page's renderer (docs/DEEPHAVEN_START_HERE.md, the desk under load, 9): deleted, with its ledger allowance (L1_REACH 7 -> 6). Checked: `//datacube:deephaven_desk_test` three runs in a row -- Open With passed each (the other tab in a renderer of its own, its first tab drawn in 19.3-21.5 s and ticking from its market while this one ticks on Deephaven; no crash), the restart passed each; run 1 (the first after a container restart) failed two earlier checks of a linked view not following a pick within 30 s (the Stock tab's news cards, 9 against its grid's 8 rows; News's chart beside the panes) -- open. `run_on_test`, `one_api_ledger_test`, `typecheck_test`, `bundle_budget_test`. "Deephaven at another address…" waits for D5's page half (§6) |
| 6 | proposed (above), waiting on the user |
| 7 | not started |
| D5 | pieces 1-2 landed (2026-10-28). **The model home holds the site's released versions:** read from its releases folder at every open -- the page's from `config.json`'s `depot.releases` (an `index.json` naming each version's files: `datacube/demo/releases/`, `query/demo/releases*/`), a server's from `--releases DIR` -- and never written to the browser's storage (`sdlc-server` `ReleasedVersions`: Depot reads it before the SDLC's own, `over`; the SDLC refuses a person's version at a released version's coordinates, 409, `Sdlc.released`). A version's hash is its files'; its entities are read from its files at lite's dialect when first asked for (Studio's explorer, Depot's entity routes), never by a page reading its model's text (`modelText`): measured, reading them at every open cost the desk 2.7 s and about 600 MB of the page's renderer (0.19 s now). **`projects[]` deleted** from DataCube and the Query app, their configs and demos: both find a project in the model home only (`depot-client` `model-home.ts` `released`; the Query app's projects are its released versions, the version picker shown with them). Checks: `//sdlc-server:released_versions_test`, every DataCube test (209 of 210: the desk lane below), the Query app's, sdlc-client's, depot-client's and Studio's (20 of 20). The desk opens in 8.4 s on Deephaven (6.7 s before: the model now read through the model home's Depot). **Open:** the desk lane's renderer crashes at Run On's switch in 4 of 5 runs since D5, against 3 of 11 before it (no kernel kill, the renderer 1.7 GB with the parse deferred; 2.3 GB before): a SIGSEGV in a worker's generated code (docs/DEEPHAVEN_START_HERE.md, the desk under load, 9). **Run On is not reliable until this is fixed.** **Piece 3, the SDLC's half (2026-10-28, `released_versions_test`):** a workspace started from a released version -- `POST /projects/{g:a}/workspaces/{w}` with `{"releasedVersion": v}`, lite's own -- makes the person's project at the release's coordinates when there is none, then the workspace, its first commit the version's elements one file each (each read from its file by its span, its section's header before it: `CoreGrammar.elements`; a section importing what the compiler does not import by itself refused by name, an element's file holding no imports), and records the version's hash: the workspace's view says `releasedFrom {versionId, hash, changed}`, `changed` once the site changes the version under the same number. **Left:** the page's half -- how a saved page whose queries name a released version runs on a workspace started from it (proposed below, §6), and Open With's "Deephaven at another address…" over it |

## 6. Proposed: a saved page on a workspace started from a released version (D5's piece 3, the page's half)

Waiting on the user (2026-10-28). The SDLC's half is landed (§5, D5). What is open is how a page whose saved queries
name a released version (legend's saved query: `groupId:artifactId:versionId`, a data space, an execution context) runs
on a workspace started from it -- legend's saved queries name versions only, never workspaces.

**Proposed.** The page, not each saved query, says where a project is read from: `{project: g:a, workspace: w}`, a
page-level *source* beside its cubes (`page-document.ts`). Every saved query of that project the page holds is planned
over the workspace's model (`home.client.pure({project, workspace})`), its data space and context as saved; the saved
queries themselves are unchanged, so the page saved back to the released version is one line removed. The page's title
shows the workspace (as Studio's does), and `releasedFrom.changed` is said by name when the page opens ("version local of
org...:trader-screens changed under the same number since this workspace started from it").

**Deephaven at another address….** In Open With ▸, under the Deephaven context: asks for an address (host:port, TLS on
or off; the key stays the vault's reference); starts a workspace from the released version (named for the address:
`deephaven-<host>-<port>`) or opens the one already there; changes the context's connection element in it
(`pureChanges` MODIFY of `market::DeephavenConnection`'s text: its `host`, `port`, `useTls`); and points the page at
it (above). The engine choice (step 5) then reads the workspace's connections: nothing else changes.

**Questions for the user.** (1) A page-level source rather than a per-query one -- right? (2) One workspace per
address, reused -- or one per page? (3) Should "Create a Class…" on a released version use the same path (it starts
a workspace and points the page at it)?

