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
  that context where this page can reach it, and the **Run on** menu switches to another of the data space's contexts
  (switching plans the same queries on the other runtime: the model is held already). A context the page cannot reach
  (Deephaven on a static site with no server) is said, the switch offered -- never a silent fallback (AGENTS.md
  invariant 4).
- **Run on → "Deephaven at another address…"** writes a connection pair, a runtime and a context into the person's copy
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
**Run on → Deephaven**.

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
4. **The Run on menu, and "Deephaven at another address…" (D2).** *Small to medium.*
5. **The engine by the connection's type, inside DataCube; a sign-in for Deephaven (D2)** -- the hosts' engine wiring
   moved into the app, `boot.ts`'s guess and the desk's hosts deleted (the example and the project stay the desk's).
   *Medium: the largest.*
6. **The Market data source (D3)** -- in the source picker; its tables from the model (`tables.ts`'s copies deleted);
   simulated and the crypto exchange first, a URL and a file next; the desk example configured "simulated", the desk
   host's start of the simulator deleted. *Medium.*
7. **The tests** -- the in-tab desk lane on the simulated source; the Deephaven desk lane by the Deephaven context, its
   port from a test copy of `environments.pure`. *Small.*

**Later, separate tracks:** lite's writes to Deephaven (the Market data source writing into a Deephaven server's
tables, which retires the Groovy app: a writer, input tables, measured first -- write rate, a created table's visibility
to other sessions, memory); Deephaven in the Database source (a server's tables listed, opened live); tables and views
created on a server through platform scripts (the Java executor's Deephaven sender, L3s); DH16 for tables other programs
publish (docs/DEEPHAVEN_START_HERE.md, open decision 5).

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
| 2-3 | landed (2026-10-28): `environments.pure` (the connection pairs, `market::InTabRuntime` and `market::DeephavenRuntime`, the data space `market::TradingDesk`), the runtime out of `market.pure`, `model.ts`'s connection strings deleted (`DESK_MODEL`, `IN_TAB`, `ON_DEEPHAVEN`); the desk's saved queries name the data space, and the page the context it runs on as legend's `executionKey` on each (`src/run-on.ts`: `runOn`, `runningOn`) -- the desk example written on its host's context, a grid on the desk's other context refused by name; the corpus lane's desk cases and the market lane read the file, a test's server's copy at its port (`environments.mjs`). Every screen plans on both contexts, the same columns (`//datacube:desk_model_test`) |
| 4-7 | not started |
