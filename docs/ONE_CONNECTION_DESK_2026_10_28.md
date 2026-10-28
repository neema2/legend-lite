# One connection: the trading desk the same on DuckDB and Deephaven (2026-10-28)

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

**D2. Connections are versioned model files; a tool may override one, explicitly.** A project holds its connections as
its own files, stored and versioned in the model home (backward compatible: a runtime names a connection, the compiler
checks it). A tool or a deployment may run a runtime on another connection -- the static site's on the tab's DuckDB, a
desk's on its Deephaven server -- through the compiler's execution overlay (`PureModelContext.withExecutionOverlay`: a
runtime and its connection laid over the compiled model as a view, never recompiled; what the service tests, the
module's plans and the cross-engine lane already run on). Four rules:

1. **Explicit, never a fallback** (AGENTS.md invariant 4): an override is named and shown on the page ("on Deephaven,
   the deployment's connection"); never "the project's connection failed, so the tab's".
2. **A page saves the project's runtime**, never an override: the override is the session's or the deployment's.
3. **Only the deployment or the person supplies one** -- never a page's content or an address's parameter (a link could
   send a person's queries to a server of its choosing). Credentials stay key-vault references.
4. **A screen run under an override is in the cross-engine check**: the targets differ in recorded edge cases
   (docs/SEMANTICS_REGISTER.md); the check is what makes a swap safe.

Add, never shadow (the overlay's own rule): an override is a named twin of the project's runtime (its mappings, the
override's connection), so nothing the model declares is silently redefined.

## 2. What differs today

| Piece | DuckDB in the tab | Deephaven | Its platform home |
|---|---|---|---|
| Connections (`market/model.ts`) | `MARKET_TAB` | `MARKET_LIVE` | the project's files (D2), the one swap |
| Engine wiring (`market-sim/local-host.ts`, `market/host.ts`, `boot.ts`'s branch) | lazy start, live readers | sign-in, bases, warming, reconnection | the app runs a runtime on the engine its connection names (D2's overlays) |
| Making the tables | the platform (the planner's DDL from the store) | `market.groovy` | the target's DDL (DH15's input tables), run by the session |
| Writing rows | the platform (the planner's inserts and upserts, Arrow appends) | `market.groovy`, in the server | a Deephaven writer: rows to an input table, appended or upserted by key |
| The simulator | TypeScript (`market-sim/`, about 2,000 lines) | Groovy (2,300 lines, a second copy) | one, the TypeScript, through the platform's writer |
| Table shapes (`market-sim/tables.ts`) | the store's columns, copied | copied again in the Groovy | read from the model |
| Address and key (`page-config.ts` `market`) | none | the server and a test key | the connection's specification and a key-vault reference (open decision 3: sign-in) |

The simulator already writes through one seam (`market.ts` `MarketSink(table, count, rows)`): on DuckDB the live
engine's append and upsert; given a Deephaven writer, the same simulator feeds Deephaven and `market.groovy` goes.

## 3. The steps, in order

Each ends in a deletion where it can. Sizes are estimates, not measures.

1. **Lite's connection words as JSON (D1)** -- the emitter and the reader, lite's own route, the tab's planner reading
   a project's elements there; `desk.ts` reads the whole model, its corpus-only reading deleted. *About 2-3 days.*
2. **Connections as the project's files (D2)** -- the desk's connections written as `.pure` files of its project (not
   TypeScript strings), the model home making entities of them at lite's dialect. *Small.*
3. **The app runs a runtime on its connection's engine; overrides by overlay (D2)** -- a Deephaven connection the
   Deephaven engine, a DuckDB one the tab's (live where the tables are live), each override named and shown; the two
   hosts' engine wiring moved into the app, `boot.ts`'s branch deleted. *About 2-3 days.*
4. **Deephaven's tables made by the platform** -- the session runs the target's DDL for the store's tables (input
   tables, keyed by the declared primary key). *Small to medium.*
5. **A Deephaven writer** -- a batch of rows to the server, added to an input table (appended; upserted by key). A
   browser cannot stream a gRPC call to the server, so the batch goes as Deephaven's own web client uploads one.
   **Measured first**: the Groovy world writes about 8,000 quotes a second inside the server; in 250 ms cycles over the
   network that is about 2,000 rows a write. Open: a bounded stream (the tape) -- input tables are not trimmed; a day's
   tape kept, or old rows deleted by key. *Medium to large: the risk is here.*
6. **One simulator** -- the TypeScript world writes through the platform's writer to either engine; for a shared
   Deephaven server it runs once, as a Node program (the same code), not in each person's tab; `market.groovy` deleted.
   *Medium.*
7. **The shapes from the model; DH16 strict for tables other programs publish** (docs/DEEPHAVEN_START_HERE.md, open
   decision 5: moot for the desk once the platform makes its tables). *Small each.*

## 4. The licence

Steps 4 and 5 are the licence questions' own ground (docs/DEEPHAVEN_START_HERE.md, open decisions 1 and 2): making
input tables is section 4's ("no third party may define or change the schema of input tables": internal use is
unlimited), and the upload and input-table calls are more message definitions written from Deephaven's `.proto` files
(the Derivative Work question). Building on this branch is unaffected -- nothing of it lands before the answer -- and
the email to Deephaven gains a line: legend-lite would create and write input tables through the public API.

## 5. Status

| Step | State |
|---|---|
| 1 | in progress (2026-10-28) |
| 2-7 | not started |
