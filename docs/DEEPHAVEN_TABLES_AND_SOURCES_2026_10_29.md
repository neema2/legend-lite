# Deephaven: every table described and set up by the platform, wherever its rows come from (2026-10-29)

**Status: decisions 2 and 3 taken (the user, 2026-10-29); decision 1 recommended after reading `###Persistence`
(§6a), awaiting the user.** Nothing below is built. It works out decision D6
(docs/ONE_CONNECTION_DESK_2026_10_28.md) as the user refined it on 2026-10-29.

## 0. In one paragraph

Every row in Deephaven comes from somewhere else: prices from a ticker plant, reference data from a reference master,
orders from the trading systems. The platform describes every Deephaven table and view in the model -- its columns,
where its rows come from, and how much it keeps -- and sets each one up on the server from that description. Who
writes the rows is a separate matter: Kafka, a loader, an app, a person, or in the demo the simulator. The hand-written
Groovy script stops describing tables; a startup program generated from the model makes them, and the simulator only
writes rows into them.

## 1. What the user decided

- **D6 (2026-10-29):** "on deep haven the platform is the way you describe/create tables even if it's outside of the
  datacube".
- **Refined the same day:** "all data into deephaven has to come from SOMEWHERE ... [the platform] must be able to
  describe/manage/configure/create all the 'tables' and 'views' for all of these data sources -- whether it's an
  input table or derived table or view on top or describing the Kafka data -- doesn't matter if the tables/views are
  persisted or not". And: "platform configuring tables/views does not also mean platform has to write the rows
  necessarily - it's okay if simulator/Kafka writes separately".
- **Still in force (2026-10-22):** Deephaven is just another backend. Its tables are declared in an ordinary
  `###Relational` store; legend-engine's Deephaven extension is not the reference (half built). See §4 for what that
  extension does, as a cross-check only.

## 2. Where a trading desk's data comes from in production

| Data | Where it really comes from | How it usually reaches Deephaven | How often it changes |
|---|---|---|---|
| Prices, quotes, the trade tape | Exchanges, through a market data vendor or the firm's ticker plant | The ticker plant publishes to Kafka; Deephaven subscribes to the topic | Thousands a second |
| News | News vendors | Vendor feed into Kafka | A few a second |
| FX rates | Live from market data; official fixings from reference data | Kafka; fixings loaded from a database | Many a second; daily |
| Reference data (instruments, exchanges, sectors, counterparties, books, desks, traders) | Security master, client master, org systems: relational databases | Loaded at the start of the day, corrections during it (reloaded, or change events) | Daily, small intraday changes |
| Orders, fills, routes, quote requests | Order and execution management systems | Event stream (often a FIX copy of every message) into Kafka | Tens to hundreds a second |
| Positions, P&L, risk limits | Position-keeping and risk systems: databases | A start-of-day snapshot loaded; intraday positions computed in Deephaven from it and today's fills | Daily, then live |
| What people enter (overrides, watchlists, limit changes) | People, through an app | Written into Deephaven input tables | Rarely |
| Derived (live P&L, VWAP, EMA, exposures, the desk's screens) | Computed in Deephaven from the rows above | Views over the other tables | Live |

## 3. Four kinds of table, by where the rows come from

| Kind | Rows written by | How the table exists on Deephaven | What the platform sets up |
|---|---|---|---|
| **Streamed** | Kafka (production); the simulator (demo) | Server code subscribes to the topic and keeps a live table | The table, its columns from the store, the topic and message format, the key, how much history it keeps |
| **Loaded** | A loader: Deephaven pulling from the source database or files, or the platform reading the source and writing the rows in | Server code reads the source, or an input table the platform writes | The table, the source (another store in the model and a query of it), when it refreshes |
| **Written** | Apps or people, through Deephaven's API | An input table: add-only, or keyed (a row replaces the one with its key; delete by key) | The input table, from the store's DDL (already written by the target: DH15) |
| **Derived** | Nobody: Deephaven computes it | Table operations over the other tables | The view, from a Pure function -- shared on the server (computed once, read by many) or per query (as DataCube's queries are today) |

Two facts about Deephaven (Community, the free edition we use) shape this:

1. **Streamed and loaded tables can only be set up by code running inside the server.** There is no API call for
   "subscribe to this Kafka topic" or "load this database": Deephaven's own docs do both in server-side Python or
   Groovy. So the platform can describe and configure them, but to create them it must hand Deephaven code -- a
   program generated from the model, run when the server starts (§5).
2. **Memory only.** A restart empties every table. The model is the one complete description, so the platform must be
   able to set everything up again from it. A startup program does that by itself: it runs again when the server
   starts.

## 4. How the model says it

Three layers, each in its own place:

1. **The table's shape** -- its columns, their types, its key: the store, a `###Relational` `Database`, as today. Every
   kind of table, the same way.
2. **Where its rows come from and how much it keeps** -- new: one description per table (or per group of tables, a
   "feed"): streamed (the topic, the message format, the key, the history kept: the latest N rows or the last T
   minutes), loaded (the source store and query, the refresh), written (keyed or add-only), derived (a function).
3. **Which environment uses which source** -- the runtime or execution context, as D2 has it: the demo binds the
   prices to the simulator, production to the real topic. The same tables and views, different feeds.

Legend precedents to align with, rather than invent beside:

- **`###Persistence`** (legend-engine; lite parses it today, its inner parts kept as raw text): "a service's output
  written into a target, on a trigger, in an ingest mode". It is close to *loaded* -- a query of the reference
  master's store, written into the Deephaven table, at the start of the day. To read in full before choosing.
- **Function activators** (legend-engine's Snowflake app, BigQuery function, hosted service): a Pure function made
  into something running on another system. legend-engine's Deephaven extension has one, `DeephavenApp`: it turns a
  function into a generated Deephaven application that creates the source table from the store's columns (with a
  writer other code fills) and the derived view from the function, and publishes both by name
  (`legend-engine-xt-deephaven-javaPlatformBinding-pure/.../deephavenAppGeneration.pure`, read 2026-10-29 at
  `1121404`). Not the reference (the 2026-10-22 ruling; it handles one source table and one query), but it reached the
  same shape: the model generates the startup program, something else writes the rows.
- **Kafka**: nothing in Legend. A lite addition, recorded in docs/SEMANTICS_REGISTER.md.

## 5. How the platform sets a table up on the server

Two routes:

| | A startup program generated from the model | Calls through Deephaven's API |
|---|---|---|
| What it can make | Every kind: streamed, loaded, written, derived | Written (input tables) and derived (views) only |
| After a restart | Comes back by itself: the program runs at startup | Gone; something must make it again |
| Names other sessions read | Stable, the server's own (`s/<name>`, as today's reads) | A shared name that lives only as long as the session that made it (measured on 42.7) |
| Changing a table | Deploy the new program and restart | At once |
| Who deploys | Whoever runs the server (an operator step) | Nobody: the platform does it |

**Proposal:** the startup program for the tables a server serves to everyone (all four kinds), so they come back on a
restart under stable names; the API for what one page makes for itself on the fly (DataCube's copies of a grid's base,
declared by the platform: D6). The program is generated, never hand-written, and only by internal users (the licence,
§4 of docs/ONE_CONNECTION_DESK_2026_10_28.md).

## 6. The demo under this design

- **The tables:** a startup program generated from the trading model makes all twenty, each as its kind says: the
  reference tables loaded (from the model's reference data), the market and the bank's flows streamed (fed by the
  simulator, standing in for Kafka), the world's status written. The three columns the strict check caught on its
  first run (DH16) could not drift: the program writes each column from the store.
- **The rows:** the market simulator stays the demo's feed -- inside the server, where it runs now -- but defines no
  table: it finds the tables the program made by name and writes into them. (Moving it out, the TypeScript simulator
  writing through the API, is possible later; at 8,000 quotes a second it would need measuring first.)
- **History:** each streamed table's kept history is in its description, so the trade tape's cap is a model setting,
  not a script variable (the desk's memory, docs/DEEPHAVEN_START_HERE.md "the desk under load", item 1).
- **In this tab (DuckDB):** unchanged. The same descriptions say what the tab's market makes there (step 6).

## 6a. `###Persistence`, read in full (2026-10-29, for decision 1)

Read in legend-engine at `1121404` (`legend-engine-xts-persistence`: the metamodel, its extensions, the relational
target and the grammar's tests). What it is, plainly: **"run this query, and write its result into that table, on
this schedule, in this way."**

- **`Persistence`**: a `trigger` (`Manual`, or a cron schedule), a `service` (a Legend Service: a query over a
  runtime -- the source), and `serviceOutputTargets`: the query's rows (`TDS { keys: [...]; deduplication: ...;
  datasetType: Snapshot | Delta }`) going into a target (`Relational #{ table: ...; database: ...; temporality: ...
  }#`), and a notifier.
- **How rows land:** a *snapshot* replaces the whole table; a *delta* adds or replaces rows by their keys (a delete
  marked by a column's value); *append-only* adds. Optional history columns (unitemporal, bitemporal milestoning).
- **`PersistenceContext`**: binds a `Persistence` to where it runs and its target's connection (`sinkConnection`) --
  the same description, a different server per environment.

**Fit, by kind of table:**

| Kind | Fit | Why |
|---|---|---|
| Loaded | **Good** | Exactly its job: a query of the reference master's store, written into the Deephaven table; snapshot at the start of the day, or deltas by key during it; the context names which server. On Deephaven: no history columns (Deephaven keeps none: refused by name), a delta is a keyed input table's add and delete by key, append-only an add-only input table's add |
| Streamed | **No** | Its source is always a query, never a topic and a message format |
| Written | **No** | There is no query: apps and people write the rows |
| Derived | **No** | A view is not written anywhere; it is computed |

## 7. Open decisions

1. **Where a table's source is described** (§4, layer 2). *Recommended after reading it (§6a):* **loaded** tables in
   Legend's own `###Persistence` (its query-into-table, snapshot or delta, schedule and per-server context, with no
   history columns on Deephaven); **streamed and written** tables in one new lite element, a *feed* -- the tables it
   fills and how: a Kafka topic and its message format (Legend's external formats, Avro or Protobuf, where one is
   declared), written through the API (input tables), or in the demo the simulator -- with each table's key and the
   history it keeps; **derived** views stay Pure functions (a shared view on the server, later). Which persistences
   and feeds an environment uses is bound beside its connections. The feed's exact words are proposed before anything
   is built. *Awaiting the user.*
2. **How tables are set up on the server** (§5): the startup program for shared tables, the API for a page's own.
   **Decided (the user, 2026-10-29: "Yes").**
3. **The demo's simulator** (§6): stays inside the server as the feed, no table definitions. **Decided (the user,
   2026-10-29).**

## 8. Order of work, once decided

1. Read `###Persistence` and settle decision 1.
2. The source description in the model, and its compile-time checks (every Deephaven table has a source; a source
   names only tables of its store).
3. The startup program generated from the model; the market's tables made by it on the demo's server; the Groovy
   script reduced to the simulator, writing rows only.
4. DataCube's copies declared by the platform (D6).
5. The trade tape's history cap as a model setting; the desk's memory measured again over a long run.
6. Then the per-pick EMA and step 6, as ordered in docs/ONE_CONNECTION_DESK_2026_10_28.md.
