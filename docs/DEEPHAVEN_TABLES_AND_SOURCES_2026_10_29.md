# Data products for the trading desk: where every table's rows come from, and the API over them (2026-10-29)

**Status: PROPOSED, for the user's review.** Nothing below is built. It works out decision D6
(docs/ONE_CONNECTION_DESK_2026_10_28.md: on Deephaven the platform describes and creates every table) on Legend's own
data products, ingest and data spaces, as the user steered it on 2026-10-29. The research it stands on is Appendix A;
every Legend fact there is read from legend-studio (`18a8f8f`) or legend-engine (`1121404`), not remembered.

## 0. In one page

Every row the desk shows comes from somewhere else: prices from a ticker plant, instruments from a security master,
orders from the order system. The design describes all of it in Legend's own terms, and adds only what Legend lacks.

- **Where data lands: ingests** (Legend's `###Lakehouse` `Ingest`). Each declares datasets -- their columns and key --
  and how rows arrive and land. Legend's ingests are pushed (a producer drops files) and materialized (a dataset defined
  by a function over other datasets). **Lite adds pull** (a dataset defined by a query of another system's store, on a
  schedule) **and streams** (a dataset fed live from a topic, keeping recent history or the latest row per key).
- **Into the class model: stores include ingests** (Legend's `include Ingest`). An ingest's datasets become a store's
  tables, so the trading model's classes and mappings keep working over them. **Classes stay** (the user, 2026-10-29).
- **The API: data products** (Legend's `###DataProduct`), one per domain, reference data split one level down. Each
  offers its classes through a model access group (its domain mapping) and, where useful, access points (functions).
- **The desk: a data space made of data products** (Legend's `mappingProvider`). Its execution contexts are the
  environments -- in this tab, Deephaven -- and its screens are the functions over classes they are today.
- **Where it runs: the environment's runtime.** Each ingest lands, and each query runs, on the database its runtime
  names: the tab's DuckDB, or a Deephaven server. On Deephaven the platform generates the startup program that makes
  every table; the market world's Groovy keeps simulating and writes rows only (D6).

## 1. What the user decided (2026-10-29)

1. **Strict type check (DH16):** "i think always strict". Built: docs/DEEPHAVEN_START_HERE.md item 5.
2. **D6:** "on deep haven the platform is the way you describe/create tables even if it's outside of the datacube";
   "all data into deephaven has to come from SOMEWHERE"; "platform configuring tables/views does not also mean platform
   has to write the rows necessarily - it's okay if simulator/Kafka writes separately".
3. **Set-up on the server:** a generated startup program for the tables a server keeps for everyone, the API for what
   one page makes for itself ("Yes"). **The demo's simulator** stays in the server, writing rows only.
4. **Shared views:** "We can phase shared views after but think about the design upfront - I would want pure/relation
   API not store view".
5. **An API, not a deploy:** "Think of this not as a deploy but as an API to the data that gives level of abstraction
   from physical table" -- which led to Legend's data products (Appendix A.2).
6. **The desk:** "Desk app will still be a DataSpace but a dataSpace made up of data products / access points".
7. **This document's three questions:** "Add pull too, products by sensible domains maybe even one level down from
   'reference'), keep classes please!"

Superseded on the way, kept in git history: a `###Pipeline` and a Kafka `###` store (2026-10-29: "Lot of new concepts
to juggle"), extending `###Persistence` with streams, a per-backend `DeephavenTable` activator, and a generic `Deploy`
element. Each reinvented a Legend concept under a new name.

## 2. Legend's concepts, as this design uses them

| Concept | What it is | Where it is in Legend |
|---|---|---|
| `Ingest` (`###Lakehouse`) | Datasets that land: columns, key, partitions, privacy; how rows arrive (`Snapshot` or `Delta` files, `CSV` or `Parquet`) and land (`append_only`, `batch_milestoned`, `batch_milestoned_business_temporal`); an owner; tests | legend-studio `IngestDefinition`; written `Ingest x Snapshot<CSV> owner=AppDir(...) [ T( c: Int ) pk=[ c ]; ]` |
| Materialized view | A dataset whose source is a Pure function over other datasets, tested | `FunctionSource`, `IngestMatViewTest` |
| `include Ingest` | A relational store whose tables are an ingest's datasets (schema the ingest's dataset group, table the dataset's), joined and mapped as any table | `includedStoreSpecifications` with `storeType: 'Ingest'` |
| `DataProduct` (`###DataProduct`) | The API: access points (functions), model access groups (a mapping, compatible runtimes, featured classes, secure views), owner, support, tests | legend-studio `DataProduct`; written `accessPoints: [ group [ id: LH(Snowflake, \|...) ] ]` |
| Access point read | `#P{product.accessPoint}#`, beside `#>{db.table}#` and `#I{ingest.dataset}#` | the value specification's accessor kinds `>`, `I`, `P`, `SQL` |
| Product composition | A mapping includes another product and joins to it across stores | `MappingIncludeDataProduct`, `xStore` associations |
| Data space over products | An execution context takes its mapping from a product's model access group; its queries run `->with(product)->from(runtime)` | `mappingProvider: { element, keys }`, `attachWithFromQuery` |

## 3. What lite adds (each a row of docs/SEMANTICS_REGISTER.md when built)

1. **Pull.** A dataset whose source is a query of another system's store, run on a schedule -- Legend's function
   source, its function allowed to read a store beyond the ingest's own datasets. The store's connection comes from
   the environment's runtime, like any query's.
2. **Streams.** A dataset whose source is a topic -- a table of a store reached through a Kafka connection (a lite
   connection type, as `Deephaven` is) -- read live.
3. **How rows land, for live data:** `AppendOnly` with the history it keeps (`keep=Last('8h')`, `Last(200000)`), and
   `LatestByKey` (one row per key, the newest).
4. **Refresh:** `Continuous`, `Every('15m')`, `Daily('06:00', 'America/New_York')` -- for pulls and materialized views
   (Legend's materialized views show no refresh setting).
5. **Live targets:** a Deephaven server and the tab's DuckDB, where ingests land and access points are served, beside
   Legend's Snowflake, Databricks, BigQuery and DuckDb. Chosen by the environment's runtime, never written in the
   ingest.

The exact words of 1-4 are proposed with the worked example (§4) and settled before anything is built; Legend's own
written forms are kept wherever they exist.

## 4. The desk, worked

### 4.1 The domains: the model's own seams

The trading model already splits by domain: each has its own class package and mapping file, joined by
`stress::AllMapping`, which includes them and carries the links between domains. The desk's products follow them,
reference data one level down:

| Product | Classes (the desk's) | Its mapping | Where its rows come from | How they land |
|---|---|---|---|---|
| `refdata::Markets` | `Exchange`, `Sector` | `refdata::RefdataMapping` | Pulled from the reference master | Snapshot, daily |
| `products::SecurityMaster` | `Instrument`, `Equity`, `Option` | `products::ProductsMapping` | Pulled from the security master | Snapshot, daily |
| `org::Organisation` | `BusinessUnit`, `Desk`, `Trader` | `org::OrgMapping` | Pulled from the HR and organisation systems | Snapshot, daily |
| `counterparty::Counterparties` | `Counterparty` | `counterparty::CounterpartyMapping` | Pulled from the client master | Snapshot, daily |
| `marketdata::MarketData` | `MarketQuote`, `MarketTrade`, `FxRate` | `marketdata::MarketdataMapping` | Streamed from the ticker plant | Trades appended, 8 h kept; quotes and rates latest by key |
| `marketdata::News` | `NewsStory` | (its share of `MarketdataMapping`) | Streamed from the news vendor | Appended, 24 h kept |
| `trading::OrderFlow` | `Order`, `Execution`, `OrderRoute`, `Rfq` | `trading::TradingMapping` | Streamed from the order system | Orders and RFQs latest by key; fills and routes appended |
| `positions::Positions` | `Book`, `Position` | `positions::PositionsMapping` | Pulled from position keeping at the start of the day | Snapshot, daily |
| `risk::Limits` | `RiskLimit` | `risk::RiskMapping` | Pulled from the risk system | Latest by key, every 15 minutes |
| `desk::Analytics` (the desk's own) | -- | -- | Materialized from the products above | Live (the shared views, phased after) |

News is its own product because its source, licence and readers differ from prices'. `Book` stays with positions,
where the model puts it. The desk-only `world` status table is the market world's, not a product.

### 4.2 An ingest of each kind (proposed words; Legend's where they exist)

```pure
###Lakehouse
// STREAMED: the ticker plant's topics (in the demo, the market world's simulator)
Ingest marketdata::MarketDataFeed Stream owner=AppDir(production='...')           // Stream: lite
[
  MARKET_TRADE = |#>{tickerplant::Topics.MARKET_TRADE}#  pk=[ PRINT_ID ]  write=AppendOnly  keep=Last('8h');
  MARKET_QUOTE = |#>{tickerplant::Topics.MARKET_QUOTE}#  pk=[ QUOTE_ID ]  write=LatestByKey;
  FX_RATE      = |#>{tickerplant::Topics.FX_RATE}#       pk=[ RATE_ID ]   write=LatestByKey;
]

// PULLED: the security master's tables, each morning
Ingest products::SecurityMasterLoad Pull<Daily('06:00', 'America/New_York')> owner=AppDir(production='...')   // Pull: lite
[
  INSTRUMENT = |#>{secmaster::DB.INSTRUMENT_V}#  pk=[ INSTRUMENT_ID ];
  EQUITY     = |#>{secmaster::DB.EQUITY_V}#      pk=[ EQUITY_ID ];
  OPTION     = |#>{secmaster::DB.OPTION_V}#      pk=[ OPTION_ID ];
]

// PUSHED: Legend's own form, unchanged -- a producer drops files
Ingest positions::StartOfDay Snapshot<Parquet> owner=AppDir(production='...')
[
  POSITION( POSITION_ID: Varchar(200), QUANTITY: Double, ... ) pk=[ POSITION_ID ];
]
```

A dataset defined by a function takes its columns from the function's result -- no column list written twice -- and
the strict check (DH16) holds what lands to them.

### 4.3 Into the class model: the trading store includes the ingests

```pure
###Relational
Database store::DB
(
  include Ingest marketdata::MarketDataFeed
  include Ingest products::SecurityMasterLoad
  include Ingest positions::StartOfDay
  ...
)
```

The domain mappings stay as written: `marketdata::MarketdataMapping` maps `MarketTrade` to `MARKET_TRADE` as today.
What changes is where `MARKET_TRADE` is declared -- by the ingest, which knows where its rows come from, rather than by
a hand-kept table list. (Legend puts an included ingest's datasets under a schema named by the ingest's dataset group
(`datasetGroup`); whether the desk's tables keep the default schema is a detail to settle, §9.)

### 4.4 The products

```pure
###DataProduct
DataProduct marketdata::MarketData
{
  title: 'Market data'
  description: 'Prices, quotes and FX rates from the ticker plant, live'
  accessPoints: [
    model ('The market data classes') [ ... ]         // a model access group: marketdata::MarketdataMapping,
  ]                                                   // its compatible runtimes, its featured classes
}
```

(The written forms in 4.4 and 4.5 are illustrative: Studio's test data shows a model access group and a data space's
`mappingProvider` only as JSON -- `modelAccessPointGroup` with `mapping`, `compatibleRuntimes`, `defaultRuntime`;
`mappingProvider: { element, keys }` -- not as text. Their shape is Legend's; the spelling is to be read from the
grammar when lite parses them.)

Each product's model access group is its domain's mapping; its owner and support are its team's. Products compose as
the model does today: the desk's mapping includes each product (Legend's `MappingIncludeDataProduct`) and carries the
links between domains -- `stress::AllMapping`'s association bindings -- where both ends are visible.

### 4.5 The desk: a data space made of the products

```pure
###DataSpace
DataSpace market::TradingDesk
{
  executionContexts:
  [
    { name: 'In this tab'; mappingProvider: { element: desk::Desk; keys: [ 'model' ] }; defaultRuntime: market::InTabRuntime; },
    { name: 'Deephaven';   mappingProvider: { element: desk::Desk; keys: [ 'model' ] }; defaultRuntime: market::DeephavenRuntime; }
  ];
  defaultExecutionContext: 'In this tab';
}
```

`desk::Desk` is the desk's own product: a model access group whose mapping includes the domain products. Its
screens -- `market::screens::stock`, `positionsLive`, `watchlist`, ... -- are the functions over classes they are
today; DataCube's saved pages are unchanged. Open With ▸ still switches between the contexts.

## 5. End to end

**A price, from the ticker plant to a grid (production):**
1. The ticker plant publishes a trade on its topic.
2. On the Deephaven server, the generated startup program's subscription for `MarketDataFeed.MARKET_TRADE` (append
   only, 8 hours kept) adds the row to the `MARKET_TRADE` table.
3. A trader's grid asks for `market::screens::stock(0.1)`'s rows for AAPL in the Deephaven context: the page's
   planner resolves the classes through the desk's mapping -- the market data product's -- to
   `store::DB.MARKET_TRADE`, the ingest's table, and sends the plan to that server; the result follows the live table.

**Reference data, each morning (production):** at 06:00 New York, the security master pull runs `INSTRUMENT_V`'s
query on the security master's connection (its runtime's) and replaces `INSTRUMENT` on the server.

**The demo:** the same ingests, products and data space; the runtimes differ. The ticker plant's and the order
system's topics are bound to the market world's simulator, which writes the rows into the tables the startup program
made; the masters' pulls read the simulator's reference rows. In this tab, the same ingests land in the tab's DuckDB,
the simulator in the tab writing them.

**A restart:** Deephaven keeps nothing; the startup program runs again, makes every table again, the streams resume
and each pull runs once at start before its schedule. (DH19 still refuses a read of a table that is missing.)

## 6. On a Deephaven server

| Declared | Set up by the generated startup program |
|---|---|
| A streamed dataset, appended, history kept | A subscription kept as an add-only table that drops rows past its history |
| A streamed dataset, latest by key | A subscription kept as one row per key |
| A pulled dataset | A keyed table, replaced or updated by key on its schedule |
| A pushed dataset (files) | A keyed table loaded from the files at its write location |
| A materialized view (phased after) | A live derived table under its name, read in place of computing it again (decision 4) |

The program is generated from the model, never written by hand, and only by internal users (the licence). A page's
own copies (DataCube's bases) stay API calls, declared by the platform (D6). The strict check (DH16) holds every
table to its ingest's columns.

## 7. What the platform checks before anything runs

- Every table a product's mapping reads is a dataset of an ingest the environment lands, or a table of a store the
  environment's runtime names.
- A dataset's key is among its columns; `LatestByKey` has a key; `keep` only on appended data.
- `Continuous` only from a source that can stream (a topic, the simulator, tables on the same server); a database pull
  needs a schedule.
- A function-defined dataset's columns are its function's result; a pushed one's are written out.
- A target that cannot do what a dataset asks (`Continuous` on a warehouse) is refused by name.

## 8. Order of work

1. The words of pull, stream, write modes and refresh, settled with the user on this example.
2. Ingests in lite: the `###Lakehouse` grammar (Legend's forms and lite's additions), `include Ingest`, and checks.
3. The desk's tables declared by ingests; `store::DB` including them; the domain mappings unchanged; the lanes green.
4. The generated startup program on Deephaven: streams, pulls, pushes. The market world's Groovy reduced to the
   simulator, writing rows only. The tape's history cap a model setting; the desk's memory measured over a long run.
5. Data products for the domains and the desk; the data space's contexts taking their mapping from `desk::Desk`.
6. The in-tab DuckDB as a target: the same ingests landed in the tab (step 6 of docs/ONE_CONNECTION_DESK_2026_10_28.md).
7. Shared views as materialized views, kept live (decision 4).
8. Access points read as `#P{...}#` where a reader wants a function rather than classes.

## 9. Open questions

1. **Schema names:** an included ingest's datasets live under a schema named by its dataset group in Legend; the desk's
   tables are in the default schema today. Keep Legend's rule and update the domain mappings, or keep the default.
2. **Pull's owner:** whose job is a pull -- the product's team (the platform runs their query) or the source system's
   (it pushes files, Legend's way)? Both can coexist; the model says which.
3. **Entitlements:** Legend's products are read through contracts on access groups. The desk has one user; who may
   read what is a separate decision, recorded and not designed here.
4. **How Legend serves an access point on its target, and refreshes a materialized view:** not visible (closed
   services). Lite's answers are its own, registered.

## Appendix A. The research

### A.1 `###Persistence`, read in full (2026-10-29, for decision 1)

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

### A.2 Legend's data products, ingest and materialized views (read 2026-10-29)

The user's reframing ("not a deploy but an API to the data that gives a level of abstraction from physical table")
led to these. They are not in the open legend-engine repo (its Query app only refers to them); their data model is in
legend-studio, read at `18a8f8f` (2026-10-29): `legend-graph/src/graph/metamodel/pure/dataProduct/DataProduct.ts`,
`.../packageableElements/ingest/IngestDefinition.ts`, their `V1_` protocol classes, the Studio editors' test data, and
`legend-extension-dsl-data-space`. Lite implements none of them today.

- **`IngestDefinition`** -- where data lands. An owner (an application directory deployment), a dataset group, and
  datasets: each a name, its columns (a relation type: `name Varchar(200)[0..1]`), a primary key, partition and
  clustering columns, a privacy classification, and either a `serializedSource` (rows pushed in by a producer, read as
  a `Snapshot` in a format such as CSV) or a **`FunctionSource`: a materialized view, a dataset defined by a Pure
  function over other datasets**, with its own tests (`IngestMatViewTest`). A write mode says how rows land:
  `append_only`, `batch_milestoned`, `batch_milestoned_business_temporal`. A query reads a dataset by its own accessor,
  `#I{model::MyIngest.MyDataSet}#` -- the logical dataset, never a physical table.
- **`DataProduct`** -- the API to the data. A title, description, owner, support, tests and sample values, and groups of
  **access points**: a `FunctionAccessPoint` (a Pure query) or a `LakehouseAccessPoint` (a function, a target
  environment -- `Snowflake`, `Databricks`, `BigQuery`, `DuckDb` -- a classification, and whether it is reproducible).
  An access point's function reads ingest datasets (`#I{...}#->filter(...)` in the test data).
- **Model access** -- a `ModelAccessPointGroup` is a group of access points plus a **mapping** (classes mapped onto the
  product's data), its **compatible runtimes** and a default one, featured classes, diagrams, and **secure views** (a
  function over an ingest dataset). A mapping can also include a whole data product (`MappingIncludeDataProduct`).
- **A data space made of data products** -- a `DataSpace`'s execution context may take its mapping from a
  `mappingProvider`: a data product and the id of one of its model access point groups, whose mapping it uses
  (`DSL_DataSpace_MappingProviderResolution.test.ts`). So a data space can be assembled from data products, the
  user's picture of the desk.

What it means here (proposed): the desk stays a data space, its execution contexts taking their mapping from data
products' model access point groups; the screens' functions are access points; the market's tables are ingest
datasets (landed by the simulator in the demo, by Kafka or a loader in production), the shared views are materialized
views (function-sourced datasets), and Deephaven joins the target environments beside DuckDb.

### A.3 Homework: how the pieces work end to end (read 2026-10-29, before the design is rewritten)

The user, 2026-10-29: "make sure we do more homework especially about how Dataspace and dataproduct work together -
how streaming might be added, how the whole thing will work end to end before we write the doc". Read in
legend-studio at `18a8f8f`: the round-trip test data (`legend-graph/src/graph-manager/__tests__/roundtripTestData/`
`TEST_DATA__DataProductRoundtrip.ts`, `TEST_DATA__IngestRoundtrip.ts`, `TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`),
the Query app's data space and data product test data, `DataSpaceQueryBuilderState.ts`,
`ValueSpecificationHelper.ts` (`attachWithFromQuery`), the value specification serializer's accessor kinds,
`legend-cube`'s access point source emitter, and `legend-server-lakehouse`'s clients. The lakehouse services themselves
(validation, deployment, how an access point is served) are not public.

**The written forms** (from the test data):

```
###Lakehouse
Ingest test::IngestDefinition Snapshot<CSV> owner=AppDir(production='297484')
[
  TEST_TABLE( int_val: Int, varchar_val: Varchar(500) ) pk=[ int_val ];
]

###DataProduct
DataProduct x::A
{
  title: 'Test title'
  accessPoints: [
    group ('Group description') [
      myId ('Id description'): LH(Snowflake, |#I{test::Ingest.TEST_TABLE}#)classification1
    ]
  ]
}
```

**The chain, producer to consumer:**

1. **Ingest** (`###Lakehouse`): datasets (columns, primary key, partition and clustering columns, privacy
   classification), how files are read (`Snapshot` or `Delta`, `CSV` or `Parquet`), how rows land (`append_only`,
   `batch_milestoned`, `batch_milestoned_business_temporal`), an owner (an application directory node, production and
   a prod-parallel), and tests (sample rows in, expected rows out). Deployed: validated, then deployed to the ingest
   environment the owner's application directory is found in (a discovery service), which answers a **write
   location** -- producers drop files there. **Push only: the platform pulls nothing.**
2. **Materialized views**: a dataset of an ingest whose source is a function over other datasets (`FunctionSource`),
   tested like any (`IngestMatViewTest`). No refresh or freshness setting is visible.
3. **Into the class model**: a relational `Database` can `include Ingest <ingest>` (`includedStoreSpecifications`,
   `storeType: 'Ingest'`): the ingest's datasets become its tables (schema the ingest's dataset group, table the dataset's),
   with joins and ordinary relational mappings over them.
4. **Data product** (`###DataProduct`): access points -- each a function, most often reading `#I{ingest.dataset}#`; a
   lakehouse access point (`LH(<target>, |<function>)<classification>`) names where it is served (`Snowflake`,
   `Databricks`, `BigQuery`, `DuckDb`); model access groups (a mapping, compatible runtimes and a default, featured
   classes, diagrams, secure views); or native model access (execution contexts of a mapping and a runtime, featured
   classes, sample queries: a data space's contents). Owner, support, tests. Deployed by its text; readers ask for
   access through contracts (an entitlement workflow over access groups).
5. **Composition**: a mapping includes another data product (`MappingIncludeDataProduct`) and joins to it across
   stores (an `xStore` association): products build on each other at the model.
6. **Consumption**: a query reads an access point as a relation, `#P{product.accessPoint}#` (accessor kinds: `>`
   relational, `I` ingest, `P` data product, `SQL`); the new DataCube (`legend-cube`) has a source that emits exactly
   that. A **data space's execution context takes its mapping from a data product's model access group**
   (`mappingProvider: { element: <product>, keys: [<group id>] }`), its runtime its own or a compatible one, and its
   queries run as `query->with(<product>)->from(<runtime>)`; one data space may mix such contexts with classic mapping
   and runtime ones. The Query app opens a product three ways: an access point (lakehouse access), a model access group,
   or a native execution context.
7. **Environments**: an owner's production and prod-parallel deployments; `ExecutionEnvironmentInstance` (services:
   a mapping and runtime per environment key) for the same query run in several environments.

**Streaming: nothing.** Ingest reads files, in snapshots or changes, at a write location; write modes are batch;
materialized views have no refresh setting; the target environments are warehouses. What streaming would add, each a
lite difference to register: an ingest read mode from a topic (`Stream<Avro>`: the write location a Kafka topic, not
a folder); stream write modes -- append with history kept (`keep`), latest by key; materialized views kept live; and
live target environments (`Deephaven`, the tab's DuckDB) for landing and for serving access points.

**Not visible (closed services):** how an access point is served on its target (a view? a table?), how materialized
views refresh, the full ingest grammar beyond the one example, and how `#P{}#` and `#I{}#` are planned.

## Appendix B. A cross-check

legend-engine's Deephaven extension (`legend-engine-xts-deephaven`, `deephavenAppGeneration.pure`) has a `DeephavenApp`
that generates a Deephaven application from a function: a source table with a writer, and a derived view. Not the
reference (the user, 2026-10-22: half built), but it reached the same shape as §6 -- the model generates the startup
program, something else writes the rows.
