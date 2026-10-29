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
  every table (D6). In a demo the simulated world stands in for the sources: behind our own Kafka door on Deephaven,
  writing the tab's DuckDB directly (§9 question 5).
- **The order of the work:** docs/DESK_DATA_ROADMAP_2026_10_29.md.

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
8. **Entitlements:** "Agree no entitlements needed for now". **Dataset groups** left unset (the desk's tables stay in
   the default schema) and **a pull owned by the source's team**, the team that owns the product: "Okay".

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
3. **How rows land, for live data:** `AppendOnly` with the history it keeps (`keep=Last('PT8H')`,
   `keep=LastRows(200000)`), and `LatestByKey` (one row per key, the newest).
4. **Schedules:** `Every('PT15M')`, `Daily('06:00', 'America/New_York')` -- for pulls. A materialized view follows its
   inputs (Legend's show no refresh setting).
5. **Live targets:** a Deephaven server and the tab's DuckDB, where ingests land and access points are served, beside
   Legend's Snowflake, Databricks, BigQuery and DuckDb. Chosen by the environment's runtime, never written in the
   ingest.

The words are §3a, worked on the desk in §4.

## 3a. The words (proposed 2026-10-29, for the user's review)

**Three rules.** (1) Every form Studio shows written is kept exactly (Appendix A.4). (2) Any other field is spelled by
the conventions of the element it is in -- `key=value` on an ingest's header and datasets (as `owner=` and `pk=`),
`key: value;` inside braces (as a data product's and a data space's) -- its key the protocol's field name. (3) What lite
adds follows the same two conventions. The JSON each form produces is the protocol's, tested against Studio's
round-trip fixtures; each spelling lite reconstructed is listed so it can be replaced when Legend's grammar is seen.

| Construct | Written | Protocol (JSON) | Whose |
|---|---|---|---|
| An ingest | `Ingest <path> <read mode> owner=AppDir(production='<id>') [ <datasets> ]` | `ingestDefinition` | Legend, seen |
| Files, whole or changed | `Snapshot<CSV>`, `Delta<Parquet>` | `readMode: { _type: Snapshot \| Delta, format: { _type: CSV \| Parquet } }` | Legend; `Delta<Parquet>` by rule 2 |
| Live rows | `Stream` | `readMode: { _type: Stream }` | lite |
| Pulled on a schedule | `Pull<Daily('06:00', 'America/New_York')>`, `Pull<Every('PT15M')>` | `readMode: { _type: Pull, schedule: { ... } }` | lite |
| A dataset of written columns | `NAME( col: Type, ... ) pk=[ col ];` | `source: { _type: serializedSource, schema }` | Legend, seen |
| A dataset made by a function | `NAME( \|<function> ) pk=[ col ];` | `source: { _type: FunctionSource, function }` | Legend's source, lite's spelling |
| How rows land | `writeMode=AppendOnly`, `writeMode=BatchMilestoned`, `writeMode=LatestByKey` | `writeMode: { _type: append_only \| batch_milestoned \| latest_by_key }` | Legend's first two by rule 2; `LatestByKey` lite |
| History kept | `keep=Last('PT8H')`, `keep=LastRows(200000)` | `keep: { ... }` | lite |
| A store over an ingest | `include Ingest <path>` | `includedStoreSpecifications: [ { storeType: Ingest, ... } ]` | Legend, named in prose |
| Topics as tables | `type: Kafka; specification: Kafka { bootstrapServers: '...'; schemaRegistry: '...'; format: Avro; };` | a `RelationalDatabaseConnection` of lite's type | lite (as `Deephaven`) |
| Rows written straight in by the market world (the tab only, §9 question 5) | `type: Simulated; specification: Simulated { world: 'market'; };` | the same, the static demo's and its tests' | lite |
| A model access group | `<id> ('<description>') Model { mapping: <path>; compatibleRuntimes: [ <id>: <runtime>, ... ]; defaultRuntime: <id>; } [ <access points> ]` | `modelAccessPointGroup` | Legend's shape, lite's spelling |
| A data space's mapping from a product | `mappingProvider: { element: <product>; keys: [ '<group id>' ]; };` | `mappingProvider: { element, keys }` | Legend's shape, lite's spelling |

Durations are ISO 8601 (`PT8H`, `PT15M`); a time of day is `HH:MM` in a named IANA zone.

**What each read mode means:** files (`Snapshot`, `Delta`) are dropped by a producer at the ingest's write location;
`Stream` rows arrive live -- from the topic a function reads, or written by the dataset's producer when its columns are
written out; `Pull` runs each dataset's function on the schedule, each run the whole set. **What each write mode
means:** `AppendOnly` adds every row (with `keep`, older ones fall away); `LatestByKey` keeps the newest row per key (a
pull's run also drops the keys it no longer returns); `BatchMilestoned` keeps every version with batch ids -- Legend's,
refused on Deephaven by name until a case needs history there.

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
| `positions::Positions` | `Book`, `Position` | `positions::PositionsMapping` | Dropped as files by position keeping each morning (Legend's own form) | Snapshot, daily |
| `risk::Limits` | `RiskLimit` | `risk::RiskMapping` | Pulled from the risk system | Latest by key, every 15 minutes |
| `desk::Analytics` (the desk's own) | -- | -- | Materialized from the products above | Live (the shared views, phased after) |

News is its own product because its source, licence and readers differ from prices'. `Book` stays with positions,
where the model puts it. The desk-only `world` status table is the market world's, not a product.

### 4.2 The ingests

```pure
###Lakehouse
// MARKET DATA, streamed from the ticker plant's topics (owned by the market data team)
Ingest marketdata::MarketDataFeed Stream owner=AppDir(production='...')
[
  MARKET_TRADE( |#>{tickerplant::Topics."tp.us-equities.trades.v3"}# ) pk=[ PRINT_ID ] writeMode=AppendOnly keep=Last('PT8H');
  MARKET_QUOTE( |#>{tickerplant::Topics."tp.us-equities.nbbo.v2"}# )  pk=[ QUOTE_ID ] writeMode=LatestByKey;
  FX_RATE( |#>{tickerplant::Topics."tp.fx.spot.v1"}# )                pk=[ RATE_ID ]  writeMode=LatestByKey;
]

// NEWS, streamed from the vendor's topic
Ingest marketdata::NewsFeed Stream owner=AppDir(production='...')
[
  NEWS_STORY( |#>{newswire::Topics."vendor.newswire.stories.v1"}# ) pk=[ STORY_ID ] writeMode=AppendOnly keep=Last('PT24H');
]

// THE ORDER SYSTEM, streamed from its drop copy
Ingest trading::OrderFlowFeed Stream owner=AppDir(production='...')
[
  TRADE_ORDER( |#>{oms::Topics."oms.equities.orders.v4"}# ) pk=[ ORDER_ID ]     writeMode=LatestByKey;
  EXECUTION( |#>{oms::Topics."oms.equities.fills.v4"}# )    pk=[ EXECUTION_ID ] writeMode=AppendOnly;
  ORDER_ROUTE( |#>{oms::Topics."oms.equities.routes.v4"}# ) pk=[ ROUTE_ID ]     writeMode=AppendOnly;
  RFQ( |#>{oms::Topics."oms.fx.rfqs.v2"}# )                 pk=[ RFQ_ID ]       writeMode=LatestByKey;
]

// THE SECURITY MASTER, pulled each morning (owned by the security master's team)
Ingest products::SecurityMasterLoad Pull<Daily('06:00', 'America/New_York')> owner=AppDir(production='...')
[
  INSTRUMENT( |#>{secmaster::DB.INSTRUMENT_V}# ) pk=[ INSTRUMENT_ID ] writeMode=LatestByKey;
  EQUITY( |#>{secmaster::DB.EQUITY_V}# )         pk=[ EQUITY_ID ]     writeMode=LatestByKey;
  OPTION( |#>{secmaster::DB.OPTION_V}# )         pk=[ OPTION_ID ]     writeMode=LatestByKey;
]
// refdata::MarketsLoad (EXCHANGE, SECTOR), org::OrganisationLoad (BUSINESS_UNIT, DESK, TRADER) and
// counterparty::ClientMasterLoad (COUNTERPARTY): the same, each from its own master

// RISK LIMITS, pulled every fifteen minutes
Ingest risk::LimitsLoad Pull<Every('PT15M')> owner=AppDir(production='...')
[
  RISK_LIMIT( |#>{riskdb::DB.RISK_LIMIT}# ) pk=[ LIMIT_ID ] writeMode=LatestByKey;
]

// POSITIONS at the start of the day: Legend's own form -- position keeping drops its files
Ingest positions::StartOfDay Snapshot<Parquet> owner=AppDir(production='...')
[
  POSITION( POSITION_ID: Varchar(200), QUANTITY: Double, AVERAGE_COST: Double, ... ) pk=[ POSITION_ID ];
  BOOK( BOOK_ID: Varchar(200), NAME: Varchar(200), ... ) pk=[ BOOK_ID ];
]

// THE MARKET WORLD'S STATUS: written live by the world itself (columns written out: no function)
Ingest market::WorldStatus Stream
[
  world( item: Varchar(16), state: Varchar(16), detail: Varchar(256) ) pk=[ item ] writeMode=LatestByKey;
]
```

A dataset made by a function takes its columns from the function's result -- no column list written twice -- and the
strict check (DH16) holds what lands to them. The topics' stores (`tickerplant::Topics`, `newswire::Topics`,
`oms::Topics`) are ordinary `###Relational` databases whose tables are named as the topics are; the masters' stores
(`secmaster::DB`, `riskdb::DB`) are the source systems' own.

### 4.3 Into the class model: the trading store includes the ingests

```pure
###Relational
Database store::DB
(
  include Ingest marketdata::MarketDataFeed
  include Ingest marketdata::NewsFeed
  include Ingest trading::OrderFlowFeed
  include Ingest products::SecurityMasterLoad
  include Ingest risk::LimitsLoad
  include Ingest positions::StartOfDay
  ...
)
```

The domain mappings stay as written: `marketdata::MarketdataMapping` maps `MarketTrade` to `MARKET_TRADE` as today.
What changes is where `MARKET_TRADE` is declared -- by the ingest, which knows where its rows come from, rather than by
a hand-kept table list. With no dataset group, each dataset stays in the default schema (decided, §1.8).

### 4.4 The products

```pure
###DataProduct
DataProduct marketdata::MarketData
{
  title: 'Market data'
  description: 'Prices, quotes and FX rates from the ticker plant, live'
  accessPoints: [
    model ('The market data classes') Model
    {
      mapping: marketdata::MarketdataMapping;
      compatibleRuntimes: [ inTab: market::InTabRuntime, deephaven: market::DeephavenRuntime ];
      defaultRuntime: deephaven;
    } [ ]
  ]
}

DataProduct desk::Desk
{
  title: 'Trading desk'
  description: 'The desk\'s model: every domain product, and the links between them'
  accessPoints: [
    model ('The desk classes') Model
    {
      mapping: desk::DeskMapping;        // includes each domain product, carries the cross-domain associations
      compatibleRuntimes: [ inTab: market::InTabRuntime, deephaven: market::DeephavenRuntime ];
      defaultRuntime: deephaven;
    } [ ]
  ]
}
```

```pure
###Mapping
Mapping desk::DeskMapping
(
  include dataproduct marketdata::MarketData       // Legend's MappingIncludeDataProduct; the words are lite's (rule 2)
  include dataproduct products::SecurityMaster
  ...
  // the association bindings stress::AllMapping carries today, where both ends are visible
)
```

Each domain product's model access group is its domain's mapping; its owner and support are its team's.

### 4.5 The desk: a data space made of the products

```pure
###DataSpace
DataSpace market::TradingDesk
{
  executionContexts:
  [
    {
      name: 'In this tab';
      title: 'In this tab (DuckDB)';
      mappingProvider: { element: desk::Desk; keys: [ 'model' ]; };
      defaultRuntime: market::InTabRuntime;
    },
    {
      name: 'Deephaven';
      title: 'Deephaven';
      mappingProvider: { element: desk::Desk; keys: [ 'model' ]; };
      defaultRuntime: market::DeephavenRuntime;
    }
  ];
  defaultExecutionContext: 'In this tab';
  title: 'Trading desk';
}
```

Its screens -- `market::screens::stock`, `positionsLive`, `watchlist`, ... -- are the functions over classes they are
today; DataCube's saved pages are unchanged. Open With ▸ still switches between the contexts.

### 4.6 The environments: where each store is

```pure
###Connection
RelationalDatabaseConnection prod::TickerPlant
{
  store: tickerplant::Topics;
  type: Kafka;
  specification: Kafka { bootstrapServers: 'tp-kafka-01.prod:9093,tp-kafka-02.prod:9093'; schemaRegistry: 'https://schemas.prod'; format: Avro; };
  auth: ...;
}
RelationalDatabaseConnection demo::TickerPlant
{
  store: tickerplant::Topics;
  type: Kafka;
  specification: Kafka { bootstrapServers: '127.0.0.1:19092'; format: Json; };
  auth: Test;
}
RelationalDatabaseConnection tab::TickerPlant
{
  store: tickerplant::Topics;
  type: Simulated;
  specification: Simulated { world: 'market'; };
  auth: Test;
}
```

| Store | Production | The demo on Deephaven | The demo in the tab (no server) |
|---|---|---|---|
| `store::DB` (where the ingests land) | Deephaven | Deephaven | the tab's DuckDB |
| `tickerplant::Topics`, `newswire::Topics`, `oms::Topics` | Kafka | Kafka at a local address, the market world behind it (§A.5) | `Simulated`: the market world writes the landing tables itself |
| `secmaster::DB`, `riskdb::DB`, the other masters | each master's database | the market world's reference rows | the same |
| Position keeping's files | its write location | the market world's start-of-day positions | the same |

The ingests, products and data space are the same in all three; only the runtimes differ. In the tab nothing is
consumed: the landing tables are made from the ingests as everywhere (their write mode, their history kept), and the
world appends and upserts into them, the way it writes the tab's live DuckDB today. The tab still ticks; only Kafka is
missing.

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

**The demo:** the same ingests, products and data space; the runtimes differ (§4.6). On Deephaven, the ticker plant's
and the order system's topics are a Kafka door the simulated world writes behind, read by the same subscriptions as
production's; the masters' pulls read a small database the world writes at start. In this tab, the same ingests land
in the tab's DuckDB, and the world writes them directly.

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
- `Stream` only from a source that can stream (a topic, the simulated world, tables on the same server); a database
  pull needs a schedule.
- A function-defined dataset's columns are its function's result; a pushed one's are written out.
- A target that cannot do what a dataset asks (`Stream` on a warehouse) is refused by name.

## 8. Order of work

In docs/DESK_DATA_ROADMAP_2026_10_29.md (proposed 2026-10-29, to read with the user): the foundation (the words,
ingests in the compiler, the desk's tables declared by ingests), then Deephaven made by the platform (the door, the
generated startup program, pulls and files, `market.groovy` deleted), the tab, the data products, the long run.

## 9. Open questions

1. **Schema names: decided** -- no dataset group; the desk's tables stay in the default schema. A group (and DH18's
   naming rule on Deephaven) when two sources' datasets collide.
2. **Pull's owner: decided** -- the source's team, which owns the product; pull is one way it publishes (the platform
   runs its query on its own database), beside dropping files. A consumer never pulls from another team's database,
   but for a stopgap stated as one, its owner and risk written down.
3. **Entitlements: none for now** (the user, 2026-10-29: "Agree no entitlements needed for now"). Legend's products are
   read through contracts on access groups; that stays out of this design.
4. **How Legend serves an access point on its target, and refreshes a materialized view:** not visible (closed
   services). Lite's answers are its own, registered.
5. **The demo's streams: settled in part** (the user, 2026-10-29: "for demo we can have no server but for DeepHaven we
   need server anyway - for static demo I guess we just dont do streaming we just do duckdb inserts"). A browser tab
   can be neither a Kafka server (it accepts no connections) nor a Kafka client (it opens no raw sockets), so:
   - **On Deephaven**, the demo's connections are `type: Kafka` at a local address, so the ingests, Deephaven's startup
     program and its Kafka reader are production's own; only the address differs. What answers there (our door or a
     real broker) is §A.5's choice, still open.
   - **In the tab**, the world writes the landing tables directly. `Simulated` stays for this one case, as a word the
     environment states. Leaving the topics unbound in the tab's environment instead would make "written directly"
     look the same as "forgot to bind", and the platform's checks must refuse the second (AGENTS.md invariant 4).

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

### A.4 The written syntax: what is Legend's, what is ours (read 2026-10-29)

**The grammars are closed.** Studio has no parser for `###Lakehouse` or `###DataProduct`: it sends their text to the
engine (`parseGrammar`, `parseCompatibleModel`), and an ingest's text comes from Legend's ingest server
(`getIngestDefinitionGrammar`). legend-engine's public history (3,000 commits, back to July 2023) never held a grammar
for either. So the spelling cannot be read from a parser; it is reconstructed from what Studio shows.

**What Studio shows, written:**
- One ingest: `Ingest test::IngestDefinition Snapshot<CSV> owner=AppDir(production='297484') [ TEST_TABLE( int_val:
  Int, varchar_val: Varchar(500) ) pk=[ int_val ]; ]` (marketplace and data product test utilities).
- Data products: two commented round-trip examples (title, description, icon, delivery frequency, coverage regions,
  groups with descriptions and stereotypes, `id ('description'): LH(Snowflake, |...)classification`, support info) and
  one more in the test utilities (`TEST_VIEW: *LH(Snowflake, |#I{test::Ingest.TEST_TABLE}#)` -- the `*`'s meaning not
  shown).
- `include Ingest` named in prose (`TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`'s comment).
- Data spaces: real round-trip grammar cases (`legend-manual-tests/.../roundtrip-grammar/cases/DSL_DataSpace-*.pure`),
  but none with a `mappingProvider`.

**What Studio shows only as JSON** (the protocol, and Studio's round-trip fixtures): model access groups (mapping,
compatible runtimes, default runtime, featured elements, diagrams, secure views), native model access, function access
points, owners and production overrides, data product tests, an ingest's dataset group, partition and clustering
columns, privacy classification, `Delta` and `Parquet`, write modes, function-sourced datasets (materialized views),
ingest tests, and the data space's `mappingProvider`.

**How lite would build it:** every written form Studio shows, exactly; every other field spelled by the conventions
those forms use (`key: value;` inside braces, `NAME( col: Type, ... ) pk=[ ... ];` for datasets), each such spelling
listed as lite's own until the real grammar is seen; and **the JSON lite produces tested against Studio's round-trip
fixtures** (`TEST_DATA__DataProductRoundtrip.ts`, `TEST_DATA__IngestRoundtrip.ts`,
`TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`, the Query app's data space fixtures) -- the protocol is what Studio and
the engine exchange, so it is matched exactly even where the text is reconstructed.

### A.5 The demo's Kafka: two brokers read, our own door measured (2026-10-29)

The question (the user): is there a small, single-binary Kafka, the way DuckDB is a small SQL database, to stand where
the production brokers stand in the demo and the tests? Redpanda and Tansu, or a Kafka front door on our own simulated
world.

**What Deephaven's reader needs.** Read from the pinned 42.7's bytecode: `KafkaIngester` calls `assign`,
`partitionsFor`, `seek`, `seekToBeginning`, `seekToEnd` and `poll`, never a consumer group. On the wire that is four
requests: ApiVersions, Metadata, ListOffsets, Fetch. The client is `kafka-clients-8.3.2-ccs` (Confluent's build); its
lowest versions, read from the jar, are Fetch 4, ListOffsets 1, Metadata 0. The extension also carries Confluent's
schema-registry client and Avro and Protobuf deserializers, for `avroSpec` and `protobufSpec`.

**Redpanda** (read from `redpanda-data/redpanda` at `4254992`, 2026-10-08):

- Licence: Business Source License 1.1 for the core. Use is free except as a hosted streaming service sold to third
  parties, and each release becomes Apache 2.0 four years after it ships. Enterprise features are under the Redpanda
  Community License (paid). Either way, it is a second licence question beside Deephaven's.
- Running it: on Linux, a package (apt, yum) or an archive unpacked into `/opt/redpanda`. On macOS, the README says
  "Docker is required".
- Not run here.

**Tansu, renamed Nisshi** (read from `nisshi-io/solace-nisshi` at `4b4c1b4`, 2026-10-29):

- Licence and owner: Apache 2.0. The README says the project is now part of Solace, and that "the license stays
  Apache 2.0".
- Size and maturity: one static Rust binary, version 0.8.1, very active (its last 50 commits span two days). Its
  releases still change what it stores: 0.7.0 changed the default cluster id, and 0.8.0 the topic configuration keys.
- Prebuilt binaries: Linux x86-64 and Arm64 (musl), and macOS on Apple silicon. There are none for Windows or Intel
  Macs; those build from source with `cargo`.
- Storage: in memory (the release binary's default), SQLite, PostgreSQL, or S3.
- Retention (`retention.ms`, `retention.bytes`): applied only on SQLite and PostgreSQL, every 10 minutes by default.
  The in-memory engine keeps every record until it stops.
- Kafka API: what clients use, so producing, fetching, consumer groups (the classic protocol) and the idempotent
  producer. It has no transactions.
- Tested clients: its CI runs librdkafka's and franz-go's test suites, and Kafka's own command-line tools 3.7, 3.9 and
  4.3, which use the Java client.
- Schemas: one file per topic in a folder (JSON Schema, Avro or Protobuf). The broker refuses a record that does not
  match its topic's schema. A topic with a schema can also be written as Parquet, Iceberg or Delta tables; the release
  binary is built with every feature. This is not the Confluent schema-registry web API, so Deephaven's Avro reader
  with a registry address would not work against it; its JSON reader would.
- Not run here: the session refused to run a downloaded binary without the user's word.

**Our own door, measured.** `experiments/kafka-door-spike` is about 200 lines of Node with no libraries. A simulated
world writes quotes into memory, and a Kafka endpoint answers the four requests above, one version each, with records
in Kafka's batch format v2. The pinned Deephaven read it with `KafkaTools.consumeToTable`, production's own reader:

| Quotes a second | Deephaven's cycle | Every quote, once, in order? | Late by, p50 / p99 / max | The door's CPU, memory |
|---|---|---|---|---|
| 2,000 (the desk world's rate) | 100 ms | yes, 143,660 | 53 / 103 / 143 ms | 6% of a core, 134 MB |
| 20,000 | 100 ms | yes, 879,000 | 55 / 104 / 151 ms | 19% of a core, 461 MB (1.4 M held) |
| 2,000 | 10 ms | yes, 89,620 | 7 / 14 / 61 ms | 7% of a core, 130 MB |

The lateness is Deephaven's cycle (half a cycle on average), not the door.

**What the door is and is not.**

- It is a server. A browser tab cannot accept a connection, so it runs as a small Node process; the tab's live DuckDB
  stays as it is.
- It hangs off the world, not off DuckDB. A Kafka topic is a log read by position, and DuckDB adds nothing to serving
  one. The world (`datacube/demo/market-sim`) already writes through one seam, `MarketSink`, where each table's write is
  a stream's append or a keyed table's upsert. A second sink would append each row to its table's topic:
  - a stream's topic is a log whose oldest records are dropped past its retention;
  - a keyed table's topic keeps the latest record of each key (Kafka's "compacted" topic).

  The world then feeds both engines, and `market.groovy`'s copy of the simulation can go.
- It is read-only. Others' producers, and Deephaven's own `produceFromTable`, would need Produce and the idempotent
  producer's InitProducerId.
- It is a stand-in. A test against it proves Deephaven reads our door, not that it reads Kafka. The surface is small
  and has been stable, but a future client could drop the older encodings the door speaks, as Kafka 4.0 dropped
  versions from before 2.1 (KIP-896). The door would then add Kafka's newer "flexible" encoding.

**The choice, for the user.**

| | Our door | Tansu (Nisshi) | Apache Kafka, one node | Redpanda |
|---|---|---|---|---|
| Licence | ours | Apache 2.0 | Apache 2.0 | BSL 1.1: a second question |
| Runs on | anywhere Node runs | Linux, Apple-silicon Mac (Windows: build it) | anywhere Java runs | Linux; Mac through Docker |
| How the world writes to it | in the same process | through a Kafka producer: a Node client to take on, or Produce written by us | the same as Tansu | the same as Tansu |
| Is it Kafka? | a stand-in, four requests | a real broker, tested against three client families | the reference | a real broker |
| Measured here | yes (above) | no: needs the user's word | no | no |

With the tab settled (§9 question 5: no Kafka there, the world writes DuckDB directly), the choice is for Deephaven
only, where a server runs anyway. "No server" is no longer a reason for the door; what remains is that it needs no new
dependency and no Kafka producer for the world, at the cost of being a stand-in. A real broker's one reason is that it
is Kafka.

Recommended: the door for the Deephaven demo and its lanes first, since it is measured and costs nothing new. Add one
lane against a real broker (Tansu, or Apache Kafka if Tansu's youth worries) when a producer outside the world exists.
That lane is where a gap between the door and Kafka would show.

## Appendix B. A cross-check

legend-engine's Deephaven extension (`legend-engine-xts-deephaven`, `deephavenAppGeneration.pure`) has a `DeephavenApp`
that generates a Deephaven application from a function: a source table with a writer, and a derived view. Not the
reference (the user, 2026-10-22: half built), but it reached the same shape as §6 -- the model generates the startup
program, something else writes the rows.
