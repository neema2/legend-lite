# The desk's data, deployed: the design (2026-11-14)

**Status: AGREED 2026-11-15 but for one point** (§13: how a keyed dataset lands, found while preparing step 0).
Written with the user on 2026-11-14 and 15, branch `claude/deephaven-3-browser`. §13 says what the user ruled, in
the user's words. Nothing here is built yet. When ratified it
supersedes, in docs/DESK_DATA_ROADMAP_2026_10_29.md, B2's generated Groovy program, B3's pulls and decision P's
runner, and the ingest words `Stream`, `Pull`, `LatestByKey` and `retention` (docs/SEMANTICS_REGISTER.md S402).
`include Ingest`, the topic stores and C0's ingest plans stay.

**Read in this order:** §0 the design in brief, §1 how we got here, §2 the principles, §3 who writes what, then the
parts. Appendix A is the whole model written out; Appendix B the turns taken on the way, so they are not walked again.

## 0. The design in brief

- **Producers publish to Kafka.** Each topic is declared once in Legend, by the team that publishes it: Legend is the
  schema registry. Reference data arrives the same way (change capture into compacted topics); start-of-day positions
  arrive as snapshot files.
- **An ingest says what lands, in upstream's Lakehouse words:** `Delta` or `Snapshot` (with a `deleteIndicator`),
  `append_only` or `batch_milestoned`. A dataset read from a topic or a database is a function of it; a dataset
  delivered to the ingest declares its own columns.
- **A keyed dataset keeps every version** (upstream's batch milestoning, `LAKE_IN_ID` and `LAKE_OUT_ID`); its latest
  rows are `LAKE_OUT_ID = 999999999`, read through an ordinary relational filter (§13, option A, recommended).
- **Materialized views** are datasets made by a function over landed datasets. **Data products** publish access
  points, each with an optional target.
- **Reading is unchanged:** a database includes ingests (`include Ingest`, upstream's), and the classes, mapping and
  screens sit on it.
- **A deployment, the one new element,** says per environment what lands (ingests or data products), from which
  sources, on which targets (`live`, `history`), keeping how much. One element serves every backend: the target's
  dialect decides how.
- **Deploying renders each target's plan as data** -- no generated code -- and installs it: a fixed, reconciling
  loader on Deephaven, exec in the tab, a writer for the lake. A check runs after installing and on every connect.
- **The demo is production with different configuration:** one path.

## 1. How we got here

- B1 and B2 were built on 2026-11-14: the market world behind lite's own Kafka door, and the planner generating a
  Deephaven startup program in Groovy (`sql/dialect/DeephavenProgram`, `planner/DeephavenStartup`) that the pinned
  Deephaven ran, nine tables typed as declared, every row once (`//datacube:deephaven_door_test`).
- The user: "Take a giant step back - do we have the right design? Especially putting groovy code in the plan which
  does not seem like the right way forward". Agreed: generated code in a plan breaks G4 (the plan is SQL and data
  only). From there: the plan as data with a fixed loader; then how a user configures everything a plan needs; then
  this design, checked against upstream's Lakehouse, `###Persistence` and function activators.

## 2. The principles

1. **The plan is data (G4).** What a server must make is a list of tables and views with their sources and what they
   keep; one fixed, versioned loader on the server reads it. Deephaven 42.7 has no request that makes a table from a
   Kafka topic (its protos read: `TableService`, `InputTableService`, `ConsoleService`), so something has to run on
   the server. The loader is the smallest form: the `CREATE TABLE ... FROM KAFKA` statement Deephaven lacks.
2. **One path, configuration apart.** The user: "Ideally the demo is built in a way that is the production flow but
   configured as private so there really is only one path, but with different config". The model, the plans, the
   loader, the checks and the screens are the same in the demo and in production; only connections and deployments
   differ (§10).
3. **Declare once, deploy anywhere.** The same ingests, views and data products land on several engines -- the desk's
   Deephaven (live), a lakehouse (history), the tab's DuckDB (private) -- by configuration. Upstream cannot: its
   ingest deploys to the lakehouse alone, and an access point's target picks which lakehouse engine to compile for
   (the user: "it's not generic across backends which is not great/very limiting").
4. **The log is shared; the engines are each desk's.** Owners publish to Kafka once. Each desk's engine subscribes to
   the topics it reads and keeps its own tables; what many desks need is published back as a topic. A server shared
   by several environments is refused by name until a model needs it.
5. **Legend is the schema registry.** The user: "The schema registry should be legend itself right? That's the whole
   point of Legend?" A topic's shape is declared, versioned and reviewed in the model. A Kafka registry, where Avro or
   Protobuf needs one on the wire, holds a copy the deploy publishes or checks; nobody edits it by hand. Upstream
   already generates Avro schemas from its model (legend-engine `AvroGenerationExtension`).
6. **Owners publish.** The team that owns data publishes every change; lite querying a master's database on a
   schedule is the fallback (§11).
7. **Upstream's words first.** Lakehouse constructs are preferred; `###Persistence` and the function activators are
   read for inspiration (§16, §17). A lite word is added only where none fits, each a register row (§14).

## 3. Who writes what

| Who | Writes | Part |
|---|---|---|
| the team that publishes a topic | its topic store: each topic's shape and key | 1 |
| the data's owner | the ingest | 1 |
| the desk's quants, or a domain's owner | materialized views | 1 |
| the modeller | the read database, classes, associations, the mapping | 1 |
| the desk's developers | the screens | 1 |
| a domain's owner | its data product | 1 |
| the platform team, or the desk's operations | connections, deployments, runtimes, the data space | 2 |

Part 1 says what the data is and names no engine. Part 2 says where it runs: the only part the demo and production
differ in. Part 3 is what happens: install, publish, open, change; nothing written.

## 4. Sources and destinations

**The rule.** The user asked: "is one a source and one a destination? If we weren't ingesting the Kafka would we
still want the table definition itself without the ingest?"

- **Delivered to the ingest** (files at its drop location, records into its own topic): **the ingest is the
  contract.** It is upstream's model: its ingest takes only files pushed to it, so the source needs no model (the
  user: "lakehouse maybe side steps this bc it only takes files as ingest input"). The dataset declares its columns
  (upstream's `serializedSource`). The positions snapshot and the world's status work this way.
- **Read from something that exists anyway** (a team's topic, a database): **it is declared on its own, by its
  owner, and the ingest refers to it.**

**A topic stays its own declaration.** Folding it into the ingest was proposed and withdrawn: the producer declares
its contract before anyone ingests it; many ingests read one topic (the desk, the history tier, another desk); a
topic may one day be queried without landing, by an engine that reads Kafka as a table; lineage needs the topic as a
node.

**A query, not a pointer.** The user: "Should Ingest be a pointer to the table instead of a query? Or both? Or always
a query". Always a query for what is read; the plain read `|#>{db.table}#` is the pointer, recognized by its shape,
its columns and key the table's. Upstream has two kinds of source (serialized, function); a pointer would be a third.

| A dataset's function reads | Allowed | Why |
|---|---|---|
| a topic | the plain read; later row-by-row shaping (filter, select, computed columns) | nothing is held between records |
| a database (a pull) | any query | the source database runs it at each pull |
| landed datasets (`#I{...}#`) | anything: it is a materialized view | each target maintains it |

A join or a group over a raw topic is refused by name: declare a derived dataset over landed datasets.

**Keys** come from the source's primary key, as its columns already do (S403); `pk=` is written for a delivered
dataset, or where the landing's key differs (the latest trade of each instrument, over an event topic).

**The door to watch.** A topic store is a relational table: flat records only, and no place for a topic's own facts
(how deletes are marked, events or changelog, the format). Those sit in the ingest's read mode, upstream's place for
them. If topics need more, upstream's external formats (a schema set bound to classes) are the vocabulary; only the
reference's spelling would change.

## 5. The ingest, in upstream's words

Upstream's ingest specification (legend-studio `V1_IngestDefinitionSpecification.ts`) splits two questions lite had
mixed:

- **The read mode says what each delivery holds:** `Delta` (changes, with an optional `deleteIndicator {
  deleteField, deleteValues }`), `Snapshot` (the whole set: a key that is missing is gone), `TemporalSnapshot`; with
  its format, `CSV`, `Avro`, `JSON` or `Parquet`.
- **The write mode says how deliveries land:** `append_only` (events), `batch_milestoned` (keyed, every version
  kept), `batch_milestoned_business_temporal`; per dataset.

| Lite today | In upstream's words |
|---|---|
| `deleted=DELETED` | `Delta` with `deleteIndicator=(deleteField: 'DELETED', deleteValues: ['true'])` |
| the read mode `Stream` | `Delta`: whether rows are live is where they arrive (a topic) and the target (a live engine) |
| `Pull<Daily(...)>` | `Snapshot` of a database query, with a schedule (the schedule is lite's) |
| `writeMode=LatestByKey` | `batch_milestoned`, and `keep: current` on a live target |
| `retention=LastRows(n)` | the deployment's `keep` |
| `format: Json` on the Kafka connection | the read mode's format, `Delta<JSON>`: one connection a cluster carries topics of many formats |

With `Snapshot`, keys that vanish from a pull vanish: what decision P's runner would have done by comparing runs.

**A milestoned dataset's columns** (option A, §13): `batch_milestoned` keeps every version and adds `LAKE_DIGEST`,
`LAKE_IN_ID` and `LAKE_OUT_ID` to every reader; the open versions are `LAKE_OUT_ID = 999999999`, upstream's
persistence component's open batch id. The read database declares a filter per keyed table and each keyed class
mapping uses it (`~filter`): the latest rows on every target, every version on the history tier by leaving it out.

**What stays lite's** (S402 shrinks to these): a function source may read stores, not only datasets; a pull's
schedule; a delivered ingest's records on a topic, not only as files; the deployment (§8).

**A materialized view needs no word of its own.** Upstream puts a dataset made by a function over landed datasets
in an ordinary ingest, beside delivered ones, under the ingest's read mode; and an ingest may have no read mode
(legend-studio `TEST_DATA__IngestDefinitionFlow.json`: `RANKED_POSITIONS` reads
`#I{markets::ingest::TradePositionsIngest.EOD_POSITIONS}#` in an ingest read as `Delta<CSV>`; the fixture's second
ingest has no read mode). So a dataset whose function reads `#I{...}#` is a materialized view, by upstream's rule;
`Derived`, proposed earlier as its read mode, is dropped.

## 6. Reading: unchanged

`include Ingest` is upstream's (`includedStoreSpecifications` with `storeType: 'Ingest'`, legend-studio
`TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`): an ingest's datasets are a store's tables, joined and mapped as any
(the user: "Keep include for read that's fine as settled"). The world's status folds into `store::DB` (today it is a
second database, `market::World`, with its own connection in every runtime). The classes, the mapping and the screens
are as today.

## 7. Data products

- **An access point's target is optional upstream** (`V1_LakehouseAccessPoint.targetEnvironment: string |
  undefined`; a `V1_FunctionAccessPoint` has none). With a target, it is served only on a target of that engine, and a
  deployment elsewhere leaves it out, by name; with none, wherever its product is deployed. No override, no
  divergence. An access point with no target is written `LH(|...)`: lite's spelling, the grammar being closed.
- **Deploy the product, not a list of ingests** (proposed): `deploys: [ desk::Desk ]` lands what the product reads --
  its model access group's mapping, and the datasets its access points read -- as upstream's deploy artifact's
  `dependencyDatasets`. Written once, it cannot drift.
- **The data space takes the desk from its product** (proposed): `mappingProvider: desk::Desk.model;` (upstream's
  4.145.0 spelling; lite parses it, nothing resolves it yet) in place of `mapping:` in each environment. Runtimes stay
  in both: an environment's runtime must be one of the product's compatible runtimes, a check.
- **With no data product, everything works:** `deploys` lists ingests, and each environment names its `mapping:`.
  What is lost: the list repeated per deployment; drift caught on connect ("the mapping reads `ORDER_ROUTE`, which
  `market::DeskOnDeephaven` does not deploy") rather than prevented; no published API. `deploys` takes exactly
  ingests and data products -- never a mapping or a database, which would walk `store::DB`'s includes again.
- Lite has no data products today: no `DataProduct` in core's main code.

## 8. The deployment

```
###Lakehouse
Deployment market::DeskOnDeephaven
{
  deploys: [ marketdata::MarketDataFeed, products::SecurityMaster, positions::StartOfDay,
             market::WorldStatus, desk::Views ];             // or [ desk::Desk ], its data product (§7)
  sources: [ market::Kafka: [ tickerplant::Topics, secmaster::Topics, market::WorldStatus ],
             market::PositionsDrop: [ positions::StartOfDay ] ];
  live: market::Deephaven keep [ appended: LastRows(10000), milestoned: current, MARKET_TRADE: LastRows(25000) ];
  history: market::Lake keep [ appended: all, milestoned: versions ];
}
```

- **Direction (Persistence's):** a deployment points at what it deploys; nothing in an ingest points at a deployment.
  Many deployments may deploy one ingest.
- **`deploys`:** ingests and data products, the lakehouse's units (§7).
- **`sources`:** each source connection and what it feeds, in upstream's runtime `connectionStores` shape. A topic
  store is read; a delivered ingest receives (the positions files, the world's status topic). One Kafka connection
  per cluster.
- **`live`, `history`:** a target is a connection (the activators'). The live target reads history before today and
  its own rows since. The target's dialect decides how it makes, lands and keeps.
- **`keep`:** per target, a default per write mode stated once and overridden per dataset. Appended: `LastRows(n)`,
  `Last('PT8H')`, `all`. Milestoned: `current` (one row a key: lite's old LatestByKey) or `versions` (every version:
  what the history tier keeps). "Latest by key" was never a fact of the data; it is how much of its history a target
  holds. An appended dataset on a live target with no `keep` is refused by name: the compiler assumes no default
  (AGENTS.md invariant 4).
- **Access points:** one with no target is served on every target; one with a target, only on a target of that engine.
- **Lifecycle:** `validate`, `render`, `install`, `uninstall` (§9).

**Not on the runtime.** The user asked: "Does it make sense to put the deployment stuff on runtime instead?" On the
runtime there would be one element fewer and nothing to disagree; it stays separate because:

1. Legend has many runtimes and an engine is set up once: every service has one, every data product lists compatible
   ones, tests make their own. Two runtimes on one server with different `keep` would have no winner.
2. A runtime is read on every query, by the people reading data; a deployment is applied at release, by whoever runs
   the server. In production, a desk's runtime may point at a server someone else's pipeline installed.
3. The history tier has no queries of its own: on runtimes it would need one nobody queries with.
4. The runtime is upstream's element, its JSON shared with Studio and legend-engine; fields added to it would be lost
   on a round trip. A separate element leaves it exactly upstream's, and ends today's Kafka bindings that no query uses.

| | The runtime (upstream's, unchanged) | The deployment (lite's) |
|---|---|---|
| Answers | where do my queries go? | what lands on this engine, from where, keeping how much? |
| How many | many per engine | one per engine |
| Used | on every query | at release |
| By | readers: screens, services, data products | whoever runs the server |

Disagreement is caught by the check on connect (§9).

**Not a function activator.** The user: "Should we just reuse function activator or no". No. An activator deploys one
function; a deployment lands datasets from sources and keeps their history, which an activator's metamodel
(`function`, `ownership`, `activationConfiguration`, `actions`) has no place for. Each activator is one backend, the
limit this design removes; an environment is dozens of elements, each an activator repeating its target and owner; an
activator's environment is the server's configuration, not the model's. The user also turned down a per-backend
`DeephavenTable` activator (2026-10-29) and called upstream's `DeephavenApp` half built (2026-10-22). Taken from
activators: the target is a connection, `ownership: Deployment { identifier }`, rendering without applying, one entry
with a generator per backend.

**The bridge to activators** (the user: "Add it to doc"): a `Deployment` is, in the metamodel, a kind of upstream's
`DeploymentConfiguration` (an empty marker class: what an activator's `activationConfiguration` points at). A later
activator can then deploy a function into the same environment -- upstream's `HostedService` serving a screen with
`activationConfiguration: market::DeskOnDeephaven`, its connections and owner reused. Activators stay the tool for
serving functions, later.

## 9. What deploying does

- **`validate`:** compile, and check that each element deployed is landable on each target (by the target's
  dialect), that every source a dataset reads is bound in `sources`, that a delivered ingest's delivery is bound, and
  that an appended dataset on a live target has a `keep`.
- **`render`:** each target's plan, as data, versioned; nothing applied. Its stages are Persistence's generator's:
  make (pre-actions), evolve (schema evolution), land (ingest), keep (post-actions).
- **`install`:** apply, reconciling: make what is missing, evolve what changed (a changed live table is rebuilt from
  the log; the history tier evolves in place), leave what matches. A server's start is an install on an empty
  server, so starting and changing are one act. **`uninstall`** removes what the deployment made.
- **On Deephaven:** the plan is data for one fixed, versioned loader that ships with lite. Each table subscribes with
  Deephaven's own reader (`KafkaTools.consumeToTable`); an appended table is a ring or an append table; a milestoned
  table kept `current` is a blink table, `lastBy` its key, its milestoning columns filled (`LAKE_IN_ID` the record's
  batch, `LAKE_OUT_ID` 999999999, `LAKE_DIGEST` a hash of its own columns), with the delete indicator's rows dropped (measured
  2026-10-30: Kafka's empty delete record fails the table, a flagged record works); a day travels as text and is made
  a `LocalDate` on the server (42.7's JSON reader takes none, measured in B1). The loader replaces the generated
  Groovy (`DeephavenProgram`, deleted).
- **In the tab:** exec runs the plans C0 built (make, land, keep, as host-session loads). The simulated world
  publishes its topics inside the tab (the source `Simulated`: a browser cannot open a Kafka connection); the tab
  lands from it with the same plans (a milestoned table's columns filled, closed versions dropped where it keeps
  `current`), and the world's direct writes into the tables are deleted.
- **In the lake:** a writer lands the topics as Iceberg or Parquet tables, keyed datasets with every version
  (upstream's milestoning columns, which lite refuses today: S404).
- **The checks:** after installing, every table and view exists with its declared types, or the install fails naming
  it. On connect, the engine a runtime names must hold every table and view its mapping reads, with the declared
  types, or the desk is refused, naming what is missing. The second check is needed anyway: no runtime knows what is
  on a server.

## 10. Demo and production

| | Demo | Production |
|---|---|---|
| Kafka connections | the door, on this machine | the firm's cluster, TLS |
| Who publishes | the simulated world, every topic | each owning team (reference data by change capture) |
| The live engine | Deephaven on this machine | the desk's Deephaven server |
| The lakehouse | a local Iceberg folder | the firm's lakehouse |
| The files' location | a local folder | the owner's drop location |
| The record format | JSON | Avro or Protobuf, Legend's schemas published to the registry |
| Who runs `install` | the start script | the desk's release pipeline |

**What cannot be identical:** a browser cannot open a Kafka connection, so the tab's log is in the tab (or a bridge
to the door): a transport below the plans. Configuration the demo never uses (Avro, a registry, real auth, topics of
several partitions) needs its own tests: one path means the same code, not that the demo runs every value. The door
stands in for Kafka (the user, 2026-10-29: "door for sure, no need for tansu just use door"); a lane against a real
broker would prove it hides nothing, and is the user's call (§13). Scale and failures (a broker restarting, a consumer
falling behind) need a long-running test.

## 11. Reference data, positions and decision P

- **The masters publish** (proposed; the user asked: "Is the right thing for ref data for the ref data team to
  publish to kafka? Or no"). Each change goes to a keyed, compacted topic, by change capture; Deephaven subscribes to
  it as to prices. The team promises a key per record, a compacted topic (not time retention, or a restarted server
  loses instruments that have not changed lately), deletes as a flagged record (Kafka's empty one fails Deephaven
  42.7), and the shape in its topic store. Fresher than a daily pull, no password for lite, nothing for lite to run.
  In the demo the world publishes them through the door.
- **Positions are a snapshot** that must be whole as of one moment (published record by record, a screen could show
  half of yesterday's positions with half of today's): files, `Snapshot<Parquet>`, at a drop location the environment
  declares (a `Files` connection).
- **Decision P** (how pulled reference data reaches Deephaven; options (a) to (d), docs/DESK_DATA_ROADMAP_2026_10_29.md
  §2) is deferred: with the masters publishing, the desk holds no pull -- its five pulls become topic reads, like
  prices -- so P has nothing to decide, and a pull on a Deephaven target is refused by name until a model needs one.
  It is relitigated only if a reference data team cannot publish themselves (the user, 2026-11-15), and (c) -- lite runs the pull and publishes to a keyed
  topic -- is still the recommendation then. A pull is `Snapshot` of a database query on its schedule.

## 12. Risks and unknowns (to measure before promising)

1. Materialized views in the tab's DuckDB: maintained incrementally, or only recomputed after each batch.
2. The history tier: DuckDB writing Iceberg, Deephaven reading it, and the cut between history and live (their
   overlap, by key or by offset).
3. `batch_milestoned`'s columns (`LAKE_IN_ID`, `LAKE_OUT_ID`; business temporal `LAKE_FROM`, `LAKE_THRU`), refused by
   lite today (S404).
4. A restart reads each topic from its beginning; it should read only as far back as each table keeps (Deephaven's
   consumer started at an offset: to measure on 42.7). A topic's own retention must cover the ingest's: the
   producer's setting, documented.
5. Deletes rest on the publisher's promise of a flagged record.
6. The door, configuration the demo never uses, scale and failures (§10).
7. Topics as relational tables: flat records only (§4).
8. The size: the order commits to the live desk on one path (§15, steps 0 to 6), then reassesses.

## 13. Decisions

**Ruled, 2026-11-14, in the user's words:**
- "Keep include for read that's fine as settled".
- "Agree deploy own element and keep moves".
- Lakehouse first: "would like to use Lakehouse constructs as a preference though", `###Persistence` and the function
  activators read "for inspiration".
- One path: the demo "built in a way that is the production flow but configured as private so there really is only
  one path, but with different config" (the user's aim, agreed).
- Legend is the schema registry (the user's question, agreed).
- The function activators not reused, the bridge recorded: "Add it to doc".
- `Deployment` in `###Lakehouse`: "Deployment in lakehouse yes".
- Upstream's ingest words (§5): "Upstream ingest yes".
- The masters publish (§11): "Reference published yes".
- A lane against a real Kafka-compatible broker: "Real Kafka maybe at the end" (the last step, §15).

**Ruled, 2026-11-15** ("Yes to all, decision P we should just say it's only relitigated if ref data team can't
publish themselves"):
- Data products at step 2, the small form: one product, `desk::Desk`, over today's mapping; the domain products
  (and splitting the desk's mapping by domain) a later decision of their own.
- Decision P: relitigated only if a reference data team cannot publish themselves.
- A deployment's owner (`ownership: Deployment { identifier }`) later, when production needs to know who may
  install; an ingest's owner (`owner=AppDir(...)`) stays as it is.
- The positions' drop location: upstream's nearest connection read first, before step 4. (Code search of
  legend-engine at `05a3d33c` found neither `ExternalFormatConnection` nor `UrlStreamExternalSource`; the reading
  is still owed.) A lite `Files` type only if nothing upstream fits.
- The history tier's connection at the history step.
- `Simulated` as the tab's source, `TabLog` dropped.
- No sandbox stage now.
- Standing from before: "if DeepHaven cannot do something refuse and keep going" (2026-11-14); "door for sure, no need
  for tansu just use door" (2026-10-29); no push to main and no PR without the user's word.

**Open: how a keyed dataset lands** (found 2026-11-15, preparing step 0). Upstream's `batch_milestoned` keeps every
version and gives every reader three more columns -- `LAKE_DIGEST` (a string), `LAKE_IN_ID` and `LAKE_OUT_ID`
(batch ids), plus `LAKE_FROM` and `LAKE_THRU` for business temporal (legend-studio `V1_CubeIngestDefinition.ts`,
`milestoningColumns`). The user, 2026-11-15: "Batch milestone is all versions but there is a deterministic way to
get latest which is lake out = 99999999". Upstream's persistence component writes the open version's batch id as
`999999999` (`InfiniteBatchIdValueVisitor.INFINITE_BATCH_ID`, legend-engine `05a3d33c`; the component's README:
`infiniteBatchIdValue`, "Value to be used for Infinite batch id", default `999999999`). The lakehouse uses the same:
legend-studio's Cube round trip over a lakehouse ingest (`legend-cube-builder/src/__tests__/
CubeIngest.engine-roundtrip-test.ts`) reads current rows by a filter `LAKE_OUT_ID = 999999999` ("The LAKE_OUT_ID of
the rows no later batch replaced"), with each row's own values first, then `LAKE_DIGEST`, `LAKE_IN_ID` and
`LAKE_OUT_ID`, the two ids `BIGINT NOT NULL`. Nine nines; the user's "99999999" read as this value.
- **(A), recommended since the user's point:** keyed datasets are `batch_milestoned` now. Their tables carry the
  three columns on every target; the latest rows are `LAKE_OUT_ID = 999999999`, read through upstream's ordinary
  relational filter (`Filter Instrument_Latest(INSTRUMENT.LAKE_OUT_ID = 999999999)` in `store::DB`, `~filter
  [store::DB] Instrument_Latest` in the class mapping). A live target keeping `current` holds only open versions (the
  filter always true there); the history tier keeps every version under the same model and filter, and an "as of"
  query reads `LAKE_IN_ID` and `LAKE_OUT_ID`. One model on every tier: nothing migrates at the history step.
  Screens do not change (they read classes). `LatestByKey` retires; S404 is lifted for `batch_milestoned`
  (business temporal stays refused). To settle in step 0, each a register row: what a batch is for a stream (each
  landed delivery; on Deephaven, a record's Kafka offset in a one-partition topic) and how `LAKE_DIGEST` is computed
  (a hash of the row's own columns by the database; upstream's algorithm is not visible).
- **(B):** keep lite's `LatestByKey` for current-only data until the history tier. Smaller now, but every keyed
  mapping migrates then.

## 14. Differences from upstream, to register

Each becomes a docs/SEMANTICS_REGISTER.md row, ruled by the user, when it is built:

- the deployment element and its `keep`;
- a function source that reads stores (topics, databases), not only datasets;
- a pull's schedule;
- a delivered ingest's records on a topic, not only as files;
- the spellings `deleteIndicator=(...)` and `LH(|...)`;
- the connection types `Kafka`, `Files` and `Iceberg`, and the source `Simulated`;
- an access point with no target served on every target of the deployment that deploys its product.

## 15. The order

0. **Upstream's ingest words:** A1's grammar and JSON, C0's plans, `planner/IngestTopics`, the desk's sources
   (`core/src/test/resources/stress/29-desk-sources.pure`), their tests.
1. **The deployment element, `validate` and `render`:** each target's plan (Deephaven's as data for the reconciling
   loader; the tab's, C0's plans); `deploys` takes ingests and data products from the start. The generated Groovy
   (`DeephavenProgram`, `DeephavenStartup`'s program) deleted; `deephaven_door_test` runs the loader and the plan.
2. **The desk's data products** (if ruled at this step): one per domain and `desk::Desk` over them; `mappingProvider`
   resolved.
3. **`install`**, with the check after it and on connect.
4. **Reference data as published topics; positions as files.**
5. **The tab on the same path:** the world publishes to the tab's log, the tab lands with the same plans.
6. **`market.groovy` deleted:** every Deephaven lane green with only the feed and the plan.
7. **Last, maybe:** a lane against a real Kafka-compatible broker (the user: "Real Kafka maybe at the end").

Then, each its own decision: materialized views on both live targets; the history tier (Iceberg or Parquet, read by
Deephaven and DuckDB; the cut between history and live); data products served wherever deployed; production
configuration (Avro with Legend's schemas published to a registry, reading less on restart). The full local gate runs after step 1: since B1 and B2 only their targeted suites have run.

## 16. Upstream, read

Pinned: legend-studio `9775e7283cf2e768293db9982573cecbe8280c4b`, legend-engine
`05a3d33c4316c05ab6828f7c416de65cd9ffe0c8`.

### 16.1 Lakehouse (preferred)

- **Ingest** (`legend-graph/.../ingest/V1_IngestDefinitionSpecification.ts`, `V1_IngestDefinition.ts`): read modes
  `Delta` (an optional `deleteIndicator { deleteField, deleteValues }`), `Snapshot`, `TemporalSnapshot`, `Undefined`;
  message formats `CSV`, `Avro`, `JSON`, `Parquet`; dataset sources `serializedSource` (a schema and a format:
  producers deliver to the ingest) and `FunctionSource` (a function); write modes `append_only`, `batch_milestoned`,
  `batch_milestoned_business_temporal`, settable per dataset; preprocessors (`TakeMaxVersionFieldRecord`,
  `OverwriteOnSnapshot`, `FilterOutExactDuplicateRecords`, ...); an owner, `AppDir` (production, prod-parallel) or
  Kerberos.
- **Reading it as tables:** a relational `Database`'s `includedStoreSpecifications` with `storeType: 'Ingest'`, the
  ingest's dataset group a schema and each dataset a table, joined and mapped as any
  (`__tests__/roundtripTestData/TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`, "ingest-generated databases").
- **Data product** (`.../dataProduct/V1_DataProduct.ts`): a `LakehouseAccessPoint`'s `targetEnvironment` is optional;
  a `FunctionAccessPoint` (a `query`) has none.
- **The deploy artifact** (`.../lakehouse/deploy/V1_DataProductArtifact.ts`): each access point's implementation holds
  `resourceBuilder`s -- a `DatabaseDDL { targetEnvironment, script, resourceType: VIEW | TABLE | PROCEDURE,
  reproducible }` or a function access point -- and its `dependencyDatasets` (each an ingest with its producer, and a
  dataset). The platform generates it; a closed service applies it. The ingest's deploy is fixed to the lakehouse:
  hence no deploy element (the user, 2026-11-14).

### 16.2 `###Persistence` (older, generic)

- **`Persistence`** says what: a trigger (`Manual`; a cron trigger is in the protocol, its parser unimplemented), a
  service (the source query), targets (v1 `persister: Batch | Streaming` with sinks; v2 `serviceOutputTargets`, a
  `Relational #{ table; database; temporality }#` target), a notifier, tests. **`PersistenceContext`** says where, for
  one environment: `persistence`, `platform` (`Default`; `AwsGlue #{ dataProcessingUnits }#`), `serviceParameters`,
  `sinkConnection`. The context points at its persistence, so one persistence has many contexts.
- **How rows land:** snapshot, delta, append-only, nontemporal, unitemporal or bitemporal; deletes by `mergeStrategy:
  DeleteIndicator { deleteField: deleted; deleteValues: ['Y', '1', 'true']; }` (v2: `datasetType: Delta {
  actionIndicator: DeleteIndicator { ... } }`), the lakehouse's words; milestoning by batch id, date-time or both.
- **Actions:** `persistence/v1/platform` `validate`, `install`, `uninstall`, dispatched by the context's platform (no
  open-source implementation found).
- **The generator** (`legend-engine-xt-persistence-component`) turns an ingest mode into SQL plans per database
  (ansi, H2, MemSQL, Snowflake, BigQuery, Postgres, DuckDB): `preActionsSqlPlan`, `schemaEvolutionSqlPlan`,
  `ingestSqlPlan`, `metadataIngestSqlPlan`, `postActionsSqlPlan`, dry runs, locks.
- `Streaming { sink }` carries a sink alone; there is no Kafka anywhere in the DSL.

### 16.3 Function activators (one entry, one backend each)

- **`FunctionActivator`** (`core_function_activator/metamodel.pure`): `function`, `ownership` (`Deployment {
  identifier: '...' }`, a deployment id), `activationConfiguration` (a `DeploymentConfiguration`, its target a
  connection), `actions`; an unused `DeploymentStage { SANDBOX, PRODUCTION }`.
- **One element per backend:** `SnowflakeApp`, `SnowflakeM2MUdf`, `BigQueryFunction`, `MemSqlFunction`,
  `HostedService`, `FunctionJar`, `DeephavenApp` (with a `###Deephaven` store); one service SPI dispatching by
  `supports(...)`.
- **Lifecycle** (`FunctionActivatorAPI`): `validate`, `publishToSandbox`, `renderArtifact`, `generateLineage`;
  production through artifact generation of a released version. The environment (sandbox or production) is the
  server's configuration.
- **Artifacts:** Snowflake `CREATE OR REPLACE SECURE FUNCTION ... AS $$ <sql> $$`, MemSQL `CREATE OR REPLACE FUNCTION
  ... RETURNS TABLE AS RETURN <sql>`, a BigQuery table-valued function, a hosted service posted over HTTP.

## 17. What each upstream construct lends

| From | Taken | Not taken |
|---|---|---|
| Lakehouse | what is deployed (ingests, data products); the ingest's words; the artifact: a resource per target, with the datasets it depends on; an access point's optional target | the one fixed platform; the owner choosing the environment |
| Persistence | the context pattern: a separate element per environment, pointing at what it deploys; `validate`, `install`, `uninstall`; the plan's stages (pre-actions, schema evolution, ingest, post-actions) | a service as the source; a platform per runtime |
| Activators | the target is a connection; `ownership: Deployment { identifier }`; rendering without applying; one entry, a generator per backend; `DeploymentConfiguration` as the bridge | one element kind per backend; the environment in the server's configuration |

The improvement over upstream: **one element and every backend.** Lite's one compiler with a dialect per target is
already the one entry with a generator per backend.

## Appendix A. The whole model, as a person would write it

One of each kind: an event stream, a keyed stream, reference data with deletes, a delivered snapshot, a delivered
topic, a materialized view, two environments. New against today: upstream's ingest words (`Delta<JSON>`,
`deleteIndicator`, `BatchMilestoned`, the key taken from the topic; a materialized view a dataset of an ingest with
no read mode, as upstream's); an access point with no target, `LH(|...)` (lite's spelling); the connection types
`Files` and `Iceberg` (`Files` until upstream's nearest connection is read, `Iceberg` at the history step) and the
tab's source `Simulated`; a Kafka connection with no `format` (it is the read mode's) and no `store` (the deployment
says what it feeds); the `Deployment` element; the keyed tables' latest-version filters (option A, awaiting the
user's yes, §13); the data product `desk::Desk`, deployed whole and providing the data space's mapping (step 2).
Everything else exists today. Before step 2, `deploys` lists the ingests and each environment names `mapping:`
(§7).

```
// ================================================================
// PART 1: WHAT THE DATA IS (names no engine)
// ================================================================

###Relational
// SOURCES: topics, each declared by the team that publishes it

Database tickerplant::Topics
(
  Table "tp.us-equities.trades.v3"
  (
    PRINT_ID      VARCHAR(200) PRIMARY KEY,
    PRINT_TIME    TIMESTAMP NOT NULL,
    INSTRUMENT_ID VARCHAR(200) NOT NULL,
    PRICE         DOUBLE NOT NULL,
    QUANTITY      DOUBLE NOT NULL
  )
  Table "tp.us-equities.nbbo.v2"
  (
    INSTRUMENT_ID VARCHAR(200) PRIMARY KEY,
    QUOTE_TIME    TIMESTAMP NOT NULL,
    BID           DOUBLE,
    ASK           DOUBLE,
    MID           DOUBLE
  )
)

Database secmaster::Topics
(
  Table "secmaster.instruments.v1"
  (
    INSTRUMENT_ID VARCHAR(200) PRIMARY KEY,
    TICKER        VARCHAR(20) NOT NULL,
    NAME          VARCHAR(200) NOT NULL,
    DELETED       BIT
  )
)


###Lakehouse
// DESTINATIONS: what lands, in upstream's words

// Market data, read from the ticker plant's topics
Ingest marketdata::MarketDataFeed Delta<JSON>
  owner=AppDir(production='1001')
[
  MARKET_TRADE( |#>{tickerplant::Topics."tp.us-equities.trades.v3"}# )
    writeMode=AppendOnly;
  MARKET_QUOTE( |#>{tickerplant::Topics."tp.us-equities.nbbo.v2"}# )
    writeMode=BatchMilestoned;
]

// Reference data, the security master's change-capture topic
Ingest products::SecurityMaster Delta<JSON>
  owner=AppDir(production='1002')
  deleteIndicator=(deleteField: 'DELETED', deleteValues: ['true'])
[
  INSTRUMENT( |#>{secmaster::Topics."secmaster.instruments.v1"}# )
    writeMode=BatchMilestoned;
]

// Positions: start-of-day snapshot files, delivered to the ingest
Ingest positions::StartOfDay Snapshot<Parquet>
  owner=AppDir(production='1003')
[
  POSITION( POSITION_ID:   Varchar(200)[1],
            BOOK_ID:       Varchar(200)[1],
            INSTRUMENT_ID: Varchar(200)[1],
            QUANTITY:      Double[1] )
    pk=[ POSITION_ID ] writeMode=BatchMilestoned;
]

// The world's status: records delivered to the ingest's own topic
Ingest market::WorldStatus Delta<JSON>
[
  STATUS( ITEM: Varchar(16)[1], STATE: Varchar(16)[1] )
    pk=[ ITEM ] writeMode=BatchMilestoned;
]

// A materialized view over landed datasets (an ingest with no read mode, as upstream's)
Ingest desk::Views
[
  TRADED_VOLUME(
    |#I{marketdata::MarketDataFeed.MARKET_TRADE}#
      ->groupBy(~[INSTRUMENT_ID],
                ~[VOLUME: x | $x.QUANTITY : y | $y->plus(),
                  TRADES: x | $x.PRINT_ID : y | $y->count()]) )
    pk=[ INSTRUMENT_ID ];
]


###Relational
// WHAT THE MODEL READS: the landed datasets as tables, joined

Database store::DB
(
  include Ingest marketdata::MarketDataFeed
  include Ingest products::SecurityMaster
  include Ingest positions::StartOfDay
  include Ingest market::WorldStatus
  include Ingest desk::Views

  Join Trade_Instrument(
    MARKET_TRADE.INSTRUMENT_ID = INSTRUMENT.INSTRUMENT_ID)
  Join Position_Instrument(
    POSITION.INSTRUMENT_ID = INSTRUMENT.INSTRUMENT_ID)

  // the latest version of each key (upstream's open batch id)
  Filter Instrument_Latest(INSTRUMENT.LAKE_OUT_ID = 999999999)
  Filter Position_Latest(POSITION.LAKE_OUT_ID = 999999999)
)


###Pure
// THE BUSINESS VIEW

Class products::Instrument
{
  instrumentId: String[1];
  ticker:       String[1];
  name:         String[1];
}

Class marketdata::MarketTrade
{
  printId:   String[1];
  printTime: DateTime[1];
  price:     Float[1];
  quantity:  Float[1];
}

Class positions::Position
{
  positionId: String[1];
  bookId:     String[1];
  quantity:   Float[1];
}

Association marketdata::Trade_Instrument
{
  trades:     marketdata::MarketTrade[*];
  instrument: products::Instrument[1];
}

Association positions::Position_Instrument
{
  positions:  positions::Position[*];
  instrument: products::Instrument[1];
}


###Mapping
Mapping desk::DeskMapping
(
  products::Instrument: Relational
  {
    ~filter [store::DB] Instrument_Latest
    ~mainTable [store::DB] INSTRUMENT
    instrumentId: [store::DB] INSTRUMENT.INSTRUMENT_ID,
    ticker:       [store::DB] INSTRUMENT.TICKER,
    name:         [store::DB] INSTRUMENT.NAME
  }
  marketdata::MarketTrade: Relational
  {
    ~mainTable [store::DB] MARKET_TRADE
    printId:   [store::DB] MARKET_TRADE.PRINT_ID,
    printTime: [store::DB] MARKET_TRADE.PRINT_TIME,
    price:     [store::DB] MARKET_TRADE.PRICE,
    quantity:  [store::DB] MARKET_TRADE.QUANTITY
  }
  positions::Position: Relational
  {
    ~filter [store::DB] Position_Latest
    ~mainTable [store::DB] POSITION
    positionId: [store::DB] POSITION.POSITION_ID,
    bookId:     [store::DB] POSITION.BOOK_ID,
    quantity:   [store::DB] POSITION.QUANTITY
  }
  marketdata::Trade_Instrument: Relational
  {
    AssociationMapping
    (
      trades:     [store::DB]@Trade_Instrument,
      instrument: [store::DB]@Trade_Instrument
    )
  }
  positions::Position_Instrument: Relational
  {
    AssociationMapping
    (
      positions:  [store::DB]@Position_Instrument,
      instrument: [store::DB]@Position_Instrument
    )
  }
)


###Pure
// A SCREEN

function desk::screens::tape(): Relation<(at: DateTime[1],
    ticker: String[1], price: Float[1], size: Float[1])>[1]
{
  marketdata::MarketTrade.all()
    ->project(~[at:     x | $x.printTime,
                ticker: x | $x.instrument.ticker,
                price:  x | $x.price,
                size:   x | $x.quantity])
}


###DataProduct
// THE PUBLISHED API

DataProduct desk::Desk
{
  title: 'Trading desk'
  description: 'The desk\'s classes, live, and its published views'
  accessPoints: [
    model ('The desk classes') Model
    {
      mapping: desk::DeskMapping;
      compatibleRuntimes: [ deephaven: market::DeephavenRuntime,
                            inTab: market::InTabRuntime ];
      defaultRuntime: deephaven;
    }
    [
      // no target: served wherever the product is deployed
      volume ('Traded volume by instrument'):
        LH(|#I{desk::Views.TRADED_VOLUME}#)
    ]
  ]
}


// ================================================================
// PART 2: WHERE IT RUNS (the only part demo and production differ in)
// ================================================================

###Connection

RelationalDatabaseConnection market::Kafka
{
  type: Kafka;
  specification: Kafka { bootstrapServers: '127.0.0.1:19092'; };
  auth: Test;
}

RelationalDatabaseConnection market::PositionsDrop
{
  type: Files;
  specification: Files { location: 'file:///var/desk/positions/'; };
  auth: Test;
}

RelationalDatabaseConnection market::Deephaven
{
  store: store::DB;
  type: Deephaven;
  specification: Deephaven
  {
    host: 'localhost';
    port: 10000;
    useTls: true;
  };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}

RelationalDatabaseConnection market::Lake
{
  store: store::DB;
  type: Iceberg;
  specification: Iceberg { warehouse: 'file:///var/desk/lake/'; };
  auth: Test;
}

RelationalDatabaseConnection market::InTab
{
  store: store::DB;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}

// the simulated world, publishing its topics inside the browser tab
RelationalDatabaseConnection market::SimulatedWorld
{
  type: Simulated;
  specification: Simulated { world: 'market'; };
  auth: Test;
}

RelationalDatabaseConnection market::TabFiles
{
  type: Files;
  specification: Files { location: 'tab://positions/'; };
  auth: Test;
}


###Lakehouse
// DEPLOYMENTS: what lands where, and how much each target keeps

Deployment market::DeskOnDeephaven
{
  deploys: [ desk::Desk ];      // what the product reads lands with it
  sources: [ market::Kafka: [ tickerplant::Topics,
                              secmaster::Topics,
                              market::WorldStatus ],
             market::PositionsDrop: [ positions::StartOfDay ] ];
  live: market::Deephaven
    keep [ appended: LastRows(10000),
           milestoned: current,
           MARKET_TRADE: LastRows(25000) ];
  history: market::Lake
    keep [ appended: all,
           milestoned: versions ];
}

Deployment market::DeskInTab
{
  deploys: [ desk::Desk ];      // what the product reads lands with it
  sources: [ market::SimulatedWorld: [ tickerplant::Topics,
                                       secmaster::Topics,
                                       market::WorldStatus ],
             market::TabFiles: [ positions::StartOfDay ] ];
  live: market::InTab
    keep [ appended: LastRows(5000),
           milestoned: current ];
}


###Runtime
// WHERE QUERIES GO

Runtime market::DeephavenRuntime
{
  mappings: [ desk::DeskMapping ];
  connections: [ store::DB: [ live: market::Deephaven ] ];
}

Runtime market::InTabRuntime
{
  mappings: [ desk::DeskMapping ];
  connections: [ store::DB: [ live: market::InTab ] ];
}


###DataSpace
// THE DESK

DataSpace market::TradingDesk
{
  executionContexts:
  [
    {
      name: 'Deephaven';
      mappingProvider: desk::Desk.model;
      defaultRuntime: market::DeephavenRuntime;
    },
    {
      name: 'In this tab';
      mappingProvider: desk::Desk.model;
      defaultRuntime: market::InTabRuntime;
    }
  ];
  defaultExecutionContext: 'Deephaven';
  title: 'Trading desk';
}
```

## Appendix B. The turns on the way, and why

So a later session does not walk them again:

1. **Generated Groovy in the plan** became a plan of data and a fixed loader (G4; the user's challenge, §1).
2. **`include Ingest`** was questioned ("I'm not sure it makes sense for databases to include ingest specs, feels a
   bit weird"), found to be upstream's (Studio's round-trip fixture), and kept for reading.
3. **Where data lands** was first inferred from the runtime's bindings and the read database's includes, then written
   explicitly, then inferred again for desks' own engines. The two tiers settled it: what a target keeps differs per
   target, so there has to be an explicit place that says it -- the deployment.
4. **A shared serving layer or each desk's engine:** settled as the log shared, the engines each desk's (§2.4).
5. **The two tiers** were put under "later" once, and restored (the user: "did you just reverse the most important
   part "two tiers, not one" this was the most important piece of your expert design").
6. **The topic contract folded into the ingest:** proposed, then withdrawn on the user's question of source and
   destination (§4).
7. **The deployment choosing a data product's target:** proposed, then withdrawn: upstream's target is optional (§7).
8. **The deployment on the runtime:** considered on the user's question, kept separate (§8).
9. **Function activators:** not reused; `DeploymentConfiguration` is the bridge (§8).
10. **Data products as what is deployed:** proposed; everything works without them (§7).
11. **Reference data:** from pulls under decision P to the masters publishing, on the user's question (§11).
12. **`Derived`:** proposed as a materialized view's read mode, dropped once upstream's fixture showed views in
    ordinary ingests (§5).
13. **`TabLog`:** proposed for the tab's source, replaced by 2026-10-29's `Simulated` (§13).
14. **`LatestByKey` onto `batch_milestoned`:** first mapped as a rename, then found to add milestoning columns to
    every reader, then (the user: "there is a deterministic way to get latest which is lake out = 99999999") read
    through an ordinary filter on the open batch id, 999999999 (§5, §13).
