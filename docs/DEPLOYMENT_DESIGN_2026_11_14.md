# The desk's data, deployed: the design (2026-11-14)

**Status: AGREED 2026-11-15.** Every decision is ruled (§13); step 0 is under way (§15.1).
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

**How a keyed dataset lands, on each tier** (ruled 2026-11-15, the user: "Agreed, record it in the design"). History
keeps upstream's batch milestoning exactly, as legend-engine's persistence component plans it (`UnitemporalDeltaPlanner`,
`05a3d33c`): a version is closed by `batch_id_out = {TABLE_BATCH_ID} - 1` when a staged row of its key has another
digest or is marked deleted; a new version is inserted only where no open version of its key has the same digest and
the row is not marked deleted; a delete closes and inserts nothing; a snapshot closes every open key it does not hold.
A live engine keeps only the open versions, so it lands for speed (the user: "I would optimize for speed instead of
space for live/streaming and maybe for space on historical"): what upstream closes it replaces or removes. Both tiers
hold the same current rows -- every `LAKE_OUT_ID = 999999999` row live is the open version history holds for its key
-- which a test proves by landing the same deliveries on both and comparing them.

| | Live (`keep: current`) | History (`keep: versions`) |
|---|---|---|
| An update | the key's row replaced | its open version closed at batch - 1, the new one inserted |
| A delete | the row removed | its open version closed at batch - 1, nothing inserted |
| An unchanged row | replaced, no comparison | a new version by default; skipped by the digest as the target's option (upstream's space saving) |
| A snapshot | the whole table replaced | every open row closed, the whole snapshot inserted as a new batch at 999999999; upstream's diff (close only what changed) as an option |
| `LAKE_IN_ID` | the record's partition-prefixed offset (`partition × 10^12 + offset`; the tab: its landing counter) | a per-table counter of landed micro-batches; a batch-metadata table holds each batch's time and the partition/offset range it covered (as-of reads; exactly-once replay) |
| `LAKE_OUT_ID` | always 999999999 | 999999999 while open, else the closing batch - 1 |
| `LAKE_DIGEST` | computed, never compared | MD5 of a canonical text of every own column in declared order, a fixed null marker and separator, each type written alike on every engine (a conformance test digests a battery of values on each); compared when the option is on |

The rulings behind it, 2026-11-15: a version closes at batch - 1, as upstream ("1 confirmed"); an unchanged row is no
new version, as upstream, on history ("2 confirmed"); a live engine deletes for real ("maybe milestoning on live engine
should actually be real delete? For speed and space"); a stream's batch -- the user's ideas, a partition prefix and an
offset or a timestamp -- is the partition-prefixed offset live and a micro-batch counter with its time and offsets
in history; the digest is "something fast like md5 and all columns and nulls normalized to match across engines",
and the space savings optional, history's alone ("I would actually not implement any space optimization in
live/stream implementation or at least make it all optional"); a snapshot makes the active rows the latest snapshot,
history by closing every open row and inserting all ("the speed optimization is to simply soft delete all current
active rows and simply insert all new records with new batch and 99999999"); delete values compared as text and
business temporal refused ("7, 8 okay"). What step 0 built is the live column; the history column is the history
tier's, after the filter fix.

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
  `Last('PT8H')`, `all`. Milestoned: `current` (one row a key: lite's old LatestByKey, landed for speed, §5's live column) or `versions`
  (upstream's milestoning, §5's history column; every version:
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
   Lite's parser does not read upstream's ingest accessor `#I{ingest.dataset}#` yet (found 2026-11-15, step 0a):
   the views' step adds it.
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
- How a keyed dataset lands on each tier, the table in §5: history upstream's batch milestoning exactly (a version
  closed at batch - 1, an unchanged row no new version, a delete a close), a live engine replacing and removing for
  speed; a stream's batch the partition-prefixed offset live and a micro-batch counter with a metadata table in
  history; the digest an MD5 of a canonical text, compared only on history, as its option; a snapshot replacing the
  live table and, in history, closing every open row and inserting all (upstream's diff an option); delete values
  compared as text; business temporal refused ("Agreed, record it in the design").
- Standing from before: "if DeepHaven cannot do something refuse and keep going" (2026-11-14); "door for sure, no need
  for tansu just use door" (2026-10-29); no push to main and no PR without the user's word.

**Ruled 2026-11-15: how a keyed dataset lands, option (A)** (the user: "Yes can be a filter or pure database view too
whatever is simplier"; a filter is simpler: a view would list every column again). Found preparing step 0: Upstream's `batch_milestoned` keeps every
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
3. **`install`**, with the check after it and on connect. (Built as §15.4a; the check on connect covers what the
   deployments land, not every table the mapping reads.)
4. **Reference data as published topics; positions as files.**
5. **The tab on the same path:** the world publishes to the tab's log, the tab lands with the same plans. (Built as
   §15.5a: the tab's own deployment and keep, its source a `Simulated` world; no log in the tab.)
6. **`market.groovy` deleted:** every Deephaven lane green with only the feed and the plan.
Before the history tier: **lite's lowering joins from a filtered class mapping** (found in step 0b), then the
keyed tables' latest-version filters. (Both done 2026-11-15: §15.6a.)

7. **Last, maybe:** a lane against a real Kafka-compatible broker (the user: "Real Kafka maybe at the end").

**The Deephaven run side, RULED 2026-11-16:** `docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md` (every request a runtime sends is one the plan declares; a grid's base the planner's, a `let`; a runtime per environment). The program after step 6: §15.7.

**State, 2026-11-15:** steps 0 to 6 built (§15.1 to §15.5a), the filtered-mapping join fixed and the 26 latest-version
filters on (§15.6a); the full local gate green on `claude/deephaven-3-browser` at 5a34e63d (529 of 541, the ten
Postgres lanes and `verify_app_test` green under `tools/postgres/as_unprivileged.sh`, this container's root). Item 7
is not built: a real broker is a binary this cloud session may not download and run; it waits on a machine or an
environment that has one. **Ruled 2026-11-15:** the positions' format CSV ("Csv is fine"), `Simulated` kept ("Keep
simulated", S410), the Deephaven lanes manual for the v0 PR and one PR ("Manual is fine and 1 PR is fine"). The check
on connect (§15.4a) is kept as lite's addition ("Why not B? Seems like a great addition", S411): upstream checks nothing
on connect.

### 15.1 Step 0 in detail (planned 2026-11-15)

Measured first: 16 keyed desk tables (`MARKET_QUOTE`, `FX_RATE`, `TRADE_ORDER`, `RFQ`, `INSTRUMENT`, `EQUITY`,
`OPTION`, `EXCHANGE`, `SECTOR`, `BUSINESS_UNIT`, `DESK`, `TRADER`, `COUNTERPARTY`, `RISK_LIMIT`, `POSITION`, `BOOK`),
each with one test-data block of 5 to 20 rows in `stress/93-testdata.pure`; about 80 mapping references to them;
no corpus test reads them raw but `BOOK` (4 reads). In order, each committed with its own tests:

- **0a. Grammar, protocol, model.** Read modes `Delta<FORMAT>` (with `deleteIndicator=DeleteIndicator(deleteField=
  '...', deleteValues=['...'])`) and `Snapshot<FORMAT>`, and an ingest with none (views only); the formats
  upstream's (`CSV`, `Avro`, `JSON`, `Parquet`). Write modes `AppendOnly` and `BatchMilestoned`; `LatestByKey` refused
  by name, naming its replacement; business temporal still refused. A `BatchMilestoned` dataset's table gains
  `LAKE_DIGEST` (VARCHAR), `LAKE_IN_ID` and `LAKE_OUT_ID` (BIGINT NOT NULL) after its own columns (S404 lifted for
  it). A pull is `Snapshot` with a function over a store and `schedule=Daily(...)` or `Every(...)`; `Stream` and
  `Pull<...>` retire. `retention=` stays until step 1 moves it to the deployment. Tests: the parser's, the JSON round
  trips, the model's.
- **0b. The desk's model.** `29-desk-sources.pure` and `market.pure` in the new words; a `Filter <T>_Latest(<T>.
  LAKE_OUT_ID = 999999999)` per keyed table in `store::DB` and `~filter` in each keyed class mapping; the test data's
  rows gain their three columns (one version each: `LAKE_IN_ID` 1, `LAKE_OUT_ID` 999999999); the corpus lanes and
  their oracle answers rerun.
  **Found 2026-11-15, the filters deferred** (landed the same day with the resolver's fix, §15.6a)**:** with the 26 `~filter`s in place, about 1,000 stress suites failed
  with `join condition references unknown column 'BOOK_ID' on its left side`: lite's lowering makes a filtered class
  mapping's table a sub-select that does not carry every column the class's joins read -- a compiler gap, since
  upstream joins from a filtered class mapping. Without the filters the corpus is green (above its floor of 5,614),
  and no answer changes: on a live target and in the test data every row is its key's open version, so
  `LAKE_OUT_ID = 999999999` is always true. The filters land with that compiler fix, which the history tier needs
  first (the order, §15).
- **0c. The planner.** `IngestTopics` reads `Delta` datasets (a topic store's table, or the ingest's own topic);
  C0's land plans for `BatchMilestoned` kept current: the key's row replaced, `LAKE_IN_ID` the delivery's batch,
  `LAKE_OUT_ID` 999999999, `LAKE_DIGEST` the database's hash of the row's own columns; a delete indicator's row
  removes its key; a `Snapshot` replaces the whole set. Tests: `HostOperationsTest`, `DuckDbIngestPlansTest`.
- **0d. The Deephaven generator,** until step 1 deletes it: the same words, the format read from the read mode (the
  Kafka connection's `format` removed), the milestoning columns filled, the delete indicator's rows dropped. Tests:
  `DeephavenStartupTest`, `deephaven_door_test`.
- **0e. The TypeScript side:** the live DuckDB engine's keyed tables, the door's topics, the world's feed, their tests.
- **0f.** S402 rewritten (and S404); the full local gate.

Then, each its own decision: materialized views on both live targets; the history tier (Iceberg or Parquet, read by
Deephaven and DuckDB; the cut between history and live); data products served wherever deployed; production
configuration (Avro with Legend's schemas published to a registry, reading less on restart). The full local gate runs after step 1: since B1 and B2 only their targeted suites have run.

### 15.2 Step 1, built (2026-11-15)

- **The element.** `Deployment` in `###Lakehouse` (`parser/section/LakehouseSectionGrammar` `parseDeployment`; the
  record `Protocol.PDeployment`, its JSON `_type:"lakehouseDeployment"` with keep rules `LastRows`, `Last`, `All`,
  `Current`, `Versions`; printed by `LakehouseComposer`; the model's `DeploymentDefinition`, found by
  `ModelContext.findDeployment`, its names resolved to full paths). Fields: `deploys` and a target required, each field
  once, a `keep` scope once. Register row S407.
- **`validate`** (`planner/DeploymentPlanner.validate`): every reason, by name -- a deployed name that is no ingest; a
  source that is no connection, or feeds what is no store; a topic whose store the sources bind to no Kafka
  connection, or to several; a dataset with no rule on the live target (none assumed), or one its write mode does not
  keep (an appended dataset kept `current`, a milestoned one `LastRows(n)`); a scope that is no deployed dataset; and,
  not landed yet, each by name: the history tier, `versions` on a live target, `Last('PT..')` on Deephaven, a live
  target other than Deephaven (the tab's is step 5), a pull, a file or a view on Deephaven (step 4 and later).
- **`render`** (`DeploymentPlanner.deephaven`): the live Deephaven target's plan as data (`sql/dialect/DeephavenLoad`,
  `kind: legend.deephaven.load`, `version: 1`): each table its name, topic, the brokers of the source's one Kafka
  connection, the format, its columns with the Java types a Deephaven column holds them as, and its keep (`rows`,
  `all`, or `current` with the key, the milestoning columns, the open batch id, the partition prefix and the delete
  indicator). The dialect checks every name and value there, so the loader meets none it must check. The boundary's
  `deephaven` request answers `plan` and `validate`; `program` is gone.
- **The loader** (`core/src/main/deephaven/legend-loader.groovy`, `//core:deephaven_loader`): fixed, versioned, never
  generated; it refuses a plan of another kind or version. `DeephavenProgram`, `DeephavenStartup` and their test are
  deleted.
- **The desk.** `environments.pure` declares `market::DeskOnDeephaven` (the three streams and the world's status, from
  one `market::Kafka` connection with no store; appended tables `LastRows(50000)`, trades 25,000, news 5,000; milestoned
  `current`); the four per-store Kafka connections and the runtime's topic bindings are gone (no query ran on them).
- **Measured** (`//datacube:deephaven_door_test`, the pinned Deephaven with the feed behind the door): the loader made
  all nine tables from the plan, each typed as its store declares it (the milestoned ones with their three columns),
  every appended row read once (trades 12,823 of 12,823), each milestoned table one row a key (quotes 38, orders 26,
  RFQs 50, world 4).
- **Left for later steps, as planned:** `deploys` takes data products at step 2 (lite has no data product element yet);
  `install` and the check on connect at step 3; the ingest's `retention` stays for the tab's landing and the door's
  topics until step 5.

### 15.3 Step 2, built (2026-11-15)

- **`###DataProduct`, the small form** (register S408): the title, description, decorations and access point groups --
  default groups and model groups with their mapping, holding lakehouse access points -- in the written forms Studio
  shows (its round-trip fixtures' comments), lite's own for a model group (`id ('...') Model { mapping: m::M; } [ ... ]`;
  Studio shows none, and a model group carries no runtimes at legend-studio `9775e72`, so Appendix A's
  `compatibleRuntimes` was withdrawn) and for an access point with no target (`LH(|...)`). Every other field refused
  by name, in text and JSON. The JSON is held to Studio's `TEST_DATA__DataProductRoundtrip.ts`
  (`core/src/test/resources/dataproduct/`).
- **`deploys` takes a data product:** it lands the datasets the product reads -- its model groups' mappings' main
  tables, as the compiled bindings stamp them, and its access points' table reads -- each found among the ingests its
  store includes (`planner/DataProducts`, upstream's `dependencyDatasets`); a table no ingest makes is named.
- **The desk:** `desk::Desk` in `market.pure` over `stress::AllMapping`; the data space's two contexts read their
  mapping from it (`mappingProvider: desk::Desk.model;`), checked by the compiler (the product, its one model group,
  its mapping) and resolved by query-store's context reader for DataCube's saved queries. The desk's deployment still
  lists its ingests: deploying `desk::Desk` would land the pulls and position files a Deephaven server does not land
  yet (step 4).
- **The bespoke world, until step 6:** `market.groovy` publishes the three milestoning columns on its keyed tables
  (the store has declared them since step 0, and the Deephaven desk lane had been refused on `LAKE_DIGEST`).
- **Measured:** `deephaven_desk_test`, `deephaven_market_test`, `deephaven_feed_test`, `deephaven_door_test` and
  `verify_live_desk_test` green; the loader now checks each table against the plan before publishing it (step 3's
  check after installing), and `datacube/demo/deephaven/install.ts` writes a deployment's install (step 3).

### 15.4 Step 4, the reference data, built (2026-11-15)

- **The masters publish** (§11): the five pulls are reads of the masters' own topic stores now --
  `secmaster::Topics` (`secmaster.instruments.v1`, `.equities.v1`, `.options.v1`), `refmaster::Topics` (exchanges,
  sectors), `hr::Topics` (business units, desks, traders), `clientmaster::Topics` (counterparties) and
  `riskdb::Topics` (`risk.limits.v1`) -- each ingest `Delta<JSON>`, `BatchMilestoned`, its columns and key the topic's
  (`core/src/test/resources/stress/29-desk-sources.pure`). The desk holds no pull; a pull on a Deephaven target stays
  refused by name, and decision P has nothing to decide.
- **The deployment** lands them: `market::DeskOnDeephaven` deploys the five, `market::Kafka` feeds their stores, each
  kept `current`.
- **The world** publishes them through the door (the feed writes each table a topic is made of: nothing new).
- **Measured** (`deephaven_door_test`): the loader made all nineteen tables from the plan, each typed as declared, every
  key read once (instruments 254, options 216, equities 30, sectors 12, counterparties 20, traders 18, limits 10,
  desks 6, exchanges 2, business units 1). `stress_suites`, the corpus gates, `verify_live_desk_test` and
  `desk_model_test` green: the landing tables did not change.
- **The positions files.** Upstream's nearest connection read first (§13), at legend-engine `05a3d33`: no
  `ExternalFormatConnection` or `UrlStreamExternalSource` remains in the tree; the nearest is the model-to-model
  `JsonModelConnection` / `XmlModelConnection` (a class's instances at a URL), which lands no dataset. So a lite `Files`
  type, as ruled ("A lite `Files` type only if nothing upstream fits"): `type: Files; specification: Files { location:
  'file:///...'; };`, no store, no query (as Kafka; register S409). A delivered ingest (a `Snapshot` in a file format,
  no schedule, its columns written out) is bound to one drop location in the deployment's `sources`
  (`market::PositionsDrop: [ positions::StartOfDay ]`); each dataset is a file there, `<location><DATASET>.<format>`.
  The Deephaven plan's table names its source, a topic or a file; the loader reads a file whole at start
  (`CsvTools.readCsv`, each column parsed as the type it is held as), its rows the server's first landing of that
  delivery (`LAKE_IN_ID` 1). **The format changed to CSV** (`positions::StartOfDay Snapshot<CSV>`, from Parquet): the
  world, standing in for position keeping, writes its files in Node with no Parquet writer (one would need DuckDB, and
  SQL, in the feed), and CSV is upstream's own example format (`Snapshot<CSV>`); a Parquet file on Deephaven is refused
  by name. **Ruled 2026-11-15** (the user: "Csv is fine").
- **Measured** (`deephaven_door_test`): all 21 desk tables land from the plan -- the nineteen topics and the two files
  (positions 148 rows, books 10), each typed as declared (an instant, a day, a truth among them), every key once.
  `stress_suites`, `core_tests_integration`, `verify_live_desk_test` and `deephaven_desk_test` green.
- **Not yet:** a new delivery while the server runs (a drop location watched); the next start reads it.

### 15.5 Step 6, built (2026-11-15): `market.groovy` deleted

- **One path.** No world runs inside the Deephaven server any more. The market world (the TS world, `demo/market-sim/`)
  publishes every topic behind lite's Kafka door and drops the positions files; the desk's deployment is installed for
  the pinned server -- its plan beside lite's loader -- and the loader makes all 21 tables from it when the server
  starts (`demo/deephaven/market/on-deephaven.ts`; for the lanes, `test/deephaven/desk-server.ts`).
  `bazel run //datacube:deephaven_desk` and the lanes `deephaven_desk_test`, `deephaven_market_test`,
  `deephaven_feed_test` and `deephaven_door_test` all start the desk's server this way. `market.groovy` and `market.app`
  are deleted; the fixture `deephaven_market_fixture` is the plain server, its application the install.
- **What changed to get there:**
  - The deployment keeps every row of an appended table on Deephaven (`appended: all`), as the bespoke world did (the
    server kept every trade); the ingests' `retention=` stays the tab's.
  - The door keeps each topic whole for the session (a keyed one compacted): a topic's retention is its cluster's, not a
    landing's (§8); it had applied the ingest's landing retention, which cut the tape a server reads from the beginning.
  - The TS world's crypto feed skips and counts what it cannot read or place, as the Groovy world did (`(n messages
    skipped)`), never taking one bad message as a refusal; its filings status is true wherever it runs.
  - The market lane's orders-against-fills check asks again for a few seconds: orders and fills are two topics, read
    each at its own pace, so a moment can catch a fill in flight between them (the Groovy world wrote both in one
    update cycle).
- **Not yet:** the door holds a session's records in the feed's memory (a long show grows it, as the server's own
  tables grow); a restart of the server re-reads every topic from the door.

### 15.4a Step 3, built (2026-11-15): `install` and the check on connect

- **`install`** (`datacube/demo/deephaven/install.ts`) writes the deployment's plan beside lite's loader and the
  server's application file; the loader checks each table it makes against the plan before publishing it, so a table
  not as declared fails the server's start, naming it (built with step 2).
- **On connect.** Before a desk reads anything on a Deephaven server, its host asks the planner what the server must
  hold (`deephaven` op `holds`; `DeploymentPlanner.onConnect`) and reads each table once on the server by the plan the
  planner wrote for it (`|#>{store.table}#`, its columns checked against the store's declaration, DH16). Every table is
  tried; any missing or held otherwise refuses every grid of that model, naming each and why
  (`engine-client/src/deephaven-held.ts` `DeephavenNotHeld`; DataCube's `deephaven-grids.ts` gates the engine each grid
  reads through on it). Measured on the desk: 21 tables, read at once.
- **Narrowed from §9's words, posted to the user.** §9 says the server must hold "every table and view its mapping
  reads". The desk's runtime maps the whole stress corpus (`stress::AllMapping`: 405 tables across its stores, measured
  2026-11-15), and its server holds the desk's 21, so that rule would refuse the desk on every connect. What is checked:
  every table a deployment whose live target the runtime connects to lands there, read through a store of the
  runtime's that offers it. A table the runtime reads that no deployment lands is refused where a query reads it
  (DH16), naming it. Checking the mapping's tables needs the tables a desk's queries read (its saved queries), not its
  mapping's: later, if wanted.
- **Upstream does not do this** (read 2026-11-15 in legend-engine `05a3d33`, the pinned commit): nothing checks a
  server's tables when a runtime connects. Its Deephaven executor (`legend-engine-xt-deephaven-executionPlan`,
  `DeephavenExecutionNodeExecutor`) sends the query; a table the server lacks fails that query, as Deephaven's own
  error ("Deephaven query execution failed: ..."), and no column's type is compared with the store's declaration;
  `DeephavenSession` opens the session and checks nothing. The one "test connection" (`core_relational_mft`'s
  `testConnection.pure`) builds a test runtime for mapping tests. Upstream's `DeephavenApp` (a function activator)
  generates an application from a query and checks no table. Read, not exhaustive: the checkout is blobless, and a
  whole-tree search fetches every file. So the check on connect is lite's addition -- earlier and whole-list where
  upstream fails at the first query -- as DH16's type comparison is (ruled "always strict", 2026-10-29). **Ruled
  2026-11-15: kept** (the user: "Why not B? Seems like a great addition"; register S411).
- **Proven on the pinned server** (`//datacube:deephaven_market_test`): the tables listed are exactly the deployment's,
  the server holds each as declared, and a deployment of a table the server's install never landed is refused, naming
  it. `//datacube:deephaven_desk_test` opens the desk through the check.

### 15.5a Step 5, built (2026-11-15): the tab lands by a deployment

- **The tab's own deployment.** `market::DeskInTab` (`datacube/demo/deephaven/market/environments.pure`) deploys the
  same ingests as `market::DeskOnDeephaven`, its live target the tab's DuckDB (`market::InTab`) with its own `keep`:
  `[ appended: LastRows(50000), milestoned: current, MARKET_TRADE: LastRows(25000), NEWS_STORY: LastRows(5000) ]` --
  exactly what the four ingests' `retention=LastRows(...)` said, which are deleted from the desk's sources
  (`29-desk-sources.pure`). An ingest says what a dataset is; what a target keeps of it is its deployment's.
- **Its source: `Simulated`.** The tab's world runs in the tab, so nothing is published to a log it reads: the
  deployment's `sources` bind every ingest to `market::SimulatedWorld`, a connection of lite's own type `Simulated
  { world: 'market'; }` (JSON `_type:"simulated"`; no store, no query, as `Kafka` and `Files`; register S410). A tab
  target must bind each landing to exactly one `Simulated` source; a Deephaven target refuses one.
- **The same plans.** The tab asks the planner for each dataset's plans with the deployment's name
  (`ingestPlans`'s `deployment`, `Boundary.ingestPlans`; `IngestPlanner.plans(..., deployment)`), and the keep plan is
  the deployment's rule for that dataset (`DeploymentPlanner.keptRows`); without a deployment the ingest's own
  `retention`, for a model that declares no deployment.
- **What a live target is** is read from its dialect, not its type's name: a server whose tables a plan makes at its
  start is Deephaven's (`tablesMadeAtStartup`), one the platform runs plans on is a tab's own; any other is refused by
  name. `deephaven`'s `plan` refuses a tab's target, naming the ingests' plans as its road.
- **Not yet:** a span of time (`Last('PT8H')`) is refused on any live target; the tab does not read a log (the door's
  topics) -- its world is simulated beside it, and a tab reading a real broker is the real-Kafka lane's question (§15,
  item 7).

### 15.6 The filtered-mapping join, diagnosed and fixed (2026-11-15)

**Fixed** (§15.6a below): the resolver's key widening (`StackBuilder.demandForKeys`) now reaches through a join to
its left side. The 26 latest-version filters are on (`<Table>_Latest(<TABLE>.LAKE_OUT_ID = 999999999)`, 16 filters in
`store::DB`, `~filter` on each of the 26 class mappings over a milestoned table), and `//core:stress_suites` is green
with them.

The diagnosis as it was found:

Reproduced on the real corpus with ONE filter (`Filter Book_Latest(BOOK.LAKE_OUT_ID = 999999999)` and `~filter
[store::DB] Book_Latest` on `positions::Book`): 1,046 stress suites fail, every one with `join condition references
unknown column 'BOOK_ID' on its left side`. The smallest failing query:

    |entitlements::AccessEntitlement.all()
        ->project(~[r:x|$x.recordId, d:x|$x.book.desk.name, b:x|$x.book.rollup.bookId])

- Each hop alone plans: `book.rollup.bookId` alone narrows the routed Book sub-select to `__route0_0, BOOK_ID`;
  `book.desk.name` alone carries `DESK_ID`. Book's own `desk` and `rollup` together plan.
- Together, from a STACK root (`*AccessEntitlement[entitlementsBase]`, its subclasses `extends` it) routed to Book's set
  (`book[positions_Book]`), the Book sub-select is the class row -- every property, `__route0_0` and `DESK_ID` -- and
  not `BOOK_ID`: the column the second hop's join (`Book_Rollup(BOOK.BOOK_ID = NOTIONAL_BY_BOOK.BOOK_ID)`) reads is a
  property's column (`bookId`), so it is carried renamed, and the lowering (`Lowerer.sideCondition`, a join tree on the
  left) looks it up by its own name.
- Without the filter Book's rows are the plain table and the name resolves. The scratch model of the same shape
  (`core/src/test/java/com/legend/lineage/ScratchFilterJoinTest.java`, not committed) does NOT fail: something of the
  corpus's union stack (its four `extends` sets with their own filters, the routed key) is needed.
- Where to fix: the routed target's demand in the resolver (`resolver/StackBuilder` route keys and extras,
  `Pipelines.materialize`), which should carry a hop's join column under its own name when it also feeds a property;
  not a name look-through in the lowering (a guessed fallback, AGENTS invariant 4). The 26 latest-version filters wait on
  it; on a live target every row is its key's open version, so nothing reads wrong meanwhile.

### 15.6a The fix (2026-11-15)

- **The cause.** A routed target's rows (Book's set under the entitlements' stack: a one-arm union, its row the class's
  properties and the route key) carry a join's key only when a hop asks for it: `StackBuilder.demandForKeys` widens the
  union's projection with each key column a condition reads that the row lacks. With two sibling hops, the second
  hop's left side is the first hop's join (Book's rows joined to DESK), and the widening looked through a filter, a
  projection and a concatenate, not a join: it returned the join unchanged, and `BOOK_ID` (carried only as the
  property `bookId`) never reached the sub-select. Without the filter Book's rows were the plain table, every column
  there.
- **The fix.** A join's row is its left's, then its right's under the join's prefix: a key the join's row lacks is
  widened on its left, the join's row rebuilt (`demandForKeys`, its `TypedJoin` arm). The lowering is unchanged: it
  reads the column by its own name, now present (no look-through, AGENTS invariant 4).
- **The SQL it writes on Deephaven.** With the filters a class's rows are a sub-select, and a statement that reads one
  twice writes it once as a common table expression (the lowering's `SharedRelations`). Deephaven's batch now makes
  each expression's operation once and every read of it a view of that operation (`DeephavenBatch.with`, `cte`); a
  recursive WITH is still refused by name.

### 15.7 After step 6: the phases (ruled 2026-11-16)

The user's order: Deephaven done end to end first; the lake and history last, as a phase of their own ("put the
history/lake last as a separate phase ... focus this phase on getting Deephaven done completely end to end").

1. **v0, one PR:** steps 0 to 6 above, the merge of `main`, and the Deephaven run side
   (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md); the desk demo without leaks, and finding 10.
2. **The Java Deephaven runtime,** its own PR: the same plans sent and read from Java, so PCT runs on Deephaven.
3. **Materialized views** (`#I{...}#` read by lite's parser first): on Deephaven a live table the loader makes; in the
   tab's DuckDB once measured (risk 1: maintained incrementally, or recomputed after each batch). How the loader
   applies a view's operations is that phase's decision.
4. **Data products served:** an access point run wherever its product is deployed, or only on its target (§7).
5. **Production readiness:** Avro or Protobuf with Legend's schemas in a registry, real authentication, a restart
   reading only as far back as each table keeps (risk 4), a long-running test of scale and failures, and the lane
   against a real broker (item 7: a machine or an environment that has one).
6. **The lake and history** (below).

**How the history tier plugs in, and what it asks of v0.** The lake is a third target of the same deployment: a writer
lands each topic as Parquet or Iceberg tables, keyed datasets keeping every version (upstream's milestoning columns,
which lite refuses today: S404). The Deephaven loader reads the lake's files as tables of the server, so a query that
reads history and live reads two tables of one server: queries stay on one target, and the query side's design is
unchanged. Its own questions are the lake's: the milestoning columns, "as of" queries over history, and where history
ends and live begins (by key or by offset). What it asks of v0, checked in v0's review: a server table is not always a
live one (a table the loader made from files); the version columns stay ordinary columns of the landed tables
(`LAKE_IN_ID`, `LAKE_OUT_ID`, as today); and keeping `current` is one way of keeping among others, never assumed.

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
says what it feeds); the `Deployment` element; the keyed tables' latest-version filters (option A, ruled 2026-11-15, §13; on since §15.6a); the data product `desk::Desk`, deployed whole and providing the data space's mapping (step 2).
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
    model ('The desk classes') Model { mapping: desk::DeskMapping; }
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
15. **A delete:** step 0 removed a deleted key's row; the user: "Delete should never delete/remove rows for batch
    milestone - it should soft delete by updating the lake out". Upstream's persistence component read: a close at
    batch - 1, the digest deciding a new version. Then, for speed: a live engine replaces and removes, history closes
    and keeps, both holding the same current rows (§5's table).
