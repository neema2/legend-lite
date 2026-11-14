# Deployment: what lands where (2026-11-14)

**Status: PROPOSED, not ratified.** Written with the user, branch `claude/deephaven-3-browser`. Ruled so far
(2026-11-14): `include Ingest` stays, for reading ("Keep include for read that's fine as settled"); the deployment is
its own element and `keep` moves from the ingest to it ("Agree deploy own element and keep moves"); Lakehouse
constructs are preferred, with `###Persistence` and the function activators read for inspiration. The rest is §6.

**History.** On 2026-10-29 the user turned down a generic `Deploy` element, among other new concepts ("Lot of new
concepts to juggle"; docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §1). What changed since: the two tiers -- one
ingest landing on several engines, each keeping a different amount (§1). This design reuses upstream's patterns
(§3) and adds a word only where none fits.

## 1. Why an element is needed

- **Declare once, deploy anywhere.** The same ingests, materialized views and data products land on several engines
  by configuration: the desk's Deephaven (live), a lakehouse (history), the tab's DuckDB (private). The demo and
  production are one path, differing only in configuration.
- **What a target keeps is the target's.** 25,000 prints live, every print in history: retention cannot sit on the
  ingest.
- **Upstream needs no such element** because its lakehouse is one platform: an ingest's owner (`AppDir`) picks the
  environment, and an access point's `targetEnvironment` picks which lakehouse engine to compile for. It is not
  generic across backends (the user, 2026-11-14).

## 2. Upstream, read

Pinned: legend-studio `9775e7283cf2e768293db9982573cecbe8280c4b`, legend-engine
`05a3d33c4316c05ab6828f7c416de65cd9ffe0c8`.

### 2.1 Lakehouse (preferred)

- **Ingest** (`legend-graph/.../ingest/V1_IngestDefinitionSpecification.ts`, `V1_IngestDefinition.ts`):
  - read modes `Delta` (with an optional `deleteIndicator { deleteField, deleteValues }`), `Snapshot`,
    `TemporalSnapshot`, `Undefined`; message formats `CSV`, `Avro`, `JSON`, `Parquet`;
  - dataset sources `serializedSource` (a schema and a format: producers deliver to the ingest) and `FunctionSource`
    (a function);
  - write modes `append_only`, `batch_milestoned`, `batch_milestoned_business_temporal`, settable per dataset;
  - an owner: `AppDir` (production, prod-parallel) or Kerberos.
- **Reading it as tables:** a relational `Database`'s `includedStoreSpecifications` with `storeType: 'Ingest'`
  (`__tests__/roundtripTestData/TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`): lite's `include Ingest`.
- **Data product** (`.../dataProduct/V1_DataProduct.ts`): a `LakehouseAccessPoint`'s `targetEnvironment` is
  optional (`string | undefined`); a `FunctionAccessPoint` has no target.
- **The deploy artifact** (`.../lakehouse/deploy/V1_DataProductArtifact.ts`): each access point's implementation
  holds `resourceBuilder`s -- a `DatabaseDDL { targetEnvironment, script, resourceType: VIEW | TABLE | PROCEDURE,
  reproducible }` or a function access point -- and its `dependencyDatasets` (each an ingest, with its producer, and a
  dataset). The platform generates it; a closed service applies it.

### 2.2 `###Persistence` (older, generic)

- **`Persistence`** says what: a trigger, a service (the source query), targets, how rows land. **`PersistenceContext`**
  says where, for one environment: `persistence`, `platform` (`Default`, `AwsGlue #{ dataProcessingUnits }#`),
  `serviceParameters`, `sinkConnection`. The context points at its persistence, so one persistence has many contexts.
- **Deletes:** `mergeStrategy: DeleteIndicator { deleteField: deleted; deleteValues: ['Y', '1', 'true']; }`, the same
  words as the lakehouse's.
- **Actions:** `persistence/v1/platform` `validate`, `install`, `uninstall`, dispatched by the context's platform (no
  open-source implementation found).
- **The generator** (`legend-engine-xt-persistence-component`) turns an ingest mode into SQL plans per database:
  `preActionsSqlPlan`, `schemaEvolutionSqlPlan`, `ingestSqlPlan`, `metadataIngestSqlPlan`, `postActionsSqlPlan`, dry
  runs and locks.
- `Streaming { sink }` carries a sink alone; there is no Kafka anywhere in the DSL.

### 2.3 Function activators (generic entry, one backend each)

- **`FunctionActivator`** (`core_function_activator/metamodel.pure`): `function`, `ownership` (`Deployment {
  identifier: '...' }`, a deployment id), `activationConfiguration` (a `DeploymentConfiguration` whose target is a
  connection), `actions`; an unused `DeploymentStage { SANDBOX, PRODUCTION }`.
- **One element per backend:** `SnowflakeApp`, `SnowflakeM2MUdf`, `BigQueryFunction`, `MemSqlFunction`,
  `HostedService`, `FunctionJar`, `DeephavenApp`; one service SPI dispatching by `supports(...)`.
- **Lifecycle** (`FunctionActivatorAPI`): `validate`, `publishToSandbox`, `renderArtifact`, `generateLineage`;
  production through artifact generation of a released version. The environment (sandbox or production) is the
  server's configuration, not the model's.
- **Artifacts:** Snowflake `CREATE OR REPLACE SECURE FUNCTION ... AS $$ <sql> $$`, MemSQL `CREATE OR REPLACE FUNCTION
  ... RETURNS TABLE AS RETURN <sql>`, a BigQuery table-valued function.

## 3. What each lends

| From | Taken | Not taken |
|---|---|---|
| Lakehouse | what is deployed (ingests, data products); upstream's ingest words; the artifact: a resource per target, with the datasets it depends on; an access point's optional target | the one fixed platform; the owner choosing the environment |
| Persistence | the context pattern: a separate element per environment, pointing at what it deploys; `validate` / `install` / `uninstall`; the plan's stages (pre-actions, schema evolution, ingest, post-actions) | a service as the source; a platform element per runtime |
| Activators | the target is a connection; `ownership: Deployment { identifier }`; rendering the artifact without applying it; one entry, a generator per backend | one element kind per backend; the environment in server configuration |

The improvement over upstream: **one element and every backend.** Lite's one compiler with a dialect per target is
already the generic entry with a generator per backend; the target's dialect decides how it makes, lands and keeps.

## 4. The element (proposed)

```
###Lakehouse
Deployment market::Desk
{
  deploys: [ marketdata::MarketDataFeed, marketdata::NewsFeed, trading::OrderFlowFeed, products::SecurityMaster,
             positions::StartOfDay, risk::DeskViews, market::WorldStatus ];       // later: [ desk::Desk ], its data product
  sources: [ market::Kafka: [ tickerplant::Topics, newswire::Topics, oms::Topics, secmaster::Topics, market::WorldStatus ],
             market::PositionsDrop: [ positions::StartOfDay ] ];
  live: market::Deephaven keep [ appended: LastRows(10000), milestoned: current, MARKET_TRADE: LastRows(25000) ];
  history: market::Lake keep [ appended: all, milestoned: versions ];
}
```

- **Direction (Persistence's):** a deployment points at what it deploys; nothing in an ingest points at a deployment.
  Many deployments may deploy one ingest.
- **`deploys`:** ingests and data products, the lakehouse's units. A data product deploys the datasets its access
  points and model access depend on, as the lakehouse artifact's `dependencyDatasets`.
- **`sources`:** each source connection and what it feeds, in upstream's runtime `connectionStores` shape. A topic
  store is read; a pushed ingest (its own columns: the positions files, the world's status topic) receives.
- **`live`, `history`:** a target is a connection (the activators'). The live target reads history before today and
  its own rows since. One element serves every backend: the target's dialect decides how.
- **`keep`:** per target, a default per write mode stated once, overridden per dataset. Appended: `LastRows(n)`,
  `Last('PT8H')`, `all`. Milestoned: `current` (one row a key) or `versions`. An appended dataset on a live target
  with no `keep` is refused by name (AGENTS.md invariant 4: no default the compiler assumes).
- **Access points:** one with no target is served on every target; one with a target, only on a target of that engine.
- **Lifecycle:** `validate`; `render` (the artifact: each target's plan, as data, nothing applied); `install` (apply,
  reconciling: make what is missing, evolve what changed, leave what matches); `uninstall`.
- **Each target's plan in Persistence's stages:** make (pre-actions), evolve (schema evolution), land (ingest), keep
  (post-actions). On Deephaven the plan is data for a fixed, reconciling loader; in the tab, exec runs it (C0's plans).
- **The runtime is untouched** (upstream's): it says where queries go. On connect, the engine a runtime names must hold
  every table and view its mapping reads, with the declared types, or the desk is refused, naming what is missing.

## 5. The whole thing, end to end

**Part 1, what the data is (names no engine):**
1. Topic stores, by their producers: a `###Relational` table per topic, its key the table's (a source that exists on
   its own; Legend is the schema registry).
2. Ingests, by the data's owner, in upstream's words: `Delta<JSON>` with `deleteIndicator=(...)`, `Snapshot<Parquet>`;
   `append_only`, `batch_milestoned`. A dataset that reads a topic or database is a function (`|#>{db.table}#`, its
   columns and key the table's); one that is delivered to declares its own columns.
3. Materialized views: datasets made by a function over landed datasets (`#I{...}#`).
4. The read database (`include Ingest`, joins), classes, the mapping, the screens.
5. Data products, in upstream's form; an access point's target optional.

**Part 2, where it runs (configuration; the only part demo and production differ in):**
6. Connections: one per Kafka cluster, the files' drop location, Deephaven, the lakehouse, the tab's DuckDB and log.
7. Deployments (§4): one per environment.
8. Runtimes and the data space, unchanged.

**Part 3, what happens (nothing written):** `install` a deployment (render, apply, check); producers publish (the
world through the door in the demo, each team in production); the desk opens on an environment and its screens tick;
a model change installed again touches only what changed.

## 6. Open, for the user

1. **Name and place:** `Deployment` in `###Lakehouse` (recommended: it deploys Lakehouse elements), a `###Deployment`
   section, or a `Context`-style name after `PersistenceContext`.
2. **`deploys`:** ingests now and the desk's data product once built, or the data products built first.
3. **Ownership** (`Deployment { identifier }`): now, or when production needs it.
4. **Upstream's ingest words:** `Delta`/`Snapshot` with `deleteIndicator`, `append_only`/`batch_milestoned`, the format
   in the read mode; `Stream`, `LatestByKey`, `retention` and `deleted=` retired (docs/SEMANTICS_REGISTER.md S402
   shrinks to: a function source may read stores; a pull's schedule; a pushed ingest's deliveries on a topic; the
   deployment).
5. **A sandbox stage** (the activators'): not now. The demo is a deployment like any other.

## 7. The order

0. Upstream's ingest words (A1's grammar, JSON, C0's plans, `IngestTopics`, the desk's sources, their tests).
1. The deployment element, `validate` and `render`: each target's plan (Deephaven's as data for a reconciling loader;
   the tab's, C0's plans). The generated Groovy (`DeephavenProgram`) deleted.
2. `install`, with the check after it and on connect.
3. Reference data as published topics; positions as files.
4. The tab on the same path: the world publishes, the tab lands with the same plans.
5. `market.groovy` deleted.

Then, each its own decision: materialized views on both live targets; the history tier (Iceberg or Parquet, read by
Deephaven and DuckDB; the cut between history and live); data products served where deployed; production
configuration (Avro with Legend's schemas published to a registry, a lane against a real broker, reading less on
restart).
