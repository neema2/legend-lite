# legend-depot behavioural contract (read routes used by Studio, Query, DataCube and the engine)

> Read 2026-10-04. Companion to `UPSTREAM_STUDIO_CENSUS.md` Part D (which lists the whole API) and in the
> style of `SDLC_CONTRACT_SLICE1.md`: this file pins down, for the routes our clients actually call, the
> exact HTTP status, the verbatim error text, the JSON key order, query-parameter semantics and how the
> dependency closure is computed. Everything is from code reading, plus three replica experiments that ran
> in `jshell` with nothing written to disk:
> * **JSON key order**: replica classes on jackson-databind 2.10.5.1 with the same `new ObjectMapper()` depot uses.
> * **Aether closure**: maven-resolver 1.9.24 (impl/util) with the depot session configuration.
> * **PMCD `distinct()`**: legend-engine-protocol-pure 4.138.2 (`PureModelContextData.Builder`).
>
> Each claim marked **[verified]** was checked by one of these experiments. Claims marked **[code]** come
> from reading the code only.

Sources (read-only):

| Alias | Repo / commit | Path |
|---|---|---|
| `dep:` | legend-depot @ `9c0a809` | `/Users/neema/legend/legend-lite-query/.scratch/legend-depot` |
| `st:` | legend-studio (packages) | `/Users/neema/legend/legend-lite-query/.scratch/legend-studio/packages` |
| `eng:` | legend-engine @ `230c159196d` (only the alloy loader and the protocol classes) | `/Users/neema/legend/legend-engine` |

Depot builds against legend-engine **4.140.6**, legend-sdlc **0.232.5**, Jackson **2.10.5 / databind 2.10.5.1**, Dropwizard **1.3.29** (Jersey 2.25.1), eclipse-collections **10.2.0**, Maven **3.9.12** (maven-resolver 1.9.x) (`dep:pom.xml:106-149`).

Abbreviations (all relative to `dep:` unless prefixed):

| Abbrev | File |
|---|---|
| `PSI` | `legend-depot-core-data-services/src/main/java/org/finos/legend/depot/services/projects/ProjectsServiceImpl.java` |
| `IMR` | `legend-depot-core-data-services/.../services/projects/InMemoryArtifactDescriptorReader.java` |
| `MDR` | `legend-depot-core-data-services/.../services/dependencies/MavenDependencyResolverImpl.java` |
| `DEU` | `legend-depot-core-data-services/.../services/dependencies/DependencyExclusionsUtil.java` |
| `DU` | `legend-depot-core-data-services/.../services/dependencies/DependencyUtil.java` |
| `PR` | `legend-depot-core-data-services/.../server/resources/projects/ProjectsResource.java` |
| `PVR` | `legend-depot-core-data-services/.../server/resources/versions/ProjectsVersionsResource.java` |
| `DR` | `legend-depot-core-data-services/.../server/resources/dependencies/DependenciesResource.java` |
| `ER` | `legend-depot-entities-services/.../server/resources/entities/EntitiesResource.java` |
| `EDR` | `legend-depot-entities-services/.../server/resources/entities/EntitiesDependenciesResource.java` |
| `ESI` | `legend-depot-entities-services/.../services/entities/EntitiesServiceImpl.java` |
| `PMR` | `legend-depot-pure-model-context/.../server/resources/pure/model/context/PureModelContextResource.java` |
| `PMS` | `legend-depot-pure-model-context/.../services/pure/model/context/PureModelContextServiceImpl.java` |
| `RDS` | `legend-depot-artifacts-services/.../services/artifacts/refresh/RefreshDependenciesServiceImpl.java` |
| `TR` | `legend-depot-core-tracing/.../tracing/resources/TracingResource.java` |
| `EB` | `legend-depot-core-data-api/.../services/api/EtagBuilder.java` |
| `ERR/` | `legend-depot-servers-common/src/main/java/org/finos/legend/depot/core/server/error/` |
| `PVM` / `PM` | `legend-depot-core-data-store-mongo/.../store/mongo/projects/ProjectsVersionsMongo.java` / `ProjectsMongo.java` |
| `BM` | `legend-depot-store-mongo/.../store/mongo/core/BaseMongo.java` |
| `AEM` / `EM` | `legend-depot-entities-store-mongo/.../store/mongo/entities/AbstractEntitiesMongo.java` / `EntitiesMongo.java` |
| `DSC` | `st:legend-server-depot/src/DepotServerClient.ts` |
| `EGS` | `st:legend-application-studio/src/stores/editor/EditorGraphState.ts` |

Base path: depot-server serves Jersey under `urlPattern` `/depot/api/*` (`legend-depot-server/src/main/resources/docker/config/config.json`). Auth: pac4j `AnonymousClient`, so there is no login. CORS: `*`. All paths below are relative to `/depot/api`.

---

## 0. Cross-cutting

### 0.1 Which ObjectMapper writes the responses

* depot-server registers its own `LegendDepotServerJacksonJsonProvider` (a `JacksonJsonProvider` + `ContextResolver<ObjectMapper>`) with `jersey.register(...)` (`legend-depot-server/.../LegendDepotServer.java:101-105`, `BaseServer.java:124`). Its `getContext` returns:
  * **`new ObjectMapper()`** (plain, nothing configured) for every type, **except**
  * `ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports()` when the type is exactly `PureModelContextData` (`LegendDepotServerJacksonJsonProvider.java:25-44`).
* Dropwizard's own `JacksonMessageBodyProvider` (the bootstrap mapper) is bound only through the HK2 `JacksonBinder`, so it is **not** a "custom" provider (dropwizard-core 1.3.29 `AbstractServerFactory.java:541`; dropwizard-jersey `JacksonBinder.java`, "allowing users to override"). Jersey 2.25.1 picks, in order: smaller Java type distance, then media-type distance, then a custom provider over a non-custom one (`jersey-common MessageBodyFactory.java:341-357`). Both providers are `Object`/`*/*`, so **depot's plain mapper writes every non-PMCD response, including error bodies.** **[code]**
* What the plain mapper (Jackson 2.10 defaults) means for responses:
  * Nulls are **written** (inclusion ALWAYS), except in classes annotated otherwise (`ErrorMessage` is `@JsonInclude(NON_NULL)`).
  * No `SORT_PROPERTIES_ALPHABETICALLY` and no `@JsonPropertyOrder` on any served class (grep). **Key order** is: the `@JsonCreator` properties first, in creator order; then fields in declaration order, superclass first; then getter-only properties. For every class below this gives a deterministic order, checked by replica. **[verified]**
  * `java.util.Date` is written as epoch millis (no route below returns one). `java.time.Instant` has no JavaTimeModule, so it is written **as a bean**: `{"epochSecond":1791115200,"nano":123000000}`. **[verified]**
  * `FAIL_ON_UNKNOWN_PROPERTIES=true` by default, but every request DTO used here is `@JsonIgnoreProperties(ignoreUnknown = true)`, so unknown request fields are ignored.
* The PMCD mapper (engine `ObjectMapperFactory.withStandardConfigurations`, `eng:legend-engine-core/legend-engine-core-shared/legend-engine-shared-core/.../ObjectMapperFactory.java`) uses `SORT_PROPERTIES_ALPHABETICALLY`, `ORDER_MAP_ENTRIES_BY_KEYS` and `NON_NULL`. See §6.

### 0.2 Status codes and error bodies

Mappers in play (`BaseServer.java:121-123` plus Dropwizard defaults, `registerDefaultExceptionMappers` default true, `ExceptionMapperBinder.java`). Jersey picks the mapper with the closest exception type. On a tie the first one in the list wins, and `register()`ed mappers come before the HK2-bound defaults (`jersey-common ExceptionMapperFactory.java:123-160`; the same analysis as SDLC_CONTRACT §0.2).

| What is thrown | Mapper that wins | Status | Body (key order as written) |
|---|---|---|---|
| `IllegalArgumentException` (all "not found" / "excluded" / "invalid client version" errors below), NPE, any other non-ISE `RuntimeException` | depot `CatchAllExceptionMapper` → `buildDefaultResponse` (`ERR/CatchAllExceptionMapper.java:35-39`, `ERR/BaseExceptionMapper.java:48-70`) | **500** | `{"code":500,"message":"<exception message>","timestamp":{"epochSecond":…,"nano":…}}`. `details` and `stackTrace` are omitted (null, `NON_NULL` inherited from Dropwizard `ErrorMessage`). `stackTrace` is present only if config `exceptionMapper.includeStackTrace=true` (default false, `ERR/configuration/ExceptionMapperConfiguration.java`). If the message is null (e.g. NPE on JDK < 15), the `message` key is absent. **[verified shape]** |
| `IllegalStateException` (evicted version "being restored", "Error calculating transitive dependencies…", "Error collecting dependencies: …") | **Dropwizard `IllegalStateExceptionMapper`** (distance 0 beats `Throwable`) → `LoggingExceptionMapper` | **500** | `{"code":500,"message":"There was an error processing your request. It has been logged (ID 0123456789abcdef)."}`. The ID is a random long as `%016x`. **The real message is hidden** and there is no `timestamp`. **[code]** |
| Jersey `WebApplicationException` (unknown route 404, wrong method 405 + `Allow`, 415, …) | depot `CatchAllExceptionMapper.toResponse(WebApplicationException)` | original status | `{"code":404,"message":"HTTP 404 Not Found","timestamp":{…}}` (3xx are passed through untouched) |
| Malformed request JSON / wrong JSON type | depot-registered `JsonProcessingExceptionMapper(true)` (`BaseServer.java:122`) | **400** | `{"code":400,"message":"Unable to process JSON","details":"<Jackson original message>"}`, e.g. `details` = ``Cannot deserialize instance of `java.util.ArrayList<…ArtifactDependency>` out of START_OBJECT token`` when the body is an object instead of an array. **[verified]** |
| `Optional.empty()` returned by a resource (always as `Response.ok(Optional)`) | Dropwizard `OptionalMessageBodyWriter` throws `EmptyOptionalException` → `EmptyOptionalExceptionMapper` | **404** | **empty body**, no JSON (`dropwizard-jersey optional/OptionalMessageBodyWriter.java`, `EmptyOptionalExceptionMapper.java`) |
| `LegendDepotServerException` | `DepotServerExceptionMapper` | its status | not thrown by any route in this document |

Notes:
* An empty request body is deserialized as `null` (jackson-jaxrs `ALLOW_EMPTY_INPUT` default on, `jackson-jaxrs-base ProviderBase.java:773-776`). The POST routes below then NPE → 500.
* A primitive `boolean` query param with a non-boolean value parses as `false` (JAX-RS `Boolean.valueOf`). It never returns 404. Unknown query params (Studio's `versioned`) are ignored.
* The GET `…/dependencies` routes declare `@Consumes(application/json)` (`EDR:64,81`). A GET that carries a non-JSON `Content-Type` header gets 415. A GET with no `Content-Type` matches.

### 0.3 Version identifiers, alias resolution and the two lookup paths

Two lookups are used, and they behave differently:

**`find(g, a, v)`** (`PSI:178-203`), used by `/versions/{g}/{a}/{v}`, the Aether descriptor reader, and inside `resolveAliases…`:
* `v.equals("latest")` (case-sensitive): look up `project-configurations(g,a).latestVersion`. Empty if the project is missing or `latestVersion` is null.
* `v.equals("head")` (case-sensitive): `<project.defaultBranch ?? config.projects.defaultBranch>-SNAPSHOT`. The config default is `"master"` (`BaseServerModule.java:74-77`). Empty if the project config is missing.
* Anything else is a literal lookup on `versions(g,a,v)`. `v == null || v.isEmpty()` → **`IllegalArgumentException("cannot find project version, versionId cannot be null")`** (`PVM:85-93`).
* `HEAD`, `LATEST` and `Latest` are **not** aliases here. They are looked up literally and are normally not found.

**`resolveAliasesAndCheckVersionExists(g, a, v)`** (`PSI:211-236`), used by every entity / dependency / PMCD route:
1. `find` is empty → `IllegalArgumentException` **`project version not found for <g>-<a>-<v>`** (`PSI:96,222`; `<v>` is the string as requested, e.g. `…-latest`) → **500**.
2. `versionData.excluded` → `IllegalArgumentException` **`project version not found for <g>-<a>-<resolvedV>, exclusion reason: <reason>`** (`PSI:95,227`) → **500**.
3. `evicted` → enqueue a HIGH-priority restore (`PSI:205-209`, which itself NPEs if the project config is missing), then `IllegalStateException("Project version: <g>-<a>-<v> is being restored, please retry in 5 minutes")` → **500 with the masked Dropwizard message** (§0.2).
4. Otherwise it records a query metric and returns the resolved exact version.

`getProject(g,a,v)` (`PSI:735-748`, used for direct deps) has the same messages for missing/excluded and no eviction check.

### 0.4 Caching headers (`TR:102-125`, `EB:41-76`)

* Routes built with `handle(…, request, etagSupplier)`: if the ETag string is non-null and the request's `If-None-Match` matches → **304, empty body, no ETag header**. This is evaluated **before** the work runs, so for routes whose ETag uses the *unresolved* version, a 304 can be returned for a version that does not exist.
* ETag value = plain concatenation `groupId + artifactId + versionId (+ clientVersion)` with no separators, sent as a strong tag, e.g. `ETag: "org.finosmyproj1.0.0"`. It is null (no ETag) when the version ends with `-SNAPSHOT`, is an alias (`latest`/`head`, case-insensitive here, `VersionValidator.isVersionAlias`), or the clientVersion is null or `vX_X_X`.
* With an ETag the response gets `Cache-Control: no-transform, must-revalidate`. Without one it gets `Cache-Control: no-cache, no-store, no-transform` (`new CacheControl()` defaults `noTransform=true`; Jersey `CacheControlProvider.toString` order). Routes built with `handleResponse` never get an ETag, so they always get the second header.

### 0.5 Ordering sources (none of the routes below sort)

* **Mongo order**: every list read is `collection.find(filter)` with no sort (`BM:176-181,246-259`). With an equality filter on `(groupId, artifactId[, versionId])` the planner normally uses one of the compound indexes. Results then come back in **index order** (e.g. `versions` by `versionId` as an ascending *string*: `"1.10.0" < "1.2.0" < "master-SNAPSHOT"`). If the index is absent, they come back in natural (≈ insertion) order. Treat it as **unspecified**. `project-configurations` with no filter is a collection scan (≈ insertion order).
* **Dependency results** come from a `java.util.HashSet<ProjectVersion>` (`hashCode` = commons-lang `reflectionHashCode` over the three strings). The entities are then fetched by `ParallelIterate.forEach`. Below 10 000 elements this runs serially (`eclipse-collections 10.2.0 ParallelArrayIterate.forEachOn:62-66`), so the list order is **HashSet iteration order**: deterministic for a given set, but arbitrary. Clients (Studio, Query) index the result by `groupId:artifactId` and do not depend on the order.

---

## 1. Dependency entities

### 1.1 Shared response shape: `ProjectVersionEntities[]`

Each element (key order **[verified]**, `legend-depot-entities-api/.../domain/entity/ProjectVersionEntities.java:27-46`):

```json
{"groupId":"org.finos","artifactId":"dep","versionId":"1.0.0","versionedEntity":false,
 "entities":[{"path":"model::Person","classifierPath":"meta::pure::metamodel::type::Class","content":{…}}]}
```

* `versionId` is the **resolved** exact version (`ESI:133,143`), even if the request used `latest`.
* `versionedEntity` is always `false` (deprecated field).
* `entities` contains every entity of that GAV in Mongo order (`AEM:145-148`). Each entity is an `EntityDefinition`, key order `path`, `classifierPath`, `content` (creator order, `legend-depot-entities-api/.../EntityDefinition.java:39-47`). `content` is the stored JSON map, re-parsed, with its key order preserved (`EM:240-256`).
* Each listed GAV is resolved with `resolveAliasesAndCheckVersionExists` (`ESI:133`). **One missing, excluded or evicted member fails the whole request** with that member's §0.3 error.

### 1.2 `POST /projects/dependenciesFromArtifactDependencies?transitive=&includeOrigin=`

Route: `EDR:129-140` → `ESI:158-170` → `PSI:311-341` → `MDR:74-119,186-232`. Built with `handleResponse`, so no ETag. Query params: `transitive` (default **false**), `includeOrigin` (default **false**). Studio also sends `versioned=false`, which is ignored.

**Request body**: a JSON array of `ArtifactDependency` (`legend-depot-artifacts-repository-api/.../ArtifactDependency.java:27-50`, `@JsonIgnoreProperties(ignoreUnknown=true)`):

| Field | Binding |
|---|---|
| `groupId`, `artifactId` | creator params |
| `version` | the creator param name |
| `versionId` | **also accepted.** Jackson infers the final field `versionId` as a mutator because there is a visible getter `getVersionId`. If both are sent, `versionId` wins. Studio sends `versionId`. **[verified]** |
| `exclusions` | `[{"groupId","artifactId"}]`, optional. `null` or absent → `[]`. Extra keys in an exclusion are ignored. |

Missing version → **500 `cannot find project version, versionId cannot be null`**. Body `null`/empty → NPE → 500. Body not an array → 400 (§0.2).

**Algorithm** (exact order):

1. **Exclusion pre-processing**, only for entries with a non-empty `exclusions` (`DEU:61-99`, `ESI:161-162`):
   * The key is `createDependencyKey(g,a,<requested version>)` = `"g:a:v".replace(":","__").replace(".","~")` (`ProjectVersionData.java:163-166`).
   * Each excluded `g:a` gets a version: the first entry with that `g:a` in the **stored** transitive closure of the dependency (the §1.3 algorithm with `transitive=true`). That set is a HashSet, so "first" is arbitrary if there are several. If the dependency itself is missing, this fails with its §0.3 error.
   * Then the exclusion's own Aether closure (step 3 below run on the excluded project alone, including the project itself) is added to the exclusion list. **If the excluded `g:a` is not in the dependency's stored closure, its version stays `null`, and this step fails with 500 `cannot find project version, versionId cannot be null`.** This includes wildcard `*` exclusions and stale exclusions. **[code]**
2. **Resolve each requested root** with `resolveAliasesAndCheckVersionExists` (500 on missing/excluded, masked 500 on evicted) (`PSI:315-321`).
3. **Aether collect** (`MDR:74-119,194-213,331-353`):
   * The roots are sorted by `(groupId, artifactId)` (stable sort) and become the `CollectRequest` dependencies (scope `compile`, type `jar`). There is no root artifact, so **the roots are depth-1 children of a null root.**
   * Each root carries Aether exclusions `g:a:*:*` taken from the map, looked up by the **resolved** version. A root requested by alias therefore silently loses its exclusions (quirk Q5).
   * The descriptor reader (`IMR:61-120`) uses `find` (aliases allowed; **no excluded/evicted check**). It returns the GAV's stored direct dependencies sorted by `(groupId, artifactId)`, each carrying the stored POM exclusions for that child (`versionData.excludedDependencies[childKey]`), plus the root-level exclusions if this GAV is a requested root. A GAV that is **not in the store returns no children but stays in the tree.**
   * Session: Maven default selectors (scope test/provided, optional, exclusions) and graph transformer `ConflictResolver(NearestVersionSelector, JavaScopeSelector, SimpleOptionalitySelector, JavaScopeDeriver)`. Losers are removed (non-verbose).
   * Conflict rule, `NearestVersionSelector` (maven-resolver-util `isNearer`, from the bytecode):
     * The **shallower** occurrence wins.
     * **Siblings** (same parent) → the **higher** version wins (GenericVersion order). This applies to two roots with the same `g:a`, whatever their order.
     * Equal depth but not siblings → the **first occurrence** in depth-first pre-order wins. Because of the sorting above, that means the earliest by `(groupId, artifactId)` path.
     * A diamond keeps one node: the duplicate under the second parent is removed.
   * Replica results **[verified]** (maven-resolver 1.9.24):
     * `a→x:1`, `b→y→x:2` gives `x:1`.
     * `a→x:1`, `c→x:3` gives `x:1` (a sorts first).
     * Roots `x:1` and `x:3` give `x:3` only.
     * A GAV missing from the store is kept as a leaf.
     * An empty request gives an empty result.
   * Aether failure → `IllegalStateException("Error collecting dependencies: …")` → masked 500.
4. **Result set** = every node of the resolved tree, **including the depth-1 roots** (`MDR:215-232`). **`includeOrigin=false` does not remove the roots.** **[code + verified tree shape]**
5. If `transitive=false`: `retainAll(union of the stored direct deps of each resolved root)` (`PSI:328-338`). The roots disappear unless one is a direct dep of another root. Direct deps whose declared version lost a conflict disappear too.
6. If `includeOrigin=true`: add `ProjectVersion(g, a, <requested version string>)` for every request entry (`ESI:111-114`). For exact versions this is a no-op (already present). For an alias it adds a second entry that resolves to the same version, so the response contains **two identical `ProjectVersionEntities`** (quirk Q6).
7. Fetch the entities of each member (§1.1). A transitive member that is missing, excluded or evicted fails the request. This is not silent, unlike in step 3.

**Response**: `200 ProjectVersionEntities[]` in HashSet order (§0.5). Studio's call (`transitive=true&includeOrigin=true`) therefore returns the full nearest-wins closure **including the direct dependencies**, with at most one version per `g:a` except for the alias duplicate.

### 1.3 `GET /projects/{g}/{a}/versions/{v}/dependencies?transitive=&includeOrigin=`

Route: `EDR:61-76` → `EntitiesService.getDependenciesEntities(g,a,v,…)` default (`legend-depot-entities-api/.../EntitiesService.java:53-56`) → `ESI:152-156` → `PSI:247-309`. ETag `withGAV(g,a,<unresolved v>)`. Params `transitive` (default false), `includeOrigin` (default false); `versioned` ignored. This is the **stored-closure** algorithm. No Aether runs at request time.

1. The root `(g,a,v)` goes through `resolveAliasesAndCheckVersionExists` (§0.3 errors), then `getProject`.
2. `deps = versionData.dependencies` (direct, as declared in the entities-module POM, sorted by `(g,a)` at ingest, `RDS:65-77`), passed through `overrideWith(deps, [root])` (`DU:34-44`). This drops any dep with the root's `g:a` but a different version, together with its whole stored closure. For a single root it is a no-op apart from cycles.
3. If `transitive` **and the root has at least one direct dep**: if `transitiveDependenciesReport.valid` is false → `IllegalStateException("Error calculating transitive dependencies for project version - <g>-<a>-<v>")` → masked 500 (`PSI:302-305`). Otherwise add `transitiveDependenciesReport.transitiveDependencies`, again through `overrideWith`.
   * **How the stored list was built** (at ingest or `PUT /artifactsRefresh/dependencies/…`, `RDS:79-100,138-150`): Aether as in §1.2 step 3, using the version's direct deps as the roots and its stored POM exclusions, then `addAll(directDeps)`. It is nearest-wins **relative to this version's own deps**. It does not contain the version itself, and it was frozen at ingest time (census Part D §7 #10).
4. If `includeOrigin`: add `ProjectVersion(g, a, <v as requested>)`. The root is **not** included otherwise.
5. Entities as in §1.1.

Differences from §1.2: there is no request-time conflict resolution; `transitive=false` returns exactly the declared direct deps; the root is excluded unless `includeOrigin=true`.

**Errors**:
* Missing, excluded or alias-unresolvable root → 500 with the §0.3 message.
* Any closure member missing or excluded → 500 naming that member.
* Evicted root or member → masked 500.
* An unknown `{g}/{a}` gives the same "project version not found" 500. There is no 404.

---

## 2. `POST /projects/analyzeDependencyTreeFromArtifactDependencies`

Route: `DR:84-91` → `PSI:371-374` → `MDR:152-182,253-309`. `handleResponse`, so no ETag. No query params. The body is the same `ArtifactDependency[]` as §1.2 (`versionId` accepted).

* **No alias resolution and no existence check on the roots.** A missing root becomes a leaf node with no children. A root given as `latest` is expanded through `find` (alias honoured), but its node keeps `versionId:"latest"` and gav `g:a:latest`. Excluded or evicted versions are traversed normally.
* Same Aether session and sort as §1.2 step 3, with the request exclusions (only the explicit `g:a`; the "closure of the excluded project" expansion of §1.2 step 1 is **not** applied here).
* The report is built by walking the **resolved** tree (`MDR:261-309`):
  * every surviving node becomes a `nodes` entry keyed `"g:a:v"`;
  * depth-1 nodes are listed in `rootNodes`;
  * the parent→child edge is recorded in `forwardEdges` / `backEdges`. Because losers and duplicates are removed, **every node has at most one back edge.**
  * `projectId` comes from `project-configurations(g,a)` and is `null` if that is absent.
* **`conflicts` is always `[]`** in this code path: nothing calls `report.addConflict`. Conflicts are resolved silently by nearest-wins, so the losing versions never appear (quirk Q1).
* Errors: body `null` → NPE → 500; Aether failure → masked 500. Missing or excluded data does not produce an error.

Response (key order **[verified]**; `ProjectDependencyReport.java:28-95`, `ProjectDependencyVersionNode.java:26-83`):

```json
{"conflicts":[],
 "graph":{"nodes":{"g:a:1.0.0":{"groupId":"g","artifactId":"a","versionId":"1.0.0","projectId":"PROD-1",
                                "forwardEdges":["g:b:2.0.0"],"backEdges":[],"id":"g:a:1.0.0"},
                   "g:b:2.0.0":{"groupId":"g","artifactId":"b","versionId":"2.0.0","projectId":null,
                                "forwardEdges":[],"backEdges":["g:a:1.0.0"],"id":"g:b:2.0.0"}},
          "rootNodes":["g:a:1.0.0"]}}
```

* `id` is always the node's own gav. It is the un-ignored `HasIdentifier.getId()`. Studio keys nodes by `id`, so it must be present (`st:legend-server-depot/src/ProjectDependencyGraphReportHelper.ts:105-140`).
* `nodes` is an eclipse `UnifiedMap`, and `rootNodes` / edge sets are `UnifiedSet`s, so their order is hash order.
* If the legacy graph builder ever produced a conflict, its entry shape would be `{"groupId","artifactId","versions":["g:a:v",…]}`. That builder is not routed.
* `POST /projects/analyzeDependencyTree` (body `ProjectVersion[]`) converts to the same Maven path (`PSI:364-368`).

---

## 3. Project configurations

### `GET /project-configurations` (`PR:55-62` → `PSI:123-127` → `PM:83-87`)
* `200 StoreProjectData[]`, in collection-scan order. There is no paging and no filter.

### `GET /project-configurations/{groupId}/{artifactId}` (`PR:64-71` → `PSI:171-175` → `PM:95-99`)
* Found → `200 StoreProjectData`. Missing → **404 with an empty body** (Optional, §0.2). There is no validation of the coordinates.

`StoreProjectData` key order **[verified]** (fields of the `CoordinateData` superclass first, then declaration order; `StoreProjectData.java:28-38`, `getId()` is `@JsonIgnore`):

```json
{"groupId":"org.finos","artifactId":"proj","defaultBranch":null,"projectId":"PROD-1","latestVersion":"1.0.0"}
```

* Nulls are written. `defaultBranch` is null for auto-created projects (`StoreProjectData(projectId,g,a)`). `latestVersion` is null until the first release.
* Census Part D §2.1 lists `projectId` first. **That is wrong for the wire; `groupId` comes first.**
* Projects ingested only via `PUT /queue/rest/metadata` have no configuration record, so they 404 here (census Part D §7 #3).

---

## 4. Versions

### `GET /projects/{g}/{a}/versions?snapshots=` (`PR:73-82` → `PSI:129-133`)
* `snapshots` default **false**. The result keeps versions where `!versionData.excluded && (snapshots || !versionId.endsWith("-SNAPSHOT"))`.
* `200 string[]` of raw `versionId`s. It **includes evicted and deprecated** versions. There are no aliases and no `HEAD`. It is not sorted (Mongo order, §0.5).
* Unknown project → `200 []`. There is no error path besides Mongo failures.
* All Studio, Query and DataCube callers pass `snapshots=true` and sort client-side with `compareSemVerVersions` (e.g. `st:legend-application-studio/src/components/editor/editor-group/project-configuration-editor/ProjectDependencyEditor.tsx:1347,1375`). Studio's `EditorSDLCState.fetchPublishedProjectVersions` (`EditorSDLCState.ts:588-598`) keeps the raw list.

### `GET /versions/{g}/{a}/{v}` (`PVR:71-89`)
* Uses `find` (§0.3): `latest` and `head` are aliases (lowercase only). **There is no excluded or evicted check**: excluded versions are returned with `versionData.excluded:true`.
* Found → `200 ProjectVersionDTO`, with `versionId` = the resolved version. Not found (including `latest` with no `latestVersion`, a missing project, `HEAD` uppercase) → **404 with an empty body**.
* Shape **[verified]** (`PVR:91-129` non-static inner class; `ProjectVersionData.java:27-43,141-144`):

```json
{"groupId":"org.finos","artifactId":"proj","versionId":"2.0.0",
 "versionData":{"dependencies":[{"groupId":"org.finos","artifactId":"dep","versionId":"1.0.0"}],
                "properties":[{"propertyName":"platform.x","value":"1.2"}],
                "manifestProperties":null,"deprecated":false,"excluded":false,"exclusionReason":null,
                "excludedDependencies":{},"dependencyExclusions":{}}}
```

* `excludedDependencies` (the field) and `dependencyExclusions` (getter `getDependencyExclusions`) are **both** serialized, with the same content: `{"org~finos__dep__1~0~0":[{"groupId":"org.finos","artifactId":"x","versionId":null}]}`. The keys use the `__` / `~` escaping (quirk Q9).
* `transitiveDependenciesReport`, `evicted`, `created` and `updated` are **not** in the DTO.

### Alias summary

| Input | `/versions/{g}/{a}/{v}` | entity / dependency / PMCD routes | ETag |
|---|---|---|---|
| `x.y.z`, `<branch>-SNAPSHOT` | literal | literal + checks | yes for releases, no for SNAPSHOT |
| `latest` | `latestVersion` → 404 if unset | resolved; 500 `project version not found for g-a-latest` if unset | no (`/projects/{g}/{a}/versions/{v}` *does* send the ETag of the resolved version, `ER:67-68`) |
| `head` | `<defaultBranch>-SNAPSHOT` | same | no |
| `HEAD`, `LATEST` | literal (normally 404) | literal (normally 500) | no (treated as aliases for the ETag) |
| Studio `HEAD` | Studio rewrites it to `master-SNAPSHOT` client-side, ignoring `defaultBranch`, but only in `getEntities` / `getEntity` / `getIndexedDependencyEntities` / generation helpers (`st:legend-server-depot/src/DepotVersionAliases.ts:21-27`; `DSC:104-142,293-306`). `getVersionEntities` / `getDependencyEntities` / `getVersionEntity` pass the value through unchanged. | | |

---

## 5. Entities

### `GET /projects/{g}/{a}/versions/{v}` (`ER:58-69`)
* `resolveAliasesAndCheckVersionExists` runs **before** `handle`, so the existence check happens before the 304 evaluation. The ETag uses the **resolved** version, so a request for `latest` gets the ETag of the version it resolved to.
* `200 Entity[]`, each `{"path","classifierPath","content"}`, in Mongo order (§1.1). A version with no entities → `[]`.
* Errors: §0.3. Unknown project or version → **500** `project version not found for <g>-<a>-<v>`. It is not a 404.

### `GET /projects/{g}/{a}/versions/{v}/entities/{path}` (`ER:88-99` → `ESI:79-84` → `AEM:107-111`)
* Exists. `{path}` is one path segment. Studio sends `encodeURIComponent("a::B")` and Jersey decodes it.
* Found → `200 Entity`. The version exists but the entity does not → **404 with an empty body**. Missing or excluded version → 500 (§0.3). The ETag uses the unresolved version.
* DataCube relies on the 404 to fall back to dependencies (`st:legend-application-data-cube/src/stores/builder/source/LegendQueryDataCubeSourceBuilderStateHelper.ts:66-80`).

### `GET /projects/{g}/{a}/versions/{v}/classifiers/{classifier}` (`ER:71-86` → `ESI:72-77` → `Entities.java:39-42` → `EM:230-238`)
* Hidden in Swagger. The `classifier == null` guard is dead code (it builds a Response and discards it, `ER:81-84`).
* Returns entities whose stored `entityAttributes.classifierPath` equals `{classifier}` exactly, **wrapped as `DepotEntity`**, despite the `List<Entity>` signature. Shape **[verified]**: `{"groupId","artifactId","versionId":"<resolved>","versionedEntity":false,"entity":{"path","classifierPath","content"}}`.
* No match → `[]`. Version errors → §0.3. The ETag uses the unresolved version.
* `…/classifiers/{classifier}/dependencies?transitive&includeOrigin` (`EDR:78-98`) is §1.3 filtered by classifier, and each `entities[]` element is a `DepotEntity` (DataCube re-parses these, `LegendQueryDataCubeSourceBuilderStateHelper.ts:84-100`).

### `/classifiers/...` routes that clients call but depot@9c0a809 does not have
* `GET /classifiers/{classifierPath}?scope=&summary=&latest=` (`DSC:183-200`) and `GET /classifiers/{classifierPath}/entities?scope=` (`DSC:166-181`).
* Here they return **404 `{"code":404,"message":"HTTP 404 Not Found","timestamp":{…}}`** (unmatched route, §0.2).
* Callers: Query data-product selector, data-space advanced search, marketplace, depot dashboard (census Part D §7 #1). None of these are on the core Studio/Query load path.

---

## 6. `GET /projects/{g}/{a}/versions/{v}/pureModelContextData`

### 6.1 What the engine sends
`AlloySDLCLoader.getMetaDataApiUrl` (`eng:legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-modelManager-sdlc/src/main/java/org/finos/legend/engine/language/pure/modelManager/sdlc/alloy/AlloySDLCLoader.java:45-51`):

```
{alloy.baseUrl}/projects/{groupId}/{artifactId}/versions/{version}/pureModelContextData?convertToNewProtocol=false&clientVersion={clientVersion}
```

* Nothing is URL-encoded.
* `version` = the pointer's `sdlcInfo.version`; `null` or `"none"` becomes `master-SNAPSHOT`.
* The pointer must have `project == null` and non-null `groupId`/`artifactId` (assertions on the request side).
* **`getDependencies` is not sent, so it defaults to `true`.**
* `clientVersion` is the engine's request clientVersion (asserted non-null, `SDLCLoader.java:170-174`). Studio usually sends `vX_X_X`.

### 6.2 What depot does (`PMR:55-78`, `PMS:61-132`)
1. `clientVersion`:
   * not null and not in `PureClientVersions.versions` → `IllegalArgumentException("Client version provided is invalid, following are the valid client versions: v1_0_0, v1_1_0, …, v1_33_0, vX_X_X")` → 500;
   * null → `PureClientVersions.production`. That is `v1_33_0` in engine 4.138.2 and 4.145.0, which bracket depot's 4.140.6 **[verified via javap]**.
   * An engine newer than depot's protocol list breaks here.
2. Resolve the version (§0.3 errors).
3. Root PMCD = the version's entities (§5 order), built by legend-sdlc `PureModelContextDataBuilder` (`legend-sdlc-protocol-pure/.../PureModelContextDataBuilder.java:175-191`):
   * `serializer = {name:"pure", version:<clientVersion>}`;
   * `origin = PureModelContextPointer{serializer: same, sdlcInfo: AlloySDLC{project:"<g>:<a>", baseVersion:<resolved v>, version:"none" (default), packageableElementPointers:[] (default), groupId:null, artifactId:null}}`.
4. `convertToNewProtocol`:
   * `true` (default): entities are converted to protocol classes. **Entities that fail conversion are silently dropped** (`withEntitiesIfPossible`).
   * `false` (the engine's choice): each element is an `EntityPackageableElement` that serializes as the raw entity `content` map (`PMS:134-169`). Its `_package` and `name` fields stay **null**.
5. `getDependencies=false` → return the root PMCD.
6. `getDependencies=true` (default) → dependency entities via the **stored closure** (§1.3 with `transitive=true, includeOrigin=false`; any member error fails the request). Those are built into a second PMCD, then `combine` = `newBuilder().withPureModelContextData(root).add(deps).distinct().sorted()` (`PMS:118-124`):
   * The serializer and origin are the root's (first non-null).
   * `distinct()` removes elements with the same `(package, name)`; the first occurrence wins, so root elements beat dependency elements.
   * `sorted()` orders by package (empty first), then name, using `String.compareTo` (`eng:…/PureModelContextData.java` Builder `removeDuplicates` / `sort`, `ELEMENT_PATH_HASH`).
   * **With `convertToNewProtocol=false`, every element has null package and null name, so `distinct()` collapses the whole model to ONE element: the root's first entity.** **[verified]**: with engine 4.138.2's builder, 4 raw elements became 1. **This is exactly the engine's call.** There is a `//TODO: fix combining…` at `PMS:79` (quirk Q2).
7. ETag: `withGAV(<unresolved v>).withProtocolVersion(clientVersion)`. There is none for SNAPSHOT, aliases, a null clientVersion or `vX_X_X`.

### 6.3 Response JSON (PMCD mapper: alphabetical, NON_NULL, map keys sorted)

```json
{"_type":"data",
 "elements":[ … ],
 "origin":{"_type":"pointer",
           "sdlcInfo":{"_type":"alloy","baseVersion":"2.0.0","packageableElementPointers":[],"project":"org.finos:proj","version":"none"},
           "serializer":{"name":"pure","version":"v1_33_0"}},
 "serializer":{"name":"pure","version":"v1_33_0"}}
```

* The type id `_type` is always first.
* Raw elements (convertToNewProtocol=false) are the entity `content` maps with **keys sorted recursively** (`ORDER_MAP_ENTRIES_BY_KEYS`), e.g. `{"_type":"class","name":"…","package":"…","properties":[…]}` (depot test `TestPureModelContextService.java` expectations).
* `"groupId"` and `"artifactId"` of the AlloySDLC are absent (null).

### 6.4 What the engine requires of the response (`SDLCLoader.java:170-196,208-320`; `SDLCFetcher.java:60-80`)
* **Status:**
  * 502/503/504 → retried, up to 5 tries, 200 ms apart.
  * Any other non-2xx → `EngineException("Error response from <uri>, HTTP<code>\n<body>")`, wrapped in `EngineException("Engine was unable to load information from the Pure SDLC using: <a href='<url>' target='_blank'>link</a>")`.
* **Body:**
  * It must deserialize as `PureModelContextData`, with `serializer != null`.
  * `origin` must be non-null (it is dereferenced), and `origin.sdlcInfo.version` must equal `"none"`, otherwise "Version can't be set in the pointer". The engine then moves `baseVersion` → `version`.
  * Every `packageableElementPointers[].path` from the request pointer must be present among `elements[].path`. Otherwise: `The following entities:[…] do not exist in the project data loaded from the metadata server. …`.
* **Dependencies:** the engine expects the response to already contain the dependency elements.
* **Caching:** the engine caches unless the version is null, `none` or contains `SNAPSHOT`, so it **caches `latest`** (`AlloySDLCLoader.java:62-65`).
* The Studio client method `getPureModelContextData(g,a,v,getDependencies)` (`DSC:278-291`) has no caller.

---

## 7. What the clients call, and how they react to errors

### 7.1 Calls and parameters

| Client (file) | Route | Params / body |
|---|---|---|
| **Studio** workspace graph (`EGS:709-835`) | `POST /projects/dependenciesFromArtifactDependencies` | `?transitive=true&includeOrigin=true&versioned=false`. Body `[{"groupId","artifactId","versionId"[,"exclusions":[{"groupId","artifactId"}]]}]`, in project-configuration order; `exclusions` is omitted when empty (serializr `optional`, `st:legend-server-depot/src/models/ProjectVersionEntities.ts:26-52`). Only called when `projectDependencies.length > 0`. |
| Studio, on duplicate `g:a` versions (`EGS:765-791`); dependency editor (`ProjectDependencyEditorState.ts:630-668`) | `POST /projects/analyzeDependencyTreeFromArtifactDependencies` | the same body |
| Studio config / dependency editors, SDLC panel | `GET /project-configurations`, `GET /projects/{g}/{a}/versions?snapshots=true` | — |
| Studio project viewer by GAV (`ProjectViewerStore.ts:309-349`) | `GET /project-configurations/{g}/{a}`; `GET /projects/{g}/{a}/versions/{v}` (HEAD→master-SNAPSHOT); `GET …/dependencies?transitive=true&includeOrigin=false&versioned=false` | — |
| Studio dev-metadata panel (`DevMetadataState.ts:111-123`) | `GET /projects/{g}/{a}/versions/{DEV_SNAPSHOT_VERSION}` | any failure = "nothing deployed" |
| **Query** open by GAV (`QueryEditorStore.ts:806-868`) | `GET /project-configurations/{g}/{a}`; `GET /projects/{g}/{a}/versions/{v}`; `GET …/{v}/dependencies?transitive=true&includeOrigin=false&versioned=false` | `v` passes through `resolveVersion` |
| Query save with `latest` (`QueryEditorStore.ts:267-276`), data-space/product info | `GET /versions/{g}/{a}/latest` | reads `versionId` |
| Query version pickers (`QueryEditorStore.ts:1866-1885` etc.) | `GET /projects/{g}/{a}/versions?snapshots=true` | — |
| Query / DataCube classifier-minimal graphs (`DepotEntityHelper.ts:52-77`) | `GET …/{v}/classifiers/{c}` and `…/classifiers/{c}/dependencies?transitive=true&includeOrigin=false&versioned=false` | `c` is not URL-encoded |
| **DataCube** (`LegendQueryDataCubeSourceBuilderStateHelper.ts:64-100`, `LegendDataCubeDataCubeEngine.ts:617,656`) | `GET …/{v}/entities/{path}` (404 → fall back to classifier dependencies), `GET …/{v}`, `GET /project-configurations`, versions | — |
| **Engine** | `GET …/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=…` | §6.1 |

Fields the clients actually read: `StoreProjectData` → `groupId`, `artifactId`, `projectId` (`st:legend-server-depot/src/models/StoreProjectData.ts`). `ProjectVersionEntities` → `groupId`, `artifactId`, `versionId`, `entities`, indexed by `"g:a"` (`ProjectVersionEntities.ts:54-71`). The report → `graph.nodes{…id…}`, `rootNodes`, `conflicts` (`RawProjectDependencyReport.ts:32-110`). `VersionedProjectData` → `versionId`. Key order is irrelevant to all of them; it matters only for byte-for-byte parity tests.

### 7.2 Error handling

* **Error message extraction** (legend-shared `NetworkUtils.ts:144-159,188-212`):
  * Any non-2xx becomes a `NetworkClientError`. Its `message` is the JSON body's `message` string (cut to 5000 chars), else the raw body.
  * For an empty body (the 404s from Optional routes) the message is `Received response with status 404 (Not Found) for <url>`.
  * A masked ISE produces `There was an error processing your request. It has been logged (ID …).`
* **Studio workspace load** (`EGS:824-833`, `EGS:435-500`):
  * A failed `dependenciesFromArtifactDependencies` call is logged as `Can't acquire dependency entitites. Error: <msg>` (sic), notified as an error toast showing the original error, and rethrown as `DependencyGraphBuilderError`.
  * The graph builder then marks the build failed and notifies `Can't initialize dependency models. Error: <msg>`. It opens Project configuration → Project dependencies. It does **not** fall back to text mode.
  * A `NetworkClientError` elsewhere in the load gives a warning toast `Can't build graph. Error: <msg>`.
* **Studio duplicate-version check** (`EGS:735-822`): if the closure contains two versions of the same `g:a`, Studio calls `analyzeDependencyTree`. Errors from that call are only logged. It then throws `UnsupportedOperationError("Depending on multiple versions of a project is not supported. Found conflicts:\n…")`, using the report's conflicts if there are any, else its own list. With depot@9c0a809 this branch is effectively unreachable (Q1, §1.2).
* **Studio dependency report** (`ProjectDependencyEditorState.ts:630-668`): on error, `fetchingDependencyInfoState.fail()` and log only.
* **Query**:
  * A failed `getProject` / `getEntities` / dependencies call fails the whole editor initialization (generic error).
  * Version-list failures → `notifyError(error)`.
  * A missing version gives a 500 with `project version not found for …`, which is shown verbatim.

---

## 8. Upstream quirks and bugs found

| # | Quirk | Evidence |
|---|---|---|
| Q1 | **`analyzeDependencyTree*` never reports conflicts.** The Maven report is built from Aether's *resolved* tree, so `conflicts` is always `[]`. Studio's conflict UI and its duplicate-version error path are therefore dead with this depot. | `MDR:253-309`; `PSI:371-374` |
| Q2 | **The engine's PMCD call returns one element.** With `convertToNewProtocol=false` and `getDependencies=true` (the engine's exact call), `PureModelContextData.Builder.distinct()` compares `(package, name)`. Both are null on raw elements, so all elements collapse to the first root entity. Engine pointer resolution and `checkAllPathsExist` would then fail for any project with dependencies. Production may run a different build; worth confirming before copying. | `PMS:75-81,118-124,148-168`; replica on engine 4.138.2 **[verified]** |
| Q3 | **"Not found" is a 500, not a 404**, for every entity, dependency and PMCD route (IAE → catch-all). Only the Optional routes (`/project-configurations/{g}/{a}`, `/versions/{g}/{a}/{v}`, `/entities/{path}`) give 404, and those have an **empty body**. | §0.2, §0.3 |
| Q4 | **IllegalStateException messages are hidden.** Dropwizard's `IllegalStateExceptionMapper` beats depot's catch-all, so "being restored, please retry in 5 minutes", "Error calculating transitive dependencies…" and "Error collecting dependencies…" reach clients as `There was an error processing your request. It has been logged (ID …).` with no `timestamp`. Census Part D says these messages are visible; they are not. | dropwizard-jersey `IllegalStateExceptionMapper.java`; `ExceptionMapperBinder.java` |
| Q5 | **Exclusions are keyed by the requested version string** but looked up by the **resolved** one, so alias-requested roots silently lose their exclusions. Separately, an exclusion that is not in the dependency's stored closure, including a wildcard `*`, fails the whole request with 500 `cannot find project version, versionId cannot be null`. | `DEU:61-99`; `MDR:92-106`; `PVM:85-93` |
| Q6 | **`includeOrigin` on the Maven route is mostly meaningless.** The roots are always returned, because the Aether roots are depth-1 nodes, so `includeOrigin=false` does not drop them. With an alias it adds a duplicate entry for the same GAV. | `MDR:215-232`; `ESI:111-114` |
| Q7 | **Exclusion semantics differ from Maven.** Excluding X also excludes *X's whole transitive closure* below that root, even when another path in the same subtree needs those artifacts. | `DEU:37-59` |
| Q8 | **Two closure algorithms disagree.** The POST route does nearest-wins across the whole request at request time. The GET `…/dependencies` route and PMCD use the per-version closure frozen at ingest, plus a "root override". The same model can resolve differently in Studio (POST) and in Query or the engine (GET / PMCD). | §1.2 vs §1.3 |
| Q9 | **`ProjectVersionData` serializes the exclusion map twice** (`excludedDependencies` and `dependencyExclusions`), keyed by the escaped `g~x__a__v~y`. The reverse parser splits on `__`, so an artifactId containing `__` (allowed by the artifactId regex) breaks `reverseDependencyKey`. | `ProjectVersionData.java:42-43,141-173` |
| Q10 | **The error `timestamp` is `{"epochSecond":…,"nano":…}`, not ISO-8601,** because the plain ObjectMapper has no JavaTimeModule. SDLC errors use an ISO string. | §0.1 **[verified]** |
| Q11 | **304 handling has two problems.** For ETag routes keyed on the unresolved version, the 304 is evaluated before any existence check (a 304 can come back for a GAV that does not exist), and the 304 carries no `ETag` header. | `TR:102-108` |
| Q12 | **`analyzeDependencyTree*` does no alias resolution or existence check.** Missing roots appear as leaf nodes and `latest` stays literal in the gav. The entities route uses the opposite policy and fails hard. | `MDR:152-182` |
| Q13 | **`/versions/{g}/{a}/{v}` returns excluded versions**, because it skips the exclusion check that every other route applies. `/projects/{g}/{a}/versions` lists evicted versions, and reading them then fails. | `PVR:79-88`; `PSI:129-133` |
| Q14 | **No route sorts.** Version lists come in Mongo / index order (string-lexicographic at best), and dependency lists in HashSet order. | §0.5 |
| Q15 | **The classifier routes return `DepotEntity` wrappers** under a `List<Entity>` signature. Their `classifier == null` guard is dead code. | `ER:71-86`; `ESI:72-77` |
| Q16 | **Studio maps `HEAD` → `master-SNAPSHOT` client-side and ignores the project's `defaultBranch`.** Depot's own alias is lowercase `head` and does honour `defaultBranch`. Uppercase `HEAD` sent raw to depot fails. | `DepotVersionAliases.ts:21-27`; `PSI:181,190` |
| Q17 | **The `projectId` lookup for an evicted version can NPE.** `restoreEvictedProjectVersion` calls `findCoordinates(...).get()`, which throws `NoSuchElementException` (→ 500) for versions without a project config, e.g. REST-ingested ones. | `PSI:205-209` |
| Q18 | **A GET with a `Content-Type` other than JSON gets 415** on the two `…/dependencies` GET routes, because of their `@Consumes(application/json)`. | `EDR:64,81` |

### Not determined

* The JDK depot runs on. On JDK 15 and later an NPE (empty POST body) carries a "helpful" message; on JDK 11 the `message` key is absent.
* Whether the Mongo indexes exist in a given deployment. This decides index order versus insertion order for version and entity lists.
* Whether any deployed depot (GS fork or newer) fixes Q1, Q2 or adds the `/classifiers/*` routes. The clone is shallow.
* Aether behaviour was replayed on maven-resolver **1.9.24**. Depot resolves 1.9.2x through Maven 3.9.12. The `NearestVersionSelector.isNearer` bytecode was checked on 1.9.25.
