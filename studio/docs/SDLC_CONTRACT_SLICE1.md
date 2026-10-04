# legend-sdlc slice-1 behavioural contract (GitLab backend + shared resource layer)

> Read 2026-10-04 for Phase 1 (design S6, S15, S21): the rules `project-store/` (the page's SDLC) and
> `sdlc-server/` port, statuses and messages verbatim. JSON key orders were checked with replica
> classes in jshell on jackson-databind 2.10.5.1 / dropwizard-jersey 1.3.29. Where lite departs (a quirk
> below not copied), `project-store/README.md` lists it.

Source: `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc` @ `1021fda`. All paths below are relative to that root.
Abbreviations used in citations:

| Abbrev | File |
|---|---|
| `BGA` | `legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/gitlab/api/BaseGitLabApi.java` |
| `GAFA` | `legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/gitlab/api/GitLabApiWithFileAccess.java` |
| `GPA` | `.../gitlab/api/GitLabProjectApi.java` |
| `GWA` | `.../gitlab/api/GitLabWorkspaceApi.java` |
| `GRA` | `.../gitlab/api/GitLabRevisionApi.java` |
| `GEA` | `.../gitlab/api/GitLabEntityApi.java` |
| `GPCA` | `.../gitlab/api/GitLabProjectConfigurationApi.java` |
| `GAT` | `.../gitlab/tools/GitLabApiTools.java` |
| `EMO` | `legend-sdlc-core/src/main/java/org/finos/legend/sdlc/core/entity/EntityModificationOperations.java` |
| `EAO` | `legend-sdlc-core/src/main/java/org/finos/legend/sdlc/core/entity/EntityAccessOperations.java` |
| `PSU` | `legend-sdlc-core/src/main/java/org/finos/legend/sdlc/core/project/ProjectStructureUpdater.java` |
| `PS` | `legend-sdlc-project-structure/src/main/java/org/finos/legend/sdlc/project/structure/ProjectStructure.java` |
| `ERR/` | `legend-sdlc-server-shared/src/main/java/org/finos/legend/sdlc/server/error/` |
| `RES/` | `legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/resources/` |

---

## 0. Cross-cutting: JSON serialization, error envelope, status mapping

### 0.1 ObjectMapper actually used for responses

* Response mapper = Dropwizard bootstrap mapper `Jackson.newObjectMapper()` (dropwizard-core 1.3.29 `Bootstrap.java:64`), Jackson 2.10.5 / databind 2.10.5.1 (`pom.xml:93,105-106`).
  * `Jackson.configure()` (dropwizard-jackson 1.3.29 `Jackson.java:56-70`): GuavaModule, GuavaExtrasModule, JodaModule, AfterburnerModule **only on Java 8** (`:60-62`), **FuzzyEnumModule** (`:63`, enum input is case-insensitive / `-`→`_` tolerant), **ParameterNamesModule** (`:64`), Jdk8Module, **JavaTimeModule** (`:66`), `AnnotationSensitivePropertyNamingStrategy` (snake_case only for `@JsonSnakeCase` classes, none here), `DiscoverableSubtypeResolver`.
  * **No** `SORT_PROPERTIES_ALPHABETICALLY`, **no** `@JsonPropertyOrder` on any served model class (only on unrelated serializer/maven classes; grep), serialization inclusion is Jackson default **ALWAYS** (nulls are written), `FAIL_ON_UNKNOWN_PROPERTIES` left at Jackson default **true** (unknown request-body fields → 400, see 0.3).
  * `BaseServer.run` adds only `WRITE_DATES_AS_TIMESTAMPS=false` (`legend-sdlc-server-shared/.../BaseServer.java:120`) and makes Jersey use the same mapper (`:123-130`).
  * `BaseLegendSDLCServer.initialize` adds mixins / subtypes only (`legend-sdlc-server/.../BaseLegendSDLCServer.java:73-76`); `PureProtocolObjectMapperFactory.withPureProtocolExtensions` only registers modules/subtypes (legend-engine `PureProtocolObjectMapperFactory.java:98-101,129+`), no ordering/inclusion changes.
* **Instant format**: JavaTimeModule `InstantSerializer` with timestamps disabled → `DateTimeFormatter.ISO_INSTANT`, identical to `Instant.toString()`: `2026-10-04T12:00:00Z`, fraction printed in groups of 3 only when non-zero (`...:01.500Z`, `...:00.123456Z`). Verified empirically (jshell, databind 2.10.5.1).
* **Property order, what Jackson really does** (Jackson 2.10 `POJOPropertiesCollector`): fields are collected first (insertion order), then getters in `Class.getDeclaredMethods()` order (JVM-dependent, NOT source order), then if the class has a `@JsonCreator` with named params, those creator properties are moved to the front in creator-parameter order. Consequences (verified by jshell replicas on JDK 25 + databind 2.10.5.1):
  * Classes with a `@JsonCreator` → **deterministic**, creator-param order: `ExtendedErrorMessage`, `SimpleProjectConfiguration`, `SimpleProjectStructureVersion`, `LegendSDLCServerFeaturesConfiguration`.
  * Anonymous/inner implementations of interfaces (`Revision`, `Workspace`, `User`, `Entity`, anonymous `ProjectStructureVersion`) → **unspecified** (getDeclaredMethods order). Observed on JDK 25: `Revision` → `{"message","id","authoredTimestamp","committedTimestamp","authorName","committerName"}`; `Workspace` → `{"projectId","userId","workspaceId"}`; `User` → `{"name","userId"}`; `Entity` → `{"content","path","classifierPath"}`; anonymous `ProjectStructureVersion` → `{"extensionVersion","version"}`. Production (likely JDK 11) may differ. **Recommendation for lite: emit interface-declaration order** (clients — legend-studio — key by name).
  * `ProjectWrapper` (GPA:1093-1140) has a private field `projectId` matching its getter → `projectId` is first; observed `{"projectId","name","description","tags","webUrl"}`.

### 0.2 Error body (`ExtendedErrorMessage`)

* Class: `ERR/ExtendedErrorMessage.java:28` extends Dropwizard `io.dropwizard.jersey.errors.ErrorMessage`, which is annotated `@JsonInclude(NON_NULL)` (dropwizard-jersey 1.3.29 `ErrorMessage.java:11`; class annotation is inherited by Jackson) → null fields omitted.
* Fields, in serialized order (creator order `ERR/ExtendedErrorMessage.java:52-60`, verified): `code` (int), `message` (string), `details` (omitted unless set), `stackTrace` (omitted unless set), `timestamp` (ISO instant, `Instant.now()` at mapping time, `:97`).
  * Example: `{"code":404,"message":"Unknown project: PROD-1","timestamp":"2026-10-04T12:00:00.123Z"}`.
* `message` = `t.getMessage()`, recursing into `getCause()` only if null (`:101-113`). `code` = the exception's status (`:75-88`).
* `stackTrace` only when `errorHandlingConfiguration.includeStackTrace` is true **and** status is 5xx; never for 4xx (`ERR/LegendSDLCServerExceptionMapper.java:47-55`, `ERR/LegendSDLCExceptionMapper.java:54-62`; config `BaseServer.java:133`).
* Mapper selection (`BaseServer.java:134-137`): `JsonProcessingExceptionMapper`, `LegendSDLCServerExceptionMapper`, `LegendSDLCExceptionMapper`, `CatchAllExceptionMapper<Throwable>`. Dropwizard's default mappers are also bound (`registerDefaultExceptionMappers` default TRUE, dropwizard-core `AbstractServerFactory.java:264,543-544`; `ExceptionMapperBinder.java:28-34`), but Jersey 2.25.1 picks the closest exception type and on ties keeps the **first** candidate (`ExceptionMapperFactory.java:122-140,163` `return !sameDistance`), and custom (`register()`ed) providers are listed before HK2-bound defaults (`Providers.java:332-347`) → legend's mappers win ties (`Throwable`, `JsonProcessingException`). Dropwizard's `IllegalStateExceptionMapper`/`EarlyEofExceptionMapper` still win for those specific types (closer distance).
* Status mapping (`ERR/LegendSDLCServerExceptionMapper.java:41-80`, same logic `ERR/LegendSDLCExceptionMapper.java:48-87`):
  * 4xx/5xx → that status, JSON body.
  * 3xx → `Response.status(s).location(new URI(message))`, **no body**; if message null/invalid URI → 500 JSON.
  * any other (1xx/2xx) → 500 JSON.
* `LegendSDLCServerException` default status when none given = **500** (`ERR/LegendSDLCServerException.java:33,37-53`); `validate*` helpers default to **400** (`:75-96`). Same for `LegendSDLCException` (`legend-sdlc-shared/.../error/LegendSDLCException.java:30-31`).
* `CatchAllExceptionMapper` (`ERR/CatchAllExceptionMapper.java:35-52`): `WebApplicationException` → keeps the original response (status + headers, e.g. `Allow`) but replaces entity with `ExtendedErrorMessage.fromWebApplicationException` (message = JAX-RS default `"HTTP <code> <reason>"`); 3xx passed through untouched. Any other Throwable → `buildDefaultResponse` → 500 (`ERR/BaseExceptionMapper.java:61-83`).
* **Unknown route** (inside the Jersey root path, typically `/api`): Jersey `NotFoundException` → `404` `{"code":404,"message":"HTTP 404 Not Found","timestamp":"..."}`. Wrong method → `405` `{"code":405,"message":"HTTP 405 Method Not Allowed",...}` + `Allow` header. Unparseable `@QueryParam` (e.g. `?limit=abc`) → JAX-RS rule → **404** `"HTTP 404 Not Found"`. Paths outside the Jersey servlet → Jetty HTML 404.

### 0.3 Request-body JSON errors

* Malformed JSON / unknown property / bad enum / setter throwing → `ERR/JsonProcessingExceptionMapper.java:39-53`: status **400**, `message` = `"Unable to process JSON"` (verbatim), `details` = Jackson's exception message. Body shape `{"code":400,"message":"Unable to process JSON","details":"Unrecognized field \"foo\" (class ...), not marked as ignorable ...","timestamp":...}`. (`InvalidDefinitionException`/`JsonGenerationException` → 500 default response, `:42-46`.)
* Missing/empty body → resource receives `null` command → each POST resource's own null check (see per-route).

### 0.4 GitLab exception translation (used by every route) — `BGA:848-1018`

`buildException(e, forbidden, notFound, default)` →
* `e instanceof LegendSDLCException` (incl. `LegendSDLCServerException`) → **rethrown unchanged** (status+message preserved) (`BGA:958-972`).
* `GitLabApiException` status 401 → GET: 302 to the request URL (`BGA:880-895`); non-GET: 503 `"Please retry request: <METHOD> <url>"` (`:898`).
* 403 → 403, message = forbidden supplier **+ `": " + gitlabMessage`** (`BGA:861-863,901-904`).
* 404 → 404, message = notFound supplier **verbatim (no appended GitLab message)** (`BGA:864-866,905-908`).
* anything else (400, 409, 500 …) → default supplier + `": " + gitlabMessage`, status **500** (`BGA:909-916,989-1004`). Separator is `": "` (`legend-sdlc-shared/.../tools/StringTools.java:30,76-95`).
* **Single-message variant** `buildException(e, () -> msg)` (`BGA:853-856`) resolves to the `Function` overload with forbidden/notFound = null: the one message is used for **every** status (403 → 403, 404 → 404, other → 500) and the GitLab message is **not** appended.
* If a supplier is null, fallbacks: default supplier, then GitLab message, then `"An unexpected error occurred (GitLab response status: <n>)"` (`BGA:919-952`); final fall-through `"An unexpected exception occurred: <msg>"` 500 (`BGA:1006-1017`).

### 0.5 Reference-info strings (appear in many messages)

* `getReferenceInfo(projectId, sourceSpec[, revisionId])` (`BGA:751-799`):
  * optional prefix `"revision <rev> of "` (raw, un-resolved revision id as passed in) (`:763-766`);
  * user workspace → `"user workspace <w> of "` (`WorkspaceType` labels `user`/`group`, `legend-sdlc-model/.../workspace/WorkspaceType.java:19-20`; access-type labels `workspace`, `workspace with conflict resolution`, `backup workspace`, `legend-sdlc-project-files/.../ProjectFileAccessProvider.java:278-302`);
  * then `"project <projectId>"`.
  * e.g. `"user workspace w1 of project PROD-1"`, `"revision HEAD of user workspace w1 of project PROD-1"`, `"project PROD-1"`.
* **Different** builder in the revision-access context (`GAFA:821-872`): `"user workspace w1 in project PROD-1"` (uses **" in "**), project scope `"project PROD-1"`.

### 0.6 Project id parsing — `BGA:143-159`, `legend-sdlc-server/.../gitlab/GitLabProjectId.java:81-113`

* Format `"<prefix>-<gitlabNumericId>"` (prefix = `gitLab.projectIdPrefix` config) or bare number when prefix is null. Split at first `-`.
* Any parse failure or prefix mismatch → **400** `Invalid project id: "<id>"` (with quotes). Done lazily — see per-route ordering.

---

## 1. `GET /currentUser`, `GET /auth/authorized`, `GET /server/features`

### `GET /currentUser`
* `RES/user/CurrentUserResource.java:44-49` → `GitLabUserApi.getCurrentUserInfo` (`.../gitlab/api/GitLabUserApi.java:95-115`).
* 200 `User`: `{ "userId": <GitLab username>, "name": <GitLab display name> }` (`BGA:1034-1057`; order unspecified, see 0.1).
* Errors: 403 `"User <u> is not allowed to get current user information: <glmsg>"`; other → 500 `"Error getting current user information: <glmsg>"`; null user → 500 `"Could not get current user information"` (`GitLabUserApi.java:103-114`).
* FS backend: `FileSystemUser{userId,name}` (`legend-sdlc-server-fs/.../domain/model/user/FileSystemUser.java`).

### `GET /auth/authorized`
* GitLab: `legend-sdlc-server/.../gitlab/resources/GitLabAuthCheckResource.java:67-105`. Body is a bare JSON boolean `true`/`false`, status 200. Finds/creates a session (PAT header client or session store); returns `GitLabUserContext.isUserAuthorized()` iff the session is a `GitLabSession`; `GitLabAuthAccessException` → logged, `false`; non-GitLab session → `false`.
* FS: always `true` (`legend-sdlc-server-fs/.../resources/FileSystemAuthCheckResource.java:39-45`).

### `GET /server/features`
* `RES/ServerResource.java:57-63` returns the bound `LegendSDLCServerFeaturesConfiguration`.
* Shape: public final fields, creator order → `{"canCreateProject":<bool>,"canCreateVersion":<bool>}` (`legend-sdlc-server/.../config/LegendSDLCServerFeaturesConfiguration.java:20-38`).
* Default when the `features` config section is absent: **both false** (`emptyConfiguration()` `:40-46`; binding `.../guice/AbstractBaseModule.java:276,623-627`). Note: this makes `POST /projects` return 405 by default (§2).

---

## 2. Projects

### `GET /projects` — `RES/project/project/ProjectsResource.java:67-88`, `GPA:115-213`
Query params:
* `search` (string, optional): passed to GitLab project search (`ProjectFilter.withSearch`) (`GPA:139-143`). Special case: if `tag` contains `sandbox` (i.e. prefixed `legend_sandbox`) and `search` is null, search = current user id (`GPA:132`).
* `user` (boolean, **default true**, `@DefaultValue("true")`): GitLab `membership` filter (`GPA:142`). Non-boolean text parses as `false` (JAX-RS `Boolean.valueOf`).
* `tag` (repeatable): each value is prefixed `"<projectTag>_"` (`projectTag` default `"legend"`, `GPA:87,1052-1056,1073-1081`); keep projects whose GitLab topic list contains **any** of them (exact, case-sensitive `Set.contains`) (`GPA:146-153`).
* `excludeTag` (repeatable): same prefixing; drop projects having any (`GPA:154-161`).
* `limit` (Integer): null → unlimited; `0` → `[]` immediately (no GitLab call); `<0` → **400** `"Invalid limit: <n>"` (`GPA:118-129`); `>0` → stream limit. (Swagger text "non-positive → no filtering" is wrong for this backend.)
* `type` (repeatable): **ignored** (`ProjectsResource.java:80-82`).
* Always also restricted to the Legend marker topic (GitLab `topic=<projectTag>` + in-memory case-insensitive check, `GPA:139-145,1041-1050`).
* Order: GitLab's order (default `created_at desc`); not re-sorted.
* Errors (single-message `buildException`, §0.4): message `Failed to find [user ]projects[ (search="<s>", tags=[a, b], excludeTags=[c])]` (`user ` present iff `user=true`; tags sorted, *unprefixed* user-supplied values; `search` is the effective one incl. the sandbox substitution), no GitLab text appended; status 403/404 mirrored, anything else 500 (`GPA:170-211`).

### `GET /projects/{id}` — `ProjectsResource.java:90-100`, `GPA:105-112`, `GPA:1019-1039`
* Invalid id → 400 `Invalid project id: "<id>"`.
* GitLab 404 → **404 `"Unknown project: <normalizedId>"`** (`GPA:1031`); GitLab project exists but lacks the marker topic → **404 `"Unknown project: <id>"`** (`GPA:1034-1037`). 403 → `"User <u> is not allowed to get project <id>: <glmsg>"`; other → 500 `"Failed to get project <id>: <glmsg>"`.
* `<normalizedId>` = `GitLabProjectId.toString()` = `prefix + "-" + gitlabId`.
* 200 `Project`: `{"projectId","name","description","tags","webUrl"}` (`GPA:1093-1140`). `projectType` is `@JsonIgnore` (`:1128-1133`) → **absent**. `tags` = GitLab topics that start (case-insensitively) with `"<projectTag>_"` and are longer than that prefix, with the prefix stripped (`GPA:1058-1071,1083-1086`); the bare marker topic is not listed. `projectId` = `"<prefix>-<gitlabId>"`.

### `POST /projects` with `CreateProjectCommand` — `ProjectsResource.java:102-121`, `GPA:215-316`
Body fields (`legend-sdlc-server/.../application/project/CreateProjectCommand.java:21-89`): `name`, `description`, `type` (`ProjectType` enum: `PRODUCTION`, `PROTOTYPE`, `MANAGED`, `EMBEDDED`; fuzzy-case input), `groupId`, `artifactId`, `tags` (list).

Validation order (each throws immediately; all 400 unless stated):
1. Body null → **400 `"Input required to create project"`** (`ProjectsResource.java:106`).
2. `features.canCreateProject` false → **405 `"Server does not support creating project(s)"`** (`:107-111`).
3. `name` null or `""` → `"name may not be null or empty"` (`GPA:218`). (Whitespace-only allowed.)
4. `description` null → `"description may not be null"` (`GPA:219`). (Empty string allowed.)
5. `groupId` invalid → `"Invalid groupId: <groupId>"` (`GPA:220`). Valid = non-null, non-empty, `javax.lang.model.SourceVersion.isName(groupId)` (dotted Java identifiers, no keywords, no `-`) (`PS:371-374`).
6. `artifactId` invalid → `"Invalid artifactId: <artifactId>. ArtifactId must follow pattern that starts with a lowercase letter and can include lowercase letters, digits, underscores, and hyphens between segments."` (`GPA:221`); pattern `[a-z][a-z\d_]*+(-[a-z][a-z\d_]*+)*+` (`PS:93,376-379`).
7. `type` non-null and not `MANAGED`/`EMBEDDED` → `"Invalid type: <TYPE>"` (`GPA:222-225`, `PS:366-369`). **`type` null is allowed → becomes MANAGED** (`PSU:144-154`).
8. Optional config patterns (`projectStructure.projectCreation.groupIdPattern/artifactIdPattern`): `groupId must match "<pattern>", got: <groupId>` / `artifactId must match "<pattern>", got: <artifactId>` (`GPA:989-1006`).

Then (`GPA:229-315`):
* GitLab project created with name, description, topics `[<projectTag>, <projectTag>_<t>...]`, visibility (config, default INTERNAL `GPA:88,1013-1017`), MRs+issues on, wiki/snippets off. Failure: 403 `"User <u> is not allowed to create project <name>: <glmsg>"`; 404 `"Failed to create project: <name>"`; other 500 `"Failed to create project: <name>: <glmsg>"`; null → 500 `"Failed to create project: <name>"`.
* Best-effort: ensure creator ≥ MAINTAINER; protect default branch (push NONE, merge MAINTAINER) — failures only logged (`GPA:266-285`).
* **Project structure**: version = `projectCreation.defaultProjectStructureVersion` config, else latest (= **13**, max registered factory `ProjectStructureFactory.java:13-19`, services file lists V0, V11, V12, V13) (`GPA:975-987`). Extension version = provider's latest for that version; the default provider is `VoidProjectStructureExtensionProvider` → `null` (`AbstractBaseModule.java:598-615`, `VoidProjectStructureExtensionProvider.java:22-25`).
* `ProjectStructureUpdater...withMessage("Build project structure").build()` (`GPA:301-305`) on the **project source spec (default branch)** (`PSU:466-472`), revision null for the empty repo (`PSU:94-133`, requireRevisionId=false via `build()` `PSU:620-627`).
* Commits: **exactly one commit**, message **`Build project structure`**, on the default branch (`PSU:197-250` → `GAFA:889-931` one `createCommit`; >512 ops would split into `"<msg> [i / n]"` commits via a temp branch, not reached here `GAFA:1028-1067`). That commit is the project's initial (and BASE) revision. Contents for v13 (artifactId `a`):
  * `/project.json` (add, `PSU:198`)
  * `/pom.xml` (`maven/MavenProjectStructure.java:76,133-139`)
  * `/a-entities/pom.xml`, `/a-versioned-entities/pom.xml`, `/a-service-execution/pom.xml`, `/a-file-generation/pom.xml` (`maven/MultiModuleMavenProjectStructure.java:140-152,517-520`; `ProjectStructureV13Factory.java:66-74`)
  * `/a-entities/src/test/java/org/finos/legend/sdlc/EntityValidationTest.java` (`ProjectStructureV13Factory.java:64,179-185`)
* `project.json` serializer: indented, map keys sorted, **properties sorted alphabetically**, NON_NULL (`PS:71-78,439-449`). For type null/MANAGED, groupId `org.finos.test`, artifactId `test-project`, id `PROD-1` (verified by jshell replica):
```json
{
  "artifactGenerations" : [ ],
  "artifactId" : "test-project",
  "groupId" : "org.finos.test",
  "metamodelDependencies" : [ ],
  "projectDependencies" : [ ],
  "projectId" : "PROD-1",
  "projectStructureVersion" : {
    "version" : 13
  },
  "projectType" : "MANAGED"
}
```
  (no trailing newline; `extensionVersion` omitted because null.) Before writing, `validateProjectConfiguration` re-checks groupId/artifactId/projectType with messages `"Invalid groupId: …"`, the same artifactId text, `"Invalid projectType: <t>"` (`PSU:285-321`).
* Structure-setup failure → 403 `"User <u> is not allowed to set up project structure for newly created project <id>: <glmsg>"`, 404 `"Error setting up project structure for newly created project <id>"`, else 500 `"Error setting up project structure for newly created project <id>: <msg>"`; **the GitLab project is not rolled back** (`GPA:307-313`).
* Response: **200** with the `Project` JSON of the new project (not a Revision) (`GPA:315`).

FS backend: no groupId/artifactId/type pre-validation, projectId = `name`, creates **two** commits (`"Initial Commit"` empty commit, then `"Build project structure"`) (`legend-sdlc-server-fs/.../api/project/FileSystemProjectApi.java:107-153`); `GET /projects` ignores all filters (`:76-105`).

---

## 3. User workspaces

Branch naming (`BGA:98-109,541-557`): user workspace = `workspace/<currentUserId>/<workspaceId>`; conflict resolution `resolution/<user>/<id>`; backup `backup/<user>/<id>`; group `group/<id>`. Source branch = project default branch (`BGA:503-528`).

### Workspace id validation — `GWA:1140-1180`
Non-empty; first and last char ∈ `[A-Za-z0-9_]`; inner chars ∈ `[A-Za-z0-9_-]` or `.` not immediately after another `.`. Failure → **400**:
`Invalid workspace id: "<id>". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain ".." and may not start or end with '.' or '-'.`
Only checked on create.

### `GET /projects/{p}/workspaces?owned=` — `RES/workspace/project/user/WorkspacesResource.java:51-63`
* `owned` default **true** → `getUserWorkspaces` = types {USER}, access {WORKSPACE}, source project, **current user only** (`legend-sdlc-backend-api/.../workspace/WorkspaceApi.java:143-146,169-172`; `GWA:111-115,198-221`).
* `owned=false` → `getAllUserWorkspaces` = types {USER}, **all access types (WORKSPACE, CONFLICT_RESOLUTION, BACKUP)**, all users (`WorkspaceApi.java:204-208`; `GWA:117-151`) — conflict/backup branches come back as indistinguishable `Workspace` objects (bug-ish).
* Order: per (type, access type in enum order WORKSPACE, CONFLICT_RESOLUTION, BACKUP), GitLab branch-list order (by name) (`GWA:140-151,198-210`).
* Element JSON `{"projectId","userId","workspaceId"}`; `projectId` is the normalized id (`GWA:161-162`).
* Errors: invalid id 400; GitLab 404 → **404 `"Unknown project: <p>"`**; 403 `"User <u> is not allowed to get workspaces for project <p>: <glmsg>"`; else 500 `"Error getting workspaces for project <p>: <glmsg>"` (`GWA:164-170`).

### `GET /projects/{p}/workspaces/{w}` — `WorkspacesResource.java:65-76`, `GWA:88-109`
* GitLab `getBranch(workspace/<me>/<w>)`. Missing (or project missing) → **404 `"Unknown: user workspace <w> of project <p>"`** (`GWA:106`). 403 `"User <u> is not allowed to get user workspace <w> of project <p>: <glmsg>"`; else 500 `"Error getting user workspace <w> of project <p>: <glmsg>"`.
* Invalid project id → 400 (`parseProjectId` thrown inside the try, re-thrown unchanged).
* 200 `{"projectId": <p as given in URL>, "userId": <me>, "workspaceId": <w>}` (`BGA:1059-1093`). No id validation on GET.

### `POST /projects/{p}/workspaces/{w}` (no body) — `WorkspacesResource.java:104-116`, `GWA:330-426`
Order:
1. Workspace id validation → 400 (before project id parsing) (`GWA:338`).
2. `getProjectConfiguration(p, <the new workspace's own source spec>) == null` → 409 `"Project structure has not been set up"` (`GWA:340-343`) — **dead code**: `GAFA:124-128` substitutes a default config, never null (and it reads the not-yet-existing workspace branch anyway). Invalid project id → 400 surfaces here.
3. Best-effort deletion of leftover `backup/<me>/<w>` and `resolution/<me>/<w>` branches (errors only logged) (`GWA:361-397`).
4. Create branch from **current HEAD of the default branch** (`getSourceBranch`, `GWA:400-401`; `GAT:305-319`). Note `getSourceBranch` → `getDefaultBranch` is outside the try and uses the single-message `buildException` (`BGA:463-474`, §0.4): missing project → **404 `"Error getting default branch for <normalizedId>"`** (no GitLab text). (Steps 2-3 against a missing project don't fail: every GitLab 404 there is read as "absent".)
5. **Already exists**:
   * branch exists **at the same commit as default-branch HEAD** → no-op, returns 200 with the Workspace (idempotent) (`GAT:267-289,291-303`);
   * branch exists at a different commit → GitLab `createBranch` returns 400 "Branch already exists" → **500** `"Error creating user workspace <w> of project <p>: <glmsg>"` (`GWA:409-415` default supplier, `BGA:909-916`). There is no explicit 409.
   * source branch never appears after 30 tries → `null` → 500 `"Failed to create user workspace <w> of project <p>"` (`GWA:416-419`).
6. Sandbox hook: if project topics contain `<projectTag>_sandbox` and `<projectTag>`, configure project (MANAGED, `com.gs.alloy.sandbox`/`my-prototype`) inside the workspace (`GWA:420-424`).
7. 200 `Workspace` JSON (not 201).

### `DELETE /projects/{p}/workspaces/{w}` — `WorkspacesResource.java:118-129`, `GWA:431-490`
* Deletes `workspace/<me>/<w>`; **GitLab 404 on delete is treated as success** (`GAT:242-265`), so deleting a non-existent workspace — or one in a non-existent project — returns success. Then deletes `resolution/<me>/<w>` and `backup/<me>/<w>`, errors only logged.
* Response: **204 No Content** (void).
* Errors: 403 `"User <u> is not allowed to delete user workspace <w> of project <p>: <glmsg>"`; other GitLab errors 500 `"Error deleting user workspace <w> of project <p>: <glmsg>"`; branch still present after 20 checks → 500 `"Failed to delete user workspace <w> of project <p>"` (`GWA:443-457`).

### `GET .../{w}/outdated` — `WorkspacesResource.java:78-89`, `GWA:223-286`
* Body bare boolean. Missing workspace → **404 `"Unknown: user workspace <w> of project <p>"`** (`GWA:244`); default branch missing → 404 `"Unknown: project <p>"` (`GWA:260`).
* `false` if workspace HEAD == default-branch HEAD; else `true` iff the default-branch HEAD commit is **not** contained in the workspace branch (GitLab commit refs) (`GWA:263-278`).

### `GET .../{w}/inConflictResolutionMode` — `WorkspacesResource.java:91-102`, `GWA:288-328`
* Missing workspace → 404 `"Unknown: user workspace <w> of project <p>"`. Returns `true` iff branch `resolution/<me>/<w>` exists; always `false` for non-WORKSPACE access type.

### groupWorkspaces mirror
`RES/workspace/project/group/GroupWorkspacesResource.java` at `/projects/{projectId}/groupWorkspaces` mirrors every route above with `WorkspaceType.GROUP` (branch `group/<id>`, labels `"group workspace <w> of project <p>"`). Differences: list param is `includeUserWorkspaces` (default **false**) → false: group workspaces only; true: `getAllWorkspaces(p)` = all types, all access types, all users (`GroupWorkspacesResource.java:51-61`, `WorkspaceApi.java:155-158,216-220`). Group `Workspace.userId` is `null`. Entities, entityChanges, revisions, configuration are mirrored under `/groupWorkspaces/{w}/...`.

FS backend: create on existing branch → 500 `"Failed to create workspace <branch> for project <p> : <jgit msg>"` (note `" : "`) (`legend-sdlc-server-fs/.../api/workspace/FileSystemWorkspaceApi.java:113-136`, `exception/FSException.java:26-29`); type→prefix lookups for USER/GROUP lists are swapped (`:84-95`).

---

## 4. Revisions

Routes: `GET /projects/{p}/revisions/{r}` (`RES/revision/project/ProjectRevisionsResource.java:66-76`), `GET /projects/{p}/workspaces/{w}/revisions/{r}` (`RES/revision/project/user/WorkspaceRevisionsResource.java:68-78`) → `GRA:68-74` → `GAFA:650-723` (wrapped `GRA:271-306`).

### Alias resolution — `BGA:1250-1295`, enum `legend-sdlc-model/.../revision/RevisionAlias.java:17-25`
* Case-**insensitive** `equalsIgnoreCase`: `base` → BASE; `head`, `current`, `latest` → HEAD; anything else → literal revision id (no format check).
* HEAD → `getCurrentRevision()` = newest commit on the ref (`GAFA:470-543`): project → default branch; workspace → `workspace/<me>/<w>`.
* BASE:
  * project → **oldest commit** of the default branch (`commits?per_page=1`, last page) (`GAFA:552-601,642-647`) = the `Build project structure` commit for a new project;
  * workspace → **GitLab merge-base(default branch, workspace branch)** (`GAFA:546-550,603-640`) = the commit the workspace was created/last rebased from.
* Then for every resolved id: fetch commit, and verify it is reachable from the scope's branch (commit refs list contains the branch name) (`GAFA:684-700`).

### Missing / bad revision — exact messages (all 404 unless stated)
* Literal id not a commit (GitLab 404): `"Revision <rev> is unknown for <desc>"` (`GAFA:680`), where `<desc>` = `"project <p>"` or `"user workspace <w> in project <p>"` (note **in**, §0.5).
* Commit exists but not on that branch: same `"Revision <rev> is unknown for <desc>"` (`GAFA:691`).
* HEAD of a non-existent workspace: `"Unknown: user workspace <w> in project <p>"` (`GAFA:504-507`, passes through `GAFA:661`).
* Alias resolves to null (repo with no commits): `"Failed to resolve revision <rev> of project <p>"` (`GAFA:666-669`).
* Other GitLab errors: 403 `"User <u> is not allowed to access revision <rev> for <desc>: <glmsg>"`; 500 `"Error accessing revision <rev> for <desc>: <glmsg>"` (and `"...<rev>for <desc>"` — missing space — in the branch-check path, `GAFA:699`).
* BASE on a missing workspace depends on GitLab's merge-base response (404 → `"Unknown: user workspace <w> in project <p>"`, other → 500 `"Error getting base revision for user workspace <w> in project <p>: <glmsg>"`) (`GAFA:633-639`).

### Revision JSON — `GAFA:1584-1634`
Fields: `id` (commit SHA), `authorName`, `authoredTimestamp` (Instant), `committerName`, `committedTimestamp` (Instant), `message` (full commit message). Nulls written as `null`. Order unspecified (0.1). Timestamps from GitLab dates (ms precision) → e.g. `"2026-10-04T12:00:00Z"` / `"…:00.250Z"`.

FS: `legend-sdlc-server-fs/.../api/BaseFSApi.java:99-130` has identical alias logic.

---

## 5. Entities (read)

Routes (user workspace + project; group mirrors): `GET /projects/{p}/entities`, `/projects/{p}/entities/{path}`, `/projects/{p}/revisions/{r}/entities[/{path}]`, `/projects/{p}/workspaces/{w}/entities[/{path}]`, `/projects/{p}/workspaces/{w}/revisions/{r}/entities[/{path}]` (`RES/entity/project/ProjectEntitiesResource.java:37-87`, `ProjectRevisionEntitiesResource.java:37-89`, `user/WorkspaceEntitiesResource.java:48-128`, `user/WorkspaceRevisionEntitiesResource.java:39-92`).

### Revision handling for entity reads — `GEA:189-192`, `GAFA:169-193`
* `revisionId` null → read the branch ref. Aliases resolved as §4. **A literal revision id is NOT validated** (no existence or branch-membership check) — files are read at that git ref directly.
* Resolution failures: 404 `"Unknown revision <refInfo>"` where refInfo already starts with "revision" → e.g. `"Unknown revision revision HEAD of user workspace w1 of project PROD-1"` (`GAFA:185`); LegendSDLC exceptions (e.g. `"Unknown: user workspace w1 in project PROD-1"`) pass through; null → 404 `"Failed to resolve  revision <r> of …"` (two spaces) (`GAFA:190`).

### Filter params (list endpoints) — `RES/EntityAccessResource.java:40-246`
* `classifierPath` (repeatable): exact string match against entity classifierPath; empty set → no filter (`:82-104`).
* `package` (repeatable) + `includeSubPackages` (default **true**): entity's package (path before last `::`) must equal one of the packages; with subpackages, may also be a descendant (`pkg::…`). Exact, case-sensitive (`:48-80,227-246`).
* `name` (regex): compiled `CASE_INSENSITIVE`; invalid regex → treated as a **literal** (`Pattern.LITERAL|CASE_INSENSITIVE`) (`:155-171`); matched with `find()` (substring) against the **simple name** only (`:222-225`).
* `stereotype` (repeatable, `PROFILE.NAME`, `PROFILE` = full profile path): entity `content.stereotypes[]` elements `{profile, value}` → `profile + "." + value` must be in the set (`:248-266,300-311`).
* `taggedValue` (repeatable, `PROFILE.TAG/REGEX`): split at first `/`, both sides trimmed; missing `/` or empty regex → regex `""` (matches any value); regex case-insensitive, invalid → literal; multiple regexes for same tag are OR-ed; matches if any `content.taggedValues[]` with `tag:{profile,value}` and string `value` `find()`s (`:113-151,268-298`). Stereotype and taggedValue filters are AND-ed.
* `excludeInvalid` (default **false**): false → any undeserializable entity file fails the whole request (500 from deserialization, see below); true → such files are silently dropped (`EAO:85-101,116-156`).
* Filters are ANDed: path predicate (package+name), classifier, content.

### List order — `EAO:158-171`, `legend-sdlc-project-files/.../CachingFileAccessContext.java:29,38-61,87-95`
Source directories in structure order (v13: `pure` dir first, then `legend` dir). With >1 source dir (v13) the reads go through `CachingFileAccessContext`, whose cache is an Eclipse `UnifiedMap` → **within a directory the order is hash order (unspecified)**. With one dir: GitLab archive order. Clients must not rely on order; lite should pick a stable order (e.g. by path).

### Single entity `GET .../entities/{path}` — `GEA:141-155`, `EAO:51-83`
* For each source directory: file = `entityPathToFilePath(path)`; first existing file wins; deserialized entity path must equal requested path.
* Missing → **404 `"Unknown entity <path> for <refInfo>"`** (`EAO:77-82`) with `<refInfo>` per §0.5 using the **raw** revision id: e.g. `"Unknown entity model::A for user workspace w1 of project PROD-1"`, `"Unknown entity model::A for revision HEAD of project PROD-1"`, `"Unknown entity model::A for project PROD-1"`.
* No path validation: an invalid path just yields the 404 above.
* File present but bad → **500** `Error deserializing entity "<path>" from file "<filePath>": <cause>` (cause e.g. `Expected entity path model::A, found model::B`) (`EAO:60-74`).
* Non-existent workspace: `project.json` read 404 → treated as no config → **v0 structure** (`/entities/...json`) → typically the 404 "Unknown entity" above; for list endpoints, GitLab archive of a missing ref: `"404 File Not Found"` is treated as an empty repo → `[]` 200, other 404 text → 404 `"Unknown user workspace <w> of project <p>"` (`GAFA:215-260`) — depends on GitLab's message.

### Entity JSON — `legend-sdlc-model/.../entity/Entity.java:27-48`
`{"path": "model::A", "classifierPath": "meta::pure::metamodel::type::Class", "content": {...}}` (property order unspecified, §0.1). `content` is a `LinkedHashMap` in source order: for `.json` files the stored (alphabetically sorted) order; for `.pure` files the engine-protocol order (NON_NULL) (`legend-sdlc-protocol/.../ProtocolToEntityConverter.java` `convertContent`).

---

## 6. `POST /projects/{p}/workspaces/{w}/entityChanges` (PerformChangesCommand)

Resource `RES/entity/project/user/WorkspaceEntityChangesResource.java:50-60` → `GEA:229-246` → `EMO:118-163,236-443` → `GAFA:889-940`.

### Body — `legend-sdlc-server/.../application/entity/PerformChangesCommand.java:26-85`, `AbstractEntityChangeCommand.java`
`{"message": string, "revisionId": string|null, "entityChanges": [ {"type": CREATE|DELETE|MODIFY|RENAME, "entityPath", "classifierPath", "content": {...}, "newEntityPath"} ]}`
* `entityChanges` absent → `[]`. `"entityChanges": null` → setter `Lists.mutable.withAll(null)` throws → 400 `"Unable to process JSON"` (`:41-45`).
* Unknown properties anywhere → 400 `"Unable to process JSON"` (0.3). `type` enum is fuzzy (case-insensitive).

### Validation order and messages
1. Body null → **400 `"Input required to perform entity changes"`** (`WorkspaceEntityChangesResource.java:54`).
2. `changes` null → 400 `"changes may not be null"` (`GEA:232`; unreachable via JSON).
3. `message` null → **400 `"message may not be null"`** (`GEA:233`). Empty string `""` is accepted.
4. `validateEntityChanges` (`EMO:139-163`, per-change rules `EMO:236-358`) — collects **all** errors for **all** changes, then one **400**:
   ```
   There are entity change errors:
   \tEntity change #<n> (<change.toString()>):
   \t\t<error>
   \t\t<error>
   \tEntity change #<m> (...):
   \t\t...
   ```
   (`\n` + tab characters, 1-based index.) `change.toString()` = `<EntityChange type=<TYPE|null> entityPath=<p> classifierPath=<c> content=<{...}|null> newEntityPath=<n>>` (`legend-sdlc-model/.../entity/change/EntityChange.java:86-95`); a null element prints `(null)`.
   Per-change error strings, in this order:
   * null element: `Invalid entity change: null` (and stop).
   * `type == null`: `Missing entity change type`.
   * `entityPath == null`: `Missing entity path`; else invalid: `Invalid entity path: <path>`; else if `content != null` and (`content.package` not a String, or `content.name` not a String, or `package + "::" + name != entityPath`): `Mismatch between entity path ("<path>") and package (<pkg>) and name (<name>) properties` where `<pkg>`/`<name>` are `"quoted"` when strings, else raw (`null`, numbers…). (This content check applies to every type, incl. DELETE/RENAME.)
   * CREATE / MODIFY: `Missing classifier path` | `Invalid classifier path: <c>`; `Missing content`; `Unexpected new entity path: <n>`.
   * RENAME: `Unexpected classifier path: <c>`; `Unexpected content`; `Missing new entity path` | `Invalid new entity path: <n>`.
   * DELETE: `Unexpected classifier path: <c>`; `Unexpected content`; `Unexpected new entity path: <n>`.
   * Path rules (`legend-sdlc-model/.../tools/entity/EntityPaths.java:28-123`): entity path = one or more package segments `[A-Za-z0-9_]+` separated by `::`, **at least one package**, final name `[A-Za-z0-9_$]+`, must **not** start with `meta::`. Classifier path = must start with `meta::`, then same grammar (`meta::X` allowed). No check that classifier matches content `_type`.
   * **Not validated**: duplicate entity paths within one request; RENAME target collisions.
5. Zero changes (`[]`) → `EMO:120-125` returns `null` → resource returns null → **204 No Content** (no commit; the stale-revision check is skipped).
6. Invalid project id → 400 `Invalid project id: "<p>"` — note this happens **after** steps 3-5 (`GEA:237` → `getFileAccessContext`).
7. Read `project.json` at `revisionId` (if given, **used verbatim as a git ref — aliases are NOT resolved**) else at workspace HEAD → project structure (`EMO:127-128`).
8. Translate each change to a file operation against that state (`EMO:129,360-443`); first failure aborts with **500** (plain `LegendSDLCException`, no status):
   * CREATE where a file for the path exists in **any** source dir: `Unable to handle operation <change>: entity "<path>" already exists` (`EMO:369-372`).
   * CREATE/MODIFY with no serializer able to take it: `Unable to handle operation <change>: cannot serialize entity "<path>"` (`EMO:375-379,404-408`).
   * DELETE / MODIFY / RENAME of a missing path: `Unable to handle operation <change>: could not find entity "<path>"` (`EMO:385-389,398-402,436`).
   * MODIFY whose serialized bytes equal the current file → no-op (`EMO:413-422`); if **all** ops are no-ops → **204** (`EMO:130-135`).
   * MODIFY whose new serializer differs from the current file's → move between directories with new content (`EMO:413-416`).
   * RENAME → `moveFile(old, new)` **without content rewrite** (`EMO:424-437`); content fetched at GitLab commit time (`GAFA:896-899,986-1026`).
9. Submit (`GAFA:889-931`): if `revisionId != null`, compare with the workspace's **current HEAD**; mismatch → **409**:
   `Expected <desc> to be at revision <revisionId>; instead it was at revision <headSha>`
   with `<desc>` = `getReferenceInfo(p, ws, revisionId)` = `revision <revisionId> of user workspace <w> of project <p>` (`GAFA:916-922,1069-1072`). Full example: `Expected revision abc of user workspace w1 of project PROD-1 to be at revision abc; instead it was at revision def`. (Because step 8 runs first against the *old* revision, a stale request with an invalid change gets the 500 from step 8, not the 409.)
   Missing workspace with a revisionId → 404 `"Unknown: user workspace <w> in project <p>"` from `getCurrentRevision`.
10. `createCommit(branch, message, actions)` on `workspace/<me>/<w>` (`GAFA:924-925`). GitLab failure → 403 `"User <u> is not allowed to perform changes on <desc>: <glmsg>"`, 404 `"Unknown <desc>"`, else **500** `"Failed to perform changes on <desc> (message: <message>): <glmsg>"` (`GAFA:933-939`) — this is where duplicate CREATEs in one request fail (GitLab "A file with this name already exists").
11. Response **200** = the new `Revision` (§4 shape) built from the commit GitLab returned (`GAFA:931`).

### Entity path → file path and serializer choice (project structure v13)
* V13 entity source directories, in this order: `/<artifactId>-entities/src/main/pure` (serializer `pure`, extension `.pure`) then `/<artifactId>-entities/src/main/legend` (serializer `legend`, extension `.json`) — the `pure` one only if `PureEntitySerializer` is on the classpath (`ProjectStructureV13Factory.java:66-67,115-117,369-373`; `maven/MultiModuleMavenProjectStructure.java:532-546`; services `legend-sdlc-entity-serialization` + `legend-sdlc-protocol-pure` `META-INF/services/...EntitySerializer`).
* File path = `<dir>` + `/` + each `::`-segment joined by `/` + `.` + extension (`legend-sdlc-project-structure/.../EntitySourceDirectory.java:92-99`): `model::domain::Person` → `/test-project-entities/src/main/pure/model/domain/Person.pure` or `/test-project-entities/src/main/legend/model/domain/Person.json`.
* Writer choice = **first directory whose serializer `canSerialize`** (`PS:174-177`):
  * `pure` (`legend-sdlc-protocol-pure/.../PureEntitySerializer.java:57-104`): classifier must be a protocol-known classifier (`PureToEntityConverter.isSupportedClassifier`), content must convert to a protocol element, compose to Pure grammar, and **re-parse**; any failure → false.
  * `legend` (`legend-sdlc-entity-serialization/.../DefaultJsonEntitySerializer.java:51-68`): always true. File body = `{"classifierPath": ..., "content": {...}}`, indented, map keys + properties sorted (`:37-43,122-125`); on read, path recomputed from `content.package` + `::` + `content.name` (`:132-152`).
* Lookup for existing entities checks each directory in order (`PS:155-166`).
* v0 (no `project.json`): single directory `/entities`, JSON (`ProjectStructureV0Factory.java:54`).

FS backend: 409 text uses `sourceSpecification.toString()` instead of the reference info (`legend-sdlc-server-fs/.../api/entity/FileSystemApiWithFileAccess.java:286-288`).

---

## 7. Project configuration

Routes: `GET /projects/{p}/configuration` (`RES/project/project/ProjectConfigurationResource.java:49-57`), `GET /projects/{p}/workspaces/{w}/configuration` (`RES/project/project/user/WorkspaceProjectConfigurationResource.java:52-61`) → `GPCA:63-87` → `GAFA:124-128`.
* Reads `/project.json` at the ref; if absent (GitLab 404) returns the **default config** `SimpleProjectConfiguration.newConfiguration(projectId, ProjectStructureVersion(0), null, null, null, null, null)` with **projectType MANAGED** (`PS:361-364`, `SimpleProjectConfiguration.java:206-209`). Because every 404 is "absent", a **non-existent workspace or project also returns 200 with the default config**.
* Served JSON = `SimpleProjectConfiguration` (read with the sorted mapper, `PS:451-461`, then served by the Dropwizard mapper) → creator order (`SimpleProjectConfiguration.java:188-202`), nulls written, verified:
  `{"projectId","projectType","projectStructureVersion":{"version","extensionVersion"},"platformConfigurations","groupId","artifactId","projectDependencies","metamodelDependencies","artifactGenerations","runDependencyTests","produceShadedServiceJar"}`
  e.g. `{"projectId":"PROD-1","projectType":"MANAGED","projectStructureVersion":{"version":13,"extensionVersion":null},"platformConfigurations":null,"groupId":"org.finos.test","artifactId":"test-project","projectDependencies":[],"metamodelDependencies":[],"artifactGenerations":[],"runDependencyTests":null,"produceShadedServiceJar":null}`.
  Default config: `projectStructureVersion` is the anonymous class (`ProjectStructureVersion.java:105-120`) → order unspecified (observed `{"extensionVersion":null,"version":0}`), `groupId`/`artifactId` null, lists `[]`.
* `projectId` in the served JSON is whatever `project.json` contains (not re-derived).
* Errors: invalid project id 400; other GitLab errors 403 `"User <u> is not allowed to access project configuration for <refInfo>: <glmsg>"`, 500 `"Failed to access project configuration for <refInfo>: <glmsg>"` (`GPCA:74-80`); unparseable project.json → 500 `"Failed to access project configuration for <refInfo>: Error reading project configuration: <jackson msg>"` (`PS:451-461`).

---

## 8. Error envelope

See §0.2–0.4. Summary for porting:
* Always `Content-Type: application/json`, body `{"code":<int>,"message":<string>[,"details":<string>][,"stackTrace":<string>],"timestamp":"<ISO-8601 instant>"}` in exactly that key order, nulls omitted.
* `code` equals HTTP status (except the documented 3xx/2xx→500 rewrites).
* Unknown route → 404 `"HTTP 404 Not Found"`; wrong verb → 405 `"HTTP 405 Method Not Allowed"`; bad query-param conversion → 404; bad JSON body → 400 `"Unable to process JSON"` + `details`.

## 9. `GET /configuration/latestProjectStructureVersion`
* `RES/project/ConfigurationResource.java:59-65` → `GPCA:178-182`: `ProjectStructureVersion.newProjectStructureVersion(13, extensionProvider.getLatestVersionForProjectStructureVersion(13))`.
* Default (Void provider) → `{"version":13,"extensionVersion":null}` (anonymous class → key order unspecified; observed `extensionVersion` first). No auth/IO, no `execute` wrapper.
* FS: same API shape.

---

## Upstream bugs / quirks found in these paths

1. **Workspace create is silently idempotent** when the branch already sits at default-branch HEAD (200), but **500** ("Error creating user workspace … : Branch already exists") when it exists elsewhere — never 409 (`GAT:267-289`, `GWA:404-415`).
2. **Dead "Project structure has not been set up" 409**: `getProjectConfiguration` never returns null and reads the not-yet-created workspace branch (`GWA:340-343`, `GAFA:124-128`).
3. **DELETE workspace returns 204 for missing workspaces and missing projects** (any GitLab 404 = success) (`GAT:247-257`).
4. `GET .../workspaces?owned=false` also returns conflict-resolution and backup branches as plain workspaces (`WorkspaceApi.java:204-208`, `GWA:129`).
5. **GET configuration on a non-existent project/workspace → 200 default config** (v0, MANAGED) instead of 404 (`GAFA:124-128`).
6. **Entity reads with a literal revision id do no existence/branch check** — any SHA in the repo is readable via any workspace URL (`GAFA:169-193`); only `/revisions/{r}` validates.
7. **PerformChanges `revisionId` is not alias-resolved**: `"HEAD"` is used as a raw git ref and always fails the 409 check (`EMO:127,136`, `GAFA:916-922`).
8. Stale-revision check runs **after** file-op computation against the stale state, so a stale request can return 500 ("could not find entity …") instead of 409 (`EMO:129` vs `GAFA:910-923`); and is skipped entirely when all ops are no-ops (204).
9. CREATE-of-existing / MODIFY-DELETE-RENAME-of-missing return **500** (status-less `LegendSDLCException`) rather than 409/404 (`EMO:371,378,388,401,407,436`).
10. No duplicate-path detection in a single entityChanges request; collisions surface as GitLab commit errors (500) (`EMO:139-163`).
11. **RENAME moves the file but does not rewrite `package`/`name` in content** → the renamed entity then fails reads ("Expected entity path X, found Y", 500) (`EMO:424-437`).
12. Message typos: `"Failed to resolve  revision …"` (double space, `GAFA:190`); `"Unknown revision revision HEAD of …"` (doubled word, `GAFA:185`); `"Error accessing revision <rev>for …"` (missing space, `GAFA:699`); `"…workspace <id>in project…"` (missing space, `PSU:124`).
13. Two different reference-info phrasings: `"… of project P"` (`BGA:761-799`) vs `"… in project P"` (`GAFA:821-872`) for the same workspace.
14. Entity list order within a v13 source directory is `UnifiedMap` hash order (`CachingFileAccessContext.java:29,38-61`).
15. JSON key order of `Revision`/`Workspace`/`User`/`Entity`/anonymous `ProjectStructureVersion` depends on JVM `getDeclaredMethods()` order (no `@JsonPropertyOrder`, no alphabetical sort).
16. `createProject` leaves an orphan GitLab project if structure setup fails (`GPA:287-313`); FS backend validates groupId/artifactId only after creating the repo.
17. `GET /projects?limit=0` returns `[]` without contacting GitLab; Swagger says non-positive means "no filtering" but negative → 400 (`GPA:118-129`, `ProjectsResource.java:79`). `type` query param ignored.
18. Tag filters are case-sensitive while the marker-tag check is case-insensitive (`GPA:146-161` vs `:1046-1050`).
19. `features` section absent ⇒ `canCreateProject=false` ⇒ `POST /projects` is 405 by default (`AbstractBaseModule.java:623-627`).
20. `"entityChanges": null` in the body is a 400 JSON-processing error rather than a validation message (`PerformChangesCommand.java:41-45`).
