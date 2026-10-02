# Legend Studio, SDLC and Depot: upstream census for a lite rebuild

Read 2026-10-01/02 from source, not docs, to build a lite Studio (a model **writer**) with its own
SDLC-lite and Depot-lite model home, the way `query/docs/UPSTREAM_QUERY_CENSUS.md` was read for Query.
Nothing was run against a live upstream deployment; where behaviour depends on deployment config
rather than code, the part says so.

**Sources (read-only checkouts):**

| Repo | Commit | Where it was read |
|---|---|---|
| finos/legend-studio | `821c74c` (2026-09-30) | `.scratch/legend-studio` |
| finos/legend-sdlc | `1021fda` (2026-10-01, shallow) | `.scratch/legend-sdlc` |
| finos/legend-depot | `9c0a809` (2026-09-30, shallow) | `.scratch/legend-depot` |
| finos/legend-engine | `230c159196d` (2026-09-09) | `~/legend/legend-engine` |
| legend-lite | `19357c167` (2026-10-01) | this repo |
| studio-lite (retired) | `292962f` (2026-04-13) | `~/legend/studio-lite` |

Each part keeps its own path prefixes and section numbers (stated at its top). Importance tags:
**C** core (the first "author → save → publish → read by name" loop), **I** important, **N** niche.

| Part | Covers |
|---|---|
| A | Studio's routes and the workspace/project lifecycle as a user lives it, with every SDLC and Depot call |
| B | The editing experience (layout, explorer, form vs text mode, compile, diagnostics, element editors, running and testing) and the full engine endpoint inventory |
| C | The SDLC server: domain model, the complete REST API (459 routes), project file layout, versioning, the GitLab and file-system backends, and upstream's own minimal-backend contract |
| D | Publishing and Depot: SDLC version → Maven artifacts → Depot ingestion, the complete Depot API, aliases and caching, storage |
| E | How the engine receives models: every `PureModelContext` and `sdlcInfo` shape, pointer resolution (URLs, caching, auth), and how Studio, Query and DataCube build what they send |
| F | What legend-lite already has toward this, the constraints a design must respect, and the gaps |

---

## Cross-check of the parts

Each part was read independently; these are the places they meet, checked against each other and,
where marked, against the source again.

| Question | Parts | Result |
|---|---|---|
| How Studio saves | A §4.3, C §1.8/§2.2 | Agree: `POST …/{ws}/entityChanges {message, entityChanges[CREATE\|MODIFY\|DELETE], revisionId}`; `revisionId` is an optimistic lock, 409 on mismatch. Re-checked at `legend-server-sdlc/src/SDLCServerClient.ts:956-965`. |
| What lands a workspace on the project line | A §4.7, C §2.3/§5.1 | Agree: only a review commit (GitLab: merge request accept, workspace branch deleted). A lite SDLC without reviews still needs *some* landing path. |
| How a version is cut | A §4.9, C §4 | Agree: `POST /projects/{p}/versions {versionType, revisionId, notes}`; the server computes the number; always from project HEAD in the UI; a git tag `release-M.m.p` in GitLab. |
| How a version reaches Depot | C §4, D §1 | Agree: **not** in OSS SDLC — deployment CI builds the Maven artifacts and calls Depot's `/queue`. A lite model home must own publishing itself. |
| What an entity is | B §8.4, C §1.1/§3.2, D §2.1, E §5.1 | Agree: `{path, classifierPath, content}`; `content` is the element's protocol JSON with `package` and `name`; `_type` in `content` decides the class, `classifierPath` is a hint; JSON entity files store no path. |
| How Studio sends the model to the engine | B §0/§4.2, E §4.1-4.2 | Agree: always a full `data` context (own elements + dependency entity contents), never a workspace pointer; Query/DataCube send `alloy` pointers. |
| The engine's one Depot call | D §4.2(c), E §2.3 | Agree: `GET {alloy}/projects/{g}/{a}/versions/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=…`. Re-checked at `AlloySDLCLoader.java:50`. |
| What that response must look like | D §3.1 #21, E §2.7/§6.1 | Agree: `serializer` present; `origin` present with `sdlcInfo.version` `"none"` and the real version in `baseVersion`; elements are raw entity `content`. |
| Cross-project classifier search | D §0.7/§7.1 | Studio and Query call `GET /classifiers/{path}` and `/classifiers/{path}/entities`; **absent** in depot `9c0a809` (only per-version `…/versions/{v}/classifiers/{c}` exists, re-checked at `EntitiesResource.java:72`). |
| Dependency request body | A §7.2, D §2.1 | D left it open. Resolved by reading `ArtifactDependency.java:35-42`: the JSON creator names the key `version`, the field is `versionId` with a getter, and Studio sends `versionId` on every workspace load in production — so a lite Depot must accept **both** keys. |
| Compile trigger | B §4.1 | Explicit only (F9 and a few actions), never on keystroke or save. Re-checked: `LegendStudioCommand.ts:60-62`, registered at `EditorStore.ts:453`. |
| lite's model reader and LSP | F §1-2 | Re-checked: `PureV1Api.java:493-500` refuses every context but `text` ("the PMCD reader is not built"); `PureLspServer.java:44-50` handles only initialize, shutdown and the three document-sync messages. |
| SDLC's minimal backend | C §5.3 | Re-checked in `docs/re-architecture.md:133-145`: storage SPI + project lifecycle + workspace lifecycle required; entities, configuration, dependencies and comparison from shared defaults; reviews, versions, patches, workflows, builds, backup, issues and conflict resolution optional. |

**Left open (deployment config or not in any repo read):** whether an SDLC or Depot client gets a
bearer token outside OIDC deployments (A §9, E §open); where the real release deploy and the Depot
`/queue` call are triggered (C, D); the `codeCompletion/completeCode` and `lambda/v1/lambdaPrefixes`
endpoints, called by the Studio client but absent from engine `230c159` (B §9); the history of
Depot's `/classifiers/*` endpoints (shallow clone).

---


---

<!-- Part A -->
# Part A — Studio application shell and the workspace / project lifecycle

Upstream sources read:
- legend-studio @ `821c74c`, root `/Users/neema/legend/legend-lite-query/.scratch/legend-studio/packages`
- legend-sdlc @ `1021fda`, root `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc` (used only to spot-check that client paths exist; the SDLC server API itself is covered by another census file)

Path prefixes used in citations:
- `LS/` = `legend-application-studio/src/`
- `SDLC-C/` = `legend-server-sdlc/src/`
- `DEP-C/` = `legend-server-depot/src/`
- `LG/` = `legend-graph/src/`
- `LA/` = `legend-application/src/`
- `EXT-SVC/` = `legend-extension-dsl-service/src/`
- `SDLC-S/` = `legend-sdlc/legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/`

Importance tags: **C** = core (needed for the minimal "author models, save, publish a version" loop), **I** = important (normal day-to-day use), **N** = niche.

---

## 0. Summary: the minimal core loop as Studio implements it

1. **Boot**: the app reads `config.json` (`sdlc.url`, `depot.url`, `engine.url`, ...). It checks `GET {sdlc}/auth/authorized`. If that is false it redirects the whole page to `{sdlc}/auth/authorize?redirect_uri=<current url>`. It then checks `GET /auth/termsOfServiceAcceptance`, `GET /server/platforms`, `GET /server/features` and `GET /currentUser` (LS/stores/LegendStudioBaseStore.ts:172-271, 577-654).
2. **Setup page** (`/` or `/setup/...`): the user searches projects (`GET /projects?search=&excludeTag=sandbox&limit=30`) and picks one. Studio then loads patches, configuration status, conflict-resolution workspaces and workspaces (user plus group). The user picks a workspace or creates one (`POST /projects/{p}/[group]workspaces/{w}`) and clicks Go, which navigates to `/edit/{p}/{w}/` (LS/stores/workspace-setup/WorkspaceSetupStore.ts:529-599, 713-843, 975-1040).
3. **Open workspace** (`/edit/...`): Studio fetches the project, patch, workspace, conflict-resolution flag and current revision, and initializes the engine. It then fetches the workspace configuration and the workspace entities, fetches dependency entities from **Depot** (`POST /projects/dependenciesFromArtifactDependencies`), builds the graph **in the browser** (TypeScript graph builder) and computes hash indexes for change detection: workspace HEAD, workspace BASE, project HEAD (LS/stores/editor/EditorStore.ts:646-1226).
4. **Edit**: change detection is entirely client-side. It compares element hash codes against hash indexes built from SDLC entities (LS/stores/editor/ChangeDetectionState.ts:484-660).
5. **Save** (Ctrl+S, "Push local changes"): Studio computes a CREATE/MODIFY/DELETE `EntityChange[]` list and sends `POST /projects/{p}/[group]workspaces/{w}/entityChanges` with `{message, entityChanges, revisionId}`. `revisionId` acts as an optimistic-concurrency token (HTTP 409 means the workspace is stale). There is **no commit-message UI**: the message is always auto-generated (LS/stores/editor/sidebar-state/LocalChangesState.ts:338-583).
6. **Review/merge**: `POST /projects/{p}/reviews {workspaceId, workspaceType, title, description}`, then `POST /projects/{p}/reviews/{r}/commit {message}`. Committing deletes the workspace on the server, and Studio offers to recreate it (LS/stores/editor/sidebar-state/WorkspaceReviewState.ts:343-543).
7. **Publish version**: from the Project Overview "Release" tab: `POST /projects/{p}/versions {versionType: MAJOR|MINOR|PATCH, revisionId: <project HEAD revision>, notes}`. This is gated by `server/features.canCreateVersion` and by the `CREATE_VERSION` authorized action (LS/stores/editor/sidebar-state/ProjectOverviewState.ts:389-438; LS/components/editor/side-bar/ProjectOverview.tsx:330-356).

A minimal rebuild needs these SDLC endpoints: auth/authorized, currentUser, server/features, server/platforms, projects (list, get, create), workspaces (list, get, create), workspace inConflictResolutionMode, outdated, revisions/CURRENT and BASE, entities (workspace, revision, project), configuration (workspace), entityChanges, reviews (list, create, commit), versions (list, latest, create), and configuration/latestProjectStructureVersion. It needs these Depot endpoints: dependenciesFromArtifactDependencies, plus project-configurations and versions for dependency picking. It needs the engine for compile. Everything else is I or N.

---

## 1. Application shell, routes and deep links

### 1.1 Shell bootstrap

| Step | Where | What |
|---|---|---|
| OIDC wrapper (optional) | LS/components/LegendStudioWebApplication.tsx:342-382 | If `extensions.core.ingestDeploymentConfig.deployment.oidcConfig` is present, the app is wrapped in `react-oidc-context` `AuthProvider` plus `LegendTokenSync`. If `enableOauthFlow` is also set, it additionally uses `withAuthenticationRequired` (:328-340). Otherwise there is no OIDC. |
| Base store init | LS/stores/LegendStudioBaseStore.ts:172-271 | Creates `DepotServerClient` (:149) and `SDLCServerClient` (:157-168). Unless the URL matches an SDLC-bypassed pattern (:193-207), it runs `initializeSDLCServerClient`, then `getCurrentUser`. If identity is still anonymous, it calls `getCurrentUserIDFromEngineServer(engineServerUrl)` (:241-256). |
| SDLC authorization | LegendStudioBaseStore.ts:639-694 | `isAuthorized()`. If false, it does a top-level `goToAddress({sdlc}/auth/authorize?redirect_uri=<currentAddress>[&client_name=])`. If true, `bootstrapSDLCSessionAfterAuth` (:577-616) runs: ToS check (alert with links), `fetchServerPlatforms`, `fetchServerFeaturesConfiguration`. In dev mode, a 401 shows an "Authenticate using SDLC" alert that opens `{sdlc}/currentUser`. |
| Popup re-auth (opt-in) | LegendStudioBaseStore.ts:289-568 | When `sdlc.enablePopupReAuth` is set, any mid-session 401 from SDLC auto-opens a popup to `/auth/authorize?redirect_uri=<origin><base>/popup-callback.html`. The popup posts `{type:'SDLC_REAUTH_DONE'}` back. There is one auto attempt per failure episode, and a manual shield button in the StatusBar (LS/components/editor/StatusBar.tsx:400-410). |
| Router gating | LegendStudioWebApplication.tsx:144-309 | Nothing renders until `initState.hasCompleted`. If `isSDLCAuthorized === undefined` (a bypassed route), only the bypassed routes are registered. If it is `true`, the full route table is registered. If it is `false`, nothing renders because the page has already been redirected. |

### 1.2 Route table

Patterns are defined at LS/__lib__/LegendStudioNavigation.ts:39-70 and registered at LS/components/LegendStudioWebApplication.tsx:148-306.

| Route pattern | Component / store | What | Imp. |
|---|---|---|---|
| `''`, `/` | `WorkspaceSetup` / `WorkspaceSetupStore` | Home and setup page (:250-259) | C |
| `/setup/:projectId?/:workspaceId?` | WorkspaceSetup | Setup with a preselected project or user workspace | C |
| `/setup/:projectId/groupWorkspace/:groupWorkspaceId/` | WorkspaceSetup | Setup with a preselected group workspace | I |
| `/setup/:projectId/patches/:patchReleaseVersionId?/:workspaceId?` | WorkspaceSetup | Patch variant. **The patch id is ignored by the setup store**: `initialize(projectId, workspaceId, groupWorkspaceId)` (LS/components/workspace-setup/WorkspaceSetup.tsx:390-394, 597-600) | N |
| `/setup/:projectId/patches/:patchReleaseVersionId?/groupWorkspace/:groupWorkspaceId/` | WorkspaceSetup | Same as above | N |
| `/edit/:projectId/:workspaceId/` | `Editor` / `EditorStore` (STANDARD mode) | Edit a user workspace | C |
| `/edit/:projectId/:workspaceId/entity/:entityPath` | Editor | Deep link to an element. The path is "internalized" and the URL is rewritten back to the workspace route (LS/stores/editor/EditorStore.ts:614-639) | I |
| `/edit/:projectId/groupWorkspace/:groupWorkspaceId/` | Editor | Edit a group workspace | C |
| `/edit/:projectId/groupWorkspace/:groupWorkspaceId/entity/:entityPath` | Editor | Deep link | I |
| `/edit/:projectId/patches/:patchReleaseVersionId/:workspaceId/` (+`/entity/:entityPath`) | Editor | User workspace on a patch branch | N |
| `/edit/:projectId/patches/:patchReleaseVersionId/groupWorkspace/:groupWorkspaceId/` (+entity) | Editor | Group workspace on a patch branch | N |
| `/text/:projectId/:workspaceId/` | `LazyTextEditor` / `LazyTextEditorStore` (LAZY_TEXT_EDITOR mode) | "Strict Text Mode (BETA)": a text-only editor that skips the full graph build (LS/stores/lazy-text-editor/LazyTextEditorStore.ts:39-55; EditorStore.ts:969-1061) | N |
| `/text/:projectId/groupWorkspace/:groupWorkspaceId/` | LazyTextEditor | Same, for a group workspace | N |
| `/view/:projectId` | `ProjectViewer` / `ProjectViewerStore` | Read-only view of project HEAD | I |
| `/view/:projectId/entity/:entityPath` | ProjectViewer | Deep link | I |
| `/view/:projectId/version/:versionId` (+`/entity/:entityPath`) | ProjectViewer | Read-only view of a released version | I |
| `/view/:projectId/revision/:revisionId` (+`/entity/:entityPath`) | ProjectViewer | Read-only view of a revision | N |
| `/review/:projectId/:reviewId` | `ProjectReviewer` / `ProjectReviewerStore` | Review page (diff, approve, commit, close, reopen) | I |
| `/review/:projectId/patches/:patchReleaseVersionId/:reviewId` (`PATCH_REVIEW`) | **not registered** | Defined at :47 but there is no `<Route>`, and `ProjectReviewerStore.currentPatch` is never set (LS/stores/project-reviewer/ProjectReviewerStore.ts:121,176-178). Patch reviews cannot be opened in Studio. | N |
| `/view/archive/:gav` (SDLC-bypassed) | ProjectViewer (GAV mode) | Read-only view of a Depot artifact (`group:artifact:version`) without SDLC auth (:150-158) | I |
| `/view/archive/:gav/entity/:entityPath` (SDLC-bypassed) | ProjectViewer | Deep link. This is the target of "view dependency element" links from the explorer (LS/stores/editor/StandardEditorMode.ts:69-81) | I |
| `/view/archive/:gav/entity/:entityPath/preview` (`PREVIEW_BY_GAV_ENTITY`) | **not registered** | It is in the bypass list (LegendStudioBaseStore.ts:201) but has no `<Route>`, so it falls through to 404. `generateElementPreviewRoute` is unused. | N |
| `/showcase/:showcasePath` (bypassed) | `ShowcaseViewer` | Showcase projects (needs `showcase.url`) | N |
| `/pct` (bypassed) | `PureCompatibilityTestManager` | PCT report (needs `pct.reportUrl`) | N |
| `/extensions/<pattern>` | Plugin page entries (`getExtraApplicationPageEntries`) wrapped in `ExtensionPageBoundary` (:287-304). The prefix comes from `generateExtensionUrlPattern` (LA/stores/navigation/BrowserNavigator.ts:70-71) | See below | N/I |
| `/extensions/update-service-query/:serviceCoordinates?` | EXT-SVC UpdateServiceQuerySetup | Pick a service to edit its query | N |
| `/extensions/update-service-query/:serviceCoordinates/:groupWorkspaceId` | ServiceQueryUpdater | Edit a service query in a group workspace | N |
| `/extensions/update-project-service-query/:projectId?` and `/:projectId/:groupWorkspaceId/:servicePath` | UpdateProjectServiceQuerySetup / ProjectServiceQueryUpdater | Same, keyed by project | N |
| `/extensions/productionize-query/:queryId?` | QueryProductionizer | Turn a Legend Query into a service in a new group workspace (EXT-SVC/__lib__/studio/DSL_Service_LegendStudioNavigation.ts:35-41) | N |
| `/extensions/promote-template-query/:gav/:dataSpacePath/:queryId?` | DataSpaceTemplateQueryPromotionReviewer | Promote a query to a curated data-space template | N |
| `/extensions/depot` | Depot dashboard extension | Depot dashboard | N |
| `*` | `NotFoundPage` | 404 page with an optional doc entry | - |

Query parameter: `?editorConfig=<base64 JSON>` (`LEGEND_STUDIO_QUERY_PARAMS.EDITOR_CONFIG`, LegendStudioNavigation.ts:22-24). It is decoded into `EditorInitialConfiguration {elementEditorConfiguration?, engineServerUrl?, engineQueryServerUrl?}` (LS/stores/editor/editor-state/element-editor-state/ElementEditorInitialConfiguration.ts:106-120; EditorStore.ts:586-612). It **can override the engine URL** used by the editor (EditorStore.ts:895-900). Ingest and DataProduct editors use it with `deployOnOpen`.

### 1.3 URL generators

All are in LS/__lib__/LegendStudioNavigation.ts.

| Generator | Line | Notes |
|---|---|---|
| `generateSetupRoute(projectId, patchReleaseVersionId, workspaceId?, workspaceType?)` | 151-167 | Chooses between the group and user variants |
| `generateEditorRoute(projectId, patchReleaseVersionId, workspaceId, workspaceType, entityPath?)` | 232-251 | 8 variants |
| `generateReviewRoute(projectId, reviewId)` | 253-260 | Used by ProjectOverview, WorkspaceUpdater and WorkspaceReview |
| `generateViewProjectRoute`, `generateViewEntityRoute`, `generateViewVersionRoute`, `generateViewRevisionRoute` | 262-306 | |
| `generateViewProjectByGAVRoute(g, a, v, entityPath?)` | 308-324 | Uses `generateGAVCoordinates` from legend-storage |
| `generateElementPreviewRoute` | 326-338 | Unused, and its route is not registered |
| `EXTERNAL_APPLICATION_NAVIGATION__generateServiceQueryCreatorUrl(queryAppUrl, g, a, v, servicePath)` | 343-354 | Builds `{query.url}/create-from-service/{gav}/{servicePath}`, a hard-coded Legend Query route |
| `generateShowcasePath` | 356-359 | |
| `EXTERNAL_APPLICATION_NAVIGATION__generateUrlWithEditorConfig(base, cfg)` | 361-365 | |

### 1.4 Navigation guard and commands

- Leaving the editor is blocked when `isInConflictResolutionMode || localChangesState.hasUnpushedChanges`. The confirm dialog reads "You have unpushed changes. Leave anyway?" (LS/components/editor/Editor.tsx:177-213).
- Editor commands are registered at EditorStore.ts:451-556. They include COMPILE (F9), GENERATE (F10), TOGGLE_TEXT_MODE (F8) and SYNC_WITH_WORKSPACE (`Control+KeyS`, LS/__lib__/LegendStudioCommand.ts:36-39). SYNC_WITH_WORKSPACE runs `localChangesState.pushLocalChanges()` (EditorStore.ts:522-530). The other commands toggle the sidebars: Explorer, Local Changes, Review, Updater.
- Activity bar entries (LS/components/editor/ActivityBar.tsx:389-503): Explorer, Test Runner, Local Changes, Update Workspace, Review, Conflict Resolution, Project (overview), Workflow Manager, Dev Mode (Beta), Register Service (Beta), End to End Workflows (Beta).

---

## 2. Workspace setup (home page)

The store is `WorkspaceSetupStore` (LS/stores/workspace-setup/WorkspaceSetupStore.ts). The components are in LS/components/workspace-setup/.

### 2.1 Page load

1. `initialize(projectId, workspaceId, groupWorkspaceId)` (:394-449). If a projectId is in the URL, it calls `GET /projects/{id}`. On failure it removes the project from recents and resets the URL to `/setup` (:416-423). On success it calls `changeProject(project, {workspaceId, workspaceType})`.
2. In parallel (WorkspaceSetup.tsx:609-616):
   - `loadProjects('')` sends `GET /projects?excludeTag=sandbox&limit=30`. `user` and `search` are undefined (:557-569).
   - `loadSandboxProject()` runs only when `TEMPORARY__enableCreationOfSandboxProjects` is set.
3. **Recents** are stored in local storage. User-data key `studio-editor.workspace-setup.recents` holds at most 10 projects and 20 workspaces (LS/__lib__/LegendStudioUserDataHelper.ts:41,57-59). An entry is recorded only after the editor has confirmed that both the project and the workspace exist (EditorStore.ts:854-883). Patch workspaces are never recorded.

### 2.2 Project search / listing

- `loadProjects(searchText)` (:529-599) works as follows:
  - Results are cached per search string, with prefix narrowing (:483-527).
  - Text is only sent to the server once it is longer than `DEFAULT_TYPEAHEAD_SEARCH_MINIMUM_SEARCH_LENGTH` (:549-552).
  - It runs two calls in parallel:
    - `GET /projects?search="<text>"&excludeTag=sandbox&limit=30`. The search string is wrapped in quotes by `exactSearch` from legend-shared `search/AdvancedSearch.ts:17`.
    - `GET /projects/{cleanedId}` if the text parses as a project identifier. The identifier gets the default prefix `PROD-` if no prefix was typed (:76-85).
  - A sync counter discards out-of-order responses.
- Sandbox project (N): see 2.6.

### 2.3 Selecting a project: `changeProject` (:713-843)

1. `GET /projects/{p}/patches`. This is skipped for sandbox projects (tag `sandbox`, SDLC-C/util/ProjectUtil.ts:17-19).
2. `GET /projects/{p}/configuration/projectConfigurationStatus` returns `{projectConfigured, reviewIds[]}`. If `reviewIds` is non-empty, Studio calls `GET /projects/{p}/reviews/{reviewIds[0]}` to get `webURL` (LS/stores/workspace-setup/ProjectConfigurationStatus.ts:32-64). If the project is not configured, a warning toast appears and the "Go" button is disabled (WorkspaceSetup.tsx:568-575, 770-778). This call is also skipped for sandbox projects.
3. `GET /projects/{p}/conflictResolution` returns workspaces in conflict resolution. These are **excluded** from the workspace list. Note that the client ignores the patch arg (SDLC-C/SDLCServerClient.ts:1184-1188).
4. `GET /projects/{p}/workspaces` and `GET /projects/{p}/groupWorkspaces`, in parallel, flattened (SDLCServerClient.ts:467-476).
5. For **each** patch: `GET /projects/{p}/patches/{v}/workspaces` and `.../groupWorkspaces`. Studio stamps `workspace.source = patchVersion` (:784-806). This is N+1 calls.
6. If the URL named a workspace that no longer exists, it is pruned from recents.

Workspace type is **inferred from the JSON**. `Workspace.workspaceType` returns USER if `userId` is set, otherwise GROUP (SDLC-C/models/workspace/Workspace.ts). The server does not return a type field.

### 2.4 Create project (C, for a fresh install)

Implemented in LS/components/workspace-setup/CreateProjectModal.tsx and WorkspaceSetupStore.ts:862-915.

- The modal has two tabs: "Create New Project" and "Import Project". The Create tab is the default only if `server/features.canCreateProject` is true (CreateProjectModal.tsx:643-652).
- Fields:
  - Project Name (required)
  - Description (optional)
  - Group ID (required). The placeholder is `extensions.core.projectCreationGroupIdSuggestion`, default `org.finos.legend.*` (LS/application/LegendStudioApplicationConfig.ts:87).
  - Artifact ID (required). Validated client-side with `^[a-z][a-z\d_]*(?:-[a-z][a-z\d_]*)*$` (CreateProjectModal.tsx:49,63,214).
  - Tags (free list)
- **There is no project-type field.** MANAGED or EMBEDDED is toggled later in the configuration editor's Advanced tab (see 5.5). There is also no project-structure-version field.
- Call: `POST /projects` with `{name, description, groupId, artifactId, tags}` (SDLC-C/models/project/ProjectCommands.ts `CreateProjectCommand`) returns a `Project`. Studio then clears the search cache and runs `changeProject(created)`.

### 2.5 Import project (N)

Implemented in CreateProjectModal.tsx:340-640 and WorkspaceSetupStore.ts:917-973.

- Fields: project identifier (an existing GitLab project id, placeholder `1234`), groupId, artifactId. Description and tags are also collected but are **ignored**: the command only carries `{id, groupId, artifactId}` (:930-934).
- Calls `POST /projects/import`, which returns `ImportReport {project, reviewId}`. Studio then calls `GET /projects/{p}/reviews/{reviewId}` and shows a "Review" button that opens `review.webURL` (the GitLab merge request that sets up the project structure).

### 2.6 Sandbox projects (N; requires `TEMPORARY__enableCreationOfSandboxProjects`)

- The store constructor initializes an engine client without graph configuration (WorkspaceSetupStore.ts:189-193, 451-481).
- `loadSandboxProject` (:601-711):
  - It has a cached fast path per user.
  - Slow path: `GET /projects?search=<userId>&tag=sandbox&limit=1`. Note that the userId is passed in the **search** slot (:659-666).
  - Then the engine call `GET {engine}/sdlc/v1/userHasPrototypeProjectAccess/{userId}` (LG/.../V1_EngineServerClient.ts:392-394).
- `createSandboxProject` (:311-392):
  - The engine `createPrototypeProject` call (`POST {engine}/sdlc/v1/createPrototypeProject`, V1_EngineServerClient.ts:378-384) creates the project.
  - Studio then reloads the sandbox and runs `POST /projects/{p}/groupWorkspaces/myWorkspace`.
- Sandbox projects cannot create reviews (WorkspaceReviewState.ts:359-364).

### 2.7 Create workspace (C)

Implemented in LS/components/workspace-setup/CreateWorkspaceModal.tsx and WorkspaceSetupStore.ts:975-1040.

- Fields:
  - Workspace Name
  - Patch selector (lists `patch/<x.y.z>` for each patch)
  - "Group Workspace" toggle, which **defaults to true** (CreateWorkspaceModal.tsx:52)
- Name collision is checked client-side against loaded workspaces of the same type and source (:83-95).
- Calls `POST /projects/{p}/[patches/{v}/](workspaces|groupWorkspaces)/{workspaceId}` with an empty body (SDLCServerClient.ts:508-522). It returns a `Workspace`, and Studio selects it in the dropdown.
- "Go" navigates to `generateEditorRoute(projectId, ws.source, ws.workspaceId, ws.workspaceType)` (WorkspaceSetup.tsx:584-595).
- Other places that create workspaces:
  - The editor's "Workspace not found" dialog has a "Create workspace" action (EditorStore.ts:771-815).
  - "Recreate workspace" after a review commit (WorkspaceReviewState.ts:258-288).
  - Patch creation (ProjectOverviewState.ts:522-529).
  - The extensions (QueryProductionizer, ServiceQueryEditor, UpdateServiceQuerySetup, UpdateProjectServiceQuerySetup, DataSpaceTemplateQueryPromotion) all create **GROUP** workspaces.
- Authorization: `canCreateWorkspace` (`CREATE_WORKSPACE` authorized action) exists in `EditorSDLCState` (LS/stores/editor/EditorSDLCState.ts:170-174). The setup page does **not** check it.

### 2.8 Patch workspaces (N)

A patch is a release branch cut from an existing version. To create one, use Project Overview > Patch tab:

1. `POST /projects/{p}/patches` with the **raw version string** as the body (e.g. `"1.2.0"`). This returns a `Patch {projectId, patchReleaseVersionId: VersionId}`. The server parses the string with `VersionId.parseVersionId` (SDLC-S/resources/patch/PatchesResource.java:54-66).
2. Immediately `POST /projects/{p}/patches/{v}/(group)workspaces/{name}`.
3. Navigate to `/edit/{p}/patches/{v}/...` (ProjectOverviewState.ts:486-562).

Releasing a patch: `POST /projects/{p}/patches/{v}/release` returns a `Version` (ProjectOverviewState.ts:440-484).

---

## 3. Opening a workspace (editor load sequence)

The entry point is `Editor` (LS/components/editor/Editor.tsx:152-175). It calls `editorStore.internalizeEntityPath(params, studioParams)` and `editorStore.initialize(projectId, patchReleaseVersionId, workspaceId, workspaceType, studioParams)`. The workspace type is derived from which route param is present (Editor.tsx:68-74).

### 3.1 `EditorStore.initialize` (LS/stores/editor/EditorStore.ts:646-918)

The steps run in order. "||" marks calls that run in parallel.

| # | Call | Endpoint | Notes |
|---|---|---|---|
| 1 | `sdlcState.fetchCurrentProject` (EditorSDLCState.ts:231-256) | `GET /projects/{p}` | On failure, Studio shows a "Project not found or inaccessible" alert with "Reload" and "Back to setup" actions, and removes the project from recents (:700-738). |
| 2 | `sdlcState.fetchCurrentPatch` (:258-284) | `GET /projects/{p}/patches/{v}` | Only for patch workspaces. |
| 3 | `sdlcState.fetchCurrentWorkspace` (:286-324) | `GET /projects/{p}/[patches/{v}/](workspaces\|groupWorkspaces)/{w}` then `GET .../{w}/inConflictResolutionMode` | If the second call returns true, Studio sets `mode = CONFLICT_RESOLUTION` and `workspace.accessType = CONFLICT_RESOLUTION`, and opens the Conflict Resolution sidebar. If the workspace is missing, Studio shows a "Workspace not found" alert with three actions: View project (`/view/{p}`), Create workspace, and Back to setup (:755-853). |
| 4 | Record recents | local storage | Non-patch workspaces only. |
| 5 | `fetchCurrentRevision` (:384-412) \|\| `graphManager.initialize(...)` | `GET .../{w}/revisions/CURRENT` (or `.../{w}/conflictResolution/revisions/CURRENT` in CR mode) \|\| engine `GET /server/v1/currentUser`, `GET /pure/v1/protocol/pure/getClassifierPathMap`, `GET /pure/v1/protocol/pure/getSubtypeInfo` (LG/graph-manager/protocol/pure/v1/V1_PureGraphManager.ts:704-766; LG/.../V1_RemoteEngine.ts:277-287, 306-320) | The current revision is stored as both `currentRevision` and `remoteWorkspaceRevision`. The engine base URL can be overridden by `editorConfig`. |
| 6 | `graphManagerState.initializeSystem()` | none (bundled system model) | |
| 7 | `initMode()` (:920-936) | | Dispatches to STANDARD, CONFLICT_RESOLUTION or LAZY_TEXT_EDITOR. |

### 3.2 `initStandardMode` (EditorStore.ts:938-967)

1. `GET .../{w}/configuration` returns a `ProjectConfiguration`. It is stored twice: once as `projectConfiguration` and once as `originalProjectConfiguration`. Studio uses these two copies for its config-diff "change detection".
2. Then, in parallel:
   - `buildGraph()` (see 3.3)
   - `sdlcState.checkIfWorkspaceIsOutdated()`: `GET .../{w}/outdated` (EditorSDLCState.ts:414-436)
   - `workspaceReviewState.fetchCurrentWorkspaceReview()`: `GET .../{w}/revisions/CURRENT`, then `GET /projects/{p}/[patches/{v}/]reviews?state=OPEN&revisionIds=<cur>&revisionIds=<cur>&limit=1`. The result is matched on workspaceId and workspaceType (WorkspaceReviewState.ts:203-256). The duplicated revisionId is in the source.
   - `workspaceUpdaterState.fetchLatestCommittedReviews()`: `GET .../{w}/revisions/BASE`, then `GET .../reviews?state=COMMITTED&revisionIds=<base>&limit=1`, then `GET .../reviews?state=COMMITTED&since=<baseReview.committedAt or base.committedAt>` (WorkspaceUpdaterState.ts:335-385)
   - `projectConfigurationEditorState.fetchLatestProjectStructureVersion()`: `GET /configuration/latestProjectStructureVersion`
   - Engine: file-generation descriptions (`GET /pure/v1/schemaGeneration/availableGenerations` and `/codeGeneration/availableGenerations`), external formats (`GET /pure/v1/external/format/availableFormats`), function activators (`GET {engine}/functionActivator/list`), and relational DB auth flows (`GET /pure/v1/relational/connection/supportedDbAuthenticationFlows`)
   - `sdlcState.fetchProjectVersions()`: `GET /projects/{p}/versions`
   - `sdlcState.fetchPublishedProjectVersions()`: Depot `GET /projects/{groupId}/{artifactId}/versions?snapshots=true` (EditorSDLCState.ts:588-608). This is used only by service registration.
   - `sdlcState.fetchAuthorizedActions()`: `GET /projects/{p}/authorizedActions`. If this fails, `authorizedActions = undefined`, which means **everything is allowed** (EditorSDLCState.ts:186-191, 570-586).

### 3.3 `EditorStore.buildGraph` (EditorStore.ts:1106-1226) and `EditorGraphState.buildGraph` (LS/stores/editor/EditorGraphState.ts:333-533)

1. Fetch entities: `GET .../{w}/entities` returns `Entity[] {path, classifierPath, content}`. The result is stored as `workspaceLocalLatestRevisionState.entities`.
2. `graphState.buildGraph(entities)` builds the graph **client-side** in TypeScript:
   - `resetGraph`, then `createDependencyManager`.
   - **Dependencies**: `getIndexedDependencyEntities()` (EditorGraphState.ts:709-835):
     - If `projectConfiguration.projectDependencies` is non-empty, Studio converts each to `ProjectDependencyCoordinates {groupId, artifactId, versionId, exclusions?}` (:837-858). The projectId is in `group:artifact` form.
     - It calls Depot `POST /projects/dependenciesFromArtifactDependencies?transitive=true&includeOrigin=true&versioned=false` with the coordinates array as the body. This returns `ProjectVersionEntities[] {groupId, artifactId, versionId, entities[]}` (DEP-C/DepotServerClient.ts:323-347).
     - If the same project appears with more than one version, Studio calls Depot `POST /projects/analyzeDependencyTreeFromArtifactDependencies` to build a conflict report, then **throws** "Depending on multiple versions of a project is not supported" (:765-822).
     - Any failure becomes a `DependencyGraphBuilderError`, and Studio opens the config editor on the Dependencies tab (:462-476).
   - `graphManager.buildDependencies(...)`, then `graphManager.buildGraph(graph, entities, {strict: enableStrictMode, TEMPORARY__preserveSectionIndex})`, then `buildGenerations`.
   - `possiblyAddMissingGenerationSpecifications()`.
   - Failure handling:
     - Deserialization error: redirect to the Model Importer.
     - Network error: warning.
     - Other errors: fall back to **text mode** through the engine grammar round-trip (:477-525).
3. Build the explorer tree. If an `initialEntityPath` was given, open that element.
4. **(Re)start change detection** (:1188-1214):
   - `changeDetectionState.observeGraph()`.
   - In parallel:
     - `preComputeGraphElementHashes()`
     - `workspaceLocalLatestRevisionState.buildEntityHashesIndex(entities)`
     - `sdlcState.buildWorkspaceBaseRevisionEntityHashesIndex()`: `GET .../{w}/revisions/BASE/entities`
     - `sdlcState.buildProjectLatestRevisionEntityHashesIndex()`: `GET /projects/{p}/entities`, i.e. project HEAD (EditorSDLCState.ts:492-549)
   - `changeDetectionState.start()`.
   - Compute aggregated workspace changes (BASE to local HEAD) and project-latest changes (BASE to project HEAD), plus potential update conflicts.

Hash indexes are computed **client-side**. `graphManager.buildHashesIndex(entities)` deserializes the entities into protocol objects and hashes each one (LG/.../V1_PureGraphManager.ts:5092-5130). This must equal `element.hashCode` on the metamodel built from the same entity, otherwise phantom "local changes" appear. **This matters for legend-lite: whatever hashes are used, the "server baseline" and the "live model" must hash identically.**

### 3.4 Load counts (standard mode)

For a non-patch workspace with no conflict-resolution state, initial load makes roughly 17 SDLC calls:
- project
- workspace
- inConflictResolutionMode
- revisions/CURRENT (three times)
- configuration
- entities
- revisions/BASE (twice)
- revisions/BASE/entities
- project entities
- outdated
- reviews (three times)
- latestProjectStructureVersion
- versions
- authorizedActions

It also makes 1 or 2 Depot calls (versions, plus dependenciesFromArtifactDependencies if there are dependencies) and about 7 engine calls. This is my count from the code above. It was not measured on a live system.

### 3.5 "Workspace out of date" and out-of-sync detection

There are two separate notions:

- **Outdated** means the project HEAD has moved past the workspace BASE, so a rebase is needed. It is checked with `GET .../{w}/outdated` at load and in the Updater panel (EditorSDLCState.ts:414-436; WorkspaceUpdaterState.ts:188-201). It is shown in the StatusBar (LS/components/editor/StatusBar.tsx:218-223) and on the Update Workspace activity.
- **Out-of-sync** means someone else pushed to the same workspace. The condition is `remoteWorkspaceRevision.id !== currentRevision.id` (EditorSDLCState.ts:166-168).
  - It is refreshed only when the Local Changes panel mounts (`refreshWorkspaceSyncStatus`, LS/components/editor/side-bar/LocalChanges.tsx:213-218) and at push time.
  - **There is no polling.**
  - When the workspace is out of sync, Studio lists incoming revisions with `GET .../{w}/revisions?since=<cur.committedAt>&until=<remote.committedAt>` (LS/stores/editor/sidebar-state/WorkspaceSyncState.ts:409-427) and fetches `GET .../{w}/revisions/{remote}/entities` to compute pull conflicts (LocalChangesState.ts:255-313).

### 3.6 Conflict-resolution mode load (`initConflictResolutionMode`, EditorStore.ts:1063-1104)

1. `GET .../{w}/conflictResolution/configuration`.
2. Studio shows the alert "Failed to update workspace" with two actions: [Discard your changes] and [Resolve merge conflicts].
3. In parallel:
   - `conflictResolutionState.initialize()`. This builds four hash indexes:
     - workspace BASE: `GET .../{w}/revisions/BASE/entities`
     - workspace latest: `GET .../{w}/revisions/CURRENT/entities`
     - CR BASE: `GET .../{w}/conflictResolution/revisions/BASE/entities`
     - CR HEAD: `GET .../{w}/conflictResolution/revisions/CURRENT`, then `.../conflictResolution/revisions/{id}/entities` (WorkspaceUpdateConflictResolutionState.ts:251-311, 378-457)
   - `GET .../{w}/conflictResolution/outdated`
   - The same project-structure, engine-description, versions and authorized-actions calls as standard mode.

The graph is **not** built until all conflicts are marked resolved (see 4.6).

---

## 4. Local changes, saving and the SDLC loop

### 4.1 Change detection (`ChangeDetectionState`, LS/stores/editor/ChangeDetectionState.ts)

Studio tracks several revisions. The diagram is in the source comment at :278-299:

```
PJL (project HEAD)
 |
CRB (conflict-res BASE) --- CRH (conflict-res HEAD) --- CRL (live graph in CR mode)
 |
WSB (workspace BASE) ------ WSH (workspace HEAD) ------ WSL (live graph)
```

Each of these is a `RevisionChangeDetectionState`, which holds `entities` plus `entityHashesIndex: Map<path, hash>`. The states are:
- `workspaceLocalLatestRevisionState` (WSH, the baseline for local changes)
- `workspaceBaseRevisionState`
- `projectLatestRevisionState`
- `workspaceRemoteLatestRevisionState`
- `conflictResolutionBaseRevisionState`
- `conflictResolutionHeadRevisionState`

How it runs:
- A MobX reaction watches `getCurrentGraphHash()`, throttled to 1000 ms. On change it runs `computeLocalChanges` (:484-531).
- A diff between two states is a path-and-hash comparison. It produces only CREATE, MODIFY or DELETE `EntityDiff`; it never produces RENAME (:560-617).
- Aggregates:
  - `aggregatedWorkspaceChanges` (WSB to WSH), used for the Review panel
  - `aggregatedProjectLatestChanges` (WSB to PJL), used for the Updater panel
  - `aggregatedWorkspaceRemoteChanges` (WSH to remote WSH), used for the pull panel
  - `potentialWorkspaceUpdateConflicts`, `potentialWorkspacePullConflicts`, `conflicts`
  - These are computed at :619-728. Conflict rules are in `EntityChangeConflict.conflictReason` (SDLC-C/models/entity/EntityChangeConflict.ts).
- **Project-configuration changes are not part of change detection.** They are saved separately (see 5) (:837-840).

### 4.2 Local changes panel (LS/components/editor/side-bar/LocalChanges.tsx)

Actions in the panel:
- Download local changes as a "patch" JSON `{message:'', entityChanges, revisionId}` (LocalChangesState.ts:215-229).
- Upload a patch file and apply it to the graph (`PatchLoaderState`, :70-181).
- Refresh change detector (re-fetch the WSH hash index).
- Pull remote changes (when out of sync).
- Push.
- Click a diff to open a three-pane EntityDiff view.

The StatusBar shows `*` when there are unpushed changes, plus a Push button and a sync status (StatusBar.tsx:80-110, 204, 309).

### 4.3 Push / "save" (`LocalChangesState.pushLocalChanges`, LocalChangesState.ts:338-583)

1. If a push is already in flight or the workspace is updating, do nothing.
2. `processConflicts()`: `GET .../{w}/inConflictResolutionMode`. If true, Studio shows a blocking alert. Note that this code path does not actually abort the push (:778-797).
3. `computeLocalEntityChanges()` (form mode, :802-841): for each own element whose hash differs from the WSH index, emit `{type: MODIFY|CREATE, entityPath, classifierPath, content}`. The `content` comes from `elementToEntity(..., {pruneSourceInformation:true})`. Baseline paths that are missing from the graph become `{type: DELETE, entityPath}`. If there are no changes, Studio returns early.
   - In text mode (`TextLocalChangesState`), the change list is whatever the last text compile produced. `globalCompile` calls engine compileText, which runs `computeLocalChangesInTextMode(entities)` (LS/stores/editor/GraphEditGrammarModeState.ts:470-560; ChangeDetectionState.ts:149-236). **In text mode the user must compile (F9) before pushing**, otherwise the push sends stale changes.
4. `fetchRemoteWorkspaceRevision`: `GET .../{w}/revisions/CURRENT`. If out of sync, Studio fetches the remote entities, computes pull conflicts, shows the alert "Local workspace is out-of-sync ... Pull remote changes" and **aborts**.
5. **`POST /projects/{p}/[patches/{v}/](workspaces|groupWorkspaces)/{w}/entityChanges`** with `PerformEntitiesChangesCommand { message, entityChanges: EntityChange[], revisionId: currentRevision.id }` (SDLC-C/models/entity/EntityCommands.ts; SDLCServerClient.ts:956-965).
   - The message is always the default: `pushed new changes from <appName> [potentially affected N entities]` (:427-435). None of the callers pass `pushMessage` (EditorStore.ts:526, StatusBar.tsx:89, LocalChanges.tsx:151, LazyTextEditor.tsx:102).
   - The response is a `Revision` (or empty). If it is empty, Studio throws "Can't push an empty change set".
   - **HTTP 409** means the revision is not HEAD. Studio warns "Please backup your work and refresh" (:565-573).
6. Studio sets `currentRevision` and `remoteWorkspaceRevision` to the returned revision.
7. It re-fetches `GET .../{w}/revisions/{newRev}/entities` to rebuild the WSH index. On a 404 or network error it offers "Use local hashes index" or "Refresh changes", working around an SDLC caching issue (:465-541). Then it restarts change detection.

`EntityChange` shape: `{type: CREATE|DELETE|MODIFY|RENAME, entityPath, classifierPath?, newEntityPath?, content?}` (SDLC-C/models/entity/EntityChange.ts). Studio never sends RENAME. A rename appears as DELETE plus CREATE.

Other write paths:
- The **Model Importer** uses `POST .../{w}/entities` with `UpdateEntitiesCommand {message, entities, replace}`, then reloads the page (LS/stores/editor/editor-state/ModelImporterState.ts:255-290, 352-380, 560-585). `replace=true` replaces the whole workspace content.
- The extensions (QueryProductionizer, ServiceQueryEditor, DataSpace promotion) also call `entityChanges`.

### 4.4 Pull (sync with remote workspace HEAD) (`WorkspaceSyncState`, LS/stores/editor/sidebar-state/WorkspaceSyncState.ts:429-578)

- If there are no local changes, or no conflicts, Studio runs `loadChanges(localChanges)`:
  - The new baseline becomes the remote entities.
  - The local `EntityChange[]` list is re-applied on top of the remote entities with `applyEntityChanges`.
  - The graph is rebuilt with `graphEditorMode.updateGraphAndApplication` (EditorGraphState.ts:595-623). This is form mode only.
- If there are conflicts, Studio offers [Resolve merge conflicts] (a three-way merge modal, `WorkspaceSyncConflictResolutionState`) or [Force pull].
- **There are no SDLC write calls in a pull.** It is purely client-side.

### 4.5 Update workspace (rebase on project HEAD) (`WorkspaceUpdaterState`, LS/stores/editor/sidebar-state/WorkspaceUpdaterState.ts)

- **Refresh** (:157-241):
  - `GET .../{w}/inConflictResolutionMode` and `GET .../{w}/outdated`.
  - If outdated, Studio fetches the committed reviews since BASE (shown as a list linking to `/review/...`) and rebuilds the PJL and WSB indexes.
  - It computes project-latest changes and potential conflicts. These are shown read-only as three-way conflict views.
- **Update** (:243-330). The UI first runs `alertUnsavedChanges`, which warns that unpushed changes will be lost (LS/components/editor/side-bar/WorkspaceUpdater.tsx:53-58).
  1. Studio checks `inConflictResolutionMode`.
  2. It shows a blocking alert.
  3. It calls **`POST .../{w}/update`**, which returns `WorkspaceUpdateReport {status: NO_OP|UPDATED|CONFLICT, workspaceRevisionId, workspaceMergeBaseRevisionId}`.
  4. On UPDATED or CONFLICT, Studio does a **full page reload**. On reload, CONFLICT is detected through `inConflictResolutionMode` and Studio enters conflict-resolution mode.

### 4.6 Conflict resolution (`AbstractConflictResolutionState` and its subclasses)

- `AbstractConflictResolutionState` (LS/stores/editor/AbstractConflictResolutionState.ts:26-55) holds `mergeEditorStates`, and defines the abstract members `resolutions`, `openConflict`, `closeConflict`, `resolveConflict` and `markConflictAsResolved`.
- There are two implementations:
  - `WorkspaceUpdateConflictResolutionState` (LS/stores/editor/sidebar-state/WorkspaceUpdateConflictResolutionState.ts). This is server-side conflict resolution after `update` returns CONFLICT.
  - `WorkspaceSyncConflictResolutionState` (WorkspaceSyncState.ts:55-366). This is the client-side merge during a pull.
- The merge editor is `EntityChangeConflictEditorState`, a three-way merge on entity JSON or grammar. It produces an `EntityChangeConflictResolution {entityPath, resolvedEntity?}`.
- Server-side CR flow:
  1. Resolve every conflict.
  2. `promptBuildGraphAfterAllConflictsResolved` builds the graph from the CR HEAD entities with the resolutions patched in (:313-376, 718-732).
  3. The user can compile.
  4. **Accept**: `POST .../{w}/conflictResolution/accept` with `{message: 'resolving update merge conflicts for workspace from <appName> [...]', entityChanges: <local changes vs CR HEAD>, revisionId}`. Then a reload (:459-527).
  5. **Discard changes**: `POST .../{w}/conflictResolution/discardChanges`, which keeps the project's version and throws away the workspace changes. Then a reload (:529-584).
  6. **Abort**: `DELETE .../{w}/conflictResolution`, which returns to the pre-update workspace. Then a reload (:586-641).
  7. The StatusBar shows an "Accept conflict resolution" button (StatusBar.tsx:117-140, 269-281).

### 4.7 Workspace review (`WorkspaceReviewState`, LS/stores/editor/sidebar-state/WorkspaceReviewState.ts; panel LS/components/editor/side-bar/WorkspaceReview.tsx)

- The panel shows `aggregatedWorkspaceChanges` (WSB to WSH, **pushed** changes only), plus a title input and Create, Close and Commit buttons.
- **Fetch review** (:203-256): see 3.2.
- **Create** (:343-420):
  - It is blocked if the project configuration contains SNAPSHOT dependencies or the project is a sandbox.
  - The button is disabled without the `SUBMIT_REVIEW` authorized action or without a title (WorkspaceReview.tsx:158-173).
  - It calls `POST /projects/{p}/[patches/{v}/]reviews` with `CreateReviewCommand {workspaceId, title, workspaceType, description}`. The default description is `review from <appName> for workspace <w>`. `labels` is not sent from this flow.
- **Close** (:290-338): this calls **`POST .../reviews/{r}/reject`**, not `/close`.
- **Commit / merge** (:422-543):
  - It is blocked by snapshot dependencies, conflict-resolution mode, or a missing `COMMIT_REVIEW` action. The UI first runs `alertUnsavedChanges`.
  - It calls `POST .../reviews/{r}/commit` with `{message: '<title> [review]'}`.
  - Studio removes the workspace from recents and shows an alert with "Create new workspace", which does `POST` create of the same id and reloads, or "Leave", which goes to the setup page.
  - **The server deletes the workspace on commit.**
- There is no approve action in the workspace panel. Approval lives only in the Review page (6.2).

### 4.8 Workflows / builds / pipelines (I) (`WorkflowManagerState`, LS/stores/editor/sidebar-state/WorkflowManagerState.ts)

There are three variants, each with list, jobs, job detail, logs, cancel, retry and run-manual:

| Variant | Used in | Endpoints |
|---|---|---|
| `WorkspaceWorkflowManagerState` (:663-744) | Editor "Workflow Manager" activity | `GET .../{w}/workflows`, `GET .../{w}/workflows/{id}`, `GET .../{w}/workflows/{id}/jobs`, `GET .../jobs/{jobId}`, `GET .../jobs/{jobId}/logs` (Accept: text/plain), `POST .../jobs/{jobId}/cancel` \| `/retry` \| `/run` |
| `ProjectWorkflowManagerState` (:839-920) | Project viewer at HEAD | Same, but `/projects/{p}/workflows...` |
| `ProjectVersionWorkflowManagerState` (:746-837) | Project viewer at a version | `/projects/{p}/versions/{v}/workflows...` |

Notes:
- The workflow list has no status filter or limit (`getWorkflows(p, ws, undefined, undefined, undefined)`).
- `EditorSDLCState.fetchWorkspaceWorkflows` (`getWorkflowsByRevision`, EditorSDLCState.ts:551-568) is **dead code**: there are no callers.

### 4.9 Versions (C for "publish") (`ProjectOverviewState`, LS/stores/editor/sidebar-state/ProjectOverviewState.ts; UI LS/components/editor/side-bar/ProjectOverview.tsx)

- **Release tab load** (`fetchLatestProjectVersion`, :299-387):
  1. `GET /projects/{p}/versions/latest`. This may be empty, in which case `latestProjectVersion = null`.
  2. `GET /projects/{p}/revisions/CURRENT`. This sets `releaseVersion.revisionId`, so **a version is always cut from project HEAD**.
  3. If a latest version exists: `GET /projects/{p}/revisions/{latest.revisionId}`, then `GET /projects/{p}/reviews?state=COMMITTED&revisionIds=..&limit=1`, then `GET /projects/{p}/reviews?state=COMMITTED&since=..` to list the "reviews since last release". If no version exists, only `GET /projects/{p}/reviews?state=COMMITTED`.
- **Create version** (:389-438):
  - There are three buttons: Major, Minor, Patch (ProjectOverview.tsx:330-336, 388-419).
  - The user must enter release notes; `CreateVersionCommand.validate` requires both revisionId and notes.
  - It calls `POST /projects/{p}/versions` with `CreateVersionCommand {versionType: MAJOR|MINOR|PATCH, revisionId, notes}` (SDLC-C/models/version/VersionCommands.ts). The response is `Version {projectId, revisionId, notes, id:{majorVersion, minorVersion, patchVersion}}`.
  - The buttons are disabled if:
    - the latest version is already at HEAD
    - `server/features.canCreateVersion` is false
    - the user lacks the `CREATE_VERSION` authorized action (ProjectOverview.tsx:340-356)
  - The Release tab is hidden for EMBEDDED projects (ProjectOverview.tsx:1166-1195).
  - The server computes the next number. Studio only sends the bump type.
- **Versions tab**: lists `sdlcState.projectVersions` (from load) with links to `/view/{p}/version/{v}`.
- Note that **publishing to Depot is out of band**. A version is cut in SDLC, and the CI pipeline publishes to Depot. Studio only reads Depot.

### 4.10 Project Overview tab: other actions (I)

- **Overview**: edit name, description and tags with `PUT /projects/{p}` and `UpdateProjectCommand {name, description, tags}`, then re-fetch the project (ProjectOverviewState.ts:230-280).
- **Workspaces**: list all user and group workspaces, plus patch workspaces (N+1 calls). Delete with `DELETE .../(group)workspaces/{w}`. If you delete the current workspace, Studio redirects to setup (:115-228).
- **Patch**: create a patch from a version, list committed reviews for a patch with `GET /projects/{p}/patches/{v}/reviews?state=COMMITTED` (ProjectOverview.tsx:552-588), and release a patch.

---

## 5. Project configuration editor

The state is `ProjectConfigurationEditorState` (LS/stores/editor/editor-state/project-configuration-editor-state/ProjectConfigurationEditorState.ts). The UI is LS/components/editor/editor-group/project-configuration-editor/ProjectConfigurationEditor.tsx.

The tabs (`CONFIGURATION_EDITOR_TAB`, :53-58) are PROJECT_STRUCTURE, PROJECT_DEPENDENCIES, PLATFORM_CONFIGURATIONS (hidden for EMBEDDED, :60-71) and ADVANCED.

### 5.1 Data model

`ProjectConfiguration` (SDLC-C/models/configuration/ProjectConfiguration.ts):
- `projectId`
- `groupId`
- `artifactId`
- `projectType?: MANAGED|EMBEDDED`
- `projectStructureVersion {version, extensionVersion?}`
- `platformConfigurations?: {name, version}[]`
- `projectDependencies: {projectId: "group:artifact", versionId, exclusions?: {projectId}[]}[]`
- `runDependencyTests?`

### 5.2 Saving: not via entity changes

`updateConfigs()` (:368-433) builds an `UpdateProjectConfigurationCommand` by diffing the current configuration against the original:
- `groupId` and `artifactId` are always sent.
- `projectStructureVersion` is sent as the current value.
- `message` is `update project configuration from <appName>`.
- `platformConfigurations` is sent only if the hash changed. It is wrapped as `{platformConfigurations: [...] | null}`.
- `runDependencyTests` is sent only if changed.
- `projectDependenciesToAdd` and `projectDependenciesToRemove` are computed by hash diff.

The call is **`POST .../{w}/configuration`**, which returns a `Revision`. Then Studio runs `editorStore.reset()`, `fetchCurrentWorkspace`, `fetchCurrentRevision` and **`initMode()`**: a full re-initialization that re-fetches entities and rebuilds the graph (:236-297). The UI wraps this in `alertUnsavedChanges` (ProjectConfigurationEditor.tsx:819-829). In other words, **configuration edits are an immediate separate commit, and unpushed entity edits are lost.**

### 5.3 Project structure

- The tab shows the current `projectStructureVersion` against `latestProjectStructureVersion` (from `GET /configuration/latestProjectStructureVersion`).
- "Update" (`updateToLatestStructure`, :299-334) sends a command with only `projectStructureVersion` set. For EMBEDDED projects, `extensionVersion` is dropped.
- groupId and artifactId are edited inline (ProjectConfigurationEditor.tsx:118-130). Changing either one triggers a strong warning: "project will lose all previous versions ..." (:757-817).

### 5.4 Dependencies (I)

- On opening the tab (once), Studio calls:
  - Depot **`GET /project-configurations`**, which lists *all* projects as `StoreProjectData {projectId, groupId, artifactId}` keyed by `group:artifact`.
  - Depot `GET /projects/{g}/{a}/versions?snapshots=true` for each existing dependency (ProjectConfigurationEditorState.ts:198-234; ProjectConfigurationEditor.tsx:830-840).
- "Add" picks the first other project and sets its version to `versions[0]` (ProjectConfigurationEditor.tsx:715-753). The version dropdown is populated from Depot `getVersions(g, a, true)` (ProjectDependencyEditor.tsx:1347).
- Snapshot versions (`*-SNAPSHOT`, `master-SNAPSHOT`) are allowed in a workspace but **block review creation and commit** (`containsSnapshotDependencies`, :184-190; WorkspaceReviewState.ts:350-358, 426-434).
- The dependency report (`ProjectDependencyEditorState`, :631-668) uses Depot `POST /projects/analyzeDependencyTreeFromArtifactDependencies`, which returns a graph plus conflicts. The UI renders a tree, a flattened view and conflict paths.
- "Validate" (:670-734) fetches dependency entities and compiles project plus dependency entities with engine `compileEntities`.
- "Resolve compatible" (:825-895) calls Depot `POST /projects/resolveCompatibleDependencies?backtrackVersions=N`, which returns `{success, resolvedVersions[], conflicts[], failureReason, suggestedOverrides?}`. "Apply" rewrites the version ids locally (:770-823).
- Exclusions are editable per dependency (:424-550).

### 5.5 Platform configurations and advanced settings (N)

- Platform configurations:
  - The default platform list comes from `GET /server/platforms`, fetched at boot as `Platform {name, groupId, platformVersion}`.
  - "Override" toggles explicit `platformConfigurations`. "Update to latest" copies the server versions (ProjectConfigurationEditor.tsx:312-400).
- Advanced:
  - The `runDependencyTests` toggle.
  - "Change project type" between MANAGED and EMBEDDED. This sends a command with only `projectType` set (ProjectConfigurationEditorState.ts:336-364; ProjectConfigurationEditor.tsx:574-590).

---

## 6. Viewer modes

### 6.1 Project viewer (`ProjectViewerStore`, LS/stores/project-view/ProjectViewerStore.ts; mode `ProjectViewerEditorMode`, LS/stores/project-view/ProjectViewerEditorMode.ts)

The editor runs in `EDITOR_MODE.VIEWER` with `disableEditing = true` (:94-96). The configuration editor is read-only (ProjectConfigurationEditorState.ts:126).

- **By projectId** (`initializeWithProjectInformation`, :154-301):
  1. `GET /projects/{p}`. Studio creates a stub workspace with id `''`.
  2. `GET /projects/{p}/revisions/CURRENT` and `GET /projects/{p}/versions/latest`.
  3. Then exactly one of the following (both version and revision together is an error):
     - **version**: `GET /projects/{p}/versions/{v}` (if it is not the latest), then `GET /projects/{p}/versions/{v}/entities` || `GET /projects/{p}/versions/{v}/configuration`
     - **revision**: `GET /projects/{p}/revisions/{r}`, then `GET /projects/{p}/revisions/{r}/entities` || `.../revisions/{r}/configuration`
     - **HEAD**: `GET /projects/{p}/entities` || `GET /projects/{p}/configuration`
  4. `GET /projects/{p}/versions`, Depot versions, and `GET .../authorizedActions`.
  5. Dependencies, through the same Depot `dependenciesFromArtifactDependencies` call.
- **By GAV** (SDLC-bypassed, `initializeWithGAV`, :309-351):
  1. Depot `GET /project-configurations/{g}/{a}`.
  2. Depot `GET /projects/{g}/{a}/versions/{v}`. `HEAD` maps to `master-SNAPSHOT` through `resolveVersion` (DEP-C/DepotVersionAliases.ts).
  3. Depot `GET /projects/{g}/{a}/versions/{v}/dependencies?transitive=true&includeOrigin=false&versioned=false`.
- **Build** (:353-511): engine init, system build, client-side build of dependencies and graph, then engine description fetches. If the graph fails to build, Studio falls back to text mode. For SDLC (non-GAV) sources it runs `globalGenerate` if generation specs exist (:558-572).
- The workflow viewer is shown for a version or HEAD but not for a revision or GAV (:599-615).
- The status bar has "visit project", which maps a GAV to an SDLC projectId through Depot (LS/components/project-view/ProjectViewer.tsx:126-172).

### 6.2 Review viewer (`ProjectReviewerStore`, LS/stores/project-reviewer/ProjectReviewerStore.ts)

`initialize()` runs the following in parallel (:201-217):
- Engine init, used only for grammar rendering of diffs.
- `GET /projects/{p}/reviews/{r}`.
- `GET /projects/{p}`.
- `GET /projects/{p}/reviews/{r}/approval`, which returns `{approvedBy: User[]}`.
- `GET /projects/{p}/reviews/{r}/comparison`, which returns `Comparison {fromRevisionId, toRevisionId, entityDiffs[{oldPath,newPath,entityChangeType}], projectConfigurationUpdated}`. Studio then makes **one call per diffed entity**: `GET .../comparison/from/entities/{path}` and `GET .../comparison/to/entities/{path}`. If `projectConfigurationUpdated` is true, it also calls `.../comparison/from/configuration` and `.../comparison/to/configuration` (:302-373).

There is no graph build. Diffs are rendered per entity.

Actions:
- Approve: `POST .../approve`.
- Commit: `POST .../commit {message:'<title> [review]'}`.
- Close: `POST .../close`.
- Reopen: `POST .../reopen` (:437-630).
- The Approve button is disabled when `currentUser.userId === review.author.name`, which compares an id to a name (LS/components/project-reviewer/ProjectReviewSideBar.tsx:212-222).

### 6.3 Dependency project viewer

- Dependency elements in the explorer link to `/view/archive/{g:a:v}/entity/{path}`. `master-SNAPSHOT` is mapped to `HEAD` (LS/stores/editor/StandardEditorMode.ts:69-81; ProjectViewerEditorMode.ts:71-83).
- "View SDLC project" (Explorer context menu, LS/components/editor/side-bar/Explorer.tsx:794) calls Depot `GET /project-configurations/{g}/{a}` to get the `projectId`, then opens `/view/{projectId}` (LS/stores/editor/DependencyProjectViewerHelper.ts:24-39).

### 6.4 Lazy text editor (`/text/...`) (N)

It runs the same `initialize` with `mode = LAZY_TEXT_EDITOR`. It fetches config and entities, builds hashes, builds a "light" dependency graph, converts entities to grammar with engine `entitiesToPureCode`, and opens a grammar-only editor that cannot leave text mode (EditorStore.ts:969-1061; LazyTextEditorStore.ts:58-70).

---

## 7. Endpoint catalogue (Studio and extensions as callers)

### 7.1 SDLC endpoints

The base is `config.sdlc.url`, for example `http://localhost:6100/api`.

Conventions:
- Every request gets `?client_name=<sdlc.client>` when that config is set (SDLC-C/SDLCServerClient.ts:184-209).
- Auth uses either a Bearer token (when a token getter is wired) or cookies only (`sdlc.useCookieAuthOnly`, :142-146).
- `{ws}` means `workspaces/{w}` or `groupWorkspaces/{w}`, optionally prefixed with `patches/{v}/`. Workspace type comes from `Workspace.userId`, and the patch comes from `Workspace.source` (:414-465).
- "adaptive" means the path is `/projects/{p}/{ws}/...` when a workspace is given and `/projects/{p}/...` otherwise.

Mark: **C** = needed for the core loop. **u** = defined in the client but unused by any caller.

| Method + path (relative to sdlc base) | Client fn (SDLCServerClient.ts line) | Request → Response | Calling site(s) / flow | Mark |
|---|---|---|---|---|
| GET `/auth/authorized` | isAuthorized :249 | → boolean | LegendStudioBaseStore.ts:631,642 boot | C |
| (browser nav) `/auth/authorize?redirect_uri=..&client_name=` | authorizeCallbackUrl :237-246 | n/a | BaseStore :426 (popup), :645 (redirect) | C |
| GET `/auth/termsOfServiceAcceptance` | :250 | → string[] (URLs to accept) | BaseStore :580 | I |
| GET `/server/features` | :226 | → `{canCreateProject, canCreateVersion}` | BaseStore :612. Used in CreateProjectModal:58,647; ProjectOverview:348,634,726; ProjectOverviewState:390; Service registration | C |
| GET `/server/platforms` | :229 | → `Platform[] {name, groupId, platformVersion}` | BaseStore :611; ProjectConfigurationEditor:332 | I |
| GET `/currentUser` | :266 | → `User {userId, name}` | BaseStore :222 | C |
| GET `/users?search=` | :268 | → User[] | Service owner pickers (ServiceEditorState:280, HostedService…:267, QueryProductionizer:351) | N |
| GET `/projects?user&search&tag[]&excludeTag[]&limit` | getProjects :317-330 | → Project[] `{projectId,name,description,webUrl,tags}` | WorkspaceSetupStore:558 (search), :660 (sandbox); QueryProductionizerStore:431; UpdateProjectServiceQuerySetupStore:163 | C |
| GET `/projects/{p}` | :277 | → Project | EditorSDLCState:238; WorkspaceSetupStore:295,412,572,636; ProjectReviewerStore:378; ProjectViewer.tsx:160; extensions | C |
| POST `/projects` | createProject :331 | `{name,description,groupId,artifactId,tags}` → Project | WorkspaceSetupStore:877 | C |
| POST `/projects/import` | :339 | `{id,groupId,artifactId}` → `{project, reviewId}` | WorkspaceSetupStore:930 | N |
| PUT `/projects/{p}` | updateProject :347 | `{name,description,tags}` → void | ProjectOverviewState:248 | I |
| GET `/projects/{p}/authorizedActions` | getAutorizedActions :361 | → `("CREATE_WORKSPACE"\|"SUBMIT_REVIEW"\|"COMMIT_REVIEW"\|"CREATE_VERSION")[]` | EditorSDLCState:573 | I (failure means allow everything) |
| GET `/projects/{p}/userAccessRole/currentUser` | getAccessRole :368 | → `{accessRole}` | — | u |
| GET `/projects/{p}/patches` | :386 | → Patch[] `{projectId, patchReleaseVersionId:{majorVersion,minorVersion,patchVersion}}` | WorkspaceSetupStore:730; ProjectOverviewState:119,285 | I (load) / N (feature) |
| POST `/projects/{p}/patches` | createPatch :389 | body = version string → Patch (typed as Workspace in the client) | ProjectOverviewState:513 | N |
| GET `/projects/{p}/patches/{v}` | :398 | → Patch | EditorSDLCState:266 | N |
| POST `/projects/{p}/patches/{v}/release` | :403 | → Version | ProjectOverviewState:456 | N |
| GET `/projects/{p}/[patches/{v}/]workspaces` and `.../groupWorkspaces` | getWorkspaces :467 (two calls merged) | → Workspace[] `{projectId, workspaceId, userId?}` | WorkspaceSetupStore:772,787; ProjectOverviewState:124,131 | C |
| GET `.../groupWorkspaces` | getGroupWorkspaces :477 | → Workspace[] | Extensions only (QueryProductionizer:474, Update*ServiceQuerySetup, DataSpace promotion) | N |
| GET `/projects/{p}/{ws}` | getWorkspace :482 | → Workspace | EditorSDLCState:295 | C |
| GET `/projects/{p}/{ws}/outdated` | :496 | → boolean | EditorSDLCState:422; WorkspaceUpdaterState:191 | C |
| GET `/projects/{p}/{ws}/inConflictResolutionMode` | :501 | → boolean | EditorSDLCState:346 (load, push, update, commit, CR actions) | C |
| POST `/projects/{p}/{ws}` | createWorkspace :508 | no body → Workspace | WorkspaceSetupStore:350,995; EditorStore:788; WorkspaceReviewState:266; ProjectOverviewState:523; 5 extension stores | C |
| POST `/projects/{p}/{ws}/update` | updateWorkspace :523 | no body → `{status: NO_OP\|UPDATED\|CONFLICT, workspaceRevisionId, workspaceMergeBaseRevisionId}` | WorkspaceUpdaterState:282 | I |
| DELETE `/projects/{p}/{ws}` | deleteWorkspace :531 | → (Workspace) | ProjectOverviewState:169; extensions (cleanup on failure) | I |
| GET adaptive `/revisions?since&until` | getRevisions :556 | → Revision[] | WorkspaceSyncState:413 (incoming revisions) | I |
| GET adaptive `/revisions/{id\|BASE\|CURRENT}` | getRevision :568 | → Revision `{id, authorName, authoredTimestamp, committerName, committedTimestamp, message}` | EditorSDLCState:367,396,445; WorkspaceReviewState:207; WorkspaceUpdaterState:342; ProjectOverviewState:311,322; ProjectViewerStore:171,222 | C |
| GET `/projects/{p}/versions` | :582 | → Version[] | EditorSDLCState:330 | C |
| GET `/projects/{p}/versions/{v}` | :584 | → Version | ProjectViewerStore:201 | I |
| POST `/projects/{p}/versions` | createVersion :589 | `{versionType, revisionId, notes}` → Version | ProjectOverviewState:412 | **C** |
| GET `/projects/{p}/versions/latest` | :598 | → Version \| empty | ProjectOverviewState:303; ProjectViewerStore:178 | C |
| GET adaptive `/configuration` | getConfiguration :610 | → ProjectConfiguration | EditorStore:941,973; EditorGraphState:640; ProjectViewerStore:249; extensions | C |
| GET `/projects/{p}/versions/{v}/configuration` | :615 | → ProjectConfiguration | ProjectViewerStore:212 | I |
| GET adaptive `/revisions/{r}/configuration` | :620 | → ProjectConfiguration | ProjectViewerStore:235 | N |
| POST adaptive `/configuration` | updateConfiguration :628 | UpdateProjectConfigurationCommand → Revision | ProjectConfigurationEditorState:250; QueryProductionizerStore:648 | C (dependencies) |
| GET `/configuration/latestProjectStructureVersion` | :638 | → `{version, extensionVersion?}` | ProjectConfigurationEditorState:439 | I |
| GET `/projects/{p}/configuration/projectConfigurationStatus` | :641 (projectId **not URL-encoded**) | → `{projectConfigured, reviewIds[]}` | ProjectConfigurationStatus.ts:39 (setup) | I |
| GET adaptive `/workflows?status&revisionIds&limit` | getWorkflows :686 | → Workflow[] | WorkflowManagerState:693,869 | I |
| GET adaptive `/workflows?revisionId` | getWorkflowsByRevision :698 | → Workflow[] | EditorSDLCState:554 (dead code) | u-ish |
| GET adaptive `/workflows/{id}` | :680 | → Workflow | WorkflowManagerState:705,881 | I |
| GET adaptive `/workflows/{id}/jobs?status&revisionIds&limit` | :706 | → WorkflowJob[] | WorkflowManagerState:670,846 | I |
| GET adaptive `/workflows/{id}/jobs/{j}` | :720 | → WorkflowJob | WorkflowManagerState:683,859 | I |
| GET adaptive `.../jobs/{j}/logs` (Accept text/plain) | :733 | → string | WorkflowManagerState:738,914 | I |
| POST adaptive `.../jobs/{j}/cancel` \| `/retry` \| `/run` | :748,761,774 | → WorkflowJob | WorkflowManagerState:714-730, 890-906 | N |
| GET/POST `/projects/{p}/versions/{v}/workflows[...]` (same set) | :818-921 | same | WorkflowManagerState:763-831 (version viewer) | N |
| GET adaptive `/entities` | getEntities :930 | → Entity[] | EditorStore:990,1115; EditorSDLCState:525 (project HEAD); ProjectViewerStore:245; extensions | **C** |
| GET adaptive `/revisions/{r}/entities` | getEntitiesByRevision :935 | → Entity[] | EditorSDLCState:457,464,495; LocalChangesState:273,375,475; ProjectViewerStore:230 | **C** |
| GET `/projects/{p}/versions/{v}/entities` | :941 | → Entity[] | ProjectViewerStore:208 | I |
| POST adaptive `/entities` | updateEntities :946 | `{message, entities, replace}` → Revision | ModelImporterState:266,362,569 | I |
| POST adaptive `/entityChanges` | performEntityChanges :956 | `{message, entityChanges[], revisionId?}` → Revision | LocalChangesState:423; extensions (QueryProductionizer:663, ServiceQueryEditor:261, DataSpace promotion:455) | **C** |
| GET `/projects/{p}/{ws}/entities/{path}` (path not encoded) | getWorkspaceEntity :966 | → Entity | UpdateServiceQuerySetupStore:300 | N |
| GET `/projects/{p}/[patches/{v}/]reviews?state&revisionIds&workspaceIdRegex&workspaceTypes&since&until&limit` | getReviews :993 | → Review[] | WorkspaceReviewState:212; WorkspaceUpdaterState:349,363; ProjectOverviewState:333,350,369; ProjectOverview.tsx:563 | C |
| GET `/reviews?assignedToMe&authoredByMe&labels&...` | getAllReviews :1008 | → Review[] | — | u |
| GET `/projects/{p}/[patches/{v}/]reviews/{r}` | getReview :1016 | → Review `{id,state,author{userId,name},title,description?,projectId,workspaceId,workspaceType,webURL,createdAt,closedAt?,lastUpdatedAt?,committedAt?,labels?}` | ProjectReviewerStore:419; ProjectConfigurationStatus.ts:47; WorkspaceSetupStore:937 | I |
| GET `.../reviews/{r}/approval` | getReviewApprovals :1022 | → `{approvedBy: User[]}` (the client's generic types for getReview and getReviewApprovals are swapped) | ProjectReviewerStore:395 | I |
| POST `.../reviews` | createReview :1030 | `{workspaceId, title, workspaceType, description, labels?}` → Review | WorkspaceReviewState:385 | **C** |
| POST `.../reviews/{r}/approve` | :1040 | → Review | ProjectReviewerStore:454 | I |
| POST `.../reviews/{r}/reject` | :1048 | → Review | WorkspaceReviewState:312 ("Close review" in the workspace panel) | I |
| POST `.../reviews/{r}/close` | :1056 | → Review | ProjectReviewerStore:604 | I |
| POST `.../reviews/{r}/reopen` | :1064 | → Review | ProjectReviewerStore:559 | N |
| POST `.../reviews/{r}/commit` | :1072 | `{message}` → Review | WorkspaceReviewState:474; ProjectReviewerStore:499 | **C** |
| GET `.../reviews/{r}/comparison` | :1092 | → Comparison | ProjectReviewerStore:306 | I |
| GET `.../comparison/(from\|to)/configuration` | :1100,1113 | → ProjectConfiguration | ProjectReviewerStore:347,354 | I |
| GET `.../comparison/(from\|to)/entities/{path}` | :1137,1162 | → Entity | ProjectReviewerStore:322,335 | I |
| GET `.../comparison/(from\|to)/entities` | :1125,1150 | → Entity[] | — | u |
| GET `/projects/{p}/conflictResolution` | getWorkspacesInConflictResolutionMode :1184 (ignores its patch arg) | → Workspace[] | WorkspaceSetupStore:765; extensions | I |
| DELETE adaptive `/conflictResolution` | abort :1189 | → void | WorkspaceUpdateConflictResolutionState:624 | I |
| POST adaptive `/conflictResolution/discardChanges` | :1194 | → void | …:567 | I |
| POST adaptive `/conflictResolution/accept` | :1201 | PerformEntitiesChangesCommand → void | …:499 | I |
| GET adaptive `/conflictResolution/outdated` | :1210 | → boolean | EditorSDLCState:418 | I |
| GET adaptive `/conflictResolution/revisions/{r}` | :1215 | → Revision | EditorSDLCState:362,391; CR state:386 | I |
| GET adaptive `/conflictResolution/revisions/{r}/entities` | :1226 | → Entity[] | CR state:398,432 | I |
| GET adaptive `/conflictResolution/configuration` | :1237 | → ProjectConfiguration | CR state:238 | I |

Unused client methods, found by grepping every package excluding tests: `getAccessRole`, `getAllReviews`, `getReviewFromEntities`, `getReviewToEntities`. `getWorkflowsByRevision` is reachable only from dead code.

Spot-checks against the server (legend-sdlc @1021fda): `/server/{info,features,platforms}` (SDLC-S/resources/ServerResource.java:31-66), `.../entityChanges` for all four workspace kinds (SDLC-S/resources/entity/{project,patch}/{user,group}/*EntityChangesResource.java:36-39), `{workspaceId}/inConflictResolutionMode`, `/configuration/latestProjectStructureVersion` (SDLC-S/resources/project/ConfigurationResource.java:62), `/projects/{id}/authorizedActions` and `/userAccessRole/currentUser` (ProjectsResource.java:179,189), `/projects/{projectId}/conflictResolution` (ConflictResolutionProjectResource.java:32), and `/patches` POST taking a version string (PatchesResource.java:54-66) all exist. I did not verify every path. The SDLC-server census should be the authority.

### 7.2 Depot endpoints used by Studio

The base is `config.depot.url`, for example `http://localhost:6200/depot/api` (DEP-C/DepotServerClient.ts).

| Method + path | Client fn (line) | Request → Response | Caller / flow | Mark |
|---|---|---|---|---|
| POST `/projects/dependenciesFromArtifactDependencies?transitive=true&includeOrigin=true&versioned=false` | collectDependencyEntities :323-347 | body `[{groupId,artifactId,versionId,exclusions?}]` → `ProjectVersionEntities[] {groupId,artifactId,versionId,entities[]}` | EditorGraphState:725 (every graph build in editor and viewer) | **C** (if there are dependencies) |
| POST `/projects/analyzeDependencyTreeFromArtifactDependencies` | analyzeDependencyTree :375-384 | coordinates[] → RawProjectDependencyReport `{graph{rootNodes,nodes}, conflicts[{groupId,artifactId,versions[]}]}` | EditorGraphState:775 (only on conflict); ProjectDependencyEditorState:644 | I |
| POST `/projects/resolveCompatibleDependencies?backtrackVersions=N` | :386-404 | coordinates[] → `{success, resolvedVersions[], conflicts[], failureReason, suggestedOverrides?}` | ProjectDependencyEditorState:836; ProjectDependencyEditor.tsx:624 | N |
| POST `/projects/dependencies/pureModelContextData` | :349-373 | coordinates[] → PMCD | DevToolPanel.tsx:97 (debug) | N |
| GET `/project-configurations` | getProjects :68-69 | → StoreProjectData[] `{projectId, groupId, artifactId}` (**all projects**) | ProjectConfigurationEditorState:202 (dependency picker) | C (to add a dependency) |
| GET `/project-configurations/{g}/{a}` | getProject :71-79 | → StoreProjectData | ProjectViewerStore:320 (GAV viewer); ProjectViewer.tsx:130,154; DependencyProjectViewerHelper.ts:32; QueryProductionizer:370 | I |
| GET `/projects/{g}/{a}/versions?snapshots=true` | getVersions :473-488 | → string[] | EditorSDLCState:592; ProjectConfigurationEditorState:212; ProjectDependencyEditorState:793; ProjectConfigurationEditor.tsx:734; ProjectDependencyEditor.tsx:1347 | C (version picking) |
| GET `/projects/{g}/{a}/versions/{v}` | getEntities/getVersionEntities :96-115 | → Entity[] | ProjectViewerStore:328 (GAV); DevMetadataState:114 | I |
| GET `/projects/{g}/{a}/versions/{v}/dependencies?transitive=true&includeOrigin=false&versioned=false` | getIndexedDependencyEntities :293-321 | → ProjectVersionEntities[] | ProjectViewerStore:340 (GAV viewer) | I |

`HEAD` is mapped to `master-SNAPSHOT` by `resolveVersion` (DEP-C/DepotVersionAliases.ts). Snapshots are detected by the `SNAPSHOT` suffix.

### 7.3 Engine endpoints touched by the lifecycle (for completeness; the engine census is separate)

| Endpoint (relative to `engine.url`) | When |
|---|---|
| GET `/server/v1/currentUser` | Engine setup and identity fallback (V1_RemoteEngine.ts:277-287; LegendStudioBaseStore.ts:241-256) |
| GET `/pure/v1/protocol/pure/getClassifierPathMap`, `/getSubtypeInfo` | Graph manager initialize (V1_PureGraphManager.ts:747-766). Errors are swallowed and defaults used. |
| GET `/pure/v1/schemaGeneration/availableGenerations`, `/codeGeneration/availableGenerations`, `/pure/v1/external/format/availableFormats`, `/functionActivator/list`, `/pure/v1/relational/connection/supportedDbAuthenticationFlows` | Editor init (EditorStore.ts:959-962) |
| POST `/pure/v1/compilation/compile` | Form-mode compile (`compileGraph`, GraphEditFormModeState.ts:439) and text compile (after grammarToJson); dependency validation |
| POST `/pure/v1/grammar/grammarToJson/model`, `/jsonToGrammar/model` | Text mode toggle, review diff rendering, lazy text editor |
| POST `/sdlc/v1/createPrototypeProject`, GET `/sdlc/v1/userHasPrototypeProjectAccess/{user}` | Sandbox projects only |

---

## 8. Surprising things, configuration and authentication

### 8.1 Application config keys (`config.json`)

Defined in LS/application/LegendStudioApplicationConfig.ts:189-345, with base keys from LA/application/LegendApplicationConfig.ts.

| Key | Required | Meaning |
|---|---|---|
| `appName`, `env` | yes | `appName` appears in every auto-generated commit message (push, CR accept, configuration update, review description). |
| `sdlc.url` | yes | SDLC base. Defaults in dev bootstrap: `http://localhost:6100/api`, engine `http://localhost:6300/api`, depot `http://localhost:6200/depot/api`, query `http://localhost:9001/query` (legend-application-studio-bootstrap/scripts/setup.js). |
| `sdlc.baseHeaders` | no | Extra headers on every SDLC request |
| `sdlc.client` | no | Adds `client_name=` to every SDLC request and to `/auth/authorize` |
| `sdlc.enablePopupReAuth` | no | Popup re-auth on 401. Requires `<base>/popup-callback.html` to be registered on the SDLC OAuth client. |
| `sdlc.useCookieAuthOnly` | no | Never send a Bearer token to SDLC |
| `depot.url` | yes | |
| `engine.url`, `engine.queryUrl`, `engine.queryClientName`, `engine.useCookieAuthOnly` | url required | Can be overridden per page by `?editorConfig=` (EditorStore.ts:895-900) |
| `query.url` | no | Legend Query deep links (service query creator) |
| `showcase.url`, `pct.reportUrl`, `legendAI.url` | no | N |
| `documentation.*`, `application.storageKey`/`settingsOverrides`, `legendCookieDomain`, `enableTokenClient` | no | Base application |
| `extensions.core.*` | no | See 8.2 |

### 8.2 Feature flags (`extensions.core`, LegendStudioApplicationConfig.ts:73-187)

- `enableGraphBuilderStrictMode`: parsed (:79,151) but **never read** anywhere in LS. The editor's strict mode comes from the user *setting* `LEGEND_STUDIO_SETTING_KEY.EDITOR_STRICT_MODE` (EditorGraphState.ts:186-189).
- `typeAheadEnabled`
- `projectCreationGroupIdSuggestion`
- `NonProductionFeatureFlag`
- `TEMPORARY__preserveSectionIndex`: passed to `buildGraph` (EditorGraphState.ts:382-384)
- `TEMPORARY__enableLocalConnectionBuilder`
- `TEMPORARY__enableCreationOfSandboxProjects`
- `TEMPORARY__serviceRegistrationConfig[]`
- `queryBuilderConfig`
- `ingestDeploymentConfig`: contains the OIDC config used to wrap the whole app
- `dataProductConfig`
- `userSearchConfig`
- `reconciliationOidcConfig`
- `enableOauthFlow`: forces OIDC login before render

### 8.3 Authentication and user handling

- **SDLC**:
  - Auth is a session cookie obtained by a top-level redirect to `{sdlc}/auth/authorize`. The SDLC server does the GitLab OAuth.
  - There is no in-app login page. With `useCookieAuthOnly=false`, the shared `AbstractServerClient` token getter may add a Bearer header. The base store does not pass one, so I could not determine whether a token is injected for SDLC in a default deployment.
  - The user id comes from `GET /currentUser` and is set on the identity service and on the SDLC client, where it is used for trace tags.
  - SDLC is the identity source. The engine `currentUser` is only a fallback.
- Navigating to an SDLC-bypassed route skips SDLC entirely (`isSDLCAuthorized = undefined`), so the GAV viewer works without SDLC.
- Authorization is enforced client-side only for buttons (`authorizedActions`), and the default is permissive when the call fails.

### 8.4 Behaviours and bugs worth knowing for a rebuild

1. **No commit-message UI for pushes.** Every push message is generated (LocalChangesState.ts:427-435).
2. **Optimistic concurrency** comes from `revisionId` in `entityChanges`. A 409 means the user must refresh. There is no automatic rebase.
3. **Out-of-sync is detected lazily**, only when the Local Changes panel opens and at push time. There is no polling.
4. **Configuration saves are separate commits and fully reload the editor.** Unsaved entity edits are discarded after a warning.
5. **Update, conflict accept, discard and abort, Model Importer load, and review "recreate workspace" all do a full page reload.**
6. **The server deletes the workspace on review commit.** Studio offers to recreate a workspace with the same name.
7. **Versions are always cut from project HEAD** (`revisions/CURRENT` of the project), never from an arbitrary revision in the UI.
8. **A dependency on two versions of the same project is fatal.** It is computed in the browser after Depot returns, and the editor cannot build the graph.
9. **Snapshot dependencies** can be saved in a workspace but block review create and commit.
10. Group workspace is the **default** in the create-workspace modal.
11. Bug: "Back to workspace setup" in the workspace-not-found alert calls `generateSetupRoute(projectId, workspaceId, workspaceType)`. Its signature is `(projectId, patchReleaseVersionId, workspaceId?, workspaceType?)`, so the workspaceId ends up in the patch slot (EditorStore.ts:845 vs LegendStudioNavigation.ts:151-156).
12. Bug: `pushLocalChanges` → `processConflicts` shows a blocking alert in CR mode but does not abort (LocalChangesState.ts:778-797, 346).
13. Bug-ish: the workspace panel "Close review" calls `/reject`, while the review page calls `/close`.
14. Bug-ish: Approve is disabled by comparing `currentUser.userId` to `review.author.name` (ProjectReviewSideBar.tsx:220).
15. Patch-review route `PATCH_REVIEW` and `PREVIEW_BY_GAV_ENTITY` are defined but not routed. The setup page ignores the patch id in its URL. `getWorkspacesInConflictResolutionMode` ignores its patch arg.
16. Workspace type has no explicit field. It is inferred from `userId != null`.
17. The `Revision` JSON uses `authoredTimestamp`/`committedTimestamp`, which the client aliases to `authoredAt`/`committedAt` (SDLC-C/models/revision/Revision.ts).
18. `CreateProjectCommand` has no project type or structure version. Import collects description and tags but drops them.
19. The project-search UI wraps the text in double quotes (exact search) and, when the input parses as an id without a prefix, prefixes it with `PROD-`.
20. Change detection relies on client-side protocol hashing being stable between "entities from SDLC" and "live metamodel". Legend-lite must provide an equivalent that is stable under round-trip, otherwise phantom diffs appear.
21. Initial editor load is about 17 SDLC calls, mostly serialized in the first half. The project overview lists workspaces with N+1 calls per patch.

---

## 9. Not determined / open questions

- Whether the SDLC client receives a Bearer token in a standard (non-OIDC) deployment. The base store passes no `getAuthenticationToken`, and `LegendTokenSync` exists only under OIDC. I did not trace `AbstractServerClient` token wiring in legend-shared.
- Exact SDLC server semantics behind `/update`, `/conflictResolution/*`, `/entityChanges` with stale `revisionId`, the version-number computation, and whether a review commit always deletes the workspace. I only read the Studio side and the client comments. Defer to the SDLC census.
- The exact number of engine calls at load. I listed the call sites but did not trace every V1_RemoteEngine wrapper.
- Whether `GET /projects/{p}/versions/latest` returns 204 or `null` when no version exists. `ProjectViewerStore` deserializes it unconditionally (:177-181), which may fail on projects with no versions. Not verified.

---

<!-- Part B -->
# Part B — The editing experience and every engine call

Sources read:
- legend-studio @821c74c at `/Users/neema/legend/legend-lite-query/.scratch/legend-studio/packages`
  - `LS`  = `legend-application-studio/src`
  - `LG`  = `legend-graph/src`
  - `LCE` = `legend-code-editor/src`
  - `LQB` = `legend-query-builder/src` (embedded in Studio for lambda editors and query builder)
  - `EXT:<name>` = `legend-extension-<name>/src`
- legend-engine @230c159 at `/Users/neema/legend/legend-engine` (`LE` prefix), used to confirm endpoint shapes.

Conventions: `file:line` citations are relative to the prefixes above. Importance: **C** = core (needed for a first useful Studio), **I** = important (expected soon after), **N** = niche / later / enterprise-specific. Anything I could not confirm is flagged **UNCLEAR**.

---

## 0. Executive summary (most load-bearing facts)

1. **Studio holds the whole model client-side as a typed graph (legend-graph `PureModel`)**. Edits in form mode mutate that graph directly (MobX). The engine is stateless from Studio's point of view: every compile / execute / test call ships the **entire** model (PureModelContextData JSON, including dependency entities) to the engine (`LG graph-manager/protocol/pure/v1/V1_PureGraphManager.ts:5321-5342`, `:5509-5538`).
2. **Compilation is explicit, never automatic on keystroke or on save.** It runs on F9 / "Compile" button (`LS stores/editor/EditorStore.ts:452-465`, `LS components/editor/StatusBar.tsx:364-366`), after delete/rename of an element in form mode (`LS stores/editor/GraphEditFormModeState.ts:178-182`, `:219-223`), when leaving text mode (`LS stores/editor/GraphEditGrammarModeState.ts:623-675`) and a handful of other places (section 4.1). Push to SDLC (Ctrl+S) does **not** compile (`LS stores/editor/sidebar-state/LocalChangesState.ts:338-420`).
3. **Compile endpoint is a single `POST /api/pure/v1/compilation/compile` with PureModelContextData JSON body** in both modes. Text mode first calls `POST /api/pure/v1/grammar/grammarToJson/model?returnSourceInformation=true` with the raw text, then merges in dependency/generated elements and posts to `compile` (`LG .../v1/engine/V1_RemoteEngine.ts:667-728`). Engine returns **only one error** (HTTP 400 with `EngineException` JSON incl. `sourceInformation`) plus `defects[]` warnings on success.
4. **Text mode = whole-project grammar text**: on entering, Studio serialises the full graph to protocol JSON client-side and calls `POST /grammar/jsonToGrammar/model?renderStyle=PRETTY` (`GraphEditGrammarModeState.ts:733-759`, `V1_RemoteEngine.ts:358-366`). Comments and original formatting are not preserved across form/text round-trips (all text is regenerated from protocol JSON). On leaving, text is compiled and the resulting entities rebuild the graph.
5. **Form-mode lambda fields** (derived properties, constraints, function bodies, mapping transforms, service queries, ...) are small Monaco editors that debounce 1000 ms then call `grammarToJson/lambda` (with `sourceId` = coordinates of the field) and render with `jsonToGrammar/lambda/batch` (`LQB components/shared/LambdaEditor.tsx:872-880`, `LG V1_RemoteEngine.ts:429-450`, `:546-577`). Compilation errors are mapped back to fields by `sourceInformation.sourceId` (`LG graph-manager/action/SourceInformationHelper.ts:22-39`).
6. **Client-side "compiler" work is structural only**: the V1 graph builder (5 passes) deserialises entities, indexes elements, resolves element/type/property references, checks duplicates (strict mode turns some warnings into errors) — but **lambda bodies stay raw JSON** (`RawLambda`, `expressionSequence = protocol.body`), so no expression type-checking happens client-side (`LG .../to/V1_ElementSecondPassBuilder.ts:526-529`, `.../to/helpers/V1_DomainBuilderHelper.ts:104-115`). All semantic validation is the engine's.
7. **Autocomplete in whole-project text mode is 100% client-side** (parser section keywords, element snippets, inline function snippets; `LS components/editor/editor-group/GrammarTextEditor.tsx:1086-1129`). Only the per-field lambda editors for **function body** and **data product** call engine `POST /pure/v1/codeCompletion/completeCode` (`LS .../FunctionEditorState.ts:206-215`, `LS .../dataProduct/DataProductEditorState.ts:272-281`). Hover = static parser docs; go-to-definition = Ctrl/Cmd+B "Go To Element" which re-parses the text via `grammarToJson/model` to get element source positions (`GraphEditGrammarModeState.ts:257-313`).

---

## 1. Layout

Top-level layout in `LS components/editor/Editor.tsx:227-305`: `ActivityBar` | resizable `SideBar` | (vertical split: `EditorGroup` (form mode) or `GrammarTextEditor` (text mode) over `PanelGroup`) | optional `ShowcaseManager` side panel; then `QuickInput`, `StatusBar`, `ProjectSearchCommand` (Ctrl+P), `WorkspaceSyncConflictResolver` modal, `EmbeddedQueryBuilder` (full-screen overlay) and embedded DataCube viewer (`Editor.tsx:252-301`).

Mode switch at render: form mode renders `<EditorGroup/>`, text mode renders `<GrammarTextEditor/>` in the same slot (`Editor.tsx:252-259`).

### 1.1 Activity bar (`LS components/editor/ActivityBar.tsx:387-514`, enum `LS stores/editor/EditorConfig.ts:29-42`)

| Activity | Side bar component | Shortcut | Disabled when | Importance |
|---|---|---|---|---|
| Explorer | `Explorer` (`side-bar/SideBar.tsx:49-50`) | Ctrl+Shift+X | never | **C** |
| Test Runner | `GlobalTestRunner` (`SideBar.tsx:67-72`) | – | conflict resolution, lazy text mode (`ActivityBar.tsx:397`) | **I** |
| Local Changes | `LocalChanges` (`SideBar.tsx:51-52`) | Ctrl+Shift+G | conflict resolution | **C** (SDLC; other census) |
| Update Workspace | `WorkspaceUpdater` (`SideBar.tsx:55-56`) | Ctrl+Shift+U | conflict resolution | I (SDLC) |
| Review | `WorkspaceReview` (`SideBar.tsx:53-54`) | Ctrl+Shift+M | conflict resolution | I (SDLC) |
| Conflict Resolution | `WorkspaceUpdateConflictResolver` (`SideBar.tsx:57-58`) | – | only enabled in conflict-resolution mode | N |
| Project (overview) | `ProjectOverview` (`SideBar.tsx:59-60`) | – | conflict resolution | I |
| Workflow Manager | `WorkflowManager` (`SideBar.tsx:61-66`) | – | conflict resolution | N (GitLab pipelines) |
| Dev Mode (Beta) | `DevMetadataPanel` (`SideBar.tsx:73-74`) | – | conflict res., lazy text | N (lakehouse) |
| Register Service (Beta) | `RegisterService` bulk registration (`SideBar.tsx:75-82`) | – | conflict res., lazy text | N |
| End to End Workflows (Beta) | `EndToEndWorkflow` (`SideBar.tsx:83-90`) | – | unless `NonProductionFeatureFlag` (`ActivityBar.tsx:510-512`) | N |
| Plugin activities | `getExtraActivityBarItemConfigurations` (`ActivityBar.tsx:375-384`, `SideBar.tsx:91-98`) | – | plugin-defined | N |

Bottom of activity bar: "Open Showcases" (F7), color theme toggle, Settings cog menu (`ActivityBar.tsx:572-592`). Hamburger menu: About, See Showcases, Documentation, doc links, Help, Back to workspace setup (`ActivityBar.tsx:219-256`), plus "Show Developer Tool" (`ActivityBar.tsx:98-101`).

### 1.2 Editor tabs (`LS stores/editor/EditorTabManagerState.ts`)

- One tab per opened element; tab label = element name, path suffix shown when names collide (`LS components/editor/editor-group/EditorGroup.tsx:470-495`). Pinning supported (`EditorTabManagerState.ts:196-198`).
- Element → editor state mapping: `createElementEditorState` (`EditorTabManagerState.ts:274-356`), fallback `UnsupportedElementEditorState` (read-only JSON/grammar view). Full table in section 5.
- Each element tab has a view-mode switcher `Form | JSON | Grammar` (`EditorConfig.ts:55-59`, `EditorGroup.tsx:235-238`) plus per-element **file generation** and **external-format schema generation** views (`EditorGroup.tsx:239-283`). JSON and Grammar views are **read-only** (`LS components/editor/editor-group/element-generation-editor/ElementNativeView.tsx:51-56`).
- Non-element tabs: EntityDiffView, EntityChangeConflictEditor, ArtifactGenerationViewer (generated file), ModelImporter (F2), ProjectConfigurationEditor, QueryConnection end-to-end workflow (`EditorGroup.tsx:384-428`).
- After a graph rebuild, tabs are cached by element **path** and recreated against the new graph (`EditorTabManagerState.ts:228-260`, recoverTabs `:364+`), because editor states hold object references into the old graph.
- Empty state splash shows shortcuts (Ctrl+P open, Ctrl+Shift+N new, Ctrl+S push, F7 showcases, F8 text mode, F9 compile) (`EditorGroup.tsx:146-230`).

### 1.3 Panel group (bottom) (`LS components/editor/panel-group/PanelGroup.tsx:60-96`, enum `EditorConfig.ts:48-53`)

| Panel | Content | Importance |
|---|---|---|
| CONSOLE | **Empty placeholder** — `ConsolePanel` renders an empty `PanelContent` with a TODO (`panel-group/ConsolePanel.tsx:20-26`). Compile messages go to toast notifications instead. | N (don't copy) |
| PROBLEMS | List of `graphState.problems` = `[error, ...warnings]` (`LS stores/editor/EditorGraphState.ts:192-194`); stale banner when graph hash changed since last compile ("please run compilation (F9)") (`panel-group/ProblemsPanel.tsx:69-90`); click → `goToProblem` (text mode: moves cursor; form mode: no-op `GraphEditFormModeState.ts:574-576`); shows `[Ln x, Col y]` only in text mode (`ProblemsPanel.tsx:57-61`). | **C** |
| DEVELOPER TOOLS | Engine client config toggles, payload compression/debug, dump grammar (`panel-group/DevToolPanel.tsx`, calls `graphToPureCode` at `:78`, `protocolToPureCode` at `:104`) | N |
| SQL PLAYGROUND | Raw SQL against a relational connection, form mode only (`PanelGroup.tsx:89-95`) — calls `/pure/v1/utilities/database/executeRawSQL` (`LS stores/editor/panel-group/StudioSQLPlaygroundPanelState.ts:167`) | N |

Panel toggled by Ctrl+` (`LS __lib__/LegendStudioCommand.ts:68-71`).

### 1.4 Status bar (`LS components/editor/StatusBar.tsx`)

Left: project / workspace links (back to setup), local-changes and workspace-update indicators, OUTDATED flag, problems counter "errors (0/1) / warnings (n)" → opens Problems (`StatusBar.tsx:177-250`). Right: conflict-resolution accept, **Push local changes (Ctrl+S)** (`:298-309`), **Generate (F10)** (`:327-329`), **Clear generation entities** (`:346-348`), **Compile (F9)** (`:364-366`), Toggle panel (`:378-380`), **Toggle text mode (F8)** (`:394-396`), re-authenticate with SDLC (`:404-410`), toggle assistant (`:423-425`).

### 1.5 Keyboard commands (`LS __lib__/LegendStudioCommand.ts:19-90`, registered in `EditorStore.ts:451-556`)

| Key | Command | Action |
|---|---|---|
| Ctrl+S | sync-workspace | push local changes (`EditorStore.ts:522-530`) |
| Ctrl+Shift+N | create-new-element | open New Element modal |
| Ctrl+P | search-element | element quick-open |
| F2 | toggle-model-loader | open Model Importer tab |
| F7 | show-showcases | |
| F8 | toggle-text-mode | form ⇄ text |
| F9 | compile | `graphEditorMode.globalCompile()` |
| F10 | generate | `graphGenerationState.globalGenerate()` |
| Ctrl+` | toggle-panel-group | |
| Ctrl+Shift+X/G/M/U | sidebar explorer / local changes / review / updater | |
| Ctrl/Cmd+B (text mode only) | Go To Element | `GrammarTextEditor.tsx:910-917` |

Commands are suppressed while the embedded query builder is open (`EditorStore.ts:443-449`).

### 1.6 Editor-level modes

`EDITOR_MODE` = STANDARD, CONFLICT_RESOLUTION, REVIEW, VIEWER, LAZY_TEXT_EDITOR (`EditorConfig.ts:17-23`). Graph edit mode `GRAPH_EDITOR_MODE` = FORM, GRAMMAR_TEXT (`EditorConfig.ts:61-64`). LAZY_TEXT_EDITOR ("strict text mode") skips building the full graph: it fetches entities, builds a "light" index-only graph, and opens text mode directly (`EditorStore.ts:969-1061`; `LS stores/lazy-text-editor/LazyTextEditorStore.ts:58-67`).

### 1.7 Workspace initialisation (engine-relevant parts)

`EditorStore.initialize` (`EditorStore.ts:646-918`): fetch project/patch/workspace from SDLC → in parallel fetch revision and `graphManager.initialize` (engine client setup + `GET /api/server/v1/currentUser`, `LG V1_RemoteEngine.ts:277-287`) → `initializeSystem()` → `initStandardMode` (`EditorStore.ts:938-967`) which in parallel:
- fetches SDLC configuration, entities (`GET` SDLC entities), builds graph (section 8),
- **engine**: `GET /pure/v1/codeGeneration/availableGenerations` + `GET /pure/v1/schemaGeneration/availableGenerations` (file generation types), `GET /pure/v1/external/format/availableFormats`, `GET /functionActivator/list`, `GET /pure/v1/relational/connection/supportedDbAuthenticationFlows` (`EditorStore.ts:959-962`).
- Dependencies come from **Depot** (`collectDependencyEntities`), not the engine (`EditorGraphState.ts:709-835`).

---
## 2. Explorer tree

### 2.1 Trees shown (`LS components/editor/side-bar/Explorer.tsx:1240-1360`, `LS stores/editor/ExplorerTreeState.ts:70-79`)

| Tree | Content | Editable | Importance |
|---|---|---|---|
| Main project tree | All packages/elements of the workspace graph (`graph` root `ROOT`) | yes (context menu) | **C** |
| System tree | `SYSTEM_ROOT` elements (built-in system model, e.g. profiles `meta::pure::profiles::*`) — context menu disabled (`Explorer.tsx:1283-1299`) | no | I |
| Dependency tree | One root per dependency project, named `@dependency__<projectId>` (`LG graph/DependencyManager.ts:52`); `isContextImmutable` (`Explorer.tsx:1301-1314`) | no; context menu offers View Project / View SDLC Project only (`Explorer.tsx:805-818`) | **C** (read-only) |
| Generation tree | `MODEL_GENERATION_ROOT` elements produced by model generation (`Explorer.tsx:1315-1328`) | no | N |
| File generation tree | Generated files (`rootFileDirectory`) from F10 Generate (`Explorer.tsx:1330-1345`); opens `ArtifactGenerationViewerState` tab (form mode) or modal (text mode) (`GraphEditFormModeState.ts:595-599`, `GraphEditGrammarModeState.ts:761-763`) | no | N |
| Empty state | "Your workspace is empty… Open Model Importer" (`Explorer.tsx:1346-1360`) | – | I |

Read-only rule: anything not in the main graph is read-only (`LG graph/helpers/DomainHelper.ts:278-279`); editor tabs for those set `isReadOnly` (`LS .../element-editor-state/ElementEditorState.ts:132-133`).

Header buttons: Open Model Importer (F2), Project Configuration panel, New Element (Ctrl+Shift+N), Collapse All, Open Element (Ctrl+P) (`Explorer.tsx:1399-1449`).

### 2.2 Context menu (`Explorer.tsx:752-960`)

- On a **package** (editable): "New <type>" for every type, grouped by category (`Explorer.tsx:820-858`), plus Rename / Remove.
- On an **element**: type-specific actions, then Rename, Remove, View in Project, Copy Path, Copy Link, Copy SDLC Project Link (dependency):
  - Class: **Query…** (embedded query builder), **Generate Sample Data…** (client-side mock data, `LS stores/editor/utils/MockDataUtils.ts`) (`Explorer.tsx:877-885`)
  - Service: Query… (`:886-892`)
  - Relational connection: Execute SQL… (SQL playground panel), **Build Database…** (schema exploration wizard) (`:912-923`)
  - Database: Query…, **Build Models** (generate classes+mapping from DB) (`:924-934`)
  - DataProduct / IngestDefinition: Query…, Run SQL… ; any DataCube-supported element: Data Cube (BETA)… (`:866-911`)
  - Plugin items (`extraExplorerContextMenuItems`, `:935`).
- Context menu is **disabled in text mode** (`Explorer.tsx:1243-1245`). Clicking a node in text mode jumps the cursor to the element's source position (`GraphEditGrammarModeState.ts:765-774`).

### 2.3 Create / rename / move / delete

| Operation | Implementation | Engine call | Citation |
|---|---|---|---|
| Create element | `NewElementState.save()` → `resolvePackageAndElementName` → `graphEditorMode.addElement(element, packagePath, openAfterCreate=true)` → `graph_addElement` (client-side) → explorer reprocess → open tab → `handlePostCreateAction` (file/model generation elements are auto-added to the single GenerationSpecification, creating it if missing) | none | `LS stores/editor/NewElementState.ts:917-1036`, `:157-202`; `GraphEditFormModeState.ts:93-109` |
| Create in root package | Forbidden for non-package types ("Can't create elements for type other than 'package' in root package") | – | `NewElementState.ts:924-932` |
| Rename / move | Single "Rename" dialog that takes a **full path** (so rename == move). Validates non-empty, not top-level, valid path, unique (`Explorer.tsx:173-213`). Functions keep their signature suffix (`Explorer.tsx:218-224`). `graph.renameElement` re-indexes, packages recursively re-path children (`LG graph/PureModel.ts:741-750`, `LG graph/BasicModel.ts:968-1016`). References are object refs, so all referrers serialise with the new path automatically. Then **globalCompile** | compile | `GraphEditFormModeState.ts:185-224` |
| Delete | Closes tabs, deletes generated children, `graph_deleteElement`, plugin post-delete actions, then **globalCompile** (failure → redirect to text mode) | compile | `GraphEditFormModeState.ts:111-183` |
| DnD | Explorer nodes are drag *sources* only (drop into editors / text mode inserts the path, `GrammarTextEditor.tsx:937-973`); there is no drag-to-move in the tree | – | `Explorer.tsx:1015` |
| In text mode | `addElement`/`deleteElement`/`renameElement` are no-ops (`GraphEditGrammarModeState.ts:338-355`) — edit the text instead | – | |

### 2.4 "New element" types and drivers

Base list (`LS stores/editor/EditorStore.ts:1411-1452`, categories `:1282-1409`; enum `LS stores/editor/utils/ModelClassifierUtils.ts:50-93`). A *driver* is a mini-form in the New Element modal that collects extra input (`NewElementState.ts:204-771`, modal `LS components/editor/side-bar/CreateNewElementModal.tsx:560-597`).

| Type | Category | Driver / defaults | Form editor after create | Importance |
|---|---|---|---|---|
| Package | (always first) | – | – | **C** |
| Class | Model | `new Class(name)` | ClassEditor | **C** |
| Association | Model | `new Association(name)` | AssociationEditor | **C** |
| Enumeration | Model | – | EnumerationEditor | **C** |
| Profile | Model | – | ProfileEditor | **C** |
| Function | Model | return type `String[1]`, body = default basic raw lambda (empty string) (`NewElementState.ts:1060-1074`) | FunctionEditor | **C** |
| Measure | Model | – | **Unsupported** (text mode only) (`EditorTabManagerState.ts:294-295`) | N |
| Data | Model | `NewDataElementDriver`: embedded data type (default ExternalFormat) (`NewElementState.ts:734-771`) | DataElementEditor | I |
| Database | Store | empty `Database` | DatabaseEditor (read-only viewer + grammar) | **C** |
| FlatData store | Store | empty | Unsupported (text mode) | N |
| ServiceStore (ext) | Store | – | Unsupported (`EXT:store-service-store …Plugin.tsx:187-209`) | N |
| Connection | Query | `NewPackageableConnectionDriver`: pick store (default ModelStore) → connection kind: PureModel (JsonModelConnection w/ class), FlatData, Relational (+plugins) (`NewElementState.ts:279-520`) | ConnectionEditor | **C** |
| Runtime | Query | `NewPackageableRuntimeDriver`: LEGACY (EngineRuntime with chosen mapping) or LAKEHOUSE (`NewElementState.ts:216-277`) | RuntimeEditor | **C** |
| Mapping | Query | empty | MappingEditor | **C** |
| Service | Query | `NewServiceDriver`: mapping (required) + compatible runtime or "custom" embedded runtime; creates `PureSingleExecution` with stub lambda (`NewElementState.ts:586-668`) | ServiceEditor | **C** |
| DataProduct (beta) | Query | `NewLakehouseDataProductDriver` (`NewElementState.ts:522-569`) | DataProductEditor | N |
| Compute (beta) | Query | `NewComputeDriver` | ComputeEditor | N |
| DataSpace (ext) | Query | `NewDataProductDriver` from data-space-studio (`EXT:dsl-data-space-studio components/DSL_DataSpace_LegendStudioApplicationPlugin.tsx:189-228`) | DataSpaceEditor | I |
| Local connection (flag) | Query | creates Database + Snowflake local connection + Mapping + Runtime in one go (`NewElementState.ts:934-1019`) | – | N |
| File generation | Generation | `NewFileGenerationDriver`: type from engine-provided descriptions; scope = all top-level packages (`NewElementState.ts:670-711`) | FileGenerationEditor | N |
| Generation specification | Generation | only one allowed per project (`NewElementState.ts:713-732`) | GenerationSpecificationEditor | N |
| SchemaSet, Binding (ext, external format) | External Format | SchemaSet driver picks format (`LS components/extensions/DSL_ExternalFormat_LegendStudioApplicationPlugin.tsx:151-276`) | SchemaSetEditor / BindingEditor | N |
| Diagram (ext) | Model | `new Diagram(name)` (`EXT:dsl-diagram-studio components/DSL_Diagram_LegendStudioApplicationPlugin.tsx:106-178`) | DiagramEditor | I |
| Text (ext) | Other | – | TextElementEditor | N |
| Persistence, PersistenceContext (ext) | Other | – | **Unsupported** (text mode) (`EXT:dsl-persistence …Plugin.tsx:132-159`) | N |
| DataQuality validations (ext) | Other | driver | DQ editors | N |

Not creatable from the modal: function activators (SnowflakeApp, SnowflakeM2MUdf, HostedService, MemSQLFunction) — created from the Function editor "Activate function" button (`LS components/editor/editor-group/function-activator/FunctionEditor.tsx:1662`); IngestDefinition / Availability (lakehouse, via other flows).

### 2.5 Generated elements and file generation output

- Model generation: `GraphGenerationState.generateModels` iterates the single GenerationSpecification's `generationNodes`, calls `graphManager.generateModel` which delegates to **plugin-provided** `V1_getExtraModelGenerators` — none are registered in OSS packages at this commit (only the extension point `LG graph-manager/protocol/pure/extensions/DSL_Generation_PureProtocolProcessorPlugin_Extension.ts:33`), so OSS model generation throws "no compatible generator" if a model-generation node exists (`LG V1_PureGraphManager.ts:2476-2508`). Results are built into `graph.generationModel` (read-only tree) (`LS stores/editor/editor-state/GraphGenerationState.ts:365-430`; `EditorGraphState.ts:672-707`).
- File generation: F10 "Generate" → `generateArtifacts` → `DEPREACTED_generateFiles` (one `generateFile` call per FileGenerationSpecification in the generation spec) + optional `generation/generateArtifacts` (disabled by default, `enableArtifactGeneration=false`) (`GraphGenerationState.ts:267`, `:320-363`, `:432-470`, `:157-205`). Output displayed as a file tree in the explorer.

---

## 3. Form mode vs grammar ("text") mode

### 3.1 Classes

- `GraphEditorMode` abstract (`LS stores/editor/GraphEditorMode.ts:31-101`): `initialize, addElement, deleteElement, renameElement, getCurrentGraphHash, globalCompile, updateGraphAndApplication, mode, goToProblem, onLeave, cleanupBeforeEntering, handleCleanupFailure, openElement, openFileSystem_File, getGraphTextInputOption`.
- `GraphEditFormModeState` (`GraphEditFormModeState.ts:65-625`) — default (`EditorStore.ts:288`).
- `GraphEditGrammarModeState` (`GraphEditGrammarModeState.ts:71-775`) holds a single `GrammarTextEditorState` with `graphGrammarText`, `sourceInformationIndex: Map<elementPath, SourceInformation>`, `forcedCursorPosition`, `wrapText` (`LS stores/editor/editor-state/GrammarTextEditorState.ts:24-74`). Graph hash in text mode = hash of the text string (`GrammarTextEditorState.ts:51-53`).
- `GraphEditLazyGrammarModeState` (strict text mode, `LS stores/lazy-text-editor/LazyTextEditorStore.ts:58-67`) — subclass used by `EDITOR_MODE.LAZY_TEXT_EDITOR`.

### 3.2 Switching form → text (F8, `EditorStore.toggleTextMode` `EditorStore.ts:1247-1280`, `switchModes` `:1454-1514`)

1. Blocking alert "Switching to text mode…".
2. `formMode.onLeave()` — closes tabs (cached by path), clears SQL playground (`GraphEditFormModeState.ts:578-582`). **No compile is done first.**
3. `grammarMode.cleanupBeforeEntering()`:
   - normal: `graphManager.graphToPureCode(graph, {pretty:true, excludeUnknown:true})` → client-side graph→protocol transform of **own elements only** → `POST /api/pure/v1/grammar/jsonToGrammar/model?renderStyle=PRETTY` with PMCD JSON, `Accept: text/plain` (`GraphEditGrammarModeState.ts:747-757`; `LG V1_PureGraphManager.ts:1848-1871`; `LG V1_RemoteEngine.ts:358-366`; `LG V1_EngineServerClient.ts:551-563`).
   - after a graph-build failure: `entitiesToPureCode(stored workspace entities, {pretty:true})` (same endpoint) (`GraphEditGrammarModeState.ts:737-746`).
   - Unknown elements (`INTERNAL__UnknownElement`, types the client has no plugin for) are excluded from the text and re-appended to the entity list on compile so they are not deleted (`GraphEditGrammarModeState.ts:201-204`, `:511-514`, `:748`).
4. `grammarMode.initialize()`: swaps local-changes state to `TextLocalChangesState`, **stops change detection**, calls `pureCodeToEntities(text, {sourceInformationIndex})` → `POST /grammar/grammarToJson/model?returnSourceInformation=true` (text/plain body) to build the element → (startLine…) index and compute local changes (`GraphEditGrammarModeState.ts:176-255`; `LG V1_PureGraphManager.ts:1915-1942`; `LG V1_RemoteEngine.ts:378-427`). If entering as a fallback after compile/build failure, also runs a text-mode compile. Cursor jumps to the currently-open element.
5. Total engine calls to enter text mode: **jsonToGrammar/model + grammarToJson/model** (2 calls; plus compile only for fallbacks).

### 3.3 While in text mode

- One Monaco editor (language `pure`) for the whole project (`LS components/editor/editor-group/GrammarTextEditor.tsx:882-919`). Every keystroke sets `graphGrammarText` and clears markers; **no engine call on change** (`GrammarTextEditor.tsx:899-907`).
- F9 / "Compile" button → `globalCompile` (section 4.2). After successful compile the graph is rebuilt **light** (index only — `buildLightGraph`, `GraphEditGrammarModeState.ts:411-466`; `LG V1_PureGraphManager.ts:1311-1350`), explorer rebuilt from it, and **local changes are recomputed only at this point** (`GraphEditGrammarModeState.ts:544-553`). Local changes in text mode are recomputed only on enter and on compile (`ChangeDetectionState.computeLocalChangesInTextMode` callers: `GraphEditGrammarModeState.ts:207`, `:548`; `LocalChangesState.ts:883`, `:918`) — so un-compiled text edits are not visible to "push".
- Ctrl/Cmd+B "Go To Element": walks words backwards from the cursor to reconstruct a `a::b::C` path (max 30 words), then re-parses the full text via `grammarToJson/model?returnSourceInformation=true` to refresh the index and jumps to the element's start line (`GrammarTextEditor.tsx:713-794`, `GraphEditGrammarModeState.ts:257-313`).
- Hover: only static documentation for `###Parser` section headers and element keywords from plugin-provided docs (`GrammarTextEditor.tsx:985-1083`). No type info.
- Autocomplete: triggered on `#` and Ctrl+Space; **client-side only** — parser keywords (`###Pure`, `###Mapping`, `###Connection`, `###Runtime`, `###Relational`, `###Service`, `###GenerationSpecification`, `###FileGeneration`, `###Data` + plugins), element snippets per section (class blank / with property / with inheritance / with constraint, profile, enumeration, association, measure, function, runtime, Json/Xml/ModelChain/Relational connection, database, generation spec, service single/multi, mapping M2M/enum/relational, …), and inline function snippets (`let`, `cast`, `if`, `case`, `match`, `map`, `filter`, `fold`, `sort`, `in`, `slice`, `removeDuplicates`, `toOne`, `toOneMany`, `isEmpty`, `endsWith`, `startsWith`) (`GrammarTextEditor.tsx:1086-1129`, snippets imported at `:100-123`; `LCE PureLanguageCodeEditorSupport.ts:78-338`).
- Advanced menu: Wrap overflowing words (persisted setting), Auto-fold elements (plugin-defined keywords) (`GrammarTextEditor.tsx:1335-1368`, `:1133-1282`).
- Drop an explorer node into the editor → inserts the element path (`GrammarTextEditor.tsx:937-973`).
- Problems click → `setForcedCursorPosition(startLine,startColumn)` (`GraphEditGrammarModeState.ts:613-621`).

### 3.4 Switching text → form (`switchModes(FORM)`, `EditorStore.ts:1482-1508`)

1. `grammarMode.onLeave()`: blocking "Compiling graph before leaving text mode…", `compileText` (grammarToJson + compile, section 4.2). On success, stash `compilationResultEntities` (+ unknown elements) (`GraphEditGrammarModeState.ts:623-675`).
2. New `GraphEditFormModeState.initialize()`: if last compile succeeded → `updateGraphAndApplication(entities)`: create new graph, re-use dependencies, dispose old graph, **full client-side `buildGraph`**, rebuild generations, re-observe graph for change detection, precompute hashes, recover tabs (`GraphEditFormModeState.ts:66-91`, `:271-397`).
3. On compile failure: `handleCleanupFailure` moves cursor to the error; if the graph cannot be built, user must fix; otherwise offers **"Discard Changes"** (revert to last good graph) or **"Stay"** (`GraphEditGrammarModeState.ts:677-731`).

### 3.5 Automatic fallbacks into text mode

- Initial graph build failure that is not a dependency/deserialization/network error → "Can't build graph. Redirected to text mode for debugging" (`EditorGraphState.ts:485-525`).
- Form-mode compile error that the open element editor cannot reveal → "Compilation failed and error cannot be located in form mode. Redirected to text mode for debugging." (`GraphEditFormModeState.ts:497-543`). Only **Class, Function and Mapping** editors implement `revealCompilationError` (`ClassEditorState.ts:69`, `FunctionEditorState.ts:388`, `mapping/MappingEditorState.ts:1369`); everything else falls back to text mode.

### 3.6 Per-element text views (inside a form tab)

Each element tab has `Form | JSON | Grammar` (`EditorConfig.ts:55-59`). Both JSON and Grammar are **read-only** Monaco views (`ElementNativeView.tsx:51-56`):
- JSON = client-side `elementToEntity(element, {pruneSourceInformation:true}).content` pretty-printed (`ElementEditorState.ts:195-231`).
- Grammar = `entitiesToPureCode([entity], {pretty:true})` → `POST /grammar/jsonToGrammar/model?renderStyle=PRETTY` (`ElementEditorState.ts:233-258`).
There is **no per-element editable text mode** in form mode; editing as text means switching the whole project to text mode (the Unsupported editor offers "Edit in text mode", `UnsupportedElementEditor.tsx:48-70`). The Database editor has its own read-only GRAMMAR tab (`DatabaseEditorState.ts:51-54`).

### 3.7 Form-mode lambda fields (mini grammar editors)

All expression-valued fields are `LambdaEditor`s (`LQB components/shared/LambdaEditor.tsx`) backed by a `LambdaEditorState` (`LQB stores/shared/LambdaEditorState.ts:30-178`):
- Display: `lambdasToPureCode(Map<lambdaId, RawLambda>)` → `POST /grammar/jsonToGrammar/lambda/batch?renderStyle=STANDARD|PRETTY` (`LG V1_RemoteEngine.ts:429-450`, `V1_EngineServerClient.ts:579-591`). Expand/collapse toggles pretty rendering.
- Edit: on change, debounced **1000 ms** (`LambdaEditor.tsx:872-880`) → `pureCodeToLambda(fullLambdaString, lambdaId)` → `POST /grammar/grammarToJson/lambda?sourceId=<lambdaId>&returnSourceInformation=true` (text/plain) (`LG V1_RemoteEngine.ts:546-577`, `V1_EngineServerClient.ts:427-449`). Parser errors (HTTP 400 → `ParserError`) are shown inline and adjusted for the hidden prefix (e.g. `|` lambda prefix) (`LambdaEditorState.ts:93-139`). Compile errors are cleared on edit (`LambdaEditor.tsx:351-370`).
- `lambdaId` encodes coordinates `elementPath@<kind>@<name>@<uuid>` joined with `@` (`LG graph-manager/action/SourceInformationHelper.ts:22-39`; class derived property / constraint ids `ClassState.ts:66-73`, `:165-172`; function `FunctionEditorState.ts:112-114`). This is how a form-mode compilation error is routed back to the field (section 4.3).
- Relational operations (join/filter/view column/property mapping expressions) use the same pattern with `grammarToJson/relationalOperationElement` (`sourceId=operationId`, returnSourceInformation=true) and `jsonToGrammar/relationalOperationElement/batch` (`LG V1_RemoteEngine.ts:579-625`).

---

## 4. Compilation and diagnostics

### 4.1 When compile runs

| Trigger | Mode | Citation |
|---|---|---|
| F9 / status-bar Compile / text-mode header Compile | both | `EditorStore.ts:452-465`; `StatusBar.tsx:152`, `:364`; `GrammarTextEditor.tsx:870-872`, `:1317-1323` |
| After element delete | form | `GraphEditFormModeState.ts:178-182` |
| After element rename/move | form | `GraphEditFormModeState.ts:219-223` |
| Leaving text mode | text | `GraphEditGrammarModeState.ts:634-646` |
| Entering text mode as a failure fallback | text | `GraphEditGrammarModeState.ts:218-229` |
| Before opening the embedded query builder (unless `disableCompile`) — query builder is refused if compile fails or is skipped | form | `LS stores/editor/EmbeddedQueryBuilderState.ts:59-104` |
| After Database Builder updates a database | form | `LS .../connection/DatabaseBuilderState.ts:1092-1097` |
| Legacy mapping test with deep-fetch problem | form | `LS .../mapping/legacy/DEPRECATED__MappingTestState.ts:776` |
| Project dependency change validation: `compileEntities(dependency + project entities)` | – | `LS .../project-configuration-editor-state/ProjectDependencyEditorState.ts:682-698` |
| Schema validation of a single SchemaSet in an isolated mini-graph | form | `LS .../external-format/DSL_ExternalFormat_SchemaSetEditorState.ts:560-575` |
| **Not** on keystroke, **not** on save/push (Ctrl+S) | – | `LocalChangesState.ts:338-420` |

Concurrency guard: compile/generate/graph-update/mode-leave/init are mutually exclusive; a blocked compile records outcome `SKIPPED` and toasts "Please wait…" (`EditorGraphState.ts:235-277`; `GraphEditFormModeState.ts:407-414`).

### 4.2 Request shapes

**Form mode** (`GraphEditFormModeState.ts:402-572` → `LG V1_PureGraphManager.ts:2062-2101` → `LG V1_RemoteEngine.ts:629-665`):
- `POST {engine}/api/pure/v1/compilation/compile`, JSON (zlib-compressed when enabled, `V1_EngineServerClient.ts:776-787`).
- Body = `PureModelContextData`: `{ "_type":"data", "serializer"?:…, "origin"?:…, "elements":[ …own elements (keepSourceInformation=true)…, …generated elements…, …dependency entity contents… ] }` (`getFullGraphModelData`, `LG V1_PureGraphManager.ts:5321-5342`; compile context `:5509-5538`; raw dependency entity contents appended at serialisation `LG …/pureProtocol/V1_PureProtocolSerialization.ts:228-248`). Own elements exclude unknown elements (`excludeUnknown: true`).
- `keepSourceInformation: true` so lambdas edited in this session carry their `sourceInformation` with `sourceId = lambdaId` (comment at `GraphEditFormModeState.ts:434-437`).

**Text mode** (`GraphEditGrammarModeState.ts:315-326` → `LG V1_PureGraphManager.ts:2103-2144` → `LG V1_RemoteEngine.ts:667-728`):
1. `POST /api/pure/v1/grammar/grammarToJson/model?returnSourceInformation=true` with the whole text (`Content-Type: text/plain`). Parse error → HTTP 400 → `ParserError`.
2. Client merges the parsed PMCD with the compile context (generated + dependency elements) via `mergeObjects` and posts it to `POST /api/pure/v1/compilation/compile`.
3. Result entities come from the **parsed** JSON (not from the compile response). The element source-information index is extracted from each element's `sourceInformation` (`V1_RemoteEngine.ts:334-356`).

**Engine side** (`LE legend-engine-core/…/legend-engine-language-pure-compiler-http-api/…/compiler/api/Compile.java:68-110`): `@Path("pure/v1/compilation") @POST @Path("compile")`, consumes JSON or zlib, optional `?clientVersion=`. Success: `200 {"message":"OK","defects":[…]}` (`CompileResult.java:23-34`). Failure: `EngineException` → **400**, other → 500 (`Compile.java:250-255`), body = `ExceptionError` `{code, message, trace, sourceInformation, errorType, status, …}` (`LE …/shared/core/operational/errorManagement/ExceptionError.java:25-36`). grammarToJson/model defaults `returnSourceInformation=true` when omitted (`LE …/grammar/api/grammarToJson/GrammarToJson.java:57-60`); jsonToGrammar defaults `renderStyle=PRETTY` (`JsonToGrammar.java:69-70`).

Client error types: `V1_EngineError {message, errorType: COMPILATION|PARSER, sourceInformation, trace}` (`LG …/v1/engine/V1_EngineError.ts:22-43`). Warnings: `defects` filtered to `defectSeverityLevel == WARN`, shape `{defectTypeId, defectSeverityLevel, message, sourceInformation}` (`LG …/engine/compilation/V1_CompilationWarning.ts:25-45`; `V1_RemoteEngine.ts:637-646`).

### 4.3 How diagnostics are shown

- Model: `graphState.problems = [error?, ...warnings]` (`EditorGraphState.ts:126`, `:192-194`). **At most one error** (engine stops at first) plus N warnings. Warnings at `(0,0,0,0)` (i.e. from dependencies) are dropped (`EditorGraphState.ts:202-216`).
- Toasts: "Compiled successfully" / "Compilation succeeded with warnings" / "Compilation failed: <message>" (`GraphEditFormModeState.ts:457-469`, `:614-622`; `GraphEditGrammarModeState.ts:525-537`, `:583-591`).
- Status bar counter error(0/1)/warnings → Problems panel (`StatusBar.tsx:231-250`).
- Staleness: `areProblemsStale` = last-compiled graph hash ≠ current hash (form: change-detection graph hash; text: hash of text) (`EditorGraphState.ts:228-233`).
- Text mode squiggles: `setErrorMarkers` for the error and `setWarningMarkers` for warnings using `sourceInformation` line/col ranges; markers cleared on every edit (`GrammarTextEditor.tsx:1199-1233`, `:899-907`; `LCE CodeEditorUtils.ts:93-152`). Cursor is moved to the error start (`GraphEditGrammarModeState.ts:575-580`).
- Form mode: `extractSourceInformationCoordinates(error.sourceInformation)` → `coordinates[0]` = element path → open element → `editorState.revealCompilationError(error)`: Class matches `coordinates[1]` ∈ {constraint, derivedProperty} and the field's `lambdaId`, switches tab and sets the field's inline compilation error (`ClassEditorState.ts:69-110`); Function and Mapping similarly. Unrevealable → switch to text mode with cursor at error (`GraphEditFormModeState.ts:497-553`).
- "compile and skip": there is no "skip compile" option for push. The only options are `disableNotificationOnSuccess`, `openConsole`, `ignoreBlocking`, `suppressCompilationFailureMessage` (`GraphEditorMode.ts:72-78`), and the embedded query builder's `disableCompile` (`EmbeddedQueryBuilderState.ts:33-37`). Strict mode (setting `EDITOR_STRICT_MODE`) makes the **client** graph builder throw on duplicates etc. instead of warning (`EditorGraphState.ts:186-189`, section 8).

### 4.4 Code editor package (LCE)

- `setupPureLanguageService({extraKeywords})` registers language id `pure`, a Monarch tokenizer (keywords incl. `Class`, `Association`, `Enum`, `Measure`, `Profile`, `function`, `Mapping`, `Runtime`, `Connection`, `FileGeneration`, `GenerationSpecification`, `Data`, relational `Schema/Table/Join/View/primaryKey/groupBy/mainTable`, plus plugin keywords), brackets/auto-closing/folding config; tokens `keyword, identifier, operator, delimiter, parser, number, date, color, package, string, comment, language-struct, multiplicity, generics, property, parameter, variable, type, invalid` (`LCE PureLanguageService.ts:17-416`, `LCE PureLanguage.ts:17-42`). Syntax highlighting is regex-based (comment at `PureLanguageService.ts:76-88`: "only allows fairly very basic syntax-highlighting").
- Themes (`LCE CodeEditorTheme.ts:47-168`), marker helpers, `moveCursorToPosition`, `normalizeLineEnding` (`LCE CodeEditorUtils.ts`). Supported languages: plaintext, pure, json, java, markdown, sql, xml, yaml, graphql, python (`CodeEditorUtils.ts:231-242`).
- **No language server, no engine-backed semantic tokens, hover types or references.**

### 4.5 Engine-backed autocomplete (only in lambda editors)

- `LambdaEditor` registers a completion provider calling `lambdaEditorState.getCodeComplete(textUntilPosition)` (`LQB components/shared/LambdaEditor.tsx:95-140`). Default implementation returns empty (`LambdaEditorState.ts:174-177`). Overridden only by `FunctionDefinitionEditorState` (`LS .../FunctionEditorState.ts:206-224`), `DataProductEditorState` (`:272-281`) and the query builder.
- Call: `POST /api/pure/v1/codeCompletion/completeCode` body `{codeBlock, model: <full PMCD or pointer>, offset}` (offset sent as `-1`), excluding the function being edited from the model (`LG V1_PureGraphManager.ts:2176-2199`; `V1_EngineServerClient.ts:838-849`; input `LG …/engine/compilation/V1_CompleteCodeInput.ts:22-44`). Response `{completions:[{completion, display}], exception?}` (`LG graph-manager/action/compilation/Completion.ts:32-42`). Gated by "TypeAhead" toggle (`FunctionEditor.tsx:1196`, `applicationStore.config.options.typeAheadEnabled`).
- **UNCLEAR:** I could not find a `codeCompletion/completeCode` HTTP endpoint in legend-engine @230c159 (searched all `*.java` for `completeCode`/`codeCompletion`). A `Completer` exists only in the REPL module (`LE legend-engine-config/legend-engine-repl/legend-engine-repl-client/src/main/java/org/finos/legend/engine/repl/autocomplete/Completer.java`). The endpoint may live in a different deployment/version.

---
## 5. Element editors

Element → editor mapping: `EditorTabManagerState.createElementEditorState` (`EditorTabManagerState.ts:274-356`) + plugin creators; renderer switch `EditorGroup.tsx:284-372`.

Legend for "Engine calls": *render* = `jsonToGrammar/lambda/batch` (show lambdas), *parse* = `grammarToJson/lambda` (debounced edit, section 3.7). Every editor tab additionally offers read-only JSON (client-side) and Grammar (`jsonToGrammar/model`) views (section 3.6).

### 5.1 Core editors (needed for a first Studio)

| Element | Editor state / component | What it offers | Engine calls | Imp. |
|---|---|---|---|---|
| **Class** | `ClassEditorState` (`ClassEditorState.ts:39`), `UMLEditor`/`ClassEditor.tsx` | Tabs: Properties, Derived properties, Constraints, Super types, Tagged values, Stereotypes (`ClassEditor.tsx:1635-1641`, enum `UMLEditorState.ts:29-42`). Properties: name, type, multiplicity, aggregation; inherited and association-contributed properties shown read-only with "visit" (`ClassEditor.tsx:163-166`); property detail panel (`PropertyEditor.tsx:78`). Derived properties and constraints are lambda editors with parameters/return type (`ClassState.ts`). Supertypes add/remove. "Visit generation parent" for generated classes. Compilation error reveal into constraint/derived-property fields (`ClassEditorState.ts:69-110`). Class "Query…" opens embedded query builder; "promote query to function" (`uml-editor/ClassQueryBuilder.tsx:114`, `:408`). | render (`ClassState.ts:122`, `:213`, `:356`, `:394`), parse (`ClassState.ts:85`, `:179`); compile before Query… | **C** |
| **Enumeration** | `UMLEditorState`, `EnumerationEditor.tsx` | Values (each with its own stereotypes/tagged values), Tagged values, Stereotypes | none | **C** |
| **Profile** | `UMLEditorState`, `ProfileEditor.tsx` | Stereotypes list, Tags list | none | **C** |
| **Association** | `UMLEditorState`, `AssociationEditor.tsx` | Two properties (type, multiplicity), tagged values, stereotypes | none | **C** |
| **Function** | `FunctionEditorState` (`FunctionEditorState.ts:300`), `function-activator/FunctionEditor.tsx` | Tabs DEFINITION (parameters with type/multiplicity, return type & multiplicity, body lambda editor with optional engine type-ahead), TAGGED_VALUES, STEREOTYPES, TEST_SUITES, LAMBDAS (= list of activators pointing to this function) (`FunctionEditorState.ts:79-85`; `FunctionEditor.tsx:1551-1775`). Actions: **Run Function** (parameter modal if params), Generate Plan, Debug Plan, Lineage, Edit Query (embedded QB), Data Cube, **Activate function** (create SnowflakeApp/HostedService/… activator) (`FunctionEditor.tsx:1568-1662`). Error reveal for the body (`FunctionEditorState.ts:388`). | render (`:160`), parse (`:121`), `codeCompletion/completeCode` (`:206-224`), `execution/execute` (`:631`), `execution/generatePlan` (`:510`), `generatePlan/debug` (`:495`), `lineage/v1/function/fullAnalytics` (`:708`), `executionManager/cancelUserExecution` (`:688`), tests → `testable/runTests` (`TestableEditorState.ts`), `lambdaRelationType` for test data (`function-activator/testable/FunctionTestableState.ts:806`) | **C** |
| **Mapping** | `MappingEditorState` (`mapping/MappingEditorState.ts:663`), `mapping-editor/*` | Tabs CLASS_MAPPINGS, TEST_SUITES (`MappingEditorState.ts:155-158`). Left: mapping explorer (class/enum/association mapping elements, add/delete) (`MappingExplorer.tsx`, `NewMappingElementModal.tsx`). Inner tab manager for opened mapping elements. Class mapping kinds: Pure instance (M2M) with source class, FlatData, Relational (main table, property mappings as relational operation expressions), Operation (union/merge), Relation function, aggregation-aware; source selector modal (`InstanceSetImplementationSourceSelectorModal.tsx`); type trees for source (`TypeTree.tsx`, `RelationTypeTree.tsx`, `FlatDataRecordTypeTree.tsx`). Enumeration mappings. Mapping **execution** builder (query via embedded QB + input data → run, promote to test) (`MappingExecutionBuilder.tsx`). Test suites (store test data per store, query, tests, assertions) (`MappingTestableEditor.tsx`). Legacy tests + migration tool. Error reveal (`MappingEditorState.ts:1369`). | render/parse for M2M transforms (`PureInstanceSetImplementationState.ts:78`, `:109`, `:168`, `:205`, `:311`, `:338`) and flat-data (`FlatDataInstanceSetImplementationState.ts:87`, `:123`, `:232`); `grammarToJson/relationalOperationElement` + `jsonToGrammar/relationalOperationElement/batch` (`relational/RelationalInstanceSetImplementationState.ts:87`, `:126`, `:350`); `lambdaRelationType` (`RelationTypeTree.tsx:94`); `execution/execute` (`MappingExecutionState.ts:812`), plans (`:883`, `:899`), cancel (`:779`); `testable/runTests` (`testable/MappingTestableState.ts:709`, `:722`); `analytics/mapping/modelCoverage` (`MappingTestableState.ts:660`) | **C** |
| **Database (relational store)** | `DatabaseEditorState` (`DatabaseEditorState.ts:300`), `database-editor/*` | Tabs VIEW and GRAMMAR (`DatabaseEditorState.ts:51-54`). VIEW = read-only schema tree + diagram canvas of tables/views/joins with focus/fit/reset layout, search, annotations (`DatabaseDiagramCanvas.tsx`, `DatabaseSchemaTree.tsx`). Join/filter/view-column/group-by formulas rendered to text. **No form editing of tables/joins** — edit via text mode or the Database Builder wizard (explorer "Build Database…" on a connection: schema exploration → generates/updates `Database`), and "Build Models" (DB → classes + mapping). | `jsonToGrammar/relationalOperationElement/batch` (`DatabaseEditorState.ts:546`, `:593`, `:639`, `:684`); builder: `utilities/database/schemaExploration` (`connection/DatabaseBuilderState.ts:777`, `:843`, `:988`), preview `execution/execute` (`:880`), parse (`:870`); models: `relational/generateModelsFromDatabaseSpecification` + `jsonToGrammar/model` (`connection/DatabaseModelBuilderState.ts:102`, `:109`) | **C** (viewer) |
| **Connection** | `PackageableConnectionEditorState`, `connection-editor/ConnectionEditor.tsx` | Per connection kind: JsonModelConnection / XmlModelConnection / ModelChainConnection (class + URL), FlatDataConnection, RelationalDatabaseConnection with tabs General / Store / Post Processors (`ConnectionEditorState.ts:84-88`). Datasource specs: static, h2Local, h2Embedded, databricks, snowflake, redshift, bigQuery, spanner, Trino (`:91-101`); auth strategies: delegatedKerberos, h2Default, snowflakePublic, gcpApplicationDefaultCredentials, apiToken, oauth, userNamePassword, gcpWorkloadIdentityFederation, middleTierUserNamePassword, TrinoDelegatedKerberos (`:108-119`); post-processor: Mapper (`:103-105`). Plugins add more (service store, external format connections). | none for the form itself; DB type/auth catalogue from `relational/connection/supportedDbAuthenticationFlows` at init (`EditorGraphState.ts:309-323`); SQL playground → `utilities/database/executeRawSQL` | **C** |
| **Runtime** | `PackageableRuntimeEditorState` (`RuntimeEditorState.ts:1021`), `RuntimeEditor.tsx` | EngineRuntime: mappings list, connections grouped per store (identified connections, pointer or embedded) (`RuntimeEditorState.ts:363-650`); LakehouseRuntime (environment/connection) (`:897-988`). | none | **C** |
| **Service** | `ServiceEditorState`, `service-editor/*` | Tabs GENERAL, EXECUTION, TEST, REGISTRATION, POST_VALIDATION (`ServiceEditorState.ts:55-61`). General: URL pattern (+path params), owners (deployment/user-list ownership), documentation, Auto Activate Updates, MCP Server (`ServiceEditor.tsx:422`, `:557-561`, `:914`). Execution: single vs multi (keyed) execution, mapping + runtime (pointer or custom embedded), query lambda editor, Edit Query (embedded QB), Run Query (param modal), plan/debug/lineage, import query from Query store, Data Cube (`ServiceExecutionEditor.tsx`, `ServiceExecutionQueryEditor.tsx:319-475`). Test: suites, shared test data (generate from query), parameters, assertions. Registration: env, execution mode (FULL/SEMI interactive / PROD), version, register + activate. Post-validation: assertions with lambdas, run validation. | render/parse (`ServiceExecutionState.ts:304`, `:335`), `execution/execute` (`:672`), plans (`:575`, `:590`), lineage (`:757`), cancel (`:735`), `pure/v1/query/{id}` + `prettyLambdaContent` (`:293-315`), `testable/runTests` (`testable/ServiceTestableState.ts:216`, `:280`), test-data generation `execution/testDataGeneration/generateTestData_WithDefaultSeed`/`_WithSeed` (`testable/ServiceTestDataState.ts:435`, `:578`) and `testData/generation/DONOTUSE_generateTestData` (`:717`), registration `server/v1/info/services` + `service/v1/register*` + `service/v1/serviceMetadata/{pattern}` + `service/v1/id/{id}` + `service/v1/generation/setActive/id/{id}` (`ServiceRegistrationState.ts:311`, `:378`, `:394`), post-validation `service/v1/doValidation` (`ServicePostValidationState.ts`) | **C** (execution/test), registration I |
| **DataSpace** (ext) | `DataSpaceEditorState` (`EXT:dsl-data-space-studio stores/DataSpaceEditorState.ts`), `DataSpaceEditor.tsx` | General editor sections: home (title/description, optional Legend-AI doc suggestion), elements, executables (templates + sample values), diagrams, support info, execution contexts (mapping + runtime + default), validation; Preview (rendered data-space viewer); Query action (embedded QB) | `pure/v1/analytics/dataSpace/render` (`EXT:dsl-data-space stores …/V1_DSL_DataSpace_PureGraphManagerExtension.ts:237-241`, called from `DataSpacePreviewState.ts:118`); render/parse for executable templates (`DataSpaceExecutableTemplateState.ts:69`, `:100`); `lambdaRelationType` for sample values (`DataSpaceExecutableSampleValuesState.ts:78`); `jsonToGrammar/model` for AI suggestion (`DataSpaceHomeTab.tsx:94`) | **C** (per brief) / I |

### 5.2 Later / niche editors

| Element | Editor | Offers | Engine calls | Imp. |
|---|---|---|---|---|
| Data (DataElement) | `PackageableDataEditorState`, `data-editor/DataElementEditor.tsx` | Embedded data kinds: ExternalFormat (content-type + text), ModelStore, Relational CSV (tables editor), DataElement reference, RelationElements (`LS stores/editor/editor-state/ExternalFormatState.ts:34-40`; `data-editor/*`) | none | I (needed for tests) |
| File generation | `FileGenerationEditorState`, `element-generation-editor/FileGenerationEditor.tsx` | Type-specific config properties (from engine descriptions), scope elements, live preview of generated files (debounced 500 ms) (`FileGenerationEditor.tsx:976-977`) | `pure/v1/{codeGeneration|schemaGeneration}/{type}` with **PureModelContextText** built via `jsonToGrammar/model` (`LG V1_RemoteEngine.ts:1090-1116`; `LS stores/editor/editor-state/FileGenerationState.ts:223-243`) | N |
| Per-element file/schema generation view | `ElementFileGenerationState`, `ElementXTSchemaGenerationState` | Any element tab can show e.g. Avro/JSON-schema output for that element (`EditorGroup.tsx:239-283`) | `generateFile`; `external/format/generateSchema` (`ElementExternalFormatGenerationState.ts:127`) | N |
| Generation specification | `GenerationSpecificationEditorState`, `GenerationSpecificationEditor.tsx` | Ordered model-generation nodes + file generations; only one allowed | (via F10) | N |
| SchemaSet / Binding (ext, external format) | `SchemaSetEditorState`, `BindingEditorState` | Schemas (content), model generation from schema, validate schema (isolated compile) | `external/format/generateModel` + `jsonToGrammar/model` (`DSL_ExternalFormat_SchemaSetEditorState.ts:274`; `LG V1_RemoteEngine.ts:1130-1141`), `grammarToJson/model` (`:330`), `compilation/compile` (`:568`) | N |
| Diagram (ext) | `DiagramEditorState`, `EXT:dsl-diagram-studio components/DiagramEditor.tsx:1458` | Canvas: add class, add relationship (inheritance/property), layout, pan, zoom (`EXT:dsl-diagram components/DiagramRenderer.ts:174-189`); side panel with embedded class editor (`stores/DiagramEditorState.ts:105-115`) | none (pure client) | I |
| Function activators: SnowflakeApp, SnowflakeM2MUdf, HostedService, MemSQLFunction, unknown | `function-activator/*EditorState.ts` | Activator config form, Validate, Render artifact, Publish to sandbox, tests (`FunctionTestableState`) | `functionActivator/validate`, `/renderArtifact`, `/publishToSandbox`, list at init `functionActivator/list` (e.g. `SnowflakeAppFunctionActivatorEditorState.ts:122`, `:141`; `SnowflakeM2MUdfFunctionActivatorEditorState.ts:112`, `:131`, `:147`; `HostedServiceFunctionActivatorEditorState.ts:187`, `:206`) | N |
| DataProduct / Compute / Availability / IngestDefinition (lakehouse, beta) | `dataProduct/*`, `compute/*`, `availability/*`, `ingest/*` | Lakehouse-specific forms, access points (lambdas), tests | `lambdaRelationType` (+ `/batch`), `codeCompletion/completeCode`, `generation/generateArtifacts`, `lineage`, lakehouse ingestion APIs (`DataProductEditorState.ts:179-945`; `IngestDefinitionEditorState.ts:156`, `:196`) | N |
| Measure, FlatData store, ServiceStore, Persistence, PersistenceContext, unknown elements | `UnsupportedElementEditorState` | "Can't display this element in form-mode" + "Edit in text mode" (`UnsupportedElementEditor.tsx:48-110`) | none | N |
| Text (ext) | `TextEditorState`, `TextElementEditor` | Markdown/plain text body | none | N |
| DataQuality validations (ext) | DQ editors | DQ config, run, profile, suggestions | `pure/v1/dataquality/*` (`EXT:dsl-data-quality graph-manager/protocol/pure/v1/V1_DSL_Data_Quality_PureGraphManagerExtension.ts:221-744`) | N |
| Project configuration (not an element) | `ProjectConfigurationEditorState` | Dependencies (validated by `compileEntities`), platform/version config | `compilation/compile` (`ProjectDependencyEditorState.ts:696`) | I |

---

## 6. Running things in Studio

### 6.1 Function run (`FunctionEditorState.ts:593-698`)
1. Re-parse body (`grammarToJson/lambda`) to be sure the protocol is current.
2. If the function has parameters → parameter values modal.
3. `graphManager.runQuery(lambda, mapping=undefined, runtime=undefined, graph, {useLosslessParse:false, parameterValues})` → `POST /api/pure/v1/execution/execute?serializationFormat=…` with `V1_ExecuteInput {clientVersion, function: RawLambda, mapping?, runtime?, model: full PMCD, context, parameterValues[]}` (`LG V1_PureGraphManager.ts:3092-3146`, `:2869-2990`; `LG …/engine/execution/V1_ExecuteInput.ts:47-75`; engine `LE legend-engine-core-query-pure-http-api/…/query/pure/api/Execute.java:176-215`). Function bodies are expected to carry their own `from(mapping, runtime)` / relation sources.
4. Cancel: `DELETE /api/server/v1/executionManager/cancelUserExecution?userID=…&broadcastToCluster=true` (`V1_EngineServerClient.ts:969-981`).
5. Result: TDS/relation/JSON result grid; execution errors come back as `V1_ExecutionError` (HTTP error payload) (`LG V1_RemoteEngine.ts:849-891`).

### 6.2 Mapping execution (`MappingExecutionState.ts`)
- Query built in the embedded query builder (or lambda editor) against the mapping's target class.
- Input data → ad-hoc runtime: M2M = `JsonModelConnection` with the JSON encoded as a data URL (base64 if `useBase64ForAdhocConnectionDataUrls`) (`MappingExecutionState.ts:281-345`); relational = `RelationalDatabaseConnection` H2 `LocalH2DatasourceSpecification` with setup SQL or CSV (`:415-470`); flat data = flat-data connection with inline text (`:358-413`).
- `runQuery(query, mapping, runtime, graph, {useLosslessParse:true})` → `execution/execute` (`MappingExecutionState.ts:785-830`). Plan/debug plan: `execution/generatePlan[/debug]`.
- "Promote to test": creates a mapping test suite with store test data (from the input) and an `EqualToJson` assertion pre-filled with the result (`MappingExecutionState.ts:227-243`, `:321-345`).

### 6.3 Service execution
`runQuery(query, executionContext.mapping, executionContext.runtime, graph, {useLosslessParse:true, parameterValues})` → `execution/execute` (`ServiceExecutionState.ts:656-730`). Multi-execution: choose key/context first.

### 6.4 Tests (testable framework)
- Testables in the global runner: services, mappings, data products, ingests, availabilities + plugin collectors; **functions are excluded** ("re-add functions once function test runner has been completed in backend") (`LG graph/BasicModel.ts:279-300`). Functions do have a TEST_SUITES tab in their editor.
- Per element (`TestableEditorState.ts`): run test (`:347`), debug test (`:392`), run suite (`:456`, `:807`), run failing tests (`:511`, `:697`), run whole testable (`:749`). Test tabs SETUP / ASSERTION (`:174-177`). Assertion kinds: EqualTo, EqualToJson, EqualToTDS, EqualToRelation (`LG graph/metamodel/pure/test/assertion/*`). **"Generate expected"** runs the test and copies the actual value out of the `AssertFail` (`TestAssertionState.ts:460-520`).
- Global Test Runner side panel (`LS stores/editor/sidebar-state/testable/GlobalTestRunnerState.ts:615-813`): tree testable → suite → test → assertion, run all, run per node, dependency testables panel, failure viewer.
- Engine: `POST /api/pure/v1/testable/runTests` body `V1_RunTestsInput {model: full PMCD (DEV client version), testables: [{testable: <elementPath>, unitTestIds: [{testSuiteId?, atomicTestId}]}]}` (`LG V1_PureGraphManager.ts:2540-2585`; `LE legend-engine-core-testable/legend-engine-testable-http-api/…/TestableApi.java:58-101`, `RunTestsInput.java:23-29`, `RunTestsTestableInput.java:22-28`). Response `{results:[…]}` with `_type` `testExecuted` (`testExecutionStatus` PASS/FAIL, `assertStatuses[]` of `assertPass` / `assertFail` / `equalToJsonAssertFail` (expected/actual) / `equalToRelationAssertFail`), `testError` (`error` string), or `multiExecutionTestResult` (`LG …/serializationHelpers/V1_TestSerializationHelper.ts:86-114`; `LG graph/metamodel/pure/test/result/TestResult.ts:20-44`). Debug: `POST /testable/debugTests` → `testExecutionPlanDebug` (plan + debug text).

### 6.5 Embedded query builder (`EmbeddedQueryBuilderState.ts:39-118`)
- Opened from: Class "Query…", Service query, Mapping execution / tests, Function "Edit Query", DataProduct, accessors, DataSpace query actions, end-to-end workflow (list of callers: `MappingTestableEditor.tsx`, `MappingExecutionBuilder.tsx`, `ServiceExecutionQueryEditor.tsx`, `FunctionEditor.tsx`, `uml-editor/ClassQueryBuilder.tsx`, `dataProduct/DataProductEditor.tsx`, `accessor/AccessorQueryBuilderHelper.tsx`, `EXT:dsl-data-space-studio components/DataSpaceQueryAction.tsx`, …).
- Form mode only; **compiles the whole graph first** (refuses to open on failure) unless `disableCompile`.
- Query builder engine calls (from `LQB`): `execution/execute`, `generatePlan[/debug]`, `compilation/lambdaReturnType`, `compilation/lambdaRelationType`, `grammarToJson/lambda`, `grammarToJson/valueSpecification[/batch]`, `jsonToGrammar/lambda[/batch]`, `jsonToGrammar/valueSpecification[/batch]`, `analytics/mapping/modelCoverage`, `analytics/store-entitlement/*`, `lineage`, query store CRUD (counts of `graphManager.*` calls in LQB: `runQuery` ×6, `getLambdaRelationType` ×4, `getLambdaReturnType` ×3, `analyzeMappingModelCoverage` ×1, …; enumerated by grep over `legend-query-builder/src`). Details belong to the Query census.

### 6.6 Generation
- F10: model generation (plugin generators; none in OSS) then file generations via `pure/v1/{codeGeneration|schemaGeneration}/{type}` (section 2.5). Engine file-generation descriptions: `GET /pure/v1/codeGeneration/availableGenerations` and `GET /pure/v1/schemaGeneration/availableGenerations` (`LG V1_RemoteEngine.ts:1065-1088`).
- In text mode, generation inputs use the text: `getGraphTextInputOption()` returns `{graphGrammar}` (requires last compile SUCCEEDED) and the PMCD is built by `grammarToJson/model` + merge (`GraphEditGrammarModeState.ts:357-366`; `LG V1_PureGraphManager.ts:2423-2433`; `V1_RemoteEngine.ts:729-743`).
- Artifact generation `POST /pure/v1/generation/generateArtifacts` `{model, elementPaths}` (`LG V1_PureGraphManager.ts:2435-2448`) — off by default for global generate; used by DataProduct/Ingest/SQL playground.

### 6.7 Lambda return/relation type
- `POST /pure/v1/compilation/lambdaReturnType` `{model, lambda}` → `{returnType}` (engine `Compile.java:112-140`). In Studio proper only used via the query builder; batched client-side by firing one request per lambda (`LG V1_PureGraphManager.ts:2201-2258`, comment "Engine currently does not support batching of lambda return types").
- `POST /pure/v1/compilation/lambdaRelationType` → relation columns `{columns:[{name, genericType, multiplicity}]}`; `/lambdaRelationType/batch` `{model, lambdas:{id:lambda}}` → `{results, errors}` (`Compile.java:142-210`; `LG V1_RemoteEngine.ts:779-834`). Used by mapping relation-function editor, data product, function tests, data space sample values.

---
## 7. Engine endpoint inventory (from `LG …/v1/engine/V1_EngineServerClient.ts`)

Notes on transport (`V1_EngineServerClient.ts:241-317`; `legend-shared/src/network/NetworkUtils.ts:34-60`, `:482-492`):
- `baseUrl` = configured engine URL (Studio passes `editorConfig.engineServerUrl ?? config.engineServerUrl`, `EditorStore.ts:894-906`; the engine's Jersey resources are normally served under `/api` — the exact base string is deployment config, **UNCLEAR** from source alone). Paths below are relative to `baseUrl`.
- Requests marked `{enableCompression:true}` are zlib-compressed with `Content-Type: application/zlib` when compression is on (Studio turns it on, `EditorStore.ts:902`); engine endpoints accept `APPLICATION_ZLIB`.
- Auth: bearer token or cookie-only mode (`useCookieAuthOnly`); optional `client_name` query param for query-server URLs and current-user calls (`:252-313`).
- Error convention: HTTP 400 with `EngineException` JSON (`message`, `errorType`, `sourceInformation`, `trace`) → mapped to `ParserError` / `CompilationError` / `ExecutionError` (`V1_RemoteEngine.ts:412-426`, `:650-664`, `:870-890`).

Column "Studio?" = **E** used while editing in Studio, **S** Studio but outside editing (setup/registration/admin), **Q** only Query / DataCube / Marketplace apps or query builder internals, **–** no caller found in the monorepo (grep over `packages/*/src`, excluding lib/tests).

| # | Client method (line) | Method & path | Request | Response | Studio caller(s) / when | Studio? |
|---|---|---|---|---|---|---|
| 1 | `getCurrentUserId` (:322) | GET `server/v1/currentUser` | – | user id string | engine setup at workspace init (`V1_RemoteEngine.ts:277-287`) | E |
| 2 | `getTerminals`, `getTerminalById` (:334-345) | GET `user/marketplace/terminals[/{id}]` | – | TDS | marketplace only | – |
| 3 | `getLambdaPrefixes` (:350) | GET `lambda/v1/lambdaPrefixes` | – | prefixes | no caller found; endpoint not found in engine @230c159 | – |
| 4 | `getClassifierPathMap` (:355) | GET `pure/v1/protocol/pure/getClassifierPathMap` | – | `[{type, classifierPath}]` | graph manager init; maps protocol `_type` → classifier path for unknown elements (`V1_PureGraphManager.ts:744-768`; `LE …/protocol/pure/v1/model/PureProtocol.java:41-73`) | E |
| 5 | `getSubtypeInfo` (:358) | GET `pure/v1/protocol/pure/getSubtypeInfo` | – | `{functionActivatorSubtypes, storeSubtypes}` (client falls back to a hard-coded list on failure `V1_RemoteEngine.ts:314-330`) | graph manager init | E |
| 6 | `transformTdsToRelation_lambda` (:361) | POST `pure/v1/compilation/autofix/transformTdsToRelation/lambda` | lambda JSON | lambda JSON | DataCube app only | Q |
| 7 | `createPrototypeProject`, `validUserAccessRole` (:378-399) | POST `sdlc/v1/createPrototypeProject`; GET `sdlc/v1/userHasPrototypeProjectAccess/{user}` | – | project / bool | workspace setup sandbox (`LS stores/workspace-setup/WorkspaceSetupStore.ts:328`, `:670`) | S |
| 8 | `grammarToJSON_model` (:405) | POST `pure/v1/grammar/grammarToJson/model?sourceId&lineOffset&returnSourceInformation` | **text/plain** Pure grammar | PMCD JSON (elements incl. `SectionIndex`) / 400 ParserError | enter text mode, go-to-element, text compile, model importer (grammar), conflict editor, showcases, schema set editor (`GraphEditGrammarModeState.ts:191`, `:260`; `V1_RemoteEngine.ts:673-677`) | **E** |
| 9 | `grammarToJSON_lambda` (:427) | POST `…/grammarToJson/lambda?sourceId&lineOffset&columnOffset&returnSourceInformation` | text/plain lambda | RawLambda JSON / 400 | every form lambda editor (debounced), service import query, prettify (`V1_RemoteEngine.ts:539-577`) | **E** |
| 10 | `grammarToJSON_lambda_batch` (:451) | POST `…/grammarToJson/lambda/batch` | `{id: {value, returnSourceInformation?, sourceInformationOffset?}}` | `{result:{id:lambda}, errors:{id:ParserError}}` | no caller found | – |
| 11 | `grammarToJSON_valueSpecification` (:483) | POST `…/grammarToJson/valueSpecification` | text/plain | ValueSpecification JSON | query builder (`pureCodeToValueSpecification`) | Q |
| 12 | `grammarToJSON_valueSpecification_batch` (:467) | POST `…/grammarToJson/valueSpecification/batch` | as #10 | `{result, errors}` | query builder | Q |
| 13 | `grammarToJSON_relationalOperationElement` (:507) | POST `…/grammarToJson/relationalOperationElement?sourceId&returnSourceInformation=true` | text/plain | RawRelationalOperationElement JSON | relational class-mapping property editors (`relational/RelationalInstanceSetImplementationState.ts:87`) | **E** |
| 14 | `grammarToJSON_relationalOperationElement_batch` (:531) | POST `…/relationalOperationElement/batch` | batch | `{result, errors}` | no caller found | – |
| 15 | `JSONToGrammar_model` (:551) | POST `pure/v1/grammar/jsonToGrammar/model?renderStyle=STANDARD|PRETTY` (Accept text/plain) | PMCD JSON | Pure grammar text | enter text mode, element Grammar view, file generation (PMC text), external-format model generation, DB model builder, diff/conflict views, dev tools (`V1_RemoteEngine.ts:358-376`) | **E** |
| 16 | `JSONToGrammar_lambda` (:565) | POST `…/jsonToGrammar/lambda?renderStyle` | RawLambda JSON | text | `lambdaToPureCode`, `prettyLambdaContent` (`V1_RemoteEngine.ts:523-544`) | **E** |
| 17 | `JSONToGrammar_lambda_batch` (:579) | POST `…/jsonToGrammar/lambda/batch?renderStyle` | `{id: lambda}` | `{id: text}` | every lambda editor display (`V1_RemoteEngine.ts:429-450`) | **E** |
| 18 | `JSONToGrammar_valueSpecification[_batch]` (:593-619) | POST `…/jsonToGrammar/valueSpecification[/batch]` | JSON | text / map | query builder | Q |
| 19 | `JSONToGrammar_relationalOperationElement` (:621) | POST `…/jsonToGrammar/relationalOperationElement` | JSON | text | no caller found | – |
| 20 | `JSONToGrammar_relationalOperationElement_batch` (:635) | POST `…/jsonToGrammar/relationalOperationElement/batch?renderStyle=STANDARD` | `{id: op}` | `{id: text}` | Database editor formulas, relational mapping (`V1_RemoteEngine.ts:579-597`) | **E** |
| 21 | `runTests` (:651) | POST `pure/v1/testable/runTests` | `{model, testables:[{testable, unitTestIds:[{testSuiteId, atomicTestId}]}]}` | `{results:[testExecuted|testError|multiExecutionTestResult]}` | test editors + global runner (section 6.4) | **E** |
| 22 | `debugTests` (:664) | POST `pure/v1/testable/debugTests` | same | debug results (plan + text) | test editors (`TestableEditorState.ts:278`) | E |
| 23 | `getAvailableExternalFormatsDescriptions` (:681) | GET `pure/v1/external/format/availableFormats` | – | `[{name, contentTypes, supportsSchemaGeneration, supportsModelGeneration, …}]` | init (`EditorStore.ts:960`) | E |
| 24 | `generateModel` (:685) | POST `pure/v1/external/format/generateModel` | `{model, config…}` | PMCD | SchemaSet "generate model" (`DSL_ExternalFormat_SchemaSetEditorState.ts:274`) | N/E |
| 25 | `generateSchema` (:698) | POST `pure/v1/external/format/generateSchema` | `{model, config…}` | PMCD (schema set) | element external-format schema view (`ElementExternalFormatGenerationState.ts:127`) | N/E |
| 26 | `getAvailableCodeImportDescriptions`, `getAvailableSchemaImportDescriptions` (:712-720) | GET `pure/v1/codeImport/availableImports`, `pure/v1/schemaImport/availableImports` | – | descriptions | no caller found | – |
| 27 | `getAvailableCodeGenerationDescriptions` (:723), `getAvailableSchemaGenerationDescriptions` (:756) | GET `pure/v1/codeGeneration/availableGenerations`, `pure/v1/schemaGeneration/availableGenerations` | – | `[{key, label, properties[], generationMode}]` | init (`EditorStore.ts:959`; `V1_RemoteEngine.ts:1065-1088`) | E |
| 28 | `generateFile` (:726) | POST `pure/v1/{codeGeneration|schemaGeneration}/{type}` | `{clientVersion, model: PureModelContextText{code}, config:{scopeElements,…}}` | `[{content, fileName, format}]` | File generation editor preview, F10 (`V1_RemoteEngine.ts:1090-1116`) | N/E |
| 29 | `generateAritfacts` (:741) | POST `pure/v1/generation/generateArtifacts` | `{clientVersion, model, includeElementPaths, excludedExtensionKeys}` | artifacts per extension/element | data product, ingest, SQL playground, F10 when enabled | N |
| 30 | `generateTestData` (:764) | POST `pure/v1/testData/generation/DONOTUSE_generateTestData` | `{query, mapping, runtime, model}` | test data | service test data (`ServiceTestDataState.ts:717`) | N/E |
| 31 | `compile` (:776) | POST `pure/v1/compilation/compile` | PMCD (or pointer) | `{message:"OK", defects:[…]}` / 400 error | **every compile** (section 4) | **E** |
| 32 | `lambdaReturnType` (:789) | POST `pure/v1/compilation/lambdaReturnType` | `{model, lambda}` | `{returnType}` | query builder only | Q |
| 33 | `lambdaRelationType` (:802) | POST `pure/v1/compilation/lambdaRelationType` | `{model, lambda}` | `{columns:[…]}` | relation mappings, data product, function tests, data space (section 6.7) | E |
| 34 | `batchLambdasRelationType` (:820) | POST `pure/v1/compilation/lambdaRelationType/batch` | `{model, lambdas}` | `{results, errors}` | data product (`DataProductEditorState.ts:945`) | N |
| 35 | `completeCode` (:838) | POST `pure/v1/codeCompletion/completeCode` | `{codeBlock, model, offset}` | `{completions:[{completion, display}]}` | function body & data product lambda editors (type-ahead) — **endpoint not found in engine @230c159** | E (optional) |
| 36 | `runQuery` (:856) | POST `pure/v1/execution/execute?serializationFormat=` | ExecuteInput `{clientVersion, function, mapping?, runtime?, context, model, parameterValues}` | ExecutionResult (TDS/relation/JSON/classes) | function run, mapping execution, service run, DB builder preview (section 6) | **E** |
| 37 | `generateLineage` (:885) | POST `lineage/v1/function/fullAnalytics` | ExecuteInput-like | lineage model | function/service/ingest/data product Lineage | N |
| 38 | `generatePlan` (:898), `debugPlanGeneration` (:914) | POST `pure/v1/execution/generatePlan[/debug]` | ExecuteInput | plan / `{plan, debug[]}` | function/service/mapping "Generate plan" | I |
| 39 | `generateTestDataWithDefaultSeed` (:930), `…WithSeed` (:948) | POST `pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed` / `_WithSeed` (Accept text/plain) | ExecuteInput (+seed data) | text | service test data generation (`ServiceTestDataState.ts:435`, `:578`) | N/E |
| 40 | `INTERNAL__cancelUserExecutions` (:969) | DELETE `server/v1/executionManager/cancelUserExecution?userID&broadcastToCluster` | – | text | Stop buttons on run | E |
| 41 | query store: `searchQueries`, `getQueries`, `getQuery`, `getQueryHistory`, `createQuery`, `updateQuery`, `patchQuery`, `deleteQuery` (:985-1037) | `{queryBaseUrl}/pure/v1/query[/search|/batch|/{id}|/{id}/history|/{id}/patchQuery]` | Query JSON | Query / LightQuery | Studio: `getQuery` (service "import query", query productionizer, data-space template promotion), `searchQueries` (query loader); create/update/delete are Query-app | S/Q |
| 42 | DataCube store (:1041-1076) | `{queryBaseUrl}/pure/v1/query/dataCube[...]` | – | – | DataCube app | Q |
| 43 | `analyzeMappingModelCoverage` (:1080) | POST `pure/v1/analytics/mapping/modelCoverage` | `{clientVersion, mapping, model}` | `{mappedEntities}` | mapping test query builder (`MappingTestableState.ts:660`), QB | E/Q |
| 44 | `surveyDatasets`, `checkDatasetEntitlements` (:1098-1128) | POST `pure/v1/analytics/store-entitlement/surveyDatasets` / `checkDatasetEntitlements` | – | – | query builder data-access panel | Q |
| 45 | `buildDatabase` (:1132) | POST `pure/v1/utilities/database/schemaExploration` | `{connection, targetDatabase, config}` | PMCD with Database | Database builder wizard | E |
| 46 | `executeRawSQL` (:1144) | POST `pure/v1/utilities/database/executeRawSQL` (Accept text/plain) | `{connection, sql}` | text | SQL playground panel | N |
| 47 | `getAvailableFunctionActivators` (:1162) | GET `functionActivator/list` | – | `[{name, description, configuration:{topElement, model, packageableElementJSONType}}]` | init (`EditorGraphState.ts:292-307`) | E |
| 48 | `validateFunctionActivator`, `renderFunctionActivatorArtifact`, `publishFunctionActivatorToSandbox` (:1168-1209) | POST `functionActivator/validate` / `renderArtifact` / `publishToSandbox` | `{clientVersion, functionActivator: path, model}` | errors / artifact / deployment result | activator editors | N |
| 49 | `generateModelsFromDatabaseSpecification` (:1215) | POST `pure/v1/relational/generateModelsFromDatabaseSpecification` | `{databasePath, targetPackage, modelData}` | PMCD | explorer "Build Models" | I |
| 50 | `getAvailableRelationalDatabaseTypeConfigurations` (:1225) | GET `pure/v1/relational/connection/supportedDbAuthenticationFlows` | – | `[{type, authenticationStrategies, datasourceSpecifications}]` | init (`EditorGraphState.ts:309-323`) | E |
| 51 | `TEMPORARY__getServerServiceInfo` (:1240) | GET `server/v1/info/services` | – | service config | before service registration (`V1_PureGraphManager.ts:4713`, `:4824`) | S |
| 52 | `TEMPORARY__getServiceVersionInfo` (:1247), `TEMPORARY__activateGenerationId` (:1261) | GET `service/v1/id/{id}`; PUT `service/v1/generation/setActive/id/{generationId}` | – | storage / – | service activation | S |
| 53 | `runServicePostVal` (:1280) | POST `service/v1/doValidation?assertionId&servicePath` | PMCD | validation result | service post-validation | N |
| 54 | `INTERNAL__registerService` (:1318) | POST `service/v1/register[_fullInteractive|_semiInteractive]?storeModel&generateLineage&generateOpenApi` | PMCD or pointer | `{serverURL, pattern, serviceInstanceId, newGeneration?, testSuccess?, status?}` | service registration, bulk registration | S |
| 55 | `pushToDevMetadata` (:1354) | POST `lakehouse/metadata/deploy/project` | request | – | Dev Mode panel | N |
| 56 | `getServiceMetadataByPattern` (:1365) | GET `service/v1/serviceMetadata/{pattern}` | – | metadata | "check registration" (`ServiceRegistrationState.ts:311`) | S |
| 57 | `isCurrentUserAnOwnerOnLatestVersion` (:1376) | GET `service/v1/isCurrentUserAnOwnerOnLatestVersion/{pattern}` | – | bool | no caller found | – |
| 58 | `getServicesInfo` (:1392) | GET `service/v1/list/detailsFromCache` | – | list | marketplace | – |
| 59 | `executeLegendUserService` (:1401) | GET `user{url}` | params | TDS | data-product extension | – |

Extension endpoints (not in the core client): `POST pure/v1/analytics/dataSpace/render` (data space preview, `EXT:dsl-data-space …/V1_DSL_DataSpace_PureGraphManagerExtension.ts:241`); `pure/v1/dataquality/{execute,generatePlan,debugPlan,propertyPathTree,profile,ruleSuggestions,reconciliation[/generatePlan]}` (`EXT:dsl-data-quality …/V1_DSL_Data_Quality_PureGraphManagerExtension.ts:221-744`).

Engine-side confirmation (paths in `LE`): grammar `…/legend-engine-language-pure-grammar-http-api/…/grammar/api/grammarToJson/GrammarToJson.java:48-150`, `…/jsonToGrammar/JsonToGrammar.java:53-151`; relational op grammar `legend-engine-xts-relationalStore/…/relationalOperationElement/*`; compile/lambda types `…/compiler/api/Compile.java:68-222` (also exposes `c3Linearization`, unused by Studio); testable `…/testable/api/TestableApi.java:58-105`; execute `legend-engine-core-query-pure-http-api/…/query/pure/api/Execute.java:104-243`; code/schema generation `legend-engine-xts-generation/legend-engine-external-shared/…/CodeGenerators.java:40-54`, `…/SchemaGenerators.java:42`; artifacts `…/ArtifactGenerationExtensionApi.java:46-64`; external format `…/ExternalFormats.java:60-126`; function activator `…/FunctionActivatorAPI.java:63-186`; relational `…/RelationalElementAPI.java:49-99`, `…/SchemaExplorationApi.java:47-87`; mapping analytics `…/MappingAnalytics.java:58-100`; protocol `…/PureProtocol.java:41-73`; service `…/ServiceModelingApi.java:58`. Not found at @230c159: `codeCompletion/completeCode`, `lambda/v1/lambdaPrefixes`.

### 7.1 Minimum engine surface for a first Studio (derived)

To reproduce core editing on legend-lite, the engine must provide (all others can be stubbed):
1. `grammarToJson/model` (text → PMCD with `sourceInformation`, parser errors as 400 with position), `jsonToGrammar/model` (PMCD → pretty text).
2. `grammarToJson/lambda` (with `sourceId` echoed in all source infos) and `jsonToGrammar/lambda/batch` (and single `/lambda`).
3. `compilation/compile` (PMCD → OK+warnings, or 400 with one error + sourceInformation whose `sourceId` is preserved from the input JSON).
4. `execution/execute` (+ cancel), `testable/runTests`.
5. Nice-to-have: `compilation/lambdaReturnType` / `lambdaRelationType` (query builder), `analytics/mapping/modelCoverage` (query builder on mappings), `relationalOperationElement` grammar pair (relational mapping forms), `protocol/pure/getClassifierPathMap` + `getSubtypeInfo` (client has fallbacks, `V1_RemoteEngine.ts:306-330`).

---

## 8. Surprises and important details

1. **Whole-model payloads on every call.** compile/execute/runTests/lambda types/completion all ship the entire workspace PMCD including **all dependency entities** (`getFullGraphModelData`, `V1_PureGraphManager.ts:5321-5342`; dependency raw entities appended at serialisation `V1_PureProtocolSerialization.ts:228-248`). An SDLC pointer is used only when the graph has an `origin` (viewer/query of a released version) and source info is not needed (`V1_PureGraphManager.ts:5309-5319`). Payloads are zlib-compressed for this reason (`NetworkUtils.ts:34-39`).
2. **Text mode loses comments, formatting and imports.** The text is always regenerated from protocol JSON via `jsonToGrammar/model` (`GraphEditGrammarModeState.ts:747-756`); the protocol model has no comment/whitespace fields. `SectionIndex` elements (which carry `import` statements) are dropped by `pureCodeToEntities` unless `TEMPORARY__keepSectionIndex` (`V1_PureGraphManager.ts:1935-1939`) and deleted from the built graph unless `TEMPORARY__preserveSectionIndex` (`V1_PureGraphManager.ts:1008-1016`, comment: "we write (serialize) only resolved paths"). So anything typed in text mode is normalised (full paths, canonical layout) on the next round-trip through form mode or re-entry.
3. **Element ↔ text mapping is by `sourceInformation` only.** Studio keeps `Map<elementPath, SourceInformation>` built from each parsed element's `sourceInformation` (`V1_RemoteEngine.ts:334-356`). There is no client-side parser; even "go to definition" re-sends the entire text to the engine (`GraphEditGrammarModeState.ts:257-268`).
4. **Entity paths vs files:** Studio only manipulates `Entity {path, classifierPath, content}` (`legend-storage/src/Entity.ts:19-23`); `content` is generated **client-side** from the typed graph (`elementToEntity` = graph → V1 protocol → JSON, `V1_PureGraphManager.ts:5138-5151`, `:5344-5353`); `classifierPath` is a client-side table (`:5393-5479`) plus the engine map for unknown types. The mapping of entity paths to files (`entities/a/b/C.json`) is entirely the SDLC server's concern — **not visible in Studio source** (see SDLC census).
5. **Source information pruning.** Lambdas edited in a session keep `sourceInformation` in memory (needed so compile errors map back to fields) but entities pushed to SDLC are pruned (`elementToEntity(..., {pruneSourceInformation:true})`, `LocalChangesState.ts:615-621`, `:818`; raw lambda transformer prunes unless `keepSourceInformation`, `V1_RawValueSpecificationTransformer.ts:40-56`; generic pruner removes every key ending in `sourceInformation`, `LG graph/MetaModelUtils.ts:123-132`). **UNCLEAR:** whether text-mode-derived entities (from engine JSON with `returnSourceInformation=true`) are fully pruned before push — I did not trace `computeChangesInTextMode` → push to the byte.
6. **Client-side "compiler" (legend-graph V1 graph builder)** — what *is* done in TypeScript: deserialise entities to V1 protocol classes (`V1_entitiesToPureModelContextData`), index all elements and check path duplication (`initializeAndIndexElements`, `V1_PureGraphManager.ts:1352-1418`), then passes: section indices → types (profiles, classes, enums, measures, functions: 2nd pass; classes/associations 3rd–5th pass for supertypes, properties, derived properties, constraints) → stores → data products → computes → availabilities → mappings → connections & runtimes → function activators → services → data elements → file generations → generation specs → plugin elements (`:1218-1310`, `:1419-1500`). It resolves **element references** (types, property owners, stores, mappings, connections) and builds a navigable object graph used by all forms. Strict mode turns duplicate stereotypes/tags/enum values/system-class association properties etc. from warnings into `GraphBuilderError` (`V1_ElementSecondPassBuilder.ts:385-445`; `V1_DomainBuilderHelper.ts:300-312`; `V1_PropertyMappingBuilder.ts:144`).
   What is **not** done client-side: any expression semantics. Function bodies are stored as raw JSON (`func.expressionSequence = protocol.body`, `V1_ElementSecondPassBuilder.ts:526-529`); derived properties/constraints/mapping transforms are `RawLambda` with only element paths resolved (`V1_buildRawLambdaWithResolvedPaths`, `V1_DomainBuilderHelper.ts:104-115`). Type checking, multiplicity checks, function resolution, mapping/runtime coverage — all engine `compile`.
   Text mode builds only a **light** (index-only) graph after each compile (`buildLightGraph`), a full build happens only when returning to form mode (`GraphEditGrammarModeState.ts:423-433`; `GraphEditFormModeState.ts:298-309`).
7. **Problems are single-error.** The engine aborts on the first compilation error; Studio models `problems = [error, ...warnings]` (`EditorGraphState.ts:192-194`). There is no multi-error diagnostics list.
8. **Form-mode error UX is weak by design:** only Class, Function and Mapping editors can point at an error; anything else (service query, runtime, connection…) kicks the user into whole-project text mode (`GraphEditFormModeState.ts:497-553`).
9. **No incremental/on-type validation.** No compile on edit or save; lambda fields only get *parse* errors (debounced 1 s). Push (Ctrl+S) does not compile. Text-mode local changes are only recomputed on compile, so un-compiled text edits are not pushable (`GraphEditGrammarModeState.ts:544-553`).
10. **Graph rebuild is wholesale and leak-prone.** After a text compile or entity reload, Studio creates a new `PureModel`, rebuilds from entities and recreates all editor tabs by path (comment block `GraphEditFormModeState.ts:234-270`). Compile/generate/update operations are serialised by a global busy flag to avoid overlapping rebuilds (`EditorGraphState.ts:235-277`).
11. **Unknown elements survive** text-mode round trips only because Studio re-appends them to the entity list (`GraphEditGrammarModeState.ts:201-204`, `:511-514`, `:659-662`); they are excluded from the text sent to `jsonToGrammar` (`excludeUnknown`).
12. **File generation sends text, not JSON:** to give generators source info without large payloads, Studio converts the PMCD to text (`jsonToGrammar`) and sends `PureModelContextText` (`V1_RemoteEngine.ts:1096-1105`).
13. **Lambda return types are not batched by the engine;** Studio fires N parallel `lambdaReturnType` requests (`V1_PureGraphManager.ts:2230-2258`). `lambdaRelationType/batch` does exist.
14. **Engine-backed completion is effectively optional**: only two lambda editors use it, behind a TypeAhead toggle, and its endpoint isn't in OSS engine @230c159 (section 4.5).
15. **Model generation has no OSS implementation** (plugin hook only, section 2.5); GenerationSpecification/FileGeneration are legacy ("DEPREACTED_" prefixes in `GraphGenerationState.ts:155-205`).
16. **Connection forms lack DuckDB**: the protocol/metamodel knows `DuckDB` (`LG …/relational/connection/RelationalDatabaseConnection.ts:54`; `V1_DatasourceSpecification.ts:243`) but the Studio connection editor's datasource list does not include it (`ConnectionEditorState.ts:91-101`) — relevant for legend-lite, which would need to add it.
17. **Mapping execution relies on engine-side H2** (`LocalH2DatasourceSpecification` with setup SQL/CSV, `MappingExecutionState.ts:441-470`); a legend-lite engine would need an equivalent ad-hoc test DB (e.g. DuckDB) to support "execute mapping" and relational test data.
18. **Console panel is an empty stub** (`ConsolePanel.tsx:20-26`); all compile/run feedback is via toasts, Problems and inline markers.
19. **Function activators and lakehouse elements** are a large fraction of current Studio code but are enterprise/Snowflake/lakehouse specific — safe to defer.

---

## 9. Things I could not determine

- The `codeCompletion/completeCode` and `lambda/v1/lambdaPrefixes` endpoints: present in the client, absent from legend-engine @230c159 sources.
- Exact base path conventions (`/api` prefix) — configuration, not code.
- Whether text-mode entities pushed to SDLC can contain residual `sourceInformation` inside raw lambda JSON (section 8.5).
- I did not read every form component line-by-line (e.g. every field of the Service registration form or every DataProduct/Compute/Ingest sub-editor); niche editors are characterised from state classes and engine-call sites rather than full UI walkthroughs.

---

<!-- Part C -->
# Part C — The SDLC server (legend-sdlc @1021fda, server + file-system backend)

- **Source**: `legend-sdlc` commit `1021fda8e4237bb0879ae1d2f6bd0808e5587061` ("Bump version to 0.234.1-SNAPSHOT", FINOS Administrator, 2026-10-01; a *shallow* clone of `master`) at `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc`.
- **Studio client**: `/Users/neema/legend/legend-lite-query/.scratch/legend-studio/packages/legend-server-sdlc/src` (`SDLCServerClient.ts`, 1242 lines).
- **Method**: every JAX-RS resource under `legend-sdlc-server/.../server/resources` was parsed mechanically (459 `@GET/@POST/@PUT/@DELETE` methods across 197 files; count cross-checked with `grep`), plus the 4 `/auth` methods that live in `gitlab/resources` and the 3 in `legend-sdlc-server-fs/.../resources`. All 29 main-source files of `legend-sdlc-server-fs` were read in full.

**Path abbreviations used in citations** (all relative to the legend-sdlc root unless prefixed `SC/`):

| Abbrev | Path |
|---|---|
| `FS/` | `legend-sdlc-server-fs/src/main/java/org/finos/legend/sdlc/server/` |
| `SRV/` | `legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/` |
| `RES/` | `SRV/resources/` |
| `API/` | `legend-sdlc-backend-api/src/main/java/org/finos/legend/sdlc/backend/api/` |
| `MODEL/` | `legend-sdlc-model/src/main/java/org/finos/legend/sdlc/domain/model/` |
| `PF/` | `legend-sdlc-project-files/src/main/java/org/finos/legend/sdlc/project/` |
| `PS/` | `legend-sdlc-project-structure/src/main/java/org/finos/legend/sdlc/project/structure/` |
| `CORE/` | `legend-sdlc-core/src/main/java/org/finos/legend/sdlc/core/` |
| `SHARED/` | `legend-sdlc-server-shared/src/main/java/org/finos/legend/sdlc/server/` |
| `SC/` | `legend-studio/packages/legend-server-sdlc/src/` |

> **Context you must know first — this checkout is mid re-architecture.** The upstream repo now contains a
> plan (`docs/re-architecture.md`, 735 lines) and a worklog (`docs/re-architecture-worklog.md`, 1111 lines) that split
> the server into layers L0–L6: model/serialization (L0) → `legend-sdlc-project-files` storage SPI (L1) →
> `legend-sdlc-project-structure` (L2) → `legend-sdlc-core` generic entity/config/dependency/comparison logic (L3) →
> `legend-sdlc-backend-api` backend SPI + capability model (L4) → backends (L5) → JAX-RS server (L6)
> (`docs/re-architecture.md:75-108`). **Phase 4 (backend SPI) is marked complete** (`docs/re-architecture-worklog.md:497-501`);
> Phase 5 (extract GitLab, *refit the FS server onto the SPI*, add an in-memory backend) is in progress. The REST surface
> consumed by Studio is explicitly a non-goal to change (`docs/re-architecture.md:49-50`), except for two *new*
> discovery endpoints (`GET /configuration/capabilities`, `GET /configuration/projectStructureVersions`) and a planned
> 501-for-missing-capability mapping. This matters for legend-lite: **upstream itself has already designed the
> "minimal backend" contract** we would implement (§5.3).

---

## 1. Domain model (L0, `legend-sdlc-model`)

All domain types are Java **interfaces / abstract classes serialized by Jackson from their getters** (no mixins on
the server; Jackson is configured with `WRITE_DATES_AS_TIMESTAMPS=false`, so every `Instant` is an ISO-8601 string —
`SHARED/BaseServer.java:118`). JSON property names are therefore the bean names of the getters (`isX()` → `x`).
Studio's serializr schemas confirm the wire names (e.g. `authoredTimestamp` aliased to `authoredAt`,
`SC/models/revision/Revision.ts:36-55`).

### 1.1 Entity, EntityChange

| Type | Fields (JSON) | Cite |
|---|---|---|
| `Entity` | `path` (e.g. `model::domain::Person`), `classifierPath` (e.g. `meta::pure::metamodel::type::Class`), `content` (the element's protocol JSON as a map; **includes `package` and `name`**) | `MODEL/entity/Entity.java:19-25` |
| `EntityChange` | `type` ∈ `CREATE`,`DELETE`,`MODIFY`,`RENAME`; `entityPath`; `classifierPath` (CREATE/MODIFY); `content` (CREATE/MODIFY); `newEntityPath` (RENAME) | `MODEL/entity/change/EntityChange.java:20-58`, `EntityChangeType.java:17` |
| `EntityDiff` | `entityChangeType`, `newPath`, `oldPath` | `MODEL/comparison/EntityDiff.java:19-25` |

```json
{ "path": "model::Person",
  "classifierPath": "meta::pure::metamodel::type::Class",
  "content": { "_type": "class", "package": "model", "name": "Person", "properties": [ ... ] } }
```

### 1.2 Project, Workspace, User, access

| Type | Fields (JSON) | Cite |
|---|---|---|
| `Project` | `projectId`, `name`, `description`, `tags[]`, `projectType` (deprecated getter), `webUrl` | `MODEL/project/Project.java:19-32` |
| `ProjectType` | `PRODUCTION` (deprecated), `PROTOTYPE` (deprecated), `MANAGED`, `EMBEDDED` | `MODEL/project/ProjectType.java:17` |
| `Workspace` | `projectId`, `userId` (null for group workspaces), `workspaceId` — **no type/source/accessType on the wire** | `MODEL/project/workspace/Workspace.java:17-23` |
| `WorkspaceType` | `USER("user")`, `GROUP("group")` | `MODEL/project/workspace/WorkspaceType.java:19-20` |
| `WorkspaceAccessType` (L1) | `WORKSPACE`, `CONFLICT_RESOLUTION`, `BACKUP` | `PF/files/ProjectFileAccessProvider.java:278-282` |
| `WorkspaceSpecification` (L1, server-side only) | id, type, accessType, source (`ProjectWorkspaceSource` \| `PatchWorkspaceSource(patchVersionId)`), userId (null = current user) | `PF/workspace/WorkspaceSpecification.java:91-122`, `PF/workspace/WorkspaceSource.java:43-53` |
| `SourceSpecification` (L1) | sealed set: `ProjectSourceSpecification`, `VersionSourceSpecification(versionId)`, `PatchSourceSpecification(versionId)`, `WorkspaceSourceSpecification(workspaceSpec)` | `PF/source/SourceSpecification.java:36-56` |
| `User` | `userId`, `name` | `MODEL/user/User.java:17-21` |
| `AccessRole` | `accessRole` (string) | `MODEL/project/accessRole/AccessRole.java:17-19` |
| `AuthorizableProjectAction` | `CREATE_WORKSPACE`, `SUBMIT_REVIEW`, `COMMIT_REVIEW`, `CREATE_VERSION` | `MODEL/project/accessRole/AuthorizableProjectAction.java:17-22` |
| `UserPermission` | `user`, `auhorizedProjectAction` (sic — typo is the wire name) | `MODEL/project/accessRole/UserPermission.java:20-24` |

**Studio derives workspace type from `userId`**: `get workspaceType() { return this.userId ? USER : GROUP }`
(`SC/models/workspace/Workspace.ts:49-51`). A backend must therefore return `userId` non-null exactly for user workspaces.

### 1.3 Revision, Version, Patch

| Type | Fields (JSON) | Cite |
|---|---|---|
| `Revision` | `id` (git commit SHA in both backends), `authorName`, `authoredTimestamp`, `committerName`, `committedTimestamp`, `message` | `MODEL/revision/Revision.java:19-31` |
| `RevisionAlias` | `base`, `head`, `current`, `latest` (HEAD/CURRENT/LATEST equivalent; case-insensitive), else literal id | `MODEL/revision/RevisionAlias.java:19-25`; resolution e.g. `FS/api/BaseFSApi.java:115-130` |
| `RevisionStatus` | `revision`, `committed`, `workspaces[]`, `versions[]`, `patches[]` | `MODEL/revision/RevisionStatus.java:23-33` |
| `VersionId` | `majorVersion`, `minorVersion`, `patchVersion` (ints); string form `M.m.p`; `nextMajorVersion()` etc. | `MODEL/version/VersionId.java:17-25,84-118` |
| `Version` | `id` (VersionId object), `projectId`, `revisionId`, `notes` | `MODEL/version/Version.java:17-25` |
| `Patch` | `projectId`, `patchReleaseVersionId` (VersionId) | `MODEL/patch/Patch.java:20-24` |
| `NewVersionType` | `MAJOR`, `MINOR`, `PATCH` | `API/version/NewVersionType.java:17-20` |

```json
{ "id": {"majorVersion": 1, "minorVersion": 2, "patchVersion": 0}, "projectId": "PROD-123",
  "revisionId": "9f3c...", "notes": "release notes" }
```

### 1.4 Review, Approval

| Type | Fields (JSON) | Cite |
|---|---|---|
| `Review` | `id`, `projectId`, `workspaceId`, `workspaceType`, `title`, `description`, `createdAt`, `lastUpdatedAt`, `closedAt`, `committedAt`, `state`, `author` (User), `commitRevisionId`, `webURL`, `labels[]` | `MODEL/review/Review.java:23-58` |
| `ReviewState` | `OPEN`, `COMMITTED`, `CLOSED`, `UNKNOWN` | `MODEL/review/ReviewState.java:17` |
| `Approval` | `approvedBy[]` (User) | `MODEL/review/Approval.java:21-23` |
| `ReviewUpdateStatus` | `updateInProgress`, `baseRevisionId`, `targetRevisionId` | `API/review/ReviewApi.java:204-226` |

### 1.5 Build / Workflow / WorkflowJob / Issue

| Type | Fields | Cite |
|---|---|---|
| `Build` (legacy) | `id`, `projectId`, `revisionId`, `status` ∈ `PENDING, IN_PROGRESS, SUCCEEDED, FAILED, CANCELED, UNKNOWN`, `createdAt`, `startedAt`, `finishedAt`, `webURL` | `MODEL/build/Build.java:19-35`, `BuildStatus.java:17` |
| `Workflow` (≈ CI pipeline) | same shape as Build; `status` ∈ `PENDING, IN_PROGRESS, SUCCEEDED, FAILED, CANCELED, UNKNOWN` | `MODEL/workflow/Workflow.java:19-35`, `WorkflowStatus.java:17` |
| `WorkflowJob` | `id`, `workflowId`, `name`, `projectId`, `revisionId`, `status` ∈ `WAITING, IN_PROGRESS, SUCCEEDED, FAILED, CANCELED, WAITING_MANUAL, SKIPPED, UNKNOWN`, `createdAt`, `startedAt`, `finishedAt`, `webURL` | `MODEL/workflow/WorkflowJob.java:19-39`, `WorkflowJobStatus.java:17` |
| `Issue` | `id`, `projectId`, `title`, `description`, `creationTime`, `lastUpdateTime`, `webURL` | `MODEL/issue/Issue.java:19-33` |

### 1.6 Comparison, workspace update

| Type | Fields | Cite |
|---|---|---|
| `Comparison` | `toRevisionId`, `fromRevisionId`, `entityDiffs[]`, `projectConfigurationUpdated` | `MODEL/comparison/Comparison.java:19-49` |
| `WorkspaceUpdateReport` | `status` ∈ `NO_OP, UPDATED, CONFLICT`, `workspaceMergeBaseRevisionId`, `workspaceRevisionId` | `API/workspace/WorkspaceApi.java:110-132` |
| `ProjectConfigurationStatusReport` | `projectConfigured`, `reviewIds[]` | `API/project/ProjectConfigurationStatusReport.java:19-23` |
| `ImportReport` | `project`, `reviewId` (non-null if configuration needed a review) | `API/project/ProjectApi.java:98-109` |
| `ProjectRevision` (downstream deps) | `projectId`, `revisionId` | `API/dependency/ProjectRevision.java:21-35` |

### 1.7 ProjectConfiguration (= the `/project.json` file)

| Field | Type | Notes / cite |
|---|---|---|
| `projectId` | string | `MODEL/project/configuration/ProjectConfiguration.java:24` |
| `projectType` | `MANAGED` \| `EMBEDDED` (default null) | `:26-29`; only these two are valid (`PS/ProjectStructure.java:366-369`) |
| `projectStructureVersion` | `{version:int, extensionVersion:int?}` | `:31`; `MODEL/project/configuration/ProjectStructureVersion.java:19-25` |
| `platformConfigurations` | `[{name, version}]` (pins e.g. legend-engine/legend-sdlc versions) | `:33-36`; `PlatformConfiguration.java:17-21` |
| `groupId`, `artifactId` | Maven coordinates; groupId must be a Java name, artifactId must match a pattern | `:38-40`; `PS/ProjectStructure.java:371-379` |
| `projectDependencies` | `[{projectId: "groupId:artifactId", versionId: "M.m.p", exclusions?: [{projectId}]}]` | `:42`; `MODEL/project/configuration/ProjectDependency.java:22-28`; "proper" = non-blank id + strict `M.m.p` (`PS/ProjectStructure.java:381-394`); a projectId without `:` is "legacy" (`:396-400`) |
| `metamodelDependencies` | `[{metamodel, version:int}]` | `:44`; `MetamodelDependency.java:20-24` |
| `runDependencyTests`, `produceShadedServiceJar` | Boolean (structure-version options; javadoc says don't add more) | `:46-66` |
| `artifactGenerations` | deprecated | `:68-72` |

Persisted with a sorted, indented, `NON_NULL` Jackson mapper (`PS/ProjectStructure.java:71-78`) at path
`/project.json` (`PS/ProjectStructure.java:96`), read back as `SimpleProjectConfiguration`
(`PS/SimpleProjectConfiguration.java:190-200`). If the file is missing, the API returns a default config with
structure version 0 (`PS/ProjectStructure.java:361-364`, used by `FS/api/project/FileSystemProjectConfigurationApi.java:52-53`).

```json
{
  "artifactId" : "my-model",
  "groupId" : "org.example",
  "platformConfigurations" : [ { "name" : "legend-engine", "version" : "4.12.1" } ],
  "projectDependencies" : [ { "projectId" : "org.example:shared-model", "versionId" : "1.0.0" } ],
  "projectId" : "PROD-123",
  "projectStructureVersion" : { "extensionVersion" : 1, "version" : 13 },
  "projectType" : "MANAGED"
}
```

### 1.8 Request bodies ("commands", `SRV/application/**`)

| Command | JSON fields | Cite |
|---|---|---|
| `CreateProjectCommand` | `name`, `description`, `type` (ProjectType), `groupId`, `artifactId`, `tags[]` | `SRV/application/project/CreateProjectCommand.java:21-28` |
| `ImportProjectCommand` | `id`, `type`, `groupId`, `artifactId` | `.../ImportProjectCommand.java:19-24` |
| `UpdateProjectCommand` | `name`, `description`, `tags[]` | `.../UpdateProjectCommand.java:19-23` |
| `UpdateProjectConfigurationCommand` | `message`, `projectStructureVersion{version,extensionVersion}`, `projectType`, `groupId`, `artifactId`, `platformConfigurations{platformConfigurations[]}`, `projectDependenciesToAdd[]`, `projectDependenciesToRemove[]`, `artifactGenerationsToAdd[]`, `artifactGenerationsToRemove[]` (names), `runDependencyTests`, `produceShadedServiceJar` | `.../UpdateProjectConfigurationCommand.java:46-59,116-144` |
| `UpdateEntitiesCommand` | `message`, `entities[]` (`{path,classifierPath,content}`), `replace` (default false) | `SRV/application/entity/UpdateEntitiesCommand.java:25-80`, `AbstractEntityChangeCommand.java:19` |
| `PerformChangesCommand` | `message`, `entityChanges[]` (EntityChange), `revisionId` (optimistic-concurrency reference; null = no check) | `.../PerformChangesCommand.java:26-60` |
| `CreateOrUpdateEntityCommand` | `message`, `classifierPath`, `content` (path comes from URL) | `.../CreateOrUpdateEntityCommand.java:19-22` |
| `DeleteEntityCommand` / `DeleteEntitiesCommand` | `message` / `message`, `entitiesToDelete[]` | `.../DeleteEntityCommand.java:17`, `DeleteEntitiesCommand.java:23-25` |
| `CreateVersionCommand` | `versionType` (MAJOR/MINOR/PATCH), `revisionId` (optional), `notes` | `SRV/application/version/CreateVersionCommand.java:19-23` |
| `CreateReviewCommand` | `workspaceId`, `workspaceType`, `title`, `description`, `labels[]` | `SRV/application/review/CreateReviewCommand.java:21-27` |
| `EditReviewCommand` | `title`, `description`, `labels[]` | `.../EditReviewCommand.java:19-23` |
| `CommitReviewCommand` | `message` | `.../CommitReviewCommand.java:17-19` |
| `CreateIssueCommand` | `title`, `description` | `SRV/application/issue/CreateIssueCommand.java:17-20` |
| create patch | **raw string** body = source version id, e.g. `"1.2.0"` | `RES/patch/PatchesResource.java:54-70` |

Errors: Jackson-serialized `ExtendedErrorMessage` `{code, message, details, timestamp, stackTrace?}`
(`SHARED/error/ExtendedErrorMessage.java:30-55`); stack traces only if `errorHandling.includeStackTrace`
(`SHARED/BaseServer.java:131-136`). Unsupported capability (new) → 501 `{capability, backendType, message}`
(`SRV/backend/UnsupportedCapabilityExceptionMapper.java:30-42`).

---
## 2. The complete REST API

Server root path is `/api` in every shipped config (`legend-sdlc-server-fs/src/main/resources/docker/config/config.json`
`"rootPath": "/api"`), so e.g. `GET /api/projects`. All resources `@Produces(application/json)` except workflow-job
`/logs` (`text/plain`, `RES/workflow/project/ProjectWorkflowJobsResource.java:79-95`) and `/auth/authorize` (`text/html`).

### 2.0 Size and shape of the surface

- **459 resource methods** in `RES/**` + 4 `/auth` methods (GitLab: `SRV/gitlab/resources/GitLabAuthResource.java:54-77`,
  `GitLabAuthCheckResource.java:67`; FS: `FS/resources/FileSystemAuthResource.java:40-64`, `FileSystemAuthCheckResource.java:39-44`).
- The surface is a **cross-product**: {project, user workspace, group workspace} × {—, conflictResolution, backup}
  × {—, `/patches/{patchReleaseVersionId}`} × {entities, entityPaths, revisions, configuration, pureModelContextData, workflows, …}.
  - **Group workspaces** (`/projects/{projectId}/groupWorkspaces/{workspaceId}/…`) mirror user workspaces
    (`/projects/{projectId}/workspaces/{workspaceId}/…`) **exactly**, except user workspaces additionally have the
    legacy `GET …/builds` and `GET …/builds/{buildId}` (72 vs 70 routes; computed by set difference).
  - **Patch tree** (`/projects/{projectId}/patches/{patchReleaseVersionId}/…`, **201 routes**) mirrors the project tree
    with these differences (computed): patch-only = `GET`/`DELETE …/patches/{v}` (get/delete the patch) and
    `POST …/patches/{v}/release`; absent under a patch = versions (all), issues, builds, `downstreamProjects`,
    `configuration/projectConfigurationStatus`, `revisions/{rev}/configuration*`, `revisions/{rev}/entities*`,
    `revisions/{rev}/entityPaths`, workspace `builds`.
- The table below lists **every non-patch, non-group route** (187 rows, project-level then user-workspace-level).
  `P` = `/projects/{projectId}`, `P/W` = `/projects/{projectId}/workspaces/{workspaceId}`.
- **Studio** column: `yes` = called by `SDLCServerClient.ts` *and* that client method is referenced by some Studio
  package (checked by grep over `legend-studio/packages`); Studio calls the group-workspace and patch-workspace
  variants of the same routes via `_workspaceByType`/`_adaptiveWorkspace` (`SC/SDLCServerClient.ts:414-465`) —
  the workspace URL is `groupWorkspaces/…` when `userId` is absent and gets a `patches/{v}/` infix when `workspace.source` is set.
  `defined, unused` = client method exists but no Studio package calls it (`getAccessRole`, `getAllReviews`,
  `getReviewFromEntities`, `getReviewToEntities`).
- **Core** column: `CORE` = needed for the "author models → save → publish a version" loop; `review` = needed only
  if the optional review step is kept.
- **FS** column: behaviour of `legend-sdlc-server-fs` (see §5.2 for evidence). "throws" = `UnsupportedOperationException`
  → generic 500 via `CatchAllExceptionMapper`.
- Query params: `?x=default`. Instants (`since`/`until`) parse ISO strings or date shorthands
  (`SHARED/BaseServer.java:247-280`, `StartInstant`/`EndInstant`).
- Entity filter params on every `entities`/`entityPaths` route: `classifierPath` (set, exact match), `package` (set),
  `includeSubPackages` (default true), `name` (case-insensitive regex on simple name; invalid regex → literal),
  `stereotype` (set), `taggedValue` (list of `tag/regex`, delimiter `/`), `excludeInvalid` (entities only, default false)
  (`RES/EntityAccessResource.java:30-171`).

| Method | Path | Query | Body | Returns | Studio | Core | FS | Resource |
|---|---|---|---|---|---|---|---|---|
| GET | `/configuration/capabilities` |  |  | CapabilitiesInfo |  |  | 500 (throwing Backend provider) | project/ConfigurationResource.java:78 |
| GET | `/configuration/latestAvailableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | ok | project/ConfigurationResource.java:69 |
| GET | `/configuration/latestProjectStructureVersion` |  |  | ProjectStructureVersion | yes | CORE | ok | project/ConfigurationResource.java:61 |
| GET | `/configuration/projectStructureVersions` |  |  | List<ProjectStructureVersionInfo> |  |  | ok | project/ConfigurationResource.java:86 |
| GET | `/currentUser` |  |  | User | yes | CORE | ok (local_user) | user/CurrentUserResource.java:44 |
| GET | `/info` |  |  | ServerInfo |  |  | ok | InfoResource.java:41 |
| GET | `/projects` | ?search ?user=true ?tag ?excludeTag ?limit ?type |  | List<Project> | yes | CORE | ok | project/project/ProjectsResource.java:67 |
| POST | `/projects` |  | CreateProjectCommand | Project | yes | CORE | ok | project/project/ProjectsResource.java:102 |
| POST | `/projects/import` |  | ImportProjectCommand | ImportReport | yes |  | throws | project/project/ProjectsResource.java:166 |
| GET | `/projects/{id}` |  |  | Project | yes | CORE | ok | project/project/ProjectsResource.java:90 |
| PUT | `/projects/{id}` |  | UpdateProjectCommand | void | yes |  | throws | project/project/ProjectsResource.java:123 |
| DELETE | `/projects/{id}` |  |  | void |  |  | throws | project/project/ProjectsResource.java:154 |
| GET | `/projects/{id}/allUsersAuthorizedActions` | ?actions |  | Set<UserPermission> |  |  | throws | project/project/ProjectsResource.java:218 |
| GET | `/projects/{id}/authorizedActions` | ?actions |  | Set<AuthorizableProjectAction> | yes |  | throws | project/project/ProjectsResource.java:188 |
| GET | `/projects/{id}/authorizedActions/{action}` | ?action |  | boolean |  |  | throws | project/project/ProjectsResource.java:203 |
| GET | `/projects/{id}/userAccessRole/currentUser` |  |  | AccessRole | defined, unused |  | throws | project/project/ProjectsResource.java:178 |
| GET | `/reviews` | ?assignedToMe=false ?authoredByMe=true ?labels ?workspaceIdRegex ?workspaceTypes ?state ?since ?until ?limit ?projectTypes |  | List<Review> | defined, unused |  | []  | review/ReviewsOnlyResource.java:55 |
| GET | `/server/features` |  |  | LegendSDLCServerFeaturesConfiguration | yes | CORE | ok | ServerResource.java:57 |
| GET | `/server/info` |  |  | ServerInfo |  |  | ok | ServerResource.java:49 |
| GET | `/server/platforms` |  |  | List<ProjectStructurePlatformExtensions.Platform> | yes | CORE | ok | ServerResource.java:65 |
| GET | `/users` | ?search |  | List<User> | yes |  | ok (local_user) | user/UsersResource.java:47 |
| GET | `/users/{userId}` |  |  | User |  |  | ok (local_user) | user/UsersResource.java:54 |
| GET | `P/backup` |  |  | List<Workspace> |  |  | [] | backup/project/BackupProjectResource.java:46 |
| GET | `P/builds` | ?revisionId ?status ?limit |  | List<Build> |  |  | throws | build/ProjectBuildsResource.java:54 |
| GET | `P/builds/{buildId}` |  |  | Build |  |  | throws | build/ProjectBuildsResource.java:71 |
| GET | `P/configuration` |  |  | ProjectConfiguration | yes | CORE | ok (HEAD) | project/project/ProjectConfigurationResource.java:49 |
| GET | `P/configuration/availableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | [] | project/project/ProjectConfigurationResource.java:59 |
| GET | `P/configuration/projectConfigurationStatus` |  |  | ProjectConfigurationStatusReport | yes | CORE | ok | project/project/ProjectConfigurationResource.java:70 |
| GET | `P/conflictResolution` |  |  | List<Workspace> | yes | CORE | [] | conflictResolution/project/ConflictResolutionProjectResource.java:46 |
| GET | `P/downstreamProjects` |  |  | Set<ProjectRevision> |  |  | generic impl (unverified) | dependency/project/DownstreamDependenciesResource.java:46 |
| GET | `P/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes | CORE | ok (POSIX only) | entity/project/ProjectEntitiesResource.java:51 |
| GET | `P/entities/{path}` |  |  | Entity |  |  | ok | entity/project/ProjectEntitiesResource.java:77 |
| GET | `P/entities/{path}/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | throws | revision/project/ProjectEntityRevisionsResource.java:52 |
| GET | `P/entities/{path}/revisions/{revisionId}` |  |  | Revision |  |  | throws | revision/project/ProjectEntityRevisionsResource.java:66 |
| GET | `P/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | always 500 (bug) | entity/project/ProjectEntityPathsResource.java:50 |
| GET | `P/issues` |  |  | List<Issue> |  |  | throws | issue/IssuesResource.java:50 |
| POST | `P/issues` |  | CreateIssueCommand | Issue |  |  | throws | issue/IssuesResource.java:74 |
| GET | `P/issues/{issueId}` |  |  | Issue |  |  | throws | issue/IssuesResource.java:61 |
| DELETE | `P/issues/{issueId}` |  |  | void |  |  | throws | issue/IssuesResource.java:85 |
| GET | `P/packages/{path}/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | throws | revision/project/ProjectPackageRevisionsResource.java:52 |
| GET | `P/packages/{path}/revisions/{revisionId}` |  |  | Revision |  |  | throws | revision/project/ProjectPackageRevisionsResource.java:66 |
| GET | `P/patches` | ?major ?minMajor ?maxMajor ?minor ?minMinor ?maxMinor ?patch ?minPatch ?maxPatch |  | List<Patch> | yes |  | throws | patch/PatchesResource.java:73 |
| POST | `P/patches` |  | String | Patch | yes |  | throws | patch/PatchesResource.java:54 |
| GET | `P/pureModelContextData` |  |  | PureModelContextData |  |  | via getEntities (untested) | pmcd/project/ProjectPureModelContextDataResource.java:51 |
| GET | `P/reviews` | ?state ?revisionIds ?workspaceIdRegex ?workspaceTypes ?since ?until ?limit |  | List<Review> | yes | review | [] | review/project/ReviewsResource.java:62 |
| POST | `P/reviews` |  | CreateReviewCommand | Review | yes | review | throws | review/project/ReviewsResource.java:112 |
| GET | `P/reviews/{reviewId}` |  |  | Review | yes | review | throws | review/project/ReviewsResource.java:79 |
| GET | `P/reviews/{reviewId}/approval` |  |  | Approval | yes | review | throws | review/project/ReviewsResource.java:179 |
| POST | `P/reviews/{reviewId}/approve` |  |  | Review | yes | review | throws | review/project/ReviewsResource.java:146 |
| POST | `P/reviews/{reviewId}/close` |  |  | Review | yes | review | throws | review/project/ReviewsResource.java:124 |
| POST | `P/reviews/{reviewId}/commit` |  | CommitReviewCommand | Review | yes | review | throws | review/project/ReviewsResource.java:190 |
| GET | `P/reviews/{reviewId}/comparison` |  |  | Comparison | yes | review | throws | comparison/project/ComparisonReviewResource.java:45 |
| GET | `P/reviews/{reviewId}/comparison/from/configuration` |  |  | ProjectConfiguration | yes |  | throws | comparison/project/ComparisonReviewProjectConfigurationResource.java:46 |
| GET | `P/reviews/{reviewId}/comparison/from/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | defined, unused |  | throws | comparison/project/ComparisonReviewEntitiesResource.java:51 |
| GET | `P/reviews/{reviewId}/comparison/from/entities/{entityPath}` |  |  | Entity | yes |  | throws | comparison/project/ComparisonReviewEntitiesResource.java:105 |
| GET | `P/reviews/{reviewId}/comparison/projectLatest` |  |  | Comparison |  |  | throws | comparison/project/ComparisonReviewResource.java:55 |
| GET | `P/reviews/{reviewId}/comparison/to/configuration` |  |  | ProjectConfiguration | yes |  | throws | comparison/project/ComparisonReviewProjectConfigurationResource.java:57 |
| GET | `P/reviews/{reviewId}/comparison/to/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | defined, unused |  | throws | comparison/project/ComparisonReviewEntitiesResource.java:78 |
| GET | `P/reviews/{reviewId}/comparison/to/entities/{entityPath}` |  |  | Entity | yes |  | throws | comparison/project/ComparisonReviewEntitiesResource.java:116 |
| GET | `P/reviews/{reviewId}/comparison/workspaceCreation` |  |  | Comparison |  |  | throws | comparison/project/ComparisonReviewResource.java:66 |
| POST | `P/reviews/{reviewId}/edit` |  | EditReviewCommand | Review |  |  | throws | review/project/ReviewsResource.java:224 |
| GET | `P/reviews/{reviewId}/outdated` |  |  | boolean |  |  | throws | review/project/ReviewsResource.java:90 |
| POST | `P/reviews/{reviewId}/reject` |  |  | Review | yes |  | throws | review/project/ReviewsResource.java:168 |
| POST | `P/reviews/{reviewId}/reopen` |  |  | Review | yes |  | throws | review/project/ReviewsResource.java:135 |
| POST | `P/reviews/{reviewId}/revokeApproval` |  |  | Review |  |  | throws | review/project/ReviewsResource.java:157 |
| POST | `P/reviews/{reviewId}/update` |  |  | ReviewUpdateStatus |  |  | throws | review/project/ReviewsResource.java:213 |
| GET | `P/reviews/{reviewId}/updateStatus` |  |  | ReviewUpdateStatus |  |  | throws | review/project/ReviewsResource.java:202 |
| GET | `P/reviews/{reviewId}/workflows` | ?revisionId ?status ?limit |  | List<Workflow> |  |  | throws | workflow/ReviewWorkflowsResource.java:50 |
| GET | `P/reviews/{reviewId}/workflows/{workflowId}` |  |  | Workflow |  |  | throws | workflow/ReviewWorkflowsResource.java:67 |
| GET | `P/reviews/{reviewId}/workflows/{workflowId}/jobs` | ?status |  | List<WorkflowJob> |  |  | throws | workflow/ReviewWorkflowJobsResource.java:53 |
| GET | `P/reviews/{reviewId}/workflows/{workflowId}/jobs/{workflowJobId}` |  |  | WorkflowJob |  |  | throws | workflow/ReviewWorkflowJobsResource.java:66 |
| POST | `P/reviews/{reviewId}/workflows/{workflowId}/jobs/{workflowJobId}/cancel` |  |  | WorkflowJob |  |  | throws | workflow/ReviewWorkflowJobsResource.java:130 |
| GET | `P/reviews/{reviewId}/workflows/{workflowId}/jobs/{workflowJobId}/logs` |  |  | Response |  |  | throws | workflow/ReviewWorkflowJobsResource.java:80 |
| POST | `P/reviews/{reviewId}/workflows/{workflowId}/jobs/{workflowJobId}/retry` |  |  | WorkflowJob |  |  | throws | workflow/ReviewWorkflowJobsResource.java:115 |
| POST | `P/reviews/{reviewId}/workflows/{workflowId}/jobs/{workflowJobId}/run` |  |  | WorkflowJob |  |  | throws | workflow/ReviewWorkflowJobsResource.java:101 |
| GET | `P/revisions` | ?since ?until ?limit |  | List<Revision> | yes |  | throws | revision/project/ProjectRevisionsResource.java:53 |
| GET | `P/revisions/{revisionId}` |  |  | Revision | yes | CORE | ok | revision/project/ProjectRevisionsResource.java:66 |
| GET | `P/revisions/{revisionId}/configuration` |  |  | ProjectConfiguration | yes |  | ok (HEAD) | project/project/ProjectRevisionProjectConfigurationResource.java:49 |
| GET | `P/revisions/{revisionId}/configuration/availableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | [] | project/project/ProjectRevisionProjectConfigurationResource.java:60 |
| GET | `P/revisions/{revisionId}/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes | CORE | revisionId IGNORED (HEAD) | entity/project/ProjectRevisionEntitiesResource.java:51 |
| GET | `P/revisions/{revisionId}/entities/{path}` |  |  | Entity |  |  | revisionId IGNORED (HEAD) | entity/project/ProjectRevisionEntitiesResource.java:79 |
| GET | `P/revisions/{revisionId}/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | always 500 (bug) | entity/project/ProjectRevisionEntityPathsResource.java:50 |
| GET | `P/revisions/{revisionId}/pureModelContextData` |  |  | PureModelContextData |  |  | via getEntities (untested) | project/project/ProjectRevisionPureModelContextDataResource.java:47 |
| GET | `P/revisions/{revisionId}/status` |  |  | RevisionStatus |  |  | throws | revision/project/ProjectRevisionsResource.java:78 |
| GET | `P/revisions/{revisionId}/upstreamProjects` | ?transitive=false |  | Set<ProjectDependency> |  |  | generic impl (unverified) | dependency/project/ProjectRevisionDependenciesResource.java:48 |
| GET | `P/versions` | ?major ?minMajor ?maxMajor ?minor ?minMinor ?maxMinor ?patch ?minPatch ?maxPatch |  | List<Version> | yes | CORE | [] | version/VersionsResource.java:60 |
| POST | `P/versions` |  | CreateVersionCommand | Version | yes | CORE | throws | version/VersionsResource.java:140 |
| GET | `P/versions/latest` | ?major ?minMajor ?maxMajor ?minor ?minMinor ?maxMinor ?patch ?minPatch ?maxPatch |  | Version | yes | CORE | throws | version/VersionsResource.java:94 |
| GET | `P/versions/{versionId}` |  |  | Version | yes |  | throws | version/VersionsResource.java:129 |
| GET | `P/versions/{versionId}/builds` | ?revisionId ?status ?limit |  | List<Build> |  |  | throws | build/VersionBuildsResource.java:54 |
| GET | `P/versions/{versionId}/builds/{buildId}` |  |  | Build |  |  | throws | build/VersionBuildsResource.java:75 |
| GET | `P/versions/{versionId}/configuration` |  |  | ProjectConfiguration | yes | CORE | throws | project/VersionProjectConfigurationResource.java:47 |
| GET | `P/versions/{versionId}/configuration/availableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | throws | project/VersionProjectConfigurationResource.java:58 |
| GET | `P/versions/{versionId}/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes | CORE | throws | entity/VersionEntitiesResource.java:51 |
| GET | `P/versions/{versionId}/entities/{path}` |  |  | Entity |  |  | throws | entity/VersionEntitiesResource.java:78 |
| GET | `P/versions/{versionId}/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | throws | entity/VersionEntityPathsResource.java:50 |
| GET | `P/versions/{versionId}/pureModelContextData` |  |  | PureModelContextData |  |  | throws | pmcd/VersionPureModelContextDataResource.java:47 |
| GET | `P/versions/{versionId}/upstreamProjects` | ?transitive=false |  | Set<ProjectDependency> |  |  | throws | dependency/project/ProjectVersionDependenciesResource.java:48 |
| GET | `P/versions/{versionId}/workflows` | ?revisionId ?status ?limit |  | List<Workflow> | yes |  | throws | workflow/VersionWorkflowsResource.java:50 |
| GET | `P/versions/{versionId}/workflows/{workflowId}` |  |  | Workflow | yes |  | throws | workflow/VersionWorkflowsResource.java:67 |
| GET | `P/versions/{versionId}/workflows/{workflowId}/jobs` | ?status |  | List<WorkflowJob> | yes |  | throws | workflow/VersionWorkflowJobsResource.java:53 |
| GET | `P/versions/{versionId}/workflows/{workflowId}/jobs/{workflowJobId}` |  |  | WorkflowJob | yes |  | throws | workflow/VersionWorkflowJobsResource.java:66 |
| POST | `P/versions/{versionId}/workflows/{workflowId}/jobs/{workflowJobId}/cancel` |  |  | WorkflowJob | yes |  | throws | workflow/VersionWorkflowJobsResource.java:131 |
| GET | `P/versions/{versionId}/workflows/{workflowId}/jobs/{workflowJobId}/logs` |  |  | Response | yes |  | throws | workflow/VersionWorkflowJobsResource.java:80 |
| POST | `P/versions/{versionId}/workflows/{workflowId}/jobs/{workflowJobId}/retry` |  |  | WorkflowJob | yes |  | throws | workflow/VersionWorkflowJobsResource.java:116 |
| POST | `P/versions/{versionId}/workflows/{workflowId}/jobs/{workflowJobId}/run` |  |  | WorkflowJob | yes |  | throws | workflow/VersionWorkflowJobsResource.java:102 |
| GET | `P/workflows` | ?revisionId ?status ?limit |  | List<Workflow> | yes |  | throws | workflow/project/ProjectWorkflowsResource.java:50 |
| GET | `P/workflows/{workflowId}` |  |  | Workflow | yes |  | throws | workflow/project/ProjectWorkflowsResource.java:63 |
| GET | `P/workflows/{workflowId}/jobs` | ?status |  | List<WorkflowJob> | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:53 |
| GET | `P/workflows/{workflowId}/jobs/{workflowJobId}` |  |  | WorkflowJob | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:65 |
| POST | `P/workflows/{workflowId}/jobs/{workflowJobId}/cancel` |  |  | WorkflowJob | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:125 |
| GET | `P/workflows/{workflowId}/jobs/{workflowJobId}/logs` |  |  | Response | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:78 |
| POST | `P/workflows/{workflowId}/jobs/{workflowJobId}/retry` |  |  | WorkflowJob | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:111 |
| POST | `P/workflows/{workflowId}/jobs/{workflowJobId}/run` |  |  | WorkflowJob | yes |  | throws | workflow/project/ProjectWorkflowJobsResource.java:98 |
| GET | `P/workspaces` | ?owned=true |  | List<Workspace> | yes | CORE | USER/GROUP swapped (bug) | workspace/project/user/WorkspacesResource.java:51 |
| GET | `P/W` |  |  | Workspace | yes | CORE | ok; 500 NPE if missing | workspace/project/user/WorkspacesResource.java:65 |
| POST | `P/W` |  |  | Workspace | yes | CORE | ok | workspace/project/user/WorkspacesResource.java:104 |
| DELETE | `P/W` |  |  | void | yes | CORE | throws | workspace/project/user/WorkspacesResource.java:118 |
| GET | `P/W/backup` |  |  | Workspace |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceResource.java:52 |
| DELETE | `P/W/backup` |  |  | void |  |  | silent no-op (bug) | backup/project/user/BackupWorkspaceResource.java:77 |
| GET | `P/W/backup/configuration` |  |  | ProjectConfiguration |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceProjectConfigurationResource.java:45 |
| GET | `P/W/backup/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceEntitiesResource.java:54 |
| GET | `P/W/backup/entities/{path}` |  |  | Entity |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceEntitiesResource.java:81 |
| GET | `P/W/backup/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceEntityPathsResource.java:53 |
| GET | `P/W/backup/outdated` |  |  | boolean |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceResource.java:64 |
| POST | `P/W/backup/recover` | ?forceRecovery |  | void |  |  | silent no-op (bug) | backup/project/user/BackupWorkspaceResource.java:89 |
| GET | `P/W/backup/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionsResource.java:55 |
| GET | `P/W/backup/revisions/{revisionId}` |  |  | Revision |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionsResource.java:69 |
| GET | `P/W/backup/revisions/{revisionId}/configuration` |  |  | ProjectConfiguration |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionProjectConfigurationResource.java:46 |
| GET | `P/W/backup/revisions/{revisionId}/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionEntitiesResource.java:54 |
| GET | `P/W/backup/revisions/{revisionId}/entities/{path}` |  |  | Entity |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionEntitiesResource.java:82 |
| GET | `P/W/backup/revisions/{revisionId}/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | 500 (no backup branch) | backup/project/user/BackupWorkspaceRevisionEntityPathsResource.java:53 |
| GET | `P/W/builds` | ?revisionId ?status ?limit |  | List<Build> |  |  | throws | build/WorkspaceBuildsResource.java:56 |
| GET | `P/W/builds/{buildId}` |  |  | Build |  |  | throws | build/WorkspaceBuildsResource.java:77 |
| GET | `P/W/comparison/projectLatest` |  |  | Comparison |  |  | throws | comparison/project/user/ComparisonWorkspaceResource.java:56 |
| GET | `P/W/comparison/workspaceCreation` |  |  | Comparison |  |  | throws | comparison/project/user/ComparisonWorkspaceResource.java:45 |
| GET | `P/W/configuration` |  |  | ProjectConfiguration | yes | CORE | ok (HEAD) | project/project/user/WorkspaceProjectConfigurationResource.java:52 |
| POST | `P/W/configuration` |  | UpdateProjectConfigurationCommand | Revision | yes | CORE | throws | project/project/user/WorkspaceProjectConfigurationResource.java:62 |
| GET | `P/W/configuration/availableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | [] | project/project/user/WorkspaceProjectConfigurationResource.java:73 |
| GET | `P/W/conflictResolution` |  |  | Workspace |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceResource.java:54 |
| DELETE | `P/W/conflictResolution` |  |  | void | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceResource.java:79 |
| POST | `P/W/conflictResolution/accept` |  | PerformChangesCommand | void | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceResource.java:104 |
| GET | `P/W/conflictResolution/configuration` |  |  | ProjectConfiguration | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceProjectConfigurationResource.java:45 |
| POST | `P/W/conflictResolution/discardChanges` |  |  | void | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceResource.java:91 |
| GET | `P/W/conflictResolution/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceEntitiesResource.java:54 |
| GET | `P/W/conflictResolution/entities/{path}` |  |  | Entity |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceEntitiesResource.java:81 |
| GET | `P/W/conflictResolution/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceEntityPathsResource.java:53 |
| GET | `P/W/conflictResolution/outdated` |  |  | boolean | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceResource.java:66 |
| GET | `P/W/conflictResolution/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionsResource.java:55 |
| GET | `P/W/conflictResolution/revisions/{revisionId}` |  |  | Revision | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionsResource.java:69 |
| GET | `P/W/conflictResolution/revisions/{revisionId}/configuration` |  |  | ProjectConfiguration |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionProjectConfigurationResource.java:46 |
| GET | `P/W/conflictResolution/revisions/{revisionId}/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionEntitiesResource.java:54 |
| GET | `P/W/conflictResolution/revisions/{revisionId}/entities/{path}` |  |  | Entity |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionEntitiesResource.java:83 |
| GET | `P/W/conflictResolution/revisions/{revisionId}/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | throws/500 | conflictResolution/project/user/ConflictResolutionWorkspaceRevisionEntityPathsResource.java:53 |
| GET | `P/W/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes | CORE | ok (POSIX only) | entity/project/user/WorkspaceEntitiesResource.java:62 |
| POST | `P/W/entities` |  | UpdateEntitiesCommand | Revision | yes |  | create-only (bug) | entity/project/user/WorkspaceEntitiesResource.java:107 |
| DELETE | `P/W/entities` |  | DeleteEntitiesCommand | Revision |  |  | ok | entity/project/user/WorkspaceEntitiesResource.java:90 |
| GET | `P/W/entities/{path}` |  |  | Entity | yes |  | ok | entity/project/user/WorkspaceEntitiesResource.java:119 |
| POST | `P/W/entities/{path}` |  | CreateOrUpdateEntityCommand | Revision |  |  | ok | entity/project/user/WorkspaceEntitiesResource.java:130 |
| DELETE | `P/W/entities/{path}` |  | DeleteEntityCommand | Revision |  |  | ok | entity/project/user/WorkspaceEntitiesResource.java:142 |
| GET | `P/W/entities/{path}/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | throws | revision/project/user/WorkspaceEntityRevisionsResource.java:54 |
| GET | `P/W/entities/{path}/revisions/{revisionId}` |  |  | Revision |  |  | throws | revision/project/user/WorkspaceEntityRevisionsResource.java:69 |
| POST | `P/W/entityChanges` |  | PerformChangesCommand | Revision | yes | CORE | ok except RENAME/move | entity/project/user/WorkspaceEntityChangesResource.java:50 |
| GET | `P/W/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | always 500 (bug) | entity/project/user/WorkspaceEntityPathsResource.java:52 |
| GET | `P/W/inConflictResolutionMode` |  |  | boolean | yes | CORE | always false | workspace/project/user/WorkspacesResource.java:91 |
| GET | `P/W/outdated` |  |  | boolean | yes | CORE | always false | workspace/project/user/WorkspacesResource.java:78 |
| GET | `P/W/packages/{path}/revisions` | ?since ?until ?limit |  | List<Revision> |  |  | throws | revision/project/user/WorkspacePackageRevisionsResource.java:54 |
| GET | `P/W/packages/{path}/revisions/{revisionId}` |  |  | Revision |  |  | throws | revision/project/user/WorkspacePackageRevisionsResource.java:69 |
| GET | `P/W/pureModelContextData` |  |  | PureModelContextData |  |  | via getEntities (untested) | pmcd/project/user/WorkspacePureModelContextDataResource.java:54 |
| GET | `P/W/revisions` | ?since ?until ?limit |  | List<Revision> | yes |  | throws | revision/project/user/WorkspaceRevisionsResource.java:54 |
| GET | `P/W/revisions/{revisionId}` |  |  | Revision | yes | CORE | ok | revision/project/user/WorkspaceRevisionsResource.java:68 |
| GET | `P/W/revisions/{revisionId}/configuration` |  |  | ProjectConfiguration | yes |  | ok (HEAD) | project/project/user/WorkspaceRevisionProjectConfigurationResource.java:48 |
| GET | `P/W/revisions/{revisionId}/configuration/availableGenerations` |  |  | List<ArtifactTypeGenerationConfiguration> |  |  | [] | project/project/user/WorkspaceRevisionProjectConfigurationResource.java:60 |
| GET | `P/W/revisions/{revisionId}/entities` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue ?excludeInvalid=false |  | List<Entity> | yes | CORE | revisionId IGNORED (HEAD) | entity/project/user/WorkspaceRevisionEntitiesResource.java:53 |
| GET | `P/W/revisions/{revisionId}/entities/{path}` |  |  | Entity |  |  | revisionId IGNORED (HEAD) | entity/project/user/WorkspaceRevisionEntitiesResource.java:82 |
| GET | `P/W/revisions/{revisionId}/entityPaths` | ?classifierPath ?package ?includeSubPackages=true ?name ?stereotype ?taggedValue |  | List<String> |  |  | always 500 (bug) | entity/project/user/WorkspaceRevisionEntityPathsResource.java:52 |
| GET | `P/W/revisions/{revisionId}/pureModelContextData` |  |  | PureModelContextData |  |  | via getEntities (untested) | pmcd/project/user/WorkspaceRevisionPureModelContextDataResource.java:49 |
| GET | `P/W/revisions/{revisionId}/upstreamProjects` | ?transitive=false |  | Set<ProjectDependency> |  |  | generic impl (unverified) | dependency/project/user/WorkspaceRevisionDependenciesResource.java:48 |
| POST | `P/W/update` |  |  | WorkspaceApi.WorkspaceUpdateReport | yes |  | throws | workspace/project/user/WorkspacesResource.java:131 |
| GET | `P/W/workflows` | ?revisionId ?status ?limit |  | List<Workflow> | yes |  | throws | workflow/project/user/WorkspaceWorkflowsResource.java:52 |
| GET | `P/W/workflows/{workflowId}` |  |  | Workflow | yes |  | throws | workflow/project/user/WorkspaceWorkflowsResource.java:69 |
| GET | `P/W/workflows/{workflowId}/jobs` | ?status |  | List<WorkflowJob> | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:55 |
| GET | `P/W/workflows/{workflowId}/jobs/{workflowJobId}` |  |  | WorkflowJob | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:68 |
| POST | `P/W/workflows/{workflowId}/jobs/{workflowJobId}/cancel` |  |  | WorkflowJob | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:132 |
| GET | `P/W/workflows/{workflowId}/jobs/{workflowJobId}/logs` |  |  | Response | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:82 |
| POST | `P/W/workflows/{workflowId}/jobs/{workflowJobId}/retry` |  |  | WorkflowJob | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:117 |
| POST | `P/W/workflows/{workflowId}/jobs/{workflowJobId}/run` |  |  | WorkflowJob | yes |  | throws | workflow/project/user/WorkspaceWorkflowJobsResource.java:103 |

### 2.1 Auth routes (not under `RES/`; one copy per backend, route-identical)

| Method | Path | Query | Returns | GitLab impl | FS impl | Studio |
|---|---|---|---|---|---|---|
| GET | `/auth/authorized` | — | boolean | looks up pac4j session (or GitLab PAT header), `GitLabUserContext.isUserAuthorized()` — `SRV/gitlab/resources/GitLabAuthCheckResource.java:67-100` | `return true` — `FS/resources/FileSystemAuthCheckResource.java:39-44` | yes (boot) |
| GET | `/auth/authorize` | `redirect_uri` | text/html; 302 to `redirect_uri` | forces GitLab OAuth (`getGitLabAPI(true)`) then 302 — `GitLabAuthResource.java:61-72` | `<html><h1>Success</h1></html>` (ignores redirect_uri!) — `FileSystemAuthResource.java:47-56` | browser redirect via `SDLCServerClient.authorizeCallbackUrl` (`SC/SDLCServerClient.ts:237-246`) |
| GET | `/auth/callback` | `code`, `state` | — | OAuth callback — `GitLabAuthResource.java:54-58` | throws — `FileSystemAuthResource.java:40-45` | no |
| GET | `/auth/termsOfServiceAcceptance` | — | `string[]` (URLs to visit) | `[]` or `[gitLabServerUrl]` when GitLab 403s with "terms of service" — `GitLabAuthResource.java:77-110` | `[]` — `FileSystemAuthResource.java:58-64` | yes (boot) |

### 2.2 What Studio calls, in order

**Boot** (`legend-application-studio/src/stores/LegendStudioBaseStore.ts`):
1. `GET /auth/authorized` (`:631`); if `false`, full-page redirect to `/auth/authorize?redirect_uri=<current URL>[&client_name=…]` (`:633-640`).
2. `GET /auth/termsOfServiceAcceptance` (`:580`) — non-empty → modal asking user to visit the URLs.
3. `GET /server/platforms` then `GET /server/features` (`:611-612`).
4. `GET /currentUser` (`:222`) → sets the app identity (falls back to engine's current user if anonymous, `:240-252`).
Every request gets `client_name=<config.sdlcServerClient>` appended if configured (`SC/SDLCServerClient.ts:184-209`).

**Workspace setup** (`stores/workspace-setup/WorkspaceSetupStore.ts`): `GET /projects?user&search&tag&excludeTag&limit`
(`:558,660`), `GET /projects/{id}`, `GET P/patches` (`:730`, errors are caught + notified), `GET
P/configuration/projectConfigurationStatus` (via `ProjectConfigurationStatus.ts:39`), `GET P/conflictResolution`
(`:765` — note the client ignores the patch argument, `SC/SDLCServerClient.ts:1184-1188`), `GET P/workspaces` **and**
`GET P/groupWorkspaces` (merged, `SC/SDLCServerClient.ts:467-476`), the same for each patch, `POST P/W` to create.

**Editor load** (`stores/editor/EditorSDLCState.ts`): `GET P/W` (`:295`), `GET P/W/inConflictResolutionMode` (`:346`),
`GET P/W/revisions/CURRENT` (`:367`), `GET P/W/outdated` (`:422`), `GET P/W/revisions/{id}/entities` and
`P/revisions/…/entities` incl. alias `BASE` (`:445-498`), `GET P/W/configuration` (`EditorStore.ts:941,973`),
`GET P/authorizedActions` (`:573`), `GET /configuration/latestProjectStructureVersion`.

**Save ("push")**: `POST P/W/entityChanges` with `{message, entityChanges, revisionId: activeRevision.id}`
(`stores/editor/sidebar-state/LocalChangesState.ts:421-438`); before that Studio asserts the server's CURRENT
revision equals its local base (`EditorSDLCState.ts:445-455`) — i.e. **optimistic concurrency on revision id is
load-bearing**. Model import uses `POST P/W/entities` (`UpdateEntitiesCommand`, `ModelImporterState.ts:266,362,569`).

**Review**: `POST P/reviews`, `GET P/reviews?state&workspaceIdRegex&workspaceTypes&revisionIds&since&until&limit`
(`WorkspaceReviewState.ts:212`, `ProjectOverviewState.ts:333-369`), `GET P/reviews/{id}`, `/approval`,
`/approve`, `/reject`, `/close`, `/reopen`, `POST /commit`, plus review comparison routes.

**Publish**: `GET P/versions/latest` (`ProjectOverviewState.ts:303`), `GET P/revisions/{id}`, `POST P/versions`
`{versionType, revisionId, notes}` (`ProjectOverviewState.ts:412`); server gates on `features.canCreateVersion` and
rejects `EMBEDDED` projects (`RES/version/VersionsResource.java:140-160`).

**Never called by Studio** (whole families): `pureModelContextData` (all variants — that is an *engine*-facing
route), `entityPaths`, entity/package revision history (`…/entities/{path}/revisions`, `…/packages/{path}/revisions`),
`revisions/{rev}/status`, `upstreamProjects`/`downstreamProjects`, builds, issues, backup (all), `/configuration/capabilities`,
`/configuration/projectStructureVersions`, `/info`, `/server/info`, `/users/{id}`, project `DELETE`, workspace-creation /
project-latest comparisons, review `edit`/`update`/`updateStatus`/`outdated`/`revokeApproval`, workflow listings of reviews.
Also note three client helpers can produce **routes the server does not have** if called with `workspace === undefined`:
`POST P/entities`, `POST P/entityChanges`, `POST P/configuration` (the `_adaptiveWorkspace` falls back to the project path,
`SC/SDLCServerClient.ts:459-465,946-965,628-637`); Studio only ever calls them with a workspace.

### 2.3 Minimal route set for a lite "author → save → (review) → publish" loop

Derived from §2.2 (the `CORE` column), the smallest Studio-compatible surface is:

```
GET  /auth/authorized                       -> true
GET  /auth/termsOfServiceAcceptance         -> []
GET  /server/platforms, /server/features    -> [...], {canCreateProject, canCreateVersion}
GET  /currentUser                           -> {userId, name}
GET  /configuration/latestProjectStructureVersion
GET  /projects  (+ ?search ?user ?tag ?excludeTag ?limit), GET /projects/{id}, POST /projects
GET  /projects/{id}/configuration/projectConfigurationStatus
GET  /projects/{id}/patches                 -> [] (or tolerate the caught error)
GET  /projects/{id}/conflictResolution      -> []
GET  /projects/{id}/workspaces, /groupWorkspaces           ; GET|POST|DELETE  .../{workspaceId}
GET  .../{ws}/outdated, .../{ws}/inConflictResolutionMode  -> false, false
GET  /projects/{id}[/workspaces/{ws}]/revisions/{rev|BASE|CURRENT|HEAD|LATEST}
GET  /projects/{id}[/workspaces/{ws}]/entities, .../revisions/{rev}/entities
GET  /projects/{id}[/workspaces/{ws}]/configuration        ; POST .../{ws}/configuration
POST /projects/{id}/workspaces/{ws}/entityChanges   (revisionId-checked; 409 on mismatch)
GET  /projects/{id}/authorizedActions                (Studio calls it on load)
GET  /projects/{id}/versions, /versions/latest, POST /versions, GET /versions/{v}/entities, /versions/{v}/configuration
(review, optional) POST|GET /projects/{id}/reviews, GET .../{rid}, POST .../{rid}/commit|close|approve, GET .../{rid}/comparison
```
Note the "review" step is the **only** way the GitLab backend moves workspace changes into the project head
(merge request accept, `SRV/gitlab/api/GitLabReviewApi.java:597`); if a lite server drops reviews it still needs *some*
way to land a workspace onto the project line (e.g. auto-commit on review create, or a "workspace = project" model).

---

## 3. Project structure: how entities live as files

### 3.1 Storage SPI (L1, `legend-sdlc-project-files`)

A backend is ultimately a `ProjectFileAccessProvider` (`PF/files/ProjectFileAccessProvider.java:33-283`):

| Piece | Contract |
|---|---|
| `FileAccessContext getFileAccessContext(projectId, SourceSpecification, revisionId)` | `getFilesInDirectories(...)`, `getFile(path)`, `fileExists(path)`; paths are project-root-relative, start with `/` (`:37-110,192`) |
| `ProjectFile` | `getPath()`, `getContentAsInputStream()` (+ bytes/string/reader defaults) (`:114-180`) |
| `RevisionAccessContext getRevisionAccessContext(projectId, sourceSpec, paths)` | `getBaseRevision`, `getCurrentRevision`, `getRevision(id)`, `getAllRevisions(predicate, since, until, limit)`; `paths` restricts history to files (`:209-242`) |
| `FileModificationContext getFileModificationContext(projectId, sourceSpec, revisionId)` | `Revision submit(message, List<ProjectFileOperation>)` — atomic commit; `revisionId` is the expected current revision (`:247-275`) |
| `ProjectFileOperation` | `AddFile(path, content)`, `ModifyFile(path, newContent)`, `DeleteFile(path)`, `MoveFile(path, newPath, newContent?)` (`PF/files/ProjectFileOperation.java:34-176`) |

Everything above that — entity enumeration, (de)serialization, create/modify/delete/rename → file operations,
`project.json` read/update, dependency resolution, comparison — is generic code in `legend-sdlc-core`
(`CORE/entity/EntityAccessOperations.java`, `EntityModificationOperations.java`, `project/ProjectStructureUpdater.java`,
`project/ProjectConfigurationUpdater.java`, `dependency/DependencyOperations.java`, `comparison/ComparisonOperations.java`).

### 3.2 Entity file formats

Two `EntitySerializer`s are registered via `ServiceLoader`:

| Name | Ext | Format | Cite |
|---|---|---|---|
| `legend` | `.json` | `{"classifierPath": "...", "content": {...}}`, pretty-printed, keys sorted (`ORDER_MAP_ENTRIES_BY_KEYS`, `SORT_PROPERTIES_ALPHABETICALLY`). **The entity path is not stored**; on read it is recomputed as `content.package + "::" + content.name` (error if absent). | `legend-sdlc-entity-serialization/.../DefaultJsonEntitySerializer.java:37-173` |
| `pure` | `.pure` | Pure grammar text of **exactly one element** (plus at most one `SectionIndex`, no imports allowed), rendered by engine's `PureGrammarComposer` in `PRETTY` style; `canSerialize` = classifier supported by the protocol converter **and** a successful serialize→parse round-trip. | `legend-sdlc-protocol-pure/.../PureEntitySerializer.java:50-140,200-262` |

`EntitySourceDirectory` maps entity path ↔ file path: `dir + "/" + path.replace("::", "/") + "." + ext`
(`PS/EntitySourceDirectory.java:92-115,197-213`). When **writing**, the first source directory whose serializer
`canSerialize` the entity wins (`PS/ProjectStructure.java:174-177`); when **reading**, all source directories are
searched (`:155-166`). A MODIFY whose preferred directory changes becomes a `MoveFile` (`CORE/entity/EntityModificationOperations.java:395-421`);
RENAME is a `MoveFile` in the same directory (`:423-436`).

### 3.3 Project structure versions present upstream

Registered factories: V0, V11, V12, V13 (`legend-sdlc-project-structure/src/main/resources/META-INF/services/org.finos.legend.sdlc.project.structure.ProjectStructureVersionFactory`).
Versions 1–10 exist only in the (private) "origin project" and are loaded by `ServiceLoader` there (`docs/re-architecture.md:532-537`).

| Version | Layout | Artifacts | Cite |
|---|---|---|---|
| 0 | `/project.json` (may be absent) + `/entities/**.json` (legend serializer); no Maven build | `entities` only | `PS/ProjectStructureV0Factory.java:48-90` |
| 11 | Multi-module Maven: `/pom.xml`, `/project.json`, module `/<artifactId>-entities/` with source dirs `src/main/pure` (`.pure`) **then** `src/main/legend` (`.json`); other modules `<artifactId>-versioned-entities`, `-service-execution`, `-file-generation`; `src/test/java/org/finos/legend/sdlc/EntityValidationTest.java` (+ `EntityTestSuite.java`) in entities module | `entities`, `versioned_entities`, `service_execution`, `file_generation` | `PS/ProjectStructureV11Factory.java:61-114`; module naming `PS/maven/MultiModuleMavenProjectStructure.java:517-546` |
| 12 | same as 11; bumps pinned platform versions (legend-sdlc 0.40.0→0.69.1, legend-engine 2.37.0→2.57.0); surefire config flag flipped | same | diff of V11 vs V12 factories (`:78,84,297`) |
| 13 (latest) | same layout; legend-sdlc 0.129.0, legend-engine 4.12.1; drops `EntityTestSuite.java` (deleted on upgrade), adds `legend-sdlc-test-generation-maven-plugin` (JUnit test generation), extension-collection deps (`legend-engine-extensions-collection-generation`/`-execution`, `legend-sdlc-extensions-collection-entity-serializer`) overridable per platform | same | `PS/ProjectStructureV13Factory.java:62-126,168-237,268-373` |

Concrete V13 tree for `artifactId = my-model`:

```
/project.json
/pom.xml                                     (parent; version 0.0.1-SNAPSHOT  MavenProjectStructure.java:274-277)
/.gitlab-ci.yml                              (only if a structure *extension* adds it, e.g. FINOS GitLab)
/my-model-entities/pom.xml
/my-model-entities/src/main/pure/model/domain/Person.pure
/my-model-entities/src/main/legend/model/domain/SomethingNotPureSerializable.json
/my-model-entities/src/test/java/org/finos/legend/sdlc/EntityValidationTest.java
/my-model-versioned-entities/pom.xml
/my-model-service-execution/pom.xml
/my-model-file-generation/pom.xml
```

Structure **extensions** (`PS/extension/ProjectStructureExtension(Provider).java`) add deployment-specific files keyed by
(structure version, extension version) — e.g. FINOS adds `.gitlab-ci.yml` for versions 11–13
(`SRV/gitlab/finos/FinosGitlabProjectStructureExtensionProvider.java:28-55`, `.../finos/finos-extension.yaml`).
Updating the structure version (`POST …/configuration` with `projectStructureVersion`) regenerates poms/test files via
`collectUpdateProjectConfigurationOperations` (`PS/ProjectStructure.java:191-198`, e.g. V13 `:168-237`), routed through
`CORE/project/ProjectStructureUpdater.java`. Two companion plans (`docs/project-structure-configuration-options.md`,
`docs/project-layout-reconciliation.md`) intend to make options namespaced and the write-side declarative; not landed.

### 3.4 What a version build produces (Maven)

Per `PS/ProjectStructureV13Factory.java:268-361`:

| Module | Plugin(s) | Output |
|---|---|---|
| `<a>-entities` | `legend-sdlc-entity-maven-plugin` (re-serializes `.pure`/`.json` sources into `target/classes/entities/**.json`, `legend-sdlc-entity-maven-plugin/.../EntityReserializer.java:77`), `legend-sdlc-generation-model-maven-plugin`, `legend-sdlc-test-generation-maven-plugin`; tests via `legend-sdlc-test-utils` | **entities jar**: JSON entity files under `entities/` (the layout `EntityLoader` reads, `legend-sdlc-entity-serialization/.../EntityLoader.java:55,248`) |
| `<a>-versioned-entities` | `legend-sdlc-version-package-maven-plugin` over the entities module output | entities with every package prefixed `<groupId as ::-path>::<artifactId>::v<M_m_p>::` (or `versionAlias`), cross-project references rewritten (`legend-sdlc-version-package-maven-plugin/.../VersionQualifiedPackageMojo.java:205-352`) |
| `<a>-service-execution` | `legend-sdlc-generation-service-maven-plugin` + shade plugin | generated Java service-execution classes (shaded jar when `produceShadedServiceJar`) |
| `<a>-file-generation` | `legend-sdlc-generation-file-maven-plugin` | file generations (Avro/Protobuf/etc. outputs of `FileGenerationSpecification` elements) |

---

## 4. Versioning and publishing

- **API**: `POST P/versions {versionType, revisionId?, notes?}` → `VersionApi.newVersion(projectId, type, revisionId, notes)`
  (`API/version/VersionApi.java:87`). Gated by `features.canCreateVersion` (405 if false) and 409 for `EMBEDDED`
  projects (`RES/version/VersionsResource.java:140-160`). There is no "explicit version number" input: the next id is
  computed from the latest existing version (`latest.nextMajor/Minor/PatchVersion()`, starting from 0.0.0)
  (`SRV/gitlab/api/GitLabVersionApi.java:70-101`).
- **GitLab semantics**: a version **is a git tag** `release-<M.m.p>` on a commit of the project's default branch
  (or of the patch branch) — no build is triggered by the SDLC server itself. If `revisionId` is null the branch head
  is used; otherwise the revision must be on that branch (400 otherwise). `notes` become a GitLab *Release* on the tag
  (`SRV/gitlab/api/BaseGitLabApi.java:122,654-669,1358-1400`). Listing versions = listing tags matching the prefix
  (`GitLabVersionApi.java:103-107`).
- **Patches**: `POST P/patches "<M.m.p>"` creates branch `patch/main/<M.m.p+1>` from the version tag; workspaces can be
  sourced from it (`patch/<M.m.p>/workspace/...`); `POST …/patches/{v}/release` tags the patch branch head as version
  `v` (`BaseGitLabApi.java:107,575-578`; `GitLabPatchApi.java:318`).
- **Build/deploy to Maven** happens entirely in the hosting platform's CI, defined by the project-structure
  *extension* file, not by SDLC. The FINOS extension's `.gitlab-ci.yml` (both `gitlab-ci-1.yml` and `-2.yml`) only has a
  `verify_snapshot` job that runs `mvn clean deploy` to a **local directory** on workspace branches and is explicitly
  disabled for tags and `master` (`SRV/.../gitlab/finos/gitlab-ci-2.yml:31-49`). **Nothing in this repo deploys release
  artifacts to a Maven repository or notifies Depot**; that wiring is deployment-specific (internal CI templates).
  I could not find, in this repo, how Depot learns about a new version — Depot is only consumed (see below).
- **Depot relation**: the SDLC server *reads* from Depot (`GET {depot}/api/projects/{groupId}/{artifactId}/versions/{v}`
  and `…/projectDependencies`, `SRV/depot/api/DepotMetadataApi.java:48-49`), and only `TestModelBuilder` uses it
  (`grep` shows `MetadataApi` referenced only from `SRV/guice/BaseModule.java` and `SRV/domain/api/test/TestModelBuilder.java`).
  Upstream/downstream dependency routes are computed from SDLC's own project configs (`API/dependency/DefaultDependenciesApi.java`).
  So "published" = (a) a tag/version in SDLC, readable through `GET P/versions/{v}/entities|configuration`, plus (b) whatever
  jars the CI deploys (entities, versioned-entities, service-execution, file-generation) that Depot later ingests.

---
## 5. Backends

### 5.1 GitLab backend (summary)

Lives in `SRV/gitlab/**` (≈50 files; to be extracted to `legend-sdlc-backend-gitlab` in Phase 5). Registered as the
only `BackendFactory` (`legend-sdlc-server/src/main/resources/META-INF/services/org.finos.legend.sdlc.backend.api.spi.BackendFactory`
→ `GitLabBackendFactory`); declares every capability (`docs/re-architecture-worklog.md:589-593`).

| SDLC concept | GitLab mapping | Cite |
|---|---|---|
| Project | GitLab project; SDLC projectId = `<prefix>-<gitlabNumericId>` (or bare id) | `SRV/gitlab/GitLabProjectId.java:23,81-112`; create with configured visibility (default INTERNAL) + tags `SRV/gitlab/api/GitLabProjectApi.java:88,216-251` |
| Project line | default branch (`master`) | `BaseGitLabApi.java:116,483-486` |
| User workspace | branch `workspace/<userId>/<id>` | `BaseGitLabApi.java:98,109` (branch-name regex) |
| Group workspace | branch `group/<id>` | `:101` |
| Conflict-resolution ws | `resolution/<user>/<id>`, `group-resolution/<id>` | `:99,102` |
| Backup ws | `backup/<user>/<id>`, `group-backup/<id>` (made when conflict resolution replaces a workspace) | `:100,103`; `GitLabConflictResolutionApi.java:95-130` |
| Patch-sourced ws | prefix `patch/<M.m.p>/…` | `:104,109` |
| Patch line | branch `patch/main/<M.m.p>` | `:107,575-578` |
| Temp branches | `tmp/…` (used by workspace update's trial rebase) | `:106`; `GitLabWorkspaceApi.java:495-590` |
| Workspace create | branch from source branch head (fails 409 if project structure not set up; cleans stale resolution branch) | `GitLabWorkspaceApi.java:342-406` |
| Workspace outdated / update | base ≠ source head; update = rebase via temp branch → `NO_OP`/`UPDATED`/`CONFLICT` (conflict ⇒ conflict-resolution workspace) | `GitLabWorkspaceApi.java:224,495-590` |
| Review | Merge request workspace→source branch, `removeSourceBranch=true`; states from MR `opened/closed/locked/merged` | `GitLabReviewApi.java:368`; `BaseGitLabApi.java:111-114` |
| Commit review | approval-count check, project-config validation, then MR accept (merge, delete workspace branch) | `GitLabReviewApi.java:580-597` |
| Version | tag `release-M.m.p` (+ GitLab Release for notes) | `BaseGitLabApi.java:122,654-669,1358-1400` |
| Workflow / job | GitLab pipeline / job (`AbstractGitlabWorkflowApi`, `GitlabWorkflowApi`, `GitlabWorkflowJobApi`) | `SRV/gitlab/api/` |
| Build (legacy) | pipeline (`GitLabBuildApi`) | `SRV/gitlab/api/GitLabBuildApi.java` |
| Issue | GitLab issue (`GitLabIssueApi`) | |
| Auth | pac4j (GitLab OIDC/OAuth, Kerberos/SAML, personal access token header) → per-user GitLab token in session | `SRV/gitlab/auth/**` |

### 5.2 `legend-sdlc-server-fs` in depth

**What it is.** A separate Dropwizard application (`FS/startup/LegendSDLCServerFS.java:18-113`, name "Metadata SDLC",
`main` at `:109-113`) that reuses all of `legend-sdlc-server`'s JAX-RS resources (bound one by one in
`FS/startup/FSModule.java:230-422`) but binds every domain API to a `FileSystem*Api` (`FSModule.java:456-477`).
The worklog characterizes it as "29 files largely because it had to stub every API" (`docs/re-architecture.md:260-265`)
and plans to replace it with `legend-sdlc-backend-fs` on the generic defaults in Phase 5 (`docs/re-architecture.md:672-677`).

**Configuration.** `LegendSDLCServerFSConfiguration` = the normal server config + `fileSystem: {rootDirectory}`
(`FS/startup/LegendSDLCServerFSConfiguration.java:6-15`, `FSConfiguration.java:6-25`). The root directory is
created at startup if missing (`FSModule.java:436-444`). Example configs:
- Docker: `legend-sdlc-server-fs/src/main/resources/docker/config/config.json` — `"fileSystem": {"rootDirectory": ${FS_ROOT_DIR}}`,
  `features {canCreateProject:true, canCreateVersion:true}`, FINOS GitLab extension provider, **and still a pac4j GitLab
  OIDC client + `gitLab` section** (so the docker image authenticates *users* against GitLab even though storage is local).
  Run with `java -cp …shaded.jar org.finos.legend.sdlc.server.startup.LegendSDLCServerFS server /config/config.json`
  (`legend-sdlc-server-fs/Dockerfile`).
- Test config: `legend-sdlc-server-fs/src/test/resources/config.yml` — pac4j `AnonymousClient`, bypass paths
  `/api/info`, `/api/server/info`, `/api/server/platforms`, `/api/auth/authorized`; `rootDirectory: /root/AlloyProjects`;
  `canCreateVersion: false`; `projectCreation.groupIdPattern`.
- `ServerPlatformInfo(null,null,null)` (`LegendSDLCServerFS.java:103-107`); a single-thread `BackgroundTaskProcessor` (`:71`).

**Storage model — one non-bare git repo per project, managed with JGit:**

| Concept | FS representation | Cite |
|---|---|---|
| Project | directory `<rootDirectory>/<name>/` containing a **non-bare** `.git`; **projectId = name** (no validation, no uniqueness check beyond git) | `FS/api/project/FileSystemProjectApi.java:108-133`; `FS/api/BaseFSApi.java:36-52` |
| Project metadata | `[project] id/name/description` in the repo's `.git/config` | `FileSystemProjectApi.java:124-127,188-204` |
| Project creation | `git init`, empty "Initial Commit", then the standard `ProjectStructureUpdater` writes `project.json`/poms as a 2nd commit "Build project structure" (default structure version = configured or latest = 13; extension version from provider unless EMBEDDED) | `FileSystemProjectApi.java:118-152,155-167` |
| Project listing | every sub-directory of root that opens as a git repo; all filter params (`user`, `search`, `tag`, `limit`) ignored | `FileSystemProjectApi.java:76-105` |
| Project line | branch `master` (hard-coded) | `BaseFSApi.java:73-76`; `FileSystemApiWithFileAccess.java:192` |
| Workspace | branch named exactly as GitLab (`workspace/local_user/<id>`, `group/<id>`, `patch/<v>/…`, etc.) created from `master`; type recorded as `branch.<name>.type = user|group` in git config | `FS/api/workspace/FileSystemWorkspaceApi.java:49-57,109-136,180-259` |
| Workspace id rule | `[A-Za-z0-9_][A-Za-z0-9_.-]*[A-Za-z0-9_]`, no `..` | `FileSystemWorkspaceApi.java:138-178` |
| Revision | git commit (id = SHA, author/committer from JGit defaults) | `FS/domain/model/revision/FileSystemRevision.java:26-101`; `FileSystemApiWithFileAccess.java:247-256` |
| Read a file | resolve **branch head** tree, read blob — the `revisionId` argument is **ignored** | `FileSystemApiWithFileAccess.java:101-128,130-150` |
| Write files | `git checkout <branch>` **in the shared working tree**, write/modify/delete files on disk, `git add .` / `git rm`, `git commit`; optimistic check: if a `revisionId` was supplied and ≠ branch head → 409 (but re-wrapped to 500, see bugs) | `FileSystemApiWithFileAccess.java:259-339` |
| User | single hard-coded user `local_user` / `local_user` | `FS/api/user/FileSystemUserApi.java:29-60` |

**API support matrix** (every method of every FS API class):

| API | Works | Returns stub value | Throws "Feature unavailable"/"Not implemented" |
|---|---|---|---|
| `ProjectApi` | `getProject`, `getProjects`, `createProject` | `getTags()`/`getProjectType()`/`getWebUrl()` = null | `deleteProject`, `changeProjectName`, `changeProjectDescription`, `updateProjectTags`, `setProjectTags`, `getCurrentUserAccessRole`, `checkUserAuthorizedActions`, `checkUserAuthorizedAction`, `importProject`, `getAllUsersAuthorizedActions` (`FileSystemProjectApi.java:225-283`) |
| `WorkspaceApi` | `getWorkspace`, `getWorkspaces`/`getAllWorkspaces` (only `WORKSPACE` access type), `newWorkspace` | `isWorkspaceOutdated`=false, `isWorkspaceInConflictResolutionMode`=false | `deleteWorkspace`, `updateWorkspace`; `getWorkspaces(…, sources=null)` (`FileSystemWorkspaceApi.java:65-136,261-283`) |
| `EntityApi` | `getEntityAccessContext` (project/workspace), `getEntityModificationContext` | — | review from/to contexts (`FS/api/entity/FileSystemEntityApi.java:79-89`); version/patch sources fail in `getRefBranchName` visitor (`BaseFSApi.java:69-83`; `PF/source/SourceSpecificationVisitor.java` defaults throw) |
| `RevisionApi` | `getRevisionContext` → `getRevision`, `getCurrentRevision`, `getBaseRevision` | — | `getRevisions` (list — `getAllRevisions` "Not implemented", `FileSystemApiWithFileAccess.java:242-245`), `getPackageRevisionContext`, `getEntityRevisionContext`, `getRevisionStatus` (`FS/api/revision/FileSystemRevisionApi.java:49-65`) |
| `ProjectConfigurationApi` | `getProjectConfiguration`, `getProjectConfigurationStatus`, `getLatestProjectStructureVersion` | `getAvailableArtifactGenerations` = [], status `reviewIds` = [] | `updateProjectConfiguration`, review from/to (`FS/api/project/FileSystemProjectConfigurationApi.java:47-105`) |
| `ReviewApi` | — | both `getReviews` overloads = [] | all others (`FS/api/review/FileSystemReviewApi.java:40-121`) |
| `VersionApi` | — | `getVersions` = [] | `getLatestVersion`, `getVersion`, `newVersion` (`FS/api/version/FileSystemVersionApi.java:35-55`) |
| `PatchApi`, `IssueApi`, `BuildApi`, `WorkflowApi`, `WorkflowJobApi`, `ComparisonApi`, `ConflictResolutionApi`, `MetadataApi` (depot) | — | — | all methods (`FS/api/{patch,issue,build,workflow,comparison,conflictresolution}/*`, `FS/depot/FileSystemMetadataApi.java`) |
| `BackupApi` | — | **silently no-op**: both methods call `FSException.unavailableFeature()` *without* `throw` (`FS/api/backup/FileSystemBackupApi.java:31-40`) | — |
| `UserApi` | all (single `local_user`) | | |
| `DependenciesApi` | bound to the generic impl (`FSModule.java:446-449`) — works only as far as config reads work | | |
| `Backend` (SPI) | — | — | `provideBackend()` throws → `GET /configuration/capabilities` 500s (`FSModule.java:424-434`) |
| `/auth/*` | `authorized`=true, `authorize`=static HTML, `termsOfServiceAcceptance`=[] | | `callback` |

**Defects (verified in code, several pinned by upstream's own characterization test
`legend-sdlc-server-fs/src/test/java/.../TestFileSystemEntityApiCharacterization.java:46-65` and worklog
`docs/re-architecture-worklog.md:236-262`):**

1. **User/group workspace listing is swapped**: `if (types contains GROUP) add branches typed "user"` and vice-versa
   (`FileSystemWorkspaceApi.java:89-98`). `GET P/workspaces` therefore returns group workspaces (as type USER with
   `userId=local_user`) and `GET P/groupWorkspaces` returns user workspaces with `userId=null`. Because Studio derives the
   type from `userId` and merges both lists, Studio shows each workspace with the *wrong* type.
2. `getWorkspace` on a missing branch → `NullPointerException` (dereferences `branch.getName()` before its null check,
   `FileSystemWorkspaceApi.java:308-314`) → 500 instead of 404.
3. `revisionId` is ignored for reads: `GET …/revisions/{rev}/entities` and `…/revisions/{rev}/configuration` return
   branch **HEAD** (`FileSystemEntityApi.java:65-76` passes `null`; `FileSystemApiWithFileAccess.java:101-128` reads the branch).
4. `getEntityPaths` (all `entityPaths` routes) **always 500s**: `getFilesInCanonicalDirectories` does
   `ObjectId.fromString(null)` and compares `/`-prefixed directories against git paths without `/`
   (`FileSystemApiWithFileAccess.java:72-99`).
5. `updateEntities` (`POST …/entities`, `POST …/entities/{path}` create-or-update) cannot see existing entities (same
   enumeration bug) → modifying fails "already exists", `replace=true` deletes nothing.
6. `MoveFile` unsupported in `submit` (`FileSystemApiWithFileAccess.java:325-328`) → **RENAME** entity changes, and any
   MODIFY that moves an entity between the `pure` and `legend` source directories, fail.
7. `getEntities` enumeration relativizes with `java.nio.Path` string concat → empty on Windows
   (`FileSystemEntityApi.java:226-267`).
8. Stale `revisionId` on submit is detected (409) but `FSException.getLegendSDLCServerException` re-wraps it as 500
   (`FileSystemApiWithFileAccess.java:284-289,334-337`; `FS/exception/FSException.java:12-16`) — Studio's push
   conflict detection therefore sees a generic error.
9. `getBaseRevision` for a workspace uses a plain `RevWalk` with two starts and takes `next()` — without
   `RevFilter.MERGE_BASE` that returns the newest commit of either branch, not the merge base, despite the comment
   (`FileSystemApiWithFileAccess.java:184-214`). Studio requests `BASE` in three places (`EditorSDLCState.ts:498`,
   `WorkspaceUpdaterState.ts:345`, `WorkspaceUpdateConflictResolutionState.ts:435`). *(This one is my reading of JGit
   semantics; not covered by an upstream test.)*
10. **Not concurrency-safe**: every write does `git checkout` in the one shared working tree and `git add .`
    (`FileSystemApiWithFileAccess.java:291-300`; also `newWorkspace` checks out master, `FileSystemWorkspaceApi.java:126`);
    concurrent writes to different workspaces of one project race on HEAD/index; any stray file in the work tree is committed.
11. No way to land workspace changes on `master` (no review commit, no merge) and no versions → an FS deployment can
    create projects and edit workspaces, but **cannot publish**.

**Intended use.** Not documented in a README (the module has none). Evidence: FINOS docker packaging, the "AlloyProjects"
root in the test config, the hard-coded `local_user`, and the plan text "the existing filesystem backend, plus an
in-memory backend for testing" (`docs/re-architecture.md:53-55`) indicate a **local/dev single-user** server. It is
*not* the closest analogue to a production model home; the closest analogue is the **storage SPI + core** (§5.3–5.4),
which the FS module only partially uses.

### 5.3 The backend SPI (L4, `legend-sdlc-backend-api`) — the contract a lite backend would implement

`Backend` (deployment-scoped) and `BackendSession` (per-user) (`API/spi/Backend.java:24-48`, `BackendSession.java:55-139`):

```
interface BackendFactory { String getType(); Class<? extends BackendConfiguration> getConfigurationClass();
                           Backend build(BackendConfiguration, BackendEnvironment); }      // API/spi/BackendFactory.java:24-49 (ServiceLoader)
interface Backend extends AutoCloseable { String getType(); Set<BackendCapability> getCapabilities();
                           BackendSession newSession(BackendSessionContext); }
interface BackendSessionContext { String getUserId(); BackendSessionStateStore getStateStore(); }   // :23-38
interface BackendSessionStateStore { String get(String key); void put(String key, String value); }   // :25-41
interface BackendEnvironment { ObjectMapper getObjectMapper(); BackgroundTaskProcessor getTaskProcessor();
                           ProjectStructureExtensionProvider …; ProjectStructurePlatformExtensions …; } // :30-38
interface BackendSession { String getUserId(); ProjectApi; ProjectConfigurationApi; WorkspaceApi; RevisionApi; EntityApi;
                           ComparisonApi; DependenciesApi; UserApi; ReviewApi; VersionApi; PatchApi; WorkflowApi;
                           WorkflowJobApi; BuildApi; BackupApi; ConflictResolutionApi; IssueApi }
enum BackendCapability { REVIEWS, WORKFLOWS, VERSIONS, PATCHES, BUILDS, BACKUP, ISSUES, CONFLICT_RESOLUTION,
                         USER_WORKSPACES, GROUP_WORKSPACES }          // API/spi/BackendCapability.java:29-…
AuthorizationRequiredException(URI) ; UnsupportedCapabilityException(capability, backendType) -> HTTP 501
```

`AbstractBackend.Session` (`API/spi/AbstractBackend.java:80-177`) requires only `getProjectFileAccessProvider()` plus
the lifecycle APIs; it supplies `DefaultDependenciesApi` and `DefaultComparisonApi` and throws
`UnsupportedCapabilityException` for reviews, versions, patches, workflows, builds, backup, conflict resolution and
issues unless overridden. Core (never-optional) concepts per the enum javadoc: projects, workspaces (≥1 flavor),
revisions, entities, project configuration, dependencies, comparison (`API/spi/BackendCapability.java:17-28`).

**All 17 domain API interfaces — abstract (non-default) methods only** (≈172 further `default` convenience overloads exist):

| Interface | Abstract methods | Cite |
|---|---|---|
| `ProjectApi` | `getProject(id)`, `getProjects(user, search, tags, excludeTags, limit)`, `createProject(name, description, type, groupId, artifactId, tags)`, `deleteProject`, `changeProjectName`, `changeProjectDescription`, `updateProjectTags(id, remove, add)`, `setProjectTags`, `getCurrentUserAccessRole`, `checkUserAuthorizedActions(id, actions)`, `checkUserAuthorizedAction`, `importProject(id, type, groupId, artifactId)`, `getAllUsersAuthorizedActions` | `API/project/ProjectApi.java:26-120` |
| `ProjectConfigurationApi` | `getProjectConfiguration(projectId, sourceSpec, revisionId)`, `getReviewFromProjectConfiguration`, `getReviewToProjectConfiguration`, `updateProjectConfiguration(projectId, workspaceSourceSpec, message, ProjectConfigurationUpdater)`, `getAvailableArtifactGenerations`, `getProjectConfigurationStatus`, `getLatestProjectStructureVersion` | `API/project/ProjectConfigurationApi.java:32-56` |
| `WorkspaceApi` | `getWorkspace(projectId, WorkspaceSpecification)`, `getWorkspaces(projectId, types, accessTypes, sources)`, `getAllWorkspaces(…)`, `newWorkspace(projectId, workspaceId, type, source)`, `deleteWorkspace`, `isWorkspaceOutdated`, `isWorkspaceInConflictResolutionMode`, `updateWorkspace` → `WorkspaceUpdateReport` | `API/workspace/WorkspaceApi.java:30-132` |
| `RevisionApi` | `getRevisionContext(projectId, sourceSpec)`, `getPackageRevisionContext(…, packagePath)`, `getEntityRevisionContext(…, entityPath)`, `getRevisionStatus(projectId, revisionId)` | `API/revision/RevisionApi.java:25-33` |
| `RevisionAccessContext` | `getRevision(id)`, `getBaseRevision()`, `getCurrentRevision()`, `getRevisions(predicate, since, until, limit)` | `API/revision/RevisionAccessContext.java:23-77` |
| `EntityApi` | `getEntityAccessContext(projectId, sourceSpec, revisionId)`, `getReviewFromEntityAccessContext`, `getReviewToEntityAccessContext`, `getEntityModificationContext(projectId, workspaceSourceSpec)` | `API/entity/EntityApi.java:21-38` |
| `EntityAccessContext` | `getEntity(path)`, `getEntities(pathPred, classifierPred, contentPred, excludeInvalid)`, `getEntityPaths(pathPred, classifierPred, contentPred)` | `API/entity/EntityAccessContext.java:23-34` |
| `EntityModificationContext` | `updateEntities(entities, replace, message)`, `performChanges(changes, revisionId, message)` (create/update/delete/rename helpers are defaults) | `API/entity/EntityModificationContext.java:28-92` |
| `ComparisonApi` | `getWorkspaceCreationComparison`, `getWorkspaceSourceComparison`, `getReviewComparison`, `getReviewWorkspaceCreationComparison` | `API/comparison/ComparisonApi.java:26-65` (default impl `DefaultComparisonApi.java`) |
| `DependenciesApi` | `getWorkspaceRevisionUpstreamProjects`, `getProjectRevisionUpstreamProjects`, `getProjectVersionUpstreamProjects`, `getDownstreamProjects` | `API/dependency/DependenciesApi.java:25-55` (default `DefaultDependenciesApi.java`) |
| `ReviewApi` | `getReview`, `getReviews(projectId, state, revisionIds, wsPredicate, sources, since, until, limit)`, `getReviews(assignedToMe, authoredByMe, labels, wsPredicate, state, since, until, limit)`, `createReview`, `editReview`, `closeReview`, `reopenReview`, `approveReview`, `revokeReviewApproval`, `rejectReview`, `getReviewApproval`, `commitReview(projectId, reviewId, message)`, `getReviewUpdateStatus`, `updateReview` | `API/review/ReviewApi.java:34-202` |
| `VersionApi` | `getVersions(projectId, min/max major/minor/patch)`, `getLatestVersion(…)`, `getVersion(projectId, M, m, p)`, `newVersion(projectId, NewVersionType, revisionId, notes)` | `API/version/VersionApi.java:23-87` |
| `PatchApi` | `newPatch(projectId, sourceVersion)`, `getPatch`, `getPatches(…ranges)`, `deletePatch`, `releasePatch` → Version | `API/patch/PatchApi.java:23-71` |
| `WorkflowApi` / `WorkflowAccessContext` | `getWorkflowAccessContext(projectId, sourceSpec)`, `getReviewWorkflowAccessContext` / `getWorkflow(id)`, `getWorkflows(revisionIds, statuses, limit)` | `API/workflow/WorkflowApi.java:24-28`, `WorkflowAccessContext.java:22-27` |
| `WorkflowJobApi` / `WorkflowJobAccessContext` | `getWorkflowJobAccessContext`, `getReviewWorkflowJobAccessContext` / `getWorkflowJob`, `getWorkflowJobs(workflowId, statuses)`, `getWorkflowJobLog`, `runWorkflowJob`, `retryWorkflowJob`, `cancelWorkflowJob` | `API/workflow/WorkflowJobApi.java:24-28`, `WorkflowJobAccessContext.java:22-34` |
| `BuildApi` / `BuildAccessContext` | `getProjectBuildAccessContext`, `getWorkspaceBuildAccessContext(projectId, wsId, type, accessType)`, `getVersionBuildAccessContext` / `getBuild`, `getBuilds(revisionIds, statuses, limit)` | `API/build/BuildApi.java:23-43`, `BuildAccessContext.java:22-27` |
| `ConflictResolutionApi` | `discardConflictResolution`, `discardChangesConflictResolution`, `acceptConflictResolution(projectId, wsSpec, message, entityChanges, revisionId)` | `API/conflictresolution/ConflictResolutionApi.java:25-55` |
| `BackupApi` | `discardBackupWorkspace`, `recoverBackupWorkspace(projectId, wsSpec, forceRecovery)` | `API/backup/BackupApi.java:22-39` |
| `UserApi` | `getUsers`, `getUserById`, `findUsers(search)`, `getCurrentUserInfo` | `API/user/UserApi.java:21-29` |
| `IssueApi` | `getIssue`, `getIssues`, `createIssue`, `deleteIssue` | `API/issue/IssueApi.java:21-29` |

The TCK (`legend-sdlc-backend-test-suite`, package `org.finos.legend.sdlc.backend.tck`) ships
`BackendContractTestSuite` (capability declared ⇒ API works; undeclared ⇒ `UnsupportedCapabilityException`/501) and
`LayoutInvariantsTestSuite` (update ≡ create; reconcile no-op) (`docs/re-architecture-worklog.md:1016-1037`). The FS
backend does not run it yet (`:427-431`).

### 5.4 Existing in-memory implementations (test scope only)

`legend-sdlc-project-files/src/test/.../InMemoryProjectFileAccessProvider.java` + `SimpleInMemoryVCS.java` (an L1 provider),
and `legend-sdlc-server/src/test/.../inmemory/backend/**` (`InMemoryBackend` + one `InMemory*Api` per interface) —
useful as a reference for the smallest working implementation; not shipped.

---

## 6. Auth / user model and startup endpoints

- **Server-level auth** is pac4j (`legend-server` `LegendPac4jBundle`, `SHARED/BaseServer.java:80`) with
  `bypassPaths` (docker FS config bypasses only `/api/info`). Filter order configured in `filterPriorities`
  (`GitLab`, pac4j `CallbackFilter`, `SecurityFilter`, `CORS`). CORS: all origins, methods GET/PUT/POST/DELETE/OPTIONS,
  configurable allowed headers (`SHARED/BaseServer.java:96-111`). Session cookie name from `sessionCookie`
  (`:88-93`).
- **Backend-level auth (GitLab)**: per-user GitLab token obtained via OAuth; `GET /auth/authorized` reports whether the
  session has a usable token; when not, either 302 to GitLab or 403 with `auth_uri` (`docs/re-architecture-worklog.md:532-541`).
  FS: always authorized, user `local_user`.
- **User shape** `{userId, name}`; `GET /currentUser` (`RES/user/CurrentUserResource.java:44`), `GET /users?search`
  (`RES/user/UsersResource.java:47`), `GET /users/{userId}` (`:54`).
- **Per-project authorization**: `GET /projects/{id}/authorizedActions?actions=…` → subset of
  `CREATE_WORKSPACE, SUBMIT_REVIEW, COMMIT_REVIEW, CREATE_VERSION` (`RES/project/project/ProjectsResource.java:188`);
  Studio calls it on editor load to enable/disable UI (`EditorSDLCState.ts:573`). FS throws.
- **Info endpoints**:
  - `GET /info` and `GET /server/info` → `{hostName, initTime, platform:{version, buildTime, buildRevision}}`
    (`SHARED/BaseServer.java:140-226`; `RES/InfoResource.java:41`, `RES/ServerResource.java:49`).
  - `GET /server/features` → `{canCreateProject, canCreateVersion}` from config `features:`
    (`SRV/config/LegendSDLCServerFeaturesConfiguration.java:25-40`; Studio model `SC/models/server/SDLCServerFeaturesConfiguration.ts:21-27`).
  - `GET /server/platforms` → `[{name, groupId, platformVersion}]` from `projectStructure.platforms`
    (`PS/ProjectStructurePlatformExtensions.java:139-201`; Studio `SC/models/configuration/Platform.ts:22-24`).
  - `GET /configuration/latestProjectStructureVersion` → `{version, extensionVersion}` (`RES/project/ConfigurationResource.java:61`).
  - New (not used by Studio): `GET /configuration/capabilities` → `{backendType, capabilities[]}` (`:78-84`);
    `GET /configuration/projectStructureVersions` → per version `{version, configurationProperties[], extensionVersions[{extensionVersion, configurationProperties[]}]}`
    (`:86-118`; `ConfigurationProperty` = `{name, description, type ∈ BOOLEAN|STRING|INTEGER|ENUM|LIST, defaultValue, required, allowedValues}`,
    `MODEL/project/configuration/ConfigurationProperty.java:25-42`).

---

## 7. Surprises / important details

1. **The repo is mid-refactor toward exactly the abstraction legend-lite needs.** L1 `ProjectFileAccessProvider`
   (files + revisions + atomic submit) + L3 generic core is upstream's answer to "minimal backend"; a lite server could
   implement L1 and the project/workspace lifecycle and inherit entity/config/dependency/comparison semantics
   (`docs/re-architecture.md:118-145`).
2. **FS backend is not a model home**: it cannot list workspace/project history, review, merge, version or publish; it
   has ≥10 functional defects including swapped user/group listing and ignored `revisionId` (§5.2).
3. **`Workspace` JSON carries no type**; Studio infers it from `userId` (`SC/models/workspace/Workspace.ts:49-51`).
4. **Optimistic concurrency is by revision id**: Studio sends `revisionId` on `entityChanges` and refuses to compute
   local changes if CURRENT ≠ its base (`EditorSDLCState.ts:445-455`); a lite server must return a stable revision id and 409 on mismatch.
5. **Entity path is derived, not stored**, in JSON entity files (`package::name` from content), and `.pure` files must
   contain exactly one element with no imports. Write location depends on whether the *engine's* grammar composer can
   round-trip the element — the SDLC server embeds legend-engine's Pure parser/composer for this.
6. **Versions are just git tags**; artifacts are produced by the project's CI from Maven poms the structure generates;
   nothing in this repo deploys release jars or informs Depot (FINOS CI template disables tag builds).
7. **`project.json` absent ⇒ structure version 0**, which is a flat `/entities/**.json` layout with no Maven build —
   the simplest possible on-disk model home and still fully understood by all SDLC tooling.
8. **The REST surface is huge but highly regular** (459 routes): a lite server can implement it with a router over
   (scope = project | user ws | group ws | patch × access type) and a handful of handlers.
9. `GET P/patches` and `GET P/authorizedActions` are called unconditionally by Studio; FS throws on both. Studio
   catches both: patches → error toast (`WorkspaceSetupStore.ts:736-743`); authorized actions → logged, actions set to
   `undefined` (`EditorSDLCState.ts:570-585`).
10. FS's docker config still configures GitLab OIDC for login — "file system backend" ≠ "no GitLab".
11. Wire typo `auhorizedProjectAction` in `UserPermission` must be preserved for compatibility.
12. `GET /projects` `user` defaults to `true` (only projects the user is a member/developer of; `RES/project/project/ProjectsResource.java:67`).

### Could not determine
- How Depot is notified/ingests a new version (not in this repo; deployment CI).
- Exact semantics of FS `pureModelContextData` and `upstream/downstreamProjects` routes (generic code over buggy FS
  file access; not exercised by any upstream test) — marked "untested/unverified" in the table.
- `getBaseRevision` merge-base defect (§5.2 #9) is inferred from JGit `RevWalk` semantics, not from a test.

---

<!-- Part D -->
# Part D — Publishing and Depot (how a released model becomes readable by GAV)

Sources read (read-only):

| Alias | Repo / commit | Path |
|---|---|---|
| `dep:` | legend-depot @ `9c0a809` ("Mirror release branches to the Goldman Sachs fork (#605)") | `/Users/neema/legend/legend-lite-query/.scratch/legend-depot` |
| `st:` | legend-studio (packages) | `/Users/neema/legend/legend-lite-query/.scratch/legend-studio/packages` |
| `sdlc:` | legend-sdlc @ `1021fda` ("Bump version to 0.234.1-SNAPSHOT") | `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc` |
| `eng:` | legend-engine @ `230c159196d` (extra source, consulted only for the PMCD-pointer resolution path) | `/Users/neema/legend/legend-engine` |

Citation convention: depot module names drop the `legend-depot-` prefix and the deep Java package path is elided with `…/`, e.g. `core-data-services/…/ProjectsServiceImpl.java:179` = `dep:legend-depot-core-data-services/src/main/java/org/finos/legend/depot/services/projects/ProjectsServiceImpl.java` line 179. Line numbers are those printed by `nl -ba` at the commits above.

The depot clone is shallow (1 commit), so git history cannot answer "when was X removed". Where this matters, the text says so.

---

## 0. Executive summary

1. **There are two servers sharing one Mongo DB.** `legend-depot-server` (`/depot/api`, read-only, anonymous) serves the query API. `legend-depot-store-server` (`/depot-store/api`, GitLab OAuth) ingests Maven artifacts into Mongo and holds all admin and refresh endpoints (`server/…/LegendDepotServer.java:65-98`, `store-server/…/LegendDepotStoreServer.java:95-147`, `server/src/main/resources/docker/config/config.json` `urlPattern`, `store-server/…/docker/config/config.json`).
2. **Publishing is Maven.** An SDLC project is a multi-module Maven build. The parent POM `groupId:artifactId:version` is the "project". It has modules `<artifactId>-entities`, `<artifactId>-versioned-entities`, `<artifactId>-file-generation` and `<artifactId>-service-execution` (`sdlc:legend-sdlc-project-structure/…/ProjectStructureV13Factory.java:66-74`, `MultiModuleMavenProjectStructure.java:527-529`). Depot ingests **only** the `-entities` jar (`entities/**/*.json`) and the `-file-generation` jar. The handler for versioned-entities is bound but never registered (`artifacts-services/…/ArtifactsServicesModule.java:106-122`).
3. **Ingestion is driven by events, through a Mongo-backed queue.** A CI job (not in OSS source) calls `GET /depot-store/api/queue/{projectId}/{groupId}/{artifactId}/{versionId}`. That call enqueues a HIGH-priority event (`notifications-services/…/NotificationsQueueManager.java:146-167`). Workers poll the queue every 20s by default (`notifications-api/…/QueueManagerConfiguration.java:24-35`). For each event a worker resolves the POMs and jars from Maven with ShrinkWrap (update policy ALWAYS), parses the entities, upserts Mongo, and pre-computes transitive dependencies with Aether "nearest wins". The "refresh all versions" sweep is **not** on a timer. It is registered as an external-trigger schedule (`SchedulesFactoryImpl.java:75-78`, `ArtifactsSchedulesModule.java:39-46`) and only runs when someone calls `PUT /schedules/{name}`.
4. **There is also a Maven-free ingestion path:** `PUT /depot-store/api/queue/rest/metadata` with `{groupId, artifactId, versionId, dependencies, restCuratedArtifacts:{entities, artifacts}}` writes entities straight to the store, synchronously (`NotificationsQueueManagerResource.java:113-121`, `ProjectVersionRefreshHandler.java:285-319`). This is the closest model to what a lite implementation needs.
5. **Version identifiers:** exact `x.y.z`, any `<branch>-SNAPSHOT`, and two aliases, `latest` (= `StoreProjectData.latestVersion`) and `head` (= `<defaultBranch>-SNAPSHOT`, default `master-SNAPSHOT`). Alias matching inside `find()` is case-sensitive lowercase (`ProjectsServiceImpl.java:181,190`). Studio maps `HEAD` to `master-SNAPSHOT` on the client side (`st:legend-server-depot/src/DepotVersionAliases.ts:21-27`). **There are no version ranges.**
6. **What the consumers need:** Query, DataCube and Studio mostly fetch raw `Entity[]` (`GET /projects/{g}/{a}/versions/{v}`) plus dependency entities (`GET …/dependencies?transitive=true` or `POST /projects/dependenciesFromArtifactDependencies`) and build the graph in the browser. **The engine uses exactly one depot endpoint**, `GET /projects/{g}/{a}/versions/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=…`, with `getDependencies` defaulting to `true` (`eng:…/AlloySDLCLoader.java:45-51`).
7. **Studio calls two `/classifiers/...` endpoints that do not exist in depot@9c0a809:** `GET /classifiers/{classifierPath}` and `GET /classifiers/{classifierPath}/entities` (`st:legend-server-depot/src/DepotServerClient.ts:166-200`, `st:legend-application-query/src/components/DataSpaceArtifactInspector.tsx:381-383`). See §7.

---

## 1. Publishing pipeline: SDLC version → Maven artifacts → Depot

### 1.1 What SDLC produces (the Maven layout)

| Item | Detail | Citation |
|---|---|---|
| Project coordinates | Parent POM `groupId:artifactId:version`; the version is the SDLC release `x.y.z`, or `<branch>-SNAPSHOT` for branch builds | `sdlc:legend-sdlc-server/src/main/resources/org/finos/legend/sdlc/server/gitlab/finos/gitlab-ci-2.yml:36` (`versions:set -DnewVersion=$(echo $CI_COMMIT_REF_NAME …)-SNAPSHOT`) |
| Module naming | `<artifactId>-<moduleName>` | `sdlc:legend-sdlc-project-structure/…/maven/MultiModuleMavenProjectStructure.java:527-529` |
| Modules (structure v11–v13) | `entities` (main), plus `versioned-entities`, `service-execution`, `file-generation` | `ProjectStructureV13Factory.java:66-74` (same in V11/V12 at :66-70) |
| Entities jar content | The `legend-sdlc-entity-maven-plugin` goal `process-entities` reserializes source entities (`pure` / `legend` serializers) into `${project.build.outputDirectory}/entities/<pkg>/<name>.json` using the default JSON serializer | `sdlc:legend-sdlc-entity-maven-plugin/…/EntityMojo.java:39-46,63-74`; `EntityReserializer.java:77`; `sdlc:legend-sdlc-entity-serialization/…/EntityLoader.java:55-56,246-249` |
| Entity JSON shape | `{path, classifierPath, content}`; `content` is the protocol JSON of the element, and `content.package` is required by depot | depot model `entities-api/…/EntityDefinition.java:29-46`; depot reads `content.package` at `entities-store-mongo/…/AbstractEntitiesMongo.java:279-289` |
| Versioned-entities jar | `legend-sdlc-version-package-maven-plugin` goal `version-qualify-packages` rewrites every non-`meta::` path to `<groupId as pkg>::<artifactId with _>::v<x_y_z>::<path>` (`vX_X_X` for non-numeric versions, or a `versionAlias`) | `sdlc:legend-sdlc-version-package-maven-plugin/…/VersionQualifiedPackageMojo.java:48-61,302-350`; `EntityPathTransformer.java:194` |
| Dependencies on other projects | In the `-entities` module POM, a dependency on project `g:a:v` becomes `g:a-entities:v`. If the same project appears at more than one version, it becomes `a-versioned-entities`. Exclusions are carried as Maven `<exclusions>` | `MultiModuleMavenProjectStructure.java:231-245`; `MavenProjectStructure.java:377-397` |
| File-generation jar | `legend-sdlc-generation-file-maven-plugin` goal `generate-file-generations`. FileGeneration elements write to `<generationOutputPath or path with ::→_>/<file>`. Artifact-generation extensions write to `<element/path/with/slashes>/<extensionKey>/<file>` (e.g. `…/dataSpace-analytics/…`) | `sdlc:legend-sdlc-generation-file-maven-plugin/…/FileGenerationMojo.java:63,186-197,231-246` |
| Deployment | The OSS CI template only verifies and deploys to a local directory (`-DaltDeploymentRepository=localRepo::default::file:…`). **The real release deploy to Nexus/Artifactory and the depot notification are not in OSS source** | `gitlab-ci-2.yml:31-49` |

**There is no `-entities` *classifier*.** The entities are a separate **artifactId** (`<artifactId>-entities`). The jar has no Maven classifier.

### 1.2 How Depot discovers and reads a version (store-server)

Step by step, for one event `(projectId, g, a, v)`:

| # | Step | Citation |
|---|---|---|
| 1 | Event arrives. Paths: `GET /queue/{projectId}/{g}/{a}/{v}` (creates a HIGH-priority `MetadataNotification`, `fullUpdate=false`, `transitive=false`, validated first); or a refresh endpoint or schedule (LOW priority); or a read of an *evicted* version on depot-server (HIGH, `fullUpdate=true`) | `NotificationsQueueManagerResource.java:101-111`; `NotificationsQueueManager.java:146-167`; `ArtifactsRefreshServiceImpl.java:225-229`; `ProjectsServiceImpl.java:205-209` |
| 2 | Validation: groupId must be a Java name; artifactId must match `[a-z][a-z\d_]*(-[a-z][a-z\d_]*)*`; version must be a valid `x.y.z` or `*-SNAPSHOT`; projectId must match the existing project. Fewer than `maximumSnapshotsAllowed` (default 5) non-evicted snapshots may exist | `ProjectVersionRefreshHandler.java:134-165`; `model/…/CoordinateValidator.java:24-38`; `ArtifactsRetentionPolicyConfiguration.java:25-36` |
| 3 | Queue storage is the Mongo collection `notifications-queue`. Workers (`numberOfQueueWorkers`, default 1) run `getFirstInQueue()` every `queueInterval` (default 20s, start delay 60s). That call is a `findOneAndDelete` sorted by `eventPriority` ascending (so `HIGH` comes before `LOW`), then `created` | `NotificationsQueueSchedulesModule.java:37-52`; `NotificationsQueueMongo.java:112-121`; `QueueManagerConfiguration.java:24-35` |
| 4 | If the project is unknown, it is auto-created in `project-configurations` (the projectId must still match `^PROD-\d+$`, see §7) | `ProjectVersionRefreshHandler.java:118-131`; `ProjectsMongo.java:70-81`; `ProjectValidator.java:28-43` |
| 5 | `validateGAV`: the version must be listed by Maven version-range resolution `g:a:[0.0,)` (remote repos come from settings.xml profiles, update policy ALWAYS) | `ProjectVersionRefreshHandler.java:177-205`; `MavenArtifactRepository.java:72,100-116,372-414` |
| 6 | Dependencies: read the parent POM's modules and keep `<a>-entities`. From that module POM, take deps with a non-null version whose artifactId ends with `entities` (this also matches `-versioned-entities`). If there are none, fall back to deps declared on build plugins. For each, load **that dep's POM parent** → its `g:a:v` is the dependency project version. Exclusions are copied | `MavenArtifactRepository.java:227-266,268-284`; `RefreshDependenciesServiceImpl.java:65-77` |
| 7 | Reject a release (`x.y.z`) that depends on a `-SNAPSHOT` | `RefreshDependenciesServiceImpl.java:102-116` |
| 8 | For each **registered** artifact type (ENTITIES, FILE_GENERATIONS): find module files `<a>-entities` / `<a>-file-generation` (if the parent has no `<modules>`, the artifactId itself is used). For snapshots, unless `fullUpdate` is set, process only files whose sha256 changed (tracked in `artifacts-files`) | `ProjectVersionRefreshHandler.java:478-530`; `MavenArtifactRepository.java:177-197,268-284`; `ArtifactsServicesModule.java:106-122` |
| 9 | Entities: `EntityLoader` reads `entities/**/*.json`. **For snapshots, all prior entities of that GAV are deleted first.** Each entity is then upserted keyed by `(g,a,v,entityAttributes.path)` and stored as `_type: entityStringData` with `data` = the JSON string | `AbstractEntityRefreshHandlerImpl.java:80-115`; `EntityProvider.java:49-66`; `EntitiesMongo.java:119-130`; `AbstractEntitiesMongo.java:292-302` |
| 10 | File generations: for snapshots, delete prior ones. Files under a FileGeneration element's output folder map to that element with `type=element.content.type`. Other files map to the entity whose `/`-path prefixes them, with `type` = the first folder after the element path (the extension key). Upsert keyed by `(g,a,v,file.path)` | `FileGenerationHandlerImpl.java:89-192` |
| 11 | Version record: `StoreProjectVersionData` is upserted with `versionData.dependencies`, exclusions (from POM), optional POM properties and manifest properties (regex-matched), the **transitive closure computed via Aether**, and `evicted=false`, `excluded=false` | `ProjectVersionRefreshHandler.java:321-364`; `RefreshDependenciesServiceImpl.java:79-100,138-150` |
| 12 | `latestVersion` on `StoreProjectData` is raised only if the new non-snapshot version is strictly greater | `ProjectVersionRefreshHandler.java:366-372`; `StoreProjectData.java:83-92` |
| 13 | Dependencies: with `transitive=false` (the default for `/queue`), missing deps are only reported as errors, so **the version still loads**. With `transitive=true`, missing or snapshot deps are enqueued | `ProjectVersionRefreshHandler.java:250-268,422-452` |
| 14 | Retry: on errors the event is re-queued with `fullUpdate=true` until `maxAttempts` (default 2). The final result is written to the `notifications` collection, which is cleaned after 30 days | `NotificationsQueueManager.java:83-142`; `MetadataNotification.java:35,228-231`; `NotificationsSchedulesModule.java:36-43` |

### 1.3 Event-driven vs polling and the "refresh" machinery

| Mechanism | Trigger | Behaviour | Citation |
|---|---|---|---|
| `/queue/{projectId}/{g}/{a}/{v}` (GET!) | CI/SDLC after deploy (external, not in OSS) | Enqueue a single version, HIGH priority. pac4j `bypassBranches` exempts `/depot-store/api/queue` from auth | `NotificationsQueueManagerResource.java:101-111`; `store-server/…/docker/config/config.json` (`bypassBranches`) |
| `PUT /queue/rest/metadata` | Any client | **Synchronous**, no Maven: stores the supplied entities, artifacts and dependencies | `NotificationsQueueManagerResource.java:113-121`; `ProjectVersionRefreshHandler.java:285-319` |
| `PUT /artifactsRefresh/{g}/{a}/{v}` | Admin | Enqueue one version (LOW) | `ArtifactsRefreshResource.java:59-72`; `ArtifactsRefreshServiceImpl.java:151-166` |
| `PUT /artifactsRefresh/{g}/{a}/versions` | Admin | Enqueue the project's HEAD snapshot (if it is in the store and not evicted) plus every repo release **not already in the store** (or all of them if `allVersions=true`) | `ArtifactsRefreshServiceImpl.java:118-149,168-223` |
| `PUT /artifactsRefresh/versions` | Admin | The same for every project | `ArtifactsRefreshServiceImpl.java:74-89` |
| `PUT /artifactsRefresh/snapshots` | Admin | Enqueue the HEAD snapshot of every project | `ArtifactsRefreshServiceImpl.java:101-116` |
| `PUT /artifactsRefresh/dependencies/{g}/{a}/{v}` | Admin | Recompute the stored transitive closure. For snapshots, recurses into dependants | `ArtifactDependenciesRefreshResource.java:53-63`; `RefreshDependenciesServiceImpl.java:118-136` |
| Schedule `REFRESH_ALL_VERSION_ARTIFACTS_SCHEDULE` | **External trigger only** (`PUT /schedules/{name}`) | Runs `refreshAllVersionsForAllProjects(false,false,false)` | `ArtifactsSchedulesModule.java:39-46`; `SchedulesFactoryImpl.java:75-78` (no timer is registered) |
| `evict-LRU-project-versions` | Timer every 24h | Evict versions whose last query is older than the TTL (365 days for releases, 30 days for snapshots) | `ArtifactsSchedulesModule.java:49-60`; `ArtifactsPurgeServiceImpl.java:258-284` |
| `deprecate-versions-notInRepository` | Timer every 48h | Mark `deprecated=true` on store versions that are missing from the repo | `ArtifactsSchedulesModule.java:62-73`; `ArtifactsPurgeServiceImpl.java:208-225` |
| `repository-metrics` and `sync-project-latest-versions` | Every 5 min, **only if Prometheus is enabled** | Compute mismatches; raise `latestVersion` to the max active release | `VersionReconciliationSchedulesModule.java:45-64`; `VersionsReconciliationServiceImpl.java:135-170` |

**Conclusion:** the system is event-driven in practice. Polling the repository only happens when an operator or an external cron triggers it. **New snapshot versions are never discovered by polling.** `findVersions` filters to release versions (`MavenArtifactRepository.java:360-368`), and the snapshot refresh only re-processes a HEAD that is already stored (`ArtifactsRefreshServiceImpl.java:136-148`).

### 1.4 SNAPSHOT / HEAD handling (how unreleased code becomes readable)

- A branch build deploys `g:a:<branch>-SNAPSHOT` (`gitlab-ci-2.yml:36`), then sends a `/queue` event. Snapshots are mutable. On each re-ingest, all of the GAV's entities and generations are deleted and reinserted (`AbstractEntityRefreshHandlerImpl.java:91-98`; `FileGenerationHandlerImpl.java:97-104`). Unchanged jars are skipped by checksum unless `fullUpdate` is set (`ProjectVersionRefreshHandler.java:484,509-530`).
- `head` resolves to `BRANCH_SNAPSHOT(project.defaultBranch ?? config.projects.defaultBranch)`. The default config value is `master`, so `head` = `master-SNAPSHOT` (`ProjectsServiceImpl.java:165-169,190-201`; `VersionValidator.java:24-29`; store config `"projects": {"defaultBranch": "master"}`). Note that depot-server reads `ProjectsConfiguration` too (for HEAD resolution); the binding was not traced.
- At most `maximumSnapshotsAllowed` (5) non-evicted snapshot versions per project. A new snapshot over the limit fails validation (`ProjectVersionRefreshHandler.java:156-163`).
- `DELETE /artifactDelete/{g}/{a}/snapshotVersions` (JSON body = list of versions) refuses to delete the default-branch snapshot (`ArtifactsPurgeServiceImpl.java:130-169`).

### 1.5 "latest" resolution, exclusions, evictions, deprecation

| State | Set by | Effect on reads | Citation |
|---|---|---|---|
| `latestVersion` (project) | Ingest (monotonic raise only); `PUT /projects/{projectId}/{g}/{a}?latestVersion=`; the sync schedule (raise only) | `latest` alias → that version | `StoreProjectData.java:83-92`; `ManageProjectsResource.java:61-75`; `VersionsReconciliationServiceImpl.java:151` |
| `versionData.excluded` + `exclusionReason` | `PUT /versions/{g}/{a}/{v}/{reason}` | Reads throw `IllegalArgumentException` → HTTP 500 "…exclusion reason…". Hidden from `/versions` lists | `ManageProjectsServiceImpl.java:89-96`; `ProjectsServiceImpl.java:224-228,130-133` |
| `evicted` | Eviction endpoints and LRU schedule: **artifacts (entities, generations) are deleted**, the version record is kept | Reads enqueue a HIGH-priority restore and throw `IllegalStateException` "being restored, please retry in 5 minutes" → HTTP 500 | `ArtifactsPurgeServiceImpl.java:171-192`; `ProjectsServiceImpl.java:205-209,229-233` |
| `versionData.deprecated` | `DELETE /artifactDeprecate/…` and the 48h schedule | Informational only; the sync-latest schedule skips deprecated versions | `ArtifactsPurgeServiceImpl.java:194-206`; `VersionsReconciliationServiceImpl.java:147-148` |
| Deleted | `DELETE /artifactDelete/{g}/{a}/versions/{v}` | Gone (entities, generations and the version record) | `ArtifactsPurgeServiceImpl.java:111-128` |

---

## 2. Depot domain model

### 2.1 Core records (JSON field names as serialized)

| Type | Fields | Notes / citation |
|---|---|---|
| `CoordinateData` | `groupId`, `artifactId` | `model/…/CoordinateData.java:25-33` |
| `VersionedData` | + `versionId` | `model/…/VersionedData.java:25-30` |
| `StoreProjectData` ("project configuration") | `projectId`, `groupId`, `artifactId`, `defaultBranch`, `latestVersion` | `core-data-api/…/StoreProjectData.java:28-38`. Mongo key = `(groupId, artifactId)` (`ProjectsMongo.java:56-61`) |
| `StoreProjectVersionData` (the "version") | `groupId`, `artifactId`, `versionId`, `evicted`, `created`, `updated`, `versionData`, `transitiveDependenciesReport` | `StoreProjectVersionData.java:30-42`; Mongo key = GAV (`ProjectsVersionsMongo.java:125-131`) |
| `ProjectVersionData` | `dependencies: ProjectVersion[]` (direct), `properties: {propertyName,value}[]`, `manifestProperties: map\|null`, `deprecated`, `excluded`, `exclusionReason`, `excludedDependencies: map<"g__a__v"(dots→~), ProjectVersion[]>` | `ProjectVersionData.java:27-43,163-173` |
| `VersionDependencyReport` | `transitiveDependencies: ProjectVersion[]` (**includes the direct deps**), `valid: bool` | `VersionDependencyReport.java:27-33`; `RefreshDependenciesServiceImpl.java:83-88` |
| `ProjectVersion` | `groupId`, `artifactId`, `versionId` | `ProjectVersion.java:28-63` |
| `ArtifactDependency` (request body) | `groupId`, `artifactId`, **`version`** (JsonCreator name), `exclusions: [{groupId, artifactId}]` | `artifacts-repository-api/…/ArtifactDependency.java:35-42`. Studio sends `versionId`; Jackson probably also binds it via the getter/field-mutator inference, **but this was not verified** |
| `Entity` / `EntityDefinition` | `path`, `classifierPath`, `content` | `EntityDefinition.java:29-46` |
| `StoredEntity` (Mongo) | polymorphic `_type` ∈ {`entityData`, `entityStringData`, `entityReference`}; `groupId`, `artifactId`, `versionId`, `entityAttributes{path, classifierPath, package}`, plus `entity` / `data` (JSON string) / `reference` | `StoredEntity.java:26-51`; the writer always uses `entityStringData` (`EntitiesMongo.java:125`) |
| `DepotEntity` (API) | `groupId`, `artifactId`, `versionId`, `versionedEntity` (deprecated, always false), `entity` | `DepotEntity.java:25-49` |
| `DepotEntityOverview` | DepotEntity fields + `path`, `classifierPath` | `DepotEntityOverview.java:23-31` |
| `ProjectVersionEntities` (API) | `groupId`, `artifactId`, `versionId`, `versionedEntity` (false), `entities: Entity[]` | `ProjectVersionEntities.java:27-46` |
| `StoredFileGeneration` | `groupId`, `artifactId`, `versionId`, `path` (= **element path**), `type`, `file: {path, content}` | `StoredFileGeneration.java:28-49` (the JsonCreator reads `fileGeneration`, the getter writes `file`) |
| `DepotGeneration` | `path`, `content` | `DepotGeneration.java:24-39` |
| `MetadataNotification` | `id`, `projectId`, `groupId`, `artifactId`, `versionId`, `eventId`, `parentEventId`, `fullUpdate`, `transitive`, `attempt`, `maxAttempts`(2), `created`, `updated`, `completed`, `responses: map<int,{messages,errors,status}>`, `eventPriority`, `status` | `MetadataNotification.java:33-95,169-173` |
| `VersionQueryMetric` | `groupId`, `artifactId`, `versionId`, `lastQueryTime` | `metrics-query-api/…/VersionQueryMetric.java:29-36` |

### 2.2 Dependencies: direct, transitive, conflicts

- **Direct** dependencies come from the POMs at ingest (`MavenArtifactRepository.java:227-266`).
- **Transitive** dependencies are pre-computed at ingest by Aether `collectDependencies`. The configuration is `ConflictResolver(NearestVersionSelector, JavaScopeSelector, SimpleOptionalitySelector)` over an in-memory descriptor reader backed by the `versions` collection, with POM and request exclusions. **A dependency missing from the store yields an empty descriptor (silent)** (`MavenDependencyResolverImpl.java:74-150,343-346`; `InMemoryArtifactDescriptorReader.java:60-117`). Any failure sets `valid=false`, and later transitive reads then throw (`ProjectsServiceImpl.java:275-305`).
- **Read time** (`ProjectsServiceImpl.getDependencies`, `:247-309`), for each requested root:
  1. Resolve its aliases (excluded or evicted → error).
  2. Take its direct deps and, if `transitive`, the stored transitive list.
  3. Apply `overrideWith`: when a dep has the same `g:a` as one of the *requested roots* but a different version, drop it **and its whole transitive closure** (`DependencyUtil.java:34-44`). This is how "root wins" conflict resolution happens for multi-root requests.
  4. The result is a **set**, so two versions of the same `g:a` reached via different roots both stay. Studio detects this and errors out (`st:legend-application-studio/src/stores/editor/EditorGraphState.ts:745-800`).
- **Maven variant** (`getDependenciesMaven`, used only by `POST /projects/dependenciesFromArtifactDependencies`) runs full Aether nearest-wins at request time, with exclusions (`ProjectsServiceImpl.java:311-341`; `EntitiesServiceImpl.java:158-170`).
- **Dependency report** (`POST /projects/analyzeDependencyTree*`): Aether report with `graph.nodes{gav → {groupId,artifactId,versionId,projectId,forwardEdges[],backEdges[]}}`, `graph.rootNodes[]`, and `conflicts[{groupId,artifactId,versions[gav]}]` (`ProjectDependencyReport.java:28-93`; `ProjectDependencyVersionNode.java:26-80`; `ProjectsServiceImpl.java:364-374`).
- **Compatible-version solver** (`POST /projects/resolveCompatibleDependencies?backtrackVersions=N`, N ≤ 30): a LogicNG MaxSAT over the last N release versions per root. Returns `{success, resolvedVersions[], conflicts[{groupId,artifactId,conflictingVersions[{version,requiredBy[]}],suggestedOverride}], failureReason}` (`ProjectsServiceImpl.java:542-604`; `DependencyResponseModel.java:25-60`; `DependencyConflict.java:25-125`).
- **Dependants (reverse dependencies)**: a full scan of every project and every version, matching direct deps on `g:a` (and `v` unless `versionId == "ALL"`, case-insensitive). `latestOnly` keeps the max non-snapshot dependant per project, compared **as a string** (`ProjectsServiceImpl.java:512-540,729-733`).

### 2.3 Generations

The `file-generations` collection holds both FileGeneration-element output and artifact-extension output (e.g. type `dataSpace-analytics`), indexed by `(g,a,v,file.path)` (unique) and `(g,a,v,path)` (`FileGenerationsMongo.java:62-67`).

---

## 3. Complete Depot REST API

Base paths: depot-server `/depot/api`, store-server `/depot-store/api` (docker configs `urlPattern`). Both also expose `GET /info` and `GET /config` (`servers-common/…/InfoResource.java:46-63`). Prometheus metrics are served on the Dropwizard **admin** port at `/prometheus` (`BaseServer.java:192`). All read endpoints use `TracingResource.handle`. Endpoints with an ETag support `If-None-Match` → 304. **An ETag is only emitted when the version is exact (not a SNAPSHOT or alias) and the clientVersion is not null or `vX_X_X`.** Otherwise the response carries `Cache-Control: no-cache, no-store` (`core-tracing/…/TracingResource.java:102-125`; `core-data-api/…/EtagBuilder.java:41-76`).

Error mapping: thrown `IllegalArgumentException` / `IllegalStateException` (e.g. "project version not found", excluded, "being restored") go through `CatchAllExceptionMapper` → **HTTP 500** with `{code, message, details, timestamp, stackTrace}` (`servers-common/…/CatchAllExceptionMapper.java:36-39`; `BaseExceptionMapper.java:48-70`; `ExtendedErrorMessage.java:62-93`). An `Optional.empty()` result is presumably turned into 404 by Dropwizard's Optional support; depot code does not show that mapping, but DataCube relies on 404 for a missing single entity (`st:legend-application-data-cube/src/stores/builder/source/LegendQueryDataCubeSourceBuilderStateHelper.ts:79-80`).

### 3.1 depot-server (`/depot/api`): read API, no auth

Legend for the last column: **S** = Studio (legend-application-studio + studio extensions), **Q** = Query (legend-application-query), **DC** = DataCube, **E** = engine, **M** = Marketplace, **SD** = SDLC server, **–** = no caller found.

| # | Method | Path | Params / body | Response | ETag | Users | Citation |
|---|---|---|---|---|---|---|---|
| 1 | GET | `/project-configurations` | – | `StoreProjectData[]` | no | S Q DC | `ProjectsResource.java:55-62` |
| 2 | GET | `/project-configurations/{g}/{a}` | – | `StoreProjectData` (Optional) | no | S Q DC M | `ProjectsResource.java:64-71` |
| 3 | GET | `/projects/{g}/{a}/versions` | `snapshots` (false) | `string[]`: non-excluded versions in **store order (unsorted)**; includes evicted | no | S Q DC | `ProjectsResource.java:73-82`; `ProjectsServiceImpl.java:129-133` |
| 4 | GET | `/projects/versions/{updatedFromMillis}` | `updatedTo` (millis, default now) | `StoreProjectVersionData[]` | no | – | `ProjectsVersionsResource.java:60-69` |
| 5 | GET | `/versions/{g}/{a}/{v}` | v may be `latest`/`head` | `ProjectVersionDTO {groupId, artifactId, versionId (resolved), versionData}` (Optional) | no | Q M (as `getLatestVersion` with v=`latest`) | `ProjectsVersionsResource.java:71-129`; `st:legend-server-depot/src/DepotServerClient.ts:490-494` |
| 6 | GET | `/projects/{g}/{a}/versions/{v}` | – | `Entity[]` (all entities of the version) | GAV | **S Q DC M SD** | `EntitiesResource.java:58-69`; `sdlc:…/DepotMetadataApi.java:48` |
| 7 | GET | `/projects/{g}/{a}/versions/{v}/classifiers/{classifier}` (hidden in swagger) | – | **`DepotEntity[]`** (wrapped: `{groupId,artifactId,versionId,versionedEntity,entity}`), despite the `List<Entity>` signature | GAV (unresolved v) | Q (`getEntities(…,classifier)`) | `EntitiesResource.java:71-86`; `EntitiesServiceImpl.java:72-77`; `Entities.java:39-42` |
| 8 | GET | `/projects/{g}/{a}/versions/{v}/entities/{path}` | – | `Entity` (Optional) | GAV | DC M S(service ext) | `EntitiesResource.java:88-99` |
| 9 | GET | `/projects/{g}/{a}/versions/{v}/entities` | `package`, `classifierPath` (multi), `includeSubPackages` (true) | `Entity[]` | GAV | – | `EntitiesResource.java:101-118`; `AbstractEntitiesMongo.java:150-170` |
| 10 | GET | `/projects/{g}/{a}/versions/{v}/dependencies` | `transitive` (false), `includeOrigin` (false); Studio also sends `versioned=false`, which is **ignored** | `ProjectVersionEntities[]` | GAV | **Q DC M S** (`getIndexedDependencyEntities` → transitive=true) | `EntitiesDependenciesResource.java:61-76`; `DepotServerClient.ts:238-261,293-321` |
| 11 | GET | `/projects/{g}/{a}/versions/{v}/classifiers/{classifier}/dependencies` (hidden) | `transitive`, `includeOrigin` | `ProjectVersionEntities[]` whose `entities` are **DepotEntity** objects (not Entity) | GAV | Q DC | `EntitiesDependenciesResource.java:78-98`; DataCube comment `LegendQueryDataCubeSourceBuilderStateHelper.ts:91-94` |
| 12 | POST | `/projects/{g}/{a}/versions/{v}/dependencies/paths` (hidden) | body `string[]` entityPaths; `includeOrigin` | `Entity[]`: searches the transitive deps (unordered set) and stops early once all paths are found | GAV | – | `EntitiesDependenciesResource.java:100-114`; `EntitiesServiceImpl.java:86-96`; `AbstractEntitiesMongo.java:120-133` |
| 13 | POST | `/projects/dependencies` | body `ProjectVersion[]`; `transitive`, `includeOrigin` | `ProjectVersionEntities[]` | no | – | `EntitiesDependenciesResource.java:116-127` |
| 14 | POST | `/projects/dependenciesFromArtifactDependencies` | body `ArtifactDependency[]` (with exclusions); `transitive`, `includeOrigin` | `ProjectVersionEntities[]` (Aether nearest-wins) | no | **S** (workspace dependency graph), service/dataspace extensions | `EntitiesDependenciesResource.java:129-140`; `EditorGraphState.ts:724-731` |
| 15 | GET | `/entitiesByClassifierPath/{classifierPath}` (hidden) | `search`, `scope` (RELEASES\|SNAPSHOT, default RELEASES), `limit` | `DepotEntity[]`. RELEASES = each project's `latestVersion`, paged by 100. SNAPSHOT = **all** `*-SNAPSHOT` versions | no | Q (Update-service setup), service extension | `EntityClassifierResource.java:49-62`; `EntityClassifierServiceImpl.java:46-101` |
| 16 | GET | `/projects/{g}/{a}/versions/{v}/projectDependencies` | `transitive` (false) | `ProjectVersion[]` (set) | GAV | SD (Studio client has the method `getProjectDependencyPointers` but no caller was found) | `DependenciesResource.java:71-82`; `sdlc:…/DepotMetadataApi.java:49` |
| 17 | POST | `/projects/analyzeDependencyTreeFromArtifactDependencies` | body `ArtifactDependency[]` | `ProjectDependencyReport` | no | **S** | `DependenciesResource.java:84-91`; `DepotServerClient.ts:375-384` |
| 18 | POST | `/projects/analyzeDependencyTree` | body `ProjectVersion[]` | `ProjectDependencyReport` | no | – | `DependenciesResource.java:93-100` |
| 19 | GET | `/projects/{g}/{a}/versions/{v}/dependantProjects` | `latestOnly` (false); v=`all` → every version | `{groupId,artifactId,versionId,dependency:ProjectVersion,platformsVersion:[{propertyName,value,projectVersionId}]}[]` | no | – (client methods exist: `getDependantProjects`, `getAllDependantProjects`; no app caller found) | `DependenciesResource.java:102-176` |
| 20 | POST | `/projects/resolveCompatibleDependencies` | body `ProjectVersion[]`; `backtrackVersions` (0) | `DependencyResponseModel` | no | **S** | `DependenciesResource.java:116-124` |
| 21 | GET | `/projects/{g}/{a}/versions/{v}/pureModelContextData` | `clientVersion` (must be in `PureClientVersions.versions`, default production), `getDependencies` (**true**), `convertToNewProtocol` (true) | `PureModelContextData`: origin `{project:"g:a", baseVersion:v}`. With dependencies, the deps' entities are merged, deduplicated and sorted | GAV + clientVersion | **E**; Studio client method exists, no app caller | `PureModelContextResource.java:55-78`; `PureModelContextServiceImpl.java:61-132`; `eng:…/AlloySDLCLoader.java:45-51` |
| 22 | POST | `/projects/dependencies/pureModelContextData` | body `ProjectVersion[]`; `clientVersion`, `transitive` (true), `convertToNewProtocol` (true); Studio's `includeOrigin` and `versioned` are **ignored** (origin is always included) | `PureModelContextData` | no | S (DevTool panel) | `PureModelContextResource.java:80-98`; `PureModelContextServiceImpl.java:84-90`; `st:legend-application-studio/src/components/editor/panel-group/DevToolPanel.tsx:97` |
| 23 | GET | `/generations/{g}/{a}/versions/{v}` | – | `DepotGeneration[]` | GAV | – | `FileGenerationsResource.java:56-66` |
| 24 | GET | `/generations/{g}/{a}/versions/{v}/{elementPath}` | – | `DepotGeneration[]` | GAV | – | `FileGenerationsResource.java:68-79` |
| 25 | GET | `/generations/{g}/{a}/versions/{v}/file/{filePath}` | – | `DepotGeneration` (Optional) | GAV | – | `FileGenerationsResource.java:81-90` |
| 26 | GET | `/generationFileContent/{g}/{a}/versions/{v}/file/{filePath}` | text/plain | `string` (Optional) | GAV | Q (data-space analytics) | `FileGenerationsResource.java:92-101`; `st:legend-extension-dsl-data-space/…/DataSpaceAnalysisHelper.ts:54` |
| 27 | GET | `/generations/{g}/{a}/{v}/types/{type}` (**note: no `/versions/` segment**) | `elementPath` | `StoredFileGeneration[]` (full record, including `type` and element `path`) | GAV | **Q** (dataSpace-analytics, data products), M, data-product ext | `FileGenerationsResource.java:103-110`; `DepotServerClient.ts:447-462` |

### 3.2 store-server (`/depot-store/api`): ingestion and admin

Auth column: "user" means `validateUser()` is called (allow-list in `authorisedIdentities.json`). "–" means no check in code; pac4j may still require a login, except for `/info` and `/queue`.

| # | Method | Path | Params / body | Response | Auth | Citation |
|---|---|---|---|---|---|---|
| A1 | PUT | `/projects/{projectId}/{g}/{a}` | `defaultBranch`, `latestVersion` | `StoreProjectData` (**full replace**) | user | `ManageProjectsResource.java:61-75` |
| A2 | DELETE | `/projects/{g}/{a}` | – | count | user | `ManageProjectsResource.java:77-92` |
| A3 | GET | `/versions` | `excluded` (bool) | `StoreProjectVersionData[]` | user | `ManageProjectsVersionsResource.java:62-70` |
| A4 | PUT | `/versions/{g}/{a}/{v}/{exclusionReason}` | – | `StoreProjectVersionData` (**replaces the record and wipes deps**) | – | `ManageProjectsVersionsResource.java:72-…`; `ManageProjectsServiceImpl.java:89-96` |
| A5 | GET | `/versions/mismatch` | – | `VersionMismatch[]` `{projectId,groupId,artifactId,versionsNotInStore,versionsNotInRepository,errors}` | – | `VersionsReconciliationResource.java:57-61`; `VersionMismatch.java:28-43` |
| A6 | GET | `/repository/versions/{g}/{a}` | – | `string[]` (release versions in the Maven repo) | – | `RepositoryResource.java:49-68` |
| A7 | GET | `/repository/versions/{g}/{a}/{v}` | text | `string` (Optional) | – | `RepositoryResource.java:70-76` |
| A8 | PUT | `/artifactsRefresh/{g}/{a}/{v}` | `fullUpdate`, `transitive` | `MetadataNotificationResponse {messages[],errors[],status}` | user | `ArtifactsRefreshResource.java:59-72` |
| A9 | PUT | `/artifactsRefresh/{g}/{a}/versions` | `fullUpdate`, `allVersions`, `transitive` | same | user | `ArtifactsRefreshResource.java:74-88` |
| A10 | PUT | `/artifactsRefresh/versions` | `fullUpdate`, `allVersions`, `transitive` | same | user | `ArtifactsRefreshResource.java:90-103` |
| A11 | PUT | `/artifactsRefresh/snapshots` | `fullUpdate`, `transitive` | same | user | `ArtifactsRefreshResource.java:105-117` |
| A12 | PUT | `/artifactsRefresh/dependencies/{g}/{a}/{v}` | – | `StoreProjectVersionData` | user | `ArtifactDependenciesRefreshResource.java:53-63` |
| A13 | DELETE | `/artifactEviction/{g}/{a}/versions/{v}` | – | response | user | `ArtifactsPurgeResource.java:56-71` |
| A14 | DELETE | `/artifactDelete/{g}/{a}/versions/{v}` | – | response | user | `ArtifactsPurgeResource.java:73-88` |
| A15 | DELETE | `/artifactDelete/{g}/{a}/snapshotVersions` | body `string[]` | text | **–** | `ArtifactsPurgeResource.java:90-99` |
| A16 | DELETE | `/artifactEviction/{g}/{a}/old/{keepVersions}` | – | response (evicts from the front of the **unsorted** version list) | user | `ArtifactsPurgeResource.java:101-115`; `ArtifactsPurgeServiceImpl.java:227-256` |
| A17 | DELETE | `/artifactDeprecate/{g}/{a}/versions/{v}` | – | response | user | `ArtifactsPurgeResource.java:117-131` |
| A18 | DELETE | `/artifactEviction/versions/notUsed` | – | response (evicts every version with no query metric) | user | `ArtifactsPurgeResource.java:133-…`; `ArtifactsPurgeServiceImpl.java:286-316` |
| A19 | GET | `/queue/{projectId}/{g}/{a}/{v}` | – | text eventId | – (pac4j bypass) | `NotificationsQueueManagerResource.java:101-111` |
| A20 | PUT | `/queue/rest/metadata` | body `RestMetadataNotification` | `MetadataNotificationResponse` | – | `NotificationsQueueManagerResource.java:113-121`; `RestMetadataNotification.java:30-48`; `RestCuratedArtifacts.java:26-40` |
| A21 | GET | `/notifications-queue` | – | `MetadataNotification[]` | user | `NotificationsQueueManagerResource.java:72-80` |
| A22 | GET | `/notifications-queue/count` | – | long | – | `:82-89` |
| A23 | GET | `/notifications-queue/{eventId}` | – | Optional | – | `:91-98` |
| A24 | DELETE | `/notifications-queue` | – | long | user | `:123-130` |
| A25 | GET | `/notifications` | `groupId`, `artifactId`, `versionId`, `eventId`, `parentEventId`, `success`, `from`, `to` (default: last 120 min) | `MetadataNotification[]` | – | `NotificationsResource.java:71-91` |
| A26 | GET | `/notifications/{eventId}` | – | Optional | – | `NotificationsResource.java:93-…` |
| A27 | GET | `/schedules` | `disabled` (false) | `ScheduleInfo[]` | user | `ManageSchedulesResource.java:73-86` |
| A28 | GET | `/scheduleInstances` | – | `ScheduleInstance[]` | user | `:88-96` |
| A29 | PUT | `/schedules/{scheduleName}` | `forceRun` | – (triggers now) | user | `:98-111` |
| A30 | DELETE | `/schedules/{scheduleName}` and `/schedules/` | – | – | user | `:113-139` |
| A31 | PUT | `/schedules/{name}/disable/{toggle}` and `/schedules/all/disable/{toggle}` | – | – | user | `:141-…` |
| A32 | GET/PUT/DELETE | `/indexes`, `/indexes/{index}/{collection}` | – | index admin | PUT/DELETE user | `store-mongo/…/MongoStoreAdministrationResource.java:70-105` |
| A33 | GET/DELETE | `/collections/stats`, `/collections`, `/collections/{id}`, `/collections/pipeline/{name}?pipeline=` | – | raw Mongo admin | mostly user | `MongoStoreAdministrationResource.java:107-155` |
| A34 | PUT | `/migrations/{migrateToVersionData, cleanupProjectData, calculateDependenciesForVersions/all, addTransitiveDependenciesToVersionData, addLatestVersionToProjectData}` | – | – | user | `core-data-store-mongo/…/CoreDataStoreMigrationsResource.java:58-122` |
| A35 | DELETE / PUT | `/migrations/deleteVersionedEntities`, `/migrations/migrateToStoredEntityData` | – | – | user | `entities-store-mongo/…/EntitiesMigrationResource.java:60-82` |

**No "metrics" REST resource exists.** Query metrics (`lastQueryTime` per GAV) are recorded in memory on every `resolveAliasesAndCheckVersionExists` call and persisted to `query-metrics` every 5 min (`ProjectsServiceImpl.java:234`; `InMemoryQueryMetricsRegistry.java:26-47`; `QueryMetricsSchedulesModule.java:35-46`). Their only use is LRU eviction.

---

## 4. Who uses what, and the CORE set

### 4.1 Studio-side client (`st:legend-server-depot/src/DepotServerClient.ts`)

| Client method | Depot endpoint (§3 #) | Callers (package) |
|---|---|---|
| `getProjects` | #1 | studio, query, data-cube |
| `getProject` | #2 | studio, query, marketplace, service/data-space exts, depot-dashboard |
| `getAllVersions` / `getVersions(snapshots)` | #3 | studio, query, data-cube |
| `getVersionEntities` / `getEntities(project,v,classifier?)` | #6 or #7 | studio, query, data-cube, marketplace, data-product ext |
| `getVersionEntity` / `getEntity` | #8 | data-cube, marketplace, service ext, dashboard |
| `DEPRECATED_getEntitiesByClassifierPath` | #15 | query (UpdateExistingServiceQuerySetupStore.ts:100), service ext |
| `getEntitiesByClassifier` | **`GET /classifiers/{path}/entities` — not in depot@9c0a809** | query (DataProductSelectorState.ts:156), data-space ext (DataSpaceAdvancedSearchState.ts:142) |
| `getEntitiesSummaryByClassifier` | **`GET /classifiers/{path}?scope&summary&latest` — not in depot@9c0a809** | query (DataProductSelectorState.ts:178), marketplace, dashboard; plus a raw `fetch` in DataSpaceArtifactInspector.tsx:381 |
| `getDependencyEntities(g,a,v,transitive,includeOrigin,classifier?)` | #10 / #11 | query (via `getIndexedDependencyEntities`), data-cube, marketplace |
| `getProjectDependencyPointers` | #16 | no caller found |
| `getPureModelContextData` | #21 | no caller found |
| `collectDependencyEntities` | #14 | studio (EditorGraphState.ts:725), data-space-studio, service ext |
| `collectDependencyEntitiesAsPureModelContextData` | #22 | studio DevToolPanel |
| `analyzeDependencyTree` | #17 | studio (EditorGraphState.ts:775, ProjectDependencyEditorState.ts:644) |
| `resolveCompatibleDependencies` | #20 | studio (ProjectDependencyEditorState.ts:836) |
| `getDependantProjects` / `getAllDependantProjects` | #19 | no app caller found |
| `getGenerationContentByPath` | #26 | data-space ext |
| `getGenerationFilesByType` | #27 | query, marketplace, data-product ext; plus raw `fetch` in DataSpaceArtifactInspector.tsx:529-538,608-615,766-775 |
| `getLatestVersion` | #5 with v=`latest` | query, marketplace |
| `getVersionedProjectData` | #5 | no caller found |

Usage counts come from `grep -rnoE "[dD]epotServerClient\.[A-Za-z_]+"` over `st:*/src` (tests and lib excluded). Helper wrappers: `retrieveProjectEntitiesWithDependencies` (#6 + #10 with transitive), `retrieveProjectEntitiesWithClassifier` (#7 + #11), `projectIdHandlerFunc` (#2 + #5) (`st:legend-server-depot/src/DepotEntityHelper.ts:34-96`).

### 4.2 CORE endpoint sets

**(a) Query / DataCube opening a model by GAV.** The browser builds the graph itself, and execution goes to the engine as an SDLC pointer.

1. `GET /project-configurations/{g}/{a}` (#2): existence check and projectId (`QueryEditorStore.ts:806-811`).
2. `GET /projects/{g}/{a}/versions/{v}` (#6): main entities (`QueryEditorStore.ts:822-825`).
3. `GET /projects/{g}/{a}/versions/{v}/dependencies?transitive=true&includeOrigin=false` (#10): dependency entities, indexed by `g:a` (`QueryEditorStore.ts:837-839`; `DepotServerClient.ts:293-321`).
4. `GET /versions/{g}/{a}/latest` (#5): resolves the `latest` label shown in the UI (`QueryEditorStore.ts:271`; `DataSpaceInfo.tsx:80`).
5. `GET /projects/{g}/{a}/versions?snapshots=…` (#3): version pickers (`QueryEditorStore.ts:1872`).
6. `GET /project-configurations` (#1): project pickers in setup screens.
7. DataCube: #8 (single function or enumeration, relying on 404), #6, #11 (`LegendDataCubeDataCubeEngine.ts:617,656`; `LegendQueryDataCubeSourceBuilderStateHelper.ts:68-96`).
8. Optional (data-space, data-product features): #27 `types/dataSpace-analytics`, #26, #7/#11 (classifier-filtered for minimal graph), #15, and the missing `/classifiers/*` endpoints.

The graph origin is set to `LegendSDLC(g, a, resolveVersion(v))` (`QueryEditorStore.ts:865-868`). Execution then sends a `PureModelContextPointer` instead of the full model (`st:legend-graph/…/V1_PureGraphManager.ts:2852-2879,5186-5200`).

**(b) Studio resolving project dependencies** (workspace editing):

1. `POST /projects/dependenciesFromArtifactDependencies?transitive=true&includeOrigin=true` (#14), body `[{groupId, artifactId, versionId, exclusions?}]` (`EditorGraphState.ts:709-731`).
2. On a duplicate `g:a`, `POST /projects/analyzeDependencyTreeFromArtifactDependencies` (#17) to explain the conflict (`EditorGraphState.ts:767-800`).
3. Dependency editor: #3 (versions), #1 (projects), #17, #20.
4. Project viewer (read-only GAV): #2, #6, #10 (`ProjectViewerStore.ts:320-345`).

**(c) Engine resolving a `PureModelContextPointer`.** The pointer is `{_type:"pointer", serializer:{name:"pure",version}, sdlcInfo:{_type:"alloy", groupId, artifactId, version, baseVersion:"latest", packageableElementPointers}}` (`st:legend-graph/…/V1_SDLC.ts:19-45`; `V1_PureProtocolSerialization.ts:57-58,133-145`).

1. The engine calls only `GET {alloy.baseUrl}/projects/{g}/{a}/versions/{version}/pureModelContextData?convertToNewProtocol=false&clientVersion={cv}` (#21). `version` null or `"none"` → `master-SNAPSHOT`. `getDependencies` defaults to true, so the response is **the full closure** (`eng:legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-modelManager-sdlc/…/alloy/AlloySDLCLoader.java:40-51`).
2. Engine caching: an alloy pointer is cacheable unless the version is null, `none` or contains `SNAPSHOT` (`AlloySDLCLoader.java:62-65`; `SDLCLoader.java:129-145`). **The alias `latest` is cached by the engine**, even though depot sends no ETag for it.
3. The SDLC server itself uses #6 and #16 (`sdlc:legend-sdlc-server/…/depot/api/DepotMetadataApi.java:48-49`; used by `TestModelBuilder.java:88-182`).

---

## 5. Versions: `latest`, `HEAD`, `master-SNAPSHOT`, ranges; caching and immutability

| Input | Server-side resolution | Citation |
|---|---|---|
| `x.y.z` | Exact lookup. It must parse as an SDLC `VersionId` to be ingested | `VersionValidator.java:36-52` |
| `<anything>-SNAPSHOT` (e.g. `master-SNAPSHOT`, `feature_x-SNAPSHOT`) | Exact lookup. Mutable | `VersionValidator.java:54-57` |
| `latest` | `StoreProjectData.latestVersion` → exact. Not found if null | `ProjectsServiceImpl.java:181-189` |
| `head` | `<defaultBranch>-SNAPSHOT` | `ProjectsServiceImpl.java:190-201` |
| `HEAD` / `LATEST` (uppercase) | **Not resolved by `find()`** (case-sensitive `.equals`), so lookup falls through to a literal version, which is not found. ETag logic treats them as aliases (case-insensitive) | `ProjectsServiceImpl.java:181,190`; `VersionValidator.java:59-62` |
| Studio `HEAD` | The client rewrites it to `master-SNAPSHOT` before calling (hard-coded, ignores `defaultBranch`) | `DepotVersionAliases.ts:21-27` |
| Version ranges | **Not supported anywhere in the API.** Ranges are used only internally to list repository versions (`[0.0,)`) | `MavenArtifactRepository.java:72,379` |
| `all` | Special value only for `dependantProjects` | `ProjectsServiceImpl.java:516` |

Caching and immutability guarantees:

- **Release versions are treated as immutable.** Re-ingest of a release upserts by path and **does not delete stale entities** (only snapshots are wiped first) (`AbstractEntityRefreshHandlerImpl.java:91-98`). A re-published release with fewer entities would therefore keep the removed ones.
- **ETag** = concatenation of `g+a+v(+clientVersion)`, with no content hash, so it is valid only under the immutability assumption. The response gets `Cache-Control: must-revalidate`. Snapshots and aliases get `no-cache, no-store` (`EtagBuilder.java:26-76`; `TracingResource.java:102-125`).
- `GET /projects/{g}/{a}/versions/{v}/classifiers/...` and the generation endpoints build the ETag from the **unresolved** `versionId`. Aliases still produce no ETag, so this is harmless (`EntitiesResource.java:85`; `FileGenerationsResource.java:65`).
- Version records are mutable (`excluded`, `evicted`, `deprecated`, `transitiveDependenciesReport`, `updated`). `GET /projects/versions/{updatedFrom}` lets consumers sync changes incrementally (`ProjectsVersionsMongo.java:64-77`).

---

## 6. Storage (Mongo DB `depot`)

| Collection | Content | Indexes (name: fields, unique?) | Mutability | Citation |
|---|---|---|---|---|
| `project-configurations` | `StoreProjectData` | `groupId-artifactId` (unique) | mutable (`latestVersion`, `defaultBranch`) | `ProjectsMongo.java:41,51-54` |
| `versions` | `StoreProjectVersionData` | `groupId-artifactId-versionId` (unique) | mutable flags and dependency report | `ProjectsVersionsMongo.java:44,53-56` |
| `entities` | `StoredEntity` (`entityStringData`: `data` = entity JSON string, `entityAttributes{path,classifierPath,package}`, `updated`) | `groupId-artifactId-versionId`; `…-entityAttributes-path` (unique); `…-entityAttributes-package`; `entityAttributes-classifier` | releases: append-only by convention; snapshots: delete and replace | `EntitiesMongo.java:56,71-78`; `AbstractEntitiesMongo.java:292-302` |
| `versioned-entities` | same shape, `versionedEntityStringData` | same pattern | **not written by ingest** (handler unregistered); `DELETE /migrations/deleteVersionedEntities` exists | `VersionedEntitiesMongo.java:40,48-55`; `ArtifactsServicesModule.java:106-122` |
| `file-generations` | `StoredFileGeneration` | `…-filePath` (unique on `file.path`); `…-elementPath` | as entities | `FileGenerationsMongo.java:39,62-67` |
| `artifacts-files` | `{path, checkSum}` per resolved jar file | `path` (unique) | mutable | `artifacts-store-mongo/…/ArtifactsFilesMongo.java:37,49` |
| `notifications-queue` | pending `MetadataNotification` | `eventPriority-created` | transient | `NotificationsQueueMongo.java:47,57-60` |
| `notifications` | processed events | `parentId`, `status`, `lastUpdated`, `groupId-artifactId-versionId`, `eventId` | 30-day TTL via schedule | `NotificationsMongo.java:51,75-79` |
| `query-metrics` | `{g,a,v,lastQueryTime}` | `group-artifact-version` | consolidated every 6h | `QueryMetricsMongo.java:53,153-156`; `ManageQueryMetricsSchedulesModule.java:33-44` |
| `schedules`, `schedule-instances` | schedule config and run leases | `name`; `schedule` | mutable | `SchedulesMongo.java:38,90`; `ScheduleInstancesMongo.java:38,69` |
| `userSessions` | pac4j sessions (if enabled) | – | – | docker configs `mongoSession.collection` |

Indexes are *registered* per module (`adminStore.registerIndexes(...)`, e.g. `ManageCoreDataStoreMongoModule.java:47-48`) and created by `PUT /indexes` (`MongoStoreAdministrationResource.java:79-92`). This census did not verify whether they are also created automatically at startup.

**Minimal storage for a lite implementation (read path that serves Query, DataCube, Studio and the engine):**

1. `projects`: `(groupId, artifactId) → {projectId, defaultBranch, latestVersion}`.
2. `versions`: `(g,a,v) → {directDependencies[], transitiveDependencies[] (pre-resolved, nearest-wins), excluded?, created/updated}`.
3. `entities`: `(g,a,v,path) → {classifierPath, package, content JSON}`, with secondary lookup by `(g,a,v)` and optionally by `classifierPath`.
4. `generations` (only if data-space analytics or data products are in scope): `(g,a,v,filePath) → {elementPath, type, content}`.

The queue, notifications, schedules, metrics, artifacts-files and versioned-entities collections are all operational machinery and can be dropped.

---

## 7. Surprises and important details

1. **Studio calls classifier endpoints that are missing from depot@9c0a809.** The calls are `GET /classifiers/{path}` (`summary`, `latest`, `scope`) and `GET /classifiers/{path}/entities` (`scope`) (`DepotServerClient.ts:166-200`). They are used by the Query data-product selector, data-space advanced search, marketplace and the depot dashboard. The only depot equivalents are #15 and the per-GAV #7. The deployed depot that Studio targets probably has them (a newer version or a fork); the shallow clone cannot tell. A lite depot must implement them if those features matter. Expected response shapes per Studio: `StoredEntity {groupId, artifactId, versionId, entity}` and `StoredSummaryEntity {groupId, artifactId, versionId, path, classifierPath}` (`st:legend-server-depot/src/models/StoredEntity.ts:21-52`), which correspond to depot's `DepotEntity` and `DepotEntityOverview`.
2. **projectId must match `^PROD-\d+$`.** This is enforced when creating or updating a `StoreProjectData`, including auto-creation during ingest (`ProjectValidator.java:28-43`; `ProjectsMongo.java:70-81`). It is a GS-ism a lite implementation should drop. In contrast, versions and entities additionally accept a REST-only groupId pattern `^lakehouse\.\d+$` (`CoordinateValidator.java:40-44`).
3. **The REST ingest path (`PUT /queue/rest/metadata`) does not create a `StoreProjectData` and does not update `latestVersion`.** It writes with `projectId=null` (`ProjectVersionRefreshHandler.java:304`). `latest`/`head` aliases and #2 therefore do not work for REST-ingested projects unless the project is registered separately.
4. **Not-found and excluded versions return HTTP 500, not 404.** `IllegalArgumentException` is thrown outside or inside `handle` and mapped by the catch-all (`EntitiesResource.java:67`; `ProjectsServiceImpl.java:212-236`; `CatchAllExceptionMapper.java:36-39`). Evicted versions also return 500, with a "retry in 5 minutes" message, while a restore is queued.
5. **The classifier-filtered endpoints (#7, #11) return `DepotEntity` wrappers instead of `Entity`.** The Java generics are erased at `EntitiesServiceImpl.java:72-77,137`. DataCube explicitly re-parses them (`LegendQueryDataCubeSourceBuilderStateHelper.ts:91-94`). A lite implementation must reproduce this quirk or change both sides.
6. **`includeSubPackages=true` uses the regex `^<package>*`.** The `*` applies to the last character only, so it actually means "starts with the package minus its last char, then that char 0+ times". It is a loose prefix match that can over-match, e.g. `a::b` matches `a::bc` (`AbstractEntitiesMongo.java:153-156`).
7. **Dependants (`dependantProjects`) is a full scan** over all versions of all projects, with no index on dependencies (`ProjectsServiceImpl.java:512-540`). `latestOnly` compares versions as strings (`:729-733`).
8. **Classifier search RELEASES scope skips one project per page of 100,** because `subList(beginIndex, lastIndex)` excludes `lastIndex` (`EntityClassifierServiceImpl.java:46-62`).
9. **`PUT /versions/{g}/{a}/{v}/{reason}` (exclude) replaces the whole version record** with a blank one, wiping dependencies and the transitive report (`ManageProjectsServiceImpl.java:89-96` + `BaseMongo.java:148-154` `findOneAndReplace`).
10. **The transitive closure is frozen at ingest time.** If a dependency is ingested *after* its dependant, the dependant's stored closure lacks that subtree (the resolver silently returns no children for missing GAVs, `InMemoryArtifactDescriptorReader.java:72-111`) until `PUT /artifactsRefresh/dependencies/{g}/{a}/{v}` is called. With `/queue` defaults (`transitive=false`), a version whose deps are missing still loads, and errors are only recorded in the notification (`ProjectVersionRefreshHandler.java:250-262`).
11. **Two dependency algorithms coexist.** Stored closure plus "root override" (`DependencyUtil.overrideWith`) serves #10, #13, #16, #21 and #22. Request-time Aether nearest-wins with exclusions serves #14. Results can differ for the same input. Studio uses #14, while Query and the engine use the stored closure.
12. **The PMCD endpoint silently drops entities that cannot be converted** (`withEntitiesIfPossible`, `PureModelContextServiceImpl.java:110-115`). With `convertToNewProtocol=false` (the engine's choice) it streams raw entity `content` (`:134-168`). Its `origin` is an AlloySDLC carrying `project="g:a"` and `baseVersion=v`. The multi-project POST variant uses an empty AlloySDLC.
13. **The engine caches `latest`** because it only treats null, `none` and `*SNAPSHOT` as mutable (`AlloySDLCLoader.java:62-65`), so a new release is not seen until the cache expires. Depot itself sends no ETag for `latest`.
14. **No polling of releases by default.** The "refresh all versions" schedule is external-trigger only, so a missed `/queue` call means the version never appears until an operator refresh (`SchedulesFactoryImpl.java:75-78`).
15. **`/queue/...` is a GET that mutates state and bypasses auth** (store config `bypassBranches`). `DELETE /artifactDelete/{g}/{a}/snapshotVersions` has no `validateUser()` (`ArtifactsPurgeResource.java:90-99`), nor does the exclude endpoint A4.
16. **Studio sends params that depot ignores:** `versioned=false` on #10/#14/#22, and `includeOrigin` on #22 (`DepotServerClient.ts:256-260,342-346,366-372`). On #22 depot always includes the origins (`PureModelContextServiceImpl.java:88`).
17. **Two path-shape inconsistencies in the generations API:** `/generations/{g}/{a}/{v}/types/{type}` has no `/versions/` segment, unlike the other generation paths (`FileGenerationsResource.java:104`). `StoredFileGeneration` serializes the file as `file` but its JsonCreator reads `fileGeneration` (`StoredFileGeneration.java:38-49,69-71`).
18. **The versioned-entities pipeline is dead code in depot** (no handler registered and no read endpoint), even though SDLC still produces the jar and the store server keeps the collection and Mongo classes.
19. **`/projects/{g}/{a}/versions` is unsorted** (Mongo natural order) and includes evicted versions. `evictOldestProjectVersions` evicts from the head of that unsorted list (`ArtifactsPurgeServiceImpl.java:234-244`).
20. **The docker store config sets `"queue-interval": 30`,** but the configuration class only binds `queueManager` (`DepotStoreServerConfiguration.java:31-41`). Whether Dropwizard rejects or ignores the unknown key was not verified.

### Could not determine (explicitly)

- Where and how the *release* deploy and the `/queue` notification are triggered in a real Legend deployment. They are not present in OSS legend-sdlc (only a verify-only CI template, `gitlab-ci-2.yml`).
- Whether `ArtifactDependency` JSON with `versionId` (what Studio sends) binds correctly. The `@JsonCreator` names the field `version`, and correct binding depends on Jackson mutator inference. Not tested.
- Whether Mongo indexes are created at startup or only via `PUT /indexes`.
- The history of the `/classifiers/*` endpoints (shallow clone).
- Exact status code for an `Optional.empty()` response (Dropwizard default assumed to be 404, which DataCube relies on).

---

<!-- Part E -->
# Part E — How the engine receives and resolves models (PureModelContext)

Sources read for this census (read-only, nothing was run):

| Repo | Commit | Local path |
|---|---|---|
| legend-engine | `230c159196d` | `/Users/neema/legend/legend-engine` |
| legend-studio | `821c74c` | `/Users/neema/legend/legend-lite-query/.scratch/legend-studio` |
| legend-sdlc | `1021fda` | `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc` |
| legend-depot | `9c0a809` | `/Users/neema/legend/legend-lite-query/.scratch/legend-depot` |

Path abbreviations used in the citations:

- `CTX` = `legend-engine/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-protocol-pure/src/main/java/org/finos/legend/engine/protocol/pure/v1/model/context`
- `MM` = `legend-engine/.../legend-engine-core-language-pure/legend-engine-language-pure-modelManager/src/main/java/org/finos/legend/engine/language/pure/modelManager`
- `SDLCL` = `legend-engine/.../legend-engine-core-language-pure/legend-engine-language-pure-modelManager-sdlc/src/main/java/org/finos/legend/engine/language/pure/modelManager/sdlc`
- `COMPAPI` = `legend-engine/.../legend-engine-core-language-pure/legend-engine-language-pure-compiler-http-api/src/main/java/org/finos/legend/engine/language/pure/compiler/api`
- `LG` = `legend-studio/packages/legend-graph/src/graph-manager/protocol/pure/v1`
- `DEPOT-PMC` = `legend-depot/legend-depot-pure-model-context/src/main/java/org/finos/legend/depot`
- `SDLC-PP` = `legend-sdlc/legend-sdlc-protocol-pure/src/main/java/org/finos/legend/sdlc/protocol/pure/v1`

---

## 1. PureModelContext and SDLC variants (JSON shapes)

### 1.1 Top-level `PureModelContext` polymorphism

`CTX/PureModelContext.java:20-26`: `@JsonTypeInfo(use = NAME, property = "_type", defaultImpl = PureModelContextData.class)`. The registered subtypes are:

| `_type` | Java class | Fields (JSON names) | Notes |
|---|---|---|---|
| `data` | `PureModelContextData` (extends `PureModelContextConcrete`) | `serializer` (Protocol), `origin` (PureModelContextPointer), `elements` (PackageableElement[]) | It is the **default impl**: a body with no `_type` deserializes as `data`. `@JsonIgnoreProperties(ignoreUnknown = true)` (`CTX/PureModelContextData.java:41-46`). |
| `text` | `PureModelContextText` (extends Concrete) | `serializer`, `code` (Pure grammar string) | `CTX/PureModelContextText.java:19-23`. Resolved by `PureGrammarParser.parseModel(code)` (`MM/ModelManager.java:179-182`). |
| `pointer` | `PureModelContextPointer` | `serializer`, `sdlcInfo` (SDLC) | `CTX/PureModelContextPointer.java:25-29`. If `sdlcInfo` is absent it defaults to `new PureSDLC()` (line 29). `ignoreUnknown = true`. |
| `combination` | `PureModelContextCombination` | `contexts` (PureModelContext[]) | `CTX/PureModelContextCombination.java:20-23`. This may be nested (it recurses, `MM/ModelManager.java:189-215`). |

The `PureModelContextConcrete` class (`CTX/PureModelContextConcrete.java:17-19`) has no fields and no `_type`, so it is abstract in practice.

**The engine has no "Combined" or "Composite" context.** The name is `combination`. Studio's client also defines a `composite` context (`LG/model/context/V1_PureModelContextComposite.ts:22-37`, `_type: 'composite'`, fields `serializer`, `data`, `pointer`; `LG/transformation/pureProtocol/V1_PureProtocolSerialization.ts:61-67,165-174`). No `composite` subtype is registered on `PureModelContext` anywhere in legend-engine. A grep for `"composite"` and `PureModelContextComposite` found only unrelated `Lineage` and `ExecutionPlan` subtypes, plus one stale Swagger text in `Execute.java:177`. Studio uses `composite` only for service registration in SEMI_INTERACTIVE mode (`LG/V1_PureGraphManager.ts:4733-4780,4864-4902`). That call goes to a service-registration server, not to open-source engine endpoints.

### 1.2 `serializer` (`Protocol`)

`legend-engine/.../legend-engine-protocol/src/main/java/org/finos/legend/engine/protocol/Protocol.java:21-24`: `{"name": string, "version": string}`. In practice `name` is `"pure"` and `version` is a client version id.

Client version ids are listed in `legend-engine/.../legend-engine-protocol-pure/src/main/java/org/finos/legend/engine/protocol/pure/PureClientVersions.java:23`: `v1_0_0` … `v1_33_0`, plus `vX_X_X`. **`production = "v1_33_0"`** (line 31).

Studio: `PURE_PROTOCOL_NAME='pure'`, `DEV_PROTOCOL_VERSION=vX_X_X`, `PROD_PROTOCOL_VERSION=undefined` (`LG/V1_PureGraphManager.ts:679-681`).

### 1.3 `SDLC` polymorphism (`sdlcInfo`)

`CTX/SDLC.java:23-33`: `@JsonTypeInfo(use = NAME, property = "_type")`. It has **no defaultImpl**, so an `sdlcInfo` without `_type` fails to deserialize. The common fields are:

- `baseVersion: String` (default null)
- `version: String` (default **`"none"`**)
- `packageableElementPointers: PackageableElementPointer[]` (default empty)

| `_type` | Class | Extra fields | Semantics |
|---|---|---|---|
| `alloy` | `AlloySDLC` (`CTX/AlloySDLC.java:21-31`) | `groupId`, `artifactId`, `project` (**@Deprecated**) | A depot/metadata-server GAV. `version` is the project version. `"none"` or null means `master-SNAPSHOT` (see 2.3). |
| `pure` | `PureSDLC` (`CTX/PureSDLC.java:19-21`) | `overrideUrl` | The legacy Pure IDE metadata server. Elements come from `packageableElementPointers`. |
| `workspace` | `WorkspaceSDLC` (`CTX/WorkspaceSDLC.java:21-33`) | `project`, `isGroupWorkspace` (boolean) | **The workspace id goes in `version`.** `getWorkspace()` returns `this.version` (line 29-33, `@JsonIgnore`). |

`LegacySDLC` is an enum `{PURE, ALLOY}` (`CTX/LegacySDLC.java:17-21`). Nothing in the repo references it outside its own file, so it is dead.

Equality/hash (used as cache keys, see 2.4):

- `SDLC.equals` compares `version` and `packageableElementPointers` (`CTX/SDLC.java:35-54`).
- `AlloySDLC.equals` also compares `project`, `groupId` and `artifactId` (`CTX/AlloySDLC.java:33-48`).
- `PureSDLC.equals` compares `overrideUrl`, `version`, `baseVersion` and the pointers (`CTX/PureSDLC.java:23-42`).
- `WorkspaceSDLC.equals` compares `project`, `version` and `isGroupWorkspace` (`CTX/WorkspaceSDLC.java:35-54`).
- `PureModelContextPointer.equals` compares `serializer` and `sdlcInfo` (`CTX/PureModelContextPointer.java:75-95`).

### 1.4 `PackageableElementPointer`

`CTX/PackageableElementPointer.java:23-52`: fields `type` (enum), `path`, `sourceInformation`. Two `@JsonCreator`s exist:

- **DELEGATING from a bare string** (`"a::B"` becomes `{path:"a::B"}`, line 40-44)
- PROPERTIES `{type, path, sourceInformation}` (line 46-52)

`equals`/`hashCode` use **`path` only** (lines 60-81).

The `type` enum (`CTX/PackageableElementType.java:17-43`) has these values: `PACKAGE, PROFILE, CLASS, ASSOCIATION, ENUMERATION, FUNCTION, STORE, RUNTIME, BINDING, MAPPING, SERVICE, PERSISTENCE, PERSISTENCE_CONTEXT, FLATTEN, DATASTORESPEC, DATASPACE, DIAGRAM, FILE_GENERATION, DATA, QUERYPOSTPROCESSOR, DATA_QUALITY_VALIDATION, CONNECTION, EXECUTION_ENVIRONMENT_INSTANCE, DATAELEMENT`.

Studio's enum is a subset: `CLASS, STORE, MAPPING, RUNTIME, FUNCTION, FILE_GENERATION, SERVICE, DATA, ENUMERATION, ASSOCIATION, BINDING` (`legend-graph/src/graph/MetaModelConst.ts:187-199`). Studio serializes `{path, type?}` (`LG/transformation/pureProtocol/serializationHelpers/V1_CoreSerializationHelper.ts:58-64`).

### 1.5 `PureModelContextData` deprecated input fields

The `@JsonCreator` (`CTX/PureModelContextData.java:86-138`) accepts these **deprecated** top-level arrays and folds them into `elements`:

- `domain` (`{classes, associations, enums, profiles, functions, measures}`)
- `sectionIndices`, `stores`, `mappings`, `services`, `cacheables`, `caches`, `pipelines`, `flattenSpecifications`, `diagrams`, `dataStoreSpecifications`, `texts`, `runtimes`, `connections`, `fileGenerations`, `generationSpecifications`, `relationalMapper`, `serializableModelSpecifications`

Output uses only `serializer`, `origin` and `elements`. The Java class has no `_type` property, so when the engine serializes a PMCD the `_type` key is absent unless a polymorphic context is in play. Studio always writes `_type: "data"` (`V1_PureProtocolSerialization.ts:199-216`).

There is **no `deserializer` field** anywhere in the engine context classes or in legend-graph. A grep for `deserializer` in `CTX/` and `"deserializer"` in legend-graph returned nothing. If a spec mentions it, it is not part of this protocol.

### 1.6 Exact JSON examples (derived from the above)

```jsonc
// pointer, by GAV (what Query / DataCube send)
{"_type":"pointer",
 "serializer":{"name":"pure","version":"vX_X_X"},          // Studio may omit (PROD) -> null
 "sdlcInfo":{"_type":"alloy","groupId":"org.x","artifactId":"proj","version":"1.2.0",
             "baseVersion":"latest",                         // Studio default (V1_SDLC.ts:20); engine ignores for alloy
             "packageableElementPointers":[]}}

// pointer, workspace (engine-internal / SQL / GraphQL; Studio never sends this)
{"_type":"pointer","serializer":{"name":"pure","version":"v1_33_0"},
 "sdlcInfo":{"_type":"workspace","project":"PROD-1234","version":"myWorkspace","isGroupWorkspace":false}}

// pointer, legacy Pure server
{"_type":"pointer","serializer":{"name":"pure","version":"v1_33_0"},
 "sdlcInfo":{"_type":"pure","overrideUrl":null,
             "packageableElementPointers":[{"type":"MAPPING","path":"a::M"}]}}

// data
{"_type":"data","serializer":{"name":"pure","version":"v1_33_0"},
 "origin":{"_type":"pointer","serializer":{...},"sdlcInfo":{"_type":"alloy",...}},
 "elements":[{"_type":"class","package":"a","name":"B",...}]}

// text
{"_type":"text","serializer":{...},"code":"Class a::B {}"}

// combination
{"_type":"combination","contexts":[ <pointer>, <data> ]}
```

Studio-side classes for comparison:

- `V1_PureModelContextPointer(serializer?, sdlcInfo?)`: `LG/model/context/V1_PureModelContextPointer.ts:21-29`
- `V1_SDLC` (`baseVersion='latest'`, `version ?? 'none'`) and `V1_LegendSDLC(groupId, artifactId, version)`: `LG/model/context/V1_SDLC.ts:19-42`
- `V1_PureModelContextData { origin?, serializer?, elements, INTERNAL__rawDependencyEntities? }`: `V1_PureModelContextData.ts:23-33`
- `V1_PureModelContextText`: `V1_PureModelContextText.ts:20-23`
- `V1_PureModelContextCombination` (the client also writes a `serializer` key here; the engine class has no such field): `V1_PureModelContextCombination.ts:19-26`, `V1_PureProtocolSerialization.ts:176-192`

**The Studio client only knows one SDLC variant: `alloy` (`V1_LegendSDLC`).** `V1_SDLCType` has only `ALLOY` (`V1_PureProtocolSerialization.ts:57-59`). The pointer schema always uses `V1_legendSDLCSerializationModelSchema` (line 156-163). A grep for `WorkspaceSDLC|PureSDLC` across all Studio packages found no protocol usage.

---

## 2. Resolution: ModelManager and loaders

### 2.1 Wiring

- `Server.run` builds `new ModelManager(serverConfiguration.deployment.mode, getModelLoaders(serverConfiguration))` (`legend-engine/legend-engine-config/legend-engine-server/legend-engine-server-http-server/src/main/java/org/finos/legend/engine/server/Server.java:283`).
- `getModelLoaders` returns exactly one loader: `new SDLCLoader(serverConfiguration.metadataserver, null)` (`Server.java:457-461`). The `subjectProvider` is **null**.
- The `ModelLoader` interface (`MM/ModelLoader.java:22-34`) has `supports`, `load(identity, ctx, clientVersion, span)`, `setModelManager`, `shouldCache` and `cacheKey(ctx, identity)`.
- `SDLCLoader.supports(ctx)` is `ctx instanceof PureModelContextPointer` (`SDLCL/SDLCLoader.java:163-167`).
- `ModelManager.modelLoaderForContext` asserts exactly one loader supports the context (`MM/ModelManager.java:235-240`).

### 2.2 `ModelManager` algorithm (`MM/ModelManager.java`)

Public API:

- `loadModel(ctx, clientVersion, identity, packageOffset)`: returns a compiled `PureModel` (91-95).
- `loadData(ctx, clientVersion, identity)`: returns a PMCD (98-105).
- `loadModelAndData`: loads the data, then compiles with that pre-resolved data (108-114).
- `getLambdaReturnType`: `loadModel` plus `Compiler.getLambdaReturnType` (117-121).

All of these route through `loadModelOrData` (129-161):

1. **Combination** (131-152):
   - Recursively split the leaves into concretes and pointers (189-215). Data and Text go to concretes; Text is parsed immediately.
   - If there are any pointers, resolve `pointers[0]` with the data cache, then fold `a.combine(resolve(b))` over the remaining pointers, then fold `a.combine(concrete)` over the concretes.
   - Otherwise fold the concretes.
   - An empty combination throws `"No content to process"`.
   - The combined PMCD is compiled. **The combined result itself is never cached**; only the pointer leaves are cached, in the data cache.
2. **Concrete (Data/Text)** (153-156): no cache. Data is used as-is; Text is parsed.
3. **Pointer** (157-160): `resolvePointerAndCache(ctx, identity, cache, ...)`. Here `cache` is `pureModelCache` for `loadModel` and `pureModelContextCache` for `loadData`.

`resolvePointerAndCache` (217-233):

- If `loader.shouldCache(ctx)` is true, compute `key = loader.cacheKey(ctx, identity)` and call `cache.get(key, () -> resolver.apply(ctx))`.
- Otherwise call the resolver directly.

### 2.3 What is fetched for each SDLC type

`SDLCLoader.load` (`SDLCL/SDLCLoader.java:170-196`) does the following:

- **Asserts `clientVersion != null`**: "Client version should be set when pulling metadata from the metadata repository" (line 173).
- Dispatches through the `SDLCFetcher` visitor (`SDLCL/SDLCFetcher.java:59-111`).
- **Post-processes `origin`** (188-193): if `metaData.origin != null`, it asserts `origin.sdlcInfo.version == "none"` ("Version can't be set in the pointer"), then sets `origin.sdlcInfo.version = origin.sdlcInfo.baseVersion` and `baseVersion = null`.

Every remote fetch uses `SDLCLoader.loadMetadataFromHTTPURL` (203-274):

- The request is an HTTP GET by default (242).
- The client comes from `httpClientProvider.apply(identity)` when one is given; otherwise it is a plain `HttpClientBuilder.getHttpClient(new BasicCookieStore())` (213-220).
- Tracing headers are injected (247).
- The body is deserialized as `PureModelContextData` with the protocol-extension ObjectMapper (253).
- It **asserts `serializer != null`** on the response (254).
- `execHttpRequest` retries up to 5 times on 502/503/504 with a 200 ms sleep (276-304). Any non-2xx response throws `EngineException` (313-318).

| sdlcInfo | URL(s) called | Code |
|---|---|---|
| `alloy` | `GET {alloy.scheme}://{alloy.host}:{alloy.port}{alloy.prefix}/projects/{groupId}/{artifactId}/versions/{version}/pureModelContextData?convertToNewProtocol=false&clientVersion={clientVersion}` | `SDLCL/alloy/AlloySDLCLoader.java:45-51` |
| `workspace` | (a) `GET {sdlc base}/api/projects/{project}/workspaces/{ws}/pureModelContextData` (or `/groupWorkspaces/`); (b) `GET {sdlc base}/api/projects/{project}/{workspaces\|groupWorkspaces}/{ws}/revisions/HEAD/upstreamProjects`; (c) one alloy load per dependency through `modelManager.loadData` | `SDLCL/workspace/WorkspaceSDLCLoader.java:63-70, 77-126, 157-167` |
| `pure` | per pointer: `GET {pure base or allowed overrideUrl}/alloy/{pureModelFromMapping\|pureModelFromStore\|pureModelFromService}/{clientVersion}/{path}{?auth=kerberos}`; cache key: `GET {base}/alloy/pureServerBaseVersion{?auth=kerberos}` | `SDLCL/pure/PureServerLoader.java:62-76, 85-114, 122-135` |

**Alloy details** (`AlloySDLCLoader.java:45-65`, `SDLCFetcher.java:59-79`):

- It asserts `project == null`: "Accessing metadata services using project id was demised…" (line 47).
- It asserts `groupId` and `artifactId` are non-null (48).
- A null or `"none"` version is replaced by `master-SNAPSHOT` (49). Any other value, including `latest`, `HEAD` or `1.0.0`, is passed through verbatim.
- The `getDependencies` query param is **not sent**, so the depot default `true` applies and the PMCD returned is transitive (see 2.6).
- After the load, the fetcher sets `loadedProject.origin.sdlcInfo.packageableElementPointers = sdlc.packageableElementPointers` (SDLCFetcher:66). This **NPEs if the response has no `origin`**.
- `checkAllPathsExist` (AlloySDLCLoader:53-60) throws if any requested pointer path is missing from the elements (SDLCFetcher:67-77).

**Workspace details** (`SDLCFetcher.java:97-111`, `WorkspaceSDLCLoader.java`):

- The workspace PMCD from SDLC server is combined with a dependencies PMCD: `loadedProject.combine(deps)`, which is distinct plus sorted.
- `upstreamProjects` is called **without `transitive`**. SDLC's default is `false` (`legend-sdlc/legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/resources/dependency/project/user/WorkspaceRevisionDependenciesResource.java:50-53`), so only direct deps come back.
- Each direct dep `{projectId:"g:a", versionId}` (WorkspaceSDLCLoader:169-188, split on `:`) becomes an AlloySDLC pointer with `serializer = Protocol("pure", clientVersion)` and is loaded through `modelManager.loadData`, which uses the alloy cache. The depot then supplies the transitive closure because `getDependencies` defaults to true.
- The dependency builder calls `removeDuplicates()` but does not sort (110-118).

### 2.4 Caching

There are two Guava caches on `ModelManager` (`MM/ModelManager.java:64-65`). Both use `softValues()`, `expireAfterAccess(30, MINUTES)` and `recordStats()`:

- `pureModelCache: Cache<PureModelContext, PureModel>`
- `pureModelContextCache: Cache<PureModelContext, PureModelContextData>`

**What is cached** (`SDLCL/SDLCLoader.java:128-146`):

- `PureSDLC` pointers: always.
- `AlloySDLC` pointers: only when `!isLatestRevision`. `isLatestRevision` is true when the version is null, equals `"none"`, or **contains `"SNAPSHOT"`** (`AlloySDLCLoader.java:62-65`).
- `WorkspaceSDLC`: never.
- Data, Text and Combination roots: never.

**Cache key:**

- Alloy uses the **pointer object itself** (`AlloySDLCLoader.getCacheKey` returns its argument, line 67-70; `SDLCLoader.cacheKey` 148-161). The key therefore includes `serializer`, `groupId`, `artifactId`, `version`, `project` and pointer paths. It is **not keyed by identity**, so results are shared across users.
- Pure builds a new pointer containing `{packageableElementPointers, overrideUrl := resolved base url, serializer, baseVersion := GET /alloy/pureServerBaseVersion}` (`PureServerLoader.java:104-114`). Each cache-key computation makes an HTTP call, so it acts as revision-based invalidation.

**Invalidation:** there is no explicit invalidation API in this code; entries expire by TTL and soft refs only. The execution plan cache is separate: `Execute.usePlanCache` needs header `x-legend-use-plan-cache` and a pointer model (`legend-engine/legend-engine-core/legend-engine-core-query-pure-http-api/src/main/java/org/finos/legend/engine/query/pure/api/Execute.java:140-142`; the header constant is in `RequestContextHelper.java:25`).

### 2.5 Auth propagation

- **Kerberos subject:** `SDLCLoader.getSubject()` uses `subjectProvider` (line 109-120). The server passes null, so this is off by default. When set, loads run inside `exec(subject, ...)` (186).
- **PureSDLC:** appends `?auth=kerberos` when there is a current Subject (SDLCFetcher:87-92; PureServerLoader:88).
- **WorkspaceSDLC:** `doAs(identity)` runs under `KerberosUtils.getSubjectFromIdentity(identity)` if present (WorkspaceSDLCLoader:128-132). If `sdlc.pac4j` is a `privateAccessToken` config, it **appends `?client_name={first pac4j profile's clientName}`** to the URL (134-155). The code that adds the PAT header is commented out (line 145).
- **Alloy:** gets no special auth. It uses only what the `httpClientProvider` adds, and the server passes null, so in the open-source build it is a **plain unauthenticated GET** with an empty cookie store.
- Incoming user cookies and tokens are **not forwarded** to depot/SDLC by this code path.
- SQL `ProjectCoordinateLoader` and GraphQL wrap their calls in `Subject.doAs` when the identity has Kerberos credentials (`GraphQL.java:57-61,70-75`).

### 2.6 Configuration keys

The config section is `metadataserver` (`MetaDataServerConfiguration`, `SDLCL/configuration/MetaDataServerConfiguration.java:21-82`):

- `host`, `port`: **@Deprecated**. They are used only as a fallback for `pure` when `pure` is null (75-82).
- `pure`: a `ServerConnectionConfiguration`, or a `PureServerConnectionConfiguration` with `"_type":"pureServerConnectionConfiguration"` and `allowedOverrideUrls: string[]` (`PureServerConnectionConfiguration.java:21-24`; the subtype registration is at `ServerConnectionConfiguration.java:20-23`).
- `alloy`: a `ServerConnectionConfiguration` for the depot/metadata server.
- `sdlc`: a `ServerConnectionConfiguration` for the SDLC server. `getBaseUrl` is followed by `/api/...`.

`ServerConnectionConfiguration` (`ServerConnectionConfiguration.java:24-53`) has these fields:

- `host`, `port`
- `scheme` (default `"http"`)
- `prefix` (default `""`)
- `pac4j` (`{"_type":"privateAccessToken","accessTokenHeaderName":...}`, `MetadataServerPac4jConfiguration.java:20-23`, `MetadataServerPrivateAccessTokenConfiguration.java:17-20`)

`getBaseUrl()` returns `scheme + "://" + host + ":" + port + prefix`.

Shipped config (`legend-engine-server-http-server/config/config.json:53-63`):

```json
"metadataserver": { "pure": {"host":"127.0.0.1","port":8090},
                    "alloy": {"host":"127.0.0.1","port":8090,"prefix":"/depot/api"} }
```

The docker config is the same with `${METADATA_*}` env vars (`src/main/resources/docker/config/config.json:64-74`). Neither ships an `sdlc` block, so WorkspaceSDLC would NPE on `getBaseUrl()` in those configs. That is an inference from the code: `new WorkspaceSDLCLoader(metaDataServerConfiguration.sdlc)` at `SDLCLoader.java:97`.

### 2.7 What the depot returns (the server a lite model home must imitate)

`DEPOT-PMC/server/resources/pure/model/context/PureModelContextResource.java:55-98`:

- `GET projects/{groupId}/{artifactId}/versions/{versionId}/pureModelContextData`
  - Query params: `clientVersion`, `getDependencies` (default **true**), `convertToNewProtocol` (default **true**).
  - Responses carry an ETag built from GAV + clientVersion (line 77).
- `POST projects/dependencies/pureModelContextData`
  - Body: `[ProjectVersion]`. Query params: `clientVersion`, `transitive` (default true), `convertToNewProtocol` (default true).

`PureModelContextServiceImpl` (`DEPOT-PMC/services/pure/model/context/PureModelContextServiceImpl.java`) behaves as follows:

- `clientVersion` must be in the depot's copy of `PureClientVersions.versions`, else `IllegalArgumentException`. If null, it falls back to `production` (92-99).
- The version is resolved through `resolveAliasesAndCheckVersionExists` (65). Aliases: `latest` maps to the project's latest release; `head` maps to `BRANCH_SNAPSHOT(defaultBranch)` (`legend-depot/legend-depot-core-data-services/.../ProjectsServiceImpl.java:179-203`).
- The response has `serializer = {name:"pure", version: resolvedClientVersion}` and `origin = pointer{serializer same, sdlcInfo: AlloySDLC{project:"g:a", baseVersion: resolvedVersion, version: "none"(default)}}` (101-116, 126-132). This explains the engine's `version == "none"` assertion and the swap of `baseVersion` into `version`.
- When transitive, the dependency entities become a second PMCD that is merged with `distinct().sorted()`; the root project wins on duplicates (75-81, 118-124).
- With `convertToNewProtocol=false`, which **the engine always sends**, each element is the **raw stored entity `content` written verbatim** (`EntityToRawPureConverter`, 134-169). Otherwise the content is round-tripped through the engine protocol ObjectMapper and silently dropped if it fails (`withEntitiesIfPossible`).
- The POST variant uses `new AlloySDLC()` as the origin, so `version="none"` and no GAV (89).

### 2.8 What SDLC server returns for the workspace endpoint

`legend-sdlc/legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/resources/pmcd/project/user/WorkspacePureModelContextDataResource.java:38-70` resolves the current revision and then calls `PureModelContextDataResource.getPureModelContextData` (`.../server/resources/PureModelContextDataResource.java:26-41`):

- `serializer` is `pure` / `PureClientVersions.production`.
- `origin` is `AlloySDLC{project: projectId, baseVersion: revisionId}`.
- `elements` comes from `withEntitiesIfPossible`, which **silently drops unconvertible entities**.

Group workspaces use `.../groupWorkspaces/{workspaceId}/pureModelContextData` (`GroupWorkspacePureModelContextDataResource.java:37`). Other SDLC PMCD endpoints exist (revisions, versions, patches, `/projects/{p}/pureModelContextData`), but the engine never calls them.

---

## 3. Which engine endpoints accept which contexts

**General rule:** every endpoint that goes through `ModelManager.loadModel/loadData/loadModelAndData` accepts all four `_type`s (data, text, pointer, combination). The real constraints are on `clientVersion` and the pointer `serializer`.

**Key hazards:**

- (a) Pointer-bearing contexts require a non-null `clientVersion` (`SDLCLoader.java:173`).
- (b) Some endpoints derive `clientVersion` from `((PureModelContextPointer) model).serializer.version`. These **NPE if a pointer has no `serializer`**. Studio's DataCube code has a matching "TODO: remove as backend should handle undefined protocol input" (`legend-application-data-cube/src/stores/LegendDataCubeDataCubeEngine.ts:680-684`).
- (c) Those same endpoints pass a **null** clientVersion for a `combination`, so any combination containing a pointer fails the assertion in (a). The workaround is the `?clientVersion=` query param, which exists on `compile` only.

| Endpoint (POST unless noted) | Input / model field | Accepts | clientVersion used | Citation |
|---|---|---|---|---|
| `pure/v1/compilation/compile` | body = PureModelContext | all | `?clientVersion` query param, else pointer `serializer.version`, else null | `COMPAPI/Compile.java:68,81-97` |
| `pure/v1/compilation/lambdaReturnType` | `LambdaReturnTypeInput{model, lambda}` | all | pointer `serializer.version` (NPE if null), else null | `Compile.java:112-126`; `LambdaReturnTypeInput.java:20-23` |
| `pure/v1/compilation/lambdaRelationType`, `.../lambdaRelationType/batch` (`{model, lambdas}`), `c3Linearization` (`{model, fullPathTypes}`) | `model` | all | `PureClientVersions.production` (hardcoded) | `Compile.java:142-156,167-185,217-231` |
| `pure/v1/compilation/autofix/transformTdsToRelation/lambda` | `{model, lambda}` | all | pointer serializer.version | `COMPAPI/Autofix.java:53,67-76` |
| `pure/v1/execution/execute` | `ExecuteInput{clientVersion, function, mapping, runtime, context, model (required), parameterValues}` | all | `executeInput.clientVersion` or production | `Execute.java:176-191`; `.../shared/core/api/model/ExecuteInput.java:25-37` |
| `pure/v1/execution/generatePlan`, `generatePlan/debug` | ExecuteInput | all | same | `Execute.java:215-271` |
| `pure/v1/testable/runTests`, `debugTests` | `RunTestsInput{model, testables}` | all | pointer serializer.version, else null | `legend-engine-testable-http-api/.../TestableApi.java:58,78-90,105-115`; `RunTestsInput.java:23-28` |
| `pure/v1/grammar/jsonToGrammar/model` | body = PureModelContext | all (resolved via `loadData`) | hardcoded `"vX_X_X"` | `legend-engine-language-pure-grammar-http-api/.../JsonToGrammar.java:53,64-77` |
| `pure/v1/grammar/grammarToJson/*` | text | n/a (no model context) | n/a | `GrammarToJson.java:48-175` |
| `service/v1/doTest`, `service/v1/doValidation` | body = PureModelContext | **Data only** ("Only Full Interactive mode currently supported") | n/a | `legend-engine-xts-service/legend-engine-services-model-http-api/.../ServiceModelingApi.java:58,78-89,116-130` |
| `pure/v1/analytics/{mapping/modelCoverage, mapping/runtimeCompatibility, Class/modelCoverage, function/modelCoverage, binding/modelCoverage, diagram/modelCoverage, dataSpace/render, dataSpace/coverage, quality/checkDataspaceQuality, lineage/*}` | `{clientVersion, model, ...}` | all | `input.clientVersion` (may be null, which fails for pointers) | MappingAnalytics.java:82-110; ClassAnalytics:80; FunctionAnalytics:78; BindingAnalytics:79; DiagramAnalytics:82; DataSpaceAnalytics:90-119; DataspaceQualityAnalytics:61; LineageAnalytics:90-215 |
| `pure/v1/analytics/store-entitlement/{surveyDatasets,checkDatasetEntitlements}` | `{clientVersion, model,...}` | all | input or production | StoreEntitlementAnalytics.java:80,104 |
| `pure/v1/generation/generateArtifacts` | `ArtifactGenerationExtensionInput{clientVersion, model, includeElementPaths,...}` | all | input or production | ArtifactGenerationExtensionRunner.java:40-49 |
| `pure/v1/schemaGeneration/{avro,daml,jsonSchema,graphql,protobuf}`, `pure/v1/codeGeneration/morphir`, `pure/v1/external/format/{generateModel,generateSchema}` | `{clientVersion, model,...}` | all; they branch on `model instanceof PureModelContextData` to decide whether the call is "interactive" | input | AvroGenerationService:76-80 etc.; ExternalFormats.java:103-156 |
| `functionActivator/{validate,publishToSandbox,renderArtifact,generateLineage}` | `FunctionActivatorInput{clientVersion, model,...}` | all | input or production | FunctionActivatorAPI.java:114-197 |
| `pure/v1/dataquality/*` | `{clientVersion, model,...}` | all | input | DataQualityExecute.java:276-753 |
| `persistence/v1/platform/{validate,install,uninstall}` | `{clientVersion, model}` | all | input | PersistencePlatformActions.java:92 |
| `pure/v1/testData/generation/DONOTUSE_generateTestData` | `{clientVersion, model}` | all | input or production | TestDataGeneration.java:75 |
| `sql/v1/schema/getSchema`; `sql/v1/execution/*` | resolved internally | engine builds AlloySDLC pointers from `g:a:v` coordinates, or WorkspaceSDLC pointers from project/workspace | production | ProjectCoordinateLoader.java:53-119; SQLExecutor.java ~505-520 (combines same-source pointers with `PureModelContextPointer.combine`); SqlSchema.java:77 |
| `graphQL/v1/execution/{execute,generatePlans}/dev/{projectId}/{workspaceId}/...` and `/prod/{groupId}/{artifactId}/{versionId}/...` | path params | engine builds WorkspaceSDLC (dev) or AlloySDLC (prod) pointers | production | `legend-engine-xts-graphQL/.../GraphQL.java:48-76`; GraphQLExecute.java:118-569 |

`PureModelContextPointer.combine` (`CTX/PureModelContextPointer.java:32-67`) works as follows:

- It requires equal `version` and `baseVersion`.
- It keeps the Alloy GAV only if **both** sides are Alloy; otherwise it produces a **PureSDLC**.
- It takes the union of the element pointers.

---

## 4. How Studio and Query build the context they send

### 4.1 The decision function

There is one central rule in legend-graph: **if the graph has an `origin` (a `LegendSDLC` GAV), send a pointer; otherwise send the full PMCD.**

- `buildPureModelSDLCPointer(origin, clientVersion)` (`LG/V1_PureGraphManager.ts:5186-5201`) returns `new V1_PureModelContextPointer(clientVersion ? Protocol('pure', clientVersion) : undefined, new V1_LegendSDLC(g, a, versionId))`. Only `LegendSDLC` origins are supported; anything else throws.
- `getFullGraphModelContext(graph, clientVersion, options)` (5309-5319) returns a pointer if `graph.origin && !keepSourceInformation`, else the full data.
- `getFullGraphModelData(graph)` (5321-5341) concatenates:
  - the graph's own known elements, transformed to protocol (`graphToPureModelContextData`, 5481-5507, `excludeUnknown: true`), and
  - `getGraphCompileContext(graph)` (5509-5535): the generated elements, plus the dependency elements.

  For dependencies it prefers to **attach the raw dependency entities as `INTERNAL__rawDependencyEntities`** when `dependencyManager.origin instanceof GraphEntities`. When the PMCD is serialized, these are appended verbatim as `entity.content` (`V1_PureProtocolSerialization.ts:233-243`).
- The resulting PMCD has **no `serializer` and no `origin`**.

Per-operation behaviour:

- **Execution** (`createExecutionInput`, 2870-2897; `runQuery`, 3092-3115):
  - The model is the pointer if `graph.origin` is set (with `serializer` **undefined**), else the full PMCD.
  - `ExecuteInput.clientVersion` is `vX_X_X` when `engine.config.useDevClientProtocol` is set; otherwise it is undefined, and the engine falls back to `production` = `v1_33_0`.
  - Floating elements make it a `combination([context, data{floating elements}])` (2880-2889).
- **lambdaReturnType / relation types:** `getFullGraphModelContext(graph, vX_X_X)` gives a pointer with `serializer{pure, vX_X_X}` or full data (2352-2377).
- **runTests:** `getFullGraphModelContext(graph, DEV)` (2545, 2592).
- **compileGraph:** always full data (`getFullGraphModelData`) to `pure/v1/compilation/compile` (2061-2101; `engine/V1_RemoteEngine.ts:629-635`).
- **compileText** (the Studio text/grammar mode):
  - Grammar is first converted with `grammarToJson/model` (with source info).
  - It is then deep-merged as JSON with the dependency/generation compile context (`mergeObjects(serialize(compileContext), mainGraph)`).
  - The merged JSON is POSTed to `compile` (`V1_RemoteEngine.ts:667-700`; graph manager 2103-2120).
- **Mapping model coverage:** pointer (no serializer) or full data, with `clientVersion: vX_X_X` (3878-3891).
- **Execution-context graph data** (`prepareExecutionContextGraphData`, 2852-2867): `InMemoryGraphData` gives the full PMCD; `GraphDataWithOrigin` gives a pointer with PROD (undefined) serializer.

### 4.2 Studio in a workspace

- In `EditorGraphState`, the graph's own entities come from SDLC.
- Dependencies come from depot `POST /projects/dependenciesFromArtifactDependencies?transitive=true&includeOrigin=true&versioned=false` (`legend-server-depot/src/DepotServerClient.ts:323-347`; called from `legend-application-studio/src/stores/editor/EditorGraphState.ts:709-735`). The `dependencyManager.origin` is set to `GraphEntities(all dep entities)` (EditorGraphState.ts:553-562).
- **The main graph has no origin**, so **Studio always sends a full PMCD**: workspace elements as protocol JSON plus dependency entity contents verbatim.
- **Studio never sends `WorkspaceSDLC` pointers.**
- The engine's WorkspaceSDLC path is used only by engine-side SQL (`ProjectCoordinateLoader`) and GraphQL dev endpoints.
- In project-view mode with a GAV (`ProjectViewerStore.ts:404-418`), the graph gets `origin = LegendSDLC(g, a, resolveVersion(v))`, so pointer contexts are used.

### 4.3 Query (by GAV)

- `QueryEditorStore` fetches entities from depot `getEntities(project, versionId)` and indexed dependency entities (`legend-application-query/src/stores/QueryEditorStore.ts:822-839`).
- It then builds the graph with `origin: new LegendSDLC(groupId, artifactId, resolveVersion(versionId))` (859-870).
- `resolveVersion` maps `HEAD` to `master-SNAPSHOT` (`legend-server-depot/src/DepotVersionAliases.ts:21-27`).
- Every engine call from Query therefore uses the `alloy` pointer. Execute uses a serializer-less pointer plus `clientVersion` (undefined, which becomes production, or vX_X_X). lambdaReturnType uses a pointer with `serializer: {pure, vX_X_X}`.
- The engine then calls depot `/projects/{g}/{a}/versions/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=...`.
- Other origin setters:
  - `IngestQueryGraphHelper.ts:106`
  - DataSpace extension `V1_DSL_DataSpace_PureGraphManagerExtension.ts:764`
  - `data-cube/ExistingQueryDataCubeViewer.ts:100`

### 4.4 DataCube

- **Existing query source** (`legend-application-data-cube/src/stores/LegendDataCubeDataCubeEngine.ts:678-692`): pointer `serializer {pure, vX_X_X}` + `LegendSDLC(queryInfo.g, a, resolveVersion(v))`.
- **Lakehouse source** (1995-2021): `combination([pointer(serializer undefined, LegendSDLC(dp coordinates)), data{packageableRuntime}])`.
  - By hazard (b)/(c) in section 3, this fails on endpoints that derive clientVersion from the serializer. It works for `execute`, which uses `ExecuteInput.clientVersion` or production.
  - In the combination, pointer elements come first, so the injected runtime only survives if its path does not collide (see 6.4).
- **User-defined function source** (`UserDefinedFunctionDataCubeSourceBuilderState.ts:150-168`) and **FreeformTDS** (`FreeformTDSExpressionDataCubeSourceBuilderState.ts:217-226`): pointer with `serializer` undefined.
- **Data-product extension:** `combination([model, data])` (`legend-extension-dsl-data-product/src/utils/QueryExecutionUtils.ts:55`) and pointers (`DataProductIngestUtils.ts:735-745`).

### 4.5 Service registration (for completeness)

`registerService` / `bulkServiceRegistration` (`LG/V1_PureGraphManager.ts:4703-4920`) build three different shapes:

- **FULL_INTERACTIVE:** full PMCD with `origin = pointer{Protocol('pure', serverInfo.services.dependencies.pure), LegendSDLC(g, a, v) with SERVICE pointer}`.
- **SEMI_INTERACTIVE:** `composite{serializer, data: PMCD{elements:[service], origin: pointer(no sdlc)}, pointer: alloy with MAPPING pointers}`.
- **PROD:** alloy pointer with a SERVICE element pointer.

These go to a separate registration server, not to these engine endpoints.

---

## 5. Entities to PMCD conversion

### 5.1 Entity shape and content

An SDLC/Depot `Entity` is `{path, classifierPath, content}`.

**`content` is already protocol JSON**: an engine `PackageableElement` with `_type`, `package` and `name`. The evidence:

- `EntityToProtocolConverter.fromEntity` just calls `objectMapper.convertValue(entity.getContent(), PackageableElement.class)` (`legend-sdlc/legend-sdlc-protocol/src/main/java/org/finos/legend/sdlc/protocol/EntityToProtocolConverter.java:32-65`).
- `EntityToPureConverter.getTargetClass` always returns `PackageableElement.class` (`SDLC-PP/EntityToPureConverter.java:22-33`).

So **`classifierPath` is NOT used for deserialization**; `_type` inside `content` decides the class. Studio does the same: it deserializes `entity.content` and passes `classifierPath` only as a hint (`V1_PureProtocolSerialization.ts:69-131`; the comment at 115-120 says it skips the classifier check).

Conversion paths:

- **Depot with `convertToNewProtocol=false`** (what the engine requests): elements are `entity.content` written verbatim (2.7).
- **SDLC server and depot with `true`:** `PureModelContextDataBuilder.withEntitiesIfPossible`, which drops entities that fail conversion. The builder sets `serializer` and `origin = pointer{serializer, sdlc}` whenever a protocol or SDLC is given (`SDLC-PP/PureModelContextDataBuilder.java:175-191`).
- **Reverse** (`PureToEntityConverter`, `SDLC-PP/PureToEntityConverter.java:28-55`): `classifierPath` comes from `ProtocolToClassifierPathLoader.getProtocolClassToClassifierMap()`. That map is assembled from every `PureProtocolExtension.getExtraProtocolToClassifierPathMap()` and throws on conflicts (`legend-engine/.../protocol/pure/v1/ProtocolToClassifierPathLoader.java:24-41`).

### 5.2 Classifier path to `_type` (complete list for engine @230c159)

This covers every `getExtraProtocolToClassifierPathMap` implementation (21 files, 20 extensions), joined with each extension's `PackageableElement` subtype registration.

| classifierPath | `_type` | Protocol class | Extension (file) |
|---|---|---|---|
| `meta::pure::metamodel::type::Class` | `class` | Class | CorePureProtocolExtension.java:199 / 118 |
| `meta::pure::metamodel::type::Enumeration` | `Enumeration` (capital E) | Enumeration | Core :200 / :117 |
| `meta::pure::metamodel::relationship::Association` | `association` | Association | Core :198 / :119 |
| `meta::pure::metamodel::extension::Profile` | `profile` | Profile | Core :206 / :116 |
| `meta::pure::metamodel::function::ConcreteFunctionDefinition` | `function` | Function | Core :103,:202 / :120 |
| `meta::pure::metamodel::type::Measure` | `measure` | Measure | Core :203 / :121 |
| `meta::pure::mapping::Mapping` | `mapping` | Mapping | Core :102,:201; `_type` in `protocol/pure/m3/PackageableElement.java:30` |
| `meta::pure::runtime::PackageableConnection` | `connection` | PackageableConnection | Core :204; m3/PackageableElement.java:32 |
| `meta::pure::runtime::PackageableRuntime` | `runtime` | PackageableRuntime | Core :205; m3/PackageableElement.java:33 |
| `meta::pure::data::DataElement` | `dataElement` | DataElement | Core :207; m3/PackageableElement.java:34 |
| `meta::external::format::shared::metamodel::SchemaSet` | `externalFormatSchemaSet` | ExternalFormatSchemaSet | Core :208 / :122 |
| `meta::external::format::shared::binding::Binding` | `binding` | Binding | Core :209 / :123 |
| `meta::pure::metamodel::section::SectionIndex` | `sectionIndex` | SectionIndex | Core :210 / :115 |
| `meta::relational::metamodel::RelationalMapper` | `relationalMapper` | RelationalMapper | Core :211 / :124 |
| `meta::relational::metamodel::Database` | `relational` | Database | RelationalProtocolExtension.java:209 |
| `meta::legend::service::metamodel::Service` | `service` | Service | ServiceProtocolExtension.java:48,96 |
| `meta::legend::service::metamodel::ExecutionEnvironmentInstance` | `executionEnvironmentInstance` | ExecutionEnvironmentInstance | ServiceProtocolExtension.java:97 |
| `meta::pure::metamodel::dataSpace::DataSpace` | `dataSpace` | DataSpace | DataSpaceProtocolExtension.java:55 |
| `meta::pure::metamodel::diagram::Diagram` | `diagram` | Diagram | DiagramProtocolExtension.java:50 |
| `meta::pure::metamodel::text::Text` | `text` | Text | TextProtocolExtension.java:50 |
| `meta::pure::generation::metamodel::GenerationSpecification` | `generationSpecification` | GenerationSpecification | GenerationProtocolExtension.java:52 |
| `meta::pure::generation::metamodel::GenerationConfiguration` | `fileGeneration` | FileGenerationSpecification | GenerationProtocolExtension.java:52 / :44 |
| `meta::pure::persistence::metamodel::Persistence` | `persistence` | Persistence | PersistenceProtocolExtension.java:39,80 |
| `meta::pure::persistence::metamodel::PersistenceContext` | `persistenceContext` | PersistenceContext | PersistenceProtocolExtension.java:40,81 |
| `meta::external::store::service::metamodel::ServiceStore` | `serviceStore` | ServiceStore | ServiceStoreProtocolExtension.java:107 |
| `meta::external::store::mongodb::metamodel::pure::MongoDatabase` | `MongoDatabase` | MongoDatabase | MongoDBPureProtocolExtension.java:72 |
| `meta::external::store::elasticsearch::v7::metamodel::store::Elasticsearch7Store` | `elasticsearch7Store` | Elasticsearch7Store | ElasticsearchV7ProtocolExtension.java:70 |
| `meta::external::store::deephaven::metamodel::store::DeephavenStore` | `deephavenStore` | DeephavenStore | DeephavenProtocolExtension.java:72 |
| `meta::external::function::activator::deephavenApp::DeephavenApp` | `DeephavenApp` | DeephavenApp | DeephavenProtocolExtension.java:73 / :50 |
| `meta::external::function::activator::snowflakeApp::SnowflakeApp` | `snowflakeApp` | SnowflakeApp | SnowflakeProtocolExtension.java:42,91 |
| `meta::external::function::activator::snowflakeM2MUdf::SnowflakeM2MUdf` | `snowflakeM2MUdf` | SnowflakeM2MUdf | SnowflakeProtocolExtension.java:43,92 |
| `meta::external::function::activator::hostedService::HostedService` | `hostedService` | HostedService | HostedServiceProtocolExtension.java:38,75 |
| `meta::external::function::activator::functionJar::FunctionJar` | `functionJar` | FunctionJar | FunctionJarProtocolExtension.java:36,63 |
| `meta::external::function::activator::bigQueryFunction::BigQueryFunction` | `bigQueryFunction` | BigQueryFunction | BigQueryFunctionProtocolExtension.java:35,67 |
| `meta::external::function::activator::bigQueryFunction::BigQueryFunctionDeploymentConfiguration` | `bigQueryFunctionConfig` | BigQueryFunctionDeploymentConfiguration | BigQueryFunctionProtocolExtension.java:49,68 |
| `meta::external::function::activator::memSqlFunction::MemSqlFunction` | `memSqlFunction` | MemSqlFunction | MemSqlFunctionProtocolExtension.java:34,60 |
| `meta::external::function::activator::memSqlFunction::MemSqlFunctionDeploymentConfiguration` | `memSqlFunctionConfig` | MemSqlFunctionDeploymentConfiguration | MemSqlFunctionProtocolExtension.java:42,61 |
| `meta::external::dataquality::DataQuality` | `dataQualityValidation` | DataQuality | DataQualityProtocolExtension.java:38,51,81 |
| `meta::external::dataquality::DataQualityRelationValidation` | `dataqualityRelationValidation` (lowercase q) | DataqualityRelationValidation | DataQualityProtocolExtension.java:60,82 |
| `meta::external::dataquality::DataQualityRelationComparison` | `dataQualityRelationComparison` | DataQualityRelationComparison | DataQualityProtocolExtension.java:63,83 |
| `meta::pure::runtime::connection::authentication::demo::AuthenticationDemo` | `authenticationDemo` | AuthenticationDemo | AuthenticationProtocolExtension.java:92 |

Two more `PackageableElement` `_type`s have **no classifier path**: `mappingClass` (MappingClass, m3/PackageableElement.java:31) and `sectionIndex`, which does have one (above).

- `PackageableElement` has **no `defaultImpl`** (m3/PackageableElement.java:28-35). An unknown `_type` therefore fails deserialization of the whole PMCD under default Jackson settings.
- I did not find any extension in this tree that disables `FAIL_ON_INVALID_SUBTYPE`. `ObjectMapperFactory.withStandardConfigurations` sets only sorting/close features (`legend-engine-shared-core/.../ObjectMapperFactory.java:32-38,66-71`).
- **FlatData:** no `flatData` `_type` or classifier path exists in this engine tree. A grep for `"flatData"` found nothing in non-test Java. It appears to have been removed or moved out at this commit.

### 5.3 legend-sdlc-language-pure-compiler

`legend-sdlc/legend-sdlc-language-pure-compiler/src/main/java/org/finos/legend/sdlc/language/pure/compiler/toPureGraph/PureModelBuilder.java` is its only main source file (lines 33-264). It is a thin **in-process** builder: it adds entities through `PureModelContextDataBuilder` (strict `addEntity` or lenient `addEntityIfPossible`), with optional `withSDLC` and `withProtocol`.

`build()` produces a PMCD and then calls `new PureModel(pmcd, CompilerExtensions (ServiceLoader, optional classLoader), null, classLoader, DeploymentMode.PROD, new PureModelProcessParameter(packagePrefix), null)` (194-221). It returns `PureModelWithContextData{pureModel, pureModelContextData}`.

It does **no HTTP** and no model-manager resolution. Its users are the Maven plugins `ModelGenerationMojo`, `ServicesGenerationMojo` and `FileGenerationMojo`, which compile a project's entities offline. It is the reference for "entities to compiled graph without a server".

---

## 6. Surprises and important details

1. **`origin.sdlcInfo.version` must be `"none"` (or absent) in a PMCD that a model home returns for an alloy or workspace pointer.**
   - The engine asserts this and then swaps `baseVersion` into `version` (`SDLCLoader.java:188-193`). The depot and SDLC put the real version or revision in **`baseVersion`** and leave `version` at its default `"none"` (depot ServiceImpl:126-132; SDLC PureModelContextDataResource:33-35).
   - For alloy, `origin` **must be present** or the engine NPEs (`SDLCFetcher.java:66`).
   - `serializer` **must be present** or the engine throws (`SDLCLoader.java:254`).
2. **`origin` carries `project: "g:a"` (deprecated).** Echoing that origin back to the engine as a pointer fails the `project == null` assertion (`AlloySDLCLoader.java:47`).
3. **clientVersion handling:**
   - The engine forwards the caller's clientVersion verbatim to the depot (`&clientVersion=`). For pointers this is `ExecuteInput.clientVersion`, falling back to `v1_33_0`, or `serializer.version` (often `vX_X_X`).
   - The depot validates it against its own `PureClientVersions` list (ServiceImpl:92-99). A model home must accept `vX_X_X` and all `v1_*` values.
   - With `convertToNewProtocol=false`, clientVersion has **no effect on content**; it only sets `serializer.version`.
4. **Duplicate handling differs by path:**
   - `combine()` and `Builder.distinct()` dedupe by `package + name`, **first one wins** (`PureModelContextData.java:166-179,193-211,356-365`). Function names already include the signature in protocol (e.g. `f_String_1__String_1_`).
   - In a `combination`, **pointer-resolved elements come first**, so a concrete element with the same path as a pointer element is **silently dropped** (`ModelManager.java:139-141`).
   - A single `data` context is **not** deduped. The compiler's `PureModelContextDataValidator` throws `"Duplicated element '<path>'"` and also requires a non-empty package and name (`.../compiler/toPureGraph/validator/PureModelContextDataValidator.java:32-56`, invoked at `PureModel.java:280`).
   - `mergeSectionIndexes()` exists (317-348) but is not called by `combine`.
5. **Snapshot caching quirk:**
   - The cache bypass applies only when the version is null, `"none"` or contains `"SNAPSHOT"`.
   - **`latest` and `head` aliases are cached for up to 30 min of idle time, even though they move** (`AlloySDLCLoader.java:62-65`).
   - Cache keys ignore identity, so cached data is shared across users.
   - Each distinct `serializer` creates a distinct cache entry for the same GAV.
6. **Hazards for pointer-bearing contexts:**
   - `compile`, `lambdaReturnType`, `runTests` and `autofix` NPE on a pointer without `serializer`.
   - They pass a null clientVersion for a `combination`, which fails `SDLCLoader`'s assertion. Only `compile` has the `?clientVersion=` escape hatch.
   - `jsonToGrammar/model` always uses `vX_X_X`.
7. **WorkspaceSDLC is an engine-internal path, not a Studio path.**
   - It uses SDLC `/api/projects/{p}/{workspaces|groupWorkspaces}/{w}/pureModelContextData` and `/revisions/HEAD/upstreamProjects` (non-transitive).
   - Each dependency then goes through the depot, which is transitive.
   - It is never cached.
   - The workspace id goes in `sdlcInfo.version`.
8. **Combination loads its pointers sequentially and does not cache the combined result.** Combining with a `text` context parses grammar at request time.
9. **`text` contexts parse with `PureGrammarParser.newInstance().parseModel(code)`**, with the default source-information setting (`ModelManager.java:181`).
10. **The depot drops nothing when `convertToNewProtocol=false`, but the SDLC server drops unconvertible entities** (`withEntitiesIfPossible`). Both depot and engine sort with `sorted()` (package then name) when merging.
11. **Studio quirks:**
    - `V1_SDLC.baseVersion` defaults to `'latest'` and is serialized. The engine ignores it for alloy keys, but `PureModelContextPointer.combine` compares it.
    - Studio serializes a `serializer` key on `combination`; the engine ignores it.
    - Studio's PMCD omits `serializer` and `origin`, which is fine for `data`.
12. **No auth is forwarded to the depot by the open-source engine.** The SDLC server only gets `?client_name=` when a pac4j PAT config is present (2.5). A lite home sitting behind the engine should not depend on user credentials for `pureModelContextData`.

## Open items / not determined

- I did not trace how `Identity` is built from pac4j profiles beyond `Identity.makeIdentity` (`legend-engine-identity-core/.../Identity.java:118-130`), nor whether any deployment-specific `httpClientProvider` adds bearer tokens. Server.java passes null.
- I did not open the SQL providers' `SQLSourceProvider` implementations beyond `ProjectCoordinateLoader`, so how the per-source contexts are produced in each provider is not enumerated.
- I did not verify whether serializr emits `"origin": null` or omits the key when Studio's `origin`/`serializer` is undefined. The engine tolerates both.
- I did not check whether depot `BRANCH_SNAPSHOT(defaultBranch)` is literally `master-SNAPSHOT` for all projects. It depends on the project's default branch.

---

<!-- Part F -->
# Part F — What legend-lite already has toward a lite Studio (2026-10-01)

| repo | path | commit |
|---|---|---|
| legend-lite (main, query worktree) | `/Users/neema/legend/legend-lite-query` | `19357c167 2026-10-01` |
| studio-lite (old React frontend) | `/Users/neema/legend/studio-lite` | `292962f 2026-04-13` (11 commits total) |

All paths below are relative to the legend-lite root unless prefixed `studio-lite/`. Every claim was
read in code or docs at these commits; "not determined" is said where it applies.

---

## 0. Executive summary

- **There is no model home, no project, no version, no workspace, no entity store anywhere in lite.**
  Every client carries the whole model as Pure grammar text (`{"_type":"text","code":...}`), and the
  server recompiles it on every request. The only persisted, versioned records are *saved queries*
  (server: one JSON file per query in `--query-store DIR`; browser: IndexedDB) and *saved cubes*
  (browser IndexedDB only). `docs/MODEL_HOME_2026_09_28.md` is a survey + options doc; no code.
- **The LSP is a stub.** `PureLspServer` (278 lines) implements only initialize/shutdown/didOpen/
  didChange/didClose and publishes **parse-only** diagnostics, with positions *guessed from message
  text* and a fixed 20-char range. No completion, hover, definition, formatting, semantic tokens,
  no compile/type errors, no cross-file resolution. Transport: one HTTP POST per JSON-RPC message
  on `/lsp` (not websocket, not stdio). Every audit calls it legacy/untrustworthy; the plan replaces
  its innards in W1.2(a) (diagnostics sink) and later phases.
- **The grammar/protocol layer is strong for text → JSON, weak for JSON → text/models.**
  `grammarToJson/model` (PMCD) and `grammarToJson/lambda` are byte-exact vs legend-engine 4.145.0;
  `jsonToGrammar/lambda` (+batch) has byte parity. **Missing:** a PMCD *reader* (model JSON →
  records; `data`/`pointer`/`combination` contexts refused), and `jsonToGrammar/model` (no model
  composer). Comments are dropped by the lexer (as upstream's JSON does).
- **Compiler entry points a Studio needs already exist:** `Compiler.parseSources(List<ModelSource>)`
  (true multi-file, per-file imports, per-file parse walls), `Compiler.buildModule` (TOLERANT,
  poison-don't-drop, every element's first error), `Compiler.compileAllBodies` (eager type-check of
  every function body, never throws). But errors are **strings** (`Map<String,String>` "walls";
  positions rendered into message text as `[line:col]`), not structured diagnostics with spans.
  Nothing in the server uses `buildModule`/`parseSources`; `compilation/compile` uses strict
  `compileModel` + `compileAllBodies` and returns only the first wall, with no `sourceInformation`.
- **studio-lite is throwaway as code**, retired by ruling (`docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md:7`).
  It is a single-file Monaco editor that calls four endpoints of which two (`/engine/execute`,
  `/engine/sql`) no longer exist and one (`/engine/nlq`) lives in another server. Reusable ideas only
  (Monarch tokenizer, marker plumbing, Cytoscape class diagram).
- **Reusable building blocks for a Studio:** `pure-protocol/` (TS V1 lambda JSON lib), Query app's
  PMCD graph index (`query/src/model/graph.ts`, `pmcd.ts`), the WASM planner (grammar, typing,
  planning in a worker), the upstream-shaped query store pattern (HTTP + IndexedDB with identical
  rules), `projects/` (59 interdependent Pure projects with a declared dependency DAG: a ready
  multi-project test corpus for SDLC/Depot-lite), `DiagramService` (class/association extraction).

---

## 1. The LSP (`core/src/main/java/com/legend/server/PureLspServer.java`)

### 1.1 What it implements

| LSP method | Implemented? | Where |
|---|---|---|
| `initialize` | yes; advertises only `textDocumentSync {openClose:true, change:1 (FULL)}` | `PureLspServer.java:63-79` |
| `initialized`, `exit` | no-op | `:45`, `:47` |
| `shutdown` | returns null result | `:81-83` |
| `textDocument/didOpen` / `didChange` / `didClose` | yes, full-text sync (takes `contentChanges[0].text` only) | `:85-132` |
| `textDocument/publishDiagnostics` (sent) | yes, returned in the HTTP response body | `:138-166`, `:243-254` |
| completion, hover, definition, references, documentSymbol, formatting, semanticTokens, codeAction, rename | **no** (unknown methods with an id get `-32601`) | `:51-56` |

The class javadoc itself says "Implements only the subset needed for MVP" (`:13-20`). The server's
javadoc and README over-claim: `LegendHttpServer.java:21` ("diagnostics, completions, etc."),
`README.md:298` ("diagnostics, completions, hover"). The `initialize` result names
`legend-lite-lsp 1.0.0` (`:73-76`).

### 1.2 What a "diagnostic" is

- **Parse only.** `rebuildAndPublishAll` calls `com.legend.Compiler.parseModel(text)` per open
  document (`:145-151`). No name resolution, no element compile (F), no body typing (G). So an
  undefined type, bad property, bad mapping, or type error produces **zero** diagnostics.
  (Audits: `docs/plan-audit-2026-09-26/stage-readings-2026-09-28/A12-periphery.md:59-62` "it only
  parses, never compiles (:147) — no type errors"; `docs/type-audit-2026-08/findings/A31-public-api.md:460`
  "F10 — the IDE/LSP surface exposes no type information at all".)
- **Each file parsed independently** (`:141-144` comment): one broken file cannot mask another, but
  also no cross-file checking at all. The "multi-file" tests (`PureLspServerTest.java:126-193`, e.g.
  `testThreeFiles_associationAcrossFiles`) pass only because nothing is resolved.
- **At most one diagnostic per file** — the first parse exception (`:157-163`, `:171-228`).
- **Positions are guessed from the message string** (`:176-209`): it searches for the substring
  `"line "`, else takes the first `'quoted'` word in the message and finds its *first occurrence
  anywhere in the file*. The core `ParseException` formats `"[line:col] msg"`
  (`parser/ParseException.java` `formatMessage`), which does not contain `"line "`, so the
  quoted-token search is what normally runs. Range end is always `character + 20` (`:219`).
  Severity always 1, source `"legend-lite"` (`:223-224`).
- The javadoc cites a "ParseCache" that does not exist (`:136`; noted in A12-periphery.md:62).

### 1.3 Transport and state

- **HTTP POST `/lsp`**, one JSON-RPC message per request; the response body is the single response
  or a JSON array of notifications; `204` if none (`LegendHttpServer.java:156-183`). No websocket,
  no stdio framing (Content-Length headers), no server→client push. (EXECUTION_PLAN W0.7 calls it
  "the stdio LSP", `docs/EXECUTION_PLAN_2026_09_26.md:498-499`, but there is no stdio main in code; the only
  `main` in core main is `LegendHttpServer` — checked by grep.)
- **One global document map for the whole server**: `private final Map<String,String> documents =
  new HashMap<>()` (`:24`), one `PureLspServer` per HTTP server (`LegendHttpServer.java:50`). Every
  browser tab/user shares the same URI space; no session or client id.
- HTTP dispatcher is single-threaded: `server.setExecutor(null)` (`LegendHttpServer.java:345`).
- Unchecked cast of `contentChanges[0]` to `Json.Obj` (`:109`) — A31 reported `contentChanges:["oops"]`
  leaks internal class names (`A31-public-api.md:439-443`).

### 1.4 Performance characteristics

- **Re-parses every open document on every didOpen/didChange/didClose** (`:95`, `:113`, `:127`);
  no incremental parse, no cache (despite the javadoc). Parse only, so per-keystroke cost is lexer+
  parser (fast), but if it were switched to compile it would recompile the world per change (no
  compiled-model cache exists anywhere except the boot layer — §3.3).
- studio-lite debounces didChange on the client (`studio-lite/src/App.tsx:294-301`).

### 1.5 Tests

`core/src/test/java/com/legend/server/PureLspServerTest.java` (382 lines, 14 `@Test`s): initialize,
shutdown, unknown method, valid file → empty diagnostics, a syntax error at line 0 col 26
(`:81-99`), change/close, three multi-file scenarios, and an HTTP round trip (`:195`). Nothing
tests completion/hover (not implemented) or semantic errors.

### 1.6 Dormant IDE infrastructure (`core/src/main/java/com/legend/ide/`)

`ModelIndex` (112 lines), `ModelIndexer` (304), `ModelOrchestrator` (165): a shallow FQN→token-range
scanner and demand-driven per-element parser facade. The package javadoc says it is "Dormant…
Currently unused by the batch pipeline… waits in this package until a wrapping IDE layer needs it"
(`ide/package-info.java:1-41`). PureLspServer does not use it; A12 lists `ide/` as dead
(`A12-periphery.md:88-89`). `ModelOrchestrator` is documented not thread-safe (A31:992).

### 1.7 What the plan and audits say

| source | verdict |
|---|---|
| `docs/COMPILER_STAGE_AUDIT_2026_08.md:500` | "Two further live surfaces are 100% legacy: `POST /lsp` (Studio Lite's diagnostics and completions) and `POST /engine/diagram`" (Aug 2026; since rerouted onto the core parser, still parse-only) |
| `A12-periphery.md:59-62` | guesses positions; fixed `+20` range; only parses; phantom ParseCache |
| `A31-public-api.md:37,42,460-523` | diagnostics "untrustworthy" (F11, F12); no type info (F10); plain HashMap, not thread-safe |
| `plan-audit…/h4-diagnostics-design-2026-09-29.md:7-37` | today: errors are exceptions with positions **rendered into message text**; `SourceInfo` spans are dropped at `model/FromProtocol` ("Positions are dropped here, on purpose", `FromProtocol.java:24-27`) and survive only on `TypedNativeCall.pos`/`TypedUserCall.pos`; no error after the parser carries a span as data |
| `h4…:48-49` design | `record Diagnostic(Code code, Severity, Phase, @Nullable Span span, List<String> args, List<Related> related, String message)` in a sink from every stage; codes are the contract |
| `docs/EXECUTION_PLAN_2026_09_26.md:537-545` | **W1.2** in four pushes: (a) types, sink, stage bridges, **the LSP reading the sink** (Phase 2, `:417`); (b) parser codes + spans + UTF-16 columns; (c) speculative scopes in the typer; (d) element-level parser recovery (Phase 4, `:433`). Metric: positioned-diagnostic % over `MutationFuzzTest` inputs. Size 4–6 sessions |
| `EXECUTION_PLAN…:182-188` | target architecture: diagnostics sink read by the LSP; a **memoized query layer** (`parse(unit)`, `declIndex`, `resolveBody(id)`, `typeBody(id)`…) keyed per model snapshot (W2.2b, D10) — this is the rust-analyzer-style substrate an IDE needs, not yet built |
| `EXECUTION_PLAN…:196-197` | "model authors in an IDE (`/lsp`)" is listed as one of lite's user groups |
| `EXECUTION_PLAN…:337` (D10, ruled 2026-09-29) | two modes: compile-all collects every diagnostic; user paths demand-driven and memoized per model |
| `datacube/docs/ENGINE_API_CONTRACT.md:35,93-95`; `docs/SERVER_PROGRAM_2026_09_26.md:418` | upstream code completion `pure/v1/codeCompletion/completeCode` is **absent from engine 4.145.0**; recommendation "match legend-engine (no endpoint)", typeahead from lite's own completion in the page |
| `docs/QUERY_APP_DESIGN_2026_09_30.md:38-39` | Query's "Next": open items, "then Studio/LSP" |

Note also `/lsp` and `/engine/diagram` are **not upstream endpoints** yet still routed
(`LegendHttpServer.java:107,136`), in tension with the 2026-09-27 ruling that lite's client surface
is upstream's APIs only (§7).

---

## 2. Grammar / protocol round trip

### 2.1 Endpoints served (`LegendHttpServer.java:217-238`, `PureV1Api.java`)

| upstream endpoint | lite | input → output | fidelity / notes |
|---|---|---|---|
| `grammar/grammarToJson/model` (E2) | yes `PureV1Api.java:88-93` | model text → PMCD JSON (`PmcdParser.parseDocument`) | "byte-exact"; envelope `{"_type":"data","elements":[…]}` + `__internal__::SectionIndex` last, section spans in the engine's coordinates (`parser/PmcdParser.java:18-35`); "PMCD parity on 5,259 sources" (`UPSTREAM_ENDPOINTS_DESIGN…:32-33`); `returnSourceInformation=false` strips spans (`LegendHttpServer.java:215-216`) |
| `grammar/grammarToJson/lambda` (E1) | yes `:80-85` | text → lambda JSON | byte-exact; test `PureV1ApiTest.java:55-85` |
| `grammar/jsonToGrammar/lambda` (+`/batch`) (E4) | yes `:105-123` | lambda JSON → text, `PRETTY`/`STANDARD` | byte parity with upstream printer (`ComposerParityTest` in `parser-equivalence/`); `PureComposer` exposes only `lambda` and `valueSpecification` (`protocol/PureComposer.java:64,73`) |
| `grammar/jsonToGrammar/model` | **no** | — | no model composer exists |
| `compilation/compile` (C1) | yes `:137-151` | `{_type:text,code}` → `{"message":"OK","defects":[]}` or the **first** failure, 400 | strict `compileModel` + `compileAllBodies`; recorded differences: no `defects` (warnings), and the refusal "carries the element in the message, not a `sourceInformation`" (`:133-135`) |
| `compilation/lambdaReturnType` (E6), `lambdaRelationType` (E5) | yes `:159-196` | `{model,lambda}` | lite type vocabulary (Integer/Decimal(p,s)/DateTime vs engine Int/Numeric/Timestamp) is a recorded difference (`UPSTREAM_ENDPOINTS_DESIGN…:41-45`) |
| `execution/generatePlan` (E9), `execution/execute` (E8) | yes `:203-248` | `ExecuteInput` | `parameterValues` bound as `let`s (`:259-297`); graphFetch JSON results (`:235-245`) |
| `pure/v1/query` store, `server/v1/currentUser` | yes `LegendHttpServer.java:114-135` | upstream Query records | see §5.1 |

Errors: upstream's shape `{"code":-1,"errorType":"PARSER"|"COMPILATION","message":…,"status":"error"}`
(`PureV1Api.java:539-548`), status per kind measured against 4.145.0 (`:510-537`). **No
`sourceInformation` in any error** — positions only inside `message` text as `[line:col]`
(`Compiler.java:94-104`, `error/LegendCompileException.java` `position()`). Upstream Studio relies on
`sourceInformation` in compilation errors to place markers; lite cannot supply it today.

### 2.2 Model contexts accepted

`PureV1Api.modelText` (`:493-500`) accepts only `_type:"text"`; anything else →
`"a model of _type '…': legend-lite reads PureModelContextText …; the PMCD reader is not built"`.
**Verified current**: MODEL_HOME's statement (`MODEL_HOME…:19-21`) still holds at `19357c167`.
`data`, `pointer`, `combination` are all refused.

### 2.3 The machinery (Java, `core/src/main/java/com/legend/protocol/`)

| piece | size | role |
|---|---|---|
| `Protocol.java` | 3,194 lines | sealed, immutable records mirroring `PureModelContextData` element shapes, clean-room (`Protocol.java:3-27`); the parser's only output |
| `ProtocolEmitter.java` | 3,506 | records → engine JSON bytes |
| `ProtocolReader.java` | 418 | **lambda/value-spec JSON → records only** (`:57-86`); unknown `_type` refused (`:46-50`). No element/PMCD reading |
| `PureComposer.java` | 875 | lambda JSON → Pure text |
| `ProtocolUpgrade.java` | — | older protocol shapes brought current (`PureV1ApiTest.java:468`) |
| `SourceInfo` / `SourceInformation` | — | engine convention (1-based, inclusive end) on protocol nodes; `strip()` for `returnSourceInformation=false` |
| `model/FromProtocol.java` | 827 | records → compiler model; **drops positions on purpose** (`:24-27`) |

So a **PMCD reader** = JSON → `Protocol` records (the mirror of 3.5k lines of emitter) then the
existing `FromProtocol`; a **model composer** = records → text. Both are named as owed:
`UPSTREAM_ENDPOINTS_DESIGN…:94-97` ("The PMCD model READER (E2's mirror): the legend-studio work"),
`datacube/docs/ENGINE_API_CONTRACT.md:55` (P1), `MODEL_HOME…:95-96`.

### 2.4 Fidelity limits relevant to Studio

- **Comments** are skipped by the lexer (`lexer/Lexer.java:203-208`); PMCD has no comment slot
  (upstream's grammarToJson also drops them). Text → JSON → text would lose comments and formatting
  (and today JSON → text for models is impossible anyway).
- Engine-strict vs lite dialect: `PmcdParser` parses at `Dialect.LEGEND_ENGINE` (`PmcdParser.java:44-47`);
  compile paths parse at `Dialect.LEGEND_LITE` (`Compiler.java:49-52`). Lite accepts some forms the
  engine refuses (docs/DIALECT_LEVELS.md); a Studio must pick which level authoring uses.
- UTF-16 vs code-point column mismatch in error columns (h4 §1 `:33-38`), fix owed in W1.2(b).

### 2.5 TypeScript side

- `pure-protocol/` (plain TS, no deps): V1 lambda JSON builder/reader with exact numbers, `toJson`
  key order equal to lite's emitter; proven byte-identical via `test/twins.test.ts`
  (`pure-protocol/README.md:1-25`). Lambdas only, no PMCD element types.
- `query/src/model/pmcd.ts` (188 lines) + `graph.ts` (393): typed PMCD element subset (class, enum,
  association, mapping, runtime, dataSpace, service, function, profile) and an index (packages,
  inherited + association properties, mappings → classes, runtimes → mappings)
  (`QUERY_APP_DESIGN…:89-99`). Read-only; a Studio explorer can reuse it.

---

## 3. Model input surfaces

### 3.1 Every way a model enters lite today

| surface | form | multi-file? | code |
|---|---|---|---|
| server `pure/v1/*` | `PureModelContextText` only | no (one `code` string) | `PureV1Api.java:493-500` |
| server `/lsp` | per-URI text, parse only | yes but no cross-file resolution | §1 |
| server `/engine/diagram` | `{code}` text, parse only | no | `LegendHttpServer.java:301-342`, `DiagramService.java:78` |
| WASM planner (`wasm/src/main/java/planner/Wasm.java`) | text strings per call: `planJsonOrError(model, lambdaJson, runtime)`, `relationTypeJsonOrError`, `modelJsonOrError(text)` (:226), `lambdaJsonOrError`, `composeLambdaOrError`, `databaseFromCatalogOrError(catalogJson)` (:251, model generated from a DuckDB catalog), `warmModel(model)` (:331) | no | no compile/diagnostics export; no PMCD input |
| Query app | config `projects[]` with `{groupId, artifactId, versionId, title, models:[".pure" urls]}` (`query/demo/config*.json`, `query/src/app/context.ts:10-17`); files fetched and **concatenated with `\n`** into one text (`query/demo/main.ts:81-83`); PMCD via `grammarToJson/model` (server or WASM) → `ModelGraph` | concatenation only | GAV is a label (`gavOf`, `context.ts:54-56`); version `0.0.0`; no Depot |
| DataCube | `trades.pure` fetched (`datacube/demo/boot.ts:261`); upload → model generated from catalog; `pure-v1.ts` sends `{_type:'text', code}` (`datacube/src/pure-v1.ts:19,97,128`) | no | — |
| tests / corpus | `Compiler.parseSources(List<ModelSource>)`, `StressCorpus.LINKED_PROJECTS` reading `projects/` (`core/src/test/java/com/legend/integration/StressCorpus.java:8-61`) | yes | test-only |
| CLI | **none** in core (only `main` is `LegendHttpServer`); `scripts/projects/check.py` compiles projects via `tools/engine-runner` (legend-engine, not lite) | — | `scripts/corpus/run.py:32,123-124` |

### 3.2 Compiler entry points (`core/src/main/java/com/legend/Compiler.java`; AGENTS.md:123-137)

| entry | behaviour | used by a server route? |
|---|---|---|
| `parseModel(String)` `:49` | parse at LEGEND_LITE | `/lsp`, `/engine/diagram` |
| `compileModel(String)` `:87-105` | A→F STRICT, first error aborts; decorates with element `[line:col]` | every `pure/v1` compile/plan/execute |
| `parseSources(List<ModelSource>, sink, dialect)` `:139-217` | **multi-file module**: each source its own unit (own imports, own positions), merged; duplicate elements reported (first wins); optional per-file parse-wall sink (unparseable file excluded, not fatal) | no |
| `buildModule(ParsedModel)` `:425-433` | A→F **TOLERANT**: poison-don't-drop, returns `BuiltModule(context, walls: FQN→first error line)` | no |
| `compileModel(List<ModelSource>)` `:438-458` | strict multi-file; error prefixed with source name + `[line:col]` | no |
| `compileAllBodies(ctx)` `:1218-1257` | eager G over every user function body; never throws; walls keyed by overload signature | `compilation/compile` (first wall only) |
| `resultType`, `target`, `plan`, `execute*` | each compiles the model again | E5/E6/E8/E9 |

AGENTS.md:135-137: "`compileModel` and `buildModule` differ by one argument … If you want every error
rather than the first, you want `buildModule`." Walls are `Map<String,String>` keyed three ways
(element FQN, overload id, source name) holding only the **first line** of a message (h4 §1 `:28-31`).
Rule 0b.9 (ruled 2026-09-28): "Strict means: collect every diagnostic in one pass, poison the failed
unit, fail the build on any error" (`EXECUTION_PLAN…:130-131`).

### 3.3 Caching

- Only the **boot layer** (system metamodel + prelude) is cached: content-addressed `ContentStore(4)`
  keyed by the hash of the boot source (`Compiler.java:259-306`). It is the only `ContentStore` in
  core main (grep).
- **User models are never cached**, server or tab (`MODEL_HOME…:17-22`). `generatePlan` compiles the
  model at least twice (`Compiler.target` `:556-559` and `Compiler.plan`) and parses it to PMCD again
  for the connection (`PureV1Api.java:209-213,420`).
- WASM: `warmModel` compiles but keeps nothing (`Wasm.java:331-332`); ~10 ms/plan, ~550 ms cold
  start (`datacube/src/wasm-planner.ts:19-24`).
- Standing ruling: no caches for slowness before the algorithm is proven right (`EXECUTION_PLAN…:127-128`);
  MODEL_HOME finding 4 owes a parse+compile measurement of a large model before designing a cache.
  Known numbers: boot re-normalization was 5.7 ms of an 8 ms compile (`Compiler.java:262-268`);
  100K-model build 15.1 s (`EXECUTION_PLAN…:209` table).

---

## 4. Studio Lite

### 4.1 studio-lite repo (`/Users/neema/legend/studio-lite`)

React 19 + Vite + `@monaco-editor/react` + Cytoscape (`studio-lite/package.json`). Source is 4 files:
`App.tsx` 812 lines, `DiagramView.tsx` 554, `lspClient.ts` 243, CSS.

| feature | how | status against today's server |
|---|---|---|
| Pure model editor | one Monaco editor, **one document** `file:///query.pure` (`App.tsx:8`), Monarch tokenizer registered in `beforeMount` (`:501-513`) | `/lsp` still answers |
| diagnostics | debounced `didChange` → `/lsp` → `setModelMarkers` via `window.monaco` (`App.tsx:224-276,294-301`; `lspClient.ts:75-106,175-185`) | works (parse errors only) |
| query diagnostics | model + query concatenated into the same URI (`App.tsx:321-339`) | hack |
| "autocompletion" (README) | no completion provider is registered (grep: no `registerCompletionItemProvider`) | not implemented |
| Pure query execution | `POST /engine/execute {code}` (`lspClient.ts:111-119`) | **route deleted** (ruling `UPSTREAM_ENDPOINTS_DESIGN…:6`; no route in `LegendHttpServer.setupRoutes`) → 404 |
| Raw SQL | `POST /engine/sql` (`lspClient.ts:125-137`) | **deleted** for security (`LegendHttpServer.java:32` "The raw-SQL route /engine/sql is gone") |
| Ask AI (NLQ) | `POST /engine/nlq`, Gemini (`lspClient.ts:157-169`) | served by a separate `nlq` server not in this repo |
| Class diagram | `POST /engine/diagram` → Cytoscape (`DiagramView.tsx`) | works; non-upstream endpoint |
| health indicator | `GET /health` | works |

No file tree, no multi-file, no persistence (state is `useState` with a hard-coded `DEFAULT_MODEL`,
`App.tsx:11-187`), no projects, no save, no auth, README still documents Maven commands for a deleted
`engine` module. **Verdict: throwaway** as code; reusable as reference: the Monarch token rules, the
diagram view, and the marker plumbing. It is retired by ruling (`UPSTREAM_ENDPOINTS_DESIGN…:7`:
"studio-lite, their other client, is being retired; a real legend-studio of our own comes later").

### 4.2 Studio-related things inside legend-lite

- Server javadoc/banner still titled "Legend Studio Lite HTTP Server" / "Backend Ready"
  (`LegendHttpServer.java:16`, `:396`).
- `DiagramService` (293 lines): parse-based extraction of classes (stereotype, description, tags,
  properties), associations, generalisations (`DiagramService.java:15-40,78`); `DiagramServiceTest`
  (335 lines).
- No Studio app directory exists (apps are `query/`, `datacube/`, `pure-protocol/`).
- Older superseded plans for multi-file Studio editing/LSP lazy loading:
  `docs/TEAM_DEPENDENCY_PROPOSAL.md:636-641,813,886`, `docs/BAZEL_DEPENDENCY_PROPOSAL.md:2014,2178-2186`
  (the latter banner: "SUPERSEDED — 2026-08-06 … Do not act on it", `:1-9`). Ideas there
  (per-element files, `legend_library` Bazel rule, project-qualified FQNs) are history, not plan.

---

## 5. Storage and persistence already in lite

### 5.1 SavedQueries (`core/src/main/java/com/legend/server/SavedQueries.java`, 609 lines)

- Port of legend-engine's query store (`ApplicationQuery` + `QueryStoreManager` + versioned Mongo DAO),
  rules ported from source, not measured (`:19-33`).
- Endpoints: `POST query`, `POST query/search`, `GET query/{id}`, `GET query/batch?queryIds=`,
  `PUT query/{id}`, `PUT query/{id}/patchQuery`, `DELETE query/{id}`, `GET query/{id}/history[?version=]`
  (`:106-128`); `query/dataCube` → 404 (`:107-110`); `query/events`/`stats` not served (`:32`).
- **Format:** `--query-store DIR` (`LegendHttpServer.java:368-372`); one file per query,
  `URLEncoder(id)+".json"` (`:606-608`), holding a JSON array of **every version**; write is
  tmp-file + `ATOMIC_MOVE` (`:593-604`). Current version = the one with `validUntil == null`; update
  = new version, old one gets `validUntil`; delete stamps `deletedAt` + `validUntil`, history kept
  (`:261-330`). All methods `synchronized`.
- Record fields in engine order incl. `groupId, artifactId, versionId, originalVersionId,
  executionContext, content, taggedValues, stereotypes, defaultParameterValues, owner, gridConfig`
  (`:56-59`); validation of groupId (Java name) / artifactId (`^[a-z][a-z0-9_]*(-…)*$`) (`:47-48`).
  Execution contexts served: `explicitExecutionContext`, `dataSpaceExecutionContext` (`:66-78`).
- **Identity:** every caller is `"anonymous"` (`:91-93`); `server/v1/currentUser` answers the same
  (`LegendHttpServer.java:132-135`). Owner checks exist but are vacuous until sign-in.
- Self-described "interim home until the warehouse keeps queries" (`:27-29`; SERVER_PROGRAM decision 8).
- Tests: `SavedQueriesTest.java` (162 lines).

This is the **only server-side versioned-record store in lite** and the closest existing pattern to
SDLC/Depot storage: append-only versions, owner, immutable history, upstream-shaped API, directory
backend.

### 5.2 Browser stores

| store | where | content |
|---|---|---|
| IndexedDB `legend-query` / `queries` | `query/src/backend/local-store.ts:32-60` | same Query records and rules as SavedQueries, every version kept (`:1-6`) |
| IndexedDB `datacube` / `cubes` (+ `handles`) | `datacube/src/cube-store.ts:203-214` | upstream `DataCubeQuery` records, `content` = lite's own cube document; exact-JSON text (`:1-17`) |
| localStorage | `query/src/app/context.ts:63-96`, `query/src/ui/theme.ts` | recent data spaces/queries (10 each), theme |

DataCube save/share (`docs/DATACUBE_SAVE_SHARE_2026_09_28.md`): store is upstream's API/record,
content ours (decision 5, `:130-153`); a saved cube never stores data (8); **model versions are
pinned with an easy upgrade path** (7); local files first, "model-backed cubes follow the model home"
(9); next step "model-backed cubes on the model home's first slice (pointer, `demo:trades:1.0.0`)"
(`:300`). Share = a link encoding the saved page with a frozen, sha256-pinned dictionary (`:245-262`).

### 5.3 Warehouse (`warehouse/`)

Stores **data, entitlements, identity, query history, results — not models** (grep finds no model/
project storage; `MODEL_HOME…:23-24`). Catalogs = one DuckDB file per catalog (`Catalogs.java:15-36`);
routes `/sql/v1/login`, `/token/refresh`, `/statements`, `/sessions`, `/history`, `/catalogs`
(`WarehouseServer.java:215-267`); identity = HMAC-signed tokens, PBKDF2 passwords, principal only
from a verified token (`Identity.java:20-30`); owners/readers, SELECT grants (SERVER_PROGRAM §3,
`:210-220`). It is the recommended future home of the query store and model pointers
(SERVER_PROGRAM decisions 8-9, `:416-417`).

### 5.4 Anything resembling projects/versions/entities

- Query app config `projects[]` with GAV labels (§3.1) — static JSON, no versions list.
- `projects/` directory: **59 Legend model projects** (`projects/*/{model,store,mapping}.pure` +
  `MANIFEST.md`), naming/prefix contract and "may ONLY refer to projects listed as your
  dependencies" (`projects/CONTRACT.md:1-60`), dependency DAG declared in
  `scripts/projects/spec.py` (`(name, layer, [deps], …)`, `:12-33`), checked ALONE and TOGETHER by
  `scripts/projects/check.py` (`:1-17`) — via legend-engine's runner, not lite. Bazel exports them
  only as a filegroup for the stress corpus (`projects/BUILD.bazel`: "Each project becomes a
  legend_library target later"). **Ideal seed data for SDLC-lite/Depot-lite (projects, dependencies,
  versions to publish).**
- Nothing else: no entity store, no workspace, no version list, no publish path.

---

## 6. MODEL_HOME_2026_09_28.md — summary

**Status:** "This is the survey … and the design options. No product code yet." (`:5-6`)

**Rulings that bind (the user, 2026-09-28)** (`:8-10`), verbatim:
> "a saved cube PINS its model version, with an easy upgrade path; sources point to models, never copy
> them; upstream APIs and shapes exactly; everything through Bazel; no Java dependencies; identity
> always, the database enforces data access."

**§1 Survey of lite** (`:12-24`): WASM compiles only the platform boot layer; user model arrives as
text and is compiled on every plan. Server takes the whole model inline as text only; `data`,
`pointer`, `combination` refused; recompiled per request. No registry: no projects, versions,
publishing; warehouse holds data and entitlements, not models.

**§2 Upstream** (legend-engine 4.145.0, legend-studio c5b2f2c78) (`:26-79`):
- `PureModelContextPointer {_type:"pointer", serializer, sdlcInfo}` with `alloy` (GAV +
  `packageableElementPointers`, a published depot version), `workspace` (SDLC project + workspace),
  legacy `pure`. Four context kinds: `data`, `text`, `pointer`, `combination`. Absent/`none` version
  = `master-SNAPSHOT`.
- `ModelManager.loadModel` → `SDLCLoader`; alloy = one depot call under the caller's identity:
  `GET {depot}/projects/{g}/{a}/versions/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=…`.
  Workspace pointers go to SDLC. Caches keyed by pointer (fetched JSON and compiled `PureModel`),
  soft values, 30 min after last access, **release versions only** (SNAPSHOT refetched/recompiled).
- Depot client API (`DepotServerClient.ts`): projects, versions, version entities, one entity,
  dependencies (transitive), projectDependencies, dependantProjects, pureModelContextData,
  classifier search. Aliases: `latest` (newest release), `master-SNAPSHOT` (= Studio HEAD), `*SNAPSHOT`
  mutable, releases immutable. **Publishing is not a depot HTTP API** (SDLC/git + pipeline → depot).
- Pointer producers: saved queries store GAV + context; DataCube Legend Query source builds alloy
  pointers; function source requires one; Studio works on workspace pointers until release.

**§3 Findings** (`:81-95`): no model home/pointer is the real gap (not packaging); adopt upstream's
`alloy` pointer exactly; pinning matches upstream's cache boundary (release immutable → cacheable;
SNAPSHOT cannot be pinned); per-request compile is fine for the demo, **measurement owed** for large
models (JVM and WASM); lite reads text, writes PMCD (E2) but cannot read it (P1).

**§4 Options** (each with a recommendation, none ruled):

| id | question | options | recommendation |
|---|---|---|---|
| D-A | contexts on `pure/v1` | `pointer` (alloy) + `combination` beside `text`, resolved like `ModelManager`; `data` needs P1 | pointer + combination now; P1 reader as its own leg before milestone 2 (`:99-105`) |
| D-B | what the model home is | 1: serve **depot's READ API shape** (versions, pureModelContextData, entities, dependencies), stored in warehouse or disk; 2: a lite-only registry | **Option 1**: "READ API exactly depot's; storage is ours"; pointing at real depot later = URL change (`:107-115`) |
| D-C | publishing | lite needs its own: Bazel-run tool and/or owner-only HTTP endpoint; compiles, refuses non-compiling models, stores **immutably** (release not overwritable, SNAPSHOT is) — "ours by necessity; name it so" (`:117-122`) | — |
| D-D | what it stores | grammar text per version, serving depot JSON via E2; or entities JSON (needs P1) | store **text** (source of truth a person wrote), serve JSON by conversion; revisit when P1 lands (`:124-130`) |
| D-E | inter-project dependencies | needed with >1 project; depot's `pureModelContextData` dependency semantics unread (depot server source not checked out) | decide after reading (`:132-136`) |
| D-F | caching | server: compiled model per release pointer, content-addressed; tab: text fetched by pointer (HTTP-cacheable for releases), compiled once per page, planner stays I/O-free | after the measurement (`:138-143`) |
| D-G | versions in DataCube | `latest` saved as the concrete release; SNAPSHOT not pinned (refuse or warn — open); upgrade path in Load dialog with drift report | (`:145-151`) |
| D-H | identity | pointer resolution under the caller's identity; who may read which project is a decision (all signed-in users = upstream norm); data access stays with the DB | (`:153-157`) |

**§5 First slice proposed** (`:159-165`): alloy pointer on `pure/v1`; model home serving depot's
`versions` and `pureModelContextData` from stored text; `demo:trades:1.0.0` published by the tool;
demo page + DataCube name the model by pointer; a saved cube reopens in a fresh page.

**§6 Owed** (`:167-171`): compile-time measurement; depot dependency semantics; whether real depot's
read API needs auth.

Nothing in MODEL_HOME discusses **SDLC/workspaces** (authoring, review, git) — it covers only the
*published* side (Depot). Studio's workspace side is a design gap.

---

## 7. Constraints a Studio design must respect

| # | constraint | source |
|---|---|---|
| C1 | **Upstream APIs and shapes exactly**; lite's client surface is legend-engine's `pure/v1` and nothing of its own; made-up endpoints are deleted, not extended | `UPSTREAM_ENDPOINTS_DESIGN…:3-7`; `PureV1Api.java:24-28`; `LegendHttpServer.java:110-111` |
| C2 | Where a server is missing, a local implementation **of the same contract** (e.g. HTTP + IndexedDB query store, same records); swapping backends swaps an implementation, not a shape | `QUERY_APP_DESIGN…:48` (D2) |
| C3 | Model home = **depot's READ API shape** (recommended, D-B); publishing is ours by necessity, named as such (D-C) | `MODEL_HOME…:107-122` |
| C4 | Saved artefacts **pin model versions** with an easy upgrade path; **sources point to models, never copy them** | `MODEL_HOME…:8-9`; `DATACUBE_SAVE_SHARE…:145-147` |
| C5 | **Everything through Bazel**; every stage its own Bazel target with only the deps it should have | `MODEL_HOME…:9`; `EXECUTION_PLAN…:138-140` (0b.12) |
| C6 | **No Java dependencies** (core's only third-party jars are JDBC drivers: `maven_core_install.json` = h2, duckdb_jdbc, sqlite-jdbc; HTTP via `com.sun.net.httpserver`, own JSON lib) — so no JGit, Jackson, Jetty, LSP4J for an SDLC-lite/LSP | `MODEL_HOME…:9-10`; `core/BUILD.bazel:215-231`; `LegendHttpServer.java:18`; `PureLspServer.java:11` |
| C7 | **Identity always**, the user's own identity end to end; **no service accounts**; the database enforces data access; entitlements in the warehouse | `MODEL_HOME…:10`; `SERVER_PROGRAM…:13-24` (rulings 1-3) |
| C8 | Server doors: loopback bind by default, Origin allow-list, no `*` CORS (W0.1) | `LegendHttpServer.java:26-32,60-90,371-391` |
| C9 | **No fallbacks, no defaulting** (invariant 4); routing by configuration per operation, never fallback on failure | `AGENTS.md:270-278`; `QUERY_APP_DESIGN…:49` (D3) |
| C10 | **Total Knowledge, demand-driven Work** (eager parse/resolve/manifest; lazy body typing) — directly shapes IDE diagnostics vs compile-all | `docs/TENETS.md:6-55`; D10 `EXECUTION_PLAN…:337` |
| C11 | "Strict" = collect every diagnostic in one pass, poison the failed unit, fail on any error | `EXECUTION_PLAN…:130-131` (0b.9) |
| C12 | No caches/memos for slowness before the algorithm is proven right | `EXECUTION_PLAN…:127-128` (0b.8) |
| C13 | Lazy loading of cross-project user elements through `ModelContext.findClass/findEnum/findFunction`; long-lived fields hold FQN strings (unenforced in core) | `AGENTS.md:280-299` (invariant 5) |
| C14 | Reference checkouts (legend-pure/engine) are spec/test input only, never runtime | `AGENTS.md:37-51` |
| C15 | Queries/lambdas built as **protocol JSON, never as built Pure text**; text only for people via `jsonToGrammar` | `QUERY_APP_DESIGN…:51` (D5) |
| C16 | Front-end apps: plain TS + DOM, no framework, no new npm dep to start; esbuild, node test runner, jsdom, strict tsc (Query/DataCube idiom) — Monaco/React would be a departure needing a decision | `QUERY_APP_DESIGN…:52` (D6); `query/package.json` |
| C17 | WASM where it can, server where it must; the WASM planner stays I/O-free (page fetches, planner compiles) | `QUERY_APP_DESIGN…:49` (D3); `MODEL_HOME…:141-143` |
| C18 | Every deliberate difference from upstream is a `docs/SEMANTICS_REGISTER.md` row; parity proven by goldens captured from legend-engine 4.145.0 | `AGENTS.md:331-335`; `UPSTREAM_ENDPOINTS_DESIGN…:80-92` |
| C19 | Pinned upstream: legend-engine 4.145.0, legend-pure 5.99.0; Studio census pins legend-studio `c5b2f2c78` / `8dc8e4ce` / `821c74c` | `EXECUTION_PLAN…:59-61`; `MODEL_HOME…:26`; `QUERY_APP_DESIGN…:8,37` |
| C20 | The compiler rebuild program owns `core/` and the plan: diagnostics/LSP work is W1.2 in its sequence; one session owns the repo's rebuild branch | `EXECUTION_PLAN…:20-27,49-50,537-545,417,433` |
| C21 | Single-threaded HTTP dispatcher today; request-reachable mutable statics (incl. the LSP's map) must go per-request or be pinned single-threaded (W0.7) | `LegendHttpServer.java:345`; `EXECUTION_PLAN…:495-501` |

---

## 8. Gaps: what Studio + SDLC-lite + Depot-lite would need that lite lacks

### 8.1 Compiler / protocol

| gap | today | notes |
|---|---|---|
| **PMCD reader (P1)**: model JSON → records → compiler | absent; `data` refused (`PureV1Api.java:493-500`) | needed for upstream Studio clients, real depot answers, `combination`; mirror of 3.5k-line emitter |
| **`jsonToGrammar/model`** (model composer) | absent (`PureComposer` lambda-only) | needed for Studio's text↔form round trips and entity-JSON storage |
| **Structured diagnostics with spans** | strings with `[line:col]` in message; spans dropped at `FromProtocol` | W1.2(a-d) planned; `compilation/compile` must return `sourceInformation` for Studio markers |
| **All errors, not the first** | `compile` returns first wall; `buildModule` exists but unused by server | Studio needs the whole wall list per element/file |
| **Multi-file over the wire** | `pure/v1` takes one `code`; Query concatenates files | upstream Studio sends PMCD (entities); `parseSources` exists in Java only |
| **Incremental / cached compile** | per-request recompile; only boot cached | memoized query layer W2.2b is a later phase; MODEL_HOME measurement owed |
| **IDE semantic services** (completion, hover, go-to-definition, find references, document symbols, rename, formatting) | none; `ide/` dormant | upstream engine 4.145.0 serves no `completeCode`; upstream Studio does much client-side over its graph |
| **Element-level parser recovery** | first parse error stops a file | W1.2(d) |
| **Compile-all API returning per-element results** | `compileAllBodies` walls by overload signature | D10 "compile-all … a lane and an API" ruled, not built as an endpoint |
| **Pointer/combination contexts + loader** | refused | MODEL_HOME D-A |
| Type vocabulary parity (Int/Numeric/Timestamp) | lite names differ, recorded | the "untangle" |

### 8.2 SDLC-lite (authoring side) — nothing exists

- Projects, workspaces (user/group), entity CRUD (`performEntityChanges`), revisions/history, diff,
  conflict detection, reviews/approval, release creation, project configuration/dependencies,
  users. Upstream Query census lists the SDLC calls Studio's productionizer makes
  (`query/docs/UPSTREAM_QUERY_CENSUS.md:646-648`). MODEL_HOME does not cover SDLC at all.
- Storage engine with no Java deps (C6): no JGit → either a directory/append-only format (the
  SavedQueries pattern), the warehouse (DuckDB tables), or shelling out to `git` — undecided.
- Identity: lite server has only `anonymous`; warehouse has real tokens. Studio authorship/review
  needs signed-in users on the lite server (identity pass-through is SERVER_PROGRAM leg E1, not built).

### 8.3 Depot-lite (published side) — nothing exists

- Read API: `project-configurations`, `projects/{g}/{a}/versions`, `versions/{g}/{a}/latest`,
  `…/versions/{v}` entities, `/entities/{path}`, `/dependencies?transitive`, `pureModelContextData`,
  classifier searches (`UPSTREAM_QUERY_CENSUS.md:617-633`; `MODEL_HOME…:60-67`).
- Publish path (D-C): compile-gated, immutable releases, SNAPSHOT mutable; Bazel tool and/or
  owner-only endpoint.
- Version aliases (`latest`, `master-SNAPSHOT`, `HEAD`) resolution.
- Dependency semantics (D-E) unread; `projects/` gives a 59-project DAG to test against.
- Entities representation: StoredEntity `{groupId, artifactId, versionId, entity{path,
  classifierPath, content}}` — content is element JSON → needs P1 if stored as JSON, or E2 conversion
  if stored as text (D-D).

### 8.4 Front end

- No Studio app: no file/package explorer, multi-tab editor, element form editors, diagram editor,
  diff view, workspace/review UI, project picker. Reusable: `pure-protocol`, Query's `ModelGraph`,
  `WasmGrammar`/planner worker, theme/icons (legend-art tokens, `query/tools/icons.mjs`), DataCube
  for result views, studio-lite's Monarch tokens and Cytoscape diagram as references.
- Editor component decision: Query/DataCube rule (D6) is no framework/no new npm dep; a code editor
  (Monaco/CodeMirror) would be a new dependency to rule on.
- WASM: no compile/diagnostics export; to give in-tab diagnostics the WASM boundary needs a
  `compile`/`buildModule` entry (today only plan/type/grammar).

---

## 9. Not determined

- Real per-keystroke cost of a full compile for a large model (MODEL_HOME finding 4 still owed; no
  measurement found).
- Whether upstream Studio's own client calls (`grammarToJson/model` with multiple files,
  `compilation/compile` with PMCD + `sourceInformation`, `jsonToGrammar/model`) were censused anywhere
  in lite — only the Query and DataCube censuses exist (`query/docs/UPSTREAM_QUERY_CENSUS.md`,
  `datacube/docs/ENGINE_API_CONTRACT.md`); no Studio census was found.
- How `/engine/diagram` and `/lsp` square with the upstream-only ruling (both still routed; no doc
  rules on them since 2026-09-27).
- The `nlq` server studio-lite's Ask AI needs (an untracked `nlq/` directory is mentioned as "not
  ours" in `EXECUTION_PLAN…:49-50`; not inspected).
- Depot's dependency semantics and auth (MODEL_HOME §6) — depot server source not checked out.
