# legend-sdlc slice-2 behavioural contract: reviews, versions, revision lists (GitLab backend + resource layer + Studio usage)

> Read 2026-10-04 as a companion to `SDLC_CONTRACT_SLICE1.md`; §0 of slice 1 (JSON mapper, error envelope, `buildException`
> status translation, reference-info strings, project-id parsing) applies unchanged and is not repeated. JSON key orders
> below were checked with replica classes in jshell on jackson-databind 2.10.5.1 + jsr310 2.10.5 (JDK 25); as in slice 1
> they come from anonymous classes, so production order is JVM-dependent. Jersey / Dropwizard behaviours were read from
> `jersey-server-2.25.1-sources.jar` and `dropwizard-jersey-1.3.29-sources.jar` in `~/.m2`.

Source: `/Users/neema/legend/legend-lite-query/.scratch/legend-sdlc` @ `1021fda`; Studio:
`/Users/neema/legend/legend-lite-query/.scratch/legend-studio`. All paths below are relative to those roots.

Abbreviations (in addition to slice 1's `BGA`, `GAFA`, `GEA`, `GPCA`, `GRA`, `ERR/`, `RES/`):

| Abbrev | File |
|---|---|
| `GRVA` | `legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/gitlab/api/GitLabReviewApi.java` |
| `GVA` | `.../gitlab/api/GitLabVersionApi.java` |
| `GCA` | `.../gitlab/api/GitLabComparisonApi.java` |
| `RR` | `RES/review/project/ReviewsResource.java` |
| `RFR` | `RES/ReviewFilterResource.java` |
| `VR` | `RES/version/VersionsResource.java` |
| `CRR` | `RES/comparison/project/ComparisonReviewResource.java` |
| `CRER` | `RES/comparison/project/ComparisonReviewEntitiesResource.java` |
| `CRPCR` | `RES/comparison/project/ComparisonReviewProjectConfigurationResource.java` |
| `RAPI` | `legend-sdlc-backend-api/src/main/java/org/finos/legend/sdlc/backend/api/review/ReviewApi.java` |
| `VAPI` | `legend-sdlc-backend-api/.../backend/api/version/VersionApi.java` |
| `VID` | `legend-sdlc-model/src/main/java/org/finos/legend/sdlc/domain/model/version/VersionId.java` |
| `WSPEC` | `legend-sdlc-project-files/src/main/java/org/finos/legend/sdlc/project/workspace/WorkspaceSpecification.java` |
| `BS` | `legend-sdlc-server-shared/src/main/java/org/finos/legend/sdlc/server/BaseServer.java` |
| `TIME/` | `legend-sdlc-server-shared/src/main/java/org/finos/legend/sdlc/server/time/` |
| `SC` | (studio) `packages/legend-server-sdlc/src/SDLCServerClient.ts` |
| `NU` | (studio) `packages/legend-shared/src/network/NetworkUtils.ts` |
| `ST/` | (studio) `packages/legend-application-studio/src/` |
| `SM/` | (studio) `packages/legend-server-sdlc/src/models/` |

---

## 0. Cross-cutting additions for this slice

### 0.1 Query-parameter conversion errors (new types in this slice)
* `since` / `until` are `StartInstant` / `EndInstant`, converted by `BS:247-363` (`TemporalConverterProvider`, registered `BS:119`).
  * Grammar (`TIME/ResolvedInstant.java:92-120`): `YYYY[-M[-D[THH[:mm[:ss[.fraction(0-9 digits)]]]]]][zone-or-offset]`, lenient, year 4-10 digits; zone optional (`Z`, `+01:00`, or a region id). No zone → **UTC** (`:179`).
  * Missing fields resolve to the start (`StartInstant`: month 1, day 1, 00:00:00.000000000, `TIME/StartInstant.java:22-65`) or end (`EndInstant`: month 12, last day, 23:59:59.999999999, `TIME/EndInstant.java:23-66`) of the given period. So `until=2026-10` means `2026-10-31T23:59:59.999999999Z`.
  * Unparseable value → the converter throws a `ProcessingException` (`BS:298-306`) which Jersey re-throws unwrapped (jersey-server 2.25.1 `SingleValueExtractor.extract`: `catch (WebApplicationException | ProcessingException ex) { throw ex; }`) → `CatchAllExceptionMapper` → **500** with message `Could not convert "<v>": Could not parse "<v>"` (separator `": "` from `StringTools.appendThrowableMessageIfPresent`). (Unlike `Integer` params, which give 404 per slice 1 §0.2.)
* Enum query params (`state`, `workspaceTypes`) use Dropwizard's `FuzzyEnumParamConverterProvider` (registered by `DropwizardResourceConfig`, dropwizard-jersey 1.3.29): case-insensitive, `-`/space → `_`, empty string → null. Unknown value → `WebApplicationException(400)`; legend's `CatchAllExceptionMapper` keeps the status but replaces the entity (slice 1 §0.2) → **400** `{"code":400,"message":"HTTP 400 Bad Request","timestamp":…}` (the Dropwizard text `state must be one of [OPEN, COMMITTED, CLOSED, UNKNOWN]` is lost).
* Repeated params (`revisionIds`, `workspaceTypes`) are collected into a `Set` (order not significant; duplicates collapse).

### 0.2 Null return → 204
Any resource method returning `null` makes Jersey send **204 No Content** with an empty body. This matters for `GET /versions/latest` (no versions), `GET /reviews/{id}/approval` (GitLab `approved_by` null) and, in theory, `fromGitLabMergeRequest` returning null. Studio's fetch layer maps 204 to `undefined` (`NU:287-288`).

### 0.3 Review id
* Review id = GitLab merge-request **IID** (project-scoped integer) as a string, e.g. `"12"` (`GRVA:980` `toStringIfNotNull(reviewId)` of `mergeRequest.getIid()`).
* Parsing: `Long.parseLong`; failure → **400 `Invalid id: <id>`** (`BGA:836-846`). Done before any GitLab call.
* "Is this MR a review?" (`BGA:1153-1165`): the MR's **source branch** must parse as a workspace branch (`BGA:296-387`, pattern `BGA:109`) with access type `WORKSPACE` (i.e. `workspace/<user>/<id>` or `group/<id>`, optionally prefixed `patch/<x.y.z>/`), **and** its target branch must equal that workspace's source branch (project default branch, or `patch/main/<x.y.z>` for patch workspaces). Otherwise the MR is invisible: lists skip it, by-id lookups 404.
* Fetch by id (`BGA:1130-1151`): GitLab 404 → **404 `Unknown review in project <P>: <id>`**; 403 → `User <u> is not allowed to access review <id> in project <P>: <glmsg>`; other → 500 `Error accessing review <id> in project <P>: <glmsg>`; MR exists but is not a review → **404 `Unknown review in project <P>: <id>`** (`BGA:1146-1149`). `<P>` here is `GitLabProjectId.toString()` (normalized `prefix-N`).

### 0.4 Review state mapping — `BGA:1205-1248`, `RAPI`, enum `legend-sdlc-model/.../review/ReviewState.java:17-20`
GitLab MR `state` (case-insensitive) → `ReviewState`: `opened`→`OPEN`, `merged`→`COMMITTED`, `closed`→`CLOSED`, anything else (incl. `locked`, null) → `UNKNOWN`. Filter direction (`GRVA:925-954`): `OPEN`→GitLab `opened`, `COMMITTED`→`merged`, `CLOSED`→`closed`, `UNKNOWN` or null → `all`.

State-precondition failures (`GRVA:872-879`) → **409** `Review is not <expected> (state: <actual>)`, both lower-cased enum names, e.g. `Review is not open (state: committed)`, `Review is not closed (state: open)`.

### 0.5 Review JSON — interface `legend-sdlc-model/.../review/Review.java:23-59`, impl `GRVA:956-1077`
Fields (all always present, nulls written): `id` (string IID), `projectId` (the **projectId string from the URL**, not re-normalized, `GRVA:335,369`), `workspaceId`, `workspaceType` (`USER`|`GROUP`), `title`, `description`, `createdAt`, `lastUpdatedAt`, `closedAt`, `committedAt` (Instants from GitLab `created_at`/`updated_at`/`closed_at`/`merged_at`, ISO-8601 `…Z`, ms precision when non-zero), `state`, `author` (`User` `{name, userId}` = GitLab MR author display name / username, `BGA:1034-1057`), `commitRevisionId` (GitLab `merge_commit_sha`; null until merged, and null for fast-forward merges), `webURL` (GitLab MR `web_url`; key is exactly `webURL`, verified), `labels` (GitLab labels list).
* Observed order (JDK 25 replica): `{"id","state","commitRevisionId","projectId","workspaceId","workspaceType","title","description","createdAt","lastUpdatedAt","closedAt","committedAt","author":{"name","userId"},"webURL","labels"}` — JVM-dependent; **lite should emit interface order** `id, projectId, workspaceId, workspaceType, title, description, createdAt, lastUpdatedAt, closedAt, committedAt, state, author, commitRevisionId, webURL, labels`.
* `workspaceId`/`workspaceType` come from parsing the MR source branch; the workspace owner (`<user>` in `workspace/<user>/<id>`) is **not exposed** — `author` is the MR author.

---

## 1. Reviews

All routes under `@Path("/projects/{projectId}/reviews")` (`RR:48`). `ReviewApi` is bound to `GitLabReviewApi`. FS backend: list → always `[]`, every other review call → 500 `Feature unavailable` (`legend-sdlc-server-fs/.../api/review/FileSystemReviewApi.java:41-121`, `exception/FSException.java:21-24`).

### 1.1 `GET /projects/{p}/reviews` — `RR:62-77` → `RAPI:233-237` (deprecated default adds `sources = {project workspace source}`) → `GRVA:87-165`

Query params:
* `state` (`ReviewState`, fuzzy enum): see 0.4.
* `revisionIds` (repeatable): non-empty → switches the **fetch strategy** (below).
* `workspaceIdRegex` (string): compiled `CASE_INSENSITIVE`; invalid regex → `Pattern.LITERAL|CASE_INSENSITIVE`; matched with `find()` (substring) against `review.workspaceId` (`RFR:25-45`).
* `workspaceTypes` (repeatable `USER`/`GROUP`): `review.workspaceType` must be in the set; empty/absent → no filter (`RFR:29,42-44`). Regex and types are ANDed.
* `since` / `until` (`StartInstant`/`EndInstant`, §0.1): inclusive bounds (`GRVA:920-923`).
* `limit` (Integer): null or `<= 0` → no limit (`GRVA:261-264`; here the Swagger text is right). Unparseable → 404.

Order of operations (`GRVA:104-155`, `:240-243`):
1. `parseProjectId` (400 `Invalid project id: "<p>"`, re-thrown unchanged by the outer catch).
2. Resolve the target-branch set = `{default branch}` via `getDefaultBranch` (single-message `buildException`, slice 1 §0.4): **missing project → 404 `Error getting default branch for <P>`** (`GRVA:108`, `BGA:463-474`).
3. Fetch MRs:
   * **No `revisionIds`** (`GRVA:138-147`): one GitLab `GET /projects/:id/merge_requests` with `state` (mapped per 0.4), `target_branch=<default>`, and time pre-filters (`GRVA:198-228`): `since` is pushed down **only when `state` is given** — `OPEN` → `created_after=since`; `CLOSED`/`COMMITTED` → `updated_after=since`; `UNKNOWN` → nothing. `until` (any state) → `created_before=until`. GitLab order = `created_at desc` (GitLab default; not re-sorted).
   * **With `revisionIds`** (`GRVA:109-137`): for each id (Set iteration order) GitLab `GET /projects/:id/repository/commits/:sha/merge_requests` (the MRs whose diff contains that commit); concatenated, de-duplicated by IID (first occurrence kept), then filtered by GitLab state string (`equalsIgnoreCase`) unless state maps to `all`. Nothing is pushed down for since/until. Revision ids are **not alias-resolved** (raw text sent to GitLab). Per-revision errors: 403 `User <u> is not allowed to get reviews associated with revision <r> for project <p>: <glmsg>`; 404 **`Unknown revision (<r>) or project (<p>)`**; other 500 `Error getting reviews associated with revision <r> for project <p>: <glmsg>` (`GRVA:125-128`) — raised lazily during collection and passed through unchanged.
4. Keep MRs whose target branch is the default branch (`GRVA:148-151`) and that are reviews (§0.3) (`GRVA:154`); convert (§0.5).
5. In-memory filters, **in this order** (`GRVA:240-243`): state (`review.state == state`), time, **limit**, then workspace id/type predicate. ⇒ **`limit` is applied before the workspace filter** (quirk 1).
6. Time predicate (`GRVA:266-324`), all bounds inclusive, a null timestamp never matches:
   * `state=OPEN`: `createdAt ∈ [since, until]` **or** `lastUpdatedAt ∈ [since, until]`.
   * `state=CLOSED`: `closedAt ∈ …` or `lastUpdatedAt ∈ …`.
   * `state=COMMITTED`: `committedAt ∈ …` or `lastUpdatedAt ∈ …`.
   * `state` null/`UNKNOWN`: `lastUpdatedAt ∈ …`, else by the review's own state: OPEN/UNKNOWN → `createdAt`, COMMITTED → `committedAt`, CLOSED → `closedAt`.
   (Combined with the GitLab pre-filter `created_before=until`, a review created after `until` is never returned even if committed/updated in range.)
* Errors from the outer catch (`GRVA:157-163`): 403 `User <u> is not allowed to get reviews for project <p>[ with state <STATE>]: <glmsg>`; 404 `Unknown project (<p>)`; else 500 `Error getting reviews for project <p>[ with state <STATE>]: <glmsg>`. (`<STATE>` = enum name, upper case.) In practice a missing project hits step 2 first.
* Response: 200 JSON array of Review (§0.5); `[]` when nothing matches.

### 1.2 `GET /projects/{p}/reviews/{reviewId}` — `RR:79-88` → `GRVA:326-344`
* `parseProjectId` 400; id parse 400 `Invalid id: <id>`; then §0.3 lookup: **404 `Unknown review in project <P>: <id>`** for a missing MR or a non-review MR. (The outer suppliers `Unknown review (<id>) or project (<p>)` etc. at `GRVA:339-342` are only reached for non-LegendSDLC exceptions; GitLab errors are already translated inside `getReviewMergeRequest`.)
* 200 Review, any state.

### 1.3 `POST /projects/{p}/reviews` (CreateReviewCommand) — `RR:112-122` → `RAPI:273-277` → `GRVA:346-378`
Body (`legend-sdlc-server/.../application/review/CreateReviewCommand.java:21-78`): `workspaceId`, `workspaceType` (fuzzy enum, default `USER` when null, `RR:120`), `title`, `description`, `labels` (list). Unknown fields → 400 `Unable to process JSON`.

Validation / execution order:
1. Body null → **400 `Input required to create review`** (`RR:116`).
2. `workspaceId` null → `WorkspaceSpecification` constructor `Objects.requireNonNull(id, "id may not be null")` → NPE → **500 `id may not be null`** (`RAPI:276`, `WSPEC:38`; CatchAll default response). (Happens before the title check.)
3. `title` null → **400 `title may not be null`** (`GRVA:351`). Empty title allowed here (GitLab may reject).
4. `description` null → **400 `description may not be null`** (`GRVA:352`). Empty allowed.
5. `parseProjectId` → 400.
6. Source branch name = `workspace/<CURRENT USER>/<workspaceId>` (or `group/<id>`) (`GRVA:356`, `BGA:541-557`): a user can only submit **their own** user workspace.
7. Target = default branch (`GRVA:357`): missing project → **404 `Error getting default branch for <P>`** (outside the try).
8. Target branch existence check (`GitLabApiTools.branchExists`) false → **409 `Review target does not exist: project <p>`** (`GRVA:362-366`; reference info of the workspace *source*, `BGA:741-744,761-799`).
9. GitLab create MR (`GRVA:368`): source = workspace branch, target = default branch, title, description, no assignee, labels (`null` when empty/absent), **`remove_source_branch=true`**.
10. Response: **200** Review of the new MR (`state` `OPEN`, `author` = current user).
* GitLab failures (`GRVA:371-377`): 403 → **403** `User <u> is not allowed to submit changes from user workspace <w> of project <p> for review: <glmsg>`; 404 → **404 `Unknown: user workspace <w> of project <p>`**; anything else → **500** `Error submitting changes from user workspace <w> of project <p> for review: <glmsg>`.
* **Not checked by legend-sdlc** (GitLab decides):
  * that the workspace branch exists — GitLab rejects a missing source branch with its "Source branch … does not exist" validation error (non-403/404 → the 500 above);
  * that the workspace has changes — GitLab accepts an MR whose source equals the target (empty diff); committing it later produces an empty merge commit;
  * **an already-open review for the same workspace** — no legend check; GitLab refuses a second open MR from the same source branch ("Another open merge request already exists for this source branch: !<iid>", HTTP 409 on GitLab) → legend **500** `Error submitting changes from user workspace <w> of project <p> for review: <glmsg>` (never 409).
  * project dependencies — checked only at commit (1.9).

### 1.4 `POST …/{reviewId}/close` — `RR:124-133` → `GRVA:380-402`
Order: `parseProjectId` 400 → id 400 → §0.3 lookup 404 → state must be `OPEN` else **409 `Review is not open (state: <s>)`** → GitLab `PUT merge_requests/:iid` `state_event=close`. Errors: 403 `User <u> is not allowed to close review <id> in project <p>: <glmsg>`; 404 `Unknown review in project <p>: <id>`; 500 `Error closing review <id> in project <p>: <glmsg>`. 200 → Review from GitLab's response (state `CLOSED`, `closedAt` set). Workspace branch untouched.

### 1.5 `POST …/{reviewId}/reopen` — `RR:135-144` → `GRVA:404-426`
Same as close but requires `CLOSED` (**409 `Review is not closed (state: <s>)`**) and sends `state_event=reopen`. Messages: `…is not allowed to reopen review…`, `Unknown review in project <p>: <id>`, `Error reopening review <id> in project <p>: <glmsg>`. (A committed review can never be reopened; a closed review whose workspace branch was deleted is GitLab's problem.)

### 1.6 `POST …/{reviewId}/reject` — `RR:168-177` → `GRVA:514-536`
**Identical to close** (requires `OPEN`, 409 `Review is not open (state: <s>)`, `state_event=close`), only messages differ: `User <u> is not allowed to reject review <id> in project <p>: <glmsg>`, `Unknown review in project <p>: <id>`, `Error rejecting review <id> in project <p>: <glmsg>`. Result state `CLOSED`.

### 1.7 `POST …/{reviewId}/approve` — `RR:146-155` → `GRVA:428-485`
* `parseProjectId` 400 → id 400 → §0.3 lookup 404. **No state check** (an approval of a non-open MR is up to GitLab).
* GitLab `POST merge_requests/:iid/approve` with `sha = MR head sha` (`GRVA:439`).
* Errors (`GRVA:446-484`):
  * GitLab **401 or 403** (401 = not authenticated *or* not an eligible approver, e.g. already approved / author approval disallowed) → **403 `User <u> is not allowed to approve review <id> in project <p>`** — exactly that; the richer message with `(see <webUrl> for more details): <glmsg>` is built but **discarded** (`GRVA:454-461`, quirk).
  * 404 → **404 `Unknown review in project <p>: <id>`**.
  * other (e.g. GitLab 409 SHA mismatch) → **500 `Error approving review <id> in project <p>: <glmsg>`**.
* 200 → the Review as fetched **before** approving, with only `lastUpdatedAt` replaced from the approve response (`GRVA:440-444`); state unchanged (`OPEN`). Approvals are not part of the Review JSON.

### 1.8 `POST …/{reviewId}/revokeApproval` — `RR:157-166` → `GRVA:487-512`
Lookup as above, no state check, GitLab `POST …/unapprove`. Errors: 403 `User <u> is not allowed to revoke approval of review <id> in project <p>: <glmsg>`; **404 `Unknown review in project <p>: <id>`** (GitLab 404s when the user had not approved); 500 `Error revoking review approval <id> in project <p>: <glmsg>`. 200 → pre-fetched Review with refreshed `lastUpdatedAt`.

### 1.9 `POST …/{reviewId}/commit` (CommitReviewCommand `{message}`) — `RR:190-200` → `GRVA:558-665`
Order:
1. Body null → **400 `Input required to commit review`** (`RR:195`).
2. `message` null → **400 `message may not be null`** (`GRVA:563`) — before project-id parsing. Empty string allowed.
3. `parseProjectId` 400 → id 400 → §0.3 lookup 404.
4. State must be `OPEN` → else **409 `Review is not open (state: <s>)`** (`GRVA:574`) (re-committing a committed review → `(state: committed)`).
5. `approvals_left` from the MR > 0 → **409 `Review <id> in project <p> still requires <n> approvals`** (`GRVA:577-581`). (GitLab only populates `approvals_left` on the single-MR payload in some editions; when null the check is skipped.)
6. Source branch unparseable → 500 `Error committing review <id> in project <p>: could not find workspace information` (`GRVA:584-588`, unreachable after step 3).
7. Read `project.json` at the **workspace branch HEAD** (`GRVA:589`; default config if absent) and reject improper project dependencies (`GRVA:769-783`): each dependency must have a non-blank `projectId` and a strict version `\d+.\d+.\d+` (no leading zeros) (`legend-sdlc-project-structure/.../ProjectStructure.java:94,381-394`). Violations → **409** `Cannot create a review with the following dependencies: <d1>, <d2>` where `<d>` = `<SimpleProjectDependency <projectId>:<versionId>>` (`Dependency.java:22-25`). (Message says "create" although it is the commit path; this is how snapshot dependencies are blocked.)
8. GitLab `PUT merge_requests/:iid/merge` with `merge_commit_message=<message>`, **`should_remove_source_branch=true`**, no `merge_when_pipeline_succeeds`, no `sha` (`GRVA:597`).
9. **200** → Review from GitLab's merge response (`state` `COMMITTED`, `committedAt` = `merged_at`, `commitRevisionId` = `merge_commit_sha`).

What the commit does to git:
* Merge strategy is **GitLab's project setting** (`merge_method`): legend passes no squash/ff option. With GitLab's default "merge commit" method, one merge commit is created on the default branch with **message = exactly the `message` from the body** (Studio sends `"<review title> [review]"`); with "fast-forward" no merge commit exists (`commitRevisionId` null) and the message is unused; squash only if the MR/project has squash on.
* Projects created by legend protect the default branch (push NONE, merge MAINTAINER) (slice 1 §2), so only merging via MR lands changes.
* **The workspace branch is deleted** by GitLab (`should_remove_source_branch=true`, also `remove_source_branch=true` at create). The `backup/…` and `resolution/…` branches of that workspace are **not** touched. No new workspace is created.
* No "outdated" requirement: an out-of-date review merges if GitLab can merge it (conflict → 406 below).

GitLab errors (`GRVA:599-664`; `(see <url> …)` present only when the MR has a `web_url`; `: <glmsg>` appended when GitLab gave a message):
* 401/403 → **403** `User <u> is not allowed to commit changes from review <id> in project <p> (see <url> for more details): <glmsg>`.
* 404 → **404** `Unknown review in project <p>: <id>`.
* 405 (draft/WIP, closed, pipeline pending/failed while required) → **409** `Review <id> in project <p> is not in a committable state (see <url> for more details): <glmsg>`.
* 406 (merge conflict) → **409** `Could not commit review <id> in project <p> because of a conflict (see <url> for more details): <glmsg>`.
* other → **500** `Error committing changes from review <id> to project <p>: <glmsg>` (note "**to** project").

### 1.10 `GET …/{reviewId}/approval` — `RR:179-188` → `GRVA:538-556`, `BGA:1167-1183`
* `parseProjectId` 400, id 400; GitLab `GET merge_requests/:iid/approvals`. **No "is a review" check** (any MR IID works).
* Errors: 403 `User <u> is not allowed to get approval details for review <id> in project <p>: <glmsg>`; 404 **`Unknown review in project <p>: <id>`**; 500 `Error getting approval details for review <id> in project <p>: <glmsg>` (inner `BGA:1177-1180`; the outer `GRVA:551-554` suppliers are effectively shadowed).
* 200 `{"approvedBy":[{"name","userId"}, …]}` (interface `legend-sdlc-model/.../review/Approval.java:21-24`); `[]` when nobody approved; **204** if GitLab omits `approved_by` (§0.2).

### 1.11 `GET …/{reviewId}/outdated` — `RR:90-110` → `GRVA:667-681,785-870`
* Lookup (404 / 400 as §0.3). State must be `opened` or `locked`, else **409 `Cannot get update status for review <iid> in project <p>: state is <STATE>`** (`GRVA:676-679`; `<STATE>` upper-case enum, e.g. `COMMITTED`; `locked` reports `UNKNOWN`).
* `updateInProgress` = MR `rebase_in_progress` (never requested here, so effectively `false`) → if true return `false`.
* base = MR `diff_refs.base_sha`, else GitLab merge-base(source, target) (`GRVA:813-846`; failure → 500 `Error getting base revision for review <iid> for project <P>[: <msg>]`); target = target-branch HEAD (`GRVA:848-870`; failure → 500 `Error getting target revision for review <iid> for project <P>[: <msg>]`, or `… for project` with **no id** when the head commit is null, `GRVA:867`).
* Body: bare boolean `base != null && target != null && base != target` (`RR:105-107`).
* Related (not used by Studio): `GET …/{id}/updateStatus` → `{updateInProgress, baseRevisionId, targetRevisionId}` (`RR:202-211`); `POST …/{id}/update` rebases (`GRVA:683-729`; 409 `Only open reviews can be updated: state of review <iid> in project <p> is <STATE>`); `POST …/{id}/edit` (`RR:224-235`, `GRVA:731-767`; null body → 400 `Input required to create review` (sic); non-open → **500** `Only open reviews can be edited: state of review <iid> in project <P> is <STATE>` — no status given).

### 1.12 Comparison — `GET …/{reviewId}/comparison` (+ `/projectLatest`, `/workspaceCreation`)
`CRR:45-75` → `GCA:154-213`.
* `/comparison` and `/comparison/projectLatest` are the same (`getReviewComparison`): from = MR `diff_refs.start_sha` (target-branch HEAD the MR diff was computed against), to = `diff_refs.head_sha` (workspace HEAD). **No state check** (works for CLOSED too).
* `/comparison/workspaceCreation`: from = `diff_refs.base_sha` (merge base), to = `head_sha`.
* Errors: lookup 400/404 (§0.3); missing sha → **500 `Unable to get revision info for review <id> in project <p>`** (`GCA:173-176,203-206`); GitLab compare errors (`GCA:228-231`, note missing spaces): 403 `User <u> is not allowed to get Comparison Information from revision <a>  to revision <b> on project<P>: <glmsg>` (two spaces before "to", none after "project"), 404 `Could not find revisions <a> ,<b> on project<P>`, 500 `Failed to fetch Comparison Information from revision <a>  to revision <b> on project<P>: <glmsg>`; head mismatch → 500 `Unexpected Comparison Result: toRevisionId does not match expected. Expected: <b>, Actual: <c>` (`GCA:242-245`).
* Body (`legend-sdlc-model/.../comparison/Comparison.java:19-50`, built `legend-sdlc-core/.../comparison/ComparisonOperations.java:80-176`): `{"toRevisionId","fromRevisionId","entityDiffs":[{"entityChangeType","newPath","oldPath"}],"projectConfigurationUpdated":<bool>}` (anonymous classes; emit interface order). Built from GitLab file diffs (`compare?straight=true`, `GCA:223`): a diff touching `/project.json` only sets `projectConfigurationUpdated=true`; a diff whose old or new file lies in an entity source directory becomes an EntityDiff with type `DELETE` (deleted_file) / `CREATE` (new_file) / `RENAME` (renamed_file) / else `MODIFY`; paths converted to entity paths (`model::A`), or left as the raw file path when the file is outside any entity dir on that side; other files ignored. For DELETE GitLab reports `old_path == new_path`, so `newPath` equals `oldPath` (Studio nulls it). List order = GitLab diff order.

### 1.13 `GET …/{reviewId}/comparison/{from|to}/entities[/{entityPath}]` and `/{from|to}/configuration`
`CRER:51-125`, `CRPCR:46-66` → `GEA:59-126`, `GPCA:89-134`.
* Lookup (400/404) then state must be OPEN or COMMITTED, else **500** `Current operation not supported for review state <STATE> on review <iid>` (`GEA:119-126`, `GPCA:127-134`; no status ⇒ 500, e.g. for CLOSED).
* `from` reads at `diff_refs.start_sha`, `to` at `head_sha` (literal sha, no reachability check — slice 1 §5). Missing sha → 500 `Unable to get [from] revision info in project <p> for review <id>` (**also used for the `to` entities case**, `GEA:95-98`); configuration `to` → `Unable to get [to] revision info …` (`GPCA:118-121`).
* Entity list: same filter params and semantics as slice 1 §5 (`classifierPath`, `package`, `includeSubPackages`=true, `name`, `stereotype`, `taggedValue`, `excludeInvalid`=false).
* Single entity missing → **404 `Unknown entity <path> for review <id> of project <p>`** (`GEA:78-81,146-153` + `EAO:77-82` with the overridden info string).
* Configuration: `project.json` at that sha, default config (v0, MANAGED) if absent (slice 1 §7).

### 1.14 Review state machine (summary)

| From \ action | close | reject | reopen | approve / revokeApproval | commit | comparison entities |
|---|---|---|---|---|---|---|
| OPEN | → CLOSED | → CLOSED | 409 `Review is not closed (state: open)` | allowed (GitLab rules) | → COMMITTED (if mergeable) | ok |
| CLOSED | 409 `Review is not open (state: closed)` | 409 same | → OPEN | GitLab decides | 409 `Review is not open (state: closed)` | 500 `Current operation not supported…` |
| COMMITTED | 409 `…(state: committed)` | 409 | 409 `Review is not closed (state: committed)` | GitLab decides | 409 `…(state: committed)` | ok |
| UNKNOWN (locked) | 409 `…(state: unknown)` | 409 | 409 | GitLab decides | 409 | 500 |

---

## 2. Versions

`VersionApi` → `GitLabVersionApi`. Versions are **git tags** named `release-<major>.<minor>.<patch>` (`BGA:122,654-657`); a tag counts only if the remainder is a strict version string (`BGA:659-664`, `VID:185-267`: three dot-separated decimal numbers, no leading zeros except `0`, each ≤ 2147483647). FS backend: list `[]`, everything else 500 `Feature unavailable` (`legend-sdlc-server-fs/.../api/version/FileSystemVersionApi.java`).

### 2.1 Version JSON — interface `legend-sdlc-model/.../version/Version.java:17-26`, impl `BGA:1297-1333`
`{"id":{"majorVersion":1,"minorVersion":2,"patchVersion":3},"projectId":"PROD-1","revisionId":"<tagged commit sha>","notes":<GitLab release description or null>}` — `id` is an **object**, not a string (`VID` getters; `toVersionIdString` is not a bean getter). `projectId` = normalized `GitLabProjectId.toString()` (list/latest/create) or the URL string (`GET /versions/{v}`, `BGA:1347`). Observed order: `{"id","revisionId","notes","projectId"}` (JVM-dependent; emit interface order `id, projectId, revisionId, notes`).

### 2.2 Version-id string parsing — `VID:123-183`
`parseVersionId(s)`: null → `Invalid version string: null`; otherwise any structural or number failure → `Invalid version string: "<s>"` (e.g. `"1.0"`, `"1.0.0.0"`, `"01.0.0"`, `"latest"`).

### 2.3 `POST /projects/{p}/versions` (CreateVersionCommand) — `VR:140-158` → `GVA:69-101` → `BGA:1358-1415`
Body (`legend-sdlc-server/.../application/version/CreateVersionCommand.java:19-54`): `versionType` (`MAJOR`|`MINOR`|`PATCH`, fuzzy), `revisionId` (optional), `notes` (optional).

Order:
1. Body null → **400 `Input required to create version`** (`VR:144`).
2. `features.canCreateVersion` false → **405 `Server does not support creating project version(s)`** (`VR:145-148`). Default when the `features` config section is absent: false (slice 1 §1) ⇒ **405 by default**.
3. Read project configuration at the default-branch HEAD (`VR:149`): invalid project id → 400; `project.json` absent or project missing → default config (MANAGED) → continues; `projectType == EMBEDDED` → **409 `Creating a version of a project of type EMBEDDED is not allowed`** (`VR:150-153`). (Not inside `execute`, so not logged/metric'd.)
4. `versionType` null → `command.getVersionType().name()` in the log-description argument throws NPE (`VR:155`) → **500** (message = JVM helpful-NPE text, e.g. `Cannot invoke "…NewVersionType.name()" because the return value of "…CreateVersionCommand.getVersionType()" is null`, or no `message` key on JVMs without helpful NPEs). The 400 `type may not be null` at `GVA:73` is unreachable.
5. Latest version = max `VersionId` over all version tags (`GVA:75,208-216`); none → `0.0.0` (`GVA:38,76`). GitLab errors listing tags: 403 `User <u> is not allowed to get versions for project <P>: <glmsg>`; **404 `Unknown project: <P>`**; 500 `Error getting versions for project <P>: <glmsg>` (`GVA:199-205`).
6. Next version (`GVA:78-99`, `VID:108-121`): `MAJOR` → `(M+1).0.0`, `MINOR` → `M.(m+1).0`, `PATCH` → `M.m.(p+1)`. First version of a project: MAJOR `1.0.0`, MINOR `0.1.0`, PATCH `0.0.1`. Always computed from the **global latest** (creating a PATCH after `2.0.0` gives `2.0.1`, never a patch of an older line).
7. Revision (`BGA:1368-1399`):
   * `revisionId` null → current HEAD commit of the default branch (`getCommit(<defaultBranch>)`); null → 500 `Cannot create version <v> of project <P>: cannot find current revision (project may be corrupt)`.
   * given → GitLab `getCommit(<revisionId>)` (raw — **aliases like `HEAD` are not resolved by legend**; GitLab interprets the ref): GitLab 404 → **400 `Revision <r> is unknown in project <P>`**; then the commit's branch refs must include the default branch, else **400 `Revision <r> is unknown in project <P>`** (same text for "not on default branch"). An empty string is not treated as null.
8. Create annotated tag `release-<v>` on the commit sha with message **`Release tag for version <v>`** (`BGA:1360-1361,1401`). If `notes != null` also create a GitLab **release** on that tag with description = notes (`BGA:1402-1405`; `""` creates an empty-description release).
9. **200** Version JSON built from the created tag — `notes` is **null in the response even when notes were supplied** (the release is created after the tag object was returned; a later GET shows the notes).
* Errors in 7-8 (`BGA:1408-1414`): 403 `User <u> is not allowed to create version <v> of project <P>: <glmsg>`; 404 `Unknown project: <P>`; else **500** `Error creating version <v> of project <P>: <glmsg>` — includes "tag already exists" (concurrent create). LegendSDLC exceptions from 7 pass through unchanged.
* **No check** that the revision differs from the latest version's revision: tagging the same commit twice (e.g. `1.0.0` and `1.0.1` on one sha) is allowed. No check that the project has a `project.json`.

### 2.4 `GET /projects/{p}/versions` — `VR:60-92` → `GVA:46-54,103-206`
* Params (all `Integer`, inclusive): `major`/`minor`/`patch` exact (each **trumps** its `minX`/`maxX`, `VR:84-90`), else `minMajor`/`maxMajor`, `minMinor`/`maxMinor`, `minPatch`/`maxPatch`. Each component is filtered independently (e.g. `minMinor=2` drops `3.1.0` even though `3.1.0 > 2.2.0`). min > max → empty. Negative values are accepted (just filters). Unparseable → 404.
* Source: all GitLab tags (100 per page) filtered to version tags.
* Order: **descending by version** (`GVA:52`), newest first.
* Errors: invalid project id 400; tag-listing errors as 2.3 step 5 (missing project → **404 `Unknown project: <P>`**).
* 200 array of Version (§2.1) (`[]` if none).

### 2.5 `GET /projects/{p}/versions/latest` — `VR:94-127` → `GVA:56-61,213-216`
Same params/filters; returns the max version. **No version → `null` → 204 No Content (empty body), not 404.** Errors as 2.4.

### 2.6 `GET /projects/{p}/versions/{versionId}` — `VR:129-138` → `VAPI:57-74` → `GVA:63-67` → `BGA:1335-1356`
* Parse (§2.2) failure → **400 `Invalid version string: "<v>"`** (`VAPI:64-67`; `LegendSDLCException` with status 400). Note `latest` is a separate route, so `/versions/latest` never reaches here.
* `parseProjectId` 400.
* GitLab `getTag(release-<v>)`: 404 (missing tag *or* missing project) → **404 `Version <v> is unknown for project <p>`**; 403 → `User <u> is not allowed to access version <v> of project <p>: <glmsg>`; else 500 `Error accessing version <v> of project <p>: <glmsg>`. `<v>` is the normalized `toVersionIdString()`, `<p>` the URL string.
* 200 Version JSON (with `notes` from the GitLab release).

### 2.7 `GET /projects/{p}/versions/{v}/entities[/{path}]`, `/versions/{v}/entityPaths`, `/versions/{v}/configuration`
`RES/entity/VersionEntitiesResource.java:37-88`, `RES/project/VersionProjectConfigurationResource.java:47-56` → `SourceSpecification.versionSourceSpecification(String)` (`legend-sdlc-project-files/.../source/SourceSpecification.java:41-44`) → files read at git ref `release-<v>` (`BGA:422`).
* Bad version string → raw `IllegalArgumentException` (not a LegendSDLC exception) → **500 `Invalid version string: "<v>"`** (contrast 400 on `GET /versions/{v}`).
* Entity filters and list order as slice 1 §5.
* Single entity missing → **404 `Unknown entity <path> for version <v>project <p>`** — the reference-info builder appends `version <v>` with **no `" of "`** before `project` (`BGA:770-773,798`; same in `GEA:194-197`). This affects every version-entity error message.
* Non-existent version (well-formed, no tag): single entity → 404 as above (file reads 404 → v0 structure → not found); list → depends on GitLab's archive 404 text (`GAFA:215-260`): `"404 File Not Found"` → `[]` 200, else 404 `Unknown version <v>project <p>`; configuration → **200 default config** (v0, MANAGED) (slice 1 §7 rule).
* **There is no `GET /versions/{v}/revisions` route** (grep of `@Path` under `RES/`). Other per-version routes exist but are out of scope (`upstreamProjects`, `pureModelContextData`, `builds`, `workflows`).

---

## 3. Revision lists

### 3.1 `GET /projects/{p}/revisions` and `GET /projects/{p}/workspaces/{w}/revisions`
`RES/revision/project/ProjectRevisionsResource.java:53-64`, `RES/revision/project/user/WorkspaceRevisionsResource.java:54-66` (group mirror `…/group/GroupWorkspaceRevisionsResource.java`) → `GRA:350-369` → `GAFA:726-801`.
* Params: `since` (`StartInstant`), `until` (`EndInstant`) (§0.1), `limit` (Integer).
* `limit`: null → unlimited; **`0` → `[]`** without calling GitLab; **`< 0` → 400 `Invalid limit: <n>`** (`GAFA:729-740`). (Swagger says "non-positive → no filtering" — wrong for this backend; contrast reviews, where `<= 0` means unlimited.)
* Ref: project → default branch; user workspace → `workspace/<CURRENT USER>/<w>` (no workspace-id validation on GET).
* GitLab `GET repository/commits?ref_name=<ref>&since=<since>&until=<until>&per_page=100` (`GAFA:775-781`; instants truncated to ms by `Date`). Semantics are GitLab's (git `--after/--before`, i.e. **committer date**, inclusive of the bound in practice — Studio removes the `since` commit itself). No path filter at this level.
* Order: **GitLab/git-log order, newest first**; not re-sorted. `limit` = first N of that order (`GAFA:760-763`).
* Empty page → if the ref does not exist → **404 `Unknown: user workspace <w> in project <P>`** (or `Unknown: project <P>`) (`GAFA:746-752,821-872`, note **"in project"**); existing ref with no commits in range → `[]`.
* GitLab errors (`GAFA:766-772`): 403 `User <u> is not allowed to get revisions for <desc>: <glmsg>`; 404 **`Unknown: <desc>`**; else 500 `Error getting revisions for <desc>: <glmsg>`. `<desc>` = `user workspace <w> in project <P>` / `project <P>`. Invalid project id → 400.
* 200 array of Revision (slice 1 §4 shape).
* Also exist (not used by Studio): `/revisions/{r}/status`, package/entity-scoped revision lists (`…/packages/{p}/revisions`, `…/entities/{path}/revisions`, `GRA:76-112`: invalid package/entity path → 400 `Invalid package path: <p>` / `Invalid entity path: <p>`; entity not found → 404 `Cannot find entity "<path>" in <refInfo>`).

---

## 4. Legend Studio usage (what lite must satisfy)

### 4.1 Transport
* Every request gets `client_name=<client>` added when configured (`SC:195-197`). Query params: `undefined` skipped, arrays → repeated keys (`?revisionIds=a&revisionIds=b`), everything else `toString()` (`NU:215-243`). `since`/`until` are sent as `Date.toISOString()` (`2026-10-04T12:00:00.000Z`) (`SC:566,1004-1005`).
* Non-2xx → `NetworkClientError` with the parsed payload (`NU:282`); **204 → `undefined`** (`NU:287-288`).
* URL builders: reviews `{baseUrl}/projects/{enc(p)}/[patches/{enc(v)}/]reviews[/{enc(id)}]` (`SC:974-991`); versions `/projects/{enc(p)}/versions[/{enc(v)}]` (`SC:577-580`); revisions `{project|workspace}/revisions[/{enc(r)}]` (`SC:542-554`); comparison `{review}/comparison…` with entity paths **not URL-encoded** (`SC:1137-1174`).

### 4.2 Routes Studio calls, and how
| Studio method | Route | Params / body | Lines |
|---|---|---|---|
| `getReviews` | `GET /projects/{p}/reviews` (or `/patches/{v}/reviews`) | any of `state, revisionIds[], workspaceIdRegex, workspaceTypes[], since, until, limit` — Studio only ever passes `state`, `revisionIds`, `since`, `limit` | `SC:993-1007` |
| `getReview` | `GET …/reviews/{id}` | — | `SC:1016-1021` |
| `getReviewApprovals` | `GET …/reviews/{id}/approval` | — | `SC:1022-1029` |
| `createReview` | `POST …/reviews` | `{workspaceId, title, workspaceType, description}` (never `labels`) | `SC:1030-1039`, `ST/stores/editor/sidebar-state/WorkspaceReviewState.ts:385-394` |
| `approveReview` / `rejectReview` / `closeReview` / `reopenReview` | `POST …/{id}/approve|reject|close|reopen` | no body | `SC:1040-1071` |
| `commitReview` | `POST …/{id}/commit` | `{message: "<review.title> [review]"}` | `SC:1072-1081`; `WorkspaceReviewState.ts:474-479`; `ST/stores/project-reviewer/ProjectReviewerStore.ts:482-540` |
| `getReviewComparision` (sic) | `GET …/{id}/comparison` | — | `SC:1092-1099` |
| `getReviewFromEntity` / `getReviewToEntity` | `GET …/comparison/{from|to}/entities/{path}` | — | `SC:1137-1174` |
| `getReviewFromConfiguration` / `getReviewToConfiguration` | `GET …/comparison/{from|to}/configuration` | — | `SC:1100-1124` |
| `getVersions` | `GET /versions` | **no params** (no min/max ever sent) | `SC:582-583` |
| `getLatestVersion` | `GET /versions/latest` | none | `SC:598-601` |
| `getVersion` | `GET /versions/{v}` | — | `SC:584-588` |
| `createVersion` | `POST /versions` | `{notes, revisionId, versionType}` | `SC:589-597`, `SM/version/VersionCommands.ts:30-71` |
| `getEntitiesByVersion` / `getConfigurationByVersion` | `GET /versions/{v}/entities`, `/configuration` | — | `SC:941-945,615-619` |
| `getRevisions` | `GET {project|workspace}/revisions` | `since`, `until` (ISO); **never `limit`** | `SC:556-567` |
| `getRevision` | `GET …/revisions/{id|alias}` | aliases sent upper-case: `CURRENT`, `BASE` (server alias match is case-insensitive) | `SC:568-573` |
| `isWorkspaceOutdated` | `GET …/workspaces/{w}/outdated` | — | `SC:496-500` |

Not called by Studio: `/reviews/{id}/outdated`, `/updateStatus`, `/update`, `/edit`, `/revokeApproval`, bulk `comparison/{from|to}/entities`, top-level `GET /reviews`, version `entityPaths`, project-level revisions list.

### 4.3 Models Studio deserializes
* Review (`SM/review/Review.ts:36-81`): reads `id, state, author{name,userId}, title, description?, projectId, workspaceId, webURL, createdAt, closedAt?, lastUpdatedAt?, committedAt?, workspaceType, labels?`. `createdAt` → `new Date(v)` **without a null guard** (`:63-66`) — must be present; the other dates are null-tolerant. `commitRevisionId` is ignored.
* ReviewApproval: `approvedBy: User[]` (`SM/review/ReviewApproval.ts:21-28`).
* Version: `id` deserialized as an object `{majorVersion, minorVersion, patchVersion}`; display string = `` `${major}.${minor}.${patch}` `` (`SM/version/VersionId.ts:23-68`).
* Revision: `authoredTimestamp`/`committedTimestamp` → `authoredAt`/`committedAt` (`SM/revision/Revision.ts:37-51`).
* Comparison: `toRevisionId, fromRevisionId, entityDiffs[{entityChangeType, oldPath?, newPath?}], projectConfigurationUpdated` (`SM/comparison/Comparison.ts:22-57`, `EntityDiff.ts:41-88`); `reprocessEntityDiffs` merges DELETE+CREATE on the same path into MODIFY and nulls `newPath` of deletes / `oldPath` of creates (`packages/legend-server-sdlc/src/util/ComparisonHelper.ts:20-59`).

### 4.4 "Does this workspace have an open review?" — `ST/stores/editor/sidebar-state/WorkspaceReviewState.ts:203-256`
1. `GET …/workspaces/{w}/revisions/CURRENT` → workspace HEAD sha (`:206-211`).
2. `GET /projects/{p}/reviews?state=OPEN&revisionIds=<head>&revisionIds=<head>&limit=1` (same id twice) (`:212-223`). No `workspaceIdRegex`/`workspaceTypes`.
3. Client-side: pick the review with `workspaceId === activeWorkspace.workspaceId && workspaceType === activeWorkspace.workspaceType` (`:224-228`); if reviews came back but none matched → warning `Opened review associated with HEAD revision '<sha>' of workspace '<type>' found, but the retrieved review does not belong to the workspace` (`:229-241`).
4. Errors → `handleChangeDetectionRefreshIssue`: **404** → blocking alert "Current project or workspace no longer exists" (`ST/stores/editor/EditorSDLCState.ts:213-229`).
⇒ Lite must return, for a revision id, the open reviews whose diff contains that commit — in particular the workspace's own open review when queried with the workspace HEAD. Because of `limit=1` + client filtering, any other open review that also contains the HEAD commit and sorts first would hide it (upstream order = per-revision GitLab order).

### 4.5 Create / close / commit from the workspace sidebar
* Create (`WorkspaceReviewState.ts:343-420`): blocked client-side for snapshot dependencies and sandbox projects; description is always `` `review from ${appName} for workspace ${workspaceId}` `` (`:381-383`); stores the returned Review (`:384`) — so the POST must return the full Review JSON.
* "Close review" button calls **`/reject`**, not `/close` (`:312-316`); then clears the review.
* Commit (`:422-543`): pre-checks `GET …/inConflictResolutionMode`; sends `{message: "<title> [review]"}`; then assumes **the workspace was deleted by SDLC** (comment `:485-487`; removes it from recents) and offers "Create new workspace" (`POST` workspace with the same id, then reload, `:258-288`) or "Leave" to the setup route (`:510-525`). Studio never calls DELETE workspace itself ⇒ **lite's commit must delete the workspace branch**.

### 4.6 Project reviewer page — `ST/stores/project-reviewer/ProjectReviewerStore.ts`
* Loads review, approvals (log-only on error), comparison in parallel (`:201-217`); comparison → for each diff fetch `from` entity (old path) and `to` entity (new path), plus from/to configuration if `projectConfigurationUpdated` (`:302-373`).
* Approve / commit / reopen / close each replace the shown review with the response body (`:437-630`) ⇒ these POSTs must return the updated Review. After commit, no navigation.
* UI (`ST/components/project-reviewer/ProjectReviewSideBar.tsx`): close hidden when COMMITTED, disabled when CLOSED; reopen only when CLOSED; approve/commit only when OPEN; status line uses `createdAt`/`closedAt`/`committedAt`/`lastUpdatedAt` by state.

### 4.7 Release (create version) flow — `ST/stores/editor/sidebar-state/ProjectOverviewState.ts:299-438`
* `GET /versions/latest`; falsy (204) → "no release" (`:303-308`).
* `GET /projects/{p}/revisions/CURRENT` → `releaseVersion.revisionId = <project HEAD>` (`:310-317`) ⇒ **Studio always tags the current default-branch HEAD, passing it explicitly.**
* With a latest version: `GET /revisions/<latest.revisionId>`, then `GET /reviews?state=COMMITTED&revisionIds=<that>&limit=1` (the review that produced it), then `GET /reviews?state=COMMITTED&since=<that review's committedAt ?? revision committedAt>` minus that review (`:321-366`); without: `GET /reviews?state=COMMITTED` (`:367-377`). These are the "committed reviews since last release" shown.
* Create: client error if `!features.canCreateVersion` (`:390-395`); `validate()` requires non-empty `revisionId` and **non-empty `notes`** (`SM/version/VersionCommands.ts:61-70`); `POST /versions {notes, revisionId, versionType}`; then re-runs the fetch (`:409-417`).
* **No client-side next-version computation** — the MAJOR/MINOR/PATCH buttons only send `versionType` (`ST/components/editor/side-bar/ProjectOverview.tsx:329-337,394-426`); the new number is whatever the server returns (re-fetched via `/versions/latest`).
* Button enabled only when `latest.revisionId !== currentProjectRevision.id` (`ProjectOverview.tsx:342-349`) — Studio itself prevents tagging the same HEAD twice.

### 4.8 Versions list and viewer
* `GET /versions` → `projectVersions`, server order used as-is (no client sort) (`ST/stores/editor/EditorSDLCState.ts:326-343`); shown as `id.id` + notes (`ProjectOverview.tsx:845-900`).
* Project viewer (`ST/stores/project-view/ProjectViewerStore.ts:154-259`): `GET /versions/latest` deserialized **without an undefined check** (`:177-181`); with a `versionId` route param it calls `GET /versions/{v}` unless it equals latest, then `GET /versions/{v}/entities` + `/configuration` (`:198-216`).

### 4.9 Workspace updater / revisions list
* `GET …/workspaces/{w}/outdated` at editor init (`EditorSDLCState.ts:414-436`) drives the "OUTDATED" status-bar button.
* Committed reviews since workspace base (`ST/stores/editor/sidebar-state/WorkspaceUpdaterState.ts:335-384`): `GET …/workspaces/{w}/revisions/BASE`, `GET /reviews?state=COMMITTED&revisionIds=<base>&limit=1`, `GET /reviews?state=COMMITTED&since=<baseReview.committedAt ?? base.committedAt>` minus the base review.
* Incoming revisions (`ST/stores/editor/sidebar-state/WorkspaceSyncState.ts:409-427`): `GET …/workspaces/{w}/revisions?since=<local HEAD committedAt>&until=<remote HEAD committedAt>`, filters out the local HEAD id (assumes inclusive `since`); errors → empty list. **This is the only list-revisions call**; Studio never lists project revisions.

---

## 5. Upstream quirks / bugs in these paths

1. **`limit` is applied before the workspace id/type filter** in `GET /reviews` (`GRVA:242`): `?workspaceIdRegex=x&limit=1` can return `[]` while matching reviews exist.
2. **Duplicate open review → 500**, not 409: no legend check; GitLab's 409 is mapped by the default supplier (`GRVA:371-377`). Missing workspace branch and "no changes" are likewise not validated (GitLab error → 500, or an empty MR is created).
3. `workspaceId` null in CreateReviewCommand → **500 `id may not be null`** (NPE) before the 400 title/description checks (`RAPI:276`, `WSPEC:38`).
4. `createReview` always targets the **current user's** workspace branch; reviews cannot be created for another user's workspace (`BGA:554`).
5. **Approve 401/403 message drops the built details** (`see <url> …: <glmsg>`) — `StringBuilder` built then a fresh message used (`GRVA:454-461`). Approve/revokeApproval don't check review state.
6. **Approve/revokeApproval return the pre-call MR** with only `lastUpdatedAt` refreshed (`GRVA:440-444,499-503`).
7. **`reject` is `close`** under another name (`GRVA:514-536`); Studio's sidebar "Close review" calls reject.
8. Commit's dependency error text says **"Cannot create a review…"** though raised at commit (`GRVA:779`); `approvals_left` check depends on GitLab populating it (`GRVA:577-581`).
9. Commit merge strategy, commit message use and `commitRevisionId` (null for ff merges) depend on the GitLab project's merge method; only the workspace branch is deleted, its backup/resolution branches are left behind (`GRVA:597`).
10. **`GET /versions/latest` with no versions → 204 empty body**, not 404 (`GVA:215`, `VR:117-126`); Studio's viewer then deserializes `undefined` (`ProjectViewerStore.ts:177-181`).
11. **POST /versions returns `notes: null`** even when notes were given (release created after the tag object is captured, `BGA:1401-1406`).
12. `versionType` null → **500 NPE** from the logging-description expression (`VR:155`); `type may not be null` 400 is unreachable (`GVA:73`).
13. Version-string errors are **400 on `GET /versions/{v}`** but **500 on `/versions/{v}/entities|entityPaths|configuration`** (raw `IllegalArgumentException`, `SourceSpecification.java:41-44`).
14. **"version 1.0.0project P"** — missing `" of "` in reference info for version sources (`BGA:770-773,798`), visible in every version-entity error (e.g. `Unknown entity model::A for version 1.0.0project PROD-1`).
15. `GET /versions/{v}/configuration` for a non-existent version → **200 default config** (slice 1 quirk 5 applies to tags too).
16. `createVersion` does **not alias-resolve `revisionId`** and reports "not on default branch" with the same text as "unknown" (`Revision <r> is unknown in project <P>`, 400) (`BGA:1387,1397`); no guard against tagging an already-versioned commit.
17. Version range filters are **per-component**, not lexicographic (`minMinor=2` excludes `3.1.0`) (`GVA:109-195`).
18. **`limit` semantics differ**: reviews treat `<= 0` as unlimited; revision lists treat `0` as empty and `< 0` as 400 `Invalid limit: <n>` (`GRVA:263` vs `GAFA:729-740`), while both Swagger texts say "non-positive → no filtering".
19. Unparseable `since`/`until` → **500** `Could not convert "<v>": Could not parse "<v>"` (Jersey re-throws the converter's `ProcessingException`, `BS:298-306`) rather than 400/404; bad enum query value → 400 with the generic `HTTP 400 Bad Request` text.
20. `GET …/comparison` works for any state, but `comparison/{from|to}/entities|configuration` give **500** `Current operation not supported for review state CLOSED on review <iid>` (no status set) (`GEA:119-126`, `GPCA:127-134`).
21. Copy-paste message: missing `head_sha` for **to**-entities says `Unable to get [from] revision info…` (`GEA:97`).
22. Comparison error texts lack spaces: `… on project<P>`, `revision <a>  to revision <b>`, `revisions <a> ,<b>` (`GCA:229-231`); target-revision error `… for review <iid> for project` with no project id (`GRVA:867`).
23. `getReviewComparison` reads the *from* structure with the workspace source spec and the *to* structure with the project source spec — swapped relative to the revisions they describe (`GCA:180-181`; harmless because the sha is literal).
24. `GET …/approval` has **no "is a review" check** (any MR IID), and can return **204** when GitLab omits `approved_by` (`GRVA:1079-1086`).
25. `edit` on a non-open review returns **500** (status-less) and a null body says `Input required to create review` (`GRVA:744-747`, `RR:229`).
26. `GET /reviews?revisionIds=…` pushes no time filter to GitLab and is one GitLab call per revision id (`GRVA:109-137`); revision ids are sent raw (no alias resolution).
27. In `GET /reviews` without `state`, `since` is not pushed down (only `until` → `created_before`), so the whole MR history is paged (`GRVA:200-225`); with any state, `until` filters on **creation** time even for COMMITTED/CLOSED.
28. Review/Version/VersionId/Comparison/Approval JSON key order is JVM-dependent (anonymous classes, no `@JsonPropertyOrder`); Studio keys by name.
29. Studio-side: `getReview`/`getReviewApprovals` client return types are swapped (`SC:1020,1026`); the approve button compares `currentUser.userId` to `review.author.name` (`ProjectReviewSideBar.tsx:217-221`); comparison entity paths are not URL-encoded (`SC:1137-1174`).
