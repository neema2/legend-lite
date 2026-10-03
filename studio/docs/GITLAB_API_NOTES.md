# H. GitLab API facts for a "GitLab backend" SDLC server

Research date 2026-10-03. Sources:
- docs.gitlab.com. Each page was read from its Markdown source in `gitlab-org/gitlab` `doc/` on master; the published URL is the one cited.
- Upstream FINOS legend-sdlc, read from the local checkout and cited as `file:line`.
- Where the docs are silent, GitLab's own Rails source on `gitlab-org/gitlab` master (read-only fetch). These are marked **(GitLab source)**.

Markers:
- **(Docs)**: quoted or closely paraphrased from the cited page.
- **(Inference)**: our own reading, not stated anywhere.

This file mirrors `studio/docs/GITHUB_API_NOTES.md` (= `G-github-api.md`). Section 12 is the side-by-side comparison.

Key URLs (short names used below):
- COMMITS = https://docs.gitlab.com/api/commits/
- REPOS = https://docs.gitlab.com/api/repositories/
- FILES = https://docs.gitlab.com/api/repository_files/
- BRANCHES = https://docs.gitlab.com/api/branches/
- MRAPI = https://docs.gitlab.com/api/merge_requests/
- TRAINAPI = https://docs.gitlab.com/api/merge_trains/
- TRAINS = https://docs.gitlab.com/ci/pipelines/merge_trains/
- TAGS = https://docs.gitlab.com/api/tags/
- RELEASES = https://docs.gitlab.com/api/releases/
- HOOKS = https://docs.gitlab.com/user/project/integrations/webhooks/
- HOOKEV = https://docs.gitlab.com/user/project/integrations/webhook_events/
- GLCOM = https://docs.gitlab.com/user/gitlab_com/
- GLRL = https://docs.gitlab.com/user/gitlab_com/rate_limits/
- LIMITS = https://docs.gitlab.com/administration/instance_limits/
- REST = https://docs.gitlab.com/api/rest/
- AUTH = https://docs.gitlab.com/api/rest/authentication/
- OAUTH = https://docs.gitlab.com/api/oauth2/

Legend-sdlc paths are relative to `.scratch/legend-sdlc/legend-sdlc-server/src/main/java/org/finos/legend/sdlc/server/gitlab/` unless a path is given in full. The short names used below:
- `Base` = `api/BaseGitLabApi.java`
- `FileAccess` = `api/GitLabApiWithFileAccess.java`
- `Workspace` = `api/GitLabWorkspaceApi.java`
- `Review` = `api/GitLabReviewApi.java`
- `Version` = `api/GitLabVersionApi.java`
- `Tools` = `tools/GitLabApiTools.java`

---

## 1. Plans: what is Free and what is paid

**Visibility on GitLab.com Free.**
- Projects and groups can be private, internal or public (Docs, https://docs.gitlab.com/user/public_access/). Internal visibility is disabled on GitLab.com (Docs, GLCOM "Visibility settings").
- So on Free you can have **both public and private projects**.
- The catch is in https://docs.gitlab.com/user/free_user_limit/ (Docs):
  - "You can add up to five users to newly created top-level namespaces with private visibility on GitLab.com". Above that, the namespace becomes read-only, including repositories.
  - The limit does not apply to public top-level groups.
  - Since 2026-01-27, new Free accounts are limited to **three top-level groups**.
  - So a private, multi-user modelling org on GitLab.com Free caps at five users. A public group does not.

**Feature tiers.** The tier comes from the page or section "Tier:" header, which applies to GitLab.com and Self-Managed alike.

| Feature | Tier | Source |
|---|---|---|
| Protected branches (project level) | **Free** | https://docs.gitlab.com/user/project/repository/branches/protected/ |
| Protected branches at group level; "Allowed to push/merge" for specific users or groups; Code Owner approval; "who can unprotect" | Premium | same page, sections "In a group", "With group permissions", "Require Code Owner approval", "Control who can unprotect" |
| Protected branches API, role-based access levels (`0`/`30`/`40`/`60`) | **Free** | https://docs.gitlab.com/api/protected_branches/. The `user_id` and `group_id` access entries are "Premium and Ultimate only". `deploy_key_id` is valid for push. |
| Protected tags | **Free** | https://docs.gitlab.com/user/project/protected_tags/ |
| Protected tags API: `create_access_level` | **Free** | https://docs.gitlab.com/api/protected_tags/ |
| Protected tags API: `allowed_to_create` with `user_id`, `group_id` or `access_level` | Premium | same page. `deploy_key_id` moved to Free in 18.10. |
| Push rules (including "Reject unverified users", signed-commit enforcement, preventing tag removal) | **Premium** | https://docs.gitlab.com/user/project/repository/push_rules/ ; https://docs.gitlab.com/user/project/repository/signed_commits/ ("Enforce signed commits with push rules": Premium) |
| Approvals | Free: optional only ("These approvals are optional and don't prevent merging without approval"). **Required approvals: Premium.** | https://docs.gitlab.com/user/project/merge_requests/approvals/ |
| Approval rules API | Premium. Approve and unapprove are Free. | https://docs.gitlab.com/api/merge_request_approvals/ |
| "Pipelines must succeed" (Require a successful pipeline for merge) | **Free** | https://docs.gitlab.com/user/project/merge_requests/auto_merge/ (the page is Free; there is no section override) |
| Auto-merge (formerly "merge when pipeline succeeds") | **Free** | same page |
| Merge request pipelines | **Free** | https://docs.gitlab.com/ci/pipelines/merge_request_pipelines/ |
| Merged results pipelines | **Premium** | https://docs.gitlab.com/ci/pipelines/merged_results_pipelines/ |
| Merge trains (and merge trains API) | **Premium** | TRAINS ; TRAINAPI |
| Merge methods: merge commit, semi-linear, fast-forward | **Free** | https://docs.gitlab.com/user/project/merge_requests/methods/ |
| Squash and merge | **Free** | https://docs.gitlab.com/user/project/merge_requests/squash_and_merge/ |
| Releases | **Free**. Release Metrics is Ultimate. | https://docs.gitlab.com/user/project/releases/ |
| External status checks | Ultimate | https://docs.gitlab.com/user/project/merge_requests/status_checks/ |
| Group webhooks | Premium. Project webhooks are Free. | HOOKS "Group webhooks" |
| Project and group access tokens | **GitLab.com: Premium.** Self-Managed: "available with any license". | https://docs.gitlab.com/user/project/settings/project_access_tokens/ ; https://docs.gitlab.com/user/group/settings/group_access_tokens/ |

**Self-managed GitLab CE (open source).**
- The "Free" tier on Self-Managed is the feature set without a license. EE-only code is layered on top of CE as `prepend_mod` modules (Docs, https://docs.gitlab.com/development/ee_features/).
- So we expect CE to have exactly the "Free" rows above (Inference):
  - protected branches and tags, auto-merge, pipelines-must-succeed, MR pipelines, merge methods, squash, releases;
  - project and group access tokens ("any license");
  - **no** merge trains, merged-results pipelines, required approvals, push rules or group webhooks.
- **Consequence for "main always green":**
  - Merge trains are Premium on GitLab.com and EE-licensed on Self-Managed.
  - On Free/CE the best available is **fast-forward-only merge plus "Pipelines must succeed"**. A fast-forward merge requires the MR to be rebased on the current target (Docs, methods page "Fast-forward merge"), and the pipeline must pass on that rebased head.
  - This is a serial queue that we drive ourselves: rebase, wait for the pipeline, then merge with `sha`. (Inference)

## 2. Atomic multi-file commit and the optimistic lock

**Endpoint.** `POST /projects/:id/repository/commits` (Docs, COMMITS "Create a commit").

Top-level attributes (Docs, COMMITS):

| Attribute | Notes |
|---|---|
| `branch` | required |
| `commit_message` | required |
| `actions[]` | see below |
| `allow_empty` | |
| `author_email`, `author_name` | override the author |
| `force` | "If `true`, overwrites `branch` with a new commit based on `start_branch` or `start_sha`, replacing the branch's existing commit history" |
| `start_branch` | "Name of the branch to use as the parent… defaults to the value of `branch`. Mutually exclusive with `start_sha`" |
| `start_sha` | "SHA of the commit to use as the parent… Must be a full 40-character SHA" |
| `start_project` | |
| `stats` | |

`actions[]` fields (Docs, COMMITS):
- `action` is one of `create`, `delete`, `move`, `update`, `chmod`.
- `file_path`.
- `content` is required except for delete, chmod and move. "Move actions that do not specify `content` preserve the existing file content".
- `encoding` is `text` (default) or `base64`.
- `execute_filemode` applies to chmod.
- `previous_path` applies to move.
- `last_commit_id`: "Last known file commit ID. Only considered in update, move, and delete actions."

The whole action list becomes **one commit**, so it is atomic. On success it returns `201` with the new commit `id` and `parent_ids` (Docs, COMMITS).

**Is there a branch-level expected SHA (compare-and-swap)? No, not in the public API.**
- **`start_sha` is not an expected-head check.**
  - If `start_sha` is given and `branch` already exists, the request is **rejected** unless `force=true`: "A branch called '…' already exists. Switch to that branch in order to make changes" (GitLab source: `app/services/commits/create_service.rb` lines 57-59 `different_branch?` is true when `@start_sha.present?`; lines 102-111 `validate_branch_existence!`).
  - With `force=true` the branch is **overwritten** with a commit whose parent is `start_sha`. Concurrent commits on the branch are discarded silently. That is the opposite of what we want (Docs, COMMITS `force`; Inference).
- **`last_commit_id` is a per-file check, not a branch check.**
  - For each update, move or delete action, GitLab compares the last commit that touched the path on `start_branch` with the last commit that touched it as of `last_commit_id`. If they differ it fails with "The file has changed since you started editing it: <path>" (GitLab source: `app/services/files/multi_service.rb` 59-73; `app/services/files/base_service.rb` 33-45).
  - The API defaults `start_branch` to `branch` when no `start_sha` is given (GitLab source: `lib/api/commits.rb` 381).
  - It is skipped for `create` actions and for actions without `last_commit_id`.
  - It does not detect a concurrent commit that touched *other* files.
  - It runs in validation, before the write, so it is not atomic with the write (Inference from the code order).
- **Internal compare-and-swap exists but is not exposed.**
  - Rails passes `target_sha` = the branch head it reads **at request time** as Gitaly's `expected_old_oid`. The comment reads "Used to prevent races in updates between different clients" (GitLab source: `app/models/repository.rb` 968-984; `lib/gitlab/git/repository.rb` 1070-1083; `lib/gitlab/gitaly_client/operation_service.rb` 684-700).
  - This protects only the window inside one request, because the server picks the value. Clients cannot pass it: the API has no such parameter (Docs, COMMITS; GitLab source `lib/api/commits.rb` 359-396).
- **There is no "update ref" REST endpoint.**
  - The Branches API has only create (`branch`, `ref`), delete, and delete-merged (Docs, BRANCHES).
  - Moving a branch over REST means delete then create, which is what legend-sdlc does (section 11). That is **not atomic**: the branch is briefly absent.
- **Conflict status code.**
  - Every Commits-API service error, including "file has changed", "branch already exists" and Gitaly index errors, is returned as **HTTP 400** with a `message` (GitLab source: `lib/api/commits.rb` 395 `render_api_error!(result[:message], 400)`).
  - The docs list only 400/401/403/404 for this endpoint. Distinguishing a stale write means matching the message text (Inference).

**How to refuse a save when the branch moved (options, best first):**
1. **Git smart-HTTP push with an explicit old value.**
   - Our server builds the commit (or reuses one created on a scratch branch) and runs `git push` of `<new>:refs/heads/<ws>` with `--force-with-lease=refs/heads/<ws>:<expected>`. Plain non-force push also works when the new commit's parent is `<expected>`.
   - The receive-pack ref update carries old-oid → new-oid, so it is a true compare-and-swap.
   - It authenticates with the user's OAuth token as `https://oauth2:<token>@host/...` (Docs, OAUTH "Access Git over HTTPS with access token").
   - (Inference: git protocol semantics; GitLab docs do not describe the lease.)
   - Cost: we need a git client and object transfer in our server.
2. **Two-step via scratch branch.**
   - `POST commits` with `branch=tmp/<uuid>`, `start_sha=<expected>`, giving commit C with parent = expected.
   - Then advance the workspace with a git push (option 1).
   - Or accept the non-atomic "check head, delete, recreate" (legend-sdlc style). That is **not** safe.
3. **API-only, best effort.**
   - Serialize saves per workspace inside our server (one writer per branch).
   - `GET` the branch head, compare with expected (our 409), then `POST commits` with `last_commit_id` set on every update, move and delete.
   - A small race window remains against writers outside our server. Mitigation: make workspace branches writable only through our server by convention.
   - Restricting push to one bot user or deploy key needs Premium on protected branches (`user_id`) or uses a deploy key (Free, `deploy_key_id`). (Docs, https://docs.gitlab.com/api/protected_branches/)

**Limits.**
- Request body over 300 MB gets `413`. Requests over 20 MB are limited to "3 requests per 30 seconds". Introduced in 18.7. Self-Managed can configure `GITLAB_COMMITS_MAX_REQUEST_SIZE_BYTES` (Docs, LIMITS "Commits and Files API limits"; COMMITS note).
- No documented maximum number of actions.
- The default JSON validation limits apply to "All other paths": max depth 32, **max array size 50,000**, max total elements 100,000 (Docs, LIMITS "JSON validation limits by endpoint"). Each action is about 4-5 JSON elements, so the practical ceiling is about 20k actions per commit (Inference).
- legend-sdlc caps one commit at 512 actions and splits larger change sets (section 11).

## 3. Reading trees and files at a commit

**Tree API.** `GET /projects/:id/repository/tree` with these parameters (Docs, REPOS "List all repository trees"):
- `path`, `ref`, `recursive` (boolean, default false), `per_page` (default 20; max 100 per REST), `pagination=keyset` with `page_token`, `with_last_commit` (19.3+, not combinable with `recursive`).

Behaviour (Docs, REPOS; https://docs.gitlab.com/api/rest/ "Pagination"):
- `ref` is documented as "Name of a repository branch or tag". Commit SHAs work in practice and in legend-sdlc, but the docs do not say so (Could not determine from docs).
- A missing path returns `404` since 17.7.
- Entries carry `id` (git object SHA), `name`, `type` (`tree`/`blob`), `path` and `mode`.
- The response does **not** give the tree SHA of `path` itself. To get it, list the parent directory without recursion and take the entry's `id` (Inference from the response shape).
- No truncation limit is documented, unlike GitHub's 100k / 7 MB. Large recursive trees are paged at 100 entries per page. Offset pagination is capped (`offset_pagination_limit` default 50,000, applied to endpoints that also support keyset pagination), and for over 10,000 records `x-total`/`x-total-pages` are omitted. (Docs, LIMITS "Max offset allowed by the REST API"; REST "Pagination")
- **Use keyset pagination for recursive project subtrees.** (Inference)

**Files API.** `GET /projects/:id/repository/files/:file_path?ref=<branch|tag|commit>` (Docs, FILES):
- Returns base64 `content`, `blob_id`, `commit_id` and `last_commit_id`.
- The `/raw` variant returns bytes.
- `HEAD` returns only metadata headers (`X-Gitlab-Blob-Id`, `X-Gitlab-Last-Commit-Id`, ...).
- Blobs over 10 MB are limited to 5 requests per minute.
- File paths containing `/` must be URL-encoded (`%2F`), as must branch and tag names that contain `/` (Docs, REST "Namespaced paths" / encoding).

**Blobs** (Docs, REPOS):
- `GET /repository/blobs/:sha` (base64) and `/blobs/:sha/raw` (bytes). Blobs over 10 MB are limited to 5 per minute.
- `POST /repository/blobs/batch` reads up to 20 files per request, truncated at 1 MB each. It is **Beta, behind a feature flag (default off)**, and limited to 5 requests per minute per user and project. Do not rely on it.

**Archive.** `GET /repository/archive[.format]?sha=<commit>&path=<subdir>&exclude_paths=…` (Docs, REPOS "Retrieve file archive"):
- Supports a **subpath**, so it can return just one project directory at a commit.
- But "For GitLab.com users, this endpoint has a rate limit threshold of 5 requests per minute".

**Best approach for "one project subdirectory at commit X, cached by SHA"** (Inference):
1. Resolve the project directory's tree SHA by walking non-recursive tree listings from the root at `ref=<commit SHA>`.
2. If that tree SHA is cached, stop.
3. Otherwise list the subtree recursively (keyset, `per_page=100`) to get `(path, blob id)`.
4. Fetch only blobs not already cached, keyed by blob SHA (immutable) via `/blobs/:sha/raw`.
5. Use the archive (`path=` + `sha=`) only for cold, bulk loads, and back off on 429. legend-sdlc does this the other way round (section 11).

**Size limits.**
- Free push limit: 100 MiB per file on GitLab.com Free (Docs, https://docs.gitlab.com/user/free_push_limit/).
- Maximum push size on GitLab.com is 5 GiB (Docs, GLCOM).
- Irrelevant for small `.pure` files.

## 4. Authorship and signing

**Author vs committer.**
- `author_name`/`author_email` on the Commits API set the author (Docs, COMMITS). "If unspecified the committers email is used" (GitLab source: `lib/gitlab/git/repository.rb` 1064-1065).
- The **committer** is the user the token belongs to: Gitaly's `user` is `resolve_composite_identity_actor(current_user)` (GitLab source: `app/services/files/multi_service.rb` 43-44; `operation_service.rb` 670).

By token type:
- **User OAuth token or PAT:** committer = that user. Permission is checked against that user ("You are not allowed to push into this branch", GitLab source `create_service.rb` 74-80). This is the natural per-user attribution and what legend-sdlc uses.
- **Project or group access token:** GitLab creates a **bot user**, and "their contributions are associated with the bot user account". Committer = bot. You can set `author_*` to the human, but permission enforcement becomes our job. (Docs, https://docs.gitlab.com/user/project/settings/project_access_tokens/ "Bot users for projects")
- **Impersonation token** (admin-created PAT "used to authenticate with the API as a specific user") or **Sudo** (admin token with `sudo` scope plus a `Sudo:` header): the commit is made *as* the user. Self-Managed only in practice, because both need instance admin (Docs, AUTH "Impersonation tokens", "Sudo").

**Signing.** GitLab signs commits it creates with an instance key (Docs, https://docs.gitlab.com/user/project/repository/signed_commits/web_commits/ and https://docs.gitlab.com/user/project/repository/signed_commits/ "Verify GitLab-signed commits"):
- Self-Managed and Dedicated: configured in Gitaly. The feature is flagged as "not ready for production use" in the page note.
- GitLab.com: "Sign web-based commits" can be enabled per group or project (GA 18.10).
- Docs scope: "commits made through the GitLab UI (Web Editor, Web IDE, and merge requests)".
- Source: `Repository#commit_files` passes `sign: sign_commits?` for **all** `commit_files` callers, which includes the Commits API (GitLab source: `app/models/repository.rb` 983, 1610-1612). So API commits are probably signed when the setting is on (Inference; not stated in the docs).
- The page says the signed commit's Committer is configurable and that push rules rely on it.
- **Contrast with GitHub:** custom author information does not stop GitLab from signing (Inference from source). GitHub refuses to sign when a custom author is given.

## 5. Rate limits and conditional requests

**GitLab.com today** (Docs, GLRL "Current rate limits"):

| Limit | Value |
|---|---|
| Authenticated API traffic per user | 2,000 requests/min |
| Raw endpoint traffic per project, commit or file path | 300/min |
| Repository files API (`GET …/repository/files/*`) per IP and file path | 500/min |
| Pipeline creation per project, user or commit | 25/min |
| Note creation | 60/min |
| Single project `GET /projects/:id` | 400/min |
| Projects list | 2,000 per 10 min |
| Archive | 5/min (Docs, REPOS) |
| Blobs over 10 MB | 5/min (Docs, REPOS) |
| Commits/Files API requests over 20 MB | 3 per 30 s (Docs, LIMITS) |
| **Tag creation** | **100 per 30 minutes per project**, shared across API, GraphQL `tagCreate`, UI and `/tag`. "The limit applies to the project, not to you." Added in 19.4. (Docs, TAGS "Create a new tag") |

**Proposed per-plan limits, announced but "not in effect yet"** (Docs, GLRL "Rate limits by plan"):

| Authenticated traffic for a user | Free | Premium | Ultimate |
|---|---|---|---|
| Sustained | 5,000/h | 15,000/h | 25,000/h |
| Burst | 100/min | 1,250/min | 2,000/min |

GLCOM says limits "apply both per user and per top-level group".

**Exceeding a limit.** You get `429`, with `Retry-After` and `RateLimit-ResetTime`. All responses carry `RateLimit-Limit`/`RateLimit-Remaining`, except the Projects, Groups and Users APIs. You "can receive a `429` response even when the headers on your previous response showed remaining quota". (Docs, GLRL)

**Per-project write budget.** There is **no documented per-project commit limit** besides the 20 MB rule (Could not determine). GitLab.com Free requests at the proposed 100/min burst would allow about 50 saves/min per user at roughly 2 calls per save (Inference).

**ETag / conditional requests.**
- REST lists `304 Not Modified` as a status code (Docs, https://docs.gitlab.com/api/rest/troubleshooting/).
- The only documented ETag mechanism is the internal Redis-backed "Polling with ETag caching" for frontend polling endpoints (Docs, https://docs.gitlab.com/development/polling/).
- Unlike GitHub, nothing says 304s are free against rate limits. (Could not determine)
- **Cache by SHA instead.** Tree, blob and commit objects addressed by SHA are immutable (Inference).

## 6. Merge requests, the MR-level compare-and-swap, queue and green

**Create.** `POST /projects/:id/merge_requests` with `source_branch`, `target_branch`, `title`, plus optional `description` (≤1,048,576 chars), `labels`, `remove_source_branch`, `squash`, `reviewer_ids`, `merge_after` and others (Docs, MRAPI "Create a merge request").

**Merge.** `PUT …/merge_requests/:iid/merge` (Docs, MRAPI "Merge a merge request"):
- Parameters: `auto_merge`, `merge_commit_message`, `squash`, `squash_commit_message`, `should_remove_source_branch`, and **`sha`**: "If present, this SHA must match the HEAD of the source branch. Use to ensure that only reviewed commits are merged".
- A group or instance setting can **require** `sha` (19.2+).
- Errors:

| Code | Meaning |
|---|---|
| `400` | "SHA must be provided when merging" |
| `401` | not permitted |
| `405` | "The merge request cannot merge" |
| `409` | "SHA does not match HEAD of source branch" |

- **This is the MR-level compare-and-swap. It checks the source head, not the target.**
- Merge readiness: use `detailed_merge_status` (`mergeable`, `ci_must_pass`, `ci_still_running`, `need_rebase`, `not_approved`, `conflict`, `checking`, `unchecked`, ...) instead of the deprecated `merge_status` (Docs, MRAPI "Merge status").

**Auto-merge.** `auto_merge=true` on the merge call ("merges when checks pass"). `merge_when_pipeline_succeeds` is deprecated (17.11). (Docs, MRAPI)
- New commits pushed to the MR cancel auto-merge.
- New commits on the target cancel it under semi-linear or fast-forward methods without automatic rebase.
- Cancel with `POST …/cancel_auto_merge`.
- (Docs, https://docs.gitlab.com/user/project/merge_requests/auto_merge/ "Pipeline success for auto-merge"; MRAPI)
- Since 19.1, `auto_merge` on a project with merge trains enqueues on the train instead of bypassing it (Docs, MRAPI history).

**Merge trains (Premium).** `POST /projects/:id/merge_trains/merge_requests/:iid` (Docs, TRAINAPI):
- Parameters: `auto_merge`, `sha` ("must match the `HEAD` of the source branch, otherwise the merge fails"), `squash`.
- Returns `201` when added, `202` when scheduled.
- `GET /merge_trains/:target_branch` lists the train. Entry `status` is `idle`/`fresh`/`stale` (active) or `merging`/`merged`/`skip_merged` (complete).

Mechanics (Docs, TRAINS):
- Each MR's pipeline runs on the target plus every MR ahead of it. Up to 20 pipelines run in parallel by default; this is configurable, and with 1 the train is sequential.
- A failed MR is removed and later pipelines restart.
- Merge trains need merged-results pipelines enabled and CI configured for MR pipelines.
- They cannot be skipped for fast-forward or semi-linear methods, which implies trains work with fast-forward.

**Does merge create a new SHA?** It depends on the merge method (Docs, https://docs.gitlab.com/user/project/merge_requests/methods/):
- "Merge commit" and "semi-linear": yes, a merge commit (`merge_commit_sha`).
- `squash`: a squash commit (`squash_commit_sha`) (Docs, MRAPI response fields).
- **Fast-forward without squash:** the target just points at the source head ("`main` now points to commit D"), so **the SHA tested on the MR is the SHA on main**.
- Merge trains run on an internal merged-results commit, whose author is "the user that initiated the merge" (Docs, TRAINS).
- **Version tagging should take the main SHA from the push webhook `after`, or from `merge_commit_sha` / `squash_commit_sha` / the source head under fast-forward.** (Inference)

**Rebase.** `PUT …/merge_requests/:iid/rebase` (`skip_ci`) returns `202` and runs asynchronously. Poll `GET …?include_rebase_in_progress=true` until `rebase_in_progress=false`, then check `merge_error` (Docs, MRAPI "Rebase a merge request").

**Is a commit on main green?** (Docs, https://docs.gitlab.com/api/pipelines/ ; COMMITS)
- `GET /projects/:id/pipelines?sha=<sha>&ref=main` with `status`, or `GET /pipelines/latest?ref=main`, which returns `403` if there is no pipeline.
- `GET /repository/commits/:sha` has `last_pipeline`.
- `GET /repository/commits/:sha/statuses` lists per-job and external statuses.
- External CI can post with `POST /projects/:id/statuses/:sha` (`state` = pending, running, success, failed, canceled or skipped; `name`/`context`).
- "Pipelines must succeed" honours external CI statuses too (Docs, auto_merge page).
- The **pipeline status** is the single "green" signal. There is no separate checks-vs-statuses split as on GitHub (Inference).

## 7. Tags and releases

**Create a tag.** `POST /projects/:id/repository/tags` with `tag_name` and `ref` (SHA, branch or tag). Adding `message` creates an **annotated** tag; without it the tag is lightweight. Subject to the 100 per 30 min per-project limit. (Docs, TAGS)

**List tags.** `GET /repository/tags?search=^prefix&order_by=version` supports `^term` and `term$` anchors and keyset `page_token` (Docs, TAGS). Useful for "versions of project X" if the tag name starts with the project path (Inference).

**Protected tags (Free).**
- They control who can create tags and "Prevent accidental update or deletion once created". Wildcards are allowed.
- "To create or delete a protected tag, you must be in the **Allowed to create** list."
- `create_access_level` defaults to Maintainer (40). Restricting to one user requires Premium; restricting to a deploy key is Free.
- (Docs, https://docs.gitlab.com/user/project/protected_tags/ ; https://docs.gitlab.com/api/protected_tags/)

**Releases.** `POST /projects/:id/releases` with `tag_name`, `ref` (needed if the tag does not exist; then the release creates the tag), `tag_message` (makes the created tag annotated), `name`, `description` (Markdown), `assets:links`, `milestones` and `released_at` (Docs, RELEASES "Create a release").
- Releases need Developer or higher. "If a release is associated with a protected tag, the user must be allowed to create the protected tag too." (Docs, https://docs.gitlab.com/user/project/releases/ "Release permissions")
- **One call can create tag plus release atomically from a SHA.** We could not find documentation that it is transactional if release creation fails after tag creation (Could not determine).

**Slashes in tag names.**
- Git ref-format rules apply. GitLab adds for branches: no spaces, no 40-hex names, case-sensitive; it advises avoiding `~^:?*[\`, `..`, `@{`, `//`, trailing `.`/`.lock`, and leading `-`/`.` (Docs, https://docs.gitlab.com/user/project/repository/branches/ "Name your branch"). We found no tag-specific rule.
- Any branch or tag containing `/` must be URL-encoded in path parameters (Docs, REST).
- Note: the Branches API says `branch` "Cannot contain spaces or special characters except hyphens and underscores" (Docs, BRANCHES). legend-sdlc nonetheless creates `workspace/<user>/<id>` branches (Base:541-557), so `/` works in practice.

## 8. Webhooks

**Push events** (`X-Gitlab-Event: Push Hook`). Payload fields (Docs, HOOKEV "Push events"):
- `object_kind`, `before`, `after`, `ref`, `ref_protected`, `checkout_sha`, `user_id`, `user_name`, `user_username`, `user_email`, `project_id`, `project{...}`, `commits[]`, `total_commits_count`, `repository`.

When a push event is **not** sent (Docs, HOOKEV):
- for tags;
- "when a single push includes changes for more than three branches by default" (`push_event_hooks_limit`). When this is exceeded, **no webhooks fire for the entire push** (Docs, HOOKS "Push event limits").
- `commits` holds only the newest 20 commits.

**Tag push events** (`Tag Push Hook`, `object_kind: tag_push`):
- Fired on tag create and delete. Same `before`/`after`/`ref` shape (`before` all zeros on create).
- Same 3-per-push limit. (Docs, HOOKEV "Tag events")

**Delivery** (Docs, HOOKS "Webhook receiver requirements"; GLCOM "Webhooks"):
- Timeout is **10 s** on GitLab.com.
- Payload cap is 25 MB.
- Up to 100 hooks per project.
- A namespace-wide webhook rate limit applies (**500/min on Free**). When it is hit, "all webhooks in the namespace are temporarily disabled and automatically re-enabled in the next minute".
- Respond quickly with 200/201 and "Prepare for duplicate events if a webhook times out".
- `Idempotency-Key`/`webhook-id` is "consistent across webhook retries" (Docs, HOOKS "Delivery headers").
- We found no documented automatic retry schedule for project webhooks (Could not determine).

**Auto-disabling** (Docs, HOOKS "Auto-disabled webhooks"):
- After 4 consecutive failures (4xx, 5xx, timeout) the hook is "temporarily disabled", starting at 1 minute and backing off up to 24 h, then re-enabled automatically.
- After **40 consecutive failures** it is **permanently disabled** until a successful test request.
- On Self-Managed the auto-disable of project hooks is feature-flagged.

**Verification** (Docs, HOOKS "Signing tokens", "Delivery headers"):
- Legacy: `X-Gitlab-Token` carries the secret token "as plain text"; GitLab no longer recommends it.
- **New (GA 19.1): signing token.** Standard Webhooks HMAC-SHA256 over `{webhook-id}.{webhook-timestamp}.{body}`, sent in `webhook-signature: v1,<base64>`. The key is the base64 part after `whsec_`. Compare in constant time.
- Other headers: `X-Gitlab-Event`, `X-Gitlab-Event-UUID`, `X-Gitlab-Webhook-UUID`, `X-Gitlab-Instance`.

**Redelivery** (Docs, HOOKS "View webhook request history"):
- **Recent events** keeps the last **two days**.
- "Resend Request" sends the same data and the same `Idempotency-Key`. It is "also… programmatically through the project webhooks API". It is impossible if the hook URL changed.

**Takeaway.** Webhooks are lossy: 3-ref push limit, auto-disable, 2-day history. The read index needs a periodic reconcile: list branches and tags and compare heads (Inference; same conclusion as for GitHub).

## 9. Auth for a server acting for many users

**OAuth2 application** (Docs, OAUTH; https://docs.gitlab.com/integration/oauth_provider/):
- Authorization code flow, optionally with PKCE. Scopes are requested per app; `api` is needed for writes.
- Access tokens "expire after two hours (7200 seconds)".
- Refresh with `grant_type=refresh_token`. A refresh "Invalidates the existing `access_token` and `refresh_token`", so **rotation is single-use. Serialize refresh per user and persist atomically.** (Docs, OAUTH)
- Refresh tokens work after the access token expires (Docs, oauth_provider).
- An OAuth token works for Git over HTTPS as `oauth2:<token>`, which enables option 1 of section 2 (Docs, OAUTH).

**Other token types:**
- **Personal access tokens:** per user, user-set expiry. Not suitable as a server credential for many users (Docs, https://docs.gitlab.com/user/profile/personal_access_tokens/).
- **Project and group access tokens:** create **bot users**. Premium on GitLab.com; any license on Self-Managed (section 1). Good for the read index and webhooks plumbing; commits would be attributed to the bot as committer (section 4).
- **Impersonation tokens:** admin-only; act as a user (Docs, AUTH). **Sudo:** admin plus `sudo` scope (Docs, AUTH). Self-Managed only in practice. A service account with impersonation would allow per-user attribution without per-user OAuth, at the cost of instance-admin power (Inference).

**What upstream legend-sdlc does:**
- **pac4j profiles produce a session.** `GitLabSessionBuilder.newSession` picks a Kerberos, OIDC or GitLab-PAT session from the pac4j profile type (`auth/GitLabSessionBuilder.java:111-125`).
- **OIDC.** If the OIDC profile's access token has scope `api` and its issuer equals the GitLab URL, it is reused directly as a GitLab OAuth2 token (`auth/GitLabOidcSession.java:132-150`).
- **PAT profile.** The token is stored as a `PRIVATE` token (`auth/GitLabPersonalAccessTokenSession.java:136`).
- **OAuth code flow.**
  - The redirect goes to `/oauth/authorize?client_id&redirect_uri&response_type=code&state=<original request>` (`auth/GitLabOAuthAuthenticator.java:430-460`). No `scope` parameter is sent, so the app's registered scopes apply.
  - The code exchange is in `auth/GitLabTokenManager.java:130-137`.
  - The token, refresh token and expiry are kept in the session object (`auth/GitLabTokenManager.java:31-33`).
  - Refresh happens at **3/4 of `expires_in`** (`auth/GitLabTokenManager.java:105-113, 125-128`). It refreshes when `shouldRefreshToken()` (`auth/GitLabUserContext.java:76-101`). If that fails, it re-authorizes or redirects (`auth/GitLabUserContext.java:109-147`).
- **Each request builds a `GitLabApi` with the user's token** (`auth/GitLabUserContext.java:104`). Every API class uses it through `getGitLabApi()` → `userContext.getGitLabAPI()` (Base:161-165). So **all writes are made as the end user**, giving per-user attribution and permission checks.
- **Avoid: token stored in the cookie.**
  - The session, including the GitLab token and refresh token, is encoded into the session cookie (`auth/GitLabTokenManager.java:145-157`).
  - The token encoding is only URL-safe base64 (`legend-sdlc-server-shared/.../auth/Token.java:43-44`).
  - The cookie is set without `Secure` or `HttpOnly` ("TODO should we make this Secure and HttpOnly?", `legend-sdlc-server-shared/.../auth/LegendSDLCWebFilter.java:233-237`).
  - Refresh is not serialized across concurrent requests, which is risky given single-use rotation (Inference).

## 10. Repository limits

- **Repository size on GitLab.com:**
  - "Repository size including LFS" is 10 GB (Docs, GLCOM "Account and limit settings").
  - Free: "Each project in a Free tier namespace on GitLab.com has 10 GiB of free storage"; exceeding it makes the project read-only unless storage is bought.
  - Premium/Ultimate: 500 GiB per project.
  - (Docs, https://docs.gitlab.com/user/storage_usage_quotas/)
- **Push:** maximum push size 5 GiB on GitLab.com; 100 MiB per new file on Free (Docs, GLCOM; free_push_limit).
- **Branches:** no documented branch-count limit (Could not determine). GitHub, by comparison, has 5,000 branches as guidance. There is a bulk "Delete all merged branches" endpoint (Docs, BRANCHES).
- **Files per directory:** no documented limit (Could not determine). GitHub's guidance is 3,000.
- **Merge requests:** 1,000,000 diff commits per MR (Docs, GLCOM "Merge request limits"). Diff display limits apply only to rendering (Docs, LIMITS "Diff limits").
- **Webhooks:** 100 per project, 25 MB payload, 10 s timeout (Docs, GLCOM "Webhooks").
- **Free user limit:** five users per private top-level namespace (section 1).

## 11. How upstream legend-sdlc maps SDLC concepts to GitLab

**Project.**
- One SDLC project is one GitLab project. The id is `<prefix>-<gitlabNumericId>` (`GitLabProjectId.java:23-41, 71`).
- New projects get visibility from configuration, default `INTERNAL` (`api/GitLabProjectApi.java:88, 242-251`).
- Initial structure is submitted as an MR `"Project structure"` (`api/GitLabProjectApi.java:449`).
- Our design differs (monorepo, project = directory), so per-project concepts must become path-scoped (Inference).

**Branch naming** (Base:98-109, 541-557):
- Workspaces are `workspace/<userId>/<workspaceId>` (user) or `group/<workspaceId>` (group).
- Conflict resolution uses `resolution/...` and `group-resolution/...`; backups use `backup/...` and `group-backup/...`.
- Patch workspaces are prefixed `patch/<x.y.z>/`; patch release branches are `patch/main/<x.y.z>`.
- Temporary branches are `tmp/<user>/[<wsId>/]<random>` (Base:586-593).
- The source branch is the project's default branch, looked up from the API (Base:394-470).

**Save (commit) = Commits API with per-request revision check** (`FileAccess`):
- Each entity add, modify, delete or move becomes one `CommitAction` with base64 content (`FileAccess:942-983`).
- A move without content gets its content fetched from the reference revision (`FileAccess:986-1026`).
- **Expected-revision check:** if the client supplied `revisionId`, the server reads the current branch head and throws **409 CONFLICT** if it differs, then calls `createCommit(projectId, branchName, message, null, null, null, actions)` (`FileAccess:906-925`).
  - **This is check-then-act, not atomic.** A concurrent writer between the read and the `createCommit` is not detected.
  - It does not send `last_commit_id` or `start_sha`.
  - The `null, null, null` arguments are presumably `startBranch`, `authorEmail` and `authorName` in gitlab4j 5.8.0 (`legend-sdlc/pom.xml:100`). We could not verify the signature locally (Inference).
  - Author = committer = the user (section 9).
  - The client's `revisionId` reaches this point from `EntityModificationOperations.performChanges(..., revisionId, ...)` (`legend-sdlc-core/src/main/java/org/finos/legend/sdlc/core/entity/EntityModificationOperations.java:109, 136`; `api/GitLabEntityApi.java:230-237`).
- **Large saves (over `MAX_COMMIT_SIZE = 512` actions, `FileAccess:102`):**
  - It creates a temporary branch at the reference revision (`FileAccess:1115-1119`) and commits in 512-action chunks, retrying each up to 10 times (`FileAccess:103, 1028-1067, 1228-1289`).
  - Then `replaceTargetAndDelete`:
    - check that the target head still equals the reference (`FileAccess:1326-1329`; note this throws without a 409 status);
    - **delete the workspace branch** (`FileAccess:1334`);
    - **re-create it at the temp head** (`FileAccess:1352`);
    - delete the temp branch.
  - **Not atomic.** The branch briefly does not exist. A crash between delete and create loses the branch ref, although the commits stay reachable from the temp branch. Concurrent saves can interleave.
- **Avoid:** delete-and-recreate as a "ref update". It also fires branch-delete and branch-create webhooks (Inference).

**Reading files at a revision** (`FileAccess:215-259`):
- The primary path downloads the **whole repository archive** at the ref (`repositoryApi.getRepositoryArchive(projectId, ref)`, `FileAccess:262-273`) and filters entries client-side by directory.
- It falls back to `getTree(..., recursive=true, 100 per page)` plus per-file `getFile` **only on 429 or 406** (`FileAccess:230-247, 320-360, 362-370`).
- With a monorepo and the GitLab.com archive limit of 5 per minute, **copy the fallback, not the primary** (Inference).

**Workspace update ("rebase on main")** (`Workspace:798-900`):
- Create a temp branch.
- Open a throwaway MR from temp into the source branch (`Workspace:810`).
- Call the MR rebase API and poll `getRebaseStatus` for up to 600 × 1 s (`Workspace:822-834`).
- On success: create a `backup/...` branch, **delete the workspace branch, and re-create it from the rebased temp branch** (`Workspace:851, 868, 885`).
- **Avoid:** this abuses MRs as a rebase service and again uses delete and recreate.

**Review = MR:**
- `createMergeRequest(workspaceBranch → defaultBranch, title, description, labels, removeSourceBranch=true)` (`Review:368`).
- `approveMergeRequest(iid, mergeRequest.getSha())` passes the head SHA (`Review:439`).
- **Commit review:**
  - It checks the MR is open and `approvalsLeft == 0`, else 409 (`Review:571-581`). It also validates the project configuration.
  - It then calls `acceptMergeRequest(iid, message, shouldRemoveSourceBranch=true, mergeWhenPipelineSucceeds=null, sha=null)` (`Review:597`). Parameter names are inferred from gitlab4j.
  - **It does not pass `sha`**, so a commit pushed after review could be merged. **Copy the idea and add `sha`.**
  - Error mapping: 401/403 → 403, 405 → 409 "not in a committable state", 406 → 409 conflict (`Review:598-650`).
- **No merge-train or auto-merge use.**
- Update review = MR rebase API (`Review:684-729`).

**Versions = tags** (Base:1359-1412; `Version:70-100`):
- The next version comes from listing **all** tags and filtering by the `release-` prefix (`Version:105-107, 213-216`).
- A supplied revision is verified to be on the source branch via `getCommitRefs` (Base:1393-1398).
- It then calls `TagsApi.createTag(tag "release-x.y.z", commitSha, message "Release tag for version …")`, which makes an **annotated tag** (Base:1401).
- It creates a release **only if notes were given**, with `ReleaseParams(tagName, description=notes)` (Base:1402-1405).
- **There is no "is this commit green" check** before tagging.
- Concurrent version creation is resolved only by tag-name uniqueness (Inference).

**Builds** read pipelines by ref through `getPipelines(projectId, …, ref, …)` (`api/GitLabBuildApi.java:198`).

**Webhooks:** the GitLab backend registers or consumes none. A grep for hook found nothing under `gitlab/`.

**Retries** (`Tools:136-147`; Base:94-95, 1020-1023):
- `withRetries` retries 5 times with a 1 s initial wait, only on 408, 502, 503 and 504.
- **It does not retry 429 and ignores `Retry-After`.** Copy the retry wrapper but add 429 handling.

**Read-after-write.**
- legend-sdlc polls after every branch create or delete (`createBranchAndVerify` with 30 × 1 s, `deleteBranchAndVerify` with 20 × 1 s; `Tools:237-267`).
- This suggests GitLab branch-ref visibility can lag right after writes (Inference).

**Copy:**
- per-user OAuth tokens and per-user `GitLabApi`;
- a 409 on expected-revision mismatch;
- base64 actions;
- branch prefixes that encode user and workspace;
- the tree-plus-file fallback;
- the MR error mapping.

**Avoid:**
- non-atomic check-then-commit;
- delete-and-recreate as a ref move;
- whole-repo archive reads;
- merging without `sha`;
- tagging without a green check;
- no 429 handling;
- tokens in an unencrypted, non-HttpOnly cookie.

## 12. GitHub vs GitLab, per capability our backend interface needs

| Capability | GitHub | GitLab |
|---|---|---|
| **Atomic multi-file save** | Git Data API: blobs/tree (inline content) + commit + ref move; ~3 calls (G §1-2) | **One call**: `POST /repository/commits` with `actions[]`; atomic (§2) |
| **Save refused if branch moved (true compare-and-swap)** | Yes: GraphQL `updateRefs.beforeOid` (multi-ref, atomic) or `createCommitOnBranch.expectedHeadOid` (G §1) | **No API compare-and-swap.** `start_sha` on an existing branch errors (or overwrites with `force`); `last_commit_id` is per-file only; no update-ref endpoint (§2). True compare-and-swap needs a **git push with old-oid** (OAuth token over HTTPS). Otherwise use server-side serialization plus a head check (small race). |
| Stale-write status code | 422 "not a fast forward" (REST, experience) / GraphQL error | 400 with message text (all Commits API errors) |
| **Read tree at SHA** | `git/trees/{sha}?recursive=1`; truncated at 100k/7 MB; tree SHAs directly (G §3) | `repository/tree?ref=&path=&recursive=true`, keyset paging, 100/page; no truncation documented; subtree SHA via parent listing; `ref` documented as branch/tag only (§3) |
| Read file/blob | `git/blobs/{sha}` (raw media type), ≤100 MB | `/blobs/:sha/raw`, `/files/:path/raw?ref=`; >10 MB 5/min; batch blobs beta (§3) |
| Subdirectory snapshot | none (tarball is whole repo) | archive `?sha=&path=`, but 5/min on GitLab.com (§3) |
| **PR/MR create + merge with head check** | `PUT pulls/{n}/merge` with `sha` → 409 (G §6) | `PUT merge_requests/:iid/merge` with `sha` → 409; group can require `sha` (§6) |
| **Queue ("main always green")** | Merge queue: org-owned public repo or Enterprise Cloud private; GraphQL `enqueuePullRequest` only (G §6) | Merge trains: **Premium/EE**; REST `POST merge_trains/merge_requests/:iid` with `sha`; ≤20 parallel (§6). Free/CE fallback: fast-forward-only + pipelines-must-succeed + our own serial rebase/merge loop |
| SHA on main after merge | Always new (merge group) (G §6) | Merge commit or squash: new; **fast-forward: same SHA as tested source head** (§6) |
| **Green check for a commit** | GraphQL `statusCheckRollup` (checks + statuses) (G §6) | Pipeline status for `sha` (`/pipelines?sha=`, `commits/:sha` `last_pipeline`), plus external `statuses`; "Pipelines must succeed" is Free (§6) |
| **Tag + release** | ref create (lightweight) or tag object + ref (annotated); Releases API; rulesets protect tags (G §7) | `POST tags` (`message` makes it annotated), **100 per 30 min per project**; Releases API can create tag + release from `ref` in one call; protected tags are Free (§7) |
| Per-project version listing | `matching-refs/tags/<prefix>` | `tags?search=^<prefix>&order_by=version` (§7) |
| **Webhook push/tag** | `push` (≤3 tags/push), `create`/`delete`; no auto-retry; 3-day redelivery; HMAC `X-Hub-Signature-256` (G §8) | Push Hook / Tag Push Hook; **nothing fires if >3 refs per push**; auto-disable after 4 (temporary) and 40 (permanent) failures; 2-day history + resend API; Standard-Webhooks HMAC signing token (GA 19.1) or plain `X-Gitlab-Token`; 500/min per namespace on Free (§8) |
| **Per-user attribution** | App user token: user + app badge; REST custom author works but is unsigned (G §4, §9) | User OAuth token: committer = user; `author_*` override; GitLab instance signing applies to API commits per source (§4). Bot tokens: committer = bot. Impersonation/sudo: admin only. |
| User token lifetime | 8 h, refresh 6 months, single-use rotation (G §9) | **2 h**, single-use rotation (§9) |
| Rate limits | 5,000/h per user; 80 content-creating/min, 500/h secondary (G §5) | 2,000/min per user today; proposed Free 5,000/h + 100/min burst; raw 300/min per path; archive 5/min; tag create 100/30 min per project (§5) |
| Conditional requests | 304 free against the primary limit (G §5) | Not documented; cache by SHA (§5) |
| Repo limits | 3,000 entries/dir, 5,000 branches (guidance), 6 pushes/min, 1 merge/min (G §10) | 10 GiB/project on Free (read-only above), 5 GiB push, 100 MiB/file on Free; branch, directory and push-rate limits not documented (§10) |
| Private multi-user on Free | private repos fine; merge queue not on Free private | private namespace capped at **5 users** on GitLab.com Free; Self-Managed CE has no such cap (§1) |

## Implications for the backend interface

1. **`commit(branch, expectedHead, changes[], author) → newHead | Stale`** must be the primitive.
   - Each backend implements it its own way:
     - GitHub: Git Data API + `updateRefs(beforeOid)`.
     - GitLab: git push with old-oid, or Commits API + server-side per-branch lock + head check.
   - Declare an honest capability: `ATOMIC_EXPECTED_HEAD` (true compare-and-swap) vs `BEST_EFFORT_EXPECTED_HEAD`.
   - Our server must also **serialize writes per branch** regardless, so the best-effort path is safe against our own clients.
   - Map backend-specific errors to one `StaleHead` result: GitHub 422/GraphQL error; GitLab 400 + "has changed" text or a rejected push.
2. **Change model.** Use `Add`/`Update`/`Delete`/`Move(content?)` with full content. It maps 1:1 to GitLab `actions[]` and to GitHub tree entries. Chunking (legend-sdlc's 512) is a backend detail, but chunked saves break atomicity, so expose a `maxChangesPerAtomicCommit` limit and refuse above it rather than silently splitting.
3. **Reads are by immutable id.**
   - Use `resolve(ref) → commitSha`, `treeOf(commitSha, path) → treeSha`, `listTree(treeSha | commitSha+path)`, `readBlob(blobSha)`.
   - Cache everything by SHA. Never use whole-repo archives on the hot path.
4. **Review, merge and queue as capabilities:**
   - `REVIEW` (PR/MR create/merge with `expectedSourceHead`) is required.
   - `MERGE_QUEUE` is optional: GitHub merge queue; GitLab merge trains on Premium/EE.
   - `AUTO_MERGE` is optional (both have it).
   - `FF_ONLY_MERGE` is GitLab Free and GitHub "rebase" without a queue.
   - When `MERGE_QUEUE` is absent, the server runs its own serialized loop: rebase, wait green, merge with `sha`, retry if main moved.
   - `merge()` must return the **resulting main SHA** (merge, squash or fast-forward), never assume the source head.
5. **Green.** Use `commitStatus(sha) → PENDING|SUCCESS|FAILED|NONE`. GitHub uses `statusCheckRollup`; GitLab uses the pipeline for `sha` plus commit statuses. Versioning must require `SUCCESS` on the exact main SHA.
6. **Versions:**
   - `createVersion(projectPath, version, mainSha, notes)` produces a tag plus a release.
   - Tag names must be path-scoped (`<projectPath>/<x.y.z>` or similar) and URL-encoded in API paths.
   - Each backend declares tag-creation budgets (GitLab: 100 per 30 min per repo; that is per *GitLab project*, so it is shared by all model projects in the monorepo) and supports a prefix-listing query.
   - Protect tags (GitHub rulesets / GitLab protected tags) so only the server identity can create them.
7. **Events:**
   - `subscribe()` yields `RefMoved(ref, before, after)` from webhooks, verified with HMAC.
   - Webhooks are lossy and can auto-disable on both backends, so the interface needs `listRefs(prefix)` for **periodic reconciliation**, and the index must be idempotent on `(ref, after)`.
   - Avoid pushes touching more than 3 refs (both backends drop events).
8. **Identity:**
   - `withUser(token)` for writes, so attribution and permissions belong to the user; `withServiceIdentity()` for reads, index and webhooks.
   - Token refresh must be **single-flight per user** (single-use rotation on both: GitLab 2 h, GitHub 8 h).
   - Store tokens server-side or encrypted, not in plain cookies (legend-sdlc's mistake).
9. **Limits as data.** Each backend publishes `RateBudget` hints (writes/min, archive/min, tag-creates per window) and honours `Retry-After`/429 (GitLab) and 403/429 secondary limits (GitHub). Retry 429 with backoff; legend-sdlc does not.
10. **Hosting plan matters.**
    - GitLab.com Free private namespaces stop at 5 users and lack merge trains and required approvals.
    - GitHub Free private org repos lack the merge queue.
    - The interface must work in "no queue, no required approvals" mode, with the server enforcing review and green itself.

## Could not determine

- **Commit SHAs as tree `ref`.** Whether `GET /repository/tree?ref=<commit SHA>` is officially supported. The docs say "branch or tag"; legend-sdlc and practice use SHAs.
- **Limit on actions per commit.** No documented maximum number of `actions[]` per commit beyond the 300 MB body limit and the default JSON array limit of 50,000.
- **Create-on-existing-file error.** The exact message and code when a `create` action targets an existing file (presumably a Gitaly index error → 400).
- **Git push semantics.** Whether GitLab documents git-push `--force-with-lease`/old-oid semantics. We rely on git protocol behaviour, which the GitLab docs do not cover.
- **API commits and webhooks.** Whether commits made through the Commits API always emit Push Hook webhooks (expected, not stated on the pages read).
- **Signing of API commits.** Whether "Sign web-based commits" signs REST Commits API commits. Source suggests yes (`Repository#commit_files` passes `sign`); the docs mention only UI, Web IDE and MR commits.
- **Webhook retries and ordering.** Automatic webhook retry schedule for project webhooks, and webhook delivery ordering.
- **Conditional requests.** Whether REST conditional requests (`If-None-Match`) are supported generally and whether 304s count against rate limits.
- **Repository-level limits.** Branch count limits, files-per-directory limits, path length limits, and per-repository push or merge rate guidance (GitHub documents these; GitLab does not, as far as found).
- **Release transactionality.** Whether `POST /releases` creating a new tag from `ref` is transactional (tag rolled back if release creation fails).
- **Plan-limit start date.** When the proposed GitLab.com per-plan limits (Free 5,000/h, 100/min burst) take effect ("GitLab announces specific dates in advance").
- **CE coverage.** Whether every "Free" feature listed is present in the CE (open-source) build, as opposed to EE running unlicensed. The docs label tiers, not editions; we inferred CE = Free.
- **gitlab4j signatures.** Exact gitlab4j 5.8.0 parameter names for `createCommit`/`acceptMergeRequest`/`createMergeRequest` as called by legend-sdlc. The jar was not available locally; the meanings were inferred from argument positions.
- **Branch names with `/`.** Whether `/` in branch names conflicts with the Branches API wording ("Cannot contain… special characters except hyphens and underscores"). Practice and legend-sdlc show `/` works.
