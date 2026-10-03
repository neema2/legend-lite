# G. GitHub API facts for a "GitHub backend" SDLC server

Research date 2026-10-03, from docs.github.com only. (Docs) means the claim is quoted or closely paraphrased from the cited page. (Inference) means it is our reading and is not stated in the docs.

Key URLs (short names used below):
- REFS = https://docs.github.com/en/rest/git/refs
- TREES = https://docs.github.com/en/rest/git/trees
- BLOBS = https://docs.github.com/en/rest/git/blobs
- COMMITS = https://docs.github.com/en/rest/git/commits
- TAGS = https://docs.github.com/en/rest/git/tags
- GITDB = https://docs.github.com/en/rest/guides/using-the-rest-api-to-interact-with-your-git-database
- GQL-GIT = https://docs.github.com/en/graphql/reference/git
- GQL-COMMITS = https://docs.github.com/en/graphql/reference/commits
- GQL-PULLS = https://docs.github.com/en/graphql/reference/pulls
- RL = https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- BP = https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api
- LIMITS = https://docs.github.com/en/repositories/creating-and-managing-repositories/repository-limits

---

## 1. Update a reference with force=false, and compare-and-swap

- `PATCH /repos/{owner}/{repo}/git/refs/{ref}` takes `sha` (required) and `force` (boolean, default `false`). With `force: false` the update must be a fast-forward, "to ensure you're not overwriting work". `force: true` allows overwriting. (Docs, REFS)
- Documented status codes are 200, 409 (Conflict) and 422 ("Validation failed, or the endpoint has been spammed"). (Docs, REFS)
- The docs do **not** say which code a non-fast-forward returns, and give no example error body. (Docs, REFS) From experience (not in the docs) it is **422** with `"message": "Update is not a fast forward"`. 409 is the code GITDB documents for "the Git repository is empty or unavailable" (Docs, GITDB). So our mapping should be: 422 plus that message means stale (our 409), and 409 means repo unavailable or empty (retry or 503). **Confirm this empirically.**
- **Atomicity:** the REST docs do not say whether the fast-forward check is atomic with the write. They also offer **no expected-old-sha parameter**. (Docs, REFS)
- The race this leaves (Inference). We create commit C with parent P = the client's expected head. If someone pushes D, a descendant of P, before our PATCH, then C is no longer a descendant of D and the fast-forward check rejects it. That case is safe. The unsafe case is a ref that has been moved **backwards or sideways** (for example a force-push or reset to an ancestor A of P): C still descends from A, so the update succeeds even though the client's expected value P is no longer the head. So "fast-forward only" is not the same as "head == expected".
- **True compare-and-swap exists in GraphQL:**
  - `updateRefs` "Creates, updates and/or deletes multiple refs in a repository… All updates are performed atomically, meaning that if one of them is rejected, no other ref will be modified. `RefUpdate.beforeOid` specifies that the given reference needs to point to the given value before performing any updates. A value of 0000…0000 can be used to verify that the references should not exist." `RefUpdate` has the fields `name`, `afterOid`, `beforeOid` and `force`. (Docs, GQL-GIT)
  - `createCommitOnBranch` takes `expectedHeadOid` ("The git commit oid expected at the head of the branch prior to the commit"). However, it "does not support specifying the author or committer… and will not add support for this in the future". (Docs, GQL-COMMITS)
  - `updateRef` (single ref) has only `refId`, `oid` and `force`, so it is **not** a compare-and-swap. (Docs, GQL-GIT)
- **Safest pattern:**
  1. Create the blobs, tree and commit through REST (full author control).
  2. Move the ref with GraphQL `updateRefs` using `beforeOid = expected head`, `afterOid = new commit` and `force: false`. Map a rejection to 409 stale.
  3. Alternatively, use REST PATCH with `force:false` and forbid force-pushes or deletions on workspace branches with a ruleset ("Block force pushes"; see section 7), which closes the backwards-move hole.

  `updateRefs` can also move several refs atomically (for example a branch plus a tag). (Docs, GQL-GIT)

## 2. Create a tree

- `POST /repos/{owner}/{repo}/git/trees`. Each entry has `path`, `mode` (`100644` file, `100755` executable, `040000` subdirectory, `160000` submodule, `120000` symlink), `type` (`blob`, `tree` or `commit`), and either `sha` or `content` (not both). (Docs, TREES)
- `base_tree`: "a new Git tree object will be created from entries in the Git tree object pointed to by base_tree and entries defined in the tree parameter". Entries with the same path overwrite base-tree entries. **If `base_tree` is omitted, files from the parent commit that are not in your `tree` parameter are deleted.** (Docs, TREES)
- **Delete a file:** `sha: null` ("If the value is null then the file will be deleted"). (Docs, TREES)
- **Nested paths:** the endpoint "creates a new tree… with nested entries". "If you specify both a tree and a nested path modifying that tree, this endpoint will overwrite the contents of the tree with the new path contents." So a single call can carry `models/a/b/C.pure` against `base_tree`. (Docs, TREES)
- `content` (inline UTF-8) can stand in for a separate blob call, which reduces the number of content-creating requests. (Docs, TREES)
- **Limits:** the Create-a-tree docs give no limit on entries or request size. (Docs, TREES; could not determine) The general limits are: single object enforced at 100 MB (1 MB recommended); push size 2 GB; directory width (entries in a single directory) 3,000; directory depth 50. (Docs, LIMITS)

## 3. Get a tree and get a blob

- `GET /repos/{owner}/{repo}/git/trees/{tree_sha}` accepts a SHA or a ref name. Passing `recursive` with **any** value (including `0` or `"false"`) turns on recursion; leave it out to disable recursion. (Docs, TREES)
- **Truncation:** "The limit for the tree array is 100,000 entries with a maximum size of 7 MB when using the recursive parameter." A truncated response has `truncated: true`. In that case, fetch subtrees one at a time without recursion. (Docs, TREES)
- **Reading a subtree efficiently:** walk from the commit's tree. Do a non-recursive GET at each path segment (`models` → `org.x` → `trades`), take the entry's `sha` for type `tree`, then GET that subtree with `recursive=1`. Tree SHAs are content-addressed, so caching by tree SHA is sound. (Docs for the mechanism, TREES; caching rationale is Inference.) Directory listing with the Contents API is capped at 1,000 files per directory (Docs, https://docs.github.com/en/rest/repos/contents), so use git/trees, not contents.
- **Get a blob:** supports "blobs up to 100 megabytes in size". The default JSON returns `content` base64-encoded. The media type `application/vnd.github.raw+json` "Returns the raw blob data". `POST` blobs accepts `encoding` `utf-8` (default) or `base64`. (Docs, BLOBS)

## 4. Create a commit: authorship and signing

- `POST /repos/{owner}/{repo}/git/commits` takes `message`, `tree`, `parents[]` (omitted or empty gives a root commit), `author {name, email, date}`, `committer {...}` and `signature`. "By default, the author will be the authenticated user and the current date." "By default, committer will use the information set in author." (Docs, COMMITS)
- So the REST Git Data API lets the server set `author` to the end user explicitly with **any** token type. The commit is then attributed in the UI by email match (Inference).
- **User access token (GitHub App acting on behalf of a user):** "the GitHub UI will show the user's avatar photo along with the app's identicon badge as the author". Audit logs show the user as actor with programmatic_access_type "GitHub App user-to-server token". (Docs, https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-with-a-github-app-on-behalf-of-a-user)
- **Signing/Verified:**
  - "Signature verification for bots will only work if the request is verified and authenticated as the GitHub App or bot and contains no custom author information, custom committer information, and no custom signature information, such as Commits API." (Docs, https://docs.github.com/en/authentication/managing-commit-signature-verification/about-commit-signature-verification)
  - So **REST commits with a custom author are unsigned ("Unverified"/unsigned)** unless we sign them ourselves (the `signature` param, an ASCII-armored PGP signature) (Docs, COMMITS).
  - GraphQL `createCommitOnBranch` commits "are automatically signed by GitHub if supported and will be marked as verified", authored by "the owner of the credential which authenticates the API request". (Docs, GQL-COMMITS)
  - If a "Require signed commits" rule is turned on, the REST path with a custom author will be rejected. (Inference from the rule plus the bot-signing conditions; see section 7.)
- The trade-off: user token plus GraphQL `createCommitOnBranch` gives a verified commit with author = user and a native compare-and-swap (`expectedHeadOid`). Costs: content is sent inline as base64 `FileAddition` (full file each time) and `FileDeletion` by path; there is no author override; the user's token is needed for each save. `FileChanges` paths must be unique, use `/` separators and have no leading slash. (Docs, GQL-GIT)

## 5. Rate limits

- **PAT or authenticated user:** 5,000 requests/hour (15,000 for Enterprise Cloud). (Docs, RL)
- **GitHub App installation token:** a minimum of 5,000/h. Installations with more than 20 repos get +50/h per repo; orgs with more than 20 users get +50/h per user; the cap is 12,500/h. Installations on a GitHub Enterprise Cloud org get 15,000/h. (Docs, RL)
- **GitHub App user access tokens and OAuth tokens** are subject to the user's limit, "combined with any requests that another GitHub App or OAuth app makes on that user's behalf and any requests that the user makes with a personal access token". (Docs, RL)
- **Secondary limits** (Docs, RL):
  - No more than 100 concurrent requests.
  - No more than 900 points/min for REST; GET/HEAD/OPTIONS cost 1 point and POST/PATCH/PUT/DELETE cost 5.
  - No more than 90 s CPU per 60 s real time.
  - **No more than 80 content-generating requests per minute and no more than 500 per hour.**
  - Exceeding a limit gives 403 or 429; honour `retry-after` or `x-ratelimit-reset`.
- The PR and Release create endpoints explicitly warn: "Creating content too quickly using this endpoint may result in secondary rate limiting." (Docs, https://docs.github.com/en/rest/pulls/pulls ; https://docs.github.com/en/rest/releases/releases)
- **Recommended practices** (Docs, BP):
  - Send requests serially, not concurrently.
  - "If you are making a large number of POST, PATCH, PUT, or DELETE requests, wait at least one second between each request."
  - Use webhooks instead of polling.
  - Back off at least one minute if there are no headers.
  - "Continuing to make requests while you are rate limited may result in the banning of your integration."
- **304s:** a conditional request (`If-None-Match` with ETag, or `If-Modified-Since`) "does not count against your primary rate limit if a 304 response is returned and the request was made while correctly authorized". (Docs, BP) Whether 304s count toward *secondary* limits is not stated.
- **Repo-level guidance:** "recommended maximum limit is 6 pushes per minute per repository"; Git reads up to 15 operations/s per repository. (Docs, LIMITS) Whether API ref updates count as "pushes" here is not stated.
- **Cost per save (Inference):** about 3 content-generating calls (one tree with inline `content` + one commit + one ref update), so the 80/min cap means roughly **26 saves/min per token**. On a single shared installation token that cap is org-wide.

## 6. Pull requests, merge queue and "green"

- **Create PR:** `POST /repos/{o}/{r}/pulls` with `head`, `base`, `title`, `body` and `draft`. **Merge PR:** `PUT .../pulls/{n}/merge` with `sha` (must match head, else 409) and `merge_method`; it returns 405 if the PR is not mergeable. `mergeable` is computed asynchronously (null means still computing). (Docs, https://docs.github.com/en/rest/pulls/pulls)
- **Enqueue:** there is no REST endpoint in the docs. GraphQL has `enqueuePullRequest` ("Add a pull request to the merge queue") with `pullRequestId`, `expectedHeadOid` and `jump` ("Add the pull request to the front of the queue"), and `dequeuePullRequest`. `enablePullRequestAutoMerge` is also available. `MergeQueueEntryState` is one of AWAITING_CHECKS, LOCKED, MERGEABLE, QUEUED or UNMERGEABLE. (Docs, GQL-PULLS) The CLI equivalent is `gh pr merge` (Docs, https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request-with-a-merge-queue)
- **Merge queue mechanics** (Docs, https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue):
  - The queue creates temporary branches with prefix `gh-readonly-queue/{base_branch}`. They contain the latest base plus the PRs ahead in the queue.
  - It dispatches a `merge_group` webhook (`checks_requested`). CI **must** trigger on `merge_group`; it is separate from `pull_request` and `push`.
  - "GitHub will merge all these changes into the base_branch once the checks required… pass."
  - You can configure the merge method (merge, rebase or squash), build concurrency (1–100), group size, "only merge non-failing" and a status-check timeout.
  - A merge queue cannot be enabled with branch protection patterns that use wildcards.
- **New SHA on main?** Yes, in effect. The merge-group branch "contain[s] different SHAs than the original pull requests" and is what gets merged (Docs, managing-a-merge-queue). So the SHA on main is never the workspace head. Version tagging must use the resulting main commit, found from the push webhook `after`. (Inference)
- **Availability:** "any public repository owned by an organization, or in private repositories owned by organizations using GitHub Enterprise Cloud." (Docs, merging-a-pull-request-with-a-merge-queue URL above)
- **Green:**
  - Check runs come from `GET /repos/{o}/{r}/commits/{ref}/check-runs` (limited to the 1,000 most recent check suites). Only GitHub Apps can create check runs. (Docs, https://docs.github.com/en/rest/checks/runs)
  - Commit statuses come from `GET .../commits/{ref}/status` (combined: `failure` if any error or failure, `pending` if none or any pending, `success` if all success); there are at most 1,000 statuses per sha and context. (Docs, https://docs.github.com/en/rest/commits/statuses)
  - The combined-status endpoint covers **statuses only**, so you must also check check-runs. (Inference from the two APIs being separate)
  - GraphQL `Commit.statusCheckRollup` "Represents the rollup for both the check runs and status for a commit", so it is the single best "is it green" query. (Docs, GQL-COMMITS)
- Repo guidance: 1,000 open PRs per base branch; "Pull request merge rate: 1 merged pull request per minute". (Docs, LIMITS)

## 7. Tags, releases and rulesets

- **Lightweight tag:** just create the ref with `POST .../git/refs` and `ref: "refs/tags/<name>"`. **Annotated tag:** first `POST .../git/tags` (`tag`, `message`, `object`, `type`, optional `tagger`), then create `refs/tags/<name>` pointing to the tag object's sha. "Creating a tag object does not create the reference that makes a tag in Git." (Docs, TAGS)
- A ref name "If it doesn't start with 'refs' and have at least two slashes, it will be rejected." You cannot create refs in an empty repository. (Docs, REFS) `GET .../git/matching-refs/tags/models/org.x/trades/` does prefix matching, which is handy for listing versions per project. (Docs, REFS)
- **Release:** `POST /repos/{o}/{r}/releases` takes `tag_name`, `target_commitish` (used only if the tag does not exist; defaults to the default branch), `name`, `body`, `draft`, `prerelease`, `make_latest` (`true`, `false` or `legacy`) and `generate_release_notes`. It "may result in secondary rate limiting" and requires push access. `GET .../releases/tags/{tag}` fetches a release by tag. (Docs, https://docs.github.com/en/rest/releases/releases)
- With many projects, set `make_latest: false` so the repo's "Latest" badge is not churned by whichever project released last. (Inference)
- **Rulesets for tags** (Docs, https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets):
  - Rules include Restrict creations, Restrict updates, Restrict deletions and Block force pushes.
  - Patterns use `fnmatch` syntax, so `models/**/*` should work.
  - Bypass can be granted to roles, teams or **GitHub Apps** (Docs, https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets).
  - Recommendation: restrict creation, update and deletion of `models/**` tags, with only our GitHub App on the bypass list, so versions are immutable and only created by the server.
- **Slashes in tag names:** allowed (refs need at least two slashes; `refs/tags/models/org.x/trades/1.2.0` is fine). docs.github.com has no further restriction that we could find. Git's `check-ref-format` rules apply (no `..`, no `~^:?*[\`, no trailing `.lock` or `/`, no component starting with `.`), but that is git documentation, not GitHub. (Could not determine on docs.github.com) Note that REST URLs that embed a ref with slashes (`/git/ref/tags/models/org.x/...`, `/releases/tags/{tag}`) should URL-encode carefully. The docs are silent on this. (Could not determine)

## 8. Webhooks

- **`push`** (Docs, https://docs.github.com/en/webhooks/webhook-events-and-payloads):
  - Fields: `ref`, `before`, `after`, `created`, `deleted`, `forced`, `base_ref`, `commits` (max 2,048), `head_commit`, `compare` and `pusher`.
  - It is not sent if more than 5,000 branches are pushed at once, and "Events will not be created for tags when more than three tags are pushed at once."
  - Payloads are capped at 25 MB; anything larger is not delivered.
- **`create`** fires when a branch or tag is created, with `ref`, `ref_type` (`branch` or `tag`), `master_branch`, `pusher_type` and `description`. It does not fire when more than three tags are created at once. **`delete`** has the same shape and the same 3-tag rule. (Docs, same URL)
- **Delivery guarantees:** "GitHub does not automatically redeliver failed webhook deliveries". You can redeliver manually or through the REST API (list deliveries, then the redeliver endpoint, for repo, org and app hooks). (Docs, https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries) "You can redeliver webhook deliveries that occurred in the past 3 days." (Docs, https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)
- **Best practices:** respond 2XX within 10 s and process asynchronously. Use `X-GitHub-Delivery` (a unique GUID, kept on redelivery) for idempotency. Check `X-GitHub-Event` and the `action` field. (Docs, https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks) Ordering guarantees are not documented. (Could not determine)
- **Signature:** the `X-Hub-Signature-256` header is `sha256=` followed by the HMAC-SHA256 hex digest of the raw payload, keyed with the webhook secret. Compare in constant time and treat the payload as UTF-8. `X-Hub-Signature` (SHA-1) is legacy. (Docs, https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)

## 9. Authentication for a multi-user server

- **GitHub App (recommended)** (Docs, https://docs.github.com/en/apps/creating-github-apps/about-creating-github-apps/deciding-when-to-build-a-github-app):
  - Fine-grained permissions and installer-chosen repositories.
  - Short-lived tokens.
  - Actions "indicate that the action was performed by the app on behalf of the user".
  - Installation rate limits scale.
  - Built-in centralized webhooks.
- **User access tokens (user-to-server)** (Docs, https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-with-a-github-app-on-behalf-of-a-user):
  - Access is the intersection of what the user can access, what the app has permission for, and where the app is installed.
  - Attribution shows the user's avatar with an app badge.
- **Expiry** (Docs, https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/refreshing-user-access-tokens):
  - The user token expires after **8 hours** and the refresh token after **6 months**.
  - Refresh via `POST https://github.com/login/oauth/access_token` with `grant_type=refresh_token`.
  - "Once you use a refresh token, that refresh token and the old user access token will no longer work". **Rotation is single-use, so refreshes must be serialized per user.**
  - Expiration can be opted out of, but GitHub advises against opting out.
- **OAuth app:** broad scopes (`repo` grants write to everything the user can access); tokens "do not expire until the person… revokes"; actions are not marked as app-performed; webhooks must be configured per repo or org; rate limits do not scale. (Docs, deciding-when-to-build-a-github-app)
- **Fine-grained PAT:** limited to a single owner and specific repos; org approval may be required (pending tokens can read only public resources); expiry is set by the user and may be capped by org policy. It cannot call the Checks API or reach multiple orgs. For integrations, "you should use a GitHub App". These are unsuitable as per-user server credentials. (Docs, https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)
- **Hybrid (Inference):**
  - Use the installation token for reads, the index and webhooks; it has the higher, scaling limit and does not count against users.
  - Use the user access token for writes (commit, ref update, PR, enqueue) so attribution and permission checks are the user's own. Secondary write limits then apply per user rather than to one shared token.
  - Alternatively, write with the installation token and set `author` explicitly. The commit is unverified (section 4), and permission enforcement becomes our job.

## 10. Other surprises

- **Directory width 3,000 entries** per directory (recommended limit). "Directories containing numerous frequently modified files can significantly increase repository maintenance costs…" (Docs, LIMITS) A package with thousands of `.pure` element files in one directory breaches this, so shard by package path.
- **5,000 branches** guidance (Docs, LIMITS). With one branch per workspace across all projects in one repo, this is a real ceiling, so delete merged and abandoned workspace branches.
- **6 pushes/min per repo** and **1 PR merge/min** (Docs, LIMITS). One monorepo for all projects concentrates this load; consider sharding repos by org or domain.
- **Recursive tree truncation** at 100k entries / 7 MB (Docs, TREES). A monorepo root tree will hit this, so always read per-project subtrees.
- **Empty repository:** Git Data endpoints return 409 and ref creation fails. Bootstrap with `PUT /contents/{path}`. (Docs, GITDB; REFS)
- **Contents API** parallel writes "will conflict… use these endpoints serially" (Docs, https://docs.github.com/en/rest/repos/contents). We avoid the Contents API anyway.
- **PR `mergeable` / merge refs** "becomes outdated without warning". Fetch the PR and poll `mergeable` instead. (Docs, GITDB)
- `recursive=0` still recurses (Docs, TREES). This is an easy bug.
- **GraphQL `FileAddition`** does no charset or line-ending normalization; GitHub recommends UTF-8 and consistent line endings. (Docs, GQL-GIT)
- **Not documented** (could not determine; see below): read-after-write consistency of refs and trees, path-length limits, case-sensitivity, and maximum files per commit.
- LFS is irrelevant for small `.pure` files. The object size recommendation is 1 MB (Docs, LIMITS).

---

## Risks and recommendations for our design

- **Do not rely on REST `force:false` as a compare-and-swap.** It only enforces fast-forward, and the error status and body are undocumented. Use GraphQL `updateRefs` with `beforeOid` (atomic and multi-ref), or `createCommitOnBranch.expectedHeadOid`. Also add a ruleset blocking force-push and deletion on workspace branches.
- If we stay on REST PATCH, map 422 "not a fast forward" to our 409, and map GitHub 409 to "repo unavailable" (retry/503). Verify the exact codes against a test repo.
- **Choose an authorship model explicitly:**
  - REST Git Data with a custom author gives no "Verified" badge, and fails if signed commits are required.
  - `createCommitOnBranch` with a user token gives verified commits authored by the user, but no author override and full-file base64 payloads.
  - Either way, use a GitHub App with user access tokens for writes.
- Budget for the **80 content-generating requests/min and 500/h** secondary limits. One save is about 2–3 creates; use inline `content` in the tree call to skip blob calls. A server-wide installation token caps the whole org at about 160 saves/hour (500/h ÷ 3). Per-user tokens spread this.
- Serialize writes per token and pause about 1 s between mutations under load. Implement `retry-after` and `x-ratelimit-reset` handling. Use ETag conditional GETs (304s are free against the primary limit).
- **Monorepo scaling limits:** 3,000 entries per directory, 5,000 branches, 6 pushes/min, 1 merge/min, and recursive tree truncation. Either shard repos or enforce branch garbage collection and directory sharding. A busy org will hit the 6 pushes/min guidance with plain saves.
- **Merge queue** needs an org-owned public repo or GitHub Enterprise Cloud (private), CI on `merge_group`, and the GraphQL `enqueuePullRequest` (there is no REST endpoint). The SHA landing on main differs from the workspace head, so tag from the push webhook `after` / `statusCheckRollup`, not from the PR head.
- **Green check:** use GraphQL `statusCheckRollup` (check runs plus statuses). REST combined status ignores check runs.
- **Versions:** create the release (or the tag ref) through our App only. Protect `models/**` tags with a ruleset (restrict create, update and delete; App on the bypass list). Set `make_latest: false`. Optionally create the tag and a ref atomically via `updateRefs` with `beforeOid = 000…0` (meaning "must not exist").
- **Webhooks are lossy:** no automatic retry, 3-day redelivery window, no tag events when more than 3 tags are pushed at once, and a 25 MB cap. Make the read index self-healing: run a periodic reconcile with `matching-refs/tags/models/` and the main head, keep dedupe keyed by `X-GitHub-Delivery`, and verify `X-Hub-Signature-256`.
- **User token refresh is single-use rotation**, so refreshes must be serialized per user, and the refresh token must be stored durably and atomically.
- Read project subtrees by walking path segments, then recursing on the project subtree. Cache by tree SHA; blobs are immutable by SHA, so cache them forever.

## Could not determine (from docs.github.com)

- The exact status code and error body REST returns for a non-fast-forward PATCH. The docs list only 200/409/422. From experience it is 422 "Update is not a fast forward". This is unverified here.
- Whether the REST fast-forward check is atomic against the ref value at write time (no documentation either way).
- Limits on entries per Create-a-tree request, request body size, or files per commit.
- Read-after-write consistency of refs and trees after writes (for example, whether an immediate GET of a ref or the new tree can be stale, or whether replicas lag).
- Path length limits and case-sensitivity behaviour for paths in trees. Git itself is case-sensitive; GitHub's docs say nothing.
- Whether API ref updates count toward the "6 pushes/minute" guidance, and whether they always emit `push` webhooks. Push events are documented for "commits or tags pushed"; that this includes API ref updates is our assumption.
- Whether 304 responses count toward secondary rate limits.
- Webhook delivery ordering guarantees.
- GitHub-specific tag-name restrictions beyond the `refs/` plus two-slash rule, and URL-encoding rules for refs containing slashes in REST paths.
- A REST (non-GraphQL) endpoint for merge-queue enqueue. None was found; GraphQL `enqueuePullRequest` is the documented API.
- Exact plan availability of rulesets for private repos. The page said "GitHub Team and GitHub Enterprise plans"; public-repo availability on Free was not confirmed in the fetched text.
