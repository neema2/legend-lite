# Independent review: Bazel P0 PRs #15, #16, #17, #18 (2026-10-03)

Reviewer: an agent that wrote none of the four PRs. This was a read-only review: nothing was pushed, commented, approved or merged.
I measured each PR against the Done-when of its items in `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`
(branch `docs/bazel-first-class-plan`) and against `AGENTS.md`.

Heads reviewed: #15 `e5e350b28`, #16 `e7b856d31`, #17 `441a5c6d6`, #18 `8faaea7c8`. All four are based on `fcdd1e606`. `main` is now at `aea95fec3`.

| PR | Items | Verdict |
|---|---|---|
| #15 | P0-01, P0-02, P0-03 | **APPROVE WITH NITS** (one should-fix, in the docs) |
| #16 | P0-04, P0-05 | **APPROVE WITH NITS** |
| #17 | P0-07, P0-09, P0-10, P0-11, P0-12 | **REQUEST CHANGES**: P0-10's Done-when (A12 pin tests) is not met |
| #18 | P0-06, P0-08, P0-13 | **REQUEST CHANGES**: the new `//core:postgres_arm_test` runs in no CI lane |

---

## #15 · CI runs every test and fails loudly (P0-01, P0-02, P0-03)

**Verdict: APPROVE WITH NITS.**

### Findings

1. **should-fix**: `docs/GATES.md:10-27` (the lane table) is not updated.
   - It has no row for `misc` or `build`.
   - The `browser` row still says "every `//datacube` target tagged `browser-ci`", but the query now also takes `//query:*` and `//site:*` (`gates-run.yml:167`).
   - The **Generated files** bullet (`docs/GATES.md:31-34`) lists label globs rather than `//:generated`.

   **Fix:** add the two rows, widen the browser row to `//datacube`, `//query` and `//site`, and name `//:generated` in the bullet. A docs-only change; `.md` files do not trigger CI.
2. **nit**: `BUILD.bazel:18-29` and `:34-43` list the same four packages twice: once in the `//:generated` test_suite and once in `//:update_generated`'s `additional_update_targets`. The old query found a new package automatically. Now a package that adds `write_source_files` but is missing from the suite drops out of CI silently. The comment "A missing suite fails the build, not silently" is true only for a misspelled label.

   **Fix:** use one list, `_GENERATED_PACKAGES = ["//core", "//datacube", "//docs", "//parser-equivalence"]`, and derive `tests = [p + ":update_generated_tests" for p in …]` and `additional_update_targets = [p + ":update_generated" for p in …]` from it. Also reword the comment to "a misspelled suite fails the build; a new package must be added to this list". A later guard (P6) can compare against the query.
3. **nit**: `gates-run.yml:142`: the step is named `bazel test ${{ matrix.lane.targets }}`, so in the build lane it reads "bazel test //...". **Fix:** `name: bazel ${{ matrix.lane.key == 'build' && 'build' || 'test' }} ${{ matrix.lane.targets }}`, or a neutral "the lane's targets".
4. **nit**: PR text and commit: "Every CI run step uses `set -euo pipefail`". The job sets `defaults.run.shell: bash`, and with an explicit bash GitHub already runs `bash --noprofile --norc -e -o pipefail {0}` (confirmed in the browser job's log). So the old browser loop's `set -u` never turned off `-e` or pipefail; the new lines are belt and braces. The real silent failures this PR fixes are these two:
   - `for t in $(bazel query …)`: a failed command substitution in a `for` list does not trip `-e`;
   - `< <(bazel query … 2>/dev/null)`: a process substitution's status is ignored.

   **Fix:** reword one sentence in the PR text: name those two, and keep the `set` lines as explicit belt and braces.
5. **nit**: the build lane's `actions/cache` saves the largest disk cache of any lane. The Windows build step took 22 min, and the post-job cache save was still running 9 min later. The repository is already over GitHub's 10 GB cache limit (23.8 GB active), so this adds churn. It is not a correctness problem, and the cache already thrashes. **Fix, optional:** give the build lane `actions/cache/restore` only, or a `save-always: false` variant.
6. **nit**: `.bazelignore` covers the two `npm_link_all_packages` roots, as the plan asks. `pure-protocol/package.json` and `query-store/package.json` also exist, so an editor `npm install` there creates a `node_modules` that is not ignored. This is harmless unless a package ships a BUILD file. **Fix, optional:** add both, or say in the comment why only the two roots are listed.
7. **Out of plan scope, but declared and honest:** commit `645aa9c2a` drops the root `target/` ignore and `tools/engine-runner/.gitignore`. `experiments/backend-probes/*/.gitignore` still ignores the Maven harness's `target/`. Nothing in the Bazel build writes `target/` or `cp.txt`. Acceptable.

### Verified, and how
- **P0-02, suite equivalence:** in `runs/p0-ci`, `bazel query 'tests(//:generated)'` and the old `attr(name, "update_generated.*_test", tests(//...))` both return the same 13 tests: diff empty.
- **P0-02:** no `bazel query` remains in the test step. The browser step now captures its query in an assignment, so a failed query fails the step under `-e`, and an empty list is refused.
- **P0-03, Done-when "every non-manual test is in some lane":**
  - Query: `(tests(//...) except attr(tags,'manual',tests(//...))) except tests(<every lane target>)`.
  - Result in `runs/p0-ci`: empty.
- **`read -r -a targets <<< "$LANE_TARGETS"`:** jq emits single-space lists, and a here-string ends in a newline, so `read` returns 0. Unlike the old `for t in $LANE_TARGETS`, it does no pathname expansion. Labels starting with `//` keep their MSYS exemption (`MSYS2_ARG_CONV_EXCL`).
- **CI (run 37143879195):**
  - The Linux build lane passed (8m55s) and the macOS low-memory build lane passed (7m49s).
  - The Windows build lane's `bazel build //...` step passed (18:33:54 to 18:56:16Z; the job was still saving its cache).
  - The Linux browser lane ran all 12 harnesses, `//query:verify` and `//site:verify` included.
  - The `checks` lane expanded to 20 targets.
  - The macOS lanes were still pending at review time.
- **actionlint:** passed in CI.

---

## #16 · Lock files are enforced (P0-04, P0-05)

**Verdict: APPROVE WITH NITS.**

### Findings

1. **nit**: `tools/bump/Bump.java:54` writes `java.util.List<String> … java.util.List.of(…)` although `java.util.List` is imported (`:17`). **Fix:** `private static final List<String> RELEASE_POOLS = List.of("maven_upstream", "maven_runner");`.
2. **nit**: two places still describe one pool and are now stale:
   - `Bump.java:38` (class javadoc: "then the upstream jar pool is repinned");
   - `Bump.java:101` (`"phase 1: move — MODULE.bazel, tools/oracle-pins.env, the upstream jar pool"`).

   **Fix:** "the release's jar pools (" + RELEASE_POOLS + ")".
3. **nit**: P0-05's Proof wants `bazel run //tools/bump -- --help` (no network) to print both pools. `Bump` has no `--help`, and its usage line does not list the pools. The PR text does not claim this proof, which is honest, but the proof is unmet. **Fix:** print `RELEASE_POOLS` in the usage message (`Bump.java:68`).

### Verified, and how
- **All 8 `maven.install`s** carry `fail_if_repin_required = True`, and `.bazelrc` has `common:ci --lockfile_mode=error`.
- **Bump still works.** In rules_jvm_external 7.1 the check is in `pinned_coursier_fetch` (`private/rules/coursier.bzl:563-567, 676, 715`), and it is skipped when `REPIN` or `RULES_JVM_EXTERNAL_REPIN` is set. Both are tracked in the extension's `environ`, so the repos re-evaluate when the variable changes. So:
  - `Bump`'s `REPIN=1 bazel run @<pool>//:pin` still works for both pools after it edits `MODULE.bazel`;
  - phase 2 (no REPIN) then sees fresh locks.
- **Bump's managed-version rewrites** (HikariCP, commons-lang3, httpcore, junit, guava) all fall inside `maven_upstream` (`MODULE.bazel:134, 179-190`), and `replaceOne` requires exactly one match. So no unrepinned pool is touched.
- **Only `maven_upstream` and `maven_runner`** use `LEGEND_ENGINE_RELEASE` or `LEGEND_PURE_RELEASE`.
- **`MODULE.bazel.lock` has no entries** for the maven extension (it is `reproducible = True`), for `npm`, or for the root's `use_repo_rule` repos. So `--lockfile_mode=error` cannot be tripped by a pool edit or by #17's https URL change.
- **No repository doc** tells anyone to run `bazel run @maven_x//:pin` without `REPIN=1` (git grep).
- **Locally:** `bazel mod deps --lockfile_mode=error` in `runs/p0-locks` exited 0.
- **CI caches:** keyed on `MODULE.bazel.lock`, `maven_*_install.json` and `.bazelversion`; this PR leaves them unaffected.
- **CI (run 37141402420):** green on Linux and Windows, including the Windows `checks` lane. The macOS `1`, `3` and `checks` lanes were pending; the other macOS lanes are green, so the lock file is accepted on all three platforms.

---

## #17 · The test environment is pinned (P0-07, P0-09, P0-10, P0-11, P0-12)

**Verdict: REQUEST CHANGES.** The code is correct and safe, but P0-10 is claimed without its Done-when.

### Findings

1. **blocker** (P0-10 Done-when): there is no pin test. The plan's Proof (A12) asks for two tests, and its Done-when says "Removing any pin fails a test (A12)":
   - `//tools/junit:pins_test`: a `junit_test` asserting that `java.io.tmpdir` equals `$TEST_TMPDIR`, that `Locale.getDefault()` is `en_US`, that `file.encoding` is UTF-8 and that the zone is GMT;
   - `//tools/java_run:pins_test`: a `diff_test` of a tiny `java_run` that prints its tmpdir's parent name, its locale and its encoding.

   Neither exists, so deleting a line from `JUnitMain.pinTempDirectory` or from `java_run`'s `pinned` list leaves every lane green.

   **Fix:** add both tests, and put them in a lane (`//tools/junit:pins_test` fits `checks`). Alternatively, retitle the PR as "P0-10 (pins only; A12 tests follow)" and leave P0-10 open in the plan.
2. **should-fix**: `tools/junit/JUnitMain.java:24-25`, the class javadoc, says "Outside Bazel this behaves exactly like ConsoleLauncher". It now throws without `TEST_TMPDIR` (`:69-75`). **Fix:** "Outside a Bazel test (no `TEST_TMPDIR`) it refuses to run."
3. **nit**: `tools/junit/defs.bzl:7-8`, the module docstring listing "the same settings", still names only `-Duser.timezone=GMT`. **Fix:** add the locale, the encoding, and the temp directory set by JUnitMain.
4. **nit**: `tools/junit/defs.bzl:45-50` and `tools/java_run/defs.bzl:54-60` pin `user.language` and `user.country` but not `user.script` or `user.variant`. The JDK fills those from the host, so a host locale with a script (zh_Hant_TW, sr_Latn_RS) yields `en_Hant_US`. **Fix:** add `-Duser.script=` and `-Duser.variant=` (empty values count as command-line settings, so the JDK leaves them alone).
5. **nit**: `tools/java_run/defs.bzl:53,73`: the scratch directory is a declared tree output, so anything left in it is cached with the action. On Linux and macOS `deleteOnExit` cleans up normally. On Windows, one example is DuckDB JDBC's native library, which it extracts to `java.io.tmpdir`: `deleteOnExit` cannot delete a loaded DLL, so it would stay in the scratch tree. This is not a correctness problem (the tree is not in `DefaultInfo`), but it bloats the cache. **Fix, optional:** a line in the comment, and a follow-up to assert the scratch dir is empty or to keep it out of the cache.
6. **nit**: P0-12 says every select gets a `no_match_error`. The PR adds it to the two selects that have no `//conditions:default` (`warehouse/defs.bzl:45`, `warehouse/BUILD.bazel:184`). Every other select in `warehouse/defs.bzl` (`:57, :164, :193, :200`) has a default and cannot fail, and `datacube/BUILD.bazel` no longer has any select. That is the right reading; say so in the plan's audit (P0-90) so the literal Done-when is not counted as missed.

### Verified, and how
- **TEST_TMPDIR refusal, non-Bazel uses:**
  - Every `java_test` in the repository goes through `junit_test`: `kind("java_test rule", //...) except attr(main_class, "JUnitMain", //...)` is empty.
  - Nothing else calls `JUnitMain` (git grep).
  - The **prerun child JVM** inherits the parent's environment (`ProcessBuilder` copies it, `JUnitMain.java:111`), so `TEST_TMPDIR` is present. The child re-sets `java.io.tmpdir` itself.
  - **`bazel run` of a java_test:** Bazel 9.2.0's `RunCommand` sets the test environment, including the tmp dir (the server jar's `RunCommand.class` references `getTmpRoot` and `maybeRelativeTmpDir`). So `bazel run` keeps working.
  - Running from the IDE does not use `JUnitMain`.
- **Windows launcher:** no environment variable is expanded in `jvm_flags`. The new flags contain no spaces, and `java_run`'s `scratch.path` is execroot-relative, so it holds no spaces even under a user profile with spaces.
- **EmbeddedPostgres** (`testing/.../EmbeddedPostgres.java:22-23, 88, 104`) uses no Unix socket, so moving its data directory under `TEST_TMPDIR` cannot hit macOS's 104-byte socket path limit.
- **P0-09:** `bazel query 'filter("^//", filter("\.md$", kind("source file", deps(tests(//...)))))'` in `runs/p0-env` is empty. The external upstream trees still contain `.md` files, which is irrelevant to `paths-ignore`.
- **P0-11:** `bazel build --nobuild --incompatible_disable_target_default_provider_fields //spec:gen_natives //spec:gen_dynafn //parser-equivalence:gen_fixtures //parser-equivalence:gen_own_corpus_draft` succeeded.
- **P0-07:** `LegendHttpServer(0)` plus `getPort()`. No bound literal port remains in `core/src/test`; the only `localhost:NNNN` strings are CORS origin values.
- **P0-12:** `MODULE.bazel` has no `http://` left.
- **Output stability:** on Linux and macOS a C/POSIX locale already maps to en_US, and JDK 18+ defaults to UTF-8, so the pins cannot change CI's verdicts. They only change desks with other locales, which is the intent. The PR says it re-ran every `java_run` and checked the 14 diff tests. CI's `checks` lane is green on Windows, Linux and macOS.
- **CI (run 37141717779):** green on all platforms except one macOS lane that was pending.

---

## #18 · The server can open Postgres; run-stress's invariant runs; one corpus suite (P0-06, P0-08, P0-13)

**Verdict: REQUEST CHANGES.**

### Findings

1. **blocker**: `//core:postgres_arm_test` (`core/BUILD.bazel:343-359`) is in no CI lane. Lane `1` runs `//core:core_tests`, a separate `junit_test`, and no suite names the new target (git grep). So:
   - the test that is P0-08's Done-when ("a test opens a real Postgres through the product path") has never run in CI, on Windows included;
   - once #15 lands, P0-03's invariant "every non-manual test is in some lane" is broken.

   **Fix:** add `//core:postgres_arm_test` to the `7p` lane. In `gates-run.yml` that means appending it to both `pct7p` strings (`:48` and `:51`), or adding it to lane `1`. Then get a green Windows run of it.
2. **should-fix**: `docs/GATES.md:43` still says `//tools/deps:core_closure_test` checks that "the drivers are exactly three". They are now five jars. **Fix:** "the drivers are exactly the named ones: h2, duckdb, sqlite, postgresql (with its checker-qual)".
3. **should-fix**: the PR text says "The locks show only the driver and its annotations-only `checker-qual` moving pools, with no version changes". `checker-qual` did change version:
   - `@maven_upstream` resolved **3.49.0** (BOM-managed);
   - `@maven_core` resolves **3.42.0** (pgjdbc's own declaration);
   - so every classpath that used to see 3.49.0 now sees 3.42.0. That holds for PCT Postgres, and for anything that reached it through `legend-engine-xt-relationalStore-executionPlan-connection`, upstream's only other dependent of the driver.

   It is annotations only, so harmless in practice, but the sentence is wrong. **Fix:** correct the PR text. Optionally add a word to the `CoreClosureTest` comment (`tools/deps/CoreClosureTest.java:31-34`).
4. **nit**: `pct/BUILD.bazel:159-162`: `_POSTGRES_DEPS` names `@maven_core//:org_postgresql_postgresql` directly, while `pct_duckdb` takes `//core:drivers` (`:99-102`). **Fix:** use `//core:drivers`, so gate 7P runs on the product's driver set, the same rule as gate 6. The closure guard then covers it too.
5. **nit**: P0-08 asked for `target_compatible_with` on the new test. It has none, the same as `pct_postgres`. `@embedded_postgres` is keyed on the host (`tools/postgres/postgres.bzl:13-30`), so on an unpinned host the fetch fails either way. **Fix:** one line in the PR text saying this was left for P1-15.
6. **nit**: `docs/GATES.md:25`: the `app` row does not mention `live_snap_test`, which `//datacube:tests` now includes. The `browser` lane (`gates-run.yml` `"browser"` entry) still lists `//datacube:live_snap_test` too, so on Linux it runs in two lanes on two runners. **Fix:** add it to the app row. Then either drop it from the browser lane's targets, or say why it stays.
7. **nit**: `docs/GATES.md:46-47`: the rewrapped line is longer than the file's wrap width, and the next line is short. Rewrap.
8. **CI note:** the failing check, the Linux browser lane (job 111258431159), is a GitHub 500 while downloading `bazel-lib-v3.7.2.tar.gz`, 25 s in. It is infrastructure, not this PR. Re-run it.

### Verified, and how
- **Lock signatures are valid**, which matters once #16 makes them fatal. `bazel fetch --force --repo=@maven_core --repo=@maven_upstream` in `runs/p0-fix` re-evaluated both pools. It printed none of rules_jvm_external's "inputs … have changed" or "outdated input signature" warnings, so the repin is real. That also means it will pass with `fail_if_repin_required = True`.
- **Who loses `org.postgresql` from `@maven_upstream`:**
  - Its only upstream dependent was `legend-engine-xt-relationalStore-executionPlan-connection`, which reaches parser-equivalence through `extensions-collection-generation`.
  - Gate 8 and the diagnostics battery are green on #18, and nothing else references `@maven_upstream//:org_postgresql…` or `checker_qual` (git grep).
  - `maven_runner` keeps its own copy for the engine runner.
  - **pct still gets the driver**, through `_POSTGRES_DEPS`; gate 7P is green on Linux and Windows.
- **`checker-qual` exists in only one pool on each test classpath** (core). The `pools_are_disjoint` guard is green (`//tools/deps:all` 5/5 per the PR; CI `checks` green).
- **`CoreClosureTest`** (an AGENTS.md guard): the `DRIVER_JARS` change carries a dated reason (2026-10-03, P0-08). The test was renamed from `…NamedThree` to `…NamedOnes`.
- **P0-06:** `offered` is bound once, as a `Set` (`run-stress.mjs:40`), and used at `:159`. No `offeredNames` remains.
- **P0-13:**
  - `//spec:corpus_lanes` is deleted, and no BUILD file, workflow or current doc names it; only `spec/BUILD.bazel:164`'s history comment does.
  - `live_snap_test` is a Node `js_test`: it needs no Chromium, so it fits the `app` lane. Windows `app` is green (7m18s).
- **PostgresArmTest:** pgjdbc's default user is `user.name`. The test creates that role, with the identifier quoted, so the case is preserved.

---

## Merge order and cross-PR interactions

**Textual conflicts: none.** I ran `git merge-tree --write-tree` (read-only) on every pair, and on all four merged in sequence onto current `main` (`aea95fec3`). Every merge was clean, including the overlaps:
- #16, #17 and #18 in `MODULE.bazel`;
- #17 and #18 in `core/BUILD.bazel` (`core_tests_lib`'s `resources` against its `srcs`) and in `datacube/BUILD.bazel`;
- #15 and #18 have no overlapping files.

**Semantic interactions:**

1. **#16 and #18:** #18 repins `@maven_core` and `@maven_upstream`. Its signatures are valid (verified above), so merging #16 first does not turn #18 red. Still, merge **#16 before #18** and rebase #18. Then #18's own CI exercises `fail_if_repin_required = True` and `--lockfile_mode=error` against its new locks, instead of relying on my fetch check.
2. **#15 and #18:**
   - #18's `postgres_arm_test` must join a lane; the lane table lives in #15's file (blocker 1 above). If #15 merges first, #18 adds the target to the `7p` strings when it rebases. If #18 goes first, #15 must add it.
   - `live_snap_test` ends up in both the `app` and `browser` lanes; decide which.
   - #15's `build` lane and #18's `app` lane both build the native image on every platform. That cost was expected (P0-03 and P0-13 Risk).
3. **#15 and #17:** no clash.
   - #15's `//:generated` runs the 13 diff tests, and #17's `java_run` pins re-run every generator once. So, merged together, `//:generated` is the proof that the pins changed no output. That is P0-10's first Proof, and it passed on #17's CI.
   - #17's `.md` excludes do not touch the generated files: no generated file is `.md`, per the empty query.
   - #17's new pin tests (once added) belong in #15's `checks` lane.
4. **#16 and #17:** #17's `http` to `https` change is not recorded in `MODULE.bazel.lock` (the root's repo-rule repos are not in it), so `--lockfile_mode=error` is unaffected.
5. **#17 and #18:** `postgres_arm_test` is a `junit_test`, so it gets #17's pins and `TEST_TMPDIR`. `EmbeddedPostgres` already builds its data directory from `TEST_TMPDIR`, and it uses no Unix socket, so there is no path-length risk on macOS.

**Recommended order:** **#16, then #15, then #17 (after the A12 tests are added), then #18 (rebased: test added to the `7p` lane, docs and PR text fixed).**
- #16 first: it is the smallest and turns lock enforcement on before the one PR that edits locks.
- #15 next: its lanes then exist to catch the other two.
- #17 and #18 are independent of each other; either order works.
