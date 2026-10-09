# Start here: the build rebuild and the self-contained bump (for a session picking the program up)

Written 2026-10-07, corrected the same day after a cold read (`COLD_READ_2026_10_07.md`). Read this first, then the
plan (`docs/REBUILD_PROGRAM_2026_10_06.md`), then the brief of the phase you are working on (this folder). The plan
says what and why; the briefs say exactly how, with the files, the commands, the checks and the traps already hit.

Do not confuse this program, **the build rebuild**, with **the compiler rebuild**: the parked compiler plan
(`docs/EXECUTION_PLAN_2026_09_26.md`), which `AGENTS.md` on main still names as "current work" (an open decision,
§3). Do not follow that plan's §0 checklist: its worktree and branch belong to another line of work.

## 1. The program in brief

legend-lite reimplements FINOS legend-pure and legend-engine. The program has four parts, in this order, each built
on the one before. **The plan's §0 has the whole picture, in the user's own words; read it first.**
1. **A first-class Bazel build, starting with a fast compile of only the product.** Bazel knows every step, what it
   reads and what it makes; nothing outside Bazel (shell scripts, Java or Python programs, Node) drives the work;
   everything pinned, nothing read from the machine, the same on Linux, macOS and Windows. The first deliverable is
   done (Phase 0): a clean compile of all the Java we ship in 12.4 s, with a guard that the compile targets only
   compile. What is left of this part (CI's build lane, Node, scripts, checks) is Phase 8.
2. **The bump as a standalone piece** (Phases 1 to 7, the main body): moving to a new upstream release runs only when
   we move the pins and reads only upstream's files. Today, files mix upstream facts with our decisions, generators
   read our own code, and the compiler decides some calls by name. So this part restructures how the product boots and
   fixes the compiler: one implementation table keyed by function id, overloads picked exactly as legend-pure picks
   them, the default world generated from upstream alone, Pure.java as rows.
3. **Our own generators, separate from the bump** (Phases 1 and 7, then Phase 8): generators made from our own code run
   when that code changes; measurements of our engine that look like generators become tests.
4. **The tests, untangled, after the bump** (Phase 8): core, the relational corpus, the stress corpus, the PCT
   flavors, parser equivalence, the browser and UI tests. Each depends on exactly the code it exercises, each CI lane is
   a `//gates` suite, and none runs on Node.

After the bump lands: the compiler's debts (`docs/PARKED_WORK_LEDGER.md` PARK-5 to PARK-14), each fixed with a design
agreed first.

The north star, in the plan's section 1: three kinds of files, never mixed. **Upstream** (the pinned archives; only
the bump changes them), **ours** (hand-written decisions and code: the implementation table, legend-lite's own
`meta::legend::lite` declarations, the system metamodel), **generated** (made from upstream and one setting, the
module choice, only by the bump; committed and sealed). Never a generator reading our code, never a hand edit of a
generated file, never a signature we typed ourselves.

## 2. Where everything is

| What | Where | Pushed? (2026-10-07) |
|---|---|---|
| The plan, the designs, the research, these briefs, the evidence | `main`: `docs/REBUILD_PROGRAM_2026_10_06.md`, `docs/BUILD_REBUILD_DESIGN_2026_10_05.md`, `docs/GENERATORS.md`, `docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md`, `docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md`, `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`, `docs/build-inventory/` (inventories, dossiers, experiments, censuses), `docs/build-inventory/program/` (this folder; its `evidence/` holds the audits, scripts and recorded results) | yes, since 2026-10-07: merged from the plan branch `docs/bazel-first-class-plan` (pushed, kept as history). Edit them on main or a branch from it, not in `runs/bazel-plan` |
| The code | `main`. Phases 0, 1 and 2 are on it (PR #25, PR #26, `ff70aef01`). | yes |
| Phase 3 | branch `build/phase3` (worktree `runs/build-rebuild`): main plus the five Phase 3 commits; state in §3 | yes (pushed 2026-10-07; not landed) |
| The program's debts | `docs/PARKED_WORK_LEDGER.md` rows PARK-5 to PARK-14 (on `build/phase3`; they land with Phase 3), anchored by `core/src/test/java/com/legend/ParkedWorkLedgerTest.java` | with Phase 3 |
| Who works on what | `docs/IN_FLIGHT.md` on `main` (the program's entry lists every core file each phase touches) | yes |
| Gate results and what moved, per change | `docs/GATES.md` (one entry per landing) | with each landing |
| The pinned upstream sources | Bazel repositories `@legend_engine_src` and `@legend_pure_src` (`$(bazel info output_base)/external/+http_archive+legend_engine_src`, `...legend_pure_src`; use `find -L` inside them); release pins in `release.MODULE.bazel` (engine 4.145.0, pure 5.99.0) | — |
| Evidence of past runs | `evidence/` in this folder: its README lists every audit report, script and recorded result the briefs cite, and what stayed in the code worktree's scratch (`runs/build-rebuild/runs/homework/`: raw logs, build outputs, big experiment outputs) with how to regenerate each | yes (since 2026-10-07) |
| The audit agents | `~/.claude/agents/` (`auditor`, high effort, a 15-minute budget; `auditor-max`, maximum effort, no budget): session tooling, not in the repo; a new definition loads only when a session starts | — |

## 3. State, and the next action

**State on 2026-10-07, after the planning session** (the one place this is kept; other documents point here):
- **The order is agreed and recorded in the plan's §4**: thirteen landings, L1 the CI landing first, L2 Phase 3, then
  3b, 6, 4, 5, PARK-5's fix, 7 (the bump is done there), then the typer's order and Phase 8. The lane set is
  `PHASE_8.md` §8; the measurements behind it are `evidence/phase8/CI_LANES_2026_10_07.md`.
- **L1a and L1b landed** (main `fd1b0ba77` and `d126b47e1`, 2026-10-07; GATES entries "Build rebuild L1a" and
  "L1b"): the lanes as suites, the product job, the downloads cache, the pinned actionlint and shellcheck, the manual
  hand targets; every browser harness a test on the pinned Chromium (DataCube's eight, Query's, the site's, Studio's),
  Linux only in CI by the tag `ci-linux-only`, which a dispatch with `linux_only_tests=everywhere` lifts. Both runs
  green on all 51 jobs. L1b's run was NOT warm (32 minutes): the runner image's version sat in the cache key and GitHub
  rotates images run to run, the prefix fallback `bazel-repo-linux-` matched the `linux-arm` entry, and the Linux
  cache was 19 GB because the product job saved after analysing everything (every pool's jars), past GitHub's 10 GB
  cap (`evidence/phase8/CI_LANES_2026_10_07.md` §8). **The fix landed** (main, commit "Build rebuild L1b, the cache fix", 2026-10-07; audited post hoc with L1c): the key without the image, `=` before the hash so one platform cannot prefix-match another, the save right after the product build. Its run (37661408648, green on all 51 jobs, 38 minutes) also ran the Linux-only harness tests on macOS and Windows: all pass; the cost is about 7 minutes of wall clock, all of it macOS's `datacube` lane (`evidence/phase8/CI_LANES_2026_10_07.md` §9). It measured the downloads cache at 16 GB on Linux and 2 GB on macOS and Windows (saved: 6.3, 1.5, 1.5 GB; linux-arm's evicted by the 10 GB cap). The likely cause is Bazel 9's repo contents cache (the UNPACKED repositories), which defaults to `{--repository_cache}/contents`, inside the cached path; L1c's run measured it (§10): Linux: `~/.cache/bazel-repo` 16 GB, of which `contents/` (the unpacked repositories) 13 GB and `content_addressable/` (the downloads) 3.1 GB; macOS: 2.0 GB, `contents/` 1.0 GB, downloads 1.0 GB; Windows: 2.1 GB, `contents/` 1.1 GB, downloads 1.1 GB. The biggest external repository on every platform is GraalVM (631 to 717 MB); on Linux the hermetic LLVM and the two sysroots sit in `contents/` as well. So the downloads alone -- what the cache was meant to hold -- fit GitHub's 10 GB cap for all four platforms with room; the unpacked repositories are what overflowed it. **The fix for that is the next landing's** (`--repo_contents_cache=` on CI, or the `contents/` folder left out of the cache); OD-5, the remote cache, is decided and landed: BuildBuddy, L1e (the same commit as L1d).
- **L1c landed** (main, commit "Build rebuild L1c: the warehouse knows nothing about Bazel", 2026-10-07; GATES entry "Build rebuild L1c"; run 37668591690 green on every job, 18:40 UTC, 26 minutes to the first verdict; the Linux `ui` job failed once on a Maven Central 404 for duckdb_jdbc 1.5.5.1 (a fetch, not a test) and was rerun on the same commit): the server finds DuckDB's library, the Postgres extension and (with `--app`) the site beside its own executable, or where `--duckdb-library`, `--duckdb-extensions` and `--site` point; `--data` not given is a fresh temporary directory, said at start; `--app` is one user plus the site beside the program (`--open` stays its own flag, so a test can start the app without a browser; `//datacube:app` passes both). `ServerRunfiles`, the runfiles dependency, the `BUILD_WORKING_DIRECTORY` read, `warehouse_run`, its bash script, `hermetic_launcher`, the 10-argument limit and the "DO NOT RENAME" couplings are gone. `warehouse_folder` (`warehouse/defs.bzl`) is the image beside its files as one folder of real copies: `//warehouse:serve` without a site, `//datacube:app` with it; `//datacube:app_package` is the app folder as `datacube-app.tar.gz` (entries under `datacube/app/`, as built: no renaming without a shell, see the BUILD comment). `//:native` stays a compile (the compile-only guard caught the first cut, which had put the files in the image's `data`). Judged: `verify_app_test` starts the app folder from a plain directory with none of Bazel's variables, on every platform; `tests_native` runs the image with no library flag; the warehouse lane runs `bazel run //warehouse:serve -- --port 'x&y z'` and expects the refusal quoting the argument (U-6, every platform). Relative paths under `bazel run` are Bazel's runfiles folder, documented. **L1d and L1e landed** (one commit, "Build rebuild L1d and L1e: …", 2026-10-07; GATES entry "Build rebuild L1d and L1e"): the saved downloads cache without the unpacked repositories -- Bazel's contents cache stays on during a job; the saved path is `content_addressable/` alone since the follow-up "Build rebuild L1e, the cache path", the first form's negated pattern having saved everything; the follow-up's run saved Linux 2.99 GB, Linux arm64 2.64 GB, Windows 1.02 GB, macOS 0.99 GB (`gh cache list` after run 37683169884, 2026-10-07 20:35 to 20:55 UTC, green on every job): 7.6 GB for the four platforms together, under the 10 GB cap (the downloads cache is 3.1 GB on Linux, 1.0 GB on macOS, 1.1 GB on Windows (the cold run's product jobs, the unpacked repositories left out; Linux arm64 not measured, at most Linux's size): under the 10 GB cap for the four platforms together), the package's entries judged, the app test's temporary data checked on every platform, and BuildBuddy as the remote cache for every CI job (the event upload asynchronous: an outage is never a red build). Two runs: cold (37676021026 (19:38 to 20:00 UTC; the cache empty): green on 49 of 51 jobs, every lane's cache hits visible in its log already (jobs read what earlier jobs of the same run uploaded); the macOS and Windows product jobs failed on the two things the amended commit corrected -- `query` refusing the build-only `ci-small` config it was handed with the lane flags, and, on Windows, the product job's `--config=bazel10` refetch of every repository re-extracting the JDK over its own java.dll (held open by a worker) once the contents cache was disabled, so the contents cache stays on and is left out of the saved cache instead), warm (37679310981 (20:04 to 20:26 UTC, 22 minutes): green on every job, every job's build actions from the cache); the lane minutes before and after are the evidence's §11. Open from L1d: the Windows desk's Ctrl+C check; optional: a `.zip`, nicer archive paths. Proposed in the plan as L1f, the user's call: lane inputs (every lane's tests reach 32 to 41 of core's 41 libraries; the prerequisite of cached test results per push). **Decided 2026-10-07 (the user: "let's do the evidence way"; the compiler plan's D25):** the compiler rebuild resumes at its Now line (`docs/EXECUTION_PLAN_2026_09_26.md` §0): W1.0b landed 2026-10-08 (the receipt `evidence/compiler/BASELINE_2026_10_08.md`: a query compiles in under a millisecond, four fifths of it in the store resolver and the lowering; the model build and the whole-world typing are the user's wait); W0.8 landed 2026-10-08 (islands lexed in place; the stress model's build 1.5 s from 5.6 s); a W1.5 slice landed 2026-10-08 (the typed tree's JVM-salted iteration order, the scope id stable; the render census checks ids now); next the D24 cleanup (one substitution engine); the order after C1 is decided at C1 on the attributed defect list; the bump's Phase 3 branch is a reference. L2 (the bump's Phase 3) waits for the compiler work. Landed on the compiler line (2026-10-07, GATES entry "The erased TDS row is read only through its accessors"): the TDSRow fix handed over by the DataCube line, corrected after its audit and the reference lane (the untyped accessors read the erased row). The design: `docs/COMPILER_RIGHT_DESIGN_2026_10_07.md` rev 2; its measurements in `evidence/compiler/`.
- **`build/phase3` is untouched and waits for L1:** main plus the five Phase 3 commits, pushed, not landed. Its
  commits by subject: step 1 (ranking), step 2 (candidates and implementations by id), step 3 (forms, TDS functions,
  `agg`, boot-layer versions by resolved names), the corpus fixes, the audit's fixes. List them with `git log
  --oneline origin/main..origin/build/phase3` (a rebase changes the ids; these documents name subjects, not ids).
  After L1 lands it is rebased onto main (L1 touches no file it touched; L1 is code, so its CI run is rerun, on the
  fast CI), the ledger rows PARK-5, PARK-13 and PARK-14 get their landing line (plan §4, "The debts, placed") in a
  small commit on top, the tip carries `[skip ci]`, and it lands as L2. The user, 2026-10-07: no docs commit on the
  branch before then. Worktree clean.
- Old ids the briefs and the first audit cite, by subject: `5bc1550ae` and `d0041969c` step 1; `38566af11` and
  `a2f4da2fc` step 2; `165a1dbff` and `3912d3c12` step 3; `bc0de1f70` and `5e8a7c263` the corpus fixes; `ad1ed0175` the
  audit's fixes (amended after the local gate with documents, reason text and one comment only). Line numbers in the
  files the fix commit changed moved a little: cite the ledger's anchors by name.
- Checks on that code (the rebase added documents only): core tests, guards, census, spec tests; the six corpus passes identical to the
  pre-Phase-3 baseline; the reference lane byte-identical to its golden; PCT 17 of 17; the local gate 290 of 290.
- The program's documents: on main since 2026-10-07 (merged from `docs/bazel-first-class-plan`, pushed, kept as history),
  with the evidence folder.
- Phases 0, 1, 2, 2b: done (2 is on main; 2b was an experiment). **Phase 3: built, audited twice, every finding fixed
  or recorded, checked; not landed.** What is left: `PHASE_3_LANDING.md` §5.
- Next, in order (plan §4): **L1b's cache fix, L1c the warehouse without Bazel, L2 Phase 3, then 3b, 6, 4, 5, PARK-5's
  fix, 7**; Phase 8 after the bump. Each landing branches from `origin/main` after the previous one lands (not from the old `build/rebuild`).
  Homework pulled forward: U4-1 (the real Phase 4 world measured with the harness) runs right after L2; the bump's
  pins-only dry run (U7-3) during L3's homework.

**The briefs in this folder** (each: goal, agreed design with sources, what to read first, the code today, steps,
checks, traps, open decisions, stale statements, homework):

| File | Covers |
|---|---|
| `PHASE_3_LANDING.md` | what is left before Phase 3 lands |
| `PHASES_3B_6.md` | Phase 3b and Phase 6 |
| `PHASES_4_5_7.md` | Phase 4 (the default world), Phase 5 (Pure.java as rows by id), Phase 7 (the self-contained bump) |
| `PHASE_8.md` | the rest of the build: about 80 items, both decision series, the old workplan mapped |
| `DEBTS_RESOLVE_AND_TYPE_ONCE.md` | the research behind PARKED_WORK_LEDGER PARK-5 and PARK-6 |
| `COLD_READ_2026_10_07.md` | a fresh session's read of all of the above: the gaps it found (fixed), the scratch files the briefs lean on, its first three actions for each phase |

The briefs were written by read-only research passes before some corrections; each starts with a note of what was
applied to the plan afterwards. Where a brief and the plan disagree, the plan wins.

**Decided on 2026-10-07** (the planning session; each with its reason in the plan's §4 or the brief named):
- PARK-5: option (a). Phase 3 lands with it recorded; the full fix is L7, right after Phase 5 (not after the program:
  Phases 4 and 5 touch the same call sites and change where declarations come from). Homework for L2: one timing of
  the in-tab compile, main's WebAssembly against Phase 3's, so the browser is not worse than the probe's +18%.
- L1's shape (`PHASE_8.md` §8): OD-10 now (`//:update_generated` manual); Meas-1 (a) now (the judge passes manual);
  Test-6 and Test-7 (suites and the lane guard); Test-8: run `postgres_live` once, then a lane or deleted with a
  reason; CI-4 and CI-5 (actionlint as a test, actions pinned by commit); the cache: the downloads only, per
  platform (OD-5: the remote cache, decided and landed as L1e on 2026-10-07, before L10); OD-3 (a) for exec's four harness commits
  (P4-02, P4-03, P4-04, P4-08: the harnesses as tests), (b) for the rest; the Linux-only browser tests by
  `target_compatible_with`, not by a lane.
- The lane names: `product`, `core`, `checks`, `corpus_duckdb`, `corpus_h2`, `pct_duckdb`, `pct_h2`, `pct_postgres`,
  `pct_channel_b`, `parser_equivalence`, `stress`, `warehouse`, `datacube`, `ui`, `sdlc`, plus the manual `heavy`;
  `spec`, `json`, `pure-protocol` and the engine-runner smoke test in `core`; `//wasm`'s tests in `datacube`.

**Open decisions waiting for the user** (ask before the phase they block; each brief gives options and evidence):
- `AGENTS.md`'s "Pushing to main" still describes PRs (rule 2 "restored by P8-01", rule 3 "fix it in a PR"), and so
  do `gates/BUILD.bazel`'s header ("goes through a PR instead") and the root `progress.txt` (stale since April);
  since 2026-10-06 there are no PRs (`PHASE_8.md` OD-9). Recommended: no PRs for any work in this repository; L1
  rewrites `gates/BUILD.bazel`'s header; `AGENTS.md` is a docs-only commit when the user says so.
- **Phase 3b and 6:** `PHASES_3B_6.md`, section 8 of each phase. Among them, 3b-O1 decides whether 3b builds the
  "platform's own Pure" row kind (the plan's decision 1 asks for it; Phase 4's D4-4 needs it): that is its one owner.
- **Phase 4:** `PHASES_4_5_7.md` §2.8, D4-1 to D4-12 (which bodies the closure follows; the three legacy TDS functions
  legend-engine does not offer; the TDS row kind; where the result views load; which parser the generator uses;
  seeding by name or id; the name lists that change; the six hand enums; the primitives; the file's name; the boot
  budget and Phase 4b; the 109 unrowed versions). Also: which harness proves "the demos' queries executed"
  (§1.4 item 3), and the boot measure `//wasm:startup` is a Node program Phase 8 removes (measure the boot before Node
  goes, or port the measure).
- **Phase 5:** `PHASES_4_5_7.md` §3.8. **Phase 7:** §4.8 (D7-4 and D7-7 are the same questions as `PHASE_8.md`
  OD-6/OD-10 and GENERATORS.md's assignment of the writer-and-diff-test guard to Phase 7).
- **Phase 8:** `PHASE_8.md` §3.4, OD-1 to OD-17, and design D1, D4, D6, D7 (§3.2); when its independent items start
  relative to 3b (its §7 proposes the CI cache first: not agreed); deleting `//core:probe` (design D8) also takes away
  the parked compiler plan's probe (`ChannelB.java:223`, a test service file, `tools/untangle/*.py`).

## 4. How we work (the user's rules; each one was learned the hard way)

**Before code**
- Plan each phase with the user and get agreement before writing code. Explain plainly: short sentences, no jargon,
  labels explained or dropped, one settled answer.
- Find the root cause before fixing anything; agree the design when it is not obvious. Never fix a slowdown with a
  cache first: fix the algorithm, and add a cache only if still needed, keyed on content.
- Never invent a new mechanism when an existing one (the implementation table, modules, manifests, the generators,
  the ledger) does the job.
- No hacks, no workarounds, nothing pushed around quietly. A shortcut that must stay is a row in
  `docs/PARKED_WORK_LEDGER.md` (date, who decided, why, what we do today, cost, acceptance, an anchor test), and it
  leaves only by being fixed (or, where the row allows keeping a behavior, by a `docs/SEMANTICS_REGISTER.md` row that
  replaces it). If a guard test rejects a change, the guard is usually right: stop and rethink.
- Never write bare "native": "upstream native" is upstream's `native function` keyword (a Java body); "platform-lowered"
  is what our Pure.java declares.

**While coding**
- Core edits: the `docs/IN_FLIGHT.md` announcement lands on main first, listing every core file the change touches
  (standing authorization to push IN_FLIGHT updates to main). No worktree sits on main: make one for the edit,
  `git -C runs/build-rebuild worktree add --detach runs/inflight origin/main` (inside a worktree's ignored `runs/`),
  edit and commit there with `[skip ci]`, `git -C runs/build-rebuild/runs/inflight push origin HEAD:refs/heads/main`,
  then remove the worktree.
- Never hand-edit a generated file; regenerate it with its narrow writer: `bazel run //core:update_generated` (DynaFn.java,
  Pure.java, CoreImports.java, engine-handlers.tsv, native-claims.tsv, prelude.pure), or the phase's own writer. The
  root `bazel run //:update_generated` also rewrites the ratchets, the ladder and other measurements (`BUILD.bazel`):
  if you use it, read `git diff` and give every moved ratchet, golden or ladder line its own reason.
- Any moved pin, ceiling, ratchet or allowlist entry carries a dated justification naming the task.
- No local paths (home or temp directories) or binaries in anything committed. Temp files go in the worktree's
  `runs/`, never `/tmp`. Use `git -C <dir>` / `env -C <dir>`, never `cd <dir> &&`.
- Run only the targets the change touches during the work. At most two heavy Bazel jobs on the machine at once
  (`docs/IN_FLIGHT.md`, `gates/BUILD.bazel`'s header): check other sessions first (`ps` for Bazel servers and test
  JVMs); do not stack worktrees or agents that run Bazel.

**Landing (no PRs, since 2026-10-06)**
1. An audit by an independent agent (`auditor`; `auditor-max` when the user asks for maximum effort).
2. Fix what it finds; rerun the checks the fixes touch.
3. Write the change's `docs/GATES.md` entry (what moved and why, with the numbers).
4. Anything the change cites must be on main (the program's documents are, since 2026-10-07). A docs-only commit to main
   carries `[skip ci]` when it holds any file that is not `.md` (CI skips only Markdown and `progress*.txt`).
5. Push IN_FLIGHT's entry to main if it is not there yet (above).
6. Rebase onto the latest `origin/main`, then the local gate once: `bazel test --lockfile_mode=error //gates:local`. The
   commit message says "local gate: //gates:local green" (AGENTS.md, "Pushing to main", rule 1).
7. The tip commit message ends with `[skip ci]`: it stops main from running CI a second time when the commit lands
   (CI runs on pushes to main only; the branch run below is the verdict). Push the branch.
8. Dispatch one full CI run on the branch: `gh workflow run gate.yml --ref <branch> -f gates= -f platforms=all`
   (`gates=` empty means every lane; a run takes 40 to 57 minutes). For a throwaway run of affected lanes only, name
   them in `-f gates=...`; a full run is needed when `MODULE.bazel`, a toolchain, `.bazelrc` or a workflow changes.
   When the change touches the parser, lexer, protocol, parser-equivalence or `MODULE.bazel`, also
   `gh workflow run diagnostics.yml --ref <branch>` (its own trigger is a push to main, which `[skip ci]` suppresses).
9. When it is green, push that exact commit to main: `git push origin "${SHA}:refs/heads/main"` (the braces are
   required in zsh). If main moved meanwhile: docs-only commits, rebase and push; code commits, rerun CI.
10. No CI runs for the push itself (`[skip ci]`). Check the next nightly run on main
    (`gh run list --workflow gate.yml --event schedule`; cron 06:23 UTC; that it fires is not yet confirmed,
    `PHASE_8.md` CI-10); if it is red because of your commit, revert it at once (AGENTS.md, "Pushing to main", rule 3).
11. Commit trailers as the session's instructions give them.

## 5. How to run each check (exact)

| Check | Command | What "pass" means |
|---|---|---|
| Local gate | `bazel test --lockfile_mode=error //gates:local` | every test passes (290 targets; it does not include PCT or the corpus passes) |
| Core unit tests | `bazel test //core:core_tests` (24 per-package targets) | all pass |
| Guards (incl. the ledger anchors, identity counts, error shapes) | `bazel test //core:guardrails //core:census` | all pass; a shrink-only count that grows is a real finding, not a pin to raise |
| Spec tests (implementation table, ratchets) | `bazel test //spec:spec_tests` | all pass |
| Generated files current | `bazel test //:generated` | every diff test passes |
| PCT (heavy, 4 GB each) | `bazel test //pct:pct_duckdb //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b` (add `--local_test_jobs=2` when the machine is shared) | identical to before the change (17 targets) |
| **The six corpus passes** | `bazel build //spec:judge_host_duckdb //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse //spec:judge_database_warehouse` | every result file identical to a baseline built BEFORE the change (below) |
| **The reference lane** (manual, about 8 GB) | `bazel build //spec:reference_lane_report`, then `diff bazel-bin/spec/reference-lane/core_relational.txt spec/src/test/resources/reference-lane/core_relational.txt`; re-bless a deliberate move with `bazel run //spec:update_reference_lane`; `bazel test //spec:reference_lane` checks every disagreement class has a reason in `reasons.tsv` | AGREE not down, no new disagreement class without a reason; every moved line explained in the GATES entry |

**The corpus passes, done right.** `//gates:local`'s corpus checks only compare committed results; they do not rerun
the corpus. A compiler change must rerun the six passes and compare them with a baseline:
1. Before changing anything, build the six targets and copy each output directory (`bazel-bin/spec/judge_<pass>/`)
   aside, e.g. to `runs/homework/<phase>/judges_base/judge_<pass>/`.
2. After the change, build them again (check the outputs' times: a cached result is not a rerun).
3. Compare the RESULT files, not the logs: the rosters (`*-fail-roster.txt`, `*-skipped-roster.txt`), the registers
   (`*-engine-order-register.txt`, `*-unordered-register.txt`, `*-database-engine-order-register.txt`), the ledgers
   (`judge-host.tsv`, `judge-database.tsv`) and `verdict.txt`, 22 files in all. Compare their sorted lines without
   comment lines (`evidence/phase3/compare_judges.py <baseline dir> bazel-bin/spec` does it). Logs (`host.log`, `database.log`) differ by timings; ignore them. (The SQL the executor sends,
   seen only with `-Dlegend.diagnostics=dump-sql`, may renumber the reflection rows' function ids when the world
   gains functions; that is not in the result files.)
4. A database pass refuses to run after its host pass fails, so an empty database output means look at the host pass.
5. A difference is fixed, or explained to the user, before landing.

Running a hand-made corpus command after another Bazel command: rebuild the corpus targets first (cached), or the
execution root lacks the upstream trees ("legend-engine checkout not present").

**World changes (Phases 4 and 6)** use the experiment harness: `docs/build-inventory/manifest-world/experiments/README.md`
(it now builds its own inputs: `rerun.sh` runs `graph.py` and `index.py` first; swap `prelude.pure` on the classpath
without code changes; rerun each corpus pass's exact command with `e6_lanes.py`; the user side with `UserSideProbe`;
the browser with `bazel run //wasm:startup`).

## 6. Traps already hit (add to this list)

- The reference lane's census walk does not follow the execution root's symlinks: point `-Dlegend.engine.root` and
  `-Dlegend.pure.root` at the real directories (realpath) when running its programs by hand. Inside the upstream
  trees use `find -L` (a plain `find` finds nothing).
- `jfr print` keeps only 5 frames per stack by default; use `--stack-depth 200` to see who calls a hot method.
- macOS `strings` fails on class files (it reads `CAFEBABE` as a fat Mach-O); use `grep -a`.
- zsh: `echo ====` fails (`=` expansion); an unquoted `$VAR` holding several paths is one word; `"${SHA}:refs/..."`
  needs the braces, and `"$c:spec/x"` is read as a modifier too.
- Segmenting upstream Pure files: a doc string belongs to the element below it; keywords at a line start inside a doc
  string or block comment are prose; names come after every `<<stereotype>>` and `{tagged value}` (`docstart.py`).
- Upstream marks tests with test stereotypes and `::tests::` packages; `::test::` packages are upstream's test
  infrastructure, which ordinary code references.
- An ownership filter must cover the functions Pure.java implements, `CoreFn`'s forms, the system metamodel's own
  versions, `TdsLegacy`'s functions and the helpers forms recognize (`agg`, `col`): missing one breaks hundreds of
  tests.
- `bazel info` blocks while another build runs in the same workspace: run `bazel info output_base` once at session
  start, before any build, and keep the path for the session (never commit it).
- `e6_lanes.py` forces `-Xmx4g` on every pass (the DuckDB and warehouse passes really run with 1 GB) and names the
  configuration directory `darwin_arm64-fastbuild`: adjust both before trusting a heap or another platform.
- The warehouse corpus passes build the GraalVM server first: slow on a cold cache.
- A guard's growth is a finding: do not raise a shrink-only pin to make a change pass.

## 7. Reading order

1. This file. 2. The plan: sections 0 (what the whole program is for) to 5. 3. `docs/PARKED_WORK_LEDGER.md` (PARK-5 to PARK-14). 4. The brief of the
next phase in this folder, and the documents it lists. 5. `docs/IN_FLIGHT.md` on main, to see who else is working.
