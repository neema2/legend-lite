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

**State at the end of 2026-10-07** (the one place this is kept; other documents point here):
- **One branch holds everything:** `main` has every document (since 2026-10-07: the plan branch merged, `AGENTS.md`'s
  pointer, IN_FLIGHT's update), and `build/phase3` is main plus the five Phase 3 commits, rebased onto main the same
  day and pushed (not landed). Its commits by subject: step 1 (ranking), step 2 (candidates and implementations by id),
  step 3 (forms, TDS functions, `agg`, boot-layer versions by resolved names), the corpus fixes, the audit's fixes. List
  them with `git log --oneline origin/main..origin/build/phase3` (a rebase changes the ids; these documents name
  subjects, not ids). If main moves again with documents only, rebase again: no rerun. Worktree clean.
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
- Next phases, in order: **3b, 6, 4, 5, 7**, with Phase 8's items interleaved where they touch other files. Each phase
  branches from `origin/main` after the previous one lands (not from the old `build/rebuild`).

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

**Open decisions waiting for the user** (ask before the phase they block; each brief gives options and evidence):
- **Before Phase 3 lands:**
  - PARK-5 (calls to platform functions resolved again at every check: typing +18% on the eager compile probe, 0 to
    4% end to end). Three options: (a) land Phase 3 now with PARK-5 recorded and fix it after the program; (b) the
    contained fix first (the typer works out a call's names once and hands them to that call's checks: 4 or 5 files
    in `compiler/spec`; leaves cross-call checks and other passes); (c) the full fix first (the resolver records every
    call's names; about 200 places that build calls; the identity program's direction). Measure any fix with the same
    probe (`DEBTS_RESOLVE_AND_TYPE_ONCE.md`, "The measurement").
  - `AGENTS.md`'s "Pushing to main" still describes PRs (rule 2 "restored by P8-01", rule 3 "fix it in a PR"), and so
    do `gates/BUILD.bazel`'s header ("goes through a PR instead") and the root `progress.txt` (stale since April);
    since 2026-10-06 there are no PRs (`PHASE_8.md` OD-9). Since 2026-10-07 `AGENTS.md` says this program lands without
    PRs; whether other work keeps a PR path is the open part.
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
