# Start here: the build rebuild and the self-contained bump (for a session picking the program up)

Written 2026-10-07. Read this first, then the plan (`docs/REBUILD_PROGRAM_2026_10_06.md`), then the brief of the
phase you are working on (this folder). The plan says what and why; the briefs say exactly how, with the files, the
commands, the checks and the traps already hit.

## 1. The program in two paragraphs

legend-lite reimplements FINOS legend-pure and legend-engine. Today, moving to a new upstream release (the "bump")
needs hand work: files mix upstream facts with our decisions, generators read our own code, and the compiler decides
what a call means partly by name. The program makes the bump self-contained: every upstream fact is generated from
the pinned upstream archives alone, by the bump alone, and sealed; everything we decide is hand-written in its own
place (above all the implementation table, keyed by function id); the compiler picks overloads exactly as legend-pure
does and asks the table how each one runs. Alongside, the build is rebuilt so "build" means compile only, every
generator and test runs on its true trigger, and Node leaves the build.

The north star, in the plan's section 1: three kinds of files, never mixed. **Upstream** (the pinned archives; only
the bump changes them), **ours** (hand-written decisions and code: the implementation table, legend-lite's own
`meta::legend::lite` declarations, the system metamodel), **generated** (made from upstream and one setting, the
module choice, only by the bump; committed and sealed). Never a generator reading our code, never a hand edit of a
generated file, never a signature we typed ourselves.

## 2. Where everything is

| What | Where |
|---|---|
| The plan, the designs, the research, these briefs | branch `docs/bazel-first-class-plan` (worktree `runs/bazel-plan`): `docs/REBUILD_PROGRAM_2026_10_06.md`, `docs/BUILD_REBUILD_DESIGN_2026_10_05.md`, `docs/GENERATORS.md`, `docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md`, `docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md`, `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`, `docs/build-inventory/` (inventories, dossiers, experiments, censuses), `docs/build-inventory/program/` (this folder) |
| The code | `main`. Phases 0, 1 and 2 are on it (PR #25, PR #26, `ff70aef01`). |
| Phase 3 (not landed yet) | branch `build/phase3` (worktree `runs/build-rebuild`): 4 commits on `293318dda`, plus uncommitted audit fixes. `PHASE_3_LANDING.md` says what is left. |
| The program's debts | `docs/PARKED_WORK_LEDGER.md` rows PARK-5 to PARK-14 (on `build/phase3`; they land with Phase 3), each anchored by `core/src/test/java/com/legend/ParkedWorkLedgerTest.java` |
| Who works on what | `docs/IN_FLIGHT.md` on `main` (the program's entry lists every core file each phase touches) |
| Gate results and what moved, per change | `docs/GATES.md` (one entry per landing) |
| The parked compiler plan (not this program) | `docs/EXECUTION_PLAN_2026_09_26.md`: W2.1's `ids` and `catalog` items were taken over by Phase 3; W1.1b and the typing work stay there |
| The pinned upstream sources | Bazel repositories `@legend_engine_src` and `@legend_pure_src` (`$(bazel info output_base)/external/+http_archive+legend_engine_src`, `...legend_pure_src`); release pins in `release.MODULE.bazel` (engine 4.145.0, pure 5.99.0) |
| Scratch evidence of past runs (not in the repo) | each worktree's `runs/homework/` (for Phase 3: `runs/build-rebuild/runs/homework/phase3x/`, holding the audit report `AUDIT_PHASE3.md`, the corpus baseline `judges_base/`, probes). Anything a later phase needs from there is copied into a brief. |

**`AGENTS.md` on main still says "Current work: the compiler rebuild"** (the parked plan). That pointer is stale for
this program; correct it when Phase 3 lands (it is a shared file: say so in the commit).

## 3. State on 2026-10-07, and the next action

- Phases 0, 1, 2, 2b: done (2 is on main; 2b was an experiment).
- **Phase 3: done on `build/phase3`, audited (max effort, "ready after fixes"), fixes in progress, not landed.**
  The blocker is fixed with tests; the rest is in `PHASE_3_LANDING.md`, with one decision open for the user:
  land with PARKED_WORK_LEDGER PARK-5 (calls to platform functions resolved again at every check: typing +18% on the
  compile probe, 0 to 4% end to end) recorded, or fix it first.
- Next phases, in order: **3b, 6, 4, 5, 7**, with Phase 8's items interleaved where they touch other files.

**The briefs in this folder** (each: goal, agreed design with sources, what to read first, the code today, steps,
checks, traps, open decisions, stale statements, homework):

| File | Covers |
|---|---|
| `PHASE_3_LANDING.md` | what is left before Phase 3 lands |
| `PHASES_3B_6.md` | Phase 3b and Phase 6 |
| `PHASES_4_5_7.md` | Phase 4 (the default world), Phase 5 (Pure.java as rows by id), Phase 7 (the self-contained bump) |
| `PHASE_8.md` | the rest of the build: about 80 items, both decision series, the old workplan mapped |
| `DEBTS_RESOLVE_AND_TYPE_ONCE.md` | the research behind PARKED_WORK_LEDGER PARK-5 and PARK-6 |

**Open decisions waiting for the user** (ask before the phase they block; each brief gives options and evidence):
- Phase 3: land with PARK-5 recorded or fix it first; whether `AGENTS.md`'s "current work" pointer moves to this
  program.
- Phase 3b and 6: in `PHASES_3B_6.md`, section 8 of each phase.
- Phase 4: `PHASES_4_5_7.md` §2.8, D4-1 to D4-12 (which bodies the closure follows; the three legacy TDS functions
  legend-engine does not offer; the TDS row kind; where the result views load and who builds the "platform's own Pure"
  row kind; which parser the generator uses; seeding by name or id; the name lists that change; the six hand enums; the
  primitives; the file's name; the boot budget and Phase 4b; the 109 unrowed versions).
- Phase 5: `PHASES_4_5_7.md` §3.8. Phase 7: §4.8.
- Phase 8: `PHASE_8.md` §3.4, OD-1 to OD-17, and design D1, D4, D6, D7 (§3.2).
- Across phases: `AGENTS.md`'s "Pushing to main" still describes PRs (rule 2 "restored by P8-01", rule 3 "fix it in
  a PR"); since 2026-10-06 there are none (`PHASE_8.md` OD-9). A shared file: the user decides.

## 4. How we work (the user's rules; each one was learned the hard way)

**Before code**
- Plan each phase with the user and get agreement before writing code. Explain plainly: short sentences, no jargon,
  labels explained or dropped, one settled answer.
- Find the root cause before fixing anything; agree the design when it is not obvious. Never fix a slowdown with a
  cache first: fix the algorithm, and add a cache only if still needed, keyed on content.
- Never invent a new mechanism when an existing one (the implementation table, modules, manifests, the generators,
  the ledger) does the job.
- No hacks, no workarounds, nothing pushed around quietly. A shortcut that must stay is a row in
  `docs/PARKED_WORK_LEDGER.md` (date, who decided, what we do today, cost, acceptance, an anchor test), and it leaves
  only by being fixed. If a guard test rejects a change, the guard is usually right: stop and rethink.
- Never write bare "native": "upstream native" is upstream's `native function` keyword (a Java body); "platform-lowered"
  is what our Pure.java declares.

**While coding**
- Core edits: the `docs/IN_FLIGHT.md` announcement lands on main first, listing every core file the change touches
  (standing authorization to push IN_FLIGHT updates to main).
- Never hand-edit a generated file; regenerate it (`bazel run //:update_generated`, or the phase's own writer).
- Any moved pin, ceiling, ratchet or allowlist entry carries a dated justification naming the task.
- No local paths (home or temp directories) or binaries in anything committed. Temp files go in the worktree's
  `runs/`, never `/tmp`. Use `git -C <dir>` / `env -C <dir>`, never `cd <dir> &&`.
- Run only the targets the change touches during the work; one heavy Bazel server at a time on the machine (check
  for other sessions' builds first; do not stack worktrees or agents that run Bazel).

**Landing (no PRs, since 2026-10-06)**
1. An audit by an independent agent (the `auditor` agent type; `auditor-max` when the user asks for maximum effort).
   Agent definitions load only at session start.
2. Fix what it finds; rerun the checks the fixes touch.
3. Rebase onto the latest `origin/main`, then the local gate once: `bazel test --lockfile_mode=error //gates:local`.
4. The tip commit message ends with `[skip ci]`; push the branch (a branch push runs no CI).
5. Dispatch one full CI run on the branch: `gh workflow run gate.yml --ref <branch> -f gates= -f platforms=all`
   (`gates=` empty means every lane). For a throwaway run of affected lanes only, name them in `-f gates=...`; a full
   run is needed when `MODULE.bazel`, a toolchain, `.bazelrc` or a workflow changes.
6. When it is green, push that exact commit to main: `git push origin "${SHA}:refs/heads/main"` (the braces are
   required in zsh). If main moved meanwhile: docs-only commits, rebase and push; code commits, rerun CI.
7. After a push, watch main's CI; revert your own commit at once if it goes red (AGENTS.md, "Pushing to main").
8. Commit trailers as the session's instructions give them.

## 5. How to run each check (exact)

| Check | Command | What "pass" means |
|---|---|---|
| Local gate | `bazel test --lockfile_mode=error //gates:local` | every test passes (about 306 targets) |
| Core unit tests | `bazel test //core:core_tests` (24 per-package targets) | all pass |
| Guards (incl. the ledger anchors, identity counts, error shapes) | `bazel test //core:guardrails //core:census` | all pass; a shrink-only count that grows is a real finding, not a pin to raise |
| Spec tests (implementation table, ratchets) | `bazel test //spec:spec_tests` | all pass |
| Generated files current | `bazel test //:generated` | every diff test passes |
| PCT (heavy, 4 GB each) | `bazel test //pct:pct_duckdb //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b` | identical to before the change |
| **The six corpus passes** | `bazel build //spec:judge_host_duckdb //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse //spec:judge_database_warehouse` | every result file identical to a baseline built BEFORE the change (see below) |
| **The reference lane** (manual, about 8 GB) | `bazel build //spec:reference_lane_report`, then `diff bazel-bin/spec/reference-lane/core_relational.txt spec/src/test/resources/reference-lane/core_relational.txt`; re-bless a deliberate move with `bazel run //spec:update_reference_lane`; `bazel test //spec:reference_lane` checks every disagreement class has a reason in `reasons.tsv` | AGREE not down, no new disagreement class without a reason; every moved line explained in the GATES entry |

**The corpus passes, done right.** `//gates:local`'s corpus checks only compare committed results; they do not rerun
the corpus. A compiler change must rerun the six passes and compare them with a baseline:
1. Before changing anything, build the six targets and copy each output directory
   (`bazel-bin/spec/judge_<pass>/`) aside, e.g. to `runs/homework/<phase>/judges_base/judge_<pass>/`.
2. After the change, build them again.
3. Compare the RESULT files, not the logs: the rosters (`*-fail-roster.txt`, `*-skipped-roster.txt`), the registers
   (`*-engine-order-register.txt`, `*-unordered-register.txt`, `*-database-engine-order-register.txt`), the ledgers
   (`judge-host.tsv`, `judge-database.tsv`) and `verdict.txt`. Compare their sorted lines without comment lines.
   Logs (`host.log`, `database.log`) differ by timings; ignore them.
4. A database pass refuses to run after its host pass fails, so an empty database output means look at the host pass.
5. A difference is fixed, or explained to the user, before landing.

Running a hand-made corpus command after another Bazel command: rebuild the corpus targets first (cached), or the
execution root lacks the upstream trees ("legend-engine checkout not present").

**World changes (Phases 4 and 6)** use the experiment harness: `docs/build-inventory/manifest-world/experiments/README.md`
(swap `prelude.pure` on the classpath without code changes; rerun each corpus pass's exact command with `e6_lanes.py`;
the user side with `UserSideProbe`; the browser with `bazel run //wasm:startup`).

## 6. Traps already hit (add to this list)

- The reference lane's census walk does not follow the execution root's symlinks: point `-Dlegend.engine.root` and
  `-Dlegend.pure.root` at the real directories (realpath) when running its programs by hand.
- `jfr print` keeps only 5 frames per stack by default; use `--stack-depth 200` to see who calls a hot method.
- macOS `strings` fails on class files (it reads `CAFEBABE` as a fat Mach-O); use `grep -a`.
- zsh: `echo ====` fails (`=` expansion); an unquoted `$VAR` holding several paths is one word; `"${SHA}:refs/..."`
  needs the braces.
- Segmenting upstream Pure files: a doc string belongs to the element below it; keywords at a line start inside a doc
  string or block comment are prose; names come after every `<<stereotype>>` and `{tagged value}` (`docstart.py`).
- Upstream marks tests with test stereotypes and `::tests::` packages; `::test::` packages are upstream's test
  infrastructure, which ordinary code references.
- An ownership filter must cover the functions Pure.java implements, `CoreFn`'s forms, the system metamodel's own
  versions, `TdsLegacy`'s functions and the helpers forms recognize (`agg`, `col`): missing one breaks hundreds of
  tests.
- `bazel info` blocks while another build runs in the same workspace; use the known paths.
- A guard's growth is a finding: do not raise a shrink-only pin to make a change pass.

## 7. Reading order

1. This file. 2. The plan: sections 1 to 5. 3. `docs/PARKED_WORK_LEDGER.md` (PARK-5 to PARK-14). 4. The brief of the
next phase in this folder, and the documents it lists. 5. `docs/IN_FLIGHT.md` on main, to see who else is working.
