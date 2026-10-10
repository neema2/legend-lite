# Evidence: the program's audits, scripts and recorded results (moved from scratch, 2026-10-07)

Until 2026-10-07 these files lived only in the code worktree's git-ignored scratch (`runs/build-rebuild/runs/homework/`),
so a fresh session or a fresh clone could not see them, although the briefs cite them. They are copied here unchanged
apart from local paths (written `<checkout>`, `<output_base>`, `<tmp>`, `~`). The briefs now cite these copies.

## What is here

| File | What it is | Cited by |
|---|---|---|
| `phase0/AUDIT_PHASE0.md` | the audit of Phase 0 (the build targets, the compile-only guard, the product jars) | the plan's Phase 0 check |
| `phase1/AUDIT_PHASE1.md` | the audit of Phase 1 (generator hygiene): answers and findings N1 ... N12 | `PHASE_8.md` (Gen-3, Gen-4, Gen-7, Short-9, Compile-9) |
| `phase1/PHASE1_CHANGES.md` | every Phase 1 change and its deferrals (items 1 ... 11) | `PHASE_8.md` (Gen-9, Short-6, Short-7) |
| `phase2/AUDIT_PHASE2.md` | the audit of Phase 2 (DynaFn and the engine handlers from upstream) | the plan's Phase 2 check |
| `phase3/AUDIT_BRIEF.md` | the brief the Phase 3 max-effort audit worked from (what to compare with legend-pure, and where) | `PHASE_3_LANDING.md` |
| `phase3/AUDIT_PHASE3.md` | the Phase 3 audit's report: blockers, should-fix, nits, each with evidence | `PHASE_3_LANDING.md` §3 |
| `phase3/AUDIT_FIXES.md` | the second (short) audit, of the fixes | `PHASE_3_LANDING.md` |
| `phase3/compare_judges.py` | compares the six corpus passes' 22 result files with a baseline: `python3 -I compare_judges.py <baseline dir> bazel-bin/spec` | `START_HERE.md` §5, `PHASES_3B_6.md` C.1 |
| `phase3/RefProbe.java` | the probe that printed a reference-lane body's resolution | `AUDIT_PHASE3.md` |
| `phase3/park5/eager_timings.tsv` | PARK-5's measurement: the eager corpus compile's `typeAll` per run, main against Phase 3 (run 1 of main was the warm-up; the means in `DEBTS_RESOLVE_AND_TYPE_ONCE.md` use runs 2 to 7 of main and 1 to 6 of Phase 3) | `DEBTS_RESOLVE_AND_TYPE_ONCE.md` |
| `phase3/park5/eager_run.sh`, `ab_run.sh`, `eager_jfr.sh` | how the probe was run outside Bazel (from its execution root, a params file from `bazel aquery`), alternated between two trees, and recorded with JFR. They carry the macOS paths of the day: read them for the method, take the class path and flags from `bazel aquery //spec:eager_corpus_compile` | `DEBTS_RESOLVE_AND_TYPE_ONCE.md` |
| `phase3/park5/jfr_agg.py`, `callers.py`, `callers2.py` | read a JFR recording exported with `jfr print --json --stack-depth 200 --events jdk.ExecutionSample` and total the samples by method and by caller (the `BareNames.catalog` table) | `DEBTS_RESOLVE_AND_TYPE_ONCE.md` |
| `phase3/park5/catalog_ids.py`, `fn_vs_t.py`, `pure_fn_scan.py` | small scans the audit used (the catalog's ids, function-typed parameters, upstream declarations) | `AUDIT_PHASE3.md` |
| `phase3b/reason-column.patch` | a prepared, NOT applied patch: a reason column in the reference lane's resolutions report (`OurResolutions`, `ReferenceLaneReport`) | `PHASES_3B_6.md` (reference lane work) |
| `phase3b/phase3b-h1-twins.tsv`, `phase3b-h2-versions.tsv`, `phase3b-h4-imports.tsv` | the Phase 3b homework's tables (H1 the twins by id with their spellings and parameter names, H2 the other versions, H4 the names with overloads under different imports), from `//spec:phase3b_probes` | `PHASE_3B_HOMEWORK_2026_10_09.md`, `PHASE_3B_LANDING.md` |
| `phase3b/AUDIT_PHASE3B.md` | the Phase 3b audit's report (1 blocker, 7 should-fix, 5 notes), each with evidence; the fixes are in the commit after fd1f69610 | `PHASE_3B_LANDING.md` §3 |
| `phase3b/census-after-1b.txt`, `census-after-4-5a.txt` | the manifest-world census's summary, per-module table and load walls after item 1b and after items 4 and 5a (`//spec:manifest_world_census`; the typing-failure body is in scratch) | `PHASE_3B_LANDING.md` §2, GATES "Build rebuild Phase 3b" |
| `homework/TRAIL.md` | the existing design trail (the upstream-boundary program, the compiler plans) read against the upstream-only homework, 2026-10-05 | the plan's design history |
| `homework/outputs.txt` | the generators' output hashes before the upstream-only changes (the baselines R1 and R3 compare with) | `upstream-only/R1.md`, `R3.md` |
| `world/e1_seeds.json`, `user_modules.txt`, `e8_extra.files`, `repos.json` | recorded results of the manifest-world experiments that the briefs quote (the seeds, the user side's modules, experiment 8's extra files, the repositories) | `PHASES_4_5_7.md`, `PHASES_3B_6.md`, the experiments README |
| `phase8/CI_LANES_2026_10_07.md` | CI measured on 2026-10-07 (written in the planning session, not moved from scratch): every lane's minutes per platform, the minutes of actual testing per lane, the browser lane step by step, the 10 GB cache cap and the five-macOS-jobs cap, the native image built three times | the plan's §4 (L1), `PHASE_8.md` §8 |
| `phase8/lane_minutes.py`, `job_steps.py`, `concurrency.py`, `test_minutes.py` | the scripts that produced it, over `gh run view <run> --json jobs`, `gh api .../actions/jobs/<job>`, the same job list, and a downloaded `test logs` artifact (`gh run download <run> -n <platform>-lane-<key>`); run with `python3 -I` | `CI_LANES_2026_10_07.md` |
| `webimage/` | the GraalVM Web Image spike (written in the protocol line's session, 2026-10-10, not moved from scratch): the tab's planner built by Web Image, its adapter, the harness that runs the repo's differentials and times both builds in Node and Chromium, the class-library probes, the size analysis, the reproducibility runs, and every number (`webimage/README.md`) | `docs/WEB_IMAGE_SPIKE_2026_10_10.md` |

## What stays in scratch, and why

Everything else in `runs/build-rebuild/runs/homework/` (about 1.4 GB, 13,500 files) stays there, on the machine that
made it. None of it carries a finding that a document here does not already state. By kind:

| Kind | Examples | Why not here | How to get it again |
|---|---|---|---|
| Raw logs | every `*.log` (gate runs, Bazel output, PCT and corpus logs) | timings and noise; the documents record the verdicts | rerun the command the document names |
| Build outputs | `phase3x/judges_base/` (the six corpus passes before Phase 3), `phase3x/audit_tmp/{dk,h2,eager}_*` (A/B runs), `phase3x/reflane/`, `phase1/diff_before/` | made by a target from a commit | build the target at that commit (`START_HERE.md` §5: a baseline is built BEFORE a change) |
| Big experiment outputs | `world/e6_lanes/` (401 MB of corpus pass outputs), `world/synth/` (78 MB of synthesized worlds), `world/probe/`, `world/e8_extra/`, `files.tsv` (1 MB), `decl_repo.json` (3.5 MB), `uni_edges.tsv` (26 MB) | regenerable, and too big for git | `docs/build-inventory/manifest-world/experiments/rerun.sh` (it builds its own inputs first) |
| Profiles | `phase3x/audit_tmp/jfr_*/rec.jfr`, `phase3x/s1/samples_*.json` (469 MB) | binary or huge | `phase3/park5/eager_jfr.sh` and the JFR command above |
| Superseded drafts | `program-docs/` (the briefs' first drafts), every `IN_FLIGHT*.md` draft (applied to `docs/IN_FLIGHT.md` on main), `pr_body.md` (on GitHub as PRs #25 and #26), `*_msg*.txt` (commit messages, in git) | the final versions exist | git history, GitHub |
| Code snapshots | `Compiler.java.keep`, `InferenceKernel.fixed.java`, `phase2/audit/*.before.java`, `phase2/audit/regen/` | the commits hold the code | git history |
| Binaries | `*.class`, `phase1/par1.par`, git index copies (`*/index`) | binary | rebuild |
| Local class paths | `phase3x/refcp.txt`, `audit_tmp/*.params`, `phase2b/cp` | one machine's paths | `bazel aquery` of the target |

A session on the same machine can still open the originals at those paths.
