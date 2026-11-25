# Handover from the B3 orchestrator session (2026-11-25, late night)

For the new orchestrator, in another account. Read `edition-program-START-HERE-2026-11-25.md` §0 (0.0 first, then 0.15), then `edition-v4-direction-2026-11-25.md`.

## 1. Saved work (all in `edition-homework/handover/`)

| File | Base commit | What it is | State |
|---|---|---|---|
| `s1-decimal-literal.patch.txt` | `2a860552` (s1's branch head; applies cleanly to the branch head) | **The DECIMAL Float literal deletion** (ruled, amendment 11). Byte-identical to `../landing-decimal-literal-wip.patch.txt`. **None of the 8 re-pins have been made since.** | Built, full round run; finish per `../landing-decimal-literal-wip.md` |
| `s5-step6-wip.patch.txt` | `50b33970` (B3 step 4) | **B3 step 6, the SQL-text fallback as test tooling** (design: `../b3-step6-design.md`) | See below |
| `s4-wip.patch.txt` | `50b33970` | Step 5a's edit tree: step 5a as built, untracked files included (`CrossTypeComparisons`, `EqualityClasses`, `TypedCrossTypeComparison`, `EngineComparisons`, `PureComparisons`, their tests, `OrderedAssertsTest`). Step 5a landed as `b0df6ea8`. This tree differs from `b0df6ea8` in 11 files (110+/62−), most likely pre-re-pin versions and review fixes that landed differently. | **Superseded by `b0df6ea8`**; reference only |
| `s3-step5a2-withdrawn.patch.txt` | `96f0d33c` | Step 5a-2, the door path for function bodies (12 files) | **Withdrawn by the user**; already kept as `../withdrawn-function-body-door-draft.patch.txt`; reference only |
| `probes/e0c-partial-superseded.patch.txt` | `7c2ac303` era | The first E0c dead-code edits | Superseded; redo from v3 §6.1 item 3, keeping `FeatureRules`' live refusal |

**Step 6 (`s5-step6-wip.patch.txt`):**
- **Built:** an after-body hook. `test/Audience.after(TypedQuery body, Ran ran, Reads reads)` (default: the run as it is) and a `Ran` record (verdicts, failure). `test/PureTestRunner` calls it after the body, on the still-open session, with the audience's `Reads` (30+/4−).
- **Left** (design §1.2-§1.3, §4's slices):
  - the door's lift of SQL-text asserts out of the body (`edge.UpstreamSqlTexts`);
  - the harness's decision per lifted marker, in body order;
  - the referee's seeds built lazily (§2);
  - Q16c, the dual golden (§3);
  - the residue (§5).
- **Measured:** nothing yet. No round was run on this patch.
- Its place in the new plan: to be re-planned against R4's presenters (START HERE §0.0). The referee stays (ruled).

**The equality measurement** (`handover/eq-measure/`): the tools `run.py`, `rewrite.py`, `classify.py`, `analyze.py`, `build_corpus.py`, `extract_abt.py`, `pqtab.py`; the analysis tables (`an/`: `classified.json`, `directions.json`, `rows.json`, `duck_*.txt`); the per-database run summaries (`runs-DuckDB.json`, `runs-H2.json`; the raw per-query outputs were not copied, they're large and regenerable); the inputs `eq.pure`, `eq2.pure`, `pct.txt`; and the product-query tables `pq.*`. That was the 2026-11-24 measurement of the product's `==` against the assert rules (`compat-b3-progress.md`, "the one equality").

**Probes not elsewhere** (`handover/probes/`):
- `step5probe-EqualityProbe.java` and `engineprobe-*`: legend-engine's own planner and executor asked for `1 == 1.0` (S127). B2's M2M/XStore probe is the same family.
- `h2probe-*`, `duckprobe-*`: small JDBC probes of H2 and DuckDB behaviours used in steps 1b-5.
- `r2tools-*`: the scripts behind the PCT sort.
- `round-decimal.sh`: the exact full round used on the DECIMAL deletion.

## 2. The last full rounds, and what was red

There is no full round on the branch head (`339e5574`) with no local change. The head's code equals B2's `418553e9`; everything after it is documentation.
- **B2's last full rounds on that code:**
  - `0dba5d604` + `978edd653`: every lane, //gates:local `--lockfile_mode=error` included. 646 passed, 1 skipped.
  - `06e3d56b0`: //gates:local and the edge lanes. 557 passed.
  - Red in both: only the **14 targets that fail as root** (initdb refuses root): the Postgres-backed `//core:*_postgres_test` targets, `//core:is_numeric_probe_test`, `//datacube:verify_app_test`. All passed when rerun unprivileged (`tools/postgres/as_unprivileged.sh`).
- **B3's round on the head plus the DECIMAL patch** (`round-decimal.sh`): 284 of 293 passed in the round, and `//gates:local` had 19 red. Every one is explained:
  - the 14 root-only;
  - `EdgeSpellingHookTest`'s pinned `floatLiteralKind` arms (3 edge targets);
  - the corpus lean-SQL pins (2);
  - the corpus roster and edge goldens (update tests).

  Nothing unexpected. The product's own `//core:lean_sql_baseline_test` passed.
- **Pre-existing, not ours:** `//pct:pct_channel_b_twice_h2_relation` (a manual lane) fails 4 cases on a clean tree: "DialectCapability: LIST_GET reached a dialect without a list encoding" (B2's M2 record). It has no owner.
- **Flaky:** none seen in these rounds.

## 3. Running or queued

- **B3:** nothing running. No rounds, no subagents.
- **B2** (`session_01XmdJ7iUHw3CS3J3T5aEFGr`, branch `claude/compat-b2-nufdlk`): its full round on toCSV exact (`SqlFn.FLOAT_FULL_TEXT`) plus S7a (A) was running. The handover message has been sent: when the round ends, push, save its S6a stash as a patch, report the round, and report the pushed head to the user. Its last messages to B3 are recorded in the amendments file (item 23).
- **Merge B2's branch into this one** when it pushes. The last merge was `ec762fad`, at B2's `418553e9`.

## 4. Rulings since 3c728c3a

None. The user's messages since were about the handover. Every ruling is in `v2-amendments-ruled-2026-11-25.md` (items 1-23). The user's fallback, to fail the host-side `substring` tests and move on, is quoted in `edition-v4-direction-2026-11-25.md` §6.

## 5. What the records don't say

- **Disk is the main trap.** About 252 GB total, with only 4-9 GB free while working. Each worktree gets its own Bazel output base under `/root/.cache/bazel/_bazel_root/<hash>/` (s1's is `d4d5f4ac…`; a measurement agent created another for s2). The shared caches are `/root/.cache/bazel-disk` (~18 GB) and `_bazel_root/cache` (~22 GB). When the disk filled, deleting an unused output base's `execroot` freed ~9 GB; its `external/` holds the pinned legend-engine and legend-pure sources, so keep that. `du` on the caches takes over 2 minutes.
- **Timings (warm caches):**
  - the round's build step: ~20-30 minutes;
  - the round's tests: ~40-60 minutes;
  - `//gates:local` afterwards: ~20-30 minutes;
  - a full round: about 1.5-2 hours.

  A fresh container in another account starts cold: expect the first build to take much longer, and see `docs/CLOUD_SESSIONS.md` (`tools/cloud-distdir.sh`) for downloads GitHub refuses.
- **Docs while a round runs:** to commit while s1 was building, B3 used a separate docs-only worktree (`/home/user/sdocs`, detached, pushed with `git push origin HEAD:claude/compat-b3-nufdlk`). s1's local branch ref was left behind the remote; fast-forward before committing from s1.
- **Subagent research outputs** were written to the scratchpad and copied into `edition-homework/` (`r1/`, `r2/`) before they could be lost. A new account has no access to the old scratchpad; everything needed is in the repo.
- **The root-only failures:** rerun those targets unprivileged before calling one of them a regression.
- **The pinned upstream sources** are in Bazel's external directory (`+http_archive+legend_engine_src`, `+http_archive+legend_pure_src`), with the PCT manifests under `pct-manifests/`. A cold container fetches them with the first build.
