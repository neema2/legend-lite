# Lane round 1: Databricks (2026-11-25)

*Measured by the Databricks lane (`session_0195rQEijYc25F3cgFg1TxX2`, Legend Dev, Opus 5.5 at medium effort, measure
only). Base `296732e2`, candidate `2cd0b2f9` (step 1 of the plan: the edge's DECIMAL Float literal deleted).*

| Suite | Base | Candidate |
|---|---|---|
| Channel A essential | PASS 341/341 | PASS 341/341 |
| Channel A grammar | PASS 132/132 | PASS 132/132 |
| Channel A relation | PASS 401/401 | PASS 401/401 |
| Channel A standard | PASS 198/198 | PASS 198/198 |
| Channel A unclassified | PASS 93/93 | PASS 93/93 |
| Channel B essential | PASS=338, ERROR=7 of 345 | identical |
| Channel B grammar | PASS=136, ERROR=1 of 137 | identical |
| Channel B relation | PASS=461, ERROR=8 of 469 | identical |
| Channel B standard | PASS=198, ERROR=7 of 205 | identical |
| Channel B unclassified | PASS=93 of 93 | identical |

- All 10 Bazel targets passed on both commits. No "does not match its ledger" block; no "the runtime wall" line.
- **Identical per test:** every outcome, every message and every channel B line (statement ids normalized).
- **The only difference is SQL text** in channel A's echo (relation 8 lines, standard 4): a bare decimal Float literal
  is now a double literal (`1L + 1L + 1.0` to `1L + 1L + 1.0E0`; `CAST(1.2 AS DOUBLE)` to `1.2E0`;
  `percentile_disc(0.6)` to `percentile_disc(0.6E0)`). With the SQL stripped, all 460 relation and 64 standard result
  lines are byte-identical: **no Float answer moved on Databricks.**

**Timings:** setup (`tools/cloud-distdir.sh`) 8 minutes; credentials under a second; base build 1m12s, channel A 5m31s,
channel B 4m06s; candidate build 4s, channel A 4m45s, channel B 3m26s. Disk free at the end: 43 GB.

**What the lane found wrong or missing in the docs:**
- `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` section 4 does not name the two coordinate keys in full: the harness reads
  `databricks.hostname` and `databricks.httpPath` as well as `databricks.url` and `databricks.property.*`.
- Its section 6 is stale: `tools/cloud-distdir.sh` now handles bazelisk and the refused archives, so the manual
  `--override_repository` steps are not needed. The `test --test_env=...` line in `~/.bazelrc` is still needed.
- Channel A's pass count includes pinned expected failures; a per-test comparison needs the test.xml messages and the
  log's echo.

The lane's credential script is `tools/cloud-lane-credentials.sh`.
