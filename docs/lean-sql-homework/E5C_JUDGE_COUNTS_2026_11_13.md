# E-5c: what BOTH sends beside the DATABASE judge's fused program, measured (2026-11-13)

The orchestrator asked for this measurement before the E-5c switch, and accepted it on 2026-11-13. It counts the
statements and characters PCT channel B sends under each judge mode. The gap is the most the E-5c switch can remove:
BOTH planned on the DATABASE judge's fused program, the host reading single-value sides as typed columns, and planEach
deleted. It does not depend on Q-E5c-1.

**Tree:** claude/total-wall-e @ 3a711976af, clean. **Script:** `e5c_judge_counts.sh.txt`. For each channel B
target it runs `bazel test --nocache_test_results` with
`JAVA_TOOL_OPTIONS="-Dlegend.diagnostics=sql-sizes -Dpct.channelb.judge=<BOTH|DATABASE>"`, then sums the
`[sql-size]` lines of the test log: one line per statement sent, with its characters and its StatementOrigin.

## Per suite

| target | BOTH statements | BOTH characters | DATABASE statements | DATABASE characters |
|---|---|---|---|---|
| pct_channel_b_essential (DuckDB) | 2,762 | 2,252,451 | 701 | 1,949,015 |
| pct_channel_b_grammar (DuckDB) | 1,357 | 1,715,179 | 386 | 1,146,837 |
| pct_channel_b_relation (DuckDB) | 1,978 | 2,815,045 | 939 | 2,181,203 |
| pct_channel_b_standard (DuckDB) | 2,314 | 2,603,481 | 413 | 2,188,709 |
| pct_channel_b_unclassified (DuckDB) | 433 | 251,670 | 186 | 241,553 |
| **DuckDB total** | **8,844** | **9,637,826** | **2,625** | **7,707,317** |
| pct_channel_b_h2_relation (H2) | 2,277 | 3,491,892 | 1,309 | 2,889,738 |

On DuckDB, BOTH sends 3.4 times the statements and 25% more characters (1.93M). On H2, the relation suite only, it
sends 1.7 times the statements and 21% more characters.

## By origin

| target | BOTH | DATABASE |
|---|---|---|
| essential | other 2,419; session 343 | body 358; session 343 |
| grammar | other 1,106; session 134; system 117 | body 135; session 134; system 117 |
| relation | other 1,508; session 469; statement 1 | body 469; session 469; statement 1 |
| standard | other 2,108; session 205; statement 1 | body 207; session 205; statement 1 |
| unclassified | other 340; session 93 | body 93; session 93 |
| h2_relation | other 1,404; session 872; statement 1 | body 436; session 872; statement 1 |

- The setup statements (session, system and statement) are the same under both judges.
- The difference is the judges' own statements:
  - BOTH's "other": the host judge's per-assert reads (today's planEach), 7,481 on DuckDB and 1,404 on H2.
  - DATABASE's "body": its fused verdict statements, 1,262 on DuckDB and 436 on H2.
  - That is about 5.9 times as many judge statements on DuckDB, and 3.2 times on H2.

## Caveats

- **An upper bound.** After the switch the host's single-value sides ride the fused statement as typed columns, which
  adds characters back. The saving in characters will be smaller than the 1.93M gap.
- **Not a full apples-to-apples comparison.** The DATABASE judge alone refuses the 13 bodies of Q-E5c-2, and leaves
  its declines unjudged. BOTH's host judge runs both. The gap slightly overstates the saving on those bodies.
- **Wall time is indicative only.** BOTH took 193 s and DATABASE 91 s, summed over the six targets. Other builds shared
  the Bazel server during the BOTH half; unclassified ran 76 s, against 9 s in an earlier, otherwise contaminated run.
