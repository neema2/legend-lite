# PR #118: the queue after the merge (2026-11-18)

These are items found by the audit, the verifications, the lanes, the heavy rounds and the fix round that are NOT
fixed before the merge. The before-merge fixes are in `README.md` (F1-F8). Each item is kept here until it is ruled,
scheduled or closed. IDs refer to `findings/` and `verification/`.

## 1. Pre-existing wrong answers (present at main or at the lanes' base)

| Item | What | Where |
|---|---|---|
| Q1 | The product probe's wrong values on cloud and other databases. Float literals are read as exact decimals (`0.1 + 0.2` gives `0.3`), and Decimal scale is lost or overflows. BigQuery 4, Snowflake 7, Databricks 3, Trino 3, ClickHouse 7 | lane reports, base = head |
| Q2 | The warehouse JDBC client refuses every bound parameter, so `//spec:corpus_warehouse_verdict` loses 41 cases. Red at main, the base and the head; the lane is manual, so no per-push run shows it | `warehouse/client/jdbc/WhStatement.java` |
| Q3 | MariaDB channel B under BOTH is nondeterministic: the host judge reads list rows in an unstable order (about 20 essential tests flip between runs) | lane report, at the base too |
| Q3b | MySQL channel B under BOTH fails its essential, grammar and standard ledgers on Decimal scale: the host's assertEq fails "expected: 3.8D actual: 3.800000000000000000000000000000D" while the database holds (toDecimal, abs, rem, round, parseDecimal tests). Also red at the base 815de5810 | lane round 2 |
| Q4 | On SQLite, a whole Float past 2^63 prints as `9223372036854775807.0` (FloatShortestText's whole-value arm casts through a 64-bit integer) | fixer, F7 |
| Q5 | `toCSV` writes a DateTime as ISO `+0000`; upstream writes `yyyy-MM-dd HH:mm:ss` | EDGE-2 |
| Q6 | `format('%r', s)` does not escape a newline | EDGE-3 |
| Q7 | The edge's `tdsBuilder` envelope aggregates rows with no ORDER BY. Order is lost at scale on DuckDB | EDGE-8 (retired by the presentation program's decision f) |
| **Q8 (first after the merge)** | An Integer property mapped to a `DECIMAL(10,0)` column is refused by the product's result decode: `project` and `filter(...)->toOne().n` fail with "a cell BigDecimal 5 is not its type's carrier" (`result/Batch.java:187`, the Longs batch). The host judge errors on every assert over it, so BOTH raises JudgesDisagree. Came in with `51688f36` ("Typed results end to end"), on main. Fix: the Longs batch accepts an integral BigDecimal (scale <= 0 or a zero fraction) and refuses a fractional one; a probe under every judge and both rule sets, and a product case | fixer, F4 probe |
| **Q9 (with Q8)** | Python's lite API v1 `query/run` answers EVERY DuckDB failure, a planned raise included, as 500 `internal` (`python/legend_lite/engine.py` `_query_run`, the `except (duckdb.Error, UntypedColumn)` branch); `_planned.py` re-raises with the words only, dropping the site's kind and where. Java answers the same run 422 `data` with its raise. Fix: carry kind and where on the re-raise, answer as Java does, add Python to the shared data case's bindings | fixer, N3 (Java half fixed in PR #118) |

## 2. Judge parity and words (goal G2), open after F4

| Item | What |
|---|---|
| J1 | `assertEquals` over a scalar pair is worded as a multiset, so BOTH raises JudgesDisagree |
| J2 | `assertEquals`' failure words name the carrier's kind ("expected 1 (Integer), got 1 (Integer)" on F4's shape) |
| J3 | `containsOf` and `subsetOf` compare by carrier, not by the declared kind (against S131) |
| J4 | Under compatibility rules, the edge's negated-assert words differ by judge ("1 should not equal 1" against "1 should not equal 1D"), and `messageWithDecimals` writes "1DD" for an already-D carrier (a one-liner) |
| J5 | The product store-read carrier spells a Decimal as `1D` (legend-pure's %r form). F4 strips it in lite's words; the real fix belongs to the presentation program |
| J6 | The two judges hold a planning-time `let` raise differently (JUDGES-1) |
| J7 | Planning catches RuntimeException at about 14 sites, so a NullPointerException or ClassCastException becomes a "planned raise" (JUDGES-4) |

## 3. Separation completeness: upstream meaning still outside the edge

| Item | What | Source |
|---|---|---|
| C1 | legend-engine's union framing shape still runs in product lowering; only its name moved | LOWERSQL-1, GUARDS-2 |
| C2 | The product planner computes legend-engine's exists-join rule | LOWERSQL-4 |
| C3 | TypeScript product records read legend-engine's shapes; Python reads `relationalType` | COMPLETENESS-1, -4 |
| C4 | Python's product engine serves `/api/server/v1/currentUser`; the product loader holds the engine entry table | HOSTS-3, -4 |
| C5 | The prelude ships legend-pure's print bodies (Pair, List, SQLNull, TDSColumn). Pair has two product forms. Needs the user's ruling on Pair | COMPLETENESS-2 |
| C6 | DataCube's product queries work around legend-engine's defects on every planner | COMPLETENESS-3 |
| C7 | "Assert failed" is a product default on one path (orchestrator's ruling, not the user's) | PRODUCT-3 |
| C8 | Lite's error shape: non-raise failures answered as `data`; `where` dropped; a compile position duplicated | PRODUCT-9 |
| C9 | The pure/v1 envelope would write a raise's kind name if a planned site with recorded words fired (no request reaches it today) | F5, refuted at the head |
| C10 | The internal natives `typeRaise` and `cellText` can be called by full name (F32, promised before landing) | COMPLETENESS follow-ups |

## 4. Guards and the safety net

| Item | What | Source |
|---|---|---|
| G1 | The census proves less than claimed: UpstreamBranchesTest matches nothing; TEXT sees about 6% of the edge's strings; L-A sees only function arguments | GUARDS-1, -3, -4 |
| G2 | No meaning guard reads the TypeScript or Python product | COMPLETENESS-10 |
| G3 | `shrink_only` cannot judge against main, and its vocabularies are unwatched for removals | GUARDS-8 |
| G4 | The golden net pins lite's SQL in `activities`, its mask hides 591 fragments, it does not check row order, and it is not in CI | GUARDS-5, -6, -7 (the presentation program's S0) |
| G5 | The 80-character blind spot in `ErrorShapeGuardrailTest` | fix review, F6 |
| G6 | The goals document still names "BOTH on every lane" as G2's guard | V-GUARDS, GUARDS-11 |

## 5. Design debt in the edge (the presentation program covers most of it)

- About 1,090 lines of SQL text builders in the edge (EDGE, the design's §2.1).
- 12 of 21 hook kinds carry presentation; `PlanStyle` is a service locator (ARCH-2, ARCH-4).
- The moved judging code was copied 71-98% byte for byte (JUDGES-4 report).
- 263 duplicated lines across PureText's 12 dialect subclasses (EDGE).

## 6. The user's rulings of 2026-11-18 (~20:10 UTC)

The user's words: "For 118 r1-r3 are fine" and "For r1-r8 later all are good except r4 we have to think thru later -
if engine is right default or pure is".

| Ruling | What | Where it lands |
|---|---|---|
| R1 | The product's default assert message is lite's "expected true, got false"; legend-pure's "Assert failed" only in PURE and ENGINE | PR #118, fix N2 (closes C7) |
| R2 | The test-data generator's two upstream spellings (`createTableRowIdentifiers(`, `createRowIdentifier([`) stay for now | PR #118, a ratifying note; revisited as ENGINE output |
| R3 | The four residue pins (DatabaseJudge 851, HostJudgments 679, BodyDriver 545, EngineTextPlanning 151) are ratified | PR #118, a ratifying note |
| R4 | **OPEN**: should LITE's default substring and indexOf follow Pure or legend-engine? PURE follows Pure and ENGINE legend-engine either way | to think through |
| R5 | A Pair's LITE form is the JSON object on every output; `<a, b>` and the prelude's other legend-pure print bodies go to PURE and ENGINE (C5) | three-mode program |
| R6 | DataCube's legend-engine workarounds apply only toward legend-engine (C6) | DataCube follow-up |
| R7 | The parser's level-less entries default to LITE | three-mode program |
| R8 | (c) C4; (h) K1 | three-mode program |

Still pending the user: (e') ENGINE's scope (recommended E-R1), (d') per-push CI additions (recommended yes), and
(N3 was ruled by the user at ~21:40 UTC: "N3 okay".)

## 7. The fix batch's own audit, and the user's rulings on it (2026-11-18, ~22:10-22:45 UTC)

Three independent xhigh reviewers audited the whole fix batch (68f20eb4d..eec3c95c7, 26 commits, 71 files); their
reports are in `fix-batch/`. Verdicts: JUDGES-WORDS SOUND-WITH-DEBT, PRODUCT-HOSTS SOUND-WITH-DEBT, BUILD-GUARDS one
blocker (BUILD-GUARDS-1, the new `//datacube:dist_imports_test` assumes `/` separators and would turn the Windows
DataCube lane red), fixed in the batch's last correction round.

The user's rulings:

| Ruling | What | Where it lands |
|---|---|---|
| R9 | N2 held two `JavaEvalLedgerTest` pins by joining lines; wrap them honestly and raise DatabaseJudge 851 -> 852 and AssertVerdicts 1464 -> 1465, dated ("Yes, raise both"; "Raise cap honestly") | PR #118, the last correction round |
| R10 | The six other in-body guards' LITE default words (assertTrue, assertFalse, assertEquals, assertNotEquals, assertEmpty, assertNotEmpty still answer legend-pure's "Assert failed"): after the merge ("After the merge"), as part of the presentation follow-up | presentation program |
| R11 | The two items where PR #118's own claims are not quite true go after the merge, "if [they] can be part of our presentation followup": (a) PH-2 / PRODUCT-1 / HOSTS-9, the product store's legend-engine statuses and its `"IllegalArgumentException"` text (0 -> 2 in the fix batch), with four host copies of the rule; (b) HOSTS-7, two undeclared doors to legend-engine's routes (`EdgeServer.main` in the server jar; Python's direct `EngineRoutes()` composition) | (a) presentation program (error presentation toward legend-engine); (b) the first small follow-up PR (Q8/Q9), being about 10 lines and not presentation |

The fix batch's other findings (all FIX-AFTER or NOTE): PH-1 (a malformed percent-escape is 500 on every host's lite
store route); PH-6 (`trustOne` and 35 other internal natives still resolve by full name); the Float print outside
FloatShortestText's windows (2^60, Infinity, -0.0; beside Q4); a Decimal-declared NaN or Infinity throwing
NumberFormatException in Equality's DECIMAL branch (suspected); F4's test covering one numeric pairing; the census
no longer seeing LiteText's REGEXP_REPLACE after the strip moved to LiteralSpelling.

## 8. The P0 list after the merge (the orchestrator's estimate, 2026-11-18 ~22:30 UTC, given to the user)

What makes an item P0: a wrong answer a user can get today; a goal of record broken; a cost that grows every day it
waits; or a gap in the safety net that lets a regression land unseen.

| P0 | Item | Why |
|---|---|---|
| A1 | Q8, Integer over `DECIMAL(10,0)` refused at decode | a common schema shape fails |
| A2 | Q9, Python's lite API answers every run failure 500 | Python's errors differ from Java's |
| A3 | Q1, Float literals as exact decimals; Decimal scale lost or overflowing (BigQuery, Snowflake, Databricks, Trino, ClickHouse) | wrong values on five databases |
| A4 | Q2, the warehouse JDBC client refuses every bound parameter | any parameterized warehouse query fails; its lane is manual |
| A5 | judge parity: Q3b (MySQL Decimal scale), Q3 (MariaDB list order, also G10), J1-J4 | a verdict depends on the judge (G2) |
| A6 | J7 / JUDGES-4 (about 14 broad catches turn a bug into a "planned raise"); JUDGES-8 (`negatedOf` reads a declined judgment as held) | bugs reported as user errors (invariant 4) |
| A7 | HOSTS-2, TypeScript `transacted` commits outside its `try` | a failed COMMIT leaves the session aborted |
| A8 | Q4, SQLite's Float past 2^63 | a wrong value, narrow |
| B1 | the presentation untangle, the plan format first (PRODUCT-4's sentences in the plan; PRODUCT-13's four silent codec changes: v59), then D1/EDGE-4, ARCH-2/-3, J5, PRODUCT-10, R10, R11(a), Q5-Q7, R5 | must precede plan export |
| B2 | a census that can see: GUARDS-1, -3, -4, COMPLETENESS-10, HOSTS-8 | until then any change can add hidden upstream meaning; the only way to the real count |
| B3 | the separation close-out: of the 16 known open items, those B1 and R11(b) do not take (C1, C2, C3's TypeScript and Python halves, HOSTS-4, LOWERSQL-7, COMPLETENESS-7, -8, PRODUCT-7, -11, JUDGES-3, -6), plus whatever B2 finds | the program's goal |
| B4 | the user's ruling R4 (LITE's substring and indexOf) | B1 and the three modes build on it |
| C1 | the edge golden net in CI; its 16-digit mask; row order (G4, GUARDS-5, -6, -7) | the edge's bytes unguarded per push |
| C2 | manual lanes in no per-push run (the warehouse lane hid Q2); the per-push CI additions (d') | red stays hidden |
| C3 | the goals document's claim that "BOTH on every lane" guards G2 (G6) | a guard claimed that does not exist |

Not P0: cosmetics, duplication and dead code (EDGE-7, -9, -11, GUARDS-13, LOWERSQL-8, JUDGES-10, the copied judging
code, PureText's 263 duplicated lines), PH-1, PH-6, ARCH-9 (corpus only).

The order: A1 + A2 + R11(b) as one small PR (days 0-1) beside C1-C3; then three tracks in parallel: B1, B2 -> B3, and
A3-A8; R4 early in B1.

## 9. Found after the merge (2026-11-19), queued

PR #118 merged to main as b75d6c73a (the user's word, 2026-11-19 ~01:35 UTC); main's CI green on the re-run
(Maven Central 429s and one Windows markets_test timeout were environmental). The compatibility program moved to its
own owner session (`docs/records/compat-program-handoff-2026-11-19.md`); this list is the product side's.

| Item | What | Source |
|---|---|---|
| PR #121 | Q8, Q9 (+ N-b, N-c), R11(b): open; its review found two Q8 blockers: a whole value written as text ("5.00", SQLite's scaled DECIMAL) refused (being fixed), and R-a (the fraction under an Integer property) awaiting the user's ruling | the PR's xhigh review |
| PR #121's follow-ups | F1 Python reads every plan's columns by position (as Java's runner does; the tab reads by name); F2 some query/run LOAD failures still answer differently from Java; F3 `redacted()` drops a raise's kind and where; F4 Python re-implements Java's classifier; F6 the tab and Python answer a fraction's text under an Integer column (N-a); F7 the judges disagree on that fraction (G2); F8 the Python door is a marker, bypassable; F9 precise Integer subtypes not covered; F10 Q8's graphFetch route untested | the PR's review |
| Parity: substring | lite answers a negative substring start differently on H2 and DuckDB (DuckDB's SubstringClamp lifts it to 1; H2 counts from the end) | H3 |
| Parity: firstDayOfWeek | lite answers 2014-12-01 (Monday) on DuckDB and 2014-11-30 (Sunday) on H2; Pure's firstDayOfWeek is Monday; the date_trunc('week') lowering differs by database; hidden on DuckDB by the engine-text verdict | B2's CP-4 probe |
| TS warehouse binds | the TypeScript warehouse client writes a whole Float parameter as "2" (JSON.stringify), so the server binds BIGINT (the Java client is fixed on Q2's branch) | Q2 |
| Q2's residue | an H2 server session is still sent a file load (the driver name equals the embedded one); the fix is a "remote" fact recorded where exec opens the session; the upload-then-read delivery is the real follow-up | Q2 |
| Register misquotes | S138 (compiled CoreHelper.acos raises, it does not answer NaN) and S63 (cites a commented-out line) | H1 |
| currentUser | Python answers GET only, with the local user; Java answers any method with "anonymous" | H4 |
| The checks lane | grew from 8-10 to 13-18 minutes on Linux with #118's guard tests (guardrails_edge, census_edge, the lean SQL baseline, the cast census); bring it back under 10 | main's CI timings |
| Manual heavy reds (pre-existing, red on main before #118) | update_reference_lane_test (out of date), manifest_world_census (28 > 27), timeseries_sql_measure and ts_examples_measure (statements outside a run), ts_scale_measure (2 SQL-size bounds; #118 fixed two), ts_line_spike_test, seam_benchmark_small_heap (H2 out of memory at the small heap), requirements.test (lock file) | the heavy round, checked at 4dfbb5b76 |
| markets_test on Windows | //python:markets_test timed out once on main and once on PR #121; passed on re-run; to confirm it is not the Python read path | main's and #121's CI |
