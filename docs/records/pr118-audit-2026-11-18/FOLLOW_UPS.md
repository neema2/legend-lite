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
