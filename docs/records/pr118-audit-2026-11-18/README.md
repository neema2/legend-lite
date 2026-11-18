# PR #118 audit (head 68f20eb4d, 2026-11-18)

The independent audit of PR #118 (`claude/total-wall-d-stage` -> `main`, the edge program). Eight area auditors and
four adversarial verifiers, all at xhigh effort, reviewed one goal: moving everything compatibility-related to the
edge, and whether that move is correct, complete and of good quality.

| File | What |
|---|---|
| `RUBRIC.md` | The bar, the grades (WRONG-ANSWER, WRONG-DESIGN, BROKEN-RULE, COSMETIC; BLOCKER, FIX-AFTER, NOTE) and the program's own rules |
| `findings/<AREA>.md` | Each auditor's report: EDGE, PRODUCT, LOWERSQL, JUDGES, GUARDS, HOSTS, COMPLETENESS, ARCH |
| `VERIFY_BRIEF.md` | The instructions the verifiers worked to |
| `verification/V-*.md` | Each blocker candidate and wrong answer checked: the walk from input to output, new or old, ruled or not, the probe, the fix |

## Verdicts

| Area | Verdict |
|---|---|
| Edge package | SOUND-WITH-DEBT |
| Product side and hooks | INCOMPLETE |
| Lowering and SQL | INCOMPLETE |
| Judges | SOUND-WITH-DEBT |
| Guards and safety net | SOUND-WITH-DEBT |
| Hosts and shipped artifacts | INCOMPLETE |
| Completeness sweep | INCOMPLETE |
| Architecture | SOUND-WITH-DEBT |

INCOMPLETE means upstream meaning is still left in the product. It does not mean that a wrong answer was found.

## Confirmed before-merge items (the fix round on `claude/pr118-fixes`)

| Fix | What | Kind |
|---|---|---|
| F1 | 8 new tests in no CI lane; the `product` lane is red on every OS | blocker (V-HOSTS) |
| F2 | `guardrails_edge`, `lean_sql_baseline_test` and `shrink_only_test` time out in CI (sizes too small) | CI red |
| F3 | DataCube's shipped `dist` lost its legend-engine bundle | regression from main (V-HOSTS) |
| F4 | judge parity: `assertEq`, `assertIs`, `assertNotEq` and `assertIsNot` compare by carrier kind on the host, declared kind on the database | goal G2 (V-PRODUCT) |
| F5 | edge-on raise words: the server's pure/v1 envelope answers the kind's name, and the tab and Python answer lite's sentence | regression from main (V-HOSTS) |
| F6 | lite's saved-query routes answer client errors 500 `internal` instead of 400 `request` | wrong answer, new route (V-PRODUCT) |
| F7 | an edge relation's toString/toCSV prints Float cells by the database's cast (H2 `2.5E7`) | blocker, regression from main (V-EDGE) |
| F8 | channel B ledgers pin the old range-refusal wording on SQLite, MariaDB and MySQL | lane red |

## Database lanes (base 815de5810 vs head 68f20eb4d)

MariaDB, MySQL, SQLite, Trino, ClickHouse, MongoDB, Snowflake, Databricks, BigQuery and Postgres channel B all came
back clean:

- channel A is identical test by test, and the SQL is byte-identical;
- no lost passes;
- the only channel B difference is F8's wording.

The product probe's wrong values on the cloud databases, Trino and ClickHouse are Decimal-scale and Float-literal
defects that are present at the base.
