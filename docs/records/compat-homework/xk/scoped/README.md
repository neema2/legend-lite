# The scoped cross-kind rule, prototyped and measured (homework, 2026-11-19; not a landing)

**The scope** (the compat owner's ask):
- `###Relational` `=` and `!=` keep the database's comparison in every case, as A3's PR1 keeps `substring` and
  `position`.
- Pure code keeps legend-pure's kinds-first rule.

**The prototype:**
- `scoped-clean.patch`: a diff against the fix (`63421e0a`). The normalizer marks a `###Relational` equality with
  the position source `xk:relop`, and the lowering's disjoint-kinds rule skips a marked call.
- `scoped-instrumented.patch`: the same plus one log line per distinct site, by category:
  - RELOP: a `###Relational` `=`;
  - PROGRAM: a call carrying the program's own source position;
  - LITE: a call lite built, with no position.
- Both live on the local scratch branch `xk-scoped`, never pushed.

**Rules of record:** it holds. The comparison stays the database's, except where the types alone decide it: the
program compares two primitives of disjoint kinds, which legend-pure answers false for every value.

## The corpus, split by cause

Three measured runs:
- **all:** the fix as is;
- **relop:** the scoped rule;
- **program:** the rule applied only to PROGRAM calls.

Per test: `split-{h2,duckdb}.tsv`. Rosters and registers: `{relop,program}-*`.

| cause | H2 | DuckDB |
|---|---|---|
| `###Relational` `=` (all minus relop): newly failing under the fix, back to passing under the scoped rule | 15 | 15 |
| lite-built comparisons (relop minus program) | **0** (the LITE category fired at no site) | **0** |
| the program's own Pure code (program minus main): newly failing under the scoped rule | **11** | **15** |
| fixed | 1 (`testPlanForDateTimeVariableESTTimeZone`, H2) | 0 |

**The scoped rule's corpus moves against `main`:**
- fail rosters: H2 276 → 286 (+11 / −1); DuckDB 108 → 123 (+15);
- each unordered register −5, the five program tests that passed by the appeal;
- the skipped rosters and the engine-order registers do not move.

## Every Pure-code case is the program's own cross-kind comparison; none is lite's

Sites: `xk-sites.txt`; source lines: `program-sites.txt`. Each failing test's comparison, as legend-engine's corpus
writes it, with the declared types read at the pin:

| test(s) | the comparison | the kinds |
|---|---|---|
| `testFilterOnRootTypeWithOrWithFilterInQualifier` | `$t.id == '1'` (`testQualifier.pure:273`) | `Trade.id: Integer` (`simpleTestModel.pure:510`) against a String |
| `testDayOfWeekNumberFunction` | `$t.date->dayOfWeekNumber(DayOfWeek.Monday) != '2'` (`testWithFunction.pure:497`) | Integer against a String |
| `testSimpleDistinctWithFilter` | `$i.id == 1` (`testDistinct.pure:44`) | `Interaction.id: String` (`simpleTestModel.pure:256`) against an Integer |
| `testInWithDynaFunction` | `$i.id == 4` (`testIn.pure:196`) | String against an Integer |
| `testQualifierProperty`, `otherwiseTestQualifierProperty` | `$p.bondDetails.duration == '5'` (`testInlineEmbeddedMapping.pure:110`, `testEmbeddedOtherwiseMapping.pure:166`) | Integer against a String |
| `testFilterOnJoinWithAggregateFunction` | `$t.latestEventDate == '2014-12-03'` (`testDynaJoin.pure:52`) | StrictDate against a String |
| the 8 `XStore::ordered` cross-store graph fetches (4 move on H2, 8 on DuckDB) | the mapping's cross-store association conditions, `$this.prodId == $that.productId` (`testOrderedCrossStoreGraphFetch.pure:131,169`) and `$this.tradeId == $that.tradeId` (`:261,316`) | `+prodId: String` (the mapping's local property, `:115`) against `productId: Integer` |
| `testPlanForDateTimeVariableESTTimeZone` (fixed on H2) | `'settlementDateTime = ...' != -1` (`executionPlanTest.pure:982`) | String against an Integer |

In each, legend-engine's own test writes a comparison of two disjoint kinds, and its expected answer is the one the
database gives after coercing the literal or the key. legend-pure's answer is "never equal".

## PCT, lite's own suites and the guards (the clean scoped rule)

`clean-run-targets.txt`, `clean-postgres-as-ubuntu.txt`:

| target | result |
|---|---|
| PCT: `//gates:pct_duckdb`, `//gates:pct_h2`, `//gates:pct_channel_b` | green |
| `//gates:local` (`--lockfile_mode=error`), lite's own suites | green except the guards below |
| the 12 Postgres and DataCube tests that refuse root | green as `ubuntu` |
| `//core:corpus_differential_test` | **green**: `CV1_QuotedPillars` was a `###Relational` case; the fix as is broke it |
| `//spec:update_rcorpus_{h2,duckdb}_{0,2}_test` | red: the roster and register moves above |
| `//spec:corpus_duckdb_verdict` | red: "differential passes ... SHRANK: 985 < 990", the 5 appeal lines that leave |
| `//core:guardrails_edge` | red: `SqlFnSortedTest.theSortIsMeasured`, a product writer not in its pin. It is the fix's, scoped or not |
