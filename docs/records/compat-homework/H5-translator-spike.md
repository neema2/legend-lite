# H5: the model-level, target-aware translator spike (2026-11-19)

The compatibility program's TRANSLATOR session, reporting to the program's orchestrator
(`session_0195rQEoSpv1FxC3WndEk5ZH`). Branch
`claude/compat-translator-spike`, made from `main` at `b75d6c73a`. A scratch prototype: it never merges as it is, no
PR is opened, and no pin, ceiling, ledger, roster or allowlist is moved (the guards it trips are listed, not edited).

The brief: plan `docs/records/compat-program-plan-2026-11-19.md` (r5) on `claude/compat-program`, phase 3 and 0.6; the
rulings R4 (Pure's substring/indexOf), E-R2, CP-4, CP-5 (provisional), CP-8, and the standing rule (a new hook in the
core: stop and report). The inputs: H3 (`claude/compat-homework-h3` at `086292ef3`), H1 (`claude/compat-homework-h1`
at `b3e3902ea`). Upstream is READ from the pinned trees (legend-engine 4.145.0 at `230c1591`, legend-pure 5.99.0), the
same archives the build pins.

**Status: in progress.** This record is committed after each finding; the counts are filled in as they are measured.

## 0. The findings so far

### F1. legend-engine's programs use `substring` and `indexOf` in TWO meanings, by where the code runs

legend-engine's meaning is the database's only for the code legend-engine SENDS to the database. Everything else in
the same program runs in legend-pure's interpreter, in Pure's meaning (0-based, end-exclusive; `indexOf` -1 on a miss).
The corpus has both, sometimes in the same test (read at the pin, `core_relational/relational/`):

| where the code is | runs in | meaning | examples |
|---|---|---|---|
| the query lambda of `execute`, `executionPlan`, `toSQLString` (and the helpers that forward one) | the database | the database's SUBSTRING/LOCATE (pureToSQLQuery.pure:4660-4666 without the flag; `indexOf` always `locate`, :10644) | `tests/query/testWithFunction.pure:102` (`$p.firstName->substring(1, 5) == 'John'`), `:198`, `:205` |
| a test body's own code after the run | legend-pure | Pure's | `executionPlan/tests/executionPlanTest.pure:936-982` (`$planAsString->indexOf('...') != -1`) |
| a library helper the test calls | legend-pure | Pure's | `tests/planExecutionTestUtility.pure:132-133` (`sortCsv`: `$_csv->indexOf('\n')`, `$_csv->substring(0, $mid)`) |
| an M2M (`Pure`) class mapping run in memory | legend-pure | Pure's | `functions/tests/testObjectReferenceIn.pure:404-405` (`$src.fullName->substring(0, $src.fullName->indexOf(' '))`) |
| the SAME M2M mapping, chained over a relational store (`ModelChainConnection`) | the database | the database's | `executionPlan/tests/executionPlanTest.pure:2216-2236` (`testModelConnectionAgg`: the M2M library's `simpleModelMapping` becomes `substring("root".fullname, 0, locate(' ', "root".fullname))`) |
| a `###Relational` dynaFunction in a mapping | the database | SQL's, in every audience (it is the DSL's own function, not Pure's) | `tests/mapping/propertyfunc/simplePropertyFunc.pure:169-173`, `tests/mapping/sqlFunction/testSqlFunctionsInMapping.pure:820` |

**Consequences:**
1. **The rewrite is per USE, not per definition.** A rename at every definition site (H3's request-level rename,
   applied to a whole model) gives host-side code legend-engine's meaning, which legend-engine never gives it. The
   translator must find the code legend-engine routes to the store: the query argument of a store-routed call, the
   helpers that forward one, and (transitively) the functions and derived properties that code calls. A function
   reached from both a query and host code has two meanings and would have to be cloned.
2. **A mapping's meaning depends on the runtime it is executed with** (M2M in memory vs chained over a relational
   store). The translator would need the runtime at each routed call (often an inline `^Runtime(...)`): data flow, not
   syntax.
3. **A flag is a property of the routed CALL, not of the lambda.** `testSubstringIndexingCorrectedByFeatureFlag`
   (`executionPlanTest.pure:1449-1464`) binds one lambda by `let` and passes it to two `executionPlan` calls, one with
   `CORRECT_SQL_SUBSTRING_INDEXING` and one without: one syntactic site, two meanings. Exact only if the translator
   copies the lambda per call.
4. **Today's lite (main) gives host-side code legend-engine's meaning too**, since the core's `substring`/`indexOf` are
   legend-engine's: `executionPlanTest.pure:936-982`'s `indexOf(...) != -1` is then always true (STRPOS answers 0 on
   a miss), a vacuous assert. Under R4 those asserts become real.

### F2. The `###Relational` dynaFunctions are the core's to split, not the translator's

7 of H3's 15 newly failing corpus tests are dynaFunction mappings (the six `propertyfunc` tests and
`sqlFunction::indexOf::testProject`). The normalizer turns the DSL's `substring`, `indexOf` and `position` into calls
of the Pure functions (`normalizer/RelOpTranslator.java:451-457`, `:469-473`, `:497-505`), so the core's change of
meaning moves them. They are not Pure code in the parsed model, so a translator over the parsed model never sees them.
The split is 3.1's, inside the core (§3).

### F3. 3 of H3's 15 are already CP-4's "not promised"

`testModelConnectionAgg`, `testModelConnectionMultipleAgg` and `testSubstringIndexingCorrectedByFeatureFlag` are in
B2's 143 (`docs/records/census-b2-sqltext-probe-2026-11-19.tsv` on `claude/census-b2`, classes a1 and a2): they judge
legend-engine's plan text and nothing else. No rewrite can make them pass (a rewrite changes the SQL text), so they
are not the translator's to win.

### F4. With the translator, H3's 15 come down to the 4 that judge legend-engine's text, and nothing else moves but one vacuous pass made real

The corpus, Pure's meaning in the core, the dynaFunction split and the use-site translator (variant V3, §2):

| lane | main (V0) | core change, translator off (V2) | core change + translator (V3) | V0 to V3 |
|---|---|---|---|---|
| H2 | 2,289 pass, 276 fail, 7 skipped | 2,281 pass, 284 fail | 2,285 pass, 280 fail | +4 fail, all 4 in B2's CP-4 list |
| DuckDB | 2,452 pass, 108 fail, 7 skipped | 2,443 pass, 117 fail | 2,447 pass, 113 fail | +5 fail: the same 4, and one vacuous pass made real |

- **V2 to V3 is the translator alone: exactly +4 passes per lane and nothing else moves**: the 3 query-lambda tests
  (`testFilterUsingSubstringFunction`, `testFilterUsingParseIntegerFunction`, `testFilterUsingParseDecimalFunction`)
  and `testToSQLStringSubstringIsUncorrectedByDefault` (a `toSQLString` lambda: rewritten, then decided by the appeal
  on rows).
- **11 of H3's 15 pass, on both lanes:** those 4 by the translator and the 7 dynaFunction tests by the core's split.
- **The 4 that stay failing are all in B2's 143** (CP-4, not promised): `testModelConnectionAgg`,
  `testModelConnectionMultipleAgg`, `testSubstringIndexingCorrectedByFeatureFlag`, `testToSQLStringWithPosition`.
  Each judges legend-engine's plan or SQL text only, with no replay the appeal can run (B2's classes a1, a2); they
  passed at main only because the edge's engine-style printer reproduces that text, which CP-4 deletes anyway.
- **8 tests now pass BY ANSWER through the appeal** (the unordered-register gains 8 lines on each lane): their SQL-text
  assert no longer matches legend-engine's golden text, so the referee replays the golden statement and compares rows
  (the propertyfunc 4 of 6, `sqlFunction::indexOf::testProject`, the two query-lambda filters,
  `testToSQLStringSubstringIsUncorrectedByDefault`). This is CP-4 and §12 I3 working as designed: the appeal is
  load-bearing. It moves a register, which is a guarded file: not moved here.
- **DuckDB's fifth: `testPlanForDateTimeVariableESTTimeZone` was a vacuous pass at main.** Its assert
  (`executionPlanTest.pure:982`) is `$planAsString->indexOf('...') != -1 || 'settlementDateTime = TIMESTAMP...' != -1`.
  Under main's meaning `indexOf` answers 0 on a miss, so the left side is always true and DuckDB short-circuits. Under
  Pure's meaning it is a real check, it is false (lite's plan text is not legend-engine's), and the right side runs:
  legend-engine's own typo compares a String with an Integer, which legend-pure answers `true` and lite's SQL sends to
  the database, where DuckDB raises a conversion error (`Could not convert string ... to INT32`). H2 already raised on
  it at main (`Data conversion error converting "CHARACTER VARYING to DECFLOAT"`). Two findings: F1.4 confirmed (the
  old meaning made host-side `indexOf` asserts vacuous), and **a product defect: a cross-kind `!=` (String vs
  Integer) raises in the database instead of answering `true`** (S127's family; for the product owner). The code is
  host-side and correctly NOT rewritten.

## 1. The spike's shape

**The translator** (`spec/src/test/java/com/legend/compatspike/EngineTranslator.java`, 494 lines, about 400 without
comments and blank lines):
- **input:** the parsed model (`Compiler.ParsedModule`) and the target, read from the runtime's connection
  (`ConnectionDefinition.databaseType()`: H2 or DuckDB; any other target refused by name);
- **work:** a deep map over the model's records (functions, classes' derived properties and constraints, mappings,
  services, test bodies): every record whose components change is rebuilt by its canonical constructor, so a renamed
  call keeps everything else, its `SourceInfo` included. A call of `substring` (2 or 3 arguments) or `indexOf`
  (2 arguments), bare or by its `meta::pure::functions::string::` name, is renamed to its twin in
  `meta::legend::compat::engine::`, H3's target-aware library (`spec/src/test/resources/compatspike/`, unchanged);
- **scopes** (`-Dcompat.translator`): `identity` (rebuild every record, rename nothing), `everywhere` (the
  definition-site rename), `queries` (the use-site rename: only the code legend-engine sends to the store);
- **`queries` finds the store-bound code** syntactically: the first argument of a routed call (`execute`,
  `executionPlan`, `toSQLString`, `toSQL`, `toSQLStringPretty`, `toNonExecutableSQLString`, `executeInternal`,
  `printDebug`, `relationalMapperSqlQuery`, `executeLegendQuery`, counted over the corpus), a lambda bound by `let` in
  the same statement list and passed to one, and the parameters of the helpers that forward one (a fixpoint over the
  model's functions, by simple name); a routed call carrying `CORRECT_SQL_SUBSTRING_INDEXING` keeps Pure's `substring`;
- **output:** the rewritten `ParsedModule` plus the library's source, handed to the normal compiler through its public
  doors: the corpus harness parses the library with the model (`Compiler.parseSources`), translates, then builds
  (`Compiler.buildModule`) and runs through the test door as before (`MinimalCorpus.java`, +24 / -3); the probe uses
  `Compiler.parseQuery`, `Compiler.query`, `Execution.runOnProjectDatabases`.

**It sits in the spec tests' source tree on purpose**: only the core's public API is reachable from there, so "no
new hook" is checked by javac.

**The core** (the only core change, `core/src/main/java/com/legend/`):
- `lowering/Scalars.java`, +20 / -26: H3 (a)'s change exactly: `substring` always `CORRECTED_SUBSTRING` (Pure's), string
  `indexOf` 0-based with -1 on a miss, and Java's `indexOf(t, from)`;
- `normalizer/RelOpTranslator.java`, +49 / -5: 3.1's split for the dynaFunctions (§3), no new native.

**The runs** (each both lanes, `bazel build //spec:judge_host_h2 //spec:judge_host_duckdb`, outputs saved under
`h5/corpus/`):

| variant | core | translator |
|---|---|---|
| V0 | main | none |
| V3 | Pure + dyna split | `queries` |
| V2 | Pure + dyna split | off |
| V4 | Pure + dyna split | `everywhere` |
| V5 | Pure + dyna split | `identity` |

## 2. The measurements

### (a) H3's 15, and what else moved: F4 above. Variants V2, V4, V5 below as they land.

### (b) The core APIs it needed; no new hook

Public API only, checked by javac (the spec tests' tree reaches nothing else):
- `Compiler.parseSources`, `Compiler.ModelSource`, `Compiler.ParsedModule` (its record constructor),
  `com.legend.model.ParsedModel` (its record constructor and `keyOf`), `Compiler.buildModule`;
- the parsed model's records (`com.legend.model.*`, `com.legend.protocol.spec.*`), read and rebuilt by reflection
  over their record components;
- `ConnectionDefinition.databaseType()`, to know the target;
- for a request: `Compiler.parseQuery`, `Compiler.query(ctx, ValueSpecification)`, `Execution.runOnProjectDatabases`.

**No new hook and no core change beyond the meaning were needed by the translator.** The two core changes are
Pure's meaning (R4, H3's diff) and the dynaFunction split (3.1), both planned slices, neither a hook. One trap found
on the way: the parsed model keys `elementSources` by `ParsedModel.keyOf` (an overload's own key), not by the
qualified name, so a translator that looks its library up by name rewrites the library's own bodies into
self-recursion and every compat call is left un-inlined ("store resolution left user call ... uninlined"). The
library must be recognised by its key (or its package).

### (c) Source positions

The probe (`spec/src/test/java/com/legend/compatspike/TranslatorProbeTest.java`; `bazel test
//spec:h5_translator_probe --test_output=all`; output `h5/probe-c.out`) plants 9 cases over one H2 model, each with
the translator off and on:
- **positions are kept on the record:** a renamed call carries the user's `SourceInfo` (line 1, columns 6-14 in the
  printed record), and every argument keeps its own;
- **but no lite error on these doors prints a position, with or without the translator:** a type error is
  `TypeInferenceException phase=TYPE position=null`; a run-time raise is `DataError: text is not an integer`, and the
  plan's raise site is `Site[kind=TEXT_NOT_INTEGER, where=null]`. So the rewrite cannot move a position there today;
  the guard I6 asks for needs a door that reports positions first;
- **the words move:** a type error at a rewritten call names the compatibility function the user never wrote: `in
  call to 'meta::legend::compat::engine::substring', argument 1: expected String, got Integer` (off:
  `'meta::pure::functions::string::substring'`). A presenter (1.2) or the translator must map the name back;
- **run-time raises are unchanged** (the same kind and words) when the raise is the user's own call, inside an
  argument of a rewritten call or over its result;
- **SQL grows:** the target-aware window is inlined at each call: the probe's answer SQL grew from 289 to 469
  characters (one rewritten call) and from 319 to 1,055 (a rewritten call under `parseInteger`), G3's concern.

### (d) Size, time, what broke

- **Size:** translator 494 lines (about 400 without comments); library 32 + 33 Pure lines (H3's); the harness door
  +24 / -3; the probe 161; core +69 / -31 (Scalars, RelOpTranslator).
- **Time, the corpus world** (11,168 elements; 1,029,638 records): `buildModule` 3.4-4.1 s. The translator:
  - **26.1-26.8 s** as first built (V3: each record's components read and made accessible again at every node);
  - **1.1-1.3 s** with each record class's shape read once (V5, `identity`, which rebuilds EVERY record: the upper
    bound of the walk). A real translator would be a typed visitor over the protocol records, not reflection, and
    cheaper still.
- **The pass-through check (3.2's first check) holds:** with every one of the 1,029,638 records rebuilt and nothing
  renamed (V5), all four rosters on both lanes are identical to the translator-off run (V2).
- **The probe's small model:** 4-12 ms to translate, against 548-690 ms to build.
- **What the translator saw in the corpus:** 5 `substring` calls in store-bound code (all 3-argument), 0 string
  `indexOf`; kept Pure's: 78 + 4 `substring`, 190 `indexOf` (mostly the list `indexOf`, the same meaning in both), 6
  three-argument `indexOf`; 34 records rebuilt. 188 helper functions forward a query into a routed call (by simple name, so an over-count). **40 routed calls have
  a first argument the syntactic search cannot follow** (a property such as `$t.query`, a function call, a string):
  data flow; none of them moved a corpus verdict here, but a real translator must either follow them or refuse.
- **What broke:** nothing beyond F4's list.

## 3. 3.1 sizing: the product's own dependencies on today's meaning

**The finding: there are three, not two.** The plan names lite's Cypher translator and the `###Relational`
dynaFunction. Lite's `#SQL` (Postgres) frontend and its `#IQ` frontend write today's meaning too.

### 3.1 (i) The `###Relational` dynaFunctions: split in the spike, cheap

- **Where:** `normalizer/RelOpTranslator.java` turns the DSL's `indexOf` (`:451-457` at main), `substring`
  (`:469-473`) and `position` (`:497-505`) into calls of the Pure functions.
- **The split built** (+49 / -5, no new native): each dynaFunction keeps SQL's meaning written over Pure's:
  `indexOf(s, t)` and `position(t, s)` become `indexOf(s, t) + 1`; `substring(s, a[, n])` becomes
  `substring(s, a - 1[, a - 1 + n])`, a literal operand folded so Pure's rule (`CORRECTED_SUBSTRING`) writes SQL's
  `SUBSTRING(s, a, n)` back with the same operands. The database answers exactly as before.
- **Measured:** the 7 corpus tests H3 lost pass again (V2, both lanes); their SQL text moves (`locate(...)` becomes
  `(locate(...) - 1) + 1` for a computed operand), so their SQL-text asserts are decided by the appeal on rows.
- **The caveat:** the split relies on Pure's `substring` lowering being a pure index shift over SQL's `SUBSTRING`.
  3.3 plans "a negative start raises, as Pure does"; then a dynaFunction `substring(col, 0, n)` (start 0 is legal SQL
  and the corpus uses it: `simplePropertyFunc.pure:169`) would raise. Once 3.3's raise lands, the dynaFunction needs its
  own rule: one lite-internal native lowered verbatim to `SqlFn.SUBSTRING` (and `STRPOS`), which moves
  `NativeCatalogGovernanceTest`'s `INTERNAL_DESUGAR` ceiling (the user's word), or a lowering of the DSL's own
  operation without a Pure call.
- **Size:** done for the spike (0.5 day); with 3.3's raise, +0.5 day and one guard move.

### 3.1 (ii) The frontends: 47 call sites in 4 files, mechanical

At main, these write `substring` with SQL's 1-based start and a length, and `indexOf` 1-based with 0 on a miss:

| frontend | file | `substring` | `indexOf` | lines at main |
|---|---|---|---|---|
| `#Cypher` | `protocol/cypher/CypherToPure.java` | 20 | 1 | 466, 491, 7988, 11674, 11677, 13123, 14455, 14469, 14471, 14472, 14480, 14774, 14793, 17781, 17817, 17833, 17881, 18964 |
| `#SQL` (Postgres) | `protocol/sql/PostgresFunctions.java` | 8 | 2 | 212, 458, 477, 692, 787, 788, 789, 908, 911, 915 |
| `#IQ` | `protocol/sql/iq/IqFunctions.java` | 10 | 4 | 667, 910, 1512, 1515, 1541, 1580, 1993, 1998, 2008, 2151, 2194, 3241, 4593 |
| `#IQ` | `protocol/sql/iq/IqAggregates.java` | 1 | 1 | 1489 |

(`CypherToPure.java:21544` writes Pure's `substr`, already Pure's meaning; `SqlParser.java:1423` builds a SQL AST node,
not a Pure call; `IqFunctions.java:1774` infers a width. None moves.)

- **The split, written for the spike:** two shared helpers in `protocol/sql/SqlToPure.java`, `sqlSubstring(s, start[,
  length])` and `sqlIndexOf(s, t)`, the same rule as the dynaFunctions', and each call head retargeted to them. The
  measurement of the parity lanes with and without it is below.
- **Size:** 1 to 1.5 days with the parity lanes (cypher, sql, iq, each against snapped answers), plus the same caveat
  as (i) once 3.3's raise lands (a frontend whose start can be 0 or below must keep SQL's rule there).

### 3.1, the total

The plan's 1.5 days covered Cypher and the dynaFunction. Measured: about 2 to 2.5 days for the dynaFunctions, three
frontends and their parity lanes; +0.5 day and one guard move once 3.3's negative-start raise lands.

## 4. 0.6, the ledger's first draft

`H5-translator-ledger-draft.tsv` beside this record (generator: `h5/ledger.py.txt`, over H1's TSV).

- **129 rows:** one per H1 row the translator touches (106 rows: every R0, R1, X other than lite-scope, and S
  defect), split by audience or database where the action differs, plus 6 rows the spike found (H5-D1 to H5-D6).
- **Actions:** rewrite 34; pass 38 (of which the parser's, the presenter's, the judge's or the core's, said in the
  row); Pure's answer recorded 21; lite answers where upstream refuses 20 (E-R2); refuse by name 16.
- **52 rows are flagged unclear**, each with its reason in the `note` column. The biggest groups:
  - **CP-5's cost** (S9, S20, D03, D08 class A): the rows move to "Pure's answer recorded" by CP-5, pending 0.4 (j);
  - **E-R2 against H1's refusals** (S50, S364, S29, D06's ENGINE half): H1 classed them X under E-R1; E-R2 says lite
    answers, so what lite answers (lite's own text, or legend-pure's) is unruled;
  - **text read inside a query** (D01, D02, D04, D06, D25, S121, S140): refused by name where the content is read
    (CP-5, 1.3's rule), presented at a root;
  - **the generator's text compared inside a body** (S191, S306): a presenter after the run cannot reach it;
  - **reproducing upstream's defects or non-determinism** (S305's vacuous pass, S360's double read): the user's word.
- **Evidence:** H1's (read at the pins by H1) unless the row says "H5 read"; this session re-read the substring,
  indexOf and null-ordering rows (`pureToSQLQuery.pure:4660-4666`, `:10644`; `dbExtension.pure:360-407`).
- **Tests:** where a test exists it is named; "to write" means none exists yet (an oracle battery or a PCT case).
