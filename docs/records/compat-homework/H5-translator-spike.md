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

| lane | main (V0) | V3 | moved |
|---|---|---|---|
| H2 | 2,289 pass, 276 fail, 7 skipped | 2,285 pass, 280 fail, 7 skipped | +4 fail, all 4 in B2's CP-4 list |
| DuckDB | 2,452 pass, 108 fail, 7 skipped | 2,447 pass, 113 fail, 7 skipped | +5 fail: the same 4, and one vacuous pass made real |

- **11 of H3's 15 pass, on both lanes:** the 3 query-lambda tests by the translator
  (`testFilterUsingSubstringFunction`, `testFilterUsingParseIntegerFunction`, `testFilterUsingParseDecimalFunction`),
  the 7 dynaFunction tests by the core's split, and `testToSQLStringSubstringIsUncorrectedByDefault` (by the appeal).
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
- **Time, the corpus world:** `buildModule` 3.4-3.6 s; the translator **26.1-26.8 s** on the
  first build (uncached reflection: each record's components read and made accessible at every node). A cached
  version (each record class's shape read once) is in V2's build onward; its time is reported below. A real
  translator would be a typed visitor over the protocol records, not reflection.
- **The probe's small model:** 4-12 ms to translate, against 548-690 ms to build.
- **What the translator saw in the corpus:** 5 `substring` calls in store-bound code (all 3-argument), 0 string
  `indexOf`; kept Pure's: 78 + 4 `substring`, 190 `indexOf` (mostly the list `indexOf`, the same meaning in both), 6
  three-argument `indexOf`; 34 records rebuilt. 188 helper functions forward a query into a routed call (by simple name, so an over-count). **40 routed calls have
  a first argument the syntactic search cannot follow** (a property such as `$t.query`, a function call, a string):
  data flow; none of them moved a corpus verdict here, but a real translator must either follow them or refuse.
- **What broke:** nothing beyond F4's list.

## 3. 3.1 sizing

(to be filled in)

## 4. 0.6, the ledger's first draft

(to be filled in)
