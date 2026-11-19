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

## 1. The spike's shape

(to be filled in: the translator, its scopes, the core change, the runs)

## 2. The measurements

(to be filled in)

## 3. 3.1 sizing

(to be filled in)

## 4. 0.6, the ledger's first draft

(to be filled in)
