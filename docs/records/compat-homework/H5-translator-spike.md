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

**Status: done (the time box's work), 2026-11-19.** Committed after each finding as it was measured.

## The answer, on one page

0. **Task 0, the user's question ("do we just lose substring/indexof or lots of other things break too?"): with no
   translator at all, little else is lost.** Beyond `substring`/`indexOf` (-4 corpus tests per lane), every other
   meaning strand switched to the product's costs the corpus -1 on H2 (-4, +3) and -4 on DuckDB, pure/v1 1 request
   (`sqrt` of a negative), PCT DuckDB and H2 nothing, PCT SQLite 5 (ruling (c)'s Decimal-as-REAL). Each loss is
   legend-engine departing from Pure's own definition or its database settings (CP-12's column). The one large
   dependency, `PureText.raw` (the whole DuckDB corpus lane), is input acceptance for the H2-written corpus, not a
   meaning. Of the options for `substring`/`indexOf`, a typed use-site translator wins +4 per lane, a rename pass by
   name wins nothing, a core flag is the mode the program deletes (§Task 0).
1. **The model-level, target-aware translator works through the public doors, with no new core hook.** On the
   relational corpus (both lanes), with Pure's `substring`/`indexOf` in the core, the translator wins back exactly the
   tests it can win (+4 per lane, nothing else moves: V2 to V3). Of H3's 15 newly failing tests, 11 pass; the 4 left
   are all in B2's CP-4 list (they judge legend-engine's plan or SQL text and nothing else). One DuckDB pass that was
   vacuous under the old meaning becomes a real check and exposes a product defect (F4).
2. **The rewrite is per USE and per TYPE, not per name and definition (F1, F5).** legend-engine gives `substring` and
   `indexOf` the database's meaning only in code it sends to the store; everywhere else they are Pure's. And a rename
   by name breaks lite's type inference (the list `indexOf`). Both were measured on the corpus (V4: worse by 4 and 6
   tests); typed retargeting, built from public classes, removes the typing breaks (V6).
3. **3.1 is the gate, and it is bigger than planned (§3).** Lite's own frontends (Cypher, `#SQL`, `#IQ`; 47 call
   sites) and the `###Relational` dynaFunctions write today's meaning. Without their split, Pure's meaning in the core
   breaks about 1,240 parity cases; with it (built here), no parity answer moves. 9 old-meaning pins and 8 text goldens
   move, listed.
4. **(c) positions:** the rewrite keeps every node's source position, but no lite error on these doors reports one
   today, with or without it; the words do leak the rewrite (a type error names `meta::legend::compat::engine::
   substring`).
5. **(d) cost:** translator 584 lines (spec tree; 494 before the typed retarget), library 65 Pure lines; 2.7 s on the corpus world's 11,168
   elements for the use-site scope, 7.1 s with typing (a first build plus 65 bodies); `buildModule` is 3.5-4 s.
6. **0.6:** the ledger's first draft has 132 rows (every H1 row the translator touches, plus 9 of the spike's), 53
   flagged; the strand rows carry task 0's measured cost.

**For phase 3, in order of consequence:**
- **3.1 lands before 3.3 (R4)**, with the frontends in it (§3); re-estimate about 2 days, +0.5 and one
  `NativeCatalogGovernanceTest` ceiling move once 3.3's negative-start raise lands.
- **3.2's skeleton must be use-site and typed:** find the store-bound code (routed calls, helpers, let-bound lambdas;
  40 corpus sites need data flow or a refusal), type it with upstream's names, retarget the String-overload calls.
  Decide early: whether `ArchitectureTest`'s entry rule admits an edge caller of `SpecCompiler` (the first possible
  hook question), and who words the compat name back in errors (1.2's presenter or the translator).
- **Two semantic questions the translator cannot answer alone:** an M2M mapping chained over a relational store
  (the meaning depends on the runtime at each routed call), and `CORRECT_SQL_SUBSTRING_INDEXING` on one of two calls
  that share a `let`-bound lambda. Both appear in the corpus only in CP-4 tests.

## Task 0 (the user's question, 2026-11-19 ~10:55 UTC): what is lost with NO translator at all?

The user: "do we just lose substring/indexof or lots of other things break too? ... I wouldn't build a whole
translation machine for substr/index but if turns out that we lose a lot more then might be worth it". The
orchestrator's brief: list every strand where today's edge makes lite compute legend-engine's MEANING (not its text or
words), switch each to the product's meaning, and count what moves on the corpus (H2, DuckDB), the pure/v1 net and
PCT.

**How it was measured.** One switch per strand, `-Dh5.off=<strand>,...` (`base/src/main/java/com/legend/base/H5Off.java`;
read by the edge's `PureText`, `EdgeLoweringForms` and the Lowerer's feature flag), on top of variant V2 (Pure's
`substring`/`indexOf` in the core, the 3.1 splits, no translator). Runs: V7 (every strand off), V8 (every strand off but
`raw`), V9 (one strand at a time on H2, where the attribution needed it); pure/v1 with every strand off. Every move
is attributed by its failure reason and, where two strands could explain it, by its single-strand run.

**The table.** Tests lost (negative) or won (positive) when the strand gives the product's meaning, against V2; the
pure/v1 net has 28 golden lines (14 requests, each also as its plan). PCT: §Task 0 (b) below.

| strand (today's hook) | corpus H2 | corpus DuckDB | pure/v1 | CP-12: against Pure's own definition | legend-engine's defect, or its real meaning |
|---|---|---|---|---|---|
| `substring`/`indexOf` (S130, S134; `FeatureRules`, the core's rule) | -4 | -4, and 1 vacuous pass made real | 0 | **departs**: Pure is 0-based, end-exclusive, -1 on a miss | **its own acknowledged legacy**: "correcting substring unconditionally would change the results of existing queries, hence the flag" (the flag's doc); `duckdbExtension.pure:237`: "TODO - pure uses 0-based indexing ... many user tests need to be fixed" |
| S20's bare-DECIMAL Float literal (`PureText`'s `floatLiteralKind`, 5 dialects) | **+2** | 0 | 0 (2 lines move in SQL text only) | **departs**: a Pure Float is a double | **defect**: its database types a bare literal DECIMAL; here it costs legend-engine's OWN goldens (`'52'` where they say `'52.0'`) |
| the plain set (`ENGINE_UNCHECKED`, pure/v1 only) | n/a | n/a | **-1** (`raise.sqrtOfNegative`: 200 null vs lite's raise); `divideByZero` words only | **departs**: Pure raises | **defect**: no check written |
| `isNumeric` (S370, `EdgeLoweringForms.isNumeric`) | 0 | 0 | 0 | **Pure is silent** (a `###Relational` dynaFunction only, `sqlDialectDefaults.pure:973`) | its real (and only) meaning, `lower(s) = upper(s)` |
| `parseDecimal`, one argument (S365) | **-2** | **-2** | 0 | **departs**: legend-pure parses exactly (`CoreHelper.parseDecimal`, a `BigDecimal`) | **defect**: a hard-coded `DECIMAL(5, 2)` that rounds and overflows (`h2Extension2_1_214.pure:474-479`) |
| `parseJson`'s `}{` tolerance (S366) | **+1** | **-1** | 0 | Pure is silent on JSON leniency; the text is not JSON | **defect** (lenient parse); and lite's edge form of it is itself broken on H2 (`REGEXP_REPLACE` flag `'g'`: "Invalid value g") |
| SQLite Decimal as REAL (`refusesDecimalArithmetic`) | n/a | n/a | n/a | **departs**: a Decimal is exact | not legend-engine's (it has no SQLite): lite's own ruling (c) for the PURE audience; PCT SQLite **-5** (below) |
| `PureText.raw` (legend-engine's H2 `executeInDb` text translated) | n/a (H2 runs it as written) | **-2,452: the whole lane** (every setup's H2 text fails) | 0 | Pure is silent (SQL text, an input) | neither: **input acceptance**. legend-engine runs its corpus on H2 only; the translation exists so lite can run that corpus on DuckDB |
| `LEGACY_SQL_NULL_UNSAFE_EQUALS` (a Feature flag, the Lowerer's) | -1 | -1 | 0 | **departs** (Pure's `==` is null-safe) | its real legacy meaning, opt-in by flag; the 1 test asserts PLAN TEXT ("expected plain equals in plan"), CP-4: no answer moved |
| `DatabaseSettingsForm` (H2 `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH`) | -1 | n/a | 0 | Pure is silent (database settings) | legend-engine's database config (CP-8: not emulated); the 1 test compares a BOOLEAN with `1`, which only LEGACY allows |
| `CrossKindNumbers` (S127, the database judge) | 0 | 0 | 0 | compiled legend-pure departs from interpreted | judge-only: the corpus is host-judged and pure/v1 has no asserts, so it cannot move them |

**Attribution:** V7 (every strand off) H2 2,280 / 285 against V2's 2,281 / 284: -4 (parseDecimal 2, nullunsafe 1,
settings 1) and +3 (S20 2, parsedJson 1), each confirmed by a single-strand run (V9: settings 2,280; S20 2,283;
parsedJson 2,282). V8 (every strand off but `raw`) DuckDB 2,439 / 121 against 2,443 / 117: -4 (parsedJson 1,
parseDecimal 2, nullunsafe 1). V7 DuckDB with `raw` off too: 0 / 2,613 (`setup failed: ... createTablesAndFillDb()`).
pure/v1 with every strand off: 8 lines move (4 requests and their plans), of which 1 request in answer.

**PCT** (channel A as it runs today: the PURE audience, judged against PCT's expected values; the ENGINE lane, judged
against legend-engine's manifests, is B1-R's and does not exist yet). `bazel test --test_env=H5_OFF=none|all` over the
5 DuckDB suites, H2's relation suite and the 5 SQLite suites (`h5/pct/`):

| strand | PCT DuckDB (5 suites, 345 essential) | PCT H2 (relation) | PCT SQLite (5 suites) |
|---|---|---|---|
| `substring`/`indexOf` (the core change, strands on) | **+3**: S134's 3 `indexOf` known failures now pass (PCT's expected values; the runner reports them as "expected an error but the test succeeded": ledger rows to move with the user's word) | 0 | **+3**, the same 3 |
| every other strand off | 0 | 0 | **-5**: `refusesDecimalArithmetic` off, so `testRemWithDecimals`, `testDecimalTimes`, `testDecimalDivide`, `testDecimalPlus`, `testDecimalMinus` are refused by name ("a Decimal's arithmetic ... reached SQLite, which holds no exact decimal: refused, never approximated") |

S20, `isNumeric`, `parseDecimal`, `parseJson` and the settings move no PCT test on any of these lanes.

**What it says:**
- **Beyond `substring`/`indexOf`, no strand loses more than 2 corpus tests on a lane, and two strands WIN tests when
  turned off** (S20 on H2, `parsedJson` on H2). The answer losses all come from legend-engine departing from Pure's own
  definition (`parseDecimal`'s `DECIMAL(5, 2)`, the unchecked `sqrt`) or from its database settings (CP-8).
- **`PureText.raw` is the one strand that matters, and it is not a meaning.** It is how lite runs legend-engine's
  H2-written corpus on DuckDB. Its home is the corpus harness (or the target-aware DDL of slice 2.3), not a translator
  of programs.
- The text-only moves (`nullunsafe`'s plan text, S20's SQL text in pure/v1) are CP-4's, not losses.

### Task 0 (c): the four options, measured where the corpus can measure them

For `substring`/`indexOf` (corpus pass counts, H2 | DuckDB):

| option | H2 | DuckDB | against (D) | cost and what it gets wrong |
|---|---|---|---|---|
| today (V0): the core in legend-engine's meaning | 2,289 | 2,452 | +8 / +9 | the core carries legend-engine's meaning for every audience, host-side code included (F1.4's vacuous asserts) |
| **(A)** a model-level translator, use-site and typed (V3; V6's typing) | 2,285 | 2,447 | **+4 / +4** | 584 lines; 2.7 s to 7.1 s per corpus-sized model; one compile per (audience, target); data flow it cannot follow (40 corpus sites), M2M chained over a store, a flag on one of two calls sharing a lambda; error words name the compat function |
| **(B)** a rename pass by name, H3's library at model level (V4) | 2,281 | 2,442 | **0 / -1** | worse than nothing: it renames host-side code (meaning) and the list `indexOf` (typing: 7 breaks) |
| **(C)** a flag in the core (legend-engine's own design, and lite's today) | as today | as today | +8 / +9, of which 4 per lane are CP-4 text the printers' deletion loses anyway | a mode in the core (against CP-1); one flag or two-form hook per strand: today's `FeatureRules` flag plus the 16 two-form hooks and 5 facets this program deletes |
| **(D)** nothing: Pure's meaning, register rows (V2) | 2,281 | 2,443 | 0 | the 4 per lane (A) wins; and the 4 CP-4 tests and DuckDB's vacuous pass, which no option keeps once the engine-text printers go |

For the other strands, (B) is not the same thing: `parseDecimal`, `isNumeric` and `sqrt` have ONE String or Float
overload, so a rename by name cannot break typing; it still renames host-side code, which legend-pure runs in Pure's
meaning. Each is a single ordinary-function rewrite (`parseDecimal($s, 5, 2)` on H2, `$s->toLower() == $s->toUpper()`,
`if($x < 0, |[], |sqrt($x))`), and each wins at most 2 corpus tests or 1 pure/v1 request (the table above).

**The answer to the user's question:** no, lots of other things do not break. With no translator at all, beyond
`substring`/`indexOf` (-4 per lane), the corpus nets -1 on H2 (-4, +3) and -4 on DuckDB, pure/v1 loses 1 request
(`sqrt` of a negative), PCT DuckDB and H2 lose nothing (and gain 3 from `indexOf`), and PCT SQLite loses the 5
Decimal-arithmetic tests ruling (c) answers with REAL today. Every one of those losses is legend-engine departing from Pure's own definition or its database
settings. The one large dependency, `PureText.raw` (the whole DuckDB lane), is input acceptance for legend-engine's
H2-written corpus, not a program meaning, and belongs with the corpus harness or 2.3's target-aware DDL.

### Task 0 (d): the independent review's three points (session `session_0195rQEnCA5MRQEk6iGfgDnb`, plan r6)

1. **"A parsed model has no types."** Measured. The spike's `queries` and `everywhere` scopes rename by name over the
   parsed model (unresolved calls), relying on H3's library overloading every upstream overload; V4 shows that is not
   enough (an overload set changes inference: 7 breaks). The typed scope (V6) resolves first: it builds the model
   through `Compiler.buildModule` (`Compiler.java:697`), types each candidate body with `SpecCompiler.compile`
   (`compiler/spec/SpecCompiler.java:84`), and renames by the typed calls' positions. **Option (B) does not survive for
   `indexOf`** (two overloads); it survives for single-overload functions.
2. **"Target-aware means one compile per (audience, target)."** Measured: the corpus world's `buildModule` is 3.4-4.1 s
   per target; the typed scope adds a second build and the typing (7.1 s). `HeldModels` keys a held model by its text's
   hash alone, the runtime being an overlay view (`HeldModels.java:27-40`): a target-aware rewrite changes the text per
   target, so a host holding one model and planning it on two targets compiles it twice, and the key must name the
   audience and the target. The server holds nothing (one compile per request), so it pays only the translator's own
   time per request.
3. **CP-12.** The table's CP-12 column: `substring`/`indexOf`, S20, the plain set, `parseDecimal`,
   `LEGACY_SQL_NULL_UNSAFE_EQUALS` and SQLite's REAL arithmetic **depart from Pure's own definition**; `isNumeric`,
   `parseJson`'s leniency, `raw` and the H2 settings are where **Pure is silent**. Under the strict reading (lite answers
   Pure's way wherever legend-engine departs from Pure's definition), the translator keeps nothing of this list but
   `isNumeric` (and `raw`, which is the corpus harness's), consistent with option (D).

### Task 0 (e): notes the orchestrator asked to be recorded

- **The words leak the rewrite (a presenter finding for 1.2):** a type error at a rewritten call names
  `meta::legend::compat::engine::substring`; the ENGINE presenter (or whatever wording the door does) must map a
  compatibility function's name back to upstream's.
- **The standing-rule question (not pursued):** a typed retarget needs, from the edge, a model's typed function bodies:
  today `ModelContext.findFunction` and `SpecCompiler.compile(TypedFunction)`, both public. Whether an edge caller of
  `SpecCompiler` passes `ArchitectureTest.PLANNER_ENTRIES_ONLY` was not checked; if it does not, that is the core entry
  the translator would need. With +4 per lane at stake for `substring`/`indexOf`, the orchestrator judges a translator
  unlikely to be built.

## 0. The findings

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

### F5. The definition-site rename is wrong twice over: in meaning and in typing

Variant V4 renames every `substring`/`indexOf` call in the model (H3's request-level rename, applied to the whole
model), against V3's use-site rename:

| lane | V3 (use site) | V4 (every definition) | moved |
|---|---|---|---|
| H2 | 2,285 pass, 280 fail | 2,281 pass, 284 fail | +4 fail |
| DuckDB | 2,447 pass, 113 fail | 2,442 pass, 119 fail | +7 fail, -1 (the vacuous pass comes back) |

- **In typing:** all 4 new failures on H2 and 6 of the 7 on DuckDB are `type: cannot access 'name' on String` (the
  `columnValueDifference*` and `rowValueDifferenceTest` tests). legend-engine's own TDS helper `tds/tdsExtension.pure:39` sorts by
  `sortBy(c|$columnsToCheck->indexOf($c.name))`, the LIST `indexOf`. Renamed to the compatibility overload set (a String
  overload and a generic `T[*]` one), lite's typer infers the lambda's `c` as a String. **An untyped rename to a user
  overload set changes type inference**, so a real translator must retarget TYPED calls: only a call that resolved to
  `meta::pure::functions::string::indexOf(String[1], String[1])` (H3's note 2, now measured on a whole model). The
  use-site scope escaped it only because no list `indexOf` sits in store-bound corpus code.
- **In meaning:** the rename gives host-side code legend-engine's meaning, which legend-engine never gives it: DuckDB's
  `testPlanForDateTimeVariableESTTimeZone` passes again, vacuously (F1.4), and one accepted divergence's witness moves
  (`columnValueDifferenceWithoutPrevalTest`).
- **The shape a typed retarget needs:** type the program with upstream's names first, collect the calls whose resolved
  callee is the String overload (by source position: each call keeps its own `SourceInfo`), then rename exactly those.
  The pieces are public today: `ModelContext.findFunction` gives a model function's `TypedFunction`, and
  `compiler.spec.SpecCompiler.compile(TypedFunction)` (`SpecCompiler.java:84`) types its body; a query's typed body is
  `TypedQuery.body()`. So it looks reachable without a new hook, at the cost of typing every body that holds a candidate
  call twice (once with upstream's names, once rewritten). One thing to check before 3.2 relies on it: whether
  `ArchitectureTest`'s listed-entries rule admits an edge caller of `SpecCompiler` (the spike, in the spec tests'
  tree, is outside the rule's scope).
- **Built and probed (`EngineTranslator.stringCallPositions`, `translateAt`):** build the model once with upstream's
  names, type every function whose body names `substring` or `indexOf` (`SpecCompiler.compile`), collect the
  positions of the `TypedNativeCall`s whose callee is `meta::pure::functions::string::substring` or `::indexOf`, and
  rename exactly the calls at those positions. In the probe (`h5/probe-c.out`, lines `F5`) it renamed `s::firstE`'s
  String `indexOf` (the answer is legend-engine's, `[[0], [3]]`), kept `s::byPos`'s list `indexOf`, and left the
  untypeable `s::bad` unrenamed: 4 bodies typed in 4 ms. **No new hook: public classes only.**
- **The corpus through it (V6: the typed retarget, at every definition):** H2 2,285 pass / 280 fail, identical to V3;
  DuckDB 2,448 / 112, V3 plus the vacuous pass back. **Every new failure of V4 is gone** (H2 4, DuckDB 7: the 6 typing
  breaks and the moved witness); what remains of V4's damage is meaning alone (host-side code renamed). It renamed 48 calls (16 `indexOf`, 32 `substring`) and kept 174
  list `indexOf` and 55 `substring` that did not type as the String overload. The cost: 7.1 s (a first
  `buildModule` with upstream's names, then 65 bodies typed in 2.6-2.8 s); 83 candidate functions do not type in lite
  (mostly legend-engine's own implementation files the corpus loads) and keep their names.
- **So the translator 3.2 needs is BOTH:** typed (rename only what resolved to the String overloads: V6) and use-site
  (only the code sent to the store: V3). Together they are `typedqueries` (in the spike, not run on the corpus: on this
  corpus it equals V3, since no list `indexOf` sits in store-bound code).

## 1. The spike's shape

**The translator** (`spec/src/test/java/com/legend/compatspike/EngineTranslator.java`, 584 lines with the typed
retarget of F5; 494 without it):
- **input:** the parsed model (`Compiler.ParsedModule`) and the target, read from the runtime's connection
  (`ConnectionDefinition.databaseType()`: H2 or DuckDB; any other target refused by name);
- **work:** a deep map over the model's records (functions, classes' derived properties and constraints, mappings,
  services, test bodies): every record whose components change is rebuilt by its canonical constructor, so a renamed
  call keeps everything else, its `SourceInfo` included. A call of `substring` (2 or 3 arguments) or `indexOf`
  (2 arguments), bare or by its `meta::pure::functions::string::` name, is renamed to its twin in
  `meta::legend::compat::engine::`, H3's target-aware library (`spec/src/test/resources/compatspike/`, unchanged);
- **scopes** (`-Dcompat.translator`): `identity` (rebuild every record, rename nothing), `everywhere` (the
  definition-site rename), `queries` (the use-site rename: only the code legend-engine sends to the store), and F5's
  `typed` and `typedqueries` (the same, renaming only the calls that typed as the String overloads);
- **`queries` finds the store-bound code** syntactically: the first argument of a routed call (`execute`,
  `executionPlan`, `toSQLString`, `toSQL`, `toSQLStringPretty`, `toNonExecutableSQLString`, `executeInternal`,
  `printDebug`, `relationalMapperSqlQuery`, `executeLegendQuery`, counted over the corpus), a lambda bound by `let` in
  the same statement list and passed to one, and the parameters of the helpers that forward one (a fixpoint over the
  model's functions, by simple name); a routed call carrying `CORRECT_SQL_SUBSTRING_INDEXING` keeps Pure's `substring`;
- **output:** the rewritten `ParsedModule` plus the library's source, handed to the normal compiler through its public
  doors: the corpus harness parses the library with the model (`Compiler.parseSources`), translates, then builds
  (`Compiler.buildModule`) and runs through the test door as before (`MinimalCorpus.java`, +40 / -3 with the typed
  scope); the probe uses
  `Compiler.parseQuery`, `Compiler.query`, `Execution.runOnProjectDatabases`.

**It sits in the spec tests' source tree on purpose**: only the core's public API is reachable from there, so "no
new hook" is checked by javac.

**The core** (every core change, `core/src/main/java/com/legend/`; none is a hook):
- `lowering/Scalars.java`, +20 / -26: H3 (a)'s change exactly: `substring` always `CORRECTED_SUBSTRING` (Pure's), string
  `indexOf` 0-based with -1 on a miss, and Java's `indexOf(t, from)`;
- `normalizer/RelOpTranslator.java`, +49 / -5: 3.1's split for the dynaFunctions (§3), no new native;
- `protocol/` (`CypherToPure`, `PostgresFunctions`, `SqlToPure`, `IqFunctions`, `IqAggregates`), +75 / -42: 3.1's split
  for the frontends (§3), applied after the corpus runs (the corpus uses none of them).

**The runs** (each both lanes, `bazel build //spec:judge_host_h2 //spec:judge_host_duckdb`, outputs saved under
`h5/corpus/`):

| variant | core | translator |
|---|---|---|
| V0 | main | none |
| V3 | Pure + dyna split | `queries` |
| V2 | Pure + dyna split | off |
| V4 | Pure + dyna split | `everywhere` (F5) |
| V5 | Pure + dyna split | `identity` (the pass-through check, (d)) |
| V6 | Pure + dyna split | `typed`: every definition, typed retarget (F5) |

## 2. The measurements

### (a) H3's 15, and what else moved

F4 (V2, V3) and F5 (V4, V6) above; V5, the pass-through check, in (d). Each variant's rosters, diffed, are in
`h5/corpus/`.

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

- **Size:** translator 584 lines (494 before F5's typed retarget); library 32 + 33 Pure lines (H3's); the harness door
  +40 / -3; the probe 205; core: the meaning +20 / -26 (Scalars), the dynaFunction split +49 / -5, the frontend split
  +75 / -42 (5 files).
- **Time, the corpus world** (11,168 elements; 1,029,638 records): `buildModule` 3.4-4.1 s. The translator:
  - **26.1-26.8 s** as first built (V3: each record's components read and made accessible again at every node);
  - **1.1-1.3 s** with each record class's shape read once (V5, `identity`, which rebuilds EVERY record: the upper
    bound of the walk); **2.7 s** for the `queries` scope (V3 re-run, the same rosters), of which 1.4 s is the
    helper fixpoint (a whole-model walk per round); **7.1 s** with F5's typing (V6). A real translator would be a
    typed visitor over the protocol records, not reflection, and cheaper still.
- **The pass-through check (3.2's first check) holds:** with every one of the 1,029,638 records rebuilt and nothing
  renamed (V5), all four rosters on both lanes are identical to the translator-off run (V2).
- **The probe's small model:** 4-12 ms to translate, against 548-690 ms to build.
- **What the translator saw in the corpus:** 5 `substring` calls in store-bound code (all 3-argument), 0 string
  `indexOf`; kept Pure's: 78 + 4 `substring`, 190 `indexOf` (mostly the list `indexOf`, the same meaning in both), 6
  three-argument `indexOf`; 34 records rebuilt. 188 helper functions forward a query into a routed call (by simple
  name, so an over-count). **40 routed calls have a first argument the syntactic search cannot follow** (a property such as `$t.query`, a function call, a string):
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

- **Measured without the split** (Pure's meaning in the core, the frontends untouched; Run A, `bazel test` of the
  three parity lanes and `//core:core_tests_integration`; all green at `main`, where they are in `//gates:local`):

  | lane | at `main` | Pure's core, no frontend split |
  |---|---|---|
  | `//core:cypher_parity_test` (the openCypher TCK and lite's cases, against snapped answers) | green | **808 problems** (`everyCaseHasItsLedgerStatusOnThePlatform`; e.g. `Create3/[5]`: a node's properties read back through `substring` as `"{num":1}`, malformed JSON), and the pinned showcase SQL moved |
  | `//core:iq_parity_test` (SAP IQ's answers) | green | **402 problems** (e.g. `a_substring_of_a_computed_length`: `expected 'ell' but got 'l'`) and the showcase SQL moved |
  | `//core:sql_parity_test` (Postgres's answers) | green | **27 regressions** (e.g. `concat_ws`, `overlay`) |
  | `//core:core_tests_integration` | green | 2 fail: `CypherIslandIntegrationTest.aMarkedListOfTemporalsReadsItsElementsBackOrIsReadWhole` and `ExtendCheckerTest$StringExtend.testIndexOf` (a pin of the old meaning) |

  H3 saw only the Cypher integration test because it ran `//core:core_tests`, not the parity lanes. **R4 cannot land
  before the frontends' split**: it would break about 1,240 parity cases.
- **The split, written for the spike:** two shared helpers in `protocol/sql/SqlToPure.java` (+33), `sqlSubstring(s,
  start[, length])` and `sqlIndexOf(s, t)`, the same rule as the dynaFunctions', and each of the 47 call heads
  retargeted to them (`CypherToPure` 18 lines, `PostgresFunctions` 10, `IqFunctions` 13, `IqAggregates` 1).
- **Measured with the split** (Run B: the three parity lanes and all of `//core:core_tests`, Pure's core meaning, the
  dynaFunction split and the frontend split together; `h5/parity/`):

  | lane, DuckDB cases | no split (Run A) | with the split (Run B) |
  |---|---|---|
  | Cypher (5,081) | 3,540 pass, 321 fail, 406 error | 4,267 pass, 0 fail, 0 error: the ledger test passes |
  | IQ (8,991) | 5,782 pass, 212 fail, 18 error | 5,984 pass, 19 fail, 15 error: the ledger test passes |
  | Postgres SQL (2,362) | 1,624 pass, 27 fail | 1,651 pass, 0 fail: the lane passes |

  **No answer regresses.** What still fails is text and pins, every one listed (`h5/parity/runB-*`):
  - **9 tests pin the old meaning** (H3's list less the two the splits fixed): `TypeInferenceIntegrationTest`'s
    `testSubstringStart`, `testStringIndexOf`, `testStringIndexOfSimple`, `testStringIndexOfFromIndex`,
    `testIndexOfOneElement`, `testSortWithKeyFunction` (in `//core:core_edge_tests`); `ExtendCheckerTest$StringExtend.
    testIndexOf`; `ValueSortComparatorTest.keyFunctionSort`; `SubstrIndexingTest.substrAlwaysCorrected` (it pins
    "substring passes its indexes straight to SQL by default"). They move with R4 (3.3), each listed;
  - **8 goldens of the frontends' generated text move**: `CypherToPureTest` 4, `PostgresToPureTest` 1,
    `IqFunctionsTest.substringReadsAnsiVirtualPositions` 1 (each compares the Pure the frontend writes:
    `substring(x, 2)` is now `substring(x, 1)`), and the Cypher and IQ showcases' pinned SQL (the same shift in the
    generated SQL). Re-blessed with the split, each a reviewed diff;
  - the Cypher island test (`aMarkedListOfTemporalsReadsItsElementsBackOrIsReadWhole`) and the DynaFunction
    property-mapping test pass again.
- **Size:** 1 to 1.5 days with the parity lanes (cypher, sql, iq, each against snapped answers), plus the same caveat
  as (i) once 3.3's raise lands (a frontend whose start can be 0 or below must keep SQL's rule there).

### 3.1, the total

The plan's 1.5 days covered Cypher and the dynaFunction. Measured: the split itself is mechanical and done in the spike
(dynaFunctions +49/-5; frontends +75/-42 in 5 files) with no answer moving on any parity lane; the work left for the
real slice is the 17 listed test moves (9 old-meaning pins with R4, 8 goldens with the split), its review and CI. About
2 days, and +0.5 day with one guard move once 3.3's negative-start raise lands. **It must land before R4 (3.3), or about
1,240 parity cases break** (Run A).

## 4. 0.6, the ledger's first draft

`H5-translator-ledger-draft.tsv` beside this record (generator: `h5/ledger.py.txt`, over H1's TSV).

- **132 rows:** one per H1 row the translator touches (106 rows: every R0, R1, X other than lite-scope, and S
  defect), split by audience or database where the action differs, plus 9 rows the spike found (H5-D1 to H5-D9); the
  strand rows' notes carry task 0's measured cost.
- **Actions:** rewrite 34; pass 39 (of which the parser's, the presenter's, the judge's or the core's, said in the row);
  Pure's answer recorded 23; lite answers where upstream refuses 20 (E-R2); refuse by name 16.
- **53 rows are flagged unclear**, each with its reason in the `note` column. The biggest groups:
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
