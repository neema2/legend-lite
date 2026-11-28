# R0: independent review of edition design v2 (2026-11-25)

Reviewed: `/home/user/s1/docs/records/edition-design-v2-2026-11-25.md`, plus the coordinator's three amendments (one meaning site; region, presenters and A3; placement "parts"). Code read at `/home/user/s2` (da231244). In the citations below, `L/` means `/home/user/s2/core/src/main/java/com/legend/` and `ENG/` means the pinned legend-engine source (`/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src/`). The [R1] numbers are not reviewed.

## Verdict

**Not ready to approve.** "Rules per query" and "the model is rule-free" hold. But the central claim fails: one call site in the typer cannot decide legend-engine's meaning. Legend-engine decides placement only after inlining, while lite's typer types each call from its callee's signature alone. The same gap breaks the corpus region in every version proposed.

---

## MUST-FIX before approval

**M1. Placement cannot be known when a call is minted (§4.4, §4.5, and amendments 2 and 3).**
- Lite's typer: "a call is typed from the callee's *signature*, never by recursing into its body" (`L/compiler/spec/SpecCompiler.java:30-33`). Bodies are typed later:
  - at G½ (`L/compiler/spec/UserCallInliner.java:596`);
  - at H (`L/resolver/ClassSources.java:200,738`; `AssociationJoins.java:1223,1360,1854`; `StagedRows.java:224`);
  - in the test planner (`L/testplan/BodyClass.java:371`; `ElementConsumption.java:244`; `BodyDriver.java:260,288`).
- Legend-engine's router does the opposite:
  - it inlines single-expression user functions before routing (`ENG/.../core/pure/router/routing/router_routing.pure:325-328, 555-559`);
  - it refuses multi-expression functions;
  - a call becomes routed when an operand is (`:553-555`: `shouldBeRouted = $lastSecondPass.shouldBeRouted || isGetAllFunction($f)`), and the flag resets at `letFunction`.
- So for `myPeople()->toOne().name->substring(1,3)`, the typer cannot tell whether `myPeople()` reads a store. The proposed rule, "the call's own operands read a store", needs its operand's body.
- A taint is per parameter, not per caller. A body called once with a store argument and once with a literal list needs a key by argument-taint vector. The memo key (function, rule set, part) is not enough.
- Function values typed at their definition and evaluated inside a row lambda elsewhere get the wrong part. A3 refuses exactly this case today (`PureMeaningRewrite.java:126-129`).
- **Fix:** choose, and say so, one of:
  - (a) a placement pass after G½ that stamps placement on typed HIR (the first review's option (a));
  - (b) eager typing of user calls in operand position, which breaks the driver's rule above; measure its cost;
  - (c) keep A3's approach (inline, then decide).

  A1 and amendment 3 do not work as written.

**M2. The corpus region cannot be found at typing time (§4.5, `forArgument`, and the region in amendment 2).**
- In legend-engine's relational sources, `execute(` takes:
  - a lambda literal about 1,745 times;
  - a variable 302 times (223 of them `execute($query`, from `let query = {|...}`, for example `.../core_relational/relational/tests/mapping/union/testUnion.pure:265`);
  - a call 7 times.
- A let-bound or parameter-passed query is typed at the `let`, or in the caller, before the typer meets `execute`.
- The test planner finds the query only after typing, by peeling:
  - `Lets.bound`;
  - β-inlining a lambda-building user call;
  - unwrapping `preval` and `withFeatureFlags`;
  - taking a literal `if`;
  - folding `concatenateTemporalTdsQueries`.

  See `L/compiler/spec/ExecuteChainAssembly.java:63-110`.
- `forArgument(TypedNativeCall, int)` cannot be called as specified: the `TypedNativeCall` does not exist until its arguments are typed (`L/compiler/spec/Overloads.java:106-151`).
- Other natives also take a query: `executionPlan(...)` (8 uses in the tests), `toSQLString` (21), `executeLegendQuery` (33), and the 3 `execute` overloads (`L/builtin/NativeFn.java:855-860`).
- **Fix:** either re-type the peeled query at the test planner under the region, or measure the misassigned fraction and rule on it. Under amendment 3 the store taint handles the store-reading code, but the region is still needed to tell legend-engine's Java part (`1 == 1.0` is `true`) from the harness's Pure part (`false`). "The corpus's switch goes" is therefore not true.

**M3. The "one place" claim misses governed constructs that never reach `Overloads.applyGeneric`.**
- **Casts** are minted as `TypedCast` (`L/compiler/spec/CastChecker.java:71`). **Slices** are minted as `TypedSlice` (`SlicingChecker.java:41`).
- **Implicit toOne guards** for multiplicity conformance are not calls at all (`L/lowering/Coercions.java:66`, `ToOneGuard.java:70`).
- 5.8's ten raise kinds are chosen today in the lowering, by `RaiseKind` (`CastPolicy.java:183`, `ListEncodings.java:265`, through `EdgeLoweringForms`). A call-site rule cannot reach casts, slices or implicit guards.
- **Post-typer mints:** `AssertMessages.format` (`L/compiler/spec/AssertMessages.java:125-127`, called from `testplan/AssertVerdicts.java:436,837`) and `VerdictQueries` (`:141,313,436,504-516`).
- **Eta-expansion:** a function reference to a native (`map(toString_Any_1__String_1_)`) mints at `L/compiler/spec/Typer.java:1124`.
- **Homogeneous lists:** `makeString`/`joinStrings` over a homogeneous Float list uses the database's cast and passes no rule (B2 `e3-design.md` §1, `PureSql.elementText`). It has no row in §5.
- **Fix:** list each site and its handling, or keep a lowering-level form for raises and say so.

**M4. Folds decide more than amendment 1 lists (the `governs()` deletion).**
- `StaticFold` folds all of these with Java semantics (`L/compiler/spec/StaticFold.java:487-512`, `:600-660`):
  - `EQUAL`, including a `TypeToken` compared to a String by name, a cross-kind `true` (`:873-890`);
  - `IN`, `CONTAINS`, `INDEX_OF` and `REMOVE_DUPLICATES` (Java `equals`: `Long(1)` ≠ `Double(1.0)`);
  - `TO_STRING`, and `MAKE_STRING`/`JOIN_STRINGS` through `stringify(Double)` (`:862-870`);
  - `SORT_BY` over mixed values.
- `StaticFold` also runs in `TdsDesugars` (`:279`), not only in normalized bodies.
- `LiteralUnroll`:
  - folds `in` across numeric kinds to `false` (`Scalar` equality includes the class);
  - folds enum-vs-other, a reference against a scalar, and `TDSNull` against a scalar to `false` (`L/compiler/spec/LiteralUnroll.java:390-438`, `:596-600`).
- `UserCallInliner`'s `groupBy` unroll keys on Java values (`:1230-1260`).
- Three consequences:
  1. Several of these folds exist to make schema facts static, such as column names and types. If they stop, checkers may refuse loudly (StaticFold's own doc, `:56-58`). This must be measured.
  2. `LiteralUnroll.neverEqual` and `StaticFold.staticEquals` carry S169 ("never equal") for lite's own function bodies, which are unmarked today. If they stop folding before R3 marks bodies, lite's product answers `true` (the database compares by name). That is a product regression.
  3. Legend-engine *pre-evaluates* NormalizeRequired bodies in Pure (`router_routing.pure:202-245`, `preval`). StaticFold in normalized bodies is the analogue: it is a fourth placement, with Pure's meaning, under `engine`. Removing the fold moves those cases to the database.

  The fold changes cannot be byte-identical, so they cannot be in R2.

**M5. Sites are wrong for code legend-engine runs in Java (§4.4).**
- "Code the store resolver demands runs in SQL by definition" is false for:
  - checked constraints and their messages, which lite inlines through the resolver (`L/resolver/GraphEmission.java:584-631`) but legend-engine runs in Java (v2's own §2.1);
  - M2M under graph fetch (Java) versus a TDS result (pushed into SQL, `chain.pure:104-116`). This is decided per query, yet v2 lists "chained M2M sources" as store-demanded without condition.
- Derived properties are demanded both through the inliner and through `compileSynthFn` (`ClassSources.java:191-201`), so each gets two sites.
- This brings back the first review's regression (a checked constraint using `substring`).

**M6. Islands and the relational mapping DSL have no mechanism (ruling 1).**
- Today `marking` is a Typer bit: query on; function bodies, normalized bodies and islands off (`L/compiler/spec/Typer.java:147-183`, `:219-257`; `SpecCompiler.java:384-391`, `:434`; `Overloads.java:129-131`).
- R3 marks bodies. Relational class-mapping functions and M2M class mappings share `SynthHat.CLASS` (`L/model/SynthHat.java:28-29`), so nothing records the language.
- v2 lists ruling 1 as "in force" (§1) but gives it no place in §4. A per-function language fact is needed, separate from the part. Without it, lite's relational mappings become compile errors, or M2M stays unmarked.

**M7. 5.5 (`TDSNull`) is modelled per edition, but it is a property of the value.**
- `TDSNull` is a class in legend-engine's own Pure (`ENG/legend-engine-core/.../core/pure/tds/tds.pure`).
- The corpus harness runs in compiled legend-pure over legend-engine's legacy TDS rows and prints `TDSNull` there. Harness rows are joined outside `execute()`: 303 files use `rows->map(r|$r.values->makeString`, and `testAssociationMapping.pure:37` is one example.
- With the harness in Pure's part and 5.5's `pure` column as "legend-pure's" (not `TDSNull`), 112 DuckDB and 107 H2 corpus tests move.
- The amendment-2 premise ("the harness only matters where non-assert code uses a governed construct ... if rare") fails. Harness texts compared by asserts are common: `toString` and `makeString` over result rows.

**M8. Flags cannot be read by the door before typing (§4.8).**
- Flags are read from *typed* trees: `ContextReading.contextFeatures(TypedSpec)` and `treeFeatures(List<TypedSpec>)` (`L/compiler/spec/typed/ContextReading.java:354-419`).
- In the corpus they sit inside let-bound lambdas: `let fn = {...}->withFeatureFlags(...); execute($fn, ...)` (`testLegacyNullUnsafeEquals.pure:32,51`).
- So `EngineRules` cannot be built from them before typing without a second, untyped reader. And they are per `execute`, not per query.
- **Fix:** name the reader and its scope, or admit the typer reads flags (a second switch).

**M9. Comparisons whose types are known only at run time have no row (§5.1).**
- `EqualityClasses` marks only statically known kinds. `Number`, `Any`, type variables and generics are never marked (`L/compiler/spec/EqualityClasses.java:28-30`, `:122-140`).
- Lite's lowering compares tagged and mixed values by kind (`L/lowering/Scalars.java:3821-3870`, `TaggedLists.sameElement`), which is Pure's meaning, on every door.
- Under `engine`, `[1, 2.0]->contains(1.0)` and Number-typed `==` should promote, but nothing at place 1 sees them unless *every* `==`, `in` and `contains` over `Any` or `Number` is rewritten into a new form.
- This is missing from §5, and it is wrong at pure/v1 today too.

**M10. "Comparisons need no placement under `engine`" was measured only for numbers (§2.2).**
- For an enumeration against a String, legend-engine's SQL compares the stored value (true where the names match), while compiled Pure answers `false`. `LiteralUnroll`'s own javadoc says so (`L/compiler/spec/LiteralUnroll.java` around `:430-436`, S169).
- Texts and raises in legend-engine's Java part are unmeasured: B2 `e3-design.md` §5 says "what legend-engine answers for a DateTime's text inside a store query has not been read or asked".
- The table in amendment 3 ("pure/v1, the rest: Pure's for substring; the database's for `==`") therefore needs measured per-part answers for every row of §5. The rule of record requires them asked of the engine runner, never assumed.

**M11. Rule conflicts the §8 amendments omit.**
- Tenet 3 says "If upstream's value differs ... answered Pure's way and recorded". The upstream-fidelity paragraph in AGENTS.md says the same.
- `EngineRules` re-computes: `1 == 1.0` is `true`, raises are plain, `isNumeric` is `lower = upper`.
- That is backed by the 11-20 ("engine only mode for engine") and 11-22 rulings. But §8.5 says "the fidelity tenet: unchanged", and tenet 3 is not amended. Say it honestly.
- §8.1 ("the switch rule stays as written for the product ... nothing in the core reads a switch") contradicts §8.2. The typer calls a door-chosen strategy, and under amendment 3 that strategy answers per part.

---

## SHOULD-FIX

- **R2's own risks.** R2 is "byte-identical", but the rule set must also reach places that type outside `TypedQuery`:
  - `PlanAllocations` calls `Compiler.lowerResolved` mid-plan (`L/planner/PlanAllocations.java:398`);
  - `ServiceTestRunner` calls `hasStatementEffects` (`L/edge/ServiceTestRunner.java:277`);
  - `PureTestRunner` calls `lowerResolved`;
  - A3 uses `query.specs()` and `UserCallInliner` (`L/edge/PureMeaningRewrite.java:107-113`).
- **§4.7's count.** It lists five entries and says "four". The four `new SpecCompiler` sites are `L/TypedQuery.java:36` and `L/Compiler.java:915, 946, 995`; `TestExecution` goes through `TypedQuery`. The claims themselves hold: phase F types no bodies, the memo is per `SpecCompiler` (`SpecCompiler.java:48`), and the model-lifetime memos (`PlanMaker.Projects`, `SystemCatalog.SeedMemo`, `MetamodelSeeds`) are rule-free and keyed by dialect class (`PlanMaker.java:1312-1316`).
- **The compile route.** `compileAllBodies` only types. Under `LiteRules` a crossing marker raises nothing until planning (`PlanMaker.java:93`), so lite's compile route reports no strict-typing errors unless it scans for markers (the withdrawn `crossingsOf`).
- **Defaults.** `MeaningRules` has defaults ("identity by default", "this by default"). Under invariant 4, give every rule set an exhaustive answer per governed native; no default methods.
- **Product forms.** "Forms in the product" should be justified one by one as irreducible:
  - `IS_NUMERIC_LOWER_EQUALS_UPPER` is `toLower($s) == toUpper($s)`;
  - the `TDSNull` text is an `if`/`isEmpty` composition;
  - `parseDecimal`'s precision is a cast.

  The 11-22 ruling puts "legend-engine's departures" at the edge as door rewrites. Where a composition of lite natives exists, the product needs no new form.
- **Missing fates in §5/§6.** Give a fate to each of these, or state the exceptions that make "census at 0" reachable:
  - the referee/appeals hook in the product's test runner (`L/testexec/EdgeRun.java`, `TestRunner.java:52,82`, `TestExecution.java:45-85`);
  - `HostForms`, `LiteHostForms`, `EdgeHearing`;
  - `result/CellText.Forms`, the injected date and structure forms (`L/result/CellText.java:46-70`; `ArrowWriter.java:50,408`; `edge/Presenter.java:171`);
  - A4's judge choice (`L/testplan/TestPlanner.java:73-79` reads `PlanStyle.PRODUCT`);
  - `UpstreamPrints`, which prints a relation in Java and substitutes the text into the body (`L/edge/UpstreamPrints.java`), a presenter's output used as meaning;
  - `Styled`, `PlanEnv`, `WhereZones`, `RefusedExecutedValues`.
- **Normalized bodies and the prelude.** Name their part and marking. Today they are unmarked (`Overloads.java:129-131`, the comment "the database's comparisons").
- **Moves in the lite product.** R3 changes lite's product: crossings in function bodies, mapping code, `~func`, derived properties and constraints become compile errors. §3's product row ("unchanged") is wrong. List these as product moves for the user, with DataCube and Python models in scope (the `//gates:local` clients).
- **The PCT Pure-mode gate.** "Every backend matches the reference" cannot be met where product gaps exist (SQLite's refused decimal arithmetic, now overridden only by the edge dialect, `L/edge/PureText.java` SQLite: `refusesDecimalArithmetic`). Make it per backend, with a ledger.
- **Size.** `SHORTEST_ROUND_TRIP` costs more than 490 characters of SQL per Float text, against 47 today (B2 `e3-design.md` §6), for PCT and now the corpus harness. Bring it to the user before R4.

## The amendments, assessed

**Amendment 1: one site; folds stop folding; `forArgument` deleted.**
- Folds:
  - **Not enough as listed.** Add the StaticFold and LiteralUnroll ops in M4, the inliner's `groupBy` keys, and the TypeToken-name fold.
  - **Order:** fold removal must come *after* R3 marks bodies (S169).
  - **Not byte-identical**, so it cannot be in R2.
  - **Guard:** StaticFold's `FoldOp` enum can be intersected with the governed-native catalog in a test. LiteralUnroll needs a declarative list (`LiteralUnrollLedgerTest` pins simple names only and already contradicts the code: it calls cross-kind equality a violation while `in` folds it).
- The judges:
  - **Correct that they don't call rules.** The database judge's verdict is "kind-faithful canon" SQL (`L/testplan/AssertVerdicts.java:198-225`). `HostForms` only words `assertError`'s caught message (`L/testexec/HostForms.java:13-24`).
  - **But harness texts do reach verdicts** (M7), and `AssertMessages` mints `format` after the typer (M3). So the premise "rare" is false for the corpus.

**Amendment 2: region; presenters; A3 at the edge.**
- **The region:** see M2. The memo key must change at about 13 `specs.compile` sites, and `compileReachable` uses the memo as its visited set (`SpecCompiler.java:150-171`). It is workable but invasive. Typed nodes from two typings differ by identity, so identity-keyed caches downstream (`reducedTerms`, `pretyped`) need checking.
- **Format.** The product is not format-free.
  - `CellText.Forms` and `ArrowWriter` take an edge-chosen form.
  - `PlanStyle.graphKeyDate/graphKeyString/traceCell` write format into SQL.
  - `LoweringForms.objectReferenceConnection` and `executeValues` sit in the lowering.

  The census's "0 F core classes" counts classes, not parts.
- **The meaning/format line.** It is broken today by ruled exceptions:
  - `UpstreamPrints`, where Java's print is used as a value;
  - `HostForms.caught`, where legend-pure's wording decides `assertError`.
- **A3 as an edge step.** Honest as "not in the product", dishonest as "one place for meaning". A3 and the test-body rewrites (`UpstreamTestBodies`: S137, S144, S131 order-free) are further meaning mechanisms. A3 also runs the planner's `UserCallInliner` at the door (`PureMeaningRewrite.java:107-113`), the pattern the user called "way too many hacky things" when it was generalised (START HERE §4 g). It is safe today because it refuses what it cannot follow (mapping code, function values, shared lets, unchecked databases). But it runs only for pure/v1, not for the corpus or PCT.

**Amendment 3: parts.**

| Question | Assessment |
|---|---|
| Can the typer know the part at mint time? | Yes for the lambda arguments of natives on a receiver that visibly reads a store, and for lets (env taint; lambda lets are typed again at their use, `Typer.deferredLetRhs`). No for user calls in operand position, function parameters, function values, and generic bodies whose taint comes from the argument (M1). `if` is fine. |
| Is "inside a lambda over rows" the right test? | No. A3's pinned case puts `let d = ...->toOne().name->substring(1,3)` in SQL, outside any lambda (`PureMeaningRewriteTest.aValueLetRewrittenAndReadByAStatementOfItsOwnIsNotRefused`). The router's rule is operand-routedness after inlining (`router_routing.pure:553-555`). |
| The let corner | `$d->toUpper()` stays on the host (the same test). That needs a let rule: a scalar let is materialized, while a relation or class-collection let is inlined into its readers (`aStoreBoundLetIsRewrittenInsideTheLet`). It is unverified upstream. |
| Shadow A3 | It is an oracle only where A3 answers: 3 natives, on H2 and DuckDB, for pure/v1 only. Where A3 refuses, there is no oracle, and comparisons, texts and raises have none at all. "Agree, then delete" validates placement for substring only. |
| Corpus: harness Pure, `execute()` arg database | Breaks M7 (`TDSNull`, harness `toString`/`makeString`). The taint must stop at `execute()`'s result, or harness code over result rows becomes "database part". In-query store-free code needs legend-engine's Java part, not Pure's. |

## Does §10 answer the first review's must-fixes?

| # | Answered? |
|---|---|
| 1. A3 / substring | Partly. A3 is described correctly. But the changed cases are not listed: constraints (M5), chained M2M per query, substring in the corpus region, PCT engine mode. |
| 2. The real list | Partly. Missing: M9 (run-time kinds); the homogeneous-list text; the enum-vs-String placement; the run-time edge channel; `UpstreamPrints`; `CellText.Forms`; A4's judge choice. |
| 3. "One read site" | No (M3, M4). Casts, slices, implicit guards and post-typer mints are unnamed. Generic code is handled for texts, not for comparisons (M9). |
| 4. E3 moves | For PCT, yes, from B2's measurement. For the corpus harness under Pure, no (M7). SQL size is not addressed. |
| 5. Held models | Yes. Verified: `L/HeldModels.java` is keyed by text; phase F types no bodies (`compiler/element` uses no `Typer`/`SpecCompiler`). |
| 6. Ruling 3 | Yes, re-asked (§11.4). |
| 7. M2M / XStore | Numbers only. Not asked: enum-vs-String (M10), relational XStore, chained-M2M TDS, constraints. |
| 8. Flags | No (M8). |

## Guards (§7)

| Guard | Writable? |
|---|---|
| 1. Direction | Exists. |
| 2. No edition in the product | As ArchUnit, "no product class depends on a `MeaningRules` implementation but `LiteRules`" can be written. "Names `pure` or `engine`" cannot, given `com.legend.builtin.Pure`, `PureTestRunner`, `EngineTestText`. |
| 3. Forms chosen only by rules | Not provable statically: exhaustive enum switches touch every constant. Writable as a run-time witness (plan every core test under `LiteRules` and assert no non-lite form in any plan) plus an ArchUnit allowlist of the classes that may reference non-lite form constants. |
| 4. No rule identity in the plan | Writable. |
| 5. The matrix | Writable, but it must add parts and run-time-kind operands. |
| 6. Ratchets | Call-site count (callers of `atCall` is 1): ArchUnit. The A3 residue list: yes. |
| 7. Lean SQL | Exists. The R4 growth needs the user's word. |
| 8. Census at 0 | Only as a symbol ban (`PlanStyle`, `Feature`, `twoForm`, `facet`). The semantic census is a reading. |
| Missing | "Folds touch no governed native"; "no governed native minted after the typer" (needs a run-time witness); the part and language fact on synthesized functions. |

## Phases (§9)

- R2 is byte-identical only without the fold change, with every rule set as identity, and with the rule set reaching `PlanAllocations`' nested `lowerResolved`.
- R3's body marking must precede fold removal (S169), and M6's language fact must land first.
- The region or part prototype is the program's largest risk. Measure it in R1 (a new M7: the share of corpus queries typed at a `let` or a parameter, and the part disagreements with A3) before R3, not "confirm in R2" (risk 2).
- R4 depends on R3's region (harness texts) and on the "row's values as text" explicit call (B2 `e3-design.md` §2), which no phase schedules.
- R5 depends on per-query M2M placement and the constraint site (M5).
- R6's DECIMAL deletion must be measured against PCT engine mode (M4 run both ways). Bucket 5 is possible where DECIMAL is what makes legend-engine pass.
- R7 cannot reach census 0 until the missing fates (SHOULD-FIX) are decided.

## What gets worse, by phase and audience

| Phase | lite product | legend-engine's door (pure/v1) | Corpus | PCT Pure mode | PCT engine mode (DuckDB) |
|---|---|---|---|---|---|
| R2 (if the folds change here) | S169 regresses in function bodies (`true`); schema folds may refuse | normalized bodies lose preval parity | the same | possible schema-fold refusals | — |
| R3 | crossings in bodies, mapping code, `~func`, derived properties and constraints become compile errors; relational DSL at risk (M6); DataCube and Python models refused | enum-vs-String in host code: the database's, not Java's `false` | harness moves from the database's to Pure's comparisons; let-bound queries (~15%) misplaced | body crossings move to `false`; "both may be empty" is a new refusal (`L/edge/PureComparisons.java:47,70-74`) | — |
| R4 | none | Java-part texts unmeasured | 112/107 `TDSNull` tests if `pure` ≠ `TDSNull` (M7); Float-text SQL about 10× | 0 moves (B2); SQL about 10× per Float text | unknown ([R1] M4) |
| R5 | none | mapping-code and graph-fetch substring better; checked constraints worse (M5); chained M2M under graph fetch wrong | in-region substring still Pure's unless parts extend | — | substring the database's (amendment 3): bucket 2 |
| R6 | none | Float arithmetic DECIMAL → DOUBLE, away from legend-engine on 5 databases | the same; H2 null ordering moves | toward Pure: better | bucket 3 or 5 risk |
| R7 | none | `planToString` and `executionPlan` inside programs refused (ruling 2) | harness moved | — | — |

## Decisions for the user (not yet asked)

1. Placement: a post-inlining stamp pass (in the core's planner), eager typing of bodies in operand position, or keeping A3's inline-then-decide (M1).
2. The corpus queries the typer cannot place (let-bound, passed as a parameter): re-type at the test planner, or accept and record them (M2).
3. Is `TDSNull` Pure's meaning for legacy TDS rows, and so in `pure` (M7)?
4. Placement of checked constraints and M2M under `engine` (M5).
5. A language fact for relational DSL and islands, separate from placement (M6).
6. Run-time-kind comparisons under `engine`: a new form for every `Any`/`Number` comparison, or record lite's answer (M9).
7. New product forms versus compositions of lite natives at the edge (11-22 ruling, G5).
8. SQL size for `SHORTEST_ROUND_TRIP` in PCT and the corpus harness.
9. Where "host judge only in compatibility" lives once `PlanStyle` is gone; the fate of the referee hook, `UpstreamPrints` and `CellText.Forms`.
10. Amending tenet 3 and the fidelity text for `engine` re-computing (M11).
11. A per-backend gate for PCT Pure mode.

## Not verified

- How common the "part" disagreements are, and the share of the corpus that is let-bound or parameter-passed. The 302/1,745 count is a regex over all of `core_relational/relational`, not the corpus roster.
- Whether removing the StaticFold and LiteralUnroll folds breaks schema computations in the corpus or PCT.
- Legend-engine's let materialization rule, and the per-part answers for texts, raises and parses.
- Whether the prelude, normalized platform bodies and lite's relational mappings contain crossings.
- Whether the shadow A3 comparison can be run on PCT engine mode at all (A3 is pure/v1-only).
- `TDSNull` in legend-pure itself, as opposed to legend-engine's compiled core.
