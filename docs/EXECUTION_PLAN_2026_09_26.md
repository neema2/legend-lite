# The execution plan: every step, what we do, what homework is done, what is owed, how it is gated

Written 2026-09-26 after `PLAN_AUDIT_2026_09_26.md`. This is the document a fresh session executes
from. It supersedes the ORDER in `REAL_PLAN_2026_09_25.md` and the first-step detail in
`A1_HOMEWORK_2026_09_25.md`; the design's stages, principles and rulings stand
(`COMPILER_DESIGN_2026_09_25.md`, corrected where its banner says). The research behind every claim
here is in `docs/plan-audit-2026-09-26/` (six files, each with file:line evidence):
`reference-matching.md`, `engine-resolution.md`, `code-traps.md`, `lowering.md`, `cycles.md`,
`renderer.md`. Read those before touching the step they cover; they are the homework, not this page.

## 0. How this works this time

The previous attempt went wrong in four repeatable ways: facts were stated from memory or from the
engine's Java instead of the reference's; a step's specification did not name what else reads the
thing it changes; a gate proved well-formedness (a regex, a green exit code) rather than substance;
and timing was read only when it was already 5x. The rules below are the mechanical answer to each.

1. **Homework before code, from primary sources.** Every step has a homework note that quotes the
   pinned trees (`$(bazel info output_base)/external/+http_archive+legend_{pure,engine}_src`) by
   file:line and lists every reader of every symbol the step changes (grep counts in the note).
   The six audit files are that note for the first steps; later steps write theirs the same way.
2. **Probe before switch.** The `LL_SHADOW=1` probe (CANDIDATES/PICK/FORM rows) or a census runs
   BEFORE the switch and is saved as a receipt; the switch is judged against it.
3. **The gate is the oracle plus the numbers.** Both corpus rosters LOST 0; conformance unchanged;
   the manifest census not grown; the identity pins shrink-only; and the CORPUS LANE TIMING LINES
   READ at every gate, on a quiet machine (`uptime` load under 2, no other Bazel), against the
   receipts' curve. A step that needs a special case to keep the rosters is wrong and stops.
4. **Deletions land in the same commit** as the switch, with the tests that pinned the deleted
   behaviour. A pin that must move gets a dated justification naming the step.
5. **One variable at a time.** Names constant while the world changes; world constant while names
   change (charter D7(c)). No two steps in flight against the same rosters.
6. **The size guard is amended, not dodged.** A file at the 3,500-line guard may be split ALONG A
   STAGE SEAM (the typer's `applyCore` and checkers; the lowerer by family) by the step that touches
   it, with the seam named in the record. Splitting by topic to stay under the guard stays forbidden.
7. **Every slice: homework → probe → switch → gate → deletion → GATES.md record → push to main**
   with the full chain green (`bazel test //... //parser-equivalence:diagnostics` and
   `bazel test //tools/deps:all`), never force-pushed, committed as `neema2 <neema2@gmail.com>`
   with the session trailers.

## 1. The steps, in order

### Step 0 — Structure: the annotation move and the target split (now, ~1 day)

**What it is.** The two marker annotations `Nullable`/`NonNull` leave `com.legend` (which makes the
whole of core one 26-package cycle); two leaf classes move; `//core` is split into the Bazel targets
the dependency graph already permits. After this, every back-edge a later step would introduce is a
build error, not a test found afterwards.

**What we do.**
- First extend `tools/untangle/move_classes.py` `ROOTS` (today `core, spec, pct, parser-equivalence`)
  to every directory that references the annotations: `warehouse` (24 files), `wasm`, `tools`,
  `testing`, `datacube` if it has Java. Then `python3 tools/untangle/move_classes.py --group A`
  (groups.txt: `com.legend.base <- com.legend.Nullable com.legend.NonNull --inline`). Then the
  build-file carriers, which the tool cannot see: `core/BUILD.bazel:42-43` AND
  `warehouse/BUILD.bazel:17-18` (its own copy of `CustomNullableAnnotations`/
  `CustomNonnullAnnotations`; grep every `BUILD.bazel` for the old FQN), `ArchitectureTest.java:273-276`
  `NULLNESS_ANNOTATIONS`, the 4 root-package files using bare `@Nullable`. Prove EVERY null gate is
  live (core's and warehouse's) by injecting one violation in each and watching it fail, then remove
  it. A build that stays green with the old FQN in a flag is a disabled gate, not a passing one.
- Keep `//core` as an umbrella target that exports the new targets, so the six consumers
  (`spec`, `pct`, `parser-equivalence`, `wasm`, `tools/engine-runner`, `warehouse`) need no change
  in the same commit; they move to fine-grained deps later, one at a time.
- Coordination with the concurrent warehouse session: `docs/IN_FLIGHT.md`.
- Groups B/C in groups.txt: `resolver.AsorRef → lowering` (the lowering↔resolver cycle is two
  constants read at `SnapshotEnvelope.java:134,140`); `compiler.element.StoreLookups → compiler`.
- `core/BUILD.bazel`: targets `annot`(base), `values`, `error`, `spi`, `lexer`, `protocol`(+spec),
  `model`, `parser`(+section), `builtin`, `platform`, `element_type`, ONE `compiler_mid` for
  {compiler, compiler.element, compiler.spec, compiler.spec.typed}, then `lowering`, `resolver`,
  `sql`, `sql_dialect`, `exec`, `normalizer`, `plan`, `lineage`, root, `server`, `ide`, `cache`,
  `probe`. A `//core:parser_tests` lane for the ~34 front-end-only test files.
- NOT now: any move of `Typer`, `InferenceKernel`, `NameResolver`, `FunctionCompiler`, `BareNames`,
  `StatementInline`, `LiteralMapUnroll`, `Temporal`, `TypeClassifier`, `KnowledgeLayer` (the last
  two were REFUTED as moves by the same-package check).

**Homework done.** `cycles.md`: measured numbers on HEAD, the front end's zero back-edges, the
validated 7-move cut set, the back-edge table, the test-isolation counts, the rebase recipe. Tools:
study `receipts/plan-audit-2026-09-26/cycles/{class-edges.py,simulate.py}`.
**Homework owed.** None before starting. Re-run `simulate.py` after the moves to confirm 28 units.
**Gate.** Chain green; `bazel query 'deps(//core:compiler_mid)'` excludes exec/server; the injected
NullAway violation fails; the two constant-inlined edges appear as strict-deps errors and are fixed
by the moves, not by exemptions. Corpus timing unchanged (no compiler code changed).
**Stop rule.** A move that needs a `resideInAPackage` exemption or a "temporary" back-edge.
**Coordination.** `datacube/dual-plane` touches 32 core files; publish the one-line rebase recipe
("drop the move commit, re-run `--group A`") with the commit; coordinate `AsorRef` only.

### Step 1 — The differential becomes a gate (half a day)

> **Done 2026-09-26** (GATES.md entry of that date; numbers in `tools/reference/README.md`): AGREE
> 42,589 / OVERLOAD 450 / PACKAGE 11 / SOURCE_DRIFT 52,266 / ABSENT 42,174 / PROPERTY_AS_CALL 31 /
> EXTRA 12,971. Two findings for steps 3 and 6 fell out: our typer inserts `toOne` (2,504) and
> `elementToPath` (1,518) calls the reference never makes.

**What it is.** Today `tools/reference/join.py` joins by (enclosing function NAME, spelling) and
compares sets, so overloads of the enclosing function merge, a call we elide shows as a
disagreement, and version-drift rows count. "Zero" is not yet a meaningful number.

**What we do.**
- `spec/src/test/java/com/legend/generators/OurResolutionsTest.java`: print the call's line and
  column (`TypedNativeCall.pos`, `TypedUserCall` likewise) and the enclosing function's FunctionId.
- `tools/reference/join.py`: key on (source, line, column); a declared elision list by FunctionId
  with a reason (`print`, `assert…` bodies we do not emit); exclude a row when its declaration is
  absent on either side (4.138.5 vs 4.145.0 drift: `sqlQueryToString`, `getTemporalTableFilter`).
- Add a compile-status differential: which function bodies each compiler accepts (ours from the
  census's failure list; the reference's from `RefResolutions` function rows). The element-lookup
  rules (no own-package tier, ambiguity is an error) are visible only here.
- Re-run both dumps (`RefResolutions` per `tools/reference/README.md`; ours via the spec test with
  `-Dour.resolutions=core_relational`) and re-count.

**Homework done.** The 28 package rows read one by one (audit §3): 15 exact-beats-type-variable
(`sort` 11+3, `distinct` 1), 8 `size` on the result of `execute` resolving to `relation::size` on our
side, 4 `plus` + 1 `contains` join artefacts of elision/merging. The 799 overload rows aggregated by
shape (study `receipts/plan-audit-2026-09-26/differential-join-747ff1c11.tsv`): 507 `isEmpty`
`[0..1]` vs `[*]`, then `average`/`median` Integer vs Number, `max`/`min` `[1..*]` vs `[*]`,
`between` DateTime/StrictDate vs Date, the comparison operators `[1]` vs `[0..1]`,
`elementToPath(Type)` vs `(PackageableElement)`, `map` `[m]`/`[0..1]` vs `[*]`.
**Homework owed.** None: the 8 `size` rows are explained in `plan-audit-2026-09-26/homework-2026-09-26.md`
§1 (our TDS erasure at typing time makes the legacy `project`'s result relation-shaped, so
`relation::size` wins where the reference keeps `TabularDataSet` a class and picks `collection::size`;
32 vs 14 across the module; the two overloads count different things).
**Gate.** The join prints per-cause counts; every remaining row is attributed to a rule in
`reference-matching.md` or to a listed elision. Receipts saved.
**Stop rule.** None; this step only measures.

### Step 2 — A2: one identity, rules registered by declaration id (2 days)

> **Done 2026-09-26** (GATES.md entry of that date). The catalog generator emits one overload group
> per declared name (`Pure.AT_…`, 488); every rule table, memo and family lookup is keyed by
> `FunctionId`, which moved to `model` beside the declaration type; the bare-name registration
> API, both indexes, the second identity and the table's bridge are deleted. Two findings for the
> record: the identity exposed four qualified-property/function collisions the spelling-based key
> hid (walls, pinned with the reason; step A4 removes the lift), and the lowering helpers were
> retyped to identities rather than widening the layering rule. Left for step 3 by count: 18
> resolver lookups at a qualified name, ~70 `isToOneCall` compares by FQN text, the typer's two
> spelled family lookups.

**What it is.** Lowering already dispatches by the resolved overload (`Scalars.lower` keys by
`callee.signatureKey()`), but under lite's own key while the tables use upstream's `FunctionId`, and
the REGISTRATION side fans bare names to overloads (`Pure.nativeKeysAt`, 158 sites). A2 picks one
identity and makes every rule an explicit list of ids on a shared rule object. It is independent of
A1 and semantics-preserving, so it is the first code slice.

**What we do** (`lowering.md` F1–F3, F9–F10; `code-traps.md` A2).
- Identity = `FunctionId`. Retire `Function.signatureKey()` as a DISPATCH key everywhere (93 readers:
  12 in lowering, the inlining stacks in `StaticFold:294` and the typer, `UserCallInliner`…); keep it
  as display text only if something prints it; delete the `catalogKey` bridge in
  `ImplementationTable.build`.
- Registration API: `rule(RuleRef, FunctionId…)` lists; a `RuleRef` is a named shared rule object
  (many ids → one rule: `plus` five overloads, `family(SqlFn.LESS, "lessThan")`). Positions:
  SCALAR, AGGREGATE, WINDOW, WINDOW_AGGREGATE, FORM(family). Feature overrides stay an overlay keyed
  by (FunctionId, Feature). Delete `Pure.nativeKeysAt` (3 overloads), `nativeNamed`, `registeredAt`,
  `Index.REGISTERED_BY_BARE`, `KEYS_BY_NAME`, the `ArchitectureTest` allowlist rows for `Pure$Index`.
- The two residual name dispatches in the lowerer: `Lowerer.isFamily(n, "get"/"equal"/"eq")`
  (:3406-3410, sites :2840, :2908) and `NativeFn.LowererForm.of(callee.qualifiedName())` (:583) →
  family rows with `Position.FORM`, dispatched by id.
- The store resolver's 35 FQN-string lookups (`resolver/Callees` 7, `TemporalFrame` 6,
  `AssociationJoins` 6, `SyntheticHeads`/`GraphEmission`/`ClassSources`/`ChainDispatch` 3 each,
  `JsonSourceFrame`/`ChainNormalizer` 2) → lookups by id.
- `NULLIF` added to `SqlFn` (the one node the design's plan lists that the MIR lacks). No new IR.

**Homework done.** `lowering.md` §1–2 (what the registries are, who reads them, the recommended
registry shape, the many-to-one precedent, the DynaFn 4th column). Registration counts by file.
**Homework owed.** None: the 81 `signatureKey()` readers are classified by fate and the three
`new TypedFunction(` sites confirmed to pass a definition in `plan-audit-2026-09-26/homework-2026-09-26.md`
§2–3 (61 become `FunctionId` compares or lookups, 14 are deleted with the index and the bridge).
**Gate.** Probe PICK rows identical before/after (the receipt from step 1's run); CATALOG_LOOKUP_BY_NAME
170 → 0 and its pin deleted; rosters LOST 0; conformance unchanged; timing at the curve; the table's
kind counts exact (Intrinsic 664, Form 217, Refused 20, Body 2194, Unimplemented 71, or the new
numbers with the reason: an overload with no rule becomes Unimplemented, which is loud and correct).
**Stop rule.** A rule that can only be registered by a name pattern.

### Step 3 — #47 + A1 as one slice: the binder's candidate set and the reference's rule (3–4 days)

**What it is.** The resolver produces, per compilation, a `Bindings` value: for each call node
(by node identity), the declaration ids it may mean; the syntax node stops carrying spellings. The
kernel chooses among those declarations the way the reference does: the candidate loop with
per-candidate lambda typing and a strict re-rank. The category gate at the function merge point is
deleted. These are one slice because the kernel's tie tolerance (`allSameShape`) is what lets a
catalog native and its bodied twin coexist today; removing it before the twins are merged by id
errors on every such call.

**What we do** (`reference-matching.md` all; `code-traps.md` #47/A1; `cycles.md` §4).
- **Bindings, not annotations.** New value produced by `NameResolver` (phase D): `Map<AppliedFunction
  identity, List<FunctionId>>` (identity-keyed). Lives in `compiler` (later `bind`), above protocol,
  model and platform; the typer's `candidatesOf` and `FunctionCompiler.functionsAt` read it.
  `AppliedFunction.candidateFqns` deleted with its ten readers (`parser/OperatorParts:67,123,136`,
  `lineage/PkInference:98`, `ResolvedNames:28`, `ValidateDesugar:283`, `StatementInline:203`,
  `SortChecker:184`, `CallShapes:87`, `Typer:2531-2542`, `NameResolver:1708-1743`,
  `NameResolutionContractTest`). NEVER put `FunctionId` on the protocol node (7-package cycle).
- **The candidate rule** (`resolveCallCandidates:362-381` rewritten): qualified → the functions of
  that name in exactly that package; bare → the section's imports ∪ the core group ∪ root. The
  core group is the reference's 29 (`m3.pure:175-213`); the three engine-only packages
  (`metamodel::variant`, `metamodel::relation`, `precisePrimitives`) are dropped from Pure-source
  resolution, after the probe counts which bare corpus names used them. The own-package tier goes
  (the reference has none; `NameResolver:252`'s citation is superseded). Root fallback added. One
  index of declared functions by (package, name) built once per World (`DeclarationTable` at
  `PureModelContext:584-591` is the seed; today the universe is rebuilt per resolve at
  `knownFqns:387-397`).
- **Engine input keeps its own rule** behind the grammar: `ParsedModel` gets the dialect; the
  handler surface (`BareNames` tier 1, `EngineHandlers.fqnsOf`) applies ONLY to `LEGEND_LITE`
  trees (server `LegendHttpServer:231`, `Compiler.plan:515`, wasm). For Pure source it stops.
  Before that: run the probe (`LL_SHADOW=1`, CANDIDATES rows, `bare/node` column) over both corpus
  lanes and the census and count, per bare name, which tier resolved it. Any name that resolves
  only through tier 1 in Pure source is a missing declaration to add, not a tier to keep.
- **The kernel** (`InferenceKernel.resolveOverload:1078-1215`): implement the loop of
  `reference-matching.md` finding 5 with the orderings of findings 6–10 and 13 (per-parameter
  `GenericTypeMatch` then `MultiplicityMatch`; `TypeMatch`: simple(C3 distance) < non-concrete <
  relation/function < bottom < null; multiplicity: exact < non-concrete < (upper, lower) < null, with
  the `[1..*]`-rejects-`[*]` and MAX-upper arithmetic; collection literals typed `[n]`; a `T`
  parameter below every concrete match). Lambda arguments are typed against the CANDIDATE
  (`TypeInference.java:108-148`), then the strict re-rank; several strict-best is
  `TypeInferenceException("Too many matches …")` naming them. Delete `score`, `scoreNonLambda`
  (and `Typer.selectRankedByPresentArgs`/`checkWithDeferred`'s pre-pick if the loop subsumes it),
  `mostSpecific`, `moreSpecific`, `allSameShape`, native-over-module, `nearestInLinearization`, the
  Nil tie-break, `paramTypeScore`/`paramMultScore` as scores (they become match constructors sharing
  `unify`/`unifyMult`).
- **The merge point** (`FunctionCompiler.functionsAt:42-110`): reads the declaration table for the
  candidate ids and nothing else. Delete `PlatformTypes.isPlatformOwnedFunction` (:690-696) and
  `PLATFORM_OWNED_FUNCTIONS`, the `PCT_PROFILE` stereotype check (:102), `addModelOverloads`'s
  suppression, `SUPPRESSED_ONCE` (:70,:97) and the `ArchitectureTest:946` allowlist row.
- **Twins.** `DeclarationTable.of:60-65` keeps the bodied twin for an id; `FunctionCompiler.compile:186`
  derives `isNative` from the definition's class. Fix at the table: one declaration per id, and
  "native" means the implementation table has an Intrinsic/Form row for it (13 `isNative()`
  readers, incl. `Typer.requiresNormalization:1674` and `StatementInline:33`). This is task #43
  landing as a consequence.
- **Walls.** Zero candidates is a WALL with a reason at the call's span until step F (the loaders
  are tolerant by design: `buildModule`, `MinimalCorpus:293`, `compileAll:117-133`).
- **Tests that retire:** `PctFunctionSuppressionTest`, `BareNamesTest` (rewritten for engine input
  only), `NativeCatalogGovernanceTest:152,165`, `NameResolutionContractTest`'s candidateFqns rows.

**Homework done.** `reference-matching.md` (twenty findings, the twelve methods to read, the risks);
`engine-resolution.md` (the two languages, settled); `code-traps.md` (readers, pins, the twin
cascade, the tolerant modes); the 28 rows; the 799 by shape.
**Homework owed** (each a line in the slice's GATES.md record):
- Read the twelve reference methods in `reference-matching.md`'s list, in that order, before
  writing the kernel.
- TDS erasure leaves the typer (`homework-2026-09-26.md` §1): `TdsErasure.refineResult` stops
  rewriting typed results; the matcher treats `TabularDataSet` as a class.
- The probe count of tier-1-only bare names in Pure source.
- Measure `ResolvedNames.names` (33 sites in 17 files, each rebuilding `BareNames.catalog(name)`)
  with a real profile on a quiet machine (`jstack` from the Bazel JDK against the
  `corpus_duckdb.runfiles` JVM, or `-Dmanifest.census.timing=1`), before and after. That, not the
  33 hash lookups, is where resolution time can go.
- Decide where `Bindings` lives (`com.legend.compiler` now; `com.legend.bind` at step 10) and that
  it is immutable.
**Gate.** Differential overload 0, package 0 (after step 1's fixes), compile-status rows explained;
rosters LOST 0 (DuckDB 2474, H2 2232 at the last record; read the current numbers from the rosters);
conformance unchanged; census walls ≤ 32, failures ≤ 1,447, kernel ≤ 164; FUNCTION_CATEGORY_CHECK
13 → 11 with the pin lowered; NAME_COMPARE/MINT pins shrink by the deleted sites; per-pass corpus
time at the curve (DuckDB ~32/37s, H2 ~20/56s per pass, in-lane); JVM boot noted separately.
**Stop rule.** A roster row that can only be kept by a name test, a tier, or a tolerance in the
kernel. "Too many matches" on a corpus call the reference compiles means our types differ from its
types (a distance computed differently); fix the type, never the rule.

### Step 4 — The typer split along its stage seam (1 day)

**What it is.** `Typer.java` is 3,489 lines against a 3,500 guard; `Lowerer` 3,494; `Scalars` 3,476.
Steps 5–7 cannot add a line. Under rule 0.6 the typer splits along the seam the design names:
`applyCore:1308-1454` and the 39 `*Checker` files become the forms package; the generic path stays.
**What we do.** Move `applyCore` and the checker dispatch to `compiler/spec/forms/` (or the
existing `spec` package's checker files under one entry), with `ReceiverOwnedFunctions` and
`CoreFn` reads beside it; no behaviour change. Amend `CodeShapeGuardrailTest`'s comment with the
rule and this step's date. The lowerer's split by family happens in step 8 when E touches it.
**Homework done.** `code-traps.md` B (the dispatch order) and D (the guard numbers); `cycles.md` §3
(the compiler cycle's carriers, so the split does not add a back-edge).
**Homework owed.** None.
**Gate.** Chain green; rosters and probe rows identical; `simulate.py` shows no new cycle.

### Step 5 — B: forms dispatched by a candidate's Form row

**What it is.** A form's checker runs because a CANDIDATE declaration (from Bindings) has a `Form`
row in the implementation table, not because the spelling matched. This is the reference's loop
applied to forms: the candidate supplies the lambda's parameter types, then the strict re-rank.
**What we do.** `Typer:560-571` (`CoreFn.of(af.function())` → `applyCore`) becomes "for each
candidate id with a Form row, run its rule"; the 21 form-dispatch-by-name sites go (Typer 7,
SortChecker 4, ScanRelations 2, SourceSubst 2, MatchChecker 2, MappingNormalizer 1, ProjectChecker,
GraphFetchChecker, DeferredArgs); the spelling checks inside checkers (`join`'s canonicalisation,
`agg`'s equals, the TDS legacy vocabulary by name in `TdsLegacy`) become rule rows. `agg` gets a
declaration (upstream declares it as a bodied function in engine `core`; a catalog row until F loads
it). Candidate sets spanning two rules (`filter` → JSON or relation; `sort`/`map` collection vs
relation) are the loop's normal case. `CoreFn.NEW` (syntax, owns no FQN) and `INTERNAL_DESUGAR`
(`CoreFn:410-414`) are the two exceptions to record.
**Homework done.** `code-traps.md` B; `engine-resolution.md` finding 4 (upstream's 8 named return
inferences and 49 parameter inferences: the list of what a form rule must compute).
**Homework owed.** Per form (65 `CoreFn` arms): which Form rows it owns, which of its argument
typing is generic (moves to the kernel) and which is the rule. One table, before code.
**Gate.** FORM_DISPATCH 21 → 0, PARSE_NAME_LOOKUP 3 → 0, pins deleted; probe FORM rows zero
disagreement; rosters LOST 0; census not grown.
**Stop rule.** A form that needs the spelling to pick its rule.

### Step 6 — D: the kernel's missing rules, classified against the reference

**What it is.** The census's 164 kernel failures (top messages "T bound to Class<Any> vs …"
53/42/19/7, unbound T 11) are classified against the reference's actual binding rules, then fixed
one rule at a time, retiring each form checker that existed only because the kernel could not type
the call.
**What we do.** First the classification: for each of the 164, which of these is missing: LUB
merging of a second binding (`TypeInferenceContext.register:330-440`, with variance), multiplicity
merge by subsumption (`registerMul:276-281`), lambda typing deferred to the parent
(`FunctionExpressionProcessor:491-523, 823-853`), function-type unification with contravariant
parameters (`TypeMatch.java:490-544`), relation-type column matching (:436-488), `Class<X>`
literal typing by variance (`InstanceValueProcessor:157-181`). Then the rules, each with its census
number as the gate. `PrintTypeInferenceObserver` in the reference prints the registration trace per
body: use it to diff a failing body against ours.
**Homework done.** `reference-matching.md` findings 4, 8, 13, 15, 19; the census numbers.
**Homework owed.** The 164 classified (a TSV: body, message, missing rule, checker if any). The
inventory of which of the 39 checkers exist only for a kernel gap.
**Gate.** kernel 164 → 0 in the census; checkers shrink-only; rosters LOST 0; conformance unchanged.
**Stop rule.** A rule that only the census's bodies need and the reference does not have.

### Step 7 — C: one SHAPE evaluator (after a charter ruling; after D)

**What it is.** One evaluator over typed terms replaces the inliners and folders, but its scope is
what determines the SHAPE of the SQL, never a value the database could compute. That is the line
the execution tenet draws, and the current `StaticFold` (27 fold ops on literals, unregistered in
`JavaEvalLedgerTest`) is already on the wrong side of it.
**What we do.** First, a ruling in `docs/TENET_CHARTER.md` (and `JAVA_EVICTION_PLAN.md` §1's
decision rule) stating the line: column lists and names, column-spec sets, static `if`/`match`
branch selection, static `map` unrolling, type tokens, literal-empty detection, and a literal that
becomes an IDENTIFIER; nothing else. A `JavaEvalLedgerTest` row for the evaluator with that scope.
Then: count, per corpus test, which of the ELEVEN rewriters fired (`StatementInline`,
`Typer.inlineNormalized`, `UserCallInliner`, `StaticFold` eval/fold, `LiteralFold`,
`NormalizeFolds`, `LiteralUnroll`, `SourceSubst.inlineLets`, `LiteralMapUnroll`, `ValidateDesugar`,
`resolver/LiteralFolds`; lowering `Fold` and `MatchFold` are fold-vs-isolate, not evaluation). Then
one evaluator keyed by declaration: inlining of `Body` rows with symbols (no alpha-renaming), an
expansion computed once per (declaration, argument terms) and shared, a depth/size budget that
becomes a diagnostic; the shape rules above; the desugars (`validate`, constraints, milestoning) as
rows. Delete the eleven and `FoldOp`.
**Why after D.** Normalise-required bodies cannot be typed standalone today (`Typer:1600-1605`);
that is why two inliners work on the untyped tree. D's TDS-erased typing removes the reason.
**Homework done.** `lowering.md` F11–F13; `code-traps.md` C.
**Homework owed.** The ruling; the eleven-rewriter census per corpus test (a probe column).
**Gate.** Fold results identical per test (probe before/after); rosters LOST 0; the ledger's
residue does not grow beyond the one row; the typer's line count falls by the folder.
**Stop rule.** An evaluator rule that computes a scalar the SQL could.

### Step 8 — E: one registry (the table owns the rules)

**What it is.** The implementation table stops being derived from the rule maps
(`PlatformRegistrations:16-22` reads `Scalars.ruleKeys()`) and becomes the owner: one row per
declaration; `Intrinsic` rows hold `Map<Position, RuleRef>`; `Form` rows hold the typing rule;
`Refused` rows the reason; `Body`; `Unimplemented`. Everything that is genuinely ownership becomes a
source of rows; everything that is not is moved to where it belongs.
**What we do** (`lowering.md` §5 table). Rows: walled bodies, walled natives, subsumed
(`Refused`), family members (`Position.FORM`). Not rows: `LITE_SURFACE` and the lite internal set
(bind/visibility facts → the World's declaration table), the TDS legacy vocabulary (→ Form rules,
done in step 5), the handler surface column (a bind fact for engine input), the DynaFn column (the
translator's rename table, keyed by engine operator). The claims ledger (`spec/.../claims/`,
`native-claims.tsv`) deleted; `PreludeGenerator`, `NativesGenerator`,
`NativeSignatureGeneratorTest` read the table instead. Dialect capability is NOT a column: it is
`SqlDialect.supports(SqlFn | structural node)` checked by one MIR walk after lowering, before
rendering. The lowerer splits by family here (rule 0.6).
**Homework done.** `lowering.md` §5, F15, F16, the registry shape.
**Homework owed.** The generators' new inputs, listed; the `Scalars` 131 lambdas' move plan.
**Gate.** Every former mechanism's readers read the table; the claims generator and its tests
gone; table kinds pinned exactly; rosters LOST 0.

### Step 9 — F: load by manifest, names held constant

**What it is.** The charter's D7: the 32 walls to zero in cost order (four parser gaps, m3 routing,
measures and units, three extension types, the M2M features), then the corpus loader reads the
manifest closure (27 modules, 1,772 files) strictly: one parse, one build. Deleted: `UpstreamFiles.
LIBRARY_FILES/SHAPE_FILES`, the prelude's engine-file lists, the one-row membership.
**Why here.** One variable at a time: steps 3–8 change how names resolve with the world constant;
F changes the world with names constant.
**Homework done.** D7 in `UPSTREAM_BOUNDARY_PROGRAM.md` (the walls by cause and cost, the census
timings: parse 0.34s, model 5.7s, the strict builder's retry loop 72.7s); the census tool.
**Homework owed.** #46's remainder: the JVM boot growth (+3s per JVM from the 4b.1 universe and
prelude) is measured here, on a quiet machine, and fixed at the algorithm (the universe built once
per World, not per resolve, is the candidate; step 3 may already have done it).
**Gate.** walls 32 → 0; both rosters unchanged; the chain within the 12-minute budget; the census
pinned; boot time back at the receipts' ~1.8s.

### Step 10 — G: the seams become packages

**What it is.** The remaining three moves and two hoists from `cycles.md` §3 (`StatementInline`,
`LiteralMapUnroll` → `compiler.inline`; `Temporal` → spec; hoist `ENUM_METACLASS_FQN`; replace the
`RelationalTypeInference.infer` call in `KnowledgeLayer:406`), then `compiler_mid` splits into
`bind` / `element` / `typer` / `typed` targets; a new `ArchitectureTest` rule for cycles below the
top-level slices (today's `:171` sees only top-level ones); the size guard drops.
**Homework done.** `cycles.md`.
**Homework owed.** Re-run `simulate.py` on the post-step-8 tree before choosing the moves.
**Gate.** Zero package cycles at class level; every package a target; the guard deleted.

### Later phase, separate program — the ANSI base and a third backend

`renderer.md`: 60 of the 184 spellings the base renderer emits are DuckDB-only, 22 more are
Postgres-family, six collide silently with another database's meaning; H2 spends 26 of its 45
override points undoing the base. The shape is mechanical (`Spellings.ANSI` = 38 rows;
`Spellings.DUCKDB = ANSI + 39`; 12 arms and ~10 defaults into `DuckDb.java` or capability throws;
H2 shrinks), ~70 sites, 2–4 days. Not during the untangle: shares no files with steps 2–8, is
unobservable by every gate (DuckDB byte-identity is guarded only by execution), and a standard base
with only DuckDB and H2 executors is a base nobody runs. Trigger: an embedded Postgres corpus lane
(`//spec:corpus_postgres`, zonky; the corpus asserts are backend-independent rows, so no new
goldens). Order: render-diff harness → data split → coded arms → H2 shrink → Postgres lane green.

## 2. The pins and how they retire

| pin (IdentityGuardrailTest unless noted) | today | retires at |
|---|---|---|
| CATALOG_LOOKUP_BY_NAME | 170 | step 2 (→ 0, deleted) |
| FUNCTION_CATEGORY_CHECK | 13 | step 3 (→ 11), step 5/7 (the rest), then deleted |
| NAME_COMPARE (function share, 20 of 207), MINT (143 minus the parser's 17), LOCAL, CASE labels | | step 3 shrinks; step 5 and the A4 note retire; the parser's 17 are the floor, not zero |
| FORM_DISPATCH 21, PARSE_NAME_LOOKUP 3 | | step 5 (→ 0, deleted) |
| CodeShapeGuardrailTest FILE_LIMIT 3500 | | amended at step 4; deleted at step 10 |
| census walls 32 / failures 1,447 / kernel 164 | | steps 6 and 9, to zero |
| claims ledger drift test | | step 8 |
| MinimalCorpusTest PER_TEST_CEILING_MS 60,000 | | stays; the timing lines are read at every gate |

## 3. Session bootstrap (for a fresh session)

- **Repo:** `~/legend/legend-lite` (the worktree used so far: `.claude/worktrees/build-audit`, branch
  `datacube/app` tracking `origin/main`). Bazel 9. Untracked `nlq/` is not ours; leave it.
- **Read, in order:** `AGENTS.md`; `docs/UPSTREAM_BOUNDARY_PROGRAM.md` §3 D (rulings, D7);
  `docs/PLAN_AUDIT_2026_09_26.md`; this file; the audit file for the step at hand; the last
  `docs/GATES.md` entries; the study `~/legend/platform-architecture/PLATFORM_ARCHITECTURE.md`
  §11–§14 for background.
- **Pinned trees:** `OB=$(bazel info output_base)`; `$OB/external/+http_archive+legend_pure_src`
  (5.99.0), `$OB/external/+http_archive+legend_engine_src` (4.145.0). Regenerate after a pin bump
  with `bazel run //core:update_generated`.
- **Gates:** `bazel test //... //parser-equivalence:diagnostics` then `bazel test //tools/deps:all`.
  Corpus lanes `//spec:corpus_duckdb`, `//spec:corpus_h2` (two passes each: host judge, database
  judge; read every timing line they print). One test: `--test_env=JAVA_TOOL_OPTIONS=-Drcorpus.test=<fqn>`.
  Probe: `--test_env=LL_SHADOW=1`. Stacks: `LEGEND_LITE_STACKS=1`. Census: the spec tests with
  `-Dmanifest.census=core_relational` (`-Dmanifest.census.timing=1` for phase timings). Our
  resolutions dump: `-Dour.resolutions=core_relational` (`OurResolutionsTest`, writes under the
  repo's out dir). Reference dump: `tools/reference/README.md` (the shaded jar and the IDE's JDK).
- **Timing discipline:** `uptime` first; never time with another Bazel running (the other account,
  `neema`, runs one; never kill its processes).
- **Receipts:** `~/legend/platform-architecture/receipts/` (`untangle-4b/`, `reference-differential/`,
  `plan-audit-2026-09-26/`). Every gate saves its numbers there and records them in `GATES.md`.
- **Commits:** `git -c user.name=neema2 -c user.email=neema2@gmail.com commit -F <file>` with the
  trailers `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session:
  https://claude.ai/code/session_0187ucQbj9UZpTg2HSpqDtK2`; rebase on `origin/main` before push;
  never force-push; never bare `git stash`.
- **Standing rulings (verbatim spirit):** no string identity for a function, ever; no PCT or
  category checks in the compiler; no tolerant load mode; no caches before the algorithm is proven;
  probe before switch; every deferral pinned with an owner; own everything the program needs.
