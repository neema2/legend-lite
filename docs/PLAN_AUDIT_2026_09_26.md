# Audit of the real plan, the compiler design and the first-step homework (2026-09-26)

Six read-only research lines, each against primary sources, before any code: the pinned
legend-pure compiler's Java (how it binds and chooses overloads), the pinned legend-engine
compiler's Java (how engine-grammar input is resolved), our own code for every step A–G, the
lowering and SQL half, the package-cycle graph re-measured on today's tree, and the ANSI
renderer's spellings classified against the SQL standard and the major databases. Plus the 28
package disagreements in the reference differential read one by one. Every claim below names its
evidence; a claim with no file:line is judgement and says so. The six research reports are
in `docs/plan-audit-2026-09-26/` (verbatim, with the file:line evidence); the step-by-step
execution plan built on them is `EXECUTION_PLAN_2026_09_26.md`. Receipts: the study directory's
`receipts/plan-audit-2026-09-26/` (graph tools, the join output).

Paths: `M3` = pinned legend-pure `legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/
legend/pure/m3`; `CMP` = pinned legend-engine `legend-engine-core/legend-engine-core-base/
legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/
engine/language/pure/compiler/toPureGraph`; `FEP` = `M3/compiler/postprocessing/processor/
valuespecification/FunctionExpressionProcessor.java`; `FEM` = `M3/compiler/postprocessing/
functionmatch/FunctionExpressionMatcher.java`.

## 0. Verdict

The plan's direction is right and survives the audit: bind once, hold declarations, one rule for
overloads, one registry, the oracle decides. The plan as written does not survive it. Nine of its
load-bearing facts are wrong or unproven, the first step as specified in the homework note would
create a new package cycle and turn three enforced tests red, and two steps contradict standing
tenets of the repo. None of this is fatal. All of it is fixable in the documents before code, and
the corrected order in §7 is executable. The two side questions: the two-file annotation move and
a first split of core into Bazel targets should happen NOW, before the first compiler slice; the
ANSI renderer's clean-up is a LATER phase, triggered by wiring a third executing backend.

## 1. Facts the plan rests on that are wrong or unproven

| # | The plan says | What is true | Evidence |
|---|---|---|---|
| 1 | The reference builds one match per candidate, sorts, takes the smallest (homework §1.3, §2.1) | The reference LOOPS over candidates in lenient order; for each it types the lambda arguments against THAT candidate's signature, then re-ranks all candidates strictly and accepts only if this one is still best; otherwise it unbinds and tries the next | FEP:121-265 (loop), :210-215 (accept), :223-227 (retry), :387 (lenient search) |
| 2 | Type-match order: exact < distance < non-concrete < relation/function < null | Actual: simple(distance n) < NON_CONCRETE < {relation, function} < BOTTOM (argument is Nil) < NULL. A `T` parameter ranks BELOW every concrete match, `Any` included. Distance is the index in the C3 linearisation, not "generalisation steps" | `M3/navigation/generictype/match/TypeMatch.java:36-101, 130-149, 203-229, 287-327, 418`; `GenericTypeMatch.java:175-177` |
| 3 | Multiplicity: exact < non-concrete < by upper then lower distance | Confirmed, with arithmetic the plan omits: `[1..*]` does NOT accept `[*]` (lower distance −1 = no match); a `*` upper bound against a bounded one is MAX, so `[1..*]` beats `[*]` only through the lower-bound tie-break; a collection literal `[1,2,3]` has multiplicity exactly `[3]`, which is why the `[1..*]` overloads win | `M3/navigation/multiplicity/MultiplicityMatch.java:187, 275-298`; `InstanceValueProcessor.java:194-211` |
| 4 | "A type variable bound twice" is a kernel failure to fix (step D) | In the reference a second binding is MERGED by least upper bound with the parameter's variance, never an error; multiplicities merge by subsumption | `M3/compiler/postprocessing/inference/TypeInferenceContext.java:330-440, 276-281` |
| 5 | `let`, `new`, `cast` are syntax; `if`, `match`, `filter` are functions (design §3.3) | `let` is the function `letFunction`, `^C(...)` is `new`/`copy`, `cast` is `cast<T|m>(Any[m], T[1])`; only literals, `@Type`, lambdas, variables and `#{}#` are values. Property access on a collection is REWRITTEN to `map($x, v|$v.p)`; on an enumeration to `extractEnumValue`; milestoned properties get dates injected | `AntlrContextToM3CoreInstance.java:1478-1488, 1645`; `cast.pure:49`; FEP:298-331, 1108-1181 |
| 6 | Element lookup: the section's package, then imports, then core (design §3.3) | No own-package tier; core and section imports are ONE set; two hits is an error "found more than one time in the imports"; zero hits falls back to a top-level element | `M3/navigation/importstub/ImportStub.java:181-234` |
| 7 | The engine resolves a bare name against the handler registry first, then the graph; the corpus is engine input (design §1, §5) | The handler map IS the engine's whole function namespace for engine input, user functions included (registered under `package::name`); qualified `meta::` spellings are collapsed to bare; overloads are chosen by hand-ordered dispatch tests, first pass wins, no tie error. The corpus is Pure source compiled by legend-pure's runtime and never touches this path | `CMP/CompileContext.java:544-635`; `CMP/Handlers.java:3127-3140, 3292-3392`; `FunctionCompilerExtension.java:79-118`; `matchGenerator.pure:70-170`; `Test_Pure_Relational.java` |
| 8 | Upstream registers 62 return-type inferences keyed by signature id (plan §"measured") | Every handler carries a return inference as a field; 830 ids in `Handlers.java` plus 61 in extensions; 8 named inference functions; the map is keyed by bare name. Nothing counts to 62 | `CMP/Handlers.java:1439, 408-549` |
| 9 | Task #46's drift is "33 spelled lookups per call" and closes inside A1 (homework §2.2) | The receipts show per-pass time back at the program's start (DuckDB 32/37s, H2 20/56s at 52e4acf6f); the residue is JVM boot (+3s), which the receipts attribute to the 4b.1 universe and prelude. No profile of resolution exists. The receipt headers show a loaded machine | `receipts/untangle-4b/corpus-curve-{duckdb,h2}.txt`; `docs/GATES.md` 4b.1 entry |

Also wrong: the implicit-import list. Our generated core group has 32 packages; the reference's
`system::imports::coreImport` has 29 (`M3/.../platform/pure/grammar/m3.pure:175-213`). The three
extra are engine-only. A bare call that resolves only through them compiles for us and fails in
the reference: that is our bug, and the differential will show it as a package row.

## 2. Traps in our code, step by step

### #47 and A1 (the kernel's rule; candidates as declarations)

- **The homework's A1 creates a package cycle.** Putting `FunctionId` (package `platform`, which
  imports `model.Function` and `SignatureMangle`) on `protocol.spec.AppliedFunction` adds the edge
  protocol → platform; platform → model → protocol already exist. Result: a 7-package, 171-file
  cycle and three enforced tests red (`ArchitectureTest` `protocolIsTheBottomLayer`,
  `modelIsPureData`, `packageDependenciesAreAcyclic`). It also contradicts the design's own §3.2
  ("the call node carries only the spelling"). The binder must produce its OWN artifact: a
  per-compilation `Bindings` value keyed by node identity (call node → declaration ids; later
  variable → symbol), produced by the resolver and read by the typer, with `candidateFqns` deleted
  from the syntax node in the same commit. Measured by the cycle line (simulate.py on HEAD).
- **#47 alone raises "Too many matches".** Today a catalog native and its bodied twin BOTH enter
  the candidate set (`FunctionCompiler.functionsAt:62,69`) and the tie is resolved by
  `allSameShape` → first wins (`InferenceKernel:1133-1139`). Delete that before the twins are
  merged by id and every such call errors. #47 and A1 are one slice.
- **A second scorer.** `InferenceKernel.scoreNonLambda:1230` pre-ranks overloads from the
  non-lambda arguments (`Typer.selectRankedByPresentArgs:2133`). If `score` becomes the
  reference's ordering and this stays a sum, the two paths disagree. The reference's loop (§1 row
  1) replaces both. `nearestInLinearization:1187` and the Nil tie-break (:1161-1183) are also
  unlisted.
- **The declaration table keeps the bodied twin.** `DeclarationTable.of:60-65` prefers the body;
  `FunctionCompiler.compile:186` sets `isNative` from the definition's class; 13 readers of
  `isNative()` (including `Typer.requiresNormalization:1674` and `StatementInline:33`) flip for
  every native the world also declares with a body. "The twin's body is simply never chosen" holds
  only where an Intrinsic or Form row exists; the 191 overloads the catalog lacks get Body rows.
- **Hidden readers of `candidateFqns`:** ten files, including the PARSER (`OperatorParts:67,123,
  136`) and lineage (`PkInference:98`). The homework lists none.
- **Tier 1 serves the corpus today.** `BareNames.fqns` applies the engine handler surface to every
  bare call at the typer (`FunctionCompiler:42`), and nothing records which grammar produced a
  tree (`ParsedModel` has no dialect field). The corpus is Pure source (§1 row 7), so tier 1 must
  stop serving it, but which bare names resolve ONLY through tier 1 is unmeasured. The probe's
  `bare/node` column can count it per name before the switch.
- **Tolerance is the gate.** `NameResolver.resolve(parsed, wallSink)`, `Compiler.buildModule`,
  the corpus loader (`MinimalCorpus:293`), `FunctionCompiler.compileAll:117-133` (drops broken
  overloads), `Typer.candidatesOf:2536` (swallows broken import candidates). "No tolerant modes"
  is true only after step F. A1's "zero candidates is a bind error" must be a WALL with a reason,
  not an abort, until then.
- **Tests that pin the behaviour A1 deletes:** `PctFunctionSuppressionTest`, `BareNamesTest`,
  `NativeCatalogGovernanceTest:152,165`, `NameResolutionContractTest`, the `ArchitectureTest:946`
  mutable-field allowlist for `SUPPRESSED_ONCE`. They retire with the code.
- **The real hot spot candidate is unmeasured.** `ResolvedNames.referents:26-33` rebuilds
  `BareNames.catalog(name)` (handler surface + 32 concatenations + parse names + a catalog lookup
  per FQN) on EVERY `ResolvedNames.names(af, X)` check: 33 call sites in 17 files. A1's index
  replaces 33 O(1) hash lookups elsewhere. Measure before claiming.
- **The category pin.** A1 removes two `FUNCTION_CATEGORY_CHECK` sites: 13 → 11, not "10 or lower".
  The rest are `isVerdictFunction`, `isStatementOnly`, the `NormalizeRequiredFunction` stereotype
  string and `_this`.
- **Own-package tier.** The homework says the reference has none; `NameResolver:252` cites an
  earlier audit ("own package always visible bare"). The reference's code settles it (§1 row 6):
  no such tier. The differential must show which corpus calls, if any, depended on it.

### A2 (the chosen declaration through lowering)

- **Lowering already dispatches by the resolved overload**, keyed by `Function.signatureKey()`
  (`Scalars.lower:2550-2557`). Only the REGISTRATION side is by bare name: 158 `nativeKeysAt`
  sites in 14 files fanning a name to every overload's key. So A2 is smaller than "158 sites
  become rules by id" and different: it is choosing ONE identity.
- **Two identities per declaration.** `signatureKey()` (lite's, `model/Function.java:51-62`) and
  `FunctionId` (upstream's mangle), bridged by a map in `ImplementationTable.build`. 93 readers of
  `signatureKey()` in main code (12 in lowering, the inlining stacks, the typer). A2 must retire
  one in the same commit or leave two truths.
- **Residual name dispatch inside the lowerer** is the real A2 target: `Lowerer.isFamily(n,
  "get"/"equal"/"eq")` via `Pure.nativeNamed` (:3406-3410) and `NativeFn.LowererForm.of(
  callee.qualifiedName())` (:583).
- **Phase H (the store resolver) looks functions up by FQN string** at 35 sites in 9 files
  (`Callees` 7, `TemporalFrame` 6, `AssociationJoins` 6, …). The plan mentions only lowering.
- **Many-to-one is the norm** (`plus` five overloads on one op; `family(SqlFn.LESS, "lessThan")`).
  Registration by id needs a shared rule object referenced by a list of ids, the DynaFn 4th column
  inverted. Expect ~500 id rows; a new catalog overload with no rule becomes `Unimplemented`,
  which is loud and correct.
- **A2 does not depend on A1.** Keying by the callee's definition works with today's candidate
  sets. It is the safest first code slice.

### A3 (mints)

- The pin regex counts the parser's 17 mints (`SpecParser` 16, `OperatorParts` 1), so "143 → 0"
  is unreachable; and a mint spelled from a constant's `qualifiedName()` passes the pin while
  still putting a spelling on the node. The gate proves well-formedness, not binding.
- 53 mints live in the normalizer (phase E), before any model or typed declaration exists; only
  the static catalog table is available there.

### A4 (elements and members)

- "The typer holds the class declaration" as an object is what AGENTS.md invariant 5 forbids
  (`Type.ClassType(String fqn)`, `TypedClass.superClassFqns`). As an identity it is what exists:
  an element's FQN IS its identity (no overloads), so most of the 175 element-name compares are
  legitimate identity checks. Member binding is already type-directed (`Typer:548-557`). A4's
  homework must sort the 175 before anything is scheduled; the plan already says so.

### B (forms by declaration)

- Forms dispatch BEFORE any argument is typed (`Typer:560` `CoreFn.of(af.function())` →
  `applyCore:1308-1454`, 65 arms, 39 checkers), and each checker types its own arguments. "By the
  chosen declaration" is impossible in that order: the overload cannot be chosen before the lambdas
  are typed, and the lambdas need the form's rule. What is feasible: dispatch when any CANDIDATE
  id (from A1's bindings) has a `Form` row. That is exactly the reference's loop (§1 row 1)
  applied to forms: the candidate supplies the lambda's parameter types. B needs A1, not A2.
- `agg` is unowned (`GroupByChecker` matches the string; upstream declares it as a bodied
  function). B needs a declaration for it: a loaded one (F) or a catalog row.
- Candidate sets spanning two rules (`filter` → JSON or relation; `sort`, `map` collection vs
  relation) are the loop's normal case, not special cases.

### C (one evaluator)

- **It contradicts the execution tenet as written.** AGENTS.md: "the QUERY COMPILER executes no
  values"; `WORLD_MAP`: "never COMPUTES a value — the database does". `JavaEvalLedgerTest`
  registers only `LiteralFold` (bare String/Boolean literals; Integer folding explicitly refused).
  `StaticFold` (27 fold ops incl. PLUS, MINUS, INDEX_OF, JOIN_STRINGS on literals) is NOT in the
  ledger: it is already an unregistered host evaluator inside the compiler, and design §3.5
  ("string and arithmetic natives on literals… one evaluator per declaration") would grow it.
- **The principled line:** plan-time evaluation may compute only what determines the SHAPE of
  the SQL: column lists and names, column-spec sets, which `if`/`match` branch, which lambda body,
  unrolling `map` over a static collection, type tokens, literal-empty detection. Never a value
  that becomes a SQL literal the database could compute. Step C needs a ruling in the tenet
  charter and a ledger row BEFORE it starts, and its scope is "one shape evaluator", not "one
  interpreter".
- **It depends on D.** "Over typed terms" requires normalise-required bodies to type standalone;
  today they cannot (`Typer:1600-1605`, "only monomorphized"), which is why two inliners work on
  the untyped tree.
- More rewriters than four: `LiteralFold`, `NormalizeFolds`, `LiteralUnroll` (684 lines),
  `SourceSubst.inlineLets`, `LiteralMapUnroll`, `ValidateDesugar`, `resolver/LiteralFolds`,
  lowering `Fold` (1,375, fold-vs-isolate), `MatchFold`. The census in C must count them all.

### The lowering design (§3.6, §4)

- **There is no new Plan to build.** The MIR (`com.legend.sql`: ~45 `SqlExpr` variants, `SqlFn`
  ~200 ops, `SqlAgg.Fn` ~35, `IS_NULL`/`COALESCE`/`IS_DISTINCT_FROM`/`NULL_SAFE_EQUAL`) IS the
  dialect-independent plan the design describes; only `NULLIF` is missing. §3.6 reads as an
  invitation to a second IR beside 15 sealed files and 8.6k lines of dialect code. That is the
  rewrite §7 forbids. Correction: keep the MIR; rules emit it; add `NULLIF`.
- **Null semantics vs parity.** The design says `[0..1] == [0..1]` renders as `is not distinct
  from` and empty aggregates yield Pure's result. The code deliberately does the opposite because
  the ENGINE emits bare `=` and plain `SUM` ("IS NOT DISTINCT FROM appears in no golden",
  `Scalars.java:83-130`; `NullSemantics.verbatim`). Principle 6 (the oracle decides) wins; the
  design text must say so and keep today's per-use rules.
- **"Rendering is data" is half true.** `Spellings` has 77 rows; `AnsiSqlRenderer.call()` has
  68 coded arms; structural differences are MIR-to-MIR rewriter passes. **"Generated from
  dynaFnToSql" is 0% true today**: `DynaFn.java`'s dialect column is membership, never a spelling;
  upstream's `dynaFnToSql` bodies are Pure functions with arity logic, so "generated and verified"
  is a new extractor, a separate later job, not a clause.
- **Dialect capability is not a declaration column.** Capability failure is render-time today
  (`DialectCapability`, 34 sites) and shape-dependent (LATERAL on H2 works only for literal
  collections). It belongs on the dialect as `supports(SqlFn | structural node)`, checked by one
  MIR walk after lowering and before rendering. That gives "a lowering error before SQL" without
  pretending capability is per declaration.
- **The table is derived from the rule maps today** (`PlatformRegistrations:16-22` reads
  `Scalars.ruleKeys()`). §4's "the row IS the rule" inverts that ownership: it means moving 131+
  lambdas into row values. Fine, but it is E's real size.

### D, E, F, G

- **D:** the 164 kernel failures (census: `kernel=164`, top messages "T bound to Class<Any>
  vs …" 53/42/19/7, unbound T 11) are mostly the LUB cases of §1 row 4. D's homework must classify
  each against the reference's `register` rule, not against "bound twice = error". No inventory
  exists of which of the 39 checkers exist only because the kernel could not type the call.
- **E:** of the mechanisms listed, `LITE_SURFACE`, the lite internal set, the handler surface's
  declared column and the TDS legacy vocabulary are BIND or TYPING facts, not ownership rows; the
  DynaFn column is the translator's rename table keyed by engine operator. The claims ledger is
  test-side in `spec` and feeds the prelude and natives generators, which need a new source.
- **F vs "alongside":** the charter's D7(c) rules "one variable at a time: names constant, then
  world constant". The plan's "E and F runnable alongside from B" contradicts it and would make
  roster diffs unattributable.
- **G last is not workable.** `CodeShapeGuardrailTest` FILE_LIMIT 3,500 with Typer 3,489, Lowerer
  3,494, Scalars 3,476: headroom 11, 6, 24 lines. Every step A1–D edits these files. "No carving a
  helper out" plus "G last" blocks each step unless it nets negative lines. Amend: a split ALONG A
  STAGE SEAM (the typer's `applyCore` and the checkers, the lowerer by family) is allowed at the
  step that touches the file, with the seam named in the record; splitting by topic to dodge the
  guard stays forbidden.
- **G's gate needs a new rule.** `ArchitectureTest:171` checks cycles across TOP-LEVEL slices
  only, so the cycle inside `compiler.*` is invisible to it.

## 3. The differential must become positional before it is a gate

The join (`tools/reference/join.py`) keys on (enclosing function NAME, spelling) and compares
sets of resolved ids. Three consequences, found by reading the 28 package rows one by one:

- Overloads of the enclosing function merge under one key (both `processProperty` bodies in
  store routing), and a call our compiler ELIDES (we emit no `print` call at all for the
  `print(if($debug…))` lines) shows up as a "package disagreement" on `plus`. Four of the 28 are
  this artefact; the `contains` row is the same shape.
- Fifteen of the 28 (`sort` 11+3, `distinct` 1) are the real defect §1 row 2 fixes: an exact
  `Relation`/`TabularDataSet` parameter must beat a type variable, and our sum does not prefer it.
- Eight (`size` on the result of `execute`) resolve to `relation::size(Relation[1])` on our side
  where the reference resolves `collection::size(Any[*])`. `Result` is not a relation carrier
  (`PlatformTypes.RELATION_CARRIERS`), so either our typer gives `execute` a relation-shaped type
  or the sum let the exact `[1]` outweigh a type mismatch. To be read before #47 is coded.
- Version drift rows exist (`sqlQueryToString`, `getTemporalTableFilter`: 4.138.5 signatures
  with fewer parameters) and are counted as overload disagreements; they must be excluded by
  declaration presence on both sides.

Both sides carry positions (`AppliedFunction.pos`, `TypedNativeCall.pos`; the reference prints
line and column). The fix is small: `OurResolutionsTest` prints the call's line and column; the
join keys on (source, line, column); elisions (print, assert) are listed by declaration with a
reason; drift rows are excluded by declaration presence. Then "zero" means zero. Add a
compile-status differential beside it (which bodies each compiler accepts), because the element
rules of §1 row 6 show up only there.

## 4. The cycles question: do the mechanical part now, the seam part at G

Measured on HEAD (`simulate.py`, class level including same-package users; the older
package-level tool is blind to those):

| | today | + move Nullable/NonNull | + 5 leaf moves + 2 one-line cuts |
|---|---|---|---|
| build units | 8 | 28 | 33 (every package) |
| largest cycle | 26 packages / 694 files | 4 packages / 209 (compiler, element, spec, spec.typed) | none across parents |
| other cycles | — | lowering↔resolver 132 (ONE class: `SnapshotEnvelope` reads two constants of `resolver.AsorRef`) | only the two sanctioned parent/child pairs |

The two annotation files still live in `com.legend` on main. The move was executed and validated
three ways on 2026-09-22 (382 files, 4,467 tests green) and landed as a re-runnable command
(`tools/untangle/move_classes.py --group A`), not as a merge. The front end (lexer, parser,
protocol, model, values, error, spi) has ZERO references into compiler, platform, builtin,
lowering or resolver today; after the annotation move it is a legal Bazel target with no code
change.

**Now, before the first compiler slice (about a day):**
1. The annotation move (`--group A`), the NullAway flags in `core/BUILD.bazel:42-43`, the
   `ArchitectureTest` predicate, the four bare same-package users. Re-prove the null gate by
   injecting a violation: a passing compile is indistinguishable from a disabled gate.
2. The two leaf moves with no design content: `resolver.AsorRef` → lowering; `element.StoreLookups`
   → compiler.
3. Split `//core` into the targets the graph already permits: annot, values, error, spi, lexer,
   protocol, model, parser, builtin, platform, element.type, ONE `compiler_mid` for the still-cyclic
   four, then lowering, resolver, sql, sql.dialect, exec, normalizer, plan, lineage, root, server.
   Eight units become about twenty-five, and every later back-edge (A1's protocol → platform
   included) is a build error instead of a test found afterwards. Front-end tests (about 45 files)
   become their own cached lane; the 84% of tests that run end to end are unaffected.

**Not now:** moving `Typer`, `InferenceKernel`, `NameResolver`, `FunctionCompiler`, `BareNames`,
`StatementInline`. Steps A1–C rewrite or delete exactly those; moving them first is churn against
the step that owns them. **At G:** the three remaining moves and two hoists, then `compiler_mid`
splits into bind / element / typer / typed-tree targets. **Never:** the full per-depth
re-layering (284 classes); it fixes a metric, not a seam.

Risk: the other account's `datacube/dual-plane` branch touches 32 core files. The annotation move
is `--inline` and re-runnable, so the rebase recipe is "drop the move commit, re-run the command",
published with the commit. Coordinate only `AsorRef` (it sits in resolver, which that branch edits).

## 5. The ANSI renderer question: later phase, triggered by a third backend

Measured: of the 184 spellings the BASE renderer emits (77 data rows, 53 coded arms, ~54
defaults), 75 are SQL standard, 27 are common to at least four of Postgres, MySQL, SQL Server,
Snowflake and BigQuery, 22 are Postgres-family, and 60 are DuckDB-only. `Spellings.java:13-16`
already concedes "a DuckDB renderer with an ANSI name". H2 spends 26 of its 45 override points
undoing DuckDB spellings in the base. The SQLite renderer and the golden-text channel both consume
`Spellings.DUCKDB`; there is no ANSI row anywhere. Six base spellings collide silently with
another database's meaning (`len`, `regexp_matches`, `%M`, `decode`, `//`, `strftime` argument
order), which is why (d) rows must leave the base as capability throws, not defaults.

The shape is clear and mechanical: `Spellings.ANSI` = the 38 standard-or-common rows;
`Spellings.DUCKDB = ANSI + 39` (pure data, byte-identical by construction); the base keeps its
38 standard arms and spells the three temporal ones in standard form; the 12 DuckDB arms and
~10 defaults move to `DuckDb.java` (grows to ~730 lines) or become capability throws; H2 drops
its 26 undo points. About 70 move sites, 2–4 days including a render-diff harness.

Why not during the untangle: it shares no files with steps A–E (they touch lowering registration
and the table, not `sql/dialect`), so it unblocks nothing and only competes for the gate budget;
with DuckDB and H2 the only executors a "standard" base is a base nobody runs, and every
portability claim above is dialect knowledge, not a probe, which is the "unit-tested but
unreachable" trap the repo's own rule forbids (a spelling exists only once probed on a real
engine); and it is unobservable: DuckDB byte-identity is guarded by nothing except execution, so
re-adding each spelling in `DuckDb.java` makes the refactor invisible to all eleven gates and a
byte slip surfaces only as rows. Do it as its own phase after step A, when an embedded Postgres
corpus lane exists (the corpus asserts are backend-independent rows, so no new goldens are
needed, only a dialect class and fixture DDL). Then each base spelling is driven by a red lane and
probed, and H2 shrinks as a side effect. Order inside that phase: render-diff harness → data
split → coded arms → H2 shrink → Postgres lane green.

## 6. What each document must change

- **A1_HOMEWORK:** §1.3 and §2.1 replaced by the loop of §1 row 1 and the orderings of rows 2–4;
  §2.2's "on the call node" replaced by the `Bindings` artifact; §2.3's "#47 first, testable
  today" replaced by "#47 and A1 are one slice, after the differential is positional"; the
  drift claim (§2.2 last bullet) withdrawn; the own-package tier settled by the reference's code;
  the deletion list extended (`scoreNonLambda`, `nearestInLinearization`, the Nil tie-break, the
  ten `candidateFqns` readers, the five pinning tests, the `ArchitectureTest` allowlist).
- **COMPILER_DESIGN:** §1 and §5 rewritten per §1 row 7 (the engine's map is the namespace for
  engine input; the corpus is Pure source; one overload rule for Pure source, and an engine-mode
  desugaring and dispatch table only if engine-input parity becomes a gate); §3.3 element and
  syntax bullets per rows 5–6; §3.4's "twice-bound is an error" per row 4; §3.5 scoped to shape
  evaluation with the tenet ruling named; §3.6 "Plan" replaced by "the MIR, plus NULLIF" and the
  null-semantics sentence corrected to parity; §4's capability moved to the dialect.
- **REAL_PLAN:** the measured-facts paragraph corrected (62; the core-import list is located:
  m3.pure); step order per §7 below; "E and F alongside" withdrawn; the size-guard rule amended;
  #46 re-scoped to boot time under F.

## 7. The corrected order

Each step still lands only with the full chain green, the timing lines read, and its deletions in
the same commit. Preconditions are stated because they are what the audit found missing.

0. **Structure, now (1 day).** §4 items 1–3. Gate: chain green; the null gate proven live; the
   target graph acyclic by construction.
1. **The differential becomes a gate (half a day).** §3: positional join, elisions declared,
   drift excluded, compile-status differential added. Gate: the 28 and 799 re-counted with
   causes; zero is now a meaningful number.
2. **A2, first code slice (2 days).** One identity (`FunctionId`), rules registered by id lists
   with shared rule objects, `nativeKeysAt`/`nativeNamed`/`LowererForm.of(name)`/the 35 resolver
   string lookups gone, `signatureKey` retired as a dispatch key. Independent of A1. Gate: catalog
   lookups by name → 0; rosters unchanged; probe PICK rows identical.
3. **#47 + A1 as one slice (3–4 days).** The resolver produces `Bindings` (call node identity →
   declaration ids; qualified → that package; bare → section imports ∪ the 29-package core group
   ∪ root; the three engine-only packages dropped or measured); `candidateFqns` deleted; the kernel
   implements the reference's loop with the five orderings; `score`, `scoreNonLambda`,
   `mostSpecific`, `allSameShape`, native-over-module, `isPlatformOwnedFunction`, the stereotype
   check, `SUPPRESSED_ONCE` deleted; twins merged by id with the `isNative` cascade fixed at the
   table; tier 1 removed for Pure source after the per-name probe; zero candidates is a wall until
   F. Gate: differential overload 0 / package 0 / compile-status explained; rosters LOST 0; census
   not grown; category pin 13 → 11; per-pass timings at the receipts' numbers.
4. **Typer split along its stage seam (1 day).** `applyCore` and the 39 checkers out of `Typer`
   under the amended guard rule, so B and D have room.
5. **B.** Dispatch when a candidate has a Form row, using the loop; `agg` declared. Gate: form
   dispatches by name 21 → 0; probe FORM rows zero disagreement.
6. **D.** Classify the 164 against the reference's merge rule first; implement LUB merging and
   the lambda-after-candidate typing; retire checkers with the census as the gate.
7. **C, after a charter ruling.** One SHAPE evaluator over typed terms, with the ledger row,
   sharing and a budget; counts all eleven rewriters first. Needs D for the normalise-required
   bodies.
8. **E**, then **F with names held constant** (walls to zero; boot time is #46's remainder and is
   measured here), then **G's remaining moves** and the new cycle rule.
9. **Later phase, separate:** the ANSI base and an embedded Postgres lane (§5).

## 8. Homework still owed before the first compiler commit (step 3)

- Read, in the pinned tree, the twelve methods the reference audit lists (the FEP loop and
  `matchFunction`, `getValidPackages`, `getBestFunctionMatch`, `FunctionMatch`, `GenericTypeMatch`,
  the five `TypeMatch` orderings, `MultiplicityMatch`, lambda parameter inference,
  `TypeInferenceContext.register`, `InstanceValueProcessor`, `ImportStub`, `C3Linearization`).
- Explain the eight `size`-on-`Result` rows and the four `plus` elisions (§3).
- Count, with the probe, which bare corpus names resolve only through the handler surface.
- Measure `ResolvedNames.names` (33 sites) with a real profile on a quiet machine.
- Decide the identity (`FunctionId`) and list the 93 `signatureKey()` readers by fate.
- The tenet ruling for C, written into the charter, before C's homework starts.
