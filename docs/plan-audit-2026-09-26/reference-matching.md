# Research line 1: how the reference (legend-pure 5.99.0) binds names and chooses overloads

Read-only audit of the pinned tree, 2026-09-26. Verbatim findings with file:line. Feeds
`PLAN_AUDIT_2026_09_26.md` §1 rows 1–6 and the homework for steps #47+A1, B and D.

Path prefixes:
- `M3` = `$(bazel info output_base)/external/+http_archive+legend_pure_src/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3`
- `RES` = `…/legend-pure-m3-core/src/main/resources/platform/pure`
- FEP = `M3/compiler/postprocessing/processor/valuespecification/FunctionExpressionProcessor.java`
- FEM = `M3/compiler/postprocessing/functionmatch/FunctionExpressionMatcher.java`

## Findings

**1. Candidate collection for a call — CONFIRMED, with three omitted details.**
FEM:46-49 splits `functionName` at `::`; FEM:159-170 `getValidPackages`: qualified → exactly
`_Package.getByUserPath(pkg)` (absolute path, never via imports; unknown package → empty set →
"can't find a match", not "unknown package"); bare → `Imports.getImportGroupPackages(fe._importGroup())`
∪ **Root** (FEM:167-168). `M3/navigation/imports/Imports.java:52-65`: packages = coreImport's imports
∪ the section ImportGroup's imports, nulls dropped. The call's own package is NOT searched. Pool =
`Context.functionsByName` keyed by bare functionName (`M3/compiler/Context.java:51,221-233,261-265`),
filtered by `_package()` membership (FEM:156).
Traps: (a) functions declared at Root are bare candidates everywhere; (b) name equality is re-checked
in `FunctionMatch.newFunctionMatch` (`FunctionMatch.java:116`); (c) no precedence between packages —
two overloads of the same shape in two imported packages tie → "Too many matches", never "nearest
import wins".

**2. The 29 implicit imports — CONFIRMED; origin is the bootstrap import group, not the parser.**
`system::imports::coreImport` (`M3/navigation/M3Paths.java:48`) declared in `RES/grammar/m3.pure:175-213`.
List (m3.pure:181-211): meta::pure::metamodel, ::type, ::type::generics, ::relationship,
::valuespecification, ::multiplicity, ::function, ::function::property, ::extension, ::import;
meta::pure::functions::{date,string,collection,meta,constraints,lang,boolean,tools,relation,io,math,
asserts,test,multiplicity}; meta::pure::router, ::service, ::tds, ::tools, ::profiles. A section's
ImportGroup contains only its `import x::*;` lines
(`M3/serialization/grammar/m3parser/antlr/AntlrContextToM3CoreInstance.java:458-478, 3969-3980`).
Trap: our `CORE_IMPORTS` has 3 extra packages (metamodel::variant, metamodel::relation,
precisePrimitives). A bare call to a function in those compiles for us and fails in the reference.

**3. Element references (design 3.3 "section's package, then imports, then core") — REFUTED.**
`M3/navigation/importstub/ImportStub.java:181-234`: qualified → absolute (:192-200); bare → (i)
primitive names + `Package` first (`_Package.SPECIAL_TYPES`, `M3/navigation/_package/_Package.java:39`),
(ii) ALL import-group packages (core ∪ section) searched as ONE set; >1 hit → error "X has been found
more than one time in the imports" (:229-232); (iii) 0 hits → top-level (Root) element (:216-223).
No own-package tier, no ordering between section and core imports.
Trap: a tiered resolver picks silently where the reference errors, and resolves same-package bare
names the reference rejects. Visible only in a compile-status differential.

**4. Zero candidates is an error — CONFIRMED, but not always immediately.**
FEP:258-261: `throwNoMatchException` only when `!someInferenceFailed`. If every candidate's
lambda/column inference failed, the expression keeps the last tried `func`, gets the raw signature
return type (FEP:553-564) and is reprocessed later when the enclosing call's reverse matching binds
more (FEP:491-523 `updateTypeInferenceContextSoThatReturnTypeIsConcrete`, `handleParameter`
:596-616, `cleanProcess` :823-853 unbinds and marks not-processed).
Trap: a single-pass kernel reports "no match" where the reference succeeds after the parent
supplies T. Shows as roster loss, not overload disagreement.

**5. The choice is NOT "build matches once, sort, pick smallest". REFUTED as stated.**
FEP:121-265 + :267-392:
(a) first pass processes every argument (FEP:796-821); untyped lambdas / empty-typed columns are
recorded as needing inference (:855-906).
(b) `findMatchingFunctionsInTheRepository(fe, lenient=true)` (FEP:387) returns ALL lenient matches
sorted by `FunctionMatch` (FEM:63-77; equal keys grouped in a HashMap → hash order within a tie).
(c) Loop over candidates in that order (FEP:131-228): set `func`, bind type/mult params from typed
args (:153/:184), infer lambda param types from THIS candidate's signature and process lambda bodies
(`processLambda` :618-679, `TypeInference.processParamTypesOfLambdaUsedAsAFunctionExpressionParamValue`
`M3/compiler/postprocessing/inference/TypeInference.java:108-148`), compute return type (:526-565).
(d) then `getBestFunctionMatch(ALL candidates, now-typed args, lenient=false)` (FEP:210); accept iff
best == this candidate; else unbind the args (:223-227) and try the next.
"Too many matches" is raised inside (d) (FEM:140-148) for a strict tie.
Trap: the winner is "the first candidate in lenient order that is strict-best when the lambdas are
typed against it". For lambda-taking families (filter/map/sortBy/fold/if/match/eval/groupBy) a
single ranking of untyped lambdas is not the rule; the lambda's inferred FunctionType participates
in the strict ranking. Two candidates can each be strict-best under their own lambda typing; then
lenient order (hash order among ties) decides — expect a few nondeterministic rows across reference
runs.

**6. `lenient` flag semantics — CONFIRMED.**
`FunctionMatch.java:131-132`: lenient → `NullMatchBehavior.MATCH_ANYTHING` (null value type/mult, i.e.
untyped lambda params/empty column types, match anything) and value `ParameterMatchBehavior.MATCH_ANYTHING`
(an argument whose type is a type parameter matches any target). Strict → `MATCH_NOTHING` /
`MATCH_CAUTIOUSLY` (a `T`-typed argument only matches `Any` covariantly, `GenericTypeMatch.java:214-217`;
a non-concrete value multiplicity only matches `[*]`, scored (MAX,MAX), `MultiplicityMatch.java:240-246`).
Target behaviour is ALWAYS `MATCH_ANYTHING` (`FunctionMatch.java:140,149`); multiplicity value
behaviour is ALWAYS `MATCH_CAUTIOUSLY` (:149). `covariant=true` at the top; flipped for FunctionType
parameters (`TypeMatch.java:510,519`) and per type parameter's `contravariant` flag
(`GenericTypeMatch.java:262-263`).

**7. `FunctionMatch.compareTo` lexicographic: all types, then all multiplicities — CONFIRMED.**
`FunctionMatch.java:69-103` (param count :76-80, types L→R :83-90, then mults L→R :93-100); equality
by `Arrays.equals` (:65). Exact multiplicity on one parameter never outranks a better type match on
any parameter. Trap for a weighted sum: args (Integer[1], String[1]) with `f(Number[1],String[1])` vs
`f(Integer[0..1],String[1])` — reference picks the second (type 0 < 1).

**8. `GenericTypeMatch.compareTo`: raw, then type args, then mult args — CONFIRMED**
(`GenericTypeMatch.java:80-99`; list compare lexicographic then by length :101-114). Omitted by the
plan: EXACT requires `genericTypesEqual` incl. type args (:162; `M3/navigation/generictype/GenericType.java:719+`);
type args skipped when target raw is Any or value raw is Nil (:236-239); value type args homogenised
up the inheritance tree to the target raw type (:247, `GenericType.java:65-80`);
`ExtendedPrimitiveType.testTypeVariableValuesCompatible` (:302) can reject precise primitives.

**9. `TypeMatch` ordering — PARTIALLY REFUTED.** Actual (`TypeMatch.java:36-101,130-149,203-229,287-327`):
`Simple(distance n, ascending)` < `NON_CONCRETE` < {`RelationTypeMatch`, `FunctionTypeMatch`} <
`BOTTOM` (value is Nil) < `NULL`. Relation vs Function compare is mutually -1.
Distance = `indexOf(target)` in the C3 linearization of the value type (`TypeMatch.java:418`;
`M3/navigation/type/Type.java:150-152`; `M3/navigation/linearization/C3Linearization.java:170-182`),
NOT "generalisation steps" — differs under multiple inheritance. FunctionType value vs `Any` target →
`Simple(1)` (:413-416); FunctionType vs FunctionType → exact iff `functionTypesEqual`
(`M3/navigation/function/FunctionType.java:45+`) else FunctionTypeMatch (params contravariant, return
covariant :490-544); Relation vs Relation → column-aligned RelationTypeMatch, candidate needs ≥
signature's columns (:436-488).
Hierarchy (m3.pure): Integer→Number→Any (:1536-1545), Float/Decimal→Number,
StrictDate/DateTime/LatestDate→Date→Any (:1569-1600), String/Boolean/Byte/Number/StrictTime/Date→Any
(:1492-1558); an Enumeration X → Enum → Any (`AntlrContextToM3CoreInstance.java:2106`).
Trap (big): a `T` parameter is `NON_CONCRETE` (`GenericTypeMatch.java:175-177`) and ranks BELOW every
concrete match — `f(Any[1])` beats `f(T[1])` for an Integer (distance 2 < non-concrete). `[]` (Nil[0])
prefers `f(T[*])` over `f(String[*])` (NON_CONCRETE < BOTTOM).

**10. `MultiplicityMatch` ordering — CONFIRMED with the arithmetic**
(`M3/navigation/multiplicity/MultiplicityMatch.java:38-62,105-126,160-302`): EXACT (identity :187 or
both distances 0 :301) < NON_CONCRETE (target `m`) < Simple(upper, then lower) < NULL. lower =
smallLower−largeLower, negative → no match (:275-279); upper: large `*` → 0 if small `*` else
`Integer.MAX_VALUE` (:284-287); small `*` vs bounded large → no match (:288-291); else difference
(:294-298).
Trap: `[1..*]` param does NOT accept a `[*]` arg. For a literal `[1,2,3]` (mult `[3]`): `[1..*]` =
(lower 2, upper MAX) beats `[*]` = (3, MAX) only via the lower-bound tiebreak after equal MAX uppers.

**11. Property access is type-directed, after the receiver — CONFIRMED, with rewrites.**
Parser: `$p.name` → SimpleFunctionExpression with `_propertyName` and receiver as arg 0
(`AntlrContextToM3CoreInstance.java:826-835`); `$p.q(x)` → `_qualifiedPropertyName` (:836-850).
FEP.matchFunction :289-345: receiver must be concrete else "The type 'T' can't be inferred yet"
(:1059-1067); Enumeration receiver → rewritten to `extractEnumValue(enum,'name')` (:298-302,
:1096-1106); RelationType to-one receiver → column (:303-310); NOT to-one receiver (`isToOne(m,
relaxed=true)`) → **automap**: rewritten to `map($src, x|$x.name)` and re-matched (:311-320,
:1108-1181); class receiver → `class_findPropertyUsingGeneralization`, then single-arg qualified
property (:954-1026); milestoned property with missing dates → generated qualified property with
dates propagated from context (:328-331, `M3/compiler/postprocessing/processor/milestoning/
MilestoningDatesPropagationFunctions.java:147-157`). Qualified-property overloads go through
`getFunctionMatches(lenient=true)` with a synthetic `this` (:1069-1094).
Trap: the reference's typed tree contains `map` and `extractEnumValue` calls the source never
spelled; our `Member` node must produce identical calls or the census diverges by construction.

**12. Functions vs syntax — REFUTED for let/new/cast.**
`let` → FunctionExpression "letFunction" (`AntlrContextToM3CoreInstance.java:1478-1488`), matched;
variable registered only after the match (FEP:246-256). `^Class(...)`/`^$x(...)` → "new"/"copy"
(:1645; FEP:236-244; `RES/grammar/functions/lang/creation/new.pure:29`, `copy.pure:33`). `cast` is
`cast<T|m>(Any[m], T[1]):T[m]` (`RES/essential/lang/cast/cast.pure:49`); `@Type` is an InstanceValue
of the type (`M3CoreParser.g4:312-321`). `if` (`RES/essential/lang/flow/if.pure:65`, and 3-arg),
`match` (`match.pure:73`), `eval` (`eval.pure:45-80`) are ordinary functions. `~col` →
colSpec/funcColSpec/aggColSpec FunctionExpressions (:1016-1059) with post-match magic (FEP:394-489).
`#{}#` → inline DSL InstanceValue. Values only: literals, `@Type`, lambdas, variables, DSL.

**13. InstanceValue typing.** `M3/compiler/postprocessing/processor/valuespecification/
InstanceValueProcessor.java:120-214`: single value → its own type/mult; a Class literal with type
params → `Class<X<Any|Nil…>>` by variance, mults `[*]` (:157-181); collection `[a,b]` → best common
covariant non-function type (:194-204) and multiplicity EXACTLY `[n]` (:206-211); each element gets
its own inference state (:64-66; `TypeInference.java:150-198` LUBs T across elements). `[]` is `Nil[0]`.

**14. Implicit promotions — CONFIRMED absent.** No `[1]→[*]` boxing, no numeric promotion; a `[1]`
arg vs `[*]` param is an ordinary (lower 1, upper MAX) match; Integer→Number is subtype distance 1.

**15. Inference binding rules — "bound twice is a failure" is not the rule.**
`M3/compiler/postprocessing/inference/TypeInferenceContext.java:330-440` `register`: first binding
wins; a second concrete binding is MERGED by `findBestCommonGenericType` with the parameter's
variance (:392-420) — LUB, not error; RelationTypes merged/concatenated or widened to Any (:365-378);
mults merged by `minSubsumingMultiplicity` (`registerMul` :276-281). Lambda params:
`TypeInference.java:108-148` — template param types made concrete from the context; any
still-unresolved type parameter → "inference failed" (retry later, see 4). Lambda return: FEP:632-674
registers the lambda's concrete return type against the template's return covariantly;
`handleTypeArgumentTypeInference` (:681-701) does NOT recurse into Relation/Function templates (TODO
at :700). Enclosing function's type params are "top" (`TypeInferenceContext.java:127`); a return type
left as an enclosing `T` is allowed (FEP:189,:536), anything else non-concrete → "The system is not
capable of inferring the return type" (:541); unresolved mult param → error at `TypeInference.java:100-103`.

**16. `TypeInferenceObserver` — debug only.** `PrintTypeInferenceObserver` prints the exact
registration trace: use it to diff against ours per body.

**17. Visibility and post-validation are outside matching.** `Visibility.isVisibleInSource` only in
the no-match error path (FEP:1247) and `VisibilityValidation.validateFunctionExpression`
(`M3/compiler/validation/VisibilityValidation.java:282-290`). `FunctionExpressionValidator.java:120-190`
re-checks type-argument compatibility after inference and can still throw "typeArgument mismatch".

**18. Multiplicity parameter `m` in a target — CONFIRMED:** NON_CONCRETE, below exact, above every
inexact simple match (`MultiplicityMatch.java:53-54,118-121`).

**19. Lambda position does not discriminate generic overloads — PLAUSIBLE.** FunctionTypeMatch
params/return against `T`/`V` are NON_CONCRETE; map/filter overloads differing only there tie; the
other parameters decide.

**20. Numeric/date examples — CONFIRMED.** `average(Integer[*])` 0 vs `average(Number[*])` 1;
`between(DateTime)` 0 vs `between(Date)` 1 for a DateTime; a `Date` arg does not match
`between(DateTime…)` at all.

## What to read before writing code (in this order)
1. FEP `process` :121-265 (candidate loop, accept :210-215, retry :223-227)
2. FEP `matchFunction` :267-392 (property/qualified/automap/enum rewrites, lenient search :387)
3. FEM `getValidPackages`/`getFunctionsWithMatchingName` :153-170 + `Imports.getImportGroupPackages` + m3.pure:175-213
4. FEM `getFunctionMatches` :63-77 and `getBestFunctionMatch` :90-151
5. `FunctionMatch.newFunctionMatch`/`compareTo` (FunctionMatch.java:69-157)
6. `GenericTypeMatch.newGenericTypeMatch`/`compareTo`/`compareMatchLists` (:80-308)
7. `TypeMatch.newTypeMatch` + the five `compareTo`s (:36-434)
8. `MultiplicityMatch.newMultiplicityMatch` + `compareTo`s (:25-302)
9. `TypeInference.processParamTypesOfLambdaUsedAsAFunctionExpressionParamValue` (:108-148) + FEP `processLambda` :618-679
10. `TypeInferenceContext.register`/`registerMul` (:261-440)
11. `InstanceValueProcessor.updateInstanceValue`/`updateCompositeInstanceValue` (:120-214)
12. `ImportStub.resolvePackageableElement` (:181-234) and `C3Linearization.getGeneralizationLinearization` (:170+)

## Top 5 risks for #47 (the rule) and D (the kernel)
1. Single ranking vs per-candidate lambda typing + strict re-rank loop (5, 4). Every lambda-taking
   family. Shows as overload disagreements on filter/map/if/match/fold/sortBy and roster losses.
2. TypeMatch ordering subtleties (9): `T` below every concrete match incl. `Any`; Nil (BOTTOM) below
   Relation/Function; C3 index as distance.
3. Multiplicity arithmetic + collection literal `[n]` (10, 13): `[1..*]` rejects `[*]`; MAX-upper tie
   then lower; literals exact `[n]`.
4. Syntax/function split and rewrites (11, 12): let/new/cast as functions; `$xs.p` → `map`; enum
   `.X` → `extractEnumValue`; milestoning date injection.
5. Element/candidate scope rules (1, 2, 3): no own-package tier for elements either, ambiguity is an
   error, Root fallback, 3 extra core imports on our side. Visible only in a compile-status
   differential — add it before A1 lands.

## Corrections from the second reading (2026-09-26, step 3 homework)

The twelve methods in the reading list above were read again in that order, method by method, and
rendered as pseudo-code with line citations in `kernel-reading-2026-09-26.md` (§A). Its §B checks
every numbered finding here against the code. Findings 1, 2, 3, 6, 7, 10, 12, 14, 17, 18, 20 hold as
written. The corrections, each of which changes what the step 3 kernel must do:

1. **Finding 5 omits the UNCONDITIONAL-ACCEPT path** (FEP:200-203). When the match came from a
   pre-resolved `func`, a simple property, a relation column or a QUALIFIED PROPERTY, the first
   candidate (lenient order) is accepted with no strict re-rank and no "Too many matches". Finding 11
   is incomplete: qualified-property overloads never tie-error; source order decides.
2. **Finding 4's "if every candidate's inference failed" is "if ANY candidate failed"** (FEP:204-207,
   :258): a later candidate that inferred fine but was not strict-best is silently kept as `func`,
   with its inferred return type, its arguments unbound by the retry cleanup. Silent survival, not
   an error, whenever any candidate failed lambda/column inference.
3. **Untyped-lambda typing THROWS** ("Can't infer the parameters' types for the lambda. Please
   specify it in the signature.", TI:114-117, :125-128) when the candidate's parameter at that
   position is not a concrete `Function<…>` (a `T[1]` or `Any[1]` parameter). Nothing catches it:
   it is a compile error raised by the first candidate reached in lenient order — so the lenient
   order must be reproduced exactly, or the error surfaces on different calls than the reference's.
4. **The `&&` short-circuit** (FEP:629, :170): after one lambda fails under a candidate, no later
   lambda is typed for that candidate.
5. **Finding 15 in merge mode**: a concrete value arriving over a NON-concrete existing binding in
   the same context is DROPPED, not merged (TIC:467-480, the FEP:591 path). Finding 5(b)'s tie
   mechanism is the name index's set iteration order (FEM:73, :156), not the HashMap of matches.
6. **Automap trigger** is `!isToOne(m, strict=false)` = NOT (concrete AND upper == 1)
   (Multiplicity.java:78-83): `[0..1]` does not automap; a multiplicity-PARAMETER receiver does.
7. **Nil is checked before the FunctionType branch** in `TypeMatch` (TM:394 before :399): a Nil
   value against a `Function<…>` target is BOTTOM, a match. GTM:236 flips the Any/Nil argument skip
   under contravariance (inside FunctionType parameter positions).
8. Not previously written: the retry cleanup runs only with more than one candidate (FEP:223);
   "The type parameter T was not resolved" is thrown only in a root context (TI:85-90) while an
   unresolved multiplicity parameter always throws (TI:100-103); `let` registers its variable in
   the PARENT variable context (FEP:250); unknown import paths vanish silently
   (Imports.java:64); `getFunctionMatches` wraps any matcher exception as "Error finding match
   for function '<name>'" (FEM:78-87).

**What the plan does with them** (recorded in `EXECUTION_PLAN_2026_09_26.md` step 3): the kernel
reproduces 1, 3, 4, 5, 6, 7 exactly (they decide which candidate is tried and which error the
reference raises). For 2 — silent survival — the kernel WALLS the call with the reference's reason
instead of keeping a wrong binding (AGENTS.md invariant 4: no fallbacks); the probe counts how many
corpus calls take that path before the switch, and each is explained in the step's record. §C of
the reading lists thirty-five implementer traps; the kernel's tests name the ones they pin.
