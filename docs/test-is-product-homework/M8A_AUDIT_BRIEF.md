# M8a audit brief — the host judgment as data (2026-11-01)

The independent audit of the test-is-the-product plan's M8a (R15/R22: one audit per step and sub-step). Read-only;
report findings as blockers / should-fix / nits with file:line and a concrete Pure body each; fold nothing. The
user's ruling in force (the plan's §6 "B3 as settled", design 2, 2026-11-01): a host arm's verdict step carries its
sides and ONE NAME for the comparison with two or three small facts; the sink dispatches on the name; the two model
reads the judge made at judging time are precomputed at planning. M8a must change no outcome, no statement text, no
census and no message text: the DuckDB corpus lanes read identical to M7's (`docs/GATES.md`).

## What M8a built (the diff since M7, commit 91a49887)

1. **`driver/HostJudgment`** (new, sealed): one record per host comparison -- `TdsEquivalent(hasTimeDelta)`,
   `GridPair(name, wantEqual, sortedOrder)`, `Size(envelopeValuesRead)`, `Contains`, `Tolerance`,
   `Condition(name, wantTrue)`, `Subset(name, wantTrue)`, `Empty(wantEmpty)`, `Eq(e, a, keys, gate)`,
   `InstanceOf(typeName, acceptedTypes)`, `Is(name)`, `Decided(held, message)`, `SameElements(e, a, gridPair, keys,
   gate)`, `Equals(name, wantEqual, incidental, gridPair, e, a, keys, gate, flat)`, `Json(incidentalRoot)`,
   `Rendered(name, wantEqual, form, eForm, aForm, sortedOrder, counted)`, `Raise(failure)` -- and the fact records:
   `SideFacts.of(TypedSpec)` (type info, class fqn, kind class, Any-stamped, Nil, map carrier, type name),
   `Keys.of(eSpec, aSpec, ctx)` (each side's key tree resolved exactly where the walk resolved it -- e where the
   classes agree or e is the narrower, a where a is the narrower -- the two subtype facts, and `byClass`: for each
   key slot that declares no keyed class, the keyed classes a value in it may carry as its own: the slot's declared
   class and `ctx.subtree(declared)`, the whole `resolutionUniverse()` for a slot declared Any, transitively through
   the keyed classes found (`ownClassesOfTheSlots`); the slot's declared class is a NEW component of
   `compiler/element/EqualityKeys.Key`, `declaredClass`, which `collect` already computed and now stores -- additive,
   the 3- and 4-argument constructors kept). **Found by the first corpus run, not by the quick suites:** the first
   cut collected `byClass` from `ctx.subtree(Any)`, and the knowledge layer's subclass index holds explicit
   generalizations alone, so every class with no `extends` was missing and 19 host-pass tests (the
   `toPostgresModel` and `debugPrint` suites: nested instances in `Node[*]` slots kept `__id`/`__type`/`__canon`)
   failed; the parity test gained `keyedClassPrograms` (a keyless abstract slot holding keyed subclass values).
   **Found by the second corpus run:** resolving EVERY class of a slot's declared subtree compiles classes the body
   never touched, and the corpus model (compiled tolerantly) holds one in `RelationalOperationElement`'s subtree whose
   property type `ResultType` is unknown -- `ModelException` at planning, 10 host-pass tests (the `debugPrint` suite,
   `selfJoin`) where the old lazy read resolved only the classes on the wire. The fix: `EqualityKeys.declaresKeys(ctx,
   fqn)` reads the PARSED definitions (a property with `<<equality.Key>>` on the class or a declared ancestor; the one
   stereotype test, `isKeyStereotype`, shared with the class compiler) and only a class that declares a key is
   resolved, so no class the body never touched is compiled; a class declaring none has no keys, so nothing is lost.
   `Keys.resolve(ef, af, eVals, aVals)` (the former `AssertVerdicts.instanceKeys` over the wire's classifiers:
   `HostJudge.allWireType`), `Keys.of(fqn)` (the former `EqualityKeys.resolve(ctx, own)` in `restrictNested`),
   `ByteGate(hasInstanceKeys, keylessCtorUnderLambda)`, `FlatFacts(bareRowStampE/A, wrappedRelationStampE/A)`.
2. **`HostSides.Check(asks, HostJudgment judgment)`**: the `Judgment` closure interface is deleted; `run` calls
   `HostJudge.judge`.
3. **`HostJudge.judge(HostJudgment, List<TypedSide>)`**: the one dispatch, a switch over the sealed records with no
   default; every `xOf` takes (record, rows) and nothing else. Each `planX` computes the facts: `facts(spec)`,
   `Keys.of`, `byteGate(e, a, keys, args, letPrefix, env)` (hasInstanceKeys = the walk's static 3-arg test:
   `keys.resolve(e, a, List.of(), List.of()) != null`; the keyless-constructor scan run under the SAME static
   conditions the byte verdict asked it: not Nil, not Any, an Instance kind, not a map carrier, no shared keys),
   `OrderView.of` for `GridPair.sortedOrder`, `Json.incidentalRoot`, `Rendered.sortedOrder`;
   `AssertVerdicts.envelopeValuesRead` for `Size`; `bareRowStamp`/`wrappedRelationStamp` for `FlatFacts`;
   `typeRefName` + `ctx.subtree(type)` for `InstanceOf.acceptedTypes` (the type itself and its strict subtree; a
   null `typeName` where the type argument is no reference -- the judgment then declines the side it read, as the
   walk's `instanceOf` did). `sqlByteVerdict(e, a, gate, ef, af, hostHeld)`, `tdsRowValuesVerdict(name, wantEqual,
   flat, ef, af, incidental)`, `restrictToKeys/restrictOne/restrictNested(..., Keys known)`, `typedSide(vals,
   SideFacts)`, `structuredSide(SideFacts, vals)` (moved from AssertVerdicts) take facts, not specs or the model.
   `planRendered(..., boolean counted)`: the host plan path passes true and `renderedOf` counts
   `sqlRoute(name, "rendered-text")` where the verdict holds (the former `AssertVerdicts.renderedPlan` wrapper); the
   walk's `rendered` passes false (the walk's `adjudicate` counts it).
4. **`AssertVerdicts`**: `HostAssert.raising`/`raisingBeforeTheArm` build `Raise(failure)`; `HostAssert.judge` →
   `HostJudge.judge`; `planArm`'s statically identified `assertIs` → `isDecided(left, right)` (a `Decided`; `isVerdict`
   answers through it); `renderedPlan` → `HostJudge.planRendered(..., true)`; `instanceKeys` (both overloads) and
   `structuredSide(TypedSpec, vals)` deleted.
5. **Guards**: `JavaEvalLedgerTest` HostJudge 1066 → 1127, AssertVerdicts 1510 → 1470 (dated); `HostJudgment.java`
   registered in `DRIVER_CLASSES`; the harness sort-site regex made the accessor `sortedOrder` (not `sorted`); the
   ordered-collection guardrail made `InstanceOf.acceptedTypes` a `LinkedHashSet`. `EqualityKeys.Key` (compiler) gained
   `declaredClass`: no ledger pins it; its readers (the identity canon in `lowering`, `SqlCanonConformanceTest`'s
   3-argument constructor calls) are unchanged.

## Questions

- **A. Fact-for-read equivalence.** For EACH precomputed fact, confirm it equals the judge-time read it replaces on
  every wire, or name the body where it differs: (i) `Keys.resolve` vs the deleted 5-arg `instanceKeys` (the two
  branches and the equal case; `e`/`a` resolved only where the original would have resolved them); (ii) `Keys.of(fqn)`
  vs `EqualityKeys.resolve(ctx, own)` -- is `{declared} ∪ ctx.subtree(declared)` complete for every `__type` a value
  in that slot can carry (typing guarantees an instance of the declared class or an explicit subclass; a class whose
  superclass is a generic head, `KnowledgeLayer.directSubtypes`' native-twin filter, a class of a dependency project
  not loaded at planning -- invariant 5)? Is `resolutionUniverse()` complete for a slot declared Any (model elements
  plus the platform's FQNs; `EqualityKeys.resolve` of a non-class FQN answers null)? Does `ownClassesOfTheSlots` walk
  exactly the slots `restrictNested` consults (nested keyed classes through their keys; a found class's own
  undeclared slots; the `seen` set's two kinds of entry)? (iii)
  `InstanceOf.acceptedTypes` vs `wireType.equals(type) || ctx.isSubtype(wireType, type)`: `KnowledgeLayer.subtree`
  walks `directSubtypes` (model subclasses minus native classes and primitives, plus `Pure.directNativeSubclasses`);
  `isSubtype` walks superclass FQNs upward through `findClass`. Name any (wireType, type) pair where the two
  relations differ. Does `subtree` raise for a type name that is no class (a primitive `Integer`, a simple name from
  `TypedTypeRef.target().typeName()`)? (iv) `ByteGate.keylessCtorUnderLambda`: the scan now runs at planning under the
  static gate alone, where the original ran it only after the dynamic declines (mixed-kind, kind-gate, rider
  declines) -- confirm it has no side effect and cannot raise (`env.ctx().findClass`, `EqualityKeys.resolve`). (v)
  `Rendered.sortedOrder` computed before the string-shape test; `Json.incidentalRoot` before the parse; `FlatFacts`
  before the grid test; `Size.envelopeValuesRead` before the Tabular test -- each a static read with no effect?
- **B. Raises moved from judging to planning.** `OrderView.of`, `chaseLets`, `EqualityKeys.resolve`, `ctx.subtree`,
  `ctx.isSubtype`, `keylessCtorUnderLambda`, `typeRefName`: can any raise on a real body? If so, where does the raise
  now land -- on the program road `planHost`'s catch (`HostAssert.raising`: the family counted, the raise at the
  assert's turn, the sides NOT sent) vs before (the sides sent, then the raise at judging); on the walk
  (`HostSides.run`: planX before `fetch`) the same shift. An outcome is unchanged either way; a statement census is
  not. Name a body or rule it out.
- **C. Census and message equivalence.** `sqlRoute(name, "rendered-text")` counted exactly where the deleted wrapper
  counted it (a non-null verdict; a failure throws before it). `Decided`'s failure message byte-identical to
  `isVerdict`'s. `Raise` thrown from `judge` reaches `judgedAtItsStep`'s catches as the closure's throw did
  (`AssertFailed | DataError` reported; any other counted as raised). `sqlFellThrough` for a null answer unchanged.
  `GridPair`'s `sqlRoute(name, "grid-pair")` unchanged.
- **D. The sink's residue.** The design's claim: the sink holds no compiler object. `HostAsserts.Planned` still holds
  the `HostAssert` whose `Check.asks` carry `TypedSpec`s (needed to PLAN the sides; dead once planned). Assess: is
  that a residue to name in the plan's §9 (the asks dropped after planning, or the Planned carrying the judgment
  alone), and does anything at judging time still reach a typed tree, a let prefix, the model or an environment
  (walk `HostJudge.judge`'s reach: `AssertVerdicts.structured`, `decodeSide`, `one`, `cells`, `sideCanonOf`,
  `Equality`, `PureAsserts`, `TdsCompare`, `CanonicalDivergence`)?
- **E. Parity coverage.** `BodyRouterParityTest`'s `assertPrograms`: which records have no parity case? Candidates: a
  keyed class pair (`restrictToKeys` through `Keys`), a polymorphic key slot (`Keys.of(fqn)`), `assertInstanceOf` of
  a class with a subclass on the wire (`acceptedTypes`), `assertSameElements` over class values, the rendered pair
  under the host judge (`Rendered.counted`), the statically identified `assertIs` (`Decided`), `assertTdsEquivalent`
  with a time delta, a raise at planning (`Raise`). Give a concrete Pure body for each missing one against the test's
  model (`x::Firm`, `x::TFirm`, the helpers).
- **F. Guards.** The ledger's two re-pins and their justifications (nothing evaluated moved into Java: the record
  unpacking and the dispatch); `HostJudgment` in `DRIVER_CLASSES`; `ArchitectureTest.hostVerdictIsReachableOnlyFromTheVerdictSeam`
  with `HostJudgment` reaching `HostJudge.allWireType` and `AssertVerdicts.isAnyStamped/typeName`;
  `ErrorShapeGuardrailTest` (no new catch); `CodeShapeGuardrailTest` (method length of `judge`, `planEquals`).
- **G. Cost.** `Keys.of` runs `subtree(ANY)` and a resolve per class when a key slot is undeclared: how often in the
  corpus (keyed class pairs with a keyless or primitive slot), and is a per-assert whole-model walk acceptable, or
  should `byClass` be scoped (the slot's declared class's subtree, which `EqualityKeys.Key` does not carry)?
- **H. Records.** The plan's §9 M8a entry should name: the judgment as data (what each record carries), the asks
  residue (D), InstanceOf's decline-after-read kept for parity with the walk, the keys' precompute rule, the two
  re-pins.

## Files

`core/src/main/java/com/legend/driver/{HostJudgment,HostJudge,HostSides,AssertVerdicts,HostAsserts,BodyRouter}.java`,
`core/src/main/java/com/legend/compiler/element/EqualityKeys.java`, `core/src/main/java/com/legend/compiler/KnowledgeLayer.java`
(`subtree`, `directSubtypes`), `core/src/main/java/com/legend/compiler/ModelBuilder.java` (`directSubclasses`),
`core/src/test/java/com/legend/driver/BodyRouterParityTest.java`, `core/src/test/java/com/legend/JavaEvalLedgerTest.java`.
Context: `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 ("B3 as settled"), §7 M8, §9;
`docs/test-is-product-homework/M7_AUDIT_BRIEF.md`. The diff: `git diff` in the working tree against HEAD (91a49887).
