# Audit of the Phase 3 fix delta (uncommitted, on bc0de1f70)

Auditor: independent, read-only, 2026-10-07. Scope: `git diff` in `runs/build-rebuild` (17 files). No Bazel run (per
the brief). Checked against legend-pure's `GenericTypeMatch.java` / `TypeMatch.java` in the pinned checkout.

## Verdict

**Ready after fixes.** No blockers. B1's fix is m3's shape for the carrier-versus-carrier case, and the three tests pin
it. The fixes needed are about documents: one `reasons.tsv` row claims more than the evidence shows, the ledger cites
a research file that is not committed anywhere, and a few ledger anchors would not go red when their item closes.
There are also two low-reach ranking edges for the code.

Counts: **0 blockers, 6 should-fix (2 of them low), 5 nits.**

## Blockers

None.

## Should-fix

### S1. `reasons.tsv` `OVERLOAD *` says every remaining OVERLOAD row is argument typing. Two classes do not fit.
- Where: `spec/src/test/resources/reference-lane/reasons.tsv:7`.
- Evidence (committed golden, disagreement classes):
  - `OVERLOAD range`: reference `range_Integer_1__Integer_MANY_` (1 argument), ours
    `range_Integer_1__Integer_1__Integer_MANY_` (2 arguments). The arity differs, so this is not "the same ranking
    picks another version because an argument is typed differently". 1 call.
  - `OVERLOAD propertyMappingsByPropertyName` (both spellings, 5 calls): reference picks the `EmbeddedSetImplementation`
    or `OtherwiseEmbeddedSetImplementation` version, ours the `InstanceSetImplementation` one. The boot layer declares
    its own `propertyMappingsByPropertyName(InstanceSetImplementation)` (`SystemMetamodel.java:1091`). PARK-12 names
    this function as one of the boot-layer versions. Also, the row's direction ("a supertype where we type the
    subclass") is the reverse here: legend-pure's pick is the subclass.
- The landing doc's "the 58 left are argument typing" is the source. The total is right (58), but the cause is
  established for only most of the rows.
- Fix: say "mostly argument typing (…); `range` (a different arity, 1 call) and `propertyMappingsByPropertyName` (the
  boot layer's versions, PARK-12, 5 calls) have other causes". Or check those two first and name what you find.

### S2. The ledger cites a research file that is not committed.
- Where: `docs/PARKED_WORK_LEDGER.md`, PARK-5 and PARK-6 "Research":
  `docs/build-inventory/program/DEBTS_RESOLVE_AND_TYPE_ONCE.md` (plan branch).
- Evidence: in `runs/bazel-plan` (branch `docs/bazel-first-class-plan`), `git status` shows
  `?? docs/build-inventory/program/`. `git ls-files` lists nothing there, and `origin/docs/bazel-first-class-plan` does
  not have the file.
- Fix: commit and push `DEBTS_RESOLVE_AND_TYPE_ONCE.md` (and `START_HERE.md` / `PHASE_3_LANDING.md` if those are
  cited) to the plan branch before this lands.

### S3. Some anchors survive their item closing, so they do not hold "only while parked".
- PARK-12 (`ParkedWorkLedgerTest.java`, anchor `boolean shadows(` in `SystemMetamodel.java`). `shadows` is already the
  by-id hide. The acceptance ("twins merged by function id, each of the 12 decided, the 7 files loading") can be met
  with `shadows` still in place, so the anchor stays green after the fix. It also never goes red while the work is
  being done.
  - Fix: anchor something that disappears when the 12 are decided. One option is a pinned count or list of the
    upstream versions at boot-layer names that still run their body, checked by a test. Another is to say in the row
    that whoever closes it must delete the row by hand.
- PARK-8 (`nativeWinners`) and PARK-14 (`af.propertyCall() || functionCandidates(af)`). Their acceptance allows "kept
  and recorded in `SEMANTICS_REGISTER.md`", and then the anchor stays green. That is fine if the row says closing it
  means deleting the row. One sentence in each row.
- PARK-10 anchors only the type-operation part (`SchemaAlgebra -> NULL`). Porting `RelationTypeMatch` or the
  type-argument mapping would not turn it red. PARK-6 anchors 3 of the 7 places it lists. Each row should say what
  its anchor covers, or add a second anchor (for example `Type.RelationType ignored -> FunctionMatch.TypeFit.of(` for
  PARK-10).

### S4. The ledger's own rule 2 (why, cost) is not met by every row.
- PARK-13 has no "Cost while parked". PARK-8, 9, 10, 13 and 14 have no row-level "Why". The section header gives the
  program-level reason (fix after the rebuild lands), which arguably covers why each was parked.
- Fix: add a one-line cost to PARK-13 (for example "a stderr trace any user can switch on, outside the diagnostics
  channel; `ObservabilityGuardrailTest` already counts it"). Optionally say in the header that its "why" applies to
  every row.

### S5 (low). `contravariantTypeFit` now ranks an enum lambda parameter that m3 accepts as a platform-rule fit.
- Where: `InferenceKernel.java:1481-1482`.
- It calls `linearizer().distance(f, a)` on raw nominal FQNs. Unlike `nominalTypeFit` (`:1413-1416`), it has no
  Enum or PrecisionDecimal case. Take a formal `{MyEnum[1]->…}` against a value `{Enum[1]->…}`. The linearizer has no
  class for `MyEnum`, so its linearization is `[MyEnum, Any]` and the distance is -1. m3 gives distance 1. The old
  code's "-1 → 1" was right here by accident; the new code gives `PLATFORM_RULE_DISTANCE`. Primitive parameters
  (`Integer` against `Number`) are affected the same way if the linearizer does not know primitive generalizations.
  I did not confirm that either way.
- Reach: overloads that differ only in a lambda parameter's enum or primitive type. None on the lanes (they are
  byte-identical).
- Fix: compute the contravariant distance with the same rules `nominalTypeFit` uses (swap the arguments, or add the
  enum and precise-decimal cases).

### S6 (low). For a bare function-type value, the new rule ranks in two ways that neither main nor m3 would.
- Where: `InferenceKernel.java:1322-1330` and `carrierFit` (`:1375-1383`), with `nominalTypeFit`'s structural branch
  (`:1397-1405`).
- (a) `Function<{->Integer[*]}>` against `Function<Any>`. `carrierFit` returns
  `simple(PLATFORM_RULE_DISTANCE).withArguments([EXACT])`. `nominalTypeFit` returns `simple(PLATFORM_RULE_DISTANCE)`
  with no arguments. `compareLists` puts the shorter list first, so `Function<Any>` wins. Before this delta the
  specific carrier won (EXACT).
- (b) A carrier formal (`SIMPLE`, platform-rule distance) now beats a bare function-type formal (`FUNCTION` kind) for
  a bare value. m3 matches the bare formal and rejects the carrier. Before this delta the two tied.
- m3 rejects every carrier pairing here, so this is a platform-acceptance case only. It can arise when the kernel
  types a value as a bare `FunctionType`. Unlikely in practice; no lane changed.
- Fix: either give `nominalTypeFit`'s structural platform-rule fit the same argument fits, or rank platform-rule fits
  without arguments. Or add one sentence to PARK-9 recording that the ranking among platform-rule fits is not m3's.

## Nits (one-line fixes)
- **N1.** `InferenceKernelTest.overload_aFunctionCarrierBeatsATypeParameterAndAny`: for the bare `ints` value, m3
  rejects the carrier and would pick `T`. This case pins the platform-rule rank (PARK-9), not m3. Say so in the
  comment.
- **N2.** `FunctionMatch.java` Linearizer javadoc: "as it fails the typing". The first audit found that acceptance
  (`isSubtype`) tolerated a class that failed to compile. Drop the clause or verify it.
- **N3.** `reasons.tsv`: `PACKAGE *` now matches no class. Every present PACKAGE class (`size`, `divide`, `plus`) has
  its own row. Remove it, as was done for `DRIFT *`, or keep it on purpose.
- **N4.** `docs/GATES.md` audit paragraph: "a pairing legend-pure rejects (a bare function type on one side)" leaves
  out the other case the code ranks there: a carrier the value's class does not extend.
- **N5.** The first audit's suggested fix used distance 0 for a bare function type. The code uses the platform-rule
  distance instead. That is a better fit with the convention and is documented in the javadoc and PARK-9. Worth one
  word in `PHASE_3_LANDING.md` B1 ("not the 0 the audit suggested").

## What I verified clean

- **B1 against m3.** `GenericTypeMatch.newGenericTypeMatch`: `genericTypesEqual` gives EXACT; otherwise a raw
  `TypeMatch` on the carrier classes by C3 order (`getGeneralizationResolutionOrder().indexOf`), then the type
  argument after inheritance mapping, compared by `FunctionTypeMatch`. Carriers have no multiplicity arguments.
  `typeFit`'s new branch does the same:
  - `formal.equals(actual)` gives EXACT;
  - `carrierFit` gives the C3 distance of the formal's raw carrier in the value's linearization;
  - `typeFit(ff, fa)` is the single type argument (EXACT when the function types are equal, else `functionTypeFit`,
    Kind FUNCTION, which matches m3's FunctionTypeMatch order after NON_CONCRETE);
  - the multiplicity list is empty.

  `deepUnwrapFunction` gives the instantiated `Function` supertype for a Property value, which is m3's inheritance
  mapping. Lambdas are typed `LambdaFunction<ft>` (`TypedLambda.java:49`), so the common case is carrier against
  carrier.
- **The pairings m3 rejects.** A bare function type on either side, or a carrier the value's class does not extend
  (`distance < 0`), go to `PLATFORM_RULE_DISTANCE` as a SIMPLE fit. That is after every real generalization and before
  NON_CONCRETE, the same convention as the TDS fit and `nominalTypeFit`'s structural branch.
- **What the new branch does not touch.** An `Any` formal (`formalKeepsCarrier`), a `Function<T>`-style formal (its
  argument is not a FunctionType), a bare formal with a bare value, and lambda literals in the lenient pass all take
  the old path.
- **The tests pin the right behavior.**
  - The nearer-carrier test declares `Function` first, so a tie (the old code's EXACT/EXACT, which goes to the
    first-declared duplicate) would fail it.
  - The carrier-beats-T-and-Any test covers a lambda, a FunctionDefinition carrier and a bare value.
  - `CompileFunctionTest` is the end-to-end case from the first audit (both the to-one and the `map` variants).
- **Ledger anchors.** I grepped every anchor over `core/src/main/java`. Each matches exactly the listed files:
  - PARK-5 `ResolvedNames.java:29`;
  - PARK-6 `Typer.java:388/506/1096`, `CallShapes.java:75`, `TdsDesugars.java:137` (the pattern matches `grecv` by
    substring, as the row says);
  - PARK-7, 8, 9 and 10 in `InferenceKernel.java` only;
  - PARK-11 `TdsLegacy.java:69`;
  - PARK-12 `SystemMetamodel.java:1477`;
  - PARK-13 `Overloads.java:413`;
  - PARK-14 `Typer.java:504`.

  The `Map.of` → `Map.ofEntries` change is mechanical, and PARK-1 to PARK-4 are unchanged.
- **Ledger facts checked.**
  - The four tie-breaks in `resolveOverload` (duplicate signature, native over module, most specific, Nil narrowing).
  - `GroupByChecker.isAgg` and `TdsLegacy.matches` exist as described.
  - `ObservabilityGuardrailTest` lists `LEGEND_LITE_RAW_EXPAND_TRACE`.
  - `SEMANTICS_REGISTER.md` exists.
  - 2,147 → 2,533 is +18%.
- **IdentityGuardrailTest.** No pin value changed. The restored history comments match `293318dda` word for word
  (NAME_CUTTING, CATALOG_LOOKUP_BY_NAME, FAMILY_LOOKUP_BY_NAME, FUNCTION_CATEGORY_CHECK, LOCAL_NAME_COMPARE). New notes
  come first.
- **GATES numbers, from the goldens.**
  - Failed-bodies sets compared against `git show 293318dda:…/core_relational.txt`: exactly **51 newly typed, 12 newly
    failing**. The 12 are the ones named (5 `getSignFunctions`, `executionPlan`, `router::execute`,
    `loadCsvToDbTable`, `extractMappingsFromFunctionDefinition`, `shouldStopFunctions`, `varToString`,
    `flattenConcatenate`). `testViewChainsWithBusinessDate` fails in neither golden.
  - `native-membership.tsv` has +27 lines since `293318dda`.
  - `ImplementationTableTest.UNROWED_MAX = 109`.
  - OVERLOAD counts sum to 58.
- **reasons.tsv coverage.** Every class present in the committed golden has a row (OVERLOAD, PACKAGE, ABSENT, EXTRA,
  PROPERTY_AS_CALL). Every specific row matches a present class. The removed rows (`isEmpty`, `min`, `DRIFT *`,
  `PROPERTY_AS_CALL connectionByElement`) are absent from the golden; DRIFT is 0. The `routingStrategy.toString()`
  location matches `router_routing.pure:718` in an engine source copy.
- **The execution plan.** The W1.1b note is removed and the W2.1 note is kept (`EXECUTION_PLAN_2026_09_26.md:660`).
  The reason (the 2026-10-07 re-scope) is recorded in PHASE_3_LANDING.
- **Wording fixes.**
  - `UserCallInliner` reports "no row for" only for `Reason.NO_ROW`; walls keep "walled body". `SpecCompiler`'s
    "walled body" covers FQN walls only (`WalledBodies`), so it is right.
  - "implements" → "declares" is consistent across `ImplementationTable` (3 places), `Implementation` and
    `FunctionCompiler`.
  - `StatementInline`'s comment no longer names the impossible case.
  - The `Overloads.checkGenericTyped` correction holds: `Typer` holds only final collaborators, and I found no per-synth
    registration.
- **Hacks and shortcuts.**
  - No guard narrowed and no pin or ratchet moved up.
  - No new catch that returns a value, and no silent fallback (N3 of the first audit was resolved by documenting a
    loud failure, not by catching).
  - Nothing special-cases a test or a name.
  - The diff adds no debug output, no `/Users` or `/private` paths, and no binaries.

## Not verified
- Whether the linearizer knows primitive generalizations (S5's primitive half).
- The PARK-5, PARK-7 and PARK-12 figures that come from the census or the profile (32 packages, about 5 lookups per
  call, ~200 built calls, 12 reference-lane `in` calls, 29 names, 7 files). They are consistent with the landing doc;
  I did not re-measure them.
