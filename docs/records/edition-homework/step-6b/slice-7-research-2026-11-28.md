# Slice 7: the two research passes of 2026-11-28, as they reported (for a session that cannot read the orchestrator's)

*Two read-only Opus passes over `claude/step-6b-slice-6`. Nothing was built or run by either. Paths are under
`core/src/main/java/com/legend/` unless marked. Line numbers are of that day's tree and drift: search by name.
Each pass listed what it did NOT verify; those lists are kept at the end and are the first things to check.*

## Pass 1: where the lowering reads the door for a comparison

**`Scalars.doorsRule`** (`lowering/Scalars.java`, about line 2336): `static @Nullable NamedRule doorsRule(TypedNativeCall
call, Set<Feature> features, Styled styled)`, body `styled.edition().lowersAs(call, features)`. Callers, all in
lowering: `Scalars.lower` (a non-null result goes to `NamedRules.lower` and skips `HeldKinds.refuseAcrossKinds`);
`Lowerer` four times (`==` as "same values" in `written`, membership's rows in `written`, `wholeSides`,
`wholeAnswer`). None in `planner/` or `testplan/`.

**The doors' rule:** `Scalars.threeNumberTypesAndWrittenDatePrecision(call, args)` (about line 3901) sets
`NumberFamilies.THREE_TYPES` and dispatches by the callee's id: `in` (two overloads) to `inOfOptionalRule`/`inRule`;
`<`, `<=`, `>`, `>=` to the call's own rule (`ownRule`); `==`/`eq` to `Equalities.upstreamsPartialDateTime`, else
`Equalities.equal(..., three)`; `contains`; `removeDuplicates`; `indexOf`. Mapped from the enum in
`lowering/NamedRules.java` (`THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION`), enum `lowering/NamedRule.java`.

**The table:** `edge/UpstreamTies.java` `RULES`, one map, no per-door column: `AT_COLLECTION_SORT` to
`SORT_TIES_AS_WRITTEN`; `AT_MATH_MIN/MAX`, `AT_COLLECTION_MIN/MAX/GREATEST/LEAST` to `EXTREME_KEEPS_THE_FOLDS_TIE`;
`AT_BOOLEAN_EQUAL/EQ`, `AT_COLLECTION_CONTAINS/REMOVE_DUPLICATES/IN/INDEX_OF`, `AT_BOOLEAN_LESS_THAN(_EQUAL)/
GREATER_THAN(_EQUAL)` to `THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION`. Read by `edge/PureTable.lowersAs`
(`UpstreamTies.rule(call)` and nothing else) and `edge/EngineTable.lowersAs` (after the max/min-of-two case, before
the "outside the store" stamp rows; so inside and outside a store query, on every database). Lite:
`lowering/LiteEditionTable` has no rows.

**How the table reaches the Lowerer:** the door picks a `PlanStyle` (`edge/PureText`: `UPSTREAM`, `PURE_V1`,
`ENGINE_TESTS`, `ENGINE_PCT`; or `PlanStyle.PRODUCT`); its `twoForm(Edition.class, ...)` gives `PureTable.DOOR`,
`EngineTable.DOOR` or the `withEdition` edition; the style goes to `TypedQuery.plan`/`planned(..., PlanStyle)`, then
`planner/ProductStatements`, then `PlanEnv.of`; `PlanEnv.edition()` to `editionTable()` (`Edition.table(db)`) to
`styled()` (the `Styled` record's `edition` field) to `configure` (`withStyled`); `Lowerer.withStyled` stores it,
`withFeatures` the features. Other builders of a `Lowerer`: `Compiler` (about 962: `Styled.PRODUCT`),
`edge/EnginePlans` (about 167), `planner/PlanMaker` (about 955), `lowering/SeedableLets`, `BodyPlanner`,
`ProductStatements` (about 453).

**`EditionTable`** (`lowering/EditionTable.java`): `lowersAs`, `checks(RaiseKind, call)`, default
`emptySideIsFalse` (true). `EngineTable` overrides it: `comparison.outsideStore()`. ONE call site:
`Lowerer.written`. `styled().edition()` is also read for `checks` in `Lowerer` (four places: casts, slices) and
`Scalars` (three) -- those are not comparisons.

**`NumberFamilies`** (`lowering/NumberFamilies.java`: `TWO_FAMILIES`, `THREE_TYPES`, `canEqual`): `Equalities` 9
mentions (the default at `equal`'s two-argument form, and the branches "the product's rule alone"), `Scalars` 9
(the product's registrations with `TWO`; the rule factories; three comparisons on the value; the door rule's
`THREE`), `SideEqualities` 2, `HeldKinds` 1, `TaggedLists` 1 as a constant and about ten as a passed argument,
`MixedEncoding` 1. None in `Lowerer`, `planner/`, `testplan/`.

**The type-checker side:** `compiler/spec/CrossTypeRule` (interface; `AS_WRITTEN`), implementations
`LiteCrossTypeRule.INSTANCE`, `edge/EngineComparisons.RULE` (`readsBodies` false), `edge/PureComparisons.RULE`
(`readsBodies` true). Injected by `SpecCompiler(ctx, CrossTypeRule)` to `Typer(ctx, kernel, rule)`;
`TypedQuery.comparing(rule)` is called by the edge (`EngineComparisons.asTheDatabases`/`query`,
`PureComparisons.asPures`); `Compiler.compileErrors(..., CrossTypeRule)` by `edge/ModelInput`. `ModelContext` holds
no edition. The hook: `Typer.compared` (about 291), reached from `Overloads.applyGeneric` (about 147) for every
native call minted there; bodies through `inFunctionScope`; calls a planner builds through
`SpecCompiler.comparedAsInAQuery`. It is NOT a full choke point: the rule is consulted only for two types that
cross (`EqualityClasses.crossing`); code typed UNREAD returns early (below); the derived shadow returns before it;
the resolver builds calls after typing.

**A lite-owned function, end to end** (`hasSameElements`): name constant and `LITE_SURFACE` in `builtin/Pure.java`
(about 655, 728, 744), signature `HAS_SAME_ELEMENTS__ANY_MANY__ANY_MANY` (about 2406), family
`AT_LEGEND_LITE_HAS_SAME_ELEMENTS`; generated `core/src/main/resources/com/legend/builtin/native-claims.tsv`
(`bazel run //core:update_generated`); pins `NativeCatalogGovernanceTest` (`LITE_SURFACE.size() <= 10`),
`spec/.../CatalogUpstreamDiffTest` (`NOT_UPSTREAM_MAX = 77`), `everyLiteConstantIsARegisteredNative`; lowering
registration in `Scalars` (about 2032, a refusal rule; the real answer from `planner/RootComparison` and `Lowerer`);
type-checker `EqualityClasses` (about 76, 109).

**The edition package** `com.legend.compiler.spec.edition` does not exist. `docs/records/edition-design-2026-11-25.md`
section 5: `enum Edition {LITE, PURE, ENGINE}`, `sealed interface EditionRule {governs(TypedNativeCall);
meaning(Edition, TypedNativeCall)}`, `EditionRules.of(edition).apply(call)`, "the one hook" in `Overloads`.
`edition-program-START-HERE-2026-11-25.md` (about lines 135 and 466-482): only the typer reads it; E1 plans
`ModelContext.edition()`, `SpecCompiler` reading `EditionRules.of`, `Overloads` calling `rules.apply`.

**Guards today** (`core/src/test/java/com/legend/ArchitectureTest.java`): `anEditionsTableAloneNamesARule`;
`thePlannersReadsOfTheEditionAreItsListedOnes` (`EDITION_READS`; it EXEMPTS `com.legend.lowering` and the edge);
`legendEnginesDoorTypesAQueryThroughItsSetting`. No rule restricts the lowering from reading `Styled.edition()`.
Since 2026-11-28: `ComparisonDoorReadsRatchetTest` (6, 1, 22).

## Pass 2: which comparisons a swap in the type-checker's hook alone would miss

**Comparisons made OUTSIDE the type-checker** (none carries a source position; all use lite's function, found by
name; "outside the store" false unless a later pass stamps it):
- `resolver/AssociationJoins.pkEqualityCond` (about 2093): `equal` on key columns, join conditions (called from
  `StoreResolver` about 2373 and `CorrelatedSubselects` about 433).
- `resolver/Substitution` through `eqCallee()` (about 1910): the tdsContains EXISTS equality (about 595); **a
  user's own `contains`/`in`, recognised by family (about 1086-1090), rewritten into an EXISTS with a made
  `equal(col, needle)` (about 1109)**; objectReferenceIn's key equalities (about 1829); a membership crossing
  (about 2864). `in` made at about 1810 and in `ObjectReferenceArms` (about 58).
- `resolver/TemporalFrame.cmpCall`/`dateCmpCall` (about 2799-2832): milestoning windows -- `equal` and the four
  orderings (Date overloads), in filters stamped TEMPORAL or NONE.
- `resolver/SyntheticHeads` (about 177): `equal(ordinal, k)` for at/first. `CastReRoot` (about 110): subtype key
  join. `ClassWrites` (about 217). `ClassSources` (about 1107). `ElementReferences` (about 113).
  `ChainNormalizer.identityCondition` (about 195), for `assertIs`.
- Made at the protocol level and typed LATER by the type-checker: `normalizer/RelOpTranslator` (about 701: the
  mapping DSL's EQ/NEQ become `equal`; LT..GTE become lite's `*_ANY` shims, which are not in the door's table);
  `normalizer/MappingNormalizer` (about 2496); `compiler/spec/JoinChecker` (about 268, 321);
  `compiler/spec/LambdaBodies` (about 125).
- `compiler/spec/VerdictQueries` (about 515-543) makes `equal`/`contains` but passes them through
  `SpecCompiler.comparedAsInAQuery`, so the hook sees them.

**How such made equalities are lowered on lite's door today:** `Lowerer.written` returns `NullSemantics.verbatim`
(a plain `=`) when `verbatimEquality` is set (a filter stamped `TypedFilter.Stamp.CORRELATION`; every join with
`!j.userCondition()`, which is every join the resolver built) or the `legacyNullUnsafeEquals` feature is; positive
conditions (`asCondition`) leave `=` unwrapped; a null source position turns off `Equalities.heldApart` and
`HeldKinds.refuseAcrossKinds`.

**What the door's rule does differently from lite's for two plain columns of one type:** `Scalars.lower` skips
`HeldKinds` and `FeatureRules` at the door. `Equalities.equal(..., THREE)` takes the same path as `TWO_FAMILIES`
for such columns (the branches that differ need a Nil list, a written `[]`, lists of temporal text, or
`heldApart`; `upstreamsPartialDateTime` needs a built partial date-time), both ending in `nullArms`. In
`Lowerer.written` a non-null door rule only skips `asSameValues`, `amongRows` and the whole-side planner. **The one
difference that matters is `emptySideIsFalse`:** at legend-engine's door inside a store it is false, so one optional
side in a non-positive, non-verbatim position stays a plain `=`; lite and legend-pure write `COALESCE(l = r, FALSE)`
(`Equalities.emptySidesAsPure`). So a made call kept on lite's function would change SQL only at legend-engine's
door, and only outside a positive or verbatim position. Orderings: the door uses `ownRule`; lite `HeldKinds` then
`RULES`, and `HeldKinds` is skipped for a null position.

**UNREAD code and the derived shadow:** `Typer.compared` defaults to UNREAD; QUERY is set only by
`typeQueryBodyMarked` and `comparedAsInAQuery`. Typed UNREAD: unmarked `typeQueryBody`/`typeExpression`
(`Compiler` about 917, 950; `TypedQuery` about 152; `AggregationAwareRouting`; the edge's prints), islands
(`SpecCompiler` about 423, 467), relational-language synthesized bodies (`Typer` about 168; `Overloads` about 134).
For UNREAD the hook returns the call, at most with its position stripped. A derived shadow (`Overloads` about
163-197) is a native whose first parameter is Any rewritten into the receiver class's same-name derived property:
the result is a user call, so no comparison is lost there. All of these reach the lowering today and are caught
only because the table matches by id.

**`outsideStore`:** set by `stampedOutsideStore(true)` in `UserCallInliner` (about 211, `stampAroundFrames`: test
bodies; about 358, `stampOutside`: a store-free let or last statement; asked for by `planner/ProductStatements`
about 146 through `Edition.stampsOutsideStore`/`stampsTestBodies`) and `resolver/ClassSources` (about 235:
model-to-model transforms and constraints). Kept by `withChildren`/`withInfo` and by about twenty hand rebuilds.

**Places that recognise a comparison by its family:** `Lowerer` about 406 (positive condition), 442 (same values),
457 (empty sides), 542 and 537 (whole answer and its negation), 4519 (instance-disjoint), 4717 (whole sides);
`Scalars` about 106-119 (RULES for `==`/`eq` and the orderings), 1323 (indexOf), 1401 (removeDuplicates), 1934
(contains), 2259 (in), 2400 (orderings), 3029 (eq/equal comparator, by name), 3904 (the door's rule);
`Equalities.membershipList`; `HeldKinds` about 66; `InstanceEquality` (by name); `Dedup` (by name);
`ValueCollectionOps`; `LambdaBinding`; `planner/RootComparison` about 67. In the resolver, matches per file:
`Substitution` 41, `StoreResolver` 6, `ChainNormalizer` 4, `ElementReferences` 4, `InnerDemand` 4, and ten more
files with 1 or 2. Also `compiler/spec/StaticFold` (by qualified name) and `EqualityClasses`.

**The call node:** `record TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info, @Nullable
SourceInfo pos, boolean outsideStore)`; `outsideStore` is part of equality and left out of the hash and the printed
form when false, so plan ids do not move. Related marks on other nodes: `TypedFilter.Stamp {NONE, CORRELATION,
TEMPORAL}`, `TypedJoin.userCondition`.

**Pass 2's judgement (from reading only):** a swap in the type-checker's hook alone is not enough; one pass over
the whole typed tree after the store resolver and before the lowering, keyed on the family id, reproduces exactly
what the table catches today. Its three facts: the resolver makes lite's comparisons in more than ten files and
rewrites a user's own `contains`/`in` into a made `equal`; UNREAD code and protocol-level mints are caught today
only by id; everything such a pass needs (`outsideStore`, the null position) is on the node already.

## What neither pass verified (check these first)

- Whether any test passes: nothing was run by either.
- The position (positive, verbatim, or neither) of EVERY comparison the resolver makes: the legend-engine-door
  `COALESCE` risk.
- `resolver/ChainDispatch`. Exactly where the resolver's phase ends in each planning entry. Whether
  `FeatureRules` has rows for `equal`. Whether a derived shadow's body is typed as a body.
- An `implementation-table.tsv` that `native-claims.tsv` says is replacing it (not found).
- The claim that contradicts pass 2 and has been made in earlier sessions: that the type-checker CAN see
  everything (mapping code included) once code carries its edition (`edition-design-2026-11-25.md`, and its v2, v3
  and `edition-v4-direction-2026-11-25.md`). Pass 2 read the code as it is today, where mapping bodies are typed
  UNREAD and the resolver writes its own calls; whether the edition design's phases change that (and which is the
  smaller, safer path) was not examined.
