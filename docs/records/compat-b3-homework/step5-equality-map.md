# B3 step 5: where the one equality lands (read-only research, 2026-11-24, on c152f9b0)

Prepared while step 2b's round ran, for the measurement before the build (the user's ruling of 2026-11-24, strict
typing: docs/records/compat-stage-b-plan-2026-11-24.md, "The one equality").

## Today
- **The typer checks nothing across types.** `equal`/`eq`/`in`/`contains` are declared over `Any`; `indexOf<T>` unifies
  to a common supertype; all pass `compiler/spec/Overloads.applyGeneric` (:106) -> `checkGeneric` (:115) ->
  `CallNodes.mint` (:136). `!=` parses to `not(equal(..))`. Ordering picks the Number/Date/String/Boolean overload
  (`1 < 1.0` takes Number). Joins (`JoinChecker` :268, :321), filters, the assertEquals guards (`LambdaBodies` :125-127)
  and mapping DynaFunc conditions (`normalizer/RelOpTranslator.comparisonFn` :694) all reach `applyGeneric`;
  `VerdictQueries.java:482` builds an `equal` node directly.
- **S169/S399/S127 live in G-half and the lowering**, not the typer: `LiteralUnroll.equalityFold` :390, `neverEqual` :434,
  the contains fold :470-484, the `in` fold :596; `NormalizeFolds` :95; `StaticFold.staticEquals` :876.
- **The lowering:** `lowering/Scalars.java:115-187` (equal/eq: tagged equality, partial dates, wire casts, Variant, enum
  source values, then plain `EQUAL`/`NULL_SAFE_EQUAL` -- the database converts: `1 = 1.0` true); `InstanceEquality.
  staticallyDisjoint` :413 (the nearest existing class function: Variant, two enumerations, enumeration vs non-String,
  instance vs primitive -> false); `contains` :2236, `in` :2679, `indexOf` :1535, `removeDuplicates` :1679, ordering
  :200; `TaggedLists` (`refuseCrossingNumbers` :107); `MixedEncoding.kindOf` :397 (splits DateTime by written digits,
  against S399).

## The narrowest hook
`Overloads.applyGeneric`, right after `Application a = checkGeneric(af, env)` (:115): add
`EqualityClasses.refuseCrossing(a, af, ctx)` (new, package-private, `compiler/spec/`), keyed on `a.chosen().id()`:
pairs -- `AT_BOOLEAN_EQUAL`, `AT_BOOLEAN_EQ`, the four ordering groups, `AT_LANG_COMPARE`, two-argument max/min;
element-vs-needle -- `AT_COLLECTION_IN`, two-argument `AT_COLLECTION_CONTAINS` (first parameter `[*]`),
`INDEX_OF__T_MANY__T_1`. Not distinct/removeDuplicates (one list against itself: run time only).

`of(Type) -> Optional<String>` (empty = known only at run time): INTEGER, FLOAT, DECIMAL and every PrecisionDecimal ->
Decimal (one class: `3.0d == 3.00d` must stay legal), String, Boolean, Byte, STRICT_DATE, DATE_TIME, STRICT_TIME;
`EnumType` -> enum:fqn; Variant; a user class by subtyping (refuse only when neither is a subtype of the other).
Empty: NUMBER, DATE (the abstract Date, partial literals), LATEST_DATE, Any, Nil, TypeVar, TDSNull, type classifiers,
relation and function types. Error: `TypeInferenceException` (Phase.TYPE; a CompileKind entry when it lands), naming
both types and the fix, tagged written vs compiler-built (`af.pos() == null`) so the census separates them.

## Measure before building -- the traps
- `compileAllBodies` returns walls, not throws: count walls as well as test failures.
- legend-engine's door rewrites AFTER typing (`edge/PureV1Routes.checkMeaning` :331 -> `PureMeaningRewrite.rewrite`):
  a typer refusal would stop the door from seeing the call. The landing needs the door before typing, or a typed marker
  the door rewrites.
- No literal widening: `$floatCol == 1`, `$decCol > 100`, `4/2 == 2` are all refused -- the ruling's intent, and the
  largest count.
- Compiler-built calls (`pos == null`): mapping DynaFunc conditions (`T.ACTIVE = 1` on BIT, `T.AMT = 0` on DOUBLE),
  key-list joins, the assertEquals guards -- upstream programs, counted apart.
- Store column types are not visible at G (a Number property over BIGINT is Number there).

## Pins that move by design
`testexec/EqualityWorldsConformanceTest` :101,124 (`1 == 1.0`, `8 == 8D` true; :127 `3.0d == 3.00d` stays);
`testplan/AssertBarTest` :116-121, :185-190, :236-238, :1033-1040; `testplan/NumberKindsEdgeTest` :52-53;
`testexec/EqualityCompatEdgeTest` :44-45; `exec/TaggedListTest` :279-291 (refusals become answers); integration
`UserFunctionIntegrationTest` :1586+ (enum vs String), `TypeInferenceIntegrationTest` :1201,1208 (partial dates, only if
they get a class); `DecimalKindsTest` :142 stays.

## Ruled 2026-11-24 ("Love it"): the marker and the door's report
The hook above does not throw: it types the call as a marker node (a comparison across types). The product refuses the
marker by name before planning (Phase TYPE, a CompileKind entry); legend-engine's door rewrites it to the database's own
comparison, legend-pure's to `false`; each door lists what it loosened (a migration report). This resolves the trap
"the door rewrites after typing". See docs/records/compat-b3-progress.md.
