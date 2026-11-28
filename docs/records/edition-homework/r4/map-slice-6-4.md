# Slice 6.4: a map of the code, the answers and the guards (read-only research, 2026-11-26)

Tree read: `/home/user/ll-62`, branch `claude/step-6-2`, HEAD `818c3a878`. Every path is relative to that tree and
every line number is the CURRENT one (the design `docs/records/edition-homework/b3-step5bc-design.md` cites stale
lines). `M/` = `core/src/main/java/com/legend/`, `T/` = `core/src/test/java/com/legend/`.

**State notes that change the plan**

- **Slice 6.3 has not landed.** `git log` shows 6.1, then 6.2's commits (several marked "NOT YET RUN"). 6.3 is "generics
  and classes" (research-step6-equality.md §6.5: `EqualityClasses.of(GenericType)` reads Pair/List/Map arguments; the
  run-time class compared first). `M/compiler/spec/EqualityClasses.java:123` still makes every `GenericType` a run-time
  type. 6.4's instance and Pair work assumes 6.3's "run-time class first". So either 6.3 lands first, or 6.4 does that
  part itself.
- **The first "one equality" slice has landed** (`4d44bf54`, recorded in the plan's §6a). `assertSize` is planned as
  `size(c) == n` through `VerdictSql.typedOverValues` (`M/lowering/VerdictSql.java:1067-1091`) and `SidePlanner.over`.
  The plan (lines 834-837) moves the *emptiness* family to "the slice that makes the product's list operations whole on
  every database (6.4)". So 6.4 also owes `isEmpty`/`size` of a computed list on H2.
- **The carrier ratchet appears to be over its pins at HEAD.** `T/CarrierPurityRatchetTest.java:116-235` pins
  `SqlFn\.LIST_` 172, `SqlFn\.UNNEST` 13, `new SqlExpr\.ArrayLit\(` 42 and `new SqlExpr\.OrderedListAgg\(` 3. A raw
  count over `lowering/`, `resolver/` and `plan/` (the test counts the same way, comments included) gives 173, 14, 42
  and 4. The extra sites are 6.2's: `TaggedLists.java:342` (the `UNNEST` read in `members`), the `LIST_REVERSE` read at
  `:326`, and `:301` (the windowed sort's `OrderedListAgg`). 6.2's commits are "NOT YET RUN", so check this with the
  test itself before 6.4 starts. 6.4's deletions (InstanceEquality's 5 `LIST_` sites) could absorb it, but the pins
  still move only with a dated comment.

---

## A. The code sites

### A1. The `==`/`equal`/`eq` rule: `M/lowering/Scalars.java:114-193`

It is one lambda registered for `Pure.AT_BOOLEAN_EQUAL` and `Pure.AT_BOOLEAN_EQ`:

```java
for (com.legend.model.FunctionId f : com.legend.model.FunctionId.all(Pure.AT_BOOLEAN_EQUAL, Pure.AT_BOOLEAN_EQ)) {
    RULES.put(f, (n, args) -> {
```

It sits inside the `static {}` block at `:103`, so `CodeShapeGuardrailTest` does not measure it (see E2). Earlier
dispatch matters: the Lowerer claims two shapes before this rule runs (A8). The rule's arms, in order:

| # | Lines | Condition | Answer | What it handles |
|---|---|---|---|---|
| 1 | 128-145 | a side is a LITERAL empty collection (`TypedCollection` with no elements) | both literal: `BoolLit(true)`; the other side tagged: `size(tl) = 0`; otherwise `IS_NULL(other)` | `[] == []`, `x == []`. **Bug:** a computed empty list (`[1,2]->filter(...)`) is `[]`, not NULL, so `IS_NULL` answers false (DuckDB and SQLite) and errors on H2 (measured, 6.0 §4e). `Scalars.emptiness` (`:2909-2916`) already reads a list's length. |
| 2 | 146-152 | a side is a `Tagged` value or a tagged list | `taggedEquality` (A2) | mixed values and lists (track L) |
| 3 | 153-173 | a side is a partial-date literal (`%2014`, `%2014-02`) | same precision: `EQUAL`; other side a full date: `false`; any other non-Any: `false`; Any falls through | Year and YearMonth equality |
| 4 | 174-178 | always | `CastPolicy.comparisonWireOperand` (`M/lowering/CastPolicy.java:273-289`) unwraps a wire cast when the other side is a literal of the cast's source type | mapped casts against literals |
| 5 | 181 | always | `VariantShapes.alignLiteralToJson` (`M/lowering/VariantShapes.java:31-41`) wraps a literal in `TO_VARIANT` when the other side is JSON | Any/JSON carrier against a literal |
| 6 | 182-187 | a String literal against an enumeration decode `CASE`, or two decodes | `EnumSourceValues.decodeInvert` (`M/lowering/EnumSourceValues.java:28-`) compares the source values | mapped enumerations |
| 7 | 190 | everything else | `NullSemantics.equalNullArms(n, cargs)` (A3) | the database's `=` or `IS NOT DISTINCT FROM` |

Note the comment at `:109-113`: "The residual both-NULL divergence (SQL NULL vs pure true) is the reference engine's
own behavior (bare =; IS NOT DISTINCT FROM appears in no golden)". So the core keeps legend-engine's `=` on purpose.
This bears on F5.

### A2. Tagged equality: `M/lowering/Scalars.java`

- `taggedEquality` `:3853-3911` (58 lines). Tagged against tagged: `TaggedLists.sameElement(x, y)`. Tagged against a
  value: `sameElement(x, MixedEncoding.kindOfValue(...), ...)`. Two tagged lists: `sameTaggedLists`. A tagged list
  against a list of one kind: `TaggedLists.sameListOfKind`. Against a row's cells (`ArrayLit` of variant-wrapped
  cells): re-read as a tagged list at `:3878-3886`. Against a list on the variant carrier: `EQUAL` over
  `asVariantList` (JSON) at `:3900-3906`. Otherwise it refuses: "a mixed list compared with a list whose elements' kind
  is not static and that is not on the variant carrier: not lowered" (`:3907-3908`).
- `sameTaggedLists` `:3917-3926`:
  ```java
  if (everyElementWritten(s0) && everyElementWritten(s1)) { SqlExpr written = TaggedLists.sameWrittenLists(la, lb); ...
  return la.identityPositions() && lb.identityPositions() && TaggedLists.oneChannelPerFamily(la, lb)
          ? TaggedLists.sameListByKinds(la, lb) : TaggedLists.sameList(la, lb);
  ```
- `everyElementWritten` `:3930-3933`; `variantList` `:3937-3945`; `asTaggedList` `:3949-3967` (used by `contains`,
  `in`, `at` and `removeDuplicates` too, so it stays in Scalars); `partialPrecision` `:3766-3775`;
  `isFullPrecisionDate` `:3778-3781`.
- `emptiness` `:2909-2916` and `bothEmpty` `:2919-2921`. `InstanceEquality.disjointAnswer` already uses `bothEmpty`.

### A3. NullSemantics: `M/lowering/NullSemantics.java`

- `equalNullArms` `:107-150` (44 lines). Its arms:
  - a NULL-literal operand: `NULL_SAFE_EQUAL` (`:122-125`);
  - `VariantEquality.between(...)` refuses Variants by name (`:126-131`);
  - two optional COLUMNS: `NULL_SAFE_EQUAL` (`:132-138`);
  - an optional `PlanParam`, unless an enumeration is involved: `NULL_SAFE_EQUAL` (`:145-148`);
  - otherwise bare `EQUAL` (`:149`). So an optional column against a literal or a computed value answers NULL when the
    column is empty.
- `verbatim` `:101-105` rewrites `NULL_SAFE_EQUAL` back to `EQUAL` inside a mapping's own conditions. The Lowerer
  applies it (`Lowerer.java:4321-4330`: `NullSemantics.verbatim(verbatimEquality || legacyNullUnsafeEquals, ...)`).
- `negate` `:203-221` and `notEqualNullArms` (`:68-85`): `!=` and `not(in)` add the null arms. `enumInvolved`
  `:156-167`.

### A4. Equality inside the membership and deduplication arms: `M/lowering/Scalars.java`

- **`indexOf`** `:1507-1575`. The list part is `:1517-1539`: a Variant DISJOINT folds to -1, otherwise
  `LIST_POSITION(compactIfValueLane(asList(xs)), v) - 1`, coalesced. That is the database's equality inside
  `list_position`. H2 refuses `LIST_POSITION` (C).
- **`removeDuplicates`** `:1651-1714`:
  - a to-one value: `ArrayLit[x]`;
  - Variants refused (`refuseAmong`);
  - a tagged list: `TaggedLists.dedup(mixed, depth, (p,q) -> TaggedLists.sameElement(mixed,p,q))` (`:1669-1674`), or a
    comparator `:1675-1684`;
  - plain: `ListEncodings.orderedDedup(args.get(0))` (`:1686`; `M/lowering/ListEncodings.java:280`; `dedupFilter`
    `:310-317` is `LIST_FILTER(l, (x,i) -> LIST_POSITION(l,x) = i)`);
  - a comparator: `Dedup.keptDedup` (`LIST_REDUCE`) at `:1707-1712`.
- **`distinct`** `:1738-1750`: `orderedDedup`. **`isDistinct`** `:2465-2475`: `ALL_DISTINCT(asList(xs))`.
- **`contains`** `:2245-2365`. In order:
  - a Variant DISJOINT folds false (`:2259-2264`);
  - `asList` (`:2265-2269`);
  - a comparator over a tagged list (`:2274-2278`), or over a plain list (`:2279-2293`);
  - a singleton-literal needle is unwrapped (`:2295-2299`);
  - a TDSNull needle and the 'TDSNull' sentinel become `nullMembership` (`:2308-2320`);
  - `string::contains` (`:2321-2331`);
  - a tagged list: `TaggedLists.containsElement`/`contains` (`:2335-2342`);
  - an **Any** list: `Membership(TO_VARIANT(needle), list)` (`:2348-2352`, which compares JSON);
  - a class against a primitive folds false (`:2356-2358`);
  - otherwise `COALESCE(Membership(needle, list), false)` (`:2361-2363`).
- **`in`** `:2681-2765`:
  - the `[0..1]` overload coalesces (`:2681-2689`);
  - a Variant DISJOINT (`:2693-2696`);
  - `MixedEncoding.kindMismatch` folds false (`:2699-2702`);
  - `in(x, NULL)` is false (`:2705-2707`);
  - the Any wrap is decided by the lowered shape (`:2710-2719`);
  - a tagged list (`:2723-2729`);
  - a relation or a computed list: `Membership` (`:2734-2749`);
  - otherwise flat `IN` (`:2750-2764`).

### A5. TaggedLists' equality family: `M/lowering/TaggedLists.java` (slices 6.1 and 6.2)

| Method | Lines | What it does |
|---|---|---|
| `oneChannelPerFamily` | 63-74 | whether each family has one slot |
| `sameWrittenLists` | 84-121 | two written lists compared position by position, `EQUAL` or `NULL_SAFE_EQUAL` per position; answered by the types alone only where no element raises |
| `neverNull` / `raiseNothing` | 127-142 / 146-148 | |
| `sameListByKinds` | 156-178 | size, then `NOT IS_DISTINCT_FROM` per channel array (arrays compared whole, no lambda) |
| `nullChannel` | 208-227 | |
| `asVariantList` | 232-242 | `LIST_TRANSFORM` to JSON |
| `sort` / `members` / `numberTypeOrder*` | 278-315 / 325-348 / 359-440 | 6.2's ordering |
| `dedup` | 564-566 | `Dedup.keptDedup` |
| `sameElement(tl,p,q)` | 571-596 | the same family, the values `EQUAL` |
| `sameElement(x,kind,value,type)` | 602-619 | refuses a kind of null ("...whose kind only its runtime value names: not lowered") |
| `sameList` | 622-635 | `LIST_FILTER` with a 2-parameter lambda |
| `sameListOfKind` | 639-655 | `LIST_FILTER` |
| `sameElement(x,y)` | 671-688 | |
| `contains` | 698-748 | a written list: `IN` over the elements of the needle's family (`:711-730`); otherwise `LIST_FILTER` over positions; refuses a needle kind of null at `:700-703` |
| `containsElement` | 752-761 | `LIST_FILTER` |
| `dedupBy` | 831-839 | `LIST_POSITION` |

### A6. MixedEncoding: `M/lowering/MixedEncoding.java`

- `family` `:391-393` (a date-time at any written precision is "DateTime"); `sameFamily` `:396-398`;
  `channelsOf` `:403-411`.
- `kindOf` `:415-453`: a static kind. `DateTime(n)` for a written subsecond literal stays as a print fact. An abstract
  `Date` is null unless it is a partial literal.
- `kindOfValue` `:465-485` (6.2): `kindOf`, else the kind of an upcast's source, else the SQL type of a column for an
  open type (Number, Date, Any). Null for a `CASE`, arithmetic or a parameter.
- `kindName` `:488-502`; `columnChannel` `:512-`.

### A7. InstanceEquality, every method: `M/lowering/InstanceEquality.java` (485 lines)

The javadoc at `:22-48` calls it "ONE owner of the equality relation — the same canonical renders the verdict layer
byte-judges with".

| Method | Lines | What it does today |
|---|---|---|
| constants | 54-58 | the callee FQNs as strings (`removeDuplicates`, `distinct`, `isDistinct`, `indexOf`, `equal`) |
| `claims` | 62-93 | a type-only claim: dedup/distinct/isDistinct over many class instances; `equal` over two to-one instances or collections of classes; `eq` over two to-one instances; `indexOf`, `contains` and `in` with a to-one instance needle. Excludes Pair, List and Map carriers, TDS, Any, Variant, Nil and type classifiers (`classInstanceFqn` `:378-392`) |
| `lower` | 96-125 | wraps keysOf to refuse Variant keys (`VariantEquality.refuseVariantKeys`); dispatches |
| `equality` | 127-176 | `eq`: `trackedId(l) = trackedId(r)` where both layouts carry `__id`, else null. `equal`: different STATIC fqns give `false` on the identity lane (where both carry `__id`), else null; keyless without `__id`: null; otherwise `CAST(canon AS VARCHAR) = CAST(canon AS VARCHAR)` with `instanceEqualityCanon` (the key canon text or the identity text) |
| `contains` | 178-222 | different static classes: `false` on the identity lane; `COALESCE(Membership(CAST(needleCanon), LIST_TRANSFORM(coll, e -> CAST(canon(e)))), false)` |
| `equalValue` | 226-244 | a keyed class: canon text; keyless: `trackedId`; a canon of null throws `"<op> over <cls>: its key has no canonical form here"` |
| `equalValues` | 247-252 | `LIST_TRANSFORM` |
| `removeDuplicates` | 256-277 | `LIST_FILTER(xs, (x,i) -> LIST_POSITION(values, value(x)) = i)` |
| `isDistinct` | 280-295 | `ALL_DISTINCT(values)` |
| `indexOf` | 298-324 | different classes: -1; `LIST_POSITION(values, needle) - 1` |
| `collectionsEqual` | 328-351 | null when `lf != rf` or a side has no identity; `COALESCE(values_l, CAST([] AS VARCHAR[])) = COALESCE(values_r, ...)` |
| `hasIdentityField`, `isMany`, `instanceFqn`, `classInstanceFqn` | 353-392 | |
| `staticallyDisjoint` | 413-442 | Variant DISJOINT; two different enumerations; an enumeration against a non-String, non-Any value; an instance-construction LITERAL against a primitive |
| `disjointAnswer` | 447-453 | false, unless both sides may be empty: then `bothEmpty` |
| `instanceLiteral` | 461-465 | |
| `userClass` | 471-484 | read by `Scalars.java:3040` (an instance's `toString`) |

### A8. The Lowerer's two claims and the unclaimed refusal: `M/lowering/Lowerer.java`

- `:4119-4129`, inside `scalarRelationalArms` (`:4117-4186`, 70 lines):
  ```java
  case TypedNativeCall n when (isFamily(n, Pure.AT_BOOLEAN_EQUAL) || isFamily(n, Pure.AT_BOOLEAN_EQ))
          && InstanceEquality.staticallyDisjoint(n.args()) -> InstanceEquality.disjointAnswer(n.args(),
                  a -> scalar(a, columns));
  ```
- **The unclaimed refusal**, `:4303-4318`, inside `scalarValueTailArms` (`:4240-4464`, 225 lines, so only 25 lines of
  headroom):
  ```java
  case TypedNativeCall n when instanceKeysOf != null
          && InstanceEquality.claims(n) -> {
      SqlExpr ie = InstanceEquality.lower(n, instanceKeysOf, this::sqlTypeOf, s -> scalar(s, columns), () -> "_iq" + aliasCounter++);
      if (ie == null) {
          throw new com.legend.error.NotImplementedException(n.callee().qualifiedName() + " over objects of "
                  + ... + " that carry no identity here: they are compared by their keys or their identity,"
                  + " never as structs (docs/OBJECT_IDENTITY_2026_10_20.md §3.3)");
  ```
  It fires when `lower` returns null. With the object layout on every product lane (below), that leaves: two
  collections whose STATIC classes differ but are related (`collectionsEqual` returns null when `lf != rf`); a key with
  no canon (`instanceCanon` null: a to-many key not carried as an array, an unknown field, a leaf kind with no literal
  canon); and a struct without `__id` (a store row read on the value lane: I did not trace whether every store layout
  carries `__id`).
- Lane state: `instanceKeysOf` (`:374-376`, set by `withInstanceKeys` `:471-476`) and `objectIdentity` (`:385`, set by
  `withObjectIdentity` `:464-469`). `objectId` (`:405-`) mints a NULL id (`CAST(NULL AS VARCHAR)`) where evaluations
  are not tracked. Every product planner sets both:
  - `M/planner/ProductStatements.java:467-470` (`objectLayoutOf` + `withObjectIdentity`);
  - `M/Compiler.java:963-966`;
  - `M/planner/BodyPlanner.java:155-166`, where the identity lane uses `ClassLayouts.layoutOf(ctx,t,true)`, which
    carries `__canon`; every other lane uses `objectLayoutOf`.
- Layouts (`M/compiler/element/ClassLayouts.java`):
  - the product's object layout (`objectLayoutOf` `:134-145`) is the declared properties + `__id` + `__type`, with no
    `__canon`;
  - the identity layout (`:104-126`) adds `__canon`, which `ConstructionCanon.bind` fills at construction
    (`M/lowering/ConstructionCanon.java:35-72`) with a `LIST_GET(LIST_TRANSFORM([struct], λ), 1)` let-binding;
  - **on H2 that `LIST_GET` is refused**: "DialectCapability: LIST_GET" in `T/testplan/AssertBarTest.java:891-905`.

### A9. CanonicalRenderSql's instance canon and its readers: `M/lowering/CanonicalRenderSql.java` (1,119 lines)

- `identityCanon` `:635-647` and `identityText` `:653-657`: `<id instanceOf T>`.
- `trackedId` `:662-664` and `tracked` `:666-669`: `CASE WHEN id IS NULL THEN ERROR(UNTRACKED) ELSE id END`.
  `UNTRACKED` is at `:671-673` (counted by `RaiseTextGuardTest`, E6).
- `instanceEqualityCanon` `:679-685`: keys null gives `identityCanon`, otherwise `instanceCanon`. **Its only reader is
  InstanceEquality** (`:166`, `:168`, `:206`, `:208`, `:237`). It dies with InstanceEquality.
- `instanceCanon` `:707-791` (85 lines):
  - the List carrier (an Array with one to-many key, `:712-731`);
  - a struct: `JsonObject(_type: <STATIC keys.classFqn()>, k: leaf, ...)`;
  - a to-many key: `LIST_TRANSFORM` of leaf canons, `COALESCE` to `[]`;
  - a `[0..1]` key: `COALESCE(leaf, '[]')`.
  - The `_type` is the static key owner's FQN, not the run-time `__type`.
  - Readers: InstanceEquality (through `instanceEqualityCanon`), `wrapWithCanon` `:259` (the judge's canon rider:
    `M/planner/SidePlanner.java:133`, `M/testplan/DatabaseJudge.java:708`), `taggedLeaf` `:860` (recursion), `valueCanon`
    `:962`, and `T/lowering/SqlCanonConformanceTest.java:133,154`.
- `taggedLeaf` `:845-874`: a nested `__canon`, a JSON slot, a nested key tree, or `literalCanon(field, kindOfSqlType(ft))`.
  The leaf's text comes from the field's SQL type, so `DECIMAL(2,1)` against `DECIMAL(3,2)` gives "1.0" against "1.00".
- `mapCanon` `:801-827`, `sortedEntries` `:831-834` and `valueAtKey` `:837-840` (VerdictSql's `entryRows` reads the
  last two).
- `emptyArray` `:877-879`: InstanceEquality reads it at `:345`.
- `constructionCanon` `:905-951` and `valueCanon` `:954-979`: the identity lane's `__canon` stamp.
- **After 6.4** `instanceCanon`, `identityCanon`, `trackedId` and `constructionCanon` stay for the judge's canon rider
  until 6.5. `instanceEqualityCanon` and `emptyArray`'s InstanceEquality use go.

### A10. `SAME_IN_ORDER` / `SAME_IN_ANY_ORDER` and `SameValuesForm`

- Declared in `M/sql/SqlFn.java:149-158`. The arguments are: (eList, aList, eRowsSubquery, aRowsSubquery). Each row
  subquery has its value column(s) (`__o`, or a grid's cells) and `__rn`. The javadoc says "a mixed-kind side never
  reaches it". Typing is in `M/sql/SqlTyping.java:354` (never NULL), `:401` (BOOLEAN) and `:786`.
- **Who writes it today: VerdictSql alone.**
  - `typedEquals` `:121`, `typedCollections` `:177`, `typedTagged` `:266`, `typedInstances` `:318`;
  - `typedInstanceTexts` `:1255`, `:1274`; `typedJson` `:1438`; `typedGridPair` `:2219-2224`.
  - Every call's row subqueries are `ScalarSubquery(rowsOf("<cte>", ...))` over CTEs the verdict's `WITH` defines.
    They are uncorrelated statement-level rows.
- **How each dialect writes it:**
  - **DuckDB**: `M/sql/dialect/DuckDb.java:617` `sameLists(writer, c, "list_sort")`, which is
    `(e IS NOT DISTINCT FROM a)` over the two LIST arguments (any order: both `list_sort`ed;
    `M/sql/dialect/AnsiSqlRenderer.java:1408-1424`).
  - **The portable rows form** (`AnsiSqlRenderer.nullSafe` `:1382-1393` and then `sameValues` `:1397-1404`) is written
    by: H2 `H2.java:698`, SQLite `SQLite.java:1702`, Postgres `Postgres.java:807`, Trino `Trino.java:937`, Snowflake
    `Snowflake.java:1400`, Databricks `Databricks.java:1078`, BigQuery `BigQuery.java:1579` (`nullSafeArm`), MariaDB
    `MariaDb.java:1950`, ClickHouse `ClickHouse.java:2012`, and MySQL through `super.call` (`MySql.java:124`).
  - **MongoDB** refuses it: `MongoDb.java:1045`.
- `M/sql/dialect/SameValuesForm.java`:
  - `of` `:40-48` refuses a value column that is an Array or a Map: "an assert comparing lists or maps of values has no
    typed form on a database that carries them as JSON".
  - `inOrder` `:50-78`: counts, then an inner join on `__rn` and `NULL_SAFE_EQUAL` per column.
  - `anyOrder` `:80-106`: a signed `UNION ALL` grouped by every value column.
  - Every side is a `Subselect` (a derived table) of the row subquery. **This is the H2 problem for the product**: H2
    cannot correlate into a derived table nested below a correlated subquery
    (`docs/records/database-recheck-2026-11-26/h2-bigquery-snowflake-databricks.md`, the claims table, row 2). A
    product list inside a row (a column, a lambda) would need correlated rows. On H2 this form fails there; the judge's
    CTE rows work.

### A11. VerdictSql, the inventory 6.4 must cover (`M/lowering/VerdictSql.java`, 3,034 lines, EVICT pin 2263)

The comparison members, all public unless marked:

| Member | Lines | What it compares |
|---|---|---|
| `typedEquals` | 96-148 | two sides of one typed class: a list each, `SAME_IN_[ANY_]ORDER`; two kinds never equal, or an empty side: `noRows AND noRows`; date-times read as instants (`readOnce`/`instants`) |
| `typedCollections` | 158-187 | nested `List<T>` (an array of scalars) in order; a Map as entries sorted by key (`entryRows` 196-210) |
| `typedTagged` | 245-279 | tagged or typed sides as kinded rows `{k, v0..vn}` (`kindsOf` 587-637, `kindedRows` 781-930). Decimal by value; DateTime of any precision one kind (an instant); a written Date's kind read from its text (685-751); JSON by its JSON kind (646-672) |
| `typedInstances` | 295-330 | instances of one class (a model class or a Pair) as rows of typed leaves: `__type`, then the keys (a nested keyed instance's leaves in place) or `__id`; in order or as a multiset |
| `typedContainsInstances` | 360-399 | instance membership: a join on all leaves `NULL_SAFE_EQUAL`, `COUNT > 0` |
| `typedEmpty` / `typedEmptyTagged` / `typedEmptyGraph` / `typedNotEmptyGraph` | 1177-1193 / 1198-1209 / 1097-1108 / 1112-1116 | emptiness (the family the plan moves to 6.4/6.5) |
| `typedInstanceOf` / `typedInstanceOfHeld` | 1214-1232 / 1453-1483 | not equality |
| `typedInstanceTexts` | 1239-1284 | instance DOCUMENTS compared as JSON text (G8: stays a judge member) |
| `typedJson` | 1397-1447 | JSON documents (G8: stays) |
| `typedContainsTagged` | 1492-1530 | tagged membership: a join on `k` and `NULL_SAFE_EQUAL` per kind column |
| `typedContains` | 1535-1567 | scalar membership: a join `NULL_SAFE_EQUAL` over instants |
| `typedTolerance` | 1572-1597 | `|e-a| <= d` (6.6) |
| `typedAllOf` | 1602-1618 | the quantified vector |
| `typedSubset` | 1624-1648 | the forAll-contains fold: `NOT EXISTS` `NULL_SAFE_EQUAL` |
| `typedPerElement` | 1657-1708 | a per-element `NULL_SAFE_EQUAL` of `{first, second}` |
| `typedCondition` / `typedOverValues` | 1033-1042 / 1067-1091 | the product's Boolean (the target form; stays) |
| `typedTdsEquivalent` | 2055-2115 | within tolerance (stays a member) |
| `typedGridPair` | 2192-2232 | two grids by typed cells (`typedCells` 2151-2173); `schemasDiffer` gives false at planning; cross-kind columns by a kind tag |
| `nanos` / `readsNanos` | 1772-1792 / 2401-2411 | DuckDB nanoseconds (6.6) |
| helpers | `noRows` 1002-1008, `oneCarrier` 1013-1016, `rowsOf` 1019-1026, `instants` 954-998, `typedClass` 938-945, `valueRows` 1891-1950, `listed` 1874-1884, `typedRow` 1724-1764 | |

**The typed instance leaves that move** (VerdictSql to the owner): `pairedLeaves` 334-350, `Leaf` 438-439, `leaves`
443-475, `leafClass` 479-493, `fieldType` 495-502, `tracked` 512-545, `append` 547-551, `leafRows` 558-583. Also, for
the record, `restrictedRows`/`restricted` 404-435, which stay with the record layer.

**Gap in VerdictSql's leaves:** `leaves` returns false for a to-many key (`:460`, `k.many()`). So the judge has no typed
form for legend-pure's `testEqualNonPrimitive` (`LeftClass.sides` is a to-many keyed key; see D). The product's canon
handles it on DuckDB through `LIST_TRANSFORM`. **6.4 cannot just move VerdictSql's leaves: it must add the to-many case,
or keep refusing by name what the product answers today** (F8).

---

## B. What `==` answers today: the product against the database judge's layer

Sources: *code* (read here), *6.0* (`docs/records/edition-homework/r3/measured-step6-equality.md`, run at `9c751659`,
before 6.1 and 6.2), *pin* (a test). "Judge" means VerdictSql's member as JudgmentFacts routes it. DB = DuckDB, H2,
SQLite.

| Shape | Product today | Judge today | Mark |
|---|---|---|---|
| Integer/String/Boolean/enum, both to-one | `=` (A1 #7) | `typedEquals`, `SAME_IN_ORDER` | SAME |
| Float | `=` | the same | SAME |
| Decimal `1.0D == 1.00D` | the database's `=` (by value): true on all three (6.0 §4e) | by value | SAME |
| a store Decimal carried as `DECIMAL_TEXT` against a Decimal | `=` | `oneCarrier` false, then UNJUDGED | REFUSED in judge |
| DateTime literals at two written precisions | 6.1's fold rule leaves them to the database (TIMESTAMP compare): true on DuckDB and H2; **SQLite compares the text: false** (plan §6a, 6.1's finding) | `instants` makes it true everywhere | DIFFERS on SQLite (step 6's SQLite date-time form, not 6.4) |
| an hour-precision DateTime literal (`%2015-04-16T14`) | unreadable on DuckDB and H2 (6.0 §4a, "new", 4/4/0) | read at its minute | DIFFERS (product ERROR) |
| a StrictDate against a DateTime | compile error (5a) | compile error | SAME (REFUSED both) |
| partial dates (Year, YearMonth) | `partialPrecision`: the same precision compares the ISO-prefix text; otherwise false | kinded rows (Year and YearMonth as BIGINT) | SAME (code) |
| a Variant against anything | refused by name (S401) | refused | SAME (REFUSED both) |
| one optional side empty at run time (`$x.opt == 1`, `[]->head() == []->head()`) | bare `=` gives **NULL**; NULL-safe only for a NULL literal, two optional columns, or an optional parameter (A3) | no rows: `[]==[]` true, `[]==1` false | DIFFERS (pin: `T/testexec/EqualityWorldsConformanceTest.java:115-117` `diverge(true, null, "[]->head()", ...)`) |
| a scalar against a one-element computed list (`1 == [1,2]->filter(x\|$x<2)`) | `EQUAL(INTEGER, INTEGER[])`: a binder error on DuckDB (6.0: "DuckDB's list shapes 16") | rows: true | DIFFERS (product ERROR) |
| a scalar against a written `[1]` | the typer collapses it to a scalar | the same | SAME |
| a list against a list of one type, written, Integer/String | DuckDB array `=`: answers. H2 `JSON_ARRAY(..) = JSON_ARRAY(..)` (`H2.java:1000-1005`): JSON equality. SQLite: JSON text | rows | SAME for Integers and Strings |
| the same with Decimals at two scales or date-times at two precisions | H2 and SQLite compare JSON, keeping the scale and the digits: `[1.0D,10.125D] == [1.000D,10.125D]` false on H2 and SQLite (6.0: number types 1/3/3) | by value or instant | DIFFERS on H2 and SQLite |
| a list against a list of one type, computed (`filter`/`map`) | DuckDB: answers. H2: `LIST_FILTER` refused (6.0: 64 bodies "H2's missing list encodings", rewritten form only). SQLite: JSON text | answers (the root unnests) | DIFFERS (REFUSED in product on H2) |
| a column-sourced list (a to-many property) | DuckDB array `=` | rows | did not trace the store paths |
| `[] == []`, both literal | `true` | true | SAME |
| a literal `[]` against a computed empty list | `IS_NULL(list)`: **false** on DuckDB and SQLite, an error on H2 (6.0 §4e); the computed `[]` is not NULL | `noRows`: true | DIFFERS |
| two computed empty lists, one a `[]->cast(@T)` (NULL) and one a `filter` (`[]`) | `NULL = []` gives NULL | true | DIFFERS (code; not run) |
| mixed lists, both written | `sameWrittenLists`, position by position (6.1): no list in the SQL, every DB | `typedTagged` kinded rows | SAME (TaggedListTest:188-253) |
| mixed lists, not written (reordered, computed) | `sameListByKinds` (array `IS DISTINCT`) or `sameList` (`LIST_FILTER` lambda): H2 refuses the lambda; JSON databases may split one instant at two precisions (plan §5b row) | kinded rows by kind and instant | DIFFERS (REFUSED on H2; may differ on SQLite and MariaDB) |
| a mixed list against a list of one kind, not written | `sameListOfKind` (`LIST_FILTER`): refused on H2 | kinded rows | DIFFERS on H2 |
| a value typed only at run time against a tagged value | 6.2: a column's SQL type; refused for a `CASE`, arithmetic or a parameter | kinded rows (a JSON kind, a written Date's text) | DIFFERS (product REFUSED for non-column shapes) |
| a mixed list against `[]` | `size(tl) = 0` | noRows | SAME |
| nested lists (`list([1,2]) == list([1,2])`, the List carrier) | not claimed by InstanceEquality (`classInstanceFqn` excludes List): array `=`. DuckDB native; H2 and SQLite compare JSON text | `typedCollections`: DuckDB `SAME_IN_ORDER` on lists; H2 and SQLite: `SameValuesForm` refuses an Array column | DIFFERS (product answers by text; REFUSED in judge on H2 and SQLite) |
| a Map against a Map | `=` over MAP (DuckDB); H2 has no map type (refused); SQLite JSON text | entries sorted by key | DIFFERS likely (DuckDB map `=` with no sort: not run); REFUSED in judge on JSON databases |
| a Pair against a Pair | not claimed (Pair excluded): struct `=`. DuckDB compares positionally with coercion: `pair(1, 1D) == pair(1, 1)` **true** (6.0 §4e, G4). H2 and SQLite: a struct as JSON | `typedInstances` (Pair leaves by `leafClass`; different leaf kinds: `pairedLeaves` null); `pair(1, 1.50D)` against `pair(1, 1.5D)` passes under every judge (`T/testplan/AssertBarTest.java:908`) | DIFFERS (`pair(1,1D)==pair(1,1)`: product true, Pure false, 6.3's marker); the H2 and SQLite Pair answer not run |
| keyed instances, one Integer or String key | `CAST(canon) = CAST(canon)`, the JSON text of the key | typed leaves `__type` + key | SAME (pin `T/testexec/InstanceIdentityTest.java:186-203`) |
| a compound key | the canon's JSON over all keys in order | leaves path by path | SAME |
| a Decimal key `^K(d=1.0D) == ^K(d=1.00D)` | the canon text keeps the scale: **false** | `DECIMAL IS NOT DISTINCT`: **true** | DIFFERS (plan §5b row, owned by 6.4) |
| a DateTime key at two written precisions | the canon's leaf text from the field's SQL type (`literalCanon`); a TIMESTAMP prints its value, a text carrier keeps the digits; not run | instant: `^p::K(at=...10:00:00.5)` against `.500` passes (`AssertBarTest.java:891`) | DIFFERS likely (did not run the product) |
| a nested keyed instance as a key | the canon recurses (`taggedLeaf`) | nested leaves in place | SAME |
| a **to-many key** (LeftClass.sides, `testEqualNonPrimitive`) | the canon's `LIST_TRANSFORM`: answers on DuckDB; H2 refuses it (PCT `Test_LegendLite_H2_GrammarFunctions_PCT.java:41-42`) | `leaves` returns false at `:460`, no typed form, so UNJUDGED or another family | DIFFERS (REFUSED in judge; product answers on DuckDB) |
| keyless instances | the identity text `<id instanceOf T>`, `trackedId` raising where untracked | leaves `__type` + `__id`, guarded the same way | SAME (pin `InstanceIdentityTest.java:207-214`) |
| keyless `$xs->at(0) == $xs->at(0)` | false (the design's G4 claim) | | not verified here |
| `eq` (identity) | `trackedId = trackedId` | an eq assert | SAME (pin `InstanceIdentityTest.java:175-183`, `T/integration/TypeInferenceIntegrationTest.java:1679-1684`) |
| an instance against a subclass instance (related static types) | the static FQNs differ, both carry `__id`, so **`false` is folded** (`InstanceEquality.java:151-158`) whatever the run-time classes are | leaves start with the run-time `__type` | DIFFERS where a supertype-typed value holds the subclass (`$s == $s->cast(@Super)`: product false, Pure true) |
| one static class, two run-time classes, equal keys | the canon's `_type` is the STATIC `keys.classFqn()`: **true** | `__type` differs: false | DIFFERS |
| two unrelated classes | compile error (`EqualityClasses.crossing`, `:92`), pin `InstanceIdentityTest.java:195-203` | the same | SAME (REFUSED both) |
| lists of instances (`collectionsEqual`) | `COALESCE(LIST_TRANSFORM(canons), [])` compared with `=` (DuckDB); H2 refuses `LIST_TRANSFORM`; different related static classes hit the **unclaimed refusal** | `typedInstances` in order | DIFFERS (REFUSED on H2; REFUSED for related classes) |
| instance `contains` / `in` | `Membership` over canon texts: H2 refuses it (PCT `Test_LegendLite_H2_EssentialFunctions_PCT.java:45`, `..._StandardFunctions_PCT.java:50`) | a join on all leaves | SAME on DuckDB (`InstanceIdentityTest.java:244-251`, `:314-329`); REFUSED in product on H2 |
| a table against a table (`#TDS..# == #TDS..#`) | generic `EQUAL` of the two relation operands; "ignores column names and row order" (6.0: grids, fail to pass, 4/3/4). I did not trace how a relation lowers in a scalar position | `typedGridPair`: by column name (`schemasDiffer` false), typed cells, rows in order (or a multiset) | DIFFERS |
| a table row's cells against a list (`[...] == $r.values.rows.values`) | the rowCells path in `taggedEquality` (`Scalars.java:3878-3886`) | `typedTagged` | SAME on DuckDB (pins `T/exec/TaggedListTest.java:313-322`) |
| `contains`/`in` over one-type scalar lists | `Membership` / `IN`: the database's equality | `typedContains`, a join `NULL_SAFE_EQUAL` over instants | SAME except date-time precision on SQLite |
| `contains` over an **Any** list | `TO_VARIANT` (JSON): `[1,2]->cast(@Any)->contains(1.0)` true but `1.0->in(...)` false (6.0 §4e) | JSON kinds | DIFFERS (6.2's territory; check 6.2's result) |
| `indexOf` over a list | `LIST_POSITION`: refused on H2 (`..._H2_EssentialFunctions_PCT.java:68`) | no assert form | REFUSED in product on H2 |
| `removeDuplicates` / `distinct` / `isDistinct` | DuckDB value equality (`list_position`, `ALL_DISTINCT`); H2 answers a literal through CarrierStrategies' dedup rule (`CarrierStrategies.java:212`), refuses a computed list | n/a | REFUSED in product on H2 (computed) |

**The shapes that DIFFER today, in one line:**

- empties: a computed `[]`, and a NULL against `[]`;
- one optional side in value position (NULL against a Boolean);
- a scalar against a list (DuckDB errors);
- lists of Decimals or date-times on H2 and SQLite (JSON text);
- computed lists, mixed lists not written out, and lists of instances on H2 (refused);
- nested lists and maps (the product compares text, the judge refuses);
- instance keys that are Decimals or date-times (canon text);
- run-time class against static class (the fold and the canon's static `_type`);
- to-many keys (the judge has no form);
- grids (names and order);
- an hour literal;
- `pair(1,1D) == pair(1,1)` (6.3).

---

## C. H2: what its dialect refuses on the way to `==` and membership, and the row form for each

H2 2.5.252 carries a list as JSON (`H2.java:1000-1005` `JSON_ARRAY(...)`; the `LIST` aggregate is `JSON_ARRAYAGG`,
`:1150`, `:1225-1240`). `CARDINALITY` works on it (`listLength` `:1461-1471`), and so does element access `(j)[i]`,
1-based (the class javadoc, `:25-30`). Caps: `CarrierStrategies.Caps(false,false,false,false)` (`H2.java:231`). There
are no lambdas, no LATERAL, and no correlation into a nested derived table.

| Refusal site | Message | Reached from | Row form that replaces it (probed in `docs/records/database-recheck-2026-11-26/h2-bigquery-snowflake-databricks.md`, the list-operations table) |
|---|---|---|---|
| `AnsiSqlRenderer.listCall` `:1687-1689` (H2 does not override it; `H2.java:712-715` routes `LIST_FILTER, LIST_TRANSFORM, LIST_GET, LIST_AT, LIST_POSITION, LIST_DISTINCT, LIST_REDUCE, LIST_SORT, LIST_REVERSE, ...` there) | "`<FN>` reached a dialect without a list encoding" | `LIST_POSITION`: `indexOf`, `dedupFilter`, InstanceEquality's indexOf and dedup. `LIST_FILTER`: `TaggedLists.sameList`, `sameListOfKind`, `contains` (not written), `containsElement`, `presentPositions`, `dedupFilter`. `LIST_TRANSFORM`: InstanceEquality, `instanceCanon`'s to-many keys, `asVariantList`. `LIST_GET`: `ConstructionCanon` (every identity-lane construction), `TaggedLists.at`. `LIST_REDUCE`: `Dedup.keptDedup` | position: `(SELECT MIN(I) - 1 FROM SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= COALESCE(CARDINALITY(l),0) AND CAST((l)[I] AS T) = v)`. Filter or transform: `ARRAY(SELECT ... FROM R ... ORDER BY I)` (or `JSON_ARRAYAGG` to stay on the JSON carrier). First-seen dedup: the `NOT EXISTS` second range. `LIST_GET`: `CASE WHEN n BETWEEN 1 AND CARDINALITY(l) THEN (l)[n] END`. `LIST_REDUCE` over a column: keep the refusal (a correlated recursive CTE fails) |
| `AnsiSqlRenderer.membership` `:1109-1114` | "collection membership reached a dialect without a list encoding [collection: ...]" | `Membership` over a non-literal collection: `contains`/`in` of a computed list, an Any list, instance canons | `EXISTS (SELECT 1 FROM SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= COALESCE(CARDINALITY(l),0) AND CAST((l)[I] AS T) = needle)` |
| `AnsiSqlRenderer.allDistinct` `:1675-1678` (`H2.java:724`) | "collection isDistinct reached a dialect without a list encoding" | `isDistinct`, InstanceEquality.isDistinct | `NOT EXISTS` over two ranges `I < J AND (l)[I] = (l)[J]` (written by analogy with the probed dedup; not itself probed) |
| `AnsiSqlRenderer.listExists` / `listForAll` `:1660-1684` (`H2.java:716-717`) | "collection exists/forAll reached a dialect without a list-predicate encoding" | `exists`/`forAll` over lists | `(SELECT BOOL_OR(pred) FROM R)` / `BOOL_AND` (probed) |
| `AnsiSqlRenderer.unnestProjection` `:1697-1699` (`H2.java:719`) | "UNNEST reached a dialect without an unnest placement" | `TaggedLists.sort`/`sortRows`; a list returned at the root | a JOIN to `SYSTEM_RANGE(1, (SELECT MAX(CARDINALITY(xs)) FROM T))` (probed; an unbounded range in a JOIN does not finish) |
| `SameValuesForm.of` `:40-46` | "an assert comparing lists or maps of values has no typed form on a database that carries them as JSON" | the judge's nested lists and maps | for 6.4: per-element typed reads of the nested JSON. Otherwise keep the refusal |
| `SameValuesForm`'s nesting (`inOrder` and `anyOrder` wrap each side as a `Subselect`) | not a refusal: an H2 error on correlation ("Column ... not found") | a product `SAME_IN_*` over rows correlated to an outer row | **H2 needs its own `SAME_IN_*` arm over the two LIST arguments**: in order, `CARDINALITY(a) = CARDINALITY(b) AND NOT EXISTS (SELECT 1 FROM R WHERE I <= CARDINALITY(a) AND CAST((a)[I] AS T) IS DISTINCT FROM CAST((b)[I] AS T))` (one level of correlation, and "two levels (NOT EXISTS inside it) also work"). In any order, a per-element count comparison over two ranges. The element type comes from the call's typed list (a dialect reads types; it infers none) |
| `H2.java:137` `structTypeName` | "a STRUCT type reached a dialect without struct support" | a cast to a struct type (VerdictSql's `leafRows` casts leaves to their layout types, which are scalars, so it does not hit this) | instances on H2 are JSON objects; a leaf read is `(j)."field"` (`H2.structGet` `:1505-1508`), then a typed cast |
| `H2.listLength` `:1461-1471` | none: works | | |
| a plain `EQUAL` over two `JSON_ARRAY`s | none: **it answers by JSON equality**, wrong for Decimal scale and date-time digits | `[1.0D] == [1.00D]` | the `SAME_IN_ORDER` arm above, with typed element casts |

Also from the recheck: the H2 defect `I <= CARDINALITY(NULL)` raises "Unexpected code path", so write
`COALESCE(CARDINALITY(x), 0)`. `ARRAY(...)` over a NULL list answers `[]`, so wrap with
`CASE WHEN x IS NULL THEN NULL ...` where NULL must stay NULL. The literal-pick fold (`CarrierStrategies.literalPick` /
`filteredLiteralLength`, about 38K characters on H2) is the lean-SQL program's, not 6.4's (`START_HERE.md` §5: "Step
6.4 reworks how lists are compared on every database. Read what it landed before D2 and D4.").

---

## D. Tests that pin today's product answers

| File:line | Assertion | Expected move in 6.4 |
|---|---|---|
| `T/testexec/EqualityWorldsConformanceTest.java:115-117` | `diverge(true, null, "[]->head()", "[]->head()", ...)`: the product answers SQL NULL | becomes `agree(true, ...)` if 6.4 makes two empty to-one values equal (F5's ruling) |
| `T/testexec/EqualityWorldsConformanceTest.java:110-111` | `refusedByTheProduct(false, "1", "1.0", ...)` | stays |
| `T/exec/TaggedListTest.java:188-190, 202-206, 223-253` | mixed-list `==`, `contains`, `removeDuplicates`; `:251` `[1,'a'] == []` false; `:252-253` against computed lists false | stay (DuckDB only; `TestBackends.platform()`) |
| `T/exec/TaggedListTest.java:313-322` | a row's cells against a list, ordered by the relation's sort | stays |
| `T/testexec/InstanceIdentityTest.java:150-160, 175-183, 186-203, 207-214, 216-240, 244-251, 300-329` | keyed `equal` and `==` true where keys match; eq identity; keyless identity; to-many key `sides`; `contains`; value position | must stay green with typed keys. Add: `^K(d=1.0D) == ^K(d=1.00D)` true |
| `T/integration/TypeInferenceIntegrationTest.java:1679-1709` | `eq` over two constructions false; `contains` struct against primitive a compile error; keyed `contains` true | stay |
| `T/lowering/NullSemanticsTest.java:93-101` | `$r.e == $r.f` (two empty columns) keeps every row; `$r.STREET == $r.e` is the absence test | stay; check the rewritten empty arm keeps `IS NULL` for to-one columns |
| `T/integration/DuckDBIntegrationTest.java:4821-4831` | `twoEmptyColumnsAreEqual` | stays |
| `T/lowering/SqlCanonConformanceTest.java:125-160` | `instanceCanon`'s Pair and List texts | stays (the canon stays for the judge until 6.5) |
| `T/DecimalKindsTest.java:138-145` | `assertEquals([1], ...filter(p\|$p.b == 0.2D)...)` | stays (the design says so) |
| `T/testplan/AssertBarTest.java:1037-1040` | `assert('{}'->fromJson()->get('a') == []->cast(@Integer))` | moves with the empty arm if a Variant path's absence is `IS NULL`: read the expected string there |
| `T/testplan/AssertBarTest.java:887-939` `instancesAreComparedByTheirKeysOrTheirIdentity` (every judge, every in-process DB) | `assertEquals` over instances; H2 expects `DialectCapability: LIST_GET` | judge-only today. If 6.4 deletes `__canon`'s `LIST_GET` from the product, nothing moves (the identity lane still stamps it). The `H2=` expectations move only when the identity lane stops stamping |
| `T/testplan/AssertBarTest.java:943-1040` `theLastFamiliesAreTypedOrRefusedByName` | Variant shapes, nested lists and maps refused on JSON databases | `assert(cond)` bodies go through the product's `==` (`databaseCondition`), so the `assert(... == ...)` lines here move with 6.4 |
| `T/testplan/AssertBarTest.java:1207-1258` `aTableComparisonIsOneOutcomeOnEveryDatabase` | grids under `assertEquals`/`assertSameElements` | judge-only; unchanged in 6.4; the inventory 6.4's grid `==` must match |
| `T/testplan/AssertBarTest.java:47-48, 308-310, 621-622` | `assert(1 == 2, ...)`, the forAll-contains subset | stay |
| `T/JudgesOnPlansTest.java`, `T/testplan/BodyRouterParityTest.java`, `T/lowering/VerdictRowOrderTest.java`, `T/exec/LiteralChannelTest.java` | both-judges agreement | 6.5's movers (design 5c-2); run them as the both-judges check after each 6.4 piece |
| `T/equalitymeasure/EqualityMeasure.java:147-180` | the 6.0 probes, including `"[] == [1, 2]->filter(x \| $x > 5)"` | tooling, not a test: re-run as 5c-0 |
| PCT H2, `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_EssentialFunctions_PCT.java:45` | `testContainsNonPrimitive`: Membership over `LIST_TRANSFORM` | leaves when instance membership has an H2 form |
| same file `:68` | `testIndexOf`: `LIST_POSITION` | leaves with H2's `LIST_POSITION` row form |
| same file `:70-73` | removeDuplicates: UNNEST at the root | leaves only if the root's list egress on H2 is written (likely not 6.4) |
| `..._H2_StandardFunctions_PCT.java:50` | `testInNonPrimitive`: Membership over `LIST_TRANSFORM` | leaves with instance membership on H2 |
| `..._H2_StandardFunctions_PCT.java:43,47,53,57,64,68,79,87,91` | greatest/least/max/min/mode: `LIST_POSITION` | not equality (a tagged list's extreme); leaves only if H2's `LIST_POSITION` lands generally |
| `..._H2_GrammarFunctions_PCT.java:41-42` | `testEqualNonPrimitive`, `testEqualVarIdentity`: `LIST_TRANSFORM` | leave once instance `==` (with a to-many key, `LeftClass.sides`) has an H2 form |
| `..._LiteRules_EssentialFunctions_PCT.java:52` | `testContainsNonPrimitive`: "contains compares CO_Firm with Integer" | stays (the strict rule) |
| PCT channel B H2 relation (`pct/src/test/resources/edge/channel_b_h2_relation_outcomes.tsv`, 24 list mentions) and `.../channelb/ledger.tsv` (25) | outcomes | regenerate if the H2 relation suite moves; per category not traced |

**Caveat on the expected PCT moves.** The 6.0 categories "H2's missing list encodings 64" and "H2 mixed-list syntax
error 24" were measured on the REWRITTEN form (`assert(e == a)`). PCT runs the asserts as written, through the judge.
So in PCT, 6.4 moves only bodies whose own code uses a product `==`, `contains`, `in` or `indexOf` (the pins above).
"H2's 67 list bodies answer" is a 6.5-time outcome, unless 6.0's harness is re-run on the rewritten form (5c-0).

---

## E. Guards that will trip (current values)

### E1. JavaEvalLedgerTest EVICT_SIZE (`T/JavaEvalLedgerTest.java:84-`, the rule at `:2505-2530`)

- `lowering/VerdictSql.java`: **2263** (`:611`; FROZEN since 2026-11-23, shrink-only; the last move was 2286 to 2263
  with the first slice). Moving `leaves`/`leafClass`/`fieldType`/`pairedLeaves`/`Leaf`/`leafRows`/`tracked` out
  shrinks it, a dated re-pin. A shrink of any size must re-pin (`:2524+`).
- **No pin** for Scalars, InstanceEquality, CanonicalRenderSql, NullSemantics, TaggedLists or MixedEncoding (the only
  lowering pin is VerdictSql). A new `lowering/Equalities.java` needs no pin unless the program wants one.
- Neighbours the work may touch: `testplan/DatabaseJudge.java` 827 (`:612`), `testplan/JudgmentFacts.java` 355
  (`:603`), `testplan/AssertVerdicts.java` 1335 (`:615`), `planner/SidePlanner.java` 102 (`:864`), `testexec/Equality.java`
  319 (`:314`; the user's word is needed for growth).

### E2. CodeShapeGuardrailTest (`T/CodeShapeGuardrailTest.java`)

`METHOD_LIMIT = 250` (`:37`), `METHOD_ALLOWLIST = Map.of()` (`:41`), measured by `SIG` (`:457-466`) with brace
counting (`:748-780`).

- **Holes in the guard:**
  - A static initializer is never measured. The `==` lambda (`Scalars.java:116-191`, 76 lines) and every `RULES.put`
    lambda are outside it.
  - The `SIG` class `[\w.<>\[\], ?]+` has no `@`, so **every method whose return type carries
    `@com.legend.base.Nullable` is not measured**: `Scalars.taggedEquality` (58), `InstanceEquality.equality` (50),
    `contains` (45), `lower` (30), `indexOf` (27), `collectionsEqual` (24), `removeDuplicates` (22), `equalValue` (19),
    `CanonicalRenderSql.instanceCanon` (85), `VerdictSql.kindedRows` (150), `typedEquals` (53), `typedInstances` (36).
  - The design's line "Scalars keeps one dispatch line each (this also keeps the per-method limit)" therefore protects
    nothing measured today.
- **Measured methods near the limit that 6.4 touches:**
  - `Lowerer.scalarValueTailArms` **225** (`:4240-4464`; holds the InstanceEquality arm `:4307-4318`);
  - `Lowerer.scalarStructural` **249** (`:3864`);
  - `Lowerer.scalarInner` 228 (`:3632`); `Lowerer.scalarRelationalArms` 70 (`:4117`; holds the disjoint arm).
  - Replacing the InstanceEquality arm with one `Equalities` dispatch line frees about 12 lines.
- Others measured: `Scalars.pureToString` 124, `TaggedLists.root` 60, `TaggedLists.contains` 51 (`@Nullable` param only,
  so measured), `TaggedLists.sort` 38, `TaggedLists.sameWrittenLists` 38 (annotated, not measured),
  `NullSemantics.equalNullArms` 44, `InstanceEquality.claims` 32, `InstanceEquality.staticallyDisjoint` 30,
  `CanonicalRenderSql.wrapWithCanon` 213, `wrapTdsCanon` 185, `VerdictSql.batch` 100.
- Other rules in the same class:
  - `deadPrivateMethodsOnlyShrink` (`DEAD_PRIVATE_METHODS = 0`, `:544`): a private method left unread by the move
    trips it;
  - `classNamesAreNeverLogic` (`:552`, 0): 6.1 hit it once.

### E3. CarrierPurityRatchetTest (`T/CarrierPurityRatchetTest.java:41-235`)

- Pins: `SqlFn\.LIST_` 172 (`:222`), `SqlFn\.UNNEST` 13 (`:232`), `new SqlExpr\.ArrayLit\(` 42 (`:116`),
  `new SqlExpr\.OrderedListAgg\(` 3 (`:127`).
- HEAD's raw counts appear to be 173, 14, 42 and 4 (see the state notes).
- InstanceEquality holds 5 `LIST_` sites, CanonicalRenderSql 6, TaggedLists 27 (3 UNNEST, 3 OrderedListAgg).
- A product rows-of-a-list built with `UNNEST` or `LIST_*` in lowering grows these pins, and the rule bans new direct
  carrier emission ("lower to a semantic node and add strategy rules instead"). So the list compare must stay ONE
  semantic node (`SAME_IN_*` over the two lists) and leave the per-dialect rows to the dialects: invariant 3 and this
  ratchet point the same way.

### E4. ArchitectureTest (`T/ArchitectureTest.java`)

- `theDatabaseJudgeReachesNoEdge` (`:5265-5285`): `lowering.VerdictSql` (and the testplan judge classes) must not
  TRANSITIVELY reach `com.legend.edge..` or `testplan.PlanStyleFacets`, or call `PlanStyle.facet`/`twoForm`. If
  VerdictSql later calls the new owner (6.5), the owner must not reach a styled path. Today Scalars and TaggedLists
  take a `Styled` (e.g. `TaggedLists.print`, `json`). I did not run the transitive closure. Keep the owner's leaf and
  compare code free of Scalars and Styled.
- `sqlLayerIsStandalone` / `sqlLayerIsFullyStandalone`: SameValuesForm and any new H2 arm stay in `sql/`.
- `onlyCheckedSitesMakeATemporalLiteral` (`TEMPORAL_LITERAL_MAKERS` `:4709-`) lists `com.legend.lowering.Scalars`: a
  temporal literal minted in the new owner needs an entry.
- **There is no rule today that names who mints `SAME_IN_*`.** The design adds one ("only Equalities, and VerdictSql
  until 5c-2").

### E5. SqlFnSortedTest (`T/SqlFnSortedTest.java`)

- `PRODUCT` (`:81-`) includes `SAME_IN_ANY_ORDER`, `SAME_IN_ORDER` (`:119`), `IS_DISTINCT_FROM` (`:94`) and
  `NULL_SAFE_EQUAL` (`:107`). Their writers are **not pinned** (they are not in `BOTH_WRITERS`), so a new product writer
  of `SAME_IN_*` trips nothing here.
- `BOTH_WRITERS` (`:197-`) pins the product writers of functions the edge also writes. Each entry naming an equality
  class:
  - `EQUAL` (`:267-291`): InstanceEquality, NullSemantics, Scalars, TaggedLists, VerdictSql, CanonicalRenderSql,
    MixedEncoding, Lowerer, SameValuesForm, ...
  - `IN` (`:319-321`): Scalars, TaggedLists.
  - `AND`, `COALESCE` (InstanceEquality, ...), `IS_NULL`, `IS_NOT_NULL`, `LIST_TRANSFORM` (InstanceEquality,
    CanonicalRenderSql, ...), `NOT`, `OR`, `GREATER`.
  - `BOTH_AGG` `COUNT` (`:703-712`), whose writers include VerdictSql and SameValuesForm.
- A new `com.legend.lowering.Equalities` writing any of these needs a dated entry in each. Deleting InstanceEquality
  removes it from `EQUAL`, `COALESCE` and `LIST_TRANSFORM` (a shrink, under the standing word for cleanups).

### E6. Smaller guards

- `T/CompatConstantPoolTest.java:72`: `"com.legend.lowering.InstanceEquality | TDSNull"`, from `userClass`'s
  `PlatformTypes.TDS_NULL_FQN`. Moving `userClass` changes that key.
- `T/RaiseTextGuardTest.java:47` (`CanonicalRenderSql.java`, 1: `UNTRACKED`) and `:51` (`Scalars.java`, 1). Moving
  `trackedId` or raising a new text elsewhere moves these.
- `T/TestLaneOrderGuardrailTest.java:104` names `CanonicalRenderSql.java`.
- GuardNamesExistTest reads names that guards hand in literally. A deleted class still named in a guard trips it.

### E7. Lean-SQL baseline (`core/src/test/resources/lean-sql/baseline.tsv`)

Read by `T/leansql/LeanSqlBaselineTest.java`. Growth fails; a shrink above 1% (`LeanSqlBaseline.RESHRINK`) asks for a
re-pin. Families are keyed by the outermost function. Current pins (statements / characters):

| Set, family | DuckDB | H2 | SQLite | Postgres | MySQL |
|---|---|---|---|---|---|
| channel_a `boolean::equal_Any_MANY__Any_MANY` | 17 / 924 | 17 / 1,329 (1 refused) | 17 / 1,612 | 17 / 1,032 | 17 / 1,919 |
| channel_a `boolean::eq_Any_1__Any_1` | 4 / 122 | 4 / 202 | 4 / 112 | 4 / 148 | 4 / 137 |
| channel_a `collection::contains_Any_MANY__Any_1` | 11 / 2,589 | 11 / 34,836 | 11 / 19,938 | 11 / 17,420 | 11 / 32,300 |
| channel_a `collection::in_Any_1__Any_MANY` | 2 / 331 | 2 / 3,164 | 2 / 1,678 | 2 / 1,580 | 2 / 2,748 |
| channel_a `collection::indexOf_T_MANY__T_1` | 1 / 106 | **0 / 0 (1 refused)** | 1 / 385 | 1 / 292 | 1 / 746 |
| channel_a `collection::removeDuplicates_T_MANY__T_MANY` | 4 / 1,837 | 3 / 1,768 (1 refused) | 4 / 16,529 | 4 / 6,184 | 4 / 28,801 |
| channel_a `removeDuplicatesBy` | 4 / 3,048 | 4 / 17,225 | 4 / 10,239 | 4 / 10,045 | 4 / 15,267 |
| core `boolean::equal` | 8 / 9,567 | n/a | 6 / 24,280 | 1 / 15,016 | 1 / 51,539 |
| core `contains` / `indexOf` / `isDistinct` (DuckDB) | 4 / 3,106; 2 / 1,056; 2 / 1,978 | | | | |

Directions to expect:

- Typed keys in place of the canon's JSON text should SHRINK the instance-equality statements: `eq` and `equal` in
  core; a struct of leaves against `json_object` plus casts.
- `[] == computed []` through `emptiness` adds a `COALESCE(LIST_LENGTH(x),0) = 0`, a few characters.
- **List `==` as `SAME_IN_*` GROWS every non-DuckDB database's `equal` family**, because they all use the portable rows
  form today. On DuckDB `IS NOT DISTINCT FROM` is about the size of `=`.
- H2's `indexOf` goes from refused to written: "growth" from 0 statements, which the baseline counts.
- A statement that was refused and becomes planned also grows the totals. Each such growth needs the user's word, per
  the rules.

---

## F. Risks and open questions for the owner (each with options and cost)

1. **The rows of a list INSIDE an expression, per database** (the hardest part).
   - The judge's `SAME_IN_*` reads statement-level CTE rows. The product's list is a value inside a row, so its rows
     must be correlated. Correlation is not uniform: H2 cannot correlate into a nested derived table; ClickHouse's
     correlated subqueries are limited (I have not checked the version lite runs); BigQuery arrays have no `=`; DuckDB
     needs nothing.
   - Options:
     - (a) The product mints `SAME_IN_*` with the two lists and the row subqueries built by lowering. Every
       non-DuckDB dialect then needs a working correlated explosion. This touches the carrier ratchet (E3) and invariant
       3, about 11 dialects, and needs a lane round each.
     - (b) The product mints a two-argument list form, and each dialect writes its own decomposition: DuckDB `IS NOT
       DISTINCT FROM`; Postgres and Trino native arrays (not probed here); H2 `SYSTEM_RANGE` with `NOT EXISTS` (C);
       SQLite `json_each`; BigQuery `UNNEST WITH OFFSET`; and so on.
     - (c) Do (b) for DuckDB, H2 and SQLite first, and keep `=` with its known differences on the cloud lanes, recorded
       as a register row until a lane round.
   - Recommendation: (b), with (c) as the landing order, because invariant 3 puts decompositions in the dialect.
2. **SQL size.** List `==` grows wherever the database has no list compare (E7). Measure per family on all 11
   databases before landing, as 6.1 and 6.2 did, and bring the growth to the user.
3. **legend-engine's door.** legend-engine refuses `==` over many values in its SQL generation:
   `pureToSQLQuery.pure:8379-8383` (`assert($leftParam.multiplicity->hasUpperBound() && ... <= 1, |'Left multiplicity
   error. Please use \'exists\' instead of \'==\' ...')`, the pinned checkout under `/root/.cache/bazel/.../core_relational/
   relational/pureToSQLQuery/pureToSQLQuery.pure`).
   - The corpus, which passes on legend-engine, so holds no list `==` in a store query, should therefore not move.
     `assertError` tests that expect that message would.
   - Options:
     - (a) The engine door refuses a list `==` with legend-engine's sentence: a door rewrite (switch rule) and an
       EdgeOnlyMembers entry.
     - (b) Do nothing and record it.
   - The design says "measured first, lands only if neutral". Measure with both corpora (`//spec:corpus_duckdb`,
     `//spec:corpus_h2`) after each list piece.
4. **`[] == x`, and one optional side in value position.**
   - Today the core keeps legend-engine's bare `=` (`Scalars.java:109-113`; `EqualityWorldsConformanceTest:115`
     pins NULL). Tenet: "the core follows Pure"; `==` is a total `Boolean[1]`.
   - Options:
     - (a) The product is total (`NULL_SAFE_EQUAL` wherever a side may be empty), and the engine edition keeps `=` by
       a typer rewrite. The edition is the only switch, typer-only. Its cost: corpus SQL moves; "IS NOT DISTINCT FROM
       appears in no golden", but goldens are not promised (CP-4).
     - (b) Keep `=` and record it.
   - This needs the user's ruling. It is the largest answer change hidden in 6.4.
5. **Keyless identity where it is not tracked** (a store row, a fold's reducer, a let read in a nested lambda:
   `CanonicalRenderSql.UNTRACKED`).
   - Both judges and the product raise by name today. Options: keep raising (no change); or give store rows an
     identity (their keys or row id), which is a model change and out of scope.
   - Recommendation: keep it, and say so in the slice's record.
6. **Two instances of different classes with equal keys.**
   - Pure's `equal` (EqualityUtilities) requires the classifier to match, so they are never equal.
   - Unrelated static classes are a compile error already. Related ones are decided at run time, by `__type`.
   - Today the product folds `false` on the static FQNs (`InstanceEquality.java:151-158`), which is wrong for an upcast
     alias. Its canon uses the static `_type`, which is wrong for two subclasses.
   - Fix: read `__type` on both sides (the object layout carries it everywhere: `ClassLayouts.java:123`, `:134-145`).
     Refuse by name where a struct has no `__type` (a polymorphic store row: the 5b-3 risk).
   - This is 6.3's "run-time class first". Decide whether 6.4 absorbs it.
7. **`is` native.**
   - Pure: `native function meta::pure::functions::boolean::is(left:Any[1], right:Any[1]):Boolean[1]`, "Identity
     (i.e., pointer equality)", `<<PCT.function, PCT.platformOnly>>` (`legend-pure-m3-core/.../essential/boolean/
     equality/is.pure`; it is not in lite's `builtin/Pure.java` today). Its PCT tests assert `is(TestEnum1.FIRST, 1)`
     false and `is($side1, $side2)` false for content-equal instances.
   - Questions:
     - (a) Primitives: value equality, as lite has no pointers, or a refusal?
     - (b) Is `is` a comparison of the strict rule (add it to `EqualityClasses.COMPARISONS`, so `is(E.A, 1)` is a
       compile error in lite and false at legend-pure's door), or folded false by the types?
     - (c) Instances: `trackedId` against `trackedId` (identity) is clear.
   - The answer changes PCT under the `lite` edition.
8. **To-many keys.** VerdictSql's typed leaves refuse them (`:460`). Options:
   - (a) Compare a to-many key as an ordered list of nested leaf structs (`LIST_TRANSFORM` to structs on DuckDB; the
     H2 row form). This adds new judge coverage that 6.5 then inherits.
   - (b) Keep the canon text for to-many keys only. This contradicts "the canon-text equality deleted", and the text
     keeps the Decimal scale.
   - (c) Refuse by name. This regresses `testEqualNonPrimitive` on DuckDB.
   - Recommendation: (a).
9. **"assert(cond, msg) inside forAll" (`AssertVerdicts.java:1563-1567`, "non-literal message expressions are not
   modeled").** The design lists it under 5c-1, but the brief says no assert changes in 6.4. Recommendation: move it to
   6.5.
10. **Emptiness on H2 for the first slice's waiting family** (plan lines 834-837). `isEmpty`/`size` of a computed list
    needs H2's `LIST_FILTER` row form at least. Decide whether it is in 6.4.
11. **JSON carriers' element reads.** On H2, SQLite and MariaDB an element read from a JSON list must be cast to its
    typed value before it is compared (the Decimal scale, the date-time text). SQLite's one date-time text form is
    step 6's separate item (`START_HERE.md` §5). Until it lands, SQLite's instant compare inside lists stays text.
12. **Ordering against equality.** Lists "in any order" (a multiset) are not needed by `==` itself. Pure's `==` over
    lists is ordered. `SAME_IN_ANY_ORDER` is only for `assertSameElements` (6.5) and maps. Don't build the multiset row
    form on H2 in 6.4 unless 6.5 needs it then.
13. **Maps.** DuckDB's `MAP =` order sensitivity is not verified. Probe `map([pair(1,'a'),pair(2,'b')]) == ...` in both
    orders before deciding whether the product compares sorted entries.
14. **The ratchet at HEAD** (state notes) must be green before 6.4's own moves are measured.

---

## G. A proposed split into the smallest landable pieces, in order

Each piece: the fast check (`CodeShapeGuardrailTest`, `CarrierPurityRatchetTest`, `SqlFnSortedTest`,
`JavaEvalLedgerTest`, `ArchitectureTest`, `LeanSqlBaselineTest`), then its own proof, then the cadence's round at the
end of the slice. The engine door is measured on both corpora after 6.4d.

| # | Piece | Proof (narrowest) | Deletes / moves | Answers that move |
|---|---|---|---|---|
| 6.4a | **One owner, no behaviour change.** A new `M/lowering/Equalities.java` takes: the `==` rule body (`Scalars.java:116-191`, so the lambda becomes `Equalities::equal`); `taggedEquality`/`sameTaggedLists`/`everyElementWritten`/`variantList`/`partialPrecision`/`isFullPrecisionDate` (`Scalars.java:3766-3781`, `3853-3945`); `NullSemantics.equalNullArms` (`:107-150`); the equality decisions of `contains`, `in` and `indexOf` (each arm calls `Equalities.member(...)`/`position(...)`); and InstanceEquality, whole (the Lowerer's two arms become `Equalities` calls). The lean baseline must be identical, statement for statement | `InstanceIdentityTest`, `TaggedListTest`, `NullSemanticsTest`; the baseline unchanged | InstanceEquality.java (moved). BOTH_WRITERS and CompatConstantPool keys re-pointed (dated) | none |
| 6.4b | **Empties by their size.** A literal `[]` against a to-many side: `Scalars.emptiness` (`:2909`) in place of `IS_NULL` (A1 #1). To-one keeps `IS NULL` | a new pin `[] == [1,2]->filter(x\|$x>5)` true on DuckDB, H2 and SQLite; `EqualityWorldsConformanceTest` | the `IS_NULL` arm | `[] == computed []` false to true (H2: error to true). Not `[]->head() == []->head()` (that is F4) |
| 6.4c | **Instances by typed keys.** A keyed class: a struct, or an `AND`, of the typed leaves `__type` + keys (nested in place), compared typed. Pull `leaves`/`leafClass`/`fieldType`/`Leaf`/`pairedLeaves` out of VerdictSql into the owner, so VerdictSql calls it: an EVICT shrink. Keyless: `trackedId`. The run-time class by `__type` (F6). To-many keys as an ordered list of leaf structs (F8). `contains`/`in`/`indexOf`/`removeDuplicates`/`isDistinct` over `LIST_TRANSFORM(xs, x -> leafStruct(x))`: the same carrier shape as today with typed structs, not text | `InstanceIdentityTest` plus new pins (`^K(d=1.0D)==^K(d=1.00D)` true; a date-time key at two precisions true; an upcast alias equal); `AssertBarTest.instancesAreComparedByTheirKeysOrTheirIdentity`; PCT `testEqualNonPrimitive` on DuckDB | `CanonicalRenderSql.instanceEqualityCanon`; the canon-text path of the product; the static-FQN false fold; **the unclaimed refusal at `Lowerer.java:4307-4318`** (each remaining refusal says its own reason) | the Decimal and date-time keys; the run-time class; the related-class collections that were refused |
| 6.4d | **Lists in order as one node.** A list against a list, or a scalar against a list: `SAME_IN_ORDER(a, b, …)` minted by the owner (empty normalised by rows or length). DuckDB as today (`sameLists`). For the other dialects, F1 option (b): DuckDB, H2 and SQLite first. Maps by sorted entries (`CanonicalRenderSql.sortedEntries`) if the probe in F13 says so. Nested `List<T>` | a new `ProductListEqualityTest` over `TestBackends.inProcess()` (written and computed lists, a scalar against a list, Decimals at two scales, nested lists); lean baseline per family, the growth brought to the user; the engine-door measurement (F3) | the list-against-list `=` through `equalNullArms` | `[1.0D] == [1.00D]` on H2 and SQLite; a scalar against a list (DuckDB error to answer); computed lists on H2 |
| 6.4e | **H2's row forms for membership and position.** `LIST_POSITION`, `Membership` over a computed list, `ALL_DISTINCT`, the first-seen dedup, `exists`/`forAll`, as `SYSTEM_RANGE` forms in `H2.java` (C), with typed element casts | the H2 PCT pins leave: Essential `:45`, `:68`; Standard `:50`; Grammar `:41-42`. Lean H2 rows | H2's listCall refusals for those functions | H2 answers where it refused |
| 6.4f | **Grids.** A relation against a relation: column names and kinds equal (`false` at planning when they differ, a fact of the types), then rows in order (`SAME_IN_ORDER` over row structs), the typed cells as `typedCells` reads them | a new product grid test mirroring `AssertBarTest.java:1218-1250` (order, names, `1` against `1.0`, Float cells) | the generic `EQUAL` of relations | grids: column names and row order now count |
| 6.4g | **`is`.** Declare it in `M/builtin/Pure.java` (the signature verbatim from is.pure), with a rule in the owner: identity for instances (`trackedId`), equality for primitives and enumerations; the cross-type answer per F7's ruling | PCT `boolean::tests::is::*` on the DuckDB LITE lane; one core test | none | PCT `is` bodies answer |
| 6.4h | **The guard.** An ArchitectureTest rule: only `lowering.Equalities` (and `lowering.VerdictSql` until 6.5) mint `SAME_IN_ORDER`/`SAME_IN_ANY_ORDER`; optionally `NULL_SAFE_EQUAL` too | the rule itself | | none |

Deferred, recommended out of 6.4:

- `assert(cond, msg)` inside `forAll`: to 6.5 (F9).
- The value-position NULL against a Boolean question: its own piece after the user's ruling (F4).
- H2's root list egress (`UNNEST` at the root; the removeDuplicates PCT pins `:70-73`): the lean-SQL program, or 6.5 if
  its asserts need it.

What 6.5 then needs from 6.4 (design: "what must be true before 5c-2"):

- lists in order (6.4d) and a multiset (only where `assertSameElements` needs it: build it with 6.5, F12);
- empties (6.4b);
- typed instance leaves (6.4c);
- maps and nested lists (6.4d);
- grids (6.4f);
- tagged and JSON kinds: already in TaggedLists after 6.2, though JSON-carried Any compares JSON (6.2's remainder:
  check it).
