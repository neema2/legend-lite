# Step 6.2 map: run-time types — read-only research

Tree: `/home/user/ll-edit` (branch `claude/edit-ahead`, HEAD `e1588334`, slice 6.1 in it). Paths are under
`core/src/main/java/com/legend/` (M) or `core/src/test/java/com/legend/` (T) unless written out. Nothing was built,
run or edited. "Did not trace" means exactly that.

## 0. Corrections and facts to know before typing

1. **The design's line numbers have moved.** The design (b3-step5bc-design.md, 5b-2) cites TaggedLists :400 and :494,
   Scalars :2338 and :2699, Equality.sorted :288, TaggedListTest :279-291. Today: the two run-time refusals are
   TaggedLists.java **:478-483** and **:575-580**; Scalars' Any arms are **:2347-2351** (contains) and **:2709-2731**
   (in); `Equality.sorted` is still **:288-293**; the crossing test is TaggedListTest **:337-350**.
2. **No test pins the run-time refusal.** `grep "runtime value names"` finds no test anywhere (core, pct, spec). Item 1
   needs new tests; nothing flips.
3. **The run-time refusal also fires for a class instance needle.** `MixedEncoding.kindOf` (:415-454) returns null for
   Number, Any, a non-literal abstract Date, a class, a type variable. So `[1, 'a']->contains(^P())` is refused with
   the "kind only its runtime value names" text. Under the rule an instance never equals a primitive: that is a static
   `false` (a fact fixed by the types), not a run-time kind.
4. **`kindOf`'s DateTime comment (:440-443) is stale** after 6.1 ("Pure's date equality reads the subsecond TEXT ...
   .000 is not none"); it still returns `DateTime(n)`, which `family()` now folds. Fix the comment when touching it.
5. **The SQL type fact is lite's own MIR claim, made at planning** (`SqlExpr.type()`, M/sql/TypeFact.java,
   M/sql/SqlTyping.java). "Its kind from its SQL type" needs no SQL at run time and no dialect function. It is only as
   good as the fact. Section A lists where the fact is Unknown and, worse, where it is Typed but is a PROMOTION, not
   the value's run-time kind.
6. **Item 6d's site is not on this branch.** The "Float row" (`EngineTable.FLOAT_TO_STRING`, `NamedRule.FLOAT_TEXT_BY_CAST`)
   lives on `origin/claude/edition-step8` (EngineTable.java :109-139 and `rows(...)` :275-290 there), not on
   `claude/edit-ahead`. 6d cannot start until step 8's slice lands.
7. **SqlFnSortedTest may already be red on this branch** (did not run it). `TaggedLists.contains` writes `SqlFn.IN`
   (:607, since 5d3144c8), IN is in `BOTH_WRITERS`, and IN's product set is `{CalendarAgg, Scalars, EnumValueTables,
   FloatShortestText}` (no TaggedLists) on both HEAD and `origin/claude/batch-6-1`. Run
   `//core:...SqlFnSortedTest` once before starting; if red, the fix is one dated entry, unrelated to 6.2.

## A. Where the lowering can and cannot read a value's kind from its SQL type

The type fact of the value operand (`value.type()` in `TaggedLists.sameElement`, `needle.type()` in `contains`):

| Shape (typed Number / Any / abstract Date) | Fact today | Is the fact the run-time kind? |
|---|---|---|
| a relation column (`SqlExpr.Column`, OutputCol's SqlType: BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP, VARCHAR, BOOLEAN) | Typed | **yes** — the clean case |
| an upcast of a static-kind value (`1->cast(@Number)`, `'a'->cast(@Any)`) | Typed, the source's | yes; but the source's static kind is better (read `TypedCast.source()`). Did not trace `CastPolicy.lower`'s arm for a primitive-to-Any cast |
| a mixed **Number-LUB `if`** (`if($c, 1, 1.5)`): `MixedEncoding.lubCase` :738-749 emits a raw CASE (only an **Any**-LUB if goes to the variant carrier) | Typed DOUBLE (`SqlTyping.caseType` :1031 → `uniform` :1517 → `branchPromote`: BIGINT+DOUBLE→DOUBLE) | **NO** — the Integer branch would read as a Float |
| a mixed **Date-LUB `if`** (a StrictDate and a DateTime branch) | Typed TIMESTAMP (branchPromote: DATE+TIMESTAMP→TIMESTAMP) | **NO** — a StrictDate would read as a DateTime |
| `COALESCE`/`ArrayLit` of mixed numeric kinds (same `uniform` rule) | promoted | **NO** |
| a computed (non-literal) Number-LUB collection: Lowerer.java :3756 `case TypedCollection c` plain array (only an all-literal mix gets the variant array, :3741-3754) | ARRAY(promoted) | **NO** — `[$x.i, $x.f]` holds 1 as 1.0 |
| arithmetic over Numbers (`SqlTyping.arithType` :829-950): BIGINT±BIGINT BIGINT, any DOUBLE → DOUBLE | Typed | mostly Pure's promotion; **any DECIMAL operand → UNKNOWN** (`decimalArith`, "version formulas"); `INT_DIVIDE` with a decimal UNKNOWN; `ROUND`/`ROUND_RAW` of a decimal UNKNOWN. Whether Pure's Decimal+Float is Float as SQL says: did not verify |
| a query parameter declared Number, Float, Decimal or Date (`SqlExpr.PlanParam`, SqlExpr.java :804-817: "a Float's, a Decimal's, a Number's and a Date's fact is UNKNOWN"; `DeclaredKind.NUMBER` binds "as a Decimal") | **UNKNOWN** | no answer: a Number parameter holding 1 is bound as a DECIMAL. Refuse by name, or bind its kind as a second parameter (a tag) |
| `adjust` (M/lowering/DateShifts.java :143-205) over an operand typed abstract Date | Typed TIMESTAMP (`ADD_INTERVAL` → T_TIMESTAMP, SqlTyping :520-529); only a STATIC StrictDate with a coarse unit is cast back to DATE (:198-204) | **NO** for a Date-typed value holding a StrictDate (Pure answers a StrictDate) |
| a partial date (`TEMPORAL_TEXT` carrier) | Typed TEMPORAL_TEXT | half: Year or YearMonth is not in the type. Refuse unless a literal |
| a JSON-carried Any (`SqlType.Scalar.JSON`) | Typed JSON | the kind is the JSON value's, read at run time (item 2) |
| a `Tagged` (a mixed selection's winner) | Typed TAGGED | already handled by `containsElement` / `sameElement(Tagged, Tagged)` |
| list functions `LIST_SUM/AVG/MEDIAN/MODE` on an empty arg, `MAP_*` over unknown maps (SqlTyping :430-472) | UNKNOWN | refuse |

**Did not trace:** whether G½'s inliner leaves a function's parameter typed as its declared `Number` or as the
argument's own static type after substitution. That decides how often a `Number` operand reaches the lowering at
all. The 5b-0 census in the design ("census every comparison whose side `EqualityClasses.of` leaves to run time") was
never run; run it first (step 0 below).

**Proposed reading rule** (for the owner to confirm): take the kind from the SQL type only when the value is a LEAF
whose fact is its own — a column, a literal, a cast of a static-kind value, a parameter of a static kind — and refuse
by name for a CASE, COALESCE, array, arithmetic or `adjust` whose static type is Number, Any or Date and whose
branches or operands differ in static kind. Or, for those, carry a tag (build a `SqlExpr.Tagged` from the branches,
as `MixedEncoding.encodeMixed` :594-656 does for `max`/`min`). The tag is exact; the refusal is cheaper.

## B. Item by item

### B1. A tagged value or list against a value of no static kind (Number, Any, abstract Date)

**Sites (5).**
- `TaggedLists.sameElement(Tagged x, String kind, SqlExpr value, Type valueType)` :478-496. The refusal:
  ```java
  if (kind == null) {
      throw new com.legend.error.NotImplementedException("a tagged value compared with a value of type "
              + valueType + ", whose kind only its runtime value names: not lowered");
  }
  ```
  Callers: `Scalars.taggedEquality` :3857-3862 (`TaggedLists.sameElement(x, MixedEncoding.kindOf(s1), b, s1.info().type())`
  and its mirror), reached from the `==`/`eq` rule (Scalars :114-160, the static block from :103) when a side lowered
  to `SqlExpr.Tagged`. Also `sameListOfKind` :515-531 calls it with a non-null kind.
- `TaggedLists.contains(TaggedList tl, String kind, SqlExpr needle, Type needleType)` :575-629. The refusal :577-580
  (same text, "a mixed list's membership of a value of type ..."). Callers: the `contains` rule Scalars :2337-2344
  (`TaggedLists.contains(mixedColl, MixedEncoding.kindOf(n.args().get(1)), args.get(1), n.args().get(1).info().type())`)
  and the `in` rule Scalars :2722-2728 (needle `raw`, after `CastPolicy.comparisonWireOperand`).
- `MixedEncoding.kindOf(TypedSpec)` :415-454 — where "no static kind" is decided (null for Number, Any, Date
  non-literal, class, type var; `enumerationOf` for an enum).
- `Scalars.taggedEquality` :3870-3911, the list-against-list arm: `kind = otherSpec instanceof TypedCollection ? null :
  MixedEncoding.kindOf(otherSpec)` (:3877); a `Number[*]` or `Any[*]` variable list gets kind null and, unless on the
  variant carrier, is refused at :3908-3909 ("a mixed list compared with a list whose elements' kind is not static and
  that is not on the variant carrier: not lowered").
- The new code: a helper `MixedEncoding.kindOfSqlType(SqlExpr)` (or in TaggedLists) mapping
  BIGINT/INTEGER/HUGEINT→Integer, DOUBLE→Float, `SqlType.Decimal`→Decimal, DATE→StrictDate, TIMESTAMP→DateTime,
  VARCHAR→String, BOOLEAN→Boolean, everything else (JSON, TEMPORAL_TEXT, UNKNOWN, Bottom, ARRAY, STRUCT) → null.
  `Type.kindOfSqlType` already exists (used by CanonicalRenderSql.taggedLeaf :840-870; did not read its body) — check
  whether it can be reused rather than adding a second table. A class-typed needle (item 0.3) → `BoolLit(false)`.

**Tests.** None pin the refusal (0.2). New: `TaggedListTest` cases over a Number column, a cast to Number, an Any
upcast, and the promotion shapes refused by name.

### B2. An Any carried as JSON compares by its JSON kind

**Sites (6).**
- `Scalars.taggedEquality` :3899-3906: a mixed list against a variant-carried list:
  `SqlExpr.Call.of(SqlFn.EQUAL, la != null ? TaggedLists.asVariantList(la) : a, lb != null ? TaggedLists.asVariantList(lb) : b)`
  — JSON equality of two arrays: by the JSON text/structure the database keeps.
- `TaggedLists.asVariantList` :244-254 (each channel TO_VARIANT'd).
- The `==` fallthrough, Scalars :161-180: `VariantShapes.alignLiteralToJson` (M/lowering/VariantShapes.java :31-41,
  wraps a literal side in TO_VARIANT when the other side is JSON) then `NullSemantics.equalNullArms`: **a JSON value
  against a JSON literal by `=`.**
- `contains` Any arm, Scalars :2347-2351: `new SqlExpr.Membership(SqlExpr.Call.of(SqlFn.TO_VARIANT, args.get(1)), args.get(0))`.
- `in` Any arm, Scalars :2709-2719 (`collVariant`) and :2729-2731 (`TO_VARIANT(raw)`).
- The judge already reads JSON kinds the right way, reusable as the model: `VerdictSql.JSON_KINDS` :646-647
  (String VARCHAR, Integer BIGINT, Float DOUBLE, Boolean BOOLEAN), `jsonKindIs` :650-663 (Integer is
  `JsonKind.INTEGER` or `UNSIGNED_INTEGER`), `jsonValue` :667-672, and the CASE over `JSON_TYPE` :862-879 (an object
  or array raises `JUDGED_TREE_NOT_TYPED`). Note VerdictSql's pin (2263, exact) forbids moving code INTO it; copy the
  idea into the lowering or move the helpers OUT of VerdictSql (a shrink there).

**Why JSON equality is wrong today** (from M/lowering/Lowerer.java :3696-3702's own comment and
T/lowering/AnyLiteralByteDecidabilityTest): `to_json` of a date and of its string are the same bytes; a Decimal and a
Float of the same digits are the same JSON number; `1.5D` and `1.50D` differ as JSON text (against S139). So "by JSON
kind" can only decide String, Integer, Float, Boolean, and only where no Decimal and no date can be inside.

**"May hold a Decimal or a date" is a static question** over the typed tree that produced the JSON: the branch types
of `lubCase` (Lowerer :2285, :4073), the element types of a variant array (Lowerer :3726-3735,
`MixedEncoding.variantElement` :808), a row's cell types (`TdsDesugars.rowCells` :788-813, columns known). Where the
producer is not visible (a function parameter typed Any whose argument the lowering does not see) → refuse. Did not
trace a helper that answers it today; it is new code.

**Dialects** (JSON_TYPE, each mapped to DuckDB's names):

| Dialect | Rendering | Risk for Integer vs Float |
|---|---|---|
| DuckDB | `json_type(x)` (DuckDb.java :732) | none known |
| H2 | a CASE over `IS JSON OBJECT/ARRAY`, `'null'`, `'true'/'false'`, `LIKE '"%'`, `REGEXP_LIKE(.., '^-?[0-9]+$')` → INTEGER else FLOAT (H2.java :1423-1440) | by the TEXT: a Float written `1.0` reads Float; one written `1` would read Integer (did not probe H2's JSON of a DOUBLE) |
| SQLite | `CASE json_type(x) WHEN 'integer' ... WHEN 'real' ...` (SQLite.java :1894-1897) | `json(1.0)` keeps real (did not probe) |
| MariaDB | `CASE JSON_TYPE(x) WHEN 'INTEGER' ... WHEN 'DOUBLE' ...` (MariaDb.java :2109-2112); no DECIMAL arm | a DECIMAL in a JSON doc: JSON_TYPE says 'DECIMAL' on MariaDB → no arm → NULL (did not probe) |
| ClickHouse | `multiIf(... JSONType(x) IN ('Int64','UInt64') → 'BIGINT' ..., 'DOUBLE')` (ClickHouse.java :2132-2136) | ClickHouse may write a whole Float64 as `1` in JSON → read Integer (not probed) |
| Snowflake | `DECODE(TYPEOF(TO_VARIANT(x)), 'INTEGER','BIGINT', 'DECIMAL','DOUBLE', 'DOUBLE','DOUBLE', ...)` (Snowflake.java :1626-1629) | **a Decimal reads as a Float** — confirms "refuse where a Decimal may be inside" |
| BigQuery | `CASE JSON_TYPE(x) ... WHEN 'number' THEN CASE WHEN REGEXP_CONTAINS(TO_JSON_STRING(x), '^-?[0-9]+$') THEN 'BIGINT' ELSE 'DOUBLE'` (BigQuery.java :1861-1868) | by the text; BigQuery may print a whole FLOAT64 as `1` (not probed) |

**Tests.** `T/lowering/AnyLiteralByteDecidabilityTest` (raw DuckDB facts, must not move). No product test pins JSON
equality of a run-time Any that I found (searched `to_json`/`TO_VARIANT` in T: SeamBenchmarkDuckDbTest,
SnowflakeDialectTest, NullVariantCastTest, StoredReadsTest, PostgresToPureTest, FoldCheckerTest, LowerRelationDuckDbTest,
VariantElementLawTest, CarrierPurityRatchetTest — none is an equality-by-kind pin; did not read each fully).

### B3. Scalars' Any arms read a literal Any list as a tagged list, not TO_VARIANT

**Sites (5).**
- Lowerer.java :3700-3717 — a literal Any collection becomes a TaggedList only when
  `!cellSlots && CollectionLanes.valueLane(c) && !c.elements().isEmpty()` and `MixedEncoding.taggedList` returns
  non-null. `CollectionLanes.valueLane` (M/lowering/CollectionLanes.java :106-200) is **false for any element that is a
  `TypedVariable`, `TypedPropertyAccess`, a relation**: so `[$s, 1]`, `[$x.name, 1]` take the variant carrier
  (TaggedListTest :153-156 says so: "such a list is compared on the variant lane today, its parameter bound once").
  `cellSlots` (`c.rowCells()`, a row's `$r.values`) also keeps the variant carrier.
- `MixedEncoding.taggedList` :112-237 returns null for an element of no tagged kind (an instance, a computed Any,
  a partial-date column `partialComparable(e, x) == null` :160-163, a rank-less element :172-175).
- Scalars :2347-2351 (`contains`) and :2709-2731 (`in`), the TO_VARIANT membership the slice replaces.
- `MixedEncoding.columnChannel` :463-500 already gives a row's typed cell its channel (used by ValueCollections'
  row-major flatten), so the row-cells case can be tagged with existing code.

**Open:** valueLane exists for "pure raising semantics" (its javadoc :105). Tagging a list holding a variable needs a
ruling that its elements' raising is unchanged (an element that may be empty already compacts through
`presentPositions`, TaggedLists :205-218). Did not trace why valueLane excludes variables.

**Tests.** TaggedListTest :153-156 (`'a'->in([$s, 1])`, asserts one `?`): stays true if the tagged form binds the
parameter once; check the IN-of-one rule (TaggedLists :604-607, "IN over one value that is a parameter is read as a
LIST parameter by every dialect and refused").

### B4. The rank's tie-break by type, and the host's

**Product sites (8).**
- `MixedEncoding.rankedComparableOf` :287-347: struct `{g, n, d, s, p}`. Numbers: `group = 0; n = CAST(x AS DOUBLE)`,
  `p = NULL`. Dates: `p` is the date type, clamped at HOUR since 6.1 (:337-338):
  ```java
  SqlExpr p = group == 1 ? new SqlExpr.IntLit(Math.min(precisionOf(t, e).ordinal(),
          PureDateLiteral.Precision.HOUR.ordinal())) : new SqlExpr.NullLit();
  ```
  So dates already break ties by type (Year 0 < YearMonth 1 < StrictDate 2 < DateTime 3). **Numbers have no tie-break:**
  1 and 1.0 tie. The least change: give `p` the number kind for group 0 (Integer 0, Float 1, Decimal 2 — Q4's
  recommendation), so the struct keeps five fields. Note `t == NUMBER` is also accepted as group 0 (:296): a value of
  run-time kind has no `p` — needs item B1's kind or a refusal.
- `MixedEncoding.dropUndecidingPrecision` :370-385 drops `p` where at most one distinct non-NULL `p` occurs — with
  numbers carrying `p`, a list of one number kind still drops it (today's SQL, byte for byte).
- `MixedEncoding.oneChannelOrders` :361-363: `return group == 0 || ...` — **a numbers-only list orders by `n` alone,
  always.** Must become "group 0 only where `p` was dropped" (as dates already are), or `[3, 3.0]->sort()` keeps ties.
- `MixedEncoding.taggedList` :180-200 (the groups and the one-channel shortcut) and
  `ValueCollections` :188-232 (the row-major flatten writes the rank's fields as `__cmp_<field>` columns; names from
  the struct's fields, :218, :231) and :286-294 (rebuilds the comparable from them). Adding no field (reusing `p`)
  keeps these untouched.
- `TaggedLists.sort` :290-305 orders positions by `OrderedListAgg(p, tl.comparableAt(p))`; ties are not stable inside
  the aggregate (audit S3). Proposal: the position as the last key (`sortRows` :409-438 already adds ROW_POSITION).
- `MixedEncoding.MixedElems` :37-56 and `encodeMixed` :594-656: a Number or Date list's `max`/`min`/`greatest`/`least`
  is `LIST_MIN(valList)` and `LIST_POSITION(valList, winner)` over a SCALAR comparable (DOUBLE or TIMESTAMP) — **no
  type here at all**; the first position of a tie wins (Pure's rule for numbers: TaggedSelectionTest :104-105
  `[3.0, 3]->max()` is the Float). Ties across date kinds are refused at planning by `Scalars.refuseRealTies`
  :4098-4172 (called at Scalars :1268, :1274, :1288, :1307, :1313, :1325, :1367, :1373, :1385, :1391).
- **BigQuery** (lane, 2026-11-26): `ORDER BY` a STRUCT is refused. The aggregate is rendered at BigQuery.java
  :943-944: `ARRAY_AGG(v ORDER BY <orderKey(key)>)`. Fix in the dialect (invariant 3): when the key's type is a
  `SqlType.Struct`, write one key per field (`(k).g, (k).n, ...`; `structGet` :1225-1228 spells a field read). SQLite
  (:2424-2434) and MariaDB (:2718-2731) already do this for LIST_SORT via `elementSort`; Snowflake via `structSort`
  :949-962. **But their `OrderedListAgg` arms** (SQLite :826-827, MariaDB :1072) write `ORDER BY <expr(key)>` — over
  a struct that is a JSON object's TEXT on SQLite/MariaDB (MixedEncoding's own comment at :187-189 says "a struct key
  is ordered as its text on SQLite and MariaDB"). Did not trace whether a cross-group rank reaches those arms there
  (it would order `10` before `9`). Check all three when fixing BigQuery. ClickHouse writes a `tuple` (:1298-1330):
  ordered natively; NULLs inside a tuple not probed. H2 writes `JSON_OBJECT` (H2.java :1495-1502) and refuses LIST_AT,
  so its tagged sort is refused first (did not trace which refusal).

**Host site (1).** `testexec/Equality.sorted` :288-293:
```java
out.sort(java.util.Comparator.comparingInt((Typed t) -> typeRank(t.value()))
        .thenComparing(t -> withinRank(t.value())));
```
`typeRank` :295-307 (null 0, Number 1, String 2, Boolean 3, PureDateLiteral 4, Map 5, Kinded 6) and `withinRank`
:309-321 (a Number by `new BigDecimal(String.valueOf(n))`, a date by `toInstantFloor()`). The tie-break needs a third
comparator: the number kind from `effectiveKind(t)` (:122-130, which honours the declared kind; `carrierKind` alone
would read a Float-declared Long as Integer) and a date's precision (`PureDateLiteral` carries it; `Kind` does not
tell a Year from a DateTime — every non-strict PureDateLiteral is `Kind.DATE`, :97-99). Its one caller is
`sameElements` :239-247 (and PureAssertsTest :66-68). The host's GROUP order (Number < String < Boolean < Date)
differs from the product's (numbers < dates < Boolean < String); harmless for a multiset match, but not "the same
order" if anyone reads it as one.

**Tests that hold today's order and must not move:** MixedListConformanceTest :38-77 (E-R1 `[1, 1.0]->removeDuplicates()`
two elements; D1 `[%2025-02-10T00:00:00, %2025-02-10]->sort()` StrictDate first; `[%2025-02, %2025-02-01, %2025]->sort()`;
`[1, 1.0, 2.5]->min()` is `1`), TaggedSelectionTest :104-105. **Test that flips if the date-extreme refusal goes:**
MixedListConformanceTest.anExtremeAcrossDateKindsIsRefusedByName :110-136 (asserts the "two of its elements of
different kinds compare equal" refusal). New: `[3, 3.0]->sort()` is `[3, 3.0]`, `[3.0, 3]->sort()` is `[3, 3.0]`;
the property test the design names (exactly one of `<`, `==`, `>` per pair) on DuckDB, SQLite, H2.

### B5. Crossing numbers: refusals become `false`

**Sites (5).** `TaggedLists.refuseCrossingNumbers` :171-185 and `scaleZero` :187-190, called at
:99 (`sameWrittenLists`), :144 (`sameListByKinds`, every unequal slot pair), :554 (`sameElement(Tagged, Tagged)`),
:585 (`contains`, only when the needle's family has no channel). The change: delete the method and its four calls;
every caller already continues to the `false` branch (`differ = true`, the missing-channel all-NULL array, `continue`,
`return BoolLit(false)`). The shape dependence the audit's S5 named (`2->in([2D, 'a'])` refused,
`2->in([1, 2D, 'a'])` false) disappears with it.

**Test that flips:** T/exec/TaggedListTest.numbersOfDifferentKindsAreRefusedWherePureEquatesTheirTexts :337-350 — the
three `assertThrows(NotImplementedException...)` at :345-350 (`[3, 'a']->contains(3D)` "Integer", `[3, 'a'] == [3D, 'a']`
"Decimal", `[1.5, 'a']->contains(1.5D)` "Float") become `assertEquals(false, ...)`; rename the method. :342-344 stay.

### B6. Carried in

**(a) A function typed into each call is held to the strict rule.**
- `Typer.Compared.GENERIC_BODY` (M/compiler/spec/Typer.java :174-186); set by `Overloads.applyGeneric` :129-134 when
  `requiresNormalization` (Overloads :266-277: the `NormalizeRequiredFunction` stereotype, `RelationGenerics.of(f)`,
  a schema-erased parameter or return — "a table with no declared columns").
- `Typer.compared` :228-251 hands it to `crossTypes.inGenericBody(written)` :244-246.
- `LiteCrossTypeRule.inGenericBody` (M/compiler/spec/LiteCrossTypeRule.java :50-62) returns `crossing.comparison()`,
  "the database's comparison until the run-time comparison is built, step 6". So today `$x.n == $x.f` through a
  generic function is SQL's `1 = 1.0`, true.
- The slice's choice: inside a body typed into its call both column types ARE known, so the crossing is a fact fixed
  by the types: EQUALITY/MEMBERSHIP/INDEX → `false`/`-1` (as legend-pure's door already answers markers), ORDERING →
  the database's comparison by value. That is a fold of a type fact (allowed). The plan's wording, "its generic
  positions compare by the run-time rule", needs the owner to say whether ORDERING across two types in a generic body
  is by value (then by type) or a compile error.
- Tests: T/compiler/spec/CrossTypeRuleTest.litesSettingInABodyTypedIntoItsCall :159-174 asserts NO marker for
  `x::same(~n, ~f)` — stays (no marker) if the answer is a rewrite, not a marker; aBodyTypedIntoItsCall :113-125
  (records the setting is asked "body: Integer == Float") stays. T/integration/TimeseriesLibraryTest (the period-column
  meeting; LiteCrossTypeRule's javadoc says the library refuses a day against a date-time itself at run) — must not move;
  did not trace its line.

**(b) A row's and a table's cells counted.**
- The typing: `Typer.tdsValuesRead` (Typer.java :1419-~1466; called from `TdsSurfaceReads` :64). For a row variable it
  returns `TdsDesugars.rowCells` (typed per-column reads, `rowCells=true`); for a PICK-rooted row
  (`$t.rows->at(0).values`) and for `$t.rows.values` it returns **`source` itself** (the identity carrier, ":the wire
  flatten IS row-major cell order", the 2026-08-24 reversal). `Typer.lateBoundGridMarker` :1385-1407 is the
  late-bound grid's `.values` (Any[*]) — a different shape (an executeInDb grid); did not see it involved in the probe.
- The written `size` rewrite exists: `TdsDesugars.tdsRowCellIndexRead` (M/compiler/spec/TdsDesugars.java :685-786):
  `$t.rows.values->size()` → `size(rows) * C` (:745-759); one row's `.values->size()` → `TypedCInteger(C)` (:768-771).
- The assert's condition bypasses it: `VerdictQueries.sizeOf` (M/compiler/spec/VerdictQueries.java :506-508) builds
  `size(side)` by declaration; its javadoc (:497-505) names the two gaps as "step 6.2's".
  `VerdictQueries.rowCellsRead` :700-720 is the judge's recognizer of a row's cells (rendered sides).
- Two ways: (i) `sizeOf` applies the same two rewrites to the side (cheap, local, no typing change); (ii) the typer
  types `.values` of a picked row as its cells (`rowCells(source, rt)`) and `.rows.values` as a list — the design's
  wording ("typed as one"), but it touches the carrier the 2026-08-24 reversal kept, and every consumer of that
  identity (`rowCellsRead`, the referee's row-major walk). (i) is the safe first step.
- Tests that flip: T/testplan/AssertBarTest.theLastFamiliesAreTypedOrRefusedByName (method at :943):
  :1123 `assertSize(<grid>.rows.values, 4)` → `"AssertFailed: expected size: 4, actual size: 2"` becomes `pass`;
  :1135 `assertSize(<cells>, 2)` → `"AssertFailed: expected size: 2, actual size: 1" + everyDatabase` becomes `pass`;
  :1136 `assertSize(<cells>, 3)` stays `AssertFailed` but loses `everyDatabase` (the DuckDB/H2/SQLite
  `DialectCapability: a tagged list reached the renderer` overrides, :1132-1134); :1137 `assertNotSize(<cells>, 3)`
  stays `pass`, loses the overrides. :1114-1117 (a table itself refused, S137) must not move.

**(c) A date function's result declared the abstract Date: its `toString`.**
- The rule: Scalars :2429-2456 (`AT_RELATION_TO_STRING`, `AT_STRING_TO_STRING`): `if (t == Type.Primitive.DATE_TIME)
  return dateTimeOf(styled, ...)` (:2443-2445); a date literal's text (:2447-2450); else `pureToString` → for
  `Type.Primitive.DATE` the final `new SqlExpr.Cast(x, PureSql.type(STRING), CARRIER)` (:3062) — the database's plain
  cast, `2014-01-02 00:00:00`.
- The fix by the SQL fact: an abstract-Date operand whose fact is TIMESTAMP → `dateTimeOf`; DATE → a StrictDate's text;
  TEMPORAL_TEXT → as is. **Hazard** (section A): `adjust` over a Date-typed value holding a StrictDate is TIMESTAMP
  (DateShifts :196-204 casts back only for a STATIC StrictDate), and a Date-LUB CASE is TIMESTAMP. Either type the
  result more precisely in the typer (adjust's result as its operand's type where that is static: Pure's own
  signature is `Date[1]`) or refuse those shapes by name.
- No recorded test holds it (plan record :197).

**(d) A Float's `toString` through a Number or an Any inside a store query at legend-engine's door.** Not on this
branch (0.6). On `origin/claude/edition-step8`, `EngineTable.rows` (:275-290 there) keys on
`call.args().get(0).info().type() == FLOAT`. For a Number-typed operand the row could key on the operand's SQL fact
(DOUBLE) under the same promotion hazard; for a `Tagged` the Float candidate is printed inside
`Scalars.pureToString`'s Tagged arm (:2947-2951, `MixedEncoding.perCandidate` :519-529), below the table's lookup.
Did not trace how a table row could be consulted per candidate. Also on this branch: `pureToString` prints a NUMBER
by `DECIMAL_TO_TEXT` (:3058-3060) whatever the SQL type — a DOUBLE-typed Number prints as a decimal's digits. That is
lite's own text too, and the same "kind from the SQL type" question.

**(e) The tolerance assert compares Numbers of different types.**
- Product form exists for the quantified case: `VerdictQueries.assertAsPredicate` :468-491 builds
  `lessThanEqual(abs(minus([e, a])), t)` by declaration (`minus` :543-552 over the Number overload, `native1`/`native2`
  :554-562): no crossing marker (built by declaration, Number-typed).
- The statement-root form is the judge's: `DatabaseJudge.databaseTolerance` (M/testplan/DatabaseJudge.java :648-674),
  `toleranceArithmetic` :679-690 (BIGINT if all Integer, DOUBLE if any Float, else DECIMAL(38,18)),
  `VerdictSql.typedTolerance`; host `HostJudge.planTolerance` :103.
- The rule for it: `<=` between two values whose types are known only at run time compares by value (an ordering;
  the run-time rule's "orders by value, then by type" makes `abs(..) <= t` exact only when the tie-break does not
  decide `<=` — at a tie of value `<=` is true either way). So the slice can plan it as its Pure body under the
  product without a crossing, once B1's run-time kinds exist; SQLite's Decimal arithmetic stays refused
  (AssertBarTest :1100-1101, `"pass|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability"`, must not move).
  The design put the deletion of `typedTolerance` in 5c-2/5c-3, not here; did not trace more.

## C. Guards

| Guard | What it reads | Moves? |
|---|---|---|
| T/JavaEvalLedgerTest.EVICT_SIZE (:84; check :2505-2525, comment-stripped non-blank lines, upper bound) | `testexec/Equality.java` pin **319**, measured **319** (at the pin). VerdictSql 2263 = 2263; JudgmentFacts 355 = 355; DatabaseJudge 827 = 827; AssertVerdicts 1335 = 1335; HostJudge 243 = 243; SidePlanner 102 = 102; HostJudgments 649 = 649 (all exactly at the pin). **No row** for TaggedLists, MixedEncoding, Scalars, ValueCollections, Lowerer, Typer, TdsDesugars, VerdictQueries, CrossType*. | Equality.sorted's tie-break: +3 to +8 lines → a pin bump with the user's word (the design said "about 3"). Any line added to the judges' files (DatabaseJudge for 6e) needs a word too; moving JSON-kind helpers OUT of VerdictSql is a shrink. |
| T/CodeShapeGuardrailTest (METHOD_LIMIT 250, :37; :746-782) | measured with the guard's own algorithm: **Typer.applyFunction 250 (AT the limit, :523)**, Typer.accessProperty 222, Typer.synth 205; Lowerer.scalarStructural 249 (:3864), aggValue 233, scalarInner 228 (:3632, holds the TypedCollection arms :3680-3760), scalarValueTailArms 225; Scalars.pureToString 124; MixedEncoding.encodeMixed 63; Equality.same 74; every TaggedLists/MixedEncoding/TdsDesugars method < 60. **Scalars' rules are lambdas inside `static {` (:103, :4086), which the guard does not measure.** | Lowerer.scalarInner (+22 room) is where B3's tagging of non-value-lane Any lists goes — the room is small; put new logic in a helper. Do not add a line to Typer.applyFunction or Lowerer.scalarStructural. |
| T/SqlFnSortedTest.BOTH_WRITERS (:197-457, exact sets, :676) | a function both edge and product write must list every product top class that writes it | TaggedLists/MixedEncoding already in AND, COALESCE, EQUAL, GREATER, GREATER_EQUAL, IS_NOT_NULL, LESS (TaggedLists), NOT, OR. **New writer sets to watch:** JSON_TYPE is PRODUCT only (:99), no entry; IN (TaggedLists already, possibly unlisted, 0.7); LESS_EQUAL/LESS from MixedEncoding (not listed) if the rank or B6e writes them there; IS_NULL from TaggedLists (not listed). Each new pair is one dated entry. |
| T/PlatformNamesGuardrailTest (FUNCTION_FQN_LITERALS_MAX 347 :186, BARE_NAME_ARMS_MAX 188 :189) | `"meta::...::fn"` literals and bare-name arms outside the catalog | does not apply if the slice uses `Pure.*` catalog constants (VerdictQueries :484 already has a literal `"meta::pure::functions::asserts::assertEqWithinTolerance"`; do not add another) |
| T/compiler/spec/typed/NativeCallStampGuardTest (`NEW_CALLS`) | unstamped `new TypedNativeCall(` per file: TdsDesugars 1, Typer 1, VerdictQueries 6, LiteralUnroll 1, ... | B6b option (i) in VerdictQueries via `built(...)`/`declared(...)` adds one construction site only if a new `new TypedNativeCall(` is written (use `built`, which is counted once) → likely 0; option (ii) in TdsDesugars: +1 unless it goes through `nativeCall(...)` |
| ArchitectureTest.protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer (`PROTOCOL_DESUGAR_DEBT` :4505, exact equality :5095): TdsDesugars **55**, Typer **17**; VerdictQueries has no row (0) | a new `CInteger`/`AppliedProperty`/`AppliedFunction` construction | B6b (ii) in TdsDesugars/Typer moves the pin by its count (exact both ways); (i) in VerdictQueries must build typed nodes only (no protocol), else a new row |
| ArchitectureTest.typedNodesAreMintedOnlyByCompilerLayers (:4468-4486) | typed nodes outside compiler/resolver/normalizer/lowering | does not apply (all sites are in those packages); `testexec/Equality` builds none |
| T/LiteralUnrollLedgerTest | the fold's natives | does not apply |
| T/planner/TaggedListStatementSizeTest (250K chars) | Integer and String lists only | the rank's `p` for numbers adds a field only where two number kinds tie-break; check the size pin's lists contain no mixed number kinds (6.1's map says Integer and String only) |
| lean-SQL ladder (T/ladder/LeanSqlLadderTest) and the product SQL size pins | — | did not check every rung; the `p` on numbers and the position key grow mixed-list SQL slightly |

## D. Open questions (rule before code)

1. **legend-pure's door, crossing numbers.** Pure compares an Integer and a Decimal of scale 0 (`3 == 3D`) and a Float
   and a Decimal (`1.5 == 1.5D`, `1.0 == 1.0D`) by their texts: true. Inside a mixed list the typer cannot see the
   values and nothing after the typer may see the edition. So legend-pure's door answers lite's `false` there — a
   register row (the S127 family), and PCT's legend-pure lane may move. Confirm, and say whether the row is S127
   amended or new.
2. **What orders first among equal values of different types:** Integer < Float < Decimal (Q4), StrictDate < DateTime
   (already). And against Pure's own extreme rule: Pure's `max`/`min` keep the FIRST of a tie (`[3.0, 3]->max()` is the
   Float, `[3, 3.0]->max()` the Integer). With a type tie-break `[3, 3.0]->max()` is the Float and `[3.0, 3]->min()`
   the Integer. Does the selection keep Pure's first-of-tie (then the rank's type break applies to `sort` only), or
   follow the order (a register row)? This decides whether `MixedElems` changes at all.
3. **Ordering across types at run time** (`<` between a run-time Integer 1 and a Float 1.0; a generic body's `<` of an
   Integer and a Float column): by value only (SQL), or by value then by type (`1 < 1.0` true)? The AGENTS.md text
   speaks of "a mixed list orders"; it does not say `<`.
4. **Promotion shapes** (section A): refuse a Number/Date-typed CASE, COALESCE, computed array, arithmetic with a
   Decimal, abstract-Date `adjust` by name, or carry a tag through them? And a Number/Float/Decimal/Date parameter:
   refuse, or bind its kind as a second parameter?
5. **JSON:** is "may hold a Decimal or a date" decided from the producer's static types (refuse where unseen), and is a
   whole Float that a database writes as `1` (ClickHouse, BigQuery, maybe H2: not probed) refused there or probed
   first?
6. **valueLane** (B3): may a literal Any list holding a variable or a property read be tagged?
7. **B6b:** option (i) (the assert's `sizeOf` applies the typer's two size rewrites) or (ii) (the typer types the cells
   as a list)? (ii) reopens the 2026-08-24 carrier decision.
8. **6d** waits for step 8's EngineTable; confirm it stays in 6.2 or moves to the step that lands step 8's slice.
9. **Equality.java's pin** (319, exact): the tie-break needs a bump, the user's word.

## E. Proposed order (each step small, its narrowest proof first)

0. **Measure (no code).** The 5b-0 census: every comparison whose side `EqualityClasses.of` (:109-127) leaves to run
   time, by carrier (Tagged, JSON, plain SQL, a promotion) and by place, over PCT channel A, both corpora, core.
   Run SqlFnSortedTest once (0.7). Proof: a table, no code.
1. **B5, crossings to false.** Delete `refuseCrossingNumbers`, `scaleZero`, four calls. Proof:
   `bazel test //core:... --test_filter=TaggedListTest` (the :337-350 method flipped). Guards: none move.
2. **B1, kind from the SQL fact for leaves.** The helper, the two `kind == null` arms, `taggedEquality`'s list arm;
   a class needle → false; promotion shapes refused by name. Proof: new TaggedListTest cases (a Number column, an
   Any upcast, `if(c, 1, 1.5)` refused).
3. **B4 product rank.** `p` for numbers, `oneChannelOrders` for group 0, the position as the last sort key. Proof:
   MixedListConformanceTest (must not move) + new `[3, 3.0]`/`[3.0, 3]` sort cases; then the BigQuery dialect's
   struct ORDER BY (and a look at SQLite's/MariaDB's OrderedListAgg arms) — proof: a BigQuery dialect unit test of
   the rendered text, then the BigQuery lane once.
4. **B4 host.** `Equality.sorted`'s third comparator (pin bump, with the word). Proof: PureAssertsTest and the
   both-judges round on a `assertSameElements([3, 3.0, 'a'], [3.0, 3, 'a'])` probe added to AssertBarTest.
5. **B3, Any lists tagged** (row cells via `columnChannel`, then non-value-lane literals if ruled). Proof:
   TaggedListTest :144-175 and a `contains`/`in` over `[$s, 1]`; the SQL has no `to_json`.
6. **B2, JSON kinds.** Membership and equality of a JSON-carried Any by JSON_TYPE plus the value; refused where a
   Decimal or a date may be inside. Proof: a DuckDB and a SQLite case; then the warm lanes (MariaDB, ClickHouse,
   Snowflake, BigQuery), since every dialect's JSON_TYPE differs.
7. **B6b (i)**, the size rewrites in `VerdictQueries.sizeOf`. Proof: AssertBarTest :1123, :1135-1137.
8. **B6c**, an abstract Date's text by its fact, refusing the promotion shapes. Proof: a PCT-route probe of
   `%2014-01-01T00:00:00.000+0000->adjust(1, DAYS)->toString()` at legend-pure's door.
9. **B6a**, a generic body's crossing answered by the types. Proof: CrossTypeRuleTest :159-174 and
   TimeseriesLibraryTest, then both corpora (a 5a-style regression risk: "marking these broke about 32 corpus tests
   a lane").
10. **B6e** and **B6d** last (6d after step 8 lands). Then the full round, the review, the register rows for D1-D3.

## F. What I could not determine
- How the inliner types a substituted `Number` parameter (section A).
- `CastPolicy.lower`'s arm for a primitive cast to Number or Any; `Type.kindOfSqlType`'s body.
- How each database writes a whole Float into JSON (H2, ClickHouse, BigQuery) and MariaDB's JSON_TYPE of a DECIMAL.
- Whether a cross-group rank reaches SQLite's and MariaDB's `OrderedListAgg` (ordering a struct's JSON text).
- Whether SqlFnSortedTest is green on HEAD (0.7).
- Which upstream PCT statements hold crossing numbers or ties of mixed kinds (no legend-pure checkout here).
