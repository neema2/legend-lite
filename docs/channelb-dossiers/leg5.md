# Leg 5 — comparator / key ordering semantics

**Rows:** `testSimpleSortWithFunctionVariables`, `testSimpleSortWithKey`,
`testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes`
**Symptom as charted:** wrong ORDER (not an error).

> **HEADLINE — the charter's §3 hypothesis is REFUTED on all three
> counts.** The expectation is not DESC; the comparator/key machinery is
> not the gap; and the three rows have **three different** causes, only
> one of which is an ordering bug. Two of the three rows are not winnable
> at all — they belong in the A1 ledger.

See `README.md` in this directory for the shared tenet quick-reference
and the path/provenance notes.

---

## a) REFERENCE SEMANTICS

### A1. The `sort` family — exhaustive (3 overloads, all in one file)

`/Users/neemsandv/legend/legend-pure/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/collection/order/sort.pure`

| # | Signature | Line | Kind |
|---|---|---|---|
| 1 | `sort<T,U\|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1], comp:Function<{U[1],U[1]->Integer[1]}>[0..1]):T[m]` | :22 | **native** |
| 2 | `sort<T\|m>(col:T[m]):T[m]` → `sort($col, [], [])` | :24-27 | Pure wrapper |
| 3 | `sort<T\|m>(col:T[m], comp:Function<{T[1],T[1]->Integer[1]}>[0..1]):T[m]` → `sort($col, [], $comp)` | :29-32 | Pure wrapper |

A `grep` for `::sort` over the whole platform tree returns **only these
three** (plus `tests::sort::…`). **There is no `sort(col, key)` 2-arg key
overload** — the 2-arg form is comparator-only.

### A2. The comparator return convention — **INTEGER SIGN**

Declared `->Integer[1]` (sort.pure:22, :29). Both natives use it directly
as a `java.util.Comparator` result:

- **Interpreted** `…/legend-pure-runtime-java-engine-interpreted/…/collection/order/Sort.java:66-76`
  — `(left,right) -> PrimitiveUtilities.getIntegerValue(executeLambdaFromNative(comparison, [left,right],…)).intValue()`;
  when `comparison == null`, `Compare.compare(left,right,processorSupport)` (:67).
- **Compiled** `…/CompiledSupport.java:907-954` — `(left,right) -> comp.execute([left,right],es).intValue()`
  (:918, :951); null comparator → `CompiledSupport::compareInt` (:917, :950).

**Negative ⇒ left sorts first.** It is *not* a Boolean less-than.
Contrast `removeDuplicates`' `eql`, which **is** `->Boolean[1]`
(removeDuplicates.pure:23). Two different conventions in the same
neighbourhood — do not conflate them.

### A3. Key application

- interpreted Sort.java:83-85 —
  `collect(e -> pair(key(e), e)).sortThis((l,r) -> comparator.compare(l.getOne(), r.getOne())).collect(Pair::getTwo)`.
  **The comparator receives KEYS, not elements.**
- compiled CompiledSupport.java:922-954 — same, with a memoized `ElementWithKey`.

### A4. The reference's OWN comparator → direction reduction (load-bearing)

`/Users/neemsandv/legend/legend-engine/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/corefunctions/collectionExtension.pure:183-190`:

```
function meta::pure::functions::collection::sortBy<T,U|m>(col:T[m], key:...):T[m]
{ sort($col, $key, []) }

function meta::pure::functions::collection::sortByReversed<T,U|m>(col:T[m], key:...):T[m]
{ sort($col, $key, {x, y | $y->compare($x)}) }
```

The engine itself defines **empty comparator ≡ ASC** and
**`{x,y|$y->compare($x)}` ≡ DESC**; by symmetry `{x,y|$x->compare($y)}` ≡
ASC (sort.pure:38 vs :44 pin exactly this pair). **The
comparator→(key, direction) reduction is engine-sanctioned, not our
invention.**

### A5. `testSimpleSortWithKey` — the expectation is **ASCENDING**

sort.pure:59-62:

```
assertEquals(['Smith','Doe','Branche'],
  $f->eval(|['Doe','Smith','Branche']->sort({s | $s->substring(1, 2)},
                                            {x:String[1], y:String[1] | $x->compare($y)})));
```

- comparator `$x->compare($y)` = **ASC** (A4).
- `substring(str,start,end)` is **0-based, [start,end)** — substring.pure:24,
  pinned by testStart (:29-33) and testStartEnd (:39-43).
- keys: `'Doe'→'o'`, `'Smith'→'m'`, `'Branche'→'r'`. ASC on `m<o<r` ⇒
  `['Smith','Doe','Branche']`. ✅

The result merely *looks* reversed because ASC on the full strings would
be `Branche,Doe,Smith`. The charter read the output, not the key.
**`CHANNELB_BURNDOWN_HANDOFF.md:54`'s "(expected DESC …)" is wrong.**

### A6. `testSimpleSortWithFunctionVariables` — the same expression, let-bound

sort.pure:64-69. `let key = {s:String[1]|$s->substring(1,2)}; let comp =
{x:String[1],y:String[1]|$x->compare($y)}; …->sort($key,$comp)`. Identical
collection, identical expectation. The only added surface is
function-valued `let` variables.

### A7. `testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes` — **not a `removeDuplicates` ordering test**

removeDuplicates.pure:48-51:

```
assertEquals([1, 2, 3, '1', '3'],
  $f->eval(|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates())->sort());
```

`->sort()` is applied **outside** `$f->eval(…)`. Contrast `…Explicit`
(:61), which has **no** trailing `sort()` and whose expectation
`[1, 2, '1', '3', 3]` **is** first-occurrence order.

`removeDuplicates`' doc says verbatim *"The order of elements is
unspecified"* (removeDuplicates.pure:21); its natives nevertheless preserve
**first occurrence** — interpreted `RemoveDuplicates.java:65-111`
accumulates `results.add(instance)` on first sight in all four arms.

So `[1,2,3,'1','3']` is a **default-`sort()` cross-kind order**, produced by
Pure's `compare`:

- interpreted `Compare.java:76-155` — numbers (:84-93), dates (:96-105),
  booleans (:108-117), strings (:120-129), then the
  `PRIMITIVE_TYPE_COMPARISON_ORDER` fallback (:51, :144-146).
- compiled `CompiledSupport.compareInt:550-588` +
  `PRIMITIVE_CLASS_COMPARISON_ORDER = [Long, Double, PureDate, Boolean, String]` (:140).

**The kind ladder is `Number < Date < Boolean < String < other`.** Both
runtimes agree. Hence Integers 1,2,3 then Strings '1','3'.

> `compare.pure:83-100` (`testCompareMixedTypes`) only asserts
> *antisymmetry* for cross-kind pairs — the ladder direction is pinned by
> this removeDuplicates row and by the natives above, not by `compare`'s
> own PCT.

**The charter's "first-occurrence order `[1,2,3,'1','3']`" is wrong twice
over** — the order is a sort order, and first-occurrence order would be
`[1,2,'1','3',3]`.

### A8. Stability

Both natives sort via Eclipse Collections `toSortedList(cmp)` /
`sortThis(cmp)` (interpreted Sort.java:80, :84; CompiledSupport:919, :953),
which delegate to `Arrays.sort(…, Comparator)` = TimSort = **stable**.
*(The delegation chain into the EC jar was not opened — see probe P4.)*

### A9. The reference RELATIONAL adapter supports **no comparator sort at all**

The complete relational registration table,
`…/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure`:

| Pure function | → | Line |
|---|---|---|
| `collection::sortBy_T_m__Function_$0_1$__T_m_` | `processSortBy` | 9988 |
| `collection::sortByReversed_T_m__Function_$0_1$__T_m_` | `processSortBy` | 9989 |
| `tds::sort_TabularDataSet_1__String_MANY__…` | `processTDSSortColumns` | 10245 |
| `tds::sort_…SortInformation_MANY__…` | `processTDSSortInformation` | 10246 |
| `tds::sort_…String_1__SortDirection_1__…` | `processTDSSortSingular` | 10247 |
| `relation::sort_Relation_1__SortInfo_MANY__…` | `processTDSSortSortInfo` | 10322 |
| `collection::sort_T_m__T_m_` (bare) | `processVariantSort` | 10367 |

**`sort_T_m__Function_$0_1$__T_m_` and
`sort_T_m__Function_$0_1$__Function_$0_1$__T_m_` are ABSENT.** Confirmed by
every relational PCT manifest (e.g. `…relational-duckdb/EssentialFunctions_manifest.json`,
`…relational-h2/…`):

- `sort::testSimpleSortWithKey`, `…WithFunctionVariables`, `…testSimpleSort`,
  `…testSimpleSortReversed` → `expectedError: "No SQL translation exists for
  the PURE function 'sort_T_m__Function_$0_1$__Function_$0_1$__T_m_'"`
- `removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes`
  → `expectedError: "Any is not managed yet!"`
- `sort::testMixedSortNoComparator` → duckdb `"Not supported: Number"`;
  postgres `"Couldn't find DynaFunction … toVariantList()"`

**The reference relational engine fails all three of our rows, and fails
them EARLIER (compile walls) than we do.**

### A10. Reference substring is 1-based VERBATIM in SQL — we match it exactly

`testDatabricksDynaFunctions.pure:64,67`: `$p.firstName->substring(1, 3)`
renders `substring(\`root\`.FIRSTNAME, 1, 3)` — **args passed through
unshifted** into 1-based SQL `substring(str, start, length)`. The DuckDB/H2
manifests ledger the resulting platform divergence for
`substring::testStart`/`testStartEnd` with the identical off-by-one we
ledger.

---

## b) OUR SEAM

### B1. Where the value-collection `sort` is lowered

`core/src/main/java/com/legend/lowering/Scalars.java:998-1084`, registered
under `Pure.nativeKeysAt("sort")` (:1002). **Not in `Lowerer.java`.**
Branches, in order:

| Branch | Line | Behaviour |
|---|---|---|
| `Stamps.atMostOne(arg0)` | :1006-1008 | identity (`sort([])`, `sort(x[0..1])`) |
| 1-arg, mixed **Number/Date** carrier | :1010-1038 | `MixedEncoding.mixedElems` → parallel unnest of `(id, comparable)`, `OrderedListAgg(id ORDER BY comparable)` |
| 1-arg, everything else | :1042-1043 | **`LIST_SORT(arg0)`** ← the row-3 hole |
| comparator not a bare compare | :1045-1051 | `NotImplementedException("sort comparators beyond a bare compare over the two parameters are not modeled")` |
| 2-arg (comparator only) | :1052-1056 | `LIST_SORT` / `LIST_SORT_DESC` |
| 3-arg (key + comparator) | :1057-1083 | builds `{k, i, v}` structs (`i` negated when DESC), `LIST_SORT`/`LIST_SORT_DESC` (struct sort is field-order lexicographic ⇒ key first, index second ⇒ **stable, first-occurrence ties**), then `LIST_TRANSFORM … StructGet("v")` |

Our registry declares all three collection overloads native —
`Pure.java:2159` (`SORT__T_m`), `:2160` (`SORT__T_m__FUNCTION_0_1`), `:2161`
(`SORT__T_m__FUNCTION_0_1__FUNCTION_0_1`) — so `sort()` arrives with
`args.size()==1`, not the reference's desugared 3.

### B2. `Comparators.direction` — the comparator reducer (EXISTS and is CORRECT)

`core/src/main/java/com/legend/lowering/Comparators.java:98-121`. Accepts
exactly:

- `{x,y | $x->compare($y)}` → `TRUE` (ASC) — :114-116
- `{x,y | $y->compare($x)}` → `FALSE` (DESC) — :117-119
- anything else → `null` → the caller walls.

Guard requires `TypedLambda`, 2 params, 1 body statement, a
`TypedNativeCall` to `meta::pure::functions::lang::compare`, 2
`TypedVariable` args (:103-111). **This is precisely the reference's own
reduction domain (A4).**

### B3. `LambdaBinding`'s comparator conventions — **VERIFIED, they exist**

`core/src/main/java/com/legend/lowering/LambdaBinding.java:43-70`.
`COMPARATOR_NATIVES` = the signature keys of exactly **three** natives:
`removeDuplicates`, `sort`, `contains` (:64-70). Convention (:295-315): for
these natives a **2-parameter** lambda has **both** params stamped as the
*one* list's element (`SqlExpr.Column.param(var, coll)`), whereas the
general rule stamps only **unary** lambdas.

Explicitly **excluded**, with reasons in the javadoc (:56-63): `fold` (2nd
param is the accumulator), relation `join`/`asOfJoin` (span two relations),
`removeAll`, and **`min`/`max`** — *"their comparator is STRUCTURALLY
RECOGNIZED at the rule (Comparators pattern-match, the body never lowers as
a body), and stamping its params broke the recognizer's structural equality
(gate-caught: G9 chB-std testMax/testMin, 'must apply the SAME key')."*

**But the charter's inference from this is wrong:** these conventions govern
how comparator *bodies* lower, not ordering direction. Direction is
`Comparators.direction`'s job and it already works.

### B4. ALL sites that decide ordering direction — exhaustive

**Relation lane (`SqlSelect.SortKey.ascending`):**

| Site | Line | Source of the boolean |
|---|---|---|
| `Sorts.sort` | Sorts.java:43 | `TypedSort.TypedSortKey.ascending()` |
| `Sorts.sortOnto` (post-isolation retry) | Sorts.java:85 | same |
| `Sorts.sortBy` (fold path) | Sorts.java:68 | `TypedSortBy.ascending()` |
| `Sorts.sortBy` (isolate path) | Sorts.java:74 | same |
| `Sorts.naturalSort` | Sorts.java:99 | hard `SortKey.asc(...)` |
| `Lowerer` window `over()` | Lowerer.java:2192-2194 | `TypedSort.TypedSortKey.ascending()` + explicit NULLS |
| `Lowerer` agg order key | Lowerer.java:1137, :1295 | agg `orderKey` |
| `ValueCollectionOps.relationSpaceRewrite` | ValueCollectionOps.java:78-83 | hard `true`, `pureNullOrder=true` |
| `Render.hoistOrder`/lift, `PctTdsWrap.liftOrder`, `SqlPostProcessors`, `UnqualifyPivotArgs`, `SqlRewriter` | Render.java:262,469,680; PctTdsWrap.java:146,160; SqlPostProcessors.java:246; UnqualifyPivotArgs.java:70; SqlRewriter.java:55,240,275 | **carry-through only** — `k.ascending()` preserved |
| `CarrierStrategies` reverse-window | CarrierStrategies.java:1048 | `!k.ascending()` + `flipNulls` (:1154-1156) |

**Frontend origin of relation direction:** `SortChecker` — `asc(~c)`/`desc(~c)`
→ `TypedSortInfo(col, ascending)` (:141-156); bare `~c` desugars to `asc(~c)`
(:189-201); legacy `sort(rel,'C',SortDirection.DESC)` maps loudly, rejecting
any other enum value (:53-68); `sortBy`/`sortByReversed` fix direction at
`Typer.java:1290-1291` → `SortChecker.sortBy(…, true/false)`.

**Value lane (`LIST_SORT` vs `LIST_SORT_DESC`):**

| Site | Line | Decided by |
|---|---|---|
| `sort` 2-arg | Scalars.java:1054 | `Comparators.direction` |
| `sort` 3-arg | Scalars.java:1078 | `Comparators.direction` |
| `sort` 1-arg | Scalars.java:1042 | hard `LIST_SORT` (ASC) |
| `minBy`/`maxBy` | Scalars.java:1852 | `boolean asc = name.equals("minBy")` (:1798) |
| `percentile` | Scalars.java:3007 | caller's `ascending` |
| `Comparators.select` (comparator min/max) | Comparators.java:90 | `!max`, flipped on a reversed key-difference (:57-60) |
| `Aggregates` | Aggregates.java:152 | hard `LIST_SORT_DESC` |

### B5. Row-by-row diagnosis

#### Rows 1 & 2 — **NOT an ordering bug. The substring base divergence.**

Our substring is a verbatim passthrough: `Scalars.java:1337-1340`,
`RULES.put(f, (n,args) -> new SqlExpr.Call(SqlFn.SUBSTRING, args))`, with
the intent named at :1332-1336.

So `substring($s,1,2)` emits SQL `substring(s,1,2)` = **first two chars**:
`'Do'`, `'Sm'`, `'Br'`. ASC on those = `Branche, Doe, Smith` —
**byte-identical to the pinned actual**,
`Test_LegendLite_EssentialFunctions_PCT.java:80-81`:
`"actual:   ['Branche', 'Doe', 'Smith']"`.

The key **is** applied; the direction **is** ASC and **is** correct. The
witness that proves it:
`core/src/test/java/com/legend/lowering/ValueSortComparatorTest.java` —
`descendingComparator` (:34-41) pins `{x,y|$y->compare($x)}` →
`['Smith','Doe','Branche']`, and `keyFunctionSort` (:43-53) pins key-sort
working on 1-based keys. Its javadoc (:16-22) states the conclusion outright.

`docs/PCT_EXPECTED_FAILURES.md:76-100` already classifies both rows in
**bucket C — the substring/indexOf base divergence** — the family the
charter itself calls *"user-adjudicated IRREDUCIBLE (register A1) … a
reverted draft proves the trap — do NOT re-attempt"*
(`CHANNELB_BURNDOWN_HANDOFF.md:33-37`).

**Row 2 adds nothing.** `UserCallInliner` (step G½, runs on every execution
path — AGENTS.md:89) β-reduces query-level lets:
`scope.put(let.name(), rewrite(let.value(), scope))` (UserCallInliner.java:122),
and variable reads substitute (:349-357). After G½,
`testSimpleSortWithFunctionVariables` **is** `testSimpleSortWithKey` — which
is why the two pinned actuals are byte-identical.

> ⇒ **The charter's ledger under-counts. It lists 5 substring/indexOf rows;
> the true family is 7.** Rows 1 and 2 are not winnable unless the A1
> adjudication is reopened.

#### Row 3 — a real, different bug: **cross-KIND `sort()` silently orders by literal spelling**

**Why it is a Channel-B-only row.** `ChannelB.java:25-38, :265-453` — Channel
B is the **identity adapter**: `$f->eval(|expr)` β-reduces to `expr`
(`rewrite`, :396-409) and the *whole* test body — trailing `->sort()` and
`assertEquals` included — compiles to SQL and runs in DuckDB. Channel A's
adapter receives only the serialized inner lambda
(`PctExecuteNative.java:100-101`), so the trailing `->sort()` runs in the
reference interpreter. **That is why this row is absent from
`Test_LegendLite_EssentialFunctions_PCT`'s expectedFailures (Channel A
passes it) yet listed in the charter's Channel-B table.**
`ChannelBEssentialTest.essentialCensus` asserts `out.size() == 327` (:60-61)
— the charter's 297/327 is Channel B.

`docs/M4_PRELAND_CHARTER.md:106` asserted this row was safe because *"the
hetero num/string orderings apply `->sort()` OUTSIDE `$f->eval` —
platform-side, never through the adapter"*. **True for Channel A, false for
Channel B.** That is the stale assumption that produced this row.

**The mechanism, traced end to end:**

1. `[1, 2, '1', '3', 1, 3, '3', 2]` has LUB `Any` =
   `Type.ClassType("meta::pure::metamodel::type::Any")`
   (`InferenceKernel.java:45, :558, :1378`), which has no class layout ⇒
   `Lowerer.java:2400-2443`, the **hetero-literal LITERAL carrier**: each
   element spells via `MixedEncoding.elementLiteral` → `spellByKind`
   (MixedEncoding.java:176-209), wrapped `CAST(ARRAY[…] AS LITERAL[])`.
2. Spellings are **kind-disjoint by grammar** (`LiteralSpelling.literal`,
   :92-125): Integer bare `1` (:124); String **quoted** `'1'` (:97-108);
   Decimal `D`-suffixed; temporals `%`-prefixed. Carrier ⇒
   `["1","2","'1'","'3'","1","3","'3'","2"]`.
3. `removeDuplicates()` (1-arg key `Pure.java:2099`) → `args.size() < 2` →
   `ListEncodings.orderedDedup` (Scalars.java:1477-1479) =
   `list_filter(l, (x,i) -> list_position(l,x) = i)`
   (ListEncodings.java:224-231). **First-occurrence preserving and
   kind-honest** ⇒ `["1","2","'1'","'3'","3"]` = `[1, 2, '1', '3', 3]` —
   exactly the reference's `…Explicit` expectation. ✅ **The dedup is correct.**
4. `->sort()` → `MixedEncoding.mixedElems` **returns null**, because it gates
   on `lub == Type.Primitive.NUMBER || lub == Type.Primitive.DATE`
   (MixedEncoding.java:79-82) and this LUB is `Any` ⇒ falls to
   `LIST_SORT(carrier)` (Scalars.java:1042-1043) ⇒ DuckDB `list_sort`
   (DuckDb.java:253) over **VARCHAR spellings**.
5. Binary collation: `'` = 0x27 < `1` = 0x31. **Derived actual:
   `["'1'","'3'","1","2","3"]` = `['1', '3', 1, 2, 3]`.** Expected
   `[1, 2, 3, '1', '3']`.

**Root cause, precisely: the LITERAL carrier's lexical spelling order is not
Pure's `compare` order.** There is a *comparable channel* for Number-LUB and
Date-LUB mixes and **none** for cross-kind-class (`Any`-LUB) mixes — and the
miss is a **silent wrong answer**, not a wall. That violates AGENTS.md
invariant 4 and TENET_CHARTER C2.4.

Corroborating: this is also why `testMixedSortNoComparator`
(`[342,5.0,-2.0,171,1]`, Number LUB) passes — it takes the comparable-channel
branch. `ChannelBEssentialTest.java:161-163` records that flip.

**Nothing in the harness rescues this row.** `sortedChain()` — the C2.3
order-tolerance gate — exists **only** in
`core/src/test/java/com/legend/harness/EngineTestExecutor.java` (:2175,
:2703-2713, :3017, :3039, :3061), the *relational corpus* harness, and its
allowlist is `HarnessDisciplineTest.ALLOWED` (:57+). **No PCT row is in it,
and neither PCT channel routes through it.** `docs/NOT_IMPLEMENTABLE.md:11-57,
67-77` is likewise corpus-only (H2-vs-DuckDB scan order).
`docs/LENIENCY_CATALOG.md` is about **parser** coverage — not relevant here.

---

## c) MINIMUM DESIGN — the decisions

> Governing clauses: **C2.3** (TENET_CHARTER.md:48-51) settles the cardinal
> question by name; no necessity proof is needed or offered, because
> **nothing in this design sorts in Java.** **C2.4** (:52-55) — *"Absence is
> a loud NotImplementedException, never a plausible value."* **C1.6** (:36) —
> a comparator LAMBDA is model space (compile it); the ROWS are not.
> **AGENTS.md invariant 3a** (:205-243) — *"New native = new MIR variant +
> new render arm."* **Invariant 3** (:200-203) — a dialect that cannot
> express a variant **throws** in a real arm.

### D1. Rows 1 & 2 are **not this leg's work** — reclassify, do not fix

They are downstream of the A1-adjudicated substring divergence, already
ledgered in `docs/PCT_EXPECTED_FAILURES.md:97-100`. The only correct action
is **documentary**: amend `CHANNELB_BURNDOWN_HANDOFF.md:33-37` so the
"modulo" ledger reads **7** substring/indexOf rows, not 5, and delete them
from the winnable table at :54. Attempting them means re-attempting the
reverted A1 draft the charter forbids.

### D2. The comparator→ordering reduction stays where it is

`Comparators.direction` (Comparators.java:98-121) already implements
`collectionExtension.pure:183-190` precisely. **Do not widen it
speculatively.** The 3-arg path additionally reduces `sort(c, key, cmp)` to
*(key expression, direction)* by structural dispatch on typed HIR —
sanctioned by **AGENTS.md invariant 2** (:170-173: the Lowerer *may*
"pattern-match typed HIR for **structural** dispatch"). Nothing here infers
a type.

### D3. The non-reducible comparator: **a loud wall — which we already emit**

The reference relational engine registers **no** comparator-sort translation
at all (A9), and the reference corpus *does* contain non-reducible
comparators. An exhaustive sweep of `->sort({…})` across both reference trees
found:

- multi-key mixed-direction sum-of-compares — `core_analytics_quality/checksEngine.pure:335`
- key-difference — `core/pure/executionPlan/platformBinding/typeInfo/typeInfo.pure:513`;
  `pureToSQLQuery_union.pure:617, :886`
- user-function comparators — `core/pure/tds/tds.pure:366` (`multipleColumnComp`);
  `core_pure_changetoken/diff_generation.pure:59, :115, :169`
- **conditional** comparators — `core_analytics_lineage/propertyLineage.pure:399`;
  `core/pure/lineage/scanProperties.pure:771`
- reducible key+compare — `minBy.pure:31`, `maxBy.pure:32`, `toVariant.pure:85`,
  `analyticsHelper.pure:174`, `mappedEntityBuilder.pure:138, :187, :435`,
  `diff_generation.pure:62, :108, :154, :182`

**Every non-reducible site is inside engine-compiler / analytics Pure that
the interpreter runs and the relational lowering never sees** — the family
already adjudicated in `NOT_IMPLEMENTABLE.md:79-86`. **The adapter-lane
demand is exactly the reducible set.** Keep `Scalars.java:1047-1051`'s
`NotImplementedException` verbatim.

### D4. The one real change: close the silent Any-LUB sort hole

**D4a — Make the miss LOUD (mandatory, small, no MIR change).**
Today `Scalars.java:1042-1043` emits `LIST_SORT` for *every* non-Number/Date
1-arg sort, including cross-kind-class carriers where spelling order ≠
`compare` order. Split it: `LIST_SORT` only when the operand's carrier is
**kind-uniform** (its Pure LUB is a single primitive kind, or a Number/Date
mix already routed at :1010); otherwise
`NotImplementedException("sort over a cross-kind (Any-LUB) collection: the
literal carrier orders by spelling, not by pure's kind ladder")`. A strict
improvement under C2.4/invariant 4, and it is what the reference does
(`"Any is not managed yet!"`). Cost: ~6 lines in Scalars.

**D4b — Optionally implement it (turns the wall green).** The kind ladder is
a *compile-time* fact — the element's static Pure type is known at lowering
(`MixedEncoding.encodeMixed` already dispatches on it, :113-168). Extend
`MixedEncoding` with an **`Any`-LUB comparable channel**: emit, per element,
a struct/2-tuple `(kindRank, withinKindComparable)` where

- `kindRank` is a **compile-time integer literal** from the static type, in
  the reference's ladder `Number(0) < Date(1) < Boolean(2) < String(3)`
  (CompiledSupport.java:140; Compare.java:51,84-129) — no value is inspected,
  so **C2.2 is untouched**;
- `withinKindComparable` reuses the existing per-kind arms (`CAST AS DOUBLE`,
  `strptime`/`make_timestamp`, raw string);

then reuse the **existing** `mixedElems` recipe (Scalars.java:1014-1037):
parallel unnest of `(id, comparable)` + `OrderedListAgg(id ORDER BY
comparable)`. **The database does all the sorting.**

Consequence: `[1,2,'1','3',3]` → ranks `(0,1),(0,2),(3,'1'),(3,'3'),(0,3)` →
`[1,2,3,'1','3']`. ✅

**Where it lives:** `MixedEncoding` + the `Scalars` sort rule. **Not** in
`Lowerer`, **not** in a renderer, **not** in Java.

### D5. Nothing new in the renderers; nothing new in MIR

Every piece is already a typed MIR variant with a render arm:
`SqlSelect.SortKey(expr, ascending, nullOrder, outputName)` (SqlSelect.java:72-78),
`SqlFn.LIST_SORT`/`LIST_SORT_DESC` (SqlFn.java:77), `SqlExpr.OrderedListAgg`,
`SqlExpr.StructLit`. ASC/DESC and NULLS spelling stay renderer-owned
(AnsiSqlRenderer.java:200-201, :863-864; H2.java:283, :355-378;
DuckDb.java:253-254). **No `String` encodes a SQL operation anywhere in this
design.**

### D6. First-occurrence stability is already correct and needs nothing

- `removeDuplicates()` — `list_filter(l,(x,i)->list_position(l,x)=i)`
  (ListEncodings.java:224-231) **is** the SQL form of "row-number over the
  original ordering", and matches `RemoveDuplicates.java:65-111`.
- key-sort ties — the `{k, i, v}` struct with `i` = original 1-based index
  (negated under DESC) makes DuckDB's field-order-lexicographic struct sort
  **stable, first-occurrence** (Scalars.java:1062-1083).
- `Comparators.select` breaks min/max ties on `_cx.i ASC` (Comparators.java:92).

---

## d) TRAPS

1. **Does this leg force the seam-split? NO — but only just.**
   `Lowerer.java` is **exactly 3500 lines** = `CodeShapeGuardrailTest.FILE_LIMIT`
   (:35); it may not grow by one line. **Sort lowering does not live there**:
   the value-collection sort is `Scalars.java:998-1084`, the relation sort is
   `Sorts.java` (102 lines), and `Lowerer`'s only involvement is the one-line
   delegation `Sorts.naturalSort(this, nc)` at `Lowerer.java:696`.
   **`Scalars.java` is 3424 — 76 lines of headroom.** D4a fits; D4b likely
   does not, and should land in `MixedEncoding.java` (475 lines) with only the
   branch condition changing in `Scalars`. `METHOD_LIMIT` is 250 but the sort
   rule sits inside a `static {` block, which `SIG` (:154-163) does not match —
   file length is the only binding constraint.

2. **Record-equality / stamping hazard — already bit this codebase once.**
   `Comparators.select` matches comparators by `SqlExpr` **record `.equals()`**
   after `substituteRef` (Comparators.java:50-56). `LambdaBinding.java:56-63`
   records the incident verbatim: adding `min`/`max` to `COMPARATOR_NATIVES`
   stamped their comparator params, which changed the records and **broke the
   structural recognizer** — gate-caught as *"G9 chB-std testMax/testMin, 'must
   apply the SAME key'"*. **Any change that stamps, wraps, or re-types a
   comparator lambda's params will silently break structural matching.**
   `Comparators.direction` is safe today because it matches on **typed HIR**
   (`TypedLambda`/`TypedVariable`), not lowered MIR — keep it that way.

3. **The inliner is load-bearing in both directions.** `UserCallInliner`
   β-reduces lets (:122) — the *only* reason row 2 behaves as row 1. But
   `UserCallInliner.java:499-502` notes untouched subtrees deliberately keep
   **node identity** ("F13 leans on it") — a gratuitous rebuild in the sort
   path would re-mint let-bound instances per side.

4. **`ValueSortComparatorTest.keyFunctionSort` (:43-53) pins the bug's
   *consequence*.** It asserts `['Branche','Doe','Smith']` with the comment
   *"keys are RELATIONAL (1-based) substrings"*. If the substring adjudication
   is ever reopened, this pin must move with it — it is not an independent
   witness of correctness.

5. **NULLS FIRST/LAST divergence is real and already handled.** `Fold.sortNulls`
   (Fold.java:377-385): Pure treats null as **largest**, so ASC ⇒ emit nothing
   (DuckDB default), DESC ⇒ `NULLS FIRST`; the comment records *"DuckDB defaults
   LAST both ways — probed 2026-08-19"*. `Sorts.sort` applies it only when
   `s.pureNullOrder()` (Sorts.java:44, :86) — the modern relation API — while
   `sortBy` deliberately passes `null` (Sorts.java:68, :74). **`list_sort`'s
   null placement is a separate, unmodelled question** — see probe P2.

6. **Stale charter assumptions — three, all refuted.** (a)
   `CHANNELB_BURNDOWN_HANDOFF.md:54` "expected DESC" — the expectation is ASC on
   a substring key (A5). (b) same line, "first-occurrence order
   `[1,2,3,'1','3']`" — it is a *sort* order, and first-occurrence would be
   `[1,2,'1','3',3]` (A7). (c) `M4_PRELAND_CHARTER.md:106` "never through the
   adapter" — true of Channel A, **false of Channel B**, the lane the charter is
   actually counting (B5).

7. **`removeDuplicates`' comparator arm has an ISE that should be a wall.**
   `Scalars.java:1480-1484` throws `IllegalStateException` when the last arg is
   not a 2-param lambda. Per `Comparators.java:19-21` (*"valid pure shapes beyond
   them wall via NotImplementedException, never ISE"*) this is the wrong
   exception class. Adjacent and cheap, but **not** required by any of the three
   rows — do not bundle it without its own justification.

---

## e) CONFIDENCE + LIVE PROBES

| Claim | Confidence | Basis |
|---|---|---|
| `testSimpleSortWithKey`'s expectation is **ASC on a substring key**, not DESC | **Certain** | sort.pure:59-62 + substring.pure:24,:29-43 + collectionExtension.pure:183-190 |
| Comparator return convention is **Integer sign** | **Certain** | sort.pure:22; Sort.java:75; CompiledSupport.java:918,:951 |
| Rows 1 & 2 are the substring 1-based divergence, not comparator/key semantics | **Very high** | Scalars.java:1337-1340 + pinned actual (PCT test:80-81) + ValueSortComparatorTest:43-53 + testDatabricksDynaFunctions.pure:64,67 |
| Rows 1 & 2 are the **same expression** after G½ | **Very high** | UserCallInliner.java:122, :349-357; AGENTS.md:89; identical pinned actuals |
| Reference relational has **no** comparator-sort lowering | **Certain** | pureToSQLQuery.pure :9988-10367 + 11 manifests |
| Pure's cross-kind order is `Number < Date < Boolean < String` | **Certain** | Compare.java:51,:84-155; CompiledSupport.java:140,:550-588 |
| Row 3's `->sort()` runs through our lowering in Channel B, not Channel A | **Very high** | removeDuplicates.pure:50 parenthesisation; ChannelB.java:25-38,:396-409; PctExecuteNative.java:100-101 |
| Row 3's mechanism = `mixedElems` returns null for Any LUB ⇒ bare `LIST_SORT` | **High** | MixedEncoding.java:79-82; Scalars.java:1042-1043; Lowerer.java:2400-2443; LiteralSpelling.java:92-125 |
| Row 3's **exact** actual is `['1','3',1,2,3]` | **Medium — DERIVED, not observed** | depends on DuckDB VARCHAR collation ordering `'`(0x27) before digits |
| No harness order-leniency applies to any PCT row | **High** | `sortedChain()` confined to EngineTestExecutor; `HarnessDisciplineTest.ALLOWED` lists no PCT site |
| EC `toSortedList`/`sortThis` are stable | **Medium-high** | delegation to `Arrays.sort(Comparator)` assumed; the EC jar source was **not opened** |

### Live probes needed (none require changing anything)

- **P1 (settles row 3 outright).** Run `Compiler.execute` on
  `|[1, 2, '1', '3', 1, 3, '3', 2]->meta::pure::functions::collection::removeDuplicates()->meta::pure::functions::collection::sort()`
  against `jdbc:duckdb:` and capture (a) the emitted SQL and (b) the rows.
  Equivalently `cd pct && mvn -o test -Dtest=ChannelBEssentialTest
  -Dchb.only=RemoveDuplicatesPrimitiveStandardFunctionMixedTypes` (rebuild
  `-pl core install` first — AGENTS.md common mistake #11).
- **P2.** `SELECT list_sort(['1','2','''1''','''3''','3'])` and
  `SELECT list_sort([1,NULL,2])` on DuckDB — pins both the collation order and
  `list_sort`'s null placement, neither of which our code models.
- **P3.** Confirm rows 1 & 2 are Channel-B FAIL (not ERROR) with the same
  `['Branche','Doe','Smith']` — `-Dchb.only=SimpleSortWith`.
- **P4.** Read the Eclipse Collections version's `FastList.sortThis` /
  `AbstractRichIterable.toSortedList` to close the stability gap in A8 (offline,
  no JVM).

---

## OPEN QUESTIONS

1. **Is the A1 substring adjudication reopenable?** Rows 1 & 2 are winnable
   *only* by making `substring` 0-based, which contradicts
   `testFilterUsingParseIntegerFunction`'s golden SQL and rows
   (`PCT_EXPECTED_FAILURES.md:80-85`) and the charter's explicit "do NOT
   re-attempt". If it stays closed, **the ledger must grow from 9 to 11 rows**
   and the charter's leg-5 table must lose two entries. A user-level
   adjudication, not an engineering one.

2. **D4a alone, or D4a + D4b?** D4a (wall) is ~6 lines, strictly improves tenet
   conformance, and turns a silent wrong answer into a loud refusal — but it
   converts row 3 from FAIL to **ERROR** and banks no PCT point. D4b (comparable
   channel) banks the point. Does the burn want the point, or the honesty?

3. **What exact `Any`-LUB kind-rank ladder do we commit to?** Both reference
   runtimes agree on `Number < Date < Boolean < String`, but disagree on the
   *tail*: interpreted `Compare.java:144-146` ranks unmatched primitives via
   `PRIMITIVE_TYPE_COMPARISON_ORDER` and non-primitives by type path (:152-154);
   compiled `CompiledSupport.compareInt:570-587` ranks unknown classes by
   canonical name and falls back to `hashCode` (:587). **No PCT row exercises the
   tail.** Proposal: implement the four claimed kinds and **wall** on anything
   else — needs ratification.

4. **Does `sort()` over an `Any`-LUB collection appear anywhere else in the
   adapter lane?** Comparator-`sort` sites were enumerated exhaustively, but every
   bare `->sort()` over a heterogeneous literal across both corpora was **not**
   swept. If other rows ride the same hole, D4a will surface them as new ERRORs —
   a good outcome, but count it before landing, since `ChannelBEssentialTest` pins
   are shrink-only.

5. **Is `M4_PRELAND_CHARTER.md:106` the only place the "outside `$f->eval`"
   reasoning is load-bearing?** That claim is Channel-A-true and Channel-B-false.
   If the same reasoning gated other M4 decisions, they may carry the same latent
   Channel-B gap. **UNVERIFIED** — the row and its table were read, but not every
   downstream use of the argument.
