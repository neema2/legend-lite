# What upstream answers: numbers of different types in lists, ordering, Number functions (step 6), 2026-11-26

Every answer below is either RUN (upstream's own code executed in this session) or READ (pinned source, file:line).
Nothing is recalled.

**How each was obtained (RUN):**
- **interpreted legend-pure 5.99.0**: `pure/probe/PureModes.java` with mode `interpreted`: `FunctionExecutionInterpreted`
  over a `PureRuntime` of the repositories `platform` + `core_functions_standard` and their dependency closure
  (`core_functions_json, _relation, _unclassified, _variant, platform_dsl_tds, platform_precise_primitives`), the PCT
  lane's jars (lite's own jars removed). Each expression compiled as its own function `probe::raw_<ID>():Any[*]` in its
  own source, run with `fe.start`; each result value printed with its classifier (Pure type) and its Java holder.
  Raw output: `pure/raw-interpreted.txt`.
- **compiled legend-pure 5.99.0**: the same probe with mode `compiled`: `FunctionExecutionCompiledBuilder().build()`
  (Java generated and javac-compiled in memory from the same sources), each function run through
  `FunctionExecutionCompiled.executeFunction` (reflection, to see the raw Java result) and each value's Java class
  printed (Long = Integer, Double = Float, BigDecimal = Decimal). Raw output: `pure/raw-compiled.txt`.
- **legend-engine 4.145.0**: `engine/probe/EngineProbe.java` (after `docs/result-boundary-homework/probes/EqualityProbe.java.txt`):
  `PureGrammarParser` + `Compiler.compile` over `engine/model.pure`, `PlanGenerator.generateExecutionPlan(...,
  PlanPlatform.JAVA, ...)`, `PlanExecutor...withAvailableStoreExecutors()`, over `//tools/engine-runner:plan`'s
  runtime classpath. Every plan is saved (`engine/plans/<ID>.plan.json`, `engine/plans2/`). Raw output:
  `engine/raw-engine.txt`, `engine/raw-engine2.txt`.
  - **store-less**: EVERY expression of groups A-D planned as a single `PureExpressionPlatformExecutionNode`
    (`"_type":"platform"`), implementation `_pure.plan.root.Execute.execute`, i.e. legend-engine's GENERATED Java
    (its `legendJava` platform binding, its runtime helper `org.finos.legend.engine.plan.dependencies.util.Library`),
    NOT legend-pure's compiled runtime. The generated Java body is quoted per row ("gen:").
  - **inside a store query**: `engine/model.pure`'s H2 store `p::DB.T (ID INTEGER, I INTEGER, F FLOAT, D DECIMAL(10,2))`,
    seeded by LocalH2 `testDataSetupSqls` (H2 table `F DOUBLE`), rows
    `(1, 1, 1.0, 1.00) (2, 0, 0.0, 0.00) (3, 2, 2.5, 2.50) (4, 3, 3.0, 3.00) (5, 1, 1.5, 1.50)`; queries both as a
    relation (`#>{p::DB.T}#->filter(..)->select(~[ID])->from(p::H2RT)`) and as a class/mapping
    (`p::Row.all()->filter(..)->project(..)->from(p::M, p::H2RT)`); root node `RelationalTdsInstantiationExecutionNode`;
    H2 driver 2.2.224 (the engine pool's), in-memory.

Types: "3 Integer" means the Pure Integer (interpreted classifier Integer; compiled/engine Java Long); "3.0 Float" the
Pure Float (interpreted holds it in a BigDecimal, classifier Float; compiled/engine Java Double); "3 Decimal" the Pure
Decimal (BigDecimal in all three).

## Group A: numbers of different types inside a mixed list

| expr | interpreted legend-pure | compiled legend-pure | legend-engine store-less (platform node) | how |
|---|---|---|---|---|
| A1 `[3,'a']->contains(3D)` | false | **true** | false (gen: `Arrays.<Object>asList(3L, "a").contains(new BigDecimal("3"))`) | RUN x3 |
| A2 `[1.5,'a']->contains(1.5D)` | false | false | false | RUN x3 |
| A3 `[1,'a']->contains(1.0)` | false | false | false (gen: `Arrays.<Object>asList(1L, "a").contains(1.0)`) | RUN x3 |
| A4 `3->in([3D,'a'])` | false | **true** | false (gen: `Arrays.<Object>asList(new BigDecimal("3"), "a").contains(3L)`) | RUN x3 |
| A5 `[3,'a'] == [3D,'a']` | false | **true** | **true** (gen: `Library.equals(Arrays.<Object>asList(3L,"a"), Arrays.<Object>asList(new BigDecimal("3"),"a"))`) | RUN x3 |
| A6 `[1,'a'] == [1.0,'a']` | false | false | false (gen: `Library.equals(... 1L ..., ... 1.0 ...)`) | RUN x3 |
| A7 `[3,3D]->removeDuplicates()->size()` | 2 (Integer) | 2 (Long) | 2 (Long) (gen: `Library.removeDuplicates(Arrays.<Number>asList(3L, new BigDecimal("3")), null, null).stream().count()`) | RUN x3 |
| A8 `[1,1.0]->removeDuplicates()->size()` | 2 | 2 | 2 | RUN x3 |
| A7x `[3,3D]->removeDuplicates()` | [3 Integer, 3 Decimal] | [3 Long, 3 BigDecimal] | [3 Long, 3 BigDecimal] | RUN x3 |
| A8x `[1,1.0]->removeDuplicates()` | [1 Integer, 1.0 Float] | [1 Long, 1.0 Double] | [1 Long, 1.0 Double] | RUN x3 |

Inside an H2 store query (relation filter; RUN):
| expr | SQL legend-engine wrote | rows (IDs) |
|---|---|---|
| S8 `$r.I->in([3D, 'a'])` | `where "t_0".I in (3, 'a')` | **refused by H2 at execution**: `JdbcSQLDataException: Data conversion error converting "a"` (22018); legend-engine planned it without complaint |
| S16 `$r.F->in([0, 1]) && $r.D->in([1, 3])` | `where "t_0".F in (0, 1) and "t_0".D in (1, 3)` | [1] (row 4 has D=3.00 but F=3.0 not in (0,1)) |

## Group B: max, min and sort when equal numbers have different types

| expr | interpreted | compiled | legend-engine store-less | how |
|---|---|---|---|---|
| B1 `[3,3.0]->max()` | 3.0 Float | 3.0 Double | 3.0 Double (gen: `Arrays.<Number>asList(3L, 3.0).stream().reduce(Library::max)`) | RUN x3 |
| B2 `[3.0,3]->max()` | 3 Integer | 3 Long | 3 Long | RUN x3 |
| B3 `[3,3.0]->min()` | 3.0 Float | 3.0 Double | 3.0 Double (gen: `...reduce(Library::min)`) | RUN x3 |
| B4 `[3.0,3]->min()` | 3 Integer | 3 Long | 3 Long | RUN x3 |
| B5 `[3.0,3,1]->sort()` | [1 Integer, 3.0 Float, 3 Integer] | [1 Long, 3.0 Double, 3 Long] | [1 Long, 3.0 Double, 3 Long] (gen: `Library.sort(Arrays.<Number>asList(3.0, 3L, 1L))`) | RUN x3 |
| B6 `[3,3.0,1]->sort()` | [1, 3 Integer, 3.0 Float] | [1, 3 Long, 3.0 Double] | [1, 3 Long, 3.0 Double] | RUN x3 |
| B7 `[3D,3.0,3]->sort()` | [3 Decimal, 3.0 Float, 3 Integer] | [3 BigDecimal, 3.0 Double, 3 Long] | [3 BigDecimal, 3.0 Double, 3 Long] | RUN x3 |
| B8a `max(3, 3.0)` | 3 Integer | 3 Long | 3 Long (gen: `Library.compareInt(3L, 3.0) < 0 ? 3.0 : 3L`, the Pure body inlined) | RUN x3 |
| B8b `max(3.0, 3)` | 3.0 Float | 3.0 Double | 3.0 Double (gen: `Library.compareInt(3.0, 3L) < 0 ? 3L : 3.0`) | RUN x3 |
| B9 `[3.0,'a',3]->sort()` | [3.0 Float, 3 Integer, 'a'] | [3.0 Double, 3 Long, 'a'] | [3.0 Double, 3 Long, 'a'] (gen: `Library.sort(Arrays.<Object>asList(3.0, "a", 3L))`) | RUN x3 |
| B9x `['a',3,3.0]->sort()` | [3 Integer, 3.0 Float, 'a'] | [3 Long, 3.0 Double, 'a'] | [3 Long, 3.0 Double, 'a'] | RUN x3 |

So all three agree on group B: a mixed list sorts (numbers before strings), the sort is stable, equal numbers of
different types compare 0 and keep input order; list `max`/`min` return the LAST of the tied elements; two-argument
`max(l, r)` returns the LEFT on a tie. The result's type is the type of whichever element wins.

Inside an H2 store query (RUN):
| expr | SQL | answer |
|---|---|---|
| S9b `max($r.I->toOne(), $r.F->toOne())`, `min(..)` | `case when "t_0".I < "t_0".F then "t_0".F else "t_0".I end`, `case when "t_0".F < "t_0".I then "t_0".F else "t_0".I end` | row 1 (1, 1.0): max 1.0, min 1.0; row 4 (3, 3.0): 3.0, 3.0; row 3 (2, 2.5): 2.5, 2.0 -- the tie takes the Integer column's value, but the CASE's type is H2's (DOUBLE), so it comes back a Float 1.0 / 3.0 where store-less `max(3, 3.0)` answers the Integer 3 |
| K5 class form `max($r.i, $r.f)` | same CASE shape | rows 3, 5: 2.5, 1.5 |
| `max($r.I, $r.F)` with the relation's `[0..1]` columns (S9) | refused at compile: `Can't find a match for function 'max(Int[0..1],Float4[0..1])'` | -- |

## Group C: ordering across number types

| expr | interpreted | compiled | legend-engine store-less | how |
|---|---|---|---|---|
| C1 `1 < 1.0` | false | false | false (gen: `Library.compareInt(1L, 1.0) < 0`) | RUN x3 |
| C2 `1 <= 1.0` | true | true | true | RUN x3 |
| C3 `1 > 1.0` | false | false | false | RUN x3 |
| C4 `1 >= 1.0` | true | true | true | RUN x3 |
| C5 `1.0 < 1` | false | false | false | RUN x3 |
| C6 `1 < 1D` | false | false | false (gen: `Library.compareInt(1L, new BigDecimal("1")) < 0`) | RUN x3 |
| C7a `1.5 < 1.5D` | false | false | false (gen: `Library.compareInt(1.5, new BigDecimal("1.5")) < 0`) | RUN x3 |
| C7b `1.5 <= 1.5D` | true | true | true | RUN x3 |
| C8 `1 == 1.0` | **false** | **false** | **true** (gen: `return 1L == 1.0;`, Java's own primitive comparison) | RUN x3 |
| C9 `compare(1, 1.0)` | 0 | 0 | 0 (gen: `(long) Library.compareInt(1L, 1.0)`) | RUN x3 |
| C9x `compare(3, 3D)` | 0 | 0 | 0 | RUN x3 |
| C9y `compare(1.5, 1.5D)` | 0 | 0 | 0 | RUN x3 |
| C10a `1->cast(@Number) < 1.0->cast(@Number)` | false | false | false (gen: `Library.compareInt((Number) new Long(1L), (Number) new Double(1.0)) < 0`) | RUN x3 |
| C10b `1->cast(@Number) == 1.0->cast(@Number)` | false | false | **false** (gen: `Library.equals((Number) new Long(1L), (Number) new Double(1.0))`) | RUN x3 |

Ordering is by value in all three (no type check, no error). Equality is where they part (C8 vs C10b).

Inside an H2 store query (relation `extend`, S9a; RUN). SQL:
`"t_0".I is not distinct from "t_0".F as "eqIF"`, `"t_0".I is not null and "t_0".F is not null and "t_0".I < "t_0".F as "ltIF"`,
`... "t_0".I <= "t_0".F as "leIF"`, `"t_0".D is not distinct from "t_0".I as "eqDI"`, `"t_0".I = cast(1.0 as float) as "eqI1F"`,
`"t_0".I is not null and "t_0".I < cast(1 as Decimal(32,16)) as "ltI1D"`. Rows (ID, eqIF, ltIF, leIF, eqDI, eqI1F, ltI1D):
`1: true,false,true,true,true,false`; `2: true,false,true,true,false,true`; `3: false,true,true,false,false,false`;
`4: true,false,true,true,false,false`; `5: false,true,true,false,true,false`. Every comparison is H2's own, by value.
`compare($r.I->toOne(), $r.F->toOne())` in a store query (S14) is refused: `No SQL translation exists for the PURE
function 'compare_T_1__T_1__Integer_1_'`.

## Group D: functions written for Number

| expr | interpreted | compiled | legend-engine store-less | how |
|---|---|---|---|---|
| D1 `p::amongWholes(1.0)` | false | false | false (gen, body inlined: `Arrays.asList(0L, 1L).contains(1.0)`) | RUN x3 |
| D2 `p::amongWholes(1)` | true | true | true (gen: `Arrays.asList(0L, 1L).contains(1L)`) | RUN x3 |
| D3 `p::amongFloats(1)` | false | false | false | RUN x3 |
| D4 `p::amongFloats(1.0)` | true | true | true | RUN x3 |
| D5 `p::same(1, 1.0)` | **false** | **false** | **true** (gen, body inlined with the literals: `return 1L == 1.0;`) | RUN x3 |
| D6 `p::less(1, 1.0)` | false | false | false | RUN x3 |
| D7 `1.0->in([0, 1])` | false | false | false (gen: `Arrays.asList(0L, 1L).contains(1.0)`) | RUN x3 |
| D8 `1->in([0.0, 1.0])` | false | false | false | RUN x3 |

Inside an H2 store query (RUN; rows numerically equal: I=F at IDs 1,2,4; D=I at 1,2,4; D=F at all five; I in {0,1}
at 1,2,5; F in {0,1} at 1,2):
| filter | SQL legend-engine wrote | rows |
|---|---|---|
| S1 `$r.I->in([0.0, 1.0])` | `where "t_0".I in (cast(0.0 as float), cast(1.0 as float))` | [1, 2, 5] |
| K1 class `$r.i->in([0.0, 1.0])` | `where "root".I in (cast(0.0 as float), cast(1.0 as float))` | [1, 2, 5] |
| S2 `$r.F->in([0, 1])` | `where "t_0".F in (0, 1)` | [1, 2] |
| K4 class `$r.f->in([0, 1])` | `where "root".F in (0, 1)` | [1, 2] |
| S3 `$r.I == $r.F` | `where "t_0".I is not distinct from "t_0".F` | [1, 2, 4] |
| K2 class `$r.i == $r.f` | `where "root".I is not distinct from "root".F` | [1, 2, 4] |
| S4 `$r.I < $r.F` | `where "t_0".I is not null and "t_0".F is not null and "t_0".I < "t_0".F` | [3, 5] |
| S5 `$r.I == 1.0` | `where "t_0".I = cast(1.0 as float)` | [1, 5] |
| S6 `$r.D == $r.I` | `where "t_0".D is not distinct from "t_0".I` | [1, 2, 4] |
| K3 class `$r.d == $r.i` | `where "root".D is not distinct from "root".I` | [1, 2, 4] |
| S7 `$r.D == $r.F` | `where "t_0".D is not distinct from "t_0".F` | [1, 2, 3, 4, 5] |
| S10 `$r.I < 1D` | `where "t_0".I is not null and "t_0".I < cast(1 as Decimal(32,16))` | [2] |
| S11 `p::amongFloats($r.I->toOne())` | `where "t_0".I in (cast(0.0 as float), cast(1.0 as float))` | [1, 2, 5] |
| K7 class `p::amongFloats($r.i)` | same | [1, 2, 5] |
| S11b `p::amongWholes($r.F->toOne())` | `where "t_0".F in (0, 1)` | [1, 2] |
| S12 `p::same($r.I->toOne(), $r.F->toOne())` / K6 class | `where ... I is not distinct from ... F` | [1, 2, 4] |
| S13 `p::less($r.I->toOne(), $r.F->toOne())` / K8 class | `where "t_0".I < "t_0".F` | [3, 5] |
| S11-S13 with the relation's `[0..1]` columns, no `toOne()` | refused at compile: `Can't find a match for function 'p::amongFloats(Int[0..1])'` (likewise `p::same(Int[0..1],Float4[0..1])`, `p::less`) | -- |

In a store query the very same expressions that are `false` store-less (`1->in([0.0, 1.0])`, `p::amongFloats(1)`,
`1.0->in([0, 1])`) select the numerically equal rows: the database's comparison decides, no type check is made.

## The sources, read (pinned: legend-pure 5.99.0, legend-engine 4.145.0)

`LP` = `$(bazel info output_base)/external/+http_archive+legend_pure_src`, `LE` = `.../+http_archive+legend_engine_src`
(`release.MODULE.bazel:275-287`, the same releases as the jars run).

### Interpreted legend-pure: equality of two numbers
`LP/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/EqualityUtilities.java`
- `:60-71` `eq`: two primitives are equal iff the same primitive type AND equal values:
  ```java
  if ((left instanceof PrimitiveCoreInstance<?>) && (right instanceof PrimitiveCoreInstance<?>))
  {
      return Type.findPrimitiveTypeFromExtendedPrimitiveType(processorSupport.getClassifier(left), processorSupport).getName()
              .equals(Type.findPrimitiveTypeFromExtendedPrimitiveType(processorSupport.getClassifier(right), processorSupport).getName()) &&
              ((PrimitiveCoreInstance<?>) left).getValue().equals(((PrimitiveCoreInstance<?>) right).getValue());
  }
  ```
- `:102-105` `equal` of two primitives is `eq`; `:165-180` two lists are equal iff same size and element-wise `equal`.
- `natives/grammar/_boolean/equality/Equal.java:43-46` the `equal` native is `EqualityUtilities.equal(values, values)`.
- `:193-198` the hash used by `removeDuplicates`' set: `type.hashCode() ^ instance.getName().hashCode()` (type is part
  of it). `natives/essentials/collection/operation/RemoveDuplicates.java:66-76`: with no key and no eql,
  `EqualityUtilities.newCoreInstanceSet` and `instances.add(instance)` keeps the first of each equal group.

### Interpreted legend-pure: `<`, `compare`, `sort`
- `natives/grammar/_boolean/inequality/NumericComparisonPredicate.java:46-51` (LessThan.java:26-30 accepts `< 0`):
  ```java
  Number left = NumericUtilities.toJavaNumber(...);  Number right = NumericUtilities.toJavaNumber(...);
  return acceptComparison(NumericUtilities.compare(left, right));
  ```
- `natives/NumericUtilities.java:301-383` `compare(Number, Number)`: Long vs Long `Long.compare`; Long vs Double
  `BigDecimal.valueOf(l).compareTo(BigDecimal.valueOf(d))` (`:311`); Long vs BigDecimal `BigDecimal.valueOf(l).compareTo(bd)`
  (`:319`); BigDecimal vs anything via BigDecimal (`:363-381`). By value, exactly. (The interpreter holds a Float in a
  BigDecimal: the probe printed `java=BigDecimal:3.0` for a Float.)
- `natives/grammar/lang/Compare.java:76-93`: numbers first, `NumericUtilities.compare`; a number before a non-number:
  ```java
  Number num1 = getNumberValue(instance1, processorSupport);
  Number num2 = getNumberValue(instance2, processorSupport);
  if (num1 != null) { return (num2 == null) ? -1 : NumericUtilities.compare(num1, num2); }
  if (num2 != null) { return 1; }
  ```
  then dates, booleans, strings (`:95-129`), then `PRIMITIVE_TYPE_COMPARISON_ORDER` (`:51`: Integer, Float, Number,
  DateTime, StrictDate, Date, Boolean, String) for other mixed primitives (`:131-147`).
- `natives/essentials/collection/order/Sort.java:66-80`: no comparator -> `Compare.compare`, `collection.toSortedList(comparator)`
  (a stable sort: ties keep input order).

### Compiled legend-pure: equality, `<`, `compare`, `sort`, `removeDuplicates`
`LP/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/generation/processors/support/CompiledSupport.java`
- the natives: `natives/grammar/_boolean/equality/Equal.java` -> `CompiledSupport.equal`; `.../inequality/LessThan.java`
  -> `CompiledSupport.lessThan`; `natives/grammar/lang/Compare.java` -> `CompiledSupport.compare`;
  `natives/essentials/collection/order/Sort.java:33-37` -> `CompiledSupport.toSorted`.
- `:1086-1089` `equal` of two numbers: `return (right instanceof Number) && eq((Number) left, (Number) right);`
- `:994-1011` `eq(Number, Number)`, NO class check (the class check is only in `eq(Object,Object)`, `:979`):
  ```java
  if (left instanceof BigDecimal && right instanceof Double ||
          left instanceof Double && right instanceof BigDecimal)
  {
      return false;
  }
  ...
  left = left.equals(-0.0d) ? 0.0d : left;
  right = right.equals(-0.0d) ? 0.0d : right;
  return left.equals(right) || left.toString().equals(right.toString());
  ```
  so Decimal 3 vs Integer 3 is equal (both print `3`), Integer 1 vs Float 1.0 is not (`1` vs `1.0`), Float vs Decimal
  never. `:1070-1079` lists: same size and element-wise.
- `:1501-1514` `lessThan`: BigDecimal if either side is one; Long/Long by long; otherwise `doubleValue()`.
- `:544-598` `compare`/`compareInt`: same class -> `Comparable`; different classes and both numbers ->
  `compareUnmatchedNumbers` = `toBigDecimal(x).compareTo(toBigDecimal(y))` (`:597`, Double via `BigDecimal.valueOf`,
  `:629-631`); otherwise `PRIMITIVE_CLASS_COMPARISON_ORDER` (`:140`: Long, Double, PureDate, Boolean, String).
- `:907-919` `toSorted`: no key, no comparator -> `collection.toSortedList(CompiledSupport::compareInt)` (stable).
- `Pure.java:1031-1041` `removeDuplicates` (via `CoreGen.removeDuplicates`, resources `.../support/core/CoreGen.java:133-135`):
  `PureEqualsHashingStrategy.newMutableSet()`, `list.select(set::add)`; `map/PureEqualsHashingStrategy.java:31-40`:
  hash `CompiledSupport.safeHashCode` (`CompiledSupport.java:303-322`: `object.hashCode()`), equality
  `CompiledSupport.equal`. Long(3) and BigDecimal("3") hash 3 and 93, so the set never asks `equal` and keeps both
  although `equal` says they are equal (A1/A5 true, A7 = 2).

### The Pure definitions: max, min, in, contains, sort, removeDuplicates, compare, lessThan
- `LE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-standard/legend-engine-pure-functions-standard-pure/src/main/resources/core_functions_standard/math/aggregator/max.pure`
  (max/min/in live in legend-engine's `core_functions_standard`, not in legend-pure):
  - `:27-30` `max(left:Number[1], right:Number[1]):Number[1] { if($left < $right, | $right, | $left) }` -> a tie returns `left`.
  - `:57-60` `max(numbers:Number[1..*]):Number[1] { $numbers->fold({n, m | max($n, $m)}, $numbers->at(0)) }` (`:52-55`
    the `[*]` form the same with `first()`): `n` is the element, `m` the accumulator, so on a tie `max(n, m)` returns `n`,
    the later element: the LAST of the tied elements wins.
  - `min.pure:27-30` `min(left:Number[1], right:Number[1]) { if($right < $left, | $right, | $left) }` (tie: left);
    `min.pure:52-55` `min(numbers:Number[1..*]) { $numbers->fold({n, m | min($n, $m)}, $numbers->at(0)) }` (tie: the later element).
  - `collection/in.pure:18-26`: `in(value:Any[1], collection:Any[*]) { $collection->exists(x | $value == $x) }`.
- `LP/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/`
  - `essential/collection/boolean/contains.pure:42-45` `contains(collection:Any[*], value:Any[1]) { $collection->exists(x | $value == $x) }`.
  - `essential/collection/order/sort.pure:46-48` the native `sort<T,U|m>(col, key, comp)`; `:60-63` `sort(col) { sort($col, [], []) }`;
    its own PCT test `:101-104` `testMixedSortNoComparator`: `assertEquals([-2.0, 1, 5.0, 171, 342], [342, 5.0, -2.0, 171, 1]->sort())`.
  - `essential/collection/transformation/removeDuplicates.pure:78-81` `removeDuplicates(col) { $col->removeDuplicates([], []) }`
    (default equality; "The order of the result is unspecified", `:70-71`).
  - `grammar/functions/lang/compare.pure:17-23` the native `compare<T>(a:T[1], b:T[1]):Integer[1]`; its PCT tests
    `:54-61` (`compare(1, 2.2) < 0` ...) and `:102-107` (`compare(9999999999999992d, 9999999999999992) == 0`) state compare is by value.
  - `grammar/functions/boolean/inequality/lessThan.pure:43-49` the native `lessThan(left:Number[1], right:Number[1])`.
  - `grammar/functions/boolean/equality/eq.pure:18-24` the native `eq(left:Any[1], right:Any[1])`.

### legend-engine store-less: its own generated Java
- `LE/legend-engine-xts-java/legend-engine-xt-javaGeneration-conventions-essential-pure/src/main/resources/core_external_language_java_conventions_essential/grammar/boolean.pure`
  - `:29-30` `eq` aliased to `equal`, `equal` -> `equalTo`; `:111-117`:
    ```
    let eq = if ($left.type->isPrimitive() && $right.type->isPrimitive(),
                 | $left->j_eq($right),
                 | $library->j_invoke('equals', [$left, $right], javaBoolean())
              );
    ```
    two Java primitives (`long`, `double`) -> Java `==`, whose numeric promotion makes `1L == 1.0` true (C8, D5);
    boxed (`Number`, a cast, a list) -> `Library.equals` (C10b, A5, A6).
  - `:65` `lessThan_Number_1__Number_1` -> `codeCompare` (`:100-109`): Long/Long or Double/Double -> Java `<`, else
    `Library.compareInt(l, r) < 0`.
- `.../core_external_language_java_conventions_essential/essential/collection.pure:41` `contains` -> `$collection->j_invoke('contains', $obj)`
  (Java `List.contains`, i.e. `Object.equals`: `Long(1).equals(Double(1.0))` and `BigDecimal(3).equals(Long(3))` are false);
  `:51` `sort` -> `Library.sort`; `:66` `removeDuplicates` -> `Library.removeDuplicates(col, null, null)`.
- `LE/legend-engine-xts-java/legend-engine-xt-javaGeneration-conventions-standard-pure/src/main/resources/core_external_language_java_conventions_standard/standard/collection.pure:35-36`
  `in` -> `$collection->j_invoke('contains', $obj)` (the same `List.contains`).
- `.../grammar/lang.pure:34` `compare` -> `Library.compareInt`.
- `LE/legend-engine-xts-java/legend-engine-xt-javaPlatformBinding-pure/src/main/resources/core_java_platform_binding/legendJavaPlatformBinding/planConventions/mathLibrary.pure:36-45`
  `max(Number[*])` -> `stream().reduce(Library::max)`, `min` likewise; `max(Number[1],Number[1])` -> `Library.max`
  (but the run of `max(3, 3.0)` shows the body inlined instead: `compareInt(3L, 3.0) < 0 ? 3.0 : 3L`).
- `LE/legend-engine-core/legend-engine-core-base/legend-engine-core-executionPlan-execution/legend-engine-executionPlan-dependencies/src/main/java/org/finos/legend/engine/plan/dependencies/util/Library.java`
  - `:293-329` `equals`: lists element-wise; numbers `eq`; `:331-347` `eq(Number, Number)` -- the same text as compiled
    legend-pure's `CompiledSupport.eq(Number, Number)`: BigDecimal/Double false, else `equals || toString().equals(..)`.
  - `:763-771` `max(l, r) { return safeCompareNumbers(l, r) > 0 ? l : r; }` (a tie returns `r`, so `reduce` keeps the
    later element), `min` `< 0 ? l : r`.
  - `:774-815` `compareInt`: `Objects.equals` -> 0; different classes and both numbers -> `compareUnmatchedNumbers`
    = `toBigDecimal(l).compareTo(toBigDecimal(r))` (`:813`); otherwise `PRIMITIVE_CLASS_COMPARISON_ORDER` (`:65`: Long,
    Double, PureDate, Boolean, String).
  - `:884-889` `sort`: `Collections.sort(copy, DEFAULT_COMPARATOR)` (stable). `:916-921` `removeDuplicates` with no
    key/eql: `col.stream().distinct()` (Java `equals`/`hashCode`: Long 3 and BigDecimal 3 are distinct).

### legend-engine inside a relational store: the database's comparison, no type check
`LE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/`
- `pureToSQLQuery/pureToSQLQuery.pure:10791-10792` `equal` and `eq` -> `processEquals`; `:8363-8417` processEquals
  processes both sides and emits a DynaFunction, checking only multiplicity (`:8379-8383`) and that each side's type is
  a DataType/Enum (`validateParamHasDataTypeReturnType`, `:8519-8527`), never that the two types match;
  `:8447-8462` `nullSafeEqualsOperation`: both sides `[0..1]` -> `nullSafeEqual`, else `equal`.
- `sqlQueryToString/extensionDefaults.pure:219` `dynaFnToSql('equal', ..., '%s = %s')`, `:243` `'lessThan' '%s < %s'`,
  `:231` `'in' '%s in %s'`; `sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure:199` `'nullSafeEqual' '%s is not distinct from %s'`.
- `pureToSQLQuery.pure:10526-10527` `in` -> `processIn`; `:5918-5933`: the collection's elements must be literals
  (`'IN is supported only for literal values or negative numbers'`), put in a `LiteralList` as they are; no type check
  against the value's column (hence `I in (3, 'a')`, which H2 then refuses).
- `pureToSQLQuery.pure:10570` `lessThan_Number_1__Number_1` -> `processDynaFunction` (the `lessThan` dyna function).
- NOT traced: which literal processor writes a Float literal as lowercase `cast(0.0 as float)` and a Decimal as
  `cast(1 as Decimal(32,16))` (h2Extension2_1_214.pure:152's Float processor is `CAST(%s AS FLOAT)`, upper case, so it is
  not obviously that one); the text above is the run's.

## Surprises

1. **The three runtimes disagree on cross-type equality, and not in one direction.**
   - interpreted legend-pure: a different type is never equal (`EqualityUtilities.eq`). False everywhere in A, C8, D5.
   - compiled legend-pure: equal when the TEXT matches (`CompiledSupport.eq(Number,Number)`): Decimal 3 == Integer 3 is
     **true** (A1 `[3,'a']->contains(3D)` true, A4 `3->in([3D,'a'])` true, A5 true), but Integer 1 == Float 1.0 is
     **false** (C8, D5) and Float == Decimal always false (A2).
   - legend-engine store-less: depends on the shape the generator sees. Two primitive literals -> Java `==`, so
     `1 == 1.0` is **true** (C8) and `p::same(1, 1.0)` is **true** (the body inlined with the literals, D5); the same
     values through `cast(@Number)` -> `Library.equals` -> **false** (C10b); `[3,'a'] == [3D,'a']` -> `Library.equals`
     -> **true** (A5), yet `[3,'a']->contains(3D)` and `3->in([3D,'a'])` -> Java `List.contains` -> **false** (A1, A4).
     So within legend-engine alone, `1 == 1.0` is true while `1->in([0.0, 1.0])`, `p::amongFloats(1)` and
     `1->cast(@Number) == 1.0->cast(@Number)` are false.
   - legend-engine in a store: the database decides by value: `I == F`, `I in (0.0, 1.0)`, `F in (0, 1)`, `D == I`,
     `D == F` all select the numerically equal rows. `p::amongFloats(1)` is false store-less and selects the I=1 rows
     in the store.
2. **Compiled legend-pure contradicts itself on Decimal 3 and Integer 3**: `==`/`contains`/`in` say equal (A1, A4, A5),
   `removeDuplicates` keeps both (A7 = 2), because its hash set hashes with Java `hashCode` (3 vs 93) and so never asks
   `equal`. legend-engine's store-less `removeDuplicates` (`stream().distinct()`) keeps both too, while its
   `Library.equals` says they are equal.
3. **Ordering is by value in all three, and every runtime agrees on every row of groups B and C** (1 < 1.0 false,
   1 <= 1.0 true, compare(1, 1.0) = 0, 1.5 <= 1.5D true; sort stable, numbers before strings; list max/min the last tied
   element; `max(l, r)` the left one). The winner keeps its own type, so `[3, 3.0]->max()` is the Float 3.0 and
   `[3.0, 3]->max()` the Integer 3, in all three.
4. **In a store the tie's type changes**: `max($r.I, $r.F)` on (3, 3.0) picks the Integer column's value through
   `case when I < F then F else I end`, but H2 types the CASE as DOUBLE and the answer is the Float 3.0; store-less
   `max(3, 3.0)` answers the Integer 3.
5. **Refusals**: legend-engine plans `$r.I->in([3D, 'a'])` as `I in (3, 'a')` and H2 refuses it at execution (data
   conversion error); `compare` has no SQL translation in a store query; `max`/`min` and the `Number[1]` functions are
   refused on a relation's `[0..1]` columns (`Can't find a match for function 'max(Int[0..1],Float4[0..1])'`) until
   `->toOne()` is added; the class/mapping form (`[1]` properties) takes them directly. legend-pure refused none of the
   expressions (every one compiled and ran in both modes).
6. The earlier record `core/src/test/resources/upstream-api/s127-number-equality.txt` is confirmed: engine `1 == 1.0`
   true, CompiledSupport false, interpreted false; what it did not show is that the engine's `true` is the Java `==`
   of two primitive literals only (C10b, A3, D7, D8 are false in the engine).
