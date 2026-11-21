# Item 3: upstream's assert behaviour, read from the pinned sources

Read-only research, 2026-11-21. Every claim cites a file and line in the pinned checkouts. Nothing here is from
memory. Where something is inferred and not read directly, it says so.

**Path aliases used below**

| Alias | Path |
|---|---|
| `PURE` | `/root/.cache/bazel/_bazel_root/278d50ab96a4cceb730186faf38c724e/external/+http_archive+legend_pure_src` |
| `ENGINE` | `/root/.cache/bazel/_bazel_root/278d50ab96a4cceb730186faf38c724e/external/+http_archive+legend_engine_src` |
| `T/` | `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/tests/` |
| `STR/` | `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/string/toString/` |
| `INT/` | `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/` |
| `CMP/` | `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/` |
| `CS` | `CMP/generation/processors/support/CompiledSupport.java` |
| `COREGEN` | `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/resources/org/finos/legend/pure/runtime/java/compiled/generation/processors/support/core/CoreGen.java` |
| `PEX` | `PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/exception/PureException.java` |
| `PCTT` | `PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/pct/shared/PCTTools.java` |
| `PTBC` | `CMP/testHelper/PureTestBuilderCompiled.java` |

---

## 0. The one native everything reduces to

Every assert in `meta::pure::functions::asserts::*` except `assertError` ends in the native
`assert(condition:Boolean[1], messageFunction:Function<{->String[1]}>[1]):Boolean[1]` (`T/assert.pure:17`). The overloads
are sugar:

```pure
function meta::pure::functions::asserts::assert(condition:Boolean[1], formatString:String[1], formatArgs:Any[*]):Boolean[1]
{   assert($condition, | format($formatString, $formatArgs));   }   // T/assert.pure:19-22
function meta::pure::functions::asserts::assert(condition:Boolean[1], message:String[1]):Boolean[1]
{   assert($condition, | $message);   }                             // T/assert.pure:24-27
function meta::pure::functions::asserts::assert(condition:Boolean[1]):Boolean[1]
{   assert($condition, 'Assert failed');   }                       // T/assert.pure:29-32
```

Every assert function has the same four-overload family: a default message, `message:String[1]`,
`formatString:String[1], formatArgs:Any[*]`, and `message:Function<{->String[1]}>[1]`. The last three just pass
the message to `assert`. Only the **default-message** overload has wording of its own. Section 1 lists each function's
comparison and its default wording.

The native's semantics:
- **Interpreted** (`INT/natives/essentials/tests/Assert.java:53-59`): if the condition is false, it evaluates the message
  lambda (`executeLambdaFromNative`, line 56) and throws `new PureAssertFailException(functionExpressionCallStack.peek().getSourceInformation(), message, callStack)`
  (line 57). Otherwise it returns `params.get(0)`, the condition itself.
- **Compiled** (`CS:1575-1583`): `if (!condition) { String message = function.execute(Lists.immutable.empty(), es); throw new PureAssertFailException(sourceInformation, message); } return true;`.
  The native is wired by `CMP/generation/processors/natives/essentials/tests/Assert.java:30`, with `hasSrcInformation=true`,
  so the source information is the `assert(` call expression's own location.

**In both runtimes the message lambda runs only when the condition is false.** Both evaluate it exactly once and
before the throw.

---

## 1. Every assert function: signature, comparison, default wording

`format` substitutes `%r` with `toRepresentation(x)` and `%s` with `toString(x)` (section 5). The "expected/actual"
frame is always `'\nexpected: ' … '\nactual:   ' …`. It **starts with a newline**. `expected:` has one space after
the colon and `actual:` has **three**, so the two values line up.

| Function (file:line) | Compares with | Default message (verbatim Pure) |
|---|---|---|
| `assert(Boolean[1])` `T/assert.pure:29-32` | the Boolean | `'Assert failed'` |
| `assertFalse(Boolean[1])` `T/assert.pure:34-37`; the other overloads are in `T/assertFalse.pure:17-30` | `!$condition` | `'Assert failed'` (via `assert(!$condition)`) |
| `fail()` / `fail(String)` / `fail(fmt,args)` / `fail(fn)` `T/fail.pure:17-35` | `assert(false…)` | `'Assert failed'` |
| `assertEq(expected:Any[1], actual:Any[1])` `T/assertEq.pure:17-35` | `eq($expected, $actual)` (line 24) | `'\nexpected: %r\nactual:   %r', [$expected, $actual]` (line 19) |
| `assertNotEq(notExpected:Any[1], actual:Any[1])` `T/assetNotEq.pure:15-33` (upstream's file name is misspelled) | `!eq(...)` (line 22) | `'%r should not equal %r', [$notExpected, $actual]` (line 17) |
| `assertEquals(expected:Any[*], actual:Any[*])` `T/assertEquals.pure:17-37` | `equal($expected, $actual)` (lines 26, 31, 36) | **two forms** (line 19-21): if `eq($expected->size(),1) && eq($actual->size(),1)` then `'\nexpected: %r\nactual:   %r', [$expected->toOne(), $actual->toOne()]`; else `'\nexpected: %s\nactual:   %s', [$expected->map(x\|$x->toRepresentation())->joinStrings('[', ', ', ']'), $actual->map(...)->joinStrings('[', ', ', ']')]` |
| `assertNotEquals(notExpected:Any[*], actual:Any[*])` `T/assertNotEquals.pure:15-35` | `!equal(...)` (line 24) | singleton pair: `'%r should not equal %r'` (line 18). Otherwise a lambda (line 19): `notExpected` written as a `[..]` list + `' should not equal '` + `actual` written as a `[..]` list |
| `assertSize(collection:Any[*], size:Integer[1])` `T/assertSize.pure:17-35` | `assertEq($size, $collection->size())` (line 24) | `'expected size: %s, actual size: %s', [$size, $collection->size()]` (line 19) |
| `assertNotSize(collection:Any[*], size:Integer[1])` `T/assertNotSize.pure:15-33` | `assertNotEq($size, $collection->size())` (line 22) | lambda `format('size should not equal: %s', [$size])` (line 17) |
| `assertEmpty(collection:Any[*])` `T/assertEmpty.pure:18-36` | `$collection->isEmpty()` (line 25) | `'%s is not empty', [^List<Any>(values=$collection)]` (line 20). **`%s` of a `List`** gives elements by `toString`, **no quotes** (section 5) |
| `assertNotEmpty(collection:Any[*])` `T/assertNotEmpty.pure:17-35` | `assertFalse($collection->isEmpty())` (line 24) | `'Expected non-empty collection'` (line 19) |
| `assertContains(collection:Any[*], value:Any[1])` `T/assertContains.pure:20-38` | `$collection->contains($value)` (line 27), which is `exists(x\|$value == $x)` (`…/essential/collection/boolean/contains.pure:44`) | lambda `format('%s does not contain %r', [<collection as toRepresentation list>, $value])` (line 22) |
| `assertNotContains(collection:Any[*], value:Any[1])` `T/assertNotContains.pure:19-37` | `!contains` (line 26) | lambda `format('%s should not contain %r', [...])` (line 21) |
| `assertSameElements(expected:Any[*], actual:Any[*])` `T/assertSameElements.pure:17-35` | `assertEquals($expected->sort(), $actual->sort())` (line 24) | lambda (line 19): `$expected->sort()->map(toRepresentation)->joinStrings('\nexpected: [', ', ', ']') + $actual->sort()->map(toRepresentation)->joinStrings('\nactual:   [', ', ', ']')`. This is **always the list form**, sorted, even for one element |
| `assertIs(expected:Any[1], actual:Any[1])` `T/assertIs.pure:17-35` | `is(...)`, identity (line 24) | `'\nexpected: %r\nactual:   %r'` (line 19) |
| `assertIsNot(notExpected:Any[1], actual:Any[1])` `T/assertIsNot.pure:17-35` | `!is(...)` (line 24) | `'Should not be identical to: %r', [$actual]` (line 19). Only `actual` is printed |
| `assertInstanceOf(instance:Any[1], type:Type[1])` `T/assertInstanceOf.pure:17-35` | `$instance->instanceOf($type)` (line 24) | lambda `format('expected %r to be an instance of %s, actual: %s', [$instance, $type->elementToPath(), $instance->type()->toOne()->elementToPath()])` (line 19). Example: `'expected 3 to be an instance of Boolean, actual: Integer'` (line 55) |
| `assertEqWithinTolerance(expected:Number[1], actual:Number[1], delta:Number[1])` `T/assertEqWithinTolerance.pure:17-35` | `abs($expected - $actual) <= abs($delta)` (line 24). **This is the only tolerance in the family.** `assertEquals` has no delta | `'\nexpected: %r\nactual:   %r', [$expected, $actual]` (line 19). The delta is not printed |
| `assertError(f, errorMessageMatcher)` **native** `T/assertError.pure:43` | see section 2.3 | none of its own |
| `assertError(f, message:String[1], line:Integer[0..1], column:Integer[0..1])` `T/assertError.pure:71-78` | the matcher runs `assertEquals($message, $msg, …)`, **an exact string match**, then the optional line and column | `'Execution error message mismatch.\nThe actual message was "%s"\nwhere the expected message was:"%s"', [$msg, $message]` (line 74). There is **no space** after `was:`. Line: `'Execution error line mismatch. Actual: %d where expected: %d'` (line 75). Column: `'Execution error column mismatch. Actual: %d where expected: %d'` (line 76) |
| `assertError(f, message:String[1])` `T/assertError.pure:102-105` | `assertError($f, $message, [], [])` | as above |

Engine additions, outside legend-pure's platform:

| Function | Compares | Default message |
|---|---|---|
| `meta::pure::functions::asserts::assertJsonStringsEqual(expected:String[1], actual:String[1])` `ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/corefunctions/testExtension.pure:38-41` | `equalJsonStrings` (native `ENGINE/…/core_functions_json/json.pure:29`), compiled as `CS:1119-1132`. That parses both with json-simple `JSONParser` (`CS:99`) and compares with Java `equals`, so object keys are order-insensitive (HashMap), arrays are order-sensitive, and `1` ≠ `1.0` (Long vs Double; inferred from json-simple types). A parse failure throws `PureExecutionException("Failed to parse JSON string. Invalid JSON string. " + …)` (`CS:1130`) | lambda `'JSON strings don\'t represent semantically same object \n expected: ' + $expected + '\n actual: ' + $actual` (line 40). The values are raw, unquoted. Note the space before each `\n` and the leading space before `expected:`/`actual:` |
| `meta::pure::functions::asserts::assert<T>(collection:T[*], func:Function<{T[1]->Boolean[1]}>[1])` `testExtension.pure:48-58` | a fold that calls `assert($func->eval($item))` until the first false, then `assert($r)` | `'Assert failed'` |
| `meta::pure::functions::relation::assertTdsEquivalent(one, two, delta[, timeDeltaInSeconds])` `ENGINE/…/core_functions_relation/relation/functions/tdsEquivalent.pure:20-50` | `$oneCol->size() == $twoCol->size() && $oneCol.name == $twoCol.name && $one->size() == $two->size() && if(<per-column fold>, \|true, \|fail(...))` (lines 29-49). The fold compares numbers within `delta`, dates within `timeDeltaInSeconds` via `dateDiff(... SECONDS)`, variants by `toString`, and everything else with `$oneVals == $twoVals` | `fail($one->toString(true)+'\n is not equivalent to:\n'+$two->toString(true))` (line 48). **Verdict quirk:** a column-count, column-name or row-count mismatch short-circuits `&&` (both runtimes short-circuit: `INT/natives/grammar/_boolean/operation/And.java:48-66`, `CMP/generation/processors/natives/grammar/_boolean/operation/And.java:32`). The function then **returns `false` without throwing**, and a test whose statement returns `false` **passes** (section 4.3) |
| `meta::relational::functions::asserts::assertSameSQL(sqlString, result)` `ENGINE/…/core_relational/relational/tests/testAssert.pure:18-26` | `assertEquals($sqlString, $result)` (one String each) | `assertEquals`' `%r` form, so both SQL strings appear quoted and escaped |
| `meta::relational::functions::asserts::sqlContains(subString, result)` `testAssert.pure:28-37` | `$sql->contains($subString)` | `$sql + ' does not contain ' + $subString`, raw |

How often the relational corpus uses each one, counted by grep over `core_relational`: `assertEquals` 3339,
`assertSize` 764, `assertSameElements` 763, `assertSameSQL` 505, `assert` 358, `assertJsonStringsEqual` 192, `fail` 144,
`assertEq` 51, `assertEmpty` 37, `assertNotEmpty` 35, `assertFalse` 31, `assertEqWithinTolerance` 22, `assertContains` 18,
`assertIs` 4, `assertError` 4, `assertTdsEquivalent` 2, `assertInstanceOf` 2.

### 1.1 The comparisons in detail (what makes pass/fail)

**`equal`** (used by `assertEquals` and `assertNotEquals`, by `assertSameElements` through `assertEquals`, and by `==`
and therefore `contains`). It is declared at
`PURE/…/platform/pure/grammar/functions/boolean/equality/equal.pure:18-28`: "Identity, primitive equality, collection
equality, and model-defined equality … `meta::pure::profiles::equality.Key`".
- **Collections: same size, element-wise, ORDER-SENSITIVE.** Compiled: `CS:1070-1080` and `iteratorsEqual` at
  `CS:1107-1117`. Interpreted: `INT/EqualityUtilities.java:165-180`. Upstream's tests confirm it:
  `assertFalse(['a', 'b'] == ['b', 'a'])` (`equal.pure:262`), `assertFalse(['a','b'] == ['a','b','c'])` (`equal.pure:256`).
- **Empty:** `[] == []` is true, and `[] == 1` is false (`equal.pure:232-236`). A one-element collection equals its
  element (`CS:1079, 1084`).
- **Numbers, compiled** (`CS:1086-1088` → `eq(Number,Number)` `CS:994-1011`):
  - Decimal vs Float is **always false** (`CS:997-1001`).
  - `-0.0` is normalised to `0.0` (`CS:1008-1009`).
  - Otherwise the test is `left.equals(right) || left.toString().equals(right.toString())` (`CS:1010`). So **Integer
    `1` ≠ Float `1.0`**: `Long.equals(Double)` is false and `"1"` vs `"1.0"` differ.
  - **Decimal equality is scale-sensitive:** `BigDecimal.equals` and `toString` both keep the scale. Evidence:
    engine's Java-binding manifest records `"\nexpected: 123.12300D\nactual:   123.123D"` as a failure
    (`ENGINE/legend-engine-xts-java/legend-engine-xt-javaPlatformBinding-PCT/src/main/resources/pct-manifests/java/Test_JAVA_EssentialFunction_manifest.json:246`).
  - A Decimal **does** equal an Integer when the strings agree, because `BigDecimal("1").toString()` is `"1"`. This is
    inferred from `CS:1010`; there is no upstream test.
- **Numbers, interpreted** (`INT/EqualityUtilities.java:67-71`): both sides must have the same primitive type name, and
  then `getValue().equals(...)`. So Integer never equals Float or Decimal. Float values are held as `BigDecimal`
  (`PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/coreinstance/primitive/FloatCoreInstance.java:22`).
  **This is a compiled-vs-interpreted divergence for Decimal-vs-Integer.**
- **Dates:** `AbstractPureDate.equals`
  (`PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/coreinstance/primitive/date/AbstractPureDate.java:52-58`)
  compares year, month, day, hour, minute and second, and then `Objects.equals(getSubsecond())`. The subsecond is
  compared as a string, so equality is **precision-sensitive**:
  - `%2014 != %2014-01-01` (`equal.pure:75`).
  - DuckDB's PCT manifest records `"\nexpected: %2021-06-21T09:37:37.4990000+0000\nactual:   %2021-06-21T09:37:37.499+0000"`
    as a failure
    (`ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-PCT/src/main/resources/pct-manifests/relational-duckdb/EssentialFunctions_manifest.json:164`,
    and lines 197, 200).
  - Time zones are normalised: `%2014-02-27T10:01:35.231+0500 == %2014-02-27T05:01:35.231+0000` (`equal.pure:65`).
- **Class instances:** identity, unless the class has `<<equality.Key>>` properties.
  - Compiled default `pureEquals` is `super.equals` (`CMP/generation/processors/support/coreinstance/AbstractCompiledCoreInstance.java:87-90`).
    A class with keys gets a generated `pureEquals` that checks the same class plus `CompiledSupport.equal` on each key
    property (`CMP/generation/processors/type/_class/ClassImplProcessor.java:652-688`).
  - Interpreted (`INT/EqualityUtilities.java:107-135`): different classifiers give false, no keys give false, and
    otherwise each key is compared.
  - `List` and `Pair` have Key properties (`PURE/…/platform/pure/anonymousCollections.pure:19-20, 35`).
- **`eq`** (used by `assertEq`, `assertNotEq`, `assertSize`, `assertNotSize`), declared at
  `…/grammar/functions/boolean/equality/eq.pure:18-24` ("Identity or primitive equality"). Compiled (`CS:969-992`):
  identity; then a different Java class gives false; then numbers through `eq(Number,Number)`; then Boolean, String,
  PureDate, StrictTime and Quantity through `.equals`; **anything else is false**. So `assertEq(^P(...), ^P(...))` fails
  even when `assertEquals` would pass. Interpreted: `INT/EqualityUtilities.java:60-81`.
- **`is`** (used by `assertIs` and `assertIsNot`): pointer identity (`…/essential/boolean/equality/is.pure:18-23`).
  - Interpreted: `left == right` on CoreInstances (`INT/natives/grammar/_boolean/equality/Is.java:46`).
  - Compiled: `.equals` for enum and primitive mixes, otherwise Java `==`
    (`CMP/generation/processors/natives/grammar/_boolean/equality/Is.java:47-61`). For primitives the two runtimes
    can therefore disagree.
- **`sort()` in `assertSameElements`**, compiled default comparator `CS:550-588`:
  - Numbers of different classes compare by `BigDecimal`.
  - Otherwise unmatched classes order by `Long, Double, PureDate, Boolean, String` (`CS:140, 570-578`).
  - Non-`Comparable` instances of one class order by hash code (`CS:587`).
  - Upstream's test pins the cross-type order: `'\nexpected: [1, 3, \'2\']\nactual:   [1, 4, 5, \'2\']'` (`T/assertSameElements.pure:51`).
- **No float tolerance anywhere** except `assertEqWithinTolerance` and `assertTdsEquivalent`'s `delta`.

---

## 2. How a failing assert surfaces

### 2.1 The exception and its two strings

- `PureAssertFailException extends PureExecutionException`
  (`PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/exception/PureAssertFailException.java:24`).
  Its `getExceptionName()` returns `"Assert failure"` (line 82-85). `PureExecutionException.getExceptionName()` returns
  `"Execution error"` (`PURE/…/m3/exception/PureExecutionException.java:125-128`).
- `getInfo()` is the **raw message**, the string the message lambda returned (`PEX:77-80`).
- `getMessage()` is `<ExceptionName> at (resource:<sourceId> line:<L> column:<C>), "<info>"` (`PEX:97-113, 257-260, 279-298`).
  The location is `" line:L column:C"`, or `" lines:LcC-LcC"` when it spans lines (`PEX:306-336`). A missing location
  prints `??` (`PEX:281-284`).
  Example: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: 1\nactual:   2"`.
  The line and column in this example are illustrative. The format is from `PEX`.
- Which string each consumer reads:
  - The test runners report `getMessage()`.
  - `assertError`'s matcher receives `getInfo()` (2.3).
  - PCT reads both (3).

### 2.2 Interpreted vs compiled

| Aspect | Interpreted | Compiled |
|---|---|---|
| assert's source info | the top of the call stack at the native call (`INT/natives/essentials/tests/Assert.java:57`). Then `FunctionExecutionInterpreted` **moves it to the user's call site** when every frame is inside `/platform/pure/essential/tests/` (`INT/FunctionExecutionInterpreted.java:845-865`) | the source info of the `assert(` call expression (`hasSrcInformation=true`, `CMP/…/natives/essentials/tests/Assert.java:30`). For a wrapper such as `assertEquals`, that location is **inside the platform file** (e.g. `assert.pure`'s inner `assert(` call) |
| Raw Java `RuntimeException` from a native | **wrapped** into `PureExecutionException(sourceInfo, e.getMessage(), …)` (`INT/FunctionExecutionInterpreted.java:883-911`), so `assertError` can catch it | **not wrapped**. Generated function bodies rethrow a `RuntimeException` as it is (`CMP/generation/processors/FunctionProcessor.java:190-201`), so `assertError` **cannot catch it** (`COREGEN:352` catches only `PureExecutionException`) and the test ERRORs |
| Message lambda | `executeLambdaFromNative(...)` and then `.getName()` (`Assert.java:56`) | `function.execute(Lists.immutable.empty(), es)` (`CS:1579`) |

### 2.3 `assertError`, the native

Interpreted (`INT/natives/essentials/tests/AssertError.java:59-72`) and compiled (`COREGEN:345-357`) behave the same way:

```java
try {
    <evaluate f>;
    throw new PureAssertFailException(<assertError call's source info>, "No error was thrown");
} catch (PureExecutionException e) {          // PureAssertFailException IS-A PureExecutionException
    <evaluate matcher>(e.getInfo() == null ? "" : e.getInfo(), <e's source info or null>);
}
return true;
```

The consequences, each read from those lines:
1. **The `"No error was thrown"` throw is inside its own `try`, so its own `catch` catches it.** The matcher then
   receives `"No error was thrown"` and the `assertError` call's own location.
   - For the String forms, a body that raises nothing therefore fails with the **mismatch** wording, not with
     `"No error was thrown"`:
     `Execution error message mismatch.\nThe actual message was "No error was thrown"\nwhere the expected message was:"<expected>"`.
   - **If the expected message is literally `No error was thrown`, the assert passes.**
   - With the native matcher form, a matcher that asserts nothing (e.g. `{msg, si | true}`) **passes when `f` raises
     nothing**.
   - Lite's `EdgeHostWords.noError`
     (`core/src/main/java/com/legend/edge/EdgeHostWords.java:193-196`)
     and `UpstreamVerdictWords.java:127-128` report `"No error was thrown"` as the final message. That differs from
     upstream in both wording and verdict for these two edge cases.
2. The matcher sees **`getInfo()`**, the raw message without the `Execution error at (...)` frame. A null info becomes `""`.
3. An inner assert failure is caught too. `assertError(|assert(false, |format('Test message: %d', 2 + 3)), 'Test message: 5', 70, 19)`
   passes (`T/assert.pure:70`).
4. Message comparison is **exact**, not a substring. `assertEquals($message, $msg, …)` (`T/assertError.pure:74`) is
   `equal` on two Strings. The doc string says so too: "The message must match exactly, not merely contain the
   expected text" (`T/assertError.pure:50-51, 87`).
5. The line and column checks run only when they are given: `$line->isNotEmpty() && assertEquals(...)` short-circuits
   (`T/assertError.pure:75-76`). They compare against `$si.line->toOne()` and `$si.column->toOne()`. If the caught
   error had no source info, `toOne()` itself errors (inferred from the multiplicity; not tested upstream).
6. In compiled mode, plan-execution errors from engine's `meta::legend::execute` are wrapped as
   `PureExecutionException("Failed to execute plan - <Class> - <msg>")` **only while the result streams**
   (`ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-planExecution/legend-engine-pure-runtime-java-extension-shared-functions-planExecution/src/main/java/org/finos/legend/engine/pure/runtime/execution/shared/LegendExecute.java:56-78`).
   `executeWithArgs` at line 56 sits outside that `try`.

### 2.4 What a message lambda can do

It is any Pure expression returning `String[1]`, so it can call `format`, `toRepresentation`, `map`, `joinStrings`,
`sort`, or user functions. It runs **only on failure** (section 0). If it raises, that error escapes **in place of** the
assert failure:
- In `executeTest` it is classified ERROR, not FAIL (`CS:2370-2379`).
- Inside `assertError` it is caught like any `PureExecutionException`.

Example: `format`'s "Too few arguments passed to format function. Format expression "…", number of arguments [n]"
(`INT/natives/essentials/string/toString/Format.java:199`).

---

## 3. The PCT harness: expected failures and message matching

The pinned engine's DuckDB PCT runs through the **surveyor**:
- `Test_Relational_DuckDB_PCT` calls `PureTestBuilderCompiled.buildPCTSurveyorSuite(reportScope, manifestPath)`
  (`ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-PCT/src/test/java/org/finos/legend/engine/plan/execution/stores/relational/test/duckdb/pct/Test_Relational_DuckDB_PCT.java:78-83`;
  `PLATFORM = "compiled"` at line 42).
- That builder runs `meta::pure::test::surveyor::runPCTTestsFromPath` (`PTBC:373-377`, `T/surveyor.pure:167-170`).
- The surveyor calls the native `executePCTTest(testFn, adapter, exclusions)` (`T/surveyor.pure:60, 135`).
- The manifest is JSON: `{"adapter": …, "exclusions": [{"test": <fqn>, "expectedError": <string>}]}`. DuckDB's six
  manifests live in `…/duckdb-PCT/src/main/resources/pct-manifests/relational-duckdb/`. Assert-failure exclusions
  (those containing `expected: `) number 21 in Essential, 3 in Grammar, 14 in Standard, 2 in Variant, 1 in Unclassified
  and 1 in Relation.

**Compiled `executePCTTest`** (`CS:2411-2547`):
- A pass with an exclusion is FAIL: `formatUnexpectedPass` (`CS:2460-2464`, `PCTT:246-251`).
- A `PureAssertFailException` (`CS:2471-2500`) counts as matched when **any** of these holds:
  - `errorMsg.contains(expectedError)`, where `errorMsg` is `getInfo()` (or `getMessage()` when `getInfo()` is null);
  - `e.getExceptionName().contains(expectedError)`. So an expected error of `"Assert failure"` matches **any** assert failure;
  - `e.getMessage().contains(expectedError)`, the framed form with the location and the quotes.

  A match gives PASS with `"Expected failure: " + errorMsg`. Otherwise it is FAIL with `formatExpectedFailureMismatch`
  (`PCTT:232-240`).
- Any other `Throwable` (`CS:2502-2533`) is unwrapped first by `PCTTools.unwrapExecutionError` (`PCTT:178-208`). That
  strips `InvocationTargetException` and "Unexpected error executing function" wrappers. It is then matched by
  `getMessageFromError(thrown).contains(expectedError)`, which is the framed `getMessage()`. A generic-wrapper expected
  string is never a valid match. A Java `AssertionError` matches the expected string `"Assert failure"`. A miss is ERROR.
- **All of this is substring (`contains`) matching. Nothing in the PCT harness compares exact strings.**

**Interpreted `ExecutePCTTest`** (`INT/natives/essentials/tests/ExecutePCTTest.java:115-168`):
- For an assert failure it matches only `e.getInfo().contains(expectedError)` (line 129-130). It has **no**
  exception-name or `getMessage` fallback, so its matching is narrower than compiled's.
- Other exceptions are matched as in compiled (line 153-155).

**Legacy JUnit PCT path** (`buildPCTTestSuite` → `executeFn`, `PTBC:112-194`; interpreted
`INT/testHelper/PureTestBuilderInterpreted.java:115-150`):
- The match is `PCTTools.getMessageFromError(thrown).contains(message)` (`PTBC:166`; interpreted line 132).
- A stale exclusion fails with "Test was expected to fail with "…" but now passes" (`PTBC:156`).
- A mismatch goes through `PCTTools.displayErrorMessage`: `"The PCT test runner expected an error containing: \"" + message + "\" but the the error was: \"" + …` (`PCTT:98`; "the the" is upstream's).
- An unmatched assert failure becomes `Assert.fail(thrown.getMessage())` (`PTBC:173-176`).

**`PCTTools.getMessageFromError`** (`PCTT:116-144`):
- A null message or an NPE becomes `"NullPointer exception"`.
- When the message contains `expected: '…'\nactual:` it **un-escapes literal `\n` (backslash-n) into newlines inside
  the two values**. This undoes `toRepresentation`'s escaping of multi-line strings such as TDS dumps.
- `cleanMessage` cuts everything before the first `"` when the message has an `Execution error at ` or
  `Assert failure at ` frame (`PCTT:146-153`). That is how the suggested manifest strings lose the location.

**What the manifests record**, as samples:
- Engine Java-binding manifest: `"expectedError": "\"\nexpected: 0.14D\nactual:   0.14\""` (`Test_JAVA_EssentialFunction_manifest.json:218`).
  The quotes come from the framed `getMessage()`.
- DuckDB: `"expectedError" : "\nexpected: 0\nactual:   1"` (`relational-duckdb/EssentialFunctions_manifest.json:95`).
  That is the raw info.
- DuckDB, a TDS string comparison:
  `"\nexpected: '#TDS\\n   id,payload,joined\\n   1,\\'[1,2,3]\\',1,2,3…#'\nactual:   '#TDS…'"`
  (`relational-duckdb/RelationFunctions_manifest.json:5`). The values are `toRepresentation`-escaped Strings.

**Upstream's PCT expected errors therefore depend on upstream's assert wording, by substring.** A lite that uses its
own words cannot reuse these manifest strings unchanged for assert-failure exclusions. It can for non-assert errors.

---

## 4. legend-engine's runners for the relational corpus

### 4.1 Which runner

- `Test_Pure_Relational` collects every `<<test.Test>>` under `meta::relational` and other packages with
  `PureTestBuilderCompiled.buildSuite(TestCollection, es)`
  (`ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/test/java/org/finos/legend/pure/code/core/relational/Test_Pure_Relational.java:27-47`).
- That method calls `executeFn` with an **empty** exclusion map (`PTBC:73-77`).
- The server-hosted suite `Test_Relational_UsingPureClientTestSuite`
  (`ENGINE/legend-engine-config/legend-engine-server/legend-engine-server-http-server/src/test/java/org/finos/legend/engine/server/test/pureClient/stores/Test_Relational_UsingPureClientTestSuite.java:30-74`)
  uses engine's own `PureTestHelperFramework.buildSuite`
  (`ENGINE/legend-engine-core/legend-engine-core-testable/legend-engine-test-framework/src/main/java/org/finos/legend/engine/test/shared/framework/PureTestHelperFramework.java:410-443`).
  Its `PureTestCase.runTest` calls the generated method and rethrows the target exception (lines 341-362).

### 4.2 How an assert failure is reported

- Through `executeFn`: an unmatched `PureAssertFailException` becomes `org.junit.Assert.fail(thrown.getMessage())`
  (`PTBC:173-176`). The JUnit failure carries the framed message, e.g. `Assert failure at (resource:… line:… column:…), "…"`.
- Through `PureTestCase`: the `PureAssertFailException` itself propagates as the JUnit error (`PureTestHelperFramework.java:357-361`).
- In-process m3 `TestRunner`: `AssertFailTestStatus` for a `PureAssertFailException`, `ErrorTestStatus` for anything else
  (`PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/execution/test/TestRunner.java:181-191`).
- Pure-side `executeTest`: `"FAIL"` with message `getInfo()` (or `getMessage()`) for an assert failure, and `"ERROR"`
  otherwise (`CS:2370-2379`).

**Nothing in the relational corpus runners compares messages.** Any throw is a failure and no throw is a pass. The only
message comparisons are the ones a test makes itself (`assertError`, or `assertEquals` on a caught string).

### 4.3 A test's Boolean result is ignored

- m3 `TestRunner.executeTestFunc` calls `functionExecution.start(testFunc, args)` and discards the result (`TestRunner.java:246`).
- Success means no throw (lines 177-180).
- `PureTestBuilder`'s `runTest` discards `executor.value(...)` the same way
  (`PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/execution/test/PureTestBuilder.java:108-121`).

So a body whose last expression evaluates to `false` passes. That includes `assertTdsEquivalent` on shapes that differ.

---

## 5. How legend-pure prints values for these messages

`format` (interpreted `INT/natives/essentials/string/toString/Format.java:62-216`; compiled
`CMP/generation/processors/support/PureStringFormat.java:32-169`):
- `%s` calls `meta::pure::functions::string::toString` (interpreted line 87-94; compiled `CompiledSupport.pureToString`, line 53-58).
- `%r` calls `meta::pure::functions::string::toRepresentation` (interpreted line 95-102; compiled line 59-64).
- `%d` takes an Integer only, and `%05d` zero-pads.
- `%f` and `%.Nf` take a Float only.
- `%t{…}` formats a Date.
- `%%` is a literal percent sign.
- Too few args raises `"Too few arguments passed to format function. Format expression \"…\", number of arguments [n]"`
  (interpreted line 199). Too many raises `"Unused format args. [n] arguments provided to expression \"…\""` (line 213).

**`toRepresentation`** is a Pure function, identical in both runtimes (`STR/toRepresentation.pure:40-52`):

```pure
$any->match([
     s:String[1] | '\'' + $s->replace('\\', '\\\\')->replace('\'', '\\\'')->replace('\n', '\\n') + '\'',
     t:StrictTime[1] | '%' + $t->toString(),
     d:Date[1] | '%' + $d->toString(),
     d:Decimal[1] | $d->toString() + 'D',
     n:Number[1] | $n->toString(),
     b:Boolean[1] | $b->toString(),
     p:PackageableElement[1] | if($p.name->isEmpty(), |'<' + $p->id() + ' instanceOf ' + $p->type()->elementToPath() + '>', |$p->elementToPath()),
     a:Any[1] | '<' + $a->id() + ' instanceOf ' + $a->type()->elementToPath() + '>'
    ])
```

| Value | `toRepresentation` (`%r`) | `toString` (`%s`) |
|---|---|---|
| String | single-quoted, with `\` → `\\`, `'` → `\'`, newline → `\n` (two characters). Tab and CR are **not** escaped (line 43). `'abc'` | raw contents: `abc` (`STR/toString.pure:97-101`) |
| Integer | `1`, `-1` (`STR/toRepresentation.pure:56-57`) | same |
| Float | `toString`. Compiled: `0.0` for zero (so `-0.0` prints `0.0`), otherwise `DecimalFormat("0.0")` with up to 340 fraction digits, never exponent form (`CS:134-136, 1271-1274`). Tests: `17.0`, `0.01`, `134210000.0`, `0.000000013421` (`STR/toRepresentation.pure:62-91`). Interpreted: the stored `BigDecimal.toPlainString()` (`PURE/…/m4/coreinstance/primitive/simple/SimpleFloatCoreInstance.java:32-38`) | same digits, no suffix |
| Decimal | `toString() + 'D'`. Compiled `BigDecimal.toPlainString()` (`CS:1276-1279`) **keeps the scale**: `123.12300D` (manifest line 246 above) | the digits, no `D` |
| Boolean | `true` / `false` | same |
| Date / DateTime | `'%' + toString`: `%2014-01-01`, `%2014-01`, `%2014`, `%2014-01-01T00:00:00.000+0000`. The sub-second precision is kept as written (`.0`, `.00`, `.0000`) and the time is shown in UTC as `+0000` (`STR/toRepresentation.pure:109-146`) | without the `%` (`STR/toString.pure:103-141`) |
| StrictTime | `'%' + toString` (line 44) | |
| Enum value | `<NAME instanceOf pkg::Enum>`. Inferred from line 50 and `id()`'s doc (`…/essential/meta/instance/id.pure:19-23`); no upstream test pins it | the value name, e.g. `CITY` (`STR/toString.pure:173-177`) |
| Class / enumeration / function (PackageableElement) | its full path, e.g. `meta::…::ST_Person` (`STR/toRepresentation.pure:157-165`) | the simple name: `STR_Person` (`STR/toString.pure:168-171`) |
| Class instance | `<id instanceOf my::Class>`. The id starts `Anonymous_` or `@_` (`STR/toRepresentation.pure:148-155`) | the instance's `toString()` qualified property if it has one (`INT/natives/essentials/string/toString/ToString.java:54-69`), else its id (`STR/toString.pure:162-166`) |
| `List` (`^List`) | `<… instanceOf List>` | `[a, b]`: elements by `toString`, `', '`-joined, **no quotes** (`PURE/…/platform/pure/anonymousCollections.pure:36-39`; test `STR/toString.pure:143-147`) |
| `Pair` | instance form | `<a, b>` (`anonymousCollections.pure:21-24`) |
| Relation / TDS | instance form `<… instanceOf …>` (inferred from line 50) | relation `toString(Boolean)` (`ENGINE/…/core_functions_relation/relation/functions/toString.pure:24-35`): `'#TDS\n'`, then 3 spaces, then the column names joined by `,` (with `:Type[mult]` when the flag is true), then each row as 3 spaces + cells joined by `,`, then `'\n#'`. The cell printer `s()` (`…/relation/functions/s.pure:23-38`) writes an empty cell as `null`, or `'null'` for a Variant column; a string containing `{` or `[` is single-quoted and escaped; anything else uses `toString` |
| A collection inside a default message | `[e1, e2]`: elements by `toRepresentation` joined with `', '`, built by `joinStrings('[', ', ', ']')`. Empty is `[]` (`STR/joinStrings.pure:72`) | |

The PCT relation tests compare `toString` strings with `assertEquals`. Their failure messages are therefore the `%r`
form of two Strings, with every newline escaped to `\n` inside single quotes. Section 3 shows the manifest sample.

Upstream's own tests pin the default wordings, collection forms included:
- `'\nexpected: [1, 3, 2]\nactual:   [2, 4, 1, 5]'` and `'\nexpected: [\'aaa\', 2]\nactual:   [2, \'aaa\']'` (`T/assertEquals.pure:62-64`).
- `'[1, 2] should not equal [1, 2]'` (`T/assertNotEquals.pure:61`).
- `'[1, 2] is not empty'` (`T/assertEmpty.pure:48`).
- `'[1, 2, 5, 2, \'a\', true, %2014-02-01, \'c\'] does not contain false'` (`T/assertContains.pure:48`).
- `'expected size: 3, actual size: 2'` (`T/assertSize.pure:46`).
- `'size should not equal: 2'` (`T/assertNotSize.pure:45`).
- `'\nexpected: 2.718271828459045\nactual:   2.718281828459045'` (`T/assertEqWithinTolerance.pure:75`).

---

## 6. Evaluation order and laziness a whole-body planner must respect

1. **Statements run in order, and the first throw ends the body.** The test runners treat any throw as the outcome
   (`TestRunner.java:175-193`; `PTBC:160-178`). Later statements never run, so a later statement that would error, or
   a later failing assert, is not observed. The verdict and its message belong to the first failing statement.
2. **The message lambda runs only on failure** (section 0). The `format(...)` in the `formatString` overloads sits
   inside a lambda (`T/assert.pure:21`), so formatting is lazy too.
3. **Format arguments are evaluated eagerly**, because Pure is strict at call sites. They are computed even when the
   assert passes:
   - `assertEquals/2`'s multi-valued branch maps `toRepresentation` over both collections before `equal` runs
     (`T/assertEquals.pure:21`). The singleton branch runs `toOne()` on both (line 20); it is guarded by the size check.
   - `assertEq/2`, `assertIs/2` and `assertEqWithinTolerance/3` pass `[$expected, $actual]` (already evaluated).
   - `assertSize/2` evaluates `$collection->size()` for the message as well as for the check (`T/assertSize.pure:19`).
   - `assertEmpty/1` builds `^List<Any>(values=$collection)` (`T/assertEmpty.pure:20`).
   - `assertIsNot/2` passes `[$actual]`.

   The lazy ones wrap their whole message in a lambda: `assertNotEquals` (multi branch), `assertNotSize`,
   `assertContains`, `assertNotContains`, `assertSameElements` and `assertInstanceOf`. `assertInstanceOf`'s
   `$instance->type()->toOne()` therefore runs only on failure.
4. **`assertSameElements` sorts both sides before comparing** (`T/assertSameElements.pure:24`). Its message re-sorts
   them inside the lambda (line 19).
5. **`assertError`:**
   - It runs `f` once.
   - The matcher runs once, always: with the caught error's info, or with `"No error was thrown"` when `f` raised
     nothing (2.3).
   - Any side effects of the partly run `f` stay.
   - Only `PureExecutionException`s are caught. In compiled mode a raw Java exception escapes as an ERROR (2.2).
   - Inside `assertError/4` the order is: message check, then line, then column. Each `&&` short-circuits
     (`T/assertError.pure:74-76`).
6. **`&&` and `||` short-circuit, and `if` evaluates one branch**: `and` defers its parameters
   (`INT/natives/grammar/_boolean/operation/And.java:48-66`) and compiles to Java `&&`
   (`CMP/…/natives/grammar/_boolean/operation/And.java:32`). `assertTdsEquivalent` depends on this to return `false`
   rather than fail (section 1).
7. **PCT bodies call the adapter once per `$f->eval(|…)`, in order.** Engine's relational adapter
   (`ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-PCT/legend-engine-pure-functions-relationalStore-PCT-pure/src/main/resources/core_external_test_connection/pct_relational.pure:97-167`)
   builds a plan, runs it with `meta::legend::executePlanAsJSON` (line 126), and decodes the value back to Pure
   (lines 131-166). The asserts then run **on the host**, in compiled Java, against the decoded values. Upstream's
   verdict is therefore the host's `equal`/`eq` over decoded values, with the semantics of 1.1. Decoding is part of the
   value: a Decimal column becomes `toDecimal()`, an Integer column `round()`, a Float column `toFloat()`, and a date
   is re-parsed (lines 169-187).

---

## Pointers: lite sites that carry upstream's wording today (for the difference statement)

Found by grep only, not analysed:
- `…/next/core/src/main/java/com/legend/edge/EdgePureWords.java` (lines 79-139)
- `…/edge/EdgeHostWords.java` (lines 69-220)
- `…/edge/UpstreamVerdictWords.java` (lines 20-165)
- `…/edge/EdgeLoweringForms.java:69`
- `…/testplan/AssertErrorNative.java:23`
- `…/lowering/VerdictSql.java:1681`
- `…/testexec/AssertErrorRuns.java:21`

All are under lite's `core/src/main/java/`.
Section 2.3 is the one verdict-relevant mismatch already visible there: `"No error was thrown"` is reported as the final
message, where upstream wraps it in the mismatch wording and passes when the expected message is that literal.
