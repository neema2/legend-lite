# Fold audit: every constant fold lite's compiler does in Java, compared with compiled legend-pure (2026-11-25)

Read-only research. I built nothing and ran nothing. Every claim below comes from reading source. Code: `/home/user/s2` at `da231244`.

**Path prefixes**
- `L/` = `/home/user/s2/core/src/main/java/com/legend/`
- `PURE/` = `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_pure_src/`
- `PC/` = `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/`
- `CS` = `PC/generation/processors/support/CompiledSupport.java`
- `CH` = `PC/CoreHelper.java`
- `PN/` = `PC/generation/processors/natives/`
- `CG` = `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/resources/org/finos/legend/pure/runtime/java/compiled/generation/processors/support/core/CoreGen.java`
- `M3/` = `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/`
- `ENG/` = `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/`
- `ENGSTD` = `.../legend_engine_src/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-standard/legend-engine-pure-functions-standard-pure/src/main/resources/core_functions_standard/collection/in.pure`

**Classes used in the tables**
- **T**: fixed by the types or the model, such as a column's name or a declared multiplicity. Allowed.
- **S**: list shape or structure over spelled literals: size, element order, an arm or branch chosen by a spelled value. "Compare, never compute" (WORLD_MAP §4). Allowed.
- **K**: decides a Boolean or Integer by comparing spelled constants (`==`, `in`, `contains`, `indexOf`). The 2026-11-19 "Folds" ruling allows constants. Under the amended tenet 1 it is a product question whenever that Boolean or Integer is the program's value, not only a planning choice.
- **V**: computes a new value from constants: text, arithmetic, a rounded number. This is a tenet 1 question in the product.

---

## 0. Headline findings

1. **StaticFold rewrites integer literals beyond 64 bits.** `fold()` turns every static subtree back into a literal, a lone literal included. `eval` reads an integer through `longValue()`, so it wraps (`L/compiler/spec/StaticFold.java:396`, reified at `:904-909`).
   - Inside a NormalizeRequired body, a relation-generic body (S112, product) or a schema-erased helper, `99999999999999999999` becomes `7766279631452241919`.
   - legend-pure keeps a `BigInteger` (`PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/ModelRepository.java:600-617`).
   - A Float literal's exact digits are also dropped (`:397`). That agrees with compiled Pure, where Float is a double.
2. **StaticFold evaluates `'abc'->contains('b')` and `'abc'->indexOf('c')` as list operations, which is wrong.**
   - A bare `contains` or `indexOf` of arity 2 lists both the collection and the string natives as referents (`L/compiler/BareNames.java:119-143`).
   - `FoldOp.of` matches any referent (`StaticFold.java:522-532`), and `FoldOp` includes `string::indexOf(String,String)` (`:496`).
   - `evalList('abc')` is `['abc']`, so the answers are `false` and `-1`. Pure answers `true` (`CH:1209-1212`) and `2` (`CS:1531-1534`).
   - LiteralUnroll already guards this exact trap (`LiteralUnroll.java:470-473`). StaticFold does not.
   - It also fires in any query through `TdsDesugars.java:276-283`, for example `$t.columns->map(c|$c.name->contains('a'))`.
3. **`toString`, `makeString` and `joinStrings` of a Float use Java's `String.valueOf(double)`** (`StaticFold.java:624-629`, `:862-871`). Compiled Pure uses `DecimalFormat("0.0")` with up to 340 fraction digits (`CS:130-137`, `CS:1271-1274`).
   - `1.0E7->toString()`: Java gives `'1.0E7'`, Pure gives `'10000000.0'`.
   - `0.0001`: Java gives `'1.0E-4'`, Pure gives `'0.0001'`.
   - In the product this is also tenet 1: the database computes `toString()`.
4. **StaticFold's `==` has two Pure disagreements.**
   - A type against a String compares by simple name: `Integer == 'Integer'` is `true` in lite (`:884-888`) and `false` in Pure (`CS:1095-1104`).
   - Both sides are flattened into one list before comparing (`evalAll`, `:837-847`), so `[] == [1, 1]` is `true`. Pure checks the sizes first and answers `false` (`CS:1074-1077`).
5. **StaticFold's `sortBy` crashes on mixed keys.** `[2, 1.5]->sortBy(x|$x)` throws an uncaught `ClassCastException` from `Comparator.comparing` (`:733-736`). Compiled Pure orders numbers by value (`CS:590-597`) and other mixed classes by a fixed class order (`CS:140`, `:570-578`).
6. **LiteralUnroll's `in` compares raw values, not (kind, value).** `scalar()` drops the kind (`LiteralUnroll.java:289-291`, used at `:596-600`).
   - A date literal therefore equals its own text: `%2020-01-01->in(['2020-01-01'])` folds to `true`. Pure says `false` (`CS:1104`, then `AbstractPureDate.equals` requires a `PureDate`).
   - Numbers of different kinds fold to `false`, which **agrees** with compiled Pure (`1 == 1.0` is `false` in `CS:994-1011`).
   - So the second review's "in across numeric kinds" is not a Pure disagreement. It is a disagreement with the database and legend-engine (`true`, S127).
7. **LiteralUnroll truncates counts with `intValue()`** in `limit`, `drop` and `slice` (`:348-357`).
   - `[1,2,3]->limit(4294967296)` folds to `[]`; Pure gives `[1,2,3]` (`CH:628-652`).
   - `drop(4294967296)` folds to the whole list; Pure gives `[]` (`CH:659-686`).
   - `at` truncates the same way, and so does compiled Pure's `safeGet` (`CS:221-239`), so `at` agrees.
8. **A product contradiction, not a Pure one.** LiteralUnroll compares two date-time literals by their written text (`:90-92`).
   - `|%2020-01-01T10:00:00 == %2020-01-01T10:00:00.000` folds to `false` at G½, everywhere including the query's own level (`UserCallInliner.java:1360`).
   - That is Pure's answer (`M4 AbstractPureDate.java:31-58`).
   - Lite's ruled product meaning, S399, is "one instant is equal", and the database's `==` answers `true`.
9. **Folds turn product answers into Java answers.** A query whose root folds to a Boolean or String literal (`|1 == 1`, `|'a'->in(['a','b'])`, `|2 > 1`) gets a constant plan with nothing sent to the database (`L/planner/ProductStatements.java:217-235`). That is a tenet 1 question for every K or V fold that reaches the query's own level.
10. **legend-pure's door says `false` for Integer against Decimal** (`L/edge/PureComparisons.java:41-44`). Compiled Pure says `1 == 1D` is `true`: it compares `equals`, then `toString` (`CS:994-1011`). Interpreted Pure says `false`. S127 records the split, but the door follows interpreted Pure, not compiled.

---

## 1. Where each folder runs (reach)

| Folder | Entered from | Runs on | Product? |
|---|---|---|---|
| `StaticFold.fold` (whole body) | `L/compiler/spec/Overloads.java:129-131` → `inlineNormalized` `:352-420`, fold at `:406` | bodies of `<<NormalizeRequiredFunction>>` functions, of relation-generic user functions (S112, `RelationGenerics.java:37-42`) and of schema-erased helpers (`Overloads.java:270-281`), after the caller's arguments are spliced in as source | **yes**: S112 is lite's own product (the DataCube timeseries library). For the engine corpus it is the analogue of legend-engine's `preval`, which evaluates in Pure (`ENG/router/routing/router_routing.pure:202-245`; `ENG/router/preeval/preeval.pure:508`, `:859` `reactivate`) |
| `StaticFold.foldToLiteral` | `L/compiler/spec/TdsDesugars.java:276-283` (from `Typer.java:454`, every call) | `X.columns->map(...)` when the whole result is static | **yes**: any query |
| `LiteralUnroll.fold` (native folds) | `L/compiler/spec/UserCallInliner.java:1347-1361` (`rewrite`) | **every** node the G½ inliner rewrites outside quoted code, **including the query's own level**; also the edge's `PureMeaningRewrite` (`L/edge/PureMeaningRewrite.java:108`) | **yes** |
| Literal arms (`if`, `map`, `filter`, `fold`, `groupBy`, `match` unrolls) | `UserCallInliner.java:1093-1325`, gated at `:1110-1112` | inlined user-function bodies, and the verdict source | **yes** (user functions) |
| `NormalizeFolds.foldInlined` | `UserCallInliner.java:625` → `deepFoldInlined` `:1330-1345` | every inlined user-function body | **yes** |
| `resolver/LiteralFolds` | `L/resolver/StoreResolver.java:335`, `:351`; `L/resolver/ChainDispatch.java:278` | the condition of a class-typed `if` | **yes** |
| `ConstBounds.intOf` | `L/lowering/Lowerer.java:1279`, `:1293`, `:1298-1300` | `limit`, `drop` and `slice` bounds of a relation | **yes** |
| `typed/Literals.joined` / `tokens` | `testplan/Seeds`, `BodyClass`, `TestKinds`, `DatabaseJudge`, `BodyDriver`; `planner/LetPlanner.java:164` (a predicate only) | the test road's texts: effects, seeds, messages | ruled carve-outs |
| `ProductStatements.constant` | `L/planner/ProductStatements.java:217-235` | a root that is a String or Boolean literal is answered with no statement | **yes**: it publishes whatever the folds above produced |
| `edge/PureComparisons` | legend-pure's door | rewrites cross-type comparison markers | compatibility door |

---

## 2. StaticFold (`L/compiler/spec/StaticFold.java`)

The `FoldOp` vocabulary is at `:486-512`. `fold` reifies **any** static subtree (`:130-145`). `eval` is at `:383-418`.

| # | Site | Operation | What Java computes | Compiled Pure (source) | Agree? / differing input | Class | Enables (inferred from the code's comments; removal not measured) |
|---|---|---|---|---|---|---|---|
| SF-0a | `:396`, reify `:904-909` | reading an Integer literal | `CInteger.value().longValue()`: wraps past 64 bits; the folded body gets the wrapped literal | M4 keeps a `BigInteger` (`ModelRepository.java:600-617`). Compiled codegen of a BigInteger literal UNVERIFIED | **NO**: `99999999999999999999` → `7766279631452241919` in any NormalizeRequired or relation-generic body | (corruption) | nothing: fold only what fits in a long |
| SF-0b | `:397`, reify `:908` | reading a Float literal | the double; `CFloat.exact` dropped | compiled Float is a double (`PC/generation/processors/type/TypeProcessor.java:200-202`) | yes for compiled (interpreted is BigDecimal-backed, so it would differ) | (representation) | — |
| SF-1 | `:548-557` | `+` over Strings | concatenation | `string::plus` is `joinStrings('', '', '')` (`M3/grammar/functions/string/operation/plus.pure:25-28`; `CS:876-889`) | yes | V | **static column names** (`$vc + '_1'`, `$col.name + '_x'`: StaticFold doc `:31-41`) |
| SF-2 | `:558-566` | `+` over Integers | exact BigInteger sum; not folded when the sum passes a long | `CS:1355-1359` `long + long` (wraps) | yes in range; on overflow the fold declines and the database answers, while Pure wraps (not a fold difference) | V | schema arithmetic in normalized bodies |
| SF-3 | `:569-583` | `-` over Integers | exact; declines on overflow | `CS:1402-1407` (wraps) | as SF-2 | V | as SF-2 |
| SF-4 | `:584-607` | `pair`, `zip` | pairs, cut to the shorter list | zip truncates (`CG:306-315`) | yes | S | column/output-column pairing (`iqrClassify`, `zScore`: `:589-592`) |
| SF-5a | `:608-612`, `:889` | `==` on one kind of scalar (String, Long, Double, Boolean) | `Object.equals` | `CS:1028-1105` → `eq(Number)` `CS:994-1011` | yes. Exception: `0.0 == -0.0` (Pure `true` via `CS:1008-1009`, Java `false`), unreachable from a query because `-0.0` parses as `minus(0.0)` (`L/parser/SpecParser.java:705-729`), which the fold does not evaluate | K | `if` over schema facts |
| SF-5b | `:889` | `==` on Long against Double | `false` | `CS:994-1011`: `"1"` vs `"1.0"` → `false` | **yes** with Pure. Not with the database or legend-engine (`true`, S127), nor with lite's "unmarked bodies are the database's comparison" stance (`Overloads.java:139-143`) | K | — |
| SF-5c | `:881-882` | `==` on type against type | path equality | type identity | yes | T | `$col.type == Integer` per-type accessor (`:78-80`) |
| SF-5d | `:884-888` | `==` on type against String | **simple-name** equality: `Integer == 'Integer'` → `true`; `my::Person == 'Person'` → `true` | a type instance against a String: `pureEquals` or `equals` → `false` (`CS:1095-1104`) | **NO** | K | no corpus witness found (the engine's `.type == '...'` uses are class String properties) |
| SF-5e | `:878-879` | `==` with an enumeration token | `EnumValue` record equality | instance identity; S169 | yes | K | `if` on an option enum (`:102-107`) |
| SF-5f | `:609-611` with `evalAll` `:837-847` | `==` over collections | both sides **flattened into one list**, then the comparison needs exactly 2 elements | `CS:1074-1079`: sizes, then elementwise | `[1] == 1` → `true` (agrees); **`[] == [1, 1]` → `true`; Pure `false`**; `[1,1] == []` likewise | K | — |
| SF-5g | `:72-73`, `:889` | `==` between TDS columns | record equality on (name, type) | `TDSColumn` equality keys: offset, name, type, enumMappingId, enumMapping, documentation, sourceDataType (`ENG/tds/tds.pure:25-46`) | differs when two columns share name and type but differ in offset or documentation; reachability UNVERIFIED | K | — |
| SF-6 | `:613-616` | `not` | `!b` | — | yes | K | — |
| SF-7 | `:619-623`, `:782-799` | `toOne`, `toOneMany`, `at` | the element when singular or in range | `CS:221-239` | yes; out-of-range and non-singular cases are not folded | S | — |
| SF-8 | `:624-629` | `toString` | `String.valueOf` of String, Long, Boolean, **Double** | `CS:1189-1243`; double: `CS:1271-1274` with `DECIMAL_FORMAT` (`CS:130-137`) | String, Long, Boolean agree. **Double: NO** when \|x\| ≥ 1e7 or \|x\| < 1e-3 (`1.0E7` → `'1.0E7'` vs `'10000000.0'`; `0.0001` → `'1.0E-4'` vs `'0.0001'`; `1.5E300`). `-0.0`: `'-0.0'` vs `'0.0'` (unreachable) | V | column names built from numbers (none seen) |
| SF-9 | `:634-641` | `isEmpty`, `isNotEmpty` | list emptiness | — | yes | S | **dead-branch removal**: `joinWithOptionalColumns`' else branch is ill-typed when `$cols` is `[]` (`:630-633`) |
| SF-10 | `:642-649` | `in` | `List.contains` (Java equals) | `ENGSTD:18-26` (`exists($value == $x)`) → `CS:1028-1105` | yes for spellable scalars, types, enumerations and pairs (`M3/anonymousCollections.pure:17-25`, key equality). `[1]->in([1,2])` → `false`; Pure `true` (rare). Inconsistent with SF-5d (`Integer->in(['Integer'])` → `false`, agreeing with Pure) | K | column filters |
| SF-11 | `:650-653` | `concatenate` | flatten | — | yes | S | — |
| SF-12 | `:654-657` | `removeDuplicates` | `LinkedHashSet`: first occurrence, order kept | `PC/generation/processors/support/Pure.java:1031-1042` (PureEqualsHashingStrategy set, `select`); hash `CS:303-323`, equality `CS:1028-1105` | yes for spellable values (`[1, 1.0]` keeps both in both) | K | — |
| SF-13 | `:490`, `:658-667`, match at `:522-532` | `contains` | collection membership, **also for the String overload**: a bare `contains` has both referents (`BareNames.java:119-143`, `ResolvedNames.java:27-29`; `Pure.java:1134-1135`) | string: `CH:1209-1212` `str1.contains(str2)`; collection: `M3/essential/collection/boolean/contains.pure:42-45` | collection: yes. **String: NO**: `'abc'->contains('b')` → `false`; Pure `true` | K | — |
| SF-14 | `:496`, `:668-675` | `indexOf` | `List.indexOf`, **also for `string::indexOf(String,String)`** (named in the FoldOp) | string: `CS:1531-1534` `str.indexOf(toFind)`; collection: `CH:786-789` | collection: yes. **String: NO**: `'abc'->indexOf('c')` → `-1`; Pure `2` | K | — |
| SF-15 | `:676-715` | `map`, `filter` over a static list | per element; `map` flattens | — | yes | S | **column lists**: `$tds.columns->filter(...)->map(col\|col(...))` |
| SF-16 | `:716-738` | `sortBy` | `Comparator.comparing(Comparable)` | `ENG/corefunctions/collectionExtension.pure:183-186` → `sort` → `CS:907-955`, `compareInt` `CS:550-588` | one class of key: yes. **Mixed: NO**: `[2, 1.5]->sortBy(x\|$x)` → `ClassCastException` escapes (uncaught; the compile fails). Pure gives `[1.5, 2]` (`CS:590-597`); `['a', 1]` sorts `[1, 'a']` (`CS:140`, `:570-578`) | K/S | — |
| SF-17 | `:229-234`, `:739-748` | `if` over a static condition | the taken branch | — | yes | S | the dead branch never typed |
| SF-18 | `:749-757` | `and`, `or` | reduce | — | yes | K | — |
| SF-19a | `:758-776`, `stringify` `:862-871` | `makeString`, `joinStrings` of String, Long, Boolean | joined text | `makeString` = `map(toString)->joinStrings` (`ENG/corefunctions/stringExtension.pure:258-271`); `CS:876-889` | yes | V | column-name lists |
| SF-19b | same | of a **Double** | `String.valueOf(double)` | as SF-8 | **NO**: `[1.0E7, 2.5]->makeString(',')` → `'1.0E7,2.5'` vs `'10000000.0,2.5'` | V | — |
| SF-19c | same | of a type | its simple name | a type's `toString` is `getName()` (`PC/.../coreinstance/JavaCompiledCoreInstance.java:35-38`; `AbstractCompiledLazyConcreteElement.java:65-68`) | yes | T | `$col.type` in names |
| SF-19d | same | `joinStrings` of **non-Strings** (`[1,2]->joinStrings(',')`) | `'1,2'`, folded before typing | `joinStrings(String[*], String[1])` (`M3/essential/string/toString/joinStrings.pure:40`): a compile error | **NO**: lite accepts an ill-typed program | V | — |
| SF-20 | `:777-781` | `elementToPath` | a type's path; a primitive's is its name | `M3/essential/meta/graph/elementToPath.pure:173-176` (`'Integer'`, `'String'`) | yes | T | `$c.type->elementToPath()` names |
| SF-21 | `:420-480` | `.columns`, `Col.name`, `Col.type`, `Pair.first`, `Pair.second` | schema facts from the typed relation | TDS reflection | yes | T | **static column names and types** |
| SF-22 | `:207-227`, `:279-318`, `:346-377` | `map` unroll; β-inlining of a bodied callee with static arguments | structure | the analogue of `preval` (`preeval.pure:859`) | yes | S | `rowValueDifference`, `extendMatchColumns` (`:235-241`) |
| SF-TD | `L/compiler/spec/TdsDesugars.java:276-283` | `X.columns->map(...)` → literal | `StaticFold.eval`'s whole vocabulary over column facts | as above | inherits SF-5 to SF-19: `$t.columns->map(c\|$c.name->contains('a'))` → `[false, …]` for a name like `'abc'` (SF-13) | T + K/V | asserts over column metadata |

---

## 3. LiteralUnroll (`L/compiler/spec/LiteralUnroll.java`)

This runs at G½ on every non-quoted node, including the query's own level.

**Values carried** (`literal()`, `:78-105`)
- Integer: an exact `BigInteger` (`:86`).
- Float: a double (`:88`).
- Enumeration value: the String `"fqn.value"` (`:89`).
- Date: its `canonicalText` (`:90-92`); the timezone is normalized to GMT at parse (`L/values/PureDateLiteral.java:38-48`).
- Time: `String.valueOf` (`:95-96`).
- `%latest`: the String `"%latest"` (`:97-98`).
- Decimal: the arm is **dead**. A decimal literal is typed `PrecisionDecimal(38, scale)` (`Typer.java:283`, `:1946-1953`), not `Type.Primitive`, so the `when` at `:93` never matches.

| # | Site | Operation | What Java computes | Compiled Pure | Agree? / differing input | Class | Enables |
|---|---|---|---|---|---|---|---|
| LU-1a | `:396-399` | `==` of two references | `fullPath` equality | identity | yes | K | `$v != TDSNull` |
| LU-1b | `:404-412` | TDS null carriers | `TDSNull == TDSNull` → true; against a scalar → false | legend-engine's `tds.pure` `firstNotNull` (cited at `:400-403`; not re-read) | yes (per the code's citation) | K | `firstNotNull` |
| LU-1c | `:413-418` | a reference against a scalar | `false` | `CS:1095-1104` → `false` | yes | K | — |
| LU-1d | `:419-421`, `neverEqual` `:434-438` | an enumeration value against another kind | `false` (S169) | `false` | yes | K | `Stat.mean == 'mean'` |
| LU-1e | `:422-425` | `==` of two scalars of one kind: Integer, Float, String, Boolean | value equality | `CS:994-1011`, `:1104` | yes | K | asserts and dead branches in platform programs |
| LU-1f | `:422-425` with `:90-92` | `==` of two date-times of one kind | **written-text** equality | `AbstractPureDate.java:31-58`: components plus the subsecond's text | **agrees with Pure** (`%…T10:00:00 == %…T10:00:00.000` → `false`). **Contradicts lite's ruled product rule S399** (one instant is equal: `true`) and the database's `==` | K | — |
| LU-2 | `:452-460` | `<`, `>`, `<=`, `>=` of two Integer literals | exact BigInteger compare | numeric | yes | K | — |
| LU-3 | `:463-466` | `size` of a spelled list | element count | — | yes | S | `size() == 1` fast paths |
| LU-4 | `:472-486` | `contains`: the collection overload only (guarded by qualified name, `:473`) | same-kind membership; never-equal elements skipped | `contains.pure:42-45` | yes (cross-kind is not folded) | K | — |
| LU-5 | `:488-495` | `isTrue` | the literal; empty → false | `ENG/corefunctions/langExtension.pure:15-18` | yes | S | — |
| LU-6 | `:498-500` | `assert(true, …)` | `true` | `assert` returns true | yes | S | assert plumbing |
| LU-7 | `:502-517` | `enumValues` | the declaration's values | the declaration | yes | T | — |
| LU-8 | `:520-543` | `dynamicNew`, `keyValues` | an instance or pair literal | — | yes | S | M2M over a literal source |
| LU-9 | `:544-560` | `get` on `newMap` with a spelled key | (kind, value) match | map with PureEqualsHashingStrategy (`PC/generation/processors/support/map/PureEqualsHashingStrategy.java:31-39`) | yes | K | registries |
| LU-10 | `:561-571` | `instanceOf`, `assertInstanceOf` | by the literal's class (`accepts`, `:154-168`) | runtime class | yes | T | — |
| LU-11 | `:578-595` | `not`, `and`, `or` (short-circuit) | Boolean logic | short-circuit | yes | K | — |
| LU-12 | `:596-600`, `scalar()` `:289-291` | `in` over a spelled list | **raw value** equality (kind dropped) | `ENGSTD:18-26` → `CS:1028-1105` | numbers of different kinds → `false` (**agrees** with Pure; not with the database). **Date against its text: NO**: `%2020-01-01->in(['2020-01-01'])` → `true`; Pure `false` (`CS:1104`; `AbstractPureDate.java:38-41`). Same for `%2020` against `'2020'` and `%latest` against `'%latest'` | K | verdict sources (`DatabaseType->enumValues()->filter(e\|$e->in([...]))`) |
| LU-13 | `:601-605`, `:615-644` | `isEmpty`, `isNotEmpty`, `toOne`, `first`, `last` | list shape | — | yes | S | — |
| LU-14 | `:606-610` | `at(k)` | `k.value().intValue()` (truncates) | `CS:221-239` `get((int) offset)`, which truncates too | yes for compiled (`[a,b]->at(4294967296)` → `a` in both) | S | — |
| LU-15 | `:348-351` | `slice` over a spelled list | `intValue()` bounds | `CH:735-770` (long compares, clamps) | **NO**: `[1,2,3]->slice(0, 4294967297)` → `[1]`; Pure `[1,2,3]` | S | — |
| LU-16 | `:352-354` | `limit` | `intValue()` | `CH:628-652` | **NO**: `limit(4294967296)` → `[]`; Pure the whole list | S | — |
| LU-17 | `:355-357` | `drop` | `intValue()` | `CH:659-686` | **NO**: `drop(4294967296)` → the whole list; Pure `[]` | S | — |
| LU-18 | `:313-345`, `:358-362`, `:650-692` | property field, unspelled default, cast, copy, `concatenate`, `zip`, `tail`, `init`, `defaultIfEmpty` | shape | — | yes | S/T | tier-1 recursion over literal trees (the doc, `:37-59`) |

---

## 4. UserCallInliner literal arms, and NormalizeFolds

| # | Site | Operation | What Java computes | Compiled Pure | Agree? | Class | Enables |
|---|---|---|---|---|---|---|---|
| UC-1 | `L/compiler/spec/UserCallInliner.java:1114-1127`, `:1128-1164`, `:1165-1202`, `:1207-1228`, `:1285-1322` | `if`, `map`, `filter`, `fold` and `match` over literals, in inlined bodies | branch, arm and shape; a filter keeps an element only when its predicate folds to a Boolean literal | — | yes (it inherits LU's agreements and disagreements) | S | M2M recursion over literals, `toPostgresModel`, `convertJoinStrings`; the recursion wall otherwise |
| UC-2 | `:1232-1284` (`scalarValue` `:1251`) | collection `groupBy` over a spelled list | keys are **raw** scalar values in a `LinkedHashMap` | `PN/essentials/collection/anonymous/map/GroupBy.java:80` (PureEqualsHashingStrategy map) | numbers of different kinds: yes (separate groups in both). **An enumeration value against the String `'pkg::E.V'`, or a date against its text: NO** (one group vs two). Key order: insertion order vs Pure's HAMT hash order; whether that is observable after lowering is UNVERIFIED | K/S | the "registry" newMaps in platform programs |
| NF-1 | `L/compiler/spec/NormalizeFolds.java:82-94` | `size` of an [n..n] value | n | — | yes | T | `assertEquals`' size-1 fast path (`:19-23`) |
| NF-2 | `:95-105` | `eq`, `equal` of two Integer literals | exact | `CS:994-1011` | yes | K | — |
| NF-3 | `:106-115` | `and`, `or` of two literals | Boolean | — | yes | K | — |
| NF-4 | `:46-59` | `if` over a literal, in an inlined body | the taken branch | — | yes | S | a dead branch never lowered |

---

## 5. Resolver, lowering, typer, test road and door

| # | Site | Operation | What Java computes | Compiled Pure | Agree? / input | Class | Enables |
|---|---|---|---|---|---|---|---|
| RL-1 | `L/resolver/LiteralFolds.java:33-87` (`literalEquals` `:81-87`), used at `StoreResolver.java:335-359` | a static class-typed `if` condition: `==` over literals | **numbers by numeric value across kinds** (`1 == 1.0` → `true`); a Float with exact digits by its BigDecimal | `1 == 1.0` → `false` (`CS:994-1011`; S127's compiled column); a Float as a double (`1.00000000000000000001 == 1.0` → `true`) | **NO** for Pure. It equals the database's and legend-engine's answer. Reached in an unmarked body (a function body) or after legend-engine's door; at legend-pure's door the marker is already `false` | K (selects a branch) | class query under `if` (else "class query under if() with a runtime condition is not resolvable yet") |
| LW-1 | `L/lowering/ConstBounds.java:21-63` (`Lowerer.java:1279-1300`) | relation `limit`, `drop`, `slice` bounds | literal `longValue()`; `+`, `-`, `*` in Java long (wraps) | `CS:1355-1359`, `:1402-1407`, `:1456-1460` (long arithmetic, wraps) | yes in range and on overflow. A BigInteger literal is truncated (UNVERIFIED in compiled Pure) | V | LIMIT/OFFSET literals; else "dynamic slicing bounds are not lowered yet" |
| LW-2 | `L/lowering/Scalars.java:411-420` | unary minus over a literal | a negative literal (`Long.MIN_VALUE` → decimal) | — | yes (exact spelling of a sign) | V (spelling) | — |
| LW-3 | `L/lowering/Scalars.java:4067-4156` (`refuseRealTies`, `tieKey`) | compares literals | only to **refuse** a cross-kind tie in a mixed list; no value produced | — | n/a | refusal | a loud refusal instead of a wrong extreme |
| LW-4 | `L/lowering/MatchFold.java:20-61` | `match` in scalar position | the arm by the input's **declared** type (exact primitive; the Date ladder; the Number ladder) | dispatch on the runtime value's class | **NO** when the declared type is abstract and a concrete arm comes first: `$x:Number[1]` holding `1` with `[i:Integer[1]\|'i', n:Number[1]\|'n']` → `'n'`; Pure `'i'`. Reach UNVERIFIED | T (by type) | milestoning date helpers |
| TY-1 | `L/compiler/spec/Typer.java:267-284` | a Decimal literal with scale > 38 | `setScale(38, HALF_EVEN)` | unbounded BigDecimal (`TypeProcessor.java:204-206`) | **NO**: a 39th fraction digit is rounded away. PARK-27 is open in the code comment | V (representation) | — |
| TY-2 | `L/compiler/spec/ColumnsMetaFold.java:23-101` (`Typer.java:1385`) | `.columns.name`, `.columns.type`, `.columns.documentation` | the schema's facts | TDS reflection | yes | T | static column names and types; `sort` by `.columns.name` (`SortChecker.java:57-64`) |
| TY-3 | `L/compiler/spec/TdsDesugars.java:876-895` | `extractEnumValue(E, 'X')` | the declared value; an unknown name is a compile error | the value; an unknown name is a run-time error | yes (same answer, earlier) | T | — |
| TY-4 | `L/compiler/spec/PlatformConstants.java:51-62` (`Typer.java:1078-1081`) | `PureOne`, `ZeroMany`, … | m3's declared instances | `m3.pure:1411` (lite's citation; not re-read) | yes | T | the engine's plan hooks |
| TY-5 | `L/compiler/spec/ExecuteChainAssembly.java:99-111`, `:259-300` | `preval`, `withFeatureFlags`, `if(<literal>)`, `at(k)` over lambdas | peels to the query lambda | `preval` is identity for rows | yes | S | `execute` of a query chosen by a literal |
| TP-1 | `L/compiler/spec/typed/Literals.java:27-115` | `+`, `joinStrings`, `replace` over literal Strings | Java text | `CS:876-889`, `CS:1564-1567`; `plus.pure:25-28` | yes | V | the ruled carve-outs: effects, seed CSV, messages, edge goldens (the user's Q-D 2026-11-21, Q-S7a/b 2026-11-22; `LiteralJoinCensusTest`) |
| TP-2 | `L/planner/ProductStatements.java:67-71`, `:217-235`; `L/testplan/LiteralFold.java` | a root that **is** a String or Boolean literal | answered with nothing sent | — | identity, but it publishes the results of LU, NF and SF as product answers | (publisher) | the constant plan |
| DR-1 | `L/edge/PureComparisons.java:41-52` | legend-pure's door: a cross-type `==` or `in` → `false`, `indexOf` → `-1` | by the types | compiled: Integer against Decimal compares by text, so `1 == 1D` and `3 == 3D` are **true** (`CS:994-1011`); Integer against Float and Float against Decimal are false (`CS:997-1001`). Interpreted: always false (`PC/../interpreted/EqualityUtilities.java:60-80`) | **NO** with compiled Pure for Integer against Decimal values whose texts coincide (`1 == 1D`, `[1D]->indexOf(1)` → compiled `0`, door `-1`). Agrees with interpreted. S127 records the compiled answer; the door follows interpreted | T | the door's equality |

Other constant evaluation, read but not audited for parity. These are orchestration or model data, not query values:
- `compiler/spec/typed/ContextReading.java:1127-1195`: connection URL and setup-SQL string `+`.
- `resolver/Substitution.java:1700-1760`: `objectReferenceIn` decodes spelled reference strings into pk literals.
- `compiler/spec/CsvCensusChecker` and `GenerateTestDataChecker`: test data.

---

## 6. Result lists

### List 1. Folds that agree with Pure and are allowed: keep

**Type and model facts (T)**
- SF-5c, SF-19c, SF-20, SF-21.
- TY-2, TY-3, TY-4.
- LU-7, LU-10.
- NF-1.

**Structure and list shape (S)**
- SF-4, SF-7, SF-9, SF-11, SF-15, SF-17, SF-22.
- LU-3, LU-5, LU-6, LU-8, LU-13, LU-14 (truncates like compiled `safeGet`), LU-18.
- UC-1, NF-4, TY-5.

**Comparisons of spelled constants that agree with Pure (K)**
- SF-5a, SF-5b, SF-5e, SF-6, SF-18.
- SF-10, SF-12, and the collection halves of SF-13 and SF-14.
- SF-16 with one class of key.
- LU-1a to LU-1e, LU-2, LU-4, LU-9, LU-11.
- UC-2 over numbers, strings and booleans.
- NF-2, NF-3.

A caveat on SF-5b and LU-12 (numbers of different kinds → `false`): this is Pure's answer and legend-engine's `preval` answer. It is **not** the answer of lite's "unmarked body is the database's comparison" stance, where the database says `true`. That is an edition question, not a Pure disagreement.

**New values computed from constants (V)**
- SF-1, SF-2, SF-3: needed for static column names.
- SF-19a: String, Long and Boolean texts.
- LW-1, LW-2.
- TP-1: ruled carve-outs.

### List 2. Folds that disagree with Pure, each with a fix

| Fold | Differing input | Fix |
|---|---|---|
| **SF-0a**: integer literal `longValue()` (`StaticFold.java:396`) | `99999999999999999999` → `7766279631452241919` | **Stop folding**: `eval` returns null for a `BigInteger` that does not fit a long. `fold` should never re-reify a lone literal: return `v` when `v` is already a literal. |
| **SF-13 and SF-14**: String `contains` and `indexOf` evaluated as list operations (`:490`, `:496`, `:658-675`) | `'abc'->contains('b')` → `false`; `'abc'->indexOf('c')` → `-1` | **Stop folding** when the receiver evaluates to a scalar String, and drop `INDEX_OF__STRING_*` from `FoldOp`. Or fold Pure's way: `String.contains` / `String.indexOf` when the receiver is a String scalar. StaticFold runs before typing and cannot know the overload, so stopping is safer. |
| **SF-8 and SF-19b**: Float to text (`:624-629`, `:866`) | `1.0E7->toString()` → `'1.0E7'` vs `'10000000.0'` | **Stop folding Floats.** The product's text is the database's (tenet 1). Pure's way, if a fold is needed under `engine` or `pure`: `x == 0.0 ? "0.0" : new DecimalFormat("0.0", ENGLISH)` with 340 maximum fraction digits (`CS:130-137`, `:1271-1274`). |
| **SF-5d**: type against String by simple name (`:884-888`) | `Integer == 'Integer'` → `true` | **Fold Pure's way**: `false`. Or stop folding. First check whether any corpus or PCT body needs `true`; none was found by grep. |
| **SF-5f**: `==` flattens both sides (`:609-611`) | `[] == [1, 1]` → `true` | **Fold Pure's way**: evaluate each side with `evalList`, compare the sizes, then elementwise with `staticEquals`. Or stop when either side is a collection. |
| **SF-16**: `sortBy` with mixed key classes (`:733-736`) | `[2, 1.5]->sortBy(x\|$x)` → `ClassCastException` | **Stop folding** unless every key has one class. (Pure's way is `CS:550-597`, but the tenet prefers the database.) |
| **SF-19d**: `joinStrings` over non-Strings | `[1,2]->joinStrings(',')` accepted | **Stop folding** non-String elements for `JOIN_STRINGS` (keep `stringify` for `MAKE_STRING` only). |
| **SF-5g**: column equality by (name, type) | two columns with one name, two offsets | Compare all of `TDSColumn`'s equality keys, or stop folding column equality. Reach UNVERIFIED. |
| **LU-12**: `in` by raw value (`:596-600`) | `%2020-01-01->in(['2020-01-01'])` → `true` | **Fold Pure's way**: compare `Scalar` records (kind plus value), as `equalityFold` and the `contains` fold already do. A cross-kind needle is not folded (the door decides). |
| **LU-15, LU-16, LU-17**: `slice`, `limit`, `drop` `intValue()` (`:348-357`) | `limit(4294967296)` → `[]` | **Fold Pure's way** with long bounds (`CH:628-686`, `:735-770`), or stop when the count does not fit an int. |
| **UC-2**: `groupBy` raw keys (`UserCallInliner.java:1251-1256`) | `[E.V, 'pkg::E.V']` → one group | Key on (kind, value) (the `Scalar`). Stand when the keys mix kinds. |
| **RL-1**: numeric `==` across kinds and exact Float (`resolver/LiteralFolds.java:81-87`) | `if(1 == 1.0, \|A, \|B)` picks A; Pure picks B | It matches the database and legend-engine. Under the `pure` edition, **stop folding** cross-kind literals (the marker or door answers) and compare a Float as a double. |
| **LW-4**: MatchFold by declared type (`MatchFold.java:41-61`) | `Number`-typed `1` with an `Integer` arm first → the `Number` arm | **Stop folding** (stand, or refuse by name) when the declared type is abstract and an earlier arm names a concrete subtype. |
| **TY-1**: Decimal literal rounded to scale 38 (`Typer.java:278-282`) | a 39-digit fraction | **Refuse by name** instead of rounding (Pure is unbounded). PARK-27 is open. |
| **DR-1**: legend-pure's door, Integer against Decimal → `false` (`PureComparisons.java:41-44`) | `1 == 1D` (compiled Pure `true`) | **The user's ruling**: which Pure, compiled or interpreted? Asserts run compiled. If compiled, Integer against Decimal compares by text (`CS:1010`); this needs a value, so it cannot be decided by the types alone. If interpreted, record it as a row. |

### List 3. Folds that compute values the database should compute in the product (tenet 1)

1. **A product query answered by Java.** LU-1, LU-2, LU-4, LU-11, LU-12 and NF-2 and NF-3 run at the query's own level (`UserCallInliner.java:1360`). A Boolean root becomes a constant plan with nothing sent (`ProductStatements.java:217-235`).
   - Examples: `|1 == 1`, `|2 > 1`, `|'a'->in(['a','b'])`, `|[true]->isTrue()`.
   - An Integer root (`|[1,2,3]->size()`, LU-3) is computed in Java and only echoed by `SELECT 3`.
   - Under the amended tenet 1 ("every value … product verdicts"), these are values the program produces. The 2026-11-19 "Folds" ruling allowed constants. **The two rulings need reconciling.**
   - Option: keep LU and NF only where the result chooses a shape or branch (inside inlined bodies, dead branches, recursion), and leave a root-level comparison to the database.
2. **LU-1f: dates against S399.** The fold answers `%…T10:00:00 == %…T10:00:00.000` with `false`. Lite's product rule (S399) and the database's `==` answer `true`. This is a product **wrong answer** by lite's own rule, not only a tenet question. **Fix: stop folding date or time equality and `in`; leave both to the database.**
3. **StaticFold in relation-generic product bodies (S112) and in `X.columns->map` (any query).**
   - These compute values: SF-8 and SF-19 (text), SF-1 to SF-3 (concatenation, arithmetic), SF-14 (an Integer), and SF-5, SF-10, SF-12, SF-13, SF-16 when the result is a value rather than a schema fact.
   - Allowed under "facts fixed by the types": column names and types, and the schema algebra a checker needs (rename pairs, extend names, restrict lists, dead branches). The fold doc says removing these leaves the checkers' walls loud (`StaticFold.java:56-59`).
   - **Proposal:** a declared "schema" subset that stays: `.columns`, `name`, `type`, `elementToPath`, `+` over Strings, `pair`, `zip`, `map`, `filter`, `isEmpty`, `if`, `in`, `contains`, `==` on Strings, types and enumerations.
   - Text of numbers (`toString`, `makeString` of Long or Double) and Integer arithmetic outside a column name are not folded.
   - `FoldOp` can be intersected with the governed-native catalog in a guard test, as the second review suggests.
4. **RL-1.** This computes a condition's value with SQL's semantics at planning, to choose which statements run. It is a planning choice, so it is acceptable under tenet 1 if the condition is spelled. Its cross-kind numeric rule is an edition question (List 2).
5. **LW-1 (ConstBounds).** `(page-1)*size` is computed in Java for LIMIT and OFFSET. Several dialects need a constant there. This is a named tenet 1 exception to record, not a Pure disagreement.
6. **TP-1** is a ruled carve-out on the test road, not a product value. **LW-2** spells a literal's sign. Neither needs a change.

---

## 7. What this audit did not do (UNVERIFIED or unmeasured)

- **No folding was removed or measured.** "Enables" comes from the code's own comments. Whether removing a fold makes a corpus or PCT checker refuse loudly must be measured, as review M4.1 says.
- **Not checked against the compiled code generator:**
  - How compiled legend-pure generates a `BigInteger` literal (SF-0a, LW-1).
  - Whether groupBy key order (UC-2) is observable after lowering.
  - The reach of SF-5g (column equality) and LW-4 (MatchFold).
- **Not re-read:** TDS null semantics (LU-1b) and the `m3.pure:1411` constants (TY-4); I relied on lite's citations.
- **Read, never executed:**
  - SF-13 and SF-14 rely on a bare `contains` or `indexOf` keeping both referents. `AppliedFunction.java:64-74` says a name is qualified only when one referent is found, and `BareNames.referents` lists every catalog full name declared at the call's arity.
  - LU-1f relies on two DateTime literals of different written precision reaching `equalityFold` unmarked; same type, so no cross-type marker.
- **Correction to the second review (M4):**
  - Java `equals` treating `Long(1)` and `Double(1.0)` as different (`in`, `contains`, `indexOf`, `removeDuplicates`) **agrees** with compiled Pure, which compares `"1"` against `"1.0"`.
  - The real Pure disagreements in those operations are the String-overload misfold (SF-13, SF-14), the raw-value `in` (LU-12), the flattening `==` (SF-5f) and the type-name `==` (SF-5d).
