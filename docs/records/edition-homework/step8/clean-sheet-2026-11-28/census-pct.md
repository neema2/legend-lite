*A research or audit subagent's report to the step 8 session, 2026-11-28, kept as it was written (read-only work; its scratch files were in the session's container and are not in the repository).*

# PCT print sites in legend-pure 5.99.0 and legend-engine 4.145.0

All numbers come from my scripts (`pct.py`, `analyze.py`, `report.py` in the scratch directory); examples are quoted from the files. Path prefixes used below:

- `PURE` = `legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure` (legend-pure root)
- `REL` = `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-functions-relation-pure/src/main/resources/core_functions_relation/relation`
- `STD` = `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-standard/legend-engine-pure-functions-standard-pure/src/main/resources/core_functions_standard`
- `VAR` = `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-variant/legend-engine-pure-functions-variant-pure/src/main/resources/core_functions_variant`

## Headline

1. **1357 PCT test functions** (488 legend-pure, 869 legend-engine); **501 contain a print call** from the primary set (`toString`, `toRepresentation`, `format`, `joinStrings`, `makeString`, `toCSV`).
2. **Most printing is outside the evaluated lambda (P3): 478 of 635 sites, in 457 tests.** Of these, 432 are a relation result printed by the test's own code: `assertEquals('#TDS\n...#', $res->sort(...)->toString())`. The target returns a relation; the test harness sorts and prints it.
3. **Printing as the lambda's value (P1) exists only in legend-pure:** 97 sites in 34 tests, all in three files (`toString.pure`, `format.pure`, `joinStrings.pure`). legend-engine has 0 primary P1 sites.
4. **Printing inside a lambda and used further (P2): 60 sites in 48 tests.** 35 of them are text-in, text-out (`joinStrings` over String values, or a `toString` qualified property on a test class). Only 25 sites print a possibly non-text value mid-expression.
5. **`toRepresentation`, `makeString` and `toCSV` have 0 sites in any PCT test body.** `toRepresentation` is `<<PCT.function, PCT.platformOnly>>` and its tests are `<<test.Test>>`, not PCT (`PURE/essential/string/toString/toRepresentation.pure:40`, tests at lines 54-162).
6. **No relation PCT test asserts on `toCSV`.** Relation results are asserted as `toString()` text (424 tests) or with `assertTdsEquivalent` (95 tests).

## 1. Counts

Cells are sites / tests.

| Tree | PCT tests | With a primary print | P1 | P2 | P3 |
|---|---|---|---|---|---|
| legend-pure | 488 | 42 | 97 / 34 | 16 / 7 | 1 / 1 |
| legend-engine | 869 | 459 | 0 / 0 | 44 / 41 | 477 / 456 |
| both | 1357 | 501 | 97 / 34 | 60 / 48 | 478 / 457 |

Tests by the positions they contain: P1 only 34; P2 only 10; P3 only 419; P2 and P3 together 38.

| Print function | pure P1 | pure P2 | pure P3 | engine P1 | engine P2 | engine P3 |
|---|---|---|---|---|---|---|
| `toString` | 47 / 17 | 14 / 6 | 1 / 1 | 0 | 11 / 8 | 466 / 456 |
| `format` | 42 / 13 | 0 | 0 | 0 | 0 | 0 |
| `joinStrings` | 8 / 4 | 2 / 2 | 0 | 0 | 33 / 33 | 11 / 11 |
| `toRepresentation`, `makeString`, `toCSV` | 0 | 0 | 0 | 0 | 0 | 0 |

Secondary text producers, counted separately and not in the totals above (all engine, all P1):
- `char`: 9 sites / 6 tests, e.g. lambda `65->char()` (`core_functions_unclassified/string/ascii/char.pure:22`).
- `toJson`: 3 sites / 1 test (`VAR/functions/convert/toJson.pure:28-30`).
- `formatDate`: 2 sites / 2 tests (`STD/date/operation/formatDate.pure:67,72`).
- `to(@String)` / `cast(@String)` / `toMany(@String)` appear 9 times inside evaluated lambdas (4 / 4 / 1).

| Module (top-level resource directory) | Tests | With print | P1 | P2 | P3 |
|---|---|---|---|---|---|
| pure `platform/pure/essential` | 345 | 41 | 97 / 34 | 13 / 6 | 1 / 1 |
| pure `platform/pure/grammar` | 143 | 1 | 0 | 3 / 1 | 0 |
| engine `core_functions_relation` | 469 | 367 | 0 | 42 / 39 | 384 / 365 |
| engine `core_functions_standard` | 205 | 58 | 0 | 1 / 1 | 58 / 58 |
| engine `core_functions_variant` | 88 | 33 | 0 | 1 / 1 | 34 / 32 |
| engine `core_functions_unclassified` | 93 | 0 | 0 | 0 | 0 |
| engine `core_dataquality` (xts-dataquality) | 7 | 0 | 0 | 0 | 0 |
| engine `core_scenario_quant` | 6 | 0 | 0 | 0 | 0 |
| engine `core` (compiled-core) | 1 | 1 | 0 | 0 | 1 / 1 |

## 2. P1: the print is the lambda's value (97 sites, legend-pure only)

No P1 site has a trailing `replace`/`length`/`trim`/etc. after the print; the allowed-suffix rule matched nothing.

**`toString` (47 sites), `PURE/essential/string/toString/toString.pure`.** Receiver by literal syntax: Float 14, DateTime 13, Integer 2, Boolean 2, String 2, StrictDate 2, year-month 2, year 2, `^List` 2, `^Pair` 2, a class 1, an enumeration 1, an enum value 1, a class instance with its own `toString()` 1.
- `:54` `assertEq('-1', $f->eval(|toString(-1)));`
- `:66` `assertEq('17.0', $f->eval(|17.000->toString()));`
- `:72` `assertEq('0.01', $f->eval(|.01->toString()));`
- `:77` `assertEq('134210000.0', $f->eval(|134.21e6->toString()));`
- `:85` `assertEq('0.000000013421', $f->eval(|134.21e-10->toString()));`
- `:105` `assertEq('2014-01-02T01:54:27.352+0000', $f->eval(|%2014-01-01T20:54:27.352-0500->toString()));`
- `:128` `assertEq('2014-01-01T00:00:00+0000', $f->eval(|%2014-1-1T0:00:00->toString()));`
- `:133` `assertEq('2014-01-01', $f->eval(|%2014-01-01->toString()));` and `:136` `'2014-01'`, `:139` `'2014'`
- `:146` `assertEq('[[a, b], c]', $f->eval(|^List<Any>(values=[^List<String>(values=['a', 'b']), 'c'])->toString()));`
- `:152` `assertEq('<a, 1>', $f->eval(|^Pair<String, Integer>(first='a', second=1)->toString()));`
- `:176` `assertEq('CITY', $f->eval(|STR_GeographicEntityType.CITY->toString()));`
- `:181` `assertEq('// Warning: Good for gin -- Sad times no tonic', $f->eval(|^ClassWithComplexToString(errorType=ErrorType.NoTonic,errorMessage='Sad times')->toString()));` The class's `toString()` property calls `format` (`:206-213`).

**`format` (42 sites), `PURE/essential/string/toString/format.pure`.** Every site is prefix-form `format('literal', args)` with literal args.

| Specifier | Occurrences |
|---|---|
| `%s` | 41 |
| `%.Nf` | 18 |
| `%t{pattern}` | 8 |
| `%d` | 2 |
| `%0Nd` | 2 |
| `%f` | 2 |
| `%r` | 2 |
| `%t` | 1 |

Non-String values passed to `%s`: a DateTime (`:137`), Booleans (`:160-161`), a `^List` (`:166`), a `^Pair` (`:171`).
- `:95` `assertEq('the quick brown fox jumps over the lazy 00003', $f->eval(|format('the quick brown %s jumps over the lazy %05d', ['fox', 3])));`
- `:121` `assertEq('... the lazy 10.00', $f->eval(|format('... the lazy %.2f', ['fox', 9.999])));`
- `:101` `assertEq('... the lazy 1.5', $f->eval(|format('... the lazy %f', ['fox', 1.5])));`
- `:138` `assertEq('... the lazy 2014-01-01T00:00:00.000+0000', $f->eval(|format('... the lazy %t', ['fox', %2014-01-01T00:00:00.000])));`
- `:149` `assertEq('on 2014-03-10 08:07:44.001-0500', $f->eval(|format('on %t{[EST]yyyy-MM-dd HH:mm:ss.SSSZ}', %2014-03-10T13:07:44.001)));`
- `:177` `assertEq('... the lazy %2014-01-01T00:00:00.000+0000', $f->eval(|format('... the lazy %r', ['fox', %2014-01-01T00:00:00.000])));`

**`joinStrings` (8 sites), `PURE/essential/string/toString/joinStrings.pure`.** All are over lists of String literals, so text in, text out.
- `:50` `assertEq('a,b,c', $f->eval(|joinStrings(['a', 'b', 'c'], ',')));`
- `:60` `assertEq('[a,b,c]', $f->eval(|['a', 'b', 'c']->joinStrings('[', ',', ']')));`
- `:71` `assertEq('', $f->eval(|joinStrings(['a']->tail(), ','))); `

**Secondary, engine:**
- `VAR/functions/convert/toJson.pure:29` `assertEquals('{"Hello":"World"}', $f->eval(|fromJson('{ "Hello" : "World" }')->toJson()));`
- `STD/date/operation/formatDate.pure:72` `assertEquals('2024-08-15T15:33:42.000000000', $f->eval(|formatDate(%2024-08-15T15:33:42, DateTimeFormat.ISO8601_NanoSecondPrecision)));`

## 3. P2: the print is inside the lambda and used further (60 sites, 48 tests)

There are exactly 60, so the 33 uniform S1 sites are grouped by file; the other 27 are one per line. All 60 are rows in `sites.tsv`.

**S1. `joinStrings` as a relation aggregate or window function over a String column (33 sites, 33 tests; text in, text out).** Column types read from the TDS literals.
- `REL/functions/aggregation/joinStrings.pure:117,141,163,184,203,224,245,265,285,305`, e.g. `:117` `#TDS grp, id, name ...#->groupBy(~grp, ~names : g | $g->joinStrings(~name, ',', ~id->ascending()))`; `:141` adds `->filter(r | $r.names == 'A,B,C')`.
- `REL/functions/transformation/groupBy.pure:83,116,148,181,213,246,278,312`, e.g. `:83` `...->groupBy(~grp, ~newCol : x | $x.name : y | $y->joinStrings(''))`.
- `REL/functions/transformation/extend.pure:640,677,784,822,1148,1183,1218,1254,1289,1326`, e.g. `:640` `...->extend(over(~grp, ~id->descending()), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(''))`.
- `REL/functions/size/size.pure:68,93`; `REL/functions/transformation/aggregate.pure:71` (`codes : x | $x.code : y | $y->joinStrings(':')`).
- `REL/tests/composition.pure:1173` (window, `joinStrings(':')`); `:2200` `$tds->extend(~joined:x | $x.payload->toMany(@String)->joinStrings(','))`.

**S2. `toString` in a relation column lambda, joined with `+` (7 sites, 5 tests).**
- `REL/tests/composition.pure:62` `#TDS val, str 1, a ...#->extend(~newCol:x|$x.str->toOne() + $x.val->toOne()->toString())->filter(x|$x.newCol == 'qw4');` (Integer column)
- `REL/tests/composition.pure:1097` (3 sites) `->extend(~newCol:c|$c.col_one_num->toOne()->toString() + $c.colTwoLetter->toOne()->toString() + $c._Col_3mix->toOne()->toString())` (Integer, String, Integer columns)
- `REL/functions/graph/project.pure:169` `...->project(~[ name:c|$c.str->toOne() + $c.val->toOne()->toString() ])`
- `REL/functions/transformation/extend.pure:61` `...->extend(~name:c|$c.str->toOne() + $c.val->toOne()->toString())`
- `REL/functions/transformation/extend.pure:87` same, with the TDS bound by `let t = ...; $t->extend(...)`

**S3. `toString` is the column value itself (2 sites, 2 tests).**
- `STD/date/extract/dayOfWeek.pure:40` `#TDS date 1985-01-12T06:30:00Z ...#->extend(~dayOfWeek: x | $x.date->toOne()->dayOfWeek()->toString())` (an enum value printed per row; expected cells `Saturday`, `Thursday`, ...)
- `REL/tests/composition.pure:1629` `...->project(~[name2: x|$x.name->toOne()->toString()])->groupBy(~name2, ...)` (String column)

**S4. `toString` compared inside a relation filter (1 site).**
- `REL/tests/composition.pure:1099` `->filter(x|$x._new_col->toOne()->toString()->contains('A'))` (String column)

**S5. `toString` as key or compared with `==` in a collection lambda (9 sites, 3 tests; mixed Integer/String lists).**
- `PURE/essential/collection/transformation/removeDuplicatesBy.pure:49` `assertEquals([1, 2, '3'], $f->eval(|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicatesBy({x | $x->toString()})));`
- `...removeDuplicatesBy.pure:50` same with `['1', 2, '1', '3', 1, 3, '3', 2]`
- `PURE/essential/collection/transformation/removeDuplicates.pure:118` (2 sites) `...->removeDuplicates({x, y | $x->toString() == $y->toString()})`
- `...removeDuplicates.pure:119` (2 sites) same shape
- `...removeDuplicates.pure:131` (2 sites) `[]->removeDuplicates({x, y | $x->toString() == $y->toString()})`
- `...removeDuplicates.pure:132` `[]->removeDuplicatesBy({x | $x->toString()})`

**S6. Scalar `toString` joined with `+` (4 sites, 2 tests).**
- `PURE/grammar/functions/string/operation/plus.pure:45` (3 sites) `assertEq('1 2 3', $f->eval(|toString(1) + ' ' + toString(1 + 1) + ' ' + toString(2 + 1)));`
- `PURE/essential/lang/flow/match.pure:120` `['w','w','w']->match([ i:Integer[1..4] | 'z', s:String[*] | 'address:' + $s->size()->toString() ])`

**S7. `toString` inside `map`, then `joinStrings`, wrapped with `+` (2 sites, 1 test).**
- `PURE/essential/string/toString/toString.pure:158` `assertEq('(<a, b>,<a, 1>)', $f->eval(|'('+ $pairs->map(p| $p.toString())->joinStrings(',')+ ')'));`
- `$p.toString()` is Pair's qualified property, which itself calls `toString` and `joinStrings` (`PURE/essential/anonymousCollections.pure:21-24`).

**S8. `joinStrings` over `String[*]` joined with `+` in a match branch (1 site; text in, text out).**
- `PURE/essential/lang/flow/match.pure:214` `[1]->match([{i:Integer[1], b:String[*]|'good_'+$b->joinStrings('', '', '');}, ...],['other', 'yo'])`

**S9. `.toString()` that is a test class's own qualified property (1 site; text in, text out).**
- `VAR/functions/tests/composition.pure:116` `fromJson('[...]')->toMany(@Person).previousAddresses.toString()`; `Address.toString()` is `$this.line1 + ' ' + $this.line2` (`:189-191`).

Shapes I looked for and did not find in any P2 site: a print placed directly in a list literal, `format` with a computed argument, a scalar print compared at the top level of the lambda.

## 4. P3: the print is in the test's own code (478 sites, 457 tests)

| Shape | Sites | Tests |
|---|---|---|
| R1: `assertEquals('#TDS...' text, $res->sort(...)->toString())` | 357 | — |
| R2: `assertEquals('#TDS...' text, $res->toString())`, no sort | 64 | — |
| R3: `... $res->sort(...)->select(~[...])->toString()` | 11 | — |
| R1-R3 together (relation `toString`) | 432 | 424 |
| V: Variant result printed, `$f->eval(|...)->toString()` | 34 | 32 |
| J: test-side `joinStrings` normalising a column before the assert | 11 | 11 |
| O: other | 1 | 1 |

In every R and V site the `toString` is the second argument of `assertEquals` and the expected value is a String literal (concatenated). The printed relation is never an expected `#TDS` island.

Relation `toString` is itself Pure code marked `<<PCT.function, PCT.platformOnly>>` (`REL/functions/toString.pure:19,24`). It prints each cell with `s()` (`REL/functions/s.pure:23-39`): `null` for empty, quoted for Variant and for Strings containing `{` or `[`, otherwise `$o->toString()`.

Examples:
- **R1** `REL/functions/ranking/cumulativeDistribution.pure:42-54`: `assertEquals( '#TDS\n'+ '   id,grp,name,other\n'+ '   10,0,J,1.0\n'+ '   8,1,H,0.33\n'+ ... '#', $res->sort([~grp->ascending(), ~id->descending()])->toString());`
- **R1** `STD/math/aggregator/stdDevSample.pure:78-83`: `assertEquals( '#TDS\n'+ '   id,newCol\n'+ '   1,1.0\n'+ '   2,1.0\n'+ '   3,0.5\n'+ '#', $res->sort(~id->ascending())->toString());`
- **R1** `REL/functions/olap/over.pure:2150-2168`: `'   0,2024-01-29T00:32:34.000+0000,10,20\n'+ ... '#', $res->sort([~p->ascending(), ~o->ascending(), ~i->ascending()])->toString());`
- **R1** `STD/date/extract/dayOfWeek.pure:45-51`: `'   1985-01-12T06:30:00.000+0000,Saturday\n'+ ... '#', $res->sort(~date->ascending())->toString());`
- **R2** `REL/tests/composition.pure:68-71`: `assertEquals( '#TDS\n'+ '   val,str,newCol\n'+ '   4,qw,qw4\n'+ '#', $res->toString());`
- **R2** `REL/tests/composition.pure:882-889`: `'   1,\'[2,1,3]\',\'[1,2,3]\'\n'+ ... '#', $res->toString());` (Variant cells quoted)
- **R3** `REL/tests/composition.pure:1635-1641`: `'#', $res->sort(~name2->ascending())->select(~[name2, newCol])->toString());`
- **V** `VAR/functions/navigation/get.pure:43`: `assertEquals('"world"', $f->eval(|fromJson('{"hello":"world"}')->get('hello')->toOne())->toString());`
- **V** `VAR/functions/convert/toVariant.pure:28`: `assertEquals('null', $f->eval(|toVariant([]))->toString());`
- **V** `VAR/functions/tests/composition.pure:24`: `assertEquals('[2,3,4]', $f->eval(|toVariant([1, 2, 3])->toMany(@Variant)->map(x | $x->to(@Integer)->toOne() + 1)->toVariant())->toString());`
- V sites by file: `toVariant.pure` 14, `fromJson.pure` 12, `get.pure` 4, `tests/composition.pure` 4.
- **J** `REL/functions/transformation/groupBy.pure:87`: `let resSorted = $res->extend(~newColSorted : c | $c.newCol->toOne()->chunk(1)->sort()->joinStrings('');)`, then `:97` `$resSorted->sort(~grp->ascending())->select(~[grp, newColSorted])->toString()`.
- **J** `REL/tests/composition.pure:1177`: `let res = $f->eval($expr)->extend(~newColSorted:r|$r.newCol->toOne()->split(':')->sort()->joinStrings(''))->select(...)`.
- **O** `PURE/essential/string/toString/toString.pure:164`: `let str = ^STR_Person(firstName='Pierre', lastName='Doe')->toString();`, then `:165` `assert($f->eval(|$str->startsWith('Anonymous_') || $str->startsWith('@_')));`.

**What the 432 expected `#TDS` texts contain, by the look of each cell:**

| Cell looks like | Sites | Cells |
|---|---|---|
| integer | 400 | 6912 |
| other text | 206 | 1479 |
| `null` | 69 | 328 |
| float (e.g. `1.0`, `0.33`, `101.0`) | 49 | 509 |
| datetime (`2024-01-29T00:32:34.000+0000`) | 37 | 530 |
| quoted (Variant/JSON) | 31 | 291 |
| `true` / `false` | 12 | 41 |
| date-only (`yyyy-MM-dd`) | 0 | 0 |

Float cells are mostly in the STD aggregators (`average`, `median`, `sum`, `min`, `max`, `mode`, `stdDev*`, `variance*`, `percentile`, `wavg`, `covar*`, `corr`) and in REL ranking, `over` and `extend`.

## 5. How relation tests assert their result

I counted 547 relation tests: stereotype contains `relation` (343), or a `#TDS` in the evaluated lambda (498), or asserted as TDS text or with `assertTdsEquivalent`.

| Assertion | Tests |
|---|---|
| `assertEquals(text, $res[->sort][->select]->toString())` | 424 |
| `assertTdsEquivalent(#TDS expected#, $res->sort(...), delta[, timeDelta])` | 95 |
| `assertEquals` on a non-relation value | 21 |
| `assertError` | 5 |
| `assert` / `assertFalse` | 2 |
| `toCSV` | 0 |

No test mixes the first two. `assertTdsEquivalent` is typed, not text: numbers within a delta, dates within seconds, but Variant columns by `toString() ==`; it prints `toString(true)` only in the failure message (`REL/functions/tdsEquivalent.pure:25-48`, Variant at `:39`, `fail` at `:48`). Its 95 tests: `REL/functions/olap/reduce.pure` 66, `REL/functions/transformation/pivot.pure` 12, `REL/tests/composition.pure` 6, `STD/math/aggregator/zScore.pure` 3, `core_scenario_quant` 6, `over.pure` 1, `median.pure` 1.

Five examples:
1. `REL/functions/iteration/filter.pure:22-33`: `let expr = {|#TDS val 1 3 4 #->filter(x|$x.val > 1)}; let res = $f->eval($expr);`, then text `toString` (shape R1/R2).
2. `REL/tests/composition.pure:52-71` (testExtendFilter): `assertEquals( '#TDS\n'+ '   val,str,newCol\n'+ '   4,qw,qw4\n'+ '#', $res->toString());`
3. `REL/tests/composition.pure:1093-1112`: `assertEquals( '#TDS\n'+ '   col_one_num,colTwoLetter,Result\n'+ '   1,A,1A1\n'+ ... '#', $res->sort(~col_one_num->ascending())->toString());`
4. `REL/tests/composition.pure:1762`: `meta::pure::functions::relation::assertTdsEquivalent( #TDS id,plus,times,minus ...#, $res->sort(~id->ascending()), 0.01 )`
5. `REL/tests/composition.pure:2139`: `assertTdsEquivalent( #TDS id, 'A__|__newCol', ...#, $res->cast(@Relation<(id:Integer, '\'A__|__newCol\'':Number, ...)>)->sort([ascending(~id)]), 0.00001 )`

Non-relation outcomes: `legend-engine-xts-dataquality/.../core_dataquality/generation/dataquality_relation_helper.pure:159` `assertEquals(true, $res)`; `REL/functions/olap/over.pure:957` `assertError(|$f->eval($expr), 'Invalid window frame boundary - ...')`.

## 6. Limits of the parser

- **Lexical, not a Pure parser.** It masks `'...'` strings, `//` and `/* */` comments, `'''` doc blocks, `#...#` islands and `###` section headers, then matches brackets. A `#` or `'` inside a TDS island would break masking for the rest of that file. I cross-checked the test count against grep: 1364 `PCT.test` occurrences minus 2 Profile lines, 2 comment mentions and 3 commented-out tests gives 1357.
- **Only `$<first parameter>->eval(arg)` is followed.** The lambda is inline (2290 calls) or a `let`-bound lambda in the same function (547 calls); nothing was unresolved. Prints inside functions or qualified properties called from the lambda are invisible (e.g. `Pair.toString()`, `ClassWithComplexToString.toString()`, relation `toString` itself).
- **Sites are matched by name, with no types.** Relation `joinStrings(~col, sep, order)` and `.toString()` qualified properties are counted with the string functions. "What is printed" is known only for literals; column types in section 3 were read by me from the TDS literals; `$x` in `removeDuplicates` is a mixed list.
- **`joinStrings` is counted even over Strings.** Sections 2-4 say where it is text in, text out.
- **P1 means the last statement is a pure arrow or prefix chain ending in the print.** Any top-level operator such as `+` makes every print in it P2 (S6, S7). I reviewed all 111 P1 and 60 P2 sites by eye; the 478 P3 sites were reviewed by shape and sample, not one by one.
- **P3 "eval result" follows `let` names transitively by name only.** The R/V/J split relies on the expected text starting with `#TDS` and on file location.
- **Cell kinds are by look.** A String column holding `1` counts as integer-looking; cells are split on commas outside single quotes.
- **The relation-test definition is a heuristic** (section 5). Tests are counted per function, not per `eval` call.
- **String `+` without a print call is not counted.**

## Files

All in `/tmp/claude-0/-home-user-legend-lite/a081db72-2095-5e3c-b0e3-e111003021e6/scratchpad/mine-pct/`:
- `sites.tsv`: **538 data rows plus a header** (60 P2, 478 P3; primary print functions only, since the secondary ones are all P1). Columns: tree, file, line, position, print_function, snippet, then two extras I added: shape, what.
- `report_raw.txt`: the full script output, including every P2 site and P3 samples.
- `data.json`: all 649 sites, including P1 and the secondary functions, plus per-test data.
- `pct.py`, `analyze.py`, `report.py`: the scripts.
