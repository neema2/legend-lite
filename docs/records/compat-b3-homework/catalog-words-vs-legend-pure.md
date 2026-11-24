# Lite's assert sentences against legend-pure's own (B3 step 3's homework, 2026-11-24)

Read-only research for B3 step 3 ("the catalog in Pure's words", the user's ruling of 2026-11-24: "keep errors close
to pure when cheap/fast so don't have to reword everything on compatibility"). Every legend-pure sentence was read from
the pinned sources (legend-pure 5.99.0, Bazel's `+http_archive+legend_pure_src`; legend-engine 4.145.0 where the
sentence is the engine's). `T/` is `legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/tests/`.

## How the values are printed

- **legend-pure `%r`** (`toRepresentation`, `.../essential/string/toString/toRepresentation.pure:40-51`): a String in
  `'...'` (only `\\`, `\'`, `\n` escaped); a date `%` plus its toString at the precision written (`%2014-02-01`,
  `%2014-01-01T00:00:00.000+0000`, `+0000` from the minute on); a Decimal its plain digits plus `D`; a Float
  `DecimalFormat("0.0")`, up to 340 fraction digits, never an exponent, `-0.0` as `0.0` (`CompiledSupport.java:134-136,
  1273`); Integer and Boolean as they are; an element its path; an instance `<id instanceOf pkg::C>` (a runtime id).
  `%s` is toString (Strings bare, dates without `%`, Decimals without `D`). A collection:
  `map(toRepresentation)->joinStrings('[', ', ', ']')`.
- **Lite** (`testexec/LiteWords.value`, `:35-61`): a String `"..."` with JSON escapes; a Float `messageFloat` (an
  exponent beyond DECIMAL(38,18), `-0.0` kept); a Decimal plain digits, no `D`; a DateTime ISO UTC, nine digits, `Z`, no
  `%`; an instance `m::P {"k":v}` or `m::P (instance i1)`.
- **Records hold text, not values:** `ErrorRecord` stores lite's printed texts, so an edge presenter cannot recover a
  DateTime's written precision; Pure's form must be printed where the values are typed (`AssertRecords`), or the record
  must carry typed values.
- **A cheap Pure printer exists:** `edge/Presenter.legendPure(value, pureType)` (`:44-70, 253-256`) prints Pure's
  toString (`CellText.carriedFloat` positional Floats, dates at their precision with `+0000`, Strings bare); `%r` is that
  plus a String's quotes and escapes, a date's `%`, a Decimal's `D`. It lives in `edge`; `testexec` may not depend on
  the edge (G5).

## The table

| Constant | Lite | legend-pure (verbatim, file:line) | Value forms | Verdict |
|---|---|---|---|---|
| EQUALS | `expected {expected}, got {actual}` | `'\nexpected: %r\nactual:   %r'` one value each (T/assertEquals.pure:20); `'\nexpected: %s\nactual:   %s'` over `[reps]` otherwise (:21); the same frame T/assertEq.pure:19, T/assertIs.pure:19 | `%r`; lite's `pairSide` (`LiteWords:142`) already bare-or-bracketed | CLOSE (quotes, `D`, `%`/precision); instances DIFFERENT (runtime id) |
| EQUALS_KINDS | `expected {expected} ({expectedKind}), got {actual} ({actualKind})` | none: Pure uses EQUALS' sentence; its typed `%r` tells most ties apart (`1`, `1.0`, `1.0D`, `'1'`) | -- | DIFFERENT (lite-only): fold into EQUALS |
| NOT_EQUALS | `expected a value other than {actual}` | `'%r should not equal %r'` (T/assertNotEquals.pure:18; collections :19); T/assetNotEq.pure:17 | `%r`; Pure prints both sides | CLOSE |
| SAME_ELEMENTS | `expected the elements {expected} in any order, got {actual}` | `$expected->sort()->map(toRepresentation)->joinStrings('\nexpected: [', ', ', ']') + $actual->sort()->...('\nactual:   [', ', ', ']')` (T/assertSameElements.pure:19) | both sides SORTED by Pure's compare (`CompiledSupport.java:140, 570-577`) | DIFFERENT: Pure's sort order (Java may not reorder a value: tenet 2) |
| SIZE | `expected size {expected}, got {actual}` | `'expected size: %s, actual size: %s'` (T/assertSize.pure:19) | Integers | SAME |
| NOT_SIZE | `expected a size other than {size}` | `'size should not equal: %s'` (T/assertNotSize.pure:17) | Integer | SAME |
| CONTAINS | `expected {collection} to contain {value}` | `'%s does not contain %r'`, the collection `[reps]` (T/assertContains.pure:22) | `%r` | CLOSE |
| NOT_CONTAINS | `expected {collection} not to contain {value}` | `'%s should not contain %r'` (T/assertNotContains.pure:21) | `%r` | CLOSE |
| EMPTY | `expected no values, got {collection}` | `'%s is not empty'` over `^List<Any>(values=$collection)` (T/assertEmpty.pure:20); `List.toString` maps toString (`platform/pure/anonymousCollections.pure:33-39`): Strings unquoted | toString | CLOSE |
| NOT_EMPTY | `expected at least one value, got none` | `'Expected non-empty collection'` (T/assertNotEmpty.pure:19) | -- | SAME |
| TRUE | `expected true, got false` | `'Assert failed'` (T/assert.pure:31; `fail()` T/fail.pure:19) | -- | SAME |
| FALSE | `expected false, got true` | `assert(!c)`: `'Assert failed'` (T/assert.pure:34-37) | -- | SAME |
| IS | `expected the same instance as {expected}, got {actual}` | `'\nexpected: %r\nactual:   %r'` (T/assertIs.pure:19) | no `AssertKind.IS` reference in main (assertIs goes to EQUALS or SAME_INSTANCE) | CLOSE; dead row |
| IS_NOT | `expected an instance other than {actual}` | `'Should not be identical to: %r'` (T/assertIsNot.pure:19) | `%r` | CLOSE |
| SAME_INSTANCE | `expected the same instance` | assertIs' frame over `<id instanceOf path>` | runtime ids | DIFFERENT |
| INSTANCE_OF | `expected an instance of {type}, got {actual}` | `'expected %r to be an instance of %s, actual: %s'` over [value, `$type->elementToPath()`, the value's type path] (T/assertInstanceOf.pure:19) | +1 slot (the value's type) | CLOSE for primitives; instances DIFFERENT |
| TDS_EQUIVALENT | `expected the rows {expected} within {delta}, got {actual}` | legend-engine: `fail($one->toString(true)+'\n is not equivalent to:\n'+$two->toString(true))` (engine `core_functions_relation/relation/functions/tdsEquivalent.pure:48`); a column or row-count mismatch returns false without a sentence (:29) | the table's printed text | DIFFERENT |
| WITHIN_TOLERANCE | `expected {expected} within {delta}, got {actual}` | `'\nexpected: %r\nactual:   %r'` (T/assertEqWithinTolerance.pure:19), no delta | `%r` | CLOSE |
| JSON_EQUALS | `expected the JSON {expected}, got {actual}` | legend-engine: `'JSON strings don\'t represent semantically same object \n expected: ' + $expected + '\n actual: ' + $actual` (engine `core/pure/corefunctions/testExtension.pure:40`) | the raw strings | CLOSE (lite minifies; raw texts at `AssertRecords.jsonDiffers:351`) |
| ERROR | `expected the error {expected}, got {actual}` | `'Execution error message mismatch.\nThe actual message was "%s"\nwhere the expected message was:"%s"'` over `[$msg, $message]` (T/assertError.pure:74) | raw texts, actual first | CLOSE (escaping) |
| NO_ERROR | `expected the error {expected}, got none` | the ERROR sentence with actual `No error was thrown` (`AssertError.java:62`, `CoreGen.java:350`) | fixed | SAME |
| ERROR_LINE | `expected the error at line {expected}, got line {actual}` | `'Execution error line mismatch. Actual: %d where expected: %d'` (T/assertError.pure:75) | Integers | SAME |
| ERROR_COLUMN | `expected the error at column {expected}, got column {actual}` | `'Execution error column mismatch. Actual: %d where expected: %d'` (:76) | Integers | SAME |
| ERROR_NO_POSITION | `expected the error at a source position, got an error with none` | `"Cannot cast a collection of size 0 to multiplicity [1]"` (CompiledSupport.java:340) | -- | SAME |
| MESSAGE | `{message}` | the message verbatim | -- | SAME |
| SEED_*, CSV_* | (engine's words) | verbatim (engine `testDataGeneration.pure:805, 221, 222, 272`; `toDDL.pure:108, 110, 113`) | -- | SAME (SEED_ROOT_TABLE's class name not checked) |

Lite-only frame: `FrameKind.AT_INDEX` (`" at index N"`); Pure raises the element's message with no index.

legend-pure / legend-engine sentences with no lite constant: `assertEqualsH2Compatible`'s `'More than one H2 version
found asserting test outputs'` (engine `dbSpecific/h2/h2Extension.pure:35`); the engine's `assert<T>(collection, func)`
(`'Assert failed'`, testExtension.pure:48-58); `format()`'s errors (`Format.java:108, 187, 199, 213`); the matcher form
`assertError(f, {msg, si | ...})` (T/assertError.pure:43).

Register S398's citation: the single-value `'%r should not equal %r'` is `assertNotEquals.pure:18` (:19 is the
collection form); S398 does not yet say assertEmpty prints toString (Strings unquoted) or that assertSameElements sorts.

Not confirmed: an enumeration's `%r`; how lite plans `fail()`.

## What step 3 has to decide (for the user, before building)

The sentences (frames) are cheap: SAME rows take Pure's sentence verbatim; most CLOSE rows differ only in how a VALUE
is printed inside the sentence. Pure's `%r` (a String in single quotes, a Decimal with `D`, a date with `%` at its
written precision) is not lite's own value text (S312: a DateTime ISO with nine digits and `Z`; JSON-quoted Strings).
Printing `%r` in the product's messages means a second value printer in the product (the edge's `Presenter.legendPure`
moved or mirrored), and a date's written precision carried to the message (today the record holds lite's nine-digit
text). The DIFFERENT rows (SAME_ELEMENTS' sort, instances' runtime ids, the TDS text) stay lite's and are recorded.

**Ruled by the user, 2026-11-24:** "In product they should print what we decided for lite to be consistent and nice, if
that matches pure great; if not matching AND changes tests, then we can overwrite in compatibility to match tests - if
nothing checks these in compatibility, then can also keep as lite in compatibility". So: Pure's sentences where cheap,
lite's value forms inside them, an edge overwrite only for a compatibility test whose checked message moves.
