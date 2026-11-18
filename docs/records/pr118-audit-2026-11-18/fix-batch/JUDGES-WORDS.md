# JUDGES-WORDS: verdict SOUND-WITH-DEBT

## Summary (at most 10 lines)
All nine commits in my share fix what they claim at the fix head `eec3c95c7`. I found no blocker. F4: the host judge now compares assertEq, assertIs, assertNotEq and assertIsNot by each side's declared numeric kind, the same as assertEquals and the database judge. The negated asserts inherit this through `negatedOf`, so no meaning is inverted. The D strip runs only under lite's words (`m.lite()`); the canon SQL is unchanged. N2: only `assert` with no message takes the style-worded default. PURE and ENGINE still answer "Assert failed" on every road, and lite says "expected true, got false". F7 puts main's FLOAT_SHORTEST_TEXT back for relation-text Float cells on every dialect. F8 rewords exactly 12 ledger rows and changes no verdict. Every pin move is on the approved list.
The real debt:
- **Two pins held by formatting.** f250b02d6 kept two JavaEvalLedgerTest pins: DatabaseJudge 851 by joining two lines, and AssertVerdicts 1464 by not wrapping a 171-character line. It does not say so.
- **R1 only partly applied.** Six in-body guards still answer legend-pure's "Assert failed" in the product. In-body assertFalse and top-level assertFalse now answer differently.
- **FloatShortestText is not legend-pure's print everywhere.** It differs for whole values at or above 2^53, for Infinity, and for -0.0. This was already true on main.

## Per commit (sha, claim, verdict)
- 57ac8628c F4: the host assertEq family compares by declared kind, and lite's Decimal text drops the D. **Fixes it.**
- 8a00cf5ea F4 follow-up: the strip moved into `LiteralSpelling.withoutDecimalMark`. The SQL is the same and the SqlFnSortedTest pin is unchanged. **Fixes it** (a real shared helper, see NOTE 7).
- d9fb10221 F4 review: `comparedKind` goes through `Equality.declaredNumeric`, `decimalCanon` reuses the strip, and the test runs on DuckDB, H2 and SQLite. **Fixes it.**
- f250b02d6 N2 judges' half: the vector assert's default is worded by the run's words. **Fixes it.** One pin is held by a line join (finding 1).
- 373e42960 N2 product-query half: an in-body assert's default comes from the style. **Partly** on its own: it applied to all seven guards, so assertFalse said "expected true". 9e6eeba52 corrected it.
- 9e6eeba52 N2 correction: the style default narrowed to `assert`. **Fixes it.**
- deee2d6ee N2 correction: native-claims.tsv regenerated and the AT_ list put in order. **Fixes it.**
- 788ef654e F7: an edge relation's Float cell printed as legend-pure prints it. **Fixes it** inside FloatShortestText's windows. This restores main's FLOAT_PRINT. Divergences outside the windows were already on main (finding 4).
- 4274a1ea7 F8: channel B rows carry the range refusal's new wording. **Fixes it.** Checked by a script: 12 of 12 rows changed by the prefix alone.

## Blockers
None.

## Findings

### JUDGES-WORDS-1 [BROKEN-RULE] [FIX-AFTER] Two JavaEvalLedgerTest pins held by formatting, not stated
- **Where:**
  - `core/src/main/java/com/legend/testplan/DatabaseJudge.java:924`: `boolean assertFamily = ... ASSERT || ... ASSERT_FALSE;`. Two lines were joined into one 150-character line.
  - `core/src/main/java/com/legend/testplan/AssertVerdicts.java:1489`: a 171-character line left unwrapped. At the PR head the file had 1 line over 150 characters; it now has 2.
- **What:**
  - In DatabaseJudge, f250b02d6 split the `assertMessage(root, words(env).conditionFailed(...))` argument across two lines (+1 stripped line) and joined `assertFamily` (-1). The stripped count stays at exactly 851, the value the user ratified (R3).
  - In AssertVerdicts, wrapping the new argument would give 1465 against the exact pin of 1464.
  - The commit says only "The pinned files keep their line counts".
- **Why it matters:**
  - AGENTS.md: a pin moves only with a dated justification. The ledger's own words: "evict, or bump the pin with a written justification". Holding a count through layout is the pattern the brief asks about.
  - The substance is harmless: the added code passes a message argument and evaluates nothing, and no pin moved. So the guard is not vacuous.
- **Instead:** about 4 lines. Re-wrap both sites and re-pin DatabaseJudge 851 -> 852 and AssertVerdicts 1464 -> 1465, each with a dated note ("N2, a message argument, nothing evaluated"). The DatabaseJudge move needs the user's word, because R3 ratified 851. Or keep the layout and say so in a note beside the pin.
- **Confidence:** verified. Stripped counts were measured with JavaEvalLedgerTest's own rule; line lengths with awk.

### JUDGES-WORDS-2 [WRONG-DESIGN] [FIX-AFTER] R1 is applied to `assert` alone; six in-body guards keep legend-pure's "Assert failed" in the product
- **Where:**
  - `core/src/main/java/com/legend/compiler/spec/LambdaBodies.java:129-136`.
  - The register row `CompatSpellingGuardTest.java:154-158` and `TerminalRowsFrozenTest.java:50-53`, still on the orchestrator's J29.
- **What:**
  - Only `meta::pure::functions::asserts::assert` lowers through `assertFailedText()`. In the product, an in-body `assertTrue`, `assertFalse`, `assertEquals`, `assertNotEquals`, `assertEmpty` or `assertNotEmpty` with no message raises "Assert failed". `PureOnlyTextTest.java:67-75` pins this for assertFalse and assertEquals.
  - The same guard now has two product defaults, depending on the road:
    - A top-level test-body `{|assertFalse(true)}` says "expected false, got true" (`LiteHostWords.condition`; `DatabaseJudge.java:1041`). After f250b02d6, so does the vector form.
    - `{|assertFalse($r.x < 0); 0;}` inside a lambda says "Assert failed".
- **Why it matters:**
  - The narrowing in 9e6eeba52 was right for assertFalse: "expected true" would have inverted its meaning.
  - But R1's second clause reads "legend-pure's 'Assert failed' only under PURE and ENGINE". The register note's reading, "R1 ruled assert's default alone", is the fixer's reading, not the user's words.
  - assertTrue has assert's polarity and could take TRUE with no inversion.
  - The J29 comment above the row (`CompatSpellingGuardTest.java:148-153`) still calls "Assert failed" "lite's own default sentence", which R1 contradicts. It is stale.
- **Instead:** about 15 lines. A per-guard default through the same native family: assertTrue gives TRUE and assertFalse gives FALSE, and the value guards a value-free lite sentence. The edge keeps "Assert failed" for all. Alternatively, put the six guards to the user as an R1 follow-up. Either way, update the stale J29 comment.
- **Confidence:** verified by reading. The inconsistency follows from the code paths cited.

### JUDGES-WORDS-3 [COSMETIC] [NOTE] A javadoc on the wrong constant in Pure.java
- **Where:** `core/src/main/java/com/legend/builtin/Pure.java:981-983`.
- **What:**
  - 373e42960 inserted the new signature between TYPE_RAISE's javadoc and TYPE_RAISE's declaration.
  - So `ASSERT_FAILED_TEXT` reads "A run-time type check's failure (see Lite#TYPE_RAISE)", and `TYPE_RAISE__STRING_1__STRING_1__STRING_1` has no javadoc.
  - Also, `LambdaBodies.java:129` compares a string literal where `PlatformTypes.ASSERT` exists (the file's own `ASSERT_GUARDS` uses literals too).
- **Instead:** about 2 lines. Move the line and give ASSERT_FAILED_TEXT its own javadoc.
- **Confidence:** verified.

### JUDGES-WORDS-4 [WRONG-ANSWER, already on main] [NOTE] F7's "as legend-pure prints it" holds only inside FloatShortestText's windows
- **Where:**
  - `core/src/main/java/com/legend/sql/dialect/FloatShortestText.java:110-158` (the HUGEINT whole-value arm `:137-140`; the non-finite names `:170-178`).
  - The claim: `edge/RelationTextLowering.java:508-516`.
- **What:** legend-pure compiled prints a TDS cell's double as `value == 0.0 ? "0.0" : DecimalFormat("0.0", ENGLISH, 340 digits)` (`CompiledSupport.primitiveToString(double)`). FloatShortestText differs from that here:
  - **Whole values at or above 2^53 with an exponent text.** FloatShortestText writes the exact integer through HUGEINT; legend-pure writes the shortest digits padded with zeros. Example on DuckDB: `#TDS\nx:Float\n1152921504606846976.0\n#->toString()` (2^60). By reading the edge writes `1152921504606846976.0`; legend-pure writes `1152921504606846980.0`.
  - **Infinity.** FloatShortestText writes `Infinity`; legend-pure writes `∞`.
  - **-0.0.** FloatShortestText writes `-0.0` (suspected for DuckDB's text); legend-pure writes `0.0`, as register S117 notes.
  - **Below 1e-17 or from 1e38 up.** FloatShortestText keeps the database's exponent form; legend-pure writes plain digits.
  - **SQLite past 2^63.** The value is wrong (Q4, already queued).
- **Why it matters:** F7 restores main's code exactly (main's FLOAT_PRINT is the same decomposition), so none of this is new to the batch. Only the Q4 clamp and the windows' exponent form are recorded (FOLLOW_UPS Q4, SqlFn's javadoc). The 2^53 digits and Infinity are not.
- **Instead:** queue these with Q4 for the portability program's Java writer, where `DecimalFormat` over the typed double gives legend-pure's text exactly.
- **Confidence:** Java side verified (I ran DecimalFormat). Lite side verified by reading FloatShortestText, not run.

### JUDGES-WORDS-5 [WRONG-DESIGN] [NOTE] F7 adds SQL that builds legend-pure's print; it is recorded debt (D1), and it grows
- **Where:** `core/src/main/java/com/legend/edge/RelationTextLowering.java:508-516, 576-579, 704`. The file grew from 781 to 791 lines.
- **What:**
  - The fix composes legend-pure's printed text in SQL: FLOAT_SHORTEST_TEXT reads the text about nine times, and in a CSV cell it sits inside `escapeCsv`'s six reads. The design intent was Java over typed rows.
  - `RelationTextLowering` is in D1's table (780 lines; `edge-follow-ups-2026-11-18.md` D1), so this is recorded debt, not new in kind. It is also exactly main's shape.
  - The cost is the approved channel B growth: relation judge chars +17,124 and plan +17,184; standard chars +100,145 and plan +100,499.
- **Instead:** no action before merge. The portability program's Java writer removes it.
- **Confidence:** verified.

### JUDGES-WORDS-6 [WRONG-ANSWER] [NOTE] A Decimal-declared side carried as a non-finite Double errors instead of judging
- **Where:** `core/src/main/java/com/legend/testexec/Equality.java:176-192` (the DECIMAL branch has no `nonFinite` guard) and `:434-436` (`new BigDecimal(n.toString())`).
- **What:**
  - Since F4, a Decimal property mapped to a DOUBLE column holding NaN or Infinity reaches the DECIMAL branch in assertEq. The `Cells` batch admits a Double (`result/Batch.java:166-171`).
  - There it throws NumberFormatException: `assertEq(1.5D, $p.d)` errors, where before it failed by verdict, and `assertNotEq` errors where before it passed.
  - assertEquals has had the same path since it began reading declared kinds.
- **Why it matters:** contrived input (a Decimal over a DOUBLE holding a non-finite value), but it is a loud error where a verdict is due.
- **Instead:** 3 lines. In the DECIMAL branch, `if (nonFinite(e) || nonFinite(a)) return e.equals(a);`.
- **Confidence:** suspected. Found by reading; not run.

### JUDGES-WORDS-7 [COSMETIC] [NOTE] The SqlFnSortedTest pin is kept by the helper's placement
- **Where:** `core/src/main/java/com/legend/lowering/LiteralSpelling.java:357-372`; `SqlFnSortedTest.java:345-348`.
- **What:**
  - LiteText now composes REGEXP_REPLACE through `LiteralSpelling.withoutDecimalMark`. The BOTH_WRITERS census counts writers per class, so it no longer sees LiteText as a user of REGEXP_REPLACE.
  - This is a genuine de-duplication: `decimalCanon` calls the same helper, the expression tree is identical, and the commit says so. So it is not a narrowed guard, only a limit of a per-call-site census.
- **Confidence:** verified.

### JUDGES-WORDS-8 [COSMETIC] [NOTE] F4's test covers one numeric pairing
- **Where:** `core/src/test/java/com/legend/testplan/AssertBarTest.java:322-389`.
- **What:**
  - It pins an Integer literal against a Decimal over INTEGER (both shapes), plus scale and sign.
  - By reading, the new rule also changes these pairings, all toward the database judge:
    - a Float-declared side on an integer carrier, which now equals an equal Float literal;
    - a Decimal-declared side on a Double carrier (SQLite's REAL);
    - a declared Number, which keeps its carrier as before.
  - None of them is tested.
  - The edge test (`CompiledPureNumbersEdgeTest.java:62-104`) checks the exception class only; the edge's negated words still differ by judge (J4, queued).
  - The new test also adds about 84 fresh-session runs to the medium-sized `core_tests_testplan`. Not measured here (no Bazel).
- **Instead:** two or three more rows in the same table.
- **Confidence:** verified by reading.

## Strengths
- **F4's rule is minimal and correct.**
  - `effectiveKind` (`Equality.java:125-133`) already took a numeric declaration over a numeric carrier. F4 only passes the plan's `SideFacts.declared` (`JudgmentFacts.java:33-37`, `t.typeName()`, so "Decimal(p,s)" is covered by `declaredNumeric`'s prefix arm) into `eqHolds`.
  - A non-numeric declaration (Number, Any, a class, a type variable) keeps the carrier, as before.
  - The negated natives (`AssertVerdicts.java:990-992`) negate the same judgment, so assertNotEq and assertIsNot move with assertEq.
  - The edge's `CompiledPureHostForms.equalAcrossNumberKinds` gives the same answers on these shapes as upstream's compiled eq.
  - The words name the kind that was compared (`LiteHostWords.comparedKind`), matching the database judge's sentence.
- **The D strip is gated to lite's words.**
  - `LiteText` is package-private in `lowering`. It is reached only from `LiteVerdictWords` and `VerdictSql.written` under `m.lite()` (`VerdictSql.java:2649`), which `DatabaseJudge.written` sets only when `lite(env)` (`DatabaseJudge.java:304-313, 336-338`).
  - The edge's words go through `represented`. The canon SQL (`decimalCanon`) builds an identical tree.
- **N2 keeps the meaning per guard and both judges agree.**
  - Host `defaultConditionMessage(family == ASSERT)` and database `words(env).conditionFailed(family == ASSERT)` read the same VerdictWords, so the vector assertFalse says "expected false, got true", which is its meaning.
  - PURE and ENGINE answer "Assert failed" through `UpstreamVerdictWords:184-186`, `EdgeHostWords:137-139`, `EdgeLoweringForms:68-72` and `EngineShape:253-256`.
  - The product-query default is a style-free native in the typed tree, so the shared compiler stays style-free, and N1 refuses a user's call to it (`InternalNativeCallTest`).
- **The pins match the approval exactly:**
  - INTERNAL_DESUGAR <= 38, dated in place;
  - NOT_UPSTREAM_MAX 73, dated in place;
  - Intrinsic 735, in a generated TSV;
  - four channel B ratchet rows: 1989680 -> 2006804, 34485688 -> 34502872, 1547392 -> 1647537, 24441771 -> 24542270. Nothing else in ratchets.tsv moved.
  - Removing the two register and frozen rows is the shrink both tests ask for ("a gone one is deleted here"). No guard pattern changed (`"^Assert failed$"` is intact).
- **Every new test fails before the fix and runs in a CI lane:**
  - AssertBarTest and QuantifiedRowsTest run in `core_tests_testplan` and the judge matrix.
  - CompiledPureNumbersEdgeTest, AssertErrorNativeTest, PureOnlyTextTest, FloatTextTest and RenderCsvIntegrationTest are in `core/edge_tests.bzl`. That puts them in `core_edge_tests` and the sqlite and h2 edge lanes.
- **F8 is exact.** No other ledger row carries the old range wording (grep), and the core lane floor already had the new wording.

## Measurements (numbers with the command that produced them)
- **Stripped code lines** (JavaEvalLedgerTest's rule, re-implemented in `python3 -I`: drop block and line comments, count non-blank lines), PR head / fix head:

  | File | PR head | Fix head |
  |---|---|---|
  | PureAsserts | 107 | 107 |
  | AssertVerdicts | 1464 | 1464 |
  | HostJudgments | 679 | 679 |
  | DatabaseJudge | 851 | 851 |
  | BodyDriver | 545 | 545 |
  | LiteHostWords (unpinned) | 155 | 169 |
- **Line lengths** (`awk '{print length}'`):
  - `DatabaseJudge.java:924` is 150 characters;
  - `AssertVerdicts.java:1489` is 171 characters;
  - AssertVerdicts lines over 150 characters: 1 at the PR head, 2 at the fix head.
- **F8 ledger** (a python script over `git diff 68f20eb4d eec3c95c7 -- .../channelb/ledger.tsv`): 12 rows removed, 12 added and 4 comment lines added. All 12 equal the old row with `DataError: ` replaced by `DataError: the value is outside the range this database can hold: `. `ErrorKind.java:36` holds that sentence.
- **Channel B ratchets** (`git diff --numstat 68f20eb4d eec3c95c7 -- .../channelb/ratchets.tsv`): 4/4 lines, the four approved values.
- **RelationTextLowering** (`wc -l`): 781 -> 791 lines (`git diff --numstat`: +14/-4).
- **legend-pure's print** (`java P.java`, `java Q.java` in `scratchpad/judgeswords/`, `DecimalFormat("0.0", ENGLISH)` with 340 fraction digits):

  | Double | legend-pure prints | Note |
  |---|---|---|
  | 1e-5 | `0.00001` | |
  | 2.5e7 | `25000000.0` | |
  | 1e20 | `100000000000000000000.0` | |
  | 1.5e-20 | `0.000000000000000000015` | |
  | NaN | `NaN` | |
  | Infinity | `∞` | |
  | -0.0 | `0.0` | |
  | 2^60 | `1152921504606846980.0` | the exact integer, which HUGEINT gives, is `1152921504606846976.0` |
  | 1.2345678901234567e19 | `12345678901234567000.0` | HUGEINT gives `...7168.0` |
