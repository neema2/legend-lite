*A research or audit subagent's report to the step 8 session, 2026-11-28, kept as it was written (read-only work; its scratch files were in the session's container and are not in the repository).*

Audit of a457d4d3, de7918ff + 3c99705c and b2fa9cdb (worktree /home/user/s8-edit). I read the code only; nothing was built or run.

**Findings**

1. **BLOCKER (build).** `core/src/test/java/com/legend/edge/PrintedFormatsTest.java` is not listed in `EDGE_TEST_SRCS` in `core/edge_tests.bzl` (PrintTailTest is at line 86, UpstreamPrintsTest at 118). `core_tests_lib` globs `src/test/java/**/*.java` minus that list (`core/BUILD.bazel:644-748`) and compiles without the edge. So this edge test lands in the product test library and cannot compile: the package-private `com.legend.edge.PrintedFormats` is not on that library's classpath. `ArchitectureTest.productTestsReachNoEdge` reads the same list. Fix: add the file to `EDGE_TEST_SRCS`.

2. **BLOCKER (the stated witness does nothing).** `core/src/test/java/com/legend/integration/EdgeCasesGoldenTest.java:156-198`:
   - `readLastFormat(asked, …)` builds `typed`, and its reads hit the database.
   - `PrintedRelations.lift(typed, …)` returns null for a `format` last step, because format is not a print.
   - The case then falls through to `Lean.runInStyle(model, query, …)` at :198, which runs the original query **string**. The rewritten query is thrown away.
   - So `float.format.mixed`, `float.format.throughALet`, `float.format.r` and `float.format.s.small` under `upstream` will record lite's own text (`0.333333`, `0.0`), not legend-pure's.
   - `PrintedFormatsTest`'s javadoc names "the recorded cases float.format.* of EdgeCasesGoldenTest" as the database witness of D7. That claim is false as written.
   - Fix: when `typed != asked`, run `typed` and record its answer.

3. **SHOULD-FIX (WIP gap).** `core/src/test/resources/edge/edge_cases_answers.tsv` has no rows for the four new cases, so `//core:update_edge_cases`'s diff test goes red. Re-record only after finding 2 is fixed, or the golden pins lite's text as legend-pure's door answer.

4. **SHOULD-FIX (regression), `UpstreamPrints.java:171-176` with `formatted()` at :209-228.** Once `formatted()` rewrites a format call, `substituted` returns at once and never visits the call's other arguments.
   - Example: `assertEquals('…', format('%s rows: %s', [$avg, $rel->toString()]))`.
   - Before: the relation print `$rel->toString()` inside the format was substituted, since a relation's print is read wherever it stands outside a lambda.
   - Now, if `$avg` is lifted, the relation print is left to the SQL.
   - Fix: recurse into the non-lifted arguments, or run the children pass on the rewritten call.

5. **SHOULD-FIX, `UpstreamPrints.readLastFormat` (:234-250).** It has no `writes`/effects guard; `read()` has one.
   - The argument is run as a query of its own, with the lets it reads inlined, before the main run. A let that writes (for example a relation `write`) then runs twice on a session that persists between the two runs.
   - The PCT route and the golden net call it with no knowledge of effects.
   - Fix: pass `TestExecution.facts(typed).effects()` or the equivalent, as `read()` takes it.

6. **SHOULD-FIX (guard-file justification).** `JavaEvalLedgerTest.java:2078-2087`: the `PrintTail.java` row is the edge's one "computes in Java" exception. It still lists exactly seven functions (replace … indexOf), ruled 2026-11-26.
   - `PrintTail.Joined` (Java concatenation) widens that exception without a dated line citing the 2026-11-28 word.
   - The `UpstreamPrints.java` row (:2178-2186) still says "a typed tree rewritten, the text split at a literal separator". It does not mention that format calls are now rewritten (template letter changed, argument replaced).

7. **SHOULD-FIX (stale record vs register).**
   - `docs/records/edition-homework/step8/rulings-d7-d10-2026-11-28.md` says "register S434 as amended" and "The rows S434 and S435 are amended".
   - At HEAD, S434 in `docs/SEMANTICS_REGISTER.md:294` is unchanged. Its tail list lacks `+`, and it does not say that `%s`/`%r` of a computed Float at a last step are now legend-pure's.
   - There is no row yet for `%f`/`%.Nf` as a recorded difference (D7 option (a)).
   - AGENTS.md: "a divergence that is not a row is a defect".

8. **NIT, `UpstreamPrints.java:466-476`.**
   - The comment "toString of one value shares the relation's name" is false: `TO_STRING__ANY_1` is `meta::pure::functions::string::toString`, `TO_STRING__RELATION` is `meta::pure::functions::relation::toString`.
   - By the finding's own reasoning, `SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1` (in `OrderView.ORDER_PRESERVING`, so order-free here) also writes rows' text in arrival order and is missing from `IN_ROW_ORDER`.
   - The test case `$g->toString()->length()->toFloat()` is refused although a text's length does not depend on row order. That errs to the database's side, so it is harmless, but the test comment calls it order-dependent.

9. **NIT, `PrintedFormats.slots` (`PrintedFormats.java:84-95`).**
   - It reads `%0d` and `%.f` (no digits) as slots.
   - legend-pure fails on both: `Integer.parseInt("")` in `PureStringFormat` and the interpreted `Format`; `FormatTools.validateWidth` requires at least one digit.
   - This does not misalign a lift (the run still fails), but it contradicts "read as legend-pure reads them".
   - Everything else matches `PureStringFormat` and `FormatTools.findEndOfDateFormatString`, as read: `%%`, `s r d f t`, `%t{…}` with quotes and backslashes, `%0Nd`, `%.Nf`, unknown directive, trailing `%`.

10. **NIT, `PrintedFormats.Read.with` and `common()` (`PrintedFormats.java:148-176`).**
    - The rebuilt collection is typed by hand (`Any` when mixed), not by the typer. `UpstreamPrints.unordered` re-types through `typed.specs().typeExpression`.
    - Example: `[(1.0/3.0), 3]` was typed by the typer's least common type (likely Number); after the rewrite it is `[String, Integer]`, typed `Any`.
    - `Lowerer.formatted` lowers each slot by its own type (`listLiteral` then `ArrayLit`), and `Scalars.rewriteFormatDirectives` leaves `%s` of a String as `%s`. So the lowering handles it.
    - Any pass between G½ and lowering that reads the list's type is untested, except through the already-mixed test case.

11. **NIT.**
    - A Float argument the database answers NULL (SQLite `0.0/0.0`, recorded THREW for `float.zeroByZero`) now makes the pre-read throw `IllegalStateException` ("the answer is empty"). Before, the format statement itself answered. Same exposure as the existing toString lift, but new for format.
    - Separately, `assert(cond, format('…%s', [<Float>]))` (the message argument is an assert's own argument) is lifted: an extra query for a text only shown on failure, which the "last step" ruling does not obviously cover.

12. **NIT, `EdgeCasesGoldenTest.java:126`.** Since `b2fa9cdb`, `float.inATableTextUsedFurther` (`toString() + ';'`) is a last step with a `Joined` tail at the upstream door, so the name and its comment ("a table's text used further") no longer describe it. Its recorded text probably does not move: both paths write `VALUE\tString:…;`.

**What I verified**
- Upstream claims, read in legend-pure 5.99.0:
  - `%s` is `CompiledSupport.pureToString`; `%r` is `toRepresentation`, whose Number arm is `$n->toString()`.
  - Bare `%f` is `appendFloatString` with precision -1 (no rounding; only `%.Nf` rounds).
  - The interpreted `Format` calls `toString_Any_1__String_1_` and `toRepresentation_Any_1__String_1_`.
  - `plus.pure:25-28` is `$strings->joinStrings('', '', '')`.
- `PrintTail.Joined`:
  - The parser and typer give `a + b + c` as one run (`TypedCollection.operatorRun`).
  - `joined()` takes `STRING_PLUS` only, one non-literal operand of multiplicity exactly one, and literals in order. Nested `+` makes stacked steps, which is still correct.
  - Behaviour is unchanged at legend-engine's door (`PrintedRelations.lift` with values=false refuses a tail that is not all Replace) and in `UpstreamPrints.unordered` (it skips such tails).
  - The exhaustive switch in `PrintTail.answers` is updated; there are no other switches over `PrintTail`.
  - The `PrintTailTest` pinned set is updated with a dated line.
- Finding (i), `isTdsShaped`: a superset of `isRelation`, so it only adds refusals. The sorted-legacy positive half relies on `SORT__TDS…`'s qname being in `SORT_FQNS`, which it is.
- Finding (iii), `mayBeASpelledDate`: the new rule only fires for a non-primitive `TypedUserCall` or `TypedPropertyAccess`. Enum values are `TypedEnumValue`, and `adjust` and relation columns are checked earlier. I found no case it newly refuses that should lift, and none it still lifts that it should refuse.
- `PrintedFormats.of` rejects arguments that are not each of multiplicity one, count mismatches, a template that is not a literal, and an empty list. `%r` becomes `%s` at the right slot position.
- `3c99705c`'s register row for `PrintedFormats.java` is present and dated.
- Java compile-level reading of the main sources found no errors (pattern-binding scope in `of`, private static methods in the interface, record withers).

**What I could not verify**
- That the new tests pass. In particular:
  - the platform database's `1.0/3.0` comes back as an exact double (`0.3333333333333333`);
  - `t::RT.runtimeValue`, the legacy TDS `take`/`slice`/`drop`, and `getFloat` compile in lite;
  - `[2, 1]->makeString()` is not folded and the order view calls a literal list's order defined;
  - lite accepts `format` with a count mismatch at compile time.
- Whether PCT's recorded answers or outcomes TSVs, the corpus rosters, or `PctPlanRoutePrintTest` move because of the format lift or `Joined`.
- Whether any pass between G½ and lowering reads a format argument list's type.
- The user's quoted words in the record (the D7 ruling, "section 7 item 7").
