# Item 3 code map: assert verdicts as records, and the judge rules

Read-only survey of the checkout at `dad8a710` (scratchpad/next), 2026-11-21. Nothing was built or run.
Path prefixes: `main/` = `core/src/main/java/com/legend/`, `test/` = `core/src/test/java/com/legend/`.
"LOC" = `wc -l` (raw lines). "stripped" = JavaEvalLedgerTest's comment-stripped pin.

## 0. Four facts that change the design note's premises

1. **Rule 1 is already enforced by a check since A4 (2026-11-19).** `main/testplan/TestPlanner.java:73-80`
   (`refuseTheDatabaseJudgeInCompatibility`) refuses a DATABASE or BOTH judge for any style but `PlanStyle.PRODUCT`. It is
   called first in `TestPlanner.plan` (:36). The guard test is `test/testplan/HostOnlyCompatibilityTest.java` (119 LOC).
   Every compatibility road already plans with the host judge only (see B.4). So the design's §4.1 measure ("rows only
   the database judge passes on the compatibility lanes") and §4.2 measure ("lane run time under the host alone")
   compare against a state that no longer runs. Both are zero or no-ops at this commit. What is left of rule 1 is
   **dead compatibility code still wired to the database judge's types** (B.5). No live compatibility verdict reaches
   the database judge's SQL.
2. **`UpstreamVerdictWords`' 20 SQL builders are unreachable.** The VerdictWords hook (`DatabaseJudge.words`, :306-308)
   returns the edge's form only when the style is the edge's. Under that style the database arms never run:
   `AssertVerdicts.arm` throws for any judge but DATABASE (:1571-1573), and DATABASE is refused for the edge style.
   The hook still serves the **host** judge's planner, through two `String` methods: `conditionFailed` and
   `valuesDiffer`. That path is how legend-pure's "Assert failed" and "\nexpected: ...\nactual: ..." reach the test plan
   today, inside `HostJudgment.AllRows.message` and `HostJudgment.Decided.message`.
3. **Assert words exist in two halves, and item 3's prose residue covers only one.** The test plan's planned prose is
   `Failure.message`, `AllRows.message`, `Decided.message` and `Messaged.unplanned`. The run-time half has two more
   catalogs: the host's `HostWords` (Lite and Edge forms) and the database row's `__message` column. Under Q15 = A, a
   single catalog means `EdgeHostWords` (233 LOC) and `EdgePureWords` (154 LOC) go too, and every compatibility ledger
   row that pins legend-pure's assert words moves (E.3).
4. **`Failure.message` cannot leave `PROSE_RESIDUE` on item 3's work alone.** The edge also puts `Failure` records
   that carry prose into test plans:
   - `edge/SqlTextVerdicts.java:232`: a `NOT_A_CONSTANT` raise.
   - `edge/SqlTextVerdicts.java:1465`: `Appeal.Read.failure`, built by `Failures.of` from any closed-list kind.

   These belong to B3. Item 3 can narrow the residue entry to the edge's records, or wait for B3.

---

## A. Every place an assert-failure sentence is built or carried

### A.1 The product's sentence table: `main/error/AssertSentence.java` (128 LOC)

- `enum AssertSentence` (:24-53) has 24 rows: EQUALS, EQUALS_KINDS, NOT_EQUALS, SAME_ELEMENTS, SIZE, NOT_SIZE, CONTAINS,
  NOT_CONTAINS, EMPTY, NOT_EMPTY, TRUE, FALSE, IS, IS_NOT, SAME_INSTANCE, INSTANCE_OF, TDS_EQUIVALENT, WITHIN_TOLERANCE,
  JSON_EQUALS, ERROR, NO_ERROR, ERROR_LINE, ERROR_COLUMN, AT_INDEX.
- Each row has a template with `{slot}`s (for example `EQUALS("expected {expected}, got {actual}")`).
- `pieces()` (:68-88) parses the template. `fill(Map<String,String>)` (:103-115) is the host's join.
  `VerdictSql.sentence` (below) is the database's concat.
- The javadoc (:22) still says "Compatibility mode (PCT, the corpus) keeps legend-pure's words, the edge's". Q15 = A
  makes this the one catalog.
- **Producers:** none; it is a constant table. **Consumers:**
  - `VerdictSql.sentence`, `LiteVerdictWords`, `VerdictSql.equalsOrKinds`, `DatabaseJudge.is()` (:917, SAME_INSTANCE).
  - `LiteHostWords` (through `HostJudgments.sentence`, :40-46) and `LiteWords.sentence` (:86).
  - `ProductLoweringForms.assertFailed` (:53-55, `AssertSentence.TRUE.fill`).
- **Rides a plan:** yes, indirectly.
  - Filled text rides the test plan as `Decided.message`, `AllRows.message` and `Failure.message`.
  - Concatenated SQL rides the database judge's wrapper (product node `TdsResult`, rendered SQL in
    `ExecutionPlan$Sql.statement`).
  - `assertFailed()` rides the **product** plan as a `StringLit`.
- Its test is `test/error/AssertSentenceTest.java` (43 LOC; it pins three fills and a parse round trip).

### A.2 The database judge's word hook: `main/lowering/VerdictWords.java` (101 LOC, interface)

- It is a two-form kind, listed in `PlanStyleFacets.TWO_FORMS` (`main/testplan/PlanStyleFacets.java:58-60`).
- It has `name()` (:26) plus 22 methods:
  - 20 return `SqlExpr`: `sizeDiffers` :29, `sizeEquals` :32, `notEmpty` :35, `notEmptyWritten` :38, `notContained` :41,
    `contained` :44, `notContainedCanons` :47, `notInstanceOf` :51, `elementText` :54 (a value writer),
    `elementFailure` :57, `notWithin` :61, `notEquivalent` :66, `noError` :71, `errorDiffers` :74, `lineDiffers` :77,
    `columnDiffers` :80, `frameSide` :83 (a side writer), `identical` :86, `frame` :90, `atIndex` :94.
  - 2 return `String`: `conditionFailed` :97 and `valuesDiffer` :100. These are the host judge's planner's: their
    output becomes `Decided.message` and `AllRows.message`.
- **Chosen by:** `DatabaseJudge.words(env)` = `env.options().style().twoForm(VerdictWords.class, LiteVerdictWords.INSTANCE)`
  (`main/testplan/DatabaseJudge.java:306-308`). The edge answers at `main/edge/PureText.java:120-122`.
- **Call sites:** 21.
  - `DatabaseJudge` ×14: lines 167, 202, 204, 518, 541, 542, 580, 586, 632, 682, 875, 896, 946, 987.
  - `DatabaseAsserts` :407.
  - `AssertVerdicts` :452, :614, :963, :1430.
  - `BodyDriver` :225-226 (`newVerdictBatch`, which hands the words to `VerdictBatch` and `selfContained`).
- **Plan name:** `TestDoor.java:160-164` writes `twoForm(VerdictWords.class, LiteVerdictWords.INSTANCE).name()` into
  `TestPlan.words`.

### A.3 Lite's form of the hook: `main/lowering/LiteVerdictWords.java` (175 LOC)

- `NAME = "lite"` (:26) is the same text as `TestPlan.LITE_WORDS`.
- Each method is one `VerdictSql.sentence(AssertSentence.X, Map<slot, SqlExpr>)` call (:41-174).
- `notInstanceOf` (:76-80) uses `COALESCE`. `notWithin`, `noError` and `errorDiffers` wrap values in `LiteText.of(...)`.
- `lineDiffers` and `columnDiffers` write `Long.toString(expected)` as a literal (:126, :131).
- `atIndex` writes `Integer.toString(index)` (:163). `valuesDiffer` is a Java `AssertSentence.EQUALS.fill` (:172-174).
- **Producer:** the product style. **Consumers:**
  - `VerdictSql.{size,empty,contains,instanceOfDecided,perElement,tolerance,gridTolerance,assertError,selfContained,atIndex,messageRow}`.
  - The host planner, for `conditionFailed` and `valuesDiffer`.
- **Rides:** the test plan, as SQL text inside the verdict's `wrapper` and the group's `fused` node, and as
  `Decided.message` / `AllRows.message` strings.

### A.4 legend-pure's form of the hook: `main/edge/UpstreamVerdictWords.java` (192 LOC)

- `name()` = `EdgeHostWords.NAME` = `"legend-pure"` (:40).
- Its SQL builders use `CONCAT`, `COALESCE`, `REPLACE` (`reprNewlines`, :189-191), `NOT` and a `CASE`. They write
  assertSize.pure, assertError.pure and similar files' words.
- `elementFailure` and `notWithin` return null (the frame's words). `atIndex` returns the message unchanged.
- `conditionFailed` → `"Assert failed"` (:178-180). `valuesDiffer` → `"\nexpected: "+e+"\nactual:   "+a` (:183-185).
- **Reachability:**
  - The 20 SQL methods are **dead** since A4 (§0.2).
  - The 2 String methods are live, under the edge style, from the host planner (A.8). They carry legend-pure's words
    into `HostJudgment.Decided.message` and `HostJudgment.AllRows.message` in compatibility test plans.
- **Rides:** compatibility test plans, as those two strings.

### A.5 The SQL that builds sentences: `main/lowering/VerdictSql.java` (2,797 LOC)

The verdict row schema is `schema()` (:2076-2091): `__verdict` BOOLEAN, `__expected` VARCHAR, `__actual` VARCHAR,
`__unjudged` VARCHAR?, `__lenient` BOOLEAN, `__holds` BOOLEAN, `__message` VARCHAR?. The fused form is `batchSchema()`
(:2065) with `__ix` first (`INDEX`, :1489; `batchOutputs`, :1620-1629). The run reads these by position
(`RootLoop.java:28-34`, `VERDICT_COLUMN` = 0 through `MESSAGE_COLUMN` = 6). `test/lowering/VerdictRowOrderTest.java` pins
that order.

Sentence- and message-specific members (span from the method's doc comment to its end). Sum ≈ **588 lines**; this is a
measured lower bound:

| member | lines | what | SQL functions |
|---|---|---|---|
| `sentence(row, slots)` | 82-94 | an `AssertSentence` row as SQL | `CONCAT` of literals and slots |
| `sentence(row)` | 97-99 | a slotless row as a Java literal | — |
| `Message` record | 138-211 (83) | how a side is written in the message: `lite`, `canon`, `literal`, `kindText`, `canonClass`, `sorted`, `listed`. **The 5-arg constructor defaults `lite=false`, which is legend-pure's words** (:144-147); every database-judge caller then applies `withLite(true)` | — |
| `withKinds` | 251-260 | `__ekind`/`__akind` hint columns | literals |
| `withWrittenSides` ×2 | 264-295 | `__ew`/`__aw` written sides plus the `__multiset` hint | `CASE`, `AND`, `EQUAL` |
| `writtenListOf` | 550-558 | a rows CTE as `[a, b]` | `STRING_AGG ... ORDER BY __rn`, `CONCAT`, `COALESCE` |
| `quoted` | 788-791 | legend-pure's `%r` String quoting, through `LiteralSpelling.literal(STRING)` | — |
| `writtenRows`, `writtenStatement`, `writtenFactOutputs` | 1202-04, 1259-66, 1350-55 | the written-side rows and statement | — |
| `instanceLite` / `pureKind` / `instanceWritten` | 1782-1837 | a struct instance as `Fqn {"f":v}` or `<a\|b instanceOf T>` | `CONCAT`, `COALESCE`, `BOOL_TO_TEXT`, `CAST` |
| `FailureMessage`, `messageRow`, `withFailureMessage` | 1842-1906 | the caller's message: a literal or a row read where the assert fails | `COALESCE`, `CROSS JOIN` |
| `messageAsStrings` / `sidesAsStrings` | 1908-1925 | the rendered-text family's sides framed as `%r` | `quoted` |
| `selfContained` / `joinsCells` / `withHoldsAndMessage` | 1928-2003 | **the frame**: adds `__holds` and `__message` and removes the hint columns | `NOT` and the words' `frame`/`identical`/`frameSide` |
| `withIsNotHint` / `withHint` | 2007-2026 | `__isnot` hint | — |
| `kindText` | 2030-2032 | a type name for EQUALS_KINDS | — |
| `atIndex` | 2036-2052 | AT_INDEX wrap of `__message` | words' `atIndex` |
| `equalsOrKinds` | 2055-2061 | `CASE WHEN e = a THEN EQUALS_KINDS ELSE plain` | `CASE`, `EQUAL`, `CONCAT` |
| `cellsAsList` | 2109-2112 | a grid row canon's separator replaced by `, ` | `REPLACE` |
| `written` ×3, `writtenRow` | 2267-69, 2345-56, 2366-79, 2449-64 | an element written for the message | `LiteText.of`, `CAST` |
| `liteOfCanon` ×2 | 2275-2343 (72) | lite text **re-derived from the canon** (legend-pure's print: `'a'`, `%date`, `1.0D`, `<id instanceOf T>`) | `SUBSTRING`, `LENGTH`, `STRPOS`, `REPLACE` ×4, `CONCAT`, `MINUS`, `PLUS`, `CASE` |
| `represented` | 2360-2362 | legend-pure's representation, through `styled.pureOnly()`; the product refuses it | facet |

Word arguments inside verdict arms (the rest of each arm decides the verdict):

- `size` :391-404 builds `wantText`/`countText` (`CAST ... VARCHAR`), then calls words `sizeDiffers`/`sizeEquals`.
- `empty` :528-547 calls `notEmpty`/`notEmptyWritten`.
- `contains` :565-606 builds a one-element bracket (`CASE ... CONCAT('[', t, ']')`), then calls
  `notContainedCanons`/`notContained`/`contained`.
- `condition` :612-620 carries a `String message` as a `StringLit`.
- `instanceOfDecided` :627-633 and `allOf` :639-654 carry their message the same way.
- `perElement` :669-732 calls words `elementText`, then `ARG_MIN` picks the first failing pair's texts, then words
  `elementFailure`.
- `tolerance` :739-753 calls `notWithin`.
- `gridTolerance` :801-880 calls `notEquivalent` (:874).
- `assertError` :1686-1731 holds the failure CASE (:1699-1714) over `noError`/`errorDiffers`/`lineDiffers`/
  `columnDiffers`.

Inline prose literals in VerdictSql (no catalog row):

- :1704-1705: `"assertError line/column: the caught error carries no source position (a native database error, or a
  raise emission without provenance)"`. It is duplicated in Java at `main/testexec/AssertErrorRuns.java:100-103`.
- Unjudged reasons: :361 `"tds-peer: cells not divisible by width N"`, :1306 `"null-canon-cell"`, :1311
  `"unclaimable tree cell"`.
- Expected/actual descriptor texts: :520/:544 `"empty"`/`"not empty"`, :522/:546 `" element(s)"`, :650
  `"every element true|false"`, :652 `" element(s) otherwise"`.

Who consumes the SQL: the batch.

- `VerdictBatch.defer` (:248-264) wraps each row with `selfContained` (and `atIndex` inside an unrolled element).
- `VerdictSql.batch` (:1501-1549) fuses the rows as `WITH frames ... UNION ALL`.
- `JudgePlans.runner` makes it a product `TdsResult` node.
- The node is stored as `TestPlan.Verdict.wrapper` or `TestPlan.Verdicts.fused`.

Stale javadoc: :1632-1640 still describes `__message` as "the family's name, `\nexpected:` ... `\nactual:`" (legend-pure's
form).

### A.6 Lite's text of a value, in SQL: `main/lowering/LiteText.java` (106 LOC)

- `of(Type, SqlExpr)` (:~40-77): a String is quoted JSON-style (`REPLACE` ×5 escapes, `CONCAT`); a Float uses
  `FLOAT_SHORTEST_TEXT` plus `javaExponent` (:84-105).
- `javaExponent` is SQL that **emulates Java's `Double.toString` exponent** so the database judge's message matches the
  host's. It uses `LOWER`, `STRPOS`, `SUBSTRING`, `LENGTH`, `CAST BIGINT/VARCHAR` and `CASE`.
- A DateTime goes through `toTheMinute` and `DATETIME_TO_TEXT`. Anything else goes through `Scalars.pureToString`.
- **Only callers:** `LiteVerdictWords` (8 calls) and `VerdictSql` (2). The design's option (b) deletes the whole file.

### A.7 `AssertMessages` (`main/compiler/spec/AssertMessages.java`, 134 LOC)

- `split` (:45-90) splits a call into its base overload and the caller's message as a typed expression (planning
  only).
- **`unevaluated(failure)` (:96-98)** returns `"Assert failed; its message could not be evaluated: " + failure`. This is
  lite's prose around an error's text. It has two callers:
  - `main/testplan/HostSides.java:93` (`withoutTheMessage`, :80-94) builds `HostJudgment.Messaged(inner, n, unplanned)`.
    **Rides the test plan:** `Messaged.unplanned` (HostJson :662/:669).
  - `main/testplan/AssertVerdicts.java:444-445` builds `new VerdictSql.FailureMessage(unevaluated(e.getMessage()), null)`.
    That becomes a `StringLit` in the database verdict's `__message` (`withFailureMessage`, :1876-1906). **Rides the
    test plan** as SQL text. `PlanTypesAreDataTest` counts it as a fact (`SqlExpr$StringLit.value`); the BE audit 5's
    M3 called it prose.
- Both callers run only when `Failures.atItsTurn(e)`: a run-time error raised while planning the message. The design's
  D refuses these instead.
- The record's §7 also names this prose (the "unevaluated assert message ... written by VerdictSql").

### A.8 The host judge's planning-time words, which ride the test plan

`main/testexecutionplan/HostJudgment.java` (255 LOC):

- **`AllRows(String name, boolean wantTrue, String message)`** (:87).
  - **Producers:**
    - `HostJudge.planAllRows` (`main/testplan/HostJudge.java:191-194`), called from
      `AssertVerdicts.planQuantified` :1470-1472 (`VerdictQueries.assertMessage(root, defaultConditionMessage(...))`),
      :1488/:1494 and :1550.
    - The message is either the caller's literal (`TypedCString`, which is program data) or
      **`defaultConditionMessage(wantTrue, env)` = `DatabaseJudge.words(env).conditionFailed(wantTrue)`**
      (`AssertVerdicts.java:1429-1431`). That is lite's "expected true, got false" or, on the edge, legend-pure's
      "Assert failed".
    - `VerdictQueries.assertMessage` is at `main/compiler/spec/VerdictQueries.java:154-158`.
  - **Consumer:** `HostJudgments.allRowsOf` (`main/testexec/HostJudgments.java:685-692`), which runs
    `Verdicts.fail(j.message())`.
  - **Codec:** `HostJson.java:340-352`.
- **`Decided(boolean held, @Nullable String message)`** (:171).
  - **Producer:** `AssertVerdicts.isDecided` (:1056-1072). Two type identities that differ give
    `new HostJudgment.Decided(false, words.valuesDiffer(lt, rt))` (:1061). Called from :614 (`isVerdict`, database arm)
    and :963 (host arm).
  - **Consumers:**
    - `Verdicts.decided` (`main/testexecutionplan/Verdicts.java:107-110`) runs `fail(message)`, which throws
      `AssertFailed`.
    - The host, at run: `HostJudgments.judge` case `Decided` (:326).
    - The database judge, at planning: `AssertVerdicts.planDatabase` catches the `AssertFailed` (:91-94). That gives
      `Failures.planned`, then `Failure(ASSERT_FAILED, message)`, then `RootLoop.Decided`.
  - **Codec:** `HostJson.java:525-532`.
- **`Messaged(inner, innerSides, @Nullable String unplanned)`** (:211). `unplanned` is A.7's sentence.
  - **Consumer:** `HostJudgments.messagedOf` (:479-497). It throws `new AssertFailed(unplanned)` (:491), or reads the
    message side (:494-496).
  - **Codec:** `HostJson.java:662-670`.
- Fine as facts (FACTS): `AssertError.expected` (a value the program wrote, folded at planning, A.13),
  `Negated.kind` (a code, for example `"NOT_EQ"`), `RowArg.Literal.text`.

### A.9 `Failure.message` for ASSERT_FAILED

`main/testexecutionplan/Failure.java` (140 LOC): `record Failure(Kind kind, String message, Phase phase, String element,
String unjudgedReason, ErrorRecord record)` (:29-30).

- `Kind.ASSERT_FAILED` (:106) carries no record: `RECORDED` (:36-37) excludes it. So its `message` is lite's or the
  edge's sentence.
- **Producers on lite's roads:**
  - `AssertVerdicts.planDatabase`'s catch (:91-94, through `plannedOrDropped` :303-310) catches an `AssertFailed`
    thrown by `Verdicts.decided` (a Decided message) or by the database arm's static decisions.
  - The host's `HostAssert.raising` (`AssertVerdicts.java:679-690`) and `HostAsserts.java:208, 225, 324, 333, 349,
    373, 415, 464, 480`.
  - `Failures.planned` → `Failures.of` (`main/testexecutionplan/Failures.java:34-84`) copies `e.getMessage()` when no
    record exists.
- **Where it sits in plans:**
  - `RootLoop.Decided(Failure, reported)` (`RootLoop.java:117`).
  - `HostJudgment.Raise(Failure)` (:228).
  - `Assertion.beforeTheArm` (HostJson :141).
  - `HostJudgment.AssertError.caught` (:238).
  - `VerdictBatch.Sole.Decided` (:283-286).
- **Consumers:**
  - `Failures.raised` → `rebuilt` (:110-131) rebuilds `new AssertFailed(text(f))` at the verdict's turn.
  - `DatabaseAsserts.errorVerdict` copies `String.valueOf(d.failure().message())` and `caught.message()` into SQL
    literals (`VerdictSql.Raised.raised`) (:380, :399).
- **Codec:** `HostJson.wFailure`/`rFailure` (:153-190), which writes `"message"` through `ownMessage()` and writes
  `"unjudgedReason"`.
- Since D, an unjudged decline (`unjudgedReason != null`) is refused at planning (`Failures.atItsTurn`, :48-50). On
  lite's roads it is never planned.
- Edge producers of prose `Failure`s (B3; §0.4): `SqlTextVerdicts.java:232` (NOT_A_CONSTANT) and :1465
  (`Appeal.Read.failure`).

### A.10 `TestPlan.words`: one audience per plan

- `main/testexecutionplan/TestPlan.java:43-45` (component `String words`); `LITE_WORDS = "lite"` (:48); required
  non-null (:51); carried by `withExtensionsByVerdict` (:70-71).
- **Producer:** `TestDoor.planned` (`main/testplan/TestDoor.java:160-164`) writes the VerdictWords hook's name.
  `TestPlanner.planIsolated` copies `host.words()` (:103-105).
- **Consumer:** `TestRunner` (`main/testexec/TestRunner.java:99-104`) refuses a plan whose `words` differ from the run
  hook's `hostForms().words().name()`.
- **Codec:** `TestPlanJson.java:123` (write) and :284 (read).
- **Guard:** listed as a FACT ("a code ... a style's name") in `PlanTypesAreDataTest.FACTS`.
- Names on the hook side:
  - `LiteHostWords.NAME = TestPlan.LITE_WORDS` (`main/testexec/LiteHostWords.java:28`).
  - `EdgeHostWords.NAME = "legend-pure"` (`main/edge/EdgeHostWords.java:27`).
  - `HookRulingTest.java:431-441` pins the pairing.

### A.11 The host judge's run-time words: `main/testexec/HostWords.java` (109 LOC, interface) and its two forms

- `name()` plus **26 methods**, of which 25 are sentences:
  - `rowValuesDiffer`, `notEqual`, `sameElements`, `cellMultisetDiffers`, `notEquivalent`, `grids`, `size`,
    `notContained`, `notWithin`, `negated`, `condition`, `emptiness`, `notEq`, `notInstanceOfClass`, `notInstanceOf`,
    `notSameInstance`, `atIndex`, `rowNotEqual`, `sidesNotEqual`, `jsonDiffers`, `noError`, `errorDiffers`,
    `lineDiffers`, `columnDiffers`, `unjudged`.
  - The 26th is `caught(RuntimeException, EdgeHearing)` (:108), the text an assertError matches by. It is not
    assert-failure words.
- **Chosen by:** the run hook, `HostForms.words()` (`main/testexec/HostForms.java:26`):
  - `LiteHostForms.STRICT.words()` = `LiteHostWords` (`main/testexec/LiteHostForms.java:59-62`; through
    `NoEdgeRun.java:64-67`).
  - `CompiledPureHostForms.words()` = `EdgeHostWords` (`main/edge/CompiledPureHostForms.java:63-67`; through
    `edge/Appeals.java:133-135`).
- **Consumers:**
  - `HostJudgments` (23 `words(rules)` calls; :23-25 helper).
  - `AssertErrorRuns.judged` (:75-109).
  - `DatabaseVerdicts.verdictOf` (`unjudged` only, :35-37).
  - `BodyRun` :39 (`caught`).
  - `AssertRuns.judgedRowAtItsStep` (:109-130).
- **Rides a plan:** no. Built at run, raised as `AssertFailed`, heard by `AssertListener.verdict(name, pass, detail)`.
- `main/testexec/LiteHostWords.java` (224 LOC) fills `AssertSentence` rows through `HostJudgments.sentence` and
  `LiteWords`. `unjudged` returns `" (expected e, got a)"` (:215-217).
- `main/edge/EdgeHostWords.java` (233 LOC) is legend-pure's form:
  - It composes `EdgePureWords` (`main/edge/EdgePureWords.java`, 154 LOC). That class has 11 sentence builders
    (`equals`, `sameElements`, `pair`, `notEquals`, `notContains`, `notEmpty`, `size`, `identical`, `contains`, `grids`,
    `notInstanceOf`) plus Java's `repr`/`reprList`, which is legend-pure's `toRepresentation`.
  - `notEquivalent` returns `cellDiff` (from `TdsCompare`) or a summary (:97-100).
  - `jsonDiffers` returns `"assertJsonStringsEqual: FIRST DIFF at "+diff` (:188-190), with the diff from
    `Equality.firstDiff`.
  - `caught` returns `PureMessages.compile(error)` (:227-232).

### A.12 Sentences built outside any catalog, at run

- `HostJudgments.renderedOf` (:877-905): `name + " (rendered " + form + "): " + Verdicts.firstTextDiff(et, at)`
  (:892-893).
  - `Verdicts.firstTextDiff` (`main/testexecutionplan/Verdicts.java:25-39`) returns one of three sentences:
    `"line-count N != M (expected first line: ...)"`, `"line i: expected <..> got <..>"`, or
    `"texts differ only in leniency-adjudicated cells"`.
- `AssertErrorRuns.judged` :100-103 duplicates the VerdictSql line/column literal.
- `DatabaseVerdicts.unjudged` (:57-60) and `DatabaseJudge.unjudged` (:485-490) both build
  `name + ": UNJUDGED in database mode — " + why + detail`.
- `RootLoops.messaged` (:264-293) reads `rows.get(0).get(6)` (the message row's `__message`) and throws
  `AssertFailed(text)`. Its two other branches are refusals or defects.
- `TdsCompare` :100-105 and :125-151 (`"assertTdsEquivalent: expected N cells, got M"`,
  `"assertTdsEquivalent: cell i expected x, got y"`) and `Equality.firstDiff` :363-407 build diff texts that **only
  `EdgeHostWords` prints**. `LiteHostWords` ignores `cellDiff` and `diff`.

### A.13 In-body asserts (adjacent: words in the **product** plan)

- `Pure.Lite.ASSERT_FAILED_TEXT` (`main/builtin/Pure.java:520-524`, signature :1026-1027) is lowered by
  `Coercions.java:147-150` to `StringLit(styled.forms().assertFailed())`.
- The forms are `LoweringForms.assertFailed()` (`main/lowering/LoweringForms.java:67-69`) with two implementations:
  - `ProductLoweringForms.java:51-55` → `AssertSentence.TRUE.fill(Map.of())`.
  - `EdgeLoweringForms.java:66-70` → `"Assert failed"`.
- `LambdaBodies.java:134-142`: `assert`'s default is the styled native. **The six other in-body guards hard-code
  `TypedCString("Assert failed")`** (:142) on both roads (`CompatSpellingGuardTest` row
  "compiler/spec/LambdaBodies.java | Assert failed | 1 | STAYS", :196-203).
- Under Q15 = A ("lite's own assert-failure words on every route"), these are the same question in the product plan.
  They are not in the design note.

### A.14 Folding a caller's message in Java (A.8 and B are related)

- `DatabaseJudge.plannedMessage` (:385-401) runs `Literals.fold(message, letPrefix)`. That joins `+` and
  `joinStrings` of literals in Java (`main/compiler/spec/typed/Literals.java:28-80`). The result becomes a `StringLit`
  message.
- `AssertErrorNative.args` (:64-66) folds the expected message the same way, into `HostJudgment.AssertError.expected`.
- See C.3 (rule 4).

### A.15 The appeal and referee path's prose (B3, not item 3; listed for completeness)

- `main/edge/Appeal.java` (173 LOC):
  - `record Appeal(int number, String name, Kind kind, String contract, Rows rows)` (:37). `contract` is required iff
    `Kind.CONTRACT` (:41-45).
  - **Producers:** `SqlTextVerdicts.java:522-523` (`name + " (sql-text, " + dbType + " — text is the contract)"`) and
    :1120-1121 (`"(plan-text, params unbindable — text is the contract)"`).
  - **Consumer:** `SqlTextRuns.decideAppeal` :99-101 (`contract + ": expected " + golden + ", got " + ours`).
  - **Codec:** `AppealJson.java:128/137`.
  - **Rides:** `TestPlan.extensionsByVerdict` (edge plans only).
- `Appeal.Unlisted(String simpleName, String message)` (:154).
  - **Producer:** `SqlTextVerdicts.java:1464` (`PureMessages.compile(f)`).
  - **Consumer:** `SqlTextRuns.rowsLegNow` → `underivable` (:203-209), which writes
    `"name (sql-text, rows underivable — Class: msg): expected .. got .."`.
  - **Codec:** `AppealJson.java:204-214`.
- `EngineTextJudgment(name, golden, ours, textEqual, String decline)` (`main/edge/EngineTextJudgment.java:16-17`).
  - **Producer:** `SqlTextVerdicts.java:259` (from `collect`, :106-149).
  - **Consumer:** `SqlTextRuns` (`textsDiffer`, and the oracle-declined sentence :192-195).
  - **Codec:** `EdgeCodec`.
  - **Rides:** `HostJudgment.EdgeJudged.judgment`.
- These are the source of the corpus ledgers' `(sql-text, oracle declined: ...)` and `(sql-text, rows underivable ...)`
  rows (E.3).

---

## B. The judges

### B.1 The host judge's planner

- `main/testplan/HostAsserts.java` (560 LOC) plans each statement-root assert as a `HostSides.Check(asks, HostJudgment)`
  (`main/testplan/HostSides.java`, 143 LOC).
- `main/testplan/HostJudge.java` (336 LOC) builds the judgments: `planAllRows` :191, `planCondition`, `planEq`,
  `planIs`, `planEmpty`, `negated`, `planRows`, `planPerElement`, `planInstanceOf`, and `Messaged` at :130.
- `AssertVerdicts.planHost` (`main/testplan/AssertVerdicts.java`, 1,966 LOC) classifies.
- Supported kinds are the 24 `HostJudgment` variants (`HostJudgment.java:25-30`): AllRows, Rows, Unrolled,
  TdsEquivalent, GridPair, Size, Contains, Tolerance, Condition, Subset, Empty, Eq, InstanceOf, Is, Decided,
  SameElements, Equals, Json, Rendered, Raise, EdgeJudged, AssertError, Messaged, Negated.
- These cover every `NativeFn.Verdict` family (`main/builtin/NativeFn.java:221-...`): EQUALS, NOT_EQUALS, SAME_ELEMENTS,
  SIZE, JSON_STRINGS_EQUAL, CONTAINS, EQ, EQ_WITHIN_TOLERANCE, ASSERT, ASSERT_FALSE, INSTANCE_OF, IS, NOT_EQ, IS_NOT,
  NOT_CONTAINS, NOT_SIZE, TDS_EQUIVALENT, TO_CSV, EMPTY, NOT_EMPTY, plus `assertError`.
- A pair of instances for NOT_EQ/IS_NOT is refused (`AssertVerdicts.java:972-975`).

### B.2 The host judge at run

- `main/testexec/HostJudgments.java` (906 LOC; stripped pin 679) dispatches with `judge(HostJudgment, sides, run)`
  (:308-336).
- Helpers: `PureAsserts` (173 LOC, decisions only; `CompatSpellingGuardTest.WORDED_BY_HOOK`), `Equality` (451),
  `TdsCompare` (179), `AssertRuns` (207), `AssertErrorRuns` (115), `SideShapes`.
- The equality behaviours come from the run hook's `HostForms`:
  - Lite: `LiteHostForms` (S127 cross-kind never equal, S139 Decimal by value, Float exact).
  - Edge: `CompiledPureHostForms` (Integer and Decimal equal by `String.valueOf` text, `Decimal.equals` with scale,
    2 ULP).

### B.3 The database judge

- **Planner:** `main/testplan/DatabaseAsserts.java` (731 LOC).
  - `plan` :76-355 runs `BodyDriver.newVerdictBatch(env)` (:122, 186, 221, 256), then `AssertVerdicts.planDatabase`
    for each statement-root assert (:68-110). That calls `adjudicate` → `arm(env)` = `DatabaseJudge.ARM`
    (`main/testplan/VerdictArm.java`, 116 LOC, 17 arms).
  - Arms: `tdsEquivalent`, `size`, `contains`, `tolerance`, `condition`, `empty`, `eq`, `instanceOf`, `is`,
    `sameElements`, `quantified`, `quantifiedVector`, `perElement`, `gridPair`, `equals`, `jsonStringsEqual`,
    `rendered`.
  - The negated families flip polarity: `ASSERT_NOT_EQ`/`IS_NOT` (`AssertVerdicts.java:635-650`, with `asIsNot`
    scope); NOT_CONTAINS and NOT_SIZE via `negated=true`.
  - assertError gets its own verdict statement: `DatabaseAsserts.errorVerdict` (:360-414) → `VerdictSql.assertError`
    plus `Raised.bound` parameters (`TestPlan.Verdict.RAISED_*`) (S135: the host catches, the database judges).
- **Arms that decline** (they throw `AssertFailed.unjudged`, which refuses the body since D):
  - `is` other than an enum or tracked-element pair (:920).
  - `eq` over keyed instances (:965).
  - `instanceOf` with no native (:929).
  - `perElement` kinds with no spelling, a multi-candidate number, "no literal channel", a json golden that is not a
    literal.
- `DatabaseJudge.java` (1,071 LOC; stripped pin 808) builds the sides (canon wraps), `written(...)` Messages
  (:272-327), and `liteDate` (:290-302, Java `String.format`). `runVerdict` (:414-427) defers into the batch.
- **One statement per verdict group.** `VerdictBatch` (`main/testplan/VerdictBatch.java`, 688 LOC):
  - It defers each root's rows (`defer` :248-264, made self-contained with `__holds` and `__message`).
  - At each segment's close (an effect statement or the body's end: `DatabaseAsserts.close`/`closePlanned`
    :649-695), it plans one fused `UNION ALL` statement **per session**.
  - A group longer than the dialect's `fusedStatementBudget` becomes several fused statements. A member the dialect
    cannot fuse becomes its own statement. A keyed member (one that reads a raw grid's report) is never fused.
  - Each fused row is `(__ix, __verdict, __expected, __actual, __unjudged, __lenient, __holds, __message)`.
  - The segment's `RootLoop` (`main/testexecutionplan/RootLoop.java`, 122 LOC) rides the group's last
    `TestPlan.Verdicts.roots`.
  - `VerdictBatch.words` (:195-196, set in the constructor :198-205) and `Scope{elementIndex,isNot}` with `asIsNot`/`atElement` (:170-189) exist **only** for sentence framing.
- **The compared values:**
  - `__expected`/`__actual` are canon TEXTS (VARCHAR): `CanonicalRenderSql.wrapWithCanon` and `LiteralSpelling.leaf`,
    "pure's value-spelling grammar" (`main/lowering/LiteralSpelling.java:14-44`, 594 LOC; `CanonicalRenderSql.java`
    1,111 LOC). The verdict is `IS NOT DISTINCT FROM` over them (or a count, range or `ABS(e-a)<=t` predicate).
  - Typed values are **not** returned today. The only typed column is `__lenient` (always FALSE since A4).
- **At run:**
  - `TestRunner` :250-286 → `RootLoops.Run.judge` (`main/testexec/RootLoops.java`, 351 LOC) →
    `DatabaseVerdicts.verdictOf` (`main/testexec/DatabaseVerdicts.java`, 61 LOC). It reads `__unjudged`, `__holds`
    and `__lenient`, then runs `Verdicts.fail(row[MESSAGE_COLUMN])`.
  - assertError rows go through `AssertRuns.judgedRowAtItsStep` (:109-130).
  - A caller's message that reads is `RootLoops.messaged` (:264-293).

### B.4 Where each road chooses its JudgeMode (`PlanningOptions.JudgeMode {HOST, DATABASE, BOTH}`, `main/testplan/PlanningOptions.java:43`)

| road | where | judge | style |
|---|---|---|---|
| default options | `main/ExecuteOptions.java:32` | HOST | PRODUCT |
| core tests | `test/testing/Program.java:56-71` (`laneJudge()` reads `-Dlegend.test.judge`, default HOST); `core/BUILD.bazel:1018-1050` (`core_tests_<pkg>_judge_{database,both}` for 12 packages plus root, `ci-linux-only`) | HOST, DATABASE, BOTH | PRODUCT |
| core edge tests | `test/testing/EdgeProgram.java:29-41` | HOST only | `PureText.UPSTREAM` |
| BodyRouterParityTest | `judgeFor` :37-41 (edge style → HOST) | asked, or HOST on the edge | either |
| PCT channel A | upstream's own runner (`Test_LegendLite_*_PCT`) | no lite judge | — |
| PCT channel B, compatibility | `ChannelB.compatJudge()` :435-443 (refuses anything but HOST); `COMPAT_OPTIONS` :418-432 | HOST | UPSTREAM |
| PCT LITE-PCT lane | `ChannelB.LITE_OPTIONS` :448-452 (`-Dpct.channelb.judge`, default BOTH); `pct/BUILD.bazel:498-520` (`-Dpct.channelb.rules=LITE`); its judge-SQL ratchet is `ChannelB.measuresJudgeSql` :315-319 plus `ChannelBLane.judgeSqlHeld` | BOTH (isolated) | PRODUCT |
| relational corpus | `spec/.../rcorpus/MinimalCorpus.java:336-347` ('host' only; others refused); `spec/corpus.bzl:80` | HOST | UPSTREAM |
| lean-SQL corpus | `spec/.../rcorpus/LeanSqlCorpusTest.java:53, 125-131` | HOST | UPSTREAM |
| service tests (product) | `main/edge/ServiceTestRunner.java:286-291` (`ExecuteOptions.NONE`, `NoEdgeRun`) | HOST | PRODUCT |
| product queries | `Execution.run`: no test judge; in-body asserts are raises (A.13) | — | — |

- BOTH plans each judge alone: `TestPlanner.planIsolated` :96-108 builds `TestPlan.Isolated(host, database)`.
- `main/testexec/IsolatedRun.java` (195 LOC) compares the two plans' verdicts **and message texts**, and fails with
  `JudgesDisagree` (:189-193). Under option (b), records compare by construction.

### B.5 Where compatibility semantics still touch the database judge's code (rule 1)

None is reachable by a database-judge verdict at this commit, because of the A4 refusal. These are what remains wired
or dead.

1. **`VerdictWords` as a two-form kind.** `PlanStyleFacets.TWO_FORMS` (:58-60) and `PureText.twoForm` (:120-122)
   return `UpstreamVerdictWords`. Its SQL builders are dead (§0.2). Its String methods are live through the **host**
   planner (A.8).
2. **The `VerdictSql.Message` default.** The 5-arg constructor gives `lite=false`, which means "legend-pure's words"
   (:144-147). `written(m, value)` then routes to `represented(...)` (:2355) and on to `styled.pureOnly()`, the
   PureOnlyTexts facet, which is legend-pure's representation (:2360-2362). Every DatabaseJudge site sets
   `withLite(true)` (:285, :536, `VerdictSql.java:593`), so under PRODUCT the facet refuses by name. The live
   `represented` callers are `UpstreamVerdictWords.elementText` (dead) and this default. The default is an
   invariant-4-style default that points at legend-pure.
3. **`VerdictSql.quoted` and `messageAsStrings`.** These are legend-pure's `%r` quoting
   (`LiteralSpelling.literal(STRING)`: `'a'` with `\'`). `messageAsStrings` is applied by `DatabaseJudge` for the
   rendered-text family (`renderedStrings`, :83-84) on the **product** path. It frames `__expected`/`__actual` as Pure
   `%r` strings, and the lite frame then prints them in "expected X, got Y". Whether it reaches the sentence depends on
   whether the arm has written sides; that needs a probe. (`UpstreamVerdictWords.notInstanceOf` also calls `quoted`;
   that call is dead.)
4. **The canon grammar is the comparison key, and it can be the message.**
   - `__expected`/`__actual` are Pure literal spellings (`'a'`, `1.0D`, `%2020-01-01`, `<id instanceOf T>`). The
     database judge compares those texts.
   - Where an arm has no written side, the lite frame prints them (`frameSide` over the canons), for example
     `subset`, `gridPair` and json.
   - `liteOfCanon` (:2275-2343) parses legend-pure's print back into lite's text in SQL.
   - The **rules** that differed (CrossKindNumbers, VerdictNumbers, RenderedTextRule, DateLiteralTexts, GoldenNullWord,
     JsonGrammar's read) left with A4 (`PlanStyleFacets.java:32-35`).
   - So this is a legend-pure **spelling** used as lite's equality proxy, not a legend-pure equality **rule**. Whether
     rule 1's "no legend-pure equality canons" covers it is the owner's call. The design's option (b), typed values
     returned, removes it from the message path. Removing it from the comparison is the design's open §4.3 question.
5. **Dead agreement code.** `DatabaseAsserts.errorVerdictFor` (:415-434, about 18 lines) has **no caller anywhere** in
   the repo. Its javadoc says "for the agreement judge", whose fused path was deleted 2026-11-17.

Not compatibility code: `GridSide.emptyIsNull`/`csvStrings` (toCSV's own equivalence) and `NULL_CELL`/`NULL_CELL_WIRE`
(lite's null-cell token since census C5). Only comments there still say "TDSNull".

---

## C. How a value's text is produced for a report

### C.1 The database judge (all SQL, except the noted Java)

- Compared text: the canon (`LiteralSpelling.leaf`, `CanonicalRenderSql.wrapWithCanon`/`wrapTdsCanon`).
- Message text:
  - `LiteText.of` (A.6).
  - `liteOfCanon` (from the canon).
  - `instanceLite`/`instanceWritten`.
  - `writtenListOf` (`STRING_AGG`).
  - `cellsAsList`.
  - Counts and line numbers through `CAST(... AS VARCHAR)`.
  - The first failing element through `ARG_MIN`.
- **Java texts planned into the SQL**, all report-only and all spelled at planning:
  - `DatabaseJudge.liteDate` (:290-302) uses `String.format` for a date literal's text. It duplicates
    `LiteWords.dateText` (`main/testexec/LiteWords.java:182`).
  - `kindText` type names.
  - `Long.toString(expected line/column)` and `Integer.toString(index)` (LiteVerdictWords).
  - The expected assertError message, as a `StringLit`.

### C.2 The host judge (Java, at run)

- `main/testexec/LiteWords.java` (317 LOC):
  - `value` (:35-55): a String JSON-quoted (`quoted` :91), a Float through `messageFloat` (:113, six decimals or
    shortest), a Decimal through `toPlainString`, a date through `dateText` (:182).
  - Also `values`, `side`, `pairSide`, `kind`, `instance` (:256), `identity` (:289), `canonJson` (:194, which minifies
    and sorts JSON) and `carriedSide`.
- `HostJudgments.liteRows` (:49-55).
- `EdgePureWords.repr` (legend-pure's `toRepresentation`, used only on the compatibility path).
- `AssertRuns.excerpt` (:142).
- `Verdicts.firstTextDiff`.
- All of this is **report text** that the program never reads (rules 2 and 3 allow it).

### C.3 Java printing or making a value that is not a final report (candidate rule-4 sites)

- **`Literals.fold` string joins at planning.** It joins `+` and `joinStrings` of literals and lets
  (`main/compiler/spec/typed/Literals.java:28-80`). The results feed four places:
  - (a) `DatabaseJudge.plannedMessage` (:387): the caller's message becomes a `StringLit`. The program's message
    expression is evaluated by Java (a product-path report: rule 2 wants the database to compute values the program
    uses; arguably a "printing step").
  - (b) `AssertErrorNative.args` (:64-66): the expected error text that the judge **compares**.
  - (c) `BodyDriver.foldedSide` (:646-655): a folded constant becomes a judged side (`TypedSide.Scalar/Collection`).
    The judge compares a value Java computed.
  - (d) `BodyDriver.evalStringArg` (:837-844), `Seeds` :106/:163 and `TestKinds` :470: effect and seed arguments (not
    asserts, but the same mechanism).
  - JavaEvalLedgerTest pins the fold to "joins constants alone": `theCompilersLiteralFoldJoinsConstantsAlone`, :2387.
- **`HostJudgment.RowArg.Literal(carrier, text)`.** A literal argument is printed at planning (`Double.toString` and
  similar) and parsed back at the step (`HostJudgments.literal` :738-750). The value round-trips through Java text and
  is then compared.
- **`Verdicts.constantValue(s, type)`** (`main/testexecutionplan/Verdicts.java:113-128`): a constant cell is parsed in
  Java (`BigInteger`, booleans) into a judged value.
- **`CompiledPureHostForms.equalAcrossNumberKinds`** (:18-22): `String.valueOf(e).equals(String.valueOf(a))`. Java
  prints two values and compares the texts. This is the host judge's comparison on the compatibility path, not a value
  the program reads. Rule 3 forbids a printed text feeding a comparison inside the **query**; this comparison is the
  judge's.
- **In-body guards (A.13).** `StringLit` words become a raise's text in the product plan. That is a report, but it
  rides the product plan.
- Not rule-4 sites (diagnostics only): `CanonicalForm`/`CanonicalDivergence` (`main/testexec`, 117 and 507 LOC; "NOT a
  verdict path") and `UpstreamRenderedTexts` (an edge comparison policy over database texts).

---

## D. The test plan codec and the no-prose guard

### D.1 `main/testexecutionplan/TestPlanJson.java` (432 LOC): `VERSION = 37` (:73), `productVersion` = `PlanJson.VERSION` = 60

Components that hold assert prose today, and their codecs:

| component | codec | content |
|---|---|---|
| `TestPlan.words` | `TestPlanJson.java:123` / `:284` | `"lite"` or `"legend-pure"`; the design deletes it |
| `Failure.message` (ASSERT_FAILED) | `HostJson.wFailure/rFailure` :153-190 | a planned assert failure's sentence; inside `RootLoop.Decided`, `HostJudgment.Raise`, `Assertion.beforeTheArm`, `AssertError.caught` |
| `Failure.unjudgedReason` | the same | empty on lite's roads since D; the edge's appeal reads (B3) |
| `HostJudgment$AllRows.message` | `HostJson.java:340-352` | the default condition words, or the caller's literal |
| `HostJudgment$Decided.message` | `HostJson.java:525-532` | `valuesDiffer` |
| `HostJudgment$Messaged.unplanned` | `HostJson.java:662-670` | `AssertMessages.unevaluated` |
| the verdict `wrapper` / group `fused` | `PlanJson.nodeJson` (product format) | rendered SQL with the sentence `CONCAT`s and literals, the `__holds`/`__message` columns; counted as facts (`ExecutionPlan$Sql.statement`, `SqlExpr$StringLit.value`) |
| `Appeal.contract`, `Appeal$Unlisted.message`, `EngineTextJudgment.decline` | `edge/AppealJson`, `EdgeCodec` | B3 |

What test plan v38 must change (the design's codec step):

- Drop `"words"` (top level). Delete `TestPlan.words`, `LITE_WORDS`, `TestDoor`'s name call and the `TestRunner` check.
- `Failure` for ASSERT_FAILED: an assert record (`AssertKind` plus typed operands) in place of `message`. Either a new
  component, or `ErrorRecord` with an assert code; `Failure.RECORDED` and its constructor checks move.
  `Failures.of`/`rebuilt` (:59-131) rebuild `AssertFailed` from the record.
- `HostJudgment.AllRows`: `message` becomes `{kind, callerMessage literal | none}`.
- `HostJudgment.Decided`: `message` becomes `{kind, held, operands}`.
- `HostJudgment.Messaged`: remove `unplanned` (the body is refused at planning).
- `TestPlan.Verdict` gains the verdict step's assert kind and each value's source (a literal, or a column of the row).
- The verdict row schema loses `__message` and gains typed value columns:
  - Change `VerdictSql.schema()`/`batchSchema()`/`batchOutputs()` and `RootLoop` `*_COLUMN` (:28-34).
  - `VerdictRowOrderTest` moves with them.
- `productVersion` likely stays 60. The wrapper is a `TdsResult` whose columns are data. Bump it only if a node's shape
  changes.
- The only reader of the format `legend-lite-test-plan` is Java (`TestPlanJson`). No TypeScript or Python host reads
  test plans.

### D.2 `test/PlanTypesAreDataTest.java` (367 LOC)

`PROSE_RESIDUE` (:178-201) has 7 entries. Item 3 removes:

- `"HostJudgment$AllRows.message"` (:191-192).
- `"HostJudgment$Decided.message"` (:193).
- `"HostJudgment$Messaged.unplanned"` (:194-195).
- `"Failure.message"` (:186-187). This one is conditional: the edge's prose `Failure`s (§0.4) also hold it, so either
  narrow the entry to "the edge's records, B3" or remove it with B3.

It stays: `SqlExpr$RaiseText.asData` (B2), `Failure.unjudgedReason` (B3), `Appeal$Unlisted.message`, `Appeal.contract`
and `EngineTextJudgment.decline` (B3).

`FACTS` (:208-332): item 3 removes `"TestPlan.words"` (in the "a code" set, :287-288). It adds whatever new text
components the assert record has, for example an operand's text, which would be "a value the program wrote". The guard
cannot see sentence `StringLit`s inside the wrapper SQL (`SqlExpr$StringLit.value` is a fact, the BE audit 5's M3).
Proposal: a check that no `AssertSentence` template text appears in a planned statement.

---

## E. Tests and guards that pin assert words or judge behaviour

### E.1 Guards that will move

- `test/JavaEvalLedgerTest.java` (2,501 LOC):
  - `EVICT_SIZE` stripped-line pins:
    - `AssertVerdicts` 1374 (:598), `DatabaseJudge` 808 (:595), `HostJudge` 237 (:594).
    - `HostJudgments` 679 (:1056), `AssertRuns` 142 (:1058), `AssertErrorRuns` 72 (:1059), `DatabaseVerdicts` 38
      (:1063).
    - `testexecutionplan/Verdicts` 93 (:1064), `PureAsserts` 107 (:319).
    - Edge: `SqlTextVerdicts` 1089 (:1031), `SqlTextRuns` 199 (:1062).
  - `VERDICT_FILES` (:2210, the `verdictFilesJudgeOnly` evaluation vocabulary): `AssertVerdicts`, `SqlTextVerdicts`,
    `AssertErrorNative`.
  - `SESSION_ONLY_FILES` lists the judges.
  - Pins are shrink-only, so deletions re-pin down, each with a dated note.
- `test/HookRulingTest.java` (859 LOC): the two-form kinds list includes `VerdictWords` (:371-373). :431-441 pins
  PRODUCT → LiteVerdictWords, UPSTREAM → the edge form, and `LITE_WORDS == LiteVerdictWords.NAME`.
- `test/HookLambdasTest.java` (631 LOC): :80-93 (`VerdictWords` twoForm reaches) and :122-123
  (`LiteVerdictWords` → `VerdictWords`).
- `test/CompatSpellingGuardTest.java` (637 LOC):
  - :95-98 lists legend-pure's assert sentences banned from product sources ("No error was thrown", "^Assert failed$",
    "\\nexpected: ", ...).
  - :196-203 has the `LambdaBodies | Assert failed | STAYS` row.
  - `WORDED_BY_HOOK` :480-481 lists `PureAsserts`.
- `test/VerdictChannelRegisterTest.java` (123 LOC): `JUDGE_CALLERS` :32-69 lists `HostWords`, `LiteHostWords`,
  `EdgeHostWords`, `CompiledPureHostForms`, `HostForms` and `LiteHostForms`.
- `test/EdgeOnlyMembersTest.java` (698): VerdictSql notes :71-113 and an EdgeHostWords note :128.
- `test/EdgeAuditOpenTest.java` :207 (the UpstreamVerdictWords note).
- `test/ProductFormsUseTheirArgumentsTest.java` :198 (`VerdictWords#elementText`).
- `test/SqlFnSortedTest.java`: `BOTH_WRITERS` names `com.legend.lowering.VerdictSql` and `LiteText` as product writers
  of CONCAT, DATETIME_TO_TEXT, FLOAT_SHORTEST_TEXT, LENGTH, REPLACE, STRPOS, SUBSTRING, LESS_EQUAL, NOT_EQUAL, ABS and
  others (:155-380). Entries shrink when LiteText and the message SQL go.
- `test/ErrorShapeGuardrailTest.java` :192-198 (comments naming `AssertMessages.unevaluated` and `Messaged#unplanned`).
- `test/PlanTypesAreDataTest.java`: D.2.
- `test/testplan/HostOnlyCompatibilityTest.java`: rule 1's guard; it stays.

### E.2 Behaviour and message tests (core)

- `test/lowering/VerdictRowOrderTest.java` (32 LOC) pins the 7-column order. It moves.
- `test/testplan/TestPlanJsonRoundTripTest.java` (187 LOC) constructs `AllRows(..., "message")` (:110),
  `Decided(true, "held")` (:128), `Messaged(..., "unplanned")` (:141) and `RootLoop.Decided(FAILURE, true)` (:82).
- `test/error/AssertSentenceTest.java` (43 LOC).
- `test/testplan/AssertBarTest.java` (525 LOC) pins upstream outcomes and lite sentences under HOST, DATABASE and BOTH
  (54 sentence pins plus `GAPS`). Its lite sentences stay unless value printing changes. It guards message parity.
- `test/testplan/BodyRouterParityTest.java` (2,364 LOC) has 57 `JudgeMode` uses; outcomes and messages per judge.
- Lite-sentence pins:
  - `test/testexec/IsolatedRunTest.java` (101 LOC, 5).
  - `test/testplan/QuantifiedRowsTest.java` (4).
  - `test/exec/TaggedListTest.java` (4).
  - `test/AuditRound3Test.java` (4).
  - `test/testexec/EqualityJsonUnorderedRootTest.java` (3).
  - `test/AssertVerdictsTest.java` (160 LOC; 3 lite and 2 legend-pure).
  - `test/DecimalKindsTest.java`, `test/testexec/InstanceIdentityTest.java`, `test/JudgesOnPlansTest.java` (190 LOC).
- Edge tests that pin **legend-pure's** assert words, which move under Q15 = A:
  - `test/testexec/PureAssertsTest.java` (291 LOC, 7).
  - `test/AssertErrorNativeTest.java` (3).
  - `test/AssertVerdictSpliceTest.java` (2).
  - `test/testplan/CompiledPureNumbersEdgeTest.java` (1).
  - `test/RelationCountAssertsEdgeTest.java` (1).
  - `BodyRouterParityTest` (1), `InstanceIdentityTest` (1), `DecimalKindsTest` (1).
- The ladder (`core/src/test/resources/ladder/`, 24 files):
  - The 12 `*.current.sql` pins (62,945 bytes) **all** carry `__message` and `__holds`. The 12 hand-written
    `*.lean.sql` targets (28,866 bytes) carry neither.
  - `//core:update_ladder_test` fails when current moves (`test/ladder/LeanSqlLadderTest.java` :22-26;
    `LadderRender.java` renders with the DATABASE judge).
  - Per rung, current / lean in bytes:

    | rung | current | lean |
    |---|---|---|
    | r01 | 1554 | 1142 |
    | r02 | 1554 | 1142 |
    | r03 | 1782 | 1146 |
    | r04 | 835 | 607 |
    | r05 | 921 | 693 |
    | r06 | 2721 | 1850 |
    | r07 | 1303 | 1631 |
    | r08 | 2590 | 1437 |
    | r09 | 3757 | 2932 |
    | r10 | 1883 | 1425 |
    | r11 | 38209 | 11163 |
    | r12 | 5836 | 3698 |

  - Item 3 re-pins all 12 current files. The gap is not all message: the lean targets also differ in CTE shape.

### E.3 Lane ledgers that pin assert words (compatibility words move under Q15 = A)

- **PCT channel B**, `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv` (844 lines):
  - The compatibility lanes have **52 `AssertFailed` rows in legend-pure's words**: `⏎ expected: '#TDS...'` frames,
    "No error was thrown" ×10, "Execution error message mismatch" ×8, and `⏎ expected: [N] ⏎ actual: []`.
  - The `duckdb-lite` lane (439 rows) has 9 rows in lite's sentences. Rows match by "CONTAINS".
- `pct/src/test/resources/edge/channel_b_{duckdb,sqlite}_essential_outcomes.tsv`: 5 rows with legend-pure's assert
  words.
- **PCT channel A** (`Test_LegendLite_*_PCT.java`, for example `Test_LegendLite_EssentialFunctions_PCT.java:55, 152-153`
  and `Test_LegendLite_LiteRules_EssentialFunctions_PCT.java:34-40`): legend-pure's words, but **upstream's runner**
  produces them. They do **not** move.
- **PCT judge-SQL ratchet:** `pct/.../channelb/ratchets.tsv`, `channel_b.<suite>.judge.{statements,chars,plan}`. For
  example essential has 477 statements, 7,196,265 chars and 47,493,700 plan bytes. A shrink past 1% re-pins
  (`ChannelBLane.held`, `//pct:update_ratchets`). Item 3 shrinks chars and plan on the LITE-PCT lane.
- **Corpus** (`spec/src/test/resources/edge/corpus_{duckdb,h2}_host_judge-host.tsv`, 5,590 and 5,294 lines):
  - legend-pure frame and default words: "Assert failed" 10 and 10; `expected: '...'` frames 39 and 42.
  - The appeal words (`(sql-text, oracle declined:` 26, `rows underivable` 20, `ROW verdict` and similar) are B3's.
  - `spec/src/test/resources/rcorpus/{duckdb,h2}-accepted-roster.txt` each have 19 `expected: 'select top 20...'`
    detail texts (S193 rows).

---

## F. Sizes

### F.1 Deleted by the design (LOC)

| file | LOC | note |
|---|---|---|
| `main/lowering/VerdictWords.java` | 101 | the database judge's hook |
| `main/lowering/LiteVerdictWords.java` | 175 | |
| `main/edge/UpstreamVerdictWords.java` | 192 | its SQL already dead |
| `main/lowering/LiteText.java` | 106 | only VerdictWords/VerdictSql call it; goes with option (b) |
| **subtotal (database judge's words)** | **574** | |
| `main/testexec/HostWords.java` | 109 | if the catalog renders records directly (Q15 = A) |
| `main/edge/EdgeHostWords.java` | 233 | Q15 = A |
| `main/edge/EdgePureWords.java` | 154 | Q15 = A (`repr` may survive for rule 3's printing at the edge) |
| **subtotal (host's two-form words)** | **496** | |
| VerdictSql's sentence and message members (A.5) | ≈588 of 2,797 | the measured spans; `selfContained` keeps its `__holds` half |
| fragments | ≈60 | `AssertMessages.unevaluated` (4); `HostSides.withoutTheMessage` (15); `DatabaseJudge.words` (4) and `liteDate` (13); `VerdictBatch.words` and `Scope` (≈25); `PlanStyleFacets` entry (2); `PureText` branch (4); `CompiledPureHostForms.words` (5) and `LiteHostForms.words` (4) |
| `DatabaseAsserts.errorVerdictFor` | ≈18 | dead today (B.5.5) |

The design note's "about 640 lines" matches `VerdictWords` + `UpstreamVerdictWords` + `HostWords` + `EdgeHostWords` =
635. With `LiteVerdictWords`, `LiteText` and `EdgePureWords` it is **≈1,070 whole-file lines plus ≈650 lines inside
VerdictSql and fragments**.

### F.2 Rewritten (LOC)

- `main/lowering/VerdictSql.java` 2,797.
- testplan:
  - `AssertVerdicts.java` 1,966 and `DatabaseJudge.java` 1,071.
  - `BodyDriver.java` 872 (`newVerdictBatch`), `DatabaseAsserts.java` 731 and `VerdictBatch.java` 688.
  - `HostJudge.java` 336, `TestPlanner.java` 256, `TestDoor.java` 205 and `HostSides.java` 143.
- testexec:
  - `HostJudgments.java` 906, `TestRunner.java` 483 and `RootLoops.java` 351.
  - `LiteWords.java` 317 (the catalog's value printer stays) and `LiteHostWords.java` 224 (becomes the catalog's
    renderer).
  - `AssertRuns.java` 207, `IsolatedRun.java` 195, `AssertErrorRuns.java` 115 and `DatabaseVerdicts.java` 61.
- testexecutionplan:
  - `HostJson.java` 1,193, `TestPlan.java` 642 and `TestPlanJson.java` 432.
  - `HostJudgment.java` 255, `Failures.java` 147, `Failure.java` 140, `Verdicts.java` 137 and `RootLoop.java` 122.
- `main/error/AssertSentence.java` 128 (becomes the `AssertKind` catalog).
- `main/compiler/spec/AssertMessages.java` 134.
- Adjacent (A.13): `LambdaBodies.java` 162, `LoweringForms.java` 108, `ProductLoweringForms.java` 137 and
  `EdgeLoweringForms.java` 167.

### F.3 Counts of sentence builders (today)

- `AssertSentence` has **24** templates, the product catalog.
- `VerdictWords` has **22** methods: 18 sentences plus 2 value or side writers (in SQL) and 2 Java strings. Each of
  2 forms implements all 22, for **44** implementations.
- `HostWords` has **26** methods: 25 sentences plus `caught`. Each of 2 forms implements all 26, for **52**
  implementations.
- `EdgePureWords` has **11**.
- `LoweringForms.assertFailed` has 2 forms. Inline literals add `LambdaBodies` "Assert failed" ×1 and
  `AssertMessages.unevaluated` ×1.
- Uncatalogued sentences in Java: `renderedOf`, 3 `firstTextDiff` forms, the 2 "UNJUDGED in database mode" sites and
  the assertError line/column text. Diff text builders that feed EdgeHostWords only: 2 in `TdsCompare` and about 5 in
  `Equality.firstDiff`.
- In VerdictSql SQL: 1 inline prose literal (the assertError line/column text), 3 unjudged reasons and 5 descriptor
  texts.
- **About 145 distinct places that word an assert failure.** After item 3 the target is **one**: the catalog over an
  `AssertKind` record, rendered in Java from typed values (option (b)).
