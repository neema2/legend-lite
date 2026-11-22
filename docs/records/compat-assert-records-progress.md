# Item 3: progress (branch claude/compat-assert-records-nufdlk)

The session's own notes, as START-HERE §0 asks: what each slice did, what was measured, what moved, and what this
session would add to the program's records. The design is `compat-assert-records-2026-11-21.md` on
`claude/compat-program`.

## Baseline (2026-11-21)

- `main` at `2942e983` (#131 merged). The fast round (`fast-round-targets.txt`, 68 patterns, 96 tests): **96 of 96
  green**, 1,667 s including the first build of core.
- The code map's line numbers (written at `dad8a710`): every file the map sizes has the same `wc -l` on `main`.

## The user's rulings in this session (verbatim, 2026-11-21)

- Branch: "Whatever branch makes most sense" -> `claude/compat-assert-records-nufdlk`, this session's.
- On the report values (asked: "Shouldn't it all be arrow?", "Host should also take typed arrow right?"): typed
  values end to end, no text and no JSON carriage. Proposed answer, not yet ruled: the WIDE verdict row (below).
- On the failure message ("move important to have the right expert design and architecture"): the message is a
  program value, computed by the database only where the assert fails; an error computing it is the outcome.

## S0: dead code (commits c3ff1522, 32182249, 85fc55ca)

- `VerdictSql.Message`: the five-argument constructor whose default was legend-pure's words (`lite=false`, an
  invariant-4 default every caller overrode), the `lite` and `listed` flags, `withLite`, `Message.listed`,
  `instanceWritten` and `represented` deleted; `DatabaseJudge` drops its `withLite(true)` calls.
- `DatabaseAsserts.errorVerdictFor`: no caller.
- `UpstreamVerdictWords.elementText` asks the edge's own facet (`EdgePureOnlyTexts`).
- Not in S0, deliberately: `UpstreamVerdictWords`' SQL builders. Its two String methods are live on the host planner
  (`conditionFailed`, `valuesDiffer`), so the two-form kind leaves whole at S3 with no throwaway interface.
- Guards moved (shrinks, dated in place): `ArchitectureTest`'s planner-entry census (`VerdictSql#represented`),
  `EdgeSeparationTest.HOOKS_RULED` (`VerdictSql -> PureOnlyTexts`).
- The verdict SQL is byte-identical (`//core:update_ladder_test` green).
- R1's ArchUnit guard lands at S3: the product-to-edge rule already exists (`ArchitectureTest`, "resideOutsideOfPackage
  edge"); R1's own content is "the database judge asks the plan style nothing", and its last two asks
  (`DatabaseJudge.words`, `VerdictSql.represented`) go at S3 and S0.

## S4 prototype: the wide verdict row (2026-11-21, scratchpad/proto)

One row per verdict group: each assert its own typed columns `v<i>, e<i>, a<i>` (a `CROSS JOIN` of one-row members),
the values filled only where its verdict is false. No union, no tag, no text: the plan types every column, exec
decodes it as it decodes any result (the path Arrow writes).

| | DuckDB 1.5.6 (the build's jar) | H2 2.5.252 (the build's jar) |
|---|---|---|
| five asserts (a String constant, a String multiset, a count, a Float multiset, a date) | 1,535 chars, 15 columns | 1,617 chars, 15 columns |
| three rows | v0..v3 true, their values NULL | the same |
| a fourth row | v1..v3 false; `VARCHAR[] [a, b, c] / [a, b, c, c]`, `BIGINT 3 / 4`, `DOUBLE[] [0.5, 1.5, 2.5] / [..., 3.5]`, `DATE` | the same, `CHARACTER VARYING ARRAY`, `DOUBLE PRECISION ARRAY` |

Still to prove in S4: a table side (a list of rows: DuckDB `LIST(STRUCT)`, H2 `ARRAY` of `ROW`), a class instance (a
struct), the quantified (per-element) form, `assertError`'s row.

## S0's audit (an independent reviewer, 2026-11-21): behaviour-neutral, no HIGH or MEDIUM

LOWs and NITs, fixed in the follow-up commit: `Message.styled` dead; comments describing the deleted legend-pure
branch (VerdictSql's Message javadoc, `sideFacts`, `written(Side…)`; DatabaseJudge's "as upstream writes it"); stale
guard comments (PureOnlyTextsRefusalTest, ArchitectureTest's census note, VerdictIdsTest's agreement judge); the local
`listed`. Named, not fixed: the unread `__o` projection (its removal changes the SQL: S4); c3ff1522 alone is red
(EdgeOnlyMembersTest, fixed by 32182249). The two guard moves go in the PR's pins table for the user's word.

## Homework (2026-11-21)

- **Upstream's default words, read in the pinned legend-pure** (`essential/tests/`): `assert.pure:29-32` "Assert
  failed"; `assertEquals.pure:20-21` `'\nexpected: %r\nactual:   %r'` (the `[..]` list form for collections);
  `assertEq.pure:19`, `assertIs.pure:19`, `assertEqWithinTolerance.pure:19` the same frame; `assertNotEmpty.pure:19`
  "Expected non-empty collection"; `assertContains.pure:22` `'%s does not contain %r'`; `assertSameElements.pure:19`
  the sorted list frame; `assertInstanceOf.pure:19` `'expected %r to be an instance of %s, actual: %s'`;
  `assertNotEquals.pure:19` `[..] should not equal [..]`. The register row S2 proposes is built from these.
- **Does any test's verdict hang on legend-pure's assert words?** Upstream's `assertError` calls whose expected text is
  "Assert failed": 3 (`assertFalse.pure:65-66`, `fail.pure:39`), all legend-pure's own `<<test.Test>>` unit tests, not
  PCT tests; no lite lane runs them and no ledger names them. M5 (the owner session) found no "No error was thrown".
  So Q15 = A's condition holds on the evidence: words move, verdicts do not. The lanes confirm it at S2.

## S4 prototype, part 2 (2026-11-21): tables, instances, per-element, assertError

Four members in one wide row (`proto/duck2.sql`, `proto/h22.sql`), each failing so its values ride:

| member | DuckDB 1.5.6 | H2 2.5.252 |
|---|---|---|
| a table side, in order | `STRUCT(ID, NAME, AMOUNT)[]`, decoded as a list of maps | `ROW(...) ARRAY`; the driver hands each ROW as a nested ResultSet |
| a class instance (a Pair) | `STRUCT("first", "second")` | `ROW("first", "second")` (`SECOND` is reserved: quote) |
| a quantified assertEquals over a zip | the first failing index (1) and its two typed values (2, 3) | the same |
| assertError's row | the two texts | the same |

1,509 and 1,540 characters for the four. The decision it forces: a report value travels as its database carries its
Pure type in ANY query -- DuckDB's native STRUCT and LIST (exec's JdbcDecode reads a driver's Array and Struct); H2's
arrays for lists of scalars and its JSON carrier for a row or an instance (lite's H2 dialect has no STRUCT type:
"JSON constructors only"; RowDecode reads a struct from its JSON object). The judge adds no decoding of its own.

## Warm lanes, main's baseline at 2942e983 (2026-11-21)

- Postgres/SQLite/MongoDB (session_01UmbETZRDU9ugZ8X2d1hGyb): all 28 targets PASSED. Channel A pct_{postgres,sqlite,mongodb}_*
  (345/137/469/205/93 testcases each); channel B postgres {essential 334 PASS 11 ERROR, grammar 131/6, relation 467/2,
  standard 198/7, unclassified 92/1}, sqlite {336/9, 134/3, 458/11, 205/0, 92/1}; mongodb_plan_test 21,
  core_tests_integration_sqlite 1,960, data_source_servers_test 9. Per-test outcome files kept in that container (r78/).
- The cloud lanes: Legend Dev's setup script cannot read the environment's variables (the platform's rule), so its
  credential step must run in the session; the user authorized it in three new sessions (BigQuery
  session_01XmdJ7mbtfkeVv2uC3rPzmi, Snowflake session_01XmdJ7hapZtcduFxfuVsqc2, Databricks
  session_01XmdJ7ixAjASpQmjt35KFiS), each running main's baseline.

## S4's plan (2026-11-21, before code)

The database judge in four steps, each green on core's judge_database and judge_both lanes and the LITE-PCT lane:

1. **The wide row, nothing else.** `VerdictSql.batch` joins its members as one-row subqueries (`CROSS JOIN`), each
   member's columns its own (`m<i>_...`), in place of the `UNION ALL` with `__ix`; the hoisted CTEs unchanged.
   `RootLoops` reads a verdict's columns by its member's offset. Every member still writes today's seven columns, so
   every verdict and message is unchanged -- the step only changes the carriage. A member that is not one row (a keyed
   member) stays its own statement, as today.
2. **Typed members, one rule at a time.** Each arm (`equals`, `size`, `contains`, `empty`, `condition`, `perElement`,
   `tolerance`, `gridPair`, `tdsEquivalent`, `sameElements`, `instanceOf`, `is`, `eq`, `json`, `rendered`,
   `quantified`, `assertError`) moves from the canon texts to typed comparisons (M1's forms) and writes its verdict and,
   only where it fails, its typed values (`v`, `e`, `a`, and the facts its kind needs: an index, a count). The test
   plan's verdict gains its kind and its values' columns; `RootLoops` builds the record from them, printed by
   `LiteWords`, the host judge's printer. A migrated member writes no `__message`; old and new members share a row
   while the migration runs.
3. **The caller's message** becomes a `CASE WHEN NOT v THEN <message> END` column of its member (the user's lazy
   rule), and `RootLoops.messaged`'s follow-up statement goes.
4. **Deletions** once no member writes a sentence: `VerdictWords`, `LiteVerdictWords`, `LiteText`, VerdictSql's sentence
   members (~650 lines), `AssertMessages.unevaluated`, the canon machinery the judge alone used; `IsolatedRun`
   compares records; the ladder re-pinned leaner (M1); `__unjudged`/`__lenient`/`__holds` go with the canons.
- Trino/H2 (session_01Hztqdk3qySkTkH2uUDuUdL): all 13 PASS. Channel B trino essential {339, ERROR 6: 4 date BigNumber
  DataErrors, 2 "AssertFailed: No error was thrown"}, the rest full; h2 relation {432, 37}; data_source_servers 9/9.
- MariaDB/MySQL/ClickHouse (session_018ATdMqvopmrLibQhwN4GQ4): channel A all 15 PASS; channel B 9 PASS, 6 FAIL, every
  failure main's known one -- MySQL's Decimal text (9/4/4) and MariaDB's list-order flake (15/4/1 this run);
  data_source_servers 9/9.
- Snowflake (session_01XmdJ7hapZtcduFxfuVsqc2): all 10 PASS; channel B essential {338, ERROR 7, two "No error was
  thrown"}, grammar {133, ERROR 4: "a field '__canon' of unknown type reached Snowflake"}.
- Databricks (session_01XmdJ7ixAjASpQmjt35KFiS): all 10 PASS; channel B essential {338, 7, two "No error was thrown"},
  relation {461, 8 TRUE-WIRE-BUG distinct rows}.
- To watch at milestone 1: the channel B ledgers that pin legend-pure's "No error was thrown" (Trino, Snowflake,
  Databricks, likely BigQuery) move to the catalog's words -- a word, not a verdict.
- BigQuery (session_01XmdJ7mbtfkeVv2uC3rPzmi): channel A all 5 PASS; channel B grammar, relation, standard PASS;
  essential and unclassified FAIL ON MAIN: 19 unledgered, one cause -- "Column 3|4 in UNION ALL has incompatible types:
  INT64, STRING" (16 date/parseDate tests, 3 coalesce AllEmpty). Main's own, not item 3's (compatibility runs the host
  judge alone, so not the verdict batch); for the program's list of what main has red.

## Milestone 1 measured (2026-11-21, at 6696f5c0; PCT channels A and B, both corpus lanes, lean SQL)

- **Verdicts moved: none.** PCT: every outcome ledger's verdict column identical. Corpus: every judge-host row's
  test, assert, kind and PASS/FAIL identical on DuckDB and H2. Lean SQL (baseline, both corpus lanes) green.
- **Words moved (Q15 = A):** PCT 5 outcome rows (DuckDB essential 2, SQLite essential 3: testArcCosineError,
  testArcSineError, testDateDiffWithDifferentTimeZones) and the channel B hand ledger's 2 rows (the same two
  trigonometry tests, "No error was thrown" -> `expected the error "...", got none`). Corpus judge-host rows: DuckDB 54,
  H2 64, every one an AssertFailed sentence (upstream's `expected: '..' | actual: '..'`, `Assert failed`, the rendered
  and JSON first-diff forms -> the catalog's).
- **Ratchets:** channel B essential's judge plan shrank (chars 7,196,265 -> 7,194,561; plan 47,493,700 -> 47,490,292).
- **Witnesses:** 41 accepted-roster lines (DuckDB 21, H2 20; Q15 predicted 41) whose witness substring was the old
  words; with them missing the harness fails the test ("WITNESS MISSING ... re-decide"), so the rosters show 21 / 20
  GAINED. Same divergence, same reason; only the witness text moves:
  - 19 testDataGeneration tests (S193), both lanes: `expected: 'select top 20` -> `expected "select top 20`
  - testDateTimeInclusiveRangeQuery (h2-literal-coercion), both lanes: `(2 rows) | is not equivalent to: |
    [settlementDateTime] (1 rows)` -> `[2014-12-04T21:00:00.000000Z]] within 0.0, got [[2014-12-04T21:00:00.000000Z]]`
  - testCheckedWithCircularConstraints (isDistinct-in-checked-constraint), DuckDB: `FIRST DIFF at $ (root array as a
    multiset) expected 4 element(s), got 4; missing` -> `got [{"defects":[],"value":{"firstName":"Ann","firm":{"legalName":"Firm X"}}}`
- **A diagnostic lost:** the JSON and rendered-text failures no longer name the first difference; the catalog prints
  both whole sides.

## S2+S3's fixes, re-audited (2026-11-21, an independent reviewer, at 85ea4305)

- **HIGH, fixed:** `VerdictBatch.sole()` ignored the root's unplanned message, so an `assertError` over one assert whose
  message raises dropped that error on the database judge (the host caught it). Now: a failure decided at planning
  raises the message's error; a verdict row refuses the assertError by name (a raising row cannot raise another
  error) until S4.
- **MEDIUM, fixed (refusal until S4):** a root held one message slot, but an unrolled quantified assert plans each
  element's message: a passing element's error replaced a later element's failure, and differing reading messages
  overwrote each other silently (that half predates item 3). Now refused by name; S4 carries a message per step
  (with decision 4, the host's eager element messages).
- **MEDIUM, fixed:** RootLoops reported an unplanned error of any class as a failed verdict; now an error that is
  neither AssertFailed nor DataError is raised unreported, as the host's AssertRuns does.
- **MEDIUM, fixed:** the subset fold is an assert's condition, so its failure is the condition's record (TRUE/FALSE),
  on the host and in the database judge's subset row (which now takes the condition's message); probes added.
- **LOW, fixed:** the codec round-trips a root with an unplanned message; v38's javadoc names it. A message planned
  outside a batch is an IllegalStateException (a defect), never a root failure.
- **NITs, fixed:** three stale comments (AssertMessages, ErrorShapeGuardrailTest, PlanTypesAreDataTest's dated note).

## Warm lanes at 85ea4305 against main 2942e983 (2026-11-21)

- **Postgres, SQLite, MongoDB** (28 targets): no verdict moved anywhere (every channel B census identical; channel A,
  mongodb_plan_test, core_tests_integration_sqlite and data_source_servers_test byte-identical). Three channel B
  ledger targets fail on words alone, 7 rows: postgres essential (testArcCosineError, testArcSineError), postgres
  relation (testVariantColumn_keyExtraction, testVariantColumn_filterOnKeyExtractionValue), sqlite essential
  (testDateDiffWithDifferentTimeZones, testArcCosineError, testArcSineError). Channel A's logs carry no failure text.
- BigQuery, Snowflake, Databricks: running.

## The mixed-run check: RULED option 4, on a condition (the user, 2026-11-21 ~22:50)

"Mix run fine as long as we actually get to b3". No new mechanism in item 3: the check that refused a plan made under
one plan style run with the other side's run hook (and so with the other side's host equality rules) went with S3's
words, and nothing replaces it. B3 removes the hazard (one host rulebook, lite's; E1 withdrawn 2026-11-20 ~21:00).
The condition is B3 itself: item 3's PR names it as an open dependency, and until B3 lands the pairing is a
convention kept by the callers that make both halves: PCT channel B (`ChannelB`), the corpus harness
(`MinimalCorpus`) and the edge's test helpers (`EdgeProgram` and five tests); no product code passes the edge's hook.
- **Snowflake** (10 targets): no verdict moved (channel A 1,249 identical; channel B censuses identical). One target
  red on words: channel B snowflake essential, its two ledger rows for testArcCosineError and testArcSineError
  (ledger.tsv's snowflake rows; the same two tests' rows for DuckDB, Postgres and Trino read the same old text).
  No other message moved.
- **Databricks** (10 targets): no verdict moved (2,506 per-test rows identical). One target red on words: channel B
  databricks essential, the same two trigonometry rows. No other message moved.
- **BigQuery** (10 targets): no verdict moved; main's 19 unledgered UNION ALL errors unchanged. The two trigonometry
  rows' words moved (census 16 -> 18 mismatches on a lane already red on main). The quoted database message's newline
  is written `\n` inside the quotes: the catalog prints a String value as lite's value printer does (JSON-style).
- **All six warm databases: no verdict moved.** Word rows to re-record, per lane: Postgres 4, SQLite 3, Snowflake 2,
  Databricks 2, BigQuery 2 (plus DuckDB's, measured locally). Unmeasured rows of the same two tests: Trino, MariaDB,
  MySQL, ClickHouse (CI on the PR).

## The user's rulings, 2026-11-21 ~23:00

- **Decision 1 (re-record the word moves): "1 is fine".** Held until the assert words' audience is settled (below), so
  no row moves twice; done at S9 with the pins table.
- **S398 / the assert words:** the user, "Shouldn't it be pure words everywhere with overrides in product for where
  lite is better or pure is too expensive? Honestly this is not important at all - we really need to focus on getting
  item 3 done so we can move faster on all of Phase B". This session's answer, to keep item 3 moving: item 3 ships the
  one catalog as built and measured (no verdict moves on any lane); legend-pure's words on the compatibility routes come
  with B2, whose edge presenter already writes legend-pure's printed values from typed rows -- legend-pure's value
  spelling stays at the edge (G5), never in the core. S398 is then a temporary row that B2 retires.
- **Decision 4 (quantified element messages): "Fix 4 whenever makes most sense"** -> S4, which rebuilds that code.
  Found meanwhile: the database judge drops an unrolled element's caller message entirely (`[1, 0]->forAll(i |
  assertEquals(1, $i, | $l->at($i)->toString()))` answers "expected 1, got 0 at index 1", upstream "10"); S4 fixes it
  with a probe in AssertBarTest's bar.

## S4's scope (the user, 2026-11-21 ~23:30)

"We can do it in phases but it's all part of item 3 we can start with tables/json/text out of scope for first commit,
but we should figure out how to do those too". Phase 1: the scalar and collection families typed (equals,
sameElements, size, empty, contains, condition, tolerance, instanceOf, eq/is, quantified), records on both judges, the
sentence SQL deleted. Phase 2, still item 3: tables (grids), JSON and rendered text; a read-only research agent designs
it (scratchpad/research/next-phase-design.md). Step 1 (the wide row) at 545fa65a.

## S4 step 2 progress (2026-11-22)

- Typed members built: EQUALS / SAME_ELEMENTS / IS_NOT (String, Integer, Float, Decimal, Boolean), CONDITION
  (assert, assertFalse, assertIs over elements, the quantified vector forms, the subset fold), SIZE, EMPTY, CONTAINS,
  TOLERANCE, PER_ELEMENT. Each member's verdict over typed values, portable (in order: a positional join; in any
  order: a signed UNION ALL grouped by value; no list equality, H2 has none); each side's values ride the failure as
  the LIST reducer (H2's JSON_ARRAYAGG), decoded by exec; one record choice for both judges (AssertRecords.equality,
  ofTypedRow).
- The ladder, re-pinned: r01 1,557 -> 838, r03 1,785 -> 840, r06 2,724 -> 1,180, r11 38,212 -> 1,196 characters.
- Ledger: DatabaseJudge interim 799 -> 842 -> 887 (the user's word, "I approve raising the ledger ceiling"; the
  classifier refused the edit until the user approved it explicitly); DatabaseVerdicts 38 -> 36, HostJudgments
  670 -> 666 (shrinks).
- Left on the seven-column path for now: dates, enums, instances, literal channels, instanceOf (its target type
  needs a field in the report), graph and envelope sizes, tables, JSON, rendered text (phase 2).
- Found: VerdictQueries.unrolledElement drops an unrolled element's caller message ("the MESSAGE arguments drop",
  :359) for BOTH judges -- `[1, 0]->forAll(i | assertEquals(1, $i, | $l->at($i)->toString()))` answers "expected 1,
  got 0 at index 1" on both, upstream "10". Step 3 keeps it, each element's message read only where that element
  fails (decision 4).

## The rulings for S6-S8 (the user, 2026-11-22), on the homework's briefs (scratchpad/homework/s5-s8-briefs.md)

- **Q-D, verbatim as ruled in this session (2026-11-21):** three carve-outs keep Java literal joins -- an effect's
  SQL/DDL text, a seed's CSV, the test-data generator's load text; a compared constant (727 corpus sides measured) is
  computed by the database; the guard pins fold to those callers.
- **Q-S6 "all yes":** (a) a caller's message stays the user's own failure (`fail(msg)`); (b) each in-body guard
  raises its own catalog kind (assertFalse "expected false, got true", assertNotEmpty NOT_EMPTY, ...); (c) `fail()`
  with no message raises the assert-failed kind (Pure's fail() is assert(false)).
- **Q-S7a "yes":** the edge's SQL-text and plan-text goldens keep their Java join as a fourth, TEMPORARY carve-out,
  guarded and dated, deleted by B3.
- **Q-S7b:** the user asked "why not add those to java fold?"; answered: a caller's MESSAGE joined in Java is a final
  print, within Q-D (no refusal); a joined assertError expected text and a joined JSON golden are COMPARED, so Q-D
  puts them in the database: refused by name until a use appears (none on any lane). Pending the user's last word.
- **Q-S8 "okay":** assertError(|..., 'No error was thrown') passes, as upstream; the constant is Pure's semantics,
  CompatSpellingGuardTest gains a dated STAYS row citing Q-B; a position with it is refused by name.

## Q-S7b closed (the user, 2026-11-22): "Okay on s7 questions"
- The answer it accepts: a caller's message joined in Java stays as a final print (rule 3's closed list); a compared
  side (assertError's expected text, a JSON golden) is never folded in Java -- the database computes it, or the body
  is refused by name. With Q-S7a (the edge's SQL/plan-text goldens, a temporary fourth carve-out until B3), S7 is ruled.

## S4: the tall row and the list node (the user, 2026-11-22)

- "Why do we even have 60 asserts in single statement? / Yes on list compare / ... Don't we just need 1 column per
  type that each assert reuses? Or one variant/json column" -> "Go". Tall rows (7809366a): a row per member, its number,
  one column per (position, SQL type, Pure type); the 60 cap gone (a dialect fact: SQLite's 500 compound terms). The
  list node (93307e6c): SqlFn SAME_IN_ORDER / SAME_IN_ANY_ORDER, DuckDB's list equality (probed on 1.5.6: -0.0, NaN,
  Decimal scale, duplicates, trailing spaces all agree with the portable form), every other dialect the portable form
  (now the dialect's, SameValuesForm), MongoDB refusing; Postgres portable until probed on a server. The ladder
  re-recorded once: every rung shorter (r01 838 -> 557, r12 3,540 -> 3,014).
- Coercion (the user: "I'm also open to rethinking type coercion equality if we think that 1 should equal 1.0"):
  answered keep S127 (Pure's interpreted answer); a mixed-kind side never reaches a database's list equality (DuckDB
  types [1, 2.5, 3] DECIMAL(11,1)[]); probes pin [1, 2.5, 3] vs [1.0, 2.5, 3] false on every judge. The query-vs-assert
  split of `==` noted as a separate question for later.
- Guard moves by the user's word: SqlFnSortedTest's PRODUCT (the two functions), COUNT/AND/EQUAL/NOT_EQUAL writers
  (SameValuesForm) -- "Yes, add both entries", "Yes, add it" (the classifier refused the edit until asked).

## The S4 step 3 + tall row + list node audit (2026-11-22), and what was done
- HIGH: an element's literal message over a seven-column row framed "at index n" by the database judge, bare on the
  host -> defer(..., callersMessage) leaves it unframed; probes (cross kinds, dates).
- MEDIUM: Postgres types a UNION column pairwise: bare NULL Decimal/TIME slots -> typed NULLs (nullOf).
- LOW 3: slot multiplicity [0..1]/[0..*] in the fused schema. LOW 4: the element scope a try-with-resources (the real
  error stands, the refusal suppressed). LOW 5 (recorded, not fixed): an element message that reads or raises over a
  seven-column row is refused by name on the database and agreement judges, planned on the host -- coverage differs
  until those families are typed. LOW 6: dead list/rows args on DuckDB's form, accepted.
- The fast round's guardrails: BigQuery.call / Postgres.call over 250 lines -> one shared null-safe arm (nullSafe) for
  six dialects; mutable fields -> the element message in the batch's Scope record, BodyRun's reader a final view of the
  sink's field.

## S4: the measurement, the gate fix, kinds, and dates (2026-11-22)
- Measured (a temporary print at the two deferral points, core's judge_database lanes and LITE-PCT): 2,175 typed vs
  2,963 seven-column; the typed gates asked for one kind in all where a rider lists its literal channel too, so every
  condition / tolerance / subset declined. Fixed (DatabaseJudge.oneKind, 2dbf3a5a): 3,764 typed vs 1,374.
- Kinds round (22a3900b WIP, then green): VerdictSql.typedClass is the one rule (Decimals of every precision, enums,
  cross number kinds the same only where both empty, S127); a single number's kind named by its declaration in the
  record (assertEq's parity); a Decimal LITERAL stays the seven-column row (SQLite keeps no Decimal scale).
- Dates: the user asked why dates are hard, then "Should we care about precision?", "What do you think unbiased?",
  "Let's do some homework upstream both pure and engine", "what is the normal/expert world view", "Is there maybe a
  totally different argument that this should always be string compare", "If the product does mainstream, is it really
  only literals that can differ? Can that be fixed at the edges", then "Okay sounds good let's do it". Upstream read
  (scratchpad/research/dates-upstream/): legend-pure's date equality is written-form (AbstractPureDate.java:30-59);
  legend-engine reads every TIMESTAMP at nine digits (73 precision-only expected failures in its relational PCT).
  RULED: register S399 -- the product compares a date by kind and instant (the database comparing; a TEMPORAL_TEXT
  literal parsed by the database, an hour's text read at its minute), the compatibility routes keep Pure's equality
  through the edge's HostForms (CompiledPureHostForms.equalDates), as S127; cites S139; a stated exception to the
  2026-11-03 never-more-lenient ruling. Superseded in the product: the orchestrator's ruling 3 of 2026-11-19 (an hour
  is not its minute).

## S4: tables (2026-11-22)
- assertTdsEquivalent typed (VerdictSql.typedTdsEquivalent, Family.TDS_EQUIVALENT): the verdict over typed cells as
  before, false where the columns differ; rows ride the failure as typed row structs (DuckDB LIST(STRUCT), H2/SQLite
  JSON), decoded by exec. The database judge's own "columns differ" sentence is gone (the host raised TDS_EQUIVALENT:
  the design memo's latent BOTH disagreement).
- The grid pair typed (VerdictSql.typedGridPair, Family.GRIDS): typed cells per grid by its own declared columns,
  the list node in order (sorted actual) or as a multiset of rows; a cross-kind column compared by its kind tag (S127,
  a CASE the database computes). SameValuesForm compares rows of several typed columns (a one-column side unchanged).
- Probes: AssertBarTest's five TDS probes; aTableComparisonIsOneOutcomeOnEveryDatabase (DuckDB, H2, SQLite x three
  judges: TDS equivalence, grid pairs in any order and sorted, names differ, cross kinds, assertNotEquals).
- Deleted: VerdictSql.gridTolerance and predicate(x3), VerdictWords/LiteVerdictWords.notEquivalent, HostJudgments.liteRows.
- Ledger shrinks: DatabaseJudge 928 -> 916, AssertVerdicts 1339 -> 1333, HostJudgments 663 -> 656.

## The dates slice's audit (2026-11-22, 7bb92a99..028b1553)
- HIGH 1 (fixed with tables): contains and the subset fold compared date-time texts; now instants on both sides; a
  written day read as the database's DATE.
- HIGH 2, 3, MEDIUM 4 (open, the dates round): declined date pairs keep written precision on the seven-column row
  (a Date-declared property's egress text; mixed Date lists); SQLite's cast keeps the written fraction; instants
  misreads a DATE column under DateTime, >6 subsecond digits (DuckDB truncates, H2 rounds, host nanos), short years.
- MEDIUM 5: S399's text overstated "never fails a program Pure passes" (assertNotEquals at one instant, different
  written precision: Pure passes, lite fails) and "any subsecond digits". Posted to the user with a proposed amendment
  (microsecond instants, digits past six truncated on both judges; SQLite's dialect normalizing its text); awaiting.
- LOW 6-7, NITs: date pairs skip the literal-channel check; the Decimal-literal exclusion is node-only; orphaned
  javadocs and a dead branch fixed with tables; LiteHostForms.equalDates' carrier fallback left for the dates round.

## Found while typing Decimal literals (2026-11-22)
- PRE-EXISTING (the seven-column row, not this batch): `assertEquals(1.50D, p::Q...->toOne().e)` (a Decimal literal
  against a store-read DECIMAL(10,2), whose root egress writes the D text carrier, DECIMAL_TEXT): the database judge's
  sentence "expected the elements 1.50 in any order, got -2" against the host's "[1.50] ... [-2]" -- BOTH disagrees
  on DuckDB, H2 and SQLite. The typed member declines the pair (VerdictSql.oneCarrier: a DECIMAL_TEXT side against a
  DECIMAL one); it needs a compare value (the text parsed by the database) beside the record's value (the carrier).

## Batch 2's audit (25d2c75c) and batch 3 (2026-11-22)
- HIGH 1 (fixed): emptiness by rows (noRows), SQLite's list of no rows being '[]'. HIGH 2 + MEDIUM 5 (fixed): SQLite's
  dialect writes a Decimal list element's text as its cell's (deliveredValue); exec's Java padding reverted. HIGH 3
  (fixed): carriers compared only where values are. LOW 3 (fixed): TIMESTAMPTZ an instant as it is.
- Open from it: MEDIUM 4 (a store-read Decimal column in a grid rides DECIMAL_TEXT: the record writes 1.5, the host
  1.50; toleranceCells' DOUBLE cast of '1.5D' fails), LOW 1 (instanceOf's undecided paths: the CONDITION record), LOW 2
  (Byte's name), LOW 3's 5-digit and BC years, LOW 4 (a Decimal literal against a store-read Decimal).
- Found, the product's own (one answer under every judge): SQLite's REAL keeps fifteen significant digits; DuckDB's and
  SQLite's list of Decimals is one scale ([1.0D, 10.125D] reads [1.000, 10.125]; H2 and Pure [1.0, 10.125]) -- a
  difference from Pure with no register row yet, to bring to the user.
- Typed: tagged values (typedTagged), Nil emptiness. Measured before batch 3: 4,970 typed vs 313 seven-column.

## The precision round (2026-11-22, after 1978e89d)
- The user's rule, 2026-11-22: "if something has nanos we either compare properly or refuse if cannot; otherwise
  compare with micro fidelity", then "do nanoseconds typed for whatever range allows, refuse if out of range". The
  microsecond cut 1978e89d wrote (host truncation, H2's DATE_TRUNC, SQLite's six digits, S399's "digit past the sixth
  not compared") was a session's inference, never ruled, against S181 ("refused, never cut"): withdrawn.
- Host judge: every digit (LiteHostForms.equalDates).
- Database judge, typed: a value the database types as a date-time is compared as it holds it (judgesAsHeld); H2 reads
  a text as TIMESTAMP(9); SQLite (no date-time type: its date-times are texts) one normalized nine-digit text, its
  fraction the digit run alone (a zone after it no digit).
- DuckDB, per assert: an assert whose values carry nanoseconds (a literal's, a fact of the plan: VerdictSql.nanos
  rewrites its JUDGE casts to the new reason JUDGE_NANOS) is read in TIMESTAMP_NS, typed, a value outside the days it
  holds (1677-09-22 to 2262-04-10, measured on 1.5.6) refused by name; any other assert to the microsecond in every
  year, a text with a digit past the sixth refused. Searched: no test in legend-pure, legend-engine or lite compares a
  date-time with nanoseconds outside those days; legend-pure's adjust PCT compares 1624, 2407, 25489 and -21457 without.
- A text with a nonzero digit past the ones a reading holds is refused by the database by name: one new raise kind,
  JUDGED_INSTANT_NOT_HELD (two forms: the digits; the days). A literal past its database's digits stays refused at
  planning (S181).
- A text comparison on DuckDB (a nine-digit canonical text, briefly in the working tree) was rejected by the user ("So
  we went back to string compare for timestamps?") and never landed.
- Open: ClickHouse reads a text as DateTime64(6) (measured at S9); S312's six-digit product text writes two values that
  differ only past the sixth digit alike in a failure's message (the verdict right; S312 is the user's to move); a body
  fusing a nanosecond assert with a date-time outside 1677-2262 in another assert meets DuckDB's own conversion error
  (the tall row's slot unifies to TIMESTAMP_NS).
- Probes: AssertBarTest.aDateTimeIsComparedAtEveryDigitItCarries (DuckDB, H2, SQLite x three judges);
  JudgedInstantTest (each reading on each database, its refusals by raise site).

## The audit of S8 (dacc5e3c) and batch 5 (435511a8), 2026-11-22
- No HIGH or MEDIUM. S8: 91 assertError bodies agree under all three judges on DuckDB, H2 and SQLite; batch 5's SQL
  byte-identical to before batch 4 for Pairs.
- LOW, recorded: lite raises nothing where Pure raises for toOne() over an empty instance list and 1->cast(@String),
  so assertError(..., 'No error was thrown') over them passes (product gaps the S8 change makes visible); the
  "No error was thrown at a line or column" refusal also covers a function that raises that text itself (contrived);
  more Pair disagreements on the seven-column row (H2: a Float or Boolean field; DuckDB vs SQLite: a Decimal's scale)
  -- the instances work's list; the unraised case is decided by one Java check for both judges (a fold of two
  constants, allowed); three stale javadocs; the guard row's label RULED where Q-S8 said STAYS (both final); an
  assertError expected text joined with + still folds in Java (S7's to refuse).
- Separate, pre-existing: CommonSubexpressions walks an identity map, so its SQL's order can differ run to run (G10).

## The precision round's audit (1978e89d + b7b00e31, 2026-11-22) and its fixes
- HIGH 1 (fixed): a written Date's kind was read from its text's LENGTH, so a year below 1000, of five digits or
  negative took another kind and compared equal. Now from its structure (a time after the day; else the dashes after
  the year's sign), a year and a year-month compared as integers of their components (a year-month its year times
  twelve plus its month), a mixed list's Date slot read the same way.
- HIGH 2 (fixed): SQLite's date functions answer NULL for a year outside 0000 to 9999, and two NULLs compared equal;
  such a date-time or day is read from its own text.
- HIGH 3 (fixed): a nanosecond assert's TIMESTAMP_NS value shared a fused column with another assert's year past 2262
  (DuckDB's own conversion error): a nanosecond member's columns are its own (VerdictSql.batch's Slot.nanos), and the
  records carry each side's values as it holds them. DuckDB's microsecond reading of a typed value is its TIMESTAMP,
  a nanosecond one with a digit past the sixth refused by name (it was compared as held, and DuckDB promoted the other
  value).
- MEDIUM 1 (fixed in part): a date-time literal with a nonzero tenth digit is refused at planning, under every judge
  (RootLiterals: the text carrier had passed it to a host that cut it). Open, S9: Postgres, MariaDB, BigQuery and
  Databricks read a date-time text with a cast to their microsecond type, ClickHouse to DateTime64(6).
- MEDIUM 2 (fixed in part): nanoseconds are read from a date-time literal alone (a typed one, or its TEMPORAL_TEXT
  carrier), never a String that spells one. Open: a stored TIMESTAMP_NS column's value with nanoseconds in an assert
  with no nanosecond literal is refused by name (DuckDB could compare it).
- MEDIUM 3 (fixed): the engine client's and Python's raise catalogs regenerated; their checks join the wide round.
- MEDIUM 4 (fixed but S312): the records carry the values as held, not the instants compared (a record named another
  precision than the host's; SQLite's Year printed as a Julian day). The six-digit text is S312, the user's.
- MEDIUM 5 (open, recorded in S399): a refusal at run is the database judge's; under both judges it is their
  disagreement, naming it.
- The per-assert nanosecond reading is the session's reading of the user's rule (posted 2026-11-22), not a ruling.
- Probes: the dates probe gains every body the audit measured (years 0999, 25489, -2020, year-months, wide date-times,
  the fused pair, a tenth digit, a String that spells a date-time, a minute against a nanosecond).

## Instances and Pairs typed (item 3's S4, 2026-11-22)
- VerdictSql.typedInstances: each value one row of typed leaves, as the host restricts it -- a keyed class its
  __type and its keys (a nested keyed instance's leaves in its place), a keyless class its __type and identity (S111,
  tracked or raised by name), a Pair its two values -- compared by the one list node, in order or as a multiset;
  each leaf read by its layout's type (H2 reads a JSON-carried field through its text; a date-time through the judge's
  instant). Refused (the seven-column row, for now): to-many keys, polymorphic/Any key slots, JSON fields, a keyless
  class with no identity, layouts whose leaf kinds differ.
- The record: a keyed instance restricted to its classifier and keys, as the host's (restrictOne); assertEq and
  assertIs name an instance by class and identity (RootLoop.Family.EQ, AssertRecords.notEq).
- The host's assertSameElements matches instances and maps as a multiset (sorted by text, two equal instances whose
  fields are one value written two ways were lined up apart); scalars keep upstream's sort.
- Measured (DuckDB lanes): 5,499 typed members, 108 seven-column (175 before).
- Probe: AssertBarTest.instancesAreComparedByTheirKeysOrTheirIdentity (keyed, nested keyed, keyless, Pairs, nested
  Pairs, multisets; H2 refuses a model class's construction today, under every judge -- a dialect gap).
