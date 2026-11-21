# Item 3: assert verdicts as records, and the judges' rules — the homework

Written 2026-11-21 so the work can be done fast and right. It comes after BE (#131) and is the first slice of B3. The
user ruled the rules in §1; everything else is design, for the user's word where §8 says so. The evidence is read from
the code at `dad8a710` and from the pinned upstream sources. It lives beside this file:
- `compat-assert-records-homework/code-map.md`: lite's code, every site with file:line and its size.
- `compat-assert-records-homework/upstream.md`: legend-pure's and legend-engine's assert behaviour, each claim cited.

## 1. The rules (ruled 2026-11-21, "Yes exactly!")

1. **The database judge runs on lite's flow only.** No compatibility code ever reaches it: no legend-pure equality, no
   upstream words. Compatibility runs (PCT under Pure's meaning, the corpus under legend-engine's) use the host judge.
2. **The host judge on the product path:** the database computes every value the program uses. Java compares them and
   prints the failure report.
3. **The host judge on the compatibility path** may print a value's text in Java where the printing is the LAST step.
   The closed list: `toString`, `toRepresentation`, `format` with a literal pattern, and `joinStrings` of printed values.
4. **Java never creates a value the program reads.**

Plus **Q15 = A** (the user, 2026-11-20): lite's own assert-failure words on every route, so there is one catalog,
lite's.

## 2. What "done" means

- **One catalog words every assert failure.** The catalog is lite's `AssertKind`, a fourth family of the one error
  model: `ErrorCode` permits `CompileKind`, `ErrorKind`, `FrameKind` and `AssertKind`. It replaces about 145 places
  that word an assert failure today (code map F.3).
- **Assert failures are records in the plans.** A test plan carries an assert failure as `{kind, values}`, never a
  sentence. Neither plan carries an assert sentence: not in a record component, and not as a SQL literal.
- **The database judge** returns the verdict and the compared values. It builds no sentence and no `__message`, and
  words nothing.
- **The host judge** builds the same record from the values it holds. The two judges agree by construction:
  `IsolatedRun` compares records, not texts.
- **The rules are mechanical.** A guard fails the build when any of §1's rules is broken (§6).
- **What is deleted:** about 1,070 whole-file lines, plus about 650 lines inside `VerdictSql` and other fragments
  (code map F.1).

## 3. What the homework found (it changes the draft)

1. **Rule 1 is already enforced at the door.** Since A4 (2026-11-19), `TestPlanner.refuseTheDatabaseJudgeInCompatibility`
   (`TestPlanner.java:73-80`) refuses the database judge, alone or under BOTH, for any style but the product's.
   `HostOnlyCompatibilityTest` guards it. Every compatibility road is host-only (code map B.4).
   - So the draft's measurements 4.1 and 4.2 (rows only the database judge passes; lane time under the host alone)
     would measure zero. They are dropped.
   - What remains of rule 1 is dead code and one question (§8, Q-A).
2. **`UpstreamVerdictWords`' 20 SQL builders are unreachable.** Its two String methods (`conditionFailed`,
   `valuesDiffer`) are live, through the host judge's planner (`AssertVerdicts.java:1061` and `:1429-1431`). That is
   how legend-pure's "Assert failed" and "\nexpected: …\nactual:   …" reach compatibility test plans, inside
   `HostJudgment.Decided.message` and `AllRows.message`.
3. **The words have two halves.**
   - **Planned:** `Failure.message`, `AllRows.message`, `Decided.message` and `Messaged.unplanned`.
   - **At run:** the host's `HostWords` (a Lite form and an Edge form) and the database row's `__message` column.
   - Under Q15 = A both halves go to one catalog. `EdgeHostWords` (233 lines) and `EdgePureWords` (154) go with them.
4. **Words in the product plan too.** An in-body `assert` lowers to a raise whose text is "Assert failed"
   (`LambdaBodies.java:142` hard-codes it for six guards on both roads, and `LoweringForms.assertFailed` has two forms).
   The draft missed this.
5. **Rule-4 candidates.**
   - `Literals.fold` joins `+` and `joinStrings` of literals in Java at planning. Its results:
     - a caller's message (`DatabaseJudge.plannedMessage`);
     - an `assertError`'s expected text, which is compared (`AssertErrorNative.args`);
     - a judged constant side (`BodyDriver.foldedSide`);
     - effect and seed arguments.
   - `HostJudgment.RowArg.Literal` round-trips a literal through Java text.
   - `Verdicts.constantValue` parses a constant cell in Java (code map C.3).
6. **The database verdict statement today.**
   - **Its shape:** one fused `UNION ALL` per segment per session. Its columns: `(__ix, __verdict, __expected, __actual,
     __unjudged, __lenient, __holds, __message)`.
   - **The compared values are texts**, in legend-pure's literal spelling (`'a'`, `1.0D`, `%2020-01-01`,
     `<id instanceOf T>`; `LiteralSpelling`, `CanonicalRenderSql`). The verdict is `IS NOT DISTINCT FROM` over those
     texts. The equality rules that differed left with A4. The spelling stayed as lite's equality proxy.
   - **Its size:** the ladder's 12 statements carry `__message` today, 62,945 bytes against the 28,866 bytes of their
     lean targets.
7. **Upstream's facts that shape the design** (upstream.md):
   - **One native, lazy message.** Every assert but `assertError` ends in `assert(Boolean[1], {->String[1]})`. The
     message lambda runs only on failure, but `format`'s arguments are evaluated eagerly, even on a pass.
   - **The words.** The equality frame is `'\nexpected: %r\nactual:   %r'`, with a leading newline and three spaces
     after `actual:`. `%r` is `toRepresentation`. `assertEquals` uses `%r` when both sides have one value, else the
     `[..]` list form.
   - **The comparison.** `equal` is order-sensitive over collections. Integer 1 ≠ Float 1.0. Decimal never equals
     Float. Compiled Decimal equality is scale-sensitive. Date equality is precision-sensitive.
   - **PCT matches expected failures by substring**, never exactly. The relational corpus's runners compare no
     messages at all.
   - **`assertError` when nothing is raised** compares the expected text against "No error was thrown". It passes if
     they are equal, and otherwise fails with "Execution error message mismatch…". Lite fails with "No error was
     thrown" in every case: a verdict difference in two edge cases (Q-B).

## 4. The target design

**An assert failure is an `ErrorRecord`** over an `AssertKind`: `AssertSentence`'s 24 rows, each a kind with its
values. Its values are the report's texts, printed in Java by lite's value printer (`LiteWords`) from typed values
after the run. Printing a report is the host judge's role under rule 2, and the same printer serves the database
judge's values. `ErrorRecord`'s values stay `List<String>`.

**The two judges:**

| | Host judge | Database judge |
|---|---|---|
| Decides | in Java, over typed values the database computed | in SQL, inside each verdict member, over typed values (Q-A) |
| Returns | `AssertFailed(record)` | `__verdict` plus the two compared values carried for the report (Q-A), no `__message` |
| Words | the catalog, after the run | the catalog, after the run, from the same printer |

**Decided at planning.** `HostJudgment.Decided` becomes `{kind, held, values}`. `AllRows` becomes `{kind, the caller's
message as a literal or none}`. `Messaged.unplanned` goes: a caller's message the plan cannot evaluate refuses the body
at planning, by D.

**In-body asserts** become a run-time raise kind (`ErrorKind.ASSERT_FAILED`, its operand the caller's message or none),
worded by the catalog. No "Assert failed" literal stays in the product plan.

**Test plan v38** (code map D.1):
- `TestPlan.words` is dropped: a plan no longer depends on its audience.
- The prose components become records.
- `TestPlan.Verdict` gains the step's kind and each value's source: a literal, or a column of the verdict row.
- The verdict row loses `__message`.
- `productVersion` stays 60 unless a node's shape changes. Only Java reads test plans.

## 5. The order of work, each slice green and audited

| # | Slice | What | Done when |
|---|---|---|---|
| S0 | Dead code out | delete `UpstreamVerdictWords`' SQL builders, `DatabaseAsserts.errorVerdictFor` (no caller), the `VerdictWords` two-form entry (`PlanStyleFacets.TWO_FORMS`, `PureText.twoForm`), `VerdictSql.Message`'s 5-argument constructor that defaults to legend-pure's words (an invariant-4 default) | nothing behaves differently; the guards in §6 R1 hold |
| S1 | The catalog | `AssertKind` from `AssertSentence`; `AssertFailed` carries an `ErrorRecord`; `LiteHostWords` becomes the catalog's renderer; `Failures.of` and `rebuilt` read the record | one sentence function, covered by a table test over every kind |
| S2 | The host judge | every host verdict raises `AssertFailed(record)`; `HostWords`' two forms and `EdgeHostWords`/`EdgePureWords` go (Q15 = A); the compatibility ledgers re-record (§7, M2) | core and every compatibility lane green, the moved rows listed by name |
| S3 | Planned decisions | `Decided`, `AllRows` and `Messaged` as in §4 | the planned half carries no prose |
| S4 | The database judge | each member decides over typed values (Q-A); the fused row carries the verdict and the two values for the report; `VerdictWords`, `LiteVerdictWords`, `LiteText` and VerdictSql's sentence members go; `RootLoops` builds the record | core's judge_database and judge_both lanes and the LITE-PCT lane green; `IsolatedRun` compares records; the ladder re-pinned leaner (M1) |
| S5 | Codec and guards | test plan v38; `PlanTypesAreDataTest` drops `AllRows.message`, `Decided.message`, `Messaged.unplanned` and `TestPlan.words`, and narrows `Failure.message` to the edge's appeal records (B3); the new guards in §6 | the guards fail on a planted violation |
| S6 | In-body asserts | the raise kind; `LambdaBodies`' hard-coded literal and `LoweringForms.assertFailed`'s two forms go | the product plan carries no "Assert failed" |
| S7 | Rule 4 | `Literals.fold`'s four uses, per Q-D | the fold computes no value a program reads |
| S8 | `assertError` | the no-error edge cases, per Q-B | verdicts match upstream's |
| S9 | Re-pins and audit | JavaEvalLedgerTest's pins (`AssertVerdicts`, `DatabaseJudge`, `HostJudgments`, …), the 12 ladder `*.current.sql`, the PCT judge-SQL ratchets, the lean baselines, the moved ledger rows; then an independent audit and the PR | the PR's pins table, each move dated |

S0 can start before the rulings in §8. S1 to S3 need only Q15 = A, already ruled. S4 waits for Q-A, S7 for Q-D and S8
for Q-B.

## 6. The guards that make the rules mechanical

- **R1, the database judge on lite's flow only.**
  - `HostOnlyCompatibilityTest` already exists.
  - Add an ArchUnit rule: the database judge's classes (`lowering.VerdictSql`, `testplan.DatabaseJudge`,
    `DatabaseAsserts`, `VerdictBatch`, `testexec.DatabaseVerdicts`, `RootLoops`) depend on no `com.legend.edge` class
    and no `PlanStyleFacets` two-form entry.
- **R2 and R4, Java prints a report and never a value the program reads.** A census in `JavaEvalLedgerTest`'s style
  pins every Java site in `testplan`, `testexec` and `testexecutionplan` that makes a text from a value
  (`String.valueOf`, `toString`, `format`, `Double.toString`, string concatenation of values). Each site is allowed
  only if it is the catalog's renderer or the value printer the renderer calls. The census shrinks only.
- **R3, the compatibility edge prints only as the last step.** The edge's printing entry points are an enumerated list
  (`toString`, `toRepresentation`, `format` with a literal pattern, `joinStrings`). A guard pins the list and refuses a
  printed text whose result feeds a further operation. This guard is B2's, named here so item 3 does not invent a
  second mechanism.
- **No prose.** `PlanTypesAreDataTest`'s residue shrinks by three, plus `Failure.message` narrowed. A new check: no
  catalog template's fixed text appears in any planned statement (the BE audit 5's M3 gap: a SQL literal is a fact to
  the record guard).
- **One catalog.** A test asserts that every road's assert failure equals `AssertKind.sentence(record)`, on the host,
  the database and isolated BOTH runs.

## 7. Measurements, before building, for the user's word

| # | What | How | Decides |
|---|---|---|---|
| M1 | The verdict statement's size: today, against typed members with no sentence | prototype three ladder statements (a scalar equals, a collection, a table) under Q-A's options; compare with `core/src/test/resources/ladder/*.current.sql` and `*.lean.sql` | Q-A; the SQL must get leaner |
| M2 | Every assert-failure text that moves under Q15 = A | after S2, diff the compatibility ledgers. The code map counts: channel B 52 rows plus 5 outcome rows; the corpus host-judge files, about 50 rows each on DuckDB and H2. Channel A does not move (upstream's runner produces it) | the re-record, listed by name |
| M3 | How many bodies refuse once `Messaged.unplanned` goes | count, on the PCT and corpus lanes, the asserts planned as `Messaged` with an `unplanned` text | S3's cost, like D's |
| M4 | How often `Literals.fold` fires per use | instrument the fold for one run of PCT and the corpus, per caller (message, assertError expected, judged side, seed) | Q-D's cost |
| M5 | `assertError`'s edge cases | grep PCT and the corpus for an expected text of exactly "No error was thrown", and for matcher forms that check nothing | Q-B's reach |
| M6 | Run time and judge SQL | the PCT judge-SQL ratchets on the LITE-PCT lane, and core's judge_database lanes' time, before and after | no regression |

## 8. Decisions for the user

**Q-A, how the database judge compares.** Today it compares texts in legend-pure's literal spelling. That is a
spelling, not one of the equality rules A4 removed, but it is legend-pure's on lite's own flow, which rule 1 rules
out.
- **Recommended:** decide inside each verdict member over typed values (`IS NOT DISTINCT FROM` on the typed
  expressions, a count, a range, or `ABS(e - a) <= t`). Carry the two values to the report as text in lite's own
  spelling (the database's cast, spelled per dialect), because a fused `UNION ALL` cannot carry differently typed
  values in one column.
- **Where typed equality is not available** (a collection or a table on a database without list equality, as H2's
  gaps show), the member compares lite's own canonical text of the value, never legend-pure's. A database that can do
  neither refuses that assert by name.
- M1 measures it.

**Q-B, `assertError` when nothing is raised.**
- **Recommended:** match upstream's verdicts exactly. Compare the expected text against "No error was thrown", which
  passes when equal, and let a matcher run on that text. The failure is worded by lite's catalog. It is cheap, and it
  removes a verdict difference. M5 sizes it.

**Q-C, in-body asserts in the product plan.**
- **Recommended:** a raise kind, `ASSERT_FAILED`, worded by the catalog. The words change from "Assert failed" to
  lite's "expected true, got false" on the product road, under Q15 = A. A product user then sees lite's words.

**Q-D, `Literals.fold` (rule 4).** Java joins literal strings at planning, and the program reads the result. The world
map says the unroll compares spelled tokens and never computes a value.
- **Recommended:** the database computes it. The caller's message and a judged constant side become expressions of
  the verdict statement. An `assertError`'s expected text becomes a column the host reads. The fold then joins nothing
  a program reads.
- The alternative: keep folding only a single literal, with no join, and refuse a joined literal message by name.
  M4 decides between them.

## 9. Risks

- **Verdicts can move when the database judge compares typed values instead of texts.** Number kinds, date
  precision, and collection order all matter, so core's judge_database and judge_both lanes and the LITE-PCT lane are
  the proof. A moved verdict comes back by name, never pinned silently.
- **A fused statement's union needs uniform column types.** Q-A's text carriage exists for this reason. Typed values
  stay inside each member.
- **The re-record volume under Q15 = A.** About 160 ledger rows move to lite's words (M2). Every one is a word change,
  not a verdict change. Any verdict change is a defect.
- **The `Messaged` refusal cost (M3).** If it is large, the alternative is to plan the caller's message as a column of
  the verdict statement, which the database computes. That is Q-D's design, and it may make the refusal unnecessary.
- **Scope creep into B3.** The appeal path's prose (`Appeal.contract`, `Appeal.Unlisted.message`,
  `EngineTextJudgment.decline`, the edge's prose `Failure`s) stays B3's. Item 3 narrows the residue and does not touch
  them.
