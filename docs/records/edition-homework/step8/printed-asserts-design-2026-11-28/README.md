# Step 8: a printed assert judged from the rows its own statement fetched (design, 2026-11-28)

*Written by the step 8 session after the clean-sheet record (`clean-sheet-2026-11-28/README.md`: the five tenets the
user accepted) and the step 7 research session's draft plan (`claude/slice-7-doors-research-bufdvi` at `7584fd8a`,
`docs/records/edition-homework/step-6b/slice-7-PLAN-2026-11-28.md`, section 7). A DESIGN: nothing here is built. Every
claim is marked **read** (in the code, file named), **run** (measured, how said) or **proposed**. It goes to the user
before any code is written. Section 10 is the independent review of this design, the same day, and what it
changed: sections 0, 2.2, 2.3, 3.3, 5 and 9 are as corrected by it.*

## 0. One page

**What a printed assert is.** A test compares a text upstream printed with a text the test prints:
`assertEquals('a,b\n1,2\n', $result.values->toCSV())`, or `assertEquals('#TDS\n   id,name\n   1,A', $t->sort(~id)->toString())`.
At a compatibility door the right side has to be upstream's text, written in Java from the typed rows (tenets 2 and 3).

**Today (read).** Before the body is planned, the harness hands it to `edge/UpstreamPrints.read`. For each printed side
it builds a second program (the lets the side reads, each `execute(...)` bound to a new let, the unprinted table as the
answer), runs it through the public test door on the body's session, prints the rows in Java, and puts the text back in
the body as a literal. The body then runs and compares two literals. So the table is computed twice, and about 250
lines of guards exist only to make the second reading safe (a body that writes, a clock, a draw, a pick from unordered
rows, a name a lambda binds).

**The design (proposed).** Nothing is read before the body.

1. **The door marks the side.** In the typed body it receives, the edge's test-body door replaces the print call of an
   `assertEquals` side with one call of a function lite owns, naming the print:
   `printedAs(<the unprinted table>, '<print>', [<literal replaces>], <rows in any order?>)`. The assert, its other
   side, its message and its place in the body stay as written.
2. **The test planner plans it as it plans any side.** The unprinted table is one more side, fetched as typed rows by
   the statement that fetches that side anyway. The plan carries a judgment that is plain data: which print, which
   replaces, in order or in any order.
3. **At run, Java prints and lite's judge compares.** The run hands the side's typed rows and columns to the edge it
   was given; the edge's one printer writes upstream's text; the judge compares it with the golden.

No second program, no second run, no guards about reading twice.

**Measured (run, section 3):** a temporary probe did exactly this for every printed `assertEquals` of the
relational corpus (DuckDB and H2) and of PCT's channel B (DuckDB, SQLite, H2): the unprinted table fetched as a side of
the assert's own statement, printed in Java, compared with the golden. Every printed assert the probe judged gave today's verdict (the ones it could not
judge are later asserts of a body it had stopped: 3.2), and all the tests that need upstream's text (the corpus's 18 on DuckDB and 28 on H2, PCT's date-time tables)
pass, with one exception: four PCT tests on H2, whose table does not plan as a side there (section 3.3).

**What is deleted:** `edge/UpstreamPrints` but for a small rewrite (about 520 lines to about 100), the `Reads` plumbing
in both harnesses and the test runner, and the guards of section 1.

## 1. Today, as read

| What | Where | Note |
|---|---|---|
| The harnesses call the pre-read | `spec/.../rcorpus/MinimalCorpus.java`:635, `pct/.../channelb/ChannelB.java`:419 | after `UpstreamTestBodies.admitted` / `admittedForPure`; each passes a `Reads` that runs a query through `TestExecution.runProgram` on the body's connection |
| A side is recognised | `edge/PrintedRelations.side` > `printed` | a relation's `toString()`, legend-engine's `toCSV` (three forms), a TDS's `csv` property, a Float's or a computed DateTime's `toString()`; then a tail of the closed set (`PrintTail`) |
| The second program | `UpstreamPrints.readBody`, `inlined`, `framesBound` | lets inlined, each `execute(...)` bound to a `liftedFrameN` let |
| The guards of a second reading | `UpstreamPrints.read` (`writes`), `readable`, `unrepeatable`, `readsByOrder`, `orderFree`, `answersAnew`, `BY_POSITION`; `PrintedRelations.mayBeASpelledDate` | about 250 lines; three of the second review's findings were holes in them |
| An unordered printed table | `UpstreamPrints.unordered`, `rows` | the `assertEquals` becomes `assertSameElements` of the two texts' lines; the golden's cells are cut by the printed lines' cell counts where a `replace('\n', ',')` joined the rows |
| The product's own recogniser of the same shape | `testplan/AssertVerdicts.renderForm`, `renderedPlan`, `HostJudge.planRendered`; `testexec/HostJudgments.renderedOf` | plans `HostJudgment.Rendered` with both sides as texts; since B3 step 2b its judge is `expected.equals(actual)`, and its `form`, `eForm`, `aForm` and `sortedOrder` are read by nothing at judging |
| How a side reaches the judge | `testexec/TestRun.hostSides` > `TestRunner.sides(verdict, List<PlanRunner.Rows>)` | each side arrives as exec's typed rows: `List<ExecutionPlan.Column>` and the cells. That is exactly what the edge's printer takes (`edge/PureRelationText.tds(columns, rows)`, `csv`, `tdsCsvProperty`; `PureValueText.value(cell, column)`) |
| The edge's hooks at run | `testexec/EdgeHearing` (`judged`, `hostForms`, ...), a ruled hook (`HookRulingTest`) | the product's `NoEdgeRun` refuses by name |

## 2. The design

### 2.1 The door (the edge; channel 3 of the step 7 plan's section 7)

`edge/UpstreamTestBodies.admitted` and `admittedForPure` already rewrite the typed body before it is planned. One more
rewrite there, replacing the harnesses' second call:

- **Where:** a statement that is an `assertEquals` whose one side is a golden text written as literals and whose other
  side, where it stands or through the let it is bound to, is a print the edge writes followed only by `replace` calls
  with literal arguments. Nowhere else: a print inside a lambda, under another call, or compared with a computed text
  stays as written and prints lite's text (tenet 3; register S434 and S435 say so).
- **What:** the printed side becomes `meta::legend::lite::test::printedAs(rows, print, replaces, anyOrder)`:
  - `rows`: the print's operand as written (it reads the body's own lets; nothing is inlined, no frame is rebound);
  - `print`: one of the printer's forms (`toString`, `toCSV`, `toCSV with TDSNull`, a TDS's `csv`);
  - `replaces`: the literal `replace` pairs, in the program's order;
  - `anyOrder`: whether the program defines no order for the rows (`compiler.spec.OrderView` INCIDENTAL, the fact the
    pre-read reads today).
- **A literal's print** (`134.21e6->toString()`) is written at the door as a literal, as today: it is a fact of the
  program and nothing runs for it.
- **Not carried over:** a computed Float's or DateTime's `toString()` as an assert's side read at run. No real test has one: the probe
  registered no such side in the corpus or in PCT's channel B (run, section 3). So it is not built (tenet 4), and its
  guard (`PrintedRelations.mayBeASpelledDate`) is not needed at this door. The query door keeps its own.

### 2.2 The plan (test planning; no door is asked)

- `printedAs` is one of the compiler's own natives (`builtin/Pure.COMPILER_OWN`, beside `cellText`, which is already
  "the compatibility edge's TDSNull"): program text cannot name it, only a door writes it. It has no lowering rule: it
  is a judged side, never SQL, refused by name where a test body is planned and where a query is lowered if it stands
  anywhere but as a side of an `assertEquals`. (The review, 10.3: the first draft put it beside `hasSameElements`,
  which is a function users call.)
- The host planner's `assertEquals` arm reads it first: sides `[the golden as a value, rows as a value]` (the same ask
  a table side of a grid assert gets today, `HostJudge.valueAsk`).
- **The judgment is the edge's own record, not a new kind in the product's plan** (the review, 10.2). The test door
  already holds the run's edge while it plans (`testplan/TestDoor`:142-148 asks it for each effect's and seed's
  records). One more ask of it: the print's name, its replaces and the order flag in, the edge's record out
  (`testexecutionplan/Extension`), planned as the existing `HostJudgment.EdgeJudged`. The product's plan types gain
  nothing and never hold upstream's vocabulary; the strings pass through the core unread. The first draft's
  `HostJudgment.Printed` is withdrawn.
- The database judge is not involved: compatibility runs the host judge alone (tenet 5 of the six). On lite's own door
  nothing writes `printedAs`, so lite's product path is untouched.

### 2.3 The run

- `TestRunner.sides` hands such a verdict's printed side to the run's edge as it arrived: exec's columns and typed
  cells. That needs a way past `EdgeHearing.judged`, whose sides are `TypedSide`s with columns that carry no slot and
  no precision (`testexecutionplan/Column`), which the printer needs. One new member on the ruled hook
  `testexec/EdgeHearing`: given the judgment, the golden and the rows, it
  answers the two things to compare: one text each, or each text's lines where the order is not defined (the cut of a
  joined golden by the printed lines' cell counts moves here from `UpstreamPrints.rows`, unchanged).
- Lite's judge compares them (two texts equal; two lists of lines the same elements) and words a failure as it words
  an `assertEquals` or an `assertSameElements` today.
- The product's `NoEdgeRun` refuses the member by name, as it refuses an edge-judged comparison.

### 2.4 What goes

| Deleted | Why it existed |
|---|---|
| `UpstreamPrints.read`, `substituted`, `print`, `readBody`, `framesBound`, `inlined`, `Reads`, `Printer` | the second program and its run |
| `UpstreamPrints.unrepeatable`, `readsByOrder`, `orderFree`, `answersAnew`, `BY_POSITION`, `inNoDefinedOrder`, `readable`, the `writes` gate | guards of a second reading |
| The `read(typed, facts, reads)` member of the harnesses' audience, and its implementations in `MinimalCorpus` and `ChannelB` | the plumbing that ran the second program |
| `UpstreamPrints.unordered`'s rewrite to `assertSameElements` | the judgment carries `anyOrder`; the assert keeps its own name |
| Most of `UpstreamPrintsTest` | tests of the guards |

What stays: `PrintedRelations.side`/`printed` (the one rule for "is this a print the edge writes", shared with the
query door's lift), `PureRelationText`, `PureValueText`, `PrintTail.Replace`.

### 2.5 What the query door keeps

A query whose whole answer is a print (PCT channel A, legend-engine's API) is unchanged: the door takes the final print
off, the query runs once, Java prints (`PrintedRelations.lift`). It already satisfies tenet 2.

## 3. Measured

### 3.1 The probe (run 2026-11-28, on `claude/edition-step8` at `263a9b59`; never committed)

The probe's source is beside this file (`probe-source.diff.txt`: three files). At the door, in place of the pre-read,
it replaced each `assertEquals(<golden literals>, <a relation's print with literal replaces>)` by an assert whose side
is the unprinted table (`assertContains(<table>, <a key>)`: a carrier that asks for the table with the planner's
ordinary value ask, `HostJudge.valueAsk`). Where the run handed that verdict its sides (`TestRunner.sides`), the probe
printed the table's rows with the edge's printer, applied the replaces, compared with the golden (lines in any order
where the program defines none, by the pre-read's own cut) and wrote the verdict to the log. The carrier assert itself
always fails, so a body stops at its first printed assert: the recordings the run rewrote are not evidence and were
discarded; the log lines are. Run with `bazel run //spec:update_edge_corpus` and `bazel run //pct:update_edge_contract`.

### 3.2 What it answered

| Suite | Printed asserts found | Judged | PASS | FAIL |
|---|---|---|---|---|
| Corpus, DuckDB | 146 (`toCSV` 143, `toString` 2, a TDS's `csv` 1; 41 with a `replace`; 133 in no defined order) | 144 | 123 | 21 |
| Corpus, H2 | 146 | 144 | 136 | 8 |
| PCT channel B, DuckDB, relation | 373 (`toString`, 6 in no defined order) | 365 | 365 | 0 |
| PCT channel B, DuckDB, standard | 58 | 58 | 58 | 0 |
| PCT channel B, SQLite, relation | 373 | 364 | 364 | 0 |
| PCT channel B, SQLite, standard | 58 | 58 | 58 | 0 |
| PCT channel B, H2, relation | 373 | 334 | 334 | 0 |

- **The corpus's FAILs are failures today.** All 21 (DuckDB) and 8 (H2) are calendar-aggregation asserts whose goldens
  hold another database's Float digits (`Lateral,9.267692307692307` against `9.26769230769231`; `0.383333333333`
  against `0.38333333333333336`). 17 on DuckDB and 4 on H2 are recorded FAIL in the baseline with the same two texts.
  The other four on each database are asserts the baseline never reaches: an `assertSameSQL` before them in the body
  fails first (checked for `testP12WaValue` and `testP52WaValue`; the other two by their goldens' family only).
- **The tests that need upstream's text pass.** Each of the 18 corpus tests that fail on DuckDB with lite's own text
  (clean-sheet record, 3.2: the unordered tables, `"name,firm\n\n"`, the `TDSNull` cell, the 12 validation tests)
  was found by its golden in the probe's log: PASS, on DuckDB and on H2. On H2 the only FAILs are the eight
  calendar asserts, so the ten more tests that fail there with lite's own text (its Float text) pass too. PCT's date-time tables
  (`2024-01-30T00:32:34.000+0000`) are among the 365.
- **Asserts not judged** are, in every case looked at, a later printed assert of a body the carrier had already
  stopped (two in the corpus on each database; 8, 9 and about 35 in PCT). Test by test, from each suite's log against
  its committed outcomes: on SQLite every test that passes in the baseline either has no printed assert (104 relation,
  147 standard) or reached its first one's verdict (364, 58). On DuckDB the counts close the same way (104 + 365 =
  469; 147 + 58 = 205). On H2: 102 + 334, and four that did not (3.3).
- **A printed value** (a computed Float's or DateTime's `toString()` as an assert's side): none registered in any suite.
- **Shapes met** (read in the pinned sources for the ones named): a table through a let (`$result.values->toCSV()`),
  through `toOne()` (`$result1.values->toOne()->toCSV()`), from a helper that runs the query (`validate(...)` bound by
  a let: today the pre-read runs that helper's query twice), a cast and a property
  (`$result.values->cast(@TDS<Any>).csv`), a sorted relation (`$res->sort(...)->toString()`).

### 3.3 The one gap: four PCT tests on H2

`project::testSimpleProject`, `testSimpleProjectList`, `testSimpleProjectWithEmpty` and
`composition::testFilterPostProject` pass on H2 today and fail under the probe with `DialectCapability: LIST_GET
reached a dialect without a list encoding`, raised while the side's statement is written (`JudgePlans.tabularPlanned` >
`rowsPlan` > `PlanMaker.sql`; the trace is in the session's scratchpad). Each projects a class instance written in the
test with a to-many property. Read: the side's query tree holds the instance's list as a list read by position over a
transform of a literal list, which H2 cannot write; the same table as a program's answer reaches H2's pass
`ConstantLateralsAsRows` as a constant list and passes (`Test_LegendLite_H2_RelationFunctions_PCT`, note of
2026-10-20). Binding the side's lets at the door did not change it (run). Lite's own text fails the same way there
(clean-sheet record 3.2; channel B's `ledger.tsv`, the note of 2026-11-24: these four pass only because the pre-read
reads the table as a program of its own). The review read further (10.6): `ConstantLateralsAsRows` rewrites only a lateral over a constant list, and a side is
inlined and staged by the test planner's own road (`BodyPlanner.inlinedSide`, `stagedSide` with the side's stamp),
which already binds every let, so the door experiment could not have told anything. **Why the two roads lower the
list differently is not established;** the review's inference is the side's staging or stamp, not H2. It is
the first thing the build looks at (a time-boxed slice 0); if it is not small it comes back to the user.

### 3.4 What is predicted to move when it is built

- Corpus recordings: the 111 asserts on DuckDB and 103 on H2 that are recorded as `assertSameElements` today (the
  pre-read's rewrite) are recorded as the `assertEquals` the test wrote, verdict unchanged. No verdict is predicted to
  move.
- PCT channel B: no outcome on DuckDB or SQLite; on H2 the four tests of 3.3 unless slice 0 closes them.
- Statements sent: one fewer per printed side (the corpus example of the clean-sheet record goes from 4 to 3).
  Predicted from that measurement, not re-measured under the probe.
- No product SQL, no lean-SQL pin.

## 4. Alternatives, and why not

| Alternative | Why not |
|---|---|
| Keep the pre-read, fix its guards | It computes the table twice (tenet 2), and its safety is a list of guards that three reviews have each found holes in |
| A second facet on the plan style, as the SQL-text assert has (`testplan/EngineTestText`) | It works, but it is planning code that asks the door, which steps 7, 9 and 10 remove; the step 7 plan says not to add one |
| A mark on the print call, as the step 7 plan proposes for comparisons | Needs slice 7's mark to exist and to be ruled; a function needs nothing new. If the mark lands first the door can write it instead: the planner's reading is one line either way |
| Replace the whole assert with a new assert function | The assert would lose its name in every recording, and its message and its negation would need their own handling |
| Parse the golden into a typed table and compare tables | It would test values where the test tests text; a golden is not always parseable back (a quoted cell, a date at a written precision) |
| Have the planner recognise the print itself (it already has `renderForm`) | The planner would branch on the door: a mode in the core (tenet 6 of the six) |

## 5. What it trips, and what needs the user's word

| Item | Guard or rule | Needs |
|---|---|---|
| A new member on `testexec/EdgeHearing` (run) and one on `testexecutionplan/Edge` (planning) | `HookRulingTest.RULED` (each needs the product's refusal listed), `HookRulingsAgreeTest`; the implementations are `NoEdgeRun`, `NoEdge` and the edge harness's `Appeals` and `CompatEdge` (`core/src/edge_harness`) | the user's ruling |
| A new compiler-own native `printedAs` | `Pure.COMPILER_OWN` and its ids; `INTERNAL_DESUGAR` holds 40 names against a ceiling of 40 (`NativeCatalogGovernanceTest`), so the ceiling moves or the native is not listed there; `everyLiteInternalNativeIsNamedAndVerified`; `LITE_INVENTION_CENSUS.md` gains a row | dated justifications |
| The edge's new record | the edge's codec (`EdgeCodec`), `NamedExtensions`' list of records | none beyond the code |
| Register rows S434 and S435 | they rule a Float's and a computed DateTime's `toString()` "at an assert's own argument" with a six-function tail, read before the body. The design drops both (no real test has one, 3.2) | amended rows, with open decision 3 below |
| The audience's `read` member | `core/src/main/.../test/Audience.java`:33-41 (and its product implementation), `PureTestRunner.java`:356-358, beside the two harnesses | deleted |
| Recordings | section 3.4 | named in the commit |
| The product's SQL size pins | none expected: no product SQL changes | - |
| Register | S131 (an unordered printed table's lines) reworded: the rule is unchanged, its place moves from the door to the judge | amended row |

## 6. Slices

1. **The judgment and the hook, unused.** `HostJudgment.Printed`, its codec, the `EdgeHearing` member, `NoEdgeRun`'s
   refusal, the edge's implementation over `PureRelationText`. Tests: the codec round trip; the edge's member on
   hand-built rows for each form, ordered and unordered, with and without a joining replace.
2. **The native and the planner's arm.** `printedAs` in the catalog; the `assertEquals` arm; the refusal elsewhere.
   Tests in core's edge tests: one body per form through the public test door with a recording edge.
3. **The door writes it; the pre-read goes.** `UpstreamTestBodies` rewrites; `UpstreamPrints` and the `Reads`
   plumbing deleted; both harnesses lose the second call. Re-record corpus and PCT channel B; the diff is read line by
   line against section 3's prediction.
4. **Independent audit** of slices 1 to 3 (the diff and the rules, never this document's conclusion), fixes, then the
   lanes on the final commit.

The product's vestigial `HostJudgment.Rendered` (section 1) can then be removed as its own small change: its judge is
plain text equality already.

## 7. The `TDSNull` row

Unchanged from `tdsnull-row-design-research.md` and its audit: an empty cell's word in a row's printed cells is what
legend-engine sends to the database, so it is a row of the doors' table (tenet 5), replacing the pass of the edge's
dialects (`edge/PurePrintForms`). It moves no answer. The step 7 plan (its section 6) puts it "with step 8" and notes
the one lowering arm that must read the call too (`Lowerer.scalarValueTailArms`). It is built after the printed assert,
on today's table lookup, with the audit's findings fixed first (the `"TDSNull"` literal must live at the edge, the
`SqlFn` census entries, the nullability facts).

## 8. Open, for the user

1. **Go or no go on this design.** It needs one ruling in particular: a new member on the ruled hook
   `testexec/EdgeHearing` (the run hands a printed side's typed rows to the edge).
2. **The four H2 tests (3.3).** Recommended: the build starts by finding why the side lowers that list differently
   (time-boxed). If the fix is not small: either they are recorded as H2 failures until the side road is fixed (a
   ledger row, the user's word), or the build waits.
3. **Still open from the clean-sheet record,** and assumed here: only `replace` follows a compared print (the six
   other follow-up functions, `+`, and the `format` lift serve no real test and are dropped); the `trim` fix's scope.

## 9. Not verified

- Any server or cloud database: the probe ran on DuckDB, H2 and SQLite.
- PCT's essential, grammar and unclassified suites hold no printed relation assert, so the probe says nothing about
  them; the one essential test that needs upstream's text prints a literal, which the door writes as today.
- The helper-calling tests (167 in the corpus by the lexical census): the design does not reach into a helper's body,
  and neither does the pre-read (**read**: `UpstreamPrints.substituted` does not follow a user call). Not run.
- The two unjudged corpus asserts and the unjudged later asserts in PCT: judged only when the real thing is built.
- The cause of 3.3.
- That nothing else is tripped: section 5 is from reading and from the review; the audit of the build will find more.
- **What the probe could not show** (the review, 10.5): its carrier was `assertContains`, so the `assertEquals` arm
  itself, an assert with a message, a negated one, the refusal of an unordered `assertEquals`
  (`OrderedAsserts`) and the plan's codec were never exercised; it compared with its own code, not the judge's; it
  ignored the pre-read's gate on a body that writes (such a body prints lite's text today, and would print upstream's
  under the design: an answer that could move, not measured); and it checked that passing asserts stay passing, not
  that every failing one stays failing.
- **Positions the design no longer covers.** Today the pre-read replaces a relation's print wherever it stands outside
  a lambda (under `assert(...)`, `assertEq`, `assertNotEquals`, two prints compared). The design covers an
  `assertEquals` against a literal golden alone. Measured indirectly: on SQLite and H2 no PCT test that passes in the
  baseline failed under the probe for any reason but the carrier (and the four of 3.3), and in the corpus no assert
  line outside a carrier's body moved, so no test of those suites needs upstream's text at another position. DuckDB's
  PCT log carries no reasons and closes by counts only.
- The sizes in 2.4 are estimates.

## 10. The independent review (2026-11-28) and what it changed

A reviewer given this design, the tenets and the code (not the author's reasoning) checked each claim against the
files named. Its findings, and what was done with each:

| # | Finding | Done |
|---|---|---|
| 10.1 | The design narrows today's behaviour (the positions covered; the value prints S434 and S435 rule) while saying nothing moves, and the probe measured only `assertEquals` against a literal golden | Section 9 says what is no longer covered and what the measurement does and does not support; section 5 lists S434 and S435 |
| 10.2 | A new `HostJudgment.Printed` would put upstream's print names into the product's closed plan type, reversing the move that took the engine-text judgment out of it (census C3); `EdgeJudged` with the edge's own record is the closer precedent. `EdgeHearing.judged` takes sides without slot or precision, so a raw-rows path is needed either way | Adopted: section 2.2 and 2.3 |
| 10.3 | The native's precedent was wrong (`hasSameElements` is user surface); the compiler-own natives are the fit, and a product query needs its own refusal; the catalog's ceiling of 40 is full | Adopted: section 2.2 and 5 |
| 10.4 | Section 5 understated what a new judgment record trips (three exhaustive switches, the `permits` clause, a round-trip test, a shrink-only pin on `HostJudgments.java`) | Moot with 10.2: no new record. The hook's guards are listed in section 5 |
| 10.5 | Section 0 claimed more than the probe shows | Section 0 reworded; section 9 lists what the probe could not show |
| 10.6 | A print bound by a let is underspecified: change the let, or inline at the assert? | Decided here: the let is left as written and the assert's side becomes `printedAs` over the let's operand, read in place; other readers of the let still get lite's text. To be confirmed by a test in slice 3 |
| 10.7 | Two details of the plumbing were wrong (where the audience's `read` lives; the corpus reads through `planBody`/`runBody`), and `Rendered.form` is still read in a refusal's message | Section 5 corrected; section 1's "read by nothing at judging" holds for `eForm`, `aForm` and `sortedOrder`, and `form` is read by a message alone |
| 10.8 | The four H2 tests: what can and cannot be established by reading | Section 3.3 |

Checked by the review and found as stated: the harness call sites; the forms `PrintedRelations` recognises; the
printer's signatures; that compatibility runs the host judge alone; that `valueAsk` is the grid pair's ask; that
nothing on the side's road reorders or regroups the rows relative to the pre-read (no batched cell for an assert with
a literal golden; `ScanOrder.stabilize` touches only joins with no ORDER BY; `inBranchOrder` only an unsorted union);
that `OrderedAsserts` will not refuse a `printedAs` typed `String[1]`; that the lean-SQL pins exclude verdict steps.
