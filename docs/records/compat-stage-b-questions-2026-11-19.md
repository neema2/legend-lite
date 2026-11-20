# Stage B: the open questions, for the user (2026-11-19)

The user asked (~21:25 UTC): "put the questions together but put them in context and with pros/cons and implications
and alternatives". These are the decisions stage B needs before its builders start, gathered from the final plan
(`compat-final-plan-2026-11-19.md` §5, the "Open before it is built" column), the log, and the sessions. Nothing here
reopens a ruling already made: B4's removals (D1, D2, CP-8), the table-text rulings, the switch rule and the error
records reading stand as ruled.

**Stage B in one paragraph.** Stage A makes lite's core mean Pure and puts a detector at legend-engine's door. Stage B
finishes moving everything legend-engine and legend-pure specific out of the core and into the edges:
- **B1:** errors become plain records that compatibility words in legend-pure's or legend-engine's sentences.
- **B3:** legend-pure-style test checking moves behind three narrow connections.
- **B2:** compatibility's formatters print upstream's tables and values in Java from lite's typed rows, replacing about
  2,000 lines of SQL that build upstream's text.
- **B4:** removes the last legend-engine quirks.
- **B5:** deletes the switch machinery and turns on the guards that keep the core clean.

The prototypes size it at about 6 weeks for one builder, or 4 to 5 with B1 and B3 built side by side.

Each question has the context, the choices, what each choice implies, and a recommendation.

---

## Part 1: how stage B runs

### Q1. The order, and how many builders at once

**Context.** B1 (errors) and B3 (checking) both reshape the same record, so they can be built side by side, and either
may land first. B2 (formatters) uses the connections B3 builds, so it comes after B3. B4 and B5 are cleanup at the end.

| Choice | Time | Pros | Cons |
|---|---|---|---|
| **A. B1 and B3 side by side, then B2, then B4 and B5** | 4 to 5 weeks | the shortest that is safe; the two builders touch different files (errors vs. judges) | two branches meeting on one record shape; the second to land re-measures |
| B. One builder, in order | about 6 weeks | no merge work between steps | a week or more longer |
| C. Everything at once | maybe 3 weeks | fastest on paper | B2 rewrites what B3 is still building; constant conflicts and re-measurement, which is what happened when work ran in parallel on the same files before |

**Recommendation: A.** The builders would be:
- **B1:** the error-records spike session, which already built H13.
- **B3:** the judges session.
- **B2:** the formatter spike session, which built H11, with B2's census.
- **B4 and B5:** B2's census session, which prototyped the ratchet.

### Q2. How stage B lands: how many PRs

**Context.** Stage A lands as one batch, at your request, to save CI runs and merge words. Stage B is 4 to 5 weeks of
work, about one and a half to two times all of stage A (11 to 15 working days).

| Choice | CI runs and merge words | Pros | Cons |
|---|---|---|---|
| **A. One PR per step, with B4 and B5 together (4 PRs)** | 4 | each PR is reviewable; value lands every week or so; a red CI holds one step, not all of B | 4 CI runs and 4 merge words |
| B. Two batches (B1 + B3, then B2 + B4 + B5) | 2 | fewer runs | each batch is 2 to 3 weeks of work, too big to review well; a problem in one step holds the others |
| C. One batch for all of B | 1 | one run, one word | 4 to 5 weeks unmerged while `main` moves; the review covers several thousand changed lines (B1 alone is +1,450 / −880; B2 deletes about 2,000); the risk the 2026-10-26 landing showed |

**Recommendation: A.** Each PR still gets the reviewer and one CI run, and is merged only on your word.

---

## Part 2: B1, errors as records

### Q3. Typed raise slots: should the database send back the values a message needs?

**Context.** When a program fails at run time, lite's database raises the error. Examples are dividing by zero,
`toOne()` on a list of 3, or an index past the end. legend-pure's message often includes a value:
- "Cannot divide 5 by zero";
- the size of the list that was not exactly one;
- the bad index and the list's size.

In B1, compatibility writes legend-pure's sentence in Java from the error's record. The record holds the error's kind,
its place, and the program's own constants. What it cannot hold today is a value the database computed from the data,
like a column's value that turned out to be zero.

- **Without slots:** every message the tests pin today is already exact. H13 measured it on every lane, partly because
  a list's size is worked out from its type. A message whose value comes from row data prints a placeholder
  (`<computed>`) where legend-pure prints the number. No test pins one of those today. The placeholder is recorded as a
  difference.
- **With slots:** the plan declares, for each such error, the position and Pure type of each value it needs, but never
  the value itself. The database prints the values into the error it raises, in an exact text per type. Lite reads them
  back by their declared types, and compatibility writes legend-pure's sentence with the real numbers.
  The placeholder, its register row and the size-from-type pieces (`statedSize`) are all deleted. Under tenet 3 this is
  sound: the database computes the value, and Java only re-spells it.

| Choice | Size | Pros | Cons |
|---|---|---|---|
| A. Build it inside B1 | B1 grows from 6.5–7 days to 10–11 | one step; B1 is complete | B1 becomes the long pole (longer than B3's ~8 days); a bigger PR |
| **B. Build it right after B1, as its own PR** | 0.5–1 day measuring, then 3.5–4 days (+570 / −30 lines) | B1 lands sooner and smaller; the slots are additive (B1's records already carry each value's type, `{"computed":"Integer"}`); measured before it is built | one more PR and CI run; the placeholder lives a few weeks longer |
| C. Never | none | nothing to build | legend-pure users see `<computed>` instead of a number in some messages, for good, recorded as a difference |

**What gets measured first (either A or B):**
- each database's limit on error text, across 12 raise writers;
- Snowflake's and ClickHouse's quoting;
- whether SQLite's Float text round-trips exactly;
- MongoDB's raise;
- how much lite's SQL grows, since every plan with such an error carries the slots.

**Recommendation: B.** It is the same end state, but the first PR is smaller and lands sooner, and nothing is lost by
waiting.

---

## Part 3: B3, checking

### Q4. Should a "too many values" error become catchable by `assertError`?

**Context.** A Pure test can say "this should fail" with `assertError`. When a value's count breaks its declared
multiplicity at run time (two values where the type says one), legend-pure raises an ordinary execution error, which
`assertError` catches. Lite raises an internal `IllegalStateException` instead, which `assertError` does not catch. B3
turns it into an ordinary data error, built the same way as every other raise.

| Choice | Pros | Cons |
|---|---|---|
| **A. Yes, catchable** | matches legend-pure; one kind of run-time error everywhere; measured to change 0 tests on all of PCT and both corpus lanes | a behaviour change in the product: a user's `assertError` around such a failure would now pass instead of crashing (no known case) |
| B. Keep the internal error | no change | stays different from legend-pure for no reason; an internal error is the wrong kind for a data problem |

**Recommendation: A.**

One known leftover in B3 needs no decision. The rewrite of legend-engine's S131 test bodies fixes 21 of 22 DuckDB tests
and 3 of 4 H2 tests. The one left on each has a JSON value at the root, which no rewrite reaches. It stays recorded.

---

## Part 4: B2, formatters

### Q5. The window-in-a-list bug: start its fix now?

**Context.** This is a product bug that silently returns wrong rows on SQLite and Postgres. A query takes a window
value (for example, a running string join over a group) and feeds it into a list operation (sort, split, chunk). Lite's
lowering gets it right. A later SQL clean-up pass (`SelectMerge`) then merges the subquery back and puts the window
inside SQLite's and Postgres's list subqueries. There, each window sees one row, so SQLite answers each row's own name
('A', 'E', 'C') instead of the group's ('AE', 'CG'). DuckDB is right because it writes list functions in place.

B2 cannot finish until this is fixed: H11's printed tables hit it.

The fix was paused at 13:45 in a product-track session, "Lowering: a window column read through a list operation"
(https://claude.ai/code/session_0195rQEjkEZDfj2GjXCAhyJr, branch `claude/window-list-lowering`). Its test reproduces
the bug, and its fix is designed but not written. The planned fix: each database's SQL writer declares which operations
it writes as a subquery of its own, and the clean-up pass then refuses to merge a window, aggregate or `UNNEST` into
one. Only SQLite and Postgres declare any, so every other database's SQL is unchanged.

The session left two questions open:
- **(a)** The root is in the SQL clean-up pass (`sql/dialect/SelectMerge`), not in the lowering its brief named. Is it
  fixed there?
- **(b)** One of the two PCT cases (the `chunk` one) cannot pass on SQLite anyway: SQLite refuses it first for an
  unrelated reason (a non-literal `regexp_extract_all` pattern). Does that refusal stay?

| Choice | Pros | Cons |
|---|---|---|
| **A. Take it over now and fix it in parallel with stage A's batch; (a) fix it where the root is; (b) keep SQLite's refusal** | wrong answers on two databases fixed sooner, whatever happens to stage B; B2 unblocked before it starts; the files (`SelectMerge` and two dialect declarations) are not ones the batch is changing | one more branch running now; it may move the lean-SQL baseline (numbers to you before any pin moves) |
| B. Wait until stage B starts | nothing runs in parallel with the batch | wrong rows on SQLite and Postgres for longer; B2's start waits on it |
| C. Fix it in the lowering instead | matches the old brief | the lowering is already right; the fault would just be worked around, and the clean-up pass would keep the same trap for the next case |

Your 14:30 word ("take over the paused pieces we need") already covers taking it over. The only question is **when**.
Keeping SQLite's refusal in (b) is right: it is loud, not wrong, and changing what SQLite can do is a separate product
item.

**Recommendation: A, start now.**

### Q6. The checked graph's default violation message: whose sentence?

**Context.** A class can have constraints. When a constraint without its own message fails in a checked graph fetch,
the product today writes legend-engine's sentence, `Constraint :[id] violated in the Class Name`
(`resolver/GraphEmission.java:653`). That is upstream's spelling living in the core.

| Choice | Pros | Cons |
|---|---|---|
| **A. Lite gets its own sentence; compatibility writes legend-engine's from the record** | the clean split every other message follows: the product's words are lite's, and upstream's are the edge's; lite can later improve its wording without breaking compatibility | lite's own users see a new sentence (a small change; no known dependency); about half a day inside B2 |
| B. Adopt legend-engine's sentence as lite's own, and record that | nothing to build | upstream text stays in the core under a new label; if either side ever changes, the other silently follows or breaks |

**Recommendation: A.** I'd keep lite's sentence close to today's, for example
`Constraint 'id' of class Name is violated`. The exact words are yours to choose.

### Q7. The checked graph's "defects" cell: build it in B2?

**Context.** When a checked graph fetch finds violations, legend-engine returns a "defects" cell worded by its own
presenter. 12 DuckDB corpus tests plan one, and lite answers none of them today. Wording it in the compatibility
formatter is about 120 lines, 1 to 1.5 days.

| Choice | Pros | Cons |
|---|---|---|
| **A. In B2** | 12 corpus tests gained; B2 already builds the presenter it needs | B2 grows by 1 to 1.5 days (to 7 to 9.5) |
| B. Later, its own small PR | B2 a little smaller | the 12 tests wait; a separate CI run |

**Recommendation: A.**

### Q8. May a test's plan name the program's own `toString`/`toCSV` call?

**Context.** You ruled at ~18:12 that, at the program's last step, the legend-pure route takes a printed table over:
the database returns typed rows, and Java prints legend-pure's text. For that, a test's plan must say "this side of
the assert is the program's own `toString` (or `toCSV`) of a relation". It would say only that, never any text. Each
audience's run then spells it: the product's in the database, compatibility's in Java. The program's definition of done
(done-when 3) says the plan carries no presentation, so it needs one sentence amended:

> "a plan may name the program's own presentation call at a side's root (toString, toCSV, as Rendered names its form
> today); how an audience spells it is the run's".

22 corpus `assertSameElements` sides need it.

| Choice | Pros | Cons |
|---|---|---|
| **A. Amend done-when 3 as above** | the plan states a fact of the program's typed tree (like the sort order it already carries), and no text; the ~18:12 ruling can be built as ruled | one more fact in the plan's vocabulary |
| B. Keep it out of the plan; the edge re-reads the program to find the call | done-when 3 unchanged | the edge walks the program a second time (a second road, against G1); the 22 sides need their own detection |

**Recommendation: A.**

---

## Part 5: B5, switches

### Q9. legend-engine's per-query feature flags: refuse the legacy null-equality flag, or honour it?

**Context.** A legend-engine request can carry feature flags. Lite mirrors legend-engine's five, and today lite's
lowering reads two of them, which is a mode in the core that B5 must remove. The plan for each, from the differences
page:

| Flag | What it does in legend-engine | Plan |
|---|---|---|
| corrected substring indexing | Pure's `substring` meaning | read at the door; always in effect after A3 |
| enum push-down | enum decoding in SQL | accepted; changes nothing (lite always decodes in SQL) |
| two never implemented | — | refused by name, as today |
| **legacy null-unsafe equality** | `==` written as plain SQL `=`, so a null never equals a null | **the open question** |

7 corpus tests set a flag. One needs the legacy flag, and it checks SQL text only.

| Choice | Pros | Cons |
|---|---|---|
| **A. Refuse it by name at the door** | lite follows Pure (a null equals a null); loud, never a silent different answer; costs nothing; the core's reader is deleted in B5 | a legend-engine user who sets it gets a refusal; the 1 corpus test stays failing (recorded) |
| B. Honour it with a door rewrite, as `engine` does for `substring` (a lite-internal "SQL `=` as given" function) | that user keeps legend-engine's answer | about 1 day; one more lite-internal function; a second "upstream's answer" path to keep tested for a flag no known user sets |

**Recommendation: A.** Build B only if a real user asks; the pattern is proven by `engine`, so it is a day's work
then.

### Q10. When does the `LEGEND_LITE_PURE_MEANING` switch expire?

**Context.** Your switch rule (~20:35) says every switch carries a removal date, and the build fails past it until the
switch is removed or you re-date it. The first switch, `LEGEND_LITE_PURE_MEANING`, needs one:
- `refuse` is the default;
- `warn` is an operator's choice;
- `engine` gives legend-engine's `substring`/`indexOf` answer.

B5 builds the registry, so the date is needed by then.

| Choice | Pros | Cons |
|---|---|---|
| **A. A fixed date, six months out (2027-05-19), re-datable on your word** | the rule as written; a forcing function to find out whether anyone still needs `engine` | someone must look at it in May |
| B. A shorter date (three months) | less time living with a second meaning | probably too soon for deployed projects to move |
| C. No date (permanent) | nothing to revisit | breaks your own rule's "every switch has a removal date" on its first entry |

**Recommendation: A.**

---

## Part 6: one meaning decision that is ready now

### Q11. Comparing values of different kinds (`$t.id == '1'` where `id` is an Integer)

**Context.** Pure says two values of different kinds (an Integer and a String, a date and a String) are never equal.
legend-engine lets the database convert one side and compare, so `1 == '1'` can be true there. The answer then depends
on each database's conversion rules. Lite today does what legend-engine does: it leaves it to the database. Under the
goal you ruled ("the core means Pure's definitions"), that is a Pure meaning lite does not yet follow.

This was measured at 17:55. Every case in the corpus is legend-engine's own test code comparing two kinds:
- `$t.id == '1'` with `Trade.id: Integer`;
- `$i.id == 1` with `Interaction.id: String`;
- a date compared with `'2014-12-03'`;
- 8 cross-store graph fetches joining a String key to an Integer key.

None is a comparison lite builds itself.

| Choice | Corpus tests that move | Pros | Cons |
|---|---|---|---|
| A. Keep today's (the database converts) | 0 | no change; matches legend-engine | the core does not mean Pure here; answers differ by database |
| B. Pure everywhere, including a mapping's own `###Relational` SQL `=` | 26 H2, 30 DuckDB | one rule | wrong for mappings: a `###Relational` `=` is SQL by definition, so it breaks mapping semantics (a lite differential test breaks too) |
| **C. Scoped: Pure code follows Pure (never equal, decided by the types alone, a fold your fold ruling allows); a mapping's `###Relational` `=` keeps the database's** | 11 H2 (+1 fixed), 15 DuckDB | the core means Pure; mappings keep SQL meaning; PCT and lite's own suites green | 26 legend-engine tests move away from legend-engine's answer, each recorded |

**The implication of C:** it is the same kind of difference as `substring` (D1: "loud"), so it should be treated the
same way:
- the detector flags these comparisons at legend-engine's door: refused by default, warned under `warn`;
- `engine` gives the database's comparison.

The detector already finds them by type, so I expect this to be small, but it is not sized yet.

**Recommendation: C, loud at the door like `substring`, built in stage B with B4's removals.** It would be sized first
and its moved tests listed by name before it lands.

---

## Part 7: tooling

### Q12. Fix legend-engine's test runner for tests with no database?

**Context.** Our rule is to ask legend-engine's own runner for upstream's answer rather than guess. Today the runner
cannot run any test that reads from no database. legend-engine compiles such a test to Java, and a jar it needs
(legend-engine's plan dependencies, `org.finos.legend.engine.plan.dependencies.store.shared`) is missing from the
runner's download pool. The translator hit this tonight on `substring`'s error text and answered it from legend-pure's
source instead.

| Choice | Pros | Cons |
|---|---|---|
| **A. A small separate PR right after stage A merges: add the jar to the runner's pool** | we can ask legend-engine directly for any function's answer, as the fidelity tenet wants | a build dependency change (`MODULE.bazel` and its lock), so CI on every OS; download-pool changes have broken CI before (the Maven 404s) |
| B. Leave it; read the sources when needed | nothing to change | slower and less certain answers whenever a question involves no database |

**Recommendation: A.**

### Q13. `engine` on more databases than H2 and DuckDB?

**Context.** The `engine` opt-in is being built for H2 and DuckDB first. On every other database it refuses by name.
Nobody has measured legend-engine's answers on Snowflake, Postgres, Databricks and the rest.

| Choice | Pros | Cons |
|---|---|---|
| **A. On demand: add a database when a user needs it, measured first** | no work for databases nobody has asked for; each addition is a line plus its measurement | a user on another database gets a refusal until then |
| B. Add the rest in stage B | complete coverage | days of measurement on databases we cannot run here (Snowflake, Databricks and others need accounts) |

**Recommendation: A.**

---

### Q14. Run legend-engine's corpus in `engine` mode? (added ~22:05, from the user's question)

**Context.** The rule of thumb: run each suite in the mode its authors run it in.
- **legend-engine runs its relational corpus with its `substring` correction off.** One of its tests is named
  `testToSQLStringSubstringIsUncorrectedByDefault`. So `engine` mode is the corpus's own setting.
- **legend-engine runs PCT with the correction on:** its testable runner turns it on, as lite's PCT adapter did until
  A3 piece 2 (`platform/Feature.java`'s record). So PCT stays in Pure's meaning, and is not part of this question.

Today the corpus lanes run lite's own meaning against legend-engine's expected answers, so A3 piece 2 adds 7 recorded
failures per database.

| Choice | Pros | Cons |
|---|---|---|
| **A. The corpus lanes switch to `engine` mode, as part of B3** | the 7 per database pass again; the `engine` opt-in is tested on the whole corpus, not only the batteries' rows and the route tests; no extra lane, so no extra CI time | the corpus stops showing lite's default meaning against legend-engine's on those queries (still named in the register and on the differences page; PCT tests lite's default against Pure's spec); a `substring` in a model-to-model chain or a `~func` mapping is refused under `engine`, so a few tests may move to named refusals (measured first) |
| B. Both modes, two sets of lanes | both measurements | doubles the corpus's CI time |
| C. Keep today's mode | nothing to build | the opt-in is tested only narrowly; the 7 per database stay recorded failures |

**Why not in stage A:** the corpus runs whole test programs through lite's test runner. That runner has no seam
between typing a test and running it, so the rewrite has nowhere to sit. Adding one now would be a hook. B3 gives the
corpus's own door its prepare and run halves, and this rewrite sits there beside the four corpus rewrites.

**Recommendation: A, in B3, measured first** (the tests that move, by name, before the lanes switch).

### Q15. Assert-failure words: lite's own on every route? (added ~02:40, from the user's B3 question; measured)

**Context.** When an assert fails, the verdict carries a sentence, such as legend-pure's "expected: 0 / actual: 1" or
lite's "expected 0, got 1". B3 planned a presenter that writes legend-pure's assert sentences for the compatibility
audience.

**Measured** (H13 §26, `7e6b7bed`):
- No PCT test expects legend-pure's assert-failure text (channel A 0, the ENGINE lanes 0).
- On channel B, 11 already-failing lines change their message, and 1 verdict moves (DuckDB `testDateFromHour`, the
  known judge-parity bug, moot in compatibility).
- The corpus moves 0 verdicts. Only lite's own harness data reads the words: 69 ledger message lines and 41
  accepted-divergence witnesses re-pin.
- No upstream suite, golden or client reads them.

| Choice | Pros | Cons |
|---|---|---|
| **A. Lite's own assert words everywhere** | B3 deletes the presenter and the two-form plumbing (about −640 lines) and builds none (about 0.5 to 1 day instead of 2.5); one set of words; consistent with Q3's option 1 | re-pins of lite's own harness data (41 witness rows, 69 ledger lines, 11 PCT lines) |
| B. legend-pure's assert words for compatibility, as planned | nothing re-pinned | about +300 lines and 2.5 days for words nothing upstream reads |

**Recommendation: A.**

### Q16. B3's design: option N, no compatibility judging in planning (added ~03:30, the user's idea; measured)

**Context.** The user asked whether the SQL-text fallback, which is test-harness code, could be plain Java at the edge
with no planning (~02:15).

**The spike** (H13 §27, `be7d569a`), measured on the H2 lane:
- **The query is public.** Lite's rows for a SQL-text assert's query come through public doors, with no hook: the
  query is taken from the assert and run after the test body on its own session. 1,060 reads.
- **The golden rows come from the harness's own referee.** legend-engine's H2 mirror answered 603 of the 634 asserts
  that match today. Of the other 31, 23 need facts the harness can supply, 7 are shapes the public `from()` can't
  spell, and 1 is unexplained.
- **The run-time rules stay.** The ones that must apply at run time (S15, S144, S131) already ride the edge's existing
  typed hand-off.

| | B3 as planned | **Option N** |
|---|---|---|
| Compatibility judging in planning | narrowed to three connections | **none** |
| Core main code deleted | the hooks narrowed | **3,449 lines** (the appeal's planning: SqlTextVerdicts, Appeals, EngineTextPlanning and the rest; the referee moves to the harness) |
| Size | about 8 days | about 7.5 to 9 days |
| New core hooks | none | none |

**Two decisions before it is built:**
- **(a) The corpus keeps its recorder and its referee mirror** (legend-engine's own H2 2.1.214).
  - Replaying the golden on lite's own session instead would make the referee less independent (MATCH 745 instead of
    995).
  - **Recommendation: keep them.**
- **(b) The 7 shapes `from()` can't spell** (4 runtimes with post-processors, 3 inline mappings).
  - The choices are a harness query shape, or recording them as declines.
  - **Recommendation: recorded declines**, 7 tests.

**A half-day follow-up first:** the probe's six shapes reach 632 of today's 1,146 row matches. The other 514 come
through helper functions and other shapes. Measuring them is the one number that could make the detection larger
than estimated.

**Recommendation:** adopt option N for B3's SQL-text half, with (a) keep and (b) recorded declines, after the
half-day follow-up.

## The questions at a glance

| # | Question | Recommendation |
|---|---|---|
| 1 | Order and builders | B1 and B3 side by side, then B2, then B4 and B5 (4 to 5 weeks) |
| 2 | How it lands | 4 PRs (B1, B3, B2, B4 with B5) |
| 3 | Typed raise slots | right after B1, as its own PR, measured first |
| 4 | "Too many values" catchable by `assertError` | yes |
| 5 | The window-in-a-list bug | take it over and start now; fix it in the SQL clean-up pass; keep SQLite's refusal |
| 6 | The default violation message | lite's own sentence; compatibility writes legend-engine's |
| 7 | The "defects" cell | in B2 |
| 8 | The plan names the program's own `toString`/`toCSV` | amend done-when 3 |
| 9 | The legacy null-equality flag | refuse by name; rewrite only if a user asks |
| 10 | `LEGEND_LITE_PURE_MEANING`'s removal date | 2027-05-19, re-datable |
| 11 | Comparing different kinds | scoped: Pure code follows Pure, mappings keep SQL; loud at the door; in stage B |
| 12 | legend-engine's runner, no-database tests | a small PR right after stage A |
| 13 | `engine` on more databases | on demand |
| 14 | The corpus in `engine` mode | yes, as part of B3, measured first; PCT stays in Pure's meaning |
| 15 | Assert-failure words | lite's own everywhere (no upstream reader; B3 about 2 days smaller) |
| 16 | B3's design | option N: no compatibility judging in planning, 3,449 lines out of core main, about the same size; keep the recorder and mirror; the 7 unspellable shapes as recorded declines; a half-day reach check first |

Q5 and Q12 can start before stage B is ratified. Q5 is a product bug fix, and your 14:30 word covers taking it over.
Q12 is tooling, right after stage A. The rest take effect when you ratify stage B.
