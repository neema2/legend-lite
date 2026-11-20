# Stage B, end to end: the plan and its open questions (2026-11-20)

Written for the user's question of ~10:35 UTC: "do we have an end to end holistic plan for B? What is it clearly and in
detail in plain English. Do we have outstanding questions we need to answer? If yes what are they in detail with
pros/cons, cost/implications/ramifications, and good alternatives."

It replaces `compat-stage-b-questions-2026-11-19.md` as the current list. That file stays as the history of each
question. Sizes come from the prototypes; nothing here is built.

---

## 1. Where stage A leaves us

**Done in stage A (#128):**
- Lite's core means Pure for `substring`, `substr` and `indexOf`.
- legend-engine's SQL printers are deleted.
- Lite's two judges agree on meaning.
- legend-pure-style checking has left the database judge.
- legend-engine's door always gives legend-engine's answer, on H2 and DuckDB. Edge code swaps whole calls in the
  compiled query between type-check and planning. On other databases the door refuses by name.

**Still upstream-specific inside the core.** Stage B removes all five:

| # | What | Where it lives today | Size |
|---|---|---|---|
| 1 | **Error sentences.** legend-pure's and legend-engine's wording of run-time errors | written by the database dialects (`PureMessages`, `SiteWords`, `RaisedWords`, 12 `raiseMessage` overrides) | about 1,000 lines |
| 2 | **Test checking for legend-engine's SQL-text asserts.** The "appeal" that re-runs a failed SQL-text assert as rows | planned inside lite's test planner (`SqlTextVerdicts`, `Appeals`, `EngineTextPlanning`, the referee ledger in `CompatEdge`) | 3,449 lines |
| 3 | **legend-pure's printed text.** Tables, values and Floats printed the way legend-pure prints them | built in SQL by the dialects (`PurePrintForms`, the `PURE_V1` style passed into planning) | about 2,000 lines |
| 4 | **Leftover legend-engine quirks** | the two number shortcuts; H2's legacy settings; `isNumeric`/`parseDecimal`/`parseJson`; `EngineWhereZones`, which orders WHERE conditions as legend-engine's SQL text does; DataCube's workarounds | scattered |
| 5 | **legend-engine's per-query feature flags** | read inside the core | 49 parameters, 50 calls |

In all: 142 places of upstream-specific code in the core.

## 2. The end state: what "stage B done" means

1. **The core holds no upstream-specific code, no mode and no switch.**
   - The 142 places go to 0, and so do the 49 parameters and 50 calls.
   - Guards keep it at 0: ArchUnit rules and ratchets that fail the build if any of it comes back.
2. **All compatibility lives at three edges, and depends on the product, never the reverse (G5):**
   - **The door (input).** It reads legend-engine's and legend-pure's requests. It may swap or remove whole calls in
     the compiled query, at the public type-check→plan boundary. It refuses by name what it can't serve.
   - **The formatters (output).** After the run, from lite's typed results, they write upstream's text: result
     envelopes, printed tables and values, and error sentences.
   - **The harness (tests).** It runs legend-engine's corpus and PCT through lite's public doors. Compatibility's
     verdicts come from typed rows. The SQL-text fallback and its referee (legend-engine's own H2) live here, not in
     the core.
3. **The measures:**
   - **PCT under lite's own rules closes up to the compatibility numbers.** Today on DuckDB, channel B relation is
     100/469, mostly a relation's `toString`, which lite's route refuses.
   - **The corpus loses nothing.** Its 7 `substring` tests per database come back, because the corpus runs at
     legend-engine's meaning.
   - **Every remaining difference is a register row the user ruled.**

## 3. The steps

**Order (the user, 2026-11-20 ~23:55: "we need to do the 'one error' work first"):** B1 (PR #130), then **BE, the
one error model**, stacked on B1, then B3, then B2 and B4 side by side, then B5. B6 runs throughout, in the
warm-database sessions. This session builds each step alone; tests and audits run in subagents and in the warm
sessions, the server and cloud databases among them (~17:00, ~23:40).
- *Until 2026-11-20 ~23:55 the order was B1 and B3 side by side; superseded.*
- The prototypes size the whole at 4 to 5 weeks with B1 and B3 in parallel.
- Running B2 and B4 in parallel too saves about a week. B4 touches number handling and quirks; B2 touches printing.

### B1. Errors become records (about 2 to 5 days, depending on Q3)

- **What changes:**
  - A run-time error becomes a plain record: its kind, its place in the program, and the program's own literal values.
  - Lite's own sentence is fixed at planning and carried as data. The product only serializes it.
  - **Only the compatibility edge writes upstream's sentences**, in Java, from the record (rule 3).
- **What it deletes from the core:** `PureMessages` (about 445 of its 481 lines), `SiteWords` and its recorder,
  `RaisedWords`, the site and error words in `PlanStyle`, the 12 `raiseMessage` overrides, and the size-words plumbing.
- **Two defects it fixes:**
  - The legend-engine route answers some errors with a bare code (`Raised: DATE_COMPONENT_INVALID`) instead of a
    sentence.
  - A missing store is worded with lite's sentence where the code claims legend-engine's.
- **Proven by:** the H13 prototype. Every pinned message is identical on every lane, the corpus rosters are unchanged,
  plan JSON grows +0.03%, and lite's SQL gets smaller.
- **Decision needed:** Q3.
- **Status (2026-11-20 ~23:55):** built and twice independently reviewed; PR #130, its gates running in warm sessions.
  The bullets above are the plan as of ~16:40; B1 as built follows the ruling of ~17:00: the plan carries the error's
  facts and no words, lite's sentence included, and `PureMessages` stays at the edge as legend-pure's wording.
  What B1 leaves for BE: a planned failure with no kind still carries lite's sentence in the test plan
  (`Failure.message`), and a kinded one lite's wrapper text ("in function 'a::f': ").

### BE. One error model (stacked on B1, before B3; about 2 to 3 days)

The user's question of ~21:00 ("what is the expert way to deal with errors holistically"), the model proposed then, its
place ruled ~23:55 ("we need to do the 'one error' work first").
- **What changes:**
  - **One record for every error:** a code (its kind), its typed values, where it happened, and the context it passed
    through (the function, the inlining: today lite's wrapper text) as frames. No record holds prose: not the plan, not
    the test plan, not an exception's fields.
  - **Words only at the last step, from a catalog per audience:** lite's sentences (`ErrorKind`, `CompileKind`) and
    legend-pure's/legend-engine's (`edge/PureMessages`). The hosts' tables are generated from the catalogs.
  - **Errors render themselves** (the ~20:30 (d)): the run passes the facts; the error type words itself through the
    catalog.
- **What it folds in:**
  - about 540 compiler sites that raise a plain English string get a kind and typed values (measured 2026-11-20:
    543 string sites, against about 30 kinded ones over 20 `CompileKind`s);
  - the 13 refusal-fragment sites the B1 audit named (MariaDb, SQLite, ClickHouse, AnsiSqlRenderer, DateCtorRule,
    MixedEncoding) (~20:30 (a));
  - `Failure.message` and the wrapper text in the test plan become the record;
  - the six shapes `PureMessages.compile` walks become one; the marked-message residue (a raise's carriage off its
    record, answered with the kind's name, pinned by `PureMessagesWordedTest`) goes;
  - `RaisedErrors.dataError`'s product sentence.
- **Upstream's words:** each new kind's legend-pure/legend-engine sentence read from the pinned sources. A kind with no
  upstream sentence maps explicitly to lite's sentence in the compatibility catalog, as a ledgered row (no fallback).
  S391 (a query accessor over a missing store) is a candidate fix here: the reference's origin becomes a frame.
- **Proven by:** every pinned message unchanged unless listed by name for the user; a guard that the plan's and the
  test plan's JSON hold no prose.

### B3. Test checking moves out of the planner ("option N", about 7.5 to 9 days; after BE)

Written down in full 2026-11-20 ~23:55 at the user's ask ("write all the B3 stuff down so we dont forget"). Every
decision below is a ruling of the program log (`compat-program-log-2026-11-19.md`), its time given.
- **What changes:**
  - **Lite runs every test body as lite.** The test planner plans nothing for compatibility.
  - **The judge has ONE rulebook, lite's** (E1 withdrawn, ~21:00). No compatibility code runs during the run:
    compatibility changes the program only at the door, before planning (E0 = A, ~19:45), and formats output after the
    run. The verdict is lite's judge's, as for lite's own tests.
  - **The SQL-text fallback becomes harness code.** When a legend-engine SQL-text assert fails, the harness runs the
    assert's query through lite's public `from()` door, after the body, on its own session, and compares the rows with
    legend-engine's golden rows from the referee (legend-engine's own H2 2.1.214). The corpus keeps its recorder and
    the referee's mirror (Q16(a), ~03:45).
  - **Lite's test runner is split into two public halves**, prepare and run. The corpus harness composes: prepare →
    the door's input rewrites → run. The corpus runs at legend-engine's meaning (`engine` mode, Q14, ~03:45); PCT stays
    in Pure's meaning.
  - **The input rewrites, before planning** (the rule, ~21:00: never put an upstream bug into lite as a function):
    - **S137**: a call swap to plain Pure. legend-engine counts a relation as ONE value in `assertSize`, `assertEmpty`
      and `assertNotEmpty`, so the door rewrites them to the plain-Pure form of that meaning (`assertSize($rel, n)` to
      `assertEquals($n, 1)`, and so on); lite's own runs keep refusing the three over a relation by name.
    - **S144**: lite stays strict JSON. The compatibility door cleans the two goldens' expected JSON literals (a stray
      trailing quote that json-simple 1.1.1 tolerates) before planning. No test lost.
  - **The leniencies lite added are removed: S15 (Floats within 2 ULP), S132 (its widening to every judge and to
    rendered text) and S131 (an unsorted actual compared as a multiset).** Upstream compares Floats exactly and lists in
    order (`EqualityUtilities.java:70-71, 165-174`). Ruled ~20:30 on the condition that no test is lost, and ~21:00
    that each test they carried is placed against legend-engine's own answer on that database first:
    1. switch each off on both corpus lanes (H6 measured: 0 PCT verdicts move; the corpus moves DuckDB 69 tests --
       S131 38 decisions, S137 21, S132 14, S15 2, S144 2 -- and H2 29 -- S137 21, S131 4, S132 3, S144 2);
    2. ask the oracle (the engine runner, legend-engine 4.145.0) for legend-engine's own answer to each moved test on
       that database;
    3. class each: agrees with lite / float printing (B2 may fix it) / a real difference;
    4. bring the counts and the names to the user before anything is deleted.
  - **Q15 = A** (~19:45): lite's own assert-failure words on every route, provided no test's verdict moves; any
    verdict that moves comes back by name.
  - **Q16b** (~19:45): reclaim all 7 shapes the public `from()` cannot spell today, and report if and why any cannot:
    - the 4 post-processor runtimes by **R1** (~17:00): `runtimeWithTableReplace`, legend-engine's post-processor
      runtime, applied on the product door, built inside B3;
    - the 3 inline mappings through the public door.
  - **Q16c = A** (~19:45): a named harness rule takes `assertEqualsH2Compatible`'s upgraded-H2 branch (the one lite's
    H2 2.5 and the referee's goldens are written for); if that does not work, the helper as a native.
  - **`assertError` catches the run's size refusals:** "too many values" (Q4, ~03:45) and "no rows" (~20:30 (c)).
- **What it deletes from the core:** 3,449 lines (`SqlTextVerdicts`, `Appeals`, `EngineTextPlanning`, the referee
  ledger and seed copies in `CompatEdge`, the S137 two-form), the appeal plumbing, and the S15/S131/S132 judging rules
  once their tests are placed.
- **Reach, measured on H2 (H13 §27–28):** the public door rebuilds 986 of today's 1,053 row matches (93.6%). Of the 67
  left:
  - 15 are plan-text asserts, routed to the referee's plan check;
  - 7 are shapes `from()` can't spell (Q16b: all 7 to be reclaimed);
  - 20 are other shapes, to read;
  - 32 are to read one by one.
  - Nothing ships until each is placed.
- **Decisions:** all ruled (E0, E1, Q15, Q16b, Q16c, R1, Q4, Q14, the leniencies). What comes back to the user: the
  leniencies' placed counts before deletion; any verdict Q15 moves; any of the 7 that cannot be reclaimed.
- *Superseded (kept so the history reads): until ~21:00 B3 had compatibility's judge receive each assert's two sides at
  run time (E1), and four test-body rewrites (S15, S131, S137, the door's swaps).*

### B2. Upstream's printed text in Java (about 6 to 8 days)

**What changes:**
- **legend-pure's printed tables and values** are written in Java from lite's typed rows, replacing about 2,000 lines of
  SQL that build them. The H11 prototype was byte-identical on DuckDB (431 of 431), H2 (342 of 342), SQLite (419 of 420)
  and Postgres (427 of 430).
- **At legend-engine's or legend-pure's door,** the edge removes a final `toString()`/`toCSV()` from the query and hands
  the run a table printer as its output receiver.
  - The receiver is a value passed with that one request; nothing is stored between requests.
  - The database returns typed rows, and the printer writes legend-pure's text.
- **In a test body,** the plan names "this side is the program's own `toString` of a relation" (Q8). Compatibility's run
  prints it in Java; the product's run prints it in the database.
- **Text used inside a query** (a `toString` whose result the query keeps working with) is computed by the database as
  lite's own text, and the difference is recorded.
- **Lite's own route** prints a relation's `toString`/`toCSV` in the database, replacing today's refusal. The product
  track builds this beside B2: H14 reaches 390 of PCT's 423 on DuckDB.
- **An empty cell reads `null`** (ruled ~18:36).

**What it deletes from the core:** about 2,000 lines of SQL text-building, `PurePrintForms`, and the `PURE_V1` style's
print forms.

**Decisions needed:** Q6, Q7, Q8.

### B4. Remove the last legend-engine quirks (a few days, plus Q11)

**What changes:**
- **H2's legacy settings go** (1 corpus test).
- **The two number shortcuts go** (D2, ruled). 9 legend-engine answers stop matching, and 10 PCT and 2 corpus tests are
  gained.
- **legend-engine's `isNumeric`, `parseDecimal` and `parseJson` quirks go** (D1, ruled): `parseDecimal` −2 per
  database, `parseJson` −1 DuckDB and +1 H2, `isNumeric` 0.
- **`EngineWhereZones` goes** (measured first). It orders WHERE conditions only to match legend-engine's SQL text,
  which stage A stopped promising.
- **DataCube's legend-engine workarounds move to DataCube's legend-engine side.**
- **Every moved test is listed by name before it lands.**

**Decisions needed:** Q9, Q11.

### B5. Delete the switch machinery; turn the guards on (a few days)

**What changes:**
- **legend-engine's feature flags stop being read in the core**, so the 49 parameters and 50 calls go to 0. The door
  reads the flags it honours, and refuses the rest by name.
- **The switch registry stays as a guard.** It is now empty (its one entry was deleted at ~07:55), with its ceiling of
  5, its reader test and its removal dates. Nothing in the core may read a switch.
- **The lasting guards switch on:**
  - the 142 places stay at 0 (ratchet);
  - the edge reaches the product only through public types and doors (ArchUnit);
  - no dependency runs from the product to the edge.

**Decisions needed:** E0, which sets what the guards enforce.

### B6. legend-engine's answer on the other databases (alongside, in the warm sessions)

- **What changes:** each other database (Postgres, SQLite, MariaDB, MySQL, ClickHouse, Trino, Snowflake, Databricks,
  BigQuery, MongoDB) gets checked. When lite's rewritten answer matches legend-engine's, that database is switched on at
  the door; until then it is refused by name.
- **Decision needed:** Q13b, how a database is "checked".

## 4. How we run it

These rules are folded in from stage A's mistakes.

- **Where the work collects:** one stage-B batch branch (ruled ~21:40).
- **Each piece:**
  - a builder;
  - its fast loop, **including every generated-file check** (stage A's last red was a generated file);
  - the independent reviewer's approval;
  - then the judges merge it into the batch branch.
- **Every change is asked one question before it lands:** "what does legend-engine's door answer now?" Stage A found
  its `substr` gap late because nobody asked.
- **Nothing starts on a head that is about to change.**
- **At the end, all in parallel on the final head:**
  - the full measurement (the judges);
  - the heavy and manual targets (two sessions);
  - every warm database, cloud included (five sessions);
  - the combination audit (the reviewer);
  - then ONE CI run.
- **Fixes found at the end** collect on a staging branch and go in ONE push.
- **Merging is the user's word.** Every moved pin and every new register row is listed in the PR.
- **Builders:**
  - B1: the error-records session (H13);
  - B3: the judges;
  - B2: the formatter session (H11);
  - B4 and B5: B2's census session;
  - B6: the five warm-database sessions;
  - review: the independent reviewer.

---

## 5. The open questions

Ruled already, so not asked again:
- Q1, the order;
- Q2, one batch branch;
- Q4, catchable by `assertError`;
- Q5, the window fix, done;
- Q12, the engine runner, done;
- Q14, the corpus at legend-engine's meaning;
- Q16's part (a), keep the recorder and the mirror.

Q10, the switch's removal date, is moot: the switch was deleted at ~07:55.

### E0. The edge rule: ratify it as the rule stage B builds to (NEW; proposed ~05:50, never ruled)

- **Context.** Stage A's door rewrite runs between type-check and planning. You asked how that can be "edge" code.
  The proposed rule says exactly what the edge may do, so the answer isn't "anything, anywhere".
  - Compatibility may change a program only at the public type-check→plan boundary.
  - It may do so only by swapping or removing whole calls, and only for functions with one fixed meaning (for example
    `substring` → `substringAsGiven`, or removing a final `toString()`).
  - It may format output after the run.
  - It never reaches into planning, lowering or execution, never passes a mode in, and never edits a plan or SQL.
- **Options:**
  - **A. Ratify it as written** (recommended).
    - Pros: one clear line, which B5's guards can check mechanically.
    - Cons: E1 needs its own exception.
  - **B. Stricter: the edge may only read input and format output, never change the program.**
    - Pros: purest.
    - Cons: legend-engine's door could not give legend-engine's answer at all. Your ~07:55 ruling would be undone,
      and B2 could not take a final `toString` over.
  - **C. No written rule.**
    - Cons: the confusion of ~05:00 returns. No guard can enforce what isn't written.
- **Cost:** none to ratify. B5's guard writes it as an ArchUnit rule: the edge reaches the core through public types and
  doors only.
- **RULED A** (the user, 2026-11-20 ~19:45: "E0 - yes").

### E1. Compatibility's judge at run time: an exception to E0 (NEW; part of B3)

- **Context.** In option N, compatibility's judge decides each assert from the two sides' typed values as the test
  runs. Lite's test runner already calls "the judges" as a callback; compatibility supplies its judge there. That code
  runs inside execution, which E0 forbids, so it needs a named exception.
- **Options:**
  - **A. Allow exactly this:** the runner hands the judge typed values; the judge returns a verdict (recommended).
    - It cannot send statements, open connections or change the plan, as the judges already can't
      (`hostVerdictIsReachableOnlyFromTheVerdictSeam`).
    - Pros: verdicts on the real values, as legend-pure decides them; no planning involvement.
    - Cons: one named exception in the rule.
  - **B. Judge after the run:** the runner writes every side's typed values out, and compatibility judges them
    afterwards.
    - Pros: no exception.
    - Cons: legend-pure stops a test at its first failed assert. Judging afterwards can't stop it, so the statements
      after a failed assert would still run, including any writes. More plumbing, about 1 to 2 days more.
- **Cost of A:** none beyond B3, which already designs it.
- **WITHDRAWN** (2026-11-20 ~21:00, after the user's "we want to bend the rule to not put this at the edges? ... what is
  the expert way to do this?"): no compatibility code runs during the run; the judge has one rulebook, lite's. Neither
  A nor B is taken: the leniencies that needed a compatibility judge are removed instead (B3).

### Q3. Error sentences on legend-pure's and legend-engine's routes (B1)

- **Context.**
  - Lite's product has its own short sentence per error kind (ruled 2026-11-10).
  - The question is what the legend-pure and legend-engine routes say.
  - 16 PCT tests check legend-pure's exact sentence through `assertError`. All 16 involve only constants written in the
    program: no value comes from row data (measured ~22:19).
  - The corpus has 0 such tests.
- **Options:**
  - **A. Lite's own sentence on every route,** with legend-engine's envelope and status kept.
    - Size: 2 to 3 days, −1,030 lines in all.
    - Pros: one set of words; the smallest B1.
    - Cons: **the 16 PCT tests fail for good**, recorded. You said "I really don't want to lose pct tests".
  - **B. Lean option 2** (recommended):
    - each run-time error's record carries its literal values;
    - compatibility writes legend-pure's sentence from them in Java;
    - compile errors stay in lite's words;
    - no database-computed values ("slots").
    - Size: about 4 to 5 days, not yet sized exactly (H13 sizes it in half a day).
    - Pros: **the 16 PCT tests keep passing**; rule 3 exactly (compatibility fixes upstream's output in Java from
      lite's values).
    - Cons: a second wording system kept for good, at the edge, where it belongs. A legend-pure message whose value
      comes from row data prints a placeholder. No test pins one today; it is recorded.
  - **C. Full option 2 with slots:** the database sends back data-dependent values too.
    - Size: B1 at 6.5 to 7 days, plus 3.5 to 4 for the slots.
    - Pros: legend-pure's exact sentence even for data-dependent values.
    - Cons: nothing upstream tests it today; more SQL per error.
- **RULED B** (the user, 2026-11-20 ~16:40: "we should do B to keep the PCT"). C stays possible later, on top of B, if a client
  needs it.

### Q15. Assert-failure words: lite's own on every route (B3)

- **Context.** When an assert fails, the verdict carries a sentence, for example legend-pure's "expected: 0 / actual: 1"
  or lite's own. Measured: no PCT test and no corpus test expects legend-pure's words, and no upstream client reads them.
- **Options:**
  - **A. Lite's own words everywhere** (recommended).
    - Pros: deletes about 640 lines of two-form plumbing; B3 is about 2 days smaller.
    - Cons: re-pins lite's own harness records (41 witness rows, 69 ledger lines, 11 PCT lines).
  - **B. legend-pure's words for compatibility.**
    - Cost: about +300 lines and 2.5 days.
    - Cons: words nothing reads.
- **The principle that makes Q3-B and Q15-A consistent:** write upstream's exact words exactly where an upstream test or
  client reads them, and nowhere else.
- **RULED A on its condition** (the user, 2026-11-20 ~19:45: "Q15 if it really does not change any tests then I am
  fine"): any verdict that moves comes back.

### Q16b. The 7 shapes the public `from()` can't express (B3)

- **Context.**
  - Today these 7 legend-engine SQL-text asserts pass because the appeal, inside the planner, builds their query with
    internal planning pieces.
  - Under option N the harness must rebuild each query through the public door. Public `from()` has no way to say:
    - "a runtime with post-processors" (4 tests; legend-engine runtimes that rewrite table names and similar);
    - "an inline mapping" (3 tests).
  - That is why they can't simply keep passing.
- **Options:**
  - **A. Teach the public `from()` both shapes,** as a product feature.
    - Pros: the 7 keep passing, and lite's own users gain the shapes too.
    - Cons: a public API change; the size isn't measured. Post-processors are legend-engine features lite may not
      want in its product.
  - **B. A harness-only route:** the harness builds those queries from the model it already parsed, through public
    types.
    - Pros: no product change.
    - Cons: harness code that partly re-does planning, the "second road" G1 forbids, unless it stays strictly at public
      doors. The size isn't measured.
  - **C. Record them as declines:** 7 tests fail, each one a named register row.
    - Pros: nothing to build.
    - Cons: 7 corpus tests lost.
- **Recommendation:** spend half a day sizing A for the 3 inline mappings, which lite's model may already express
  publicly. Take C for the 4 post-processor runtimes, unless a user needs post-processors.
- **RULED: reclaim all 7** (the user, 2026-11-20 ~19:45: "we really should make sure we can reclaim those 7, let me know
  if/why we cant when we get there"); the 4 post-processor runtimes by R1 (~17:00: "R1 yes do the post processor in
  product"), built inside B3.

### Q16c. legend-engine's `assertEqualsH2Compatible` (B3)

- **Context.**
  - 199 of the corpus's row matches go through this legend-engine test helper.
  - It is Pure code that asks the database `SELECT H2VERSION()` and picks its expected answer by H2 version.
  - Lite's door refuses raw SQL inside a query by design.
  - It is an upstream program, so under the world-map tenet we run it; we never port it to Java.
- **Options:**
  - **A. A named harness rule** (recommended): the harness takes the helper's upgraded-H2 branch, the one lite's H2 2.5
    and the referee's golden rows are written for. It is recorded like the other corpus rewrites.
    - Pros: the 199 keep matching; one line of harness logic; honest about why.
    - Cons: one more harness rule.
  - **B. Let lite answer `H2VERSION()`.**
    - Cons: punches a hole in "no raw SQL in a query" for a test helper.
  - **C. Lose them:** 199 matches fail.
- **Recommendation: A.**
- **RULED A, else a native** (the user, 2026-11-20 ~19:45: "Q16c - yes or overwrite as native if that does not
  work").

### Q6. A checked graph's default violation message (B2)

- **Context.** When a class constraint with no message of its own fails in a checked graph fetch, the product today
  writes legend-engine's sentence, `Constraint :[id] violated in the Class Name` (`GraphEmission.java:653`). That is
  upstream's wording in the core.
- **Options:**
  - **A. Lite gets its own sentence; compatibility writes legend-engine's from the error record** (recommended).
    - Example: `Constraint 'id' of class Name is violated`. The words are yours to choose.
    - Pros: the same split every other message follows.
    - Cons: lite's own users see new words. No known dependency. About half a day.
  - **B. Adopt legend-engine's sentence as lite's own**, with a register row.
    - Pros: nothing to build.
    - Cons: upstream text stays in the core under a new label. If either side changes, the other silently follows or
      breaks.

### Q7. The checked graph's "defects" cell (B2), corrected

- **Context.** legend-engine returns a "defects" cell worded by its own presenter. The earlier claim that wording it
  gains 12 corpus tests was wrong: 12 tests plan one, but 0 of their expected answers carry one (measured ~03:45). It
  gains 0 tests.
- **Options:**
  - **A. Don't build it now; build it when a client needs it** (recommended). About 1 to 1.5 days then.
  - **B. Build it in B2 anyway.** Cons: 1 to 1.5 days for no test and no known reader.

### Q8. A test plan may name the program's own `toString`/`toCSV` (B2)

- **Context.**
  - In a test body, an assert may compare a relation's `toString`. You ruled (~18:12) that on legend-pure's route the
    database returns rows and Java prints legend-pure's text.
  - For that, the test plan must carry one fact: "this side is the program's own `toString` of a relation". It carries
    no text, just that fact, like the sort order it already carries.
  - The program's done-when 3 says the plan carries no presentation, so it needs one sentence: "a plan may name the
    program's own presentation call at a side's root; how an audience spells it is the run's."
  - 22 corpus sides need it.
- **Options:**
  - **A. Amend done-when 3 as written** (recommended).
    - Pros: your ~18:12 ruling can be built as ruled.
    - Cons: one more fact in the plan's vocabulary.
  - **B. The edge re-reads the program to find the call.**
    - Cons: a second walk over the program, which G1 forbids. The 22 sides need their own detection.

### Q9. legend-engine's legacy "null-unsafe equality" flag (B4/B5)

- **Context.**
  - A legend-engine request can set a flag that makes `==` plain SQL `=`, so a null never equals a null. Pure says null
    equals null.
  - 7 corpus tests set some flag; 1 needs this one, and it checks SQL text only.
  - With your ~07:55 rule (legend-engine's door always gives legend-engine's answer), honouring it is the consistent
    choice, but no known user sets it.
- **Options:**
  - **A. Refuse it by name at the door, for now** (recommended).
    - Pros: costs nothing, and is loud. It is the same "not built yet → refused by name" pattern as unchecked
      databases.
    - Cons: a legend-engine user who sets it gets a refusal; 1 corpus test stays failing, recorded.
  - **B. Honour it now.** The door swaps `==` for a lite-internal "SQL `=` as given" function, as it does for
    `substring`.
    - Size: about 1 day, plus guard moves for one more internal function.
    - Pros: complete.
    - Cons: built and tested for a flag no known user sets.

### Q11. Comparing values of different kinds (`$t.id == '1'` where `id` is an Integer) (B4)

- **Context.**
  - Pure says values of different kinds are never equal. legend-engine lets the database convert one side, so
    `1 == '1'` can be true, and the answer depends on the database.
  - Lite today does what legend-engine does, which is not Pure.
  - Every case in the corpus is legend-engine's own test code; none is a comparison lite builds itself.
- **Options:**
  - **A. Keep today's behaviour.**
    - Pros: 0 tests move.
    - Cons: the core doesn't mean Pure here, and answers differ by database.
  - **B. Pure everywhere, including a mapping's `###Relational` SQL `=`.**
    - Cons: wrong for mappings, whose `=` is SQL by definition. 26 H2 and 30 DuckDB corpus tests break.
  - **C. Scoped, plus the door rule** (recommended):
    - Pure code follows Pure: never equal, decided by the types alone, a fold your ruling allows.
    - A mapping's `###Relational` `=` keeps SQL's meaning.
    - At legend-engine's door the comparison is swapped for the database's comparison, so legend-engine's clients and
      the corpus keep legend-engine's answer. That needs one more lite-internal "compare as given" function, with its
      guard moves.
    - Refused by name on unchecked databases.
    - Pros: the core means Pure; 0 corpus tests lost once the corpus runs through the door's swaps (B3).
    - Cons: unsized (expected small; the detector already finds these by type). PCT may gain tests (unmeasured).
- **Recommendation: C, sized first, its moved tests listed by name before it lands.**

### Q13b. How a database counts as "checked" for legend-engine's answer (B6, NEW)

- **Context.**
  - Your ~03:45 ruling: measure the other databases in the warm sessions and switch each on when its comparison
    agrees.
  - But legend-engine's own test runner only runs H2 and DuckDB here. There is no legend-engine to ask on Snowflake.
- **Options:**
  - **A. Read and run** (recommended):
    - read legend-engine's SQL for each changed function on that database from its pinned source (its per-database
      SQL generation);
    - confirm lite's rewritten statement is the same SQL;
    - run that SQL on the warm database for the battery's rows.
    - Same SQL on the same database gives the same answer.
    - Size: about half a day per database.
    - Pros: cheap; uses the warm sessions we already have.
    - Cons: legend-engine's answer is inferred from its SQL, not executed by legend-engine itself. Recorded as such.
  - **B. Run legend-engine's own runner against each cloud database.**
    - Pros: exact.
    - Cons: each database's legend-engine connector, credentials wired into the runner, days per database.
  - **C. On demand only:** a database is checked when a user asks.
    - Cons: legend-engine clients on those databases get refusals until then.

### The no-check lean mode (not stage B; noted so it isn't lost)

Your ~01:35 follow-up: a power-user "no check" mode for the leanest SQL, with the user taking the risk. It would be a
switch under the switch registry (ceiling 5). It is designed later and needs no decision now.

---

## 6. Outside stage B: product-track items that need an owner and a time

These are not compatibility work, but several are wrong answers, and nobody owns them right now.

| Priority | Item | What happens today | Recommendation |
|---|---|---|---|
| **1** | `runtimeWithTableReplace` ignored on the product door | **a silent wrong answer**: reads the original tables (PlanMaker.java:121) | fix first, in parallel with B1/B3: apply the renames or refuse |
| **1** | The numbers-q1 branch (`session_0195rQEjTP2YHCipAiqZmEma`), not merged | **wrong Float/Decimal values** on BigQuery, Snowflake, Databricks, Trino and ClickHouse (`0.1+0.2` → `0.3`; a Decimal product → `370370367`), on `main` today | it waits on your rulings in that session (five failures to rule); bring them to you now |
| 2 | BigQuery channel B, 19 tests, on `main` | a loud error, from PR #118 (an untyped NULL in a host side batch) | type each NULL cell by its column; small |
| 2 | MariaDB list order changing from run to run | a determinism finding (G10) | an owner and a ledger row |
| 3 | MySQL's 30-digit decimals (Q3b) | 17 channel B rows | with numbers-q1 |
| 3 | `sqlSubstring` following the SQL standard; SelectMerge and draws-afresh values; the test-data scan; ValidateDesugar; a packaged ModelChainConnection; H2's `weekOfYear` locale; the 9 dead hooks | listed in #128 | the product track, in order |

**Who owns them:** the product orchestrator, `session_0195rQEoRb1AGCHxtZChjWsg`, which paused these tracks, or this
session alongside stage B. Your call.
