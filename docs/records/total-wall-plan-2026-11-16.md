# The total-wall plan, all streams in one place (2026-11-16)

Written by the orchestrator at the user's prompting, 2026-11-16, about 13:20 UTC: "Do we need to pause and take giant
step back to write down the plan again in detail so we don't keep making mistakes?"

## 0. How this document is used

- **It is the one plan.** It covers stream D (compatibility to the edge), E-Java (the judges, and no bad Java) and
  E-SQL (lean product SQL). The workers' own plans stay as their detail:
  - `docs/records/d-plan-2026-11-16.md`;
  - `docs/records/e-java-plan-2026-11-16.md`;
  - `docs/records/e-sql-plan-2026-11-16.md`;
  - their decision logs.
  Where they conflict with this document, **this document wins.**
- **Every correction goes here, dated** (§12), not only into a chat message. A worker re-reads §2 to §4 before it starts
  each item. When a correction changes work in progress, the orchestrator interrupts the worker.
- It lives on `claude/total-wall-orch`. Read it with
  `git fetch origin claude/total-wall-orch && git show origin/claude/total-wall-orch:docs/records/total-wall-plan-2026-11-16.md`.

## 1. The goal

The user's words: "everything compatibility-related to the edge". The product does everything typed, fast and cheap.
Backwards compatibility stays as close to legend-pure and legend-engine as possible, and lives at the edge
(`com.legend.edge`, the edge's dialects, the one hook `PlanStyle`), which the product never reaches.

When all three streams are done, this is true:

1. **A product answer** comes in lite's own typed forms:
   - a Float in six decimals;
   - a DateTime in ISO with `Z`;
   - an error as its kind.

   It is computed by the database in plain, lean SQL. Java computes none of legend-pure's or legend-engine's spellings
   for it, in SQL or in Java.
2. **A legend-engine client** gets exactly what legend-engine and legend-pure give. That covers pure/v1, a PCT run and
   the relational corpus. The edge makes it, by formatting typed values in Java, and inside a query by using exactly
   the SQL legend-engine writes.
3. **The judges** that run tests are lean, fast and readable, and compute nothing in Java that the database should.
4. **No compatibility spelling is left in product code**, and a test fails if one appears. Every deliberate difference
   from upstream is a register row the user ruled.

## 2. The rulings that govern every step (the user's words, verbatim)

### Product and edge
- (2026-11-15/16, D's plan §1) "everything compatibility-related to the edge".
- (S311, 2026-11-10) A product Float's text: "let's go back to 6 ... and use decimal where need". Reaffirmed
  2026-11-16: "I thought for product we are printing formatted to 6 decimals across all backends".
- (2026-11-16) "Have we been wasting time trying to print floats in the database again? Didn't we agree to do all the
  compatibility in JAVA? And for toString inside an expression use engine cast( as varchar)".
  - **Reading, settled with the user the same day.** On the product road, a Float's text is S311's six decimals: one
    simple format call per database. On the edge road, a Float's `toString` inside an expression is exactly
    legend-engine's own SQL for that database (`cast(%s as varchar)` on H2, Snowflake, Trino, Postgres-like;
    `cast(%s as String)` on ClickHouse; `cast(%s as string)` on BigQuery; per the pinned extensions). legend-pure's
    exact print of a VALUE is Java at the edge, from the typed value.
- (G6, ratified 2026-11-07) "The database does the work. Small Java formatting, the appeals and the referee are allowed
  only at the edge."
- (S312, 2026-11-10) A product DateTime: "ISO 8601 in UTC, six fraction digits and `Z`".
- (U5, 2026-11-15) "We should look at dataformats and tsshow to see if really need those exceptions at all - same with
  anything that is an exception we should look one by one and see if really needed (should not really have any
  exceptions)".
  (U5, 2026-11-16) "We said to check all the exceptions they probably are not needed, probably bad code remnants of
  compatibility that we should not have".
- (Upstream fidelity, AGENTS.md) At the edge, lite does exactly what legend-pure and legend-engine do. It reads their
  answer from the pinned sources or asks the engine runner, and never recalls or guesses it. A divergence is a register
  row the user ruled.

### Lean SQL (E-SQL)
- (2026-11-15) "the leanest fastest most expert SQL for the product, every SQL trick and expert knowledge on every
  backend to make it lean, human readable, fast, and correct".
- (2026-11-15) "subselects only when needed, repetition only when needed, none of the old pure craziness that made us
  do crazy sql, most simple and fast sql everywhere".
- (E-SQL's D2, approved 2026-11-16) Speed first: a shorter but slower form is not used. On equal speed, the shorter
  form wins. Answers stay the same, and the pins only shrink.

### Judges and Java (E-Java)
- (2026-11-15) "making sure we're not doing anything bad in java and making sure judges are lean and fast and
  readable".

### Process
- (2026-11-16) "If you want to chop D into phases that can become stacked PRs that fine but I don't want you to stop
  until whole thing is done and working end to end, do not stop and ask questions and not not run unsafe commands that
  need my approval, or for me to merger things or approve things, just keep going and don't stop until done".
- (2026-11-16) "Remember that we need fast dev loop too for all of this work".
- (2026-11-16) Nothing lands until D has everything at the edge.
- (2026-11-15) Review before CI. The order is:
  1. a staging branch, with no PR;
  2. an independent review, plus a lane round if SQL moved;
  3. the fixes checked;
  4. one push to the PR;
  5. one CI run.
- PRs don't edit shared docs. A PR's records go in `docs/records/`.
- Nothing merges without the user's word.
- Credentials are never read, printed or copied, and the plan never holds a secret.

## 3. What we will NOT do (each one is a mistake we made, and its lesson)

1. **No SQL that reproduces legend-pure's or legend-engine's printed text, anywhere:** product, judges or edge.
   - At the edge, upstream's text of a value comes from Java formatting of the typed value.
   - Inside an expression at the edge, it is the database's own cast, exactly as legend-engine writes it.
   - *(Mistake: legend-pure's Float print is still built in SQL per dialect, `FLOAT_PRINT`. It is being moved to Java.)*
2. **No exactness engineering in a product form.** A product form is one simple, fast call per database. Where the
   databases differ at the extremes (a halfway tie, a huge or tiny value), the difference is written down in the
   register row. It is not engineered away.
   - *(Mistake: W3 and its follow-ups grew about ten per-database exact-digit forms for a Float's text, including
     256-bit integer arithmetic on ClickHouse and a session setting sent with every ClickHouse statement. One path was
     10x slower. We drove it with a 288-value instrument. It is being reverted, 2026-11-16.)*
3. **No session setting sent for a form's sake.** Exec sends only the plan's statements (charter A3).
4. **No probe or instrument stricter than the ruling.** A probe checks the rule as ruled, on ordinary values.
5. **No lane round on a commit that is about to change.** Lanes measure the final state of a step.
   - *(Mistake: round 65's timings were taken on Float code being reverted. Its outcome check was still useful.)*
6. **No orchestrator paraphrase without the user's words beside it.** When a message reads two ways, take the narrower
   reading and check it.
   - *(Mistake: "use engine cast" was read onto both roads at 12:19 and corrected at 12:24.)*
7. **No correction by chat message alone.** It goes into §12 here, and the worker is interrupted if the correction
   changes its current work.
   - *(Mistake: D worked on the old direction for about 30 minutes with two corrections waiting in its queue.)*
8. **No guard pin grows without a dated reason.** A transitional growth names the step that takes it back.
9. **No fallbacks or defaults** (G7, invariant 4).
10. **No new mechanism outside this plan without asking.** See §4.
11. **No PR edits shared docs.**
    - *(Mistake: PR1 edited GATES.md and the register. Moved to `docs/records/`.)*
12. **No relayed ruling without checking the ruling log.**
    - *(Mistake: E-Java was told "Q7 no" against the user's approval, then corrected.)*

## 4. When a worker stops and asks

A worker stops and asks the orchestrator before it builds or pushes, when:

1. a product form needs more than one simple call per database, or is slower than the form it replaces;
2. anything would print upstream's text in SQL;
3. it adds a new mechanism: a plan field or a plan-format bump, a hook or facet, a session statement, a new family of
   classes, or a new probe or instrument;
4. a PCT, corpus or channel B outcome or text moves at the edge;
5. a guard pin grows other than by a file moving, or a register row is added or changed;
6. an item runs past twice its size;
7. two rulings seem to conflict, or a ruling's reading is unclear.

The orchestrator decides what the rulings already settle and records it in §12. It brings to the user only what needs
a ruling, batched in §10. It does not wait on that batch unless the work is blocked.

## 5. Stream D: compatibility to the edge (worker session_01XmdJ7kRamr4Sexmcrguc94, branch `claude/total-wall-d-stage`)

### Done (checked by lane round 65 on all 12 databases: no PCT outcome or text moved)
- **One staging branch.** Holds D's three branches and E's frozen PR1 (merged at 26717de291).
- **Errors are typed kinds in the product.** The product words them in lite's sentences; legend-pure's and
  legend-engine's sentences are spelled at the edge. That covers:
  - raises at run time;
  - compile errors;
  - a missing parameter;
  - a match failure;
  - the query store's refusals.
- **The plan says which errors each statement can raise.** The kinds are recorded where the SQL is written, not
  found by scanning the text. This fixed BigQuery, whose escaping hid the marker.
- **DateTime.** ISO with `Z` in the product. legend-pure's `+0000` is the edge's printers only, and a date literal
  carries no zone.
- **One compatibility hook, `PlanStyle`, with a closed list of facets.** The product refuses each facet by name; only
  the edge implements them.
- **H2 quotes its own keywords.** legend-engine's word list is the edge's. legend-engine's scan order on DuckDB is the
  edge's.
- **legend-engine's result writers and cell text are at the edge.**
- **Product forms from D1.2 and D1.3:**
  - a TDS cell's text;
  - a relation's text refused in the product;
  - lists and maps as JSON;
  - `-0` prints as `0`;
  - an instance's `toString` refused;
  - pivot names bare.
- **H2 refuses `toOne` over a list by name.**
- **Under way:** legend-engine's plan text (generatePlan's text, `toSQLString`, a plan handle's node model, an
  execute's activity SQL) moving out of the planner to the edge (W9's planner half). It is built and green on the fast
  suites; the corpus verdicts are pending.
- **The checkpoint audit** of the above (two independent read-only reviewers at `89d4eae3b`) is running.

### Left, in order
1. **The Float revert (now; the only work allowed during the freeze).**
   - The product's Float text goes back to one simple six-decimal format call per database (D1.2's forms).
   - **Deleted:**
     - the exact-digit machinery in AnsiSqlRenderer, DuckDB, Postgres and Databricks (`shortestWhenHuge`,
       `shortestFrom`, `roundedShortest`);
     - Snowflake's `shortestDigits`;
     - BigQuery's `PARSE_BIGNUMERIC` path;
     - ClickHouse's 256-bit digits and its `precise_float_parsing = 1`;
     - SQLite's exact path and its refusal from 1e8;
     - MariaDB's and MySQL's `DECIMAL(65,6)` path;
     - MongoDB's Decimal128 path;
     - Postgres's NUMERIC rounding;
     - `FloatTextInstrumentTest` as a gate;
     - the proposed S311 rows (§12 of D's log, withdrawn).
   - **Kept:**
     - S311's recorded differences between databases;
     - the typed raises;
     - `-0` → `0`;
     - the channel B re-pin;
     - the date and writer work.
   - The probe's Float case checks the simple rule on ordinary values (1.5, 2, 0.3, 1/3, 1234567.89, -0.0, NaN).
   - **Timed:** no slower than before W3, alternated on DuckDB and SQLite.
2. **The edge's Float.**
   - legend-pure's text of a Float VALUE (result cells, pure/v1 answers) is printed in Java at the edge from the typed
     value. Each dialect's `FLOAT_PRINT` arm whose only use is that is deleted.
   - A Float's `toString` inside an expression at the edge is legend-engine's own SQL for that database.
   - Before switching:
     1. Measure which PCT tests move, on DuckDB, H2 and SQLite channels A and B.
     2. Where legend-engine itself gives the database's text, the move becomes an expected failure with legend-engine's
        reason.
     3. The list goes to the user (§10) before the switch lands.
   - The judges' share of this is E-Java's (§6 item 2).
   - **First, an inventory.** List every SQL site in every dialect that spells a Float's text, with what reaches it
     (product, edge or judge). Each gets its fate:
     - product → S311's one simple call;
     - an edge value → Java;
     - an edge expression → legend-engine's cast;
     - a judge → a typed comparison (E-Java).

     Known so far: Snowflake's `CAST(double AS VARCHAR)` still goes through `floatText`'s exact-digits machinery
     (`shortestDigits`, from October), alongside `floatCanon` and `floatPrint`. The revert did not touch these.
   - **The inventory (D, 13:33) and the decisions on it (orchestrator, 13:40):**
     - **A Float's toString inside an edge expression:** legend-engine's cast. Built on a work-in-progress branch;
       its PCT moves are measured before it lands.
     - **A Float in a raise sentence at the edge** (`PureMessages`): the sentence stays in the query, so the Float
       uses legend-engine's cast. No new mechanism. Its moves are measured with the rest.
     - **The judges' `%r` and canons:** typed (E-Java's W10a and W10b). The `FLOAT_PRINT` arms are deleted after
       W10a, the `FLOAT_CANON` arms after W10b.
     - **The product reaches legend-pure's Float print** through `toRepresentation` and `format`'s `%r`. Both are
       refused by name in the product. S314 already rules "`%r` and padding at the edge", and `toRepresentation` is
       `%r`'s function. Confirmation is in §10.
     - **ClickHouse's product `format('%s', Float)`** writes Pure's literal text. `%s` of a value is its toString,
       so it becomes S311's one call. This is a product fix.
     - **The dialects' text of a double** (Snowflake's, BigQuery's, SQLite's and MariaDB's shortest-text emulations;
       ClickHouse's `.0` regexp) is deleted once neither a judge nor a carrier reaches it.
       - **CAUTION, carriers:** a value carried as text (a list or JSON carrying a double) must read back exactly
         (same answers). A database's own cast does NOT always do that. Snowflake's own cast writes ten significant
         digits, measured on its lane on 2026-10-21. So a carrier must carry the typed value, not its text, on such
         a database. That is E-SQL's work, and it is measured per database before any emulation is deleted.
3. **Finish moving legend-engine's plan text to the edge.** Run the corpus verdicts, then the rest of the planner's
   engine-text rows.
4. **The guard to zero.**
   - Every remaining row of `CompatSpellingGuardTest` is reviewed one by one: engine plan text 37, judges 38 (with
     E-Java), D1.9 8, the hook 1, the four marked STAYS.
   - The default is REMOVE: move the row to the edge, or delete it.
   - A row survives only with evidence that it is product semantics (who calls it, what breaks without it), for the
     user's ruling.
   - This includes the product H2 session still sending legend-engine's `NON_KEYWORDS`.
5. **Loose ends:**
   - lists and maps as JSON in the CSV and Arrow writers;
   - the PE ratchet;
   - `CatalogUpstreamDiffTest`'s ceiling;
   - S311's and S312's row text updated to what is built.
6. **Records:** D's register rows and its v1 record, in `docs/records/`.
7. **Landing:**
   - one final review of the whole of D;
   - one lane round;
   - D's PRs stacked by phase (P1+P2 forms, errors, writers and dates; P3 engine text and judges at the edge; P4 the
     guard at zero), opened together;
   - one CI run each;
   - the user's merge word.

### D is done when
- `CompatSpellingGuardTest` is at zero, or holds only rows the user ruled in.
- The edge is unchanged against upstream, except the ruled engine-cast moves: PCT A and B on every database and both
  corpora.
- Product forms follow S311 and S312, by one simple call per database, no slower.
- Every lane is green, and every divergence is a ruled row.

## 6. Stream E-Java: the judges, and no bad Java (worker session_01XmdJ7mGZpJB7BmQRiqXNSR)

### Done
- **PR1, frozen at 26717de291** (lands first).
  - The judges run one road.
  - The Float canon is staged once per side.
  - The database judge is self-contained and lean.
  - BOTH judges agree on every lane.
  - The ledger audit's false green is fixed.
  - Lane round 62 was clean on all 12 databases, none slower.
- **Moving legend-engine's test text to the edge, judge half (W9).** The judges read engine-test text only through the
  hook's facet, and the product refuses it by name (S1, S2a, S2b on D's stage).
- **Lean judges, on `claude/total-wall-e-v3`.** A host side over a one-row let rides the fused row. Grammar's judge
  statements went 291 → 215.

### Left, in order
1. **Finish moving the engine-text assert family to the edge.**
   - Its planner arm, gates, runner and appeals all move.
   - The two files that grew in S2b go back below their earlier size (`AssertVerdicts` ≤ 1,520 lines, `SqlTextVerdicts`
     ≤ 1,157), and the temporary architecture allowance is removed.
   - It must be a move, not new design. **Allowed during the freeze.**
2. **The judges' print forms (W10, approved; starts after the freeze lifts).**
   - **(a) Messages.** A failed verdict's message is formatted in Java at the edge from typed columns, with the host
     judge's own formatter, so both judges give the same text by construction.
   - **(b) Comparisons.** Canons become typed comparisons in SQL:
     - a Float as a number with the one ruled tolerance (one constant, shared with the host judge);
     - a Decimal exactly;
     - a date as a date;
     - mixed kinds by lite's tag-plus-typed key.

     No printed text of legend-pure's is computed in SQL.
   - **(c) Goldens.** A rendered-text golden is parsed by the edge into typed rows at planning.
   - **Conditions:**
     - no verdict flips;
     - a message parity test across both judges;
     - judge pins shrink-only;
     - deferred goldens counted and put to the user.
   - The W10 record's opening paragraph must be corrected. It says the product's in-query Float becomes legend-engine's
     cast; the product's Float stays six decimals, and only the edge's in-query Float is legend-engine's cast.
3. **v2: lean and readable judges** (built in E-Java's own files; lands after PR1 and D):
   - at most 1.6 judge statements per PCT test on every suite;
   - judge characters down (grammar −20%, essential −10%);
   - no judge class over 1,000 lines;
   - one page "how a verdict is made";
   - the DATABASE-alone CI pass retired if it differs from BOTH nowhere.
4. **v3:** Java's evaluation of values shrinks in E-Java's own files, 4,448 → ≤ 3,800 counted lines, each remaining row
   justified or scheduled.

### E-Java is done when
- the judges name no upstream text, and print no legend-pure text in SQL;
- v2's and v3's targets are met;
- no lane is slower and no verdict changed.

## 7. Stream E-SQL: lean product SQL (worker session_01XmdJ7jKZMVTrr2TX1aqSzN)

### Done
- **E-v2, closed at 20cc9df83** (lane round 64 clean on all 12 databases; Snowflake re-timed, no slower):
  - a `let` over a mapped class written once;
  - a row's repeated value computed once (`RowValues`; inline on DuckDB, where a lateral is 38x slower);
  - on DuckDB:
    - `COUNT(*)` without a wrapper;
    - a sort by the key directly;
    - a filter over groups as `HAVING`;
    - a built object without an identity guard;
    - fewer casts and CASEs;
  - a standing timing set;
  - the mixed-list correctness floor (refusals by name where a value can't be held exactly).

### Built on `claude/total-wall-e-sql-v3` so far (E-v2 merged in; the gate 40/40 green)
1. Snowflake reads a NULL-free list directly with `FLATTEN`; channel A 762K → 719.5K characters.
2. Snowflake writes `IFF` for a one-WHEN CASE.
3. Postgres: no fence around `unnest` of a constant list; subselects 1,696 → 1,267. A map's `unnest` is read once.
4. Every SQL database: compacting a list that cannot hold a NULL is the list itself. Postgres's literal-step range
   writes `g < stop`.
5. Postgres writes a list literal's type once.
6. Postgres builds a small literal list as `jsonb_build_array(x)`. Its checks were running at the freeze.

Snowflake's items are timed on its lane in E-v3's lane round. Text-level spellings that cannot be slower are the only
kind allowed without a timing.

### Left, in order (each stacked on the last, one review and one lane round each)
1. **E-v3: Snowflake and Postgres.**
   - Snowflake: read its largest repeats once, its own idioms (`IFF`, `ARRAY_*`), and RowValues read the faster way by
     timing.
   - Postgres: subselects per statement 0.94 → 0.40, by named causes.
   - Also:
     - the root count without a scalar subquery (after E-Java reads a one-row aggregate root);
     - the mixed-list floor's real fix (a list keeps its kinds through concatenate and range, so fewer refusals);
     - the may-skip set narrowed by per-dialect proof;
     - the casts the renderers write themselves, classified.
2. **E-v4:** Databricks, ClickHouse, BigQuery.
3. **E-v5:** MariaDB and MySQL, Trino, SQLite, H2, MongoDB.

**Not E-SQL's:** print forms in product SQL. After D's revert, only S311's simple forms should remain. E-SQL measures
any left and hands the list to D.

### E-SQL is done when
- every database's named causes of size are worked down, or named as needed;
- answers are the same, none slower by timing, and every pin shrank.

## 8. How we check (the fast loop and the lane rounds)

- **The worker's fast loop, minutes not hours:**
  - the guardrails and census;
  - the suites the change touches;
  - PCT DuckDB channels A and B;
  - for an edge change, both corpus verdicts;
  - for a product form, a timing alternation on the in-process databases;
  - for any edge change, the suites that assert upstream's words and texts: `//query-store:wasm_test`,
    `//datacube:saved_pages_page_test`, `//datacube:python_execute_test` and `UpstreamProtocolFieldsTest`. The lane
    rounds don't run them. *(Lesson: the P2 audit found edge routes answering in lite's words, which round 65 could
    not see.)*

  No full gate per step.
- **Lane rounds:** six lane sessions, measuring only, on all 12 databases. One round per finished step, on its final
  commit. Any timing flag is settled by alternating old and new commits, each run alone.
- **Checkpoint audits:** two independent read-only reviewers at each of D's phase ends, and on each E PR before its
  lane round.
- **CI:** once per PR, on the landing candidate.

## 9. Landing order

1. PR1 (E-Java v1, frozen at 26717de291).
2. D's stacked PRs.
3. E-SQL's E-v2, E-v3, E-v4, E-v5.
4. E-Java's v2, v3.

For each: merge the main of the day, give the plan-format number in merge order, one lane round if SQL moved, one CI
run, and the user's merge word.

## 10. Waiting for the user's rulings (the end batch; none blocks the work)

1. **E's register rows:** S341 and R1 to R6; the E1 ledger split.
2. **An unread derived column that can raise.** Pure raises; lite prunes the column and answers the rows. Keep lite's
   answer as a row, or evaluate the column?
3. **The CSV writer's Float cell.** It writes `3.0` (Pure's print form of a carried value), while S311 rules a
   `toString` `3`. Keep `3.0`, or write S311's text?
4. **A DateTime value written at a coarser precision** (`%2024-01-02T10:30`). It now prints six digits and `Z`;
   before, it kept its written precision.
5. **A value finer than a microsecond** (DuckDB TIMESTAMP_NS). It is cut to six digits. Cut it, or refuse it by name?
6. **The edge's in-query Float `toString` as legend-engine's cast:** the list of PCT tests that move (to come from
   D, §5 item 2).
7. **H2 `toOne` over a list,** refused by name.
7a. **A product `toRepresentation` or `format('%r')` of a value**, refused by name. This extends S314's "`%r` and
    padding at the edge" to `%r`'s function; the edge keeps legend-pure's text.
8. **Any guard row that survives the one-by-one review** (§5 item 4), with its evidence. **The first candidate:**
   H2's product quoting rule (legend-engine's word list as written, the session-freed words upper-cased) and the
   session's `NON_KEYWORDS`. The evidence is that they decide which column a declared name reads in H2 data made
   legend-engine's way. W6 dropped them, and a lowercase-quoted `"order"` column then failed to be found (P2 audit,
   probed on H2 2.5.252), so they are restored pending your ruling.
9. **Deferred goldens in W10,** if any are found.
10. **The orchestrator's decisions under delegation,** listed in §12.

## 11. Parked (not in this plan)

- The cross-language fix batch (resumes after D and E's first versions land).
- PR #102 (waiting on its author).
- Deephaven.
- From D's plan:
  - the Decimal print form;
  - D3 (engine plans in and out);
  - the layered edge;
  - the date-format fidelity PR;
  - lineage rebuilt upstream's way.
- From E-SQL's plan: its parking list.

## 12. Change log of this document and the orchestrator's decisions

| When (UTC) | Change |
|---|---|
| 2026-11-16 13:30 | Written. A design freeze for all three workers until the user has read it: D does only the Float revert, E-Java only S2c's move, E-SQL only measures. |
| 2026-11-16 12:24 | (recorded) The Float direction corrected: product = six decimals by one simple call per database; edge in-query = legend-engine's cast; legend-pure's print of a value = Java at the edge. Replaces the 12:19 message. |
| 2026-11-16 12:55 | (recorded) S2b's pins grew transitionally (AssertVerdicts 1,520 → 1,529, SqlTextVerdicts 1,157 → 1,173); S2c takes both back below the earlier numbers. |
| 2026-11-16 13:05 | (recorded) D interrupted: commit the planner-text move as it stands, then the Float revert before anything else. |
| 2026-11-16 13:10 | (recorded) Snowflake's timing re-run stopped (it was timing code being reverted); it is timed fresh after the revert. |
| 2026-11-16 13:25 | E-Java's S2c: only S2c-1a (a pure move, no guard row) goes ahead during the freeze. The rest, moving `SqlTextVerdicts` to the edge, reads about 20 test-planner members. **Decided (b):** one public facade class in `testplan` (the typed seam E-Java proposed at S2's start), with one architecture entry row, rather than about 20 entry rows. It is narrower, one place to read, and one row to guard. It is built after the freeze lifts. |
| 2026-11-16 13:35 | D's Float revert pushed (`4ff4f86e7`). Every product `floatToText` is D1.2's form, the exactness machinery and `precise_float_parsing` are gone, and the instrument is deleted. Its SQL is D1.2's statement on every dialect, so no alternation is needed. Found while checking: Snowflake's cast of a double to text still uses the October exact-digits machinery. It goes into §5 item 2's inventory. Lane round 66 is sent on `4ff4f86e7`. |
| 2026-11-16 13:40 | D's Float-text inventory received. Decided (§5 item 2):<br>- a raise sentence's Float stays in the query with legend-engine's cast;<br>- the product's `toRepresentation` and `format('%r')` are refused by name (S314; confirmation in §10 item 7a);<br>- ClickHouse's product `format('%s', Float)` becomes S311's one call;<br>- the shortest-text emulations are deleted only when no judge and no carrier reaches them;<br>- carriers carry typed values where a database's cast does not read back (Snowflake), which is E-SQL's work, measured first.<br>All of it is built after the freeze lifts. |
| 2026-11-16 13:50 | P2 checkpoint audit, reviewer B (at `89d4eae3b`): four MUSTs, three verified by the orchestrator. (1) A merge dropped three register amendments (S138, S170, S181). (2) The query-store page answers upstream's API in lite's words, so two TS suites go red. (3) The edge's host routes lost upstream's words and prefix for a missing parameter, so the Python execute test goes red and a test was widened. (4) Product lowering throws legend-pure's window-frame sentence. Plus SHOULDs S1 to S8. All went to D as fixes, allowed during the freeze. S8 decided: the edge receiver's refusals move to planning, so the run decides nothing (G4). Reviewer A pending. |
| 2026-11-16 14:00 | P2 checkpoint audit, reviewer A: four MUSTs. (1) W6's H2 quoting reads a different column: a lowercase-quoted `"order"` is no longer found (probed). Decided: restore the pre-W6 product quoting on H2 exactly, and keep the rule as one guard row for the user's ruling (§10 item 8). (2) The edge's host routes and ServiceTestRunner word errors in lite's words. (3) The host's Arrow route writes the product's S312 dates where it wrote legend-pure's. (4) pure/v1's missing-parameter prefix changed, and a test was widened. Plus SHOULDs S1 to S5. All went to D. The fast loop gains the upstream-words suites (§8). |
| 2026-11-16 13:25 | E-SQL's item 6 (Postgres `jsonb_build_array`), committed just before the freeze, stays. It is a dialect spelling within E-v3's plan, and E-v3's review and lane round judge it. |
