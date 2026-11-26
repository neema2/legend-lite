# The edition program: the design and the plan to finish Stage B (2026-11-25, revised late night)

*Written by the orchestrator session that took over from B3 (`session_0195rQEk16FvwAXgknSU73Fi`). For a session with no context: read `edition-program-START-HERE-2026-11-25.md` §0, then this file. This file is the design's mechanism and the order of work. Everything else in `edition-design-v3-2026-11-25.md` still holds (the three editions, rule zero, what legend-engine's answers are matched and which are not, the gates), with every ruling in `edition-homework/v2-amendments-ruled-2026-11-25.md`.*

**Status.** Proposed to the user on 2026-11-25 and discussed through the night; the user's words on it are quoted in §7. The user wants to go through it once more, and then have it built end to end without stopping. Two parts of the design are not yet proven and are built first (§5, step 2).

---

## 1. What "Stage B done" means

- **The product** holds no knowledge of legend-pure or legend-engine, and the build fails if any comes back.
- **Compatibility** lives at the edge: the door each audience comes in by, the code that writes answers out, and the test tooling.
- **The tests say so:**
  - PCT's Pure run fails only where a database can't do what Pure does, each case recorded;
  - in PCT's legend-engine run on DuckDB, nothing legend-engine passes fails in lite;
  - the relational corpus loses no passing test;
  - the product's SQL never grows for compatibility. It is welcome to get leaner.
- **Every difference** from legend-engine is a row of the register that the user ruled.

**What we judge by: the tests' answers** (core, PCT, the relational corpus, the stress corpus), and the product's SQL size pins, which may only go down. We do not require that SQL text stays the same, and we build no new harness to check it (the user, §7 item 4).

## 2. The design: four kinds of difference, each decided where it can be

Three audiences disagree about what a program means: **lite** (the product), **legend-pure** and **legend-engine**. They disagree in four ways.

| # | The difference | Example | Decided | How |
|---|---|---|---|---|
| 1 | What may be compared | `1 == 1.0` | when code is type-checked | the door gives the type-checker one setting for the query: a compile error (lite), the database's comparison (legend-engine), Pure's answer (legend-pure) |
| 2 | What SQL a function becomes | `substring`, `round`, whether a zero divisor is checked | when SQL is produced | a small table the door hands in; lite's is empty; each row is one recorded difference |
| 3 | Which side of legend-engine a piece of code runs on | a `let` that never touches the database; a corpus test's own code around `execute()` | when functions are expanded into the query, the last moment it can still be told | a stamp on each call that is outside any store query; the stamp picks the table's column |
| 4 | How a value is written out | `2.0` or `2`, a date's text, an error's wording | after the database answers | Java at the edge; the SQL does not change |

Separately, the machinery that exists only to run upstream's tests moves out of the product into test tooling.

### 2.1 Why the comparison setting reaches functions and mapping code

Type-checking is not one moment. The query is checked first; a function's body is checked when the function is expanded into the query; mapping code (M2M, `~func`, associations, derived properties) is checked when the store is resolved. All of it is checked by the same per-query checker, through the same piece of code (`compiler/spec/Overloads.applyGeneric`, where step 5a's marker is made today; function bodies and mapping functions enter through `Typer.inFunctionScope`, `Typer.java:152`, which switches the rule off today). A setting inside the checker therefore applies whenever anything is checked. The earlier door rewrite ran once, after the query was checked, and so never saw the rest.

The typed bodies are remembered per query, not per model (`SpecCompiler`'s memo is a field of the compiler `TypedQuery` builds), so a model held for two audiences can't hand one the other's typing.

**One change must come first.** The relational mapping language's `=` and `<>` are written as plain Pure `equal` in two places (`normalizer/RelOpTranslator.java:701-702`; the enumeration match at `MappingNormalizer.java:2490`), while its `<`, `<=`, `>`, `>=` already go to lite-only functions. `=` and `<>` move to a lite-only function too, so the relational language keeps comparing by SQL's rule (the user's ruling 1) and the setting never touches it.

### 2.2 The table

The lowering already takes an object from the door on every path (`lowering/Styled.forms()`, built by `planner/PlanEnv`) and asks it at about 20 places. The table is that, made regular: a function mapped to another product-named rule (`substring` to `substringAsGiven`), a raise kind mapped to checked or plain, and a few whole-statement choices. It reaches functions and mapping code because by then everything is one tree.

### 2.3 The stamp

One bit on a call: "this call is outside any query sent to a store".

| Door | What is stamped |
|---|---|
| pure/v1 | every call in a `let` whose value reads no store, including calls from functions it uses; and the model's and mappings' code legend-engine's own Java runs (below) |
| the corpus | every call in the test body that is not inside the query handed to `execute()`; the judges' own calls |
| the product, PCT | nothing |

Mapping code arrives later, when the store is resolved. Most of it exists only inside a store query and is never stamped. Two kinds are legend-engine's own Java's, and the resolver stamps them as it brings them in (amended 2026-11-26, as built, after the old rewrite's own rule and the independent audit, `r3/audit-step2-3.md`):
- a model-to-model mapping's transforms, under a graph fetch or over a model connection (not in a TDS query over a chain to a store, where legend-engine pushes them into the SQL), with the code of the functions they call and the qualified properties a graph fetch reads off the classes such a mapping makes;
- a class's constraints, where a checked graph fetch evaluates them.

The rule for a query's own statements, as built:
- A statement that reads a store is that store query's code: not stamped.
- A relation bound by a `let` and not yet executed (no `from`) is a fragment of the query that executes it, so the statements that build on it are that query's code too. A `let` that was executed holds a value; a statement that only uses the value is outside.
- A function a `let` holds is never stamped where it is bound. Its code runs where it is applied, and is stamped there or not. A function value that is computed some way other than by writing the lambda is refused by name.

The door hands in two columns, and the stamp picks one:

| Door | Not stamped (inside a store query) | Stamped (outside) |
|---|---|---|
| pure/v1 | legend-engine's | Pure's, as legend-engine's own Java |
| the corpus | legend-engine's | legend-pure's (the test harness) |
| PCT's legend-engine run | legend-engine's | legend-engine's |
| PCT's Pure run | legend-pure's | legend-pure's |
| the product | lite's | lite's |

It is a field on the call, like the source position a call already carries, so it rides along when a pass rebuilds the call. About 13 places rebuild a call by hand and must copy it; a guard keeps new ones honest. With it, today's `substring` rewrite at the pure/v1 door (A3, about 900 lines) can be deleted, `substring` works inside mapping code on pure/v1, and a `let` that reads no store is exact for every row, not only three functions.

### 2.4 Why the earlier designs failed

Each chose one place for everything, and no one place has everything.
- **The door rewrite** had the right facts for comparisons but ran once, too early to see function bodies and mapping code.
- **v1** put everything in the type-checker, which can't know which side code runs on and has nothing to hold on to for raises.
- **v3 and v4** put everything after store resolution or at lowering, where all the code is visible but the `let`s are gone.
- **A marker carried to lowering for comparisons** (this session's first proposal) fails too: about ten passes in the store resolver refuse a node they don't know (`edition-homework/r3/trace2-marker-risk.md`).

## 3. What is proven, and what is not

**Read in the code** (three traces, `edition-homework/r3/`):
- the SQL differences fit the table: `isNumeric`, `parseJSON`, `parseDecimal`, `parseInteger`, the raises where the lowering already asks, the texts only legend-pure writes;
- the type-checker reaches every body and all mapping code (§2.1);
- a wrapper node does not survive the store resolver, and the `execute()` boundary does not survive it for class-rooted frames: which is why §2.3 is a field on the call and not either of those.

**Not proven, and built first:**
- that the stamp survives from function expansion to SQL, and marks the same code today's `substring` rewrite does;
- that the comparison setting, switched on inside functions and mapping code with legend-engine's value, moves no test.

**Known limits:**
- an enumeration compared with a String: legend-engine's SQL and its Java disagree, so one answer is a recorded difference;
- the implicit `toOne` checks are not calls and carry no stamp; they follow the unstamped column;
- the corpus keeps one comparison setting per test body, the database's, as today (the census found one harness comparison across types in the whole corpus).

## 4. How we work

**The fast loop, for every small change (minutes):** compile, then run only the tests that exercise the change: one test class (`--test_filter`), one corpus test (`bazel run //spec:corpus_one -- <duckdb|h2> host <test>`), one PCT suite. No full round, no regeneration, per small change.

**At the end of a big step, all at once, in parallel:**
- the full round and the local gate, in a separate session, so this one keeps working;
- an independent audit by a subagent on Opus 5.5, given the diff and the rules;
- a push to the pull request, so CI runs on every operating system;
- the heavy and manual lanes, in another session;
- the warm database lanes (the server and cloud databases).

Work on the next step continues meanwhile; what the checks find is fixed in the next commit.

**Parallel sessions** start at steps 8 and 9, one session each. Before that the work is one line.

**What still comes to the user:** every test whose answer moves, and every new difference from legend-engine, with its register row. They are gathered and brought at the end of each big step, not one at a time.

## 5. The plan, in order

| # | Step | Done when |
|---|---|---|
| 1 | **Finish what is ruled.** The edge's decimal Float literal deleted (in progress); B2's CSV work merged when it is pushed; the dead choices deleted; `acos`/`asin` raise; H2's lateral; SQLite's `chunk`; PCT channel A on compiled legend-pure | its tests pass |
| 2 | **Prove the two unproven parts.** (a) The stamp, with nothing reading it, then compared against today's `substring` rewrite on its own tests and pure/v1's answers. (b) The relational `=` moved to its own function, then the comparison setting switched on inside functions and mapping code with legend-engine's value | (a) the two mark the same code, or every disagreement is listed; (b) no test moves |
| 3 | **The table, a row at a time.** First the rows that exist today in other forms (plain raises, the parses, `isNumeric`, `parseDecimal`, `parseJSON`). Then the stamp's two columns: pure/v1's `substring`, `indexOf` and `toOne` move to the table and the old rewrite is deleted; the corpus gets legend-pure's meaning in the test's own code and legend-engine's inside `execute()` | answers unchanged for the first rows; then each move listed |
| 4 | **Lite is strict inside functions and mapping code.** A comparison across types there is a compile error, as in the query. The door's comparison rewrites are deleted. Lite's own tests that compare across types are fixed (four M2M tests known) | the list of what breaks is ruled; the compile check and the run agree |
| 5 | **legend-engine's meanings, a row at a time:** unchecked operations, `round`, `dateDiff` in weeks, `splitPart`, `toDecimal`, `rem`, the casts' text, `parseDecimal` per database, an enumeration by name, counts over a key. PCT's Pure run stops inheriting legend-engine's meanings | PCT's legend-engine run on DuckDB: nothing legend-engine passes fails |
| 6 | **The rest of the one equality.** A value whose type is known only at run time carries its type; asserts are planned as plain comparisons; the judges' separate comparison code is deleted | both judges agree; about 2,500 lines gone |
| 7 | *(reserved: anything steps 2 to 5 show is needed before the parallel lanes start)* | |
| 8 | **Presentation at the edge** (its own session). Upstream's text written in Java from typed values; the SQL that builds it deleted | PCT's printed-table tests and legend-engine's result answers unchanged; about 2,000 lines gone |
| 9 | **Test machinery out of the product** (its own session). The SQL-text check as test tooling (`edition-homework/b3-step6-design.md`); about 60 classes moved | the corpus's results unchanged; about 3,400 lines out of the core |
| 10 | **Clean up and lock.** The folds cut back to the rule (measured first, the list ruled); the old switch machinery deleted (`PlanStyle`, the feature flags in the lowering, `LoweringForms`, the edge's dialect subclasses); the guards on; legend-engine's meaning refused by name on databases where it was not checked; DataCube's legend-engine workarounds; `EngineWhereZones` | the build fails if audience knowledge returns to the product |
| 11 | **Land.** Full rounds on every gate, an independent audit, one pull request to `main` | the user's word |

**Size, an estimate from the pieces, not measured:** about six weeks one after another; about three and a half with steps 8 and 9 in their own sessions.

**If step 2 fails:** (a) the stamp does not hold: today's `substring` rewrite stays on pure/v1, as the user ruled before (amendment 24), and the corpus keeps one table per test body; (b) the comparison setting moves tests: each is read before anything else is built on it.

**Not in Stage B:** editions per project; PMCD and two-way SQL plans; checking legend-engine's answers on databases beyond DuckDB and H2; a lite-native service test runner.

**Open, with no owner:** `//pct:pct_channel_b_twice_h2_relation` (a manual lane) fails 4 cases on a clean tree.

## 6. State on 2026-11-25

- **On `main`:** B1, the one error model, item 3 (PR #132).
- **On `claude/compat-b3-nufdlk`** (91 commits ahead of `main`, then B3's handover `296732e2`): B3 steps 1 to 5a, B2a, B2b through S5c, every record.
- **On `claude/modest-galileo-h6swr4`** (this session's, on top of it): step 1's change, its regenerated pins, its gates record; these records.
- **Step 1:** the confirming round running. Its audit found the code complete and two faults in the documents, to fix: register row S20 and the differences page say "every database and door", but ClickHouse, BigQuery and Snowflake still write a DECIMAL in the product's own dialects; and the deletion changes Postgres, Databricks and Trino at the edge too (legend-engine answers `0.3` there by its code, read, not measured), which the row does not yet say.
- **Found on the way:** the corpus edge goldens were stale on the branch by one line a lane (`testEnumInRelation`'s verdict name), so those two golden tests were already red; recorded in `docs/GATES.md`.
- **Awaited from B2** (another account): `toCSV` exact and S7a, `engine-java-part.md`, the S6a stash as a patch.
- **The warm lane sessions** named in `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md` §9 answer "not found" from this session, and their environment is not among this account's; the user to say how to reach them.

## 6a. Progress log (kept current; newest last)

**Step 1 (2026-11-25).** The edge's DECIMAL Float literal is deleted, with its regenerated pins and its gates record;
B2's branch (`a047d2bd`) is merged; the audit's document fixes are in. Checked by a full round (292 of 292; the local
gate 555 passed, the 14 root-only and one test the machine killed, which passes alone), an independent audit
(`r3/audit-step1.md`) and three lanes (`lanes/`). **One moved test awaits the user's ruling:** PCT's
`testFloatStdDev` on Postgres now fails (`3.1399999999999997` for Pure's `3.14`): Postgres's own `stddev_samp` over
doubles is one unit low, confirmed outside the driver and Java (`lanes/round1-postgres-trino.md`). The choices put to
the user: record it as a Postgres limit, or write the two-pass formula on Postgres. The pull request waits on it. Not
started: `acos`/`asin`, H2's lateral, SQLite's `chunk`, PCT channel A on compiled legend-pure.

**Step 2a, the stamp: proven on the main road (2026-11-25).** Built: the field `TypedNativeCall.outsideStore` (an
unstamped call prints and hashes as before); the 18 hand-rebuild sites keep it; the stamping in
`UserCallInliner.inlineBody` (a `let`'s value, or a last statement, that reads no store; a lambda's value never; a
term an earlier `let` bound keeps its own stamp); `lowering/EditionTable` and `LiteEditionTable`; the lookup in
`Scalars.lower`; `edge/EngineTable` with `substring`, `substr` and `indexOf`; the five dead lowering choices deleted.
`EngineTableShadowTest` plans nine of the old rewrite's own test queries both ways and gets the same statement from
each. The guards (nine tripped, all fixed in one pass from `r3/research-guards-step2a.md`), the census, every edge
test and the product's SQL size pins pass with the table inert. A temporary switch, `-Dlegend.probe.stamp=true`
(`PureText.STAMP_PROBE`), puts the table in the old rewrite's place at pure/v1's door; **it must be removed before
this lands.**

**What the wider run showed, and the work list before the old rewrite (A3) can be deleted.** With the switch on,
1,044 of 1,065 edge tests pass. The 21 that differ:
1. **Six: the old rewrite's refusals no longer happen** (a database where legend-engine's answer is unchecked; a graph
   fetch's computed property; a function value it cannot follow). The door must keep them.
2. **Two: `toOne` of an empty value.** The static-empty arms (`Coercions.java:63-67`, `:174-179`) are not routed
   through the raise choice; routing them is small (`r3/research-engine-rows.md`).
3. **Five: tests that inspect the rewritten tree for the old rewrite's function names.** To be rewritten to check SQL
   and answers.
4. **Seven: the test-data routes, the Arrow road and the activity SQL answer differently.** The old rewrite renames
   calls in the tree, so it works on every road; the table works only where the lowering is handed the door's table.
   Some roads pin the product's style (`ProductStatements.lowerTestDataSeed`, `PlanMaker` about line 955,
   `Compiler.lowerResolved`), and some build their own inliner. Each must take the door's table and stamp. The stamp
   should ride on the per-query compiler so no road has to be told.
5. One: this session's own sanity test, meaningless with the switch on.

**What the research changes** (`r3/`):
- **The table needs the database** (`r3/research-engine-rows.md`): at least six rows differ by database. `PlanEnv`
  already knows it.
- **The stamp must also reach the raise choice,** and whole-statement choices (a keyed count, the null-unsafe flag)
  need a slot of their own.
- **Stamping a corpus test body** needs a walk inside the inliner that knows the frames' nodes
  (`r3/research-strict-and-corpus.md`, part B): the draft's statement-level test would stamp a spliced frame's own
  query code. It is dormant: only pure/v1's switch turns stamping on.
- **Strict typing inside functions** (the same report, part A): three kinds of synthesized function are wholly
  relational and can carry a flag; a `~func` mapping, a cross-store association and a model join mix the two
  languages; and lite's own exposure includes DataCube's timeseries library, about 105 comparisons of a value column
  with an integer literal, unmeasured.
- **Rows that need posting to the user before they land:** H2's `isNumeric` in legend-engine 4.145.0 is a regex, so
  register S370 is stale; `parseJSON`'s leniency is not in legend-engine's SQL; DuckDB's `split_part` differs from
  Pure beyond the empty token; `dateDiff` differs for every unit, not only weeks; legend-engine's H2 `parseDate` fails
  every PCT input.

**The lanes (2026-11-25).** Six sessions in Legend Dev on Opus 5.5 at medium effort, each with stored logs
(`lanes/`): Postgres and Trino, Databricks, SQLite and MongoDB, Snowflake, BigQuery have reported; MariaDB, MySQL and
ClickHouse is running. Found: the Postgres test above; Snowflake's exact Float text is up to 38 digits
(`lanes/baselines-c06ebbab.md`), for the presentation step; 19 BigQuery channel B tests fail on `main` on an untyped
NULL in the harness's batch union, suggested to the user as a separate task. The lanes' credential script is
`tools/cloud-lane-credentials.sh`; the way of working is `AGENTS.md`, "The fast setup".

**Steps 2 and 3, the table and the stamp in the old rewrite's place (2026-11-26, on the session branch; not yet
landed).** legend-engine's door no longer rewrites the typed query. The old rewrite and its detector
(`edge/PureMeaningRewrite`, `edge/PureMeaningCheck`) are deleted, and with them the temporary switch.

What was built to get there, after the wider run's work list above was read test by test (most of the 21 were tests
that inspected the rewritten tree, or expected a refusal before planning; three were real):
- **A door's edition has two sides.** The planner's side (`planner/Edition`, the kind a door's style hands over; the
  product's is `LiteEdition`) answers what the planner needs: what is stamped, whether written flags are read, and
  the lowering's table for the statement's database. The lowering's side (`lowering/EditionTable`) is one lookup,
  `lowersAs(call, features)`, and sees no database: the architecture guard on the lowering's dependencies refused a
  first draft that handed it one. `EngineTable` has four rows (`substring`, `substr`, the two-argument `indexOf`, and `toOne` of an operand
  that is always empty) and refuses by name, while the statement is planned, a call a row answers on a database where
  legend-engine's answer is not yet checked.
- **At legend-engine's door a statement's own feature flags reach its lowering.** A `withFeatureFlags` call written
  in a query is read through by the resolver before the lowering sees it, so on the product's road its flags never
  arrived (the old rewrite read them itself, at the door). `ProductStatements.prepare` now reads them from the
  statements as written, where the door's edition says the door reads them (first `Edition.readsWrittenFlags`:
  legend-engine's does, lite's own doors do not, as before). After the audit the door reads only the flag its table
  has a row for, as the old rewrite did (`Edition.writtenFlagsRead`).
- **The seed is planned in the door's style** (`ProductStatements.lowerTestDataSeed(query, runtime, style)`).
- **A model-to-model mapping's transforms are stamped where the resolver composes them.** legend-engine runs them in
  its own Java under a graph fetch and over a model connection, and pushes them into the SQL in a TDS query over a
  chain. The edition states that rule (`Edition.transformsOutsideStore(fetchesAGraph, chained)`), the planner asks
  it per statement, and the resolver, made with the answer, stamps the transforms (`ClassSources.build`). The rule is the old detector's, statement-wide, unchanged.

**What changes for a caller of legend-engine's door.** Nothing the old rewrite answered changes: while both stood,
the two were planned side by side over the old rewrite's own test queries (25 of them at the end) and wrote the same
statement for each. Three kinds of code the old rewrite **refused by name are answered now**, because everything that
becomes SQL passes the lowering. Each is to be confirmed with the user:
1. a graph fetch's computed (qualified) property: the database's meaning, as legend-engine computes it (asked of
   legend-engine 4.145.0, E-M1);
2. a model-to-model mapping's transforms in a TDS query over a chain to a store: the database's meaning, as
   legend-engine pushes them into the source query (read in `chain.pure:111-116`; not yet asked of the engine runner);
3. a function value read out of a collection and applied in a store query: the database's meaning, by the one rule
   (legend-engine's own answer not asked). A function value a `let` holds is placed the same way, where it is
   applied;
4. a statement that builds on a relation a `let` bound and did not execute: the database's meaning (the old rewrite
   left it legend-pure's; legend-engine cannot compute it anywhere but the database).
A refusal on a database not yet checked now names the first call it meets, not every call of the query.

**The tests.** `PureMeaningCheckTest`, `PureMeaningRewriteTest` and the side-by-side test are replaced by
`EngineTablePlacementTest`: each case plans in the door's style and is read against the same query planned with the
product's table (the same statement means no row applied). `PureMeaningEngineTest` keeps every route's answers and
gains the three newly answered cases and the refusals. `EngineSubstitutesTest` keeps the natives' dialect, answer and
internal-name tests; the deleted substitutes' tests went with them.

**The independent audit, and what it changed (2026-11-26; `r3/audit-step2-3.md`).** It confirmed the product's road is untouched (an unstamped call compares, hashes and prints as before; the product's edition answers no to every question) and found gaps at legend-engine's door, each fixed and pinned by a test in `EngineTablePlacementTest`:
1. *A `let` whose value becomes a lambda only once reduced* (`[{..}]->toOne()`) was stamped, and its code carried the stamp into the store query. Now a function value is never stamped where it is bound and is placed where it is applied; one that does not reduce to a written lambda is refused by name. (The old rewrite refused all of these.)
2. *A runtime of model data alone* counted as a checked database, because it runs on the platform's own engine. It names no database now, so a call a row would answer in the query's own code is refused by name, as before the table.
3. *A statement that builds on a relation not yet executed* was stamped, as the old rewrite also left it alone. It is that query's code now and gets the database's meaning (an answer that moves from the old rewrite's, in legend-engine's direction).
4. *Code legend-engine's own Java runs that the first cut did not stamp:* the qualified properties a graph fetch reads off a class a model-to-model mapping makes; a checked graph fetch's constraints; the code of functions a transform calls.
5. *The feature flags a query writes:* the door reads the one its table has a row for (`CORRECT_SQL_SUBSTRING_INDEXING`), as the old rewrite did. The first cut read them all, which would have turned on `LEGACY_SQL_NULL_UNSAFE_EQUALS` and refused unknown flags at that door; that is withdrawn.
6. *The hosts' two plan halves* (`HostAnswers.arrowPlan`, `jsonPlan`) took legend-engine's meaning from whatever style the caller handed. They refuse a style that is not pure/v1's now. Production already handed it; three tests handed legend-pure's door's style and were corrected, one of which pinned a raise (division by zero) that pure/v1 has written plain since B7.

Recorded and not changed:
- The seed of a test-data request is planned whole in the door's style, so its other forms are the door's too (plain raises, legend-pure's texts), not only its table.
- Roads that plan a frame's own query inside a pure/v1 request (`execute(...)` written in a query, the plan-text natives) get the table's rows and no stamp, as the old rewrite also gave them the database's meaning throughout.
- A `let` bound to an executed relation, built on by a later statement, keeps the old rewrite's answer (outside); what legend-engine does there has not been asked.
- A graph fetch whose tree mixes classes a model-to-model mapping makes with relationally mapped ones stamps every qualified property in it.
- No test model exercises a transform that calls a function: lite's composition refused the shapes tried.
- Design note from the audit (its F5): the door's edition is read after the type-checker in four places (the statement road, the inliner's stamp, the resolver's stamp, the lowering's lookup). That is this plan's design (section 2), and it stands beside the wording of AGENTS.md's edition exception ("only the typer reads it"), which was written for the earlier design; the two are to be reconciled when the rules are next amended.

**Step 3, the plain raises as the table's (2026-11-26).** Which of the product's checks a door's audience writes was
a lowering form of the style (`LoweringForms.raise`, with the edge's set of plain kinds), which knew the kind of raise
and not the call. It is the door's table's now (`EditionTable.checks(kind, call)`), so the stamp decides it: at
legend-engine's door a check its SQL does not write (a zero divisor, a square root of a negative, a shift out of
range, a `toOne` over several, a parse) is the plain operation inside a store query and is written in a host-side
`let`, where legend-engine's own Java raises as Pure does. `LoweringForms.raise` and the edge's two form instances
are gone (one remains). **Answers that move at legend-engine's door, to confirm with the user** (the independent
audit of the step listed them in full, `r3/audit-step2b-3raise.md` finding 4): wherever the planner stamps code as
outside any store query, an operation legend-engine's SQL would not check raises Pure's error now; until now it was
the plain operation in lite's SQL, and the database's own error or answer. That is:
- a host-side `let` (`let x = 5->rem(0)`);
- a whole query that reads no store (`|5->rem(0)`, `|sqrt(-1.0)`, `|[1, 2]->toOne()` sent to pure/v1);
- a model-to-model mapping's transform under a graph fetch or over a model connection, and a constraint a checked
  graph fetch evaluates (a `rem(0)` written there).
In each, legend-engine's own Java runs the code and raises as Pure does, so this is closer to legend-engine; none was
measured against the engine runner yet. A cast that can never succeed and a slice are their own nodes and carry no
stamp: they stay plain at that door wherever they are written, as before.

`acos`/`asin` out of range (ruled 2026-11-25 to raise as legend-pure does) is drafted on `claude/edit-next` with
in-domain functions per dialect, so the product's check is the one check in the SQL. It waits on the corpus's door:
legend-engine's own tests (`testFilterUsingArcCosFunction`, `...ArcSin...`) expect the database's answer inside the
query, so the raise must be plain there, which needs the corpus's edition and its stamping (below).

**Step 2b, the comparison setting in the type-checker (2026-11-26).** A door now hands the query its comparison
setting when it types it (`TypedQuery.comparing`; `compiler/spec/CrossTypeRule`), and the type-checker asks the
setting at the one place it types a comparison across types, for the query (`inQuery`) and for every body the query
reaches (`inBody`: a function's body, a derived property, a body typed into its call). The two doors' rewrites of the
typed query after typing are gone; `edge/EngineComparisons` and `edge/PureComparisons` are the two settings. Nothing is
answered differently: lite's setting leaves the marker in the query (its planner refuses it by name) and answers a
body's comparison with the call; legend-engine's answers the call everywhere; legend-pure's answers Pure's way in the
query and the call in a body, all as before. A test (`CrossTypeRuleTest`) lists what a recording setting is asked.
- **What step 4 changes from here:** lite's answer in a body becomes the compile error (the list of what breaks,
  measured, is `edition-homework/step4-exposure-2026-11-26.md`, for the user's ruling); legend-pure's answer in a body
  is a later row, each test it moves read first.
- **Not yet told apart, and it must be before a body's answer differs from the call:** the relational mapping
  language's own `=` (the normalizer writes it as Pure's `equal`, in a relational class mapping, a join and a view,
  and in the enumeration decode inside a `~func` mapping). It reaches the setting as a body's comparison today. The
  fix is the plan's: a flag on the three wholly relational kinds of function and a lite-only function for the decode.
- **Seen on the way:** inside a body typed into its call, two literals compared across types (`1 == 1.0`) are folded
  by `StaticFold` before the setting is asked. Which answer the fold gives is not checked here; step 4 reads it.
- **A side effect to know:** the query a door admits is a new typed query (its own type-checker), so a test harness
  that remembers a query by identity from `Compiler.query(text)` no longer finds the admitted one (the lean-SQL
  capture, for product queries only; `JudgeJsonTest` compares the bodies now).

**Lite's own clients under the query-level rule (2026-11-26).** The measuring session found the python tests red on
this branch, and not on `main`: 19 cases in `//python:engine_test`, `edge_test` and `notebook_test`, which the local
gate does not run (they are the warehouse gate's). All come from B3 step 5a, which made a comparison across types in a
query a compile error:
- **12** were the fix step 5a itself made: one shared test query went from `> 5` to `> 5.0`, and a parsed Float
  literal is read exactly (as a Decimal), which the tests then wrote with the standard library's JSON writer. They use
  the library's own writer now.
- **6** are what a person types into a filter or a calculated column: a Float column against a whole number
  (`$x.qty > 0`, `$x.pnl->toOne() < 0`). Written `0.0` now. **This is the rule's cost to a user, and it is the
  question for the user below (a literal against a column).**
- **1** is DataCube's own code: its Oversample wrote `rank <= count * (100 - n) / n`, an Integer against a Float. It
  writes `rank * n <= count * (100 - n)` now, in whole numbers and exact, and reads that form back
  (`datacube/src/row-sample.ts`). No DataCube test planned an oversample, which is why its lanes were green.
Pushed as `593aca57`. The measuring session ran it: the python group 17 of 17 targets (the three that failed pass:
`edge_test` 24 tests, `engine_test` 358, `notebook_test` 32), and the DataCube and wasm group 228 of 228, with
`typecheck_test` and `flow_sheet_test`. The oversample plans through the compiler now and keeps the same rows (with
n = 25 and one row of interest: rank * 25 <= 75, the three rows the division kept).

**The independent audit of step 2b and the plain raises (2026-11-26, `r3/audit-step2b-3raise.md`):** no blocker, no
major finding; the product road and the SQL inside a store query at legend-engine's door are as before. Fixed from
it: legend-pure's refusal of an equality whose two sides may both be empty was raised while the query was typed, where
the search of a call's overloads could meet it inside a candidate it then drops, so it is raised once the query is
typed, as before the step; the type-checker looked for a body's comparisons across types although no setting answers
them yet, which asks the model whether two classes are related (a broken superclass refuses), so a setting now says
whether it reads bodies and none does; the door guard files the setting as a callback, not an option; stale comments
and an unused helper left; tests added for each.

**Step 3, legend-engine's own tests' door: the corpus (2026-11-26).** The relational corpus is legend-engine's test
suite: each test is Pure code, which legend-pure runs, around queries it hands to legend-engine (`execute(|...)`,
`toSQLString(|...)`, `executionPlan(|...)`, a `->from(...)`). Until now the corpus planned under the same style as
PCT, with lite's meaning on both sides. It has its own style now (`PureText.ENGINE_TESTS`), whose edition is
legend-engine's table with the test's own code stamped (`EngineTable.TESTS`, `Edition.stampsTestBodies`):
- **The queries get legend-engine's meaning** (the table's rows, the plain operations), as at its API.
- **The test's own code keeps Pure's.** Every call of a test statement is stamped as outside any store query, except
  what is a query's: anything under an execution context (a `from`; a frame's query is wrapped in one), a reference to
  a frame's CTE, and a call that hands legend-engine a query. A value the test computes in a `let` and a query then
  reads is stamped where it is bound, so the query reads the value with Pure's meaning
  (`UserCallInliner.ofTestBody`, `stampAroundFrames`, `inlineFrameQuery`).
- **The judges' sides** are the test's own code too, and are stamped the same way, with the mapping code
  legend-engine's own Java runs (`ProductStatements.sideStamp`).

*Measured first, with no stamping at all* (a separate session, both lanes, on `212b167c`): nothing newly failed, and
nine tests newly passed on each lane. Three of the nine passed for the wrong reason: a test's own
`$planAsString->indexOf(...) != -1` was answered by the database's position function, which is 0 on a miss, so the
assert could not fail. That is what the stamp is for.

*With the stamp* (this session, both lanes): **six tests gained on each lane, none lost.**
- Inside the query, `substring` is the database's own, as legend-engine sends it; lite's version raised Pure's range
  error: `testFilterUsingSubstringFunction`, `testFilterUsingParseIntegerFunction`,
  `testFilterUsingParseDecimalFunction`.
- The SQL text of a query with `substring` has no range check, as legend-engine's has none:
  `testToSQLStringSubstrFoldsLiteralIndexes`, `testToSQLStringSubstrWithComputedIndex`,
  `testToSQLStringSubstringIsUncorrectedByDefault`.
- The three that passed for the wrong reason without the stamp (`testPlanForDateTimeConstantParameter` with a GMT, an
  EST and no time zone) fail again, as before the step and rightly: the text they search is lite's plan text, which
  writes a timestamp `TIMESTAMP '2019-04-08T18:00:00'` where legend-engine writes `TIMESTAMP'2019-04-08 18:00:00'`
  (upstream's SQL text is never promised, CP-4).

*Limits, each to close or to record:*
- A `let` written INSIDE an executed query's lambda that reads no store is legend-engine's own Java's (as at pure/v1),
  and is not stamped yet: it gets the database's meaning.
- An assert over every element of a list (`forAll`, the unrolled elements) reduces each element with an ordinary
  inliner: the test's own code there is not stamped.
- PCT's legend-engine run still plans under pure/v1's style.
- The lowering asks the table for the statement's database; every corpus statement named a checked one (no refusal on
  either lane).

**Still open from the work list:** a runtime's own model-chain connection is not counted as "chained" (the
old detector did not count it either).

## 7. The user's words on this plan (2026-11-25, late night)

1. On the `substring` rewrite: "Substring you can leave until 16 yes" (amendment 24). Then, on the stamp: "why cant we stamp the substring if database or not when we know it and then use that at the right time? dont we need something like this to do corpus blended pure and engine?" The stamp is §2.3; if step 2 proves it, the old rewrite goes in step 3, and if not, amendment 24 stands.
2. On the folds: "Folds we need to fix but can be a later step in the plan at the end" (amendment 25). Step 10.
3. On the comparison setting: "is one in the typer enough? what about m2m and ~func?" Answered in §2.1.
4. On SQL text against answers: "how much do we care that the sql did not change? i hope a lot of the sql gets leaner for example - do we care about the sql or the test answers? i wouldnt spend time building yet another test harness when we already have so many (core, PCT, core_relational corpus, stress corpus) unless you think this is giving us something else important". No SQL-text capture is built; the answers judge (§1).
5. On speed: "make sure we are doing much faster dev loops than you are doing now - right now every small chnage you have done is waiting over an hour - we have to go much much much much much faster than that - i would do very fast loops and then at end of a big chunk or big step or big milestone do in parallel round of testing, independent audit, update a PR so CI can run in parallel early, and the subset of the manual tests and heavy tests in subagent or another session, and use the warm lanes here that have all the server databases setup with credentials - can do all of those in parallel in big chunks - but otherwise would go really fast with very fast tests that test the small thing". §4.
6. On parallel sessions: "once we get to 8 and 9, we can parallelize where you can spin up new sessions for those to specifically work only on those - right now assuming before we get there not much benefit from parallelizing".
7. On what happens next: "also lets write this all down so we dont lose it, and then after we go through this one more time i want you to just blast through the whole thing and keep going until done".

**Not yet ruled:** this plan as a whole (the user reads it once more first); that comparisons are decided at the type-checker and SQL differences at the lowering, where an earlier ruling said one place for compiler rules; what "keep going until done" does with a test whose answer moves (§4 proposes: gathered and brought at the end of each big step).

## 8. Decisions still to come

Each arrives with what was measured:
- the scalar lift (step 8);
- PCT's legend-engine run: everything through SQL, or legend-engine's own pre-evaluation of some functions;
- comparisons whose types are known only at run time, under legend-engine's meaning;
- SQLite's Decimal arithmetic under legend-pure's meaning;
- DataCube's legend-engine workarounds (step 10);
- the list of folds that stay (step 10);
- the wording of the rules of record, whose edition paragraph predates this design.

## 9. The evidence

- `edition-homework/r3/trace1-table-inventory.md`: every place lite chooses by audience today, and which can be table rows.
- `edition-homework/r3/trace2-marker-risk.md`: why a comparison marker can't be carried to lowering, and where relational and Pure mapping code can be told apart.
- `edition-homework/r3/trace3-corpus-boundary.md`: every road an `execute()` query takes to lowering, and where its boundary is lost.
- `edition-homework/r3/audit-step1.md`: the independent audit of step 1.
- `edition-homework/r3/design-sql-capture.md`: the SQL-text capture's design, not built (§7 item 4); kept for its notes on how the existing harnesses plan each door.
- Earlier: `edition-homework/third-review-v3-2026-11-25.md`, `r2/sql-inventory.md`, `r1/`.
