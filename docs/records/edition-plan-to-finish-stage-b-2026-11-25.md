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

The lowering already takes an object from the door on every path (`lowering/Styled.forms()`, built by `planner/PlanEnv`) and asks it at about 20 places. The table is that, made regular: a function mapped to another product-named rule (`substring` to the rule `SUBSTRING_AS_GIVEN`; a rule of the lowering's, `lowering/NamedRule`, since 2026-11-26, where it was a native of the language until then), a raise kind mapped to checked or plain, and a few whole-statement choices. It reaches functions and mapping code because by then everything is one tree.

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

**Before a push to the pull request's branch (learned 2026-11-26, from two CI readings that found what targeted
local runs had missed):** the targets a change touches are not enough. The fixed set, run here first: `//core:guardrails`,
`//core:guardrails_edge`, `//core:census`, `//docs:all`, `//core:core_edge_tests`, `//core:core_tests_integration`
(with its `_sqlite` and `_h2` lanes when a dialect moved), `//core:lean_sql_baseline_test`, `//spec:spec_tests`,
`//spec:corpus_duckdb`, `//spec:corpus_h2`, the two `//spec:lean_sql_corpus_*`, `//pct:engine_lane_duckdb_test` and
`_h2_test`, `//pct:update_edge_contract_tests`, `//core:update_edge_pure_v1_tests`, `//core:update_generated_tests`;
then `--lockfile_mode=error //gates:local` (about 35 minutes; 14 Postgres-backed targets fail only because the
container runs as root). Every push restarts CI on every operating system, so a push is made when this is green, not
before.

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

**Open, with no owner:** `//pct:pct_channel_b_twice_h2_relation` (a manual lane) fails 4 cases on a clean tree. *(Owned and fixed 2026-11-26: section 6a, the heavy gate attributed.)*

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

`acos`/`asin` out of range (ruled 2026-11-25 to raise as legend-pure does) is built on `claude/edit-next` with
in-domain functions per dialect, so the product's check is the one check in the SQL. It waited on the corpus's door
(legend-engine's own tests, `testFilterUsingArcCosFunction` and `...ArcSin...`, expect the database's answer inside
the query), which exists now. It waits on the user for one thing: its SQL size pins grow (section 6b, item 7).

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

*The independent audit of the step (`r3/audit-step3-tests-door.md`), before anything was pushed:* the product road
and the other doors are untouched (no counterexample). At the tests' door it found three faults, fixed with tests:
- **A query written as a `let` and then handed to legend-engine** (`let q = {|...}; execute($q, ...)`, about 427
  such lambdas in the corpus) was stamped where the `let` bound it, so it got Pure's meaning. A function value a
  `let` binds is left alone now: handed to legend-engine it is the query's; applied by the test's own code it is
  stamped there. The measured lanes could not see the fault (the tests it touches fail before and after).
- **The stamp told a frame's code by the identity of its nodes,** and nodes are shared (an untouched subtree, a
  compiled function's body), so a helper used in both a query and the test's own code could be left unstamped in the
  test. The stamp reads shape alone now (a query under its execution context, a frame reference, a call that hands
  legend-engine a query).
- **A slice with inverted bounds and a cast that can never succeed are not calls and carry no stamp,** so the test's
  own became the plain operation. At the tests' door their checks stay written on both sides, as before the step.
Also taken from it: a `let` INSIDE the executed query that reads no store follows the query rule, as at pure/v1
(legend-engine's own Java's, Pure's meaning).

*Limits that remain, each to close or to record:*
- An assert over every element of a list (`forAll`, the unrolled elements) reduces each element with an ordinary
  inliner: the test's own code there is not stamped.
- The values a test passes to `executeLegendQuery` as variables, and the arguments of a helper that builds a query
  lambda, reach the query unstamped.
- A judged side that reads two frames of different shapes (a graph fetch and a TDS over one model-to-model chain)
  decides once which mapping code is legend-engine's own Java's.
- The table is made for the lane's database (every corpus statement plans under the lane's runtime), not for the
  runtime an `execute()` names; a corpus lane on a database where legend-engine's answer is not checked (Postgres)
  refuses every query a row answers.
- A test's function value that is computed some way other than by writing the lambda is left unstamped whole (at
  pure/v1 it is refused by name).
- PCT's legend-engine run still plans under pure/v1's style.

*Answers that move at this door, for the user:* inside the queries the corpus's tests hand legend-engine,
`substring`, `substr`, the two-argument `indexOf`, `toOne` of an always-empty operand and the checks legend-engine's
SQL does not write are legend-engine's now (they were lite's). Six tests a lane gain from it and none is lost; no
register row changes (the rows on these functions already say what legend-engine does and what lite does).

**Step 1's leftover, SQLite's `chunk` (2026-11-26).** `chunk` with a written size writes a literal pattern
(`'.{1,5}'`), where it composed the pattern in SQL for every size; a computed size still does. SQLite's
regular-expression functions take a literal pattern alone, so PCT's own checking code (`...->chunk(1)->sort()->
joinStrings('')`) runs there now: ten tests of the Relation suite pass on SQLite's channel B (the eight
`testSimpleGroupBy_*`, `testOLAPAggStringWithPartitionAndUnboundedWindow`, `testJoinStrings_GroupBy_Unordered`).
Their ten rows left channel B's ledger and the edge outcomes file was re-recorded (ten rows from error to pass, no
other row of any file). The SQL is shorter on every database; no size pin moved. Checked: 49 of 50 targets on the
change (the fiftieth was the outcomes file), then the goldens again.

**The same change on the server and cloud lanes (2026-11-26, round 4).** The lanes found the same ten tests pass
now on BigQuery, MariaDB and MySQL as well (forty tests gained across four databases); nothing differs on Postgres,
Trino, ClickHouse, Databricks, Snowflake or MongoDB. Their thirty rows left channel B's ledger. The MariaDB, MySQL
and ClickHouse lane also listed 28 older rows that pass on every one of its five rounds (MySQL 21, ClickHouse 5,
MariaDB 2): gains of the program's earlier steps that no server lane had run. They left the ledger too, with a dated
note.

**Step 1's leftover, H2's lateral join (2026-11-26; the user's ruling of 2026-11-25 on the Pure-mode gaps: "H2's
lateral as an inner join (4 tests)").** H2 has no `LATERAL`. Where a lateral subquery only filters its source by the
row before it (`... , LATERAL (SELECT * FROM t WHERE t.k = left.k AND t.v > 1)`), H2's dialect now writes the same
thing as an ordinary join on that filter (`JOIN (SELECT * FROM t WHERE t.v > 1) ON t.k = left.k`): a new pass of
H2's dialect, `sql/dialect/CorrelatedFiltersAsJoins`, from the design in `r3/design-h2-lateral.md`. It moves only
what is safe to move: the subquery has no DISTINCT, GROUP BY, HAVING, QUALIFY, ORDER BY, LIMIT or OFFSET, its columns
are plain columns of its source or stars, and no name of the row before is redefined inside. Any other lateral is
returned as it was and H2 still refuses it by name. Only H2's dialect has the pass, so no other database's SQL
changes. Four PCT tests pass on H2 on both channels (`testLateralJoin`, `testLateralJoinAreInnerJoins`,
`testLateralJoin_Chained`, `testLateralJoin_CorrelatedColumnNameAlsoOnInner`): their four expected failures left
`Test_LegendLite_H2_RelationFunctions_PCT`, their four rows left channel B's ledger, and the edge's two H2 answer
files were re-recorded (those four tests from H2's error to their answers, no other line). The pass has its own unit
tests (`CorrelatedFiltersAsJoinsTest`: the join it makes, a filter of the subquery's own kept inside, an inner column
read by the name it is projected under, every other shape returned untouched, the pass's place among H2's).

**The pull request's first CI reading (2026-11-26, #141 on `593aca57` and `2e56223f`), and what it found that the
local runs had not.** The local runs were the targets each change touches, so three sets of recorded files that only
the CI lanes (and a full round) compare were stale:
- **The corpus's recorded answers at the edge** (`spec/src/test/resources/edge/corpus_*_host_judge-host.tsv`): not
  re-recorded when the corpus got its own style. Re-recorded: the same 13 assert lines on DuckDB and on H2, the ones
  that step's per-assert comparison listed (the six gained tests' asserts pass; three plan-text tests that still fail
  carry legend-engine's unchecked `substr` in their message). No other line.
- **The legend-engine lane's scoreboards** (`pct/engine_lane/`: PCT run as legend-engine's audience, classified
  against legend-engine's own manifests). Re-recorded: nine error tests (five on DuckDB, four on H2: `testRemError`,
  `testSquareRootError`, `testRangeStepError`, the two shifts past 62 bits) and H2's four lateral tests move from
  "both fail" to "lite passes". The lateral four are the fix above. The nine are a side effect of the stamp: a PCT
  case reads no store, so at legend-engine's door it is planned as code legend-engine's own Java runs, and lite's
  checks are written. That is not how legend-engine's own PCT runs a case (its adapter sends the whole case to the
  database), so PCT's legend-engine run gets its own edition next, and these nine are expected to return to "both
  fail" with it. No test moved to "lite is behind".
- **Postgres's standard deviation on macOS.** The expected failure recorded for `testFloatStdDev` (register S421)
  failed on the macOS lane: Postgres built for arm64 answers `3.14` itself; the low digit is the x86-64 build's. The
  row is channel A's x86-64 lanes' alone now, and the register and the differences page say so.

**Not this branch's, found by the same reading:** the corpus lane on macOS fails five trigonometry tests in the last
digit (`acos`, `asin`, `atan2`, `tan`: DuckDB takes them from the operating system's maths library). `main` passes
on macOS only because the job reuses Linux's cached result; the pull request's job really ran on macOS. PCT's
recorded answers have per-platform overlays for exactly this; the corpus has none. Filed as a suggested task, not
fixed here. And `//datacube:live_channel_test` failed once on macOS (one case of 52, a timing-sensitive page test);
unconfirmed, watched on the next run.

**Step 5 begun, and step 3's remainder for PCT (2026-11-26).** Three pieces, built together:
- **A row names a rule, not a function.** Until now a row of legend-engine's table mapped a call to another native
  of the Pure language (`substringAsGiven`, `substrAsGiven`), a leftover of the old rewrite, which had to write a
  real function into the typed query. The table is read where the SQL is produced, so a row needs only a rule to
  name: `lowering/NamedRule`, a closed list (`SUBSTRING_AS_GIVEN`, `SUBSTR_AS_GIVEN`, `SQL_POSITION`, `TRUST_ONE`,
  and now `ROUND_AS_GIVEN`), each rule written in the lowering (`NamedRules`, one arm a name, no default).
  The two natives left the language: four overloads off the catalogue, the pins down (`INTERNAL_DESUGAR` 42 to 40,
  `NOT_UPSTREAM_MAX` 80 to 76, implementation kinds 742 to 738), and nothing for the name resolver to refuse.
  Step 5's rows therefore add no function to the language. **Two pins the user ruled were replaced, not dropped**
  (`ClaimRegistryTest`: "`substringAsGiven`'s one product claimer is Scalars", 2026-11-19, and its sibling's,
  2026-11-20): the same fact, that no product class chooses one of these rules, is now
  `ArchitectureTest.anEditionsTableAloneNamesARule` over the compiled classes of core, the hosts' doors and the
  edge, with a planted violation. No answer and no SQL changed with this piece.
- **`round` at legend-engine's door** (register S422; a delegation ruled matched, amendment 22). Inside a query sent
  to DuckDB or H2, `round` is the database's own, which sends a half away from zero (16.5 is 17), as legend-engine's
  own manifests record; a host-side `let` and lite's own doors keep Pure's half to even (16). On any other database
  the door refuses such a query by name, as it does `substring`.
- **PCT's legend-engine run has its own edition** (`PureText.ENGINE_PCT`, `EngineTable.PCT`). A PCT case reaches
  lite as the one expression legend-engine's own PCT adapter sends to the database, so all of it is a store query's
  code: nothing is stamped and every row answers. Until now the run planned under pure/v1's style, where a case
  that reads no store is legend-engine's own Java's.

**What the legend-engine lane shows with it** (`pct/engine_lane/`, each row read): 24 rows move to "both fail", and
on the delegations lite's answer is legend-engine's own manifest's, to the character:
- `substring::testStart`, `testStartEnd`, `indexOf::testSimple`, `indexof::testIndexOfOneElement` (DuckDB and H2):
  `5` for `4`, `1` for `0`, the database's own cut of the text;
- the two `round` tests (DuckDB and H2): `-17` and `17`;
- the nine error tests return to "both fail" (the database decides, as in legend-engine's SQL);
- three tests legend-engine refuses and lite answers, on DuckDB, now fail against Pure
  (`sort::testSimpleSortWithKey`, `testSimpleSortWithFunctionVariables`,
  `removeDuplicatesBy::testRemoveDuplicatesByPrimitive`): their key function is a `substring`, which is
  legend-engine's in this run.
- Nothing moved to "lite is behind" (1 on DuckDB, 6 and 27 on H2, unchanged: `testBigFloatAbs`, and H2 functions
  the product does not write yet). H2's four lateral tests stay "lite passes".

**The independent audit of these three pieces** (`r3/audit-step5-start.md` holds its findings): one blocker, the
generated claims ledger (regenerated); the rest fixed the same day: the guard reads the hosts' doors too and names
its limit (a class literal is not an access); every row's rule is tested on every overload it maps, in the place of
the deleted test that compared a row's native's signature with its call's; `round` at a scale is run on H2; and
the row's rule writes a SQL function of its own, `ROUND_RAW`, the database's own ROUND, in the place of
`ROUND_HALF_UP` (the audit's finding 4, and the corpus's SQL size check, which caught 14 characters a plan: untyped,
a Float `round(x, 2)` was cast to DOUBLE a second time. Typing `ROUND_HALF_UP` itself was tried and backed out: it
made twelve of the product's plans for a decimal `divide` larger. The database's own ROUND is also what
legend-engine writes, so a database checked later needs no new rule).

**Where step 5 stands (2026-11-26, branch tip `810dbfe7`, pushed to the pull request).** Checked here on that
commit: the guards and the census, the edge's, lowering's, SQL's, builtin's and compiler's tests, spec's tests, the
product's SQL size baseline (unchanged) and the corpus's (four `round` pins down, none up), both corpus lanes and
their recorded answers, the legend-engine lane's pins, the parser check, pure/v1's recorded answers, the generated
files: 42 of 42 targets, then 8 of 8 after the last change. A full round with the local gate is asked of the
measuring session on `810dbfe7`; its round on `b4c9bf0a` was clean but for the nine recorded files since re-recorded
(690 targets: 658 passed, 22 failed only as root and pass unprivileged, 9 stale recordings; the local gate 556
passed, 14 root-only). The database lanes were not asked for a round: the product's SQL did not move.

**The rows left, in the order the sheet gives (`r3/research-engine-rows.md`), with what each waits on:**
1. *The plain parses* (`parseInteger`, `parseFloat`): today a pass of the edge's dialect unguards them for every
   compatibility style, PCT's legend-pure run included (`edge/PurePrintForms.UnguardedParses`). As rows they are
   legend-engine's inside a store query alone; PCT's legend-pure run and a host-side `let` get lite's checked parse.
   Ruled (v3 section 5.3, S-2). **Built and parked the same day** (6b, item 12): it raises one ceiling that moves
   up only with the user's word.
2. *`isNumeric`, the one-argument `parseDecimal`, `parseJSON`*: today in the forms object the edge hands every
   compatibility style (`edge/EdgeLoweringForms`). Each needs a point settled first: H2's `isNumeric` in
   legend-engine 4.145.0 is a regular expression, so the `lower = upper` form and register S370 are stale;
   `parseDecimal` is `DECIMAL(5,2)` on H2 and `DECIMAL(18,3)` on DuckDB; `parseJSON`'s tolerance of a missing comma
   is legend-engine's own Java's, not its SQL's, so its row belongs to the stamped side (the table has rows for the
   unstamped side only today).
3. *`dateDiff`*: after the user's answer on the units (6b, item 11).
4. *`toDecimal` and `parseDecimal` on DuckDB*: a new SQL function for "the database's DECIMAL with no precision
   written"; H2 2.5.252's bare DECIMAL is not measured.
5. *A DateTime's and a Boolean's `toString` used further*, *`splitPart`* (DuckDB), *an enumeration by name*, *the
   keyed count*, *the null-unsafe flag*: as the sheet lists them.
`rem` needs no row (lite already writes the database's `MOD`; only the zero-divisor check differs, which is the
table's `checks`).

**The second CI reading (2026-11-26, on `810dbfe7`) and the heavy gate on `b4c9bf0a`.** CI: three new failures on
every operating system, each from a target outside the local set, fixed the same day: `//core:guardrails_edge` (the
named rules listed as the edge's entries into the lowering, `ArchitectureTest.PLANNER_ENTRIES`; `ROUND_RAW` sorted
PRODUCT in `SqlFnSortedTest`, by the user's standing rule of 2026-11-23 for a function only product code writes),
`//docs:update_dialect_capability_test` (the generated table, regenerated), and
`WindowReadThroughAListSQLiteTest` (it pinned SQLite's refusal of a `chunk(1)` shape; the shape answers since
`chunk`'s literal pattern). The macOS Postgres and DataCube failures of the first reading passed. The macOS corpus
lane failed again on the five trigonometry tests (not this branch's; 6b item 8). The heavy gate
(`//gates:heavy`, one pass in the measuring session, no baseline): 83 of 150 passed; 36 failures need cloud
credentials, the rest are being attributed against the merge-base before anything is concluded -- among them
`//spec:corpus_warehouse_verdict` (41 tests failing that its roster does not list).

**The full round on `810dbfe7`, the third CI reading, and the heavy gate attributed (2026-11-26).**
- *The measuring session's round on `810dbfe7`:* `//...` 690 targets, 660 passed; the 29 that did not are the 22 that
  fail only as root (27 of 27 pass unprivileged, the Postgres PCT suites and their channel B among them) and the
  three CI had found (the edge guard target, the generated dialect table, the SQLite chunked-window test in five
  integration targets), fixed at `c7ffb6af`. `//gates:local`: 549 passed, 21 failed (14 root-only and those seven).
  Against its round on `b4c9bf0a` nothing else changed verdict; the nine stale recordings pass. Both corpus totals
  are unchanged (DuckDB 2281 passed, 276 failed, 7 skipped; H2 2140, 422, 7); the size pins, the parser check, the
  legend-engine lane's pins, python and DataCube pass.
- *The local gate here on the fixes* (`c9b82f4d`): 556 passed, the 14 root-only failing.
- *CI on the pushed head* (`55f5e3d9`, gates run 45788970603): every lane passes on Linux (16), Windows (16) and
  Linux arm; on macOS 11 lanes had passed and 5 were still running when read, the corpus lane among them (6b item 8).
  The first green CI on Linux and Windows for this branch.
- *The heavy gate, attributed against the merge-base `c04c30ec`:* twelve of the thirteen targets that could have
  been this branch's fail there too, with the same tests and first lines: `//spec:corpus_warehouse_verdict` (the same
  41 tests its roster does not list), the reference lane and its pin, the manifest-world census, the line-spike
  test, DataCube's two measures, the python lock file, the small-heap benchmark, the scale measure, and MariaDB's
  channel B order failures (which differ run to run: not deterministic). None is this program's.
- *The thirteenth is the program's own:* `//pct:pct_channel_b_twice_h2_relation`, the manual lane that plans and
  runs each body twice. It passes on `main` and fails four tests on this branch (`project::testSimpleProject`,
  `testSimpleProjectList`, `testSimpleProjectWithEmpty`, `composition::testFilterPostProject`: "LIST_GET reached a
  dialect without a list encoding"), the item section 5 lists as open with no owner. Cause: since B3 step 2b the run
  reads a relation's printed text at the edge before the body, and the guard's planning half still planned the
  body before that read, so H2, which prints no relation itself, refused it. The guard now plans the body the run
  plans.

**Still open from the work list:** a runtime's own model-chain connection is not counted as "chained" (the
old detector did not count it either).

## 6b. Waiting on the user (gathered 2026-11-26)

1. **A number literal against a number column** (step 4's design question, and the equality rule's cost to a user
   today). The rule as ruled makes `$x.qty > 0` a compile error when `qty` is a Float: write `0.0`. Measured: 6 of
   lite's python tests, 63 of the 65 places in DataCube's timeseries library, one M2M test model, the `CreateClass`
   tool and a stress service are exactly this (`edition-homework/step4-exposure-2026-11-26.md`). 23 of the library's
   are in functions generic over the caller's column, where no literal is right for every caller. Options:
   - (a) keep the rule: a literal has its written type; lite's own code is fixed place by place, and the generic
     functions convert (`->toFloat() == 0.0`);
   - (b) a number literal written against a value of another number type reads as that type when it is exact there
     (`0` against a Float is `0.0`, `50000` against a Decimal is `50000D`, `2.375` against a Decimal is `2.375D`);
     two columns, a column and a computed value, or a literal that is not exact in the other type (`1.5` against an
     Integer) stay a compile error. Recommended: it keeps "a value is what it denotes, not how it was written", and
     it is how most typed languages read a number literal;
   - (c) numbers of different types compare by value everywhere (undoes the ruling for numbers).
2. **Step 4's other places** once 1 is ruled: the `CreateClass` tool copies a request's comparison into the model it
   generates (refuse at the request, or leave to the compile error); `$v != TDSNull` in a function (leave the SQL
   null out of the rule, or write `isEmpty()`).
3. **Answers that moved at legend-engine's door** (pure/v1), all toward what legend-engine's own Java does. **Asked
   of the engine runner since (2026-11-26, `r3/measured-step5-facts.md`, D11): confirmed** -- a host-side `let`'s
   `round`, `substring` and `indexOf` answer Pure's (16, `'bc'`, 1) and its `rem(0)` raises in legend-engine's Java,
   while the same calls inside the store query answer the database's (17, `'abc'`, 2). The moves: an operation its SQL does not check now raises Pure's error when it is outside any
   store query (a host-side `let`, a query that reads no store, a model-to-model transform under a graph fetch, a
   checked constraint). And the four kinds answered since step 2 that the old rewrite refused (register S130).
4. **The corpus as legend-engine's own tests** (six tests gained on each lane, none lost): the limits listed in 6a.
5. **Step 8, presentation at the edge:** its research is `edition-homework/r3/research-step8-presentation.md`. It
   found the realistic size is 900 to 1,100 lines, not 2,000, and eight questions to rule first (two printers at one
   door; whether `TDSNull` is meaning or format; whether legend-engine's door's text is a table row; whether the
   corpus's SQL size pins may grow). Its own session can start when these are ruled.
6. **The pull request** (#141, draft): whether this session should watch it and act on CI failures.
7. **`acos`/`asin` raising costs SQL size** (ruled 2026-11-25 to raise as compiled Pure does; built and parked on
   `claude/edit-next`, measured 2026-11-26). The two PCT error tests pass with it on DuckDB (both channels). The
   check and its message (`Unable to compute acos of 2.0`, the value written by the database) make the product's SQL
   for these two functions longer on most databases, and the size pins are shrink-only: on DuckDB 354 to 438
   characters for the family, on BigQuery 131 to 538, on Databricks 111 to 574, on ClickHouse 1389 to 2330; the plan
   for a query using one about doubles (the raise's record travels in it). It shrinks on H2, SQLite and Snowflake
   (their own domain guards leave). 41 pins grow, 10 shrink. The same question as `substring`'s range check, which the
   user answered "Okay let's raise the lean sql" (2026-11-20): may these pins be raised for this ruled check?
   Run on `92e828cb` (60 of 68 targets pass: PCT on DuckDB, H2 and SQLite, the H2 corpus). Left to do on the branch
   once ruled: the size pins; the edge answer files re-recorded (four suites, the two tests' rows); two dialect
   methods one to three lines over the 250-line limit (H2's and Postgres's `call`); the two new SQL functions sorted
   in `SqlFnSortedTest`; channel B's product-style essential lane; each database lane's confirmation that its two
   rows leave.
8. **The corpus lane on macOS** (not this branch's; a suggested task is filed): five trigonometry tests differ from
   Linux in the last digit whenever the lane really runs on macOS. The choices are a per-platform overlay for the
   corpus's recorded files, as PCT's have (new pinned files), a tolerance in those five comparisons, or not running
   the corpus lanes on macOS. Until one is chosen the pull request's macOS corpus job can be red for this alone.
9. **Two pins the user ruled were replaced by an architecture rule** (item "Step 5 begun" in 6a): the claim pins
   for `substringAsGiven` and `substrAsGiven`, with the two functions themselves, which are rules of the lowering's
   now. Say if the old pins should be kept in some form.
10. **Answers that moved with step 5's start:** (a) `round` inside a store query at legend-engine's door: half away
   from zero on DuckDB and H2 (legend-engine's), and **refused by name on every other database** until its answer
   is checked there (it was answered Pure's way before); (b) PCT's legend-engine scoreboard: 24 rows to "both
   fail", listed in 6a.
12. **The plain parses as rows: built, parked on `claude/parked-parse-rows` (`d56c9d6b`), waiting on one ceiling.**
   `parseInteger` and `parseFloat` inside a store query at legend-engine's door become the database's own cast by
   two rows (two new SQL functions, `CAST_INT` and `CAST_FLOAT`, an arm in every dialect), and the edge's pass that
   unguarded every parse in every compatibility style is deleted. So PCT's legend-pure run and a host-side `let`
   get lite's checked parse back, as ruled (v3 section 5.3, S-2). Run on that commit: 75 of 78 targets pass (the
   guards, the census, the lowering's and SQL's tests, the product's SQL size baseline, both corpus lanes, the
   legend-engine lane's pins, PCT on DuckDB, H2 and SQLite, every recorded answer file). The three that do not:
   two corpus size pins that go DOWN and want re-pinning (`stringToFloat`, 123 characters a plan on each lane), and
   **`InterimEdgeSitesTest`'s ceiling, 4 to 5**: the count of raise sites in a legend-pure-style plan whose sentence
   prints a value only the database holds (`<computed>`, register S389). The fifth is the checked `parseInteger` of
   a column ("Could not convert string '<computed>' to INT64"), which the deleted pass used to hide. The ceiling
   moves up only with the user's word. Not yet run: the server and cloud lanes (PCT's legend-pure run could move on
   each).
13. **Step 6, the rest of the one equality: its research is `edition-homework/r3/research-step6-equality.md`**
   (2026-11-26; it checks the earlier design `b3-step5bc-design.md` against the code). Twelve decisions are listed
   at its end. The ones that shape the step:
   - **what "about 2,500 lines gone" means:** the database judge's own comparison layer is about 2,200 raw lines; the
     product's `==` has to grow to carry what it does, so the net is about 0 to 1,000 lines fewer, and the host judge
     stays (tenet 5). Recommended: restate the step's "done when" that way;
   - **a value typed only at run time whose column fixes its type** (`$p.weight == 1` over a DOUBLE column): compare
     at run time by the column's type (recommended), a compile error (needs column types in the type-checker), or a
     recorded difference;
   - **order against step 4:** the slices that fix the product's `==` can go first; the slice that makes asserts
     plain comparisons needs step 4 or a small planning gate (recommended: the gate, so step 6 does not wait on
     item 1);
   - **size pins:** the product's `==` changes may grow SQL size pins, which only shrink.
   A first slice needs no ruling and moves no verdict: `assertSize` and `assertEmpty` planned as the product's
   `size()` and `isEmpty()`, and the two measuring classes of the retired byte channel deleted.
14. **`isNumeric` at legend-engine's door: built, parked (`edition-homework/parked/`), waiting on one size pin.**
   Measured, and asked of legend-engine itself: its `isNumeric` on H2 is a regular expression (a plain decimal's
   text), not the `lower = upper` lite's door writes there today, which calls `'+3'`, `''` and `'1,000'` numeric.
   The row answers each database's own. It makes one corpus SQL size pin grow, H2's `isNumeric` family, 549 to 590
   characters (its plan shrinks). No corpus test's verdict moves.
   **One question covers the three parked pieces (items 7, 12, 14): may these pins move up for these ruled
   answers?** (a) 41 product SQL size pins for `acos`/`asin` raising; (b) the ceiling of error sentences whose value
   the edge cannot print, 4 to 5, for the checked parse in PCT's legend-pure run; (c) one corpus size pin, 41
   characters, for `isNumeric` on H2.
11. **Step 5's rows that carry a decision** (from `r3/research-engine-rows.md`): `dateDiff` (legend-engine writes
   the database's own for every unit; only weeks were ruled); `splitPart` on DuckDB differs from Pure beyond the
   empty token; H2's `isNumeric` in legend-engine 4.145.0 is a regular expression, so register S370 is stale;
   `parseJSON`'s leniency is not in legend-engine's SQL; legend-engine's H2 `parseDate` fails every PCT input.

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
