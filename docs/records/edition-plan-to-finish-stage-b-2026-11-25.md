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
| pure/v1 | every call in a `let` whose value reads no store, including calls from functions it uses |
| the corpus | every call in the test body that is not inside the query handed to `execute()`; the judges' own calls |
| the product, PCT | nothing |

Mapping code arrives later and is never stamped, which is right: mapping code exists only inside a store query.

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
- **The table takes the whole call and knows its database.** `EditionTable.lowersAs(call, features)`; the planner
  binds the table to the statement's database (`PlanEnv.styled()`, `EditionTable.on`), so the lowering still names no
  database. `EngineTable` has four rows (`substring`, `substr`, the two-argument `indexOf`, and `toOne` of an operand
  that is always empty) and refuses by name, while the statement is planned, a call a row answers on a database where
  legend-engine's answer is not yet checked.
- **At legend-engine's door a statement's own feature flags reach its lowering.** A `withFeatureFlags` call written
  in a query is read through by the resolver before the lowering sees it, so on the product's road its flags never
  arrived (the old rewrite read them itself, at the door). `ProductStatements.prepare` now reads them from the
  statements as written, where the door's table says the door reads them (`EditionTable.readsWrittenFlags`:
  legend-engine's does, lite's own doors do not, as before). Two consequences at legend-engine's door, both in
  legend-engine's direction and both to confirm with the user: `LEGACY_SQL_NULL_UNSAFE_EQUALS` written in a query is
  honoured (it was ignored), and a flag nothing in lite consumes is refused by name (it was ignored).
- **The seed is planned in the door's style** (`ProductStatements.lowerTestDataSeed(query, runtime, style)`).
- **A model-to-model mapping's transforms are stamped where the resolver composes them.** legend-engine runs them in
  its own Java under a graph fetch and over a model connection, and pushes them into the SQL in a TDS query over a
  chain. The table states that rule (`EditionTable.transformsOutsideStore(fetchesAGraph, chained)`), the planner asks
  it per statement, and the resolver stamps the transforms (`StoreResolver.withTransformsOutsideStore`,
  `ClassSources.build`). The rule is the old detector's, statement-wide, unchanged.

**What changes for a caller of legend-engine's door.** Nothing the old rewrite answered changes: while both stood,
the two were planned side by side over the old rewrite's own test queries (25 of them at the end) and wrote the same
statement for each. Three kinds of code the old rewrite **refused by name are answered now**, because everything that
becomes SQL passes the lowering. Each is to be confirmed with the user:
1. a graph fetch's computed (qualified) property: the database's meaning, as legend-engine computes it (asked of
   legend-engine 4.145.0, E-M1);
2. a model-to-model mapping's transforms in a TDS query over a chain to a store: the database's meaning, as
   legend-engine pushes them into the source query (read in `chain.pure:111-116`; not yet asked of the engine runner);
3. a function value read out of a collection and applied in a store query: the database's meaning, by the one rule
   (legend-engine's own answer not asked).
A refusal on a database not yet checked now names the first call it meets, not every call of the query.

**The tests.** `PureMeaningCheckTest`, `PureMeaningRewriteTest` and the side-by-side test are replaced by
`EngineTablePlacementTest`: each case plans in the door's style and is read against the same query planned with the
product's table (the same statement means no row applied). `PureMeaningEngineTest` keeps every route's answers and
gains the three newly answered cases and the refusals. `EngineSubstitutesTest` keeps the natives' dialect, answer and
internal-name tests; the deleted substitutes' tests went with them.

**Still open from the work list:** the stamp does not yet reach the raise choice (a host-side `let` that divides by
zero is the plain operation at pure/v1, where legend-engine's Java raises; this is older than this step); the corpus
door's stamping needs the frame-aware walk; a runtime's own model-chain connection is not counted as "chained" (the
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
