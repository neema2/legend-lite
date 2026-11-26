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
| 6 | **The rest of the one equality.** A value whose type is known only at run time carries its type; asserts are planned as plain comparisons; the judges' separate comparison code is deleted. *Carried in (section 5b): bodies typed into their call held to the strict rule; the literal rule's edges* | both judges agree; about 2,500 lines gone |
| 7 | *(reserved: anything steps 2 to 5 show is needed before the parallel lanes start)* | |
| 8 | **Presentation at the edge** (its own session). Upstream's text written in Java from typed values; the SQL that builds it deleted | PCT's printed-table tests and legend-engine's result answers unchanged; about 2,000 lines gone |
| 9 | **Test machinery out of the product** (its own session). The SQL-text check as test tooling (`edition-homework/b3-step6-design.md`); about 60 classes moved | the corpus's results unchanged; about 3,400 lines out of the core |
| 10 | **Clean up and lock.** The folds cut back to the rule (measured first, the list ruled); the old switch machinery deleted (`PlanStyle`, the feature flags in the lowering, `LoweringForms`, the edge's dialect subclasses); the guards on; legend-engine's meaning refused by name on databases where it was not checked; DataCube's legend-engine workarounds; `EngineWhereZones`. *Carried in (section 5b): `parseDecimal` and `parseJSON` as rows, an enumeration by name, the keyed-count row, the SDLC's way to take a project with legend-engine's meaning; PCT on compiled legend-pure (deferred from step 1)* | the build fails if audience knowledge returns to the product |
| 11 | **Land.** Full rounds on every gate, an independent audit, one pull request to `main` | the user's word |

### 5a. The closing table (added 2026-11-26, after the user's "Why are we not closing out our work?")

**Closed means:** the step's "done when" is met; its code is on the program branch `claude/compat-b3-nufdlk`; CI is
green on it; and every test whose answer moved and every new difference from legend-engine has been brought to the
user. "Built" is not closed. The orchestrator reports from this table and keeps it current.

**The rule that goes with it (the user, 2026-11-26: "I really want us to focus on getting B completely done"):** one
batch is verified and landed before the next is started; nothing is added to a batch under verification; no side
work (a new lane, a new suite, a finding outside the step) starts while a step is open, it is written down as a task
for later. The parallel sessions the plan names (steps 8 and 9) go on, and land the same way.

| Step | Built | Closed | What remains to close it |
|---|---|---|---|
| 1 | all (PCT on compiled legend-pure deferred to step 10 by the user) | **yes, 2026-11-26** | nothing |
| 2 | all | **yes, 2026-11-26** | nothing |
| 3 | all | **yes, 2026-11-26** | nothing |
| 4 | all | **yes, 2026-11-26** | nothing (its recorded gap, bodies typed into their call, is step 6's: section 5b) |
| 5 | `round`, `toDecimal`, the plain parses, `dateDiff`, `splitPart`, `isNumeric`, the unchecked operations | **yes, 2026-11-26** | nothing in it. Its leftover rows are deferred by the user's word (section 5b); the Float, date and `TDSNull` text rows are step 8's. One recorded difference at its finish line (PCT's legend-engine run on DuckDB: `testBigFloatAbs`, register S120) |
| 6 | the first slice (`assertSize`, in the next batch); slice 6.1 built and measured on a scratch branch, the fold rule ruled by the user | no | the next batch lands; then 6.1 with the fold rule; then slices 6.2 to 6.6 in order, each landed before the next; the product SQL changes brought to the user per slice |
| 8 | slices 1 and 2 (2 is in the next batch); the Float deletion slice and the date-time slices built, the user's rulings given | no | its slices land; the `TDSNull` row |
| 9 | slice 1 (in the next batch); slice 2 built | no | slice 2 lands; the later slices after steps 6 and 8 |
| 10 | nothing | no | after 6, 8 and 9. Its first item: no line of the edge names a database (section 5b) |
| 11 | nothing | no | the user's word |

**How steps 1 to 5 closed (2026-11-26, 18:22 UTC).** Their code is batch A, `ae275024`: the program branch
`claude/compat-b3-nufdlk` was fast-forwarded to it. Verified before: the fixed pre-push set, the local gate with its
root-only targets run unprivileged, the shrink-only check of the guard pins, ten SQL databases' lanes, and an
independent audit whose findings were fixed. CI on the pull request (#141) at that commit: 53 of 55 jobs green, none
red. The two that did not run are macOS jobs that never got a runner (`macos / pct_duckdb`, cancelled in the queue
after 75 minutes; `macos / edge_net`, still queued): both lanes are green on Linux and Windows, macOS's other
thirteen jobs are green (its corpus lane with the new overlay among them), and both run again on macOS with the next
batch's push. Every test whose answer moved and every pin was brought to the user (`edition-homework/
landing-steps-4-5-2026-11-26.md`; the rulings in section 7a).
**One thing batch A got wrong, found after:** MongoDB's lane was not re-run on its last commit, and the two `acos`
and `asin` error tests fail there (the server's optimizer evaluates the operator over a literal before the
statement's guard). Fixed in the next batch (`MongoDb.java`, the operand read through the row), verified on the lane
345 of 345.

### 5b. Carried forward (so nothing deferred is forgotten; each line names the step that owns it)

| Item | Owner | From |
|---|---|---|
| A function typed into each call (generic over a relation's columns, a table with no declared columns, one marked for the normalizer) is held to the strict rule: a comparison of two known different types in it is its compile error; its generic positions compare by the run-time rule | **step 6** (with the run-time types, slices 6.2 and 6.3) | the audit of batch A, S1; the user, 2026-11-26: "let's fix it in the step not forget" |
| `parseDecimal`, one argument, as a row of legend-engine's table per database (H2 `decimal(5,2)`, DuckDB its bare DECIMAL), out of the old forms object | **step 10** (with the forms object's deletion) | step 5's leftover; the user: "defer okay but make sure they on later steps" |
| `parseJSON`'s tolerance as a row on the stamped side, out of the old forms object | **step 10** | the same |
| An enumeration compared with another enumeration's value by name at legend-engine's door (the fold stops answering it) | **step 10** (with the folds cut back to the rule) | the same |
| Counts over a declared key: legend-engine trusts no key, lite folds over a declared one | recorded as a difference (a register row), **not built**; the row is written in step 10's records pass | the same |
| A way for a project in lite's SDLC to be taken with legend-engine's meaning (today the SDLC's compile check is lite's own, strict) | **step 10** as a design note, built when projects declare their edition (the edition design's "per project") | the user, 2026-11-26: "keep sdlc but we need a way later to accept engine - put that on followup so we don't lose it" |
| The compile route at legend-engine's door refuses a model whose function calls a plan-text function (today it accepts the model and refuses the call) | **step 9**, with the plan-text classes' move | register S427's remaining difference |
| The literal rule's edges the audit noted: a Float literal too large for a Decimal (`1e40`) and an Integer literal past 38 digits against a Decimal become a database's error, not a compile error *(built in slice 6.1, 2026-11-26: a number no Decimal holds is not exact there, so the comparison stays a compile error)*; `max($x.i, 1.0)` keeps the Number overload (not a comparison: recorded, not changed) | **step 6** (slice 6.1, one value within one type) | the audit of batch A, notes |
| An instance whose key is a Decimal or a date-time, compared by the key's value: `^K(d=1.0D) == ^K(d=1.00D)` is false today because the product compares instances by a text of their keys, which keeps the written scale. Not fixed in slice 6.1 by trimming that text in SQL (more SQL, for code slice 6.4 deletes): it comes free when 6.4 compares the keys as typed values | **step 6** (slice 6.4, instances by typed keys) | slice 6.1, 2026-11-26; the research's item 4 |
| A Float's `toString` reached through a Number or an Any (a mixed selection's candidate) inside a store query at legend-engine's door: lite's text today, legend-engine's is the database's cast (the Float row reads the operand's declared type, Float alone) | **step 6** (slice 6.2, where the mixed path is reworked: the table asked inside it) | step 8's deletion slice, its review, 2026-11-26 |
| A date function's result is declared the abstract `Date` (`adjust`, and the others), so its `toString` is not a DateTime's text: at legend-pure's door on DuckDB `%2014-01-01T00:00:00.000+0000->adjust(1, DAYS)->toString()` prints the database's plain cast, `2014-01-02 00:00:00`, where legend-pure prints `2014-01-02T00:00:00.000+0000` (run 2026-11-26 through PCT's route; no recorded test holds it; the same family as the differences page's "33 differ only by an abstract Date's text") | **step 6** (slice 6.2: a value whose type is known only at run time carries its type, so its text is its type's) | step 8's date probe, 2026-11-26 |
| BigQuery refuses a sort of a list of mixed kinds (`|[1, 'a', true]->sort()`): lite orders by the rank as a STRUCT, which BigQuery does not order ("ORDER BY does not support expressions of type STRUCT<g INT64, n FLOAT64, d DATETIME, ...>"; confirmed on the account by the lane, 2026-11-26; a list of Numbers alone sorts correctly; no PCT test on the lane covers it). The fix is to order by the struct's fields | **step 6** (slice 6.2, which gives the rank its tie-break by type and so rewrites this order) | the database re-check and the BigQuery lane's probe |
| Two folds still compare a Decimal or a date by its written form (Java's `equals`), wrong under S139 and S399: the map's `get` over a written map (`newMap([pair(1.0D, 'a')])->get(1.00D)` folds to nothing; a date-time key at another written precision the same) and the inliner's `groupBy` over a written list (keys `1.0D` and `1.00D` make two groups). And two more are exact but outside the fold rule's letter: the whole-number ordering fold and the normalizer's whole-number `==` | **step 10** (the folds cut back to the rule, the list ruled) | the audit of the step 6.1 batch, S4, 2026-11-26 |
| Whether a whole number against a Decimal in a mixed list is refused or answers false depends on the list's shape (`2->in([2D, 'a'])` is refused, `2->in([1, 2D, 'a'])` is false; `[1, 'a'] == [1D, 'a']` refused, `[1, 'a'] == [1D, 'a', 'b']` false). The refusals stand in for legend-pure's comparison of two numbers by their texts; under lite's equality rule the two are two types, never equal | **step 6** (slice 6.2, which makes these refusals answers) | the same audit, S5 |
| A mixed list that is NOT written out and holds one type in two arrays (a relation's date-time cell beside a literal one, Decimals at two scales) compares element by element, which needs a lambda: refused on H2 where the array compare answered (wrongly, false); and on databases that hold a list as JSON text (SQLite, MariaDB) two date-times of one instant at two written precisions may still compare as different inside a list | **step 6** (the SQLite date-time form; slice 6.4 for lists on every database) | the same audit, S6; not measured |
| A list of mixed types written leanly: (a) where the list is a literal, the compiler knows each element's type, so membership of a value of one type is `value IN (the elements of that type)` on every database, a fact fixed by the types and no value compared in Java (`1->in([1, 2, 5, 2, 'a', true])` is `1 IN (1, 2, 5, 2)`; today 220 characters on DuckDB, 286 on Snowflake, about 3,000 on H2); (b) where it is computed, the carrier uses what each database has: H2 its ARRAY type (today JSON and a CASE per element), BigQuery one `ARRAY<STRUCT<...>>` (today one single-type array per type, each operation a correlated `UNNEST ... WITH OFFSET`) | **step 6** (the fold-rule slice for (a); slices 6.2 and 6.4 for (b)) | the user, 2026-11-26, on the fold rule's measured SQL: "H2 has array type, and bigquery has array and struct - array on bigquery has to be single type" |
| **Every database where the SQL blows up, looked at again** (the user, 2026-11-26, with the fold rule's ruling): the version lite runs against the latest released, and each "this database cannot" of lite's dialects against the database's own documentation and the database itself, "don't trust our docs". Measured over PCT's channel A, the same 2,300 statements: DuckDB 529 thousand characters, Databricks 655, Snowflake 845, Trino 918, BigQuery 934, Postgres 941, SQLite 1,543, MariaDB 1,613, ClickHouse 1,785, MySQL 2,074 (H2 457 over fewer statements). Known already: ClickHouse writes its SETTINGS clause, about 350 characters, on every statement; ClickHouse's parse of a TEXT to a Float is its fast one, a last digit off on some values and the smallest subnormal read as zero (measured 2026-11-26 on 26.3 with `clickhouse local`, three of ten values; the numeric literal is exact, and so is the text parse under `precise_float_parsing = 1`) -- lite's Float literals no longer go through it (the next batch), a text parsed at run time (`parseFloat`, a cast of a string) still does; H2's dialect carries a list as JSON though H2 has ARRAY; BigQuery's a mixed list as one array per type though it has STRUCT | **researched 2026-11-26, deferred by the user to right after Stage B** ("I don't think we should distract ourselves right now on the database list lean sql but we should write down all the research and all the context and any artifacts so we can do it next as a follow up right after B is done"): `docs/records/database-recheck-2026-11-26/START_HERE.md`, the ranked list, the three reports and the probes. One item stays in Stage B, in step 6 before slice 6.5: SQLite's one date-time text form, which the product's `==` needs to compare an instant there | the user's ruling 1 of about 18:10 UTC and their word of about 19:15 |
| **No line outside a dialect names a database to decide anything** (the user, 2026-11-26: "Why is anywhere allowed to name database anywhere except dialect? Why did we allow anywhere outside dialect"). The edge's `PureText` names databases on six lines, four of them added in step 5 by a pin moved up (the sets of databases a row of legend-engine's table answers on), against the guard's own rule ("the fix is a dialect method, never a pin bump"). They move to the edge's own dialect object for each database, which a row asks; `DialectBoundaryTest`'s count for the file goes to zero. What stays, as defaults and not behaviour: `Databases` (the platform's default database, the replay referee's) and legend-engine's test database | **step 10**, its first item (steps 8 and 9 have the file open until their slices land); brought to the user as a before and after, with what it means for step 10's deletion of the edge's dialect subclasses | the user's question, about 18:19 UTC |
| A table's cells counted, `assertSize(<table>.rows.values, n)`: the typer keeps the bare read as the table's rows, so every judge on every database counts the rows where Pure counts the cells (2 against 4 in the probe); the same at the size slice's base, so older than it. And one row's cells, `assertSize(<table>.rows->at(0).values, n)`. The typer keeps a row's bare `.values` as the row itself (one value), so the host judge counts 1 where Pure counts the cells; the database judge asks the product's `size`, whose value is the cells as a mixed list under a to-one stamp, which the renderer refuses by name (`DialectCapability: a tagged list reached the renderer`). **A move of the first slice** (measured at its base, `ae275024`, on DuckDB, H2 and SQLite): until it the database judge counted the side's rows itself and agreed with the host on 1, so all three judges gave one wrong verdict; since it the database judge and both-judges runs stop on the refusal. No test of PCT, the corpus or core holds the shape. A written `.values->size()` is right (the typer rewrites it to the column count). Held as it is by `AssertBarTest` | **step 6** (slice 6.2, where a list of values whose types are known at run time is typed as one; then both judges count the cells) | the audit of the next batch, S1, and the probes it asked for, 2026-11-26 |
| A graph fetch inside a test body's `execute` is not planned ("class query under TypedGraphFetch is not resolvable"), under every judge | recorded, **not in Stage B** (the test body's planning, the product track) | the same probes |
| H2's dialect: 135 PCT tests it does not write or writes wrongly | a task for the user (the product track), not a step of this plan | PCT on H2, 2026-11-26 |

**Parked until step 5 is closed (not lost, not in flight):** the refusal of legend-engine's plan-text functions at its
door (built on `claude/edit-ahead`, `973eaca4`; one existing test, `PureV1SqlTextTest`'s DDL case, pins the old
answer and moves with it); PCT's four more suites on H2 (`claude/pct-h2-full`, green on its own); the H2 dialect's
135 gaps (a task for the user).

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

**The independent audit of H2's lateral pass and of `chunk` (2026-11-26, `r3/audit-lateral-chunk.md`): one blocker,
found after both had landed.** `chunk` above size 1 answered wrongly on MariaDB and MySQL since its written size
became a literal pattern: their `regexp_extract_all` counted matches by asking whether the text splits into k of
them, which overcounts where a match can be cut into shorter ones (`'abc'->chunk(5)` gave `abc`, `c`, `c`); until
that change both databases refused `chunk` by name. Fixed in the emulation: each earlier match is an atomic group, so
the count is the scan's. Measured by the lane on MariaDB 11.4 and MySQL 8.4, by direct SQL and through lite: Pure's
answers; its round on the fix moved nothing. Also from the audit: the literal pattern is written for sizes 1 to 1000
alone; the lateral pass leaves a subquery holding a column with no table, raw SQL or a value drawn afresh, and
counts a row's position and a star as reads; a LEFT lateral and the new bails are tested. Open product gaps of
`chunk` it listed, from before this program: a size below 1 does not raise as Pure does, `.` skips a newline, and
the engines count code points where Pure counts UTF-16 units. **The lesson, kept:** a change that lets something new
reach a dialect's emulation gets its independent review before it is pushed; every lane's tests passed while the
answer was wrong on inputs they do not hold.

**Still open from the work list:** a runtime's own model-chain connection is not counted as "chained" (the
old detector did not count it either).

**2026-11-26, after the user's rulings (section 7a): the approved pieces stacked, step 4 started, three sessions
measuring.**

- **The lanes read legend-engine's own SQL on every database it has** (the user: "Run all the round/substring stuff
  on all the warm lanes"). Five reports, each template read at the pin and run beside lite's rule:
  `edition-homework/lanes/engine-templates-2026-11-26.md`, with the table of which row answers where. In short:
  `substring`, `substr` and the String `indexOf` give legend-engine's answers on Snowflake, Postgres, Trino,
  Databricks and BigQuery as on DuckDB and H2 (ClickHouse differs on multi-byte text); `round` on Snowflake, Trino,
  Databricks and BigQuery's one-argument form (Postgres rounds a Float half to even without legend-engine's cast);
  `toDecimal`, `isNumeric`, `dateDiff` and `splitPart` are written differently on almost every database, and several
  have no legend-engine SQL at all. MariaDB and MySQL are not legend-engine databases. **Not yet in the table's code:**
  the rows still answer on DuckDB and H2 alone; widening them needs its tests reworked (they use Postgres as the
  database that refuses) and a lane round, its own batch.
- **One batch, built together** (the edit tree, then a detached copy; not on the session branch until verified):
  - `toDecimal`'s row with its review's fixes (the register and the differences page say whose value each is; the
    refusal names no audience);
  - the plain parses as rows, the edge's unguarded-parses pass deleted, `InterimEdgeSitesTest`'s ceiling 4 to 5 by
    the user's word; register **S424**, new: the cast is to Pure's width (64 bits, a double) where legend-engine's
    SQL names a 32-bit `integer` on four databases and a single-precision `float` on DuckDB and ClickHouse (measured:
    DuckDB's `cast('3.14' as float)` is `3.140000104904175` as a double). Not new behaviour, recorded for the first
    time: **posted to the user**;
  - `isNumeric` as legend-engine's own form on each database; every row of the table now carries the databases it
    answers on, sets `edge/PureText` holds, so the table names no database;
  - `acos` and `asin` raising.
  - Guard moves in it, each dated in its file: `DialectBoundaryTest`'s count of database names in `PureText` 2 to 4
    (the two `isNumeric` sets); `SqlFnSortedTest` (four new functions sorted PRODUCT; `LOWER`, `UPPER` and
    `PARSE_INT` are the product's alone again, a shrink; `EQUAL` gains `lowering.NamedRules`).
  - Fast checks on it: the guards, the census, 1,036 edge tests, the lowering's and SQL's tests pass. Then one
    regeneration of every recorded file, the fixed pre-push set, and an independent review (running).
- **Two more rows written, not yet built:** `dateDiff` as the database's own function for every unit ("Yes all
  units"; no new SQL function: the plain `DATE_DIFF` was already there; register S425), and `splitPart` on DuckDB as
  its own `split_part` ("Yes match split fully"; S426), a row only where legend-engine writes the database's
  function: on H2 legend-engine calls its own Java (`StringUtils.split`, which drops empty parts as Pure does), so
  H2 keeps Pure's rule.
- **Step 4 started.** The literal rule (`compiler/spec/NumberLiterals`) and strict bodies (lite's comparison setting
  reads bodies; a body's marker is refused when the body is typed whole) are written with their tests,
  compile-checked, and **pushed to the session branch as `c64292af`, marked not yet verified**, so that a measuring
  session and CI run it in parallel (this container's one Bazel was regenerating). The earlier promise that the
  session branch never carries an unverified commit is broken for this one commit, on purpose, and said here.
  Recorded: AGENTS.md's equality rule (two bullets), register S127, the differences page's new section on typing.
- **Step 6.0's measurement is in** (`edition-homework/r3/measured-step6-equality.md`): 1,697 assert bodies, as
  written and as plain comparisons, both judges, three databases. No body passes under one judge and fails under
  the other; 206 bodies differ between their two forms, each grouped; the product still disagrees with itself where
  a side is typed only at run time (`[1,2]->cast(@Any) == [1.0,2.0]->cast(@Any)` is true on DuckDB and false on H2
  and SQLite). A second run on the literal rule's commit is asked for.
- **Step 8's session reported three times.** Its first slice (the legend-pure Float text moved out of the product)
  is built and measured, and parked by the orchestrator: it leaves one SQL function the product renders and only
  the edge writes, which no guard category allows, and makes Snowflake's and BigQuery's edge SQL several times
  longer. The direction is the user's amended tenet 1: a last-step print is Java's, a print used further is lite's
  own text; the session is measuring which answers that moves. Its print-tail proposal: a closed set (`replace`,
  `length`, `trim`, `startsWith`, `endsWith`, `contains`, `indexOf`, `+` with literal Strings), built together with
  the scalar value lift, because alone the tail reaches no test today's rule misses (measured: all 41 corpus cases
  are `toCSV->replace`, already lifted).

**2026-11-26 ~12:15: what the checks found in the hour after the push, each fixed in the next commit.**

- **Step 4's core did not build** (two measuring sessions, at once): the build's null-safety check refused
  `NumberLiterals` (a collection's element type could be null to its eyes). The orchestrator's own compile check had
  run plain `javac`, which does not run that check. Fixed; **a lesson:** a compile check outside Bazel says nothing
  about the build's own checks.
- **The literal rule read two literals when one was a Decimal literal** (the step 6 measurement's second run):
  `1D == 1` answered true, because a Decimal literal was not counted as a literal. Fixed: two literals of different
  number types are a compile error whatever their types; tests added. That second run otherwise: 15 bodies newly
  answered, none newly refused, no crash; the judges' disagreements unchanged.
- **The independent review of the batch** (`edition-homework/r3/audit-step5-batch.md`): two blockers, both fixed --
  a product test still expected `acos(2.0)` to be NaN; and the two new cast functions were not in the set of
  functions that can raise, so a failing parse in an unread column could have been pruned away and rows returned
  where the database should have refused. Also fixed from it: the parse rows answer only on the eight databases
  legend-engine has (on SQLite, MariaDB, MySQL and MongoDB a cast answers a value for a text that is no number, so
  those keep the checked parse); a NULL argument of `acos`/`asin` at legend-engine's door stays NULL on DuckDB,
  Postgres and Snowflake (it answered NaN); three records said more than was measured.
- **`toDecimal` on DuckDB pulled back** (PCT's legend-engine lane, regenerated): see the ruling row above.

**2026-11-26 ~12:40: who is doing what (the sessions the orchestrator started today; each reports by message).**

| Work | Session | Branch | State |
|---|---|---|---|
| Design, code, the narrow loop; the batch's regeneration and pre-push set | the orchestrator | `claude/edit-ahead` (edit tree, scratch), `claude/build-tree` (regenerated files, scratch), `claude/modest-galileo-h6swr4` (the pull request's, #141) | steps 4 and 5 in verification |
| Step 4's measurement: lite's own code under strict bodies (core, stress, timeseries, DataCube, wasm, python) | `session_0195rQEkqKvFbL1ncs3DMFdw` | measure only | its own tests pass (14 of 14); the wide lanes running |
| Step 6.0: the equality measurement and the product SQL change list (`tools/equality-measure/`; one command per pair of commits) | `session_0195rQEmtbQHaPU6nxVzr6b1` | `claude/step6-equality-measure` (merged into the edit tree) | four runs delivered; now the fastest builder for narrow checks |
| Step 8: presentation at the edge | `session_0195rQEm7Fs6u58NaDY4RToB` | `claude/edition-step8` | first slice parked; measuring which answers move when a print used further is lite's own text |
| Step 9, slice 1: 15 harness classes out of the product (`edition-homework/r3/research-step9-inventory.md`) | `session_0195rQEkrmV5QVzoXb9upZBB` | its own, from `9c751659` | started |
| The corpus lane on macOS: a per-platform overlay | `session_0195rQEnbpNHDwoxFUbVt29b` | its own, from `9c751659` | facts confirmed (five DuckDB trigonometry tests, one unit in the last digit); building |
| The database lanes (measure only) | Postgres and Trino `session_0195rQEnn8FAUPKYnpH5gmBg`; MariaDB, MySQL, ClickHouse `session_0195rQEjDbpHcLjorPDXJjG6`; Databricks `session_0195rQEijYc25F3cgFg1TxX2`; BigQuery `session_0195rQEo31vtHdx1NMEUJvJr`; Snowflake (its id is in the lanes' records) | none | idle, warm; next: a round once the batch is on the pull request's branch |

**The product's SQL under step 4** (the user's deliverable, first instalment; `edition-homework/step6/sql-changes-9c751659-19ad03b4.md`):
of 2,451 expression queries (on DuckDB, H2 and SQLite) and 3,527 model queries, no existing query's SQL changes for
equality; three that were refused are planned (two fold to a constant); none that was planned is refused; 13 change
for `acos`/`asin`, which is not equality. The first build of the rule had made ten queries 22 characters longer each
(`CAST(100 AS DECIMAL(3,0))` for `100`); the rule now leaves a whole-number literal as written.

**2026-11-26, afternoon.**

- **The batch of steps 4 and 5 is verified piece by piece and lands as one push.** Every moved pin, roster, golden and
  ledger has its dated reason on one page: `edition-homework/landing-steps-4-5-2026-11-26.md`. One build break came
  from changing the compile check's entries without building the edge's tests (`ReferenceIntegrityTest`,
  `PlannerSurfaceTest`): found by a measuring session in ten minutes, fixed in two test files. The lesson stands: a
  signature change is built against EVERY test library before it is called done, and a builder session does that
  while the working tree goes on.
- **PCT on H2 is all five suites** (branch `claude/pct-h2-full`, session `session_0195rQEizYjy8rVS8hbgqhKy`; the
  record is `docs/records/h2-pct-full-2026-11-26.md`): 1,249 tests, 1,020 pass, 229 pinned by message (212 new). The
  five suites are in the H2 gate (about 3 minutes of tests). The worker's first report filed most of the new pins as
  "H2's limits" ("H2 has no list type"), and the orchestrator repeated it to the user unchecked; the user caught it
  ("H2 has array type I thought?"). Every "H2 lacks" claim was then probed on lite's own jar (2.5.252), and the 212
  are:
  - **5 H2 truly cannot** (base64);
  - **135 lite's H2 dialect does not write, or writes wrongly, though H2 can:** 76 list operations over a constant
    list (H2 has `ARRAY[...]`, `CARDINALITY`, `ARRAY_GET`, `ARRAY_CONTAINS`, `ARRAY_SLICE`, and `ARRAY_AGG ... ORDER
    BY` and every aggregate over `FROM UNNEST(ARRAY[...])`: 16 of 16 probes gave Pure's value; lite's dialect carries
    a list as JSON and refuses these), 36 other forms (the bit operations 14, time buckets 11, a date-time from parts
    2, `'%.Nf'` 3, date arithmetic past 32 bits 6), and 23 defects (14 write a function H2 does not have and are not
    refused by name; 9 wrong answers or SQL H2 cannot read);
  - **58 beyond one expression, or not settled:** 20 need a lambda or a comparator H2 cannot run (`fold`, a sort with
    a comparator), 38 are open (lists of instances or of mixed types, an empty map, a signed 64-bit hash, two regular
    expression functions, one date token);
  - **14 the platform's own** (13 fail the same way on DuckDB's lane; one differs in a spelling).
  What H2 truly lacks: `LATERAL`, a correlated `UNNEST`, lambdas over an array, a map type. The 135 are put to the
  user as one task of the product track, in three parts (the defects, the forms, the constant lists with a design
  first). The lesson: a worker's label for WHY a database refuses is a claim about that database; it is probed on the
  database before it is repeated.
- **Step 6 starts with the size assert alone.** `assertSize(c, n)` is planned as the Pure condition it means,
  `size(c) == n`, over the sides as named values, lowered by the product over the columns that hold them, each
  side's value read once and returned for the failure's record (`VerdictSql.typedOverValues`, `SidePlanner.over`,
  `Lowerer.over`). That is the whole new road on the one family where no rule of types is in play.
  *How the condition is typed (corrected after the audit of the next batch, 2026-11-26, S1):* the two calls are built
  by declaration over the sides the typer already typed as the assert's arguments, each the exact declared function
  (`VerdictQueries.declared`), as the typer's own desugars build a call; only the `==` then passes through the door's
  comparison setting (`SpecCompiler.comparedAsInAQuery`). They are not parsed and typed afresh: a node of the parsed
  form is built by the parser and the normalizer alone (an architecture rule). So the two rewrites the typer makes
  of a WRITTEN `size` call are not made here: a table's cells (rows times columns) and one row's cells (its columns).
  Measured, each held as it is by `AssertBarTest` and owned by step 6.2 (section 5b's row): the table itself is
  refused as a relation before any judge plans (register S137); its `.rows.values` is counted as its ROWS by every
  judge on every database (2 where Pure counts 4 cells), the same at the slice's base, so older than the slice; one
  row's cells are the shape the slice moved.
  *One departure from the research (the same audit, S2):* the research kept a graph's size as a member of the judge's
  own; the slice plans the product's `size` for a graph side too and deletes that member, because the product's count
  of a graph is its root objects, which is what the host judge counts. `aGraphsSizeIsItsRootObjectsUnderEveryJudge`
  holds it on a root with three children beside a root with none.
  *Noted, not changed (N1):* the old member compared null-safely; the product's `==` does not, so an EXPECTED size
  that is NULL at run (the assert's `n`, a nested `toOne()` over an empty cell; the count itself is never NULL) fails
  `assertNotSize` under the database judge where the host passes it: the two judges disagree there, which is what
  running both is for.
  *Noted, not changed (N8):* `tools/census` depends on `//core:edge` without `//core:edge_harness`; it names no class
  that moved, and would need the harness only to decode a plan that holds the edge's records.
  Two families the design had put beside it wait, each for a reason found on reading the code:
  - *emptiness* needs the side as a LIST bound once, and the product's `isEmpty` of a list is not written on every
    database the judge's `NOT EXISTS` runs on today (H2 holds a list as JSON): it goes with the slice that makes the
    product's list operations whole on every database (6.4), or two reads of the side, which would grow the SQL;
  - *tolerance* compares numbers of different types by the assert's own meaning (its parameters are `Number`), so
    its condition is typed over values whose type is known at run time: slice 6.2's work, not a plain comparison of
    two typed sides, which lite's rule would refuse.
- **Slice 6.1, one value within one type, built and measured on a scratch branch (2026-11-26, `claude/edit-ahead` at
  `b64dc639`; not landed, it follows the next batch).** Four parts:
  1. *The fold rule* (awaiting the user's ruling): the inliner's fold answers an equality, an `in` and a `contains`
     of constant Strings, Booleans and enumeration values only; numbers and dates are the database's to compare, so
     `%2020-01-01T10:00:00 == %2020-01-01T10:00:00.000` and `1.0D == 1.00D` are the database's `true`.
  2. *A list written in the query needs no list in the SQL* (the user, on the fold rule's measured SQL: "H2 has array
     type, and bigquery has array and struct"): membership of a value in a written mixed list is `value IN (the
     elements of its type)`; two written lists compare position by position. Measured over PCT's channel A on eleven
     databases: with it the fold rule's total is BELOW today's on ten (H2 492,904 to 457,463 characters; BigQuery
     973,275 to 934,386; SQLite 1,562,673 to 1,543,087; DuckDB 529,909 to 529,252; Snowflake 846,314 to 844,951) and
     above on ClickHouse alone (1,774,195 to 1,785,425: every statement there carries its SETTINGS clause, about 350
     characters, so each constant that becomes a statement costs that).
  3. *Inside a mixed list a value is compared by what it denotes:* Decimals of two scales by their number (S139),
     date-times at two written precisions by their instant (S399), in membership, equality of two lists and
     `removeDuplicates`; date-times of one instant tie in a mixed list's order. The written digits stay as the
     value's presentation (the result decoder reads them).
  4. *The literal rule's two edges:* a number no Decimal holds (past 38 digits) is not read as one.
  **Measured at `b64dc639` against the commit before parts 3 and 4:** builds; every named test class passes; the
  product's SQL sizes are identical on eleven databases, statement for statement; no PCT outcome differs (DuckDB both
  styles, SQLite, H2). **Answers that move, all inside rulings S139 and S399:** `[1.5D, 'a'] == [1.50D, 'a']`,
  `[%2014-01-01T00:00:00.000, 1]->contains(%2014-01-01T00:00:00)` and `[%...00.000, 1] == [%...00, 1]` false to
  true; `[1.0D, 1.00D, 'a']->removeDuplicates()` three elements to two.
  **One database answers differently, found by this measurement:** on SQLite a date-time is its text, so the
  product's `==`, `in` and `contains` compare two date-times written at different precisions as different
  (`...00:00:00.000` is not `...00:00:00`), where DuckDB and H2 compare the instant. It is so today for a column
  against a literal too; the database judge normalizes each value in SQL (about 300 characters a value). Put to the
  user: record it as SQLite's own answer, or pay that SQL in the product.
  **Not in this slice:** an instance's Decimal or date-time key (section 5b, slice 6.4).

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

## 7a. The user's rulings of 2026-11-26 (on the decisions gathered in 6b; the words are the user's)

The orchestrator put each open decision to the user with its options (the message of 2026-11-26 ~11:10). The
answers, in the user's order:

| # | On | The user's words | What it settles |
|---|---|---|---|
| 1 | `acos`/`asin` raising costs SQL size (6b item 7) | "sql for acos/asin as long as it's only for these functions that sql gets bigger that's fine" | the size pins of these two functions' families may grow; the parked piece lands |
| 2 | the plain parses as rows (item 12) | "yes for parse functions, which counter goes up though" | the ceiling of `InterimEdgeSitesTest` moves 4 to 5; the parked piece lands (the counter explained to the user) |
| 3 | `isNumeric` on H2 (item 14) | "agree for h2" | the corpus size pin for H2's `isNumeric` family may grow by its 41 characters; the parked piece lands |
| 4 | a whole number against a Float or Decimal (item 1) | "okay for float/decimal literal compare - don't understand the follow-ups though" | option (b): a number literal written against a value of another number type reads as that type when it is exact there. Step 4 is unblocked. The two follow-ups (item 2) explained again |
| 5 | step 8 | "start 8 - I think we should also look at toString/toCSV in the middle - for example we saw toCSV->replace() and we should do that as one call at the end to replace newline in csv, so maybe small set up string/count functions chained should all be in java" | step 8 starts in its own session. New scope for it: at a compatibility door, a small closed set of string and count functions chained after a final `toString`/`toCSV` is computed in Java at the edge with the print, so the text is upstream's through the tail. The set is proposed by step 8's session and brought back |
| 6 | `TDSNull`'s text | "tdsnull what are downsides if treat as table row? Seems like no brainer ?" | a row of legend-engine's table (the downside explained: its SQL stays; nothing moves) |
| 7 | a Float's or a date's text inside a legend-engine store query | "agreed on float/date text for engine" | rows of legend-engine's table, not presentation |
| 8 | step 6's "about 2,500 lines gone" | "fine to restate target, the real target is unified equals , deleting code is byproduct" | step 6's "done when": one equality, both judges agreeing; the lines are reported, not targeted |
| 9 | a value typed only at run time whose column fixes its type | "runtime compare is fine" | compared at run time by the column's type |
| 10 | step 6's order against step 4 | "is decision 2 made here now? Or still need gate" | answered: ruling 4 makes it; step 4 goes first, no gate |
| 11 | size limits in step 6 | "size limits - what exactly will grow? Thought we decided on exactly everything that will print but teach me first" | not ruled: explained to the user; measured numbers come before any pin moves |
| 12 | legend-engine's answers on the other databases | "Run all the round/substring stuff on all the warm lanes on parallel so we get answers for everything?" | every warm lane reads legend-engine's own SQL for each row on its database and runs it, so a row answers there instead of refusing |
| 13 | `dateDiff` | "Yes all units" | the database's own `datediff` for every unit at legend-engine's door |
| 14 | `splitPart` on DuckDB | "Yes match split fully" | the database's own `split_part`, in full |
| 15 | `parseJSON` | "Parsejson okay" | its tolerance is legend-engine's own Java's: a row on the stamped side |
| 16 | the corpus lane on macOS | "What does macos overlay mean" | not ruled: explained |
| 17 | the two claim pins replaced | "Okay for substr functions" | confirmed |
| 18 | landing on the program branch | "Landing whatever you think is right is fine" | the orchestrator moves `claude/compat-b3-nufdlk` at verified milestones; never `main` |
| 19 | PCT on compiled legend-pure | "Defer pct compiled that's fine" | deferred to step 10 |

**The user, later the same day (2026-11-26 ~12:13), on the orchestrator's status and its open points:** "Okay I'm
good with all your answers - bring back all the product SQL changes for equal when you have them".

| On | What it settles |
|---|---|
| the plain parses' width (register S424) | Pure's width stays: 64 bits and a double, where legend-engine's SQL names a narrower type |
| `toDecimal` on DuckDB | the row answers on H2 alone until the door spells a Decimal as legend-engine's does (step 8) |
| step 8's decisions (the first slice parked; a last-step print Java's, a print used further lite's own text; the print tail built with the scalar value lift) | stand |
| the guard moves reported | stand |
| **a new deliverable** | every change the one equality makes to the PRODUCT's SQL (step 4's literal rule and strict bodies, then step 6's run-time types) is gathered, the SQL before and after for each changed query with its size, and brought to the user before any size pin moves for it |

Not covered by it: the corpus lane on macOS (no answer of the orchestrator's stood open there; still the user's).

**The user, ~12:19:** "Yes 64 bit and yes Macos". The plain parses keep 64 bits and a double (register S424, ruled).
The corpus lane on macOS gets a per-platform overlay of its recorded answers, as PCT's lanes have (the option the
orchestrator recommended): started the same minute as its own session, on its own branch, merged into the pull
request's branch when it reports, CI's macOS job the proof.

**The user, ~12:25, to the orchestrator's one request on pace:** "Def can push to scratch branches or make any
number of branches you need without my permission". Recorded in AGENTS.md ("The fast setup"). From here unverified
work goes to scratch branches (the edit tree is pushed as `claude/edit-ahead`), worker sessions build and measure
from them, and the pull request's branch takes verified batches only.

**The user, ~13:57, on six questions the orchestrator put (the words are the user's):**

| # | The question | The user's words | What it settles |
|---|---|---|---|
| 1 | an assert that compares two different types on lite's own door (`assertEquals(1D, 1)`): a compile error, as any comparison across types? | "yes compile error" | step 6's D4: lite refuses; the 21 deliberate probes in lite's own tests are rewritten to expect the error; the compatibility doors are unaffected |
| 2 | the host judge's ledger pin growing by about 3 lines for the tie-break Integer before Float before Decimal | "yes growth okay" | step 6's D9 |
| 3 | at legend-pure's door, a Float's text used further in a query is lite's own text (six decimals), with no recorded test moving | "yes that's right but remember we have a list of chained functions that are okay after that you came up with" | the register row may land with step 8's deletion slice; a print followed by the tail's functions stays Java's, with upstream's text |
| 4 | does legend-engine's door promise `toSQLString` and `planToString` inside a program? | "I think those are platform functions - useful if we need for backwards compatibility - but I don't immediately see a product use for it - I don't think users can use it - can we check somehow?" | not ruled: to be CHECKED (asked of legend-engine itself through the engine runner, and read in lite's routes), then brought back |
| 5 | when to widen legend-engine's table to the other databases | "whenever you think is right time is fine" | the orchestrator's call: after steps 4 to 6 land, with a proof run through lite on each database |
| 6 | the print tail's members (`replace`, `length`, `trim`, `startsWith`, `endsWith`, `contains`, `indexOf`), which need a guard exception because Java computes a length, an index or a true/false over text the edge printed | "yes should add guard" | the set is ruled as proposed, pinned by its guard (`PrintTailTest`, the dated ledger entry) |

**The user, ~14:31, on the two points the orchestrator had open:** "I'm good with refusing those engine functions in
the user/product path

Should we run full h2 pct like duck?"

- **The plan-text functions are refused to a user at legend-engine's door** (register S427). `toSQLString`,
  `toSQLStringPretty`, `planToString`, `executionPlan` and the DDL text generators answer in legend-engine's own tests
  alone (the corpus's and PCT's styles); a user's query that calls one, directly, through a `let` or through a model's
  function, is refused by name in legend-engine's compile-error envelope, as legend-engine itself refuses it
  (`r3/measured-plan-text-reachability.md`). The classes behind them (about 25 classes, 5,300 lines) are then test
  machinery, and leave the product library with step 9's later slices; the two pieces the routes' own Java calls
  (a generated plan's support functions, the test-data routes' timestamp text) stay.
- **PCT on H2, all five suites.** Today lite's own door runs only the relation suite on H2 (`//pct:pct_h2`); the
  legend-engine door's lane runs three (essential, relation, standard). The orchestrator's answer: yes. H2 is
  legend-engine's own first test database and one of the two databases its table answers on, and a change to an H2
  arm outside the relation suite is seen today only through the legend-engine door's lane and the corpus. A session
  (`claude/pct-h2-full`) adds the four missing suites with H2's own expected failures and reports them grouped by
  cause; whether they join a gate is decided on its report (each is one more JVM of about 4 GB in the H2 lane).

**The user, ~16:08, on five questions the orchestrator put before the batch of steps 4 and 5 closes (the words are
the user's):** "1 pins okay / 2 isnumeric okay / 3 defer okay but make sure they on later steps not forgotten / 4 keep
sdlc but we need a way later to accept engine - put that on followup so we don't lose it / 5 yes but let's fix it in
the step not forget".

| # | On | What it settles |
|---|---|---|
| 1 | three size pins that go up because three queries that were refused are planned (core's filter family one statement of 624 characters; the `and` and `or` families' plans about 269 characters on each database; channel B's grammar ratchet two statements and 520 characters) | the pins move; no existing query's SQL grew for the equality work |
| 2 | H2's `isNumeric` in the corpus, 47 characters over where 41 were ruled | the pin moves by its 47 |
| 3 | step 5's leftover rows | deferred, each to a named step (section 5b): `parseDecimal` per database, `parseJSON` and an enumeration compared by name to step 10; counts over a declared key recorded, not built; `rem` needs nothing. Step 5 closes with the batch |
| 4 | lite's SDLC refusing a tree whose function compares across types | kept: the SDLC's check is lite's own. A way for a project to be taken with legend-engine's meaning is a follow-up (section 5b), not to be lost |
| 5 | bodies typed into their call not yet held to the strict rule | the batch lands with the gap recorded; it is fixed inside step 6, not left |

**The rulings of about 18:10 UTC (the user, 2026-11-26, on the seven questions put that afternoon; the words are the
user's):** "1 Yes apply fold rule, but also relook at any database where the sql blows up - make sure we are running
latest versions and relook at sql/database docs don't trust our docs one what is and is not supported / 2 step 8 yes
but teach me why / 3 match engine for engine / 4 datetime for engine should use engines database form, datetime for
pure is there a cheap format in database we can use or no? If there is a cheap standard format we can use maybe we
should use everywhere for pure - if not go with lite in the middle / 5 I don't know what assert not size should do did
we check upstream? / 6 you mean just compare whatever string was given? Shouldn't we normalize to at least pretend
it's a date? Or no? Is it not just a cheap string format? / 7 what is the edges database naming count?"

| # | On | What it settles |
|---|---|---|
| 1 | the fold rule (the inliner's fold answers an equality, an `in` and a `contains` of constant Strings, Booleans and enumeration values only) | **applied**, with step 6.1. The size pins of the families whose constants become statements (`eq`, `equal`, `not`, `and`, `or`) go up by this word; `in` and `contains` go down by more. **And a task with it:** every database where the SQL blows up is looked at again -- the version run against the latest, and each "this database cannot" of lite's dialects against the database's own documentation and the database itself, never lite's own notes (section 5b) |
| 2 | step 8's Float deletion slice, the corpus plan size 234 characters up a lane | the pin moves. Why (asked): the plan holds the SQL as a tree; the tree held a short product node that the edge's dialect re-spelled as a cast when it wrote the text, and now holds the cast, whose stored spelling is 39 characters longer, six times; the SQL text is the same |
| 3 | a date-time literal's `toString` at legend-engine's door | legend-engine's answer, the database's cast: three engine-lane tests go from "lite passes, legend-engine fails" to "both fail" on DuckDB and H2 |
| 4 | a computed date-time's text | at legend-engine's door, the database's own form. At legend-pure's door: a cheap standard database format if there is one, else lite's own in the middle of a chain. Read: legend-pure's text trims the fraction's trailing zeros and ends `+0000`, which no database's format function writes in one call (DuckDB, H2 and Postgres reach it from their cast with two edits; that is three databases, not a standard); the cheap standard one is lite's own (`DATETIME_TO_TEXT`, one format call). So: lite's in the middle, Java writes legend-pure's at a last step, a literal keeps its written text |
| 5 | `assertNotSize` over a relation | asked: was upstream checked? Read then, legend-pure `platform/pure/essential/tests/assertNotSize.pure`: `assertNotEq($size, $collection->size(), ...)` over `collection:Any[*]`, the mirror of `assertSize`'s `assertEq($size, $collection->size(), ...)`. Over a relation both count it as one value, so S137's rule covers it as it covers `assertSize`. Told to the user; held as built unless they object |
| 6 | SQLite's date-times compared as text | not recorded as SQLite's own answer: normalized. The plan put to the user: every date-time SQLite holds is written in one fixed text form at its source (a literal by the dialect, free; a load; a function's result), so the plain comparison is the instant's with no SQL at the comparison. The form and its measured cost come back before it is built (step 6.1's SQLite part) |
| 7 | `DialectBoundaryTest`'s count of the edge's lines naming a database, 6 to 7 for the date row | explained, not ruled. With ruling 4 it may not be needed: where legend-engine's source writes the plain cast on a database, the row answers by the existing set of legend-engine's databases. Step 8 reads the source first |

**The fold rule, made precise (the user, 2026-11-26, about 19:55 UTC): "Whole numbers fine".** Put to them after the
independent audit of the step 6.1 batch: with every number out of the fold, a function that recurses over a written
list no longer ended at its guard (`$l->size() == 0`) and five corpus tests failed on DuckDB (`toPostgresModel`). So
the compiler compares two written values only of a kind whose written token is the value: a String, a Boolean, an
enumeration's value, a whole number. A Float, a Decimal and a date go to the database. Measured with it: 14
constants a database become statements (39 with every number out), and the product's SQL is smaller than today on
ten of eleven databases (ClickHouse 1,232 characters more, 0.07%).

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
