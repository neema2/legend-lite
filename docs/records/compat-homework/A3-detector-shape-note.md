# A3 piece 3: the detector, the shape note (2026-11-19)

**Revision 2 (~19:30): the user changed the ruling at ~19:20 ("I think it has to be refuse then for now"), and the
compat owner gave the go.**
- **The default is `refuse`.** A legend-engine request whose answer would differ is refused in legend-engine's error
  envelope, naming each call and its position, and the header still carries the codes. `warn` stays a value for an
  operator who chooses it. Unset means `refuse`, and any other value stops the server's start.
- **`engine` is not accepted** until the user rules H16. The setting is parsed by a `switch` that a third value could
  join.
- **The load-time report is not built now.** No body is typed ahead of time, and there is no endpoint, no saved-query
  scan and no `compilation/compile` trigger. Section 5 below is kept as history only.
- **New size: about 1.5 to 2 days.** About 380 main lines (`PureMeaningCheck` about 300, wiring about 80) and about 400
  test lines (one test class).
- **Guard moves: 2.** `ObservabilityGuardrailTest.ENV_FLAGS` +1, and **one** funnel-register row (`PureMeaningCheck`).
- **The owner confirmed the three points:**
  - constraints are flagged only through `validate` or a DataQuality validation;
  - M2M transforms are flagged by the query's shape;
  - Studio's client is read in its source at the slice.

B1-R (track O), on the compat owner's brief of ~19:03 UTC: "THE DETECTOR at the legend-engine door, with its
load-time report".

**Read against:**
- the spec, final plan §4a (`docs/records/compat-final-plan-2026-11-19.md` on `claude/compat-program`, `49306f885`);
- the rules of record (`compat-rules-of-record-2026-11-19.md`), checked first, in section 8;
- the translator's A3 shape note, revision 3, §3, §3a and §3b (`claude/compat-translator-spike`, `f4a2dd53`), and its
  handoff of ~19:15.

**The rulings this carries:**
- ~~warn by default, with the report (the user, ~14:55)~~, superseded at ~19:20: **refuse by default, no report**;
  `warn` by `LEGEND_LITE_PURE_MEANING=warn`;
- the literal-0 exemptions "per target and arity, as measured" (the compat owner);
- built tested on flagging, landing in the batch after the core switch (the compat owner, ~19:07). Before the
  switch, lite's `substring` is SQL's, like legend-engine's, so a warning would name a difference that does not
  exist yet.

**A shape note only: nothing is built yet.** Its guard moves are named from a throwaway spike that was run against the
guard targets and never pushed (section 6).

## Counts first

| | count |
|---|---|
| days | **about 3**: the detector on 5 routes with its reach, 1.5; the report, 1.5 |
| main lines | about 650: `PureMeaningCheck` about 280, `PureMeaningReport` about 300, the routes' and the composition root's wiring about 70 |
| test lines | about 550, in 2 new classes: 15 detector tests and 7 report tests |
| files | 2 new (edge), 3 changed (`PureV1Routes`, `TestDataRoutes`, `EdgeServer`), 2 new tests, 1 test list (`core/edge_tests.bzl`) |
| guard moves | **2**: the getenv allowlist +1 (`LEGEND_LITE_PURE_MEANING`); the funnel register +2 rows (the two new edge classes) |
| pins, goldens and ledgers that move | **0**: the pure/v1 net records status and JSON only, and none of its requests calls either function; the batteries read status and JSON |
| core hooks | **0**: public API only, inside the edge |

## 1. Where it runs: 5 routes, at the edge alone

| route | where it hooks | what it reads |
|---|---|---|
| `execution/execute` | `PureV1Routes.execute`, after `Compiler.query` | the `TypedQuery` the route already holds |
| `execute?serializationFormat=ARROW_IPC` | `PureV1Routes.executeArrow`, the same | the same; the header sits beside `x-legend-response-format` |
| `execution/generatePlan` | `PureV1Routes.generatePlan`, after `Compiler.query` | the same |
| `generateSeedData`, `generateTestData_WithSeed` and `_WithDefaultSeed` | `TestDataRoutes.doored`, where each types the request's query | the same, with the request's own `mapping` |

The census route (`DONOTUSE_generateTestData`) is left out because it runs nothing. The grammar, compile,
`lambdaReturnType` and `lambdaRelationType` routes compute no value. `compilation/compile` triggers the report
(section 5).

**The setting** is read once, at the edge's composition root, `EdgeServer.UPSTREAM`:
- the values are `refuse`, `warn`, or unset (meaning `refuse`, the user's ruling of ~19:20);
- any other value stops the server's start, so there is no fallback;
- it reaches the routes as a parameter (`PureV1Routes.route(..., meaning)`), and every existing overload refuses.

The spike showed it must be read there: `//core:edge` cannot see the server library, and `EdgeServer` is
`//core:edge_server`'s. The product server is untouched.

## 2. What it flags, and the reach (refined by this session's reads)

**A flag is a call of `meta::pure::functions::string::substring`** (the 2- or 3-argument String overloads,
`Pure.AT_STRING_SUBSTRING`) **or the 2-argument String `indexOf`, in code legend-engine sends to the store.** It is
matched by the typed callee's `FunctionId`, never by name.

**Store-bound or host-side, ASKED of legend-engine 4.145.0 through slice R's oracle** (`h16/hostside-*` on this
branch):
- `{|let x = 'abcdef'->substring(1, 3); ...->filter(r|$r.region == $x)...}` plans as SQL `where region = 'bc'`.
  Pure's answer is computed before the query, so a store-free `let` is host-side and is not flagged;
- `|let x = ...; $x` plans as a `platform` node;
- a literal `substring` inside the routed lambda goes to SQL as written (`select substring('abcdef', 1, 3)`), so it
  is flagged, literal arguments or not.

**The rule:** a statement whose tree reads a store (a `from`, a store accessor, a `getAll` under a mapping) is
store-bound, with the lambdas its functions take. Any other statement is host-side.

**The reach**, read in legend-engine's pinned sources (each citation checked):

| reached from the query | flagged? | because, in legend-engine 4.145.0 |
|---|---|---|
| the query's store-bound statements | yes | sent to the store |
| user functions they call, transitively (memoized by `FunctionId`, cycle-safe) | yes | the same code, sent with the query |
| qualified properties they read | yes | lite types them as calls of their lifted bodies (`Typer.java:555-592`), so the walk follows them as user calls |
| an M2M class mapping's transforms (`<mapping>$class$<C>`, `$assoc$`), when the query is a relation or TDS over a mapping chained on a store | **yes** | `chain.pure:111-116` rewrites each transform into the source query and routes it to the relational mapping. `executionPlanTest.pure:2190` expects `substring("root".fullname, 0, locate(' ', ...))` |
| the same transforms under a graphFetch | **no** | `storeContract.pure:89-91` sends graphFetch to `planInMemoryGraphFetchExecution`, which runs the transform as generated Java (`graphFetchInMemory.pure:742`): Pure's meaning |
| a `Relation` (`~func`) class mapping's expressions | yes | translated to SQL (`pureToSQLQuery.pure:2259-2295`, `:2443-2460`) |
| class constraints, read by a query | **no** (a refinement of §4a's "the constraints that reach SQL") | checked in Java (`graphFetchInMemory.pure:132`, `graphFetchRelational.pure:1002`): Pure's meaning |
| class constraints reached through `meta::relational::validation::validate` or a DataQuality validation's query | **yes** | these two turn constraints into negated SQL filters (`validation/functions.pure:95-117`; `dataquality.pure:99`). Lite serves `validate` (`CoreFn.VALIDATE`) and reads DataQuality validations (`DataQualityReader`) |
| `###Relational` dynaFunctions | no | piece 1 writes them as lite-internal natives with SQL's meaning everywhere |
| a lambda read from an instance, a function value, an untypeable body | `unverified` | never reported clean |

**Quiet under the corrected option:** `substring` is quiet when `CORRECT_SQL_SUBSTRING_INDEXING` is on either carrier,
`TypedFrom.context().features()` or `ExecutionContext.treeFeatures(body)`. **`indexOf` always warns** (legend-engine's
option corrects `substring` alone, `pureToSQLQuery.pure:4662` against `:10644`).

**The literal-0 exemptions, as measured** (`a3/*-literal-zero.tsv` on the translator's branch). The target is the
database type of the request's runtime connection for the query's store: the connection `PureV1Routes.connectionOf`
reads, for the store `query.target()` names. (The handoff's `ExecutionContext.databaseType()` is not a method; the
type is read from the connection's record.)

| target | `substring(s, 0)` | `substring(s, 0, n)` |
|---|---|---|
| H2 | exempt | exempt |
| DuckDB | exempt | flagged |
| any other database | flagged | flagged |

## 3. What it does

`PureMeaningCheck.of(TypedQuery, target)` returns data: the flagged sites (code, position, element, kind) and the
unverified ones. It never changes the query. **The check runs right after `Compiler.query` types the request.**
- Under `refuse` a flagged request is refused there, before anything is planned or run, so it sends nothing to the
  database.
- Under `warn` the route plans and runs the `TypedQuery` it already holds, as before, and adds the header.

- **`warn`, an operator's choice (revision 2: no longer the default):**
  - the header `x-legend-lite-pure-meaning: substring/3=2, indexOf/2=1, unverified=1`. It has at most four codes and
    their counts, never the query's text, and is bounded by construction;
  - `Access-Control-Expose-Headers: x-legend-lite-pure-meaning`;
  - one log line on `System.err`, in the server's `WARNING:` style, with positions and elements, no text and no
    data.
- **`refuse`, the default (revision 2):** a `NotImplementedException` with the same sentence, the header still
  carried on the refusal. `EngineEnvelope.answer` turns it into 500
  `COMPILATION`, legend-engine's error body. An unverified site is refused too.

**The answer is never changed.** Its body is byte-identical with and without the check: a test compares them.

## 4. Tests: `PureMeaningCheckTest`, in `//core:core_edge_tests`

Each test is shown to catch a plant: a planted wrong check turns it red, as the lane's `AudienceCheck` did.

1. A planted request is warned on execute and on generatePlan: 200, the header names both calls, one log line. The
   body equals the same request's body with the check off.
2. The corrected option, on each carrier, quiets `substring` and **not** `indexOf`.
3. A `###Relational` dynaFunction mapping is not flagged.
4. A host-side `let` before the query is not flagged (legend-engine's answer above).
5. `refuse`: the planted request answers 500 `COMPILATION`, naming the calls and the setting.
6. `unverified`: a lambda read from an instance's property is reported, and `refuse` refuses it.
7. Arrow: the header sits beside `x-legend-response-format`.
8. A `#SQL{...}#` island's `substring` is not flagged (piece 1's native).
9. The three test-data routes are warned; the census route is not.
10. The literal-0 exemptions, per target and arity (the table above).
11. The reach:
    - flagged: a user function, a qualified property, an M2M transform under a TDS query over a chain, and a
      constraint reached through `validate`;
    - not flagged: the same transform under a graphFetch, and a constraint a graphFetch checks.
12. The header is bounded: 50 flagged calls give four codes and their counts, under a fixed length.
13. Clients ignore the header: lite's DataCube in engine mode answers the same with it present. Studio's fetch layer
    reads body and content type only; that is read in its source at the slice, since no Studio runs here.
14. The setting: `warn`, `refuse` and unset are taken; any other value refuses to start, naming the value.
15. The pure/v1 net is unchanged: `PureV1EdgeGoldenTest` stays green with its goldens untouched.

## 5. The load-time report: NOT BUILT (the user, ~19:20; kept as history)

As §4a designs it and the user ruled (~14:55): at the edge only, once per model, on a background thread, never on an
answer's path.

**Its question:** where an answer changed at A3. Lite runs every call in the database, so every String `substring`
and 2-argument `indexOf` lite lowers is listed, each with its kind. The store-bound ones are marked.

**Its triggers:**
- `compilation/compile`, which Studio calls when it loads a project;
- the first execute, generate-plan or test-data request for a model not yet reported;
- on demand: `POST /api/compat/v1/pure-meaning` with a model.

**What it scans:**
- the model's bodies, through public calls: `ctx.functionFqns()`, `findFunction`, `SpecCompiler.compile`. Phase E
  lifted every body site into a function: functions, qualified properties, mapping transforms, constraints, service
  queries;
- a name pre-filter first, so only a body naming `substring` or `indexOf` is typed;
- the saved queries (the one store, mounted at both `/api/pure/v1/query` and `/api/lite/v1/queries`):
  - typed against a model that defines the query's mapping;
  - otherwise listed untyped as candidates.

**What it emits:** a log block once per model, at most 200 site lines with `+N more`, and
`GET /api/compat/v1/pure-meaning` answering every site, in JSON. No file.

**Its state:** held by an instance the server's mount creates: a bounded map of 1,024 models, least recently used
out, and one daemon worker. The spike showed a static map fails `ArchitectureTest.staticStateIsConstantOrRegistered`.

**Its cost:** about 4 s once per model on the corpus world (about 8 s when compile triggers it, since compile keeps no
context). H5's V6 measured it by proxy.

**Tests (`PureMeaningReportTest`):** the 7 of the A3 note's §3b:
- a model's four kinds listed, and a list `indexOf` and a dynaFunction not listed;
- once per model;
- never on the answer's path: `compile`'s answer is byte-identical and arrives while a latch holds the report;
- the saved queries, typed and untyped;
- an untypeable body listed `unverified`;
- the 200-line cap;
- `POST` with a model.

## 6. The guard moves, from the spike

The spike: the skeleton in a throwaway worktree at #124's head, run against `//core:guardrails`,
`//core:guardrails_edge` and `//core:census`. It needs exactly 2 moves:

| guard | what tripped | the move |
|---|---|---|
| `ObservabilityGuardrailTest.noNewDebugEnvFlags` | `System.getenv("LEGEND_LITE_PURE_MEANING")` | **`ENV_FLAGS` +1**, with a dated note: deployment config, ruled by the user ~14:55, like `LEGEND_LITE_BIND` |
| `JavaEvalLedgerTest.theFunnelPackagesAreClosedRegisters` | new classes in `com/legend/edge` | **+2 rows**, `PureMeaningCheck.java` and `PureMeaningReport.java`, each with its tenet argument: they read the typed tree, compute no value and change no answer |

**Avoided by design, so not moves:**
- `ErrorShapeGuardrailTest.broadCatchCountsPinnedPerFile`: catch the typing exception (`TypeInferenceException`, as
  `StatementEffects.reaches` does), never `RuntimeException`;
- `IdentityGuardrailTest` (`LOCAL_NAME_COMPARE`): the setting is read with a `switch`, never `.equals`, and calls are
  matched by `FunctionId`;
- `ArchitectureTest.staticStateIsConstantOrRegistered`: the report's state belongs to the mount's instance.

**Not tripped:** `EdgeOnlyMembersTest` (every product member the walk reads has product callers), `PlannerSurfaceTest`,
the `System.err` ceiling (32 of 34 today; the detector and the report share one log site, so it takes 1), and
`HookRulingTest`.

`core/edge_tests.bzl`'s `EDGE_TEST_SRCS` gains the two test files: a build list, not a guard.

## 7. Size and order

1. **Day 1:**
   - `PureMeaningCheck`: the walk, the store-bound rule, the reach, the exemptions, the option;
   - the 5 routes' wiring and the setting;
   - tests 1 to 10 and 14.
2. **Day 2:**
   - the reach's mapping kinds (test 11);
   - the bounded header and the clients (tests 12 and 13);
   - the net (test 15);
   - the report's scan and its state.
3. **Day 3:**
   - the report's triggers, saved queries and endpoint;
   - its 7 tests;
   - `//gates:local`, then one full CI run on the branch.

It lands in the batch after the core switch (piece 2), whose PR it joins or follows, as the compat owner ordered. The
engine-answer opt-in (H16, sized separately) would sit on top of it, only on the user's word.

## 8. The rules of record

| rule | holds | how |
|---|---|---|
| the detector never changes a query | yes | the check returns data; the route plans the `TypedQuery` it already holds; test 1 compares the bodies |
| no hook, switch or mode in the core | yes | the setting is read at the edge's composition root, and the core is untouched. The route parameter is the edge's own |
| 1: the database computes | yes | it computes no value; it reads types |
| 2 and 3: presentation | yes | a header and a log line, from typed facts; no answer is reworded |
| 4: the host judge | not touched | |
| tenet 6: no modes in the core | yes | the audience difference lives at the door |
| the eager typing | ruled | §4a and the user's ~14:55 row; it is the work `compilation/compile` already does, off the answer's path |

## 9. Points for the compat owner

1. **Constraints:** §4a says "the constraints that reach SQL". In legend-engine they reach SQL only through
   `validate` and DataQuality's negated query. (Corrected in section 10: lite's pure/v1 door does not serve
   `validate`, which it refuses at typing; the desugared form is flagged.) So the detector flags a constraint reached
   through either, and no other; the report lists every constraint either way, since A3 changed their answers too.
   Confirm, or name another case.
2. **M2M:** flagged only for a relation or TDS query over a chain, as legend-engine plans it, and never under a
   graphFetch. This follows the read above rather than "flag every transform".
3. **Studio:** its fetch layer is read in its source at the slice (test 13); no Studio runs in this container.

## 10. The reviewer's findings, fixed (record 88e3def8, `docs/records/compat-pr-reviews-2026-11-19.md` on `claude/compat-review`)

**M1, the untested reaches.** Five tests were added, plus two for S2 and the second UNVERIFIED site. Each one fails on
its plant. Each plant is a sabotage of `PureMeaningCheck.java`, run against `PureMeaningCheckTest` (29 tests); the
source was restored after each run.

| plant | the tests it turns red |
|---|---|
| P1 `w.mappingReach(features)` deleted | `anM2MTransformInATdsQueryOverAChainIsFlagged`, `aMappingTheModelDoesNotDeclareIsUnverified` |
| P2 the `chained`/`graphFetch` gate inverted | the chained, the graphFetch and the unchained M2M tests |
| P3 the function-value UNVERIFIED site dropped | `aLambdaReadFromAnInstancesPropertyIsUnverified_andRefused` |
| P4 the untyped-body UNVERIFIED site dropped | `aFunctionWhoseBodyDoesNotTypeIsUnverified` |
| P5 a filter's predicate not walked (validate's inlined filter skipped) | `aConstraintValidateTurnsIntoAFilterIsFlagged_andTheDoorRefusesAValidateRequest`, `aHostSideLetIsNotFlagged_butTheSameCallInTheQueryIs` |
| P6 each read class's constraints walked | `aConstraintIsNotFollowed_legendEngineChecksItInJava`, `aQualifiedPropertyTheQueryReadsIsFollowed`, the validate test, the S2 test |
| P7 an unresolved mapping skipped as clean (S2) | `aMappingTheModelDoesNotDeclareIsUnverified` |

The first P5 and P6 plants did not turn anything red, because they did not plant what their names say:
- A filter types as `TypedFilter`, not as a `TypedNativeCall` named filter, so the first P5 plant never fired.
- No typed query calls a constraint's lifted body. A constraint reaches SQL in lite only in phase H, for a graphFetch
  `checked` (`GraphEmission`), which legend-engine checks in Java. So skipping the CONSTRAINT hats in `follow` changes
  nothing, and the honest plant for "a check that wrongly follows constraints" walks each read class's constraints.

Both were re-planted as the table shows.

**M2.** The constraint is now `substring(1, 2)`. The same P6 plant run on the old `substring(0, 1)` leaves
`aConstraintIsNotFollowed` and `aQualifiedPropertyTheQueryReadsIsFollowed` green, because H2 exempts a literal 0 start.
On the new constraint both turn red. That is the reviewer's point, measured.

**S1.** `PureMeaningCheck`'s javadoc, and the test's, now cite "D1, ruled by the user 2026-11-19 ~13:25 ("Okay agree
with all")" and the default ruled ~19:20. They no longer quote program wording as the user's.

**S2.** In `mappingReach`:
- a mapping the model does not declare is a site `unverified` ("mapping X (not in the model)");
- a binding whose lifted body the model does not hold is one too.

Neither is skipped as clean. `aMappingTheModelDoesNotDeclareIsUnverified` plants a test-data request's mapping that
is not in the model.

**Two findings the tests turned up:**
1. **`validate` at the pure/v1 door.** The door types a request through `Compiler.query`, which does not run
   `ValidateDesugar`; only the query front door `Compiler.resolveQuery` does (the test runners and service tests). So
   a pure/v1 `validate` request is refused at typing (500, "validate is desugared before typing") and never answered.
   No answer with Pure's meaning escapes, so the detector has nothing to flag there.
   - The validate test proves the desugared form (`resolveQuery` then `queryResolved`) is flagged as store-bound
     code.
   - It also proves that the door refuses a validate request.
   - Point 9.1's "lite serves both" was wrong for this door.
2. **A chain in a packaged runtime.** A `###Runtime`'s `ModelChainConnection` does not reach the typed context:
   `ExecutionContext.chainMappings` is filled from a runtime value, or from `withChainedMappings` placed right before
   `from`. That is lite's known gap (StressServiceSuitesTest: "the test runtime losing the ModelChainConnection's
   mapping"). Such a request fails at lite's planning, so no wrong answer escapes. The chain tests use
   `withChainedMappings`, the form lite types.

**The model text these tests needed** moved the parser-equivalence ratchet `own_corpus.matched` 3312 → 3315. Four new
one-line declarations are new own-corpus candidates, and a verdict is per element. The move is under the user's ruling
of ~20:10 ("Yes" to the ratchet regenerated for the detector's test models, the final value regenerated on the merged
tree), applied to it by the compat owner at ~20:57. On the merged batch with A2's 3310 → 3311 it is regenerated on the
merged tree: 3310 + 2 + 3 + 1 = 3316, expected. The compat owner sent both findings above to the product track as
recorded gaps.

**The proof** (the reviewer's gap) is recorded in the commit that carries these fixes:
- `//core:core_edge_tests`, with the 29 detector tests;
- `//gates:edge_net`, which runs `PureV1EdgeGoldenTest` and its golden diff. No golden file changed since #124's head
  `f832a6671` (`git diff --name-only f832a6671` lists none);
- `//pct:batteries_test`, the batteries, which call the route under `warn`.

**The section fix (the judges' fast loop on the batch, ~21:30).** `//parser-equivalence:parser_parity` found six
of `PureMeaningCheckTest`'s model literals without their `###Section` headers: a Database, two Mappings, a
Connection and two Runtimes. `OwnDialectCensusTest` refused them at LEGEND_LITE, and they added
`GRAMMAR-REFUSAL-nullmsg=6` to the leniency ledger. Each non-Pure element's literal now carries its own header.

The class holding a function value (`Function<{String[1]->Integer[1]}>`) was replaced:
- It is lite's own dialect, and the reference grammar refuses it ("The type ... is not supported yet").
- So no legend-engine client can send it to this door.
- It also moved three pins: PURE-DIALECT-function-types 20 → 21, a new adjudication kind, and an extension host.

The unverified function value is now a lambda read out of a collection (`[{s|...}]->at(0)->eval(...)`), which
legend-engine's grammar writes. Plant P3 still turns it red. After the fix `parser_parity` is green, and only
own_corpus.matched moves, 3315 → 3321, under the ~20:10 ruling.

## 11. Past the store's result (the reviewer's E-M1, record 285f98e18): asked of legend-engine

The question: a call applied to the store's result after a step legend-engine might run on the host. The detector
makes the whole statement store-bound, so under `engine` it would be rewritten.

legend-engine 4.145.0 was asked one probe per shape, on H2 2.1.214 and DuckDB v1.3.0, through the engine runner. The
model was the trades model with a class, a relational mapping and a runtime appended. Evidence:
`docs/records/compat-homework/a3-engine-probes/`, holding the requests, legend-engine's answers and plans, and lite's
answers under `engine` and `warn`.

| shape | legend-engine | where |
|---|---|---|
| a class query's terminal `->toOne().desk->substring(1, 3)`, `(1)`, `(0, 2)`, `indexOf('e')`, after `toUpper()` | `Cre`, `Credit`, H2 `Cr` / DuckDB `C`, `3`, `CRE` | in the SQL |
| `map(t\|$t.desk->substring(1, 3))`, inside `from` and after it; `map(...)->toOne()`; `map(...)->distinct()->toOne()` | `Cre` | in the SQL |
| a class `project` and a class `filter` with `substring` | `Cre`, the row | in the SQL |
| a graphFetch, then `->toOne().desk->substring(1, 3)` | `Cre` | in the SQL |
| a graphFetch tree reading a qualified property (`deskPart()`, a `substring`; `deskAt()`, an `indexOf`) | `Cre`, `3` | in the SQL |
| `{\|let d = ...->toOne().desk; $d->substring(1, 3);}` | `re` | **on the host**, Pure's meaning |
| `{\|let n = ['ax', 'bx']->map(s\|$s->indexOf('x')); ...->filter(r\|$r.qty->in($n))...}` | `qty in (1, 1)` in its SQL | **on the host**, Pure's meaning |
| `serialize(...)->substring(...)`, a TDS `.rows->at(0).getString(...)` | refused | n/a |

So every shape legend-engine answers past the store's result is computed in the database, which is what the `engine`
rewrite gives. The one host-side position is a separate statement reading a let. The detector does not make it
store-bound, so it is neither flagged nor rewritten.

Lite's door today:
- It refuses every scalar or collection result before planning, on every setting ("a SCALAR/COLLECTION result's
  serialization is unprobed"), so none of the value shapes can be answered wrong there today.
- It answers the TDS, filter and relation shapes, and they agree under `engine`.

**One gap the probes found, fixed here.** A graphFetch tree's properties are not the node's children
(`TypedGraphFetch.children()` is its source alone). So the detector did not follow a qualified property the tree
reads, though legend-engine computes it in the database with SQL's meaning. After the switch, `refuse` would have
answered it with Pure's meaning, silently.
- The walk now follows each tree node: a derived property's body, each node's arguments, and each class the tree
  reaches (added to the mapping reach).
- A property it cannot find is `unverified`.
- Test `aQualifiedPropertyAGraphFetchReadsIsFollowed`, with its partner. Plant P8 (the tree not walked) turns it red.

**A second gap, from the reviewer's E-S1, fixed here.** The detector walked a let named by store-bound code whenever its
value *held* a lambda anywhere. That includes a value computed with one, such as
`let n = ['ax', 'bx']->map(s|$s->indexOf('x'))`.

legend-engine computes such a store-free let on the host before the query, with Pure's meaning: its SQL reads
`qty in (1, 1)`. The consequences of the old rule:
- under `refuse`, an over-refusal;
- under `engine`, if the value still held a lambda after inlining, the database's meaning where legend-engine gives
  Pure's.

The walk now takes a let only when its value IS a lambda. Test `aLetComputedWithALambdaIsHostSide_notStoreBound`,
against `aLetBoundLambdaTheQueryNamesIsStoreBound`. Plant P9 (a computed let walked again) turns three tests red.

**The reviewer's notes on record a9e827e2e (E-M1 closed, e4496b763 approved):**
1. **The host-side shape is pinned.** `aCallOnAStoreReadLetInAStatementOfItsOwnIsHostSide_andInTheOneStatementIsFlagged`
   checks two things: `{|let d = <store>...->toOne().name; $d->substring(1, 3);}` is clean, and the same chain in one
   statement is flagged. Until now only `StatementEffects.reaches` not following variables held this.
2. **A serialize tree naming a qualified property the fetch tree lacks: probed.** legend-engine never computes it, on
   H2 and on DuckDB:
   - `deskPart()` (a `substring`) serializes as `null`;
   - `deskAt()` (an `indexOf`) fails mid-stream with a NullPointerException.

   No `substring` or `indexOf` runs, so the detector need not walk the serialize tree. Lite computes the property
   (`"initials()":"ab"` today). That difference is the serialize tree's, not a meaning's: it holds for any qualified
   property. The probes are in `a3-engine-probes/` as `graph.serialize.extra.*`.

   **The compat owner's call (~23:14): no door refusal.** Lite computing a qualified property the serialize tree
   names and the fetch tree lacks is Pure's meaning, since legend-pure computes it. legend-engine's `null`, or its
   NullPointerException mid-stream, is legend-engine's quirk. Under the goal ("lite follows Pure ... the rest is
   recorded"), it is a recorded difference, not a gap. **Ruled by the user ~23:20 ("Yes to A", confirmed ~23:25,
   "Keep it"): register row S388** (lite computes the property from its own Pure body; legend-engine serializes
   `null` or fails), on `claude/compat-a3-docs`, with these probes as its witness.
3. **Under `engine`, a qualified property a graph-fetch tree reads is REFUSED by name.** The detector reaches it through
   `follow`, a body the rewrite cannot reach, since the store resolver inlines a graph tree's properties after the
   body exists. That is a conservative refusal: legend-engine computes it in SQL, so `engine` could answer it once the
   rewrite reaches phase H. The test is `aGraphFetchsQualifiedPropertyIsRefusedByNameUnderEngine` on the review branch.

**The reviewer's round on record 7b70932b3: a probe and R2-S1.**
- **The probe: a statement that reads a store-bound let through a variable, with no store read of its own.** The
  detector does not follow the variable. legend-engine (`let.rel.*`, `let.cls.*`, `let.value.*` in
  `a3-engine-probes/`, H2 and DuckDB), shape by shape:
  - a relation let read by a later `extend` or `select`, with `from` inside or after it: refused ("No SQL translation
    exists for the PURE function 'letFunction...'", or "RelationStoreAccessor should be used with a from(Runtime)
    expression");
  - a class collection let read by a later `map(p|$p.desk->substring(1, 3))`: computed on the host, Pure's meaning,
    `["re"]`;
  - a value let whose own value holds the call (`let d = ...->toOne().desk->substring(1, 3); $d->toUpper()`): the
    let's call in its SQL, `substring(desk, 1, 3)`, answering `"CRE"`.

  So not following a variable is right. legend-engine either refuses the shape, or computes the later statement on
  the host. A let whose own value holds the call is a store-bound statement, so it is flagged. Three tests pin these:
  `aCallOverAStoreReadCollectionLetIsHostSide_andInTheOneStatementIsFlagged`,
  `aCallOnAStoreReadLetInAStatementOfItsOwnIsHostSide_andInTheOneStatementIsFlagged` and
  `aStoreBoundValueLetsOwnCallIsFlagged`.
- **R2-S1:** an `eval` of a variable bound by a let the walk did not take is UNVERIFIED. The typer substitutes a
  literal lambda let into its uses, so a variable left under an `eval` is a let whose value is a function computed some
  other way (`[{s|...}]->toOne()`, a function's answer, a cast), and its body is never read.
  - Only an `eval` met in the query's own tree counts. A followed function's own parameter is unaffected.
  - Test `anEvaluatedLetWhoseValueIsNotALiteralLambdaIsUnverified`; plant P10 (the site gone) turns it red.


## 12. substr and the corrected substring (2026-11-20; the compat owner's question, asked of legend-engine)

The detector flagged neither. `substr` was not matched, and `substring` under `CORRECT_SQL_SUBSTRING_INDEXING` was quiet
on purpose, since legend-engine's processSubstring honours the flag. Both rested on lite's corrected answer being
legend-engine's. Once S383's range check is under them (the translator's 97d4c78b2), that holds in range only. Out of
range lite raises, as legend-pure does, and legend-engine answers the database's cut-short SUBSTRING
(docs/records/compat-homework/a3-engine-probes/substr/). So the detector now flags four more forms: `substr/2`,
`substr/3`, `substring(corrected)/2` and `substring(corrected)/3`, each marked as differing out of range alone.

- These forms have no literal-0 exemption: none is measured.
- Under `engine` the uncorrected `substring`'s literal-0 exemption is off too (the reviewer's M-a1,
  `PureMeaningCheck.forEngine`). `refuse` and `warn` keep it.
- The engine rewrite of these forms is in A3-engine-rewrite-design.md §9.
- Under `engine` they become the lite-internal native `substrAsGiven` (the user's word ~07:45, "Okay 1"), its
  positions computed by the database, a computed index included.
- Still with the user: what the default `refuse` does with these forms (A: refuse them; B: answer them with the header,
  since no answer is silently different).
