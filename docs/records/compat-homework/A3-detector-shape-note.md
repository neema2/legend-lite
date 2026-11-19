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
   `validate` and DataQuality's negated query, and lite serves both. So the detector flags a constraint reached
   through either, and no other; the report lists every constraint either way, since A3 changed their answers too.
   Confirm, or name another case.
2. **M2M:** flagged only for a relation or TDS query over a chain, as legend-engine plans it, and never under a
   graphFetch. This follows the read above rather than "flag every transform".
3. **Studio:** its fetch layer is read in its source at the slice (test 13); no Studio runs in this container.
