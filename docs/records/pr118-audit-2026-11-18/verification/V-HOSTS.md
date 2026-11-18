# V-HOSTS verification

PR #118 head `68f20eb4d`, main `4dfbb5b7`. Everything below was read at the head checkout. Runs: none of the repository's
tests. One scratch probe of DuckDB's transaction state (duckdb-python 1.4.1 in a scratch venv), for HOSTS-2.

| Finding | Claimed | Verdict | New in PR? | Ruled? | Covered by a test? | Fix size |
|---|---|---|---|---|---|---|
| ARCH-1 | BLOCKER, WRONG-ANSWER | **FIX-AFTER**. The narrow fact holds: hosts never read `siteWords`. The premise is false: the server does not answer legend-pure's words either. For a planned site it answers `Raised: <KIND>`. No route matches legend-engine, and the kinds are owned by F43/F44. | Yes, both halves: K10 (`b241648e2`) for the hosts; sites (v56) plus `EngineEnvelope` for the server. At main the hosts passed the raw envelope on, and the server answered `Raised: <legend-pure sentence>`. | Not ruled. S308 (2026-11-10, "Pure's full sentence ... for PCT and pure/v1") was superseded on pure/v1 by the census ruling (2026-11-17, "the pure/v1 route matches legend-engine"). Class B kinds → F43, class E → F44. | No. `m2_...` checks only that the plan half carries `siteWords`, and it runs under `UPSTREAM`, not the hosts' `PURE_V1`. The golden has no checked-site raise. No host test sends a raising pure/v1 execute. | Server ~4 lines (`EngineEnvelope`). Hosts ~15 lines each, in the edge's host code. Or fold all of it into F43/F44. |
| HOSTS-1 | BLOCKER | **BLOCKER (confirmed)**. Trivial to fix. | Yes: all 8 targets and their test files were added in the PR. | No. None is tagged `manual` or listed in `heavy`. | CI's lane guard catches it: that is why `linux / product` is red. | 8 lines in 5 suites, then one CI run. |
| HOSTS-6 | FIX-AFTER | **FIX-AFTER (confirmed regression)**. Fold it into the lane-fix commit. | Yes: H11 moved the plane into `legend-engine.js`, and `make-dist.mjs` was not updated. | No | No | 2 lines, plus a ~20-line dist test |
| HOSTS-7 | FIX-AFTER, BROKEN-RULE | **NOTE**. Both bypasses are real, but neither breaks K2: each needs the caller to name an edge symbol explicitly. | `EdgeServer` is new. At main, legend-engine's routes were always on. | Partly: ruling 6 says "the split later is one more main class". | `ArchitectureTest` names `EdgeServer`. No test covers the Python bypass. | ~4 lines plus a planted main (Java); ~5 lines (Python) |
| HOSTS-3 | FIX-AFTER | **FIX-AFTER**. Real: with the edge off, Python still answers one legend-engine route. Pre-existing, and narrowed by the PR. | No: the route was on main, where every legend-engine route was the product's. | No. It contradicts ruling 6's "legend-engine's are absent" when the edge is off. | Product tests pin the route itself (`test_engine.py:1211-1242`, `test_wheel.py:133-134`). | ~20 lines with tests |
| HOSTS-4 | FIX-AFTER | **NOTE / FIX-AFTER (low)**. Real coupling. Nothing loads with the edge off. | Yes (K2's native split) | No | The EdgeOff test reads the same hand-kept list | ~30 lines moved |
| HOSTS-5 | FIX-AFTER | **REFUTED as a K2 break (ruled)**. A NOTE. | Yes | **Yes**: Lite API v1 §6, ruled 6, "one artifact serves lite's routes and the edge's legend-engine routes"; K2, "one artifact per surface". | n/a | 0 |
| HOSTS-2 | WRONG-ANSWER, FIX-AFTER | **REFUTED as a wrong answer**. A NOTE. The code is unchanged from main. On DuckDB a failed COMMIT leaves no transaction open (measured), so no ROLLBACK is owed. | No: identical at `main:engine-client/src/plan-runner.ts:240-258` | No | No | 0 (3 lines for symmetry, if wanted) |

**Extra finding from ARCH-1 (V-HOSTS-A), FIX-AFTER.** K10's server half is vacuous on pure/v1. `RaisedWords` rewords the
`DataError`, but `EngineEnvelope.answer` writes the error's *cause* (the `Raised`), so the route answers
`Raised: DATE_PART_ABSENT`. That is lite's internal kind name. At main the same request answered legend-pure's sentence.
The reworded message reaches PCT only, which reads `DataError.getMessage()`.

## Details

### ARCH-1: the edge host routes drop legend-pure's raise words

**The input.** pure/v1 `POST /api/pure/v1/execution/execute`, legend-engine's JSON, with function
`|#>{<db>.<table>}#->extend(~[h: x|%2024->hour()])->select(~[h])->from(<runtime>)`:
- on Python, over a registered frame (`legend_lite.edge.engine(frames)`);
- in the tab, through `BrowserEngine`;
- on the server, the same query over `trades::h2::DB.TRADES_SCHEMA.TRADES`, `->from(trades::h2::RT)`.

**Lowering, shared by every route.**
1. `Scalars.java:1243-1250`: a partial date without the hour → `PureSql.raise(new RaiseText(DATE_PART_ABSENT, [StringLit("hour"), <the date>]))`.
2. A partial date literal lowers to a string literal (`Lowerer.java:3694-3696`, "represented as string literals here").
3. Both operands are therefore literals, so the site is wordable (`PureMessages.java:111-114`, `:132-145`). Every edge
   dialect then writes a site (`PureText.java` DuckDb, H2 and the others: `raiseMessage` → `wordable ? site(r) : sentence(r)`).
4. `AnsiSqlRenderer.java:1241-1262`: the statement raises the bare token `LG#1`.
5. Under `PURE_V1`, DATE_PART_ABSENT is not in `ENGINE_UNCHECKED` (`PureText.java:205-211`), so the raise stays.

**Python at the head.**
1. `_pure_v1.py:174` → `_api` → `:183` `_execute_json` → `:231` `execute_json_plan` → native `lite_execute_json_plan`.
   That is `EngineEntries.java:70-75` → `EdgeBoundary.java:72-73` → `HostAnswers.jsonPlan(body, models, PureText.PURE_V1)`.
2. `HostAnswers.java:276-279` writes two things:
   - `sites`: n 1, kind DATE_PART_ABSENT, and the product message "the date has no such part: it is coarser"
     (`ExecutionPlan.Site.sentence`, `ErrorKind.DATE_PART_ABSENT`);
   - `siteWords`: `[{"n":1,"words":"Cannot get hour for 2024"}]` (`SiteWordsRecorder.java:46-66`).
3. `_pure_v1.py:235` `declare_sites` declares `sites` alone (`_raised.py:45-70`).
4. The run: `run_rows_json` → `send` → `_planned.py:440` `worded(e, sql)` → `raised_words` → `sites[0]['message']` (`_raised.py:95`).
5. `_api`'s `except` (`_pure_v1.py:185-197`) makes the message `InvalidInputException: the date has no such part: it is coarser`.
   `refusal()` then answers **500 `{"code":-1,"message":"InvalidInputException: the date has no such part: it is coarser","status":"error"}`**.

**Tab at the head.**
1. `edge-engine.ts:58-64` `executeText` → `wasm-edge.ts:144-155` `executePlan`, which declares `sites` (`declared-raises.ts:34-47`).
2. `#engine.run(plan.run)` fails in DuckDB-WASM, and `raised.ts:58-67` `failure` turns it into
   `PlanRaise(kind DATE_PART_ABSENT, "the date has no such part: it is coarser")`, thrown to the caller.
   Before K10 it was `site.upstream ?? site.message` (`git show b241648e2 -- engine-client/src/raised.ts`).

**Server at the head.**
1. `EdgeServer.java:38-42`: `RaisedWords.run(PureText.PURE_V1, ...)` → exec's `RaisedErrors.dataErrorOf` (`RaisedErrors.java:272-277`).
   The result is `DataError(DATE_PART_ABSENT, null, <product message>, cause = Raised)`.
2. That `Raised`'s own message is `\u001cDATE_PART_ABSENT` (`RaisedErrors.java:108-111`).
3. `RaisedWords.worded` (`RaisedWords.java:39-50`) makes a new `DataError(..., "Cannot get hour for 2024", cause = the same Raised)`.
4. `PureV1Routes.answer` (`:60-63`) → `EngineEnvelope.answer` (`EngineEnvelope.java:58-71`). The error is not a text
   refusal, so `n = e.getCause()` = the `Raised`, and the message is `typeName(n) + ": " + PureMessages.compile(n)`.
5. `compile` returns the kind's name for a message that starts with the mark (`PureMessages.java:320-328`). The answer is
   **500 `{"code":-1,"message":"Raised: DATE_PART_ABSENT","status":"error"}`**, or `Positioned: ...` when the lambda
   carries source positions.
6. The reworded `DataError` message is never read on this route.
7. Corroboration: the edge golden net recorded exactly this shape for site raises before B7. `raise.divideByZero` was
   `{"code":-1,"message":"Raised: DIVIDE_BY_ZERO","status":"error"}` at `16ee7d660`, `719953eb3` and `6a016424e`
   (`git show <c>:core/src/test/resources/edge/pure_v1_answers.tsv`). Caveat: those were recorded through the product's
   runner, before F31. The envelope mechanism is the same.

**legend-engine 4.145.0.** The census, class B (`docs/records/pure-v1-engine-census-2026-11-17.md`), says it refuses at
planning:
- on H2, Postgres and Snowflake: "Ensure the target system understands Year or Year-month semantic.";
- on DuckDB: "DuckDB doesn't support YEAR and YEAR-MONTH".

**At main (`4dfbb5b7`, plan v51, no sites).**
- The product dialect wrote legend-pure's sentence in SQL.
- The server answered `Raised: Cannot get hour for 2024` (`main:PureV1Api.java:1308-1311` `named`; the `Raised` message
  is the clean text, `main:RaisedErrors.java:76-79`).
- The hosts unwrapped nothing: `raised.ts` and `_raised.py` do not exist at main.
  - Python answered `InvalidInputException: Invalid Input Error: \x1f…Cannot get hour for 2024\x1f`.
  - The tab threw a `QueryError` with DuckDB's raw text (`main:engine-client/src/duckdb.ts:563-567`).

**So.** No route answered upstream's bytes for this kind at main, and none does at the head. At the head the server says
lite's kind name and the hosts say lite's product sentence. ARCH-1's "for the same query, the server answers legend-pure's
words" is false.

**Reachability.**
- **TS: no shipped app reaches it.** `BrowserEngine` is constructed only in tests (`datacube/test/python-engine/execute.test.ts:149`,
  `engine-client/test/plan-runner.test.ts:637`, `studio/test/engine.test.ts:119`). Root `BUILD.bazel:63-67` says "No
  shipped app wires that configuration yet". `engine-client` is `"private": true`.
- **Python: reachable only through `legend-lite[engine]` plus `legend_lite.edge.engine()`** and a pure/v1 client.
  `//python:engine_wheel` is built by no CI job.

**Ruled?** S308 (in `docs/records/d-register-rows-2026-11-16.md`, not in `SEMANTICS_REGISTER.md` at the head) quotes the
user: "Pure's full sentence with the value is spelled at the edge, for PCT and pure/v1". For pure/v1 this was superseded by
the census ruling, "**the pure/v1 route matches legend-engine**". The follow-ups own the kinds (`edge-follow-ups-2026-11-18.md:90-91`):
- F43: the class B kinds (DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, DATE_COMPONENT_INVALID);
- F44: the lite-only kinds.

No row rules what the head answers.

**Covered?** No.
- `PureV1ApiTest.m2_theHostsPureV1HalvesRaiseInPuresWords` (`:1008-1032`) only checks that the plan half carries
  `siteWords`. It plans under `PureText.UPSTREAM`; the hosts plan under `PURE_V1` (`EdgeBoundary.java:62,73`), where its
  divide-by-zero is a plain operation with no site at all.
- `pure_v1_answers.tsv` has no checked-site raise.
- A grep of `python/tests` and `engine-client/test` finds no raising pure/v1 execute.

**Probes.**
1. Server: add to `PureV1EdgeGoldenTest.REQUESTS`
   `REQUESTS.put("raise.datePartAbsent", "|" + TRADES + "->limit(1)->extend(~h: r|%2024->hour())->select(~[h])" + RT);`
   then run `bazel run //core:update_edge_pure_v1 && git diff core/src/test/resources/edge/pure_v1_answers.tsv`.
   Predicted: `{"code":-1,"message":"Raised: DATE_PART_ABSENT","status":"error"}`.
2. Python, in `python/tests/test_edge.py`, class `UnexpectedFailures`:
   ```python
   def test_a_planned_raise_answers_as_the_server(self):
       status, refused = self.error('/api/pure/v1/execution/execute',
                                    self.execute_input(self.query('->extend(~[h: x|%2024->hour()])->select(~[h])')))
       self.assertEqual(status, 500)
       self.assertIn('Cannot get hour for 2024', refused['message'])
   ```
   Predicted to fail. The message will be `InvalidInputException: the date has no such part: it is coarser`.
3. TS, in `engine-client/test/plan-runner.test.ts` beside `:636`: `await assert.rejects(browser.execute(<input with %2024->hour()>), /Cannot get hour for 2024/)`.
   Predicted to fail with "the date has no such part: it is coarser".

**Fix.** The right fix is F43/F44, which replace these answers with legend-engine's. Until then, two small fixes:
- **Server (V-HOSTS-A).** In `EngineEnvelope.answer`, write a `DataError` whose cause is a `Raised` with a site as
  `typeName(cause) + ": " + e.getMessage()`, the reworded words. About 4 lines; the probe-1 golden line proves it.
- **Hosts.** The edge's host code (`edge-engine.ts`, `_pure_v1.py`) rewords from `siteWords` by site number. The
  product readers stay lite's (H9). `PlanRaise` and Python's `worded` copy need to carry `n`. About 15 lines per host,
  with probes 2 and 3 as the tests.

**Grade: FIX-AFTER.** It is not a blocker:
- no route ever gave upstream's bytes for these kinds;
- the server ARCH-1 compares against answers a lite kind name;
- the TS path ships in no app;
- recorded follow-ups own the kinds.

### HOSTS-1: 8 tests in no CI lane

**Facts.** The CI log is saved under the scratchpad at `audit/verify/hosts_ci/product.log` (lines 729-737). It holds
`##[error]tests in no lane of gates/BUILD.bazel:` followed by the 8 targets. The guard is `gates-run.yml:258-259`
(`tests(//...) except tests(//gates:lanes) except tests(//gates:heavy)`).

- All 8 test files are new in the PR (`git log --diff-filter=A main..68f20eb4d`).
- None is tagged `manual`. None is in `heavy`. None is in D's 41-target heavy round (handoff §7.2's list).
- Two package suites leave them out: `//engine-client:tests` (`engine-client/BUILD.bazel:659-673`, "every test here: the
  local gate and CI's `ui` lane run it") and `//studio:tests` (`studio/BUILD.bazel:253-263`).

| Target | What it tests | Lane by existing membership |
|---|---|---|
| `//core:is_numeric_probe_test` (`core/BUILD.bazel:1167`) | J20 / S367: isNumeric planned and run on DuckDB, H2, SQLite and the embedded Postgres, with the same answers on each | `core`, beside the embedded-Postgres probes (`postgres_arm_test`, `relation_to_one_postgres_test`) |
| `//core:lite_api_conformance_test` (`:1396`, tagged `edge_test`) | lite API v1's shared cases against the browser binding, on the JVM | `core`, beside `raise_envelope_conformance_test` and `keyed_expansion_conformance_test` (edge-tagged tests already sit in lanes: `guardrails_edge` and `census_edge` are in `checks`) |
| `//engine-client:engine_module_test` (`:561`) | K2: the tab's engine module loads only for `'legend-engine'`; any other word is refused and nothing opens | `ui`, via `//engine-client:tests` (its sibling `plan_runner_test`, also `engine = True`, is there) |
| `//engine-client:lite_api_server_test` (`:576`) | lite API v1's cases against `//core:server` over HTTP | `ui`, via `//engine-client:tests` (sibling `//query-store:lite_test` is in `ui`) |
| `//python:edge_test` (`python/BUILD.bazel:533`) | K2 with the edge on: the edge's door, the engine library's exports, pure/v1 byte-exact | `warehouse`, beside `//python:engine_test` (it also builds `//native:engine`, a second native image) |
| `//studio:run_test` (generated by the glob, `studio/BUILD.bazel:197`) | H3: Run's parameter literals read into run values | `ui`, via `//studio:tests` |
| `//wasm:engine_routes_test` (`wasm/BUILD.bazel:259`) | K2 with the edge on: the engine module's routes against the pins in `engine-routes.json` | `datacube`, beside `//wasm:differential_test`, `zone_test` and `conformance_test` (or `parser_equivalence`, where `//wasm:round_trip_test`, also `engine = True`, lives) |
| `//wasm:product_module_test` (`:251`) | K2 with the edge off: the planner module runs alone and exports no legend-engine route | `datacube` |

**Why it is a blocker.**
- The PR's CI is red because of it, so the PR cannot merge green.
- These are the only runtime proofs of K2 for the tab and for Python. In no lane, a regression of K2 would reach `main`
  unseen: a guard made vacuous in effect.
- They have never run in CI, so adding them may surface failures.

**Fix.** About 8 lines in 5 suites:
- `//engine-client:tests` += `:engine_module_test`, `:lite_api_server_test`;
- `//studio:tests` += `:run_test`;
- `gates/BUILD.bazel` `core` += the two `//core:` targets;
- `datacube` += the two `//wasm:` targets;
- `warehouse` += `//python:edge_test`.

Then one CI run at the new head. The lane guard itself is the test.

### HOSTS-6: DataCube's shipped app lost its legend-engine plane

**The walk, by reading.**
1. Open `//datacube:app`, or `//datacube:dist` (which `:app` serves, `datacube/BUILD.bazel:2457-2476`), at
   `index.html?planner=engine&engine=http://localhost:6300`, with a legend-engine running there.
2. `boot.ts:55` offers the `engine` plane. `planners.ts:44-45` → `onServer(model, 'engine')`.
3. The health check `GET <url>/api/server/v1/info` passes (`:88-90`).
4. `:93` calls `import(new URL('./legend-engine.js', import.meta.url))`. That fetch is a 404: `make-dist.mjs:57-59`
   copies only `bundle` and `bundle-engine` with their `chunks-*` directories, and the file is unchanged in this PR.
5. The import rejects with a TypeError, "Failed to fetch dynamically imported module: …/legend-engine.js".
6. `boot.ts:261-268` shows "failed to start: …". The URL is named, but this is no refusal by name.

**At main.** `planners.ts` imported `UpstreamPlanner` statically (`main:datacube/demo/planners.ts:23,75`). The plane was
in `bundle.js` and worked from `//datacube:dist`.

**At the head.**
- The plane works from `//site:dist` (the one-origin GitHub Pages site): `site/make-dist.mjs`'s `LEFT_OUT` does not exclude
  `legend-engine.js`.
- It is broken from `//datacube:dist`, `//datacube:app` and `datacube-app.tar.gz`.
- Python's `wheel_site` takes only the engine page and the widget, so it is unaffected.

**Covered?** No.
- `verify-picker.mjs:191` checks `?planner=engine` only up to the refusal when no engine is running, which happens before
  the import.
- No test fetches `legend-engine.js`.

**Fix.** Add `['legend-engine.js', 'chunks-legend-engine']` to make-dist's copies (2 lines: the bundle is built with
`--splitting`, so the chunk directory must go too). Then add a dist test: for each `new URL('./X', import.meta.url)` in
`datacube/demo/*.ts`, assert that `dist/X` exists (about 20 lines). Probe:
`bazel build //datacube:dist && ls bazel-bin/datacube/dist/legend-engine.js` (predicted: no such file).

### HOSTS-7: a second, undeclared way to legend-engine's routes

**Real, by reading.**
- **Java.** `EdgeServer.java:92-94` has a `public static void main` that calls `serve(args, UPSTREAM)`. It ships in
  `//core:server_deploy.jar` through `:edge_server` (`core/BUILD.bazel:565-580`). Only the planted guard targets name it
  as a main (`:2845-2861`).
- **Python.** `Engine(frames, edge=EngineRoutes())` (`engine.py:540-549`, `_edge_seam.py:48-54`) skips the door's
  `engine_library()` pre-check (`edge/__init__.py:29-34`). Without the engine library, each call then answers
  `500 EngineMissing: legend-engine needs the legend-lite[engine] extra ...`, which is still refused by name, through
  `_api`'s two `except` blocks (`_pure_v1.py:185-197`).

**It does not break K2.** The default entries load nothing of the edge:
- the jar's `main_class` is `LegendHttpServer`;
- `configured(null)` never calls `ServiceLoader`;
- no product Python module imports `legend_lite.edge`.

Each bypass needs the caller to name an edge symbol (`java -cp … com.legend.edge.EdgeServer`, or `from legend_lite.edge
import EngineRoutes`), which is an explicit opt-in under another spelling. Lite API v1 §6, ruled 6, anticipates this: "The
split later is one more main class". At main, legend-engine's routes were always on. **NOTE.**

Optional cleanup: delete `EdgeServer.main` and give the planted targets a planted main (~4 lines plus 1 class); make
`EngineRoutes()` take the door's private token (~5 lines).

### HOSTS-3, HOSTS-4, HOSTS-5: Python with the edge off

**Does the shipped product wheel, with the edge off, load any edge code?** No.
- `wheel_package` ships `legend_lite/edge/*.py` (`python/BUILD.bazel:896-905`).
- No product module imports it: a grep finds only `legend_lite/edge/` importing `.edge`, and `__init__.__getattr__` never
  reaches it. The layer rule `tests/test_run_closure.py:465-471` enforces this.
- `import legend_lite_engine` (`_loader.py:220`) runs only inside `engine_loaded()`, which only `edge/_engine.py` calls.

**Does it expose an upstream spelling?** One route.
- **HOSTS-3: real.** `engine.py:150` and `:613-615` answer `GET /api/server/v1/currentUser` before any lite route, edge on
  or off. Python's `EngineRoutes.serves` does not claim it (`_pure_v1.py:159-162`).
  - No product client asks it: query-store's `client.ts:92` says it is "the edge's, never asked here".
  - Product tests pin it (`test_engine.py:1211-1242`, `test_wheel.py:133-134`).
  - It was on main, where every legend-engine route was the product's, so the PR did not introduce it. The PR moved the
    rest and left this one.
  - With the edge off, Java answers 404 and Python 200. This contradicts ruling 6 ("a boot test with the edge module off
    proves lite's routes answer and legend-engine's are absent") for Python.
  - **FIX-AFTER:** move the route into `EngineRoutes` and point the cookie tests at lite's user route, about 20 lines.
- **HOSTS-4: real, low.** `_loader.py:62-75` (`ENGINE_ARITY`, `ENGINE_NOT_PLANNING`) and `:201-237` (`_find_engine`,
  `engine_loaded`) are lite's own FFI symbol names and the opt-in package's location, held in a product module.
  - That is coupling, not an upstream spelling, and nothing of it runs with the edge off.
  - `EdgeOff.test_the_product_library_exports_no_edge_entry` (`test_engine.py:777-785`) checks against that hand-kept
    list, although its docstring says "never a list kept by hand".
  - **NOTE / FIX-AFTER:** move the code into `edge/_engine.py`, and derive the test's list from the engine library's
    exports.
- **HOSTS-5: ruled.**
  - K2, as the user ruled it (plan of record log, 2026-11-18 06:05): "one artifact per surface (Lite API v1 §6, ruling 6)".
  - Lite API v1 §6 (`docs/records/lite-api-v1-2026-11-17.md:103-110`): "**One server for now (ruled 6...)**: one artifact
    serves lite's routes and the edge's legend-engine routes".
  - Java's jar carries the edge in the same way. The engine wheel carries the native binary alone, by the orchestrator's
    packaging decision (`python/BUILD.bazel:980-983`).
  - **REFUTED as a K2 break.** NOTE only.

**Is any of this a broken K2 rule introduced by this PR?** No.
- HOSTS-3 is a K2 incompleteness that predates the PR (the PR narrowed it from every route to one).
- HOSTS-4 is placement.
- HOSTS-5 is ruled.

### HOSTS-2: TS `transacted` commits outside its `try`

**Facts.**
- `engine-client/src/plan-runner.ts:412-429`: the commit loop at `:427` sits after the `try`/`catch` that alone rolls back.
- The code is identical at main (`main:engine-client/src/plan-runner.ts:240-258`). It is not new.

**Shipped paths that reach it, all on DuckDB-WASM in the tab, or on the warehouse's DuckDB through its session:**
- `establish` with loads: a project's Data, or a connection's test data (`:670-684`);
- `inLoadsTransaction`: a person's file put into a declared table, `model-data.ts putFile` (`:436-445`);
- `runPlan` and `runPlanRows` of a plan with `update`/`effect` steps (`:840`; `runPlanRows` is new and is used by the
  product's `browser-engine.ts:125` for query/run, and only a writing plan reaches the transaction);
- a keyed snapshot read (`:974`);
- DataCube's table writer, which calls `transacted` directly.

**What state does a failed COMMIT leave? Measured.** The script is under the scratchpad at
`audit/verify/vhosts_scripts/commit_fail.py`, and its output at `audit/verify/vhosts_scripts/commit_fail.out`
(duckdb-python 1.4.1):
1. Two cursors each insert primary key 1, and the first commits.
2. The second's `COMMIT` fails with `TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation`.
3. A `ROLLBACK` after it fails with `cannot rollback - no transaction is active`.
4. The next statement on that cursor runs: `[(1,)]`.

So on DuckDB a failed COMMIT ends the transaction itself. TS's shape, which throws the commit's error and sends no
rollback, leaves the session usable. The claim that "the session stays in an aborted transaction and the next plan on it
fails" is refuted for DuckDB. Python's in-`try` shape would instead add a second, spurious failure to its message ("--
and rolling its transaction back failed: cannot rollback - no transaction is active"). That is cosmetic too.

Not run on DuckDB-WASM's own build. The engine is the same C++, and the PR pins DuckDB-WASM 1.33.1-dev57.

**Grade: REFUTED as a wrong answer.** NOTE: old code, harmless on the hosts' database. If symmetry is wanted, 3 lines in
either host. A test would need a session whose COMMIT statement throws.
