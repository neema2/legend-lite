# PYTHON: verdict MIXED

## Summary
Python plans by calling the Java planner in process, through ctypes into the GraalVM native image (`_loader.Loaded.call`).
That is one isolate per process with per-thread attachment, and a text protocol (`OK\n...` / `ERR\nkind\nmsg`). Python
runs the plans itself on duckdb-python. The core of the split is sound and mostly idiomatic Python, not Java
transliterated. It rests on a context-variable run frame, a narrow `Database` facade around the one `send`, a `str`
subclass brand checked with `type(x) is`, and a real Bazel library split whose closure test imports only `:run`. Its
behavioural tests are good. The quality debt sits around that core:
(1) 1,230 lines of bespoke AST guards, with count pins, cover about 1,730 lines of run and seam code, and two Java tests
regex-read the Python source as well, so routine edits mean editing guards;
(2) the plan format is ported by hand from TypeScript as untyped dicts, with the version copied by hand and knowledge of
the plan's fields in four places;
(3) re-export shims from the half-finished move, stale cross-references, and comments made mostly of process history
(469 dates in the package and its tests);
(4) duplicated small concepts (two "model refused" exceptions, two copies of one set, a doubled version check) and
string-typed error kinds.
A new engineer could read the runtime code within a week, but would hit the guard tax on the first change.

## Strengths
- **The run frame is idiomatic and correct.** `_runframe.py:324-354` uses a `contextvars.ContextVar`, so each thread and
  each asyncio task has its own frame. `runs`/`in_run` reset by token in `finally`. The pool carries the caller's
  context for every submit (`_pool.py:230-236`). It is the same shape as TypeScript's `run-frame.ts` (`AsyncLocalStorage`):
  the same concept with the same names across hosts.
- **The capability is narrow by construction.** `_planned.Database` (`_planned.py:341-414`, `__slots__`) exposes only
  `send`, `cursor`, the transaction bounds, `register`, `unregister` and `import_stream`. DuckDB's `sql`, `query`,
  `read_csv` and `from_df` cannot be reached through it (tested at `tests/test_planned.py:83-98`). `require` checks
  `type(statement) is PlannedSql`, and a test proves that a subclass spoofing `__class__` is refused
  (`_planned.py:290-300`, `tests/test_planned.py:144-160`).
- **The build-level split is real.** `python/BUILD.bazel:21-82` defines `:seam`, `:loader`, `:run` and `:compiler`, and
  `:run` has no planner in its deps. `run_closure_test` imports every run module with `:run` alone
  (`tests/test_run_closure.py:545-550`), so a module-level planner import fails the build. This is the right mechanism,
  and proportionate.
- **The behavioural witness tests are good.** `_Witness` records what DuckDB actually received
  (`tests/test_planned.py:189-201`). `WitnessTest` (`:227`) asserts that a load and a stream hand the database exactly
  their plan's statements, in order. `ASnapshotsCopyTest` (`:317`) proves rollback and that the earlier table is
  preserved. `FollowupTest` covers rollback-failure notes and redaction at the fetch. The test_headless additions cover
  how a read ends in every order of failures. These test behaviour, not structure.
- **Fixtures are shared across languages.** `run-load-conformance.json` and `keyed-expansion.json` are run by Python
  (`tests/test_engine.py:328-353`, `tests/test_planned.py:383-397`) and by TypeScript. This partly offsets the
  hand-ported runner.
- **Secrets are handled carefully.** `_reads.redacted` (`_reads.py:114-127`) copies the exception, clears its dict and
  raises it outside the `except`, so the database's original error, which still holds the URL, is not chained through
  `__context__`. A failed rollback is reported with its cause or with `add_note` rather than swallowed
  (`_runs.py:280-288`, `_reads.py:321-339`).
- **Idiomatic structure in most places.** Frozen dataclasses (`Landed`, `HostLoad`, `Plan`, `Column`), context managers
  (`HeldModel.__enter__/__exit__`, `ProjectDatabases.using`, `Frames.reading`), and a GC finalizer that only queues work
  for later (`datacube.py:514-540`). Duplication inside Python is low: jscpd finds 0.25% duplicated lines.
- **The public top-level API is unchanged by the split.** `legend_lite/__init__.py:19-43` exports the same compiler
  names, with lazy doors.

## Findings

### PYTHON-1 [S1] The guard layer is out of proportion to the code it guards, and its count pins tax routine edits
- Where: `python/tests/test_run_closure.py:39-541` (about 25 rules) and `:578-708` (91 planted violators);
  `python/tests/test_planned_guard.py:36-196` (pin tables `IMPORTS`, `SENDS`, `BRIDGE`, `MINT_REFERENCES`
  (`{'compiler.py': 9, 'engine.py': 2, '_runs.py': 14}`), `REFERENCES`, `ATTACHES`, `SUBMITS`, `NETWORK`, `RUNNERS`,
  `AUTHORED`), and the counting at `:220-254`; `core/src/test/java/com/legend/PlannerSurfaceTest.java:1786-1831` (Java
  regex over `_library.py`'s `frozenset` literal and over `_headless.py`'s `def _calls(` body); `python/BUILD.bazel:452-455`
  (`planned_guard_test` is `medium`: 28-34 s on CI's Windows).
- What: Two hand-written AST analyzers (1,230 lines) police about 1,730 lines of run and seam code.
  - They overlap each other. Each has its own import reader (`imported`/`imports` at `test_run_closure.py:162-245`,
    `census` at `test_planned_guard.py:205-312`), and both largely re-prove what the Bazel split already proves.
  - Many rules target adversarial forms (`getattr(builtins, "__import__")`, a `Protocol` whose `__call__` returns
    `PlanText`, `type(p)(text)`, `str.__new__(PlannedSql, ...)`), although the guard's own docstring says the rules
    "catch drift, not a determined adversary" (`test_run_closure.py:423-425`).
  - Counts pin incidental facts. `SUBMITS` counts every string constant `'submit'` in any module, and `AUTHORED` counts
    any name or constant `authored` (`test_planned_guard.py:238-254`). Adding one `from_plan` call, one `.register` on a
    new receiver or one network import fails the build until a count or allowlist is edited, with a dated comment.
  - The guards also shape product code: `_library.py:14-25` re-declares `_loader.NOT_PLANNING` and checks equality at
    import, because the Java regex reads `_library.py` (finding PYTHON-6). The witness also forces accidental complexity
    into the runner: `_runs.py:249-262` wraps DuckDB's foreign-thread pulls in `context.run(next, ...)`, and
    `warehouse.py:143-147` frames a sign-in as a "run".
- Why it matters: Every change that touches a send, a mint, an attach or an import pays a guard edit. Moving a function
  between modules means updating up to six pin tables. The 91 meta-cases are a test suite for the test. A new engineer's
  first PR goes red for reasons unrelated to behaviour.
- Expert alternative:
  - Make the split visible in the package, as `legend_lite/_seam/`, `legend_lite/_run/` and `legend_lite/_plan/`.
  - Keep the Bazel `:run` closure test, which is the real proof, and the runtime witness.
  - Replace the AST rules with a 15-20 line `import-linter` contract set (`layers: _plan | _run -> _seam`,
    `forbidden: _run -> _plan, legend_lite.compiler`), which also covers imports inside functions.
  - Keep one small lint for "PlannedSql and PlanText are constructed only in `_seam`".
  - Delete the count pins and the Java regex readers of Python source.
- Confidence: verified

### PYTHON-2 [S1] The plan format is re-implemented by hand per host, as untyped dicts, with a hand-copied version
- Where:
  - `python/legend_lite/_runs.py:26` (`PLAN_VERSION = 51`) and `:34-293` (`run_load`, `run_write`, `run_host_load`,
    `_stream`), against `engine-client/src/plan-runner.ts:131` and `:386-460`. They match line for line, error strings
    included.
  - `_plans.py:221-261` against `engine-client/src/plan-params.ts`.
  - `_planned.py:114-121` and `:219-246`: `Answered._STATEMENT_FIELDS`, a heuristic walker that decides which JSON
    fields are statements (`'count'`, `'read'`, `'session'`, `'after'`...), with exclusions per step type in
    `_NOT_HERE`.
  - `_planned.py:249-287` (`_keyed_text`) against `expandKeyed` and Java's `PlanRunner.expand`.
  - `_reads.py:86-103` (`held_texts`) against `connection-settings.ts`.
  - A fourth walker in the tests: `tests/test_planned.py:204-224` (`_statements_of`).
- What: Python knows the plan format through 82 string-keyed accesses in `_runs.py` alone (`step['sql']['target']
  ['transaction']['begin']`). Missing fields surface as a raw `KeyError` or `TypeError`, not the named `LoadRefused` used
  elsewhere (for example `root['steps']` at `:62` and `step['sql']['target']` at `:137-140`). The version must equal
  exactly and is edited by hand. `PlanVersionsAgreeTest` reads only the TypeScript constant, so Python's copy is held
  only by a fixture's behaviour.
- Why it matters: Every PlanJson change has to be hand-ported to TypeScript, to Python's runner, to `Answered`'s field
  heuristics and to the test walker, and the version has to be bumped in two hand-written constants. The shared
  fixtures catch some drift but not new fields.
- Expert alternative: The repository already has the machinery for this.
  - `_type_facts.py` and `lite-facts.ts` are generated from Java (`python/BUILD.bazel:130-136`). Generate PlanJson's
    step kinds the same way, as frozen dataclasses or `TypedDict`s, together with `PLAN_VERSION` and a `statements(node)`
    visitor. Parse once into typed steps and dispatch with `match step:` (Python 3.12 is required, `BUILD.bazel:891`).
  - Or follow the warehouse precedent (`warehouse.py:4-7`: the SQL API protocol lives once in Java behind the service
    key, and Python only performs the I/O). A planning-free native exec entry would walk a plan and yield
    `(statement, params)` steps. This is likely compatible with G4, because `//core:exec` reaches no planner; that part
    is not confirmed (suspected).
  - Either way, have the planner's answer envelope carry its own list of statements, so `Answered.record` reads one field
    instead of guessing.
- Confidence: verified (the duplication); suspected (the feasibility of the native exec entry)

### PYTHON-3 [S2] Two representations of "a plan", two `read_plan`s, a parse-dump-parse round trip, and dicts used as records
- Where: `compiler.py:105` `def read_plan(answer: str) -> Plan` against `_planned.py:58`
  `def read_plan(plan: Any, where: str) -> dict`; `compiler.py:262-266` (`project_data` parses the answer, re-serializes
  `answered['load']` with `_json.dumps` to brand it `PlanText`, and `_runs.run_load` parses it again at `_runs.py:38`);
  `compiler.project_data -> dict[str, Any]`, consumed as `statements['open']`, `['use']`, `['close']` and `['load']` in
  `_runs.open_project` (`_runs.py:312-334`) and `projects.py:83-97`.
- What: A query plan is a typed `Plan` dataclass. A load or write plan is a branded JSON string that each runner
  re-parses and walks as a dict. Two module-level functions share the name `read_plan`, with different arguments and
  results.
- Why it matters: Readers must learn two plan types and two reading paths. The brand guarantees nothing that the typed
  object would not, since `plan_from_answer` is public and the tests call it on hand-written plans
  (`tests/test_planned.py:113-125`). The dict records hide which keys are required.
- Expert alternative: One reader module (`_seam/plans.py`) turns planner answers into `Plan`, `LoadPlan(identity,
  cadence, steps: tuple[LoadStep, ...], content)` and `WritePlan(target, effects)`, plus
  `ProjectData(name, open, use, close, load: LoadPlan | None)`. The runners take those objects. `PlanText` and
  `_planned.read_plan` go away.
- Confidence: verified

### PYTHON-4 [S2] The half-finished move: re-export shims kept, and consumed by internal code
- Where: `compiler.py:17-18`, `frames.py:48-51`, `projects.py:22-26` (`# noqa: F401`, with comments such as "named
  here as they always were"); `engine.py:69` imports `run_host_load` and `run_write` from `projects`, not `_runs`;
  `engine.py:65` imports `Reading`, `UntypedColumn` and `redact_held` from `frames`, not `_reads`; `frames.py:50`
  re-exports `TYPE_KEY`, `held_texts` and `redacted`, which nothing reads; `tests/test_planned.py` uses
  `projects.run_load`, `projects.PLAN_VERSION` and the like at 28 sites.
- What: The runners and reads moved to `_runs` and `_reads`, but the old paths still work and are still the ones in use.
- Why it matters: The same function has two names, so grep and refactoring miss uses. A "door" module (engine.py) imports
  run-side functions through another door (projects.py), which hides the dependency the split exists to make visible.
- Expert alternative: Import from the defining modules everywhere and delete the shims. If `frames`, `projects` or
  `compiler` names are public for users, keep a module `__getattr__` that warns with `DeprecationWarning` for one
  release.
- Confidence: verified

### PYTHON-5 [S2] The public API regressed: `Frames.connection` became a wrapper with no capabilities, so tests reach `_con`
- Where: `frames.py:296-302` (`self.connection = connect() if connection is None else Database(connection)`); 37 test
  sites reach `frames.connection._con` (test_headless 20, test_engine 7, test_frames 6, test_page 2, ...), for example
  `tests/test_engine.py:442` and `tests/test_frames.py:213`.
- What: A user who lets `Frames()` open the database now has no public way to run their own SQL beside their frames. The
  public attribute is `_planned.Database`, a private type with no `execute`. The repository's own tests have to bypass it
  for fixture setup.
- Why it matters: This confines a public object to suit an internal rule. Tests that rely on a private field break on any
  refactor of the facade.
- Expert alternative: Keep the facade private (`self._db = Database(...)`). Expose the person's connection as it is
  (`Frames.duckdb`: theirs when given, a documented escape hatch when Frames opened it). Tests create their own
  connection and pass it to `Frames(con)`.
- Confidence: verified

### PYTHON-6 [S2] Small concepts duplicated: two "model refused" exceptions, a set written twice, a version check pasted twice
- Where:
  - `frames.py:71-84` (`ModelRefused`, `_MODEL_REFUSALS = frozenset({'PARSER', 'COMPILATION'})`) and `projects.py:28-36`
    (`NotAModel`, `_NOT_A_MODEL`, the same set).
  - `engine.py:1547`, `:1664`, `:1749`, `:1775` and `:1789`: five near-identical
    `except ModelRefused/NotAModel: return _planned({'plan': self.planner.refused_by(...)})` blocks.
  - `_library.py:14-25` and `_loader.py:57-65`: `NOT_PLANNING` written twice, with an `ImportError` raised at import time
    if the two differ ("one set, written twice for PlannerSurfaceTest").
  - `_runs.py:38-43` and `:125-129`: the format and version check, copied.
- Why it matters: Each pair has to be changed together, and nothing but the reader's memory keeps them together, except
  where an import-time assertion does.
- Expert alternative: One `ModelRefused(LegendError)` in `compiler.py`, raised by a `compiler.hold(model)` helper that
  maps the kind, plus one engine helper `_refused_model_plan(e)`. One `NOT_PLANNING`, imported by `_library`, with the
  Java test pointed at the loader. One `_require_plan_version(plan) -> dict`.
- Confidence: verified

### PYTHON-7 [S2] Error design: string-typed kinds, an unnamed refusal, and no hierarchy
- Where: `compiler.py:29-32` (`LegendError.kind: str` holds `'PARSER'`, `'COMPILATION'` or `'500'`, compared against
  string sets in `frames.py:79` and `projects.py:31`); `_runframe.py:390` (`planning()` raises a bare `RuntimeError`,
  while the symmetric refusal has its own `Unframed` class, `_planned.py:424`); `_runs.py:126-159` (a refused plan of
  writes raises `LoadRefused`); the missing-field `KeyError`s in the runners (PYTHON-2).
- What: Callers and tests identify failures by message regex. `tests/test_planned.py` has 25 `assertRaisesRegex` or
  `assertRegex` calls, several of them on `RuntimeError` text such as `"the planner was asked 'plan' inside a run"`.
- Why it matters: Callers cannot catch "planned inside a run" without catching every `RuntimeError`. A change of wording
  breaks tests in two languages.
- Expert alternative: `class RefusalKind(StrEnum): PARSER, COMPILATION, MODEL, INTERNAL`; `class
  PlannedInRun(RuntimeError)` beside `Unframed`; `class PlanRefused(ValueError)` with `LoadRefused` and `WriteRefused`
  under it; one `LegendLiteError` base for the package.
- Confidence: verified

### PYTHON-8 [S2] Module cohesion: `_planned.py` holds four concerns, and the line between `_reads` and `_runs` is arbitrary
- Where:
  - `_planned.py` holds four concerns: the brand and its mints (`:31-90`); the V8 bridge's allowlist with keyed
    matching (`Answered`, `:93-287`), which only `_headless` uses yet sits in the seam that every run module imports; the
    DuckDB facade and the one send (`:306-447`); and the run-frame check (`:424-447`).
  - `_runs.ask` is a read (`_runs.py:304-308`), and `_reads.run_bridged_statement` runs DataCube's writes
    (`_reads.py:353-358`).
  - `_plans.host_rows`/`_cell` (`_plans.py:264-305`) is a JSON row serializer for the execute route, kept in "the
    plan's records".
- Why it matters: The module names do not predict where code lives, and the 450-line seam changes for bridge policy
  reasons that have nothing to do with the run side.
- Expert alternative: `_seam/planned.py` for the brand and mints; `_run/session.py` for `Database`, `Result` and `send`;
  `_run/loads.py`, `_run/queries.py` and `_run/redaction.py`; `_bridge.py` for `Answered` and `across_the_bridge`, next to
  `_headless`; `host_rows` beside the execute route.
- Confidence: verified

### PYTHON-9 [S2] The bridge allowlist's membership test has side effects and matches keyed bodies by brute force
- Where: `_planned.py:187-197` (`__contains__` moves entries in the LRU, so `x in answered` mutates state);
  `_planned.py:249-263` (`_written_for_keys` rebuilds the whole text for n = 0, 1, 2, ... keys until it is longer than the
  candidate, computing `_keyed_text(..., n - 1)` again on every iteration); `_planned.py:193` (on a miss in `_texts`,
  every keyed body, up to 256, is tried this way).
- What: The cost per check is roughly O(len² / region length) for each keyed body. The `in` operator hides an LRU touch.
- Why it matters: The cost is hidden on the hosted DataCube's hot path (a refused or keyed statement), and the surprising
  `in` semantics hurt readability.
- Expert alternative: An explicit `admits(text) -> bool`. For a keyed body, solve n from the lengths in O(1): each region
  contributes `lead + n * copy + (n - 1) * separator`, so n follows from the text's length. Then compare once, after a
  prefix check on the text before the first region.
- Confidence: verified (the code); suspected (that the cost is material in practice)

### PYTHON-10 [S3] Comments and docstrings are mostly process history, and several are stale (pattern)
- Count: 469 ISO dates in comments and docstrings under `python/legend_lite` and `python/tests`.
  `test_planned_guard.py` has 53 dates and 56 audit or ruling words in 493 lines; `_planned.py` has 13 and 23.
- Examples of history: `_planned.py:41-45` (the `PlanText` docstring: "the re-audit's M1, 2026-11-12 ... projects.py's
  until C5-P"); `_reads.py:149-164` (a measurement diary of DuckDB exception classes); `_runframe.py:357-366` (a
  narrative of exemptions that no longer exist).
- Stale or wrong:
  - `compiler.py:231`, `:241`, `:250` and `:259` name `projects.run_host_load`, `projects.run_write` and
    `projects.run_load`, which now live in `_runs`.
  - `_pool.py:8-12` says the direct `_workers().submit` sites "hand the pool's thread no context", which contradicts
    `_pool.py:230-236`: `submit` now copies the context for every caller.
  - `_planned.py:67-68` and `tests/test_planned.py:3-4` name `tools/guards/PlannedSqlTest.java` as the Python guard. It
    is `tests/test_planned_guard.py` (`PlannedSqlTest.java:30` says so).
- Identifiers named after process steps: `TheReAuditsSendTest` (`tests/test_planned.py:144`), `C5SplitTest` (`:684`),
  `FollowupTest` (`:885`), `test_the_census_sees_the_final_audits_forms` and `..._re_audits_forms`
  (`test_planned_guard.py:343`, `:358`).
- Expert alternative: Docstrings say what the code does now and why. History goes in commit messages and `docs/`. Name
  test classes for behaviour (`WarehouseFramingTest`, `ReadRollbackTest`).
- Confidence: verified

### PYTHON-11 [S3] Typing: the package ships `py.typed`, no type checker runs, and the hints are loose or wrong (pattern)
- Examples: `_planned.py:435` `send(target, statement: Any, ...)` (should be `PlannedSql`; keep the runtime check);
  `_runs.py:119`, `:163` and `:209` (`connection: Any`, while sibling functions take `Database`); `frames.py:278`
  (`_File.settings: tuple[tuple[str, ...]]`, passed to `_reads.begin_read`, typed
  `Iterable[tuple[PlannedSql, Sequence[Any]]]` at `_reads.py:304`); `frames.py:36` (`from typing import Callable,
  Iterator, Mapping`, deprecated aliases, while every other module uses `collections.abc`). No mypy, pyright or ruff
  appears in `python/BUILD.bazel` or CI.
- Expert alternative: Run pyright or mypy on `legend_lite` in the gate, then tighten `Any` to the real types.
- Confidence: verified

### PYTHON-12 [S3] Residue and dead code (pattern)
- `_planned.py:325-338`: four `Result` forwarders (`fetchmany`, `fetch_arrow_table`, `fetch_record_batch`, `df`) that
  nothing in the repository calls.
- `_runframe.py:357-378`: `EXEMPTIONS = frozenset()` and `exempt()`, a mechanism kept with no entries, used only by a test.
- `Answered.statements()` (`_planned.py:202`) is used only by tests.
- `_reads.py:140`: `with in_run(current_run() or 'a statement redacted')` is redundant, because every product caller is
  already `@runs`.
- `_runs.py:61`: `content = root.get('content') or {}`, the kind of defaulting the same series removed from
  `statement_params` as a G7 breach (`_plans.py:237-240`).
- `_plans.py:273-277`: the stdlib imports sit inside the per-cell function `_cell`.
- `_library.py:28-60`: `Library` is a stateless class used as a singleton (`_key = Library()`, `library()`), while its
  twin key `_services.py` is plain module functions.
- Expert alternative: Delete the unused members, inline the redundant frame, and make `_library` module functions like
  `_services`.
- Confidence: verified

### PYTHON-13 [S3] A few tests pin structure or absence rather than behaviour (pattern)
- `tests/test_planned.py:100-104` (`test_the_send_only_redaction_is_gone`: `hasattr` checks that a deleted function's
  name is absent); `:561-574` (`test_no_crossing_is_known_and_none_is_exempt` asserts an empty set and that two deleted
  names are refused); `tests/test_engine.py` (#111) repeats the inline "broken live frame" helper three times
  (`UnexpectedFailures.broken`, `test_an_executes_unexpected_failure...`, `test_a_failure_no_route_answers...`);
  `tests/test_planned.py` has 56 accesses to private members.
- Expert alternative: Delete the absence tests (git history and the closure rule cover them), share one fixture helper,
  and drive the tests through public seams.
- Confidence: verified

## Cross-unit observations
- Java tests regex-read Python source. `PlannerSurfaceTest.java:1786-1831` parses `_library.py`'s `frozenset` literal and
  `_headless.py`'s `def _calls(` body. That couples a Java test to Python's formatting, and it forced the duplicated set
  in PYTHON-6. `PlanVersionsAgreeTest.java:33` regex-reads `plan-runner.ts` but not Python's `PLAN_VERSION`.
- `engine-client/src/plan-runner.ts:100-130` keeps the plan version's changelog as a 30-line comment.
- What counts as "a send" is inconsistent. The hosted DataCube's HTTP to a SQL API (`_headless.answer_fetch`,
  `_headless.py:110-120`) goes out with no run frame, while `warehouse._urlopen` refuses outside one
  (`warehouse.py:116-122`). The bridge check does cover its `sql`.
- `engine.py` (mostly older than the series) has one `Engine` class with 71 methods over about 1,600 lines. Its
  `answer` method is an if-chain of routes with cyclomatic complexity 50. It also has two unrelated methods named
  `_answered` (`engine.py:617`, `:2248`).
- The TypeScript and Python runners share names and shapes well (`from_plan`/`fromPlan`, `PlanText`,
  `run_load`/`runLoad`, `in_run`/`inRun`). Consistency is good; the problem is that the logic is duplicated.

## Measurements
- Run and seam code against its guards: `wc -l _reads.py _runs.py _planned.py _runframe.py _plans.py _services.py
  warehouse.py` gives 1,731 lines; `wc -l tests/test_run_closure.py tests/test_planned_guard.py` gives 1,230.
- Planted guard cases in `test_run_closure.test_the_rule_sees_every_break`: 91 (counted with an `ast` script).
- Copy-paste: `jscpd --min-lines 6 --min-tokens 60 --format python python/legend_lite python/tests` reports 3 clones,
  0.25% of lines.
- `python3 -m lizard -l python python/legend_lite -C 12` flags:
  - `_runs.run_load` CCN 16; `run_write` CCN 21; `run_host_load` CCN 14; `_stream` CCN 19, 85 lines.
  - `_planned.Answered._plan` CCN 20.
  - `engine.Engine.answer` CCN 50.
- String-keyed dict accesses (`grep -oE "\[['\"][a-zA-Z_]+['\"]\]|\.get\(['\"]..."`): `_runs.py` 82, `_planned.py` 27,
  `_plans.py` 12, `compiler.py` 13.
- Dates in comments (`grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" -r legend_lite tests | wc -l`): 469.
- Private-field access in tests (`grep -c "connection._con" python/tests/*.py`): 37.
- `planned_guard_test` takes 28-34 s on CI's Windows (its own BUILD comment, `python/BUILD.bazel:453-454`).
- Python changes in the series: 114 commits touch `python/` (`git log --oneline 770acc55^1..4dfbb5b7 -- python | wc -l`).
