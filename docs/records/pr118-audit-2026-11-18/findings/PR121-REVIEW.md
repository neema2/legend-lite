# PR #121 review: claude/followup-q8-q9 -> main (head f059e5ec5, on main b75d6c73a)

> Copied 2026-11-19 from the orchestrator's scratchpad so the follow-up workers can read the exact sites. Written against
> PR #121 at f059e5ec5; B1 is resolved by S371 (6baad4af1), B2 by 64fef331a/8102ddd3e, F5 by 91221d954.

Reviewer: independent; I wrote none of it. I read the diff, the code around each change, Java's `LiteV1Api.dataRefusal`
and `RaisedErrors`, the SQLite dialect, `ColumnFacts`, the Python run side and the CI check runs. Nothing was built or run
(no Bazel). Line numbers are at the PR head.

## Verdict: FIX FIRST

There are two blockers:

1. **Q8 lands a divergence from upstream that the user has not ruled on and that is not in the register.** The PR's
   own record calls it R-a, "not yet a row".
2. **Q8 refuses a whole value written as text ("5.00").** On SQLite this means an Integer over DECIMAL(p,s>0) holding a
   whole value is refused, while DuckDB and H2 answer it.

Q9 and R11(b) are sound as far as they go. The findings on them below are follow-ups.

---

## BLOCKERS

### B1. Q8 is a divergence from upstream that is neither ruled nor ledgered
- **Where:** `core/src/main/java/com/legend/exec/JdbcDecode.java:283-288`, `core/src/main/java/com/legend/result/RowDecode.java:132-135`,
  and `docs/records/q8-upstream-answers-2026-11-19.md` (section "R-a, for the user's ruling (not yet a row of
  docs/SEMANTICS_REGISTER.md)"). `docs/SEMANTICS_REGISTER.md` is not touched and has no row on this.
- **What:**
  - The engine runner's answers (legend-engine 4.145.0) are:
    - the relation `project` answers `5.50` under a column typed Integer;
    - the object read (`graphFetch`) refuses every DECIMAL column under an Integer property, a whole value included.
  - After Q8, lite:
    - refuses the fraction on every route, including the edge's pure/v1 relation, the compatibility surface;
    - answers a whole value on every route, the object read included.
  - Against main, Q8 adds one new divergence: the whole value on the object road. Main's batch refused every such cell,
    which matched upstream there by accident.
  - It also keeps one old divergence, still unledgered: the refused fraction on the relation road.
- **Why it matters:** this is the upstream-fidelity tenet in AGENTS.md. A divergence must be "POSTED to the user before
  it lands ... and LEDGERED as a row of docs/SEMANTICS_REGISTER.md ... A divergence that is not a row is a defect; a row
  the user did not rule is a defect too." The orchestrator's ruling covered the mechanism ("the carrier is the decode's
  to choose"); it did not cover the fidelity question. The record says plainly that R-a still goes to the user.
- **Fix:** take R-a to the user and land the row in this PR, quoting both upstream answers. The row should name the
  edge's pure/v1 relation explicitly, since that is where upstream's `5.50` is the compatibility contract. Otherwise,
  split Q8 off and merge Q9 and R11(b) alone.

### B2. A whole decimal spelled with a zero fraction is refused, so the answer depends on the database
- **Where:**
  - `JdbcDecode.java:412-418` and `RowDecode.java:297-303` (`wholeText`: `new BigInteger(text)`). The Integer-over-DECIMAL
    arm reaches them through `integral()` / `integer()`.
  - The cause: `core/src/main/java/com/legend/sql/dialect/SQLite.java:444`. SQLite delivers a root DECIMAL(p,s>0) cell as
    `printf('%.sf', x)` text, and its DDL makes every scaled column REAL (`SQLite.java:3364`).
- **What:**
  - Take an Integer property over DECIMAL(10,2) holding 5:
    - **SQLite:** the driver hands the String `"5.00"`, `BigInteger("5.00")` throws, and the column is refused ("is not
      of that kind").
    - **DuckDB and H2:** the driver hands `BigDecimal 5.00`, the BigDecimal arm strips the zeros, and the answer is 5.
  - So the same query answers 5 on two in-process databases and is refused on the third.
  - The JSON twin has the same gap: `Json.Num 5.00` is accepted, `Json.Str "5.00"` is refused. The same goes for
    `"-0.00"` and `"1E+3"`, which the BigDecimal and JSON-token arms accept.
- **Why it matters:**
  - It breaks the rule this PR introduces. `JdbcDecodeDuckDbTest.java:110` pins `-5.00 -> -5` with the comment "the value
    decides, never its scale", and the RowDecodeTest javadoc says "a whole value at any scale included".
  - The tests miss it. The in-process product, judge and edge models put only `5` in a DECIMAL(10,0) column and only the
    fraction `5.50` in a DECIMAL(10,2) one (`IntegerOverDecimalTest.java:41`, `AssertBarTest`'s `INTEGER_ON`). No
    in-process test has a whole value at scale > 0.
- **Fix:**
  - Narrowest change: in the Integer-over-DECIMAL arm, read the cell as a decimal first (`decimal(v, c)`, with a
    `NumberFormatException` refused by name). Then return `toBigIntegerExact()` when `stripTrailingZeros().scale() <= 0`,
    and refuse anything else by name. This keeps the INTEGRAL and BY_PURE_TYPE text arms as they are.
  - Alternatively, make `wholeText` parse a `BigDecimal` under the same rule. That also widens those two arms; decide
    that deliberately.
  - Tests: add a row `F = 5.00` to IntegerOverDecimalTest and AssertBarTest on every in-process backend, and a quoted
    `"5.00"` case to RowDecodeTest.

---

## FOLLOW-UPS

### F1. N-b reads every plan by position, for every caller
- **Where:** `python/legend_lite/_reads.py:51-83` (`typed_by_plan`). Callers: the frames/DataCube reads,
  `run_on_plane` (the warehouse), `run_plan` (projects) and query/run.
- **What:** the counts are checked; the names never are.
  - If the statement answers a different number of columns, it is refused, which is good.
  - If it answers the same count under other names, or in another order, the columns are silently renamed to the plan's
    names and typed by position. A swapped Integer/String pair would come out mislabelled with no error.
- **Why it matters:**
  - In its favour: it matches Java's exec (`JdbcStatements.rows` checks the count, then `JdbcDecode.Reader` reads by
    position), and `_reads` is Python's exec.
  - But other places read by name:
    - Its twin, engine-client's `typedByPlan` (`engine-client/src/query-engine.ts:94`), still reads by name, so Python and
      the tab now differ.
    - The planner pairs by name with exactly one named exception: `ColumnFacts.mapBinderValue` (`ColumnFacts.java:96`),
      one `value` column over one `u_map__*` output. That is precisely N-b's case.
    - The JSON decode refuses a positional read outright (`RowDecode.cells`; "E1 item 6, G7").
  - The docstring still says "A result column the plan does not type is refused ... (engine-client's typedByPlan)" and
    "a statement answering other columns than the plan's is refused"; neither is true any more.
- **Fix:** pair by name, plus ColumnFacts' one named pairing (a sole `value` plan column over a sole field starting
  `u_map__`). Or keep the positional read but refuse when a field's name is a different plan column's name. Correct the
  docstring, and either port the rule to `typedByPlan` or record the difference.

### F2. Q9 still answers some query/run load failures outside lite's shape, or in another kind than Java's
- **Where:** `python/legend_lite/engine.py:785-792`; `python/legend_lite/_runs.py:68` (`statement` steps are not
  wrapped) and `:88`; `_runs.open_project` (the `open` and `use` statements).
- **What:**
  - **(a) Lost connection during a load, or a LoadRefused raised before the database saw it.** The PR re-raises it. It
    ends in `_answered` -> `unexpected` -> a 500 in `text/plain`. The run branch (`engine.py:738`) and Java both answer
    `{kind: internal}` JSON.
  - **(b) Not every failed load statement reaches the new branch.** If the database refuses a load's `statement` step
    (DDL) or the project's open/use statements, the error is never wrapped in LoadRefused. A raw `duckdb.Error` escapes
    `_query_run` and becomes a 500 text. Java answers DATABASE_ERROR, a 422 data.
  - **(c) A raise inside a load statement loses its kind.** It is answered as data with no `raise` and no `where`; Java
    keeps both.
- **Fix:**
  - Answer the re-raised branch as `refused(LegendError(msg, INTERNAL))`.
  - Catch `duckdb.Error` around `projects.using`, with the same mapping as `run()`.
  - Read `raised_of(load.__cause__)`.

### F3. `redacted()` drops a raise's kind and where
- **Where:** `python/legend_lite/_reads.py:131-141`.
- **What:** the copy it makes clears `__dict__`, which removes `legend_raised`. A planned raise whose message contains a
  held value is then answered as data with no `raise` or `where`. This happens on the frames path (`Reading._failed`)
  and the project path (`fetched_redacted`).
- **Fix:** carry `legend_raised` onto the redacted copy.

### F4. Q9 writes a second copy of Java's classifier in Python
- **Where:** `engine.py:738-751`, `_raised.wire_name` (`_raised.py:141`).
- **What:** `LiteV1Api.refusal` is documented as "THE one classifier, which every binding reads (the server's routes,
  and the browser and Python bindings' folds)". Python now hand-codes `dataRefusal` and Java's `lowerCamel`. A future
  change on the Java side will not reach Python. Only three kinds are tested (`test_planned.py`).
- **Fix:** fold through the native library, for example a `lite_data_refusal(kind, where, message)` entry. At least
  generate the wire names into `_type_facts` and test every `ErrorKind`.

### F5. Q9's mapping is only partly tested
- **What:** nothing tests `duckdb.ConnectionException` -> 500 `{kind: internal}`. A user's `fail(...)` -> 422 data with
  no raise is tested only at unit level (`worded`), never through the route.
- **Fix:** add both at engine level, for example by closing the project cursor, and with a `fail('x')` query.

### F6. Q8 applies on Java alone, so the bindings disagree
- **Status:** called N-a and deferred in the Q8 commit message.
- **What:** for an Integer over DECIMAL(10,2), Python's query/run answers the decimal text, `"5.50"`, and `"5.00"` for a
  whole value, under an Integer column. The causes are `_reads._integer`, which leaves a scale > 0 decimal as it is, and
  `engine._lite_cell`, which formats a Decimal. Java's server answers 500 internal for the fraction and 5 for the whole
  value. The record's "one rule ... on every route" holds for Java's routes only.
- **Fix:** once R-a is ruled, add a shared case to `engine-client/test/fixtures/lite-api-cases.json`.

### F7. G2: the judges still disagree on the fraction
- **What:** HOST refuses by name, DATABASE answers AssertFailed "expected 5, got 5.50", so BOTH reports JudgesDisagree.
  This was there before the PR. It is recorded only in a commit message, a test comment and the record.
- **Fix:** track it wherever G2 gaps are tracked. R-a's ruling should also settle the database judge's side, for example
  by refusing in the plan.

### F8. R11(b)'s Python door is a marker, not a guard
- **Where:** `python/legend_lite/edge/_pure_v1.py:158,170`.
- **What:** three easy ways round it:
  - `EngineRoutes(_pure_v1._DOOR)`;
  - `object.__new__(EngineRoutes)`;
  - `Engine(other_frames, edge=edge.engine(f).edge)`, a one-liner using public names that composes legend-engine's
    routes on a second engine without passing the door.

  The risk is low, because the engine-library check has already run in the last case. No legitimate path breaks: the
  only constructor is the door, and no test stubs EngineRoutes.
- **Fix:** if the door matters, add a static check to `test_run_closure` (no reference to `_DOOR`, and no
  `EngineRoutes(` call, outside `legend_lite/edge`), and say in the docstring that reusing the door's instance is
  allowed.

### F9. Precise Integer subtypes are not covered
- **Where:** `JdbcDecode.java:286`, `RowDecode.java:135`.
- **What:** both match the literal string `"Integer"` (and `numericPure` ignores precise types). A column typed
  `meta::pure::precisePrimitives::Int` or `BigInt` over a DECIMAL slot still decodes as a BigDecimal. Python treats
  those types as Integer (`_types.is_integer`).
- **Fix:** check whether the product's plans can carry these names. If they can, decide by the lattice.

### F10. Q8's object read is untested
- **What:** the record says "lite's graphFetch path not probed", yet R-a argues for "one rule on every route".
- **Fix:** add a graphFetch case to IntegerOverDecimalTest.

### F11. CI is not green yet
These are the check runs at f059e5ec5 when I looked. Four lanes had failed:
- **linux / product and linux / parser_equivalence:** the fetch of `h2-2.1.214.jar` from Maven Central returned 404.
  That is environmental, not this PR; the same lanes passed on main's last three commits.
- **windows / datacube:** `cube_page_shell_test` failed on menu order. That is the TypeScript UI, which this PR does not
  touch.
- **windows / warehouse:** `//python:markets_test` timed out at 300s. It is probably flaky, but it runs Python's read
  path, which N-b changed, so confirm on a rerun.

About 23 lanes were still queued or running, including PCT and corpus. Those are the lanes where Q8's decode change
could move rosters.

### Minor
- The `IntegerOverDecimalTest` javadoc says "A driver hands its cell as the decimal it is (a BigDecimal)". On SQLite the
  driver hands text (B2).
- `test_a_runs_failure_is_data_as_javas_server_answers_it` unpacks `[case] = ...`, so it breaks as soon as the fixture
  gains a second data case.
- `EdgeServerTest.theEdgeHasNoDoorOfItsOwn` restates the deletion (by reflection). It is acceptable as a pin.

---

## Q9 against Java's `LiteV1Api.dataRefusal`, case by case

| Failure | Java (server) | Python query/run after the PR |
|---|---|---|
| A site's planned raise | 422 data, `raise` = lowerCamel(kind), `where` = site's | 422 data, `raise` = `wire_name(kind)`, `where` = site's. Matches; F3 is the exception |
| A user's `fail` (envelope, no site) | 422 data, no raise | 422 data, no raise. Matches |
| The database's own error during the run | 422 data (DATABASE_ERROR) | 422 data. Matches |
| The database's own error in a load's blob statement | 422 data | 422 data (LoadRefused branch). Matches |
| The database's own error in a load's `statement` step, or the project's open/use | 422 data | 500 text/plain. Differs (F2b) |
| Lost connection during the run (Java: SQLSTATE class 08 types) | 500 internal JSON | 500 internal JSON. Matches |
| Lost connection during a load | 500 internal JSON | 500 text/plain. Differs in shape (F2a) |
| A raise inside a load statement | 422 data with raise and where | 422 data without them (F2c) |
| An undeclared raise token | 500 internal JSON (IllegalStateException) | 500 text/plain (`unexpected`); this was so before the PR |
| The decode refuses a cell (Q8's fraction) | 500 internal JSON | 200 with the decimal text (F6, N-a) |
| The planner refuses a project's Data (N-c) | Java's own fold | the native fold's kind, where and raise. Matches by construction |

## Strengths
- **Q8 is narrow.** The decode chooses the carrier by the declared type, and Batch is untouched.
  - A parse error with no column (NumberFormatException) becomes a refusal by name.
  - The long boundary is right: `bitLength() <= 63`, so `Long.MIN_VALUE` stays a long and 2^63 becomes a BigInteger.
  - A BigDecimal with negative scale, and a JSON exponent token, both resolve exactly. Neither code nor JSON can produce
    a negative zero.
  - The tests went red first, under HOST, DATABASE and BOTH, on all three in-process databases, plus the edge.
- **Upstream's answers were asked of the engine runner, not recalled.** The bytes are recorded, and the record is honest
  about both divergences and the unprobed route.
- **The `_raised.py` refactor keeps `raised_words` and `raised_kind` behaving as before** (checked branch by branch),
  and the worded copy now carries the kind and where.
- **Q9's run-path mapping matches Java** on every case above except those marked.
  - N-c removes a 500-in-text answer.
  - The conformance fixture gains the Python binding for the data case, and the host-route test now checks `raise` and
    `where` as well as `kind`.
- **R11(b) is clean.**
  - The plants still bite: `planted_edge_main` has no deps and is no edge target, and the root regex matches its
    package, so ShippedArtifactRootsTest's assertions hold, `assertFalse(reach.contains(planted_opt_in_carrier))`
    included.
  - Both guard lines moved with dated justifications: the `not_layers` entry and `DUCKDB_NAMES` gaining
    `ConnectionException`.
  - Nothing in the build, the scripts or the docs referenced `EdgeServer.main`.
- **No other guard file is weakened.** The behavioural check N-b relaxes is covered in F1.
