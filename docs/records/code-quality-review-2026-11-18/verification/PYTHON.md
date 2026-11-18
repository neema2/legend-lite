# PYTHON verification

## Verdict on the unit: agree (MIXED)

The two S1 findings hold. Each needs a correction: the reviewer missed a forcing constraint, and the proposed
alternative is bigger than claimed. The S2 tier is inflated. PYTHON-5 (the "public API regression"), PYTHON-7, PYTHON-8
and PYTHON-9 are real but smaller than reported, and I would set them at S3. Even so, the unit carries notable debt
around a sound core: hand-ported plan runners, a costly guard layer, a half-finished move, and comments that record
history. MIXED is still the right verdict, and the strengths I checked are real.

The report also has a reliability problem the owner should know about. Its line numbers for three files are wrong, by
a consistent offset:
- `_runframe.py` has 86 lines, but the report cites `:324-354`, `:357-378` and `:390`. The real lines are `:19-49`,
  `:52-73` and `:85` (an offset of 305).
- `_pool.py` has 87 lines, but the report cites `:230-236`. The real lines are `:36-42`.
- `_plans.py` has 132 lines, but the report cites `:221-261`, `:237-240`, `:264-305` and `:273-277`. The real lines
  are `:48-90`, `:64-69`, `:91-132` and `:101-104` (an offset of 173).

The content described is correct in every one of these cases. Line references for the other files check out, a few of
them off by one.

## Findings

| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| PYTHON-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | 737 + 493 = 1,230 guard lines against 1,731 lines of run and seam code (recounted). 91 planted cases (checked with an ast script) plus 51 in `test_planned_guard`. The pin tables and incidental counting are as described: `SUBMITS` counts any constant `'submit'` and `AUTHORED` any name, at `test_planned_guard.py:253-254` and `:238-242`. The `NOT_PLANNING` copy that the Java regex forces is at `_library.py:19-25` and `PlannerSurfaceTest.java:1801-1810`. Missed constraint: the user asked for this lock-down ("let's lock this down", `test_run_closure.py:1`; "how do we stop this ... without loud alarm bells?", `test_planned_guard.py:151-152`), and G4 requires proof at compile time. The alternative is understated: import-linter covers only the import rules, and the rest is medium-sized work (see Details). |
| PYTHON-2 | S1 | CONFIRMED-WITH-CORRECTION | S1 | `PLAN_VERSION = 51` is at `_runs.py:26` and `plan-runner.ts:131`. `PlanVersionsAgreeTest.java:33` reads only the TypeScript constant. 82 string-keyed accesses (recounted). `_STATEMENT_FIELDS` and `_NOT_HERE` are at `_planned.py:114-121`. The fourth walker is at `tests/test_planned.py:203-224`. `root['steps']` at `:62` and `step['sql']['target']` at `:137-140` raise a raw `KeyError`, and `root = p.get('root') or {}` at `:132` defaults silently. Corrections: the runners are parallel, not "line for line". Python's `run_load` refuses `file` steps, while TypeScript's `deliver` also runs `file`, `bound` and `stream` steps. The TypeScript runner is hand-typed too (`plan-runner.ts:155`), so this is three hand-written runners (Java, TypeScript, Python). G1 ("ONE runner (`PlanRunner`)", GOALS_OF_RECORD:27) supports the finding. |
| PYTHON-3 | S2 | CONFIRMED | S2 | The two `read_plan`s are at `compiler.py:105` and `_planned.py:58`. The parse, dump and parse again happens at `compiler.py:264` and `_runs.py:38`. The dict records are at `_runs.py:316-331` and `projects.py:83-97`. The test mint is at `tests/test_planned.py:121`. This overlaps PYTHON-2: the root cause and the fix (typed plan readers) are the same, so do not count it as independent debt. |
| PYTHON-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The shims are as stated. `TYPE_KEY`, `held_texts` and `redacted` occur in `frames.py` only on its import line, and nothing reads them through `frames`. Corrections: the engine imports are at `engine.py:68` (projects) and `:64` (frames), not `:69`/`:65`. Use of the shims is larger than reported: 22 sites in `test_planned.py` (not 28), plus 17 in `test_engine.py` and 2 in `test_frames.py`. Stale `projects.run_*` names appear at 6 sites, not 4 (also `engine.py:1478` and `frames.py:158`). |
| PYTHON-5 | S2 | OVERSTATED | S3 | "No public way left" is false. Pass your own connection: `con = duckdb.connect(); Frames(con); con.execute(...)`. The constructor is typed for that (`frames.py:296`), the comment says the connection "stays theirs, to use as they will" (`:297-298`), `_planned.py:348` says "theirs to keep", and the README says the session is set "on a connection it opens or one it is given" (`python/README.md:51-52`). `Frames.connection` was never documented for raw SQL, in the README or in docs/. Count correction: 36 real `._con` sites. The 37th is a planted string at `test_planned_guard.py:331`. The change was deliberate and documented, but by the session and not by the user (see Details). |
| PYTHON-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low; S3 defensible) | `ModelRefused`/`NotAModel` and their twin sets are at `frames.py:71-84` and `projects.py:28-36`. The pasted version check is at `_runs.py:39-43` and `:126-129`. The `NOT_PLANNING` pair is at `_library.py:19-25` and `_loader.py:61-64`. Correction: four of the five engine blocks have the `_planned({'plan': self.planner.refused_by(...)})` form (`engine.py:1667`, `:1750`, `:1777`, `:1792`). The one at `:1547` answers `_said(422, ...)`. |
| PYTHON-7 | S2 | OVERSTATED | S3 | These are the facts: `LegendError.kind: str` at `compiler.py:24-32`, a bare `RuntimeError` at `_runframe.py:85` (not `:390`), and 25 regex assertions in `test_planned.py`. But the kinds are legend-engine's `errorType` spellings, carried by the native answer protocol (upstream fidelity), so a `StrEnum` would only wrap the same strings. "Planned inside a run" is a G4 defect that no caller recovers from, so "cannot catch it specifically" costs nothing in practice. A named class would be consistent with `Unframed`, but that is a nicety. The cross-language wording coupling is deliberate: `run-load-conformance.json` carries `LoadRefused`'s words so that both hosts say the same thing. |
| PYTHON-8 | S2 | OVERSTATED | S3 | `_planned.py` has three concerns, not four: the "run-frame check" at `:424-447` is part of `send`. `across_the_bridge` mints `PlannedSql`, and the guard confines minting to `_planned.py`, so only `Answered` (about 195 lines) could move without a rule change. `_runs.ask` against `_reads` is a naming quibble. A 447-line module with three related concerns is a cleanup, not a refactor. |
| PYTHON-9 | S2 | OVERSTATED | S3 | The code is as described: `__contains__` touches the LRU at `_planned.py:187-197`, and `_keyed_text(..., n-1)` is recomputed at `:260`. But touching an LRU on lookup is standard (as in `lru_cache`), the cost is only suspected, and it is bounded by `KEYED_CAPACITY = 256`. The "O(1) solve" overstates the fix: each counter writes `str(k)`, whose length grows with k's digits, so it is a monotone search (O(log n) builds) or a prefix check. The reviewer missed one point: keyed bodies are tried oldest first (`:193`), so the most likely match is tried last. |

## Details

**PYTHON-1 (forcing constraint and the true size of the alternative).**
- **The mandate.** The guards are not an agent's indulgence:
  - The user asked for the lock-down: "let's lock this down" (`test_run_closure.py:1`) and "how do we stop this from
    actually being able to be added without loud alarm bells?" (`test_planned_guard.py:151-152`).
  - AGENTS.md's G4 says the separation "is proven at run time (`RunScope`) and at compile time".
  - G9 asks for "a guard for every goal".
  - So *some* static guard is required. Its *shape* is not: count pins of incidental tokens, two overlapping import
    readers, and a Java regex over Python source.
- **What import-linter cannot replace.** The reviewer's "15-20 line import-linter contract set" replaces only the
  import half. It cannot express these rules:
  - `test_run_closure.py:281-317`: every send must sit in an `@runs` function or a `with in_run(...)` block. The
    runtime `Unframed` refusal (`_planned.py:428-447`) already covers this on exercised paths.
  - `test_planned_guard.py:221-273`: no `.execute` on a raw receiver outside the seam. A person's raw connection reaches
    `frames.py:296` without any `duckdb` import, so an import contract cannot see a send on it.
  - The mint-site rules.
- **A realistic replacement.** The package split, import-linter, one AST lint of 100-200 lines (mint sites, and raw
  sends outside `_seam`), and the existing runtime witness. That is a medium change, not a small one.
- **Two extra pieces of evidence the reviewer could have cited.** The guards shape product code in `_reads.py:121-125`:
  `copy.copy` plus `__dict__.clear()`, because "no value's own type is called outside _planned.py". And a third Java
  test (`DataMechanismCensusTest.java:370`) also scans `python/legend_lite`.
- **The tax, measured.** 22 of the 63 series commits that touched the run and seam modules also edited a guard. That
  is a real tax but not "every change". Steady state after C5-P is too short to measure (3 of 5 commits).

**PYTHON-2 (constraint).**
- **G11.** G11 ("run by any runner, including the WebAssembly planner's host") licenses several runners, and the shared
  fixtures (`run-load-conformance.json`, `keyed-expansion.json`) are a deliberate mitigation. G1 ("ONE runner") argues
  for the reviewer's direction, so the finding is aligned with the goals, not against them.
- **The native exec entry.** It is feasible for Python: the GraalVM native image is already in process, and
  `//core:exec` reaches no planner. But it needs a per-statement ctypes callback, or a step iterator, into
  duckdb-python, plus a third key beside `_library`/`_services`. For the browser it would need exec built under TeaVM.
  That is large.
- **The cheaper fix.** Generate typed step records, `PLAN_VERSION` and a `statements()` visitor, the way
  `_type_facts.py` is generated (`python/BUILD.bazel:126-136`). That is medium and stays within today's constraints.
- **Python's version constant.** It is held only by `test_engine`'s fixture run, as `_runs.py:23-25` admits. Adding a
  regex for Python to `PlanVersionsAgreeTest` is a one-line fix.

**PYTHON-5: is there really no public way to run your own SQL, and was the change ruled?**
- **The facts.** Commit `4ea13c0a4` (2026-11-12, "C checkpoint's second audit: Python's modules hold the host's
  Database, never a raw DuckDB connection") changed `Frames.connection` from duckdb's connection to `_planned.Database`.
  Its message states the intent: "Frames wraps a connection a person hands it, and the person keeps their own handle",
  and "The tests reach the raw connection through `._con` for their own fixtures". So the change was deliberate and
  documented in the commit.
- **The umbrella ruling.** It was made under the user's ruling C3 (`docs/TOTAL_WALL_START_HERE_2026_11_11.md:593-595`:
  "in Python the host's `_planned.Database` sends only a plan's statement").
- **What was never ruled.** I found no ruling by the user, no `SEMANTICS_REGISTER` row, and no README, changelog or
  docs/ note on the public attribute itself. The change came out of an agent audit's SHOULD-FIX (`docs/GATES.md:21976-21977`
  lists it among "the second round" fixes).
- **What was left stale.** The class docstring was not updated ("Use the connection through Frames' methods...",
  `frames.py:290-293`). The parameter is typed `duckdb.DuckDBPyConnection`, while the attribute is now a private type.
- **What is actually lost.** Your own SQL on a database that *Frames* opened. That combination matters for one reason
  the reviewer missed. Remote files are read only on a Frames-owned connection (`engine.py:740-741`: "these frames are
  on a database of your own"), so a person who wants both remote files and their own SQL beside them has no public
  route.
- **Why this is S3.** It is a narrow gap in an undocumented attribute, and the documented path still works. The
  fixture smell (36 `._con` reaches in tests) is cheap to fix: `test_planned.py:80` already uses the right pattern ("the
  test's own fixture, on its own connection"). The reviewer's `Frames.duckdb` escape hatch would survive the guard
  (`self.duckdb.execute` from a product module would still be caught as an unpinned receiver). But it contradicts the
  session's stated design ("modules hold ... never a raw DuckDB connection"), so it is the owner's call.

**PYTHON-7, PYTHON-8, PYTHON-9 (downgrades).** The reasons are in the table. Each describes real code, but the claimed
cost is either a non-use-case (catching a G4 defect), a constraint the report missed (the mint must stay in the seam;
the wording is shared on purpose), or a suspected cost with a standard idiom (touching an LRU on lookup).

## S3 and strengths spot-checks

- **S3 PYTHON-10 (469 dates): confirmed exactly.** `grep -oE "20[0-9]{2}-..." -r legend_lite tests | wc -l` gives 469.
  `test_planned_guard.py` has 53 dates and `_planned.py` 13. The "56 audit or ruling words" count is reproduced at 51
  to 55, depending on the word set. The stale references were checked:
  - `_pool.py:8-12` contradicts `submit`, which is at `:36-42`, not `:230-236`.
  - `_planned.py:65-66` and `tests/test_planned.py:3-4` name `tools/guards/PlannedSqlTest.java` as the Python guard.
    `PlannedSqlTest.java:28-30` points to `test_planned_guard.py`.
- **S3 PYTHON-12 (dead `Result` forwarders): confirmed.** `fetchmany`, `fetch_arrow_table`, `fetch_record_batch` and
  `.df()` have no caller in `python/` or `datacube/test`. `EXEMPTIONS = frozenset()` is at `_runframe.py:61` (not
  `:357`).
- **S3 measurement (jscpd 0.25%): reproduced.** Three clones, 30 lines.
- **Strength: the exact-type brand: confirmed.** `require` uses `type(statement) is PlannedSql` (`_planned.py:294`).
  The `__class__` spoof test is at `tests/test_planned.py:150-160`. The facade has no `execute`, `sql`, `query`,
  `from_arrow` and so on (`tests/test_planned.py:83-98`).
- **Strength: redaction without chaining: confirmed.** See `_reads.py:114-127` and the raise outside the `except` in
  `fetched_redacted`. One nuance: its `copy.copy` shape is partly dictated by the guard's `type(x)(...)` ban
  (`:123`).
- **Strength: the run frame: confirmed in substance.** It uses a `ContextVar` and resets by token (`_runframe.py:19-49`,
  not `:324-354`), and the pool copies the context (`_pool.py:41`).

## Missed (if any)

- **The report's own line numbers.** The `_runframe.py`, `_pool.py` and `_plans.py` references are wrong throughout,
  as described above. An owner who navigates by them will land on nothing.
- **`Frames.connection`'s real cost.** The remote-file feature requires a Frames-owned connection (`engine.py:740`), so
  "own SQL" and "remote files" are mutually exclusive for a user. This is the concrete user-facing cost behind
  PYTHON-5, and the report does not name it.
- **`Answered.__contains__` order.** It iterates keyed bodies oldest first (`_planned.py:193`), which defeats the point
  of keeping them in LRU order for the lookup.
