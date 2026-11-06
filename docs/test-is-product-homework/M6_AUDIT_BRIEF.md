# M6's independent audit: the brief (2026-11-01)

The brief the M6 audit was started with (an agent, read-only, while the chain ran). Re-use it verbatim for the next
session's audit of the code on `claude/wizardly-dirac-rw6tim-seam-wip` (`89e604de`); the design it audits is the
handoff's §4.1 (`docs/TEST_IS_THE_PRODUCT_HANDOFF_2026_11_01.md`). The auditor must not edit files or run Bazel while a
chain runs.

## What to audit (find real defects; rank them blocker / should-fix / nit)

A. **Walk equivalence.** For every arm converted in `driver/HostJudge` and for the grid pair, rendered text, IS
   (`staticallyIdentified` vs `isVerdict`), tdsEquivalent (now one fetch of 3-4 asks where the walk fetched the grids
   then the tolerances in two runs), contains/jsonStringsEqual/instanceOf (judgments that may return null after
   fetching): is the walk's outcome, message, statement order and census identical to the code before the change
   (compare against `git show <base>:<path>`)? Census ordering (sqlFamily before the fetch; sqlRoute only where the old
   code recorded it), sqlFellThrough, BOTH mode (AgreementArm untouched: `planHost` is for `JudgeMode.HOST` only),
   DATABASE mode (untouched).

B. **Program-road equivalence for the moved class.** Does the router reproduce the walk's listener calls, census,
   outcome, raw-SQL ledger, statement order and answer? A side the database refuses; a side whose planning raises; a
   classification that raises; a judgment that declines (null) after fetching; non-DataError runtime exceptions while
   reading sides; asserts after a failing one (never reached); the let prefix captured per assert (`HostAsserts`
   copies it) vs the walk's live list; `registerHandlesIn`; `frameReplaceEnv`/`spliceHook` with no frames; the last
   statement rule.

C. **The class rule** (`testplan/BodyClass.assertProgram`). Can a body reach `ASSERT_PROGRAM` that the walk would run
   differently? (A let whose `LetPlanner` kind is VALUE but the walk treats differently, a statement-root assert that
   is a user function wrapping asserts, seeds, a print, effects inside an assert argument, a value statement before
   the last, a trailing let.) Is it a named rule over the typed shape, never by catching a failure (R16)? The router
   also requires `HostAsserts.plan != null` (`planHost` returns null for shapes it does not plan): judge whether that
   is a static rule.

D. **Guards and invariants.** AGENTS.md (no fallbacks or defaulting, invariant 4), the ArchUnit rules
   (`hostVerdictIsReachableOnlyFromTheVerdictSeam`, `theTestPlanningLibraryIsTheDriversAlone`,
   `plannerIsExecutionFree`), `CodeShapeGuardrailTest`'s allowlist addition (a dated justification), mutable state in
   records and closures (judgment closures capture the let prefix and the environment), NullAway-sensitive code.

E. **B3/B4/B5 honesty.** The briefs ask for a verdict record carrying every fact a judge reads, the assert module in
   the test-planning library, the census by step. This change keeps the assert module in the driver and judgments as
   closures over plan-time objects (reading the model's key trees at judge time). Name precisely what residue remains,
   so the records can name it.

F. **Anything else.** Dead code, wrong javadoc claims, misleading messages, tests that do not test what they claim
   (`driver/BodyRouterParityTest.assertPrograms`).

Report: a concise list of findings, each with file:line, severity, the concrete failure scenario (input -> wrong
result) and a suggested fix; then the residue items for the records.
