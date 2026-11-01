# M6's independent audit: the findings (2026-11-01)

Audited: the M6 code, commit `89e604de` on `claude/wizardly-dirac-rw6tim-seam-wip` (the handoff's §4.1), against the
brief `M6_AUDIT_BRIEF.md`. Read-only; nothing was built or run by the auditor. **None of these findings is folded yet**:
the next session folds the should-fix items before M6 is committed to the working branch (the handoff's §4.3).

**No blockers.** The walk's HOST dispatch matches the base arm by arm:
- assertEquals keeps the base's order: mayArm (a superset of tryArm's gates), rendered text, cell pool, grid pair,
  equals. sameElements, size, JSON, contains, eq, tolerance, condition, instanceOf and empty match as well.
- `staticallyIdentified` is true exactly when `isVerdict` is non-null. The host `staticallyDecided` was a no-op.
- On non-raising paths the census matches: `sqlFamily` is counted before the fetch; `sqlRoute` fires only where the base
  fired it (the grid pair, and rendered text on a held verdict); `sqlFellThrough` is unchanged.
- BOTH and DATABASE dispatch are untouched, apart from the shared HostJudge halves (N1).
- The guards hold: the census goes through AssertVerdicts statics (the V3 rule, nested `HostAssert` included); the
  testplan rule and `plannerIsExecutionFree` hold; the CodeShapeGuardrailTest allowlist entry is dated.

## Should-fix

- **S1. Planning failures outside the planned-raise try** (`driver/HostAsserts.java`, the `registerHandlesIn` and
  `frameReplaceEnv` calls).
  - Both run outside the try that turns failures into planned raises. `FramePlanner.renames` calls
    `SqlPostProcessors.reachableRenames`, which inlines user calls and can throw.
  - Example: `assert(true); assertEquals(1, my::helper());`, where the inliner refuses `helper`. The router fails the
    whole body before assert 1 sends or reports. The walk reports assert 1 passed, then raises at statement 2.
  - Fix: plan it as a raise at the assert's turn WITHOUT the census or the listener. The walk raises it before
    `tryAdjudicate`: no `sqlEnter`, no verdict.
- **S2. Only `DataError` reaches `sidesFailed`** (`exec/PlanRunner.java`, the HOST sides' catch).
  - These bypass the sink: an `UncheckedIOException`, an `IOException`, an `IllegalStateException` or
    `NotImplementedException` from `result` or decoding.
  - The walk counts `sqlEnter`, the family and `not-attempted <family> raised` with a listener (the family alone
    without one); the router counts nothing.
  - Fix: catch RuntimeException and IOException too, and route them to a sink hook that enters and raises the census.
    Report a listener verdict only for a DataError, as `tryAdjudicate` does.
- **S3. A judgment that declines after its sides ran fails the moved body** (`driver/BodyRouter.java`, `sides`).
  - Concrete case: `{| assertInstanceOf(1, type(2));}`.
    - It is ASSERT_PROGRAM: `type()` is a native call, not a `TypedPackageableRef`.
    - `planHost` plans it, and its side is a constant.
    - `instanceOfOf` returns null.
  - The router raises "NotImplementedException: assertInstanceOf on the program road...". The walk counts a
    fell-through and adjudicates by `prepareValue`'s generic path.
  - Fix: `planHost` returns null where the decline is static (`typeRefName(args.get(1)) == null`; tdsEquivalent sides
    not relation-typed), keeping HostJudge's fetch-then-decline for the walk. Decide contains (a non-[1] value) and
    JSON (a non-String side) statically, or record them as residue.
- **S4. `BodyRouterParityTest.assertPrograms` cannot catch walk drift.**
  - It compares the NEW walk with the router, so drift from the base (N1, N2) is invisible to it.
  - Cases it lacks:
    - an assert after a failing one (planned, never reached);
    - a planned raise, from classification and from side planning;
    - a judgment decline (S3);
    - a body `planHost` refuses (walked, `BODIES_MOVED` unchanged);
    - assertIs, assertInstanceOf, assertTdsEquivalent, rendered text;
    - a let between asserts;
    - a non-DataError side failure (S2).
  - Add these; the census and verdict comparisons are already there.

## Nits

- **N1. Walk drift from the base: tdsEquivalent** (`HostJudge.planTdsEquivalent`). The tolerances are now fetched in
  the same group as the grids:
  - where a grid is not tabular, the base fell through without evaluating the tolerances; now computed tolerances are
    sent, and a tolerance's refusal raises;
  - a tolerance on another session now raises "two sessions";
  - tolerance sides get the to-one check.
  
  With literal tolerances the outcome is identical. BOTH mode is touched through AgreementArm's host half. Keep the
  two-group fetch, or record it as movement.
- **N2. Census drift from the base on classification raises.** The base counted `sqlFamily` before
  `narrowSideStamps` and the arm's classification (renderForm, OrderView, elementTyped, identityCondition).
  `planHost` classifies before `enter()`, so on both roads such a raise lands on "(pre-arm) raised" instead of
  "<family> raised" (rare).
- **N3. The moved set is decided in the driver.**
  - What moves is ASSERT_PROGRAM minus the bodies `planHost` refuses, and that test lives in the driver
    (`BodyRouter`), not in BodyClass. `BodyClass.of` answers ASSERT_PROGRAM for bodies that are then walked, and
    BodyClassTest pins only the class.
  - `SqlTextVerdicts.lookThrough` decides "no producer" by catching a RuntimeException from the inliner: the letter
    of R16 (never route by catching a failure) is broken there.
  - Otherwise the rule is static.
- **N4. A session opened per assert vs once per program.**
  - The walk opens a session for each assert; the program road opens one. When autocommit is off (the
    `PlanRunner.opened` memo false), the session statements are re-sent at each open on the walk and once on the
    program road.
  - A keyed side's snapshot brackets the whole program, not one assert's group.
  - Identical under the harnesses' autocommit-on sessions.
- **N5. Javadoc.**
  - `HostSides.Check` says judgments read facts "captured when it planned (... the model's key trees)": they read the
    model and the typed tree at judge time.
  - `BodyClass.of(body, runtime)` says "under the database judge"; it means any non-HOST mode, BOTH included.
- **N6. Code.**
  - Bare `{}` blocks are left in `HostJudge.subsetOf` and `PlanRunner.databaseVerdicts`.
  - 13 new lines in HostJudge exceed 120 characters.
  - The verdict lookup (an IdentityHashMap in HostAsserts) relies on PlanMaker passing the `Verdicts` node through by
    identity. `withoutUnreadStamps` already rebuilds `Check` steps; a future rebuild of `Verdicts` would surface as an
    IllegalStateException.
  - `HostSides.Planned` holds a live Session used only as a boolean.
  - `raising()` writes wantEqual true into the plan's Verdict.
  - On the walk the judgment closures capture the live let-prefix list: safe only because the walk judges at once.
- **N7. A guard hole that predates M6.** The `SIG` pattern in CodeShapeGuardrailTest excludes `@`, so `METHOD_LIMIT`
  (250 lines) never scans methods with a `@Nullable` return type. `AssertVerdicts.adjudicate` is about 300 lines, and
  `planHost` is never scanned.

## Residue to name in the records

- **B3.**
  - `ExecutionPlan.Verdict` carries name, arm, wantEqual, base and wrapper only: no `check`, no judge-time facts.
  - The judgment is a closure the driver holds over the TypedSpec arguments, a copy of the let prefix and the ExecEnv.
  - It reads the model at judge time: `ctx.isSubtype` (instanceOf); `instanceKeys`/`restrictToKeys` (equals,
    sameElements, sqlByteVerdict); `keylessCtorUnderLambda`.
  - It reads the typed tree at judge time: `info().type()` via typedSide, KindClass, isAnyStamped and structuredSide;
    `typeRefName`; `envelopeValuesRead`; `OrderView.of` in renderedOf and jsonOf.
  - The plan cannot be judged without the driver.
- **T1.** The sides are the walk's host-side plans (`BodyDriver.planHostSide` -> BodyPlanner/SidePlanner, with the
  identity lane, the fold and the splice hook), not the product statement road's.
- **R14.** A planned raise is an exception captured in a closure, not a raising plan step. Every planning failure,
  constructs refused by name included, is raised at its turn for parity with the walk (R23); the compile-error vs
  planned-raise split is not applied.
- **B4.** The assert module (`planHost`, `HostJudge.planX`, `HostSides.plan`, `HostAsserts`) stays in
  `com.legend.driver`. testplan only places opaque Verdicts through `TestDoor.Asserts`, so the B4 rule passes trivially.
- **B5.** The census is counted by the driver's sink against the global `CURRENT_FAMILY`. The runner counts nothing per
  verdict step, and Verdicts steps carry no statement origin.
- **Run-time declines.** On the program road, contains with a non-[1] value and JSON with a non-String side are
  refused by name at run time (S3).
- **Scope gates.** DATABASE and BOTH bodies stay on the walk; the `planHost` gate sits in the driver (N3).
- **Sessions.** One session per program vs one per assert, and the snapshot scope (N4).
