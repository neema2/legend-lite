# Audit 20c — Architecture coherence, range `1f5bb41d^..HEAD`

Range: B2 design + B2a Result surface (1f5bb41d), groupBy stage 2 +
GroupBySynthesis (ede59210), B2b platform result frame + TestBody surgery
(874c0625), null-semantics + NullSemantics extraction (f67b12b8),
core-bridge wiring (2cd5e491, ce9cd0e1), auto-map slices (90f27f52,
4859a84f).

## Verdict

This range's features landed substantially as coherent architecture, not
as a smattering. B2b executed its own design doc faithfully: the envelope
splice rules moved to the platform in ONE place (StatementExecutor's
result frame), typed, with exact-FQN recognizers replacing the harness's
string peels; TestBody kept only orchestration + comparison policy;
the auto-map folds route through the existing object-space funnel
(`scalarMapAsProject`) instead of growing a parallel path; NullSemantics
and GroupBySynthesis are honest single-owner extractions; the core bridge
is a documented zero-decision bijection with no silent fallback.
ArchitectureTest and CodeShapeGuardrailTest both pass with EMPTY
allowlists (verified by run). Three debts keep this from a clean bill:
(1) the Typer's `.rows` marker is minted globally in G but erased only in
one caller's optional hook — I VERIFIED by probe that it leaks on every
non-hook path and regresses previously-working public API spellings
(HIGH); (2) StatementExecutor now conflates three roles (sequencing,
result frame, K-native dispatch) and is one feature away from needing the
same Doors-style split the normalizer already got (MED); (3) Scalars.java
sits 18 lines under the 3,500 file guardrail — the next rule lands as an
unplanned mid-feature split (MED). Counts: 1 HIGH, 4 MED, 4 LOW.

---

## 1. B2b result frame in StatementExecutor — coherent component, wrong container (MED)

`core/src/main/java/com/legend/StatementExecutor.java` (875 lines).

The frame itself is COHESIVE: a banner-delimited section (lines 199–463)
with the `ExecFrame` record (:212), exact-FQN envelope recognizers
`AT_FQN`/`TO_ONE_FQN`/`SIZE_FQNS` (:218–223 — honors the exact-FQN
tenet, an upgrade over the harness's simple-name peels it replaced),
`buildFrame` (:231), `aliasFrame` (:306), `spliceHook` (:351),
`valuesFrame` (:423), `spliceValuesRead` (:437). The splice rules did
move VERBATIM and exist NOWHERE else as evaluation logic (grep-verified;
see §2 for the harness's policy-only residue). The class doc still says
"nothing in this class decides pipeline ORDER" and that holds — the frame
composes typed trees and hands them to the same
`resolve → executeTyped` tail as everything else.

The problem is the CONTAINER, not the component. StatementExecutor now
stacks three separable roles:

- statement sequencing + call frames + effect analysis (:68–157,
  :522–601) — the audit-17 extraction's original mandate;
- the result frame (:199–463) — B2b;
- K-native dispatch: executeInDb, dropAndCreateTableInDb/Schema,
  print, setUpDataSQLsV2, the effectful-map arm (:603–836).

That is exactly the accretion shape MappingNormalizer had before
ViewRelation/JoinChainEmission/GroupBySynthesis. The frame's dependency
surface is narrow (`specs`, `env`, `letPrefix`, and `executeTyped` as the
one callback), so a `ResultFrame` class in the same package extracts
cleanly. PHASE_B2_RESULT_VALUE.md does not literally demand a class
("StatementExecutor's statement loop gains a RESULT FRAME"), so this is
not a design violation — but §B2c's "move the seam split if file sizes
demand" is approaching.

Internal duplication inside the frame: `aliasFrame` (:310–341) and
`spliceHook` (:378–398) each implement the values/at/toOne peel AND the
`at(k>0)` loud-guard, with the identical error string spelled twice
(:335–337 vs :394–396). One envelope-walk helper should own both.

## 2. TestBody after surgery — one-owner held for evaluation; policy re-encodes the peel vocabulary (MED)

`core/src/main/java/com/legend/harness/TestBody.java` (1,795 lines).

The one-owner rule HELD where it matters: no envelope EVALUATION
survives in the harness. `ExecHandle`, the `.values` substitution arms,
`relationRooted`, `envelopeIndexError` are gone; execute-touching
statements forward verbatim (`execStmts`/`execVars`, :245–247, :301–307)
and every read rides `Compiler.executeResolved` — the one back-half
sequence (`evalSpliced`, :1041–1054). `substitute` (:1592) explicitly
does NOT touch execute-binding reads. The forwarding machinery is
minimal orchestration, not a shadow layer: it decides WHICH statements to
forward, never what they mean.

Two qualified findings:

- **The peel vocabulary lives twice.** `orderView` (:133–151) and
  `recordExecChain` (:155–181) re-encode the envelope shape — peel
  `.values`, `at`, `toOne` (by bare simple name, no arity/receiver
  checks) to find the query chain — so `endsInSort` can see a sort inside
  the lambda. `containsValuesRead` (:853) recognizes `.values` for the
  advisory/mixed-assert classification. Both are genuinely
  order/advisory POLICY (the design doc explicitly leaves compare policy
  in the harness), and a wrong answer only weakens ordered-compare to
  multiset or widens Unsupported — it cannot produce a wrong VALUE. But
  it is shape knowledge that will drift silently if the platform's
  envelope rules ever change; the untyped AST makes exact-FQN
  identification impossible here, which is the structural reason the
  duplication exists at all. Watched duplication, acceptable today.
- **Re-execution amplifier.** `evalStatements` fires at EVERY forwarded
  binding with the WHOLE prefix (:306), and each assert read re-submits
  the prefix again (:1046–1048); StatementExecutor's eager `buildFrame`
  then re-runs each `execute()` per submission. A body with N bindings
  and M reads executes early queries O(N+M) times where the engine runs
  each once. Correctness-neutral for read-only queries (which execute()
  queries are), but it is harness-side behavior the platform frame was
  supposed to fully own; a session-scoped executor handle is the fix
  when it matters.

## 3. UserCallInliner hook — clean extension point, with a fence to watch (LOW)

`core/src/main/java/com/legend/compiler/spec/UserCallInliner.java`
:116–137, :303–309.

Judged a clean seam, not a backdoor: the contract is documented
(same-reference = no-op; a replacement is recursed into), the hook is
optional with the old ctor delegating, and it exists precisely to honor
the B2 design's "never two substitution mechanisms" — the alternative
was a second full TypedSpec walker in StatementExecutor duplicating the
54-case vocabulary switch. Arguments are hook-processed before entering
`env`, so β-spliced occurrences cannot smuggle unprocessed frame reads
past it.

Residual risks, both convention-held rather than type-held: (a) the
splice semantics now execute INSIDE phase G½ — the phase named "user
call inlining" quietly also does K-frame rewriting when one specific
caller asks; (b) `rewrite(h, env)` after a hook replacement can loop
forever if a hook returns fresh non-fixpoint nodes — nothing enforces
termination. Fine with exactly one hook user; a second user should force
extraction of a named typed-rewriter seam instead of stacking operators.

## 4. The `.rows` marker leaks on every non-hook path — VERIFIED regression (HIGH)

`Typer.java:1134–1143` mints the marker for EVERY relation-typed
`.rows` read; the ONLY erasure is `StatementExecutor.spliceHook`
(:359–364), which is installed on exactly one of the five
inliner-to-lowerer paths:

| Path | Hook? |
| --- | --- |
| `StatementExecutor.executeStatements` main loop (:123–124) | yes |
| `Compiler.plan` / `compile` (:271) — public API | **no** |
| `Compiler.lowerResolved` (:412) — toSQLString surface | **no** |
| `StatementExecutor.toSqlString` (:184–186) | **no** |
| `StatementExecutor.buildFrame` — the execute() lambda body itself (:248) | **no** |
| `executeCallStatement` arg evaluation (:537) / resolver test helpers | **no** |

StoreResolver passes the marker through untouched (the `default ->
yield n` "pure relation query" arm, StoreResolver.java:412–419), so it
reaches the Lowerer. Probe (scratch JUnit, deleted after run): through
`Compiler.compile`, `#TDS…#.rows->size()` and `#TDS…#.rows->map(r|$r.val)`
both now throw `NotImplementedException: lowering not yet implemented
for TypedPropertyAccess`. Before this range the Typer returned `source`
(identity) and these spellings lowered. So the marker is (a) a
behavioral REGRESSION on the public plan path, and (b) a misattributed
failure — it presents as a lowering vocabulary gap when it is an
unerased G-phase marker. The Typer comment "no other consumer sees it"
(:1141) is false today. Silent-wrong-answer risk is low (the
variable-source arm `columns.resolve(v.name(), "rows")`,
Lowerer.java:2017, needs a relation-typed lambda variable, which the
current vocabulary doesn't produce) — the leak is loud but dishonest.

**Right home for erasure: phase H.** Every path to the lowerer passes
through `StoreResolver.resolve`; the K hook runs pre-H, so its
`$r.values.rows->at(k)` vs `$r.values->at(k)` disambiguation (which is
correct and genuinely needs the marker — traced: the hook peels the
marker to its source, then splices `.values`, leaving `at(k)` as a real
row index) keeps working unchanged. A one-line identity arm in
`resolveNode` (TypedPropertyAccess "rows" over RelationType → resolved
source) restores the invariant "H output contains no G markers" and
un-regresses the plan path. Erasure-in-the-hook alone couples a global
invariant to one caller's optional argument — that is the smell.

## 5. Auto-map folds in StoreResolver — right seam, mechanical edges (MED)

`core/src/main/java/com/legend/resolver/StoreResolver.java`.

The seam choice is right for the current size: `resolveNode` measures
234 lines (guardrail 250) with the fold arms as thin routes (:261–276)
into extracted bodies, and the folds themselves target
`scalarMapAsProject` → `resolveChain` — the EXISTING object-space
funnel, not a parallel machine. `HopChain` (:517) and `substituteParam`
(:645) belong here: they are object-space demand analysis, same layer as
`isObjectSpace`/`containsGetAll`. A Doors-style `AutoMapFolds` split is
not yet warranted — but the family is explicitly sliced (slice 1, 2a, …)
and the next slices (nested `->map` chains, non-root positions) should
trigger it before resolveNode's arms multiply.

`substituteParam`'s reuse of the inliner's let-reduction is SOUND —
verified against the capture machinery: `reserveFreshNames` clears
user-written `_iN` collisions over the whole synthetic body including
the injected read; nested binders α-rename unconditionally under a
non-empty env, so a mapper body's inner `p`/`r` binder cannot capture
the injected free variable; and the one deliberate capture (the free
`p`/`r` in the hop-read binding to the NEW wrapper lambda's parameter,
including the mapper-param-named-`p` self-capture case) is exactly the
intended binding. One substitution engine, no second walker — correct
call.

Mechanical edges to clean at next touch:

- `classExtentCount` (:535–576) was extracted VERBATIM with the old
  switch-arm 16-space indentation intact — a cut-paste, not a seam.
- `foldScalarHop` (:578) and `foldHopMap` (:598) duplicate ~20 lines of
  hop-read + wrapper-lambda construction.
- `foldScalarHopFilter`'s guard (:270–275) uses
  `containsGetAll + instanceof TypedPropertyAccess` where its siblings
  use `hopChainOf` — same family, different idiom.

## 6. Normalizer extractions — one normalizer in four files, not four components (LOW)

GroupBySynthesis (265 lines), JoinChainEmission (663), ViewRelation, vs
MappingNormalizer (3,332). The extractions are honestly labeled
("file-size seam") and each owns a real behavior slice — GroupBySynthesis
owns ALL of ~groupBy synthesis + stage-2 grouped navigation (callers:
MappingNormalizer.java:2102, :2109, :2122; ViewRelation.java:139). But
the package-private call graph is BIDIRECTIONAL: GroupBySynthesis calls
back `MappingNormalizer.seedAliasScope` and reads
`MappingNormalizer.AGGREGATE_FNS` (GroupBySynthesis.java:137, :221);
JoinChainEmission makes 12+ callbacks into MappingNormalizer statics
(findPropertyTypeDeep, resolveViewRefsInJoin, determineTargetTable,
suffixTargetReads, findPhysicalColumn, …) and all share the mutable
`Pipeline p` frame. That is a web, not a layering — coherent only read
as "one normalizer, physically split." Acceptable under guardrail
pressure and consistent with the package's own precedent; a real seam
would hoist `Pipeline` + the shared store-lookup statics into a package
utility so the helpers stop reaching back into the god-file. No behavior
is duplicated across the fragments (checked: groupByOpsMatch has one
definition, used by both consumers).

NullSemantics (57 lines) is the clean counter-example: one behavior
(the engine's processNotEqual/processNotIn null arms), zero callbacks,
one caller (Scalars.java:146–176), engine-file provenance cited. The
not()/COALESCE partition policy staying in Scalars' `not` rule while the
notEqual arms moved is a defensible line (the rule table owns rule
dispatch; NullSemantics owns the emission), though the null-consistency
`Set.of(LESS, …)` inline at Scalars.java:~165 is the same null-semantics
family and could live beside its siblings.

## 7. Guardrail health (MED)

Verified by run: `ArchitectureTest` passes (exit 0);
`CodeShapeGuardrailTest` limits are METHOD 250 / FILE 3,500 with EMPTY
allowlists — the burn-down ledger is at zero, which is excellent.

- **Scalars.java: 3,482 / 3,500 — 18 lines of headroom.** The
  NullSemantics extraction removed 57 lines, i.e. the smallest cut that
  ducked the ceiling; the null-consistency additions in the same commit
  spent most of it back. The next scalar rule forces an unplanned split
  in the middle of whatever feature needs it. Split PROACTIVELY: the
  RULES registration blocks group naturally (string family, temporal
  family, numeric family).
- MappingNormalizer 3,332; StoreResolver 3,028; TestBody 1,795;
  StatementExecutor 875 — all legal; the first two have their split
  pattern established, StatementExecutor's is §1.
- Method lengths: resolveNode 234 (guardrail-adjacent by design —
  the fold arms were extracted specifically to stay under; the pattern
  is working as intended).

## 8. Core-bridge commits — acceptable transitional seam (LOW)

2cd5e491 / ce9cd0e1. The shape is right for a module slated for
deletion: default routes to CORE (`System.getProperty("legend.pipeline",
"core")`), the legacy path needs an explicit `-Dlegend.pipeline=engine`,
and there is NO silent fallback — a core failure fails the scoreboard,
which is the stated point. CoreBridge itself
(engine/src/main/java/com/gs/legend/server/CoreBridge.java) honors the
bridge rule: field-by-field re-wrapping, one static type table, loud on
anything unrepresentable, and a self-destruct notice ("this class dies
with the engine module"). Given the engine module's fate, a
property gate beats any structured DI — building a real seam into
dying code would be over-engineering.

Two nits, both tolerable:

- The gate is PER-OVERLOAD, not per-class:
  `PlanGenerator.generate(String, …)` (:90–101) routes to core but the
  `generate(PureModelBuilder, …)` overload runs legacy V1 un-gated — a
  caller holding a model builder silently measures the legacy pipeline.
  Today's callers (QueryService legacy arm, tests) are consistent, but
  the seam's honesty depends on call-site discipline.
- `QueryService.execute(3-arg)` (:98–106) still builds a legacy
  `PureModelBuilder` for connection resolution even on the core path —
  the source parses twice per call. Known, commented, transitional.

---

## Positive findings (for the record)

- B2 migration discipline held: each envelope shape's harness arm died
  in the same range its platform twin landed — no dual-owner window in
  the final state (PHASE_B2_RESULT_VALUE.md §Migration was followed).
- The frame recognizers use exact FQNs (`AT_FQN`, `TO_ONE_FQN`,
  `SIZE_FQNS`, `PlatformTypes.EXECUTE`) where the harness they replaced
  used simple names — the typed move genuinely upgraded identification.
- `MapChecker`'s relation-row-map multiplicity fix landed in the
  checker layer, not compensated downstream.
- `ExecuteFrameTest` (157 lines) tests the platform frame directly —
  the behavior has a platform-level owner AND a platform-level test.
- The auto-map folds reuse the inliner as the single substitution
  engine rather than growing a third walker — the B4 lesson stuck.
