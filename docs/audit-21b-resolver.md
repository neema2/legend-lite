# Audit 21b — resolver machinery correctness (auto-map flatten + correlation pass + bare-lambda typing + scalar guard)

Range audited: 90f27f52, 4859a84f, 5b6c0ca5, 2307d6ff, 2402aece, 21333d32,
8bdd9ecb (HEAD at audit time: 28245c49). Probes: scratch mains against
`core/target/classes` (`/Users/neema/.claude/jobs/9693939d/tmp/aud21/Corr*.java`),
engine ground truth `testQualifier.pure` / `simpleTestModel.pure` /
JoinChainEmission + MappingNormalizer emissions.

## VERDICT

The machinery is architecturally right and the assoc-route correlation pass is
end-to-end correct (first actual verification — the sweep at 2402aece had zero
passing coverage of it), but the range contains **two live wrong-rows/values
holes and one mis-bookmarked loud bug**. (1) The nav-route correlation wiring
(21333d32) SILENTLY DROPS the correlated conjunct whenever the demanded
navigate step sits below a `TypedJoinSlot` in the pipeline spine — a mapping
PM-declaration-order accident — because `augmentNavPredicates` walks only the
Navigate/Filter spine while `Pipelines.navSteps` (and the demand scan) walk
everything, and the one `applyToPipe` site that could catch the leftover
deliberately skips correlated preds; probe-confirmed `ON t0.ID = t1.TID` with
the `AND … = t0.TDATE` conjunct gone. This is exactly the wrong-rows trap
2402aece's four guards were built to make impossible. (2) The navigate-slot
flatten arm (`flattenNavSlot`, 5b6c0ca5) emits LEFT OUTER with no null-skip,
violating the flatten's own documented INNER ≡ LEFT+reader-skip contract:
`Trade.all().events` over a Join-PM mapping serializes a phantom all-null
object for childless parents and `->size()` counts it (probe: 4 vs correct 3);
the assoc arm's INNER is the correct emission and matches engine expected
values in all downstream-op cases including counts. (3) The bookmarked
"pass-1 scope mismatch" is misdiagnosed in the commit message: pass 1 works;
the bug is PASS-2 VARIABLE CAPTURE — the nav condition's emitted binders are
literally `{s,t|…}` and the corpus's canonical projection param is `t`, so
`free.remove(tgtParam)` deletes the USER's outer variable from the free set
and `$t.date` lowers as a target-row column read (`t1.date`). Renaming the
outer param makes the same query produce the engine-identical ON clause and
correct rows, and the assoc route has the identical latent capture for outer
vars named `srcRow`/`tgtRow`. Everything else mandated checks out: the
pred-application census is compose-or-loud (modulo hole 1), pred-internal
lambda params cannot leak (shadow-stops verified by probe), the class-result
beta-fold's double-splice decorrelation lands loud on today's vocabulary
walls, bare-lambda param types resolve through NameResolver's import scope
before `namedType`, and the Executor scalar second-row guard cannot break any
legitimate shape because multi-valued roots are COLLECTION/TABULAR by
construction.

## Findings

### F1 — HIGH (live, silent wrong rows): nav-route correlation drops when the nav step is below a join slot

`StoreResolver.augmentNavPredicates` (line ~479) recurses only through
`TypedNavigate.source()` and `TypedFilter.source()`. `Pipelines.navSteps`
(line ~77) collects through ALL children. A class pipeline's step order
follows mapping PM declaration order, so a scalar-through-join PM declared
AFTER the class-typed Join PM leaves the `TypedNavigate` below a
`TypedJoinSlot` — unreachable by the augment walk. The demand scan still
demands it; `materializeRoot` materializes it with the UNAUGMENTED condition;
and the lifted-pred apply site (line ~1161) deliberately skips correlated
preds (`correlatedPred(liftedHead) == null` check) on the assumption the
augment composed them. No guard fires.

Probe (Corr4, identical query, param already renamed to avoid F2):

- PM order `ref` before `events`: `ON t0.ID = t1.TID AND t1.EDATE = t0.TDATE` (correct)
- PM order `events` before `ref`: `ON t0.ID = t1.TID` — **correlated conjunct silently gone**, rows explode.

Fix direction: make the augment walk the same node set `navSteps` walks, and
add the missing invariant — after materializeRoot, any demanded nav alias
whose head still carries an unconsumed `corrPred` must throw (or make
`SyntheticHeads.applyToPipe` loud on unconsumed corrPreds; today it returns
the pipe unchanged for corr-only heads, which is what makes this drop silent).

### F2 — HIGH (loud, the bookmarked bug, now precisely diagnosed): pass-2 variable capture, not a pass-1 mismatch

21333d32's bookmark ("the pred's own-param read is not yet rewriting through
the TARGET bindings — a pass-1 scope mismatch") misattributes the failure.
Ledger evidence and probe agree: the failing ON clause is
`t1.eventDate = t1.date` — the LHS proves pass 1 DID rewrite the pred's own
param through the target bindings. The actual bug, in
`AssociationJoins.andCorrelatedIntoCondition` (line ~360):

- Nav-route conditions are emitted `{s,t|…}` (JoinChainEmission line ~255:
  `Variable("s")`, `Variable("t")`). The corpus's qualifier bodies inline
  `$this` to the projection lambda's param, which the corpus spells `t`
  (`project([t | $t.tradeDateEventTypeInlined])` → corr pred
  `{e | $e.date == $t.date}`).
- Pass 2 computes `free = collectVarNames(body) − {tgtParam, srcParam}` =
  `− {"t","s"}` — the USER's outer var `t` is deleted by name collision,
  never substituted through the parent bindings, and lowers as a raw column
  read on the condition's target row: `t1.date`.

Probes (CorrProbe): A (outer param `t`) reproduces the exact binder error;
B (outer param `x`) produces the engine-identical
`ON t0.ID = t1.TID AND t1.EDATE = t0.TDATE` and correct rows; C2 shows the
ASSOC route has the same latent capture when the outer var is named
`tgtRow`/`srcRow` (MappingNormalizer line ~963 binders).

Correct scope construction (the mandate's question): (a) alpha-freshen BOTH
condition binders collision-driven before composing — the exact discipline
`Substitution.rewriteExists`/`filteredNavLeafRead` already implement with
`tRenamed` (Substitution ~1334-1347, audit-18 comment: "λ(s,t) everywhere —
an inner 't' would SHADOW the enclosing scope's correlation var");
(b) compute the free set from the ORIGINAL pred (reads minus pred params,
shadow-aware) BEFORE pass 1 — after pass 1, introduced tgtParam reads and
user vars named `t` are indistinguishable, so no post-hoc collection can ever
be correct. Note `Substitution` line ~795 would already have thrown loudly on
a nested binder colliding with the unfreshened var ("fresh-var selection must
avoid user names") — andCorrelatedIntoCondition violates that invariant by
construction.

### F3 — HIGH (latent wrong rows/values, corpus-masked): flattenNavSlot emits LEFT with no null-skip

Mandate 1a answer first: the assoc arm's INNER emission IS row-identical to
the engine's LEFT + reader-skip in all cases including row-counting
downstream ops — pure semantics says childless parents contribute nothing,
`size`/`at`/`slice`/implicit-serialize all operate on the post-skip sequence,
and probes confirm (assoc model: `Trade.all().events` → 3 objects, no
phantom; `->size()` → 3).

But the NAVIGATE-SLOT arm (`flattenNavSlot`, 5b6c0ca5) does not emit INNER:
it rides `Pipelines.materialize`, which emits LEFT OUTER, and lite has no
reader-skip. Probe (Corr2, Join-PM mapping, trade 3 has no events):

- `Trade.all().events` → 4 objects incl. `{"edate":null,"etype":null}` — a
  phantom instance the engine's reader would skip. WRONG ROWS.
- `Trade.all().events->size()` → `COUNT(*)` over the LEFT join = **4** (true
  answer 3). WRONG VALUE.
- `->at(1)` windows over rows including the phantom — element-shift hazard.

The corpus sweep missed this because its fixtures give flattened parents full
child coverage and/or apply null-rejecting filters. Fix: the nav-slot flatten
must emit the same INNER the assoc arm emits (or filter null-PK rows), and a
pin with a childless parent belongs in ResolveNavigationTest.

### F4 — MED: predClosedOverParam is scope-blind (same capture family as F2)

`SyntheticHeads.predClosedOverParam` (line ~514) collects ALL nested lambda
param names globally into `bound`. An outer variable that happens to share a
name with ANY lambda param anywhere in the pred body makes a CORRELATED pred
look CLOSED → parked in `preds` → applied inside the target pipeline where
the outer row does not exist; the outer read passes through `Substitution`
verbatim (`case TypedVariable v -> v`) and binds to whatever that name means
in the emitted context. Silent mis-placement is possible. The closed-ness
test must be shadow-aware (a name is bound only within its binder's subtree).

### F5 — MED: flatten binding re-point manufactures phantom columns / misattributed eager throws

- Slot-backed target binding under the flatten: `prefixBinding` →
  `Pipelines.prefixColumns` rewrites `$r.SLOT.COL` to the phantom flat column
  `prefix_SLOT` + hop — silent at re-point; probe J only goes loud downstream
  ("serialize leaf 'venue' references column 'events_EV_VENUE', unresolvable
  in the envelope source") — loud by accident of the envelope check, message
  misattributed. `prefixColumns` lacks the prefixes/stripped loud guards its
  sibling `rewriteRowReads` has; the flatten also materializes the target
  with ZERO slot demand (`Set.of()`), so slot-backed leaves can never
  resolve. Wants a demand pass or a NAMED wall at flatten time (mandate 1c).
- OTHERWISE bindings (mandate 1b): `prefixBinding` unwraps only
  `toOne(ctor)`; an `otherwise(^Inner(...), $row.slot)` composition falls to
  `prefixColumns`, whose default arm throws
  "resolver bug: association-leaf rewrite hit TypedNewInstance" — LOUD (no
  silent mis-prefix), but misattributed and EAGER: it fires for every
  binding of the target class even when the otherwise property is never read,
  walling whole flattens unnecessarily. Same for ctors under `TypedIf`.

### F6 — LOW (guard wanted): class-result beta-fold double-splice is loud only by accident

`substituteParam` rides `UserCallInliner`'s let reduction, which splices the
value at EVERY read site (documented multi-eval trade for user lets). For a
MAPPER PARAM this is not double evaluation but DECORRELATION — a second
`Trade.all()` extent replaces the row-correlated read. Probes D/H
(`map(t|$t.events->filter(e|$e.edate == $t.tdate))`) land loud today
("TypedGetAll is not substitutable"), i.e. the H2 vocabulary walls catch the
second splice — protection by accident, not by design. A named guard (mapper
reads its param more than once → NotImplementedException) would make the
invariant explicit before some future vocabulary expansion makes a silent
shape reachable (mandate 1d).

### F7 — VERIFIED GOOD: pred-application census (mandate 2c)

Compose-or-loud at every site except the F1 hole: AssociationJoins:79
(aggregated route — loud guard), :328 (assoc condition — composes),
StoreResolver:1297 (exists-navigation — loud), :1699 (ExistsSub navCond —
composes), :1905 (navigate-step chain — loud), :1814 (chained mid-hop with
pred — loud), :1161 (skip-when-correlated — the F1 dependency). Guard count
in code is three `requireNoCorrelatedPred` sites plus the inline aggregated
throw, matching the four routes 2402aece names.

### F8 — VERIFIED GOOD (mandate 2b): pred-internal lambda params cannot leak

`collectVarNames` DOES put nested lambda params (e.g. `k` in
`exists(k|$k==1)`) into the free set, but each pass-2 substitution is a no-op
inside the binding lambda (Substitution's shadow-stop, line ~800), and a
binder colliding with the substitution's row var throws the line-795 loud
guard. Probe F: the mixed pred composes correctly into the ON clause. A free
let-bound scalar var would hit the bare-variable loud wall, not a silent
rewrite.

### F9 — VERIFIED GOOD (mandate 3): bare-lambda literal typing

Annotated param types reach `Typer.namedType` AFTER `NameResolver`
FQN-resolution through the file's import scope (`resolveLambda` →
`resolveVariableList` → `resolveType`), so corpus-spelled simple names
resolve correctly; `namedType`'s fallbacks are fixed prelude packages and
unknown names throw. The arm fires only in non-call positions (previously
unconditionally loud), so it cannot override call-signature typing; a wrong
annotation surfaces at unification, not silently. Multi-statement thunks bind
leading lets forward; non-let intermediates stay loud. No silent-acceptance
path found beyond NameResolver's pre-existing ambiguity policy (out of this
range).

### F10 — VERIFIED GOOD (mandate 4): Executor scalar second-row guard

`ResultShape.of` classifies from the ROOT's declared multiplicity — every
legitimately multi-valued root is COLLECTION/TABULAR/GRAPH by construction,
so no corpus shape can rightly return a second row under SCALAR. The toOne
pass-through stand-in's multi-row leak is precisely what the guard closes:
before 8bdd9ecb the SCALAR arm read row 1 and never checked, so 4859a84f's
"a multi-row source surfaces at the value compare, never silently" was only
true when row 1 happened to differ from the expected value. Zero sweep deltas
because the corpus's resolved toOne sources are genuinely single-row
(key-filtered eager lets); the guard turns any future leak from
possibly-passing into loud. No legitimate shape is broken.

## Probe inventory

- `CorrProbe.java` — A/B capture repro + control; C assoc-route end-to-end
  (first real verification of andCorrelatedIntoCondition: engine-identical ON,
  correct rows); C2 assoc-route latent capture; F shadow-stop.
- `Corr2.java`/`Corr3.java` — nav-slot vs assoc flatten arms: LEFT-phantom
  vs INNER (F3); size/at probes; double-splice loudness (F6).
- `Corr4.java` — PM-order silent correlation drop (F1).
- `Corr5.java` — slot-backed binding under the flatten (F5).
