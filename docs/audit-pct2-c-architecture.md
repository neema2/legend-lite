# Audit PCT2-c — Architecture coherence, range `64311065..HEAD` (pct-native)

Range: PURE_ORDERED caller-declared order contract (fa9da3b2), fromJson
canonicalization + the Scalars guardrail split (00abdcb7), pct.only +
plain-sort null-order ledger entry (84eae148), pure temporal spelling in
TDS cells 991→1022 (b8712c76), DATE lattice parse-back 1022→1029
(4a6de11c), the red-gate heal + log lesson (45bf4f9d), average-is-Float
1029→1031 (8bd4aa98). Plus a whole-branch structural re-check. Judged
against AGENTS.md, the ENGINEERING_LOG standing tenets, and
docs/audit-pct-c-architecture.md as the findings baseline. Note on the
baseline: the audit-fix wave itself is commit 64311065 — the BASE of this
range — so several prior findings were closed at the range boundary; the
ledger below reports status at HEAD and says where each fix landed.

## Verdict

The range is behaviorally excellent — every one of the 49 new passes came
from a root-cause fix at the right layer (a typed MIR variant + dialect
arm for fromJson, not a comparator band-aid; a STRICTER order contract
that held all 982 existing passes; kind-recovery in the one component
that owns the wire) and the one process failure (a red gate pushed) was
healed in the next commit AND distilled into a standing log lesson. But
structurally the range is living on margin the prior audit already
called: `checkAssert` is now **244/250** — six lines of headroom — with
14 SHAPE-bucket tests demanding the next arms (MED, prior §5 now
critical-path); the OrderPolicy enum is the RIGHT contract at the RIGHT
owner but was delivered as a 13th positional parameter perl-threaded
through 19 sites instead of the eval-context record this signature has
needed for two audits (MED); the TemporalEmission split is an honest
verbatim helper extraction that nevertheless re-homed the
SQL-function-names-in-lowering smell under a javadoc that now names
DuckDB approvingly in the Lowerer layer, without flagging the debt
(MED); StatementExecutor's presentation native grew again (+19 lines of
temporal cell arms) with the K-dispatch split still uncut and the 2-arg
`toString(rel, typesAndMuls)` STILL registered with no execution owner in
any layer (MED, prior §1 worse by mass); and PctCorpusRunner moved its
bucketing policy while rcorpus's copy stayed put — the two hand-kept
scoring policies diverged further, the §6 kit still unbuilt (MED). No
finding has silent-wrong-answer polarity. Counts: **0 HIGH, 5 MED, 5
LOW.**

---

## Prior-audit ledger (audit-pct-c: 6 MED, 4 LOW)

| # | Finding | Status at HEAD |
|---|---|---|
| M1 | StatementExecutor 4th-K-native accretion; ownerless 2-arg toString | **WORSE** — +19 lines into `tdsCell`/`pureDateTime` (:272–306), no `PresentationNatives` split; 2-arg overload still registered (Pure.java:1233) and still guarded out (`args.size() == 1`, :159) with no owner anywhere |
| M2 | relationToString fidelity (header quoting, Variant arm) | **FIXED** at 64311065 — `tdsHeaderName` (:263) ports the trim/quote rule, `tdsCell` keys Variant arms on DECLARED column type; the range's temporal arms extend fidelity (grounded, PCT-pinned) |
| M3 | Silent supplant on textual signatureKey | **HALF-FIXED** at 64311065 — stderr-once diagnostic added (FunctionCompiler:71); the textual-key identity remains as-is |
| M4 | assertError misclassification + dropped positional pins | **FIXED** at 64311065 — `LegendCompileException` rethrown (TestBody:702–708), 4-arg non-empty pins → SHAPE (:686–690), verified in code |
| M5 | checkAssert 226/250 | **WORSE** — now **244/250** (TestBody:513–756); see §1 |
| M6 | PctCorpusRunner ↔ rcorpus duplicated scoring policy | **WORSE** (mildly) — see §5 |
| L7 | Typer leading-lets loop duplicated | **UNCHANGED** (Typer:169, :795 — both copies live) |
| L8 | Parser consume-and-drop edges (skipMeasure EOF) | **UNCHANGED** |
| L9 | tdsEquivalent stricter-direction deltas | **PARTIALLY FIXED** at 64311065 (declared-type keying, real dateDiff SECONDS truncation, epochSeconds UTC pin); Number-through-double and no-Variant-arm remain |
| L10 | Nits (REPO_ROOT cwd, plan-doc substring claim, Pure.java ordering, guardrail scope) | plan-doc **FIXED** at 64311065; REPO_ROOT, Pure.java:1232 ordering, core-main-only guardrail scan **UNCHANGED** |

---

## 1. checkAssert at 244/250, with 14 tests already queued behind the missing arms (MED)

`core/src/main/java/com/legend/harness/TestBody.java` :513–756 = **244
lines** against the 250-line method guardrail (CodeShapeGuardrailTest,
`METHOD_ALLOWLIST` empty). The prior audit measured 226 and said
"extract the per-assert bodies BEFORE the next arm forces it"; this
range instead spent 18 of the 24 remaining lines on the (necessary)
OrderPolicy parameter threading — line-wrap inflation, zero new
behavior in the method. Meanwhile the generated scoreboard
(docs/PCT_CORPUS.md :59–61) shows `assertError/4` ×6, `assertIs/2` ×6,
`assertInstanceOf/2` ×2 in the SHAPE buckets — the corpus is actively
demanding arms that cost 10–30 lines each. The next assert arm CANNOT
land without either breaching the guardrail or starting the extraction
mid-feature (the exact Scalars-at-the-ceiling shape this branch has now
hit twice). Prescription, unchanged from pct-c §5 and now blocking: the
switch stays as the dispatcher; each `case` body becomes a call into a
package-private `Asserts` (or per-family `AssertEquality` /
`AssertError` / `AssertJson`) taking the eval context — which is the
same record §2 wants. Do it as the NEXT slice, before assertIs lands.

## 2. OrderPolicy — right contract, right owner, wrong delivery vehicle (MED)

`TestBody.OrderPolicy` (:245), `ordered()` (:1881), threaded through
`run` → `checkAssert` → `eval`/`evalScalar` → `runPerDriverLoop` (19
`order, lets` sites).

The DESIGN is correct and the audit affirms it against the alternative:

- The enum is a caller-declared semantic contract, exactly where it
  belongs — the harness owns compare policy (design-doc rule), and the
  two callers genuinely have two semantics (rcorpus expectations encode
  H2's incidental order; PCT expectations share pure's ordered-list
  semantics with the execution). Deriving the policy inside TestBody
  (e.g. sniffing the corpus) would have been a fallback; a boolean would
  have lost the names. The commit's honesty evidence is real: all 982
  passes HELD under the stricter unconditional-ordered compare, so the
  change certifies MORE while forgiving nothing new.
- The threading is COMPLETE and compile-enforced — the signature change
  forced all 19 sites; all three `new Eval(...)` constructions route
  through `ordered(order, orderView(...))` (:1094, :1113, :1135); the
  joinStrings multiset guard (:1131) and CSVJOIN path are policy-off
  under PURE_ORDERED exactly as the commit claims; `runPerDriverLoop`
  forwards to `checkAssert` and evaluates nothing directly (:804);
  bare `endsInSort` survives only inside `ordered()` and its own
  recursion. No inconsistency found. `assertSameElements` staying
  unordered and `tdsEquivalent` staying always-ordered are both correct
  (each is that assert's own reference semantics, not policy).

Two debts in the vehicle:

- **The 13th positional parameter.** `checkAssert` now takes 12
  parameters, `eval` 10, `runPerDriverLoop` 13. `order` is the second
  cross-cutting policy bit threaded positionally (after
  `emptinessUnverifiable`), and every future one repeats this 19-site
  mechanical edit. The seam is an `EvalCtx` record (order, lets,
  execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn) —
  one parameter, and the §1 assert-body extraction gets its argument
  type for free. Cut both in the same slice.
- **`Eval.sortedChain` now lies** (:1001). Under PURE_ORDERED the field
  stores "order is a contract," not "the chain ends in sort" — a reader
  of `compare`'s `ordered && actual.sortedChain()` (:1207) will
  mis-derive the semantics. Rename to `orderContract` (LOW-severity
  content, listed here because the rename should ride the EvalCtx
  change).

Also noted: the 7-arg `run` overload defaults SQL_ORDER_POLICY, so the
rcorpus caller never actually DECLARES its contract (Runner.java:593
compiles untouched — "behavior-identical by construction," which is how
the corpus gate stayed green). Acceptable migration mechanics; when the
runner kit (§5) is built, make both callers explicit and delete the
defaulting overload — a defaulted contract is half a contract.

## 3. TemporalEmission — honest verbatim split; the layer smell moved house and got a friendlier nameplate (MED)

`core/src/main/java/com/legend/lowering/TemporalEmission.java` (141
lines), Scalars 3498 → 3387 (ceiling 3500 — the split happened at TWO
lines of headroom, again mid-feature; audit-20c §7 called this pattern,
pct-c §5 called it again).

What is right: the extraction is genuinely VERBATIM (javadoc says so;
diff confirms — zero behavior change inside a commit that also changed
behavior elsewhere, cleanly separable); the five helpers (`intervalFn`,
`diffPart`, `dateArg`, `dateDiffExpr` + `elapsed`/`sundayIndex`/
`backOneDay`) are one cohesive family; the NullSemantics precedent cited
is the honest analogy — helpers extracted, RULES stay in the table.

Three qualified debts:

- **The extraction did NOT note the smell it moved.** `intervalFn`
  returns DuckDB function NAMES (`"to_years"`…) and `diffPart` SQL part
  names, which lowering wraps in `SqlExpr.StringLit` args that the
  renderer then CASTS BACK and splices as raw SQL function names
  (`AnsiSqlRenderer` ADD_INTERVAL :438–440, TIME_BUCKET :448–450 —
  `((SqlExpr.StringLit) a.get(0)).value()` emitted as the callee). That
  satisfies MIR rule 3a by the letter (StringLit is a data literal) and
  violates it in spirit — a String field encoding a SQL operation,
  smuggled through a literal node. Pre-existing, not introduced here;
  but the extraction was the moment to flag it, and instead the new
  javadoc (:25, "The DuckDB to_* interval constructor") NAMES the
  execution dialect in the Lowerer layer as if it were the design.
  Prescription: give ADD_INTERVAL/TIME_BUCKET a typed unit — an
  `SqlExpr.IntervalAdd(DurationUnit unit, SqlExpr n, SqlExpr d)` record
  (Pure enum name in MIR, per the Cast carve-out philosophy) with the
  `to_*` mapping in the dialect where it belongs. `diffPart`'s outputs
  are closer to the EXTRACT-part StringLits already accepted
  ("quarter", "month") — same treatment when the variant is cut.
- **`Scalars.enumName` widened private → package-private** (:3331) so
  the extracted class calls BACK into its host. That is a bidirectional
  coupling: Scalars → TemporalEmission (16 call sites) and
  TemporalEmission → Scalars (4 call sites). `enumName` is a generic
  typed-HIR reader used by many non-temporal rules, so it could not
  move — which is the tell that its home is neither class. A tiny
  `TypedReads` (or a static on the typed node itself) breaks the cycle.
- **Helpers moved; the family didn't.** The temporal RULE registrations
  (dateDiff, adjust, timeBucket, the EXTRACT family, the is*Day
  sextet, precision predicates) — several hundred lines — remain in
  Scalars, which sits at 3387/3500 and this range added ~14 lines to it
  (average + fromJson). The next grounded family lands at the ceiling
  a third time. The registration-family seam (audit-20c: a
  `TemporalRules.register(RULES)` alongside the emission class) is the
  real split; do it before the next temporal feature, not during.

## 4. StatementExecutor — the presentation native keeps accreting; the ownerless 2-arg toString persists (MED, carried)

`core/src/main/java/com/legend/StatementExecutor.java` (984 lines; 965
at range base, 943 at the prior audit).

The range's addition is small and well-grounded — `tdsCell` temporal
arms + `pureDateTime` (:286–306) print pure's DateTime form instead of
the JDBC carrier's, unlocking 31 over.pure/asOfJoin grids, with the
UTC-pin dependency documented. And the base commit genuinely improved
this component (header rule, Variant arms — prior M2 closed). But the
structural findings both stand and both aged:

- The K-native family in the statement loop (toSQLString,
  execute-in-result-position, relationToString, executeInDb/DDL/print
  arms) is unchanged in shape; the `PresentationNatives`/`KDispatch`
  split promised in the 64311065 commit message ("pct-c's structural
  findings queued: K-native split") did not happen while the component
  grew a third consecutive range. `relationToString` + its three
  helpers are now ~90 self-contained lines with zero coupling to the
  frame machinery — the extraction is a pure file move. Do it before
  the fifth K-native.
- **The 2-arg `toString(rel, typesAndMuls)` is still a registered
  catalog native with no execution owner in any layer** (Pure.java:1233
  registers; StatementExecutor:159 guards it out with
  `args.size() == 1`; the base commit's H2 fix excludes the FQN from
  the scalar name family, so it now falls to the named missing-binding
  throw — loud, but misattributed as a lowering gap). Two audits have
  named this. Either dispatch it (~10 lines: header cells gain
  `:Type[mult]` suffixes) or unregister it; a signature that
  type-checks and can never execute is a standing misattribution
  channel.

Positive note within the same component: the temporal spelling lives
ONLY here — grep confirms no second `#TDS` renderer, and the
DuckDBIntegrationTest pin was moved to the KIND (Timestamp), not the
string, in the same heal commit. One owner held.

## 5. PctCorpusRunner — the duplicated scoring policy diverged further; the §6 kit is still unbuilt (MED, carried)

`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java`
(462 lines; 445 at base) vs `engine/src/test/java/com/gs/legend/rcorpus/
Runner.java`.

The range's runner changes are individually sound: `pct.only` (:132–147)
is a clean stateless debug instrument (full detail for one test, capped
buckets for the sweep — the right split of concerns); moving `bucket()`
from classification-time (:220–240 now stores RAW detail) to
ledger-render-time (:389, :401) is what makes pct.only possible and is
arguably the better factoring. And `PURE_ORDERED` is declared at the
runner with the justification in a comment (:181–184) — the contract
reads exactly as the commit intends.

But every one of those edits touched the HALF of a two-copy policy that
pct-c §6 flagged: `classify` vs `Runner.score` still carry the drifted
strings ("sql-only: N advisory assert(s)" vs "…advisory golden-SQL
assert(s), no row verification"; PASS detail `""` vs `"N assert(s)"`),
and now also differ on WHERE bucketing/escaping happens (pct: render
time; rcorpus: `.replace("\n", " | ")` at the ERROR catch). Each
divergence is small; the trajectory is the audit-17 stereotype-switch
story in slow motion. The seam spec in pct-c §6 (shared kit owning the
Status enum + one `score()` with its strings, `importScopeOf`, the
profile-match helper, the UTC connection factory, bucket/cap/escape
primitives) is unchanged and correct — and it is a BLOCKER for the
engine-deletion move already on the queue, so it is not optional work.
Build the kit when rcorpus moves core-only; until then, stop editing the
shared-in-concept pieces on one side only.

## 6. Executor.latticeKind — the DATE arm follows the NUMBER precedent; the discriminator is still a grammar convention, not a type (LOW)

`core/src/main/java/com/legend/exec/Executor.java` :106–150.

The new arm (:126–136) parses DATE-typed String cells back to kinds by
print-form shape (`+0000` strip; length-10-dash-at-4 → LocalDate;
`T`-at-10 → Timestamp). Assessed acceptable, three reasons: (a) it is
exactly symmetric with the established NUMBER identity channel and the
method's own doctrine ("self-describing wire encodings ONLY" — the
kind travels FROM SQL, never guessed from values); (b) every guard
failure is loud — a malformed near-miss throws from `LocalDate.parse`,
and the `return v` residual is a non-conversion whose polarity is
false-FAIL at the comparator, never a guessed kind (partial-date Years
correctly stay strings on BOTH sides of the compare — the branch is not
a fallback in the tenet-4 sense); (c) the fix is at the right layer —
the alternative was harness bridging, which is the cardinal sin. The
standing weakness is that the discriminator is a print-form GRAMMAR
spelled twice (the SQL that emits it, the Java that parses it) with no
shared definition; a typed wire discriminator (a kind tag column or a
plan-carried per-column lattice marker) remains the stronger design.
`latticeKind` is now ~45 lines of per-type sniffing — at the third
lattice family (NUMBER, DATE, plus the midnight-Timestamp narrowing),
cut a `WireKinds` seam and pin the grammar with a unit test that
round-trips every print form the lowering can emit.

## 7. The average guard — legal dispatch, but it predicts another rule's output (LOW)

`core/src/main/java/com/legend/lowering/Scalars.java` :1069–1082.

`isToOne(n.args().get(0)) && !(… instanceof TypedCollection)` is NOT
the forbidden type-dispatch: `isToOne` reads TypeInfo multiplicity
(explicitly allowed) and `TypedCollection` is a structural node KIND
(explicitly allowed — it distinguishes a collection literal from a
scalar expression, not an Integer from a Float). The rule's semantics
(`average` ALWAYS Float; cast the scalar identity; singleton collection
literals ride LIST_AVG) are correctly grounded and PCT-pinned. The
smell is subtler: the instanceof exists to predict whether
`args.get(0)` — an ALREADY-LOWERED SqlExpr sitting right there — is
array-valued. That knowledge is re-derived from HIR when it is directly
observable in the MIR operand; if collection lowering ever changes what
a singleton `[1]` becomes, this guard desyncs silently (polarity: a
CAST over an array → loud DuckDB error, so no wrong-value channel).
Prescription: dispatch on the lowered operand (`args.get(0) instanceof
SqlExpr.ArrayLit` / the list-producing call set), or better, have
`isToOne` and the collection question answered by ONE helper the other
list-vs-scalar rules (:129, :139, :177 use bare `isToOne`) share —
several of them have the same latent singleton-literal edge this fix
just patched for average alone.

## 8. DuckDb.normalize + compactJson — right seam, but canonical-compact now has two implementations and one is dialect-private (LOW)

`core/src/main/java/com/legend/sql/dialect/DuckDb.java` :23–55,
`SqlFn.JSON_PARSE` + `AnsiSqlRenderer` :444, `Executor.unwrap` :286.

Placement verdict: **dialect is the right owner.** `SqlDialect.normalize`
is the pre-existing, documented seam for JDBC wire quirks (SQLite dates
already ride it), the Executor calls it with a typed `SqlType`
discriminator (no sniffing), and whether JSON text comes back with
decorative whitespace IS a DuckDB wire fact. The fromJson half is the
textbook fix: a typed MIR variant (`JSON_PARSE`) with a dialect render
arm, replacing the text-preserving CAST — root cause, not a normalizer
band-aid, and the javadoc on the enum constant says exactly why.

Two qualified debts: (a) the canonical-compact POLICY (a pure-semantics
fact: "the Variant contract is the JSON text; pure prints compact") is
now implemented twice — SQL-side by `json()` for fromJson ingress,
Java-side by `compactJson` for every other JSON cell — which is
one-behavior-two-mechanisms; tolerable because both are anchored to the
same documented contract, but the comment in neither names the other.
(b) `compactJson` — a correct hand-rolled scanner (string/escape states
verified) — is `private` to DuckDb; the next execution dialect
re-implements it or diverges. Hoist it to a shared home (a static on
`AnsiSqlRenderer` or an exec-side util) and let dialects opt in.

## 9. Small findings (LOW)

- **Lowerer null-order comment duplicated** (:1290–1295, :1330–1335):
  the same 5-line engine-parity note pasted into both plain-sort loops —
  because the LOOPS themselves are duplicated. The comment doubling is
  the tell; one `plainSortKeys()` helper owns both loops and the note.
  (The DECISION documented is good: an honest, corpus-measured
  engine-vs-pure divergence pinned in the ledger instead of silently
  chased — tenet 3 applied to a semantics gap.)
- **AGENTS.md MIR doc drift**: rule 3a and the MIR-rules section still
  describe `Cast(expr, pureTypeName)` with a Pure-type STRING; the code
  is `record Cast(SqlExpr value, SqlType target)` (SqlExpr.java:173) —
  a typed enum, i.e. STRONGER than the documented carve-out. Update the
  doc; a reader auditing against it will misjudge the Cast sites.
- **Guardrail scope** (carried): CodeShapeGuardrailTest scans core
  `src/main/java` only; TestBody's growth is covered but
  PctCorpusRunner (462, test tree) and the future runner kit are not.
- **Process, for the record**: 4a6de11c was committed and pushed with a
  red engine gate (2729/1) because a gate-grep chained into `git commit`
  exits 0 on matching a FAILURE line. The heal (45bf4f9d) fixed the
  regression AND added the standing log lesson naming the mechanism —
  the correct response shape; noted here so the lesson's origin is in
  the audit trail too.

---

## Positive findings (for the record)

- **The order contract change is a pure strictness increase** verified
  the right way: 982/982 held under unconditional ordered compare
  before the range advanced — no pass was order-luck, and the commit
  says so with the sweep as evidence.
- **fromJson was fixed at the root**: new typed MIR variant + dialect
  arm + the old CAST's wrongness documented on the enum constant; the
  FAIL ledger's six fromJson entries died without any comparator or
  harness change. Zero evaluation compensation entered TestBody in this
  range (the only harness change was policy threading).
- **The base audit wave held the protocol**: both pct-b HIGHs closed
  with pinned probes, four of six MEDs closed or half-closed, and the
  commit message QUEUED the structural three rather than claiming them.
- **Kind-recovery stayed in the Executor** (the one component allowed
  to know wire conventions), with the value-consulting heuristics
  doctrine restated at the site; the engine test pin moved to the KIND
  in the same commit — no dual-convention window.
- **pct.only is orchestration-only**: stateless, test-tree, no new
  evaluation surface; full-detail-vs-bucketed split is the right
  reporting factoring.
- **The threading, though manual, is complete**: compile-enforced at
  all 19 sites, all three Eval constructions and both multiset
  leniencies verified policy-consistent; `runPerDriverLoop` forwards
  the contract correctly.
