# Audit PCT-c — Architecture coherence, range `dc6086d1..HEAD` (pct-native)

Range: A1 parse-wall burn (726fb66b), A1c `(?:?)` wildcard type slot
(1076b86a), A2 execution — 597/1199 (c1c2070a), A2b native-replaces-stub
(a77531b4), bare zero-arg multi-statement lambda typing (bbf4b246), A3
assert vocabulary + the `#TDS` presentation native — 754→968 (80be7659).
Judged against AGENTS.md, docs/TENETS.md, the ENGINEERING_LOG standing
tenets (one owner, no fallbacks, loud walls, exact FQN), and
docs/PCT_NATIVE_PLAN.md as the design contract; docs/audit-20c-architecture.md
is the severity/rigor reference. Real-checkout grounding was re-verified
against `~/legend/legend-pure` and `~/legend/legend-engine` for
assertError.pure, tdsEquivalent.pure, toString.pure and s.pure.

## Verdict

The range substantially honors the plan and the tenets: the pct-corpus
module is exactly what PCT_NATIVE_PLAN.md Mode A specified (core-only
dependency, test-tree-only code, checkouts read in place, assumeTrue-gated,
generated scoreboard with an on-disk denominator), the harness deleted its
`toString` peel in the SAME commit the platform native landed (no
dual-owner window — the B2 discipline held), the new Typer arm keeps the
loud no-type throw for everything it doesn't handle, and no new mutable
state appears anywhere. But the range also walked STRAIGHT INTO audit-20c's
standing prediction: `relationToString` is the fourth K-native stacked into
StatementExecutor — the "one feature away" of 20c §1 arrived and the
Doors-style split did not (MED); `checkAssert` now sits 24 lines under the
250-line method guardrail with the two next asserts already visible in the
SHAPE buckets — the Scalars-18-lines shape again (MED); the
native-replaces-stub merge is the right seam but supplants silently on a
spelling-sensitive textual key (MED); the `assertError` arm conflates
harness/compile failures with the asserted raise and silently drops the
positional pins while scoring fully verified (MED); the `#TDS` renderer
diverges from its cited toString.pure/s.pure grounding on header quoting
and the 2-arg overload (MED); and PctCorpusRunner re-implements five
rcorpus Runner idioms near-verbatim, with the classification-policy strings
already drifting (MED — the extraction seam for the engine-deletion move is
specified in §6). No finding has silent-wrong-answer polarity: every
divergence found fails loud or fails FALSE-FAIL. Counts: **0 HIGH, 6 MED,
4 LOW.**

---

## 1. relationToString in StatementExecutor — the fourth K-native; 20c §1's accretion prediction realized (MED)

`core/src/main/java/com/legend/StatementExecutor.java` (943 lines; 875 at
audit-20c): dispatch :152–164, `relationToString` :219–255, `tdsCell`
:257–264.

Audit-20c §1 found StatementExecutor conflating three roles (sequencing,
result frame, K-native dispatch) and said it was "one feature away from
needing the same Doors-style split the normalizer already got." This IS
that feature, and the split did not happen. The K-native family in the
statement loop is now: toSQLString, execute-in-result-position,
relationToString, plus the executeInDb/DDL/print/setUpDataSQLsV2 arms
below — a fifth member lands in the same switch unless a
`PresentationNatives`/`KDispatch` seam is cut first. The component itself
is fine (exact-FQN recognition via `PlatformTypes.RELATION_TO_STRING`,
same `letPrefix` + `resolve → executeTyped` tail as its siblings, loud
`NotImplementedException` on a non-tabular result); the container debt
just grew.

Two dispatch-shape observations, both loud but misattributed:

- **Result-position-only.** The native dispatches only when the toString
  call is the statement ROOT (after From/Let peeling). `$tds->toString()`
  as an assert argument works (TestBody's `evalSpliced` makes each read a
  statement root), but `'x' + $tds->toString()` or
  `$tds->toString()->length()` falls through to StoreResolver/Lowerer and
  dies as "lowering not yet implemented" — a lowering-vocabulary error for
  what is really an undispatch-able presentation native. This is the
  toSQLString precedent (same limitation, same layer), so it is
  consistent — but with toSQLString the argument was that SQL-text
  generation is intrinsically a statement-level surface; a String-typed
  toString composes into expressions in real pure, so the position
  restriction is a semantic gap here, not just a plumbing one. When it
  bites, the right home for the general form is the resolver/lowerer (a
  presentation lowering over the executed relation), not more
  result-position carve-outs.
- **Guard conjunction routes silently.** The `args.size() == 1 &&
  arg0 instanceof RelationType` conjunction means the registered 2-arg
  `toString(rel, typesAndMuls)` overload (Pure.java:1233) type-checks but
  has NO execution owner anywhere — it falls to the generic lowering
  error. A registered catalog native with no owner in any layer is a
  latent misattributed failure; either dispatch it (it is ~10 lines — the
  header gains `:Type[mult]` suffixes) or don't register it yet.

## 2. relationToString fidelity vs its cited grounding (MED)

The javadoc claims the grid is spelled "the way real pure's TestTDS
toString does" and that "cell spelling follows s.pure." Verified against
the real sources
(`legend-engine/.../core_functions_relation/relation/functions/toString.pure`,
`s.pure`):

- **Cell arm: faithful.** `tdsCell` (empty → `null`; a String containing
  `{` or `[` quoted with `\\`/`\'` escapes; else print) matches s.pure's
  String and null arms exactly.
- **Header arm: not implemented.** toString.pure trims each column name
  and QUOTES non-simple names (`isSimple = starts-with-quote or matches
  ^[a-zA-Z0-9_]+$`). `relationToString` appends `columns().get(c).name()`
  raw. A corpus test over a non-simple column name renders differently and
  FAILs (string-exact compare — loud, not silent-wrong), but the
  grounding claim overstates what was ported.
- **Variant arm absent.** s.pure quotes Variant cells (and spells a
  Variant null as `'null'` quoted); `tdsCell` has no Variant notion —
  variant-typed cells will take the String-or-toString path. The variant
  suite is mostly walled today, so this is future-FAIL noise, not a
  current wrong-pass.

Tenet 5 says features must be grounded in the real mechanism — the
mechanism WAS read (the pinned tests pass), but the port is partial and
the comment does not say so. Cite the skipped arms explicitly or port
them; a reader trusting "follows s.pure" will not re-check.

## 3. FunctionCompiler native-replaces-stub — right seam, silent supplant on a textual key (MED)

`core/src/main/java/com/legend/compiler/element/FunctionCompiler.java`
:43–66.

The placement is correct: `functionsAt` is documented as "THE native+user
overload merge — every question routes here," and the rule (a registered
native supplants a same-signature .pure declaration; the declaration is
the signature) is real pure's own, which is exactly what killed the
"ambiguous: 2 candidates tie" wall for the PCT reference bodies.
Different-signature user overloads still join the set — the
stringExtension `[0..1]` totals case is handled, not fallback-ed. Two
qualified debts:

- **The supplant is fully silent.** The adjacent platform-owned arm
  (:67–72) prints a stderr-once notice when it suppresses user
  definitions; the same-signature supplant — which likewise discards a
  user body — reports nothing, anywhere. For the meta:: namespace this is
  correct semantics, but the merge applies to EVERY fqn where a catalog
  native exists; a user body silently never-running is precisely the
  shape "loud beats silently-different" exists for. Route it through the
  same stderr-once channel (or the structured wall channel when it
  reaches this layer).
- **Identity is `signatureKey()` — a textual key.** The key concatenates
  `p.type().toString()` + multiplicity; it matches only when the user
  spelling resolves to the identical string (FQN leaves post-NameResolver,
  same type-parameter NAMES, same multiplicity spelling). A semantically
  identical signature spelled differently (renamed `<T>`, unresolved leaf
  on some path) silently re-enters the overload set and resurrects the
  ambiguity tie — loud, but the failure names overload resolution, not
  the spelling mismatch that caused it. Fine while the corpus spells
  signatures identically; fragile as a general rule. A resolved-signature
  comparison (or a note pinning the assumption) belongs here.

## 4. TestBody assertError — misclassified failures, silently dropped conjuncts (MED)

`core/src/main/java/com/legend/harness/TestBody.java` :639–669.

Verified against `assertError.pure`: message comparison is
`assertEquals` — EXACT — and the implementation matches (the plan doc's
"message substring verified" is stale; the code is right). Two real
problems:

- **Any RuntimeException counts as "the raised error."** The catch arm
  captures every `RuntimeException | SQLException` from evaluating the
  thunk — including G-phase type errors, NameResolver failures, and the
  arm's own structural limitation (thunk statements are evaluated
  one-by-one via `evalScalar`, so a multi-statement thunk's `let` scope
  is DISCARDED between statements; statement 2 reading statement 1's
  binding raises). All of these score as an assert FAIL ("expected X, got
  <compile error>") when they are ERROR/SHAPE-class facts about the
  platform, not about the asserted behavior. `NotImplementedException` is
  correctly re-thrown to stay SHAPE — the same courtesy should extend to
  the compiler's exception types (TypeInference/resolution errors), which
  are recognizable. No wrong-PASS is plausible (a compile-error message
  equaling a pinned pure runtime message), so this is ledger pollution,
  not corruption — but the FAIL ledger is the instrument, and 20b-style
  bucket analysis will mis-bin these.
- **The 4-arg form's line/column pins are silently unchecked** while the
  assert scores fully verified. Real assertError asserts line and column
  when present. The comment says "positional args are ADVISORY here"
  (pinned to P3 source spans) — but unlike golden-SQL advisories, this is
  not ACCOUNTED: nothing increments the advisory counter, and the
  runner's "sql-only" honesty gate never sees it. This is the audit-9
  mixed-assert principle (never silently skip a conjunct) at lower
  stakes. Minimum fix: count the positional sub-assert as advisory so the
  scoreboard carries the truth.

## 5. checkAssert at 226/250 — the guardrail-adjacent growth pattern, again (MED)

`TestBody.checkAssert` spans :485–710 = **226 lines** against the 250
method guardrail (CodeShapeGuardrailTest, empty allowlists — verified).
TestBody the file is 1,941/3,500 and StatementExecutor 943/3,500 — both
comfortable. But the A3 additions grew checkAssert by ~90 lines, the plan
names more vocabulary still to land (`assertContains`,
`assertInstanceOf` — both ALREADY appearing in the generated SHAPE
buckets, i.e. demanded by the corpus), and each arm costs 10–30 lines.
This is exactly audit-20c §7's Scalars.java finding (18 lines of headroom
spent mid-feature). The switch's arms are self-contained and share only
the eval helpers — extract the per-assert bodies (the tdsEquivalent
family already lives outside; assertError/assertJson can follow) BEFORE
the next arm forces it. Note the helpers stayed clean: `literalNumber`,
`tdsEquivalent`, `epochSeconds`, `cellMismatch` are each single-owner and
under 40 lines.

## 6. PctCorpusRunner ↔ rcorpus Runner — five re-implemented idioms, and the extraction seam for the engine-deletion move (MED)

`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java` (445
lines) vs `engine/src/test/java/com/gs/legend/rcorpus/Runner.java`
(1,104) + `RelationalCorpusRunner.java` (216).

What is genuinely SHARED (the important part, and it held): the entire
assert/eval/compare machinery is TestBody in core — the pct runner adds
ZERO evaluation or comparison logic; module assembly is
`Compiler.parseSources`/`buildModule`/`compileAllBodies`; discovery rides
the real `ElementParser`. The runner is orchestration + reporting only,
which is the correct division.

What is RE-IMPLEMENTED (enumerated precisely):

1. **`importScopeOf`** — VERBATIM copy (PctCorpusRunner:189–201 vs
   Runner:394–406; identical body, section imports + own-package
   wildcard). Even the javadoc cites "the rcorpus importScopeOf rule."
2. **Outcome classification** — `classify` (:203–223) vs `score`
   (Runner:610–630): the same four-way switch INCLUDING the policy
   strings, which have ALREADY drifted ("sql-only: N advisory assert(s)"
   vs "sql-only: N advisory golden-SQL assert(s), no row verification";
   PASS detail `""` vs `"N assert(s)"`). Two hand-kept copies of one
   scoring policy is the audit-17 stereotype-switch story replaying.
3. **Status enum** — `Outcome {PASS, FAIL, ERROR, SHAPE}` (:73) vs
   `Runner.Status` (:34), same semantics.
4. **Stereotype dual-accept matching** — `isPctTest` (:256–265, bare
   `PCT` or FQN profile + stereotype `test`) mirrors `testKindOf`
   (Runner:333–350, bare `test` or FQN + `Test`/exclusions). Simple-name
   profile acceptance is NOT an exact-FQN violation here: the parser does
   not import-resolve profile references, both runners document the
   dual-accept, and rcorpus is the standing precedent — but it is the
   same knowledge twice.
5. **Per-test environment** — fresh in-memory DuckDB + `SET
   TimeZone='UTC'` (:154–157 vs Runner:587–590), the ERROR-catch-and-
   bucket idiom, and the synthesized Runtime source
   (`pctcorpus_rt.pure` vs `rcorpus-runtime.pure`).

Scoreboard/bucket rendering is a re-implementation in CONCEPT (bucket →
cap → count-sorted table → per-test ledger) but not in code — the pct
version is richer (escaped multi-line buckets, discovery-mismatch
section, FAIL/ERROR ledgers) and rcorpus's `writeScoreboard` is
family-keyed; treat as convergent, not copied.

**The seam, for when rcorpus moves core-only (engine-deletion plan):** a
shared corpus-runner kit (core test-jar, or the future core-only corpus
module) should own exactly the POLICY and PLUMBING that already exists
twice: (a) the Status enum + the one `score(TestBody.Outcome)`
classification with its strings — this is the piece where drift is
already observable; (b) `importScopeOf`; (c) a
`profileMatches(bareName, fqn)` stereotype helper; (d) the test
connection factory (in-memory DuckDB, UTC pin); (e) bucket/cap/escape +
the scoreboard table/ledger primitives as composable pieces (headers and
sections stay per-corpus). What stays PER-corpus, correctly: suite/root
enumeration and denominators (PctCorpus vs Corpus), module-assembly
strategy (PCT: whole-suite single module + empty-runtime source; rcorpus:
family/file/cross-family pull + DDL/seed replay + setup universe — none
of which PCT needs), the adapter binding (`identityAdapter` +
single-param check), rcorpus's `expandHelperCalls`/mapping-ref discovery,
and each scoreboard's prose. Do NOT extract the discovery predicate
itself — `isPctTest` vs `testKindOf` differ in exclusion semantics and
should stay visibly distinct.

## 7. Typer — the leading-lets loop now has two owners (LOW)

`core/src/main/java/com/legend/compiler/spec/Typer.java` :162–191 (new
bare-lambda arm) vs :792–806 (call-position lambda path).

The new arm's comment says it applies "the SAME statement semantics the
call-position path applies" — and it does, by re-implementing the
identical 15-line loop (letFunction/CString destructure → synth → scope
extend → TypedLet) rather than sharing it. The divergence risk is real
but bounded: the call-position copy throws loudly on a non-let
intermediate statement while the new arm sets `statementsOk = false` and
falls to the (equally loud) no-type throw — same polarity, different
message. One `typeLeadingLets(body, scope)` helper owns both. The arm
itself is architecturally sound: extension of the documented 19d-B4
decision, exhaustive-switch position preserved, parameterized lambdas
still loud, no fallback.

## 8. Parser consume-and-drop family — honest walls-deferred, one silent edge (LOW)

`ElementParser` (Measure skip :445–460, class type-variable params
:391–401, `<T|m>` multiplicity params dropped :448–466 of the hunk,
Profile stereotype parity, tagged-value `+`-concat), `SpecParser`
(type-variable VALUES `^X(10)(...)` dropped, instance names dropped,
range sugar `[a:b:c]` → `range` call, octal/unicode escapes), `Lexer`
(negative-year dates, leading-dot floats), `TokenStreamCursor` (`(?:?)`
wildcard type slot).

Each drop is documented with the same contract — the element/value is
ABSENT and any downstream reference fails loud at name resolution or
type-check — which is the established pattern for unmodeled constructs
and consistent with "loud beats silently-different" (nothing here can
produce a wrong VALUE; it can only produce a loud miss). The grounded
additions are genuinely grounded: escapes cite and match
`StringEscape.UNESCAPE_PURE` (OctalUnescaper 3-digit rule included, with
pinned tests), range sugar cites the real m3 `sliceExpression` → `range`
emission, the wildcard column type cites `mayColumnType`. Watched edges:
(a) `skipMeasure` brace-counts to EOF with no loud check that depth
reached 0 — a truncated Measure silently swallows the rest of the file
(the scoreboard's discovery-mismatch section would surface it, but
indirectly); (b) type-variable VALUES are parsed-then-discarded — under
the Total Knowledge tenet this is knowledge the AST deliberately does not
retain, fine as a wall but worth a `DESIGN_DEBT` line so the next session
doesn't assume they flow.

Also in this family, a genuine latent-bug fix: `parseSingle`'s bare
`parser.error(...)` call was a NO-OP (error() returns the exception);
trailing tokens after an element slice were never actually rejected. Now
thrown, with the null-element (Measure) case also loud. Good catch,
correctly commented.

## 9. tdsEquivalent comparator — sound port, three stricter-direction deltas (LOW)

`TestBody.tdsEquivalent` :1583–1633, `epochSeconds` :1636–1649. Verified
against the real `tdsEquivalent.pure`: ordered column names (zip ≡
ordered-equal), row counts, both-empty-equal, `<= abs(delta)` numeric
tolerance — all match. Three knowing deltas, ALL of which can only
produce false-FAIL, never wrong-PASS: (a) temporal compare uses
fractional epoch seconds where real pure uses `dateDiff(SECONDS)`
(truncating) — a 0.9s delta under timeDelta 0 fails here, passes there;
(b) the Number arm compares through `doubleValue()`, losing BigDecimal
exactness beyond 2^53 — immaterial under tolerance semantics; (c) no
Variant arm (real pure string-compares variants) — variant cells fall to
`wireEquals`. It correctly does NOT apply the harness order policy
(always ordered) — matching the real function's zip semantics, and PCT
feeds it literal `#TDS` grids. `literalNumber` refusing computed
delta/timeDelta arguments (→ SHAPE, never a guessed tolerance) is the
no-fallback tenet applied exactly right. The comparator sits beside
`compare`/`gridEquals` as a per-assert policy rather than inside them —
consistent with the design doc's "compare policy stays in the harness,"
and it reuses `wireEquals` for the exact arm rather than growing a second
cell-equality.

## 10. Module/plan conformance nits (LOW)

- **PctCorpus.REPO_ROOT** = `Path.of("").toAbsolutePath().getParent()` —
  correct under surefire (cwd = module dir), wrong under any other cwd
  (repo root → REPO_ROOT becomes `~/legend`). The failure mode is a
  silent assumeTrue-skip (roots resolve to nonexistent siblings), which
  is the conventional gate — but the sibling-default is an assumption the
  rcorpus `Corpus` avoids by hardcoding an absolute default. A
  `-Dlegend.lite.root`-style explicit override or a marker-file walk
  would make it cwd-independent. (The sibling-relative default is
  otherwise an IMPROVEMENT over Corpus's `/Users/neema` hardcode.)
- **Plan-doc drift:** PCT_NATIVE_PLAN.md A3 says assertError verifies the
  message by SUBSTRING; the implementation (correctly, per
  assertError.pure) uses EQUALS. Update the plan, not the code.
- **Javadoc imprecision:** the runner claims "exactly two synthetic AST
  nodes (the let binding and the runtime source)" — the runtime is a
  SOURCE string through the ordinary parser, which is actually the
  stronger claim; say that.
- **Pure.java placement:** `TO_STRING__RELATION_1(_)` (:1232–1233) sits
  between `SIZE__RELATION_1` and `SIZE__T_MANY`, breaking the file's
  ordering convention.
- CodeShapeGuardrailTest scans `src/main/java` of core only — pct-corpus
  (like rcorpus) lives outside every shape guardrail. At 445 stateless
  lines it needs none today; worth remembering when the shared kit (§6)
  gets a home.

---

## Positive findings (for the record)

- **The one-owner migration discipline held again:** the harness's
  `toString` tail-strip died in the SAME commit the platform
  `relationToString` landed (TestBody:1054–1059 documents the ownership
  move; only `toCSV` still strips, and the comment says exactly why). No
  dual-owner window; grep confirms no other `#TDS` renderer.
- **No evaluation compensation entered the harness:** every A3 arm
  evaluates BOTH sides through `eval`/`evalScalar` → the one back-half
  sequence; the new comparators are policy over wire values only.
- **Exact-FQN identification** in the new K dispatch
  (`PlatformTypes.RELATION_TO_STRING`), catalog + `native-catalog.txt`
  updated together, and the platform-owned list growth is documented at
  both declaration sites.
- **Classification honesty end-to-end:** unknown asserts → SHAPE, never
  skipped; `assertNotEmpty`'s missing emptiness-guard is the CORRECT
  polarity (can only false-FAIL under failed seeds); the scoreboard's
  discovery-mismatch section keeps the textual census honest; the
  1204-textual vs 1199-discovered delta is explained, not hidden.
- **No new state anywhere:** PctCorpusRunner is entirely static/stateless
  records; the FunctionCompiler merge uses only locals; the Typer arm
  threads scope functionally.
- **Escape handling graduated loud→grounded correctly:** the stay-loud
  stance was retired only when a corpus file demanded it (char.pure's
  `\0`), with the commons-text octal 3-digit rule ported exactly and
  pinned by new tests.
- **A2's adapter binding is the B4 lesson applied:** one synthetic
  `letFunction` + the platform's existing UserCallInliner β-reduction —
  no second substitution walker was built for eval.
