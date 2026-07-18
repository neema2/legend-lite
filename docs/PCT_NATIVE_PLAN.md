---
description: PCT without boundary hacks — two modes, one platform. Mode A runs PCT
  fully through core (PCT-as-data, rcorpus-style); Mode B keeps the real-pure
  delegation seam but de-compensates ExecuteLegendLiteQuery to a thin bridge.
status: active
depends:
  - docs/PHASE_B2_RESULT_VALUE.md (typed Result surface — extended by P1)
  - docs/PCT_BURNDOWN.md (current instrument + ledgered debt this plan retires)
blocks:
  - engine/ module deletion (agreed: deletion waits for this work)
---

# PCT Native Plan — run PCT through core, hack-free

## Context

PCT (Pure Compatibility Tests) is the function-level parity instrument:
`<<PCT.test>>` functions co-located with the `<<PCT.function>>` natives they
exercise, living as `.pure` files in the real checkouts. Suites and (approx)
test counts:

| Suite | Repo | Root | ~tests |
|---|---|---|---|
| essential | legend-pure | `legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/` | 329 |
| grammar | legend-pure | `.../platform/pure/grammar/functions/` | 132 |
| relation | legend-engine | `.../legend-engine-pure-functions-relation-pure/src/main/resources/core_functions_relation/` | 93 |
| standard | legend-engine | `.../legend-engine-pure-functions-standard-pure/src/main/resources/core_functions_standard/` | 141 |
| unclassified | legend-engine | `.../legend-engine-pure-functions-unclassified-pure/src/main/resources/core_functions_unclassified/` | 96 |
| variant | legend-engine | `.../legend-engine-pure-functions-variant-pure/src/main/resources/core_functions_variant/` | ~40 |

Test contract: `testX<Z|y>(f:Function<{Function<{->Z[y]}>[1]->Z[y]}>[1]):Boolean[1]`;
body = ordinary `assert*` statements around `$f->eval(|expr)`; the adapter `f`
decides how the payload executes. The in-memory reference adapter is literally
`$f->eval();` (identity).

**Today's architecture** (`pct/` module, 1109/1109 green): the REAL legend-pure
interpreted runtime executes the entire test body — asserts, lets, closure
capture — and only the `$f->eval` payload is diverted through
`pct_adapter.pure` (serialize substituted lambda to grammar text) → native
`executeLegendLiteQuery` → `ExecuteLegendLiteQuery.java` (1055 lines) →
`QueryService.execute` → DuckDB → convert results back to interpreted-Pure
`CoreInstance`s.

**The problem**: `ExecuteLegendLiteQuery` performs ~22 distinct
compensations (inventoried below) that exist only because the platform lacks
capabilities it should have. Most of PCT's ledgered debt (values-only
verification, annotation-dropping wire, escape guessing, identity erasure,
column-precision loss) is a direct consequence of this seam. Per the standing
tenets, harness/bridge compensation for platform gaps is the cardinal sin.

**The goal**: two ways to run PCT, neither with compensation:

- **Mode A — full core.** A `PctCorpusRunner` (rcorpus-style, PCT-as-data)
  reads the suite `.pure` files IN PLACE from the checkouts, zero munging,
  and core runs the ENTIRE test function: parse → type → inline → lower →
  DuckDB. No upstream dependency at all.
- **Mode B — de-compensated delegation.** The current architecture — real
  interpreter runs the body + asserts, thin `pct_adapter.pure` shim ships the
  eval payload — but the Java bridge shrinks to a decision-free bijection
  under THE BRIDGE RULE (CoreBridge precedent: field-by-field, zero decisions,
  loud on unrepresentable).

Both modes ride the same platform capabilities (§P). Mode B's 1109/1109 suite
is the regression gate that proves each de-compensation slice; Mode A is the
end state that removes the upstream dependency entirely.

## Compensation inventory (what dies, and what kills it)

From `ExecuteLegendLiteQuery.java` + `pct_adapter.pure`. Grouped by the
platform capability whose absence forced them.

### Killed by P1 (typed result fidelity)
- C3 Map flatten to `[k1,v1,...]` + Pure-side `newMap()` rebuild (`wrapPctMap`)
- C4 List scalar unwrap + Pure-side `^List` rebuild (`wrapPctList`)
- C6 declared-generic-type handoff for class-typed collection elements
- C9/C20/C21 struct→instance reconstruction, `Any`-padded generic args
- C12/C13/C14 BigDecimal/Double → Decimal-vs-Float disambiguation in the bridge
- C15/C18 date re-coercions (Timestamp→StrictDate promotion, string→date re-parse)
- C16/C17 String → canonical `Type` / enum-instance identity resolution
- C8 + `formatAsTds`/`buildTypedHeader`: TDS synthesized as a STRING with
  data-driven `[1]`/`[0..1]` multiplicities and DATE/TIMESTAMP columns typed
  as String — the reason PCT verifies values only, never relation typing
- C1 GraphResult stringified; C2 null→`[]`; C22 `toString()` fallback

### Killed by P2 (real model compilation)
- regex `extractClassMetadata`/`extractEnumDefinitions` off the interpreter graph
- the hardcoded `PURE_MODEL` preamble with five verbatim helper-function copies

### Killed by P3 (error fidelity)
- `remapErrorMessage` (DuckDB shift-overflow → pure's "max bits allowed is 62";
  transport-prefix stripping)
- adapter-frame stack walking to fake source positions for `assertError`

### Killed by P4 (wire/parse fidelity)
- `reEscapeStringLiterals` (escape-convention guessing; ledgered corruption risk)
- `inlineFunctionLiterals` (serialized concrete function → lambda rewrite)

### Legitimately survives (not compensation)
- Pure-side closure-variable substitution (`substituteOpenVariables`) — inherent
  to serializing a lambda that captures outer values; it is the wire, not a fix.
- Mechanical CoreInstance construction from FULLY TYPED values — the
  interpreter needs CoreInstances; building one from a typed value is a
  bijection (the CoreBridge shape), not a decision.

### Permanently ledgered (no boundary can fix; Mode A territory)
- Instance identity through a value-serializing wire (`assertIs`, `eq` on
  instances, map-relationship identity trio) — reference identity cannot
  survive by-value serialization. Under Mode A these become named walls
  (SQL-lowered execution has no host object graph — tenet 1).
- `deactivate()` / expression reflection — legend-lite holds no expression
  tree at runtime. Named wall in both modes.

## P — shared platform capabilities

**P1. Typed result fidelity** (extends the B2 Result surface;
`com.legend.exec.ExecutionResult`): results carry full Pure types —
per-column DECLARED types and multiplicities from the compiled relation type
(never data-driven), enum values as typed enum references (fqn + name),
temporals as typed values with precision preserved, Decimal scale preserved,
Decimal vs Float decided by the compiled type IN CORE. Acceptance: the Mode B
bridge maps values type-directed with zero `instanceof`-driven guessing; the
relation-typing "values only" ledger entry closes.

**P2. Model from source, not regex**: the bridge (and Mode A runner) compiles
the ACTUAL PCT `.pure` files into a `ModelContext` (`Compiler.parseSources`/
`buildModule`, whole files verbatim, tolerant per-file). The native executes
the shipped expression against that context. Acceptance: `PURE_MODEL`
constant and both regex extractors deleted.

**P3. Error fidelity**: core raises pure-parity messages where pure defines
the behavior (e.g. the 62-bit shift guard becomes a core guard, not a DuckDB
error rewrite), and source spans survive the pipeline (known audit-2026-07
debt) so errors carry real positions. Acceptance: `remapErrorMessage` and the
stack-walk deleted; `assertError` line matches come from real spans
(column remains advisory — the executing frame is in the database).

**P4. Wire/parse fidelity**: core's lexer/parser accepts exactly what pure's
grammar printer emits — escapes, serialized concrete function definitions.
Acceptance: `reEscapeStringLiterals` and `inlineFunctionLiterals` deleted.

## Mode B — de-compensation ladder (slices, each gate-protocol'd)

The 1109/1109 suite must stay green through every slice. Each slice = platform
capability lands in core → bridge arm deleted → suite green → commit
documenting the delta. Order chosen so early slices are independent:

- **B-p2**: compile real sources; delete regex extraction + `PURE_MODEL`.
- **B-p4**: wire fidelity; delete the two input-munging passes.
- **B-p1a**: scalar fidelity (enum/date/decimal/Type identity) — delete
  C10–C19 decisions; bridge maps type-directed.
- **B-p1b**: collection/Map/List/class-instance fidelity — delete
  C3/C4/C6/C9/C20/C21 + `wrapPctList`/`wrapPctMap` in the shim.
- **B-p1c**: relation fidelity — typed columns end-to-end; delete
  `formatAsTds`/`buildTypedHeader` string synthesis; TDS crosses the wire as
  typed data (`stringToTDS` only as the final interpreted-side constructor,
  fed from typed columns, or replaced outright if the runtime allows direct
  TDS construction).
- **B-p3**: error fidelity; delete remapping + stack walking.
- **B-final**: what remains of `ExecuteLegendLiteQuery` is renamed
  `PctBridge`, documented under THE BRIDGE RULE, target < ~150 lines of
  mechanical mapping. `pct_adapter.pure` keeps only: substitute captured
  vars → print → native call → mechanical return.

## Mode A — full-core runner (phases)

Lives in a new **`pct-corpus/`** Maven module depending on `legend-lite-core`
only (no upstream artifacts — those stay quarantined in `pct/` until
retirement). Checkout roots via `-Dlegend.pure.root` / `-Dlegend.engine.root`
(rcorpus `Corpus.ENGINE_ROOT` pattern, `assumeTrue`-gated when absent).

- **A0 — census.** `PctCorpus` (roots, six suite scopes = dir + package),
  discovery via the REAL parser (`FunctionDefinition` with stereotype `test`
  on profile `meta::pure::test::pct::PCT`, exact-FQN), whole-file verbatim
  module assembly, PASS/FAIL/ERROR/SHAPE classification, generated
  `docs/PCT_CORPUS.md` scoreboard (writeScoreboard pattern; the denominator
  is ALL discovered PCT tests, not the official adapter's 1109). Zero
  platform changes; everything lands SHAPE/ERROR. Establishes reality.
- **A1 — parse the PCT surface.** `<Z|y>`-parameterized function definitions
  parse and compile (CONVERGES WITH task #50 `<T|m>` parse walls — unlocks
  37+ relational-corpus tests too). Measure: % files parsed, % tests
  discovered vs grep census.
- **A2 — adapter binding + eval β-reduction.** The runner constructs ONE AST
  node per test — `AppliedFunction(testFqn, [identityAdapterLambda])` — and
  the platform does the rest: `UserCallInliner` (G½) inlines the test call
  and β-reduces `eval` of a known lambda. Uses the single existing
  substitution engine (the B4 lesson: never a second walker). Grounded in
  real pure's `eval` semantics.
- **A3 — assert vocabulary.** Extend `com.legend.harness.TestBody.checkAssert`
  (already: assert/assertFalse/assertEquals/assertNotEquals/assertSameElements/
  assertEqWithinTolerance/assertSize/assertEmpty/assertJsonStringsEqual) with
  the PCT tail: `assertEq`, `assertTrue`, `assertNotEq`, `assertNotSize`,
  `assertNotEmpty`, `assertContains`/`assertNotContains`, `assertInstanceOf`,
  `assertTdsEquivalent`, `assertError` (thrown-error expected; message
  substring verified; line from real spans once P3 lands; column advisory).
  `assertIs`/reflection = named walls. Unknown assert = SHAPE, never skipped.
- **A4 — burn-down** by suite (grammar/essential first — mostly scalar
  expressions; then standard/unclassified; relation exercises `#TDS` literals
  core already types; variant last). Scoreboard baseline-diffed per the gate
  protocol; every delta classified.
- **A5 — retirement.** When Mode A coverage ≥ the 1109-equivalent set with
  zero compensation: delete `pct/` (module, upstream deps, bridge, shim) and
  fold the PCT gate onto `pct-corpus`. Engine deletion unblocks.

## Gates and coordination

- Every slice/phase runs the standing gate protocol (core → engine →
  relational corpus sweep → PCT) — Mode B slices must additionally keep PCT
  1109/1109; deltas classified per-commit.
- The relational-corpus session continues independently; **engine/ deletion
  is agreed to WAIT for this work** (Mode B touches the pct/ module the
  deletion plan wanted to port; this plan IS that port, done to the end
  state).
- `docs/PCT_CORPUS.md` (Mode A) is GENERATED — never hand-edited, same rule
  as RELATIONAL_CORPUS.md. `docs/PCT_BURNDOWN.md` stays as the historical
  Mode B ledger; its "ledgered debt" section burns down with the slices.

## Acceptance (end state)

- `grep -c 'C[0-9]' ExecuteLegendLiteQuery.java` — file no longer exists.
- Mode B: `PctBridge` mechanical-only; `pct_adapter.pure` shim with zero
  return-value coercion beyond typed construction.
- Mode A: PCT suites run `mvn -pl pct-corpus test` with only a DuckDB jar
  and the two checkouts; scoreboard green at ≥ the official-adapter-parity
  set; remaining non-passes are named walls or honest FAILs, ledgered.
- The four PCT wire-debt ledger entries (values-only typing, annotation drop,
  escape guessing, column precision) are closed or reduced to Mode-B-only
  notes.
