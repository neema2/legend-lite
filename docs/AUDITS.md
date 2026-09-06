# Audits — index

Audit rounds have run on `legend-lite` since 2026-07. Their output is a set of documents with
**separate lifecycles** — do not merge them into one queue; separate docs are what got the first
two executed.

> **Index refreshed 2026-08-06.** The 2026-08 round added five documents (below), and every
> corpus figure in this file predates the current ledger — **2,398 pass of 2,798** (2,298 of the
> 2,575 runnable). Take numbers from `docs/GATES.md` and `docs/RELATIONAL_CORPUS.md`, not from here.

### The 2026-09 round

| Doc | Covers |
|---|---|
| [`COMPILER_ARCHITECTURE_AUDIT_2026_09_06.md`](COMPILER_ARCHITECTURE_AUDIT_2026_09_06.md) | The compiler as **architecture** — layering, concept ownership, duplication, fail-loud discipline, overfitting, and burn-down sustainability. Eleven adversarial reviewers; documentation banned as evidence. Headline: **the scored path and the product path are different code** (`Compiler.executeWire` never touches `StatementExecutor`). Also: consolidation cadence collapsed 29% → 6% while marginal cost per corpus test rose ~15×; three implementations of the milestone window predicate give two answers; 28.3% of native signatures exactly match real Pure. §9 lists eight claims investigated and **dropped**, so they are not re-derived. |

### The 2026-08 round

| Doc | Covers |
|---|---|
| [`PARSER_IMPLEMENTATION_AUDIT_2026_08.md`](PARSER_IMPLEMENTATION_AUDIT_2026_08.md) | The parser: totality, span fidelity, grammar divergence, architecture, robustness. Found a 33% operator-precedence divergence invisible to every gate. |
| [`COMPILER_STAGE_AUDIT_2026_08.md`](COMPILER_STAGE_AUDIT_2026_08.md) | Stage 2 (PMCD → PureModel): name/import resolution, the oracle question, type inference, element compilation, diagnostics. **§7 records a decision the project must make** — legend-pure and legend-engine disagree on overload resolution. |
| [`GRAMMAR_COMPATIBILITY_2026_08.md`](GRAMMAR_COMPATIBILITY_2026_08.md) | Every `###` section: what it would take to be 100% compatible. |
| [`TEXT_SURGERY_AUDIT_2026_08.md`](TEXT_SURGERY_AUDIT_2026_08.md) | Every regex and string-manipulation site, censused exhaustively. |
| [`ARCHITECTURE_AUDIT_2026_08.md`](ARCHITECTURE_AUDIT_2026_08.md) | 12 feature areas: did we build the right design, or fix point tests with point solutions? |

### The 2026-08 type-system round

| Doc | Covers |
|---|---|
| [`TYPE_SYSTEM_AUDIT_2026_08.md`](TYPE_SYSTEM_AUDIT_2026_08.md) | The type system end to end — source text through every pipeline stage to decoded results. 105 defects, 14 root causes. Finding: the type system computes types rigorously and then does not enforce them. A `[1]` multiplicity is violated in 117 of 117 executable cells of an exhaustive mapping matrix, **including the matched diagonal**. Evidence base in [`type-audit-2026-08/`](type-audit-2026-08/); fixes in its `REMEDIATION.md`. |

Two results in that round bear on documents elsewhere in this index. First, the native catalog
diverges from real FINOS Legend in **183 of 721 signatures (25.4%)**, measured against 24,172
declarations from cloned upstream repositories — so `Pure.java`'s "VERBATIM … NO divergence
categories remain" header is false, and `NativeFunctionTest.catalogMatchesTheGoldenFile` cannot
detect it because it compares `Pure.all()` to a file generated from `Pure.all()`. Second, the suite
is green (4,278 tests) and structurally blind to the defects: of 1,671 end-to-end test methods,
exactly one relates a declared column type to the delivered Java carrier.

Also present and not previously indexed: [`PERFORMANCE_AUDIT.md`](PERFORMANCE_AUDIT.md),
[`PCT_AUDIT.md`](PCT_AUDIT.md), [`NAME_RESOLUTION_BUG.md`](NAME_RESOLUTION_BUG.md),
[`SIMPLE_NAME_AUDIT.md`](SIMPLE_NAME_AUDIT.md) (superseded), and the dated
`audit-20a/20b/20c`, `21a/21b`, `22a/22b` finding sets.

| Doc | Fixes | Status |
|---|---|---|
| [`ARCHITECTURE_REMEDIATION.md`](ARCHITECTURE_REMEDIATION.md) | **Shape** — phase boundaries, N-backend decoupling, concept ownership | T0–T3 executed (corpus 1,258 → 2,074). T4 pending |
| [`CORRECTNESS_REMEDIATION.md`](CORRECTNESS_REMEDIATION.md) | **Answers** — silent wrong rows, and the scoreboard's own honesty | Tier C0 (four diagnostics) is the entry point |
| [`TENET_REMEDIATION.md`](TENET_REMEDIATION.md) | **Who does the work** — tenet #1 conformance, 20 ranked violations | V0 (tenet text) first; it makes the rest adjudicable |
| [`AUDIT_PROGRAM.md`](AUDIT_PROGRAM.md) | **What to audit next** — exhaustive `null` / `try` / `if` sweeps | Plan, not findings. Audit N is a gate and starts now |
| [`CORPUS_TAXONOMY.md`](CORPUS_TAXONOMY.md) | **The burndown** — root causes behind the 406 non-passing corpus tests, not error messages | ⚠ **Its own banner disowns §8** as historical and supersedes it with `CORPUS_STUDY_2026_08.md`. Use `CORPUS_BURNDOWN_HANDOFF.md` as the burn-down entry point instead. |
| [`NULL_GATE_VERIFICATION.md`](NULL_GATE_VERIFICATION.md) | **Verification of delivered work** — did the null gate land correctly, and is it aggressive enough | Audit of `a814ffa9..aa5df4f7`. G0–G3 executed |
| [`STATE_AUDIT.md`](STATE_AUDIT.md) | **State discipline** — every void method, all mutable and ambient state, determinism | 306 voids / 476 records / 10 ThreadLocals enumerated. Start at S0 |
| [`H2_BACKEND.md`](H2_BACKEND.md) | **H2 as a real backend** — capability map (206/256 constructs), golden-text census, 13-step sequencing | ADOPTED as the loop plan 2026-07-31; verification addendum covers 2.4.240 |
| [`AUDIT_23_SPECIAL_CASING.md`](AUDIT_23_SPECIAL_CASING.md) | **Keyed special-casing** — every name/FQN/magic-string conditional across ~37k LOC, censused | Complete. Read before any `if` audit |
| [`AUDIT_2026_07.md`](AUDIT_2026_07.md) | Earlier round | Complete |

## Reading order for a new session

1. **`AUDIT_PROGRAM.md` §1–§2** — the method (why enumerations beat themes) and the sequencing
   criterion (*does this corrupt the instrument we steer with?*). Both generalize beyond their own doc.
2. **`CORRECTNESS_REMEDIATION.md` §1** — the meta-finding: every scoreboard bucket was mislabeled, and
   always flatteringly. Read before trusting any pass rate.
3. **`TENET_REMEDIATION.md` §1.1** — five invariant headers falsified, every one flattering. Read
   before trusting any `CONTRACT:` comment.
4. **`docs/GATES.md`** — before claiming anything is green. It names three ways this chain
   reports success without having checked anything.
5. **`CORPUS_BURNDOWN_HANDOFF.md`** — the burn-down entry point ("start here").
6. Then whichever tier you're working.

## Three things every one of them agrees on

- **A comment is a claim, not evidence.** Ten falsified self-descriptions across two audits, all
  drifting toward more discipline than the code holds.
- **Prefer a gate to a finding.** `CsvSeed` regressed `ARCHITECTURE` T3.1 because the fix was
  hand-applied to three sites and nothing checked the third.
- **Report a denominator.** An audit that publishes only its hits reads as complete when it isn't.

## What each doc says NOT to do

Each carries a refuted-hypotheses section, kept so the next pass doesn't reopen settled questions:
`ARCHITECTURE` §2, `CORRECTNESS` §6, `TENET` §7, `AUDIT_PROGRAM` §8. Read the relevant one before
proposing work in its area.
