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
| [`HARNESS_AUDIT_2026_09_07.md`](HARNESS_AUDIT_2026_09_07.md) | The rebuilt test harness, soup to nuts — driver, referee, main-side oracle residue, ambient state, guard integrity, roster/floor validation (gates run), and end-to-end verification strength. Seven adversarial reviewers; `END_TO_END_PLAN_2026_09_08.md` treated as claims, not evidence. **Headline: batch 115 deleted the writers of `H2Verify.ORDERED_QUERY`/`SORT_KEYS` and left the readers — row order silently stopped being a contract (95 passes affected), in the direction that raises the floor.** Also: only 1,511 of 2,454 passes (61.6%) are differentially verified; the gate has one assertion; the §7 floor arithmetic does not close; 127 H2 failures are one renderer bug. §9 lists six claims investigated and **dropped**. §10 is a 15-point machine-checkable definition of DONE. |

| [`TWO_DESIGN_LEGS_2026_09_07.md`](TWO_DESIGN_LEGS_2026_09_07.md) | Feasibility and design homework for the two proposed design legs — **single-shot** (setup+query+asserts as one statement) and **code+metamodel as data**. Static analysis only; nothing executed. **Headline: single-shot is realistic at 77.6% (1,998 of 2,575 tests are already one statement after inlining), with the boundary at the session, not the test** — 21,676 of 33,808 inlined statements are DDL/DML and no SQL dialect can create a table and select from it in one statement. The `setupDbAndLoadCsvAndExecute` worry is 213 tests / 28 names / 7 shapes, and `StatementInline` already unrolls 6 of the 7. `let`→CTE needs **no MIR work** (`SqlWith` exists; only `SqlUnion`'s `boolean all` must become a `SetOp`), and **zero** lets rebind corpus-wide. For leg 2: **recursive CTEs are the part not to build** — max class depth 5, mapping-include depth 2, closure tables already the declared policy (`SystemMetamodel.java:260-263`), and both pinned dialects have unbounded-blast-radius defects (H2 has no cycle protection and OOMs; DuckDB silently under-returns, `#13974` closed not-planned). Stored procs: DuckDB has none, H2's are Java, 7 of 22 backends have no procedural language. §4 is **30 machine-runnable probes**; §6 lists nine hypotheses **dropped**. |

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
