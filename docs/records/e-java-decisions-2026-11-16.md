# E-Java's decision log (from 2026-11-16)

The user, 2026-11-16, relayed by the orchestrator: no questions to the user; decide by the rulings and tenets, log the
reasoning here, and keep going. Proposed register rows go to the orchestrator for the user's batch at the end.

The plan is `docs/records/e-java-plan-2026-11-16.md`, revision 2, on `claude/total-wall-e` (frozen). This log lives on
`claude/total-wall-e-v3` because PR1's branch takes only review fixes.

## E1. The Java-evaluation ledger keeps counting whole files; D5 is re-based on real reductions (2026-11-16)

**Context.** Item 9's audit (`docs/records/e-java-ledger-audit-2026-11-16.md`) found that the ledger's pins count
whole files. About 3,300 of the 4,448 counted lines in E-Java's files are planning and plumbing that evaluate nothing.
D5's target (≤ 3,800) could be reached by retiring those rows.

**Decision.** Do not retire or narrow any row. Reach D5 by real reductions only, and send the scope change to the
orchestrator as a proposal (below).

**Why.**

- AGENTS.md: guard files are load-bearing, and a pin moves only with a dated justification, "never as an
  edit-in-passing to make a build green".
- G8 (a guard for every goal).
- Narrowing a guard to meet my own target is the move those rules forbid.

**Consequence.** D5 becomes "E-Java's files' residue reduced by the audit's actions 3-5 and 7, plus item 10's movable
work". Expected: about 4,448 → 4,100-4,200. This is measured as each action lands.

**Proposed to the orchestrator (for the user's batch).** Split the ledger into:

- **(a)** run-side code that touches values (shrink-only, as now);
- **(b)** a planner size register, for the planning-side files `RunScope` and the ArchUnit walls already prove evaluate
  nothing.

Under that split, E-Java's run-side residue would be about 1,800.

## E2. The DATABASE-alone channel-B pass may go (item 3 met O5's condition) (2026-11-16)

**Context.** O5 allows the DATABASE-alone CI pass to retire only after BOTH matches it verdict for verdict on every
lane, measured. Q4 was approved on that condition.

**Measured** (`973b3753ca`, all run locally: DuckDB's five suites, H2 relation, SQLite five, Postgres five):

| What | Result |
|---|---|
| Tests compared | 4,216, over 16 lanes |
| Status differences | 0 |
| Verdict rows | identical wherever both judges print |
| Message differences | 8, all errors under both judges |
| Verdict rows printed only under DATABASE | 39, on 3 lanes |

- **The 8 message differences** come from statement shape. BOTH's fused statement carries host columns:
  - H2 echoes the whole statement in its error;
  - DuckDB and Postgres report the first failing expression or its position, which differs.
- **The 39 DATABASE-only rows:** traced on Postgres grammar (execution `398fbe71`). BOTH sends the same standalone
  verdict statement, but reads its row on a path that skips the `[verdict]` diagnostic print. It is a print gap, not a
  judging gap.

**Decision.** The condition is met for the lanes measurable here. Item 14 (retire the pass) goes in v2, after the
same comparison on the CI-only databases (MariaDB, MySQL, ClickHouse, Trino and the cloud lanes) at the next CI lane
round. Until then, the pass stays in CI.

**Why.** O5's wording is "on every lane, measured". Five databases weren't measurable here.

## E3. D1 fixed toward upstream, with no register row (2026-11-16)

**Context.** The service-test EqualToJson passed numbers within 2 ULP. The engine compares `decimalValue()` exactly.

**Decision.** Delete the leniency, with no register row.

**Why.** The upstream-fidelity tenet: lite does exactly what upstream does wherever possible. Removing a divergence
needs no row; keeping it would have needed one. This is commit `176eaf3251`.

## Corrections

**C1 (2026-11-16): D1's ledger effect was +13, not −1.**

- Commits `176eaf3251` and `a60462ce55` said "net −1" and "4,415".
- Measured: Equality 452 → 360 (−92), and TestAssertions newly pinned at 105, so +13. The 105 holds TestAssertions'
  existing wrapper, now counted, and a copy of the `decimal` helper.
- The running residue in E-Java's files is 4,448 + 13 (D1) − 3 (D3) − 26 (dead code) = **4,432**.
- D1's purpose was correctness (exact decimals, as upstream), not size.
