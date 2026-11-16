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

## E4. The ledger split built; D2 and action 6 deferred, each with its reason (2026-11-16)

**E1 ruled.** The orchestrator ruled the split under the user's "don't stop and ask", for the user's end batch. Built
in `7d33dfba3e`: `RUN_SIDE_VALUE` (15 files, 3,338 lines; E-Java's 2,509 of them) and `PLANNER_SIZE` (14 files,
7,120 lines). No row dropped, no pin moved; `theRegistersHoldTheirSides` keeps a run-side file out of the planner's.

**D4 and D5 fixed** (`242f7a8a54`). D4 was a silent wrong answer: a failed typed assertSameElements whose untyped
re-compare held passed.

**D2 deferred, not dropped.** Both judges pass two spellings of one value in a rendered text today (S132's last
sentence). Fixing the host alone would make the judges disagree (G2), so it lands with the database judge's ruled row
changed in the same commit and a corpus measurement.

**Action 6 handed off.** Projecting a keyed class to its key tree in SQL means the object JSON wire must emit, per
runtime class, only the key fields (the subtype case reads each value's own `__type`). That is the product's object
lowering, E-SQL's, not E-Java's files. Proposed to the orchestrator for E-SQL; the host's projection stays until then.

## E5. L1: a side over a one-row let binding rides the fused row (2026-11-16)

**Measured** (channel B grammar, DuckDB, BOTH): `testEqualNonPrimitive` and `testEqNonPrimitive` sent 72 statements;
70 were the host's per-assert side reads, refused as fused cells because LetBinding puts each side over
`FROM (SELECT <repeats>) AS __lets`, and `VerdictSql.singleValueColumns` took only a select of no FROM.

**Decision.** Accept a side whose one source is a one-row derived table of no FROM; each of its columns rides the
fused row as an uncorrelated scalar subquery over the binding, which stays one evaluation.

**Result.** 72 statements to 4 (two session, two fused); both tests PASS. Characters 493K to 537K (+9%): the fused
row computes each side again, because the verdict's own copy of the side was lowered separately (its lambda
variables numbered differently), so O5's reuse (`readingTheVerdictsValues`) does not match it. Round trips are the
larger cost; the reuse is a follow-up (match the side up to its lambda variables' names, or lower it once).

## E6. The DATABASE-only printed rows on the last two lanes, traced (2026-11-16)

Measured on D's stage (`ec87b5d333`), channel B essential with `-Dlegend.diagnostics=dump-sql`, each judge alone:

| Lane | `[verdict]` rows, DATABASE | BOTH | Status differences |
|---|---|---|---|
| SQLite essential | 837 | 831 | 0 (both PASS) |
| Postgres essential | 837 | 836 | 0 (both PASS) |

- **SQLite, 6 rows:** all `string::tests::format::testFormatDate`'s `assertEq`s. DATABASE sends one body statement
  holding them; BOTH sends six standalone `WITH __se` statements beside the host's side reads, read on a path that
  does not print `[verdict]`. No judge declined (`judge-declines` printed nothing). A print gap with a different
  statement shape, not a judging gap.
- **Postgres, 1 row:** an `assertEquals` of `1973-11-05`, the same print gap as the 39 rows traced on Postgres
  grammar (E2).

**Decision.** Item 3's comparison is complete on every local lane: no status difference anywhere. The print gap is
a diagnostic, not a verdict; making BOTH print the rows it reads is a small follow-up in `testexec`. Item 14 (retiring
the DATABASE-alone pass) still waits for the CI-only databases, as E2 says.

## Corrections

**C1 (2026-11-16): D1's ledger effect was +13, not −1.**

- Commits `176eaf3251` and `a60462ce55` said "net −1" and "4,415".
- Measured: Equality 452 → 360 (−92), and TestAssertions newly pinned at 105, so +13. The 105 holds TestAssertions'
  existing wrapper, now counted, and a copy of the `decimal` helper.
- The running residue in E-Java's files is 4,448 + 13 (D1) − 3 (D3) − 26 (dead code) = **4,432**.
- D1's purpose was correctness (exact decimals, as upstream), not size.
