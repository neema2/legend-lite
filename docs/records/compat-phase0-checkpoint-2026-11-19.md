# The compatibility program's phase-0 checkpoint (DRAFT, filling as results land)

What phase 0 found, the numbers behind it, and the decisions it puts to the user before phases 1 to 3 are built.
- **The plan:** `docs/records/compat-program-plan-2026-11-19.md`.
- **The running log:** `docs/records/compat-program-log-2026-11-19.md`, both on `claude/compat-program`.
- **Status:** a draft. Rows marked PENDING are still being measured.

## 1. The questions phase 0 had to answer, and the answers so far

| # | Question | Answer | Evidence |
|---|---|---|---|
| Q1 | Is the design right? | **Yes, with changes.** An independent reviewer, who wrote none of it, formed its own view first and then attacked the plan. Its verdict was "sound with changes": 13 findings, all folded into the plan (r6, r7). 35 of the plan's claims were checked against the code; 24 held, and the other 11 are corrected | `docs/records/compat-independent-review-2026-11-19.md` on `claude/compat-review` (`838e280c`) |
| Q2 | Does lite already answer legend-engine's clients well? | **Better than legend-engine itself**, on legend-engine's own PCT. DuckDB, 1,249 tests: lite 1,212, legend-engine 1,005. H2 relation, 469: lite 448, legend-engine 411. Lite is behind once (a 20-digit Float literal, S120). Where legend-engine refuses (59 on DuckDB), lite gives PCT's expected answer in 55 | B1-R, H7 on `claude/compat-engine-lane` |
| Q3 | Can the translator work at the model level, without a core hook? | **Yes.** The spike passes 11 of the 15 legend-engine tests that `substring`/`indexOf` broke, with no new core hook (494 lines, public API only). The other 4 are already "not promised" | the translator, H5 on `claude/compat-translator-spike` (`4da71056`) |
| Q4 | What is lost with NO translator? | **For `substring`/`indexOf`: almost nothing.** legend-engine itself uses two meanings by where code runs: the database's for code sent to the store, Pure's everywhere else. The corpus has only 5 store-bound `substring` calls and 0 text `indexOf`. 7 of the 15 tests are fixed by a small core change (the mapping language's own `substring` rule), not a translator. The other strands: PENDING (V2, and the strand-by-strand run) | the translator, H5 |
| Q5 | What does "SQL text never promised" cost? | **143 of 2,761 corpus tests (5%)** move to "not promised", with the SQL-text appeal kept. 3 answers differed: 2 are legend-engine defects (`joinStrings`), and 1 is a lite bug (`firstDayOfWeek` on H2, now with the product track) | B2, `docs/records/census-b2-sqltext-probe-2026-11-19.md` |
| Q6 | Can PCT be lite's own test suite under both judges? | **Yes, cheaply, and it finds real bugs.** On channel B DuckDB, 786 of 1,249 pass. 449 failures are tests of legend-pure's printed text or words (the formatter's checks). 5 are real judge-parity bugs, now with the product track. It costs about 75 s. Channel A under LITE confirms lite's relation answers are right by value. legend-pure's comparison leniencies changed 0 verdicts on DuckDB channel B; the other lanes are PENDING | the judges session, H6 on `claude/compat-judges-homework` (`df5a1a37`) |
| Q7 | How big are the formatters? | **The small case.** 95 of 98 query-side prints sit at the root. 0 prints that are composed or feeding involve a Float, a Decimal or a date. 1 test is refused (a composed Pair print) | B1, H9 (0.4 (f)) |
| Q8 | Where does legend-engine skip lite's safety checks? | **16 test cells on 8 databases** (none on Postgres, Spanner or SQL Server). 3 of today's "unchecked" entries are dead. Lite plants 27 kinds of check; PCT exercises 9 | B2, H8 §(c) |
| Q9 | Is the strand map right? | **Yes**, confirmed body to body against the code, with 5 corrections and 1 missing strand. The ratchet's start: `PlanStyle` has 10 methods, 49 parameters in 19 classes and 50 calls | B2, H8 §(a) |
| Q10 | What do the compatibility checks cost per push? | PENDING: the edge net is 568 s locally as its own parallel lane; its CI time is not in yet. The ENGINE lane's pinned compare: PENDING | B1 (0.2), B1-R |

## 2. Product bugs phase 0 found (handed to the product orchestrator)

| Bug | Found by | Track |
|---|---|---|
| `firstDayOfWeek` answers Sunday on H2 and Monday on DuckDB (Pure says Monday) | B2's CP-4 probe | the product's judges track |
| 5 judge-parity bugs: `testDateFromHour`, `testAdjustBy{Days,Hours,Months,Weeks}BigNumber` | the LITE-PCT measurement | the product's judges track |
| a cross-kind `!=` (String against Integer) raises instead of answering true | the translator spike | hosts and planner, before R4 lands |
| a list aggregate's empty pivot cell on DuckDB is a list of nulls (S373, pending) | H1, recorded in 0.5 | to be scheduled |

## 3. The decisions for the user at this checkpoint

| # | Decision | Recommendation | Evidence it rests on |
|---|---|---|---|
| CP-12 | Where legend-engine departs from Pure's own definition, copy it or answer Pure's way? | PENDING the translator's full strand table. Leaning: answer Pure's way (the strict reading), since it shrinks the translator to almost nothing | Q4 |
| CP-14 | What the legend-pure-style checker is: a copy of lite's judge, lite's judge with a rules setting, or verdict records worded by a formatter | (iii), records plus a formatter, if the leniency measurement holds on every lane | Q6 |
| CP-13 | A smaller plan: defer export and import, and hand lite's own host drift to the product track | Lean yes, about 20 to 25 session-days cut | the independent review |
| CP-6 | The typed facts results carry for the formatters | yes | the plan |
| CP-7 | Error wording never in the plan; lite's own wording leaves later; one guard entry | yes | the plan, B1's S1 |
| — | The 6 tests on legend-engine's plan structure | not promised | Q5 |
| CP-5 | No special functions | PENDING B2's (j): the legend-engine answers that would stop matching, by case | Q8 |
| CP-11 | SQLite Decimal as floating point for legend-pure programs | keep the earlier ruling, as a translator rule | the plan |
| CP-9 | The per-push cost | PENDING Q10 | Q10 |

## 4. The plan after phase 0

PENDING: the sizes, the calendar and the order, restated once the decisions above are made.
