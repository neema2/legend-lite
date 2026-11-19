# The compatibility program's phase-0 checkpoint (DRAFT, filling as results land)

What phase 0 found, the numbers behind it, and the decisions it puts to the user before phases 1 to 3 are built.
- **The plan:** `docs/records/compat-program-plan-2026-11-19.md`.
- **The running log:** `docs/records/compat-program-log-2026-11-19.md`, both on `claude/compat-program`.
- **The readable design and plan:** `docs/records/compat-design-and-plan-plain-2026-11-19.md`, on the same branch.
  Where this checkpoint and that document differ, that document is the later one.
- **Status:** a draft, corrected 2026-11-19 ~13:00 UTC: the decisions table now shows what the user ruled, and Q2's
  refusal counts are the corrected ones. Rows marked PENDING are still being measured.

## 1. The questions phase 0 had to answer, and the answers so far

| # | Question | Answer | Evidence |
|---|---|---|---|
| Q1 | Is the design right? | **Yes, with changes.** An independent reviewer, who wrote none of it, formed its own view first and then attacked the plan. Its verdict was "sound with changes": 13 findings, all folded into the plan (r6, r7). 35 of the plan's claims were checked against the code; 24 held, and the other 11 are corrected | `docs/records/compat-independent-review-2026-11-19.md` on `claude/compat-review` (`838e280c`) |
| Q2 | Does lite already answer legend-engine's clients well? | **Better than legend-engine itself**, on legend-engine's own PCT. DuckDB, 1,249 tests: lite 1,212, legend-engine 1,005. H2 relation, 469: lite 448, legend-engine 411. Lite is behind once (a 20-digit Float literal, S120). Where legend-engine refuses at planning (about 110 on DuckDB and 36 on H2; the first count of 59 and 0 came from a classifier that undercounted, corrected by the independent reviewer), lite answers. 245 such tests were read: all but 2 assert exact values | B1-R, H7 on `claude/compat-engine-lane` |
| Q3 | Can the translator work at the model level, without a core hook? | **Yes, if it is use-site and typed.** The spike passes 11 of the 15 legend-engine tests that `substring`/`indexOf` broke, with no new core hook, using public classes only. Its best variant (V6, typed retargeting) gives H2 2,285 and DuckDB 2,448 corpus passes. A rename over whole definitions breaks typing. **3.1's frontend split regresses no parity answer** (Cypher 4,267/0, IQ 5,984/19, SQL 1,651/0) | the translator, H5 (`c3befded`) |
| Q4 | What is lost with NO translator? | **For `substring`/`indexOf`: almost nothing.** legend-engine itself uses two meanings by where code runs: the database's for code sent to the store, Pure's everywhere else. The corpus has only 5 store-bound `substring` calls and 0 text `indexOf`. 7 of the 15 tests are fixed by a small core change (the mapping language's own `substring` rule), not a translator. **Measured (V2): with no translator at all, the corpus loses exactly 4 tests per lane** (H2 2,281 against 2,285; DuckDB 2,443 against 2,447). A whole-model rename does worse than nothing, because it breaks typing. **But 3.1 must come first:** lite's own Cypher, SQL and IQ frontends write the old meaning at 47 sites, and Pure's meaning alone breaks about 1,240 of their parity cases. **The other meaning strands, measured with no translator** (corpus H2, corpus DuckDB, pure/v1): S20's bare-DECIMAL literal +2, 0, 0 (off WINS 2 H2 tests); the plain set: 1 pure/v1 answer (`sqrt` of a negative); `isNumeric` 0; `parsedDecimal` -2, -2; `parsedJson` +1, -1; `LEGACY_SQL_NULL_UNSAFE_EQUALS` -1, -1 (a plan-text assert, already "not promised"); H2's legacy settings -1; `CrossKindNumbers` 0. **So beyond `substring`/`indexOf`, at most 3 real answers are lost per lane, offset by 3 wins on H2.** The exception is `PureText.raw`: it is input acceptance (the corpus's H2 setup SQL converted for DuckDB), carries the whole DuckDB lane (-2,452 without it), and stays at the door. **PCT (the legend-pure audience): every other strand off moves 0 tests on DuckDB and H2; the `substring`/`indexOf` change gains 3; on SQLite, dropping Decimal-as-REAL loses 5.** | the translator, H5 task 0 (`d5ba073e`) |
| Q5 | What does "SQL text never promised" cost? | **143 of 2,761 corpus tests (5%)** move to "not promised", with the SQL-text appeal kept. 3 answers differed: 2 are legend-engine defects (`joinStrings`), and 1 is a lite bug (`firstDayOfWeek` on H2, now with the product track) | B2, `docs/records/census-b2-sqltext-probe-2026-11-19.md` |
| Q13 | Does making the fallback decide every SQL-text test cost time or passes? | **No.** It then decides 1,433 H2 and 1,475 DuckDB tests by answer (469 and 485 more than today), in the same run time (H2 187 s → 179 s; DuckDB 628 s → 630 s). Today 964 and 990 tests already pass through it. Only 3 tests execute generated DDL, and they pass with lite's own | B2, H8 §(e) (`0b720d27d`) |
| Q6 | Can PCT be lite's own test suite under both judges? | **Yes, cheaply, and it finds real bugs.** On channel B DuckDB, 786 of 1,249 pass. 449 failures are tests of legend-pure's printed text or words (the formatter's checks). 5 are real judge-parity bugs, now with the product track. It costs about 75 s. Channel A under LITE confirms lite's relation answers are right by value. legend-pure's comparison leniencies change **0 verdicts on every PCT lane**, but **the corpus needs them: 69 tests on DuckDB, 29 on H2** (S131, S137, S132, S15, S144). So CP-14's option (iii) needs four small door rewrites | the judges session, H6 on `claude/compat-judges-homework` (`df5a1a37`) |
| Q7 | How big are the formatters? | **The small case.** 95 of 98 query-side prints sit at the root. 0 prints that are composed or feeding involve a Float, a Decimal or a date. 1 test is refused (a composed Pair print) | B1, H9 (0.4 (f)) |
| Q14 | Can legend-pure's printed tables be written in Java, after the run? | **Yes.** With the SQL builder switched off, 431 of 431 printed tables in PCT equal the expected text byte for byte on DuckDB; every print sits at an assert side's root; H2's relation suite gains 4 passes; no core hook was needed (137 + 125 lines against the builder's 791) | H11 on `claude/compat-relation-text-spike` (`a9df8c39`) |
| Q8 | Where does legend-engine skip lite's safety checks? | **16 test cells on 8 databases** (none on Postgres, Spanner or SQL Server). 3 of today's "unchecked" entries are dead. Lite plants 27 kinds of check; PCT exercises 9 | B2, H8 §(c) |
| Q9 | Is the strand map right? | **Yes**, confirmed body to body against the code, with 5 corrections and 1 missing strand. The ratchet's start: `PlanStyle` has 10 methods, 49 parameters in 19 classes and 50 calls | B2, H8 §(a) |
| Q10 | What do the compatibility checks cost per push? | The ENGINE lane: 272.7 s serial, about 1.5 to 2 minutes of wall time on a 16 GB Linux runner and 4.5 minutes on macOS; the batteries 7.7 s. The edge net: 568 s locally as its own parallel lane, CI time PENDING | B1 (0.2), B1-R (H7) |
| Q12 | What does dropping legend-engine's H2 legacy settings cost (CP-8)? | **1 corpus test** (its own model compares a BIT column with 1). Null ordering moves nothing. The referee keeps its own isolated legend-engine H2 2.1.214 | B2, H8 §(h) (`d37427ef4`) |
| Q11 | Is legend-engine's manifest a trustworthy oracle? | **Yes:** 21 of 21 sampled rows agree when legend-engine itself is asked, including its own wrong answers | B1-R (H7) |

## 2. Product bugs phase 0 found (handed to the product orchestrator)

| Bug | Found by | Track |
|---|---|---|
| `firstDayOfWeek` answers Sunday on H2 and Monday on DuckDB (Pure says Monday) | B2's CP-4 probe | the product's judges track |
| 5 judge-parity bugs: `testDateFromHour`, `testAdjustBy{Days,Hours,Months,Weeks}BigNumber` | the LITE-PCT measurement | the product's judges track |
| a cross-kind `!=` (String against Integer) raises instead of answering true | the translator spike | hosts and planner, before R4 lands |
| ~~a list aggregate's empty pivot cell on DuckDB is a list of nulls~~ WITHDRAWN: H1 read it, it was never measured, and it did not reproduce when the hosts worker ran it | H1 (a reading) | none |
| a window column read through a list operation gives wrong rows on SQLite and Postgres | H11, the relation-text spike | the product's `claude/window-list-lowering`; it gates the formatter step |

## 3. The decisions at this checkpoint, and where each stands

| # | Decision | Where it stands (2026-11-19 ~13:00 UTC) | Evidence it rests on |
|---|---|---|---|
| CP-13 | A smaller plan: defer export and import, and hand lite's own host drift to the product track | **RULED yes** ("Yes to the six", ~12:10) | the independent review |
| CP-14 | What the legend-pure-style checker is | **RULED (iii)**: lite's verdict records, worded by a formatter, with four small door rewrites of corpus test bodies | Q6 |
| CP-6 | The typed facts results carry for the formatters | **RULED yes** | the plan |
| CP-7 | Error wording never in the plan; lite's own wording leaves later; one guard entry | **RULED yes** | the plan, B1's S1 |
| — | The 6 tests on legend-engine's plan structure | **RULED: not promised** | Q5 |
| CP-11 | SQLite Decimal as floating point for legend-pure programs | **RULED: kept**, as one rule at the door | the plan |
| CP-9 | The per-push cost | ruled in principle (~10:45); the edge net's CI time is still PENDING (PR #123) | Q10 |
| — | **The goal's sentence** (new, from the reviewer's re-check): "Lite follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. The rest is recorded." | **OPEN, asked of the user ~12:56.** It is ruled before D1 and D2, and AGENTS.md's upstream-fidelity tenet and G8 are amended with the user's word | the re-check (`5d2ce214` on `claude/compat-review`) |
| D1 (was CP-12) | Where legend-engine departs from Pure's own definition (`substring`, `indexOf`): copy it, or answer Pure's way? | **OPEN, asked of the user ~12:56.** Recommended: Pure's way, LOUD (a detector at the legend-engine door that warns, or refuses by a switch; no translator) | Q4; the re-check |
| D2 (was CP-5) | Keep or remove S20's exact-decimal literal and the unchecked-maths plain set | **HELD** until B2's (j) count on the ENGINE lane lands | Q8 |

## 4. The plan after phase 0

The stages, their order, their sizes and what each one's proof is are in
`docs/records/compat-design-and-plan-plain-2026-11-19.md` §5: stage A (A1 to A4, about 2 to 3 weeks), a stop, then
stage B (B1 to B5, about 3 to 4 weeks). The final plan is written fresh once D1 and D2 are ruled and the running
prototypes (H10, H12, H13) have reported; the independent reviewer checks it; the user ratifies it.
