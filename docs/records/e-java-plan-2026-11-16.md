# E-Java: the end-to-end plan (2026-11-16)

E-Java's half of stream E. E-SQL writes the SQL half separately, and the orchestrator puts the two together for the
user. Branch: `claude/total-wall-e` (PR1). Head when this was written: `973b3753ca`. Nothing has landed.

## 1. Outcome

The user's words (2026-11-15): "making sure we're not doing anything bad in java and making sure judges are lean and
fast and readable".

When E-Java is done, this is true for a user:

- **Nothing bad in Java.** A test or a query never has its values computed by Java where the database should compute
  them (G6). Every place Java still touches a value is named, counted and pinned: the Java-evaluation ledger, read by
  `JavaEvalLedgerTest`. Each one is either justified as the edge's or scheduled for removal, and the counted residue
  is smaller than today's.
- **The judges are lean.** A PCT test sends about one statement for its verdicts under any judge. The judges' SQL is
  pinned and can only shrink.
- **The judges are fast.** No PCT lane runs slower than on `main`, and the judges' share of a lane's time is measured.
- **The judges are readable.** A newcomer can find, in one short document and a handful of classes, how an assert
  becomes a verdict under each judge. No judge class is a 2,000-line grab bag.

## 2. Definition of done

Every check below can be measured. "Today" is measured at `973b3753ca`.

| # | Check | Target | Today | How it's measured |
|---|---|---|---|---|
| D1 | PR1's three blockers closed | all three | done (§3) | the orchestrator's independent review of the head |
| D2 | Judge statements per test, PCT channel B, reference lane, BOTH | ≤ 1.6 per suite, never above the pin | essential 549/345 = 1.59; relation 644/469 = 1.37; standard 267/205 = 1.30; grammar 291/137 = 2.12; unclassified 100/93 = 1.08 | `pct/.../channelb/ratchets.tsv` (`judge.statements` / `discovered`); `bazel run //pct:update_ratchets` |
| D3 | Judge characters per lane | ≤ today's pin; grammar −20% and essential −10% (v2 target) | essential 2,273,093; relation 2,320,669; standard 2,463,370; grammar 1,108,619; unclassified 251,729 | the same `ratchets.tsv` (`judge.chars`); corpus: `rcorpus/lean-sql-judge-baseline.tsv` |
| D4 | No lane slower than `main` | every PCT and corpus lane ≤ main + 10% (run alone, same settings) | SQLite channel B: at or under main after `9dfa506b6b` | `bazel test <lane> --local_test_jobs=1 --cache_test_results=no` on the head and on main's merge base, same machine |
| D5 | Java-evaluation ledger residue | ≤ 9,500 counted lines (−10%), every row with a named owner and fate | 10,490 over 28 files (testplan 4,451, testexec 2,389, planner 1,811, plan 862, testexecutionplan 148) | `JavaEvalLedgerTest`'s pins summed |
| D6 | Judge readability | no testplan/testexec class over 1,000 lines; one "how a verdict is made" document | 4 over 1,000 (AssertVerdicts 2,138; SqlTextVerdicts 1,694; BodyClass 1,316; DatabaseJudge 1,039) | `wc -l` over `core/src/main/java/com/legend/{testplan,testexec}` |
| D7 | Answers and verdicts unchanged | 0 verdict changes on every lane | — | the channel B ledgers, the corpus registers, the `[verdict]` diff on PCT essential |
| D8 | Guards hold | every guard green, no pin moved without a dated note | green | `//core:census`, `//core:guardrails`, ArchitectureTest, `pct_discipline` |

## 3. Where we are

**Built on `claude/total-wall-e`** (173 commits since main's `f4baf1d10d`, `main` merged in twice, last at `973b3753ca`):

- E-1: the capture is out of the product.
- E-4: a typed reason on every cast and case, pinned by the cast-case census.
- E-5:
  - E-5a: the Float canon staged.
  - E-5b: the canon once per side.
  - E-5c: BOTH on the database judge's fused program, its host values as typed columns. It came with the O5 terms: the
    HOST channel-B pass and G10's run-twice guard (`ChannelBTwiceTest`).
  - E-5d: the judge's SQL pinned, PCT and corpus.
- DE4: BOTH plans on one road. `planEach` and the second road are deleted (ruling R1).
- DE5 group 2: the 5 corpus bodies the second road used to plan.
- R2-R6 (the rulings record, `docs/records/pr1-rulings-2026-11-15.md`).
- The PR1 review fixes (items 4-15) and lane rounds up to 58.

**PR1's three blockers, all closed today:**

1. **SQLite channel B 3.8x slower: fixed** (`9dfa506b6b`).
   - Cause: E-5a's staged canon. SQLite's flattener copied a recursive CTE into every read, and spent seconds
     preparing each verdict statement.
   - Fix: a SQLite-only `OFFSET 0` fence (`fencesFloatText`).
   - Every SQLite channel B lane is now at or under main (standard 15.2 s vs 21.9 s).
   - It moves SQLite product SQL wherever a Float canon is rendered (`LIMIT -1 OFFSET 0`), with answers unchanged. This
     is flagged to E-SQL, and its OK is decision Q1.
2. **The judges' text parity** (`02fa0efebe`).
   - Assert names: BOTH's suffix now names asserts as DATABASE does.
   - Position 3431 vs 3398: explained, not aligned. BOTH's statement carries 33 more characters of host columns
     before the failing expression. This is Q2.
3. **The 5 corpus bodies under BOTH: confirmed.** Each matches HOST on DuckDB (`14f221a17a`, `3c7b435a7b`).

**Half-done:**

- The DATABASE-alone lane pass is still in CI. O5 retires it only after BOTH matches it verdict for verdict on every
  lane, measured.
- R3's and R4's register rows wait for their numbers until PR1 lands (`docs/records/pr1-rulings-2026-11-15.md`).
- PR1 has had no full CI run on this head and no lane round since round 58.
- The full `//gates:local` doesn't fit this container's disk, so targeted sets were run.

## 4. The work, in order

Hours are working hours for one session, lanes included.

| # | Item | Why it serves the outcome | Size | Verified by | PR |
|---|---|---|---|---|---|
| 1 | Independent review of `973b3753ca`, and its findings fixed | D1. Nothing lands unreviewed | 2-4 h (fixes) | the reviewer's list, each item closed or ruled | v1 |
| 2 | Lane round on the head: every PCT channel A/B lane, both corpus lanes, the HOST pass, `ChannelBTwiceTest` | D4, D7 | 3 h | lane outputs, compared with round 58 and with main | v1 |
| 3 | Measure DATABASE-alone vs BOTH verdict for verdict on every lane (O5's condition) | Lets the second CI pass go (lean CI). Proves agreement | 2 h | a per-lane diff of `[verdict]` lines: 0 differences | v1 (measure only) |
| 4 | After D's v1 lands: merge main, re-run the targeted set and one lane round, then one full CI | D1. PR1 lands right after D | 3 h + CI | CI green on every OS | v1 |
| 5 | PR1 opened, register rows numbered (R3, R4 and the S-rows the records name), rulings moved from `docs/records/` into `docs/SEMANTICS_REGISTER.md` | The process rule: records live in the register once landed | 1 h | `pct_discipline` and the register checks green | v1 |
| 6 | v2 measurement: per-lane judge time (planning vs database vs host), judge statements and characters per suite, the top 20 statements by characters per lane | D2-D4 with a real baseline before any refactor | 3 h | a record in `docs/records/` with the numbers | v2 |
| 7 | The lean items found by 6. Expected: grammar's 2.12 statements per test (which body shapes take a second statement); the remaining canon text in verdict messages; the session statement per test | D2, D3 | 6-10 h | `ratchets.tsv` re-pinned lower, D7 holds | v2 |
| 8 | Readability: split the four classes over 1,000 lines along the seams they already have. AssertVerdicts: gating, rendered-text arm, quantified arm. SqlTextVerdicts: golden reads, engine text. BodyClass: classes, reasons. DatabaseJudge: run-time rules, failure texts | D6 | 8-12 h | D6's count; ArchitectureTest; every lane unchanged | v2 |
| 9 | "How a verdict is made" document: one page per judge, from assert to verdict, naming the classes | D6 | 2 h | read by the orchestrator | v2 |
| 10 | Java-eval ledger audit (judges step 4): every row classified as edge, scheduled removal or a defect; the defects fixed or put to the user | D5, "nothing bad in Java" | 6-8 h | `JavaEvalLedgerTest` pins lowered with dated notes; a record listing each row's fate | v3 |
| 11 | The ledger's residue shrunk to the D5 target. Candidates: the host judge's comparisons that the fused row can carry; PlanText (862); EnginePlans (717) | D5, G6 | 8-12 h | the ledger at ≤ 9,500; lanes unchanged | v3 |
| 12 | Retire the DATABASE-alone CI pass if item 3 showed 0 differences (the user's call, Q4) | Faster CI, one road | 1 h | CI config diff; the HOST pass and BOTH remain | v2 |

## 5. The PRs

Each PR follows the same process: staging on its branch, one independent review, one lane round, one PR, one CI run,
then the user's merge word.

**v1 = PR1 (`claude/total-wall-e`).**

- Contents: everything in §3's "built" list, plus the three blocker fixes and the merge of main.
- Before it can land:
  - items 1-5;
  - D's v1 on main first (the user's order);
  - the user's word.
- Dependencies:
  - D's v1. PR1 carries D's commits it was branched over, and once D lands its diff shrinks to E's alone.
  - E-SQL's OK on the SQLite fence (Q1).

**v2: the judges lean, fast and readable.**

- Items 6-9 and 12, on a fresh branch from main after PR1 lands.
- No product SQL moves; the judge's SQL only shrinks.
- Dependencies: PR1 landed.

**v3: the Java-evaluation ledger.**

- Items 10-11.
- Dependencies: v2 landed, because v2's splits move the ledger's files and pinning twice is waste.
- Process as above. Any row whose fix changes an answer goes to the user as a register row first.

## 6. Out of scope, and where each piece goes

| Piece | Goes to |
|---|---|
| Product lowering, the dialects, LetBinding, `SqlFn.MAY_SKIP_AN_ARGUMENT`, the product SQL baselines | E-SQL |
| Mixed lists' kinds-kept carrier (O2 step 3) and R3/R4's lifting | E-SQL (product lowering) |
| P3 decode repairs (the database returns typed values) | E-SQL (exec's decoding of product results), unless the orchestrator assigns it to E-Java |
| The product's Float print forms (`FLOAT_PRINT`) out of product SQL | E-SQL |
| Everything at the edge: compatibility, engine spellings, D1.x | D |
| The build rebuild, Windows and macOS CI lanes | the build program |

## 7. Decisions for the user, all up front

**Q1. The SQLite fence moves SQLite product SQL. Accept it in PR1?**

- What: the fix for the 3.8x slowdown adds `LIMIT -1 OFFSET 0` inside the Float canon on SQLite. That canon reaches
  product SQL wherever a Float is printed by its literal spelling.
- Options:
  - (a) Accept it in PR1. Pros: the fix is small, measured, pinned, and answers are unchanged. Cons: product SQL text
    moves in a judge PR.
  - (b) Turn staging off on SQLite instead, i.e. back to main's unstaged form. Pros: no new product SQL shape. Cons:
    SQLite channel B is about 6x slower per canon than fenced, though still no slower than main.
  - (c) Move the fix to E-SQL's PR. Cons: PR1 waits on E-SQL.
- Cost: none for (a). (b) is about 1 h.
- Recommendation: **(a)**, with E-SQL's sign-off recorded in the PR body.

**Q2. Position 3431 vs 3398: accept the explanation, or align?**

- What: Postgres reports a character offset into the statement. BOTH's statement is longer, because it carries the
  host's typed columns.
- Options:
  - (a) Accept it. The message text is otherwise identical and the verdicts are the same.
  - (b) Align it by moving every host column into a trailing CTE. Cons: about 6 h, a plan-shape change, and more SQL.
  - (c) Strip `Position:` from the message. That edits the database's text, which is not allowed.
- Recommendation: **(a)**.

**Q3. Do the v2 targets suit you?**

- The targets: D2 ≤ 1.6 judge statements per test; D3 grammar −20% and essential −10% characters; D6 no judge class
  over 1,000 lines; D5 ledger ≤ 9,500.
- Alternatives: tighter targets cost more hours in items 7, 8 and 11 (roughly +50% each for twice the reduction).
  Looser targets ship v2 sooner.
- Recommendation: **these targets**. They're grounded in today's measurements and leave no lane slower.

**Q4. Retire the DATABASE-alone CI pass once BOTH matches it everywhere?**

- O5 allows it only after the measured match (item 3).
- Pros: one road, less CI time. BOTH runs the database judge's own program, and the HOST pass keeps the host's road
  exercised.
- Cons: one fewer independent witness of the database judge's own planning.
- Recommendation: **yes**, if item 3 measures 0 differences.

**Q5. Who owns P3 decode repairs?**

- Recommendation: **E-SQL**. It is product result decoding, not judging.

## 8. Risks, and how we'd know early

| Risk | Early signal | Response |
|---|---|---|
| D's v1 merge conflicts with PR1 (both touch the edge, `TestDoor` and the plan versions) | the merge of main after D lands (item 4); a dry merge of `origin/claude/total-wall-d` now | merge D's branch into a scratch branch today to size it |
| A lane slower on a database not measured here (Snowflake, BigQuery, ClickHouse and the servers run in CI only) | the lane round's timings vs main (item 2) | the same method as blocker 1: dump the statements, time prepare vs run |
| CI red on Windows or macOS (never run on this head) | the branch's full CI (item 4) | fix in PR1 before the merge word |
| Readability splits (item 8) move the Java-eval ledger and the layer pins | `//core:census` and ArchitectureTest | re-pin with dated notes in the same commit; v3 follows v2 for this reason |
| The local disk can't hold `//gates:local` | "No space left" | targeted sets plus CI. `bazel clean` frees 9-11 GB |

## 9. Parking list (not in this plan)

- Probes for the hunt, and any lane finding that doesn't block D1-D8.
- The product's Float print forms, mixed lists' O2 carrier and P3 decode (E-SQL's, §6).
- Re-staging the Float canon on BigQuery and the server dialects (E-5a's "not yet measured").
- `GridReads.tryLower`'s deletion (the relation-typed `fetchDb` leg).
- A G10 guard for product queries, which today covers test bodies only.

## 10. Timeline

| Milestone | When |
|---|---|
| Items 1-3 (review fixes, lane round, DATABASE-vs-BOTH measure) | today, about 8 h after the review arrives |
| PR1 (v1) ready to land | within 4 h of D's v1 landing (item 4: merge, lane round, CI), then the user's word |
| v2 (lean, fast, readable) | about 2-3 working days after PR1 lands (items 6-9, 12: 20-30 h) |
| v3 (the ledger) | about 2 working days after v2 (items 10-11: 14-20 h) |
| Done (D1-D8 all met) | about 5 working days after PR1 lands |
