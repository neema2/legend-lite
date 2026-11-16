# E-Java: the end-to-end plan (2026-11-16, revision 2)

E-Java's half of stream E. E-SQL writes the SQL half separately, and the orchestrator puts the two together for the
user. Branch: `claude/total-wall-e` (PR1). Code head: `973b3753ca`. Nothing has landed.

**Revision 2** (the orchestrator, 05:04) accounts for two facts:

- PR1 carries D's branch, and E's v1 lands only after D's v1.
- D's W9/W10 moves the judges' engine text and print forms to the edge, through the same files the judges' clean-up
  needs.

D's plan is read from `origin/claude/total-wall-d-stage:docs/records/d-plan-2026-11-16.md`. In it, D v1 is one PR at
about day 20, W9 runs days 10-14 and W10 days 15-17.

## 1. Outcome

The user's words (2026-11-15): "making sure we're not doing anything bad in java and making sure judges are lean and
fast and readable".

When E-Java is done, this is true for a user:

- **Nothing bad in Java.** A test or a query never has its values computed by Java where the database should compute
  them (G6). Every place the product's Java still touches a value is named, counted and pinned: the Java-evaluation
  ledger, read by `JavaEvalLedgerTest`. Each one is either justified or scheduled for removal, and the product's
  counted residue is smaller than today's.
- **The judges are lean.** A PCT test sends about one statement for its verdicts under any judge. The judges' SQL is
  pinned and can only shrink.
- **The judges are fast.** No PCT or corpus lane runs slower than on `main`.
- **The judges are readable.** A newcomer can find, in one short document and a handful of classes, how an assert
  becomes a verdict under each judge. No judge class is over 1,000 lines.

## 2. Definition of done

Every check can be measured. "Today" is `973b3753ca`.

| # | Check | Target | Today | How it's measured |
|---|---|---|---|---|
| D1 | PR1's three blockers closed | all three | done (§3) | the orchestrator's review |
| D2 | Judge statements per test, PCT channel B reference lane, BOTH | ≤ 1.6 in every suite, never above the pin | essential 1.59; relation 1.37; standard 1.30; **grammar 2.12**; unclassified 1.08 | `pct/.../channelb/ratchets.tsv`: `judge.statements` / `discovered` |
| D3 | Judge characters per suite | ≤ the pin; grammar −20%, essential −10% | essential 2,273,093; relation 2,320,669; standard 2,463,370; grammar 1,108,619; unclassified 251,729 | the same file's `judge.chars`; the corpus's `rcorpus/lean-sql-judge-baseline.tsv` |
| D4 | No lane slower than `main` | every PCT and corpus lane ≤ main + 10% | SQLite channel B at or under main since `9dfa506b6b` | each lane run alone (`--local_test_jobs=1 --cache_test_results=no`) on the head and on main, same machine |
| D5 | Java-evaluation ledger, product side | E-Java's files (4,448 today) −15%, to ≤ 3,800. D's W9/W10 files are re-measured after D v1 and get a target then | 10,490 over 28 files: 4,448 in files only E-Java touches, 5,213 in files D's W9/W10 changes, 829 in others | `JavaEvalLedgerTest`'s pins, summed by file |
| D6 | Judge readability | no `testplan`/`testexec` class over 1,000 lines; one "how a verdict is made" document | 4 over 1,000: AssertVerdicts 2,138, SqlTextVerdicts 1,694, BodyClass 1,316, DatabaseJudge 1,039 | `wc -l` over `core/src/main/java/com/legend/{testplan,testexec}` |
| D7 | Answers and verdicts unchanged | 0 verdict changes on every lane | — | the channel B ledgers, the corpus registers, the `[verdict]` diff on PCT essential |
| D8 | Guards hold | every guard green, no pin moved without a dated note | green | `//core:census`, `//core:guardrails`, ArchitectureTest, `pct_discipline` |

## 3. Where we are

**PR1 against `main` is 481 files and +32.8K lines.** It carries D's branch, merged at `392ce271b`.

- **E-only content: 89 commits.** These are the commits not on D's staging head `0e9cf9a366`. They hold:
  - **E's product SQL work from before the split**, now E-SQL's half:
    - E3 measurements and baselines (the lean-SQL guard in `//gates:local`, channel A, core, corpus);
    - P1a (LetBinding);
    - E-4 (typed cast and case reasons, and the census);
    - O2's D1/D2 (mixed lists' date order);
    - R3-R6;
    - the may-skip set;
    - the order-in-aggregate and Databricks fixes.
  - **E's judge work:**
    - E-1 (the capture out of the product);
    - E-5a to E-5d (the Float canon staged, the canon once per side, BOTH on the fused program, the judge-SQL pins),
      with O5's HOST channel-B pass and G10's run-twice guard;
    - DE4 (one road, `planEach` deleted);
    - DE5 groups 1 and 2.
  - **PR1's review fixes 4-15, rounds 55f-58, and today's three blockers:**
    - `9dfa506b6b`: the SQLite fence;
    - `02fa0efebe`: the assert names;
    - `14f221a17a` and `3c7b435a7b`: the 5 corpus bodies.
  - The records: `pr1-rulings`, `pr1-e1-refusals`, this plan.
- **A trial merge with D's staging head** (`git merge-tree`, 2026-11-16) **conflicts in 25 files.** Among them:
  - core: `BUILD.bazel`, TypedQuery, PureV1Routes, PlanJson, TestPlanJson, LiteralSpelling, Scalars, AnsiSqlRenderer,
    SQLite, TestDoor, TestPlanner;
  - tests: ArchitectureTest, PlannerSurfaceTest;
  - `docs/SEMANTICS_REGISTER.md`, `docs/GATES.md`;
  - `tools/deps/core-layers.txt`, ChannelB, the parser-equivalence ratchets and `engine-client/src/plan-runner.ts`.
  Most are both sides adding beside each other: plan versions, layer pins, register rows, ratchets.

**PR1's three blockers are closed:**

1. **SQLite channel B 3.8x slower: fixed** (`9dfa506b6b`).
   - Cause: E-5a's staged canon. SQLite's flattener copied a recursive CTE into every read.
   - Fix: a SQLite-only `OFFSET 0` fence.
   - Every SQLite channel B lane is at or under main.
2. **Text parity** (`02fa0efebe`).
   - BOTH names asserts as DATABASE does.
   - Position 3431 vs 3398 is explained (33 characters of host columns earlier in BOTH's statement) and accepted by
     the orchestrator.
3. **The 5 corpus bodies under BOTH: confirmed** equal to HOST on DuckDB.

**Half-done:**

- The DATABASE-alone CI pass stays until BOTH is measured equal to it on every lane (O5).
- R3/R4's register rows wait for their numbers.
- PR1 has had no full CI on this head and no lane round since 58.

## 4. The work, in order

Hours are one session's working hours, lanes included. "Files" says whose files the item touches; §5.3 has the rule.

| # | Item | Why | Size | Verified by | Files | PR |
|---|---|---|---|---|---|---|
| 1 | Independent review of the staged v1, and its findings fixed | D1 | 2-4 h | the reviewer's list closed or ruled | any | v1 |
| 2 | Lane round on the head: every PCT channel A/B lane, both corpus lanes, the HOST pass, `ChannelBTwiceTest`; timings vs main | D4, D7 | 3 h | lane outputs vs round 58 and vs main | none | v1 |
| 3 | DATABASE-alone vs BOTH, verdict for verdict, every lane (O5's condition) | Proves agreement; lets the second CI pass go | 2 h | a per-lane `[verdict]` diff: 0 | none | v1 (measure) |
| 4 | **Keep v1 current with D:** merge D's staging head into PR1 now (the 25 conflicts), then at D's two checkpoints (W8 about day 9, W10 about day 17) and at D's landing | When D lands, PR1's diff against main is E's alone, and no conflict waits until the end | 4 h now, 2 h at each checkpoint | core, census, layering and the targeted set green after each merge | shared | v1 |
| 5 | After D v1 lands: merge main, one lane round, one full CI, then the PR opened with the register rows numbered and the records moved into `docs/SEMANTICS_REGISTER.md` | D1. E lands right after D | 4 h + CI | CI green on every OS | any | v1 |
| 6 | v2 measurement: judge time per lane (planning, database, host), statements and characters per suite, the top 20 statements per lane | Baseline before change | 3 h | a record in `docs/records/` | none | v2 |
| 7 | **Lean, in E-Java's files:** grammar's 2.12 statements per test (which shapes take a second statement: VerdictBatch, DatabaseAsserts, AgreementAsserts) and the session statement | D2, D3 | 6-8 h | `ratchets.tsv` lower; D7 holds | E-Java's | v2 |
| 8 | **Readable, in E-Java's files:** BodyClass (1,316) and DatabaseJudge (1,039) split along their seams (classes and reasons; run-time rules and failure texts) | D6 | 5-7 h | D6's count; ArchitectureTest; lanes unchanged | E-Java's | v2 |
| 9 | **The ledger audit (read only), E-Java's 4,448 lines:** each row classified as justified, scheduled removal or defect | D5 | 4 h | a record of each row's fate | none | v3 |
| 10 | The ledger shrunk in E-Java's files: Equality (452), HostJudgments (633), TdsCompare (236), PureAsserts (218), the planner rows (1,094). The host's comparisons the fused row can carry go to the database | D5, G6 | 8-10 h | E-Java's files ≤ 3,800; lanes unchanged | E-Java's | v3 |
| 11 | **After D's W10 lands on the stage:** AssertVerdicts (2,138) and SqlTextVerdicts (1,694) split, as W9/W10 leave them. The canon text left in verdict messages trimmed | D3, D6 | 6-8 h | D6; `ratchets.tsv`; lanes | D's until W10 | v2 |
| 12 | After D v1: the ledger rows in D's files re-measured, with a target set and put to the orchestrator | D5's second half | 2 h | a record | none | v3 |
| 13 | "How a verdict is made": one page per judge, assert to verdict, naming the classes, written last so it names the final classes | D6 | 2 h | read by the orchestrator | docs | v2 |
| 14 | Retire the DATABASE-alone CI pass if item 3 shows 0 differences (Q4) | Faster CI, one road | 1 h | the CI config diff | CI | v2 |

## 5. The PRs, and working around D

### 5.1 v1 = PR1, landed as E's alone after D v1

- **Contents.** The 89 E-only commits in §3, the review's fixes, and the merges that keep it current. Nothing new is
  added to v1's scope.
- **How it stays current while D runs (item 4).** Merge D's staging head into PR1, not a rebase at the end:
  - **Now:** the 25 conflicts are small while fresh.
  - **At D's W8 and W10 checkpoints:** each one resolved while both sides remember it.
  - **At D's landing:** merge `main`. Since `main` then holds D's stage, PR1's diff against `main` is exactly E's
    content.
  - Rebasing 89 commits across D's 20 days would replay every conflict commit by commit, so merging is the default
    (Q6).
- **What's left before it can land:**
  - items 1-5;
  - D v1 on `main`;
  - E-SQL's OK on the SQLite fence (Q1);
  - the user's word.
- **Process:** staging on `claude/total-wall-e`, one independent review (when this plan's v1 is staged, per the
  orchestrator), one lane round, one PR, one CI run, the user's merge word.

### 5.2 v2 (judges lean, fast and readable) and v3 (the ledger)

- **Branches.** Work stacks on PR1 as `claude/total-wall-e-v2` and `-e-v3`, merging PR1 in as it moves.
  - v2 opens once PR1 lands, v3 once v2 lands.
  - Each is rebased or merged onto `main` then, and gets the same process: one review, one lane round, one PR, one CI,
    the user's word.
- **v2:** items 6, 7, 8, 11, 13, 14.
- **v3:** items 9, 10, 12. v3 follows v2 because v2's splits move the ledger's files, and pinning twice is waste.
- **Building before PR1 lands.** E-Java's items 6-10 can be built on the stacked branches while D runs, since they're
  in files D doesn't touch (§5.3). The orchestrator decides whether they're built then or wait (Q7).

### 5.3 Who goes first in which files

| Files | Owner until D v1 | E-Java meanwhile |
|---|---|---|
| D's W9: AssertVerdicts, SqlTextVerdicts, BodyDriver, HostJudgment, HostJson, Verdicts, Seeds, EnginePlans, EngineText, PlanAllocations, PlanEnvelope, ProductStatements, PlanText | **D** | No restructuring. A defect fix only, sent to D first. Splitting AssertVerdicts/SqlTextVerdicts (item 11) waits until W10 lands on D's stage |
| D's W10: Repr, CanonicalForm, VerdictSql, SideShapes, LiteralSpelling, FloatCanonText/FloatPrintText and every dialect's float spelling | **D** | Nothing. Today's SQLite fence (`fencesFloatText`) is already in these files; D carries it at W10 behind its hook, and the next merge settles it |
| E-Java's: BodyClass, DatabaseJudge, DatabaseAsserts, AgreementAsserts, VerdictBatch, HostJudge, EffectPlanner; testexec's TestRun, AgreementRuns, RootLoops, Equality, HostJudgments, TdsCompare, PureAsserts, AssertRuns; the planner's ledger rows (BodyPlanner, FramePlanner, LetPlanner, SidePlanner, AggAwareActivities) | **E-Java** | Items 7, 8, 10. D does not restructure these. A D change that must touch one is sent to E-Java first |
| Shared pins: `JavaEvalLedgerTest`, ArchitectureTest, `core-layers.txt`, the plan versions, `docs/SEMANTICS_REGISTER.md` | both | Each side changes only its own rows. Conflicts are settled at the merges in item 4 |

## 6. Out of scope, and where each piece goes

| Piece | Goes to |
|---|---|
| Product lowering, the dialects, LetBinding, `SqlFn.MAY_SKIP_AN_ARGUMENT`, the product SQL baselines | E-SQL |
| Mixed lists' kinds-kept carrier (O2 step 3) and lifting R3/R4 | E-SQL |
| P3 decode repairs (typed product results) | E-SQL, unless assigned otherwise (Q5) |
| The judges' print forms and engine text, the Float canon and print in every dialect | D (W9, W10) |
| Everything else at the edge | D |
| The build rebuild; Windows and macOS lanes | the build program |

## 7. Decisions for the user, all up front

**Q1. The SQLite fence moves SQLite product SQL. Accept it in PR1?**

- What: `LIMIT -1 OFFSET 0` inside the Float canon on SQLite, wherever a Float is printed by its literal spelling.
  Answers are unchanged.
- Options:
  - (a) Accept it. It's small, measured and pinned.
  - (b) Unstage the canon on SQLite: no new product shape, but about 6x slower per canon, though no slower than main.
  - (c) Move it to E-SQL's PR. PR1 then waits on E-SQL.
- Implication: D's W10 moves the canon behind an edge hook anyway, and the fence goes with it.
- Recommendation: **(a)**, with E-SQL's sign-off in the PR body.

**Q3. Are the targets right?**

- The targets: D2 ≤ 1.6 judge statements per test; D3 grammar −20% and essential −10% characters; D6 no judge class
  over 1,000 lines; D5 E-Java's ledger files ≤ 3,800, with D's files' target set after D v1.
- Alternatives: tighter targets cost about +50% of hours in items 7, 8, 10 and 11. Looser ones ship sooner.
- Recommendation: **these**.

**Q4. Retire the DATABASE-alone CI pass once BOTH matches it on every lane?**

- Pros: one road, shorter CI. The HOST pass keeps the host's road exercised.
- Cons: one fewer independent witness of the database judge's own planning.
- Recommendation: **yes**, if item 3 measures 0 differences.

**Q5. Who owns P3 decode repairs?**

- Recommendation: **E-SQL**. It's product result decoding, not judging.

**Q6. Keep PR1 current by merging D's stage (recommended) or by rebasing onto `main` at the end?**

- Merge:
  - Pros: conflicts are settled in small pieces while fresh, and PR1's diff against `main` is still E's alone once D
    lands.
  - Cons: a merge-heavy history.
- Rebase at the end:
  - Pros: a linear history.
  - Cons: 89 commits replayed over D's 20 days of change, with every conflict met at once at the worst moment.
- Cost: merging is about 4 h now plus 2 h at each of D's checkpoints. A rebase is about 10-16 h at the end.
- Recommendation: **merge**.

**Q7. Build v2/v3's E-Java-only items (6-10) while D runs, or wait for PR1 to land?**

- Building now:
  - Pros: v2/v3 are mostly ready when PR1 lands, saving about a week.
  - Cons: another stacked branch to keep current.
- Waiting:
  - Pros: one branch.
  - Cons: E-Java idles for about 3 weeks, apart from item 4.
- Recommendation: **build now, in E-Java's files only** (§5.3). Nothing lands before D v1 and PR1.

Q2 (the Position offset) was accepted by the orchestrator, 2026-11-16.

## 8. Risks, and how we'd know early

| Risk | Early signal | Response |
|---|---|---|
| The 25 conflicts with D's stage hide a semantic clash (plan versions, the edge's routes, the layer pins) | the targeted set and census after item 4's first merge | settle each with D before the next checkpoint |
| D's W10 hook replaces the SQLite fence and the slowdown returns | SQLite channel B timing at the W10 merge | re-measure in item 4; the fence's pin test fails if the hook drops it |
| D slips past day 20 and PR1 waits | D's checkpoint reports | PR1 holds its scope; v2/v3 continue in E-Java's files |
| A lane slower on a database measured only in CI | the lane round (item 2) and CI timings | the same method as blocker 1 |
| CI red on Windows or macOS (never run on this head) | the full CI in item 5 | fix in PR1 |
| The local disk can't hold `//gates:local` | "No space left" | targeted sets plus CI; `bazel clean` frees 9-11 GB |

## 9. Parking list (not in this plan)

- Probes for the hunt, and any lane finding that doesn't block D1-D8.
- Re-staging the Float canon on BigQuery and the server dialects.
- `GridReads.tryLower`'s deletion.
- A G10 guard for product queries (today it covers test bodies).
- The product's Float print forms, O2's carrier and P3 decode (E-SQL's).

## 10. Timeline (days counted from the plan's approval, aligned to D's plan)

| Day | E-Java |
|---|---|
| 0-1 | item 4's first merge (D's stage into PR1); items 1-3 once the review arrives |
| 2-8 | items 6-8 on `-e-v2`, item 9 (if Q7 = build now) |
| about 9 (D's W8) | item 4's checkpoint merge; item 10 begins on `-e-v3` |
| about 17 (D's W10) | item 4's checkpoint merge; item 11 |
| about 20 (D v1 lands) | item 5: PR1 lands (merge main, lane round, CI, the user's word) |
| about 22-23 | v2 lands (items 13, 14 added; one review, round, CI) |
| about 25 | v3 lands (item 12 measured, item 10 complete); D1-D8 met |
