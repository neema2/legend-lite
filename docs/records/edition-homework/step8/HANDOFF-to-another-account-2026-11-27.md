# Step 8, handed to a session in another account (2026-11-27, 21:20 UTC)

*Written by the orchestrator of the edition program at the user's request ("Is A in a shape where another account can
finish step 8 in parallel and not conflict with B? If so can we write up detailed instructions for the other
account"). You are that session. You have none of the orchestrator's context and cannot message it: everything you
need is in this file and in the files it names, and everything you produce reaches the program through a branch you
push and a note you write.*

## 1. What you are doing, in one paragraph

legend-lite replaces legend-pure and legend-engine. The edition program (Stage B of the compatibility program) makes
the product answer one way and moves everything that exists only to look like legend-pure or legend-engine to "the
edge". **Step 8 is one part of it: where legend-pure or legend-engine prints a value differently from lite (a Float's
text, a date-time's text, a printed table), that text is written by Java at the edge from the typed value the
database computed, and the SQL that used to compose upstream's text is deleted.** Two slices are landed, two more
are built and audited, and the work was paused on 2026-11-26 so that all attention could go to step 6. Step 6's
first half ("A") landed on 2026-11-27. You finish step 8 while the orchestrator's account does step 6's second half
("B").

## 2. Read these first, in this order

1. `AGENTS.md` (always loaded). Hold to it. The parts that bite here: tenets 1 to 3 of "the six tenets" (the database
   computes; Java decides nothing about a value; compatibility re-spells, never re-computes), the upstream-fidelity
   tenet, "Guard files are load-bearing", "The fast dev loop", and "Pushing to main".
2. `docs/records/edition-program-START-HERE-2026-11-25.md`: the program, its rulings, how every check is run.
3. **In this branch:** `docs/records/edition-homework/step8/STATE-when-paused-2026-11-26.md`.
   It is step 8's own account of what is done, what is not verified, the second review's findings (not applied), and
   the exact commands. The records beside it are each slice's design and measurements.
4. **In this branch too** (copied here from `claude/edit-ahead`, so you need no other branch): the outside audits,
   `docs/records/edition-homework/audits/step8-slice2-independent-2026-11-26.md`,
   `step8-slice2-repair-recheck-2026-11-26.md` and `step8-slices3-4-independent-2026-11-26.md`.
5. `docs/records/edition-homework/open-decisions-2026-11-26.md`, section "Step 8's": decisions D7 to D10, which the
   user has not ruled.
6. `docs/CLOUD_SESSIONS.md` (Bazel in a cloud session) and `docs/GATES.md` (read before calling anything green).

## 3. The branches

| Branch | Commit | What it is | May you push to it? |
|---|---|---|---|
| `claude/compat-b3-nufdlk` | `c81297490` when this was written | the program's branch: steps 1 to 5, and step 6 "A" | **No.** One writer: the orchestrator's account merges your branch into it |
| `claude/edition-step8` | `584060165`, then the one commit that added this file and the audits | step 8's branch, paused; based on `4d44bf54f`, which is before step 6 | **Yes.** It is yours now |
| `claude/modest-galileo-h6swr4` | the draft pull request #141's branch | every push to it runs all 54 CI lanes | **No.** Never |
| `main` | | | **Never**, and no pull request to it |
| any new `claude/step-8-...` | | scratch branches, as many as you need | Yes, freely (AGENTS.md, "Scratch branches") |

A push to `claude/edition-step8` or to a scratch branch starts no CI: CI runs on pushes to `main` and on the pull
request's branch only.

## 4. Step zero: bring step 8's branch up to the program branch

Step 8's branch is 32 commits past `4d44bf54f`; the program branch has all of step 6 "A" since then. **Merge, do not
rebase** (`git fetch origin claude/compat-b3-nufdlk && git merge FETCH_HEAD` on `claude/edition-step8`).

The orchestrator tried this merge on 2026-11-27 in a throwaway tree, against the program branch at `c81297490`: **250 files merged by themselves, 8 conflicted,
11 conflict hunks in all.** Each conflict and its rule:

| File | Hunks | Rule |
|---|---|---|
| `core/src/main/java/com/legend/lowering/NamedRule.java` | 1 | Both sides added rules to one list. **Keep both.** Step 6 also RENAMED one: `NUMBERS_OF_THREE_TYPES` is now `THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION`; use the new name everywhere |
| `core/src/main/java/com/legend/lowering/NamedRules.java` | 1 | The same: keep both sides' arms; the switch has no `default` arm, so javac tells you if one is missing |
| `core/src/test/java/com/legend/SqlFnSortedTest.java` | 1 | A list both sides added to: keep both, in the order the test asks for |
| `docs/SEMANTICS_REGISTER.md` | 1 | Both sides added or amended rows. Keep both. Step 8's rows are S428 and S429; step 6 added S430 to S433 and amended S399 and S432. No number is used twice; check after merging |
| `core/src/main/resources/com/legend/builtin/native-claims.tsv` | 4 | **Generated. Do not merge by hand:** take either side, then `bazel run //core:update_generated` |
| `parser-equivalence/.../PeRatchets.java` and `ratchets.tsv` | 1 + 1 | Keep both sides' history lines in `PeRatchets.java`, then `bazel run //parser-equivalence:update_ratchets` writes the `.tsv` |
| `pct/engine_lane/summary_h2.tsv` | 1 | **Recorded. Do not merge by hand:** take the program branch's side, then `bazel run //pct:update_engine_lane` and read every line that moves |

Then, **before you change anything else**:
1. Build, and run the fast set (section 8).
2. Re-run step 8's five recordings (section 8) at the merged commit and commit them as "the baseline after the merge".
   **Every recorded line that moves here was moved by step 6 or by the merge, not by step 8's remaining work.** Read
   each against section 5. This is the only way to tell the two apart later; AGENTS.md's trap applies ("a golden
   regenerated at the end of a step can carry a line that is not that step's").

## 5. What step 6 "A" changed under you

The full account is `docs/records/edition-homework/landing-step-6-closing-2026-11-27.md` (on the program branch). What
matters for printing:

- **Equality is strict** (AGENTS.md, "The equality rule"): two values of two types are never equal; an Integer and a
  Decimal are compared by value; an exact number and a Float never are. Tests of yours that compare a printed number
  with a number of another type may now be refused when compiled.
- **`assertEquals` compares in order.** A side whose order the query does not define is a compile error on lite's own
  door; at a compatibility door such an `assertEquals` is read as `assertSameElements` (three lines of
  `spec/src/test/resources/edge/corpus_*_host_judge-host.tsv` moved for this).
- **Dates' carriers moved.** A year, a year-month, and a date built to the hour or the minute
  (`date(2015, 4, 16, 14)`) are carried as the text they print (`SqlType.Scalar.TEMPORAL_TEXT`); on SQLite a
  date-time is one canonical text. **This is next to your slice 4 (the date-time text).** After the merge, check every
  date-time case of `edge_cases_answers.tsv` first.
- **A comparison of two whole sides** (computed lists, maps, tables) is planned as a statement of its own
  (`planner/RootComparison`, `lowering/SideEqualities`). It is not yours; see section 6.
- **Known, not fixed, not yours:** on DuckDB a comparison of two computed lists inside a list's lambda that does not
  read the lambda's element fails at run ("subqueries in lambda expressions are not supported"). If one of your tests
  hits it, write the comparison outside the lambda and note the test in your landing note.
- **Measured on 2026-11-27, useful for your review's finding 1:** a `let` is not computed once; its expression is
  written again wherever it is read.

## 6. Whose files are whose

Step 8 and step 6 "B" run at the same time in two accounts with no messages between them. They do not conflict if
each keeps to its files.

**Yours (step 8):**
- `core/src/main/java/com/legend/edge/`: the printing classes (`PrintedRelations`, `UpstreamPrints`, `PrintTail`,
  `PureText`, `PureValueText`, `PurePrintForms`, `PureRelationText`, `PureRelationTextWriter`, `EdgeDateTimeTexts`,
  `EdgePureOnlyTexts`) and their tests.
- The print and Float-text arms of `sql/dialect/*`, the print entries of `sql/SqlFn`, `lowering/LiteralSpelling`,
  the lines of `lowering/Fold` step 8 already touches.
- Step 8's recorded files (`core/src/test/resources/edge/edge_cases_answers.tsv`, the edge contract, the edge corpus,
  `pure_v1`, the engine lane) and its records under `docs/records/edition-homework/step8/`.

**Shared: add your own rows only, never reorder or reformat, and expect a small merge:**
- `lowering/NamedRule.java`, `lowering/NamedRules.java`, `edge/EngineTable.java`, `edge/PureTable.java`: rows for
  PRINTS only. **Never a row for `==`, `eq`, `contains`, `in`, `indexOf`, a deduplication or an ordering:** those are
  B's, and B will change how a door's comparison reaches the lowering. If B replaces the table mechanism itself, the
  orchestrator moves your print rows; you do not need to prepare for it.
- `lowering/Scalars.java`: the print rules only.
- `core/src/test/java/com/legend/ArchitectureTest.java`, `JavaEvalLedgerTest.java` and the other guard files: your
  own entries, each with a dated comment naming step 8. Never loosen a guard that is about comparisons.
- Generated and recorded files (`native-claims.tsv`, `ratchets.tsv`, the lean-SQL baselines, PCT's expected
  failures): always regenerated by their tool, never merged by hand.

**B's: do not edit. If one of these blocks you, write it in your landing note and work around it:**
- `lowering/Equalities`, `SideEqualities`, `TaggedLists`, `InstanceEquality`, `NullSemantics`, `HeldKinds`,
  `NumberFamilies`, `MixedEncoding`, `VerdictSql`, `CanonRider`, `CanonicalRenderSql`, the comparison rules of
  `Scalars` (equal, contains, in, indexOf, removeDuplicates, the orderings), `Lowerer.asCondition` and `written`.
- `planner/RootComparison`, the comparison parts of `planner/ProductStatements` and `BodyPlanner`.
- `testplan/` (`DatabaseJudge`, `AssertVerdicts`, `DatabaseAsserts`, `HostAsserts`, `JudgmentFacts`), apart from the
  one line of `PlanStyleFacets` step 8 already changed.
- `compiler/spec/EqualityClasses`, `LiteCrossTypeRule`, `NumberLiterals`, `OrderedAsserts`, and the type-checker's
  package for a door's meanings (`compiler.spec.edition`, which B creates).
- `sql/dialect/SameValuesForm` and each dialect's `equalArm`; `edge/UpstreamTies`; `tools/equality-measure/`.

## 7. What to do, in order

0. Step zero (section 4): the merge, the fast set, the baseline recordings.
1. **Apply the second independent review's findings** (the last section of the STATE note; nothing of it is applied):
   three code fixes (`UpstreamPrints.inNoDefinedOrder` misses a legacy `TabularDataSet`; `PrintedRelations
   .mayBeASpelledDate` does not err to yes for a user function or a class property that is no primitive;
   `UpstreamPrints.orderFree` takes `makeString`, a relation's `toString` and `toCSV` for order-free), and one for
   the records (answers moved by the DateTime narrowing with no words in S429). Each with a test both ways.
2. **Verify the lane probe here** on DuckDB, H2 and SQLite (STATE note, "Not done" item 3): it must print its
   `PROBE CASE` lines before anyone runs it on a lane. Then run it on D10's six databases through your lane sessions
   (section 8) and put the answers in the D10 draft.
3. **The full measurement** of everything after `5e9a8879b`: the STATE note's "exact next action" block. Every
   recorded line that moves is named and explained. The narrowed rule R1 lifts more than the measured repair did and
   the DateTime rule lifts less: expect moves in both directions.
4. `docs/dialect-capability/reach.tsv` (nit 2) and a core test that runs the Float row against a database (nit 5).
5. **The records and ONE table for the user:** the response record's table and "Measured" section; the amendment
   drafts for S428 and S429; the repair record's re-check section; then the two tables of the STATE note joined into
   one, with a draft for D7, D8, D9 and D10 (problem, options, what each costs, your recommendation). Upstream's
   answers in it are READ in the pinned sources or RUN, never recalled (AGENTS.md).
6. One commit that is not scratch; the local gate (section 8); push `claude/edition-step8`; write the landing note
   (section 9). **Stop there until the user has ruled D7 to D10.**
7. Only after those rulings, step 8's later slices, none started: `TDSNull`'s row; `+` in the tail; a last-step
   `format` lift if the user rules it (B2 (b)).

## 8. How to check your work

- **Setup** (once a container): `export PATH=/root/.local/bin:$PATH`, then `tools/cloud-distdir.sh <your targets>`
  (`docs/CLOUD_SESSIONS.md`). One Bazel at a time. Long runs in the background; never a foreground wait loop.
- **The fast set**, after every edit (5 to 15 minutes): `bazel test //core:guardrails //core:guardrails_edge
  //core:census //core:core_edge_tests //pct:pct_route_print_test`.
- **The five recordings:** `for t in //core:update_edge_cases //pct:update_edge_contract //spec:update_edge_corpus
  //core:update_edge_pure_v1 //pct:update_engine_lane; do bazel run $t --jobs=3; git status --short; done`. Read each
  diff line by line.
- **The full set and the local gate:** the two commands at the end of the STATE note, with `--jobs=3
  --local_test_jobs=2` (at Bazel's default the container's memory limit kills a heavy test now and then). Fourteen
  targets fail only because the container runs as root: rerun them through `tools/postgres/as_unprivileged.sh`
  before calling one a regression.
- **A single failure** goes through the narrowest runner first: `bazel run //spec:corpus_one -- <duckdb|h2> host
  <test>`, or the test's own target with `--test_filter`.
- **Database lanes.** Your account has its own warm database lane sessions, with credentials (the user,
  2026-11-27: "It has warm database sessions it can use with creds"). Use them as AGENTS.md's "The fast setup" says:
  one session per group of databases, **measure only** (no tracked file edited, no commit, no push), started once, on
  your FINAL commit, each told the commit to check out, the targets to run (`bazel test //pct:pct_<name>
  //pct:pct_channel_b_<name> --keep_going --test_timeout=3600 --cache_test_results=no`) and exactly what to report.
  Run every database a print could differ on, the cloud ones included, and D10's probe on all six of its databases
  (Postgres, Trino, Snowflake, ClickHouse, Databricks, BigQuery) once the probe prints its lines here. Nobody pastes
  a key into a session, a message or the repository, and no session prints one. A lane you could not reach is named
  in the landing note as not run.
- **Independent review:** before the non-scratch commit, give a fresh subagent the diff and the rules (never your
  conclusion) and apply or answer each finding.
- **SQL size pins only go down.** A pin that would go up is a question for the user, not an edit.

## 9. How your work comes back

1. Push `claude/edition-step8`.
2. Write `docs/records/edition-homework/step8/LANDING-<date>.md` on it: the commits; what each fix did; **every
   recorded answer that moved, as a table** (the expression, legend-pure's answer, legend-engine's, lite's before,
   lite's now, its register row); the decision drafts; what you ran and its result, with anything not run named; and
   any file of section 6's "B's" list you needed and did not touch.
3. Tell the user the branch and the commit. The user passes it to the orchestrator, who reviews it and merges it into the
   program branch (and runs any lane you named as not run). You never push to the program branch or the pull request's branch.
4. If the program branch has moved when you reach a milestone (B lands in slices), merge it again first, by section
   4's rules, and re-run the fast set and the recordings.

## 10. Rules that have cost this program time when broken

- No push to `main`, no pull request, no merge of one, without the user's own word in your session.
- No push to the pull request's branch: it runs every CI lane.
- Nothing is called verified that was not run; a claim read from code says "read", one run says "run".
- A guard's pin, allowlist or ledger moves only with a dated comment naming the reason; never to make a build green.
- Every moved test answer and every new difference from legend-engine or legend-pure goes to the user as a table
  with its register row; a difference with no row is a defect.
- Write to the user in plain English, the conclusion first.
