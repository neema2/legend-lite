# The edge program: handoff for a redo (2026-11-18)

**Status: DRAFT, written while the last trains land. The final numbers (d-stage's head, the census, CI) are filled in
when the PR opens.** This document goes with the one PR that takes `claude/total-wall-d-stage` to `main`.

> **For the reviewing account, read this first.**
> - **The live copy of this document is on branch `claude/total-wall-orch`.** The copy inside the PR is a snapshot
>   taken when the PR opened. Results that arrive later (D's heavy round, CI, the PR number) are written only to the
>   live copy. They are not pushed to the PR branch, because any push there restarts CI (§8.4).
> - **Where to start:** §8 is the final sequence (what is in the PR, what ran where, what to expect). §7 is how to
>   verify it: the database lanes (§7.2) and the audit's questions (§7.3).
> - **Rules that hold for you too:** nothing merges without the user's word. No force push. Never read, print or
>   copy a credential. A pin grows only with the user's word, or in small explained steps under the user's
>   delegation. Report findings to the user graded as in §7.3 before fixing anything.

**Why it exists.** The user decided on 2026-11-18 (11:25 UTC): "Finish to zero, then PR with a detailed hand off
document". The user means to redo this work from scratch later, in another session or account. This document is for
that redo. It records what was built, how it is checked, what the user ruled, what went wrong and why, and what is
left.

**The audit.** The user, 2026-11-18: the final audit "might happen in another account that has token budget to do at
xhigh", in parallel with the PR's CI. This document is that audit's starting point; the follow-ups are in
`docs/records/edge-follow-ups-2026-11-18.md`.

**Where the detail lives** (all on branch `claude/total-wall-orch` unless named):

| Document | What it holds |
|---|---|
| `docs/records/total-wall-edge-program-2026-11-16.md` | The plan of record: the goal (§1), the design (§2), the rulings verbatim (§3), what we would not do (§4), the checks (§5), the classification table (§6), the phases (§7), the follow-ups F1-F52 (§9), the rulings asked of the user (§10), and the dated log (§12, newest first). |
| `docs/records/edge-audit-2026-11-17.md` | The audit list: items J1-J43, K1-K16, the host items H1-H11 and the lens audit L1-L14, each with its file, lens, ruling and owner. |
| `docs/records/edge-portability-program-2026-11-18.md` | The next program, approved by the user: the edge as portable plans (one plan, two presentations; import and export of upstream plans). |
| `docs/records/edge-follow-ups-2026-11-18.md` | Every follow-up in one list: decisions waiting on the user, the plan's F1-F52, items deferred during the program, and what was found while finishing. |
| `AGENTS.md` (main) | The architectural invariants and the goals of record G1-G11. Read it first. |

---

## 1. What the program set out to do

In the user's words: **"everything compatibility-related to the edge"**, with **"product and test code being super
slim and fast, then compatibility at hopefully mostly at java edge on top of clean typed results on platform"**.

Done means:

1. **The product answers in lite's own typed forms.** A Float's text is six decimals (S311), a DateTime is ISO with
   `Z` (S312), an error is its kind, the SQL is lean, and no legend-pure or legend-engine spelling is computed for a
   product answer.
2. **A legend-engine client gets exactly upstream's bytes** (pure/v1, PCT, the relational corpus, engine-style service
   tests). The edge makes them.
3. **The test code is slim and fast.** The judges compare typed values.
4. **The separation is mechanical.** Product code cannot reach the edge, by the build, and a census of guards counts
   every remaining place where the product still knows upstream's meaning. The program ends when that count is 0.

## 2. What was built

### 2.1 The split

- **The product** is everything outside `com.legend.edge` and the hosts' edge modules. It plans and runs lite's own
  forms only.
- **The edge** (`com.legend.edge`, about 78 files and 19,800 lines at d-stage `4e8702016`) makes upstream's bytes. By
  share of lines, measured then: 48% text kept alongside, 21% hook points that change the SQL that runs, 15% door
  adapters, 8% the referee after the run, 6% judging rules, 3% the service-test runner. The edge never edits SQL text;
  its four MIR rewrite passes are `PurePrintForms`, `TdsNullCells`, `UnguardedParses` and `StableScanOrder`.
- **Shipped artifacts start at the product (K2, the user's ruling of 2026-11-18 06:05: "Product root, edge
  opt-in").** `//core:server`'s main is the product server; legend-engine's routes load only with `--engine
  legend-engine`, through a `ServiceLoader` `MountProvider`. The query-store page is product-rooted (H6). The wasm
  planner and the native compiler follow the same shape (the last host train).

### 2.2 How the product meets the edge: hooks

A **two-form kind** is an interface the planner asks at planning time; the plan style (`PlanStyle`) picks which form
answers. Lite's form is the product's; upstream's form lives in the edge. The hook criteria, enforced by guards:

- (a) the kind is types only: no statics and no upstream logic on it (`TWO_FORM_EXCEPTIONS` stays empty);
- (b) the product answers lite's form or refuses by name;
- (c) every non-product form lives in the edge;
- (d) every hook is pinned in `HookRulingTest` (`HOOK_ENTRIES`, `TWO_FORMS`).

A **facet whose product form refuses** (for example `ResultEnvelopes`, legend-engine's JSON result envelope) is a ruled
hook (`HOOKS_RULED`); its argument types are `HOOK_ENTRIES`, tied to that facet alone.

### 2.3 The meaning rule (how a finding is judged)

What decides is what a thing **means** (lite's or upstream's), not who reads it or what it is named:

- a fact about the user's program (a declared type, an accessor, a mapping fact) is product API (`READ_API`), even
  when only the edge reads it;
- a hook's own result type that only the edge constructs is not a finding;
- a product method that computes upstream's alternative and hands it to a hook IS a finding;
- a branch on a two-form kind's answer is the designed mechanism;
- renaming a thing does not close a finding; changing what it means does.

### 2.4 The checks

| Check | What it proves |
|---|---|
| `EdgeSeparationTest` (BUILD, CODE, TEST, TEXT, RULES, AUDIT, EDGE-ONLY, LENS) | the census: every open line, by bucket. Zero means done. |
| `EdgeAuditOpenTest` | every audit item (J, K, H) still open, by id |
| `EdgeOnlyMembersTest` | no product member is called only by the edge (a transitive fixpoint), unless ruled `READ_API`, `HOOK_ENTRIES` or moved |
| L-A: `ProductFormsUseTheirArgumentsTest`, `UpstreamBranchesTest` | the product computes no upstream alternative and branches on no upstream shape |
| L-B: `EdgeOnlyDataTest` (`IN_MEANING`, `RULED_IN_MEANING`) | no product data that only upstream's forms use |
| `ShippedArtifactRootsTest` (`REACH_OPEN`, `ROOTS_OPEN`) | no shipped artifact roots at or reaches the edge except through the declared opt-in |
| `HookRulingTest`, `PlannerSurfaceTest`, `ServiceLoaderSitesTest` | the hooks and the planner's public surface are exactly the ruled ones |
| `shrink_only` (`//gates`) | no register, ledger or pin grows without a dated reason |
| `//pct:edge_contract_test` (the edge golden net) | every upstream-facing output on DuckDB, H2 and SQLite matches goldens recorded at `26717de291`, unless a ruled move re-records them |
| `//core:product_raise_probe` | the product's forms (typed raises, Float and DateTime text, carriers) on every database |
| lane rounds | PCT channels A and B plus the probe on the databases CI does not run (MariaDB, MySQL, ClickHouse, Trino, MongoDB, SQLite, Snowflake, Databricks, BigQuery) |

How to run the fast loop and the heavy round: `bazel test --lockfile_mode=error //gates:local`;
`bazel test //core:census //core:census_edge //core:guardrails //core:guardrails_edge`; `//gates:heavy` for the
heavy round (it includes `//pct:edge_contract_test` and `//core:cypher_parity_test`). Bazel in a cloud session:
`docs/CLOUD_SESSIONS.md`.

## 3. Where it ended

(Filled in at PR time: d-stage's head, the census, the last heavy round, CI.)

- **The count:** 0 on d-stage `<head>` (GUARD's census of record).
- **Trains:** `<N>` integration trains landed on `claude/total-wall-d-stage`, each through one integrator (D) with one
  heavy round.
- **The host train** (train 19) carried the Python, TypeScript and DataCube halves together, including plan format
  v57/v58, the user's two pin raises (below) and E-P5's K10 trim in the same train.

**The user's pin words in this program** (each typed in D's session; a pin grows only with the user's word):

- 2026-11-18 ~05:10: "Yes, raise both" (EnginePlans 762 → 763, AggAwareActivities 211 → 226).
- 2026-11-18 ~11:12: "Yes, raise both: K10 essential +40,874 (transitional), and v57 +31 per Decimal(p,s) column."
  E-P5's trim lands with it and ends the essential pin 526 below its pre-K10 value.
- 2026-11-18 ~11:35 (in the orchestrator's session): "yes to L14 +75" (lean_sql_baseline plan JSON on DuckDB core:
  TypedDrop +8, TypedLimit +25, TypedSlice +42, from explicit NULLS_LAST on formerly bare sort keys; SQL unchanged).

## 4. What went wrong, and what a redo should do differently

The user's own verdict (2026-11-18): the work is to be redone from scratch. These are the causes the orchestrator can
name, measured where possible.

1. **The worker sessions ran at the default effort.** The orchestrator's session ran at `xhigh`. Every worker it
   created (D, E-Java, E-SQL, F, GUARD, DataCube, E-P5, the host sessions, the lane sessions) was created with no
   effort level, so it ran at the default. The orchestrator never set it and never told the user. **Redo: set the
   effort level explicitly on every session at creation, at the level the user chooses, and say so in the first
   report.**
2. **The census measured where code lives, not how it works.** A line closed when its code moved, and the fastest move
   was byte for byte. About 1,978 edge lines still build legend-pure's text inside SQL (`RelationTextLowering` 780,
   `PureMessages` 479, `UpstreamVerdictWords` 191, `PurePrintForms` and `TdsNullCells` 184, and smaller), contrary to
   the plan's own §2.2 ("the edge is Java over typed results") and §4 item 1 ("no SQL that reproduces upstream's
   printed text"). **Redo: write the edge as Java over typed rows from the start; a guard that no edge class builds
   upstream text as SQL, except a short, user-ruled list (kind B).** This is the approved portability program.
3. **The safety net rewarded copying.** The edge golden net demanded byte-identical edge SQL, so redesigning a piece
   into Java looked like a regression. **Redo: compare outcomes (verdicts, answer bytes, messages), not the edge's SQL
   text.**
4. **The count grew late, when better lenses were added.** It went 9 → 31 on 2026-11-18 when the only-edge-callers
   guard and the meaning lenses (L-A, L-B) were added, and the host items H1-H11 were found outside the count. **Redo:
   build the meaning guards first, before moving code; count by meaning from day one.**
5. **Redesigns after building.** Examples on the last day: a hook kind given a static (`oneBare`), breaking
   criterion (a), so the fold had to move into the edge; an identity-set design that lost its marks across a rebuild
   and was redone as a neutral fact; two shapes built for one finding (PlanParam). **Redo: a worker sends the shape
   (what leaves the product, where it goes, the format and pin effect) before building, and the orchestrator rules
   first.**
6. **The host train paid for every Java landing.** Each Java train forced the host train to re-merge and rerun its full
   passes (about 1.5 hours each). **Redo: pre-merge continuously (the host train merges the open Java train while it
   tests), run only the merge-touched targets, and never hold a train for one item.**
7. **Very long-lived sessions.** Workers ran for days with 650K-730K tokens of context, so every turn re-read most of
   that context, which made each session expensive. **Redo: shorter sessions with written handoffs; one plan file as the shared memory.**
8. **Guard and ledger weight.** The registers, ledgers and shrink-only pins made drift visible, but every move needed
   a guard edit, a re-pin and a census run, often by a separate session. **Redo: fewer, broader guards that measure
   meaning; pins only where a number is a real budget.**

**What worked and is worth keeping:** the typed-result product (cells as typed values, errors as kinds with typed
operands); two-form hooks chosen at planning; the meaning rule; a census of guards as the count of record; one
integrator per staging branch with trains; lane rounds on every database; the user's rulings written down verbatim with
dates; nothing merged without the user's word.

## 5. What is left after this PR

**The next program (approved):** the edge as portable plans, `docs/records/edge-portability-program-2026-11-18.md`.
Its open decisions (§9) go to the user when it starts: imported plan nodes that need Java or Pure (refuse, or plan the
query again from its Pure source); each kind-B row re-ruled under "one plan"; `EngineResultEnvelopes` (SQL) beside
`PureV1Writer` (Java).

**Deferred until the count reached 0** (not done in this program):

- the VerdictSql ordering race (E-Java);
- item 3, `VerdictChannelRegister`;
- DuckDB Integer placeholder typing (E-SQL);
- pure/v1's compile parses at the LITE level with no register row (found by E-P5);
- the Lowerer builder's configuration into one final record, and `CodeShapeGuardrailTest.mutableInstanceStateIsExplicit`
  seeing multi-line field declarations;
- the fused shared typed per-assert read for the BOTH judge (the user's ruling of 2026-11-16 15:36);
- proposal A: drop the unread MIR `tree` from run plans (89% of plan JSON), a format change for the user.

**Follow-ups F1-F52** in the plan of record's §9 (lists done properly, `toOne` by counting, `type()` at run time, the
Decimal print form, lean-SQL items, edge fidelity items, judge size, the cross-language fix batch, PR #102, compiler
performance (waiting on the user's choice of fixes), stream S (injection safety, its session stopped failed),
lineage, and the rest).

## 6. The sessions (left idle, not archived, by the user's choice)

| Session | Role | Last state |
|---|---|---|
| `session_01XmdJ7k8FEjxWeePpScQtuR` | orchestrator (xhigh) | this document |
| `session_01XmdJ7kRamr4Sexmcrguc94` | D: the integrator of d-stage | (at PR time) |
| `session_01XmdJ7mGZpJB7BmQRiqXNSR` | E-Java: judges and Java | idle |
| `session_01XmdJ7jKZMVTrr2TX1aqSzN` | E-SQL: lean product SQL, lowering | (at PR time) |
| `session_01XmdJ7iF9einRFCeFCpks6y` | F: the presenter, plan style | (at PR time) |
| `session_01XmdJ7krBsi9f7ZVuyLUf11` | GUARD: the census and the guards | (at PR time) |
| `session_01XmdJ7jxuQqKFD955g8QWGc` | DataCube: the host train's integrator | (at PR time) |
| `session_01XmdJ7ibnPFMP2CDDcXwRhi` | E-P5: pure/v1, K10, the server | (at PR time) |
| `session_01XmdJ7jDL3eka1mimhpFnLR` | the TypeScript host | (at PR time) |
| `session_01XmdJ7jxDz3DX8jUiMTqS6a` | the Python host | (at PR time) |

## 7. Verifying the PR from another account: the audit and the database lanes

The user's plan (2026-11-18): once the PR is open, a session in another account audits it at xhigh effort and runs
every database lane in parallel with the PR's CI. Nothing below needs this account; everything is in the repository or
in the environment you create.

### 7.1 What changed in the final train (look here first)

The final train is `claude/total-wall-h-train19` (DataCube's integration of the old trains 19 and 20) landed on
`claude/total-wall-d-stage`. The changes most likely to break something:

| Change | Owner | Risk to check |
|---|---|---|
| **L10: union frame and WHERE conjunct order** (`LoweringForms.unionFrame`, `where`; the rename framing was dropped on 2026-11-18 because inlining a renamed projected source broke `ScaleCostsDuckDbTest.aDoublingChainStaysLinear`, 4 -> 16 reads) | F | Changes the **product's SQL shape**: one subselect fewer on some channel A shapes (BigQuery, Databricks, MariaDB, MySQL, Trino measured), and conjuncts appended, not reordered. Answers must not move. New product rule: never prune a UNION DISTINCT's columns. |
| **executeValues** (the form takes the product's own serialization: the object when to-one, the wrapped array when many) | E-SQL | No product answer may move; the edge folds the array with `oneBare`. |
| **The fold move and L9's remainder** (computedPruning, existsJoin composed by the edge) | E-SQL (train 18) | Edge contract byte-identical; criterion (a), no statics on a two-form kind. |
| **K2: shipped artifacts start at the product** (`//core:server`, `//wasm:planner`, `//native:compiler`, `//query-store:page`; legend-engine via `--engine`, `//wasm:engine_planner`, `//native:engine` and the `legend-lite[engine]` extra) | E-P5, the TS host, the Python host, DataCube | An edge-off run must load no edge class; an edge-on run must answer legend-engine's routes byte-exactly (`//wasm:engine_routes_test`, the Python edge tests). |
| **K10: a raise site carries lite's message only** (no `upstream` field; plan v58) | E-P5 | legend-pure's words come only from the edge's recorder; PCT channel B unchanged. |
| **v57: a precise decimal's digits as plan data** (`decimal: {precision, scale}`, omitted when not a decimal) | DataCube | +31 plan characters per Decimal(p,s) column, approved by the user; no reader yet (follow-up D2). |
| **L14: explicit NULLS_LAST on sort keys** | F | SQL unchanged; +199 plan characters on DuckDB core (approved / delegated). |

### 7.2 The lanes

CI runs DuckDB, H2 and Postgres (Linux). MariaDB, MySQL, ClickHouse, Trino, MongoDB, SQLite, Snowflake, Databricks and
BigQuery run only in lane sessions (the plan of record's §5.1).

**Use your warm lane sessions.** The reviewing account already has warm sessions with the databases and their
credentials set up. Give each one the brief below; nothing needs to be set up again. (Only if a lane has no session:
`docs/RESULT_BOUNDARY_START_HERE.md` §7 and `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 and §6 describe the setup;
credentials come only from an environment's setup script, never the repository or a message.)

**What to run on each database, at TWO commits** (the base, `claude/total-wall-d-stage` before the final train, i.e.
`815de5810`, and the PR head), then compare per test:
- **PCT channel A:** `//pct:pct_<db>_<suite>` for each suite: `essential`, `grammar`, `relation`, `standard`,
  `unclassified`. The databases are `duckdb`, `postgres`, `sqlite`, `mariadb`, `mysql`, `mongodb`, `trino`,
  `clickhouse`, `snowflake`, `databricks` and `bigquery`; the clouds read `LEGENDLITE_PCT_CLOUD_PROPERTIES`. H2 has one
  target, `//pct:pct_h2` (the relation suite). The expected
  failures per lane are the ledgers `Test_LegendLite_<Lane>_<Suite>_PCT.java`.
- **PCT channel B:** `//pct:pct_channel_b_<suite>` (DuckDB) and `//pct:pct_channel_b_<lane>_<suite>` for the lanes
  `postgres`, `sqlite`, `mariadb`, `mysql` (plus the H2 variant). Verdicts must be identical per test.
- **The product probe:** `//core:product_raise_probe` with `LEGENDLITE_PROBE_DATABASE=<db>` (mariadb, mysql,
  clickhouse, trino, mongodb, or a cloud with the properties file). It prints each statement's SQL: typed raises, the
  Float and DateTime forms, values that must read back exactly. Zero wrong values.

**The brief each lane session gets** (plain text; the Snowflake session refused encoded blobs once):
> MEASURE ONLY: no commits, no pushes, no credential reads, no tracked file edited. `git fetch origin <branch>`;
> `git checkout --detach <commit>`; a clean tree. Run channel A and channel B (where the lane has it), all five suites,
> and the product probe, on <databases>, at the base `815de5810` and at the PR head. Report one table per database:
> every lost pass, every moved message and every channel B difference in full (test name, expected and actual text),
> the probe's wrong values, the route markers counted (`grep -c '\[LegendLite PCT\] engine JSON:'`), and the wall time
> of each suite at both commits.

**Reading the reports:** `docs/result-boundary-homework/probes/compare_lanes.py.txt` classifies channel A failures by
wording (STALE, MOVED, LOST). **Pass means:** no lost pass, no channel B difference, probe zero wrong, on every database.
A moved message on an already-failing test is reported, not a failure.

**Known, not regressions:** lane round 77 (2026-11-17) measured slowdowns from the host judge's per-side reads (2.55x
round trips): Databricks about 2x, Trino 30-50%, MariaDB/MySQL/ClickHouse 13-23%, BigQuery 1.3-2.5x. That is follow-up
C7, not this PR's to fix. The base-versus-head comparison above shows whether the final train adds anything.

### 7.3 The audit's questions

Grade every finding as one of: wrong answer, wrong design, broken rule (AGENTS.md, the goals of record G1-G11), or
cosmetic.
1. Does the product, alone (edge off), contain anything that means legend-engine's or legend-pure's form? (The meaning
   rule, §2.3.) The guards say no; look for what they can't see.
2. Is the edge Java over typed results, or does it still build upstream text in SQL? (It still does in about 1,978
   lines; follow-up D1. Confirm the number; it should not have grown.)
3. Do the final train's SQL-shape changes (§7.1) keep every answer on every database? (The lanes answer this.)
4. Is any guard or pin weakened without a dated reason? (`shrink_only` is the mechanical check; read the dated notes
   in the final train's diff.)
5. Is anything in `docs/records/edge-follow-ups-2026-11-18.md` actually a blocker for merging?


## 8. The final sequence (2026-11-18), written for the reviewing account

### 8.1 What the PR holds, in order

1. **Train 18**, landed on d-stage at `815de5810` (12:31 UTC, 31/31 heavy-round targets green). This is the **base**
   for the lanes' comparison in §7.2.
2. **The final train**, `claude/total-wall-h-train19`, integrated by DataCube (merges only; ratchet conflicts resolved
   key by key). It holds the old trains 19 and 20 batched together (the user, 12:36: "finish everything and then one
   last test"):
   - GUARD's final guards (`3af89e4bde`): census 1 on the train, only L-A.unionAlias left. The audit list, EDGE-ONLY,
     REACH_OPEN and ROOTS_OPEN are empty. Closed in this train: H5, J18, J36, J9, K10.
   - The host halves of K2: the wasm planner (the TS host, `17a87ddff5`), the native compiler and H5 (the Python
     host, `claude/total-wall-h-python-k2-final`), and DataCube's part (`a47a16b4`). J40.ModelInput was moved by the
     TS host (`claude/total-wall-host-modelinput`). J40.Compiler was ruled READ_API (`Compiler#compileErrors` is a fact
     about the user's program).
   - E-P5's K10 (a raise site carries lite's message only; plan v58) with its trim.
   - F's L14 (explicit NULLS_LAST; SQL unchanged).
   - **F's lean-baseline regen** `dc923b45f` (merged as `90cf4edad`): exactly 40 rows, every one an approved raise.
     39 rows are v57's +31 per Decimal(p,s) column (the user, ~11:12, reconfirmed 11:58 "keep it"). One row is
     DuckDB TypedSort +124, L14's part, under the 11:45 delegation.
   - **E-SQL's executeValues** `7591f37b7` (merged as `1dd715ab2`). E-SQL measured identical lean rows with and
     without it, so it changes no product SQL and no answer.
   - **F's L10** (the user, 13:05: "Include both"), narrowed at 13:19: item 2 (rename framing) was DROPPED, because
     renaming a projected source in place inlines its computed projections. That broke
     `ScaleCostsDuckDbTest.aDoublingChainStaysLinear` (the base column read 16 times instead of 4), so the extra
     nesting level is a product cost guard. L10 is now the union frame (no extra "unionalias" wrap), the new product
     rule never to prune a UNION DISTINCT's columns (with a test that fails without it), and WHERE conjuncts appended
     rather than reordered. `WhereMerge.merge`/`Zones` moved to the edge. F's first measurement: the edge contract,
     the edge corpus, both corpus verdicts and channel B were byte-identical. Product SQL moved on channel A only:
     BigQuery, Databricks, MariaDB, MySQL and Trino each lost a subselect (-129 to -265 SQL chars) and gained +7
     "repeated chars", accepted under the delegation with the reason in F's commit. Re-measured after the narrowing;
     the final figures are in F's commit message. **This is the change the database lanes must check most closely.**
3. **D's landing push** on d-stage: the final train's head, this document and the follow-ups document, and the
   deletion of `.github/workflows/stage-linux.yml` (a throwaway Linux-only workflow for D's staging branch; it never
   goes to main).

The census of record on the PR head: **0** (filled in from GUARD and D at PR time: `<census on head>`).

### 8.2 What ran where

| Where | What | Who |
|---|---|---|
| **The PR's CI** (`gate.yml` / `gates-run.yml`) | every lane in `gates/BUILD.bazel` on Linux, macOS and Windows (product, core, checks, corpus_duckdb, corpus_h2, pct_duckdb, pct_h2, pct_postgres, pct_channel_b, parser_equivalence, stress, warehouse, datacube, ui, sdlc), the warehouse on Linux arm, the product build, and `shrink_only` against the merge base | GitHub, started when the PR opens |
| **D's heavy round** | the same full set as train 18's (31 targets from `//gates:heavy`, the manual suite CI never runs), including the edge golden net `//pct:edge_contract_test`, plus the build sweep, on the PR's own commit, in parallel with CI | D (`session_01XmdJ7kRamr4Sexmcrguc94`); results in §8.3 |
| **The database lanes** | PCT channel A and B and the product probe on MariaDB, MySQL, ClickHouse, Trino, MongoDB, SQLite, Snowflake, Databricks and BigQuery, at `815de5810` and the PR head (§7.2) | the reviewing account's warm lane sessions |
| **The audit** | §7.3's five questions, at xhigh | the reviewing account |

The census and guardrails (product and edge) are in CI's `checks` lane; cypher parity is in CI's `core` lane. The
edge golden net is NOT in CI (follow-up D6/F33), so D's heavy round is its only run on the final commit.

### 8.3 Results (filled in on the live copy as they arrive)

| Check | Commit | Result |
|---|---|---|
| D's heavy round (31 targets) + build sweep | `<PR head>` | `<pending>` |
| The PR's CI | `<PR head>` | `<pending>` (PR `#<N>`) |
| GUARD's census | `<PR head>` | `<pending>` (expected 0) |

### 8.4 What to expect, and traps

- **Any push to the PR branch restarts CI.** `gate.yml` cancels an in-progress `pull_request` run on a new push, and
  its `**/*.md` ignore does not help once the PR's diff holds code. Do not push documentation to the PR branch while
  CI runs. Write to the live copy on `claude/total-wall-orch` instead.
- **A red found by D's round or CI** is fixed by a NEW commit on the PR (no force push, no rebase). That restarts CI
  once.
- **Known infrastructure noise:** the `sdlc` lane has failed on a Maven HTTP 429 (rate limit) before (d-stage run 19).
  That is infrastructure, not code; a re-run decides it.
- **`lean_sql_baseline`:** a 40-row red was seen on `28c25b40a`, which predates F's regen. It is not a regression. On
  the PR head it must be green. Any row that grows needs an approved raise, named in the commit that grew it (§3).
- **Known slowdowns on the external databases** (lane round 77, follow-up C7): see §7.2. The base-versus-head
  comparison decides whether the final train added any.
- **The sessions** are left idle, not archived (the user's choice). The workers ran at the default effort (§4 item 1),
  so treat their reports as claims to check, not as verdicts.
