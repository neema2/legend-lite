# The edge program: handoff for a redo (2026-11-18)

**Status: DRAFT, written while the last trains land. The final numbers (d-stage's head, the census, CI) are filled in
when the PR opens.** This document goes with the one PR that takes `claude/total-wall-d-stage` to `main`.

**Why it exists.** The user decided on 2026-11-18 (11:25 UTC): "Finish to zero, then PR with a detailed hand off
document". The user means to redo this work from scratch later, in another session or account. This document is for
that redo. It records what was built, how it is checked, what the user ruled, what went wrong and why, and what is
left.

**Where the detail lives** (all on branch `claude/total-wall-orch` unless named):

| Document | What it holds |
|---|---|
| `docs/records/total-wall-edge-program-2026-11-16.md` | The plan of record: the goal (§1), the design (§2), the rulings verbatim (§3), what we would not do (§4), the checks (§5), the classification table (§6), the phases (§7), the follow-ups F1-F52 (§9), the rulings asked of the user (§10), and the dated log (§12, newest first). |
| `docs/records/edge-audit-2026-11-17.md` | The audit list: items J1-J43, K1-K16, the host items H1-H11 and the lens audit L1-L14, each with its file, lens, ruling and owner. |
| `docs/records/edge-portability-program-2026-11-18.md` | The next program, approved by the user: the edge as portable plans (one plan, two presentations; import and export of upstream plans). |
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
