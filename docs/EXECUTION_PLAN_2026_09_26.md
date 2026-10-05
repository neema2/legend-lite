# The execution plan: the whole compiler, rebuilt stage by stage behind the oracles

**Rev H4, 2026-09-29 — the re-cut.** This is the ONE living plan. History: rewritten 2026-09-28 after the whole of `core` was read stage by
stage (`plan-audit-2026-09-26/architecture-review-2026-09-28.md`, `stage-readings-2026-09-28/`); rev H1 after an
adversarial audit of every wave (`plan-audit-2026-09-26/h1-plan-audit-2026-09-29/`); **rev H2 after a six-lens meta-audit
of the plan itself** (`plan-audit-2026-09-26/meta-audit-2026-09-29/`, synthesis in its `README.md`) and the user's rulings
D13–D18; **rev H3 after a five-reviewer tractability audit** (`plan-audit-2026-09-26/tractability-2026-09-29/`, synthesis
and the engineering decisions E1–E13 in its `README.md`); **rev H4 re-cuts the ORDER after a macro review the user
approved: fix and learn first, decide at C1, then the middle by risk and value, the front end where it pays (§4).** Rev H1's text is in git history at `89dc45871`. Audit findings
are cited as `[W2 #5]` (H1 report `W2-resolved-tree.md`, finding 5), `[L3 #4]` (meta-audit lens 3, finding 4) and `[T4]`
(tractability report 4). **Paths without a module prefix are under `core/src/main/java/com/legend/`.**

The research files in `plan-audit-2026-09-26/` are the homework; this page says what to build, in what order, gated how.
Keep it current: when an item lands, move it to §3 with its GATES.md heading, and update §0's "Now" line in the same push.

---

## 0. Start here (a fresh session with no context)

**Now (update in every push):** **the D24 cleanup phase, one substitution engine first** (it is self-contained in `compiler/spec`; the variable ids touch 35 files and 30 of them are the store resolver's, so they go with the middle). D23's tool and damaged data are in (GATES "Rebuild D23 (1)", "(2)"; S22); attribution of the remaining seed disagreements and the next damage kinds continue beside the cleanup. The seed-data comparison is in (GATES "Rebuild D23 (1)": 22 row disagreements, 21 engine-only, 6 lite-only, dossier in `plan-audit-2026-09-26/wrongrows/`). Next: (a) `tools/wrongrows/damage.py`, the damaged data set from the seeds; both engines on it; (b) every disagreement, seed and damaged, attributed to a stage (H, I, J) and a side (engine defect → register; lite defect → the list) with the files a fix would touch; (c) the mapping-heavy set named by FQN. Then the remaining W0.6 pushes (9, 4, 5, 5b, 10 judged by the tool; 6, 6b; 12). The scope, design choice,
tests and gate of every W0.6 push are in `plan-audit-2026-09-26/w0.6-homework/README.md` §"Push list" (homework and a
dry run against the code done); **the order is §4 Phase 1's (D22, D23): the six small pushes now (2, 3, 7, 8, 11, 13);
then the wrong-rows tool over the stress corpus (W1.10, as rewritten under D23); then the remaining pushes, each judged by
the engine's rows where rows are the question.** Open decisions that block only single fixes: D20, D21. Then the rest
of **Phase 1** (§4): W1.0b, W3.7, W0.7, and **C1**.

**What this program is, in one paragraph.** legend-lite (`core/`, ~229k lines of product Java) is a clean-room
replacement for legend-pure's compiler and legend-engine's query execution: Pure text → parse → resolve names → type →
inline → resolve classes to tables → SQL → run on DuckDB/H2. It works (corpus rosters, PCT lanes), but its middle is
tangled: a typer that was 3,500 lines (split to 1,748 by W1.6, still one class doing too much), a 36k-line one-pass store resolver, names compared as strings, semantics carried as flags
that rebuilds drop. The program rebuilds it stage by stage into a conventional expert compiler (§1), landing on `main`
slice by slice, every slice gated green, never a big bang. Goals (D17): cleaner, more bulletproof, much less code,
faster, the cruft deleted, and wrong answers found and fixed first.

**Words used on this page.** *Stages* A–K are the pipeline in `AGENTS.md` (A–C parse, D resolve names, E prepare
mappings, F build declarations, G type, **G½** inline and fold, H classes to tables, I build the SQL tree, J print SQL, K
run). *HIR* = the typed tree G produces (`TypedSpec`); *MIR* = the SQL tree I produces (`com.legend.sql`). *Reference lane*
= `//spec:reference_lane`, which compares, call by call, what legend-pure's own compiler resolves with what lite resolves;
its buckets (AGREE, OVERLOAD, ABSENT, EXTRA, "bodies FAILED") are defined in GATES "Rebuild W1.1 (1)". *Rosters* = the
committed lists of corpus tests that fail today; *LOST* = a test newly failing, *GAINED* = newly passing. *Probe rows*
(CANDIDATES, PICK, OVERLOADS, UNKNOWN-FN) are the counts `--test_env=LL_SHADOW=1` writes (`probe/Shadow.java`,
`tools/untangle/probe_counts.py`). *PCT* = legend's Platform Compatibility Tests (per-function semantics); *PMCD* = Pure
Model Context Data (the engine's JSON model input); *M2M* = model-to-model mapping; *LUB* = least upper bound of types;
*FEP*, *TIC*, *FM*, *GTM*, *MM* = legend-pure's FunctionExpressionProcessor, TypeInferenceContext, FunctionMatch,
GenericTypeMatch, MultiplicityMatch (under `legend-pure-m3-core/.../m3/`). *E.6* = the normalizer's re-resolution step
(`h2-resolved-expr-design-2026-09-29.md` §3). *Register row Sn* = a row of `docs/SEMANTICS_REGISTER.md`.

**Checklist, every session:**
1. `cd ~/legend/legend-lite/.claude/worktrees/build-audit` (one session owns the repo since 2026-09-29; untracked `nlq/`
   is not ours, leave it). `git fetch origin && git status -sb`. Work on branch `compiler/rebuild`; it must equal or be
   ahead of `origin/main` (D18: every gated slice is pushed to both). If `origin/main` moved, `git merge --ff-only
   origin/main`; if that fails, stop and ask.
2. Read, in order: this §0; §2 (decisions, do not re-ask a ruled one); the item's entry in §4 and the homework it cites;
   the item's stage reading in `stage-readings-2026-09-28/`; the three newest `— Rebuild` entries in `docs/GATES.md`
   (they are NOT at the end: `grep -n '— Rebuild' docs/GATES.md | head -3`; rebuild entries are newest-first, starting
   near line 5685). `AGENTS.md` (repo root) holds the architectural invariants; `docs/TENETS.md` and
   `docs/TENET_CHARTER.md` the tenets. `docs/SEMANTICS_REGISTER.md` lists every deliberate difference from legend-pure
   and legend-engine.
3. Pinned upstream sources (spec, read-only): `OB=$(bazel info output_base)`; `$OB/external/+http_archive+legend_pure_src`
   (5.99.0) and `$OB/external/+http_archive+legend_engine_src` (4.145.0); jars at the same releases in `@maven_upstream`.
   `find` needs `-L` there. Never use another cache (one holds pure 5.103.0). The `~/legend/legend-pure` and
   `~/legend/legend-engine` checkouts lag the pins; do not cite them.
4. The slice (rule 0b.6): homework → probe → switch → gate → deletion → GATES.md entry → push.
5. Before the full chain, run the lanes that catch per-file pins (three of four pushes on 2026-09-29 went red here
   first): `bazel test //core:guardrails //core:census //parser-equivalence:parser_parity //spec:spec_tests`. If you
   added, moved or renamed a file, check the registers that name files: `JdbcSurfaceCensusTest`,
   `ObservabilityGuardrailTest.ENV_FLAGS` (any new `LEGEND_LITE_*`/`LL_*` variable), `OwnCorpusParityTest.MIN_MATCHED`,
   `ErrorShapeGuardrailTest` (broad-catch counts per file), `ArchitectureTest.PROTOCOL_DESUGAR_DEBT` (per file),
   `JavaEvalLedgerTest` (exact line pins), `IdentityGuardrailTest`; regenerate generated files with `bazel run
   //:update_generated` (the claims ledger — `native-claims.tsv`, which native is implemented where — has an `also`
   column that moves with any file move).
6. The gate chain on the exact tree: `bazel test //...` then `bazel test //tools/deps:all` (Bazel's wildcard over that
   package; `//...` already contains those five tests, and the second command is the user's standing rule naming them
   explicitly, so it normally reports `(cached)`). Read the summary line (`Executed N out of M tests: M tests pass`),
   never only the exit code. For any front-end slice also `bazel test //spec:reference_lane` (manual, 8 GB);
   "front-end" = any change under `lexer/`, `parser/`, `protocol/`, `model/`, `builtin/`, `compiler/` (including
   `compiler/spec`, so G½'s inliners count) or `normalizer/`. Read per-test times of the corpus lanes against the
   previous entry: a green lane that got 10× slower is a regression. **Rosters are floor AND ceiling**
   (`MinimalCorpusTest.java:29-43`): a fix that makes a corpus test pass turns its lane red as GAINED; remove the name
   from `spec/src/test/resources/rcorpus/<lane>-fail-roster.txt` (and any database-mode register naming it) in the same
   push, with the reason in the GATES entry. `docs/RELATIONAL_CORPUS.md` is a Maven-era scoreboard the Bazel chain does
   not regenerate: judge by the roster files. `//parser-equivalence:diagnostics` runs only on its triggers (a pin bump, a
   parser/lexer/protocol change, a corpus manifest change). Manual lanes (`//spec:reference_lane`,
   `//core:scale_stresstest100k`) never run inside `//...`: run them by name when an item's gate names them.
7. One test class: `//core:core_tests` is one package-wide `junit_test` and ignores `--test_filter`
   (`tools/junit/defs.bzl:25-61`); run the target, or drive the jars in `bazel-bin/core/core_tests.runfiles` from jshell
   (`"$(bazel info output_base)/external/rules_java++toolchains+remotejdk25_macos_aarch64/bin/jshell" --class-path
   "$(find -L bazel-bin/core/core_tests.runfiles -name '*.jar' | tr '\n' ':')"`, after one `bazel build //core:core_tests`). One corpus test:
   `--test_env=JAVA_TOOL_OPTIONS=-Drcorpus.test=<fqn>`. A PASSING `@KnownDefect` test means the defect is still present.
8. A timing is a lane run alone with `--nocache_test_results`, `uptime` load under 3 at the start, nothing else
   building. A lane's time inside `bazel test //...` is not a timing.
9. Commit: write the message to a file, then `git -c user.name=neema2 -c user.email=neema2@gmail.com commit -F <file>`;
   the message ends with the trailers the harness's attribution reminder gives (today `Co-Authored-By: Claude Opus 5.5
   <noreply@anthropic.com>` and `Claude-Session: <the session URL in that reminder>`; if the harness gives none, the
   `Co-Authored-By` line alone). Push: `git push origin HEAD:compiler/rebuild HEAD:main` (fast-forward only).
   Never force-push; never bare `git stash`.
10. GATES.md entry (insert above the newest `— Rebuild` heading), template:
   `## <date> — Rebuild <item>: <one-line result>` then: what changed and why (files); the probe and its numbers; the
   gate lanes with their summary lines and quiet timings if any; pins moved, each with its reason; what was deleted
   (lines); the program number the item moves (§1a); cost (commits, wall time, chain runs, red first chains); the
   receipt path.
11. Receipts: `~/legend/platform-architecture/receipts/rebuild-<item>-<short-sha>/` (not in git; older folders lack the sha): probe outputs, lane
    logs, timing tables; the GATES entry names the folder.

**Definition of a session:** one working context of one agent, including the subagents it launches. Sizes in this plan
are judgement; the first week showed mechanical and probe items overestimated 3–5× and re-planning as the real cost
driver [L5 §2]. Every GATES entry logs the item's actual cost; sizes are re-fitted at checkpoint C1.

---

## 0b. Rules

1. **Homework before code, from primary sources**: the pinned trees and our code, by file:line.
2. **Probe before switch**; the probe's receipt is saved before the switch and the switch is judged against it.
3. **The gate is a few oracles plus the numbers** (§1c). The core gates, where they exist: corpus rosters LOST 0 (by the
   roster files); rows equal to legend-engine's on the mutated fixtures (W1.10, once built); the reference lane's
   disagreement set not grown and its coverage pins not shrunk (front-end items only); the product-SQL snapshot
   byte-identical for refactor slices (W1.7, once built); `bazel test //...` and `//tools/deps:all` green; timings read
   alone and quiet. Tools such as the verifier and lint (W1.3) and the guardrail tests support these; they are not extra
   ceremony to satisfy (W1.14 prunes the ones that catch nothing). **Every item states its own gate**; an item without
   one is not ready.
4. **Deletions land with the switch.** A pin that moves carries a dated reason naming the item.
5. **One variable at a time.** Names change with the world held constant; the world changes with names held constant; a
   new tree lands with today's rule, and the rule changes in a later push.
6. **Every slice:** homework → probe → switch → gate → deletion → GATES.md record → push to `compiler/rebuild` and `main`.
7. **A timing is a lane run alone** (§0 step 8).
8. **Standing rulings:** no string identity for a declaration; no PCT or category checks in the compiler; no caches or
   memos for slowness before the algorithm is proven right (D10's per-model query memo is the semantics of demand-driven
   typing, ruled, not a performance cache); every deferral pinned with an owner; we own everything the program needs.
9. **"Strict" means: collect every diagnostic in one pass, poison the failed unit, fail the build on any error**
   (ruled 2026-09-28). Never abort on the first error; never accept bad input. D10 rules the unit.
10. **Every new stage gets its own IR type.** A phase boundary is proved by javac, not checked at run time; every switch
    over an IR family is exhaustive with no `default` (enforced by W1.11's checker).
11. **Defects (narrowed by D14):** a reproduced wrong answer, wrong binding or security defect is fixed now, in place,
    with a correct targeted fix, and its test becomes a regression test. Only a cosmetic or diagnostic defect whose fix
    belongs to a later wave is pinned `@KnownDefect(owner, reason)` (W0.0); the pin flips when the owner lands. A fix
    that is only correct in the new structure is reported and pinned, never patched around.
12. **Every stage is its own Bazel target, with only the dependencies it should have** (ruled 2026-09-29). A rewrite is
    done only when its stage is carved out as a target whose direct dependencies match the measured map (§1b).
    `//tools/deps:core_layering_test` compares Bazel's graph (a genquery per target) with `tools/deps/core-layers.txt`
    exactly; W1.12 hardens it.
13. **Outcomes, not mechanism (D15).** Lite matches legend-pure's observable outcomes (accept/reject and error class,
    the chosen declaration where it changes lowering or result type, the result type and multiplicity) and legend-engine's
    rows. It never copies a reference's nondeterminism (hash-order ties), its self-declared bugs, or its internal
    representation (e.g. automap `map` nodes, `extractEnumValue` strings). Every deliberate difference is a row of
    `docs/SEMANTICS_REGISTER.md` with its evidence and owner; the reference lane pins it by class.
14. **Oracles are cited for what they can see (§1c).** DuckDB-equals-H2 is evidence for dialect work only, never for
    store resolution or lowering; the reference lane is evidence for resolution and types only.
15. **Every item names what it deletes and the program number it moves (§1a).** Code that a new structure replaces is
    deleted in the push that switches, not later.
16. **One source of status:** this page's §0 "Now" line and §3. `docs/IN_FLIGHT.md` points here. A decision is recorded
    once, in §2, with the date, who ruled, and (for OPEN ones) when it must be decided.
17. **Net deletion** (the re-cut, D17): every GATES entry states the push's net change in product lines (`*/src/main`,
    W1.0b's counter). From Phase 3 on, an item ends net-negative, or its entry says why it could not; every phase ends
    net-negative. New code is written to replace old code in the same item, not beside it for later.
18. **Build, don't re-plan** (the re-cut): no whole-plan audit until C1. Between checkpoints each item is checked by its
    own homework and gate; the plan is edited only when an item lands or a finding changes a later item.

---

## 1. The target (what "expert" means here)

The product is judged by outcomes against legend-pure (resolution, types, accept/reject) and legend-engine (rows), under
rule 0b.13. The architecture is a conventional production front end (how rustc and Roslyn are built) and a query compiler
back end (how Calcite is built):

| # | stage | IR out (a distinct sealed type) | identity it carries | today's code | stage reading |
|---|---|---|---|---|---|
| A–C | lex, parse | syntax tree (`protocol` records, `ImportScope`), wire trivia in a side record | spans | lexer/, parser/, protocol/ | A01, A02 |
| W | World (eager knowledge) | declaration tables per kind; per-section import groups; C3 linearizations and variance | `FunctionId` (the reference's element name); `ClassId`/`EnumId`/`StoreId` as typed FQN wrappers; `DeclId` handles internal to the tables | model/, builtin/, compiler/ModelBuilder and friends | A03, A04 |
| D | resolve | the resolved declaration family carrying `ResolvedExpr` (D12) | calls: `Candidates(List<FunctionId>)`; dot access `Member(name)`; binders `VarId`; element references `Ref<Kind>`; every node an `ExprId(body, local)` | compiler/NameResolver | A04 |
| F | elements | typed declarations keyed by id | ids | compiler/element | A04 |
| G | type | typed HIR: every node typed; every call ONE declaration **with its recorded instantiation** (type and multiplicity arguments); every member its `PropertyId` | ids, `VarId`, `ExprId` | compiler/spec | A05, A06 |
| E′ | mapping elaboration (after F) | typed mapping IR: per set a typed relation expression; bindings keyed by a sealed `BindingKey` | ids, spans | normalizer/ | A06 |
| G½ | inline, shape-evaluate | typed HIR; substitution by `VarId` with the recorded instantiation; **no unification and no overload choice in G½** | fresh ids per copy | UserCallInliner, StaticFold, SourceSubst, AlphaRename, StatementInline, LiteralMapUnroll | A05 |
| R | **algebraize (if the D11 experiment passes)** | a logical relational algebra (Scan, Filter, Project, Aggregate, Window, Join, Sort, Slice, SetOp, Unnest) over class-level scans, with a scalar family; semantics as node properties: equality kind, null-strictness, determinism/collation, nullability | node ids | none today (forms are typed kinds; ~38 lowering sites decide relation vs scalar at run time) | A05, A09 §3 |
| H | store resolution, as passes | if R: rewrites of the algebra (mappings are views; navigation becomes joins); if not: a relational skeleton (routed sets, a join tree, `ColumnRef(JoinNodeId, column)`) with typed-HIR scalar leaves | `SetId`, `NavPath`, `JoinNodeId`, `PropertyId` | resolver/ (35,925 lines; four sequential sub-passes, one mutable state) | A07-A08 |
| I | lower | SQL MIR: semantic nodes (units, parts, join kinds, equality kinds as distinct records); block formation by the `Fold` predicates | `FunctionId` → one immutable rule table | lowering/ | A09 |
| J | legalise + render | SQL text per dialect; each dialect declares its required session settings | — | sql/, sql/dialect | A10 |
| P | plan | staged plan IR (SqlExec, Sequence, Allocation, Effect barrier, late-bound SchemaProbe/DynamicPivot/ForEach with bind parameters, VerdictBatch) | — | plan/, StatementExecutor | A11 |
| K | run | results | — | exec/, a thin runner | A11 |

**Cross-cutting machinery (each owned by an item):** `Diagnostic(code, severity, phase, span, args)` in a sink from
every stage, read by the LSP (W1.2); a **pass manager** that runs every stage through one pipeline object with the
verifier, a type-consistency lint, dumps and timings between passes (W1.3); per-pass golden dumps with a parseable text
form (W1.5); a **memoized query layer** (`parse(unit)`, `declIndex`, `resolveBody(id)`, `typeBody(id)`,
`specialize(id, args)`, `elaborate(mapping, set)`) keyed within a model snapshot, so compile-all and demand-driven are the
same computation (W2.2b, D10); an **exhaustiveness checker** (W1.11); the oracles of §1c; the semantics register.

---

## 1a. The program frame (D17)

**Users** (from code, not from any earlier plan): DataCube (the browser app; plans queries in the browser through the
TeaVM WASM planner, `wasm/README.md`, `datacube/src/wasm-planner.ts`, and speaks legend-engine's API to `server/`);
legend-engine API clients (Studio and services, `server/PureV1Api.java`); model authors in an IDE (`/lsp`,
`server/PureLspServer.java`); the planned warehouse/server modes (`docs/SERVER_PROGRAM_2026_09_26.md`, paused while this
program runs).

**What each phase gives users, and the number that must move** (printed into GATES.md by a tool, never hand-typed):

| phase | user outcome | the number (baseline printed by W1.0b unless given) |
|---|---|---|
| 1 | no known wrong answers; we know where the remaining ones are; the middle's design decided | open wrong-results defects 14 → 0; engine-row disagreements on mutated fixtures, found and attributed |
| 2 | changes can be proven safe cheaply; the IDE shows positioned diagnostics | SQL snapshot and dumps in place; per-push red-chain rate down; positioned diagnostics % |
| 3 | routing, joins, milestoning and SQL proved on adversarial data; the middle smaller | engine-row disagreements → 0 or registered; resolver lines 35,925 → down; lowering lines → down |
| 4 | overload picks and types match legend-pure by our own solver; names bound by id | OVERLOAD rows 769 → pinned residue; "reference typed, we FAILED" bodies 1,342 → shrinking; heap after resolve on `//core:scale_stresstest100k` |
| 5 | a thin runner; a multi-request-safe server; dialect breadth | request-reachable static state → 0 (W0.7 starts it); PCT fail rosters by class |
| every phase | less code, faster | net product lines (229k at `89dc45871`) → down (rule 0b.17); plan latency p50/p95; WASM bytes; `//wasm` cold start; 100K-model build time (15.1 s measured at `327d43365`, GATES "Rebuild W0.6 push 1"; an earlier 2.7 s on this page was not reproduced on this branch; W1.0b prints the baseline) and heap |

Known baselines at `89dc45871`: corpus fail rosters DuckDB 107, H2 361 (`spec/src/test/resources/rcorpus/*-fail-roster.txt`);
reference lane AGREE 72,081, OVERLOAD 769, ABSENT 68,232, EXTRA 15,825, bodies FAILED 1,521, sources DROPPED 32
(GATES "Rebuild W1.1 (1)"); product Java 229,168 lines under `*/src/main` (core 217,084: compiler 38.6k, resolver 35.9k,
parser 25.2k, lowering 23.7k, sql 14.1k); quiet lane times at `ed85b5166`: corpus DuckDB 75.7 s, H2 79.8 s, core 29.9 s,
stress 23.0 s, guardrails 9.5 s. **Budgets** (the numbers a slice may not exceed) are set at checkpoint C1 from W1.0b's
measurements; until then, a slice may not make any of them worse by more than noise (two quiet runs).

**Checkpoints** (the user reviews at each; the plan is re-cut at each; §4 places them):
- **C1, end of Phase 1 — decide:** D11 (on W3.7's report), D9, D19 (and D20/D21 if open); the cut list and the minimum
  expert compiler below; budgets from W1.0b; C3's thresholds from Phase 1's attributed defect list; the §1d scope
  boundaries (graphFetchChecked and constraints, M2M, external formats, service tests); every size re-fitted; a cold read.
- **C2, end of Phase 2:** the core gates exist and run; whether W1.9 stays in Phase 2.
- **C3, go/no-go before W4.3 steps 1–9** [T4 §4]: if D11 = R, the new H is the algebra rewrite (decided at C1); if D11 = S,
  the rule (N and the multiplier fixed at C1 from the defect list): **stop** — fix the store resolver's defects in place
  and end the middle rebuild — if at most N defects are attributed to H, each fixable in ≤ 1 session with no new special
  case; else **extract passes** if Phase 3's items so far ran within 1.5× their re-fitted size; else **a new H behind a
  per-query router**: a static per-construct table, never a try-new-then-old fallback (AGENTS.md invariant 4), gated on
  rows.
- **C4, end of Phase 3 (the middle);** **C5, end of Phase 4 (the front end);** then at the end. Tag `main` at each
  checkpoint (`rebuild-C<n>`).

**The minimum expert compiler, proposed for C1** (nothing is cut before the user rules): everything in §4 except the cut
list, which is: W5.5 Postgres (until a user needs it); W5.4 (per-dialect delivered types beyond what W5.2 needs); W6.1's
staged plan IR beyond bind parameters, session settings and one transaction; carve-outs where no item rewrites the code
(the no-new-edges test stays); W2.6 `Ref<Kind>` outside W2.3a push 1a's site list; W1.9 if Phase 3 needs the budget; W7 as
a separate step (its deletions fold into the owning items). W3.5's scope is ruled after W3.3, from the new solver's residue.

**Risks** (owner is the session; each has a trigger and a response): W4.3 overrun → C3; oracle drift (the reference lane
non-deterministic) → W1.1d; performance/WASM regression → budgets from C1; the other account's machine contention → no
timings until quiet; CI capacity (macOS runner 7 GB; Linux/Windows public runners 16 GB, not verified) → W1.1d; an
upstream pin bump → forbidden during the program except as its own slice at a checkpoint.

**Rollback:** every slice is on `main` behind a converter seam or a gated switch; abandoning the program at any
checkpoint leaves a working product with fewer defects. Record at each checkpoint which seams are live.

---

## 1b. The target map (rule 0b.12), measured

Measured from the whole class graph at `ee7617ec8` (715 files, 7,717 file edges, 241 package edges; 39% of edges are
fully-qualified references that import-only analysis misses) and simulated acyclic [L4 §3]. Tools and outputs:
`~/legend/platform-architecture/receipts/meta-audit-2026-09-29/java-dependency-graph/` (`graph.py`, `sim.py`,
`out/corrected-map-sim.txt`, `out/package-edges.*`; the violations table below is in the lens-4 report). Re-run `graph.py` before each carve-out; the table is the target,
the tool the truth.

| target | contents | direct deps it needs | carved in |
|---|---|---|---|
| `base`, `json`, `values`, `error`, `spi`, `cache` | as today | base | exists |
| `sql`, `sql_dialect` | SQL tree; per-database printers | base; `sql` | done (W0.2(e)) |
| `ids` | `FunctionId` without `of(Function)` (34 callers move the mangling out); later `ClassId`, `PropertyId`, `VarId`, `ExprId` | base | W2.1 |
| `syntax_tree` | `protocol`, `protocol.spec`, `ImportScope` | base, json, values | W1.9 |
| `parser` | lexer, parser, parser.section | syntax_tree, spi, values, error | W1.9 |
| `decls` | today's `model` records (parsed declarations) and the converter (`FromProtocol`, `MappingFromProtocol`, `RelOpFromProtocol`) | syntax_tree, base, error | W1.9 |
| `catalog` | builtin, platform (platform declarations generated at build time, not parsed at class load) | decls, ids, syntax_tree, error | W2.1 |
| `types` | `compiler.element.type` with `Type → SqlType` moved out | catalog, ids, syntax_tree | W2.1 |
| `world` | ModelBuilder, KnowledgeLayer, TableIndex, SynthFqn, StoreLookups, RelationalKinds, DerivedProps, SymbolTable | decls, syntax_tree, types, catalog, error | W2.3a |
| `resolved` | the D12 family, `ResolvedExpr` | ids, syntax_tree, error, types | W2.3a |
| `binder` | NameResolver, BareNames, ResolvedNames | syntax_tree, decls, catalog, resolved, error | W2.3a |
| `elements` | compiler.element | world, binder, catalog, types, decls, syntax_tree, ids, error | W2.3a |
| `typed` | compiler.spec.typed without `ContextReading` | types, ids, values; (elements, sql, decls only until call nodes carry ids, W3) | W3 |
| `inliner` | StatementInline, LiteralMapUnroll, SourceSubst, Env (then the one G½) | typed, types, binder, elements, syntax_tree | W3 / W4.2 |
| `typer` | compiler.spec plus `ContextReading` | typed, elements, types, catalog, binder, inliner, ids, values, error | W3 |
| `mappings` | normalizer | today world, binder, decls, syntax, catalog; from W4.1a typer, typed, resolved | W4.1a |
| `store` | resolver plus `LazyRows` and `PlanRows.scopeId` | typed, types, elements, typer (named edge until W6.2), catalog, decls, ids, values, error; **never** lowering or plan | W4.3 (the 15 lowering/plan sites move in W1.12) |
| `lowering` | lowering | typed, types, sql, catalog, ids, values, elements, error | W5 |
| `lineage` → `plan` → `runner` → `validation`/`testdatagen` → driver → `server`/`ide` | the periphery | as measured | W6 |
| `probe`, test support | out of the product jar | | W6.4 |

Cuts the map assumes (sites): W1.9 parser→decls 112; builtin's class-load parses 11; `Type`→SqlType/SqlExpr/
ClassDefinition 15; `FunctionId.of` 6; `Temporal.java:87,98,99`, `TypeClassifier.java:47`, `KnowledgeLayer.java:406`;
`ExecutionContext`→`ContextReading` 6; store→lowering/plan 15. The one class-level cycle crossing a boundary:
`SetId ↔ ClassMapping/MappingDefinition` (`SetId` stays in `decls` until W4.3 gives it a real id).

---

## 1c. The oracles, and what each can see

| oracle | sees | cannot see | where it gates |
|---|---|---|---|
| corpus rosters (`//spec:corpus_duckdb`, `//spec:corpus_h2`) | the engine tests' expected values on their small fixtures | wrong rows on data the fixtures lack (orphans, NULLs, ties, milestone versions) | every slice |
| reference lane (`//spec:reference_lane`, manual) | legend-pure's resolution and (after W1.1b) types per call, Pure source | engine input; store resolution; SQL; rows | front-end slices |
| engine-input differential (W1.13) | legend-engine's compile of engine-grammar input: accept/reject, chosen function, return type | rows | W2.3b, W3.3, D13 |
| rejection corpus (W1.1c) | programs legend-pure refuses, by error class | — | W2, W3 |
| product-SQL snapshot (W1.7) | any change in emitted SQL | whether the old SQL was right | refactor slices W4–W5 |
| adversarial-data old-vs-new (W1.10a) | refactor-induced row changes on data built to separate right from wrong | a bug present in both | Phase 1 on (lite vs engine), then per Phase 3 item |
| metamorphic TLP/NoREC/PQS (W1.10b) | internal inconsistency of filter/project/count under NULL and three-valued logic | a consistent wrong answer | W4.0 on, W5 |
| legend-engine execution (W1.10c) | the engine's rows for the same model, query and data | engine bugs (registered, not copied) | W4.3, D13 |
| PCT lanes (`//pct:pct_duckdb`, `//pct:pct_h2`) | per-function value semantics | mapping/routing | W5 |
| DuckDB = H2 over the SQL tree (W5.0) | dialect legalisation differences; each register row names its adjudicator (PCT or engine) | anything above J | W5 only |
| soundness monitor (W1.3) | a result whose shape (columns, cardinality) contradicts the typer's type | — | every corpus run |

---

## 1d. Semantics nobody owned, now owned [L2 §5, L3 table]

| area | owner item |
|---|---|
| graph fetch output (`serialize` config, `@type`, date/float/decimal JSON forms, property order, nulls) | W4.3 step 8, with a golden JSON class |
| `graphFetchChecked`, class constraints, defects | scope ruled at C1 (constraint checking must be SQL) |
| M2M scope boundary (chains, JSON source, union) | W4.4b; boundary written at C1 |
| null semantics in SQL (`==` on empty, `NOT IN` with NULL, `sum([])`, `toOne` failure) | W0.6 (reproduced ones), W5.2 (equality kinds as nodes), `EqualityWorldsConformanceTest` as a W5 gate |
| date/time (partial dates, StrictDate vs DateTime, connection timezone, `now`) | W6.1 session settings; a non-UTC JVM lane (W1.10) |
| decimal/float (Integer division, rounding, Float computed as DECIMAL, literal magnitude cliff, NaN/Inf) | W5.2; register row "Float computed as DECIMAL" |
| string collation, identifier case folding | W5.3 (`Identifier` with per-dialect folding; `C` collation) |
| Unicode length/substr (code points vs UTF-16) | W0.6 suspect list, then W5.2 |
| enumeration, embedded, inline, otherwise, merge, inheritance mappings | W4.1a, each with a shadow-probe row |
| aggregation-aware | W1.10c engine-result lane |
| service parameters (`[*]` in `in`, nulls, dates and timezone) | W6.1 homework |
| service tests and mapping testSuites (deleted; Studio's "run tests" fails against lite) | recorded in `SEMANTICS_REGISTER.md`; scope at C1 |
| cross-store (walled), external formats | recorded out of scope in `SEMANTICS_REGISTER.md` |

---

## 2. Decisions

| id | question | status |
|---|---|---|
| D1 | The call node's type | **RULED 2026-09-29 (the user): a distinct `ResolvedExpr` family.** §6 records the alternative |
| D2 | Binding scope | **RULED 2026-09-28: everything**: calls, members, binders (`VarId`), element references, on one new tree type; ids added in separate pushes (W2.3a, W2.5, W2.6) |
| D3 | A reference lane at the pinned release | **RULED 2026-09-29 (the user): build it.** Built (W1.1 push 1) |
| D4 | "No tolerant modes" | **RULED 2026-09-28**: rule 0b.9 |
| D5 | Plan the whole program now | **RULED 2026-09-28**: this page |
| D6 | How the corpus certifies product SQL | **RULED 2026-09-29 (the user):** the scan-order ORDER BY lives only in the harness, and only for tests where an unordered compare cannot be correct (a `first`, `take`, `limit`, `slice`, `at` or positional read whose rows depend on scan order); every other test runs the product's exact SQL compared without order. W0.4 implements it, including the always-on pass on the assert path the ruling's text missed |
| D7 | The judge charter | **RULED 2026-09-29 (the user): keep BOTH judges** (host judge and database judge), joined per assert as today (`pinJudgeDifferential`). W6.3 shrinks to the judge SPI |
| D8 | Computing a query's column names at compile time | **RULED 2026-09-29 (the user): yes, as type checking, very ring-fenced** (TENET_CHARTER C6.2a): only schema positions of `NormalizeRequiredFunction` bodies and column-metadata reads; a closed, pinned operation list; never a row value; anything else a clear error. The database computes every value. Owner W4.2. See D19 for the fence's one open edge |
| D9 | The manifest world | **OPEN; to be ruled at C1** (it blocks W4.4a). The 2b stdlib question, `docs/WORLD_MAP.md` §8 (the deletion test), and whether roadmap test files may be excluded by a named, pinned register [W4 F9] |
| D10 | The failure unit under rule 0b.9 | **RULED 2026-09-29 (the user): two modes** (`docs/TENETS.md`). Compile-all types the whole world, collects every diagnostic, fails on any (a lane and an API). User paths are demand-driven and memoized per model; demand-driven is an optimisation only (equal by construction through the query layer, W2.2b, and checked by running the corpus both ways); Knowledge errors are eager. **The difference from legend-engine is kept** (a query touching only valid code runs even if another body is broken) and is a row of the semantics register. Sub-rule from the meta-audit [L1 #4]: a declaration-header reference is Knowledge (eager); an unknown name inside a body poisons that body only on user paths and is reported in compile-all |
| D11 | Where the relational form begins | **RE-SCOPED 2026-09-29; decided by the W3.7 experiment (ruled by the user), run in Phase 1 and ruled at C1.** Option R: an explicit algebraize step after G½ turns the query into a logical relational algebra with semantics as node properties, and store resolution becomes rewrites of it. Option S (today's direction): relational only after store resolution, a skeleton with typed-HIR leaves. Pass criteria in W3.7. If S wins, the original narrower question (what type the skeleton's scalar leaves have; homework `d11-homework-2026-09-29.md`) is answered by the D11 census at W4.0 |
| D12 | The `ResolvedExpr` design | **RULED 2026-09-29 (the user):** a resolved declaration family carrying `ResolvedExpr`, not a side table; readers take names from declaration ids; candidate sets fixed at resolution (subject to push 1a's probe, the note's §7 ruling 2); StaticFold and AlphaRename carried
over onto `ResolvedExpr`. Read `h2-resolved-expr-design-2026-09-29.md` with its revision and its reading guide (form recognition by id moves into W2.3a) |
| D13 | Which overload rule engine input gets | **RULED 2026-09-29 (delegated by the user):** one rule, legend-pure's outcomes, over the engine's namespace (its 32 imports and handler names). The engine's first-match handler order is registration order, not semantics; no second matcher. Gate: W1.13 lists every disagreement before the rule serves engine input; each is classified (engine defect, lite superset, or a real difference ruled case by case) into the register |
| D14 | Wrong-results defects: now or in their owner waves | **RULED 2026-09-29 (the user): now**, rule 0b.11 |
| D15 | The typer's contract with legend-pure | **RULED 2026-09-29 (the user): our own expert inference engine** judged by observable outcomes, with a written rule table and a deterministic tie rule; reference artefacts pinned by class, never copied. Rule 0b.13 |
| D16 | The legend-engine SQL text (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`) | **RULED 2026-09-29 (the user): a backwards-compatible product dialect**: a printer over the same SQL tree as every dialect (it never re-lowers); rows are its gate (its SQL runs and must return the native dialect's rows); byte differences from engine goldens allowed only as register rows (e.g. D8: lite leaves constants to the database, the engine pre-evaluates them). Owner W5.6 |
| D17 | The program's goals | **RULED 2026-09-29 (the user): cleaner, more bulletproof, much less code, faster, cruft deleted.** §1a; rule 0b.15 |
| D18 | Where the rebuild lands | **RULED 2026-09-29 (the user): on `main`**, every gated slice; `compiler/rebuild` kept only as the working branch name, always equal to `main` after a push |
| D19 | D8's fence and helper functions | **OPEN; to be ruled at C1 (before W4.2's schema evaluator).** The motivating branching sits in an unmarked private helper (`extendMatchColumns`, `tdsExtension.pure:68-94`) called by the marked `rowValueDifference` (declared :22/:29, calls at :39 and :56); name arguments sit inside row lambdas (`$r.isNull($col.name + '_1')` at :73; `$r.getInteger($col.name + '_1')` at :114 inside `columnValueDifference`) [L2 F6, T5]. Recommendation: the fence is "schema positions reachable from a marked call site after inlining its callees", with TDSRow accessor name arguments listed as schema positions; the Relation API constructors among the marked functions (`over`, `rows`, `range`, `ascending`, `descending`, `lead`, `lag`) are already lite natives and fall under WORLD_MAP §8, not D8 |
| D20 | `splitPart` with a multi-character separator | **OPEN; blocks only its W0.6 fix.** Pure's `split` doc says the separator is "matched literally" (`split.pure:17-21`) but the interpreter tokenizes on a character set (`Split.java:54-60`, `StringTokenizer`, adjacent separators collapse); the multi-character PCT is commented out as "incorrect behaviour … TODO" (`splitPart.pure:46-54`); engine-H2 uses a character set, engine-DuckDB the whole string. Lite: DuckDB whole string with collapse, H2 character set (report 4 F). Recommendation: the documented literal semantics on every dialect (the reference marks the other behaviour as incorrect), the empty-token rule taken from `split`'s documented contract; a register row for the engine-H2 difference |
| D24 | Cleanup before the middle, under a budget | **RULED 2026-09-30 (the user):** after the damaged-data run (D23 slice 2, started first, the engine's hour in the background), a CLEANUP phase of items that give the middle a cleaner base and change no SQL: unique variable ids on the typed tree (deleting pushes 1–2's renaming scaffolding), ONE substitution engine (six today), mapping elaboration as ONE owner (the normaliser, resolver and lineage each re-derive it today), the gate diet (W1.14). Gates: rows equal to the engine on the seeds, the corpus, the reference lane, byte-identical SQL. **Budget fourteen sessions**; whatever remains on the cleanup list waits; every cleanup push ends net-negative on product lines or it is not cleanup. Then the shape experiment and the middle with the tool as referee. **The parser is untangled LAST** (healthiest code; the middle never reads it). The user's own guard: "quick wins first" is how the last plan grew from 15 to 200 sessions |
| D23 | The wrong-rows tool is built on the stress corpus, with swappable data | **RULED 2026-09-29 (the user):** the stress corpus (`core/src/test/resources/stress`, 4,745 service tests, engine grammar, expectations from an independent Python oracle, run through legend-engine once on the `test-corpus` branch; lite passes 4,700 on DuckDB) is the base of the wrong-rows work and of the mapping-heavy set; no new corpus. **The good data is kept:** the seed `###Data` elements are never edited; damaged data is a separate, generated set (deterministic, regenerable), and the runner takes WHICH data set to use as an argument, since every suite reaches its data by name (`Reference #{ … }#`). Every test runs on both: the original seed, where the expected rows are known, and the damaged set, where legend-engine's rows are the judge and every disagreement is recorded, not copied (the engine is not perfect: on `stress::F38_FirstDayTypes` it prints a week's first day as a timestamp where the test and lite say a date). Order: the six small W0.6 pushes first (2, 3, 7, 8, 11, 13; about three sessions), then the tool, then the remaining pushes judged by the tool; whether the four resolver pushes are fixed in place or left pinned as acceptance tests of the rebuilt store resolver is decided WHEN the tool has run over the damaged data and shows how many rows each gets wrong |
| D22 | W0.6's resolver pushes and the engine row oracle | **RULED 2026-09-29 (the user):** pushes 4 (prefix keys), 5 and 5b (the killed head match, both channels) and 10 (the equality-kind node) change temporal joins and equality in the store resolver, and their expected rows were derived by reading. They run after W1.10c and take legend-engine's rows on their fixtures as the expected values (an engine defect is registered, not copied, rule 0b.13). Every other W0.6 push runs first, in the homework README's order |
| D21 | Float literals: the magnitude cliff | **OPEN; blocks only its W0.6 fix.** Under NUMERIC_CHARTER Rule 1 literals render bare and the database types them; `AnsiSqlRenderer.plainFloat` (`:1370-1378`) switches to exponent form outside 1e-6..1e15, which DuckDB types DOUBLE, so `i * 0.00000013 == 0.00000039` is false on DuckDB and true on H2 and in the interpreter (report 4 G, ran). Whether the engine's `%s` formatting has the same cliff is not verified. Recommendation: no cliff (a value's kind must not depend on its magnitude, charter C2.2): spell plainly with per-value DECIMAL precision; register the engine difference if the engine has the cliff |

---

## 3. Done

| item | what | record |
|---|---|---|
| 0–2 | annotations to `com.legend.base`; `//core` as 29 targets; the reference differential by call; lowering by `FunctionId` | GATES 2026-09-26 "Execution plan step 0/1/2" |
| 3-homework, 3-probes | kernel reading; resolver per-statement fix; nine counts before any switch | GATES 2026-09-26/27 |
| audits | program audit; architecture review and stage readings; H1 plan audit; **meta-audit (six lenses)** | `plan-audit-2026-09-26/` |
| H1–H6 | plan audit, `ResolvedExpr` design, reference-lane spike, diagnostics design, quiet baselines, superseded docs marked | `h1-…/`, `h2-…`, `h3-…`, `h4-…`; GATES "Rebuild H5 and W1.6" |
| W0.0, W0.1, W0.2 | expected-failure pins; the server's doors (loopback bind, Origin allow-list, `/engine/sql` gone); confirmed defects (a), (b), (d), (e) — (c) moved to W3.3; the static-final rule | GATES "Rebuild W0, first batch", "second batch" |
| W0.3 | ten latent defects reproduced and pinned | GATES "Rebuild W0.3" |
| W0.5 | the line guard, DanglingState rule 2, the JDBC test register, 15 zero rows dropped (commit `ed85b5166`) | GATES "Rebuild rev H2" (the owed record) |
| W1.6 | the typer split: TdsDesugars and Overloads out of Typer (3,499 → 1,748 lines); `accessProperty` stayed in Typer (`Typer.java:~1200`); W2.4 changes it in place, W3.3 moves it out (its owner) | GATES "Rebuild H5 and W1.6" |
| W1.1 (1) | the reference lane, calls first | GATES "Rebuild W1.1 (1)" |
| rev H2 | this page; D13–D18 ruled; automap reading corrected; `main` fast-forwarded to the program (`89dc45871`) | GATES "Rebuild rev H2" |
| W0.6 homework | four root-cause reports and the push order; D20, D21 opened | `plan-audit-2026-09-26/w0.6-homework/`; GATES "Rebuild W1.0" |
| W1.0 | the documents executable: rev H2, the register, routing, banners; the cold read passed after fixes (its 15 contradictions and 12 gaps fixed) | `plan-audit-2026-09-26/cold-read/2026-09-29-rev-H2.md`; GATES "Rebuild W1.0" |
| rev H3 | the tractability audit (five reviewers) folded in: every W0.6 push dry-run against the code and fully specified; W1 items given tools, spikes, sizes; W2.3a, W3.3, W4.3 decomposed; W3.7 made runnable; C4 given a rule; the consistency sweep's fixes; E1–E13 | `plan-audit-2026-09-26/tractability-2026-09-29/`; GATES "Rebuild rev H3" |
| W0.6 push 1 | capture-avoiding substitution in the inliner, `SourceSubst` and `MatchFold`; the `InlinerMatchCaptureTest` pin removed; three further wrong answers reproduced and fixed; 14 latent captures in library code fixed; six bodies newly typed in the reference lane; a purpose-built stress (compile time by size against the previous commit, and a permanent correctness test) | GATES "Rebuild W0.6 push 1" |
| D23 tool (2) | the damaged data set and generator; the engine's `count()` over an empty navigation (S22, 37 new + 9 seed cases); the DuckDB loader's masking defect fixed | GATES "Rebuild D23 (2)"; `wrongrows/damaged-2026-09-30.md` |
| D23 tool (1) | rows mode and data override on both runners; the multiset comparator; the batch driver; the engine and lite compared on the seed data (4,686 equal of 4,729; 22 row disagreements to attribute) | GATES "Rebuild D23 (1)"; `plan-audit-2026-09-26/wrongrows/` |
| W0.6 push 13 | `first()` over a group spelled `MIN` on H2 for comparable scalars, a capability wall for JSON carriers | GATES "Rebuild W0.6 push 13" |
| W0.6 push 11 | `sum`, `plus`, `times` over an empty list give their unit in the list rules; group and window forms untouched | GATES "Rebuild W0.6 push 11" |
| W0.6 push 8 | service-test provisions keyed by value records, runtimes named by ordinal; the pin removed; three latent content-hash ids pinned for W6.4 | GATES "Rebuild W0.6 push 8" |
| W0.6 push 7 | the sourceless Pure mapping resolves (one line); register row S21 for the model-build refusal the engine does not make | GATES "Rebuild W0.6 push 7" |
| W0.6 push 3 | `TypedFilter`'s defaulting constructor deleted; `rebuilt` keeps the stamp, 46 sites converted; the nested-exists pin removed; three rebuild sites shown live by the probe, no verdict changed on the fixtures | GATES "Rebuild W0.6 push 3" |
| W0.6 push 2 | the Barendregt convention at both boundaries: `Lowerer.lower(List)` and `StoreResolver.resolve` rename every binder spelled like a query-scope name before their flat let maps read it; both `LowererLetScopeTest` pins removed, eight cases fixed; 69 α-renamings per corpus run, no verdict changed | GATES "Rebuild W0.6 push 2" |
| independent audit; D22 | one session's own audit of rev H4 against the code (input for C1); the user ruled the W0.6 reorder | `plan-audit-2026-09-26/independent-audit-2026-09-29.md` |
| rev H4 | the re-cut (approved by the user): phases in §4; learn and decide before building; the middle before the front end; net-deletion and build-don't-re-plan rules; the gate diet | GATES "Rebuild rev H4" |

---

## 4. The phases (the order) — the re-cut of 2026-09-29

**Why this order** (the macro review the user approved, 2026-09-29): the target (§1) stays; the *order* changes. The old
order followed the pipeline (front end first, the middle after ~70 sessions). But the type checker already agrees with
legend-pure on ~72,000 calls (769 differ), while wrong answers and the worst tangle live in the middle (the 36k-line store
resolver, the SQL builder). And two unknowns decide the rest: where the middle's relational form begins (D11), and where
the real defects are (only an independent row oracle shows that). So: **fix what is known, learn what is unknown, decide,
then build by risk and value, with the front end rebuilt where and when it pays.** Item ids (W0.6, W1.10c, W3.7, …) are
identifiers in the §5 catalogue, not an order; this section is the order.

**Phase 1 — Correctness and knowledge** (≈14–20 sessions). Ends at **C1**.
1. W0.6, the six small pushes: 1 (done), 2, 3, 7, 8, 11, 13 (D23; about three sessions).
2. **The wrong-rows tool on the stress corpus** (W1.10 as rewritten under D23; about three to four sessions): (c) the
   runner prints the engine's rows on demand and takes the data set as an argument; (a) the damaged data set, generated
   from the seeds and kept apart from them; the mapping-heavy set = the stress services that cover the mapping kinds of
   §1d, named by FQN. Both engines run on seeds and on damaged data; every disagreement is attributed to a stage (H, I,
   J) with the files a fix would touch — the defect list that orders Phase 3.
3. The remaining W0.6 pushes: 9, 4, 5, 5b and 10 judged by the tool's rows (fix in place or pin for the rebuilt
   resolver: decided here, D23); 6 and 6b judged by the reference lane; 12 by its repros.
4. W1.0b baselines, including **net product lines** (rule 0b.17's number).
5. W3.7, the D11 experiment (on the mapping-heavy set, judged by the engine rows from step 2).
6. W0.7, request-reachable static state.
**C1 — decide** (the user): D11 on W3.7's report; D9 and D19; D20 and D21 if still open; the cut list and the minimum expert
compiler (§1a); budgets from W1.0b; C3's thresholds from the defect list; the scope boundaries in §1d; every size re-fitted
from logged cost; the next cold read.

**Phase 2 — The gates that matter** (≈10–16). Ends at **C2**.
W0.4 (the corpus certifies product SQL) → W1.5 (determinism, dumps) → W1.7 (the SQL snapshot) → W1.1b (type rows and
recorded instantiations) → W1.3 (the pass manager: routed entries, verifier, lint) → W1.11 (exhaustiveness, spike first;
before any kind split) → W1.12 (layering hardened; the store stops reaching into lowering and plan) → W1.10b (metamorphic
tests) → W1.2(a) (the diagnostics sink, read by the LSP) → W1.14 (the gate diet) → W1.1c and W1.1d (the reference lane's
rejection bucket and trust) → W1.9 (the syntax targets and the caching proof the user asked for; it may slide to Phase 4
if Phase 3 needs the session budget, by the user's call at C2).

**Phase 3 — The middle, by risk and value** (≈40–65, re-sized at C1 from D11 and the defect list). Ends at **C4**.
W4.3 step 0 (explicit state) first → the Phase-1 defect list, worst user impact first, each fixed properly in its stage →
W4.0 (the H gate) → if D11 = R: W4.1r (algebraize, forms recognised by the `FunctionId` already on typed calls) then W4.1a
(mappings as views); if S: W4.1a and the D11 census → W3.4 and W4.2 (G½ substitutes, never re-types; one G½; the D8
schema evaluator — binder uniqueness from W0.6 push 2 stands in for `VarId` until Phase 4) → **C3, go/no-go** → W4.3
steps 1–9 (structural `NavPath` keys use property names with their owning class until `PropertyId` exists) → W4.4a →
W5.0 (the dialect fuzzer) → W5.1a → W5.1b → W5.1c → W5.2 (semantic SQL tree: null-strictness, determinism, literal
typing) → W5.6 (the engine SQL format as a dialect). Every Phase 3 item ends net-negative on product lines (rule 0b.17).

**Phase 4 — The front end made expert** (≈35–55). Ends at **C5**.
W2.1 → W2.2 → W2.2b(1) → W2.3a pushes 1a…5 (re-plan if more than 15 sessions) → W2.2b(2) → W1.13 (the engine-input lane,
before the rule changes) → W2.3b → W2.4 → W2.5 (`VarId`; then G½, H and I re-keyed from names to ids) → W2.6 → W2.7 → W2.9
→ W3.1 → W3.2a → W3.2b → W3.0f → W3.3a → W3.3 (lite's own solver, D15) → W2.8 → W3.5 → W3.6 → W1.2(b–d) → W1.4.

**Phase 5 — The back end and the edges** (≈15–25). W5.3 → W5.4 → W5.5 (cut candidate) → W6.3 → W6.1 → W6.2 → W6.4 → W4.4b
→ W7 (or folded into the owning items).

**Size (judgement):** about 114–181 sessions in all; the minimum expert compiler (everything but the cut list) about
100–150. The first week says mechanical items run 3–5× under estimate, so the low end is the likelier; C1 re-fits.

## 5. The items, by id (a catalogue; §4 sets the order)

Each item is one push unless it says otherwise. Sizes are judgement (§0). Every item names its gate, what it deletes,
and its number; every rewrite ends by carving its stage as a target (rule 0b.12).

### W0 — Correctness and safety (catalogue; order in §4, Phase 1)

- **W0.6 Fix the reproduced wrong-results defects now** (D14). **Homework done** (`plan-audit-2026-09-26/w0.6-homework/`)
  **and dry-run against the code** (`plan-audit-2026-09-26/tractability-2026-09-29/1-…`, `2-…`); the homework README's
  "Push list" is the exact scope, design choice, test table and gate of every push; **the order is §4 Phase 1's (D22:
  pushes 4, 5, 5b and 10 after W1.10c); push 1 is done.** By number: (1) capture-avoiding
  substitution in the inliner, `SourceSubst` and `MatchFold`; (2) the lowerer's let scope, by the Barendregt convention at
  the lowering boundary (E1); (3) the `TypedFilter` stamp (46 constructor sites); (4) prefix keys from the materialization
  map; (5) the killed head match, nav channel, then (5b) the association channel (E3); (6) section imports as a keying
  change, (6b) the `###` import leak after its probe (E6); (7) the sourceless mapping; (8) value-keyed service-test
  provisions (E7); (9) null-safe `==` by multiplicity (A); (10) the equality-kind node for ModelJoin/XStore conditions (A2,
  E4, after its fixture confirms); (11) empty `sum`/`times` (H, E8); (12) the three match suspects (MatchFold's arm choice
  and `extraParam`, MatchChecker's second parameter name), each reproduced first; (13) `ANY_VALUE` on H2 (E5; not a wrong
  result). After D20/D21: `splitPart` (F), the Float literal cliff (G). Every predicted defect gets a failing lite test
  first. Gate per push: the pin removed and its test asserting the right answer; the push's adversarial cases added;
  rosters LOST 0 **by the roster files** (§0 step 6), any GAINED name trimmed with its reason; front-end pushes (1, 6, 6b,
  7) also run `//spec:reference_lane`; the chain green. The pins' `owner` fields (W2.5, W4.2, W4.3, W2.2, W2.3a, W6.4)
  predate D14; each fix removes its pin. If D20 or D21 is still unruled when the other pushes are done, F and G are pinned
  `@KnownDefect(owner = "D20")`/`("D21")`, the user is asked, and W0.6 closes. §3 gains a row per push. Number: open
  wrong-results defects 14 → 0 (the pinned seven, the head match ×2 channels, A, A2, H, the arm choice, the extra
  parameter; D is a loud failure, not counted). Size 6–9.
- **W1.0b Baselines for §1a** (runs here, before W0.4, so every later push has numbers to move): a `tools/metrics` `bazel
  run` target reading declared outputs and invoking the timed binaries (timings cannot be test actions): product lines per
  package (`wc` over `*/src/main`); plan latency p50/p95 over `wasm/corpus/queries.tsv` (69 DataCube-shaped queries, model
  `wasm/corpus/model.pure`, JVM side `wasm/src/main/java/planner/JvmMain.java`), compile only, alone and quiet; `//wasm:planner`'s
  `classes.wasm` bytes (a `stat` on the declared output, `tools/teavm/defs.bzl`) and `//wasm:startup`'s cold start by
  phase (`wasm/startup.mjs`); `//core:scale_stresstest100k` (manual) build time plus a heap-after-GC read added to
  `StressTest100K.java` (:209-212 prints parse+build only); reference-lane buckets read from the last
  `bazel-testlogs/spec/reference_lane/test.log` (never re-run for metrics: 8 GB); the fail-roster sizes. Gate: the output
  pinned in GATES with its receipt. Budgets are set from it at C1. Size 1–2.
- **W0.4 The corpus certifies product SQL**, per D6. Facts [L5 #4–#8, T3]: the installed pass is
  `sql/dialect/StableScanOrder` (the key is `sql/ScanOrder.java`: every join tree rooted at a bare scan), installed in
  `sql/dialect/DuckDb.java:218-219` when `Boolean.getBoolean("legend.exec.engineScanOrder")`, set unconditionally at
  `MinimalCorpusTest.java:139`; firings counted by `exec/Census.java:92` (`StableScanOrder.firings()`); registers
  `spec/src/test/resources/rcorpus/duckdb-engine-order-register.txt` (993, host mode) and
  `duckdb-database-engine-order-register.txt` (936, database mode); the H2 registers are empty. A second, always-on
  application: `lowering/CanonicalRenderSql.java:436` (`ScanOrder.stabilize(plan)` on the assert/verdict path). Steps:
  (1) count dynamically with BOTH applications switched off (a harness flag): rerun both registers' tests, compare rows
  unordered, classify each test order-only (passes) or scan-order-dependent (fails); write the split as two register
  files (static classification is not enough: positional reads in the Pure test body happen on the host). (2) Move the
  pass out of `DuckDb` into a harness-injected rewriter on `ExecEnv` (precedents `exec/SqlReplayOracle.java`,
  `exec/AssertListener.java`; `Compiler.dialectOf`, `Compiler.java:745,804`, is a static package-private choice to replace
  by the seam; the rewriter lives in `spec` test code so `core-layers.txt` gains no edge), applied only to the
  scan-order-dependent tests; the `Census.java:92` read moves with it. (3) Move `CanonicalRenderSql`'s stabilize behind the
  same seam; record in the GATES entry that the assert surface has no product user (service tests are deleted, register
  S11), or add a register row. (4) Prefer set-of-valid-answers verdicts (page membership, `spec/.../harness/H2Verify.java:369-383`)
  where they suffice. Gate: zero harness ORDER BYs in product DuckDB SQL outside the registered tests (the moved census
  row); rosters LOST 0; the two new registers pinned; `TestLaneOrderGuardrailTest` re-pinned. Deletes: the system property
  and the product-side pass. Size ~2.
- **W0.7 Request-reachable static state** [L6 F12, T3]: compute reachability, do not hand-grep (681 `static final`
  collection/atomic fields and 6 `ThreadLocal`s exist in core main: `StampCensus:48`, `SqlTypeCensus:86,385`,
  `StatementOrigin:53`, `Shadow:59`): an ArchUnit breadth-first walk from the `com.legend.server` HTTP handlers over method
  calls and field accesses, keeping mutable holders (immutable `Map.of`/`List.of` initializers filtered). The stdio LSP
  (`PureLspServer`) is in scope. Each reachable mutable static becomes per-request or immutable, or is recorded as safe
  under the single-threaded dispatcher (`LegendHttpServer.java:314`, `setExecutor(null)`) with a test pinning
  single-threadedness. Gate: the computed list in GATES; ArchUnit `staticFieldsAreFinal` still green. Number:
  request-reachable mutable statics → 0. Size ~1.

### W1 — Gates and foundations (catalogue; order in §4, Phases 1, 2 and 4)

- **W1.0 This page executable** — done (rev H2, the cold read and its fixes, rev H3). Repeat the cold read at every
  checkpoint.
- **W1.1b Type rows and recorded instantiations** [W0-W1 #2, L1 #2, L5 #11, T3]: the reference side prints
  `_genericType`, `_multiplicity`, `_resolvedTypeParameters` and `_resolvedMultiplicityParameters` per call (all on the
  pinned `legend-pure-m3-core-5.99.0.jar`'s `FunctionExpressionAccessor`; printing via
  `navigation.generictype.GenericType.print(CoreInstance, ProcessorSupport)`; extend the walker at
  `tools/reference/RefResolutions.java:86-113`; the `ref_dump` rebuild is ~35 s / ~6 GB, `tools/reference/BUILD.bazel:1-4, 59`).
  Our side: result type and multiplicity are on the call nodes (`c.info()`); the instantiation is recorded **permanently
  as a field** on `TypedNativeCall` and `TypedUserCall` (100 constructions in core main, 20 in `resolver/Substitution.java`;
  a defaulting secondary constructor, the existing `pos` idiom at `TypedNativeCall.java:34-36`, keeps the change small),
  filled from `Bindings` (`compiler/spec/Bindings.java`); this supersedes the side-table wording in GATES "Rebuild W1.1
  (1)". About 40 bespoke checkers (`FilterChecker`, `ProjectChecker`, `GroupByChecker`, …) type relation natives without
  unification and have no instantiation: their rows go to a bucket "checker-typed, no instantiation on our side", not
  counted as disagreements. Type text: one grammar, two implementations (M3 `CoreInstance` and lite `Type`), with a golden
  test of both on shared cases (M3 FQNs, Nil, function types, relation column types, multiplicity spellings); sample 100
  rows by eye before blessing. Form nodes' ids and spans (the ABSENT bucket, 68,232) are joined by W2.3a's `ExprId`, not
  here (`reasons.tsv`'s ABSENT row re-owned to W2.3a). Gate: the type rows joined and bucketed; reasons per class; the
  AGREE floor and coverage pins held. Size 2–3.
- **W1.1c The rejection bucket** [L1 #6d, T3]: part one exists (`tools/reference/join.py:17-18` prints the compile-status
  differential both ways): pin "we typed, the reference failed" as its own bucket. Part two, the negative corpus, **starts
  with a ≤ 0.5-session spike**: the pinned pure tree has ~988 failure-assert sites (`assertPureException(` 621,
  `assertThrows(PureCompilationException` 367, 117 test files) whose Pure sources are Java string concatenations, and
  `@maven_upstream` has no pure test jars; harvest by running the pure tests with a recording shim (as
  `parser-equivalence`'s `FixtureHarvestGenerator` does for the engine; needs the pure `-tests` jars in a quarantined pool)
  vs a static harvest of literal-only cases; pin the error class, never the message. Gate: both pinned. Size 1–2.
- **W1.1d The lane is trustworthy and runs somewhere** [T3]: rows sorted before diffing (`ref_dump` iterates
  `pkg._children()`, `RefResolutions.java:48-49`); 10 runs agree byte-for-byte, one of them a cold `ref_dump` rebuild
  (`--nocache` plus an empty disk cache: `.bazelrc` sets `--disk_cache=~/.cache/bazel-disk`); peak RSS measured after
  W1.1b; a Linux CI job runs the lane nightly if the runner has ≥ 8 GB free for it plus the Bazel server (`free -g`
  first; 16 GB applies to public repos only, not verified). Gate: the ten-run receipt; the CI job green or the reason it
  cannot run. Size ~1.
- **W1.2 Diagnostics foundation, in four pushes** per `h4-diagnostics-design-2026-09-29.md` (its §3 step 6, "the LSP in
  W6.4", is superseded here) [T3]: **(a)** the types, the sink, a bridge at each stage boundary, and the LSP reading the sink
  (`server/PureLspServer.java:171-219` guesses positions from message text; `PureLspServerTest` exists); `Phase.RENDER`
  (`error/LegendCompileException.java:30`) resolved; **(b)** parser codes at helper granularity (the parser raises through
  helpers: `throw c.error(` 208, `throw error(` 199, `TokenStreamCursor.throwAt(` 129, `fail(` 31 — ~600 raise points, 24
  direct `new ParseException`), spans, UTF-16 columns (the four code-point sites); **(c)** speculative scopes (the 30
  `catch (` sites in `compiler/spec` listed and classified); **(d)** element-level parser recovery. The number:
  positioned-diagnostic % over `parser-equivalence`'s `MutationFuzzTest` inputs (a ready corpus of bad input). Gate per
  push: the LSP test reads a code and a span (a); the % pinned (b); rosters unchanged (c, d). Size 4–6.
- **W1.4 Rosters by failure class = (phase, exception class, diagnostic code if any)** [T3]: needs W1.2(a) (a failure that
  is not a `LegendCompileException` — hundreds of raw `IllegalState`/`IllegalArgument` — takes the phase of the stage
  boundary the bridge records). Both lanes stay one JVM each. The stress lane's count ratchet (`MIN_PASS` 4700 / H2 4622,
  `StressServiceSuitesTest.java:143-154`) becomes a roster. Gate: rosters rewritten with classes, LOST 0 by (name,
  class). Size ~1.
- **W1.3 The pass manager** [L1 #6a,b; W0-W1 #13; T3]: one pipeline object every **top-level driver call** runs through:
  `Compiler`'s two sequences (`Compiler.java:592-614`, `:1173-1194`; 8 `new SpecCompiler` there) and the executor's own
  (`StatementExecutor.java:510` builds a StoreResolver; 9 `new UserCallInliner`; 2 `new Lowerer`), routed now, the copies
  deleted in W6.2. **Nested re-entrant runs** (a pass invoked from inside another: `compiler/spec/ExecuteChainAssembly` 6
  inliners inside the typer, `BodyCompiler` 1, `VerdictQueries` 2, `SqlTextVerdicts` 2, `resolver/Pipelines` and
  `RoutingContext` 1 each, `resolver/SubQueryLift` 3 nested StoreResolvers, `ClassSources` 1, `lowering/SeedableLets` 1
  Lowerer) go to a named, shrink-only register, since a between-pass verifier cannot wrap them. Between passes, in tests:
  the phase verifier (post-conditions true today; known violations such as `SqlSource.Join.Kind.sql` and interval unit
  strings pinned shrink-only, owned by W5.2; every variable bound in scope; after W0.6 push 2, no binder shadows a
  query-scope name; ids unique once W2.5 lands); a type-consistency lint (re-derives a node's type from its children where
  the rule is local; violations shrink-only; the store resolver hand-builds 410 `ExprType`s — `grep -ro 'new ExprType('
  resolver/` — nothing re-checks); per-pass timing; the soundness monitor (result shape vs the typer's type). Gate: a test
  enumerates the top-level entries and the nested register; verifier and lint green against their pinned sets. Size 2–4.
- **W1.5 Golden dumps per pass, and the mapping-heavy set** [T3, T4]: first make the store resolver deterministic
  (`resolver/NavReducer.java:63,75` names temporaries `"_e" + System.identityHashCode(..)`; moved here from W1.7); then
  deterministic line-text printers for the typed HIR and the SQL tree (none exist today; a parser back into typed HIR is
  a separate, later item), blessed by `//:update_generated`. **Define the mapping-heavy set here:** a pinned list of test
  FQNs with its selection criteria (every mapping kind in §1d's list, milestoning of each kind, association and ModelJoin
  navigation, graph fetch, M2M chains, and the three W3.7 cases), drawn from `core/src/test/resources/stress` and the
  corpus; W1.10, W3.7 and W4.0 use it. Gate: dumps stable across two runs with `--nocache_test_results` and no disk cache
  (or twice inside one action); the set pinned. Size 1–2.
- **W1.7 The product-SQL snapshot** [W4 F10, T3]: an injected observer records every statement's SQL at the one JDBC
  boundary, before `exec/ExecutionTrace.java`'s `stamp` (its trace uuid comment stripped), per test per lane; `EngineStyleH2`
  rendered from the same plan without executing (render-only); aliases canonicalised; plus the DataCube set
  (`wasm/corpus/queries.tsv`); storage one file per lane, sorted by test, blessed by `//:update_generated`. Refactor slices
  gate on byte identity; semantic slices on a reviewed diff. Needs W0.4 and W1.5's determinism. Size 2–3.
- **W1.11 Exhaustiveness and language level — spike first** [L4 §4.1, §4.5, T3]: the Error Prone in rules_java's
  `JavaBuilder_deploy.jar` is 2.50.0; `@maven_tools` has no `error_prone_check_api`; `tools/nullaway/defs.bzl:12` runs
  `-XepDisableAllChecks`; no Error Prone check covers sealed types. Spike (≤ 0.5): a custom `BugChecker` compiled against
  `error_prone_check_api:2.50.0` (compile-only, `--add-exports` for `jdk.compiler`), enabled with `-Xep:<Name>:ERROR`
  after the disable-all flag; its version is coupled to java_tools, so a rules_java bump can break it. Fallback: a
  source-scanning guardrail test (repo precedent). The check fails a `switch` over a sealed IR family that hides cases
  behind a `default`, with a shrink-only baseline (76 of 77 typed-tree switches have one); a ratchet on `instanceof
  Typed[A-Z]` (1,781 in core main, `grep -rE 'instanceof Typed[A-Z]'`). The language level rises from 21 to 25 in
  `.bazelrc` (`--java_language_version` and `--tool_java_language_version`, :6) after building `//wasm:planner` and
  `//wasm:differential_test` at 25 (TeaVM 0.15.0 reads class files at 25; classlib coverage of JDK 22–25 APIs and NullAway
  0.13.8 at `-source 25` not verified). Gate: the check fires on a seeded violation; baselines pinned; `//wasm` green.
  Size 1–2.
- **W1.12 Layering test hardened; store stops reaching into lowering and plan** [L4 §4.8, §2d, T3]: enumerate targets with
  `kind(java_library, deps(//core:core))` and `--output=build` (genquery probably rejects `//core:*`; this form also shows
  `exports` and `runtime_deps`), plus a test parsing `core/BUILD.bazel`'s target names to catch one the umbrella does not
  export; compare against a committed map file (§1b) plus shrink-only named exceptions each with an owner item;
  declared-but-unused deps (4 today: `cache→base`, `lowering→protocol`, `resolver→platform`, `exec→platform`) found with
  javac's `.jdeps` protos (a small Starlark rule exposing them, parsed with protobuf); carved targets get `visibility`;
  `graph.py` committed under `tools/deps/` if a gate relies on it. In the same item, one push: the 13 store→lowering/plan
  references in 6 files move (`lowering.AsorRef` 3 below both; `lowering.Aggregates.isReducer`/`isDemandReducer` 4 to
  catalog or a typer annotation; `plan.LazyRows` 4 and `PlanRows.scopeId` 2 into store). Gate: the edges gone from
  `core-layers.txt`; the test fails on a seeded new edge, a seeded `exports`, and a new unlisted target. Size 1–2.
- **W1.9 The syntax targets** (rule 0b.12's first carve-out, [L4 §2a–c, T3]): `syntax_tree` (protocol, protocol.spec,
  `ImportScope`), `parser` (lexer, parser, section; deps spi, values, error) and `decls` (today's `model` records and the
  converter). The parser returns syntax only; the conversion (`FromProtocol`, `MappingFromProtocol`, 25 uses; the 19
  section grammars' `toModel`, declared `RawSectionGrammar.java:39`, `LexableSectionGrammar.java:50`, called
  `ElementParser.java:385,471`) moves to `decls`; the five constructions straight to model with no protocol record
  (`ElementParser.java:505, :1332, :1373, :2504`; `parser/OverlayElementSink.java:41`) get protocol records; `ParsedModel`'s
  consumers read the syntax result plus the converter; refusals that move phase (`ElementParser.java:741`) listed and their
  roster class change recorded; `AGENTS.md`'s layer table edited. **Proof by caching, in steps:** open `visibility` of
  `:parser`/`:syntax_tree`/`:decls` to `//parser-equivalence`; `pe_tests_lib` depends on them instead of the `//core`
  umbrella (the PE tests import only parser, model, lexer, base, protocol, json and `//testing`); replace `//core:srcs`
  (`glob(["src/**"])`) in `parser-equivalence/BUILD.bazel`'s `_INPUTS` with the own-corpus harvest action's output (the
  `OwnCorpus*` walkers read that file only); Bazel's early cutoff then keeps `//parser-equivalence:parser_parity` and
  `//parser-equivalence:diagnostics` `(cached)` on a typer edit. Gate: rosters and both lanes unchanged; `core-layers.txt`
  updated; a no-op typer edit leaves both lanes cached; a parser edit reruns them. Size 3–5.
- **W1.13 The engine-input differential lane** (D13) [L2 F7, L1 #5, T3]: inputs `parser-equivalence/src/test/resources/engine-grammar-fixtures.jsonl`
  (harvested engine-grammar inputs) and `wasm/corpus/queries.tsv` (DataCube-shaped); home: a new test in
  `//parser-equivalence` (the engine compiler at the pin already shares a classpath with `//core` in `pe_tests_lib`); rows
  (input, accept/reject, chosen function — the engine side from `FunctionExpression._func()` in the compiled `PureModel` —
  return type). Expected differences: `between` only for Date, Number, String (`Handlers.java:2847-2849`); user overloads
  first-registered; 32 imports, not 29. Gate: the lane runs; every disagreement classified into register row S3's
  sub-rows. Size 2–3.
- **W1.14 The gate diet** (the re-cut, D17): every guardrail test, register and ledger under `core/src/test` classified by
  what it has caught (its git history of red chains): *catches real defects* (kept), *invariant now held by a type, target
  or test* (deleted, the holder named), *ceremony* (deleted). `JavaEvalLedgerTest` and anything `AGENTS.md` names as an
  enforcement changes only with an `AGENTS.md` edit in the same push. Gate: the classification table in GATES; the chain
  green; the pre-chain lanes' time down. Number: guardrail lines and per-push red-chain rate. Size 1–2.
- **W1.10 The wrong-rows harness** (before W4.0) [L3 #4, L2 F3, L1 #6c, T3]. **Rewritten under D23:** the base is the
  stress corpus, not the engine's Pure-source corpus; (c) is a mode of the existing runner (`bazel run
  //tools/engine-runner:testable` runs a service test through legend-engine today and already prints the engine's actual
  rows on a failing assert — a flag prints them always, and an argument names the data set), not a new `RowsMain`; (a)'s
  mutator writes a separate generated `###Data` set from the seeds and never edits them; the seeds stay the known-answer
  run. The text below is the earlier design and stands where it does not conflict.
  - (a) **adversarial-data old-vs-new, spike first**: `testdatagen` extracts minimal supporting rows
    (`testdatagen/TestDataGenerator.java:26-48`); it does not mutate. Build a mutator over `DatabaseDefinition` (joins as
    foreign keys, nullability, milestoning columns; reuse `ScanRelations`' relation walk, `necessaryColumns` :100,
    `MilestoningDates` :66) applied after each test's setup: orphans on every foreign key, NULL in every nullable column,
    duplicate business keys, several milestone versions with boundary dates, ties on sort keys, empty tables, non-BMP
    strings, extreme numerics; spike on 10 tests. Needs W1.7's stored SQL. Until W4 there is no "new" SQL, so the W1 gate
    is "the harness runs and old agrees with old". Size 3–4.
  - (b) **metamorphic tests at Pure level** over the mapping-heavy set: Pure is two-valued (a `filter` lambda is
    `Boolean[1]`; `==` on empty is false), so the Pure-level TLP invariant is `R->filter(p)->size() +
    R->filter(!p)->size() == R->size()` (the three-valued partition is what lowering must hide); NoREC (filter count =
    count of true in `project(p)`); PQS (a pivot row must be returned). The real cost is a random well-typed predicate
    generator over the model. Size 3–5.
  - (c) **legend-engine as the executing oracle** for store resolution: a new `RowsMain` in `//tools/engine-runner` (today
    it runs Testable suites only, `TestableMain.java:92-119`; its `@maven_runner` pool at the pin has plan generation and
    execution and H2 2.2.224, `MODULE.bazel:212-231`): compile the PMCD, generate a plan for the lambda, execute against an
    H2 seeded by one setup SQL script shared with lite, emit rows as TSV; lite's H2 version vs 2.2.224 checked. (Maven
    Central's shaded server jar stops at 4.138.5: not the oracle.) Size 2–3.
  - A non-UTC lane: `-Duser.timezone=` in the target's `jvm_flags` and an explicit DuckDB `SET TimeZone` (`.bazelrc:15`
    sets `TZ=GMT` for every test; do not rely on a target `env` overriding `--test_env`).
  - **Phase 1 runs (a) and (c) together as lite versus the engine on the mutated fixtures** (the knowledge step, §4); the
    old-vs-new use of (a) starts once W1.7 stores the baseline SQL.
  - Every disagreement is recorded with its **attributed stage (H, I or J), the files and lines a fix touched, and whether
    the fix added a special case** (C3's data). Gate: all three run on the mapping-heavy set, disagreements pinned by
    class; each is a W0.6-style fix push or a register row, budgeted at C1.

### W2 — The resolved tree (catalogue; order in §4, Phase 4)

- **W2.0 = the ResolvedExpr note (H2)**, ruled (D12). Read its revision and reading guide.
- **W2.1 World tables, the index in three layers, and the `ids`, `catalog`, `types` targets** [W2 #9, L4]: a cached
  boot index (platform declarations generated at build time as a serialized resource, not 854 parses at class load,
  `builtin/Pure.java:148/318/738`, `Prelude.java:55`, `SystemMetamodel.java:1517`; a generated class would exceed the 64 KB
  static-initializer limit; whether the WASM planner can load the resource is checked first), a graph index built before
  D, an extension after E. Refuse model↔model duplicate ids now; native↔model twins wait for W2.8. A collision guard on the
  mangle (`Pure.java:616`). `FunctionId.of/ofAll(Function)` moves out of the id type. Gate: CANDIDATES identical; boot time
  within budget. Size 2–3.
  *Finding 2026-10-03 (`docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md` §3.2, C4):* the `ExecutionContext` reader (`ContextReading`) does not follow
  `->from(m, ^Runtime(connectionStores = helper()))` or some `toSQLString` runtime forms; four `"H2"` defaults stand in
  for what it cannot read (`ContextReading` ~:803/:845, `StatementExecutor` ~:471/:760, `planConnOf`). Instrumented:
  they fire in exactly 21 corpus tests, every one of which DECLARES its database (e.g. `shared::getConnection()` →
  `TestDatabaseConnection(type = DatabaseType.H2)`). Upstream's `DatabaseConnection.type` is `[1]`, no default. Fix:
  read the declaration; refuse by name what cannot be read; delete the defaults.
- **W2.2 Import groups per section**, the structural completion of W0.6 pushes 6/6b: every `elementImports`/`elementOffsets`
  reader reads sections. The core group becomes the reference's 29 for Pure source only after a probe of resolutions
  served only by `variant`, `relation`, `precisePrimitives`. Gate: CANDIDATES identical, or the probe's rows explained.
  Size ~1.
- **W2.2b The query layer, two pushes** [L1 #4, L4 §4.6, T4]: (1) before W2.3a push 2a: the named, pure, memoized queries
  of §1 over a model snapshot (over today's parse bodies; W2.3a's pushes 2a and 4b re-plumb `typeBody`/`resolveBody` onto
  resolved bodies); each result carries its diagnostics; an in-progress sentinel reports cycles; diagnostics sorted by
  (span, code); deterministic iteration; a retention policy tied to the server's model cache (`cache/HandleStore`); no
  `ConcurrentHashMap.computeIfAbsent` recursion (cf. `PureModelContext.java:249`, `ClassLayouts.java:81`); today's
  per-`SpecCompiler` memo (`SpecCompiler.java:48`) becomes a query. (2) after W2.3a: D10's compile-all failure flip (the
  compile-all lane and API fail on any body diagnostic; the note's "G4"). Gate: the corpus run both ways gives equal
  diagnostics per body; per-query latency within budget. Size 2–3.
- **W2.3a The resolved model with TODAY'S rule** (D12) [W2 #4, #5, T4] — ten pushes behind a converter seam (the note's §6
  shape, refined): **1a** the site-key change first (the (body, span, spelling) key and the mint multiset, note §6) with
  today's probe otherwise unchanged; the Form table as data; shadow probes (a)–(e) plus (f) candidate sets disagreeing on
  form and (g) the name readers outside `compiler/spec` counted per site (~63: `lineage/ScanRelations.java` 25,
  `lineage/PkInference.java` 3, `normalizer` 12, `validation/ValidateDesugar.java` 9, `NameResolver` 7, testdatagen 2,
  StatementInline 2) and the site list W2.6 needs; if (f) ≠ 0, push 2c takes the roster change and its register rows.
  **1b** `ResolvedExpr` records with `ExprId(body, local)` — a *body* is (declaration id, body slot), the note's §1 slot
  inventory; typer mints allocate ids deterministically per body — the span table, the resolved declaration family and
  the converter, used by nobody. **2a** typer entries, `Env` and the five rewriters (StaticFold 845 lines, SourceSubst 262,
  CallShapes, LambdaBodies, AlphaRename) carried over through the query layer's `typeBody`; gate: snapshot byte-identical.
  **2b** G's ~300 mints and ~60 rebuilds through the builder; `PROTOCOL_DESUGAR_DEBT` rows → 0; the ArchUnit rule that
  `compiler.spec` constructs no `protocol.spec` node lands. **2c** the 121 `compiler/spec` name readers (18 `CoreFn.of`, 22
  `ResolvedNames`, 81 `.function()`) switch to ids and the Form table (the semantic push; W2.3a is the single owner of form
  recognition). **3a** E declare-then-define (still on parse trees). **3b** E mints through the builder; E.6 re-resolution
  deleted. **4a** the post-D consumers off the parse tree (PlanAllocations, testdatagen, validation, StatementExecutor,
  NameResolver's D½ chain); lineage moves straight to the typed HIR (W6.4's design, E9), not to `ResolvedExpr`. **4b** D
  emits the resolved model; the converter, `candidateFqns` and IDEMPOTENT deleted; the accessor rule lands. **5** carve
  `world`, `resolved`, `binder`, `elements` (§1b). `spelled` is read only by diagnostics and printers (a guard). Poisoning
  per D10. Gate per push as stated, overall: CANDIDATES by (site, candidate set) identical except (f)'s rows; poisoned
  calls by code against the UNKNOWN-FN baseline (the probe's count of calls resolving to no function); heap after build on
  the 100K test within budget; the reference lane unchanged. Deletes: E.6, the name-dispatching readers, the parse
  records' body use after the resolver. Size 11–15.
- **W2.3b The reference's candidate rule** (imports ∪ core ∪ Root, no own-package tier; `reference-matching.md` 1–3) for
  Pure source; engine input keeps the engine's namespace (D13) [W2 #6]. The parser's minted and bare names (`col`, `agg`,
  `func`, `olapGroupBy`, `tdsRows`, `tableReference`) each get a declaration or a syntax node D binds [W2 #7]. Gate: the
  reference lane's rows; moved rows listed by class; W1.13's rows unchanged or classified. Size 1–2.
- **W2.4 Member semantics** [W2 #19, W4 F3, L2 F1, F4, F8, F10]: the typer resolves `Member` against the receiver's type.
  **Automap (corrected reading):** a `[0..1]`, `[0]`, `[*]`, `[1..*]` or multiplicity-parameter receiver automaps; only
  `[1]` takes the direct path (FunctionExpressionProcessor, "FEP", :306, :325, :359 call `isToOne(m, true)`). Lite's IR
  keeps `Member(PropertyId)` with the lifted multiplicity and `Ref<EnumValue>` for enum values; it does not emit the
  reference's `map(v_automap|…)` or `extractEnumValue(enum,'NAME')` (register S5, S6); the reference lane's join
  normalises those rows. Milestoning date propagation into lambdas follows the reference's rule, keyed by declaration id
  (the reference propagates only into `map`, `filter`, `exists`, `project`, the `getAll` date forms and `subType`,
  `NativeFunctionIdentifier.java:26-32`, gate at `MilestoningDatesPropagationFunctions.java:127-130`): a date-less
  milestoned property elsewhere is a compile error there; lite matches it or registers the difference. `new` has two
  overloads (`new.pure:29, :37`), `copy` two (`copy.pure:25, :33`); the String argument is an object id. `PropertyId`
  introduced on member access. `accessProperty` is changed in place here and moved out of Typer by W3.3 (its owner).
  Gate: reference-lane PROPERTY_AS_CALL rows; a `[0..1]` receiver case for a property and a qualified property; a
  milestoned-property-in-`sortBy` case; rosters. Size 2–3.
- **W2.5 `VarId` for every binder** [W2 #11–#13]: D allocates ids for explicit and implicit binders (`this`, service
  parameters, `_path`, `_gf<n>`, `_s<N>_`); `ParameterDefinition` gains a `VarId`; G gets a fresh-id supply; the record
  keeps only the id in equality (the display name is a table entry) [L4 §4.3]. D, E and G readers switch here; G½ switches
  in W4.2, H in W4.3 step 0/2, I in W5.1b. Alpha-renaming becomes id-freshening per copy. Gate: verifier (bound in scope,
  unique) and golden dumps modulo ids. Size 2–3.
- **W2.6 Element references as `Ref<Kind>`** (FQN wrappers; `DeclId` never in a tree) [W2 #14, #15], limited to the
  site list W2.3a push 1a produces unless C1 rules otherwise. Gate: rosters; element-name compares shrink. Size 1–2.
- **W2.7 Typer desugars bind their declaration** [W2 #25]. Gate: PICK rows identical. Size ~1.
- **W2.9 Mapping-DSL references resolved in D** [W4 F5]: the normalizer's private resolvers
  (`AssociationSynthesis.resolveAssociation`, `StoreSubstitutionRewrite`, `MappingClosures`) replaced by D's output.
  Gate: rosters. Size 1–2.
- **W2.8 The merge point by table**, after W3.3 [W2 #17]: `FunctionCompiler.functionsAt` reads the declaration table
  only; `isPlatformOwnedFunction`, the PCT stereotype check and `SUPPRESSED_ONCE` deleted; broken overloads reported
  (`FunctionCompiler.java:139-158`). Gate: OVERLOADS and PICK rows identical. Size ~1.

### W3 — Lite's own expert typer (catalogue; W3.7 in Phase 1, W3.4 in Phase 3, the rest in Phase 4)

- **W3.1 TDS inventory and the carrier decision**: ~100 relation readers, `TdsErasure.refineResult`
  (`InferenceKernel.java:1050`), `eraseTdsRow`/`TDS_ROW`, `TypeAnnotations:152-163`, `CastChecker:33-39`, the
  `isSchemaErased` inlining gates; how the schema fact rides on 557 `new ExprType(` sites (all of core) [W3 #14]. Carrier:
  nominal `TabularDataSet`, schema as a side fact. Gate: the inventory in GATES with each reader's fate; a §2 ruling if
  the carrier changes. Size ~1.
- **W3.2a Matcher prerequisites** as sub-pushes [W3 #10]: C3 linearization (ours is BFS, `KnowledgeLayer.java:139`,
  `InferenceKernel.java:1836`); class type-parameter variance (the parser drops `-U`); lambda values carrying
  `LambdaFunction<{…}>`; schema-algebra formals as NON_CONCRETE; a PrecisionDecimal row. Gate per sub-push: a unit test
  per prerequisite against the reference's documented behaviour; CANDIDATES/PICK unchanged. Size 2–3.
- **W3.2b The matcher**: one lexicographic key over (type distances by C3 index, multiplicity distances), types before
  multiplicities (FunctionMatch, "FM", :69-103), a type parameter NON_CONCRETE (GenericTypeMatch, "GTM", :170-177),
  multiplicity arithmetic as MultiplicityMatch ("MM") :273-301; one test per matcher trap (the step-3 traps C1–C11 and C31,
  `step3-design-2026-09-26.md` ~:600-620). **First a one-day spike** [L5 §4]: call the pinned `legend-pure-m3-core` jar's
  GTM on two World types through a Type → M3 bridge (`MODULE.bazel:131`); if it cannot be driven, compare against a table
  of reference outcomes generated by the reference dump. Then the differential property test over type pairs drawn from
  the World. Gate: the property test green; the trap tests green. Size 2–3.
- **W3.0f Front-end differential fuzzing** (before W3.3) [L1 #6g, T4]: random well-typed Pure expressions over the World,
  compiled by legend-pure (through `tools/reference`, whole expressions) and lite; rows (expression, accept/reject, pick,
  result type and multiplicity); a pinned disagreement set. Gate: the fuzzer runs in a manual lane with its set pinned.
  Size 2–3.
- **W3.3a Type facts before the loop** [W3 #1, #2]: the TDS switch and the argument-type facts (literal multiplicities,
  relation/Variant multiplicities, lambda carrier types, Nil/Enum/Any escapes). Gate: the reference lane's type rows not
  grown; rosters. Size 1–2.
- **W3.3 The candidate loop, lite's own solver** (D15) [W3 #3–#9, L2 F2, L1 #7, T4], **with a shadow period**: **3.3-1**
  `docs/TYPER_RULES.md` (first binding wins, relation concatenation/widening, reverse inference, the failure semantics of
  each of the 30 `catch (` sites in `compiler/spec` — 18 catch `TypeInferenceException`; LUB with variance is W3.5's) and
  firing counts per selector and per catch site (no behaviour change); **3.3-2** the new solver (one inference context per
  call carried across candidates, keyed by (context, name); a deterministic tie rule of lite's own, declaration order, with
  hash-order ties pinned as "reference-nondeterministic"; the merge-mode drop of `TypeInferenceContext` ("TIC",
  :472-480, a bug by its own comment) and candidate leftovers not copied unless a probe shows an observable class on the
  corpus) runs beside the old one under `LL_SHADOW`; the pick/type/instantiation disagreement set, bucketed, is the work
  list; **3.3-3…n** one selector family per push: the general call path (`Overloads.java:490`), then `accessProperty` (out
  of Typer; `Typer.java:1061`), then the checker routes (`resolveOverload` at ~13 sites in 12 files, `kernel.accepts` 14)
  in two batches; each family's catch sites become W1.2(c)'s speculative scopes as it switches; the kernel's positional
  fallback (`InferenceKernel.java:88-90`, `asSuper(...).orElse(ag0)`) after its firing-count probe; **3.3-last** both old
  algorithms deleted (Typer 1,748, InferenceKernel 2,049, Overloads 1,209 lines are the code replaced). Gate: reference
  lane OVERLOAD rows to the pinned residue; type rows and the rejection bucket not grown; W1.13 and W3.0f rows classified;
  a quiet timing within budget. Size 6–10.
- **W3.4 G½ substitutes, never re-types** [L1 #2]: with instantiations recorded (W1.1b, filled by W3.3's solver),
  `UserCallInliner`'s re-unification (`UserCallInliner.java:453-479`, swallowing `TypeInferenceException`), `redispatch`
  (`:485-522`) and `resolveStamps` (`:529-540`) are replaced by substitution with the recorded instantiation; any genuine
  re-selection becomes a named typer query `specialize(FunctionId, typeArgs)` and a register row. Gate: G½ contains no
  `unify` and no candidate selection (an ArchUnit rule); fold results identical per test; snapshot byte-identical. Size 1–2.
- **W3.5 Kernel rules, second half** (owner of LUB with variance): `register`, LUB with variance, `GenericTypeOperation`;
  checkers that existed only for kernel gaps retire; scope ruled after W3.3 from the new solver's type-row residue. Gate:
  reference lane type rows; the retired checkers deleted. Size 1–2.
- **W3.7 The D11 experiment** (ruled 2026-09-29) [L1 #9, T4] — runs **in Phase 1** (§4), on a throwaway branch `spike/d11`
  (never merged; its report committed to `docs/plan-audit-2026-09-26/d11-experiment/`). Time box 5 sessions; failure or
  timeout means option S, with the report. Build: a sealed algebra family (Scan over a class, Filter, Project, Join,
  Aggregate, plus the nodes §1 row R lists: Window, Sort, Slice, SetOp, Unnest) and a sealed scalar family; an algebraize
  step from the typed HIR after G½; a rewrite that expands a class mapping into tables (for M2M: view-over-view
  substitution of `$src` bindings; for milestoning: a temporal predicate on the Scan with its date parameter); **its own
  small algebra → `SqlQuery` lowerer** (converting back to `TypedSpec` would itself be an escape hatch). Any node added
  beyond this list is a recorded outcome. Cases, chosen by test FQN from the mapping-heavy set before starting and narrowed:
  navigation across a business-temporal association, a one-level graph fetch (a Nest/JSON-aggregate node, recorded), a
  one-hop M2M chain. **"No object-level escape hatch" is mechanical:** the spike's Bazel target has no dependency on
  `//core:resolver`; no field of the two families is typed `TypedSpec`, `ClassSource`, `TemporalFrame` or `ClassMapping`
  (a reflection test); the scalar family admits none of `d11-homework-2026-09-29.md`'s 27 relation-class kinds and no
  class-valued kind. Rows: bag-equal and unordered on the named W1.10a fixture mutations; graph fetch compared as
  normalised JSON trees; where today's rows differ, W1.10c's engine rows adjudicate (equal to the engine is a pass and a
  W0.6-style defect in today's code). Pass: all three cases. Record per case the nodes used, the rewrite rules and their
  lines, any new node kind, whether a scalar leaf needed a relational child (d11 homework open question 1), and a size
  estimate for W4.1r plus W4.3-under-R. D11 is ruled on the report at **C1**. Size 3–5 (narrowed cases).
- **W3.6 Forms by declaration** [W3 #15]: the Form table (W2.3a) drives the typer. If D11 = R the typer emits
  uniform calls and algebraize (W4.1r) recognises forms; if S, each ambiguous typed kind splits into relation and scalar
  kinds by the chosen overload and `TypedRelationOp` grows to every relational kind. Gate: the lowerer's run-time relation
  checks counted and shrinking; rosters. Size 1–3.

### W4 — The middle (catalogue; order in §4, Phase 3)

- **W4.0 The H gate**: W1.7's snapshot plus a post-H dump over the mapping-heavy set; W1.10 green on it; any open W0.6
  store-resolver pins listed. If D11 = S: the D11 census (`d11-homework-2026-09-29.md`, the questions after §6; hooks
  at `StoreResolver.resolve`'s exit, `Lowerer` `relation()` default and scalar catch-all, `Anchors.spaceOf`, the six
  native relation arms; the probe SPI `DecisionProbe` lives in `builtin`, which cannot name `TypedSpec`, so it needs a new
  SPI and a recorded layer edge) decides the leaf type by rule: a kind that ever reaches H's output in scalar position is
  in the scalar family. Size 1–2 (+2–3 for the census).
- **W4.1a Mapping elaboration after F** (today's `ClassSource` shape if D11 = S; if D11 = R its output is the algebra, a
  mapping being a view) [T4 G3]: per (MappingId, SetId), on demand through the query layer; a set's
  failure poisons the set; bindings keyed by a sealed `BindingKey` (Property | Local | SubtypeColumn | PrimaryKey)
  [W4 F4]; the set kinds listed and each with a shadow-probe row: embedded, inline, otherwise, merge, inheritance,
  enumeration mappings, aggregation-aware. Gate: the shadow probe (new `ClassSource` = old up to renaming) for every
  (mapping, set) the corpus and stress lanes touch [W4 F16]. Size 7–10 with W4.1b. **W4.1b** (join edges as data) is its
  own push inside W4.3 step 6.
- **W4.1r (if D11 = R) Algebraize**: the step from W3.7 made real: every relational form recognised by declaration id;
  semantics as node properties (equality kind: Pure total equality vs SQL `=`; null-strictness; determinism/collation;
  nullability — the equality kind node already exists from W0.6 push 10); the lowerer's ~38 run-time relation/scalar
  decisions deleted as their kinds disappear. Gate: snapshot reviewed; W1.10 (a, b, c) green on the mapping-heavy set.
  Size from W3.7's estimate.
- **W4.2 One G½ and the schema evaluator** (after W3.4 and D19, ruled at C1; keyed by names under W0.6 push 2's binder uniqueness until W2.5 re-keys it by `VarId`): one hygienic engine over the typed
  HIR by `VarId`, replacing SourceSubst, UserCallInliner, `StaticFold.inlineUserCall`, AlphaRename, StatementInline,
  LiteralMapUnroll and the resolver's private inliners, one slice each [W4 F8]; source-level β-expansion during typing
  ends. The D8 schema evaluator replaces `StaticFold`: folds only in schema positions over the pinned operation list;
  homework first: the operations the corpus's `NormalizeRequiredFunction` bodies use (selection criterion stated; counts
  differ: 55 marked functions vs 97 applications in 38 files [L2 F11]). Gates: fold results identical per test; the
  snapshot byte-identical; the operation-list pin; the never-a-row-value test; `columnValueDifferenceTest`,
  `rowValueDifferenceTest`, `zScoreTest` on DuckDB unchanged. Size 4–6.
- **C3 go/no-go** (§1a, with its decision rule) before W4.3 steps 1–9.
- **W4.3 Store resolution as passes** (if D11 = R: as rewrites of the algebra, and steps 2–7 below collapse into it), each
  slice landing alone [W4 F2, F3, F13, F14, T4]: **0 explicit state**: one `ResolutionState` value replacing the shared
  mutable fields (`temporal` reassigned by nested resolutions, `StoreResolver.java:121-123, :1496, :2773, :2952`;
  `letBindings` shared by reference with every TemporalFrame, :90-91; `freshVarCounter`, `serializeTypeCfg`,
  `checkedEnvelope`), the three setter-injected cycles removed (`setConstructedRows`, `setOwnStepSplicer`,
  `setNavMaterializer`, :137-151), `Anchors`' identity-keyed memos (`Anchors.java:66,132`) keyed structurally, and H's
  variables keyed by `VarId` (W2.5); 1 the desugar pre-pass (ChainNormalizer, ChainDispatch, the chain-op
  rewrites, SubQueryLift); 2 a structural `NavPath` key replacing dotted chain keys, `#fN/#dN` heads and identity-keyed maps
  (10 `IdentityHashMap` constructions in `resolver/`, 27 in all of core by `grep -rn 'new IdentityHashMap'`; SQL
  byte-identical); 3 route as an annotation on `TypedGetAll`; 4 temporal spec collection, then
  one demand trie keeping first-read order; 5 temporal attribution `NavPath → TemporalContext`; 6 an inventory of the
  join-strategy decisions, **each marked observable (fan-out in `project`, null-extension for optional properties, EXISTS
  de-duplication in `filter`) or shape-only** [L2 F3], then an explicit join tree whose nodes carry their ON predicate set
  (the verifier asserts no null-supplying-side predicate outside its ON) [L3 #9], with **W4.1b** (join edges as data);
  7 read lowering through the join tree, a naming pass, the relational skeleton behind a print-back adapter whose
  deletion is pinned to W5.1c; 8 graph fetch in two halves; 9 relocations and the unowned concerns (metamodel rows, JSON
  source frames, M2M composition, execution-option appends, the 37 `findFunction` re-picks). Gates: steps 0–5 snapshot
  byte-identical; steps 6–9 W1.10 (a, b, c) rows green plus a classified SQL diff in which every changed statement is
  attributed to a named join-strategy decision from step 6's inventory (byte identity cannot hold across a new join tree
  and naming pass); W1.10 green per step. Deletes: the one-pass resolver's state. Size 19–32 (re-sized at C1).
- **W4.4a Load by manifest, non-M2M walls** (after D9); **W4.4b** M2M on the new passes, scope boundary from C1. Gate:
  census pins, boot-time growth within budget. Size 2–4 (W4.4b open-ended until its C1 boundary).
  *Finding 2026-10-03 (`docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md` §4 A2):* `StatementExecutor.containsEffect` catches `TypeInferenceException` and scores
  an un-typeable callee non-effectful. Measured over the full suite: 23 callees — 17 upstream library functions (all in
  the pinned trees, so legend-pure compiles them) and 6 deliberately ill-typed core test helpers; "it never runs" is
  FALSE for 6 of the 17 (the inliner hits them too); 40 corpus tests reach it, 19 rostered, 21 PASS only because the
  failing `match` arm is not taken (20 of them `toPostgresModel::tests`). Causes: platform functions we lack
  (`isDigit`, `containsAny`, `orElse`, 2-argument `replace` — part of the 191 missing overloads, W2.1), typer gaps on
  valid upstream Pure (generics left as `V`; `Union` where `QuerySpecification` is expected; two multiplicity checks),
  upstream functions absent from the loaded graph (`toRelation::transform`, `getMappingsFromRuntime`,
  `featureFlag::contextHasFlag`, `relation`), and upstream compiler machinery reached as an entry (router, the SQL
  printer's extension builders, pureToSqlQuery aliases, toPostgresModel). Under D10 the scan should be demand-precise on
  user paths and compile-all should report every broken body; whether upstream's unit tests of its own compiler
  internals belong in the world is D9's question (the user's lean: fix what is general, wall upstream's internal-
  structure entries by name).

### W5 — The back end (catalogue; W5.0–W5.2 and W5.6 in Phase 3, the rest in Phase 5)

If C3 chooses "stop the middle rebuild", W5.1c is dropped and W5.2's determinism trait lives on today's SQL tree
[T4 G4].

- **W5.0 The dialect fuzzer** (moved from W1.8, which had no MIR to fuzz) [L3 #4, #5, T3]: random well-typed trees of
  today's `sql` package (`SqlExpr`, `SqlQuery`; `SqlTyping` is the well-typedness oracle) rendered for DuckDB and H2, rows
  compared; a register of declared DuckDB/H2 divergences in which every row names its adjudicator (a PCT test or an engine
  run) and which dialect is wrong (a row without one is a defect); seeded with the `splitPart` divergence. Gate for W5 only
  (rule 0b.14). Size 2–3.

- **W5.1a One lowering table, derived** [W5-W7 #1, #2]: an immutable `LoweringTable`, `(FunctionId, Position) → Rule`,
  built once, refusing duplicates, checked one-to-one against the implementation table; first a probe naming each
  silently overridden `RULES.put` (`times`, `Scalars.java:294, :311`; `startsWith`, `endsWith`, `median`, `hash`,
  `dayOfWeekNumber`). Gate: snapshot byte-identical. Size 2–3.
- **W5.1b Lowering decisions become typer annotations** (cast policy, static disjointness, match subtyping, compare kinds,
  lambda parameter types) [W5-W7 #3]; the lowerer's variables keyed by `VarId` (W2.5), including the 16 name-blind row
  resolvers (`Lowerer.java:795…:2389`, `Sorts.java:113, :122`). Gate: snapshot; the lowerer imports no model lookup.
  Size 3–5.
- **W5.1c The lowerer reads the relational form**; W4.3's adapter deleted [W5-W7 #4]. Gate: snapshot reviewed; W1.10
  green. Size 2–3.
- **W5.2 Semantic MIR** [W5-W7 #5, #6, L3 #3, #6, #7]: each unit or part a distinct record; `Join.Kind.sql` removed;
  (equality kinds are already nodes, from W0.6 push 10); a **null-strictness property per `SqlFn`** (an exhaustive whitelist replacing the
  blacklist `SqlTyping.nullStrict`, `:708-729`); a **determinism/collation trait** on relational nodes (the source of D6's
  classifier); literal typing from the HIR, not magnitude (`AnsiSqlRenderer.java:1370-1378`), non-finite floats spelled or
  refused; the carrier ladder, gated by `CarrierPurityRatchetTest` reaching zero; deep immutability after a probe. Gates:
  snapshot reviewed; PCT rosters by class; `EqualityWorldsConformanceTest`'s declared divergences as a gate; W1.10b.
  Size 2–4.
- **W5.3 Legalisation and one escaper per dialect** with a verifier before emission; an `Identifier` value with
  per-dialect case folding; escaping also reaches the SQL concatenated outside the dialects (`StatementExecutor.java:3050,
  3218`, `exec/Ddl.insertText`, `plan/InProtocol`, `TestDataGenerator`) [W5-W7 #11]. Gate: an injection test per site.
  Size 2–3.
- **W5.4 Semantic types and per-dialect delivered types** (reframed from "the ANSI split") [L3 #6]: `Spellings.ANSI`,
  DuckDB as a layer, `SqlTyping` per dialect used only to place conform casts. After W5.1b and W5.2. Gate: snapshot
  reviewed; W5.0. Size 1–2.
- **W5.5 The Postgres lane** (after W5.2; cut candidate) [W5-W7 #10, L3 #11]: homework first (collation, QUALIFY,
  `ROUND(double,int)`, integer `/`, strict casts, `chr(0)` in `stringLit`, identifier folding); zonky embedded Postgres;
  a Postgres dialect; `Compiler.dialectOf` dispatch fixed (today a non-H2 session silently renders DuckDB,
  `Compiler.java:748-758`); a first fail roster. Size 3–6.
- **W5.6 Engine SQL text as a product dialect** (D16): `EngineStyleH2` (1,901 lines), `EngineStyleDB2` (308),
  `EngineStyleComposite` render from the same MIR and never re-lower; their goldens are kept; their SQL executes and must
  return the native dialect's rows (a lane); every byte difference from an engine golden is a register row (D8
  constants; alias shapes the MIR does not carry). `StatementExecutor`'s `toSQLString`/`planDialect` choice
  (`:473-480`, `:1179-1190`) becomes an explicit option. Gate: the rows lane; goldens pinned; the register. Size 1–2.
  **Finding 2026-10-03 (the database-owner line, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md C6): D16 is
  being REVERSED by the user — the engine printers become test tools, not a product dialect.** Measured: their H2 text,
  replayed on the oracle, does not run for 273 of 1,087 differing asserts (alias renames pointing nowhere; DuckDB
  spellings inherited where no golden pinned H2) and answers differently for 41; a rows-first verdict with exact engine
  text only where rows cannot run (119 asserts) changes one verdict versus today. Do not build W5.6's rows lane for
  these printers; C6 carries the move.

### W6 — Plan, runner, periphery (catalogue; Phase 5)

- **W6.3 The judge SPI** (D7), before W6.2: a public judge SPI; the per-assert join of the two judges stays a gate.
- **W6.1 A staged plan IR**: late-bound nodes (SchemaProbe, DynamicPivot, ForEach(values, template), Effect barrier)
  **passing values as JDBC bind parameters** typed by the compile-time column type, never re-spelling database values as
  SQL literals (`exec/DynamicPivot.java:61-111`) [L3 #8]; service and user parameters bound too; probe and main query in
  one transaction; each dialect's required session settings applied and verified once per connection
  (`DuckDb.java:32-33` TimeZone, `H2Settings.java:45-51`) [L3 #12]; one printer gated by lite's own plan goldens (engine
  plan text is D16's dialect, not this printer's gate). Gate: goldens; an injection test through parameters.
- **W6.2 The runner**, after W6.3 and W4.2: resolves late-bound nodes in order; `StatementExecutor` stops re-running G,
  G½, H, I (W1.3 routed them; now the copies are deleted); the census and env tracing become an injected observer;
  global static state removed; `Executor.decodeAny` and the `WireTypes` rewrite named and typed by the plan. Gate: the
  nested-run register (W1.3) empty; rosters; the snapshot. Size 3–5.
  *Finding 2026-10-03 (the database-owner line, `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md` §3.3–§3.5, C2):* the plan/execution split is measured and
  ready: every plan-side library is free of `java.sql`; the root package splits into a `planner` library (`Compiler`'s
  ~20 plan methods + 9 clean classes, no `:exec` dep) and the execution side (`Compiler`'s 11 execution entry points
  → an `Execution` front door, with `StatementExecutor`, `BodyCompiler`, the judges, `LiteralFold`, `PlanEnvelope`,
  `VerdictArm`, `CsvLoad`, `SeedSqlForms`, `PlanAllocations`); `//wasm:boundary` then depends on `:planner` only. The
  one plan→execution crossing is the effect analysis (`programFacts` → `StatementExecutor.containsEffect`; and
  `Compiler.containsTdgGenerator` used by `BodyCompiler`): it moves to the compiler first. Not done there: this item's.
  *Update 2026-10-04 (user): the STRUCTURAL split moves to the database-owner line* (its C1 then C2, after C3): the
  effect analysis into the compiler, the `planner` library with a compile-once API, the `Execution` front door, the
  ~127 callers moved. W6.2 keeps the runner rewrite only. B2/B3 and C4 stay here.
- **W6.4 Periphery**: lineage over the typed HIR plus H's binding map; test-data generation through the MIR; the server a
  thin adapter; test runners and the probe out of the product jar. Gate: the product jar's contents list.

### W7 — Close-out (catalogue; Phase 5, or folded into the owning items)

- The target map met (§1b), `core-layers.txt` equal to the map file with no exceptions left.
- Each regex guard deleted after the type or verifier that asserts its invariant: `IdentityGuardrailTest` pattern by
  pattern; `STRING_DISPATCH_SITES` with a charter C6.1 amendment; `VerdictChannelRegisterTest` after W6.3;
  `JavaEvalLedgerTest`'s residue register after W6.2/W6.3 and an `AGENTS.md` edit; the ArchUnit rules that javac or the
  layering test now enforce.

Per-item sizes are in each item; phase and total sizes are in §4.

---

## 6. The alternative D1 did not take (record only)

W2.3–W2.6 put the resolution on the parse nodes instead: `AppliedFunction`'s callee becomes a sealed `Callee` (`Spelled` |
`Bound` | `Member(name)`); `VarId` and element ids become fields on `Variable`, `LambdaFunction` parameters,
`PackageableElementPtr` and `EnumValue`, null before resolution. Rule 0b.10 is then waived for D.
