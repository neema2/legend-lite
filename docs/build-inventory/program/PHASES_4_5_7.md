# Phases 4, 5 and 7: execution brief (the default world, Pure.java as rows by id, the self-contained bump)

**Applied on 2026-10-07, after this brief was written** (so a reader does not redo them): the plan now carries D4-1
(the closure's bodies) and D4-5 (the generator's parser) as open decisions in Phase 4, the corrected legacy TDS facts
(D4-2: 14 query handlers; `columnByName` a class member; three left for the user to decide), D4-12's schedule for the
109 unrowed versions, D4-13's owner for F-L1 (Phase 3b item 1), decision 1's note that the "platform's own Pure" row
kind is not built yet, decision 3's full list of rule texts, and Phase 5's "several landings, no PRs" with D5-4/D5-5
noted. `docs/PARKED_WORK_LEDGER.md` PARK-11 is corrected the same way. The open decisions themselves are not decided.

**After the cold read (`COLD_READ_2026_10_07.md`), these parts of the body are superseded:** D4-13 is decided (F-L1
belongs to Phase 3b item 1); §2.6's quoted bar ("the 18 legacy TDS functions ... cannot hold as written") is replaced
by the plan's corrected Phase 4 check (the 14 query handlers, `columnByName` as the class member, the other three as
decided); §0 item 2 quotes PARK-11's old text (corrected); §3.2 and D5-8's "the plan still says Several PRs" (fixed:
"several landings, no PRs"). D7-7 is assigned: GENERATORS.md's update note gives the writer-and-diff-test guard to
Phase 7. D7-4 is the same question as `PHASE_8.md` OD-6 and OD-10: decide them together. The "platform's own Pure"
row kind (D4-4) has one owner, the 3b brief's 3b-O1. `WORLD/` now regenerates from the repository: `rerun.sh` runs
`graph.py` and `index.py` first, and `e1_handler_files.txt` is committed.


Written 2026-10-07 for a session with no memory of the work so far. Read-only research: nothing in either repository
was changed and no Bazel command was run. Every fact carries a path; **UNVERIFIED** marks what could not be checked.
Where the documents leave a design open, it is listed as a decision for the user, with options and evidence, and a
recommendation only where the evidence supports one.

## How to read this

**Paths.**
- `BR/` = the worktree `runs/build-rebuild` (branch `build/phase3`: 4 Phase 3 commits on `293318dda`, HEAD
  `bc0de1f70`, plus the uncommitted audit fixes). Code paths below are relative to it unless they say otherwise.
- `BP/` = the plan worktree `runs/bazel-plan` (branch `docs/bazel-first-class-plan`). The plan is
  `BP/docs/REBUILD_PROGRAM_2026_10_06.md`, **read as the working copy**; it was being edited while this was written
  (340 lines at 03:01), so plan statements are cited by section and quoted, not by line.
- `EXP/` = `BP/docs/build-inventory/manifest-world/experiments/` (the experiment harness and its outputs).
- `WORLD/` = `BR/runs/homework/world/`: the experiments' scratch data (`closure.json`, `uni_edges.tsv`,
  `decl_repo.json`, `e1_seeds.json`). Not committed; `EXP/rerun.sh` regenerates it.
- `ENGINE/`, `PURE/` = the pinned archives, legend-engine 4.145.0 and legend-pure 5.99.0
  (`BR/release.MODULE.bazel:24-25`): `$(bazel info output_base)/external/+http_archive+legend_engine_src` and
  `.../+http_archive+legend_pure_src`. Use `find -L` in them (the tree is symlinked; a plain `find` finds nothing).

**Words used here, in plain terms.**
- **Default world:** the upstream declarations (classes, enums, functions, with upstream's bodies) that every user
  program starts with. Today that is `core/src/main/resources/com/legend/builtin/prelude.pure`; Phase 4 regenerates it
  from upstream alone.
- **Query surface:** the functions legend-engine's own compiler lets a user query call (its handler registrations:
  `Handlers.java`, `CoreCompilerExtension.java`, `RelationalCompilerExtension.java`), plus the classes those two
  compiler modules create.
- **Closure:** add everything the chosen elements refer to, then everything those refer to, until nothing new comes in.
- **Function id:** upstream's signature id string, e.g. `meta::pure::functions::boolean::and_Boolean_1__Boolean_1__Boolean_1_`
  (`core/src/main/java/com/legend/model/FunctionId.java:11-18`; computed by `SignatureMangle.mangle`).
- **Row:** one entry of the implementation table, keyed by function id, saying how that function runs
  (`core/src/main/java/com/legend/platform/Implementation.java:17-62`: Form, Intrinsic, Body, Unimplemented, Refused).
- **Result views:** the system metamodel's tables, mapping and small classes that let Pure read the results of
  `executionPlan`, `execute`, `scanRelations`, `scanColumns` (the plan's decision 1).
- **Seal:** a committed file holding the release and a hash of every bump-generated file; a daily test compares.
- "Upstream native" = upstream's `native function` keyword (a Java body upstream). "Platform-lowered" = what our
  Pure.java declares. Bare "native" is not used.

---

## 0. The short version

- **Phase 4** replaces the hand-steered prelude generator with one that builds the default world from upstream
  alone: legend-pure's `platform*` plus the engine's `core_functions_*` (the repositories legend-pure itself compiles
  first, `PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/serialization/runtime/PureRuntime.java:248`),
  tests stripped by upstream's markers, plus the query surface, closed. The result views stop loading at startup; the
  legacy TDS functions get rows by function id; the stale rule texts are corrected; boot speed is profiled and
  improved before any pre-built boot layer is considered.
- **Phase 5** removes Pure.java's 821 copies of upstream signature text. Each becomes a row "function id → how the
  platform runs it"; the declaration comes from the world. The membership list, the claims ledger and their generators
  go.
- **Phase 7** makes the bump self-contained: one writer for the upstream files only, a seal checked every day instead
  of regenerating, `//:update_generated` for our own files only and manual, and one real bump end to end.

**What has to be decided or measured before coding (details in each phase):**
1. **"Bodies of runnable functions" needs our code.** The measured closure decided which bodies to follow by reading
   Pure.java, `CoreFn.java` and `WalledBodies.java` (`EXP/closure.py:61-77`). That contradicts "no generator reads our
   code". Following every body instead is upstream-only and adds 80 names, 74 KB at the measured size
   (`EXP/closure.txt`: 563 names, 402 KB against 483 names, 328 KB). (D4-1)
2. **The closure does not bring three of the legacy TDS functions, and the fourth is not a function.**
   `columnValues` (2 versions), `renameColumn` and `window` (all in `ENGINE/.../core/pure/tds/tds.pure:516-533,748`)
   are referenced by no non-test upstream element; `columnByName` is a qualified property of `TabularDataSet`
   (`tds.pure:21`). PARK-11 says "the other 4 come through its closure": wrong. legend-engine itself does not let a
   user query call the three (no handler registers them). (D4-2, §5.1)
3. **No experiment measured the real Phase 4 world.** Every measured world dropped upstream functions by name
   (`EXP/synth_prelude.py:53-67`), the rule Phase 3 deleted. The real world is bigger (it keeps the upstream versions
   that rule removed), so the boot costs on record (+74 ms JVM, +590 ms WASM) are most likely a floor. (U4-1)
4. **Phase 5 changes what users can call.** 70 of Pure.java's 821 upstream rows (45 names) are not in the measured
   default world (`executionPlan`, `toSQLString`, `fromEpochValue`, `sortByReversed`, `sqlNull`/`sqlTrue`/`sqlFalse`,
   ...; §5.2). Most are corpus helpers and legend-engine does not let users call them either, but the platform's own
   code calls `sqlNull` in every world (`compiler/spec/Typer.java:233-238`, `builtin/DynaFnDecisions.java:239-242`).
   (D5-4, D5-5)
5. **Which parser the default-world generator uses decides whether the seal can be trusted.** If it uses our parser,
   a parser change can change the world without a bump and the seal will not see it. (D4-5)
6. Smaller ones: where the module choice is kept (D7-2), the 6 enums Pure.java declares by hand that upstream core
   also declares (D4-8), who adds the "platform's own Pure" row kind decision 1 needs (D4-4), the 109 upstream versions
   still without a row (D4-12), and whether `native_declarations` retires or becomes a sealed report (D7-1).

---

## 1. The four questions, answered

### 1.1 The catalog resource (UPSTREAM_ONLY_HOMEWORK §2) or rows by id (plan Phase 5): which is current?

**Current: the plan's Phase 5.** "No signature text: every row names an upstream function id and its implementation.
Declarations come from the world (the default world for users, the program's own files for the corpus and PCT)."

**Superseded:** the design of 2026-10-05 in `BP/docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` §2 (the `gen_natives` row)
and `BP/docs/build-inventory/upstream-only/R1.md` §1 A-C: one upstream-only catalog resource of about 16.5k rows,
`Pure.java` as `X = catalog("<id>")`, "native_declarations widened IS this catalog", and a join action in core's
build embedding about 800 rows for WASM. It was written before the manifest-world experiments. What replaced it:
- `BP/docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` §5: "Pure.java becomes decision rows keyed by `FunctionId`
  (experiment 2), with the implementation table as the ownership filter";
- `BP/docs/GENERATORS.md` lines 7-11 (update note): Phases 3-5 replace §2's "basic split now" for items 5-8;
- the plan's decision 5 and Phase 5 "Retires: ... the natives generator".

**What Phase 5 builds**, from the plan plus what the code forces (each open point is a decision in §3.8):
- Pure.java's 821 upstream `signature("...")` constants (`BR/core/src/main/resources/com/legend/builtin/native-membership.tsv`,
  821 rows over 456 names; 864 `= signature("` sites in `builtin/Pure.java` minus the 43 `meta::legend::lite` ones)
  become rows: an upstream function id plus its implementation, through the registries the implementation table
  already reads (`platform/Registrations.java`, assembled in `lowering/PlatformRegistrations.java:54-95`).
- No signature text and no catalog resource: the declaration table is built from the world's declarations, not
  `Pure.all()` (today `compiler/element/PureModelContext.java:588`, `lowering/PlatformRegistrations.java:45-52,93`).
  A row whose id the world does not declare does nothing in that world.
- The 43 `meta::legend::lite` declarations stay ours (the north star lists them under "Ours"); where they live is open.
- The row test: "a row matching nothing fails and lists that name's real ids", reading the pinned archive. Its
  ancestor is `spec/src/test/java/com/legend/generators/CatalogUpstreamDiffTest.java` (EXACT/DIVERGENT/MISSING over
  every upstream declaration, `DIVERGENT == 0` at `:184-187`).
- Retired: `native-membership.tsv`, `//spec:native_membership_draft` and `//core:draft_native_membership`,
  `native-claims.tsv` with `//spec:gen_claims`, `//spec:claims_generator_lib`, `//core:core_next`,
  `//core:core_next_prelude` (decision 5), and `//spec:gen_natives` with `NativesGenerator.java`. Not named by the
  plan but left without a purpose: `//spec:native_declarations` (it calls `NativesGenerator.upstreamDeclarations` over
  the membership list, `spec/src/gen/java/com/legend/generators/NativeDeclarations.java`), `NativeSignatureGeneratorTest`,
  and the `Claims` library with `ClaimRegistryTest` (D5-6, D7-1).

### 1.2 After Phases 4 and 5: which bump records exist, which generators retire, what the seal covers

GENERATORS.md §2's items 5-8 ("split now ... full catalog later") and §6 steps 4-5 are superseded (§1.1). The table
below is the state after Phases 4 and 5, from the plan and the code:

| # (GENERATORS §2) | Generator | Writes | After Phase 4 | After Phase 5 | Phase 7 |
|---|---|---|---|---|---|
| 1 | `//parser-equivalence:gen_fixtures` | `parser-equivalence/src/test/resources/engine-grammar-fixtures.jsonl` | unchanged | unchanged | bump record, sealed |
| 2 | `//parser-equivalence:gen_manifest` | `parser-equivalence/src/test/resources/corpus-manifest.tsv` | unchanged | unchanged | bump record, sealed |
| 3 | `//tools/engine-runner:vocab` | `tools/engine-runner/vocab.tsv` | unchanged | unchanged | bump record, sealed |
| 4 | `//spec:gen_imports` | `core/src/main/java/com/legend/compiler/CoreImports.java` (upstream-only since Phase 1) | unchanged | unchanged | bump record, sealed |
| 5 | `//spec:gen_natives` | Pure.java's signature text and `AT_*` groups | still runs (reads membership, the committed Pure.java and our parser) | **retired** | — |
| 6 | `//spec:gen_dynafn` | `core/src/main/java/com/legend/builtin/DynaFn.java` (upstream-only since Phase 2) | unchanged | unchanged | bump record, sealed |
| 7 | `//spec:gen_engine_handlers` | `core/src/main/resources/com/legend/builtin/engine-handlers.tsv` (upstream-only since Phase 2) | unchanged | unchanged | bump record, sealed |
| 8 | `//spec:gen_prelude` (`PreludeGenerator.java`) | `prelude.pure` | **replaced** by the default-world generator | — | its output is a bump record, sealed |
| 9 | `//tools/reference:ref_imports` | — | deleted in Phase 1 (plan, Phase 1 status) | — | — |
| 10 | `//parser-equivalence:pmcd_reachability` | `parser-equivalence/pmcd-reachable.tsv` (committed since Phase 1, `parser-equivalence/BUILD.bazel:500-503`) | unchanged | unchanged | bump report, sealed |
| 11 | `//spec:native_declarations` | build output, `manual` (`spec/BUILD.bazel:431-450`) | unchanged | no purpose left | **open** (D7-1): retire, or widen and commit (GENERATORS §2 row 11, §6 step 5) |
| — | `//spec:gen_claims` + `core_next` + `core_next_prelude` + `claims_generator_lib` | `native-claims.tsv` | unchanged | **retired** (decision 5) | — |
| — | `//spec:native_membership_draft`, `//core:draft_native_membership` | draft over `native-membership.tsv` | unchanged | **retired** | — |

Outside the seal, by GENERATORS.md §4-5: our own generators (DataCube facts, icons, the stress corpus, the native
image's metadata) and the measurements (the ratchets, the ladder, keyword coverage, `docs/protocol-roster.tsv`, the
corpus rosters, the reference-lane golden), which the user set aside on 2026-10-05.

**What the seal covers** (GENERATORS.md §3 step 5): "the release, plus the sha256 of every bump record and report",
i.e. rows 1, 2, 3, 4, 6, 7, 8 (the default world), 10, and 11 if kept. The north star says the default world is
made from upstream "and one setting, the module choice"; recommendation (evidence: the default world changes when
the setting changes, and the seal is the only daily check): the seal records the module choice too (D7-2).

### 1.3 What decision 2 (boot speed) requires, concretely

Decision 2: "profile and optimize the boot first; the pre-built boot layer (Phase 4b, the parked compiler plan's W2.1
'generated at build time, not parsed at class load') is decided with measured numbers."

**The baseline on record** (2026-10-06, branch `build/rebuild` at `1689703f2`, before Phases 1-3):

| Measure | Today's prelude | Measured world S1D | Source |
|---|---|---|---|
| JVM cold boot | 350 ms | 424 ms | `BP/docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` §1 table; probe `EXP/probe/BootProbe.java` / `UserSideProbe.java` (single cold runs; raw logs not kept: **UNVERIFIED** how many runs) |
| WASM first answer (instantiate → first plan), median of 5 | 1,499.8 ms | 2,088.2 ms | `EXP/e7_wasm_first_answer.txt` |
| WASM module | 4.71 MB | 5.03 MB | experiments §2.7 |
| elements in the boot world | 641 | 929 | experiments §1 table |
| parse `prelude.pure` (WASM) | 193.0 ms | 349.6 ms | `EXP/e7_wasm_today_phases.txt`, `EXP/e7_wasm_S1D_phases.txt` |
| system metamodel class init (WASM) | 91.3 ms | 90.8 ms | same |
| `NameResolver.resolve` of the boot (WASM, timed alone) | 240.6 ms | 305.4 ms | same |
| SHA-256 of the boot source (WASM) | 12.1 ms | 19.8 ms | same |
| boot layer resolve + normalize + index (WASM) | 835.3 ms (55%) | 1,163.4 ms (56%) | same |
| first plan with all above warm / warm p50 | 70.0 / 50.8 ms | 79.5 / 64.0 ms | same |

Cautions: the "of which" rows of `wasm/startup.mjs` are timed separately and overlap the boot-layer row (do not add
them up); the "browser" numbers come from Node, not a browser (`//wasm:startup` is a `js_binary`, `wasm/BUILD.bazel:151-162`,
run with `--experimental-wasm-exnref`); `startup.mjs` prints "read 4.2 MB from disk" as a fixed label
(`wasm/startup.mjs` rows table) whatever the size; and `wasm/README.md:84-90`'s "cold (first plan) 557-771 ms" is an
older measurement by another harness. Compare only like with like.

**What to profile (both, before and after the new world):**
- **JVM.** The cold-boot measurement `BootProbe` makes: `Compiler.buildModule(Compiler.parseSources(List.of()).model())`
  timed in a fresh JVM (`EXP/probe/BootProbe.java:10-13`), five fresh JVMs, median. Then the same under Java Flight
  Recorder (`-XX:StartFlightRecording`), read with `jfr print --json --stack-depth 200 --events jdk.ExecutionSample`
  (the default 5 frames hide callers: `BP/docs/build-inventory/program/START_HERE.md` §6). Attribute the time to:
  - class initialization: `builtin/Prelude.java:55-56` (parses the whole file), `builtin/SystemMetamodel.java`
    (`ELEMENTS`, parses its source), `builtin/Pure.java` (864 `signature(...)` parses through `ElementParser`,
    `Pure.java:748-766`), `compiler/NameResolver.java`'s static universes (`PLATFORM_TYPE_FQNS` :257,
    `PLATFORM_FQNS` :274, `QUERY_SCOPE` :520), `builtin/EngineHandlers.java:47-93` (the join);
  - `Compiler.boot()` (`Compiler.java:267-292`): `SystemMetamodel.withoutSystemShadows`, `NameResolver.resolve(boot)`,
    `normalizeLayer` (`KnowledgeLayer.adoptAssociationQualifiedProperties`, `ModelBuilder.from`,
    `ModelNormalizer.normalize`, `Compiler.java:237-242`), `PureModelContext.checkLayer`; and `BootKey`
    (`Compiler.java:297-302`, a hash over both sources).
- **WASM.** `bazel run //wasm:startup`, five runs on an idle machine, median (it prints the phases above). For
  function-level detail, a V8 CPU profile of the same run (**UNVERIFIED** that TeaVM's WASM-GC output gives readable
  frames).

**"Optimize first", under the user's rules:** remove redundant work at its cause; no new cache first (the boot is
already computed once per process and keyed by content, `Compiler.java:254-302`). Questions the profile must answer:
why resolving the boot's names alone costs 240 ms in WASM; how much of the boot is Pure.java's 864 parses, which
Phase 5 removes; how much the hand-written SHA-256 costs. Then the numbers go to the user, who decides Phase 4b.
No budget exists ("tracked numbers without budgets yet", `BP/docs/build-inventory/manifest-world/WORLD_H.md` §5.7);
the old plan's gate was "boot time back at the receipts' ~1.8s" (WORLD_H §1.2).

**If Phase 4b is chosen:** W2.1 (`BP/docs/EXECUTION_PLAN_2026_09_26.md:659-664`) describes "platform declarations
generated at build time as a serialized resource, not 854 parses at class load ... a generated class would exceed the
64 KB static-initializer limit; whether the WASM planner can load the resource is checked first". Such an index is
built by our compiler from the default world and the system metamodel, so it is a build output, never a bump record
and never committed (the three-kinds rule).

### 1.4 The open items: which are settled, which remain, who owns them

**`MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` §6:**

| # | Item | Status | Owner |
|---|---|---|---|
| 1 | Browser start-up: accept +0.6 s or pre-bake the boot layer | Process settled by decision 2; the numbers and the 4b decision remain | Phase 4 (profile, optimize), then the user (4b) |
| 2 | The boot's demands: 12 harness types as a boot registration, or the plan/lineage mappings conditional on the world | Settled by decision 1: the result views move off startup and load with `core` and `core_relational`. The 10 names the system metamodel needs beyond the measured world are exactly those views' types (`WORLD/closure.json` key `S1\|boot system only`: `FunctionParametersValidationNode`, `ParameterValidationContext`, `SequenceExecutionNode`, `ColumnWithContext`, `RelationTree`, `AggregationAwareActivity`, `QueryMetadata`, `RelationalInstantiationExecutionNode`, `SQLExecutionNode`, `SQLResultColumn`). The trigger and where the views live remain open (D4-4) | Phase 4 |
| 3 | Execution on the user side (demos executed, not only type-checked) | Open; it is a Phase 4 check. The plan names no harness; candidates: `//query:verify` (`query/BUILD.bazel:143`, runs the Query app's queries in the tab, on the server and on a warehouse, `query/demo/verify.mjs:1-8`), `//studio:verify_test` (`studio/BUILD.bazel:128`), `//site:verify`, `//datacube:verify_app_test` (**UNVERIFIED** that they cover every demo query) | Phase 4 |
| 4 | Visibility semantics for manifest-loaded worlds | Open, untested | Phase 6 (it loads by manifest); decide before Phase 4 relies on Phase 6's loader |
| 5 | The corpus on its real manifest | Phase 6 | Phase 6 |
| 6 | Other engine extensions' handlers (data quality, service, JSON, external format, Elasticsearch, data space) | Settled for now: the module choice is core + relational | None in this program; a change goes through the bump's setting |
| 7 | Pure.java without signature text: no run without the catalog yet | Open. Also needs the user side, not only corpus and PCT (§3.10 U5-1) | Phase 5 homework |

**`MANIFEST_WORLD_HOMEWORK_2026_10_05.md` §6:** 1 (D9): the "2b" question is answered by the experiments; rule 8 and the
roadmap-test register belong to the corpus (Phase 6); Phase 3b decided not to type upstream's unrun tests or the
engine machinery ("Not doing", plan), and the default world strips tests by upstream's markers. 2 (default manifest): settled, a module-level choice. 3 (boot, browser): measured; Phase 4 per
decision 2. 4 (m3): settled, printed from `m3.pure` as today. 5 (visibility): open, Phase 6. 6 (stale documents):
Phase 4 (decision 3), with more texts than the plan names (§2.9).

**`UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` §4:** 1 (does our key map one-to-one onto upstream ids; a header scanner):
the ids are settled by `CatalogUpstreamDiffTest` (DIVERGENT 0); the scanner is moot (no catalog resource). 2 (catalog
resource in WASM): moot; the default world's size in WASM is a Phase 4 measure. 3 (boot cost): measured, Phase 4.
4 (parse walls; class-init order): parse walls 0 over 16,445 upstream elements of 32 modules (experiments §2.4;
`WORLD/uni_edges.tsv.walls` is empty); class-init order (`EngineHandlers` reads Pure and Prelude) to recheck in Phases 4
and 5. 5 (CORE_IMPORTS spelling, the handler table's effect on DynaFn): moot since Phases 1-2. 6 (cross-platform
determinism): today proven by `//:generated` on three CI platforms; after Phase 7 the seal test is the daily check.

**`WORLD_H.md` §6:** 1-2 (what "2b" means; its measurement): moot / done. 3 (whether respelled upstream natives satisfy
WORLD_MAP rule 7): becomes "whether upstream natives in the default world with `Unimplemented` rows satisfy rule 7";
Phase 4 (rule texts). 4 (D10 against D7(a) for the corpus build): settled in practice on 2026-10-07 (plan Phase 3b
item 2 keeps the reference lane strict and leaves the corpus on the tolerant `Compiler.buildModule`), though no text
says D10 supersedes D7(a). 5 (ChannelB): open; Phase 6's PCT review. 6-7 (duplicate counts,
world-2 and census numbers): Phase 6. 8 (non-test size): done (upstream core 0.49 MB, `EXP/closure.txt:1`). 9 (our
`SignatureMangle` equals upstream's id for every declaration): partly evidenced (the reference lane's AGREE class means
same id on both sides for 74,586 calls; DRIFT 0, `spec/src/test/java/com/legend/generators/ReferenceJoin.java:27-29,149-158`),
never checked declaration by declaration; Phase 5 homework (U5-3), and Phase 4 if it seeds by handler id. 10 (a user
element shadowing a platform element in legend-engine): open; Phase 4 (U4-9). 11: moot.

**R2 (`BP/docs/build-inventory/upstream-only/R2.md`) §4:** risk 3 (the 6 hand enums duplicated) is live and unplanned
(D4-8); risk 4 (ownership at boot changes the resolver's universe) is now Phase 3's table plus Phase 4's re-measure
(U4-1); risk 7 (#11(c)) is moot once the generator reads no committed file.

---

## 2. Phase 4: the default world from upstream

### 2.1 Goal
Users boot on a world we hand-steer today: `PreludeGenerator` decides what upstream content enters by scanning our
Java for names, by reading our claims and enums, and by our path lists and exclusions (R2 §1, items 1-11). That makes
the bump depend on our code and lets our lists miss what users need: 146 body failures in the 56 model projects are one
function, `orElse`, that upstream's own handler list has and our lists did not (experiments §1). Phase 4 generates the
default world from the pinned archives and one setting (the module choice) only. It also moves the result views off
startup, gives the legacy TDS functions rows by function id (closing PARK-11), corrects the rule texts that still say
"declarations only", and measures and improves boot speed before anyone builds a pre-built boot layer.

### 2.2 Agreed design and decisions (with sources)
- **Contents.** "upstream core whole (legend-pure `platform*` and engine `core_functions_*`, tests stripped by upstream's
  own markers), plus upstream's query surface (the handler registrations of the core compiler and the relational
  extension, and the classes those modules instantiate), closed through declarations and the bodies of runnable
  functions; the m3 metamodel printed from upstream's m3 graph, as today's generator does" (plan, Phase 4). Measured as
  455 handler functions (443 core compiler, 13 relational; one overlap) and 156 instantiated classes (`EXP/e1.txt:1,8-9`).
- **Test markers.** Test stereotypes and `::tests::` packages; `::test::` packages are upstream's test infrastructure
  and stay (plan Phase 4; experiments §1 item 1; the experiments' rule is `EXP/modworld.py:15,18`).
- **Inputs.** "the pinned archives and the module choice. No Java scan, claims, hand enums, path lists or exclusions"
  (plan Phase 4). The module choice is "the core and relational compiler modules", "kept in the bump's configuration
  beside the pins" (north star).
- **m3.** The declarations upstream has only in `m3.pure`'s instance form stay "built in", printed by today's reader
  (`MANIFEST_WORLD_HOMEWORK` §6 update, item 4; `spec/src/gen/java/com/legend/generators/PreludeGenerator.java:995-1199`
  `M3`/`M3Reader`, `:1202-1224` `m3Declarations`; it uses no core class).
- **Result views off startup** (decision 1): they load in a program whose manifest includes `core` and
  `core_relational`, never at boot; their classes stay upstream's; their three functions (`allNodes`,
  `routerExtensions`, `relationTreeAsString`, `builtin/SystemMetamodel.java:1128-1156`) are chosen by implementation-table
  rows of a new kind, "the platform's own Pure". Phase 6 lands first so nothing temporary sits between (plan §4).
- **Legacy TDS functions by id** (plan Phase 4 bullet; Phase 3 correction; `docs/PARKED_WORK_LEDGER.md` PARK-11): rows by
  function id ("the platform's desugar", a form row), recognition through the names a call resolves to, the typer's
  synthesized calls use full names, and the spelling fallbacks deleted (`builtin/TdsLegacy.java:62-70`,
  `compiler/spec/GroupByChecker.java:43-45`).
- **Rule texts** amended (decision 3): AGENTS.md and TENET_CHARTER C6.3 to match WORLD_MAP rule 2 as amended 2026-09-08.
- **Boot speed** (decision 2): §1.3.
- **Preconditions** (plan §4): Phase 3 landed (the table decides by id), Phase 3b landed (the boot layer's twins merged
  by id, F-L1, the qualified-property lookup), Phase 6 landed (the corpus loads by manifest). Phase 3b's condition:
  "Phase 4 opens by re-measuring the library bodies upstream core brings in (each types or is refused with a reason)".
- **Process** (plan header and §5; `START_HERE.md` §4): plan agreed with the user before code; the IN_FLIGHT entry on
  main first, listing every core file; an audit; one full CI run on the branch; push that commit to main. No PRs.

### 2.3 Read first (in order)
1. `BP/docs/build-inventory/program/START_HERE.md`: the rules, the exact checks, the traps.
2. The plan: §1 (north star), §2 decisions 1-3, §3 Phases 3 (status and correction), 3b, 4, 6; §4.
3. `BP/docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`: the default world as measured; §3 lists what the design needs.
4. `EXP/README.md`, then the scripts: `docstart.py` (segmenting upstream files), `modworld.py` (whole-module worlds,
   test stripping), `e1.py` (the query surface), `probe/ClosureProbe.java` and `closure.py` (the closure),
   `bootdemand.py`, `synth_prelude.py` (how a world was written, including its by-name filter), `e6_lanes.py`,
   `probe/UserSideProbe.java`, `probe/BootProbe.java`.
5. `EXP/LOOSE_ENDS.md` §2-3: the 13 names that leave and the 18 test-infrastructure names that stay.
6. `EXP/phase2b/README.md`: upstream versions at names we implement; "the faithful 117 ... does not boot without closing
   over their dependencies (Runtime, ConnectionStore): Phase 4's closure work".
7. `EXP/phase3b-census/README.md`: which library bodies fail to type and why.
8. `BR/docs/PARKED_WORK_LEDGER.md` PARK-11 and PARK-12.
9. `BP/docs/build-inventory/upstream-only/R2.md` §1-2 and `BP/docs/build-inventory/generators/G1_dossiers.md`
   "//spec:gen_prelude": everything today's generator reads.
10. `BP/docs/build-inventory/manifest-world/WORLD_H.md` §2 (the rules), §3.1-3.3 (today's loading), §4 (how upstream
    loads), §5.6-5.7 (boot and WASM numbers).
11. Code (§2.4), and the rule texts to amend: `BR/AGENTS.md:37-61`, `BR/docs/TENET_CHARTER.md:196-199`,
    `BR/docs/WORLD_MAP.md:56-63,115-131,174-196`.
12. Upstream: `ENGINE/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph/handlers/Handlers.java`
    (3,565 lines; e.g. `register(...restrict...)` at :2363), `.../toPureGraph/CoreCompilerExtension.java`,
    `ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph/RelationalCompilerExtension.java`,
    `.../toPureGraph/CompileContext.java:476-570` (how a user call resolves), `ENGINE/.../legend-engine-pure-code-compiled-core/src/main/resources/core/pure/tds/tds.pure`.

### 2.4 The code today (entry points)

| Where | What it does now |
|---|---|
| `//spec:gen_prelude` (`spec/BUILD.bazel:466-489`) | java_run of `com.legend.generators.PreludeGenerator`; srcs: both upstream trees, `:gen_natives`' Pure.java, `//core:main_java` (core's Java as text); deps `:generators`, which links `//core` (`spec/BUILD.bazel:55-71`) |
| `spec/src/gen/java/com/legend/generators/PreludeGenerator.java` (1,716 lines) | reads our inputs: `EXCLUDED_PACKAGE_PREFIXES` :100, `ENGINE_LIBRARY_FUNCTIONS` :122 (`removeAll`), `ENGINE_SPEC_ROOTS` :140, `CORPUS_ROOT` :150, `handDeclaredFqns` :1601, the Java demand scan (`javaDemand` :244-359), `Claims.claimedBareNames()` :430 and :542, `engineNatives` :1391; imports core's `Compiler`, `Lexer`, `ElementParser`, `NameResolver`, `CoreImports` (:6-22) |
| `spec/src/gen/java/com/legend/generators/UpstreamFiles.java` | hand path lists `PLATFORM_ROOTS`, `STDLIB_ENGINE_ROOTS`, `LIBRARY_FILES`, `SHAPE_FILES` (also read by tests: `CatalogUpstreamDiffTest`, `SpecBodyCensusTest`) |
| `core/BUILD.bazel:711-718` `_GENERATED`; `:803-808` `//core:update_generated` | `prelude.pure` is written by `//spec:gen_prelude`, diff-tested in `//:generated` |
| `core/src/main/resources/com/legend/builtin/prelude.pure` | 7,018 lines, 300,247 bytes; footer receipts from :6584 (485 classes, 23 enums, 153 functions; 66 respelled upstream natives; 245 "platform-owned names"; 21 system-owned; T4 receipts) |
| `builtin/Prelude.java:39-56` | reads the resource once and parses it at class load; `CLASS_FQNS`, `FUNCTION_FQNS`, `FUNCTION_IDS` (:63-89) |
| `Compiler.java:245-302` | `boot()`: system metamodel + prelude, resolved, normalized and checked once per process, cached by the hash of both sources; `withoutPreludeShadows` (:327-350) drops a program's copies of prelude classes and enums by name and functions by id; `normalizeWithSystem` (:360-373) |
| `builtin/SystemMetamodel.java` (1,587 lines) | one Pure source (`SOURCE` :537) parsed at class load; the result views are in it: plan node kinds :143, activity kinds :194, plan and lineage mapping sections :237-450, the three functions :1128-1156; `shadows` by function id :1477-1499; `withoutSystemShadows` :1565 |
| `builtin/TdsLegacy.java` | 18 members; `matches` (:62-70) falls back to the spelling when the resolver found nothing (the PARK-11 anchor) |
| `compiler/spec/TdsDesugars.java` | the desugars: `RENAME_COLUMN` :211, `COLUMN_VALUES` :290, `WINDOW` :452 and :465, `COLUMN_BY_NAME` :606 (a two-argument call, `$tds.columnByName('x')`, read as a column name) |
| `compiler/spec/GroupByChecker.java:43-45` | `isAgg` = `TdsLegacy.AGG.matches` (`meta::pure::tds::agg` only) |
| `platform/ImplementationTable.java`, `Implementation.java`, `Registrations.java`, `CoreFn.java` | the table: rows by id; forms, walls and subsumed keyed by full name and applied to every declaration there (`ImplementationTable.java:118-159`); `Form` holds a `CoreFn` (`Implementation.java:23`); no "platform's own Pure" kind |
| `builtin/EngineHandlers.java:47-93` | joins `engine-handlers.tsv` (upstream's names and ids) with the platform's declarations (`Pure.all()` and `Prelude.elements()`) by id at class init; `UNDECLARED` = ids nobody declares (ratchet `engine.handlers.undeclared 142`, `spec/src/test/resources/com/legend/generators/ratchets.tsv`) |
| `builtin/Pure.java:234-249`, `:338-356` | 12 primitive classes and 6 enums declared by hand text; all 6 enums are upstream's, 5 in upstream core (`WORLD/decl_repo.json`: `relation::SortType` platform, `date::StrictDateFormat` and `DateTimeFormat` core_functions_standard, `hash::HashType` core_functions_unclassified, `relational::metamodel::SortDirection` and `join::JoinType` platform_store_relational) |
| `wasm/src/main/java/planner/PreludeResources.java:22`, `sdlc-server/src/main/java/com/legend/sdlc/page/PageResources.java:15` | the two TeaVM builds embed `prelude.pure` and `engine-handlers.tsv` (a WASM module has no class path) |
| `.gitattributes` | `prelude.pure -text`: kept byte for byte (it carries CR copied from upstream) |
| Tests that move | `spec/.../PreludeGeneratorTest.java`, `SpecBodyCensusTest.java` (hand roots), `ManifestWorldCensusTest.java`, `core/src/test/java/com/legend/ParkedWorkLedgerTest.java` (PARK-11 anchor), `core/src/test/.../builtin/EngineHandlersTest.java`, the spec ratchets (`engine.handlers.undeclared`, `implementation.*`) |
| The harness | `EXP/` scripts (above); `bazel run //wasm:startup` |

### 2.5 Steps

**Step 0. Baseline (before any change).** On the current main with Phases 3, 3b and 6 in:
- the six corpus passes, outputs copied aside (`START_HERE.md` §5, "The corpus passes, done right");
- PCT, all suites; the reference lane report; `//gates:local`;
- the user side: `EXP/probe/UserSideProbe.java` over the 56 projects and 3 demos (the module list is
  `WORLD/user_modules.txt`: `projects/*`, `datacube/demo`, `query/demo`, `studio/demo`);
- boot: five cold JVM runs (`BootProbe`), five `bazel run //wasm:startup` runs; the JFR profile (§1.3).

**Step 1. Homework the plan needs (each produces a number or a list for the user; §2.10).**
U4-1 the real world measured; U4-2 the library bodies census; U4-3 the unrowed versions inside the world; U4-4 the
handler ids against our ids; U4-5 legend-engine's own answer for the three TDS functions; U4-6 the upstream names our
Java quotes that the world lacks; U4-7 the 8 unmatched handler strings.

**Step 2. Agree the plan** with the user, deciding §2.8's open decisions. Then the IN_FLIGHT entry on main, listing
every core file (standing authorization).

**Step 3. The generator.** Replace `PreludeGenerator.java` and `//spec:gen_prelude` with a generator whose inputs are
the archives and the module choice (D4-5 decides how it reads them; D7-2 where the setting lives). It keeps today's
m3 reader. Its output is the default world (D4-10: the file name).
- Files: a new program in `spec/src/gen/java/com/legend/generators/` (delete `PreludeGenerator.java`); `spec/BUILD.bazel`
  (the target, its srcs: upstream only; its library: one that does not link `//core`, as Phases 1-2 did for
  `dynafn_generator`, `engine_handlers_generator`, `imports_generator`, `spec/BUILD.bazel:73-87`, if D4-5 allows);
  `core/BUILD.bazel:711-718`; `UpstreamFiles.java` loses its generator role (tests may still read it).
- Verify: two runs give the same bytes; the output parses with our parser and boots; the element count, size and a
  diff against today's prelude by category (added: upstream core functions today's generator dropped by name and the
  query surface; removed: the 13 names from the old "all upstream natives in the read roots" rule, `EXP/LOOSE_ENDS.md` §2;
  shapes only our Java pulled in).

**Step 4. The world at boot.**
- The 6 hand enums leave `builtin/Pure.java:338-356` (upstream core now declares them; R2 §4 risk 3); every Java use of
  `Pure.SORT_TYPE`, `STRICT_DATE_FORMAT`, `DATE_TIME_FORMAT`, `HASH_TYPE`, `SORT_DIRECTION`, `JOIN_TYPE` reads the world's
  declaration instead (D4-8). The 12 primitives: D4-9.
- If the file is renamed: `builtin/Prelude.java:42`, `wasm/.../PreludeResources.java:22`,
  `sdlc-server/.../PageResources.java:15`, `.gitattributes` (keep `-text`), `wasm/startup.mjs` labels, the tests that name
  it (`core/src/test/java/com/legend/builtin/NativeFunctionTest.java`, `compiler/element/PureModelContextTest.java`,
  `spec/.../ClaimRegistryTest.java`, `NativeSignatureGeneratorTest.java`, `pct/.../extension/ModelPacker.java:219-221`).
- Verify: the boot succeeds (`BootProbe`), `bazel test //core:core_tests //core:guardrails //core:census`.

**Step 5. The result views move off startup** (decision 1; D4-4 for the mechanism).
- Files: `builtin/SystemMetamodel.java` (split the core views from the result views); the loader that adds them for a
  program whose manifest includes `core` and `core_relational` (Phase 6's corpus loader; PCT if it needs them); the
  implementation-table row kind "the platform's own Pure" if Phase 4 owns it (`platform/Implementation.java`,
  `ImplementationTable.java`, `Registrations.java`).
- Verify: the default world boots with none of the 10 result-view names (`WORLD/closure.json` `S1|boot system only`);
  the six corpus passes identical.

**Step 6. Rows the world now needs.**
- The upstream versions at names we implement that the world now carries and that have no row (at most 109, the
  ratchet `implementation.unrowed`): each gets a row ("runs as built-in X") or stays refused (`NO_ROW`) on purpose,
  recorded (D4-12). Today's world never showed them: the generator dropped them by name.
- The legacy TDS functions: rows by id for every declaration the world has (D4-2, D4-3), `columnByName` through its
  class member (D4-2); delete the spelling fallback (`TdsLegacy.java:67-69`) and `isAgg`'s name test; the typer builds
  full names; delete PARK-11 and its anchor (`ParkedWorkLedgerTest`).
- Verify: `bazel test //core:guardrails` (ledger anchors, identity counts going down with dated notes), the six corpus
  passes, PCT.

**Step 7. The rule texts** (decision 3). Amend `AGENTS.md:37-51` (the reference-checkout tenet: "spec by
VERIFICATION, never by LOADING", "platform-owned so parsed twins suppress", and the deleted guard
`Runner.registerLibrarySource`, today `MinimalCorpus.refusePlatformNamespace`,
`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:783`) and `AGENTS.md:53-61` ("no other Pure bodies"),
`docs/TENET_CHARTER.md:196-199` (C6.3 "declarations only"), and also `docs/WORLD_MAP.md:56-63` (§3 "declarations only
... no bodies"), `:115` (rule 1 "Never loaded"), `:117-131` (rule 2's first text and its 2026-09-04 amendment, which
describes generation "by demand" from "the platform's own Java names"). AGENTS.md is shared: say so in the commit.

**Step 8. Boot** (§1.3): profile the new world, fix causes, record the numbers; the user decides Phase 4b.

**Step 9. Checks (§2.6), audit, land.**

### 2.6 Checks and acceptance
- The plan's bar: "projects' body walls 146 to 0 (`orElse`), corpus and PCT identical, the demos' queries executed, not
  only type-checked; boot times recorded; the 18 legacy TDS functions declared by the default world with rows by id and
  no spelling test left (PARK-11's anchor gone, its row deleted)." The last clause cannot hold as written (D4-2).
- Commands (`START_HERE.md` §5): `bazel test --lockfile_mode=error //gates:local`; `bazel test //core:core_tests
  //core:guardrails //core:census //spec:spec_tests //:generated`; the six passes `bazel build //spec:judge_host_duckdb
  //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse
  //spec:judge_database_warehouse`, every result file identical to Step 0's copies; `bazel test //pct:pct_duckdb
  //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b` identical; the reference lane (`bazel build
  //spec:reference_lane_report`, diff against `spec/src/test/resources/reference-lane/core_relational.txt`; AGREE not
  down; moved lines explained); `UserSideProbe`: body walls 0, build walls 0 after F-L1; the demos executed (§1.4 item 3);
  `bazel test //projects:tests`.
- Expected moves, each re-blessed with its reason: `engine.handlers.undeclared` falls toward 0 (U4-4);
  `implementation.kinds.*` and `implementation.unrowed` move; `CatalogUpstreamDiffTest`'s counts may move.
- Watch: `EngineHandlers.fqnsOf` gains names once the default world declares every handler function
  (`EngineHandlers.java:47-56` joins against `Prelude.elements()`), so bare-name resolution for engine input
  (`compiler/BareNames.java`) and DynaFn's PURE lists (`DynaFnDecisions.java`, `Declarations`) can change;
  `spec/.../DynaFnRegistryTest.java` and the corpus passes show it.
- Boot: JVM and WASM, before and after, five runs each, in the GATES entry.

### 2.7 Pitfalls already hit
- Segmenting upstream files: a doc string belongs to the element below it; keywords at a line start inside a doc string
  or block comment are prose; names come after every `<<stereotype>>` and `{tagged value}` (`EXP/docstart.py:1-5`;
  plan §5).
- `::test::` (singular) packages are upstream's test infrastructure that ordinary code references; only `::tests::` and
  test stereotypes mark tests (plan §5). `TestedByResult` must stay because `Extension` names it (`EXP/LOOSE_ENDS.md` §3).
- Without an ownership rule the boot failed (`_classMappingByClass` defined twice), `^Class(...)` bound to upstream's
  `new` (901 user walls) and legacy TDS `project`/`groupBy` with `agg`/`col` broke 107 corpus tests (experiments §3.1).
  Phase 3's table now decides; Step 1's measurement proves it holds without any name filter.
- Declarations-only closure made 17 DuckDB and 9 H2 tests newly fail: bodies are needed (`removeAll`,
  `joinWithOptionalColumns`, lineage `PropertyPathNode`) (experiments §3.3).
- Taking the 15 user-facing engine files whole added 928 names, 1.07 MB (experiments §2.4): rejected.
- Phase 2b: one upstream version, `collection::get(T[*], String)`, changed results through overload ranking until Phase 3
  ranked as legend-pure does; the 117 extra versions in the query surface's files "do not boot without closing over
  their dependencies (Runtime, ConnectionStore)" (`EXP/phase2b/README.md`).
- The first corpus-closure attempt failed: stripped test files left 12 missing test types; the same element in the base
  and the test module was dropped from the test's graph (experiments §4). The loading rule is "each element once".
- Hand-run corpus commands after another Bazel command: rebuild the corpus targets first, or "legend-engine checkout not
  present" (plan §5). `bazel info` blocks while another build runs (`START_HERE.md` §6).
- In zsh, `echo ====` fails and an unquoted variable holding several paths is one word (plan §5).
- The JVM boot numbers on record are single cold runs (§1.3); the WASM harness labels its module "4.2 MB".

### 2.8 Open decisions for the user

**D4-1. Which bodies the closure follows.** The plan says "the bodies of runnable functions" and "No Java scan, claims,
hand enums, path lists". The measured version knew which functions are "runnable" (not lowered, not a form, not walled)
by reading `Pure.java`, `CoreFn.java` and `WalledBodies.java` (`EXP/closure.py:61-77`): our code.
- (a) Follow every body. Upstream only. At the measured size: +563 names, +402 KB on top of upstream core, against
  +483 names, +328 KB for runnable bodies (`EXP/closure.txt:11,52`). Never run through the corpus, PCT or user side.
- (b) Runnable bodies only, which needs the implementation table as a generator input: contradicts "never a generator
  reading our code".
- (c) Declarations only: breaks 26 corpus tests (experiments §3.3).
- Recommendation: (a), measured with the full harness before it is adopted (U4-1).

**D4-2. The legacy TDS functions the closure does not bring.** Measured for this brief (§5.1):
- `meta::pure::tds::columnValues` (2 versions, `tds.pure:516-527`), `renameColumn` (`:530-533`), `window` (`:748`) are
  in the engine's `core` module, are not handler registrations (`engine-handlers.tsv` has none of them; none of the 12
  handler files the experiment listed, `WORLD/e1_handler_files.txt`, names them: `Handlers.java`, the core, relational,
  service, data-quality, JSON, external-format, Elasticsearch and data-space extensions),
  and no non-test upstream element refers to them (only upstream tests call them, e.g.
  `ENGINE/.../core_relational/relational/tds/tests/testTDSJoin.pure:709`). No closure mode brings them
  (`WORLD/closure.json`).
- `columnByName` is not a function upstream: it is a qualified property, `TabularDataSet.columnByName(s:String[1])`
  (`tds.pure:21`); `TabularDataSet` is in the closure, so it comes with the class. legend-pure's tree never mentions it.
- legend-engine resolves a user query's calls only through registered handlers and the user's own functions
  (`CompileContext.java:544-570`: an unregistered name returns `null`, "error reporting will happen later"; user functions
  are registered at `FunctionCompilerExtension.java:97`). So in legend-engine a user cannot call these three either
  (code reading; U4-5 checks it with a run).
- Options: (a) leave the three out of the default world (faithful to legend-engine; the corpus gets them from its own
  manifest files after Phase 6; no project, demo or app source calls them (grep of `projects/`, `*/demo/`: 0); the one
  use outside the corpus, the core unit test `core/src/test/java/com/legend/resolver/ResolveNavigationTest.java:473`
  (`col(window(...))`), changes); (b) a wider rule that brings them (taking their file whole is a rule of ours, and
  whole files were rejected in general); (c) keep the spelling rule (PARK-11 stays open).
- Recommendation: (a), once U4-5 confirms legend-engine's behaviour. Either way the plan's Phase 4 bullet and PARK-11's
  "Why it waits" must be corrected (§2.9).

**D4-3. How a TDS row is written.** A `Form` row holds a `CoreFn` (`Implementation.java:23`); `TdsLegacy` is not a
`CoreFn`. Options: the TDS members become `CoreFn` forms owning their full names, or `Form` accepts either kind. Both
reuse the existing row kind, as the plan asks ("a form row"). `columnByName` is a class member, not a function id:
the table's existing member mechanism (`Registrations.members`, `ImplementationTable.java:102-116`, today for families)
or the lifted-property id (`SynthHat.PROP`).

**D4-4. The result views: trigger, home, and the new row kind.**
- Trigger: decision 1 says "load in a program whose manifest includes those two modules"; the north star says "join a
  program whose world has their classes". Options: (a) the program loader (Phase 6's corpus loader) adds the views when
  its manifest closure contains `core` and `core_relational`; (b) the views become a code repository of ours with its
  own manifest depending on `core` and `core_relational` (the pattern exists:
  `pct/src/main/resources/core_legend_lite_pct.definition.json`). Both use manifests, the existing mechanism.
- Home: today in `core` (`SystemMetamodel.java`), whose Java (`PlanRows`, `LineageRows`) produces the rows.
- The row kind "the platform's own Pure": decision 1 says Phase 3 replaces the name rule; Phase 3 built
  `SystemMetamodel.shadows` by function id (`SystemMetamodel.java:1477-1499`) and no new row kind (`Implementation.java`
  still has 5). PARK-12 closes in Phase 3b item 1. Which phase adds the row kind is unassigned; Phase 4 needs it when the
  three functions move.

**D4-5. Which parser and resolver the generator uses.** The closure needs names resolved.
- (a) Ours (`Compiler.parseSources`, `NameResolver`), as `PreludeGenerator` and `EXP/probe/ClosureProbe.java` do. Then
  the default world can change with our parser, not only with upstream, and the seal (Phase 7) cannot notice: the record
  is "upstream plus our parser" (GENERATORS.md §2 / BUILD_REBUILD_DESIGN §4.2 group B).
- (b) Upstream's own compiler from the pinned jars, as `tools/reference/RefResolutions.java` does
  (`PureRuntime.loadAndCompileCore` then `loadAndCompileSystem`): fully upstream; heavy ("a few minutes and ~10 GB",
  `tools/reference/README.md`).
- (c) A text scanner for element spans (`EXP/docstart.py`'s rules) plus reference extraction: a small resolver of our
  own; fragile.
- No recommendation from the evidence; the same question was open for `gen_natives` (BUILD_REBUILD_DESIGN §6 D3). If
  (a), the daily check must still cover the default world (a diff test kept, or the seal's premise restated).

**D4-6. Seeding by full name or by exact id.** The experiment mapped each handler id to a full name and took every
version at it (`EXP/e1.py:18-24`). Seeding by id is narrower. Only the first was measured. 8 handler strings matched no
function (`EXP/e1.txt:1`; which ones: UNVERIFIED).

**D4-7. Two names lists change: record them.** The 13 names carried by the old rule "the prelude carries all upstream
natives in the read roots" (user-approved, `CLAIM_REGISTRY_DESIGN_2026_09_10.md:227-228` per `EXP/LOOSE_ENDS.md` §2) leave:
a call becomes "unknown function", which that rule rejected. The 18 surveyor and PCT helpers in `meta::pure::test::`
stay (upstream core whole), against `LOOSE_ENDS.md` §3's "The 18: no".

**D4-8. Pure.java's 6 hand enums** are upstream's (§2.4 row). Upstream core whole brings them; keeping both declares
each twice. Remove them from Pure.java in Phase 4 (R2 §4 risk 3: "delete them from Pure.java and run //gates:local").

**D4-9. The 12 primitives** (`Pure.java:234-249`). `m3.pure` declares them as `^PrimitiveType` instances (e.g.
`m3.pure:1492` `String`, `:1503` `Boolean`), which today's reader does not print (it prints `Class` and `Enumeration`,
`PreludeGenerator.java:1205-1208`). Keep them built in (the m3 bootstrap, as upstream bootstraps M3 in Java), or teach
the reader to print them.

**D4-10. The file's name:** keep `prelude.pure` (no churn) or rename (§2.5 Step 4 lists the readers).

**D4-11. Boot budget and Phase 4b** (§1.3).

**D4-12. The upstream versions without a row** (`implementation.unrowed 109`, `ratchets.tsv`). The plan's Phase 3 says
"the default world (Phase 4, before which the rest of the 184 rows come)", and the ratchet's own comment says "each gets
a row (a membership line) before the default world takes upstream core whole (build rebuild Phase 4)"
(`spec/src/test/java/com/legend/generators/ImplementationTableTest.java:143-149`); no phase's step list schedules the
work. Options: rows before Phase 4 lands for every unrowed version inside the world (as membership lines, which Phase 5
then converts), or a recorded refusal per version. Today's user world never met them: the prelude generator dropped
upstream versions at claimed names.

**D4-13. Who fixes F-L1** ("a view inside a Schema is lifted twice"): both Phase 3b item 1 and Phase 6 claim it (plan).
Phase 4's "build walls 4 to 0" depends on it.

### 2.9 Stale or contradictory statements (Phase 4)

| Location | Says | Correct statement |
|---|---|---|
| `BR/docs/PARKED_WORK_LEDGER.md` PARK-11, "Why it waits for Phase 4" | "14 are upstream handler registrations; the other 4 come through its closure" | 3 (`columnValues`, `renameColumn`, `window`) are referenced only by upstream tests and no closure brings them; `columnByName` is a qualified property of `TabularDataSet` (§5.1) |
| Plan, Phase 4, legacy TDS bullet | "`meta::pure::tds::` ... columnByName"; "the closure must bring them (check it, or say how they come)" | checked: it does not (D4-2); `columnByName` is not a `meta::pure::tds::` function |
| `BR/core/src/main/java/com/legend/builtin/TdsLegacy.java:15-18` | "every member names a function upstream declares ... (col, func, window, columnByName)" | `columnByName` is a qualified property, not a function |
| Plan Phase 4 ("bodies of runnable functions" with "Its inputs: the pinned archives and the module choice"); experiments §1 ("Our only input is a module-level choice ... no Java scan") | an upstream-only closure | the measured closure read Pure.java, `CoreFn.java`, `WalledBodies.java` (`EXP/closure.py:61-77`) (D4-1) |
| Experiments §1 table and §2 | the default world measured: 929 elements, 424 ms, 2,088 ms, corpus and PCT identical | every measured world removed upstream functions by name (`EXP/synth_prelude.py:53-67`: claims, `CoreFn` names and bare form names, today's footer lists, `e6_owned_final.txt`); Phase 3 deleted that rule; the real Phase 4 world is unmeasured (U4-1) |
| Experiments §1 item 4 | "Built in: ... and lite's stub" | `GrammarInfoStub` is upstream's, from `m3.pure`; `synth_prelude.py:52` copies it from today's prelude |
| Experiments §1 table header | "Browser first answer" | measured in Node (`//wasm:startup`, a `js_binary`) |
| Experiments §3.1 | "the legacy TDS functions `TdsLegacy` implements in Java by name (17)"; plan Phase 3 item 3 "`TdsLegacy`'s 17" | 18 members (`TdsLegacy.java:24-44`) |
| `EXP/LOOSE_ENDS.md` §3 | "The 18: no" (they do not belong in a default world) | the agreed default world keeps upstream core whole, `::test::` included (D4-7) |
| `EXP/LOOSE_ENDS.md` §2 against experiments §2.3 | "dropping them is a decision, not a cleanup" / "they leave the default world by themselves" | the old rule's retirement needs the user's explicit record (D4-7) |
| `BP/docs/GENERATORS.md` §2 row 8 | the prelude "shrinks to 'what Java needs at boot'" | the default world grows (upstream core whole plus the query surface); the same "shrinks" in `COMPILER_DESIGN_2026_09_25.md` §3.1 and `UPSTREAM_BOUNDARY_PROGRAM.md:276` is superseded |
| `BP/docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` §2 `gen_prelude` row; R2 §2 "Concrete design" | a boot filter `withoutPlatformOwned`; a hand seed list `platform-shapes.tsv` for classes our Java names | superseded: the implementation table by id (Phase 3), the result views off startup (decision 1), no hand seed list |
| Plan decision 1 | "today a name-based rule does this, and Phase 3 replaces it" | Phase 3 built id-based shadowing, not the row kind (D4-4) |
| Plan Phase 3, "Not here" | "the default world (Phase 4, before which the rest of the 184 rows come)" | 109 remain (`ratchets.tsv`); no phase's steps include them, though the ratchet's comment puts them before Phase 4 (D4-12) |
| `BR/AGENTS.md:37-61`, `BR/docs/TENET_CHARTER.md:196-199`, `BR/docs/WORLD_MAP.md:56-63,115,117-131` | declarations only; never loaded; the guard `Runner.registerLibrarySource` | contradict WORLD_MAP rule 2 as amended (`WORLD_MAP.md:189-196`) and Phase 4; the guard is `MinimalCorpus.refusePlatformNamespace`. Decision 3 names only AGENTS.md and C6.3 |
| `BR/core/src/main/resources/com/legend/builtin/prelude.pure:4` | "regenerate: bazel run //:update_generated" | after Phase 7 only the bump regenerates it |
| `BR/wasm/startup.mjs` rows table | "read 4.2 MB from disk" | the module is 4.71-5.03 MB (experiments §2.7) |

### 2.10 Unknowns that need homework before coding
- **U4-1. The real world, measured.** Write it as Phase 4 would (no name filter; D4-1's choice of bodies; every version
  at a seeded name), swap it in with the harness (`EXP/README.md`: first on the class path, or
  `-Xbootclasspath/a` through `--test_env=JAVA_TOOL_OPTIONS` for Bazel tests), then run: the boot (does it boot at all;
  JVM and WASM times), the user side, the six passes (`EXP/e6_lanes.py`), PCT. Record the element count and size.
- **U4-2. The library bodies census** (plan Phase 3b condition): type every body of the generated world once
  (`Compiler.compileAllBodies` skips boot bodies, `Compiler.java:719-736`; `SpecBodyCensusTest` types the hand roots,
  so point a census at the world); each body types or is refused with a reason. Prior numbers: W0 200 failures (199
  tests, 1 library), W1 232 (227 tests, 5 library) (`MANIFEST_WORLD_HOMEWORK` §3).
- **U4-3. The unrowed versions inside the world:** `ImplementationTableTest.theVersionsWithoutARowOnlyShrink` writes the
  109 to `unrowed-versions.txt` in its test outputs (`ImplementationTableTest.java:151-161`); keep those whose full name
  the generated world declares.
- **U4-4. Handler ids against our ids:** after the swap, `EngineHandlers.undeclaredIds()` should list only ids the world
  lacks; any id whose function the world has is a mangle mismatch (WORLD_H §6 item 9).
- **U4-5. legend-engine's answer:** compile a one-line model calling `->renameColumn('a','b')`, `->columnValues('a')` and
  `col(window(...), 'x')` against 4.145.0 (the reference lane's jars, `tools/reference/BUILD.bazel`), expecting
  "Can't find a match" for each.
- **U4-6. Names our Java quotes that the world lacks:** 74 at name level against the measured world (§5.3), mostly
  corpus helpers and result-view types; a few are not (`date::ISO8601DateFormat` and `SimpleDateTimeFormat` in
  `lowering/Render.java`, `collection::sortByReversed` in `CoreFn.java`, `ClassSorts.java` and `OrderView.java`,
  `executionPlan::features::Feature` in `platform/Feature.java`, `relational::validation::validate` in `CoreFn.java`).
  Review each: a path only programs with the corpus's manifest reach, or a user path that needs the declaration.
- **U4-7. The 8 unmatched handler strings** (`EXP/e1.txt:1`): print them (`EXP/e1.py:31` keeps them in `unmatched`).
- **U4-8. Visibility** (experiments §6 item 4): Phase 6's.
- **U4-9. A user element with a default-world element's name:** legend-engine's behaviour (WORLD_H §6 item 10) against
  ours (`withoutPreludeShadows` drops the user's copy silently, `Compiler.java:319-350`).

---

## 3. Phase 5: Pure.java as rows keyed by function id

### 3.1 Goal
Pure.java holds 821 copies of upstream signature text (generated by `gen_natives` from our membership list, our parser
and the committed Pure.java itself; R1 §0) plus 488 overload groups computed from them. That is a second declaration of
each function beside the world's, kept in sync by a generator that reads our code. Phase 5 keeps only our decision: for
each upstream function we implement, its function id and how we run it. The declaration is the world's. The
membership list, the claims ledger, the generators and the second core compile they need all go, and a test proves
every row still names a real upstream function.

### 3.2 Agreed design and decisions (with sources)
- "No signature text: every row names an upstream function id and its implementation. Declarations come from the world
  (the default world for users, the program's own files for the corpus and PCT)." (plan Phase 5)
- Retires: "the membership list and its draft, `native-claims.tsv` with `core_next` and `gen_claims` (D2), the natives
  generator" (plan Phase 5; decision 5).
- "A test checks every row against the pinned archive: a row matching nothing fails and lists that name's real ids."
- "Check: the experiment harness identical."
- The ids line up: of 837 overloads then, 794 matched an upstream id exactly, 0 diverged, 43 are `meta::legend::lite`
  (experiments §2.2; `CatalogUpstreamDiffTest.java:184-201`).
- Never a signature we typed ourselves (north star); legend-lite's own `meta::legend::lite` declarations are "Ours".
- Process as Phase 4. The plan still says "Several PRs"; there are no PRs (plan header). It can land in a few steps.

### 3.3 Read first (in order)
1. The plan: north star ("Never"), decision 5, Phase 5; this brief's Phase 4.
2. Experiments §2.2, §5, §6 item 7.
3. `BP/docs/build-inventory/upstream-only/R1.md` §0 (facts still true: the text is rendered, not verbatim; 524 of the
   then 794 rows are upstream bodied functions we implement in Java) and §2 risks 1-2 (re-keying; ids drop or keep the
   return type). Its §1 design is superseded.
4. `G1_dossiers.md` part 0.5 (every reader of `native-claims.tsv`; what retiring it loses) and the dossiers of
   `gen_natives`, `gen_claims`, `native_declarations`, `native_membership_draft`.
5. Code: `builtin/Pure.java`, `native-membership.tsv`, `platform/ImplementationTable.java`, `Registrations.java`,
   `lowering/PlatformRegistrations.java`, `lowering/RegistryKeys.java`, `builtin/NativeFn.java`, `builtin/EngineHandlers.java`,
   `builtin/DynaFnDecisions.java`, `compiler/NameResolver.java`, `compiler/BareNames.java`, `compiler/ResolvedNames.java`,
   `compiler/element/FunctionCompiler.java`, `compiler/element/PureModelContext.java`.
6. Tests: `CatalogUpstreamDiffTest.java`, `ImplementationTableTest.java`, `NativeSignatureGeneratorTest.java`,
   `spec/src/test/java/com/legend/claims/ClaimRegistryTest.java`, `core/src/test/java/com/legend/builtin/NativeCatalogGovernanceTest.java`.

### 3.4 The code today (entry points)

| Where | What it does now |
|---|---|
| `builtin/Pure.java` (2,813 lines) | 864 `= signature("...")` sites, 43 of them `meta::legend::lite` (grep); each parsed at class load (`signature`, :748-766); 12 `nativeClass` (:234-249), 6 `nativeEnum` (:338-356); 488 `AT_*` groups, `FunctionId.ofAll(<constants>)`, at :2318-2806; `all()` :585; `nativeFunctionsAt` :724; `Lite` :381; `LITE_SURFACE` :559; `walledNativeFqns` :1519. Header (:13-19) still says "HAND-CURATED ... add the verbatim signature" |
| Uses of Pure.java | 286 distinct `Pure.AT_*` groups referenced 482 times in 32 main files; 271 distinct signature constants referenced in 20 main files; `Pure.all()` in 5 main files (`PlatformRegistrations`, `EngineHandlers`, `PureModelContext`, `NameResolver`, `probe/Shadow`); `nativeFunctionsAt(` in 7 (`Pure`, `BareNames`, `GroupBySynthesis`, `ResolvedNames`, `FunctionCompiler`, `StatementInline`, `ObjectReferenceArms`) (grep over `core/src/main/java`) |
| `lowering/PlatformRegistrations.java:45-95` | assembles `Registrations`: the catalog (`Pure.all()`), lowering keys by id (`RegistryKeys`), families (each `NativeFn` member's Pure.java overloads), forms (`CoreFn.ownedFqns`, by name), walls, subsumed, class members; `catalogTable()` (a table over the catalog alone, "for a lowering with no model behind it") |
| `platform/ImplementationTable.java:161-195` | the `NO_ROW` refusal uses the catalog's full names: a bodied declaration at a name the catalog declares, with no row, is refused |
| `builtin/EngineHandlers.java:47-56` | the platform's declarations for the handler join are `Pure.all()` plus `Prelude.elements()` |
| `builtin/DynaFnDecisions.java:239-242`; `compiler/spec/Typer.java:233-238` | `sqlNull`/`sqlTrue`/`sqlFalse` resolve to `Pure.SQL_*`'s names; a bare `TDSNull` is typed as a synthesized `sqlNull()` call, in every world |
| `compiler/NameResolver.java:257,274,286` | the platform's type and function universes read `Pure.all*()` |
| `//spec:gen_natives` (`spec/BUILD.bazel:383-405`) | rewrites Pure.java's signature text and `AT_*` block from upstream, the membership list and the committed Pure.java, on committed core |
| `//spec:native_declarations` (`:431-450`, manual), `//spec:native_membership_draft` (`:452-464`, manual), `//core:draft_native_membership` (`core/BUILD.bazel:854-859`, manual) | a re-keying aid and a draft writer over the membership list |
| `//spec:claims` (`spec/BUILD.bazel:92-104`), `//spec:claims_generator_lib` (`:490-505`), `//spec:gen_claims` (`:507-530`), `//core:core_next` (`core/BUILD.bazel:866-891`), `//core:core_next_prelude` (`:893-897`) | the claims ledger, built on a second compile of all of core's main sources |
| `core/BUILD.bazel:711-718` | `Pure.java` and `native-claims.tsv` are generated files (diff tests in `//:generated`); `builtin` excludes `native-claims.tsv` from its resources (`:88-95`) |
| Tests | `NativeSignatureGeneratorTest` (membership against the constants); `ClaimRegistryTest` (`UNCLAIMED_MAX = 0`; its `RESOURCE` is never read, G1 0.5); `CatalogUpstreamDiffTest`; `ImplementationTableTest.build()` (the catalog, the whole standard library and every upstream version at a registered name); `NativeCatalogGovernanceTest` (the Lite catalog's rules) |

### 3.5 Steps
0. **Preconditions:** Phase 4 landed (the default world declares the query surface). Baseline as Phase 4 Step 0.
1. **Homework** (§3.10): run without the catalog (U5-1); list every row whose id the default world lacks, at id level
   (U5-2); the mangle check (U5-3); every call the platform builds by name (U5-4).
2. **Agree the design** (§3.8) and the IN_FLIGHT entry (every core file; the registries touched are many).
3. **Convert, in steps that each keep every lane identical:**
   - a. The row form (D5-1) and the overload groups (D5-2): every registry that holds a `NativeFunctionDefinition`
     (the `NativeFn` families, lowering keys, `DynaFnDecisions`' residue, `PlatformTypes` lists, `CoreFn`, `Subsumed`,
     `WalledBodies`, `Pure.walledNativeFqns`) keys on function ids; the 32 files using `AT_*` and the 20 using constants
     move with it.
   - b. The declaration table comes from the world: `PureModelContext.java:588`, `PlatformRegistrations.java:45-52,93`
     (`catalogTable`), `ImplementationTable`'s `NO_ROW` rule (the full names of our rows instead of the catalog's),
     `EngineHandlers`' join, `NameResolver`'s universes, `BareNames`, `ResolvedNames`, `FunctionCompiler`.
   - c. Remove the 821 upstream signature texts; the 43 Lite declarations per D5-3; the 12 primitives per D4-9.
   - d. Delete: `native-membership.tsv`; `NativesGenerator.java` and `//spec:gen_natives`; `NativeMembershipDraft.java`,
     `//spec:native_membership_draft`, `//core:draft_native_membership`; `ClaimsGenerator.java`, `//spec:gen_claims`,
     `//spec:claims_generator_lib`, `//core:core_next`, `//core:core_next_prelude` and their entries in the core layer
     queries (`core/BUILD.bazel:902-903`); `native-claims.tsv`; the two `_GENERATED` entries; `NativeSignatureGeneratorTest`.
     Per D5-6 and D7-1: `Claims.java`, `ClaimRegistryTest`, `//spec:claims`; `NativeDeclarations.java`,
     `//spec:native_declarations`.
   - e. The row test, from `CatalogUpstreamDiffTest`: every row's id exists among the pinned archive's declarations
     (whole trees: corpus and PCT rows may name ids outside the default world); a miss fails and prints that name's real
     ids; the Lite rows are checked against our own declarations.
   - Verify each step: `bazel test //core:core_tests //core:guardrails //core:census //spec:spec_tests //:generated`;
     then the six passes, PCT, the user side.
4. **Checks (§3.6), audit, land.**

### 3.6 Checks and acceptance
- The harness identical: the six corpus passes (every result file), PCT (every case), the user side (56 projects, 3
  demos: no new wall), the demos executed, the reference lane not worse.
- The row test green; `ImplementationTable.dangling()` empty for the default world (no registration names nothing there,
  except rows that only the corpus's or PCT's files declare, which need their own account).
- Guards: identity counts (`IdentityGuardrailTest`: `CATALOG_LOOKUP_BY_NAME` sites go) and `PlatformNamesGuardrailTest`
  (Pure.java is exempt as a catalog, `:84`) reviewed with dated notes; spec ratchets re-blessed with reasons.
- Boot: Pure.java no longer parses 864 texts at class load; record JVM and WASM boot.

### 3.7 Pitfalls already hit
- Our key is not upstream's id: ours drops the return type and spells full type names; upstream's keeps the return type
  and uses short names (R1 §2 risk 1). Phase 3 added "two upstream declarations with one id are an error, and a collision
  guard on the id (it uses short type names)" (plan Phase 3 item 2).
- 524 of the then 794 membership rows are upstream bodied functions we implement in Java; "natives only" was never the
  set (R1 §0.4).
- `gen_natives` reads its own committed output and the committed `CORE_IMPORTS`; one `//:update_generated` run was not
  a fixed point (G1 0.4). Moot once it is gone; do not reintroduce a generator reading committed generated files.
- A quick fix that copied a name rule into string checks was rejected by the identity guard, rightly (PARK-5 notes in
  `BP/docs/build-inventory/program/DEBTS_RESOLVE_AND_TYPE_ONCE.md`): change identity by declaration, not by spelling.

### 3.8 Open decisions for the user
- **D5-1. The row form.** (a) Function-id constants in Pure.java (`X = id("...")`) whose implementations live in the
  existing registries; (b) a table file of ours read at class init. "Pure.java as rows" points to (a). R1 §1.B's
  `catalog("<id>")` referenced a catalog resource that no longer exists.
- **D5-2. The 488 overload groups.** Today each is the ids of the constants at one name. `FunctionId` is "compared whole
  — never parsed back apart" (`FunctionId.java:14-16`), so a group cannot be computed by cutting ids. Options: each row
  carries its full name and groups are computed from rows; or groups are the world's declarations at a name (but worlds
  differ: the default world and the corpus's files); or the call sites ask for the row set they need.
- **D5-3. Where the 43 Lite declarations live:** in Pure.java as Pure text (ours), or a hand `.pure` resource (R1 §1.B
  suggested `lite.pure`; it would join the TeaVM resource lists, `PreludeResources.java:22`, `PageResources.java:15`).
- **D5-4. What users lose.** Against the measured default world, 70 rows at 45 names are declared today only by Pure.java
  (§5.2): corpus helpers (`toSQLString`, `toDDL::*`, `executionPlan`, `testDataGeneration::*`, `scanRelations`,
  `scanColumns`, `planToString`) and a few general ones (`date::fromEpochValue`, 2 versions in
  `ENGINE/.../core/pure/corefunctions/dateExtension.pure:543,548`; `collection::sortByReversed`; the JSON helpers
  `meta::json::toPrettyJSONString`, `getValue`, `tdsToJSONKeyValueObjectString`; `meta::legend::executeLegendQuery`).
  None is a handler registration (checked by id prefix in `Handlers.java`, `CoreCompilerExtension.java`,
  `RelationalCompilerExtension.java`, `ServiceCompilerExtensionImpl.java`, `ElasticsearchCompilerExtension.java`: 0 of 45;
  the 12 handler files of `WORLD/e1_handler_files.txt` contain none of `fromEpochValue`, `sortByReversed`,
  `toPrettyJSONString`, `executeLegendQuery`, `sqlNull`/`sqlTrue`/`sqlFalse`, `executionPlan`, `toSQLString`,
  `scanRelations`), so legend-engine users cannot
  call them either (D4-2's reading of `CompileContext.java:544-570`); no project, demo or app source uses them (grep of
  `projects/`, `*/demo/`, `datacube/src`, `query/src`, `studio/src`, `engine-client/src`: 0). Options: accept and record
  each in `docs/SEMANTICS_REGISTER.md` if it differs from legend-engine; or change the module choice.
  Recommendation: accept, after U5-2 confirms the list at id level.
- **D5-5. Functions the platform itself calls that the default world lacks.** `sqlNull` is built by the typer for a bare
  `TDSNull` (`Typer.java:233-238`) and, with `sqlTrue`/`sqlFalse`, is where the dynafunctions resolve
  (`DynaFnDecisions.java:239-242`). They are `core_relational` functions, not in the default world (§5.2). Without a
  declaration, a user's TDS query using `TDSNull` and a user mapping using those dynafunctions break. Options: (a) the
  three resolve to legend-lite's own declarations (the existing SHIM resolution, `DynaFnDecisions.java:264-266`, maps a
  dynafunction to a `Pure.Lite` function); (b) the default world must bring them (a rule beyond the module choice); (c)
  accept the break. U5-4 lists every such case.
- **D5-6. The claims class and test.** `Claims.java` (`spec/src/gen/java/com/legend/claims/Claims.java`) is read today by
  `PreludeGenerator` (gone in Phase 4), `ClaimsGenerator` (gone with `gen_claims`) and `ClaimRegistryTest`
  (`UNCLAIMED_MAX = 0`). After Phase 5 only the test is left, and a row without an implementation cannot exist in Phase
  5's form, so the test has nothing left to catch: retire `//spec:claims` with it, or restate the check over rows.
- **D5-7. `native_declarations`** (D7-1).
- **D5-8. How many landings.** The plan says "Several PRs (837 signatures ...)"; the count is now 864 sites (821
  upstream + 43 Lite). One branch, a few commits, one CI run is the program's process.

### 3.9 Stale or contradictory statements (Phase 5)

| Location | Says | Correct statement |
|---|---|---|
| `UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` §2 `gen_natives` row, §3 "Large: the natives catalog", §4 items 1-2; `R1.md` §1 A-C | one catalog resource of ~16.5k rows; `X = catalog("<id>")`; "native_declarations widened IS this catalog" | superseded by the plan's Phase 5 (§1.1) |
| `GENERATORS.md` §2 row 5 ("split now; full catalog later"), row 11 ("widen ... then commit"), the "Items 5 to 8" paragraph, §6 steps 4-5 | the split and a widened catalog report | superseded for 5-8 (its own update note, lines 7-11); row 11 and §6 step 5 are not covered by the note: open (D7-1) |
| `GENERATORS.md` §1 rule 4 | people edit inputs: "native membership, DynaFn resolutions, the Lite extensions" | DynaFn's resolutions are a hand class since Phase 2 (`builtin/DynaFnDecisions.java`), not a generator input; membership goes in Phase 5 |
| `GENERATORS.md` last line | "Decisions still open: D2" | decided: decision 5 |
| `BUILD_REBUILD_DESIGN_2026_10_05.md` §6 D2, D3 | open | D2 decided; D3 moot for `gen_natives`, back as D4-5 |
| Plan Phase 5 | "Several PRs (837 signatures ...)" | no PRs (plan header); 864 sites today (821 + 43) |
| Experiments §6 item 7 | "Settled by switching the implementation table on and rerunning the corpus and PCT" | also the user side, which loses functions (D5-4, D5-5) |
| `WORLD_H.md` §0 item 6, §3.3; `MANIFEST_WORLD_HOMEWORK` §5 | "837 signature texts ... 488 AT_* groups" | 864 and 488 now |
| `G1_dossiers.md`, `gen_natives` item 14 | "It cannot be bump-only, because membership edits are everyday work" | superseded: retired |
| `builtin/Pure.java:13-19` | "HAND-CURATED ... To add a native: add the verbatim signature citing its .pure path" | generated since 2026-09-11; rewritten by Phase 5 |

### 3.10 Unknowns that need homework before coding
- **U5-1. A run without the catalog** (experiments §6 item 7): the corpus, PCT and the user side with Pure.java's 821
  upstream declarations kept out of the declaration table. A switch for this does not exist (UNVERIFIED that one can be
  added without editing core; otherwise a scratch branch).
- **U5-2. Rows the default world does not declare, at id level:** the FQN-level answer is §5.2 (70 rows, 45 names);
  repeat it against the generated world by id.
- **U5-3. Our ids against upstream's for every declaration** of the default world (WORLD_H §6 item 9): compare
  `SignatureMangle.mangle` with legend-pure's own ids (the reference dump prints each resolved function's signature id,
  `tools/reference/README.md`; or legend-pure's `FunctionDescriptor` through the pinned jars).
- **U5-4. Calls the platform builds by name:** 138 `new AppliedFunction("<literal>", ...)` sites in `core/src/main/java`
  (grep; most common `map` 13, `tableReference` 7, `project` 7) plus the one through `Pure.SQL_NULL.qualifiedName()`
  (`Typer.java:238`). Each name must resolve in the default world (a test can assert it); PARK-5's research counts 232
  such sites in all (`DEBTS_RESOLVE_AND_TYPE_ONCE.md`).

---

## 4. Phase 7: the self-contained bump

### 4.1 Goal
Today the bump runs `bazel run //:update_generated` once and then `bazel test //...` (`tools/bump/Bump.java:148-157`).
That writer regenerates everything, ours and the measurements included, so moved test expectations are re-blessed
silently before the tests run, and one run was not a fixed point (`G6_machinery.md` §2a, §4, §8 P2, P4). After Phases
1-5 every upstream file is generated from the archives alone. Phase 7 gives the bump its own writer for exactly those
files, a seal that a millisecond test checks every day instead of regenerating, makes `//:update_generated` a manual
writer of our own files, and proves the whole with one real bump that needs no hand edit to a generated file.

### 4.2 Agreed design and decisions (with sources)
- Plan Phase 7: "`//:update_upstream` (the upstream records only, checked to be a fixed point); the reports committed;
  the seal and its everyday test; `//:update_generated` becomes ours only and `manual`; `bazel run //tools/bump --
  <release>` runs decide, move, regenerate, reports, seal, test (nothing re-blessed first), and leaves the judging to a
  person. Acceptance: a real bump to the next legend-engine release, end to end, with no hand edit to any generated file."
- `GENERATORS.md` §3 (the steps and the daily seal check: "One test reads the committed bump records and
  `upstream.seal`, and fails if any record's hash differs ... Its message says 'generated by the bump; run `bazel run
  //tools/bump`'"; "the bump generators never run in the everyday gate, even on a cold cache"), §1 rules 2-3, §6 step 6.
- The module choice lives "in the bump's configuration beside the pins" (north star).
- Line endings: `.gitattributes` forces LF and keeps `prelude.pure` byte for byte (`-text`), "so a seal's hashes hold on
  every platform" (`UPSTREAM_ONLY_HOMEWORK` intro).
- Bazel changes reviewed by an audit agent (decision 4).

### 4.3 Read first (in order)
1. The plan: north star ("The bump"), Phase 7, Phase 8's measurement-group bullet.
2. `GENERATORS.md` §1-3, §5, §6.
3. `G6_machinery.md` §2, §2a, §3, §4, §5, §7, §8 (written at `669b39ad1`, before Phase 1: check each finding against
   today's files).
4. `BUILD_REBUILD_DESIGN_2026_10_05.md` §4.2 (groups A-F and the "Mechanics" list).
5. Code: `tools/bump/Bump.java`, `tools/bump/test/BumpTest.java`, `tools/bump/BUILD.bazel`; root `BUILD.bazel:77-126`;
   `core/BUILD.bazel:711-718,803-808`; `parser-equivalence/BUILD.bazel:493-504`; `release.MODULE.bazel:18-37`;
   `.gitattributes`.

### 4.4 The code today (entry points)

| Where | What it does now |
|---|---|
| `tools/bump/Bump.java` (317 lines) | phase 0 decide (the release must be on Maven Central; pure derived from the engine pom; tag commits over HTTPS, :93-117); phase 1 move (the PINS block rewritten whole, `PINS` :60-64, `readPins` refuses any other name :174-190; `REPIN=1 bazel run @maven_upstream//:pin` and `@maven_runner//:pin`, :139-142); phase 2 `bazel run //:update_generated` (:148-151); phase 3 `bazel test //...` (:153-157); prints NEXT (:159-167, naming "prelude.pure / Pure.java / native-*.tsv / DynaFn.java / CoreImports.java / corpus-manifest.tsv / pmcd-reachable.tsv / protocol-roster.tsv") |
| `//tools/bump:bump_test` (`tools/bump/BUILD.bazel:23-36`) | the PINS rewrite against the real `release.MODULE.bazel`; in the checks lane (`docs/GATES.md`) |
| `//:update_generated` (root `BUILD.bazel:106-126`) | `testonly`, not `manual`: runs `//core:update_generated`, `update_ladder`, `update_stress_corpus`, the DataCube, engine-client, docs, saved-queries, parser-equivalence and legend-art writers, the three ratchet writers (`//pct:update_ratchets` is a manual 2 GB target pulled in, `pct/BUILD.bazel:275-282`), keyword coverage, vocab, the native image's metadata |
| `//:generated` (root `BUILD.bazel:77-98`) | every diff-test suite; in CI's checks lane and `//gates:local` |
| `//core:update_generated` (`core/BUILD.bazel:803-808`) | DynaFn.java, Pure.java, CoreImports.java, engine-handlers.tsv, native-claims.tsv, prelude.pure (`_GENERATED` :711-718); message "This file is generated from the pinned upstream release" (wrong today for the two that move with our code, G6 §7) |
| `//parser-equivalence:update_generated` (`parser-equivalence/BUILD.bazel:493-504`) | `corpus-manifest.tsv`, `engine-grammar-fixtures.jsonl`, `pmcd-reachable.tsv` |
| `//tools/engine-runner:update_vocab` | `tools/engine-runner/vocab.tsv` |
| `release.MODULE.bazel:23-37` | the PINS block (engine 4.145.0, pure 5.99.0, repos, tag commits, archive integrities, five engine-managed versions); no module-choice setting exists anywhere |
| Not existing yet | `//:update_upstream`, `upstream.seal`, its test (grep of the root, `tools/`, `core/`, `spec/` BUILD files) |

### 4.5 Steps
0. **Preconditions:** Phases 4 and 5 landed: every generator of §1.2's records reads upstream (and the setting) only.
   If D4-5 chose our parser for the default world, settle how the daily check covers it first.
1. **Decide** §4.8's open points.
2. **`//:update_upstream`:** a `write_source_files` over the records only (§1.2 rows 1, 2, 3, 4, 6, 7, 8, 10, and 11 if
   kept), tagged `manual`; its pieces leave `//core:update_generated`, `//parser-equivalence:update_generated` and the
   root writer. Fixed point: run it twice in the bump and require the second run to change nothing (D7-6).
   Verify: on a clean tree, `bazel run //:update_upstream` twice, `git status` empty.
3. **The seal and its test:** the bump writes the seal after regenerating (the release, the setting, a sha256 per record
   path); one test reads the committed records and the seal and fails on any difference, naming `bazel run
   //tools/bump`. It replaces the records' diff tests in `//:generated` (GENERATORS §3). Verify: edit one byte of a record,
   the test fails with that message; run it on the three CI platforms.
4. **`//:update_generated`:** ours only, `manual` (D7-4 for the measurements); fix its messages.
5. **`Bump.java`:** phases decide, move, regenerate (`//:update_upstream`, the fixed-point check), reports, seal, test
   (`bazel test //...` with nothing re-blessed: every moved expectation is a red diff, listed together), then print the
   judgement steps (rewrite NEXT). Extend `BumpTest` to the seal writer and the setting.
6. **Instructions:** `README.md:294-295`, `docs/GATES.md`'s generated-files and "Upstream" paragraphs,
   `release.MODULE.bazel:18-21`, `tools/bump/BUILD.bazel:12-14`, the generated files' header lines
   (`prelude.pure:4`, `engine-handlers.tsv:1`), the diff-test messages (G6 §5, §7).
7. **Acceptance:** a real bump (D7-8), end to end, no hand edit to a generated file; the judgement written in GATES.
8. Audit (decision 4), CI (a full run: workflows or MODULE may change), land.

### 4.6 Checks and acceptance
- `bazel run //:update_upstream` twice on a clean tree: nothing changes.
- The seal test: green on a clean tree; red with the bump's message after a one-byte edit of any record; green on Linux,
  macOS and Windows (the hashes depend on `.gitattributes`; a record that can carry CR must be `-text`, as `prelude.pure`
  is).
- `//:generated` no longer runs any bump generator: check with `bazel aquery` or `cquery` that `//:generated`'s tests do
  not depend on the generators of §1.2 (they depend only on the committed files and the seal).
- `bazel build //...` does not run `//:update_generated` or `//:update_upstream` (both `manual`).
- `//tools/bump:bump_test` green.
- The real bump: the PINS block moves, the records regenerate from the new archives, the seal is rewritten, `bazel test
  //...` lists every moved expectation red together, a person re-blesses each with a reason, no generated file is touched
  by hand.

### 4.7 Pitfalls already hit
- One update run was not a fixed point because generators read committed generated files through compiled core
  (`G1_dossiers.md` 0.4; G6 §2a). Phases 1-2 removed this for imports, DynaFn and the handler table; keep every record's
  generator free of committed generated inputs.
- The bump re-blessed measurements before testing (G6 §4 "What it assumes", §8 P4): the ratchets, the ladder, keyword
  coverage and `docs/protocol-roster.tsv` were rewritten, so the tests could not report the move.
- `//:update_generated` is not manual, so `bazel build //...` ran the 2 GB `//pct:ratchets` (G6 §8 P1); still true
  (`BUILD.bazel:106-126`, `pct/BUILD.bazel:275-282`).
- Manual lanes are not in `bazel test //...`: the reference-lane golden and `//pct:update_ratchets_test` are checked by
  no bump (G6 §4).
- `.bazelrc`'s Windows bash path and other Windows follow-ups are open (plan Phase 8); the bump runs `$BAZEL_REAL` or
  `bazel` and needs the network (Central, GitHub).
- zsh: `"${SHA}:refs/heads/main"` needs the braces when landing (`START_HERE.md` §4).

### 4.8 Open decisions for the user
- **D7-1. The record list,** including `native_declarations`: retire it (the Phase 5 row test lists a name's real ids,
  which is what it was for) or widen and commit it as a sealed report (GENERATORS §2 row 11). Recommendation: retire,
  since nothing reads it (`G1_dossiers.md`, its item 8) and the row test covers its use.
- **D7-2. Where the module choice lives, and whether the seal covers it.** `Bump.PINS` is a fixed list and `readPins`
  refuses anything else in the PINS block (`Bump.java:60-64,174-190`). Options: a new PINS entry (Bump.java and BumpTest
  change); its own block or file beside `release.MODULE.bazel`; a constant in `tools/generators/defs.bzl`. Recommendation:
  the seal records it (§1.2).
- **D7-3. The seal's file and its test's home** (root or beside `release.MODULE.bazel`; the checks lane and
  `//gates:local` through `//:generated`).
- **D7-4. The measurements:** GENERATORS §6 step 6 removes them from `//:update_generated` now; the plan's Phase 8 says
  the measurement group, "set aside", is decided then. Leave them until Phase 8, or move them now. Either way the bump
  must not run their writers.
- **D7-5. Whether the bump runs the manual lanes** (the reference lane, about 8 GB; PCT's ratchet test; the manifest
  census).
- **D7-6. The fixed-point check:** two runs in the bump, or proof by construction (no record generator reads a committed
  generated file) plus a guard.
- **D7-7. The guard that every writer and diff test belongs to a suite and a gate** (GENERATORS §6 step 7): Phase 7 or
  Phase 8; neither names it.
- **D7-8. Which release proves it.** The bump picks a published release (`Bump.java:98-100`); `README.md:295` shows
  4.146.0 as an example (**UNVERIFIED** that it is published). If upstream added syntax we cannot parse, the bump stops
  ("fix the platform first", `Bump.java:149-151`): agree how much platform work the acceptance may pull in. A pins-only
  dry run (`bazel run //tools/bump -- <release> --pins` on a scratch branch) scopes it early.

### 4.9 Stale or contradictory statements (Phase 7)

| Location | Says | Correct statement |
|---|---|---|
| `GENERATORS.md` §3 step 3 | `//:update_upstream`: "items 1 to 8" | after Phase 5: items 1, 2, 3, 4, 6, 7 and the default world (8's successor) |
| `GENERATORS.md` §3 step 4 | "Reports: items 9 to 11, committed" | 9 deleted in Phase 1; 10 committed since Phase 1; 11 open (D7-1) |
| `GENERATORS.md` §1 rule 3 | nothing in the everyday gate needs the archives "except tests that use them (PCT, parser-equivalence)" | spec's tests read them too (`spec/BUILD.bazel:154-169`, `upstream = True`), and so will Phase 5's row test |
| `GENERATORS.md` §6 step 6 against plan Phase 8 | remove the measurements now / decide the measurement group in Phase 8 | unassigned (D7-4) |
| `tools/bump/Bump.java:21-51,159-167` | phase 2 = "every generated file"; NEXT names Pure.java and native-*.tsv | stale after Phases 5 and 7 |
| `release.MODULE.bazel:18-21` | "regenerates every generated file ... then the judgement half (docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md §5 phases 3-6)" | that target is Maven-era (G6 §5); rewrite with the new steps |
| `README.md:294` | "regenerate every generated file from the pinned upstream release" | seven writer groups do not come from upstream (G6 §4); after Phase 7 the bump owns the upstream files |
| `docs/GATES.md`, the "Generated files" bullet | lists `//query:update_generated_test`, Pure.java's signatures, native-claims.tsv, "Query's icons.ts" | `//query` has no writer (G6 §5); after Phases 5 and 7 the list changes again |
| `core/BUILD.bazel:805` | every file "generated from the pinned upstream release" | wrong today for `native-claims.tsv` and `prelude.pure` (G6 §7); replaced by the seal's message |
| `tools/bump/BUILD.bazel:12-14` | the bump calls "repin, //:update_generated, `bazel test //...`" | stale after Phase 7 |
| North star | the module choice "kept in the bump's configuration beside the pins" | no such setting exists yet (D7-2) |

### 4.10 Unknowns that need homework before coding
- **U7-1. Platform independence of the records once the generators leave `//:generated`.** Today the checks lane runs
  every generator on Linux, macOS and Windows and compares with the committed files, which proves their bytes do not
  depend on the platform. After Phase 7 they run only inside the bump, on one machine. Before relying on the seal, run
  `//:update_upstream` once on each CI platform (a throwaway CI job) and compare the bytes, the new default-world
  generator above all.
- **U7-2. What `bazel test //...` reports after a bump when no measurement writer runs.** List the diff tests that hold
  each measurement and where they run: the suites `//spec:update_ratchets_tests`, `//parser-equivalence:update_ratchets_tests`,
  `//scripts/parser:update_keyword_coverage_tests`, `//core:update_ladder_tests` and `//docs:update_generated_tests` are in
  `//:generated` (root `BUILD.bazel:80-98`); the corpus rosters are in the corpus lanes; `//pct:update_ratchets_test` and
  the reference lane are `manual` (G6 §4). Whatever is manual needs D7-5's answer.
- **U7-3. The next published release and its cost.** Which engine release follows 4.145.0 on Maven Central (needs the
  network; the bump's own check, `Bump.java:98-100`), and what a pins-only dry run moves (D7-8).
- **U7-4. The pools.** Only `maven_upstream` and `maven_runner` name the release (`release.MODULE.bazel:66-81,159-250`;
  `Bump.RELEASE_POOLS`, `Bump.java:70`); confirm no Phase 4-5 change adds a pool or archive keyed on it.

---

## 5. Measurements made for this brief (how to repeat them)

All read existing data; no build. `WORLD` and `BR` as defined at the top; run Python with `-I`.

**5.1 The legacy TDS functions and the closure.**
- `closure.json`'s keys `S1 upstream lists|declarations`, `|with bodies`, `|runnable bodies` contain none of
  `meta::pure::tds::columnValues`, `renameColumn`, `window`, `columnByName`; they contain `meta::pure::tds::TabularDataSet`
  and all 14 handler-registered TDS names (which are also seeds in `e1_seeds.json`).
- `awk -F'\t' '$3=="meta::pure::tds::columnValues"' WORLD/uni_edges.tsv` (and the other three): no element refers to
  them; `columnByName` has no edges at all (not a function).
- `grep -rn --include='*.pure' -E "columnValues\(|renameColumn\(" ENGINE` outside test directories: only the declarations
  (`tds.pure:518-532`); `window(` hits outside tests are `^$window(...)` instance copies in `pureToSQLQuery*.pure` and
  `core_external_query_sql`'s own `window` (`function_processors.pure:494-583`).
- `grep -n "renameColumn\b\|columnValues\|tds::window\|columnByName" Handlers.java CoreCompilerExtension.java
  RelationalCompilerExtension.java` and the other handler-registering extensions (found with `grep -rln
  getExtraFunctionHandlerRegistrationInfoCollectors ENGINE --include='*.java'`: service, relational, Elasticsearch,
  plus the core interfaces): only `renameColumns` (`Handlers.java:2357`). The same names, grepped in all 12 files of
  `WORLD/e1_handler_files.txt`: 0 hits.

**5.2 Pure.java's rows against the measured default world (name level).** For each name in `native-membership.tsv`
(column 2): in upstream core if `decl_repo.json` lists a repository starting `platform` or `core_functions`; else in
the world if in `closure.json['S1 upstream lists|runnable bodies']`. Result: 821 rows, 456 names; upstream core 637
rows / 318 names; the query-surface closure 113 / 92; only with all bodies 1 / 1; **not in the world 70 rows / 45 names**
(`core_relational` 29, `core` 16): `alloy::objectReference::*` (3), `alloy::service::execution::setUpDataSQLs*` (2),
`core::runtime::connectionByElement`, `json::getValue`, `json::tdsToJSONKeyValueObjectString`, `json::toPrettyJSONString`,
`legend::executeLegendQuery`, `alloy::connections::relationalMapperPostProcessor`, `executionPlan::execute`,
`executionPlan::executionPlan` (6 versions), `executionPlan::toString::planToString*` (2), `collection::sortByReversed`,
`date::fromEpochValue` (2), `graphFetch::execution::alloyConfig` (5), `lineage::scanColumns`, `lineage::scanRelations`
(2), `router::execute` (3), `router::preeval::preval` (2), `sqlQueryToString::sqlFalse`/`sqlNull`/`sqlTrue`,
`sqlstring::toNonExecutableSQLString`/`toSQL`/`toSQLString`/`toSQLStringPretty`, `toDDL::*` (6 names),
`execute::executeInDbToTDS`, `milestoning::concatenateTemporalTdsQueries`, `postProcessor::*` (3),
`testDataGeneration::*` (4 names), `tests::csv::toCSV` (3).

**5.3 Upstream names our Java quotes.** Every `"meta::..."`/`"core::..."` literal in `core/src/main/java` except
`Pure.java`: 503 names, 415 of them upstream elements; 240 in upstream core, 101 in the closure, **74 not in the world**
(`core_relational` 47, `core` 27), mostly in `NativeFn.java`, `PlatformTypes.java`, `SystemMetamodel.java` (the result
views' plan nodes), `TestDataGenerationNatives.java`, `TdsLegacy.java`; the others are listed in U4-6.

**5.4 How legend-engine resolves a user call.** `CompileContext.buildFunctionExpression` (`:476-485`) calls
`Handlers.buildFunctionExpression` (`Handlers.java:3127-3139`), which calls `CompileContext.resolveFunctionBuilder`
(`:544-570`): the registered handler map (core list plus extensions' `getExtraFunctionHandlerRegistrationInfoCollectors`,
`Handlers.java:2026`) and, through imports, the same map; an unknown name returns `null` and fails later. User functions
enter the map at `FunctionCompilerExtension.java:97` (`new UserDefinedFunctionHandler`). (Code reading; U4-5 runs it.)

**5.5 Calls built by name.** `grep -rhoE 'new AppliedFunction\(\s*"[^"]*"' core/src/main/java | wc -l` → 138;
`grep -rn 'new AppliedFunction(com.legend.builtin.Pure' core/src/main/java` → `Typer.java:238` (`SQL_NULL`).

---

## 6. Process notes for these phases
- Plan each phase with the user first; settle §2.8, §3.8, §4.8 in plain words, one answer each.
- IN_FLIGHT on main before the first core edit, listing every file (Phase 4 touches core, spec, wasm, sdlc-server, docs;
  Phase 5 touches a large part of core's registries).
- A shortcut that must stay becomes a `docs/PARKED_WORK_LEDGER.md` row with an anchor in `ParkedWorkLedgerTest`.
  Phase 4 closes PARK-11 (and needs PARK-12 closed by Phase 3b first).
- No local paths in anything committed: if this brief is copied into the plan branch, keep its paths relative.
- The plan file is being edited concurrently: re-read its Phase 4, 5, 7 sections before starting.

**Program-level statements to correct (not tied to one phase):**

| Location | Says | Correct statement |
|---|---|---|
| Plan Phase 5 (same file as the header that says "No PRs since 2026-10-06") | "Several PRs" | no PRs: audit, local gate, one full CI run on the branch, push that commit (`START_HERE.md` §4) |
| `docs/IN_FLIGHT.md` on `origin/main`, the program's entry | "one PR per phase" | no PRs (already listed in `BP/docs/build-inventory/program/PHASE_3_LANDING.md` §5 item 3) |
| `AGENTS.md` on main, "Standing documents" | "Current work: the compiler rebuild. Start at `docs/EXECUTION_PLAN_2026_09_26.md` §0" | this program's sessions start at `START_HERE.md` (noted in `START_HERE.md` §2; a shared file: ask the user) |
| `BP/docs/build-inventory/manifest-world/WORLD_H.md` header; `EXP/LOOSE_ENDS.md`; `R1.md`-`R3.md` | line numbers measured at `1689703f2` (before Phases 1-3) | some have moved: R1 puts `NameResolver`'s `PLATFORM_TYPE_FQNS`/`PLATFORM_FQNS`/`QUERY_SCOPE` at :298-357 and :561-566, today :257, :274, :520 (`CORE_IMPORTS` left the file in Phase 1); R1 puts Pure.java's `AT_*` block at 2280-2768, today :2318-2806 (Phase 3 added rows). Re-find symbols; do not trust old line numbers |
