# In flight

This file says who is working, on what files, in what order, and the rules between sessions; each line's status lives
in its own plan (plan rule 0b.16).

**Active lines and their order (the user, 2026-10-04).** Several sessions push to `main`; each announces a cross-area
edit here before landing it and lands with the full chain. The priorities are **Bazel, Studio, and the database owner
with the compiler's plan/execution split**, in this order:

1. **The Bazel program** (`docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`, log `docs/BAZEL_EXECUTION_LOG.md`, both on
   main since 2026-10-07; its remaining work is the build rebuild's Phase 8): build files — `core/BUILD.bazel`, `tools/deps`, `MODULE.bazel` (splitting out a
   `release.MODULE.bazel`), `pct`, `spec`, `parser-equivalence`, the guards. Batch 8 landed (`053e15006`); the old
   plan's remaining work lives in the rebuild program below. It also carries the
   reference-lane fix the user decided ("fix rule to match pure": legend-pure's overload for `min`/`max` over a list
   literal), in `core/compiler/spec` — no one else edits those files until it lands. **Announced 2026-10-05:** two small core edits the
   guards found — `core/src/main/duckdb/com/legend/exec/DuckDbAppenderLoad.java` (its `finally` replaced an in-flight
   unchecked error with the staging drop's; found when P3-27 put `src/main/duckdb` under the guards), and the core
   test sources the guards and censuses read (declared file lists, P3-27/P3-05). F-L1 in `projects/FINDINGS.md` (a view
   inside a Schema is lifted twice; one line in `ModelBuilder`) is now the build rebuild's (Phase 3b item 1). **Announced 2026-10-05 (P4-18):** `core/.../server/LegendHttpServer.java` and the warehouse server gain
   `--exit-with-parent` (exit when stdin reaches EOF; only tests set it), so no test stops a server with `taskkill`.
   **Parked (2026-10-06)** with the rebuild program's Phase 8: not landing now.
   **Announced 2026-10-07 — the CI landing, the rebuild's L1 (plan §4; the lane set `docs/build-inventory/program/PHASE_8.md` §8), branch `build/ci-lanes`, before Phase 3 lands.**
   L1a, no source file: `.github/workflows/gate.yml` and `gates-run.yml` (the build lane builds the product; every
   lane a `//gates` suite; the lane guard; the cache reduced to the downloads; actionlint and the actions pinned);
   `gates/BUILD.bazel` (the suites, `all`, `heavy`, `local` as a suite of suites; its header's PR sentence goes); the
   root `BUILD.bazel` (`//:update_generated` manual); `spec/corpus.bzl` and `spec/BUILD.bazel` (the judge passes
   manual); `core/layers.bzl` (the layer queries manual: a Bazel file under `core/`, no Java); `tools/census`,
   `tools/junit`, `wasm` BUILD files (hand tools manual); `tools/guards/` (two new tests: every hand tool compiles,
   actionlint over the workflows); `MODULE.bazel` and `third_party/` (actionlint's archives); `docs/GATES.md`. L1b:
   `tools/browser/defs.bzl` (a Linux-only option), `datacube/BUILD.bazel`, `query/BUILD.bazel`, `site/BUILD.bazel`,
   `studio/BUILD.bazel` and `datacube/demo/*.mjs` (the harnesses as tests: `bazel/exec`'s P4-02, P4-03, P4-04 and
   P4-08 rebased) — Studio's line owns `query/`, `site/` (below; `datacube/` is the sixth line's since 2026-10-09): noted
   here once, proceeding.
   **L1a landed 2026-10-07 (`fd1b0ba77`); L1b landed 2026-10-07 (`d126b47e1`).** Its cache fix follows on
   `build/ci-cache` (`gates-run.yml` only).
   **Landed 2026-10-07 — L1c, the warehouse knows nothing about Bazel** (commit "Build rebuild L1c: the warehouse knows nothing about Bazel"; GATES entry "Build rebuild L1c"). The DataCube line's edits to `warehouse/` rebase onto it: `ServerRunfiles.java` is gone, `WarehouseServer.commandLine` resolves no path, `//warehouse:serve` and `//datacube:app` are `warehouse_folder` targets, `duckdb_extensions` is `postgres_extension`.
   **The build rebuild and the self-contained bump** (`docs/REBUILD_PROGRAM_2026_10_06.md`, on main since 2026-10-07 with its research and
   evidence; a session picking it up starts at `docs/build-inventory/program/START_HERE.md`. Phases 0 and 1 landed as PRs;
   since 2026-10-06 there are no PRs: one full CI run on the branch, then that commit pushed to main). **Announced 2026-10-06, replacing the 2026-10-05 note — PR 1, Phase 0:** the build targets
   (`//:java`, `//:web`, `//:wasm`, `//:native`, `//:sites`) and their compile-only guard, the product's jars on
   http_jar, `//:web` without Node, stamping off (`docs/BUILD_REBUILD_DESIGN_2026_10_05.md`, which the PR carries).
   Files: the root `BUILD.bazel`; `//:__pkg__` visibility on `//core:server`, `//sdlc-server:server`,
   `//sdlc-server:page` and `//site:dist`; **core's runtime drivers** in `core/BUILD.bazel`, now http_jars, with
   **Postgres JDBC 42.7.4 → 42.7.13 without checker-qual** (the server's Postgres arm); `warehouse/BUILD.bazel`'s
   DuckDB jar label; **Studio's** `datacube/`, `query/` and `studio/` BUILD files (each app's `:bundles`, built by
   `esbuild_bundle`) and their `package.json` and `pnpm-lock.yaml` (esbuild removed, relocked with the pinned pnpm);
   `MODULE.bazel` (the product-jar extension, esbuild's archives), `release.MODULE.bazel`, `.bazelrc`, `tools/deps`,
   `tools/guards`, `tools/js`, `tools/jars`, `pct/`, `spec/`, `docs/GATES.md`, and the CI cache key. No source
   edits; no `depot-server/` or warehouse visibility edit (the 2026-10-05 note was wrong there).
   **Phase 1 (generator hygiene, no behavior change), branch `build/phase1-generators`, stacked on PR 1; announced
   2026-10-06, the full scope:** core source: `CORE_IMPORTS` leaves `core/src/main/java/com/legend/compiler/NameResolver.java`
   for a new generated `CoreImports.java` (`SEQUENCE`), read by `NameResolver`, `BareNames` and `PreludeGenerator`
   (`DiagramService` a comment); core tests: two pins (`HarnessDisciplineTest`'s count for the trimmed eager-compile
   probe, `ParserBoundaryArchTest`'s entry for it); `core/BUILD.bazel`: visibility for the narrowed generators
   (`diagnostics`, `protocol`, `parser`, `sql_dialect`, `database`, `compiler_element_type`, `compiler`, `plan`,
   `planner`, `lowering`), a `stress_dense` group, `manual`/`testonly` tags and the generated-file map. **Studio's:**
   `datacube/` (the catalog generator split into `CatalogRulesFacts.java` + `CatalogFacts.java`, `catalog-facts.ts`'s
   header line regenerated, `manual`/`testonly` tags, narrowed deps) and `engine-client/BUILD.bazel`. Also: `wasm/`
   (`ZoneMain.java`, BUILD), `warehouse/BUILD.bazel` (the native image's metadata in its own library),
   `parser-equivalence/` (new programs `PmcdReachability`, `PmcdWorklist`, a committed `pmcd-reachable.tsv`; deleted
   `MigrationSizing`, `PmcdReachabilityCensus`), `spec/` (BUILD, `ImportsGenerator`, `PreludeGenerator`,
   `CoreImportsParityTest`, `EagerCorpusCompileProbe`), `scripts/corpus/`, `scripts/parser/` (`keywords.py`),
   `tools/` (`par`, `reference`, `engine-runner`, `java_run`, `generators`, `untangle`), `docs/BUILD.bazel`,
   `docs/GATES.md`. Every changed generator's output is byte-identical.
   **Phase 2 (announced 2026-10-06): the upstream tables joined when the classes load**, branch `build/phase2-tables`.
   core: `builtin/DynaFn.java` generated whole from upstream (members, dialects, inference); a new hand-written
   `builtin/DynaFnDecisions.java` holds the platform's resolutions, keyed by the generated members (an unlisted name is
   UNSUPPORTED); `builtin/EngineHandlers.java` joins upstream's (name, id) table with the platform's declarations by
   function id when it loads, and `engine-handlers.tsv` keeps upstream's two columns; `builtin/Pure.java` gains
   `liteSurfaceFunctions()` (the surface by function id); `native-claims.tsv` regenerated (its last column);
   comments in `normalizer/RelOpTranslator.java` and `core/BUILD.bazel`. core tests: `EngineHandlersTest`;
   `PlatformNamesGuardrailTest` (DynaFn.java's entry leaves) and `ParkedWorkLedgerTest` (PARK-3's anchor restated),
   each with a dated note; `docs/PARKED_WORK_LEDGER.md` follows. spec: BUILD, `DynaFnGenerator`,
   `EngineHandlersGenerator`, `DynaFnRegistryTest`, `SpecRatchets` and `ratchets.tsv` (the undeclared engine ids,
   shrink-only). `.github/workflows/gate.yml` runs nightly on main. `DynaFn`'s API is unchanged, so its users are
   not edited beyond that one comment.
   **Phase 3 landed 2026-10-09 (1e4a2bd40; run 38000346579; GATES "Build rebuild Phase 3"): rebased over 231 commits and re-judged on that main.** **Phase 3b landed 2026-10-10 (991df952b; run 38022354707; GATES "Build rebuild Phase 3b"; `PHASE_3B_LANDING.md`)** — files touched beyond the announced list (all consequences of the agreed design): `platform/Registrations.java` and `platform/PlatformPure.java` (new: the row kind's registration, merge and version decisions), `lowering/PlatformRegistrations.java`, `compiler/StatementInline.java`, `compiler/spec/UserCallInliner.java` (a comment), `probe/Shadow.java`, `compiler/element/ModelIntegrity.java`, `test/PureTests.java`, `core/BUILD.bazel` (`:platform` gains `:error`), `spec/BUILD.bazel` (the manual `phase3b_probes` target), parser-equivalence's `Sectionize.java`. ****L7 "resolve once", step 2, announced 2026-10-10** (branch `build/l7-resolve-once`; the design `docs/build-inventory/program/L7_RESOLVE_ONCE_DESIGN_2026_10_10.md`, agreed with the user: the call's record is `referents`, step 2 lands on its own, step 3 follows with its own announcement). **Landed 2026-10-10 (f32037517; run 38081706046, green on every job; GATES "L7 step 2"; the design note's §10 has the audit's departures)** — files touched beyond the announced list: `datacube/tools/offer-facts/OfferFacts.java` (the record's rename, one line; the DataCube line told) and `spec/src/test/java/com/legend/rcorpus/EagerCorpusCompileProbe.java` (a guard the baseline measurement needed). **L7 "resolve once", step 3, announced 2026-10-10** (branch `build/l7-step3`; the design's §6 step 3 and §11, sized from the sites): the `Calls` builder (new `compiler/spec/Calls.java`: `platform`, `form`, `exact`, `like`, `carrier`), the 195 built-call sites converted in `compiler/spec/Typer.java`, `TdsDesugars.java`, `CallShapes.java`, `SortChecker.java`, `JoinChecker.java`, `JsonChecker.java`, `GroupByChecker.java`, `IsDistinctChecker.java`, `LambdaBodies.java`, `StaticFold.java`, `SourceSubst.java`, `ReceiverOwnedFunctions.java`, `GetAllChecker.java`, `compiler/StatementInline.java`, `normalizer/RelOpTranslator.java`, `MappingNormalizer.java`, `JoinChainEmission.java`, `ViewRelation.java`, `DeclaredCoercions.java`, `XStorePureEnds.java`, `validation/ValidateDesugar.java`; then the deletions (`ResolvedNames`' no-record rule and its `BareNames` dependence, `Overloads.candidatesOf`'s bare path, `DecisionProbe.bareCall`), the ArchUnit rule in `core/src/test/.../ArchitectureTest.java`, the pins in `IdentityGuardrailTest.java` and `ParkedWorkLedgerTest.java` (PARK-5 closed). Not touched: the parser, the protocol readers, `NameResolver.java`, `Compiler.java`'s entry points. **Landed 2026-10-10 (472f0f817; run 38094185136, green on every job after one re-run of windows / warehouse (a runner filesystem error in the Node module tree during analysis of a Python target the change does not touch; main's own run of the same base, 38091778746, passed it); GATES "L7 step 3"; the design note's §12 and §13)** — files touched beyond the announced list: `compiler/BareNames.java` (the rule's per-arity table, the prelude's arities), `compiler/ResolvedNames.java` (the deletion), `compiler/element/FunctionCompiler.java` (the bare branch deleted), `builtin/DecisionProbe.java` (`bareCall` deleted), `protocol/spec/AppliedFunction.java` (`asPlainCall`), `compiler/spec/Calls.java`'s five extra shapes, the checkers that read a form by its parse name (`IfChecker`, `LetChecker`, `MatchChecker`, `EvalChecker`, `GraphFetchChecker`, `TableReferenceChecker`, `FlattenChecker`, `NavigateChecker`, `JsonChecker`, `RenameChecker`, `NewChecker`, `GroupByChecker`, `GroupLambdaAggs`, `IsDistinctChecker`), and the tests named in the GATES entry. L7 is complete; PARK-5 closed. **Announced 2026-10-11 — a duplicate mapping is refused (branch `build/dup-mapping`):** the DataCube line reported a model holding two mappings of one name crashing the normalizer's include ordering with an index error (the cycle check counted the parsed mappings against the ordered names). Two core files: `normalizer/StoreSubstitutionRewrite.java` (the check counts the names) and `compiler/ModelBuilder.java` (a parsed mapping takes its slot in the shared element namespace, so the duplicate is `ModelIntegrity`'s "Duplicated element", as a class's is); one test in `IncludeRulesTest`; the ledger's PARK-25 records the kinds the namespace still misses (services, data, connections, runtimes). **Landed 2026-10-11 (0b29453e1; run 38145210104, green on every job after one re-run of windows / warehouse (a runner filesystem error in the Node module tree during analysis of `//datacube:jupyterlab_test`, which the change does not touch; the same error L7 step 3's run met once); the first run on the tree before the rebase onto datacube-m3, 38122177071, green on every job; GATES "A duplicate mapping is refused")** — files touched beyond the announced list: `compiler/element/ModelIntegrity.java` (`refuseDuplicates`: the refusal before Phase E reads the index, the engine's order), `compiler/element/PureModelContext.java` (its public delegate), `Compiler.java` (`normalizeLayer` calls it), `ParkedWorkLedgerTest.java` (PARK-25's and PARK-26's anchors), `integration/RelationalMappingIntegrationTest.java` (the judges' one find: a test's class named as its mapping, which the engine refuses), `parser-equivalence/src/test/resources/com/legend/equivalence/ratchets.tsv` (the ratchet moved with the new test models), `parser-equivalence/src/test/java/com/legend/equivalence/Sectionize.java` (the planner declines a snippet holding two elements of one path: the parsed model's side maps key by path alone, recorded in PARK-25). PARK-26 (two include spellings refused as a cycle) recorded; PARK-25 widened to clean-sheet mappings and to a path that is also a package. **Announced 2026-10-11 — a `~func` descriptor spelling a type by its package path is refused (branch `build/func-descriptor`):** the DataCube line's find against legend-engine 4.145.0: a relation class mapping's `~func f():meta::pure::metamodel::relation::Relation<Any>[1]` is accepted by lite, which cuts the descriptor at its signature and resolves the name alone, and refused by the engine: legend-pure's `FunctionDescriptor.validateType` (m3-core, `navigation/function/FunctionDescriptor.java`) allows no package path in a descriptor's types ("Package path not allowed in types"), so `functionDescriptorToId` fails and the compiler looks the raw text up as an element ("Can't find the packageable element", `compiler/toPureGraph/PureModel.java`). One core file: `model/MappingFromProtocol.java` (`relationFunction`, the model door the text and the JSON both pass: a package path in the signature's parameter or return types is a `ModelException` naming the descriptor, at every dialect, since the signature has no meaning in lite beyond the engine's id format and a model written so compiles nowhere). One new test, `core/src/test/java/com/legend/model/RelationFunctionDescriptorTest.java`; two fixtures rewritten to the unqualified form: `integration/RelationMappingWindowSeamTest.java`, `resolver/StackDesignWitnessTest.java`. Not touched: the parser's descriptor scan (`TokenStreamCursor.parseFunctionDescriptor`), the activator's `function:` descriptor (parse-only in lite), the wire, the corpora (no corpus text uses the form). **Landed 2026-10-11 (eabda91b8; run 38152032814, green on every job, 50 of 50, no re-run; main's own run of the previous landing (38147522593, on 0b29453e1) had timed out once in `//warehouse:tests_native` on linux / warehouse (300 s), a job the same commit passed in 38145210104 and again in this run, so its failed job was re-run rather than rolled back; GATES "A `~func` descriptor spelling a type by its package path is refused")** — as built, two differences from the announcement: the refusal is the model door's `UnsupportedMappingShape` (a positioned `ParseException` from text, `Unrepresentable` reported as `NotImplementedException` from JSON), not a `ModelException`; and a function type in a parameter or return position is refused too ("Unsupported type", the same engine rule). Files touched beyond the announced list: `core/src/test/java/com/legend/ParkedWorkLedgerTest.java` (PARK-28's anchor), `IdentityGuardrailTest.java` (NAME_AFFIX_TEST 51 → 52, dated), `docs/PARKED_WORK_LEDGER.md` (PARK-28: the signature never matched against the function's; the `~src` tail; the activator's descriptor), `docs/SESSION_HANDOFF_2026_09_02.md` (its guidance corrected). **Announced 2026-10-11 (evening) — TeaVM's unmodifiable set and map wrappers made to look up instead of scan (branch `build/teavm-unmodifiable`):** the user ruled, through the DataCube + Python line's session ("lets get it all done"), to land the one-file port now and keep the TeaVM 0.16.0 bump as a landing of its own (not L8, the upstream Legend bump), which deletes the file (`docs/build-inventory/program/TEAVM_UNMODIFIABLE_DESIGN_2026_10_11.md` §5, option 1). One new file of `third_party/teavm_classlib`: `src/org/teavm/classlib/java/util/TCollections.java`, TeaVM 0.15.0's with TeaVM's own fix (master `5971683cf1`, released in 0.16.0) ported: `unmodifiableSet` and `unmodifiableMap` answer `contains`, `containsAll`, `isEmpty`, `get`, `containsKey`, `containsValue` and `size` from the wrapped instance, as the JDK's do, instead of `TAbstractCollection`'s and `TAbstractMap`'s scans through the iterator; its JVM test `test/org/teavm/classlib/java/util/TCollectionsTest.java` (a counting set and map: the lookups open no iterator, the mutations still refuse), the package's `BUILD.bazel` (the test package selected), `README.md`, `PortedFromTest`'s list. The module every tab loads changes (`//wasm:planner` and every other `teavm_wasm` target); no edit in `core/`, `tools/teavm/` or `wasm/`. Judged by the JVM test, `//wasm:conformance_test`, `differential_test` and `round_trip_test`, the tabs' tests, and one full CI (every lane, every platform, as for a toolchain change); the DataCube line reruns `//datacube:model_scale_measure` on the landed module. **Landed 2026-10-11 (run 38187695487, green on every job after one re-run of linux-arm / warehouse: the DataCube line's `//datacube:python_engine_test` closed its browser page in its last case, the same failure as that line's own run 38175909649, a race in the test that line is fixing; GATES "TeaVM's unmodifiable set and map wrappers look up instead of scan")** — files touched beyond the announced list: `gates/BUILD.bazel` (the core lane's comment on `//third_party/teavm_classlib:tests`), the design note and the evidence README (the audit's corrections: the like-for-like production numbers, the site count, where delegation can change an answer), `docs/GATES.md`, `START_HERE.md`. Core files of step 2: `protocol/spec/AppliedFunction.java` (the record `candidateFqns` becomes `referents`, every full name a call can mean), `compiler/NameResolver.java` (records the platform's names for every bare call), `compiler/ResolvedNames.java` (reads the record), `compiler/BareNames.java` (the resolver's and the table's only caller), `compiler/spec/Overloads.java` (the bare candidate path deleted), and the readers of the old field: `compiler/spec/CallShapes.java`, `SortChecker.java`, `StatementInline.java`, `builtin/TdsLegacy.java` (read only), `validation/ValidateDesugar.java`, `lineage/PkInference.java`, `parser/OperatorParts.java`; guards `IdentityGuardrailTest`, `ParkedWorkLedgerTest` (PARK-5's anchor goes with the fix). Not touched: `Compiler.java`'s entry points, the parser's grammar. Then D24's second slice. **Phase 3 (announced 2026-10-06): one table decides, by function id**, branch `build/phase3`. core:
   `compiler/spec/FunctionMatch.java` (new: legend-pure's overload ranking), `InferenceKernel.java`, `Overloads.java`;
   `compiler/element/FunctionCompiler.java` (candidates merged by function id; the PCT rule and the platform-owned list
   gone), `compiler/element/type/PlatformTypes.java` (the owned and assert-family lists gone), `Compiler.java`
   (`withoutPreludeShadows` by id), `builtin/Prelude.java`, `platform/ImplementationTable.java` and
   `Implementation.java` (a version of a function the platform implements with no row is refused),
   `compiler/StatementInline.java`; `builtin/Pure.java` and `native-membership.tsv` (27 versions get rows),
   `native-claims.tsv` regenerated, `lowering/Aggregates.java`; `compiler/ResolvedNames.java` and `platform/CoreFn.java`
   (a form by the names a call resolves to) with the 21 form-dispatch sites (`compiler/spec/` DeferredArgs,
   GraphFetchChecker, MatchChecker, Overloads, ProjectChecker, SortChecker, SourceSubst, TdsDesugars, Typer;
   `lineage/ScanRelations.java`, `normalizer/MappingNormalizer.java`), `builtin/TdsLegacy.java`,
   `compiler/spec/GroupByChecker.java`, `builtin/SystemMetamodel.java`, `builtin/NativeFn.java` (executeInDb's
   ConnectionStore version joins its family), and with the audit's fixes (2026-10-07) `compiler/spec/UserCallInliner.java`
   (a refused version reported as "no row for"). core tests: `InferenceKernelTest`, `CompileFunctionTest`,
   `PickByTableTest`, `DeclarationTableTest`, `IdentityGuardrailTest` (pins lowered, dated), `ParkedWorkLedgerTest`
   (PARK-5 to PARK-14 anchored), with `docs/PARKED_WORK_LEDGER.md`. spec:
   `ImplementationTableTest`, `SpecRatchets`, `DynaFnRegistryTest`, `SubsumedRegistryTest`, `ratchets.tsv`, the
   reference lane golden and its `reasons.tsv`. Also **Studio's** `datacube/src/generated/offer-facts.ts` (regenerated: one more offered
   function), `parser-equivalence`'s ratchets, `docs/GATES.md`, and `docs/EXECUTION_PLAN_2026_09_26.md`: W2.1's
   `ids`/`catalog` items now belong to this program (Phase 3), so nobody redoes them there; W1.1b stays with that plan
   (this program's Phase 3b was re-scoped on 2026-10-07 to the files Phases 4 and 6 load and what users meet).
2. **Studio** (`docs/STUDIO_FULL_PLAN_2026_10_04.md`; `studio-m1` landed as PR #24 on 2026-10-05): `studio/`,
   `legend-art/`, `query/`, `site/`, `engine-client/` (`datacube/` moved to the sixth line on 2026-10-09: the user),
   `sdlc-*`/`depot-*`; and the **protocol program** in core (`docs/PROTOCOL_PROGRAM_2026_10_05.md`: files in the fifth
   line's notes). **Resumed 2026-10-07 by session `neema-8f`**: see the fifth line's 2026-10-07 note.
3. **The database owner** (the fourth line below; `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md`; since
   2026-10-05 also the execution plan boundary, `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md`, which takes the rebuild's
   W6.1 and W6.2). Session "Plan Gen / Exec Split" (was `neema-20`), worktree `legend-lite-pgspike`. C3a–C3c, C1, C2a and C2b
   landed; the plan's step 1 (the plan records, `//core:execution_plan`) landed 2026-10-05; step 2's first piece,
   a connection's setup moved to the plan side (`//core:setup`), landed 2026-10-08 (`847b41df4`), and its landing 1 (the
   records, one parameter list) the same day (`f05fe7ada`). **E, the dialects write through one writer, landed
   2026-10-09** (`46fc131b8`; GATES "E"; run 37965399077; `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §10): every dialect writes all of its SQL into one `SqlWriter`, the
   legacy engine-text printer writes legend-engine's exact text, and PARK-16's product half (DDL and DML spell names as
   queries do). **Step 2's landing 2, the planner makes lite plans, landed 2026-10-09** (`682c6fedf`; GATES "Execution plan step 2,
   landing 2"; run 37988343620): `TypedQuery.executionPlan`, every parameter kind bound as a value where it is written
   (scalars, optionals, enumerations through value tables, lists as one array); what is not bound yet is refused by name
   (PARK-19, PARK-20, PARK-21). **Step 3, the runner, landed 2026-10-10** (`a2e879a8e`; GATES "Execution plan step 3";
   run 38070603189): `exec.PlanRunner` runs a lite plan with no model, its values typed and checked in its own words,
   each placeholder bound as the plan says (type holes where the type is the value's: H2's, and a date-time's on every
   database; PARK-19 fixed), sessions shared by the target's content; measured against legend-engine 4.145.0 and Pure
   itself (`probes/engine-reference/`). **Now: PARK-24, the output layer** (`docs/OUTPUT_LAYER_DESIGN_2026_10_10.md`; the user's ruling 2026-10-10: an answer
   is interpreted Pure's value, its text the served API's — a whole Float's `.0`, an extreme Float's plain digits, a
   decimal result's scale, a DateTime's text), then step 4 (the callers switch, the old paths deleted), both on branch
   `dbowner/output-layer`, landed together. Files: in `core/src/main/java/com/legend/` `lowering/` `Render.java`,
   `WireRender.java`, `LiteralSpelling.java`, `Fold.java`, `Scalars.java`, `MixedEncoding.java`; `sql/DateFmt.java`;
   `sql/dialect/` `AnsiSqlRenderer.java`, `DuckDb.java`, `H2.java`, `Postgres.java`; `exec/CanonicalForm.java`,
   `exec/PlanRunner.java`; `server/PureV1Api.java`; `Execution.java`; a new output-matrix test in `core/src/test/`, and
   `ParkedWorkLedgerTest.java`. DataCube's engine client (`parseExact`, `timestampFromText`) only if the new text needs
   it, asked of the DataCube line first.
4. **DataCube + Python** (resumed 2026-10-07 by the user; the sixth line below): worktree `legend-lite-dcsnap`, branch
   `datacube-m1`. `native/`, `python/` and, since 2026-10-09, all of `datacube/`. Landed: the compiler as a native
   library (`bc8107c4e`), `ll.show(df)` and the notebook cube on every platform, a real JupyterLab test and marimo
   (`169e0f062`), DataCube pages phase 1, the layout (`e397f107a`), and phase 2, a page of its own saved with every
   source (`eb2e38fa8`), phase 3, sheets and stacked tiles (`93e506e9b`), Python pages, `ll.Page` (`94ee44b08`), and
   **the KPI tile with the DataCube model design** (`f68b19aad`; GATES "DataCube: the KPI tile"). **Now: the model plan**
   (`docs/DATACUBE_MODEL_DESIGN_2026_10_10.md` §13, agreed with the user: prep makes classes, ModelJoins relate them,
   dashboards are built on them), its steps in order: 1 compile once everywhere; 2 Make a class; 3 dashboards on
   classes (the charts' batch 1 page filters land here); 4 combining in prep (every join form); 5 the model view; 6
   projects and publishing; 7 measures in the model.
   **Step 1 landed 2026-10-10 (fe7961b7b; run 38090412797; GATES "Compile once, plan many").** **Step 2's first
   landing, Create a Class…, landed 2026-10-11 (0cee56761; run 38109599876; GATES "Create a Class…")** -- no file
   touched beyond the announced list. **Step 2's second landing, Create an Association… and Create a Data Space…,
   landed 2026-10-11 (6e57d350c; run 38118975945; GATES "Create an Association…, Create a Data Space…")** -- beyond
   the announced list, in `core/`: the engine check's pins, `core/src/test/resources/upstream-api/model-writer/` (its
   recorder `docs/datacube-model-homework-2026-10-11/record-upstream.sh`); next in step 2: Python's create_class,
   create_association, create_data_space. **DataCube headless in Python landed 2026-10-11, ahead of those (the user's
   ruling; `docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md`; runs 38177320130 and 38178422811; GATES "DataCube
   headless in Python")** -- no `core/` file. Beyond this line's directories: comment-only edits in
   `wasm/src/main/java/planner/Boundary.java`, `wasm/README.md`, `tools/teavm/TeaVmCompile.java`,
   `query/src/backend/cube-planner.ts`, `engine-client/src/model-data.ts` (the rename, `WasmPlanner` to `LitePlanner`);
   `engine-client/src/arrow-ipc.ts` (new); `pure-protocol/test/lite.ts` and `twins.test.ts` (awaited); `gates/BUILD.bazel`
   (the warehouse lane's two tests); `tools/python/requirements.in`, its lock and `MODULE.bazel.lock` (mini-racer). The
   native entry points (`lite_plan_text_held`, `lite_relation_type_text_held`, `lite_test_data_sql`) are `native/`'s,
   over `Boundary`'s existing functions, reviewed by the Plan Gen / Exec Split line; the build by the Compiler Rewrite
   line. **Now (2026-10-11): DataCube as one app for every host** (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md`,
   agreed with the user: one app in `datacube/src/app/` over services -- the browser tab, Python's tab and widget, and
   Python headless each a set of services; Python's model home an SDLC, the browser's in Python's terms). **Steps 1 and
   2, and PlanAndRun (the design's §15), landed 2026-10-12 (36b72ee13, PR #31; its run 38323008283 green on all 52
   checks, main's own 38327366349 green; GATES "DataCube one app, steps 1 and 2 and PlanAndRun").** Step 3 (Python's
   model home: the SDLC and Depot from the same Java in the native library) touches `sdlc-server/`'s BUILD (the Studio
   line's, reviewed by it) as well as `datacube/`, `python/` and `native/`, and is announced here before it is built;
   step 5 (Studio and Query started from Python) is built with the Studio line. **The rest of the
   program is `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`** (agreed 2026-10-12: twin hosts, one implementation per store,
   one engine path, legend-lite as a drop-in legend-engine; its phase L taken by the Studio line). B1 (the model actions
   over services) and B2 (the page's settings kept by Python) are built on branch `datacube/one-app-3`. **B3 announced
   2026-10-12 (same branch): Python's model home, the SDLC and the Depot from the same Java in the native library.**
   Files: `sdlc-server/BUILD.bazel` (`GitStorage` its own target; `:rules` visible to `//native`),
   `sdlc-server/grammar/BUILD.bazel` and `depot-server/BUILD.bazel` (visible to `//native`) -- the Studio line's,
   reviewed by it, and by the Compiler Rewrite line as Bazel; `native/BUILD.bazel` (`:entry` gains them) and a new
   `native/src/main/java/com/legend/nativelib/ModelHome.java` (`lite_sdlc_handle`, `lite_depot_handle`: the rules over
   `GitStorage` in `ll.home()/projects`, as `SdlcPage` and `SdlcServer` wire them); `python/` (the engine serves
   `/sdlc/api` and `/depot/api` under its token and a lock held across processes; a page-sent model planned only when
   every table it declares is one Python serves); `datacube/` (Python's pages' model home service). No edit in
   `sdlc-server/src`, `depot-server/src` or `core/`. **B1 to B3 landed 2026-10-13 (8bd1eaf9e, PR #36; its run
   38342569828 green on all 51 checks after one re-run of a runner's filesystem error; GATES "The program's B1 to
   B3")** -- files touched beyond the announced list: `sdlc-client/BUILD.bazel` (`:conformance`, a js_library its own
   two tests now use too) and `gates/BUILD.bazel` (the new test's lane). Python's `create_class`, `create_association`
   and `create_data_space` are B4: twin TypeScript and Python libraries (the program's §5 B4; first shapes parked on
   branch `datacube/python-model-parked`). **The program is now a runbook** (rewritten 2026-10-13): every step's design,
   files, proof and reviews, and the order of the pull requests (§6), the next being G1 + G5. **The rest of the
   program is being built stacked on one branch, `datacube/python-parity`, pushed once with one CI run (the user,
   2026-10-13: "make sure you stack the PRs so can push everything together").** Built and reviewed there so far: G5 +
   G1 (Python parity), B4 (into a project: the new `model-changes/` package and `legend_lite/model.py`), D's first part
   (a grid's export one core). Files beyond this line's own so far: `MODULE.bazel` (the iceberg and avro pins),
   `core/BUILD.bazel` (`//core:server` visible to `//python`, one line), `tools/platforms/defs.bzl` (a comment),
   `gates/BUILD.bazel`, `sdlc-client/src/{client,project-text,index}.ts` and its conformance test (a 204 save answers no
   revision; `projectText` moved there -- the Studio line's, approved by it), each reviewed by its owner. **B5 announced
   2026-10-13 (same branch): saved queries on one implementation.** Files: `core/src/main/java/com/legend/server/
   SavedQueries.java` split from its storage -- the rules over a records interface (files on the server and in Python's
   native library, records the page passes in the tab), the rules and records a library of their own over `//json` and
   `//base` only, so they compile to WebAssembly; `core/BUILD.bazel` (that library); a new TeaVM module for the tab's
   query store; `query-store/src` (the store in the page answered by that module, `local-server.ts`'s port deleted --
   the Studio line's, reviewed by it); `native/` (`lite_saved_queries` over a folder, as `lite_sdlc_handle` is);
   `python/` (Python serves `/api/pure/v1/query` from `ll.home()/queries` under the folder's lock); `datacube/` and
   `query/demo/main.ts` (the store's factory). Reviews: the Studio line (query-store, query), the Compiler Rewrite line
   (the BUILD files, the TeaVM target), an audit; the acceptance test is the Studio line's L1 saved-query differential.
   B5 built and approved there (Studio, Compiler Rewrite, the audit's fixes). **B6 announced 2026-10-13 (same branch):
   DataCube's saved pages on one implementation.** Files: new `core/src/main/java/com/legend/queries/`
   `{SavedCubes,FileCubeRecords,MemoryCubeRecords}.java` (the rules of `datacube/src/cube-store.ts`'s `RuleStore`, moved
   to Java beside the saved queries' and in the same `//core:saved_queries`), `core/.../server/LegendHttpServer.java`
   (`/api/pure/v1/query/dataCube` answered from `--query-store DIR/dataCube`); `query-store/java/.../QueriesPage.java`
   (a `cubes` export on the same module), `query-store/src/wasm-server.ts` (its type), `query-store/test/lite-server.ts`
   (the server's start and stop out of `lite.test.ts`, for both suites) and `query-store/BUILD.bazel` (visibility to
   `//datacube`) -- the Studio line's; `native/` (`lite_saved_cubes`), `python/` (Python's dataCube routes over
   `ll.home()/pages`, `page.save()`), `datacube/` (the one client), `gates/BUILD.bazel` (the new test's lane). Reviews:
   the Studio line, the Compiler Rewrite line, an audit. B6 built and approved there (Studio, Compiler Rewrite, the
   audit's fixes). **PR 4 announced 2026-10-13 (same branch): C1 + C2 + C4, one engine path.** Files: C1 --
   `core/src/main/java/com/legend/server/PureV1Api.java` (`jsonPlan`, execute's plan half in the engine's JSON, beside
   `arrowPlan`, one planning step shared), `wasm/src/main/java/planner/{Boundary,TabExports}.java` (its export),
   `engine-client/src/legend/{browser-engine,wasm-grammar,planner-answer}.ts` (the tab's execute answered by that Java,
   its rows the tab's; return type and execution plan through the tab's pure/v1), the callers in `query/demo/main.ts`,
   `studio/src/app/app.ts` and `studio/test/engine.test.ts`; C2 -- one planner protocol in `engine-client` answered by
   the tab's worker and `python/legend_lite/_planner.py` (DataCube's, Query's and Studio's planners on it); C4 -- one
   table of the site's file types, one saved-query context reader (DataCube's and Query's). Reviews: the Plan Gen /
   Exec Split line (the boundary), the Compiler Rewrite line, the Studio line (Query, Studio), an audit. PR 4 built and
   approved there (files beyond the announcement: `//site:types`, a Java table of the site's media types the warehouse's
   server and Python read; `query-store/src/context.ts`; `native/` entries). **PR 6 announced 2026-10-13 (same branch;
   PR 5, C3, after it): G11 + E, Studio and Query from Python.** Files: G11 -- `sdlc-server/.../{GitStorage,SdlcServer}.java`
   (the repository's own lock, which the model home's server and Python's native library take; the Studio line's),
   `native/.../ModelHome.java`; E -- `python/` (Query's and Studio's sites served by Python's engine with a
   `config.json` each pointing at Python, `ll.query()`, `ll.studio()`, the token exchanged for a cookie on the first
   load, `/api/server/v1/currentUser` and `/api/lite/v1/*`), `python/BUILD.bazel` (the two sites in the wheel), and what
   Query's and Studio's clients need to carry it. Reviews: the Studio line (its apps' configuration), the Compiler
   Rewrite line (the wheel), an audit. PR 6 built and approved there (files beyond the announcement: `core/.../server/
   PureV1Api.java`'s refusal text and a `ParkedWorkLedgerTest` row, PARK-35: a project's queries run only in a tab).
   **PR 5 announced 2026-10-13 (same branch): C3, warehouse tables on Python's pages.** Files: `warehouse/` -- the
   one binding's JSON surface `sqlapi/SqlApiCalls.java` beside `NativeBinding` (which gains the key sign-in and the
   poll), `ApiJson`'s readers checked by kind, `CatalogColumn`'s structured fields, a page module
   `//warehouse:sqlapi_page`, a recorded corpus, `warehouse/BUILD.bazel`; `native/` (`lite_warehouse_call`,
   `lite_warehouse_read`); `engine-client/src/warehouse.ts` (the tab's driver over that binding, its hand spelling of
   the API gone) and `engine-client/src/snap.ts` (a host's snap beside the page's); the callers in
   `query/demo/main.ts` and `query/BUILD.bazel` (the module at `vendor/warehouse/`); `python/` (`legend_lite/warehouse.py`,
   the twin driver; Python signs in, writes and holds a table's model, runs its plans Live and snaps it into its
   DuckDB); `datacube/` (a table opened through its host). Reviews: the Compiler Rewrite line (the warehouse's and the
   native library's BUILD), the Studio line (Query, engine-client), an audit. **Pull requests 1 to 6 landed 2026-10-13**
   (one fast-forward of the stacked branch; its run 38398000209 green on all 50 jobs; GATES "The program's pull
   requests 1 to 6"). Files beyond the announcements: `datacube/demo/make-dist.mjs` (the page modules in DataCube's
   built folder), `engine-client/src/warehouse-session.ts`. **G4 and G12 landed 2026-10-13** (branch
   `datacube/python-parity-2`; its run 38416200845 green on all 50 jobs, every lane and platform; GATES "G4 and G12"):
   a picked file read by its encoding, and the tab's DuckDB extensions from the site with one start of it. G2 was
   dropped by the user (PARK-36). Files beyond the runbook's lists: `tools/browser/pinned-chromium.mjs` (the guard),
   `site/verify.mjs` (guarded by hand), `datacube/test/browser-guard.mjs` (its test). Reviews: the Compiler Rewrite
   line (MODULE.bazel, the BUILD files, the guard), the Studio line (Query's and Studio's BUILD, `duckdb-tab.ts`,
   `site/verify.mjs`), an audit. **C5 announced 2026-10-13 (branch `datacube/c5-project-data`; the runbook's C5, the
   user's design): a project's Data rows in a database of its own inside the host's database, named by what it holds
   and kept** (the tab's DuckDB, Python's, legend-lite's server). Core files: `setup/ProjectData.java` (new),
   `sql/dialect/HostOperations.java` and `DuckDbHostOperations.java` (new files, clear of the dialect program's
   `DuckDb.java`; its section, item 3), `compiler/ModelBuilder.java`, `compiler/element/ModelContext.java` and
   `PureModelContext.java` (`dataElements()`), `server/ConnectionResolver.java` (a held in-memory database made holding
   the model's Data rows, keyed by them, opened outside the store's lock), `server/PureV1Api.java` (a refusal's words),
   `core/BUILD.bazel` (`:setup` reaches `:cache`, `:protocol`; `:server_lib` reaches `:setup`),
   `tools/deps/core-layers.txt`, `ParkedWorkLedgerTest` (PARK-35's anchor gone: PARK-33 and PARK-35 closed); tests
   `sql/dialect/HostOperationsTest`, `server/ProjectRowsTest`. Beyond core: `wasm/` Boundary and TabExports (the
   planner-protocol kind `projectData`), `native/` (`lite_project_data`), engine-client, Query, Studio, DataCube,
   `python/`. Reviews: the Compiler Rewrite line (core), Plan Gen (the boundary), the Studio line (engine-client, Query,
   Studio), an audit. Then D5 (no SQL text in TypeScript or Python's host, claimed in the dialect program's section),
   stacked on it -- **announced 2026-10-13 there (item 3)**. **C5 and D5 landed 2026-10-13 (fbd216a3f; branch run
   38454642203, main's 38460744776, green; GATES "C5 and D5")** -- files beyond the announced lists: `gates/BUILD.bazel`
   (the lanes), `tools/guards/BUILD.bazel`, `python/BUILD.bazel`, `engine-client/BUILD.bazel`, `datacube/BUILD.bazel`
   (the projects' copy, the trading copies, the new tests), `query/BUILD.bazel`, `datacube/test_imports.bzl`. Then D5b
   (the dialect program's section, item 3); then F. **Step 2's second landing announced 2026-10-11 (rule 5; branch `datacube-m3`; the design's §15.6a,
   agreed with the user): a cross-area edit in `core/`** -- the route renamed and two more beside it,
   `/api/lite/v1/model/createClass` (was `/api/lite/v1/relational/promoteRelationToModel`), `createAssociation`,
   `createDataSpace`, one atomic writer under them. Core files: `core/src/main/java/com/legend/server/`
   `ModelChanges.java` (new: the writer -- actions applied in order, checked once, the net changes),
   `CreateClass.java` (`PromoteRelationToModel.java` renamed and split), `CreateAssociation.java` and
   `CreateDataSpace.java` (new), `PureV1Api.java` (the three cases), `core/BUILD.bazel` (`pure_v1`'s srcs, `server_lib`'s
   excludes), `core/src/test/.../JavaEvalLedgerTest.java` (the server register's rows), their tests under
   `core/src/test/java/com/legend/server/`, parser-equivalence's `ratchets.tsv`. Then `datacube/` (the windows; Create a
   Class… on every plane) and `site/verify.mjs` (the Studio line's file, its change reviewed by it). **Step 2, Create a Class… (the design's §15, agreed with the user; branch `datacube-m2`), announced
   2026-10-10 (rule 5): a cross-area edit in `core/`** -- legend-lite's route `/api/lite/v1/relational/promoteRelationToModel` (§15.6, agreed with the
   Studio line: a relation made a class of the model, its files answered in the SDLC's change shape). Core files:
   `core/src/main/java/com/legend/server/PromoteRelationToModel.java` (new: the writer, printed by `ModelComposer`),
   `server/PureV1Api.java` (one route case and its method), `core/BUILD.bazel` (`pure_v1`'s srcs gain the writer, its
   deps `:compiler`; `server_lib` excludes it), `tools/deps/core-layers.txt` (`pure_v1` reaches `compiler`, dated),
   `core/src/test/.../JavaEvalLedgerTest.java` (the server register's row), the test
   `core/src/test/java/com/legend/server/PromoteRelationToModelTest.java`, parser-equivalence's `ratchets.tsv` (the
   test's snippets). Reviewed by the Compiler Rewrite line (the BUILD lines) and Plan Gen / Exec Split (the planner's
   route call). The rest is this line's: `datacube/` (the window, the page's project in the model home, page document
   version 4; `datacube/BUILD.bazel`'s `:src_base` gains `//sdlc-client:sdlc_client`) and `python/legend_lite/page.py`
   (version 4). **Step 1 as announced (rule 5): a cross-area edit in `core/`, amended before landing** -- compile once, plan
   many (the design's §9, reviewed by the Compiler Rewrite and Plan Gen / Exec Split lines: a held `PureModelContext` is
   safe, one thread at a time). Core files: `core/src/main/java/com/legend/cache/HeldStore.java` (new: a generic store of
   values held by their users, keyed by a content `Hash` -- Invariant 3 -- each hold its own handle, a value built
   outside the store's lock; no BUILD line: `//core:cache` globs its package), `core/src/main/java/com/legend/HeldModels.java`
   (new: a host's held models over it, an instance, no static state in core) and `core/BUILD.bazel` (`_PLANNER_SRCS`
   gains it); tests `core/src/test/java/com/legend/cache/HeldStoreTest.java` and `core/src/test/java/com/legend/HeldModelsTest.java`.
   Beside core: `wasm/src/main/java/planner/Boundary.java` (owns the module's instance) and `TabExports.java`;
   `native/src/main/java/com/legend/nativelib/Compiler.java` (Python's held entries); `python/legend_lite/` (`HeldModel`,
   frames holding their model); `datacube/src/wasm-planner.ts`, `planner-worker.ts`, `cube.ts` (a planner's `dispose`)
   and `datacube/demo/boot.ts` (a grid's planners let go with its last grid). Not touched: `Compiler.java`, L7 step 3's
   files. **The server holds no models** (the Studio line: its leg 8 compiles once per request); `pure/v1` reusing a held
   model is a later change, on a measurement.

**Parked:** the compiler rebuild (`docs/EXECUTION_PLAN_2026_09_26.md`; paused, coming back later — its open items C4,
B2/B3 and the W6.2 runner wait for it); the server
program (`docs/SERVER_PROGRAM_2026_09_26.md`); NLQ (the untracked `nlq/` directory is not ours; leave it).

**Shared machine:** at most two heavy Bazel/JVM jobs at once; before a full `bazel test //...`, check that another
session is not running one (a browser test, `//studio:verify_test`, times out under two full chains).

## A second line, 2026-09-29: DataCube against the warehouse (the user's ask, rule 5)

In the worktree `legend-lite-dcsnap`, branch `datacube-live-snap`. **Owns:** `datacube/`. **Touches `warehouse/`** (one
line each, announced here before landing):
- `server/Identity.java`, `server/WarehouseServer.java`: `POST /sql/v1/token/refresh` (a valid token for a fresh one,
  never past the sign-in's session limit, 12h default); tokens carry their sign-in time (`principal|expiry|signedInAt`);
  `--token-key-file` (a key kept across restarts), `--token-minutes`, `--session-hours`; `Config.sessionLimit`.
- `server/Statements.java` (+ `Identity.hasUser`): a GRANT must name an object (or schema) that exists and a
  grantee (or role member) who is a user or role; REVOKE stays open. Test:
  `WarehouseEntitlementsTest.aGrantNamesSomethingThatIsThereForSomeoneWhoIs`.
- `sqlapi/SqlApiBinding.java`, `sqlapi/NativeBinding.java`: `refresh(token)`. Tests: `IdentityTest` (new),
  `WarehouseServerTest.aValidTokenRefreshesAndTheFreshOneWorks`.

**Touches CI** (`.github/workflows/gates-run.yml`, `gate.yml`): a `browser` lane, Linux only --
`//datacube:live_snap_test` and the `browser-ci` harnesses; the other lanes unchanged. `docs/GATES.md`: its row.

Nothing in `core/`, `spec/`, `tools/`. Runs a warehouse (`:9090`) and the DataCube site (`:8000`), idle: stop them before
a timing (rule 4).

## A third line, 2026-09-30: DataCube track A and the BI plan (the user's ask, rule 5)

`docs/BI_AND_ETL_PLAN_2026_09_29.md`; the user: "start track A now, and go in the suggested order for everything that does
not need any server side work". **Owns:** `datacube/` (and its CI lane). No edit in `core/`, `spec/`, `tools/` or
`warehouse/`; the plan's server-side items (F1 write, F2's resolver, F4, F5, unpivot) wait for the rebuild's say (§8).

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "announce it and do
it"). So a file opened in DataCube's tab has its model written WITHOUT the WebAssembly module, on any planner (legend-engine
included), and the writer cannot drift from the compiler's:
- STRUCTURED, no type string parsed (the user: "do the homework first of how to do this CORRECTLY in structured form"):
  `DuckDb.CATALOG_COLUMNS_SQL` reads a table's columns from `duckdb_columns()` joined on the type id to
  `duckdb_types()` -- each column's canonical type, and a DECIMAL's precision and scale as numbers. `catalogType` is
  data over that (`CATALOG_TYPES`, `CATALOG_ALIASES` for JSON, `CATALOG_REFUSED` with reasons); the regexes are gone.
  `CatalogModel.Column` CHANGED: `(name, dataType, logicalType, precision, scale)`. `CatalogModelTest` reads real
  DuckDB tables, and a completeness test fails on a canonical type with no decision.
- Callers moved with it: `wasm/.../Wasm.java` `databaseFromCatalogOrError` takes the structured column; the warehouse's
  `/sql/v1/catalogs/{c}/objects` adds `logicalType`, `precision`, `scale` to each column (`type` kept).
- `datacube/tools/catalogfacts/` generates `datacube/src/generated/catalog-facts.ts` (the tables and the catalog
  question) and `datacube/test/generated/catalog-corpus.ts` (what `CatalogModel.database` answers for real DuckDB
  tables); `datacube/src/catalog-model.ts` is DataCube's writer, tested against the corpus case for case.
  `WasmPlanner.databaseFromCatalog` is kept (its input is now the structured column); DataCube no longer calls it.
  (2026-10-08: the generators, `catalog-model.ts` and `WasmPlanner.databaseFromCatalog` are deleted; DataCube calls
  the compiler's writer, `tableModelOrError` -- the sixth line.)

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "i think we should fix calc
column"). legend-engine refused 13 DataCube features that legend-lite passes; one cause is legend-lite's own leniency:
- `core/.../compiler/spec/Typer.java` `collection`: a literal of more than one value requires each element to be exactly
  `[1]`, as legend-engine (`ValueSpecificationBuilder.visit(Collection)`) and legend-pure (`InstanceValueValidator`) do.
  `$x.notional * 1.1` over a nullable column is then refused, as on engine (write `->toOne()`). This reverts the
  2026-09-11 loosening (`MULTIPLICITY_AUDIT_2026_08_20.md` §4a), whose premise "real pure accepts it" legend-pure's
  source contradicts; `MultiplicityStrictnessTest` flips back to expecting the refusal.
- NOT changed in `core/` (the user: engine's gaps are compensated in DataCube and recorded, not "fixed" in legend-lite):
  `over(String[*], SortInfo[*], Frame[0..1])` (in engine's over.pure, never registered by its Handlers.java) and BIT
  typed Boolean (engine's RelationalCompilerExtension says TinyInt; legend-pure says Boolean). Both get register rows.

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "do them"). A column DuckDB's
catalog says is NOT NULL is declared `NOT NULL` in the written Database, so legend-lite and legend-engine type it `[1]`
and `$x.n * 1.1` compiles over it without `->toOne()`:
- `DuckDb.CATALOG_COLUMNS_SQL` reads `duckdb_columns().is_nullable`; `CatalogModel.Column` gains `notNull`; a line is
  `name TYPE NOT NULL` for such a column. Callers move with it: `Wasm.databaseFromCatalogOrError`, the warehouse's
  catalog listing (adds `notNull`), DataCube's generated writer and corpus.

## A fourth line, 2026-09-30: the Query app (the user's ask; design `docs/QUERY_APP_DESIGN_2026_09_30.md`)

**STATUS 2026-10-01: v1 LANDED on main** (the branch `query/app`, fast-forwarded after the full gate). Everything
below is on main; the line continues on the same branch -- next, upstream's look and layout, then the gaps in the
design doc's v1 status. A cross-area edit from here on is announced here first, as before.

In the worktree `legend-lite-query`, branch `query/app`. **Owns:** `query/` (new). **Touches, one line each:**
- `fixtures/saved-queries/` (new, 2026-10-01, asked by the DataCube line): saved queries exactly as
  `GET /api/pure/v1/query/{id}` answers -- explicit context, data-space context, defaultParameterValues, a graph
  fetch (not a cube source) -- made through the real API and each run on the demo model (`make.mjs`); README.md
  says how to read one. `js_library` `//fixtures/saved-queries:records`, visible to `//query` and `//datacube`:
  Query tests its reading against it (`query/test/saved-queries.test.ts`, through `persist.ts contextOf`); DataCube's
  saved-query source will test against the same files.
- `.github/workflows/gates-run.yml` (2026-10-01): the browser lane runs every `browser-ci` target in `//query` as
  well as `//datacube` -- `//query:verify` (the end-to-end steps, both planes), its server on Bazel's JDK.
- `core/src/main/java/com/legend/server/PureV1Api.java` + `LegendHttpServer.java` routes (+ `PureV1ApiTest`):
  the upstream `pure/v1` endpoints the Query app needs and lite lacks, each in legend-engine's shape, measured
  against 4.145.0 -- `execute` with `parameterValues`, `compilation/compile`, `compilation/lambdaReturnType`,
  graphFetch results, the query store. Each routes to existing lite functions; no new compiler logic.
- `core/src/main/java/com/legend/Compiler.java`: `executeWire(model, ValueSpecification, ...)` gains the graph-fetch
  branch its text twin already has (one `if`, no compiler logic) -- `execute` answers graphFetch as the engine does.
- `core/src/main/java/com/legend/server/SavedQueries.java` (new): the engine's query store, in a directory the
  server is started with (`--query-store DIR`).
- NOT on this line (2026-09-30, the user's call): `analytics/mapping/modelCoverage` and `analytics/dataSpace/render`
  are engine ANALYSES lite does not have -- new platform features, proposed for core (design doc §1, G6/G9), not
  added here. The Query app shows every property and lists a mapping's classes as the mapping declares them.
- Graph-fetch trees: ONE representation (2026-09-30, crosses parser/protocol/compiler -- announced here).
  `GraphFetchLiteral` is the tree only (class, property nodes, root subtype entries); the column-list desugaring
  it carried is gone. The parser reads a tree with its grammar only (the `IslandScan` character scanner's graph
  half is deleted; the tree re-lexes its slice at its real line/column for wire spans). `ProtocolReader` gains
  `rootGraphFetchTree` (the two print-and-parse workarounds in PureV1Api and Wasm are gone). `NameResolver` keeps
  the tree (class names resolve in place; call arguments are its children). `GraphFetchChecker`, `IsDistinctChecker`
  and lineage `ScanRelations` walk the tree; everything after the checker (`TypedGraphTree`) is unchanged.
  Graph-position argument spellings live only in `ProtocolEmitter.gftParam` and `ProtocolReader.graphArg`.
  Evidence: `//spec:corpus_duckdb` + `:corpus_h2` per-test pass/fail lists and judge ledgers identical to the
  pre-change baseline (trace ids masked); `//parser-equivalence:diagnostics`, `//core:core_tests` (4295),
  `//core:guardrails` (no pin raised), `//query:verify` green. Fixes W1.2's `graphFetchKeepsSubTypeTrees` known
  defect. Unchanged gaps, noted: a tree typed on its own still refuses (the engine's type is
  `RootGraphFetchTree<T>`); lineage does not trace inside subtype views; `prop()` vs `prop` is not on the wire.
- `wasm/src/main/java/planner/Wasm.java`: `modelJsonOrError` (E2's twin, byte-identical to the server's).
- `datacube/BUILD.bazel`: ONE line, `visibility = ["//query:__pkg__"]` on `:src` (2026-09-30), so the Query app runs
  its planned SQL on DataCube's engines (`engine.ts`, `duckdb.ts`, `warehouse.ts`). And (2026-10-01, user-approved) a
  `styles` filegroup (`src/**/*.css`, visible to `//query`): Query's results grid is a `CubeApp` over the query, so
  its page loads DataCube's stylesheets. Query uses `CubeApp`, `Planner`, `sourceColumns`,
  `RemoteRun`/`LegendEngineExecutor`, `relationColumns` as they are.
- `datacube/src` (2026-10-01, the user's ask, cleared with the DataCube line): a DataCube-owned "controls hidden" mode.
  `CubeApp` option `controlsHidden` + `setControlsHidden()`/`controlsHidden`; one root class
  (`dc-controls-hidden`, `app.css`) hides the title bar, drag zones, columns panel and status bar; the way back is the
  right-click menu's LAST entry, `view.controls` "Hide Controls"/"Show Controls" (`ui/menu.ts`; the order before it
  untouched; the user: "right click only"). View state: no query, no undo step, not saved. Tests: `menu.test.ts`,
  `app.test.ts`. Query opens its embedded cube with it.
- `datacube/src` (2026-10-01, the user's ask, cleared with the DataCube line): a DARK THEME, opt-in by the page --
  `<html data-dc-theme="dark">` (Query sets it from its own light/dark switch and the system setting). `theme.css`: the
  same tokens redefined under `:root[data-dc-theme='dark']` (palette roles kept: --tw-white the surface, --tw-black
  the ink, the neutral ramp reversed, tints dark; `color-scheme: dark`) and the chart panel's `--dc-chart-*`.
  `grid/screen-colours.ts` `onScreen`, at the grid's two paint paths only: a configured colour equal to its DEFAULT
  becomes `var(--dc-theme-*, <default>)`; a chosen colour is painted as chosen; exports never pass through it (they
  stay light). The last hard-coded colours became token references with their old value as fallback (grid.css total
  row, band, red-50 tint; app.css error ink). PROOF: 853 DataCube elements' computed colours (grid, menu, Properties)
  identical before/after in light. Tests: `screen-colours.test.ts`; `typed-values.ts` reads a cell's painted colour
  (jsdom does not resolve `var()`). Known gap: a heatmap's default low end (#ffffff) is interpolated in TS, still white.
- `MODULE.bazel`: a second `npm_translate_lock` (`npm_query`, `//query:pnpm-lock.yaml`), so `datacube/`'s lock
  is not touched.

Nothing in `datacube/`, the compiler packages, `warehouse/`.

**Found for the compiler's owners:** (1) FIXED on this line 2026-10-01 (the user: "def fix the lambda bug"):
`Compiler.resultType` on a lambda WITH parameters typed the lambda itself (`LambdaFunction<{Integer[1] -> Relation<...>}>`),
so `lambdaRelationType`/`lambdaReturnType` answered a function type where legend-engine types the result with the
parameters in scope. `SpecCompiler.typeQueryBody` now binds a lambda's declared parameters (`Typer.declaredType`, shared
with the annotated-lambda fold) and types the body; undeclared parameters stay refused. Test: `PureV1ApiTest`
`e5e6_aQueryWithParameters_typesAsItsBody`; the relational corpus (duckdb, h2) per-test lists identical with and
without the change. (2) NOT fixed here: a derived property `{$this.quantity *
$this.price}: Float[1]` (Integer * Float) compiles in lite; legend-engine refuses it ("'Number' is not a subtype of
'Float'").

**2026-10-01, the DataCube line: several sources on a page, and the source picker** (the user: "New is fine; Picker
should offer everything"). In `datacube/` (the Query line also edits it; ping before landing either way):
`src/app.ts`, `src/ui/menu.ts` (Insert becomes **New ▸ Source… / Grid / Chart**), `src/page/cube-page.ts` (a grid over
another source), `src/planner.ts` + `src/wasm-planner.ts` (`withModel`: a planner over another model sharing the same
worker or server), a new `src/ui/source-picker.ts` and `src/saved-queries.ts` (upstream `/pure/v1/query`, read only),
`demo/boot.ts` (the Data window becomes the picker). Nothing in `core/`.

**2026-10-01, the DataCube line: a saved query as a source** (the user: "lets work on saved queries now"). One line in
`query/`: `query/BUILD.bazel` exports `demo/models/*` to `//datacube` (read only), so DataCube's site serves the trading
project a saved query compiles against -- the same files, not a copy that could drift. In `datacube/`: `src/saved-queries.ts`,
`src/planner.ts`, `src/wasm-planner.ts`, `src/pure-v1.ts`, `src/planner-worker.ts` (`ModelOptions`: a mapping, read as
`->from(mapping, runtime)` outermost as Query runs it; the model's enumerations; `modelElements`), `src/relation-type.ts`,
`demo/boot.ts`, `demo/page-config.ts`, `demo/config.json`. Reads `fixtures/saved-queries/`, writes nothing there.

**2026-10-01, the DataCube and Query lines: one query store, in a package of its own** (the user: "the right
architecture", "i really do want the private browser sharing mode with no server", "a share link that encodes everything
you need about a Saved Query"). A new top-level package `query-store/`: upstream's `Query` records, ONE typed client
(`QueryStoreClient`, read-only `QueryReader` for DataCube) over a `fetch`, and an IN-PAGE server -- upstream's
`/api/pure/v1/query` answered from IndexedDB, ported from legend-lite's `SavedQueries.java` -- that the client is handed
in place of the network when no server is configured. One wire-level suite runs against both: the in-page server and
legend-lite (`//core:server`, `--query-store`). And a saved query's share link (`#q1.`). **Moves out of `query/`**:
`src/backend/local-store.ts` and the `Query` types of `src/backend/wire.ts` (re-exported there); `HttpEngine`'s store
calls go through the client; `query/BUILD.bazel`, `query/tsconfig.json`, `query/demo/main.ts`. In `datacube/`:
`src/saved-queries.ts` loses its own client. Then one site serving both apps from one origin, so the browser store is
shared. Nothing in `core/`.

## A fourth line, 2026-10-03: one owner for every database decision (the user's ask, rule 5)

`docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md` (homework done; its §4 the steps). The user: "make sure
there is really only one owner and one single place through the whole code that makes this decision". **This line
does ONLY B1 and C3** of that plan; everything that lives in the rebuild's areas is handed to the execution plan (see
below), not done here.

**Announced cross-area edits (each lands with the full chain):**
- B1, the guards: `core/BUILD.bazel` (`//core:guardrails` also loads `:duckdb_load`, so `exec/DuckDbAppenderLoad` is
  checked), `core/src/test/java/com/legend/ArchitectureTest.java` (`CORE_PROD_CLASSES` = exactly the product: test
  support `com.legend.testing..`/`com.legend.tools..` out; F1.3b's text says it measures JDBC API use).
- C3, the database registry: a new `com.legend.database` (one entry per `DatabaseType`, data and plan-side objects
  only, no `java.sql`), and its consumers, edited only where they decide by database: `Compiler.dialectFor` /
  `executesOn`; `StatementExecutor`'s engine-text renderer selection (`toSQLString`, `planDialect`, `H2_DDL`,
  `ENGINE_TEXT`) and `PlanAllocations`; `plan/InProtocol`; `SqlTextVerdicts`; `exec/SystemDatabase`;
  `exec/JdbcMetadata`; `server/ConnectionResolver`; `test/StorelessRuntime`; `sql/dialect/AnsiSqlRenderer` and its
  subclasses (the `jdbcProduct` constructor argument leaves); `DialectBoundaryTest`; `tools/deps/core-layers.txt`.
  C3a landed 2026-10-03: `//core:database` (`core/BUILD.bazel`, `tools/deps/BUILD.bazel`), `PlanEnvelope`,
  `ArchitectureTest`'s library map, and the `dialectFor` callers `wasm/.../Wasm.java` and
  `datacube/tools/catalogfacts/CatalogFacts.java` (now `Databases.dialect`; that generator is deleted 2026-10-08).
  C3b announced 2026-10-03 (plan doc §4 C3b and its audit): the connecting side's one owner, a new `exec/Sessions`
  (`exec/JdbcMetadata` deleted into it); `Compiler` (`executesOn` returns the target, `dialectOf`'s session check, new
  execute/executeWire/executeStreaming entries taking a connection opener, `NO_RUNTIME` replaced); `CrossStoreGuard`
  (moved beside `executesOn`, silent returns deleted); `StatementExecutor` (its `CrossStoreGuard` and `SystemDatabase`
  calls only); `exec/SystemDatabase`; `server/ConnectionResolver` (`resolve` deleted; becomes the opener) and
  `server/QueryService`; `compiler/element/ModelContext` (`isModelConnection` default; a `databases()` view);
  `sql/dialect/SqlDialect` + `AnsiSqlRenderer` and subclasses (`jdbcProduct` deleted); `test/StorelessRuntime`;
  `pct/.../PctBackend`; `docs/SEMANTICS_REGISTER.md` S27; the tests that pin `NO_RUNTIME` and the server's resolver.
  Then (user, 2026-10-04) C1 and C2 on this line: `compiler/spec/StatementEffects` (new; the effect scan from
  `StatementExecutor` and `Compiler`), a new `//core:planner` library (the compile-once API replacing `Compiler`'s
  plan statics), a `com.legend.Execution` front door, and every `Compiler.*` caller switched (core, server, wasm, pct,
  spec, datacube tools); `core/BUILD.bazel`, `tools/deps`, `ArchitectureTest`, AGENTS.md's entry-point table.
- **2026-10-07, announced before the first edit: the execution plan, phase 1 steps 2–4**
  (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §9; the user's question: can lite run legend-engine's plans). What it
  does: the planner produces ONE plan (SQL rendered at plan time with typed bind slots, each statement's connection,
  its setup statements and in-memory identity); a new runner in `exec` validates the caller's parameter values against
  the plan, binds, runs and streams — with no compiled model. Then the server's execute paths run plans, and what they
  replace is deleted. Each step lands alone with the full chain; no behaviour change (the same answers on DuckDB, H2
  and Postgres, checked by a differential test until step 4). Files:
  - step 2: `TypedQuery.java` and the planner (a new `executionPlan`), `lowering/PlanParams`, `lowering/WireRender`,
    `sql/dialect/AnsiSqlRenderer` and its subclasses (`SqlExpr.PlanParam` rendered as a bind placeholder),
    `StatementExecutor` (its plan-text parameter builder moves to the one shared function), `setup/CsvSeed`
    (setup rendered at plan time; moved out of exec 2026-10-08, the running half `exec/SetupRunner`), `server/ConnectionResolver` (the in-memory identity computed planner-side),
    `executionplan/*`; `core/BUILD.bazel` (`:planner` depends on `:execution_plan`) and `tools/deps/core-layers.txt`
    — Bazel edits shown to the Bazel program session before landing;
  - step 3: `exec/PlanRunner` (new), `exec/Sessions` (`Source` no longer takes the model), a shrink-only guard on what
    `exec` reads from planning libraries (`ArchitectureTest`);
  - step 4: `server/PureV1Api` (`pure/v1/execution/execute` only: plan once, run with `parameterValues`;
    `boundParameters` deleted), `Execution` (`wireOn`/`streamOn` deleted), `server/QueryService`,
    `server/ConnectionResolver`, and their tests (`PureV1ApiTest`, `PureV1HttpTest`, the server and streaming tests).
  **Overlaps:** `PureV1Api.java` with the protocol program (its `grammar/*` routes; different methods — whoever
  lands second merges); `TypedQuery`/`Compiler` with the compiler design (`docs/COMPILER_RIGHT_DESIGN_2026_10_07.md`,
  no code yet — this work adds a method beside `plan`, it does not change typing or naming) and with the rebuild's
  Phase 3 reference branch (`Compiler.withoutPreludeShadows`, not touched here). Not touched: `generatePlan` (its
  legend-engine shape waits for the compatibility mode, phase 4), judging and tests (Studio's A4 stays parked).

**Handed to the rebuild (not touched here):** the effect scan's swallowed compile errors (`StatementExecutor
.containsEffect`; D10's demand-driven rule), the `ExecutionContext` reader's `"H2"` defaults (`ContextReading`), the
missing platform functions (W2.1 / W4.4a, decision D9), the plan/execution split (W6.2's first cut) — each with its
measurements in the plan document above.

## A fifth line, 2026-10-03: Studio, SDLC-lite and Depot-lite (the user's ask, rule 5)

On the remote branch `studio` (worktree `legend-lite-query`); everything lands there first, only this announcement on
`main`. Design `docs/STUDIO_DESIGN_2026_10_02.md` and census `studio/docs/` (both on `studio`). **Owns, all new
top-level modules:** `studio/` (the app), `sdlc-client/` and `depot-client/` (TypeScript clients), `sdlc-server/` and
`depot-server/` (Java: the model home's rules, written once and run both as servers and compiled to WebAssembly for
the page; they depend on `//core`'s public API, never the reverse), and the dogfood model.

**Spike landed (2026-10-04, on `studio`):** the SDLC's rules in Java compile to WebAssembly by `sdlc-server/`'s own
`teavm_wasm` target and pass the same conformance suite as the server over a real git repository. No edit in `core/`,
`json/` or `tools/`.

**2026-10-04, announced before landing: one edit in `wasm/`** for Studio's in-tab compile.
`wasm/src/main/java/planner/Wasm.java` gains one export, `compileOrError(model)`: exactly what the server's
`compilation/compile` does (`Compiler.compileModel` then `Compiler.compileAllBodies`), answered as `"OK\n" + [every
body error]` or the folded refusal, like the module's other exports. Nothing else in `wasm/` changes.

**2026-10-04, announced before landing: a cross-area edit in `datacube/` and `query/`** (the user: "let's also pull
out stuff that query is using from datacube"; "should we call it engine-client"). No behaviour changes, moves only:
- **New `engine-client/`** (TypeScript): where a query runs, shared by DataCube, Query and Studio. MOVED from
  `datacube/src/`: `engine.ts`, `relation-type.ts`, `result.ts`, `receipt.ts`, `values.ts`, `types.ts`, `pure-v1.ts`,
  `duckdb.ts`, `warehouse.ts`, `engine-remote.ts` (and whatever they alone import). Every importer in `datacube/`
  (about 50 files, one line each) and `query/` points at the new paths; BUILD and typecheck targets follow.
- **New `datacube/src/embed.ts`**: DataCube's one public entry for an app embedding the cube (`CubeApp`, `CubeView`,
  `RemoteRun`, the snapshot type, `sourceColumns`); Query imports only from it, no longer from DataCube's internals.
- Later in the same line: legend-art's tokens and the icon generator shared by Query, Studio and DataCube.
Gated by DataCube's own suites (its tests, `//wasm:all`, the browser lane) and Query's. A DataCube session working
on `datacube/` should rebase after this lands.

**2026-10-04, announced before landing: four one-line edits for the Bazel program's guards** (after merging `main`'s
P1-20/P1-25 into `studio`): `//sdlc-server` and `//depot-server` join the visibility lists of `//base:base`,
`//json:json` and (`//sdlc-server` only) `//core:core`; `tools/deps/pools.bzl` lets `sdlc-server` use `@maven_teavm`
(its page module, built like `//wasm:planner`), with the reason. The two modules use `legend_java_library`.
And `//engine-client` joins `//core:core`'s visibility: DataCube's type-facts generator (it runs legend-lite) moved
there with the type reader.

The compiler rebuild is paused by the user while this line works (design S11). Core pieces it will need later, each
announced here with its files before it lands: the model JSON reader and printer (opening existing upstream projects),
W1.2 diagnostics, and (v1) name-resolved entity JSON with the function-resolution fix
(`docs/function-resolution/README.md` on `studio`).

**2026-10-05, announced before landing: the reference-lane fix in `core/`** (the user: "fix rule to match pure for
sure"). The rule `c1f9bac5b` added (a list literal's elements are `[1]`) is legend-pure's and stays. The lane's two
new failures (`dataTypeToSqlTextH2`, `max([$MIN, min([$v.size, $MAX])])`) come from overload choice: legend-pure picks
`math::min(Integer[1..*]):Integer[1]`, which our catalog lacks, so we pick `min(Integer[*]):Integer[0..1]`. The fix adds
the six `[1..*]` overloads legend-engine registers (`math::min`/`max` over `Integer`, `Float`, `Number`) to
`native-membership.tsv` and `Pure.java` (their text and the `AT_MATH_MIN`/`AT_MATH_MAX` groups regenerated); lowering
already dispatches on the groups. Gate: the full chain plus `//spec:reference_lane` back at its golden.

**2026-10-05, announced before the first edit: the Studio plan's core work and the Snap move** (the user: "do the whole
thing on a branch"; `docs/STUDIO_FULL_PLAN_2026_10_04.md` A4, B1, A6). All of it on the branch `studio-engine`, landing
through its PR after `studio-m1` and `query-by-name`; nothing on `main` but this note.
- **A4, tests (the engine's testable framework)**, from `docs/DEFERRED_TEST_EXECUTION.md`. Files:
  `core/src/main/java/com/legend/test/` (the existing `ServiceTestRunner`/`TestAssertions` extended to mapping and
  function suites, and split into a planner half -- each test's query, data and assertion -- that runs without JDBC, so
  the tab can execute it on DuckDB, and the judgment, written once); the typed suite records in
  `core/src/main/java/com/legend/model/` (`MappingDefinition`, `ServiceDefinition`, `MappingFromProtocol`: the raw
  `testSuitesSource` replaced as the charter says); `core/src/main/java/com/legend/server/PureV1Api.java` and
  `LegendHttpServer.java` (`testable/runTests`); `wasm/src/main/java/planner/Wasm.java` (exports for the tab's runner).
- **B1, the model round trip**: `core/src/main/java/com/legend/protocol/` (a model composer beside `PureComposer`:
  model JSON to Pure text, element by element, upstream's grammar composer as the spec), `PureV1Api.java`
  (`grammar/jsonToGrammar/model`), `Wasm.java` (its export).
- **A6, Snap for every app**: `datacube/src/` (Snap's engine half -- freezing rows into the tab's DuckDB, the stamp,
  the refusals, receipts -- moved to `engine-client/`; DataCube keeps its cube behaviour on top) and DataCube's Snap
  tests, which move with it. Gated by DataCube's suites and browser lane.
Shared with the database owner's C2 (the execution front door): `core/.../test/` runs suites through
`Compiler.executeResolved`; whoever lands second merges.

**2026-10-05, later: A4 PARKED; B1 becomes the protocol program** (the user). **A4 (user tests) is parked** behind the
round trip as its own design program (tab and server must give the same verdict; judging's home is the database
owner's open question): nothing in `core/.../test/`, `com.legend.model` suite records, `testable/runTests` or `Wasm.java`
test exports is being changed; the provisional work sits unmerged on `studio-tests-parked`. **B1 is now the protocol
program** (`docs/PROTOCOL_PROGRAM_2026_10_05.md`, branch `protocol`): parse, emit, read and compose all on the typed
protocol records. Files: `core/src/main/java/com/legend/protocol/` (a model reader beside `ProtocolReader`, the
composers -- `PureComposer` and the element composers -- moved from JSON onto records, one public face),
`PureV1Api.java`'s `grammar/*` routes, `Wasm.java`'s grammar exports, and the oracles in `parser-equivalence/`. A6
proceeds as announced. **Correction, the reader leg (on `protocol`, not `main`):** the parser and the emitter changed
too, mechanically -- every `SourceInfo` in `Protocol`'s records is nullable (JSON without spans reads back), the
emitter omits an absent span as the engine's `NON_NULL` does, `PmcdParser.parseModel` returns the records
(`parseDocument` is its emit), and two shape flags (`EnumValue`, a connection's `element`) replace span-presence tests.
Gate 8 holds the emitter byte-identical with the engine through it. A session editing `Protocol.java`, the emitters or
the protocol parsers should expect to merge with `protocol` when it lands.

**2026-10-07: the Studio line resumes (session `neema-8f`, worktree `legend-lite-query`).** Nothing lands on `main`
today but this note. What it is doing now: bringing its three unlanded branches up to date with `main` (161 to 223
commits behind), locally first: `query-by-name` (Query and DataCube open Studio's projects by name; 7 commits),
`studio-engine` (running in the tab, the query builder in Studio, the editing features, the Snap move; 19 more) and
`protocol` (the protocol program: the model printer and the model reader, both proven exact over the corpus; 21
commits). The rebased branches are pushed under NEW names (rule 1: no force-push); the old ones stay as they are.
Done the same day: `query-by-name-1007`, `studio-engine-1007` (the first plus 20; its BUILD diff reviewed by the build
program's session) and `protocol-1007`, each on `c9a18b1ed`; the two touch disjoint files and merge cleanly.
**Landed 2026-10-08: `studio-engine` (with `query-by-name`), as 61ad2dbaa (GATES.md, "The Studio line"); `protocol`,
as e5640e35a (its core files: the list below); DataCube's busy signal, as 355a86035; Studio's setup lists and Query's
navigation, as 609fae95e; Studio's status bar, as 8ab4a5bdc; the tab's test data from the planner's statements, as
fb3acc2f1 (`sqlType`'s copy gone; `planner.Wasm.testDataSqlOrError`, `wasm/` one export).** Parked on the Studio
line (the user, 2026-10-08: the round trip first): its harnesses' sites off Node's file server; `verify_remote_test`'s
fetch of DuckDB-WASM's httpfs extension from the network, to be vendored as a pinned file.
**The protocol program's leg 2 is landed (2026-10-09)** (`docs/PROTOCOL_PROGRAM_2026_10_05.md` §4.2). Steps 1 and 2
landed as 447102e56 (run 37870936729): a table reference keeps how it was written (`AppliedFunction.island`), and the
reader reads the older JSON legend-engine 4.145.0 reads (`SEMANTICS_REGISTER` S29 to S34; S34 is the compiler line's).
Step 3 landed as a12a64d24 (run 37923006492; GATES 2026-10-09): every printer in
`core/src/main/java/com/legend/protocol/` prints the records, the parity counts held exactly, and the reader reads the
older or hand-written shapes the step's audits found refused (S35 to S37; `own_corpus.matched` 2734). **Leg 4 landed (2026-10-09)** as a0fdf4e6f (run 37947981268; GATES 2026-10-09): the model printer in PRETTY as
well as STANDARD (exact over the corpus in both), legend-engine's `jsonToGrammar/model` in `PureV1Api`, the tab on
`pure/v1`'s grammar, and one boundary for the embedded hosts with thin adapters (`planner.Boundary`, `Folded`,
`TabExports`; `native/` Python's adapter), leg 3 folded into it (invariant 5 revised with the user; legs 6 to 8 added).
**Leg 5, the round trip proven, landed (2026-10-09)** as a5db1086e (run 38018795800; GATES 2026-10-09): text to JSON
to text to JSON exact over legend-engine's test collection, lite's 60 projects and the eight upstream Legend showcase
projects, in both styles, in the JVM (`//parser-equivalence:round_trip_proof`) and in the tab (`//wasm:round_trip_test`,
9,423 inputs byte for byte); S38 and S39 (lite keeps the braces and the column multiplicity the engine's printer drops);
TeaVM's `isBlank` and double conversions made portable and guarded; PARK-22 and PARK-23 parked. **2026-10-10:** a spike
built the tab with GraalVM Web Image (`docs/WEB_IMAGE_SPIKE_2026_10_10.md`); the user kept TeaVM, held to the JDK by
tests, and PARK-23's fix moved into TeaVM's class library. **Landed 2026-10-10 (3f9388f7b; GATES 2026-10-10):** the
conformance test of TeaVM's class library against the JDK (`//wasm:conformance_test`, its ledger of known differences
`wasm/conformance-known.tsv`), and TeaVM's number conversions made exact (`third_party/teavm_classlib`, put first on
every `teavm_wasm` build's class path by `tools/teavm/defs.bzl`; `MODULE.bazel` declares `teavm-interop`): every
number family equal to the JDK in the tab; PARK-23 restated to its one speed clause. **And from the same branch:**
`PortableText` retired, the server's win -- the protocol and the parser back on the JDK's own conversions
(`Double.toString`, `Double.parseDouble`, `BigDecimal.valueOf`) now that the tab's are exact (the 22 call sites in
`core/.../protocol/` and `core/.../parser/`; `protocol/NumberText.java`, `json/.../PortableText.java` deleted; the
`isBlank` half of `ArchitectureTest.textTheTabWritesDifferentlyIsWrittenPortably` stays). **Landed 2026-10-10:
leg 6** (`docs/PROTOCOL_PROGRAM_2026_10_05.md` §4, its decision and as-built note; GATES 2026-10-10) -- one
whole-model compile, `Compiler.compileErrors`, behind two routes on the one dispatcher:
`/api/pure/v1/compilation/compile` (legend-engine's exact answer, the first error) and the new
`/api/lite/v1/compilation/compile` (every error); the tab's `compileOrError` and the SDLC server's copy gone, Studio on
the lite route; `relationTypeJson` (E5's twin) gone from both adapters, `planJson` stays; PARK-17 closed (the fold
writes a refusal's kind as the routes name it, the DataCube + Python line's choice). **Landed 2026-10-10: leg 7**
(6305ebef5; GATES 2026-10-10), the SDLC's rules free of Pure -- in `core/BUILD.bazel`, only two visibility lists
(`:core` no longer visible to `//sdlc-server`; `:plan_side` visible to the new `//sdlc-server/grammar`, where
`CoreGrammar` moved); the rest in `sdlc-server/` and the docs. **Announced 2026-10-10 (core): leg 8**, a model sent
as JSON straight from the protocol records into the compiler (the user's decision; design in
`docs/PROTOCOL_PROGRAM_2026_10_05.md` leg 8, agreed with the Compiler Rewrite line -- positions -- and the Plan Gen /
Exec Split line -- execute's compiled-model seam), on branch `leg8-records`, landing only after the Compiler Rewrite
line's L7 step 2 has. Files: `core/.../model/` (`ModelFromProtocol`, new; `FromProtocol`; `ParsedModel`, a span map and
its `position` accessor), `core/.../parser/section/` (each grammar's `toModel` delegating to the door),
`core/.../parser/ElementParser.java` (`Data` through the door), `core/.../Compiler.java` (`compileModel` over records;
the three error-decoration sites through `ParsedModel.position`), `core/.../Execution.java` (`executeWire` over a
compiled model), `core/.../server/{PureV1Api,QueryService}.java` (the data context; `connectionOf` over records), and
their tests; a text-versus-JSON oracle in `parser-equivalence/`. **Reordered (the user, the same day): one parser
first** -- the compiler's text through the protocol parser's one section walk and import scoping (imports bound to
their section, as legend-engine and legend-pure bind them), judged by the Compiler Rewrite line's census, reference
lane, corpus passes and shadow probe before it lands. Its files beyond the list above:
`core/.../parser/PmcdParser.java` (the one assembly, taking the dialect), `core/.../parser/ElementParser.java` (its
own top-level assembly deleted; `parse` the door over the one parser), `core/.../protocol/{Protocol,ProtocolEmitter,
ModelReader}.java` (lite-only records for native functions, `Primitive` and a foreign grammar's opaque elements, which
the emitter refuses by name; then the JSON hop made lossless), `core/.../model/FromProtocol.java` (their converters).
Nothing in `compiler/` without the Compiler Rewrite line first. Its last step (the parser returning protocol only,
`ElementParser.parse`'s callers moved) is announced on its own after L7 step 3. **Landed 2026-10-11: step 1 and step
2's first part** (GATES 2026-10-11; the program doc's as-built note), after L7: the one parser, the door, the lite-only
records, and the records mirroring the engine's JSON where it was cut so far -- beyond the files above, in
`compiler/` with the Compiler Rewrite line's review: `spec/Typer.java` (the zero-argument qualified-property lift
takes a parameterized class), `spec/GraphFetchChecker.java`, `spec/typed/TypedGraphTree.java` and
`resolver/GraphEmission.java` (no `qualified` mark), `NameResolver.java` and `normalizer/` (the mapping's dead
`testSuitesSource`). Step 2 continues on `leg8-records`: a relation column's `[0..1]`, a decimal decided by its kind,
the operator spelling (each with the Compiler Rewrite line, judged as before), and the dropped-text rule, the user's
to decide first. Then leg 9, imports and comments
(added by the user, design first). Open, and not yet in the plan's legs: printing lite-only
mappings (a class mapping by function, a function association), which needs a design first.
**Planned landings, in this order** (each: the local gate, one CI run on the branch, then a fast-forward of `main`):
first `studio-engine-1007` (`query-by-name` inside it; CI lanes `ui`, `datacube`, `sdlc`), then `protocol-1007`
(engine code: `core/.../protocol/`, eleven files of `core/.../parser/`, `native-claims.tsv`; the engine's lanes). For
branches open elsewhere: `datacube-chart-spec` and `build/phase3` each raise `own_corpus.matched` in
`parser-equivalence/.../ratchets.tsv` from 2685, as `protocol` does (to 2716); whoever lands second rebases, reruns
`//parser-equivalence:parser_parity` and writes the number it measures. `protocol` adds two whole-corpus parity tests
to that target (about two minutes on a desk; well inside its limit).
Files the rebase touches beyond the line's own folders: the BUILD files of `query`, `sdlc-server`, `studio`,
`datacube` and `engine-client`, `gates/BUILD.bazel` and the CI lane lists (moving the line's tests into the new
`//gates` suites, the `ui` and `sdlc` lanes), and, for `protocol`, `core/src/main/java/com/legend/protocol/` (records,
emitters, readers, composers), `core/src/main/java/com/legend/parser/` (mechanical: `parseModel`, nullable spans),
`wasm/src/main/java/planner/Wasm.java` (one grammar export), `parser-equivalence/` (the oracles). Landing follows
the rebuild program's rule (branch rebased on `main`, an audit, the local gate, one CI run on the branch, then a
fast-forward), after the user decides the order. **Overlap to watch:** the compiler design's F1
(`docs/COMPILER_RIGHT_DESIGN_2026_10_07.md`, names resolved once) changes how call nodes (`protocol.spec`
`AppliedFunction`) are built, and the protocol program changes the same records (nullable spans, the `list` factory);
whichever lands second merges, and the two should agree before either starts on those files.

## A sixth line, 2026-10-07: DataCube + Python (the user's ask, rule 5)

Worktree `legend-lite-dcsnap`, branch `datacube-chart-spec`, rebased on `main` (`fd3d007e4`) on 2026-10-07. The goal:
DataCube on a Python dataframe, from a script and in a notebook, with Python running the SAME compiler the browser runs
as WebAssembly (one compiler source, so Python and DataCube share models and answers). **Owns** two new packages:
`native/` and `python/`, and, **since 2026-10-09, all of `datacube/`** (the user: "This session should own all the
datacube stuff"): the DataCube app, its page layout and page save, its notebook cube. Another line editing `datacube/`
asks this line first, as it asked the Studio line before.

**Landed 2026-10-07 (`bc8107c4e`; GATES entry "The compiler as a native library"): the compiler as a native library, with
Python bindings (L1).**
`//native:compiler`: `//wasm:boundary` (`planner.Wasm` over `//core`) built by GraalVM native-image as a SHARED library, with the warehouse image's C
toolchain (the host's checked, zlib from source, lld on Linux); Linux and macOS for now. `python/legend_lite`:
bindings on the standard library alone (ctypes). `//python:bindings_test`, a `py_test` on the repository's Python
3.12: the bindings' suite and the planner differential corpus, 69 of 69 answers identical to the JVM's. Cross-area:
`MODULE.bazel` and a new `maven_native_install.json` (a compile-only pool, `@maven_native`: GraalVM's `nativeimage`
and `word` API 25.0.2, in `@maven_teavm`'s shape), `tools/deps/pools.bzl` (its one user, `//native`),
`gates/BUILD.bazel` (`//python:bindings_test` in the `warehouse` lane, which builds native images already),
`wasm/BUILD.bazel` (visibility only). Nothing in `core/`. Also CI's product step (`gates-run.yml`: `//native:compiler` named
on its own, with `--skip_incompatible_explicit_targets`) and `tools/guards` (the compile-only guard walks it in the
native tier, except on Windows). **The Bazel program session reviewed and accepted the Bazel edits (2026-10-07).**

**Moved to the compiler line (2026-10-07, the user):** the typing fix this line found while testing the native library
(an erased `TDSRow` trusted any column name, so `#>{db.T}#->filter(x|$x.nope == 1)` typed; legend-engine refuses it at
typing). It is branch `compiler/tdsrow-erased-row` (one commit on `main`: `Type.java`, `PlatformTypes.java`, `Typer.java`,
`TdsRowReceiverTest`, `own_corpus.matched` +1), handed to the Compiler Rewrite session with its diagnosis and
measurements; it is W3.1's territory (`docs/EXECUTION_PLAN_2026_09_26.md`). That line decides whether and when it lands.

**Then, in this order** (each on the branch, each announced here with its files before it lands):
- **Frames in duckdb-python, and ONE model writer for a table (announced 2026-10-07, before the first edit; the
  user: "make sure Datacube actually does move to this exact same code").** **Landed 2026-10-08 (`0a20eb895`, with
  `live_snap_test` moved into the browser; GATES entries "Frames" and "DataCube writes every table's model").** Two
  commits, in order:
  1. **The writer moves into the compiler's boundary; Python uses it.** `wasm/src/main/java/planner/Wasm.java` gains
     `tableModelOrError` -- a table's catalog rows in; the whole model out (the Database, its connection and runtime,
     and the snap runtime when asked), the relation that reads it, the conversions as data AND as the converting
     select list in the dialect's own quoting, the columns left out, the BIT columns -- what DataCube's `infer.ts`
     writes by hand today -- and `catalogColumnsSqlOrError(schema, table)`, the catalog question
     (`DuckDb.CATALOG_COLUMNS_SQL`, filled). Then `native/` (two entry points), `python/legend_lite` (`register(name,
     frame, mode)`, Live the default: the frame re-read as Arrow at each query; Snapped: copied into DuckDB once;
     `execute(query)` -> an Arrow table), `python/BUILD.bazel`, `tools/python/requirements.in` and its lock (duckdb,
     pandas and polars for the tests; pyarrow is already pinned). In `core/`, one additive method:
     `core/.../sql/dialect/CatalogModel.java`, `Database.copySelectList()` -- the select list a copy applies, beside the
     writer's own identifier quoting -- with its test in `CatalogModelTest`.
  2. **DataCube moves to the same code** (after Studio's `studio-engine` lands; files agreed with the Studio line
     first): `datacube/src/infer.ts` (`inferModel` becomes the planner's `tableModel`), `upload.ts`, `catalog-model.ts`
     (its TypeScript copy of the writer deleted), `generated/catalog-facts.ts` and its generator where nothing else
     reads them, `wasm-planner.ts` and `planner-worker.ts` (the two calls), `demo/boot.ts` (its callers), and the tests
     that call `inferModel` or the TypeScript writer. **Settled 2026-10-08 (the user):** the writer is legend-lite's
     module whichever planner the page chose (`Engine.tables`: the planner in the tab; beside a server's planner, the
     module loaded in a worker on first use), so the TypeScript writer and both generators (`datacube/tools/
     catalogfacts`, their `datacube/BUILD.bazel` rules) go; also `demo/planners.ts`, `demo/stress.ts`, and comments
     only in `core/.../sql/dialect/CatalogRules.java` and `DuckDb.java` and `engine-client/src/snap.ts` and
     `warehouse.ts` (they named the deleted files). Diffs of `boot.ts` and `live-snap.ts` sent to the Studio line (no
     collision); the Bazel edits reviewed by the Bazel program session (accepted; with them `tools/deps/jars_table.bzl`
     drops `datacube` from `duckdb_jdbc_warehouse`'s users, and `docs/GENERATORS.md` loses the two generators).
- **`datacube.show(df)` -- steps 1 and 2 LANDED 2026-10-08 (`f49033d8f`, run 37857474050); steps 3, 5 and 6 LANDED
  2026-10-09 (`2b32f6d25`, run 37872741038; GATES entry "DataCube on a Python dataframe, steps 3, 5 and 6"): DataCube's
  page of one cube on an engine, `ll.show(df)`, the wheel per platform, LICENSE and NOTICE, CI's kept wheels. Steps 7
  and 8 LANDED 2026-10-09 (`ead4a7857`, run 37915130012; GATES entry "steps 7 and 8"): the cube under a notebook's cell
  and Windows. The real JupyterLab test and marimo first class LANDED 2026-10-09 (`169e0f062`, run 37926766513; GATES
  entry "a real JupyterLab, and marimo").** (Announced 2026-10-08, before the first edit; the design, agreed with the user:
  `docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`).** DataCube as the UI in its remote-run mode; Python a small Legend
  engine answering upstream's `pure/v1` slice (parse, print, a query's types, execute with Arrow results) through the
  native library and duckdb-python. Files, in order: `wasm/src/main/java/planner/Wasm.java` (the refusal answer and the
  execute metadata, beside the boundary's functions), `native/` (their entry points), `python/` (the server, `show()`,
  its tests); `engine-client/src/engine-remote.ts` and `pure-v1.ts` (Arrow as well as JSON, each engine's format
  declared); DataCube's start for an engine at an address (`datacube/demo/boot.ts`, `planners.ts` -- the Studio line's,
  agreed with it before the first edit); a browser test in `live_snap_test`'s shape; `gates/BUILD.bazel` (its lane).
  **Amended 2026-10-08, before the first core edit:** Python's answers come from legend-lite's own server code, not a
  second copy in the boundary. `core/src/main/java/com/legend/server/PureV1Api.java` keeps its package and name and
  moves into a plan-side library of its own, `//core:pure_v1` (as `:planner` sits beside `:driver`), so the boundary
  can call it. Its one database call, execute's run, is handed in by the caller (`PureV1Api.Runner`; legend-lite's
  server passes `QueryService.executeUpstream`, unchanged). The path-to-endpoint switch moves from
  `LegendHttpServer.PureV1Handler` into `PureV1Api.route`, so the two servers share one routing table. New in it:
  execute's Arrow half (`?serializationFormat=ARROW_IPC`): the SQL to run and the Arrow schema metadata, in the layout
  measured against legend-engine 4.145.0. Files: `PureV1Api.java`, `LegendHttpServer.java` (the switch only),
  `core/BUILD.bazel` (the library; the plan side and `:server_lib` reach it), `tools/deps/core-layers.txt`,
  `ArchitectureTest` (the library's sample class), the tests that call `execute` (`PureV1ApiTest`,
  `ConnectionLeaseTest`). **Overlaps:** the protocol program's `grammar/*` routes and the execution-plan line's step 4
  (`execute`) are in the same file, in other methods: whoever lands second merges, and step 4's "plan once, then run"
  fits the runner. The Bazel edits go to the Bazel program session before landing.
- **DataCube pages (announced 2026-10-09, before the first edit; the design, agreed with the user:
  `docs/DATACUBE_PAGES_DESIGN_2026_10_09.md`).** Every tile equal, the page owning the board; a page as bands (scrolling
  or fitting the window), the layout picker, drop zones, dividers, maximise, smart placement, edit and view, a narrow
  page stacked; then (phase 2) the whole page saved with several sources, (3) tabs, (4) Python pages. **Phase 1 LANDED
  2026-10-09 (`e397f107a`, run 37952097518; GATES entry "DataCube pages, phase 1").** Its files, all in `datacube/`:
  `src/layout/bands.ts`, `band-board.ts`, `src/ui/layout-picker.ts` (new); `src/page/cube-page.ts`, `src/app.ts`,
  `src/page-document.ts`, `src/export-model.ts`, `src/export-xlsx.ts`, `src/snapshot.ts`, `src/ui/menu.ts`, `src/app.css`,
  `demo/boot.ts`, `tools/link-dictionary/make.ts`; `src/layout/board.ts` and `tile-layout.ts` deleted; their tests;
  `demo/verify-layout.mjs` with `datacube/BUILD.bazel` and a comment in `gates/BUILD.bazel` (Bazel-reviewed). **Phase 2
  LANDED 2026-10-09 (`eb2e38fa8`, run 37980936512; GATES entry "DataCube pages, phase 2").** Its files, all in
  `datacube/`: `src/page/page-app.ts` (new), `src/page/cube-page.ts`, `src/app.ts`, `src/page-document.ts`,
  `src/layout/band-board.ts`, `src/file-handles.ts`, `src/upload.ts`, `src/ui/menu.ts`, `src/app.css`, `src/theme.css`,
  `demo/boot.ts`, `demo/verify-{charts,cubes,features,layout}.mjs`, `test/page-app.test.ts` (new),
  `test/bundle-budget.test.ts`, `test/chart-tiles.test.ts`, `test_imports.bzl`, and one `_EXTRA_DATA` row in
  `datacube/BUILD.bazel` (Bazel-reviewed). **Phase 3 LANDED 2026-10-09 (`93e506e9b`, run 38005815382; GATES entry "DataCube pages, phase 3").** Its
  files, all in `datacube/`: `src/ui/sheet-tabs.ts` (new), `src/page/page-app.ts`, `src/page/cube-page.ts`,
  `src/layout/bands.ts`, `src/layout/band-board.ts`, `src/page-document.ts`, `src/app.ts`, `src/ui/menu.ts`,
  `src/app.css`, `demo/boot.ts`, `tools/link-dictionary/make.ts`, `demo/verify-{cubes,layout}.mjs`, their tests,
  `test_imports.bzl`.
- **Python pages (announced 2026-10-09, before the first edit; the design, agreed with the user:
  `docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md`).** A whole DataCube page built from Python (`ll.Page`: grids over
  frames, charts, sheets, layouts, stacks; `show`, `save`, `load`, live), Python writing DataCube's own page document.
  Branch `datacube-python-pages`. Step 1, the page over an engine: `datacube/src/cube-document.ts` (a frame source),
  `src/page/page-app.ts`, `datacube/demo/engine-page.ts` (new), `demo/engine.ts`, `demo/widget.ts`, `demo/boot.ts`,
  `datacube/BUILD.bazel` (the widget's sources: one file added; to the Bazel program session first),
  `python/legend_lite/engine.py` (`page.json`), their tests and the Python engine's browser test. Step 2:
  `python/legend_lite/page.py` (new), `datacube.py`, `notebook.py`, `python/tests/test_page.py`.
- **DataCube charts, batches 2-5 (announced 2026-10-14, before the first edit; the design, widened with the user the
  same day: `docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md` §3).** Every chart we can realistically draw: the chart spec's
  version 2 (a slot per role), the calculated columns DataCube already has packaged as presets (grain, bins, show as)
  for the chart and the grid, batch 2's dashboard-grade charts, then batch 3's new kinds, batch 4's aggregates and the
  kinds they unlock, batch 5's maps -- each batch landing on its own with an example page (a sheet per family). Branch
  `claude/epic-rubin-ay7upl`, from main; built by its own session and reviewed by this line before it lands. Batch 2's
  files, all in `datacube/` and `python/`: `src/chart-spec.ts`, `src/chart-option.ts`, `src/chart-render.ts`,
  `src/calc-presets.ts` (new), `src/calc.ts` (the date functions), `src/examples/` (new: the example pages),
  `src/samples.ts` (the `sales` sample), `src/ui/chart-panel.ts`, `src/ui/chart-form.ts` (new: the options form, its own
  chunk), `src/ui/menu.ts` and `src/app.ts` (the grid's presets; a column starts from the bins preset in the column
  editor, which is not changed), `src/page-document.ts` (version 2 read), `src/page/cube-page.ts`
  (drill, brush), `src/app/datacube-app.ts` (Examples in the Open window), `src/app.css`, `demo/verify-charts.mjs`,
  their tests, `test/bundle-budget.test.ts` (the chart chunk's budget, raised on purpose with a measured note),
  `test_imports.bzl`, `datacube/BUILD.bazel` (new sources and tests: to the Compiler Rewrite session),
  `python/legend_lite/page.py` and `python/tests/test_page.py` (Python's words). Not touched: D5b's files
  (`engine-client/src/snap.ts`, `duckdb.ts`; `datacube/src/cube.ts`, `app/sources.ts`, `upload.ts`, `infer.ts`,
  `lite-planner.ts`), core. Batch 4 adds aggregates to `src/snapshot.ts` and `src/query.ts`, announced here before it
  starts. **Widened 2026-10-15 with the user, in PR neema2/legend-lite#42 before it lands (batches 2 and 3 together):**
  (1) the chart rules written once -- Python's charts made and judged by DataCube's own code, headless (`src/chart-spec.ts`
  `readChart`/`chartFrom`, `src/headless/grid.ts` `gridChart`, `src/headless/model.ts` `pageModelKept`,
  `src/page-document.ts`; `python/legend_lite/page.py`'s copy of the rules deleted); (2) one package, two things: the
  wheel alone the compiler, its `datacube` extra everything DataCube needs, V8 included (`python/legend_lite/_extra.py`
  new, `frames.py`, `engine.py`, `__init__.py`, `python/README.md`, `python/BUILD.bazel`, `python/tests/test_wheel.py`);
  (3) the static site slimmed -- one DuckDB build (eh), one copy of the apps' WebAssembly, no page that needs a server
  (`engine-client/src/duckdb-tab.ts` and its `BUILD.bazel`; `query/BUILD.bazel`, `query/demo/main.ts`,
  `query/src/app/context.ts`; `studio/BUILD.bazel`, `studio/src/app/app.ts`; `site/BUILD.bazel`, `site/make-dist.mjs`
  new; DataCube's `demo/` config, boot and planners, `src/services/browser.ts`); (4) every app's bundles minified, names
  kept (`datacube/`, `query/`, `studio/` BUILD files; the budgets measured down); (5) the playground on GitHub Pages
  (`.github/workflows/pages.yml` new; `site/index.html`, `site/verify.mjs` served under `/legend-lite/`). Reviews: the
  DataCube + Python line (DataCube, Python), the Studio line (Query, Studio, the site, `duckdb-tab.ts`), the Compiler
  Rewrite line (the BUILD files, the workflow), an independent audit of the whole PR. The other Python twins of
  DataCube's logic the audit found are the next program: `docs/PYTHON_ASKS_DATACUBE_2026_10_15.md`. **The reviews'
  fixes, 2026-10-15, files beyond the list above:** `src/ui/chart-panel.ts` and `src/ui/chart-form.ts` (every edit
  through `chart-spec.ts` `editChart`, the one change by hand Python's charts use too); `python/legend_lite/_pool.py`
  (new: the compiler's threads, on Python's standard library), `model.py`, `_home.py`, `_headless.py`;
  `query/demo/config*.json` and `datacube/demo/config.json` (one `vendor` key each); `MODULE.bazel` (the `wasm_mvp`
  extension pins removed; the lock unchanged) and `engine-client/BUILD.bazel`; `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`
  (that note).
- **The notebook widget**, `DataCube(df)`: the same two calls over the notebook's widget channel.
- Later: model handles and `execute` from Python, typed Pythonic queries, the shared warehouse from Python.

## A seventh line, 2026-10-11: Postgres SQL in Pure, the `#SQL{...}#` island (the user's ask, rule 5)

Session "AlloySQL porting and exploration", branch `claude/loving-euler-gjta59`; the study and plan are
`docs/LEGEND_SQL_STUDY_2026_10_11.md` (on the branch, landing with it). Phase 1's translator: a Postgres `SELECT`
inside any Pure query becomes an ordinary Relation expression, typed once by the Typer.

**Announced 2026-10-11, before landing: a cross-area edit in `core/`.** It lands as one merge of the branch, after one
full CI run on it.
- **New files.**
  - `protocol/sql/`: `SqlAst`, `SqlLexer`, `SqlParser`, `SqlSyntaxException`, `SqlToPure`, `SqlTranslationException`.
  - `protocol/spec/SqlIsland.java`, `compiler/spec/SqlIslands.java` (the seam), `error/IslandSyntaxException.java`.
- **Edits.**
  - `compiler/NameResolver.java` and `compiler/spec/Typer.java`: the island's arm.
  - `lowering/Lowerer.java`: four lowering fixes the island's cases found, each with a test that fails without it:
    - a union's sorted or limited branch becomes a subselect;
    - `exists` over a sliced relation is isolated first (it was a silent wrong answer);
    - a join condition reads an enclosing row through the enclosing scopes;
    - a relation `map` used as a value pushes the enclosing row.
- **Tests.**
  - `CodeShapeGuardrailTest`: five scanner and counter fields allowlisted, dated.
  - `integration/` `ConcatenateFlattenCheckerTest`, `CorrelatedRelationCheckerTest`, `SqlIslandIntegrationTest`,
    `SqlParityTest`.
  - `server/` `SqlIslandPostgresTest`, `SqlParityPostgresTest`; `testcases/` `SqlIslandCases`, `SqlParity`.
  - `protocol/sql/` `SqlParserTest`, `SqlToPureTest`.
  - `src/test/resources/sql-parity/`: the ledger, the answers snapped from Postgres 16, and the `lite/` cases.
- **Build.**
  - `core/BUILD.bazel`: `sql_island_postgres_test`, `sql_parity_test`, `sql_parity_postgres_test`.
  - `gates/BUILD.bazel`: the core suite gains the three.
  - `third_party/legend_engine_src.BUILD`: the `sql_parity_tests` filegroup, upstream's parity corpus as test input.
- **Docs.** `docs/SEMANTICS_REGISTER.md` (S43 to S47), `docs/GATES.md`.

Not touched: the database owner's `lowering/` files (`Render`, `WireRender`, `LiteralSpelling`, `Fold`, `Scalars`,
`MixedEncoding`), `sql/dialect/`, the parser.

**Landed 2026-10-12 (ebc419f, the merge of PR neema2/legend-lite#28 at d312dfd over main d2eefc5; gates run 38198865199,
green on every job after one re-run of windows / warehouse; diagnostics run 38198865034, green; GATES "`#SQL{...}#`").**
- **The re-run:** `//datacube:python_engine_test` failed once in its teardown. Answers to a taken-down widget view were
  still in flight when the browser closed: the race the TeaVM landing met, which the DataCube line is fixing. This
  change touches nothing under `datacube/`.
- **Files beyond the announced list:** none.
- **Found by the PR's CI on the way:**
  - the Postgres parity harness's snapped answers held on Linux only: `bytea` was snapped as a JVM identity hash, and
    float answers differ in their last digit between platforms;
  - two `AGE(x)` answers drifted a day after their snap.

  Both are fixed in the harness (`testcases/SqlParity.java`).
- **The independent review's one finding** (fix 4 let a row's own window column resolve to the enclosing row's) is
  fixed in `Lowerer.scopedResolver`, with two tests.

**Announced 2026-10-12 -- a stored Variant column leaves a star-framed statement as JSON (branch
`core/variant-star-egress`):** the DataCube line's finding (2026-10-12): under a bare source, `limit`, `filter` and
`sort` a native STRUCT column typed Variant left the query as stored, under `select` as JSON text
(`docs/VARIANT_STORAGE_CENSUS_2026_09_27.md` D2: V2's `columnRef` hook fires on a reference, and `SELECT *` renders
none). Files: new `core/src/main/java/com/legend/sql/dialect/VariantEgress.java` (a DuckDB render pass, root only);
`sql/dialect/DuckDb.java` (`storedVariant` over an output column, the pass last in `passes()`; the SQL island line's
announced `variantElements` edit is another method of the same file);
`core/src/test/java/com/legend/lowering/LowerRelationTest.java` (one test over six shapes and three storages, the
storages' set-up shared with the census's test); the census's addendum; `docs/GATES.md` at landing. Judged by the
judges, the render census, the corpora and the local gate; one full CI on the branch; the push to main after the
DataCube line's PR #31 is in. The Plan Gen line agreed: lowering only, and result typing assumes JSON for a Variant
output in every shape. **Landed 2026-10-12 (rebased onto main 36b72ee13 after PRs #31 and #33, the code byte-identical;
run 38327604817, 50 jobs green first time, the summary job skipped on a dispatched run, the first run 38310588988 before
the rebase green too, main's own run on the landing 38331688457 green; `//gates:local` 348 pass, 2 skipped (two guards
incompatible on this Mac) tests; GATES entry "A stored Variant column leaves the statement as JSON, whatever the shape
of the query")** -- the audit widened the pass from star frames to every root-level star (`extend`'s `SELECT t0.*, x`, a
UNION's branches, a WITH's body) and the test from six shapes to ten; files touched beyond the announced list: none.

**Landed 2026-10-12 -- one `DUCKDB_PLATFORMS` (branch `build/duckdb-platforms`; run 38332350300; GATES entry "One
`DUCKDB_PLATFORMS`: DuckDB's spelling of our platform names, in one place"):** a Bazel-only tidy-up after PR #31: the
mapping from our platform names to DuckDB's extension names, spelled by hand in `python/BUILD.bazel` and
`warehouse/defs.bzl`, is one constant in `tools/platforms/defs.bzl`; the same five repositories on the same five
platforms. Audited; the DataCube line approved python's part. No announcement went first: no `core/` file, no behaviour.

**Landed 2026-10-13 -- one `//tools/teavm:api` (branch `build/one-teavm-api`; run 38406686025, main's own run on the
landing 38409010126 green; GATES entry "One `//tools/teavm:api`: TeaVM's compile-time annotations in one library"):** a
Bazel-only tidy-up: the four identical neverlink libraries in wasm, sdlc-server, query-store and warehouse are one in
`tools/teavm`, visible to those four packages; the same two jars on the same compile classpaths. Audited; the DataCube
and Studio lines approved their packages' parts. No announcement went first: no `core/` file, no behaviour.

**Announced 2026-10-12 -- the semi-structured step: jsonb, LATERAL, set-returning functions** (the user's ask: JSON
and Variant are what lite's DuckDB users do; branch `claude/loving-euler-gjta59`, three commits over main c03ac6b; the
study's §9.2). It lands as a pull request, merged after its full CI run.
- **The SQL front end:** `protocol/sql/` `SqlAst.java` (subscripts, `ARRAY[...]`, LATERAL subqueries), `SqlLexer.java`
  (a slice's `:`), `SqlParser.java`, `SqlToPure.java` (jsonb navigation over Variant; LATERAL; `generate_series`,
  `unnest`, `json(b)_array_elements(_text)`, `json(b)_object_keys` in FROM and the select list; a query without FROM).
- **A cross-area edit in `sql/dialect/`, the database owner's** (two fixes the island's cases found in plain Pure):
  - `DuckDb.java` (`variantElements`): `toMany(@Variant)` keeps an array's JSON null elements (a cast to `JSON[]` made
    them SQL NULL); a value that is not an array is still Pure's `toMany` error;
  - `Postgres.java` (`decode`, the `to(@Map)` cast): a value of the wrong shape is Postgres's own error, as Pure's and
    DuckDB's are, not an empty list or no map.
  Judged by `//pct:pct_duckdb`, `//pct:pct_postgres`, `//pct:pct_channel_b`, the core suite and the six corpus passes.
- **Tests:** `SqlParserTest`, `SqlToPureTest`, `testcases/SqlParity.java` (a JSON cell compares as its JSON value),
  `lowering/LowerRelationTest.java` (two shape pins follow the new elements SQL); `sql-parity/` the ledger, the answers,
  `lite/json.yaml`, `lite/lateral.yaml`.
- **Docs:** `docs/SEMANTICS_REGISTER.md` (S49: a Variant is jsonb), the study's §9.2, `docs/GATES.md`.
- **Widened 2026-10-12** (the user: the basics first -- filters, aggregations, joins, select lists, functions -- before
  the PR; it was closed to fold them in): a randomized check of the basics against Postgres (generated queries, both
  lanes), and what it and the independent review found. One more cross-area edit in `sql/dialect/`:
  `RootNumericTypes.java` -- under DISTINCT, an ORDER BY key that was a projection it casts to its numeric type is
  ordered by the cast expression (Postgres: "for SELECT DISTINCT, ORDER BY expressions must appear in select list";
  Postgres, Trino, ClickHouse, Databricks and Snowflake run the pass; the cast changes no value). The SQL front end
  gains `FILTER (WHERE ...)`, `DISTINCT` in every aggregate, integer division (exact for every bigint),
  `string_agg(... ORDER BY ...)`, and Postgres's typing of CASE, COALESCE, GREATEST, LEAST, ceil, floor, sign, round,
  extract and date_part. Judged as above.
- **Widened again 2026-10-12:** casts, `bool_and`/`bool_or`/`every`, `percentile_cont`/`percentile_disc`,
  `corr`/`covar_*`, `lag`/`lead` with a default (on the branch, d75e4b8). One more cross-area edit in `sql/dialect/`:
  `QuantileOrder.java` -- DuckDB's descending discrete percentile reads element `ceil(p * N)` of the sorted list, which
  at `p = 0` is element 0 of a 1-based list: NULL. Pure's percentile and Postgres's `percentile_disc` both take the
  first value there; the index becomes `greatest(ceil(p * N), 1)`. A test beside
  `TypeInferenceIntegrationTest.testPercentile_GroupBy_Discrete_Descending` pins it. Judged as above.
  **Widened 2026-10-12 by the PR's independent review:** the same file's ascending discrete percentile too. DuckDB's
  `quantile_disc` picks element `ceil(p * N)` in exact arithmetic; Postgres's `percentile_disc` multiplies in float8
  (`ceil(0.28 * 25)` is 8 there, 7 in DuckDB). Both directions are now element `greatest(ceil(CAST(p AS DOUBLE) * N),
  1)` of the sorted list. PR neema2/legend-lite#33.

**Landed 2026-10-12 (e647bf3, the merge of PR neema2/legend-lite#33 at fee1fd8 over main 6d5f0e1; gates run
38316829892 green on every job -- Linux, macOS, Windows, linux-arm, "gates green" -- and on the previous head d774f93
too; GATES "the semi-structured step" and "the basics").** It replaced neema2/legend-lite#32, closed so that the
basics could land with it.
- **Files beyond the announced list:** none. The `sql/dialect/` edits are the four announced ones (`DuckDb.java`,
  `Postgres.java`, `RootNumericTypes.java`, `QuantileOrder.java`).
- **The independent review** of the basics found eight wrong answers, each fixed and pinned before the merge (f81c6e7;
  the study's §9.3), and three recorded.
- **Next on the line:** regular expressions (`~`, `regexp_*`), then the HTTP route and the wire.

**Announced 2026-10-13 -- the long tail: text, numbers, date spellings, aggregate and window permutations** (the user,
after the basics landed: the functions first, "all the long tail ... including the date/time bucket", "all the
string/math functions", and every permutation of window and aggregate functions; regular expressions last, if ever).
Branch `claude/loving-euler-gjta59`, three commits over main acd7a1d (733fa90, d19b606, and one to come); the study's
§9.4. It lands as a pull request, merged after its full CI run.
- **The SQL front end:** `protocol/sql/SqlToPure.java` -- text (`concat_ws`, `format`, `overlay`, `quote_literal`,
  `quote_nullable`, `translate`, `split_part`, the trims naming `' '`), numbers (`div`, `mod` and `%` of numerics,
  `log(b, x)`, `log10`, `cot`, `width_bucket`, a numeric `power` and `^`), `extract`'s `dow`, `week`, `decade`,
  `century`, `millennium` and a date's `epoch`, `isfinite`, the `regr_*` aggregates, ORDER BY inside aggregates,
  FILTER on window aggregates, frame `EXCLUDE`; new `protocol/sql/PostgresDateTime.java` (Postgres's date and timestamp
  spellings, datetime.c's ParseDateTime and DecodeDateTime ported).
- **One cross-area edit, a guard file:** `CodeShapeGuardrailTest` -- eleven fields of `PostgresDateTime`'s decoder
  (datetime.c's struct pg_tm and its field mask, scoped to one constant's read, the parser-cursor lifecycle)
  allowlisted, dated.
- **Tests:** `SqlToPureTest`, new `PostgresDateTimeTest`; `sql-parity/` the ledger, the answers, `lite/text.yaml`,
  `lite/math.yaml` and `lite/aggregates.yaml` (new), `lite/dates.yaml`, `lite/windows.yaml`.
- **Docs:** the study's §9.4, `docs/GATES.md`.

**Landed 2026-10-13 (77d32ac, the merge of PR neema2/legend-lite#35 at df7cd2a over main afb8c00; gates run
38338369857 green on every job -- Linux, macOS, Windows, linux-arm, "gates green"; GATES "the long tail").**
- **Files beyond the announced list:** none. Nothing in `sql/dialect/` or `lowering/`; the one guard-file edit is the
  announced `CodeShapeGuardrailTest` entry.
- **The independent review** found four wrong answers in the step and two older ones it reaches, each fixed and pinned
  before the merge (5ac5ddd; the study's §9.4), and four recorded.
- **The first CI run** (eca523c) failed every lane that compiles core to WebAssembly: `translate()` called
  `Character.toString(int)`, which TeaVM's class library lacks; fixed in e14af16 (`new String(Character.toChars(cp))`).
  The local suite builds no TeaVM target, so a JDK method TeaVM lacks reaches CI first: a change to `core/` that calls
  a JDK method new to core should build `//sdlc-server:page` and `//wasm:planner` locally.
- **Next on the line:** the remaining refusals the study lists (`mode()`, `string_agg` DISTINCT and over a window,
  GROUPS frames, ROLLUP/CUBE/GROUPING SETS, intervals and `timestamptz`, `to_char`/`to_date`, `date_bin`, the rarer
  text and number functions); regular expressions last, if ever. Two decisions wait on the user: DuckDB's trims of
  Unicode spaces and DuckDB's JSON numbers as doubles.

Not touched: `sql/dialect/`, `lowering/`, the Pure parser.

**Announced 2026-10-14 -- the stress corpus and its fixes** (the user: "an insane sql corpus that stress tests our
parser and sqlgen"; branch `claude/loving-euler-gjta59`, its first commit 5c7719f: the corpus, the runner
`testcases/SqlStress.java`, `core/src/test/resources/sql-stress/` with the baseline). Every query that finished
gave Postgres's answer; what does not finish is scale. The fixes, measured by the corpus before they land:
- **In `protocol/sql/`:** `SqlToPure.java` -- a strict operation's NULL guard merged, not re-tested (a 10-term `+`
  chain was 96 KB of SQL); the translator's recursion over long operator chains (500 deep overflows a default stack).
- **One cross-area edit, the compiler's:** `core/src/main/java/com/legend/compiler/spec/JoinChecker.java` --
  `sideAgnosticTdsCond` types both inputs of every join to read their columns, then `checkGeneric` types them again,
  so a chain of N joins types its innermost input 2^N times (20 tables: 36 s to plan; 30: past 60 s). The change:
  it types them only when the condition reads a column by name (a legacy TDS `get*('col')`, the only reads it
  re-points), so no behaviour changes; a test pins a long join chain's planning. The compiler line may prefer its
  own fix; say so here and this edit waits.
- **Not in this step:** CTE chains that reuse earlier CTEs grow their SQL as the copies multiply (a CTE is copied into
  every reference; sharing one needs a relation-valued `let` to lower to a `WITH`, a lowering and typer change, to be
  proposed separately), and `greatest`/`least` nested deep (each level writes its operands three or four times).
- **Landed 2026-10-14, a doc and a script, no build or source edit:** `docs/CLOUD_SESSIONS.md` and
  `tools/cloud-distdir.sh` (the user: "write this up and check it in somewhere that another session can do the same"):
  how a cloud session runs Bazel when its proxy refuses GitHub's archive downloads (a `--distdir` seeded from Bazel's
  mirror or a git clone, every pinned checksum still checked), and the container's other traps. Pointed to from
  AGENTS.md's standing documents and START_HERE.md §6. The Bazel line owns `tools/`: move or fold it as it likes.
- **Announced 2026-10-14: two more cross-area edits, the compiler's lowering, on the same branch** (the user: "fix
  the bugs here too"). The tenth line found three #SQL wrong answers (its notes above); two are the planner's, and they
  are wrong for plain Pure too:
  - `core/src/main/java/com/legend/lowering/Fold.java`: an aggregate with no GROUP BY is a boundary, like DISTINCT.
    `filterSlot` merged a filter that reads no column into the aggregate's WHERE, so
    `aggregate(~[n:x|1:y|$y->count()])->filter(r|false)` answered one row (#SQL's `HAVING 1 = 0`). `groupByFolds`
    merged a second aggregate into the first, so `aggregate(count)->aggregate(count)` counted the rows read, not 1.
    Both now isolate (`aggregatesWithoutKeys`).
  - `core/src/main/java/com/legend/lowering/NullSemantics.java`: `equalNullArms` gains the NULL-literal arms
    `notEqualNullArms` has. `[] == []` is TRUE and `x == []` is `x IS NULL`; two NULL constant columns compared
    by #SQL's `SELECT NULL INTERSECT SELECT NULL` were `NULL = NULL`, and answered no row.
  - Also on the branch, in this line's own files: nested casts test an integer's range once (ten casts were 9 MB of
    SQL), the default name of a cast is read once a level, and `SqlToPure.At.logical` lets a dialect use the AND/OR
    chain reader (sent to the tenth line, which takes it after this lands).
  - Judged by the core suite, the corpus lanes and the parity lanes; a test per wrong answer. The compiler line may
    prefer its own fix: say so here and these edits wait.
  - **Added 2026-10-14 after the independent review:** the same keyless-aggregate mistake in three more places, each
    a wrong answer before this branch: `Fold.projectionFolds` (a column that reads none of the aggregate, as
    `aggregate(count)->extend(~[one:r|1])->select(~[one])`, read a row per row), and in
    `core/src/main/java/com/legend/lowering/SubselectPrune.java` (`unionArmsPrunable`: a union arm pruned to its
    constants) and `core/src/main/java/com/legend/lowering/RelationPredicates.java` (`searchedColumn`: a searched
    relation's WHERE added to it). And the NULL-literal equality is written null-safe, so the legacy null-unsafe
    mode keeps the engine's `=`.

**The Compiler Rewrite line, 2026-10-14 -- the two lowering edits: go ahead, with four asks** (the compiler line's
  answer to the announcement above; the branch's `JoinChecker` edit read with them).
- **Both fixes are the right ones; this line has none of its own to prefer.** An aggregate with no GROUP BY is one row
  whatever it read, so a filter of that row is not a filter of its rows and an aggregate of it is not an aggregate of
  them: a boundary, as DISTINCT is. And Pure's `[] == []` is true and `x == []` is the absence test, as
  `notEqualNullArms` already says for `!=`; the arm's place before the column rule is right (a NULL literal is no
  column), and `verbatim` never sees it (it rewrites NULL_SAFE_EQUAL only).
- **Ask 1: one aggregate predicate.** `Fold.aggregates` is a fourth copy of "does this expression aggregate its select"
  (`SqlTyping.aggregateRooted`, `ExplosionMarkers.aggregates`, `StableScanOrder.aggregates`), and it differs from the
  typing's in one thing: `aggregateRooted` stops at a `SqlExpr.Lambda` (a list function's lambda reduces its list, not
  the select) and Fold's does not, while `children()` walks into a lambda's body (`SqlExpr.java`, the `Lambda` and
  `FoldCall` arms), so a fold over a list in a projection would isolate as an aggregate. One predicate with the lambda
  stop, as a public static both packages reach (`SqlAgg`, say), used by `SqlTyping.aggregateRooted` and
  `Fold.aggregatesWithoutKeys`. The two dialect-layer copies ask narrower or other questions (ExplosionMarkers:
  aggregate or window) and are not this edit's to touch.
- **Ask 2: a plain-Pure pin per wrong answer.** The branch's tests are the parity cases and the stress corpus, which pin
  the `#SQL{...}#` path; the announcement says both answers are wrong for plain Pure too, and the unit tests of both
  files exist and test these very methods: `FoldTest.filterBoundaries` (ISOLATE for `BARE` with a `count` projection and
  no GROUP BY, and `groupByFolds` false for it) and `NullSemanticsTest` (`[] == []` to TRUE, `x == []` to `x IS NULL`,
  as the `!=` arm's cases).
- **Ask 3: the `JoinChecker` pin, in the core suite.** The edit does what was asked: `rowRead` is the one leaf, and
  `readsByName` walks what `swapMisplacedReads` walks (a function's parameters, a property's receiver, the last body
  expression), each comment naming the other; keep them in step. The pin is not in the core suite: `SqlStress` is the
  hand harness (a local Postgres, a 60 s clock). `JoinTortureTest`, or a test beside `JoinChecker`'s, planning a chain
  of twenty modern-condition joins is the pin: it typed the innermost input 2^20 times before (36 s) and once a level
  after; the test's own timeout bounds it, no clock read.
- **Ask 4: the reference lane and the census.** Two typing and lowering edits on the compiler line's files: run
  `//spec:reference_lane` and the render census before the landing says measured, and name both runs in the landing note
  here and in GATES, as the compiler line's entries do.
- - **Later the same day, the branch at f3d95e07d: the four asks are taken; go ahead as it stands.**
  `SqlAgg.aggregatesSelect` is the one predicate (the typing's `aggregateRooted` is gone into it; the two dialect-layer
  copies stay, as said); `FoldTest.aKeylessAggregateIsABoundary` and `NullSemanticsTest.emptyColumnsAreEqual` pin plain
  Pure; `JoinCheckerTest.aLongJoinChainTypesEachInputOnce` plans a chain of forty joins under the test's own timeout.
  The three further keyless-aggregate sites (`projectionFolds`, `SubselectPrune.unionArmsPrunable`,
  `RelationPredicates.searchedColumn`) are the same boundary wherever a select's row count is read, and the null-safe
  spelling of the NULL-literal arms keeps `verbatim` and the legacy mode at the engine's `=`: both right. The tenth
  line's "planning time doubles with each join" (its notes) is this edit's 2^N; its eighteen-join repro through
  `#SQL{...}#` is worth one timing on the branch before the landing note says covered. Say the reference lane's and the
  census's runs in the landing note, as asked. This line's three planner fixes landed on main (d619c5cd6) while the
  branch's run 38611340083 was queued: no file of the two changes overlaps (this line's are `LiteralUnroll`,
  `NormalizeFolds`, `TypedCInteger`, `StampCensus`, `ConcatenateChecker`, `ExecuteChainAssembly`, `TypedConcatenate` and
  five resolver files), so the rebase is plain.

**Landed 2026-10-15 (10bc6fb, the merge of PR neema2/legend-lite#41 at ec003fe over main 7cf39c0; the PR's run
38663358847 green on every job -- Linux, Linux ARM, macOS, Windows, "gates green"; main's own run 38667214956; GATES
"the stress corpus and its fixes").**
- **Files beyond the announced list:** `sql/SqlAgg.java` and `sql/SqlTyping.java` (ask 1's one predicate, where the
  compiler line asked for it), and `core/src/test/resources/dialect-leaks/ledger.tsv` (after #39's ratchet: the stress
  runner's two connection URLs pinned with a dated reason). Nothing in `sql/dialect/`.
- **The compiler line's asks:** taken on f3d95e07d (its note above). Ask 4's runs: `//spec:reference_lane` and
  `//spec:update_reference_lane_test`, both corpora, PCT DuckDB, H2 and channel B (26 of 26); the render census, 0 of
  21,462 lines differ (`docs/build-inventory/program/evidence/sql-stress/judges.txt`).
- **The tenth line's "planning time doubles with each join"** is covered: its repro through `#SQL{...}#` on the landed
  code plans 18 joins in 67 ms (LEFT), 28 ms (INNER) and 39 ms (comma), where it measured 28 s.
- **The independent review** found one blocker of the branch's own (a merged NULL test skipped an operand's error when
  another operand was NULL) and the three further keyless-aggregate sites; each fixed and pinned before the merge.
- **Next on the line:** the deep shapes that still overflow a default stack (400-deep parentheses in the SQL parser,
  200-term `+` chains in the lowering, a 400-branch CASE in the name resolver), the CASE fix waiting on the user's
  choice (a balanced decision tree in the translator, or the compiler's stack); CTE chains as a `WITH`; the NULL-first
  evaluation order. For the compiler line: the committed `tools/census/RenderCensus.java` passes no runtime, which the
  planner refuses, so every case fails at lowering on both sides; the census above ran a copy with PCT's storeless
  runtime.
  (The compiler line, 2026-10-15: the committed tool lowers with the storeless runtime on DuckDB since 487a0f460 on
  main; GATES "The compiler's two scale costs".)

**Announced 2026-10-15 -- main's Windows core timeout: CI's test JVMs on the C1 JIT only** (the seventh line; the
diagnosis is this line's throwaway draft PR neema2/legend-lite#44, branch `claude/diag-windows-core-hang`, not the
tenth line's). `//core:core_tests_integration` timed out at 300 s on Windows on main's runs after PR #41 (38667214956,
twice) and after the scale-costs landing (38693372933). Measured on the CI runners themselves:
- **Alone it is fast on both platforms:** 64 s on Windows, 70 s on Linux; its test thread on the CPU 57 of 60 s; file
  I/O 0.07 s in all (17,000 events), disk latency 1-4 ms, kernel time 4%; Defender's real-time scanning is already off
  on the runners. Not Windows, not I/O, not a hang (a run with the time limit raised finished at 262 s with no test
  over 4 s), and not PR #41's code (the stress suites and the target's own time unchanged or faster with it, locally).
- **In the core lane it takes 220-277 s:** about four test JVMs share the runner's four vCPUs, and each spends nearly as
  much CPU in the JIT's C2 compiler as in its tests (the lane's 40 targets: 1,160 s of process CPU on Windows, 710 s of
  it JIT; 1,035 s on Linux, 1,483 s JIT time).
- **With `-XX:TieredStopAtLevel=1` on every CI test JVM** (run 38688819347, the core lane on both platforms, every test
  passing): the target 277 -> 113 s on Windows, 220 -> 103 s on Linux; the lane's process CPU 1,160 -> 605 s and
  1,035 -> 565 s; JIT time 710 -> 67 s and 1,483 -> 94 s; the lane step 8.4 -> 6.7 min and 9.8 -> 8.1 min; no target
  slower by more than 20%.

The change: one line in `.bazelrc`, `test:ci --jvmopt=-XX:TieredStopAtLevel=1` (CI only: a desk keeps C2), on branch
`claude/loving-euler-gjta59`, with the measurements as evidence and a GATES entry; its pull request's CI runs every
lane on every platform with it. `.bazelrc` is the Bazel program's file: the Phase 8 brief's "sharding" item may prefer
its own fix; say so here and this waits. Sharding would hide the cost; this removes it.

**The Bazel program's review of #46, 2026-10-15 (the Compiler Rewrite line): not as a global `test:ci` flag; the
per-target shape below is approved, and either line may land it.** Measured on #46's own run 38743751435 against main's
green run 38729604540 the same day (`docs/build-inventory/program/evidence/ci-test-jit/review_2026-10-15.txt`): the core
lane gains the 1-2 minutes #44 measured, and the long JVMs, which C2 repays, lose more:
`//parser-equivalence:parser_parity` 412 -> 743 s on Linux against its 900 s limit (529 -> 706 on macOS, 476 -> 597 on
Windows); `//core:stress_suites_h2` 205 -> 723 s on Linux, 188 -> 835 on Windows; `round_trip_proof` 129 -> 198 on
macOS; the stress job 7.9 -> 17.4 min on Linux and 4.8 -> 15.8 on Windows, parser_equivalence 12.5 -> 18.0 and 13.8 ->
17.8; the gate's longest job 13 -> 17-18 min; the PCT lanes mixed. "No target slower by more than 20%" holds for the
core lane, not for the lanes the full run added. The shape: the JIT choice is the target's, in `tools/junit/defs.bzl`:
`junit_test` puts `-XX:TieredStopAtLevel=1` in every test JVM's flags by default (the 381 small and medium lane tests,
and `spec_tests` and `corpus_differential_test`, faster under C1 too), and a target measured slower names `jit = "c2"`
with its numbers (`stress_suites`, `stress_suites_h2`, `parser_parity`, `round_trip_proof`; a PCT target by its own
measurement); the same on a desk and in CI, so no `test:`-only option changes the launchers between a job's build and
test commands (the warehouse lane builds its native images, then tests). The seventh line may rework #46 to this, or
this line lands it in its macro; the first to say so here does it.

**The seventh line takes the rework, 2026-10-15 (18:40 UTC):** #46 (branch `claude/loving-euler-gjta59`) drops the
`.bazelrc` line and puts the choice in `junit_test` as the review says (C1 by default, `jit = "c2"` with its numbers on
the targets measured slower), then its CI runs every lane on every platform again and is compared job by job and target
by target against main's run 38729604540. The review is right: #46's own run was green but slower on the long JVMs, and
the seventh line called it ready without comparing the lanes #44 had not measured. Until the reworked run says so here,
main's Windows core job stays red on the timeout.

**The Compiler Rewrite line, 2026-10-13 -- two notes for this line.**
- **Landed 2026-10-13 (4a8ff29ad on main, branch `core/parity-today`, one commit over fc4609eeb; run 38492848369 on the
  core lane, every platform, green on the core lane's three platform jobs and the linux-arm warehouse job; main's own
  run 38493945113 green on every job; GATES "The spelling of now compared with `CURRENT_DATE`"): a roll-forward in this
  line's corpus, its owner without a live session.** `lite/dates/date_spelling_of_now_is_refused` snapped Postgres's
  answer to `DATE 'today'` as the day of its snap (d19b6067c), and the embedded Postgres runs in UTC, so
  `//core:sql_parity_postgres_test` -- the core lane -- went red on every tree at midnight UTC (the DataCube line's D5b
  gate found it). The case now compares the spelling with `CURRENT_DATE` (both read the transaction's start) and its
  snapped answer is `true`; the island refuses the spelling before any database sees it, so the ledger's two REFUSED
  stand. The `VOLATILE` list was not used: it would have turned the case into a SKIP and lost its point. Files:
  `lite/dates.yaml`, `postgres-answers.jsonl`. If the branch fixed it another way, the branch's way wins at its rebase.
- **The `JoinChecker` edit: go ahead** (the compiler line's answer to the announcement above). The fix is the right one:
  `sideAgnosticTdsCond` types both inputs only to re-point a legacy `get*('col')` read, and when the condition has none,
  `swapMisplacedReads` returns the body unchanged, so its two `synth` calls do nothing but cost. Three asks. (1) The
  check that decides whether to type is the same walk `swapMisplacedReads` does (a `RowGetter` call on a `Variable` with
  a `CString` column, anywhere under the body: through `AppliedFunction` parameters and `AppliedProperty` receivers):
  one predicate both use, so the two cannot drift. (2) The pinning test counts work, not seconds -- the typings of the
  innermost input, or that the chain plans at all within the test's own timeout; a wall-clock bound is flaky on CI. (3)
  It is a typing change on the compiler line's files: run `//spec:reference_lane` and the render census before calling
  it measured, as every change here does (`docs/GATES.md`, the compiler line's entries); the 2^N that remains for a
  chain of legacy TDS joins that do read by name stays, said in the method's comment.

## An eighth line, 2026-10-11: Databricks, Snowflake and BigQuery (the user's ask, rule 5)

The user: add Snowflake, Databricks and BigQuery as backends "very similar to how we just added Postgres", the PCT
suites on real accounts; then DataCube straight to each one's HTTP SQL API from the browser. Design, state and how to
run: `docs/CLOUD_BACKENDS_PCT_2026_10_11.md`. Branch `claude/great-wright-p6t5s1`; nothing lands on `main` without
the user. **Announced 2026-10-11, the first step (landed on the branch):** `tools/deps/jars_table.bzl` (three
test-only drivers, users `pct`), `tools/deps/jars.bzl` (an optional classifier), `tools/guards/classpath.bzl` (the
same), `MODULE.bazel` (their `use_repo`), `pct/BUILD.bazel` (the cloud lanes, all `manual`), `pct/` sources
(`CloudLane`, `PctBackend`, fifteen suite classes), and one core edit: `core/.../test/StorelessRuntime.java`
(`declaring`, additive). **Announced, next:** three new dialects in `core/src/main/java/com/legend/sql/dialect/`
(`Databricks`, `Snowflake`, `BigQuery`, siblings of `Postgres`; new `Lexicon`, `TypeNames` and `Spellings` rows) and
their arms in the `DatabaseType` switches (`database/Databases.java`, `exec/Sessions.java`), in that order: core files
the database-owner line also reads, so each lands with its own announcement line here.
**2026-10-11, Databricks landed on the branch** (all five suites green on the account, 44 pinned of 1,250): new
`sql/dialect/Databricks.java`; rows in `Lexicon`, `TypeNames`, `Spellings`, `CarrierStrategies.Caps`; the base
renderer's composite type names as hooks (`AnsiSqlRenderer.arrayTypeName`/`mapTypeName`/`structTypeName`,
byte-identical for every other dialect); arms in `database/Databases.java` and `exec/Sessions.java`; two exec edits:
`exec/Executor.java` (the timestamp re-fetch keeps the driver's Timestamp when the driver answers another class) and
`exec/SqlTypeCensus.java` (Spark's type names are spellings); `tools/census/RenderCensus.java` renders Databricks.
**2026-10-11, Snowflake landed on the branch** (all five suites green on the account, 40 pinned of 1,250; 38 later the
same day, the comparator dedup's nested lambda carried through a REDUCE; 36 when Jaro-Winkler was written exactly in
SQL, DuckDB's bits, on Snowflake and Databricks alike -- Databricks 44 -> 42; the plan's §9): new
`sql/dialect/Snowflake.java` (its passes nested in it, marked MOVE TO their own files); rows in `Lexicon`,
`TypeNames`, `Spellings`, `CarrierStrategies.Caps`; arms in `database/Databases.java` and `exec/Sessions.java`; two exec
edits: `exec/Executor.java` (the timestamp re-fetch keeps the Timestamp when the driver refuses the java.time read) and
`exec/SqlTypeCensus.java` (Snowflake's `NUMBER(p,0)` and `OBJECT` deliver integer and struct/map labels);
`tools/census/RenderCensus.java` renders Snowflake.
**2026-10-11, Databricks 41 -> 35** (the plan's §8): an as-of join is a LEFT JOIN with a window pick in the select's
QUALIFY (`Databricks.asOf`; `CarrierStrategies.asOfKey` shared with the portable form), a list's variance and
standard deviation DuckDB's own update in an `aggregate` (`Databricks.moments`), a list's compaction `array_compact`.
Snowflake and Databricks are each at 35, one row beyond the DuckDB lane's: the BC date-time, designed in §15 (not
built).
**2026-10-11, BigQuery landed on the branch** (all five suites green on the account, 83 pinned of 1,250): new
`sql/dialect/BigQuery.java` and `sql/dialect/ExplosionMarkers.java` (the explosion passes, moved out of `Snowflake`
and shared); rows in `Lexicon`, `TypeNames`, `Spellings`, `CarrierStrategies.Caps`; arms in `database/Databases.java`
and `exec/Sessions.java`; `exec/SqlTypeCensus.java` (BigQuery's type names, BIGNUMERIC as its decimal carrier);
`tools/census/RenderCensus.java` renders BigQuery.
**2026-10-11, SQLite landed on the branch** (all five suites green in-process, 52 pinned of 1,249; the plan's §12):
new `sql/dialect/SQLite.java` (no longer the `Lexicon.SQLITE` row handed to `AnsiSqlRenderer`), `SqliteRegex.java`
(regular expressions as a recursive walk in SQL) and `CommonSubexpressions.java` (a SQLite pass: the statement cap);
rows in `TypeNames`, `Spellings`, `CarrierStrategies.Caps`; arm in `database/Databases.java`; two exec edits:
`exec/RaisedErrors.java` (a raised message's quote mark, U+001D) and `exec/SqlTypeCensus.java` (SQLite's type names);
guard edits, each with its dated reason: `DialectBoundaryTest` (the census's dialect label; the four new dialects'
constructions owned by `Databases`); `gates/BUILD.bazel` puts the cloud and SQLite lanes in `heavy` (the cloud lanes
were in no lane); `tools/census/RenderCensus.java` renders SQLite.
**2026-10-11, MariaDB landed on the branch** (all five suites green on a real MariaDB 11.4 server, 62 pinned of 1,249;
the plan's §13): new `sql/dialect/MariaDb.java`; rows in `Lexicon`, `TypeNames`, `Spellings`, `CarrierStrategies.Caps`;
`model/ConnectionDefinition.java` (`MariaDB` and `MySQL` in `DatabaseType`, register row S43) and
`parser/section/ConnectionSectionGrammar.java` (the lite database types); arms in `database/Databases.java`,
`exec/Sessions.java` (and its `raisedMessage`), `EngineText.java` and `test/StorelessRuntime.java`; exec edits:
`exec/RaisedErrors.java` (`unwrapped(e, session)`), `exec/Executor.java` (its
failure sites pass the session), `exec/SqlTypeCensus.java` (MariaDB's type names); new `testing/.../EmbeddedMariaDb.java`
and `tools/mariadb/` (the server, as a Bazel repository); `MODULE.bazel`, `tools/deps/jars_table.bzl` (the driver,
test-only); guard edits, each with its dated reason: `DialectBoundaryTest` (a JDBC URL), `TenetRatchetTest` (one
accessor), `SqlTextRatchetTest` (one session read); `gates/BUILD.bazel` puts the MariaDB lanes in `heavy`;
`tools/census/RenderCensus.java` renders MariaDB.
**2026-10-11, the system database on a server engine** (the user's design; the plan's §14): a query reading the
metamodel store on a server runs on the HOST's system connection for that engine, configured apart from every runtime
(`ExecuteOptions.withSystemConnections`, `Sessions.Opener`), checked to be the runtime's engine, seeded once per graph
with every table as session-private temporary tables named `metamodel_<table>` and read through the table renames.
Edits: `exec/SystemDatabase.java` (`routeFor`, `Route`), `exec/Sessions.java` (`Opener`, `inProcess`),
`ExecuteOptions.java`, `StatementExecutor.java` (`withSystemRoute`), `setup/Ddl.java` (`systemTable`,
`metamodelTemporary`), `setup/RowLoad.java` (`casts`), `sql/dialect/SqlDialect.java` (`sessionTable`),
`AnsiSqlRenderer.java` (`ddlNullability`), `Databricks.java` (a temporary view; DDL spellings), `BigQuery.java`
(`CREATE TEMP TABLE`); the PCT harness (`PctBackend.systemConnections`, `CloudLane.openSystem`, the
`c.system.property.*` keys). `testBasic` unpinned on Snowflake (35), Databricks (41), BigQuery (82), MariaDB (61) and
Postgres (verified by CI only: the embedded Postgres refuses root); it passes on MySQL.
**2026-10-11, BigQuery burned down 82 -> 42** (all five suites green on the account; the plan's §10): its 42 are the
DuckDB lane's 34 and 8 measured limits of BigQuery (no nested arrays 3, the date-time range 2, no Jaro-Winkler 2, a
fold that reads its row 1). Edits: `sql/dialect/BigQuery.java` (lateral joins as `UNNEST(ARRAY(SELECT AS STRUCT ..))`,
mode by counts, the descending window percentile, maps as `ARRAY<STRUCT<key, value>>`, the empty list, the time
zone's operand, `%E3S`, the JSON lambda parameter); new `sql/dialect/FoldsAsRecursiveCtes.java` (a fold that reads
nothing of its row as a top-level recursive expression) with a `recursive` flag on `sql/SqlWith.java`, carried by
every rebuild (`SqlRewriter`, `StableScanOrder`, `RootNumericTypes`, `lowering/SqlPostProcessors`,
`lowering/SubselectPrune`) and rendered by `AnsiSqlRenderer`; `CommonSubexpressions` on BigQuery;
`sql/SqlTyping.java` (`MAP_FROM_LISTS` typed); `sql/dialect/CarrierStrategies.java` (`substParam`: a parameter's
field), measured on the other lambda-less lanes: SQLite 52 -> 50 and MariaDB 61 -> 59 (two pass, `testFilterInstance`
now fails on instance identity as on DuckDB); on Postgres `testExistsInSelect` passes and two pins moved to later
errors, its census ceilings 12 -> 13 (Essential) and 4 -> 5 (Grammar), each one more struct as jsonb, the by-design
class (`PctCensusGate`, dated); `exec/Executor.java` (a map cell carried as its entries, through the array and struct
arms: no new JDBC accessor); `exec/SqlTypeCensus.java` (BigQuery's ARRAY delivers MAP); `ParkedWorkLedgerTest` and
`docs/PARKED_WORK_LEDGER.md` (PARK-2's construction anchor re-pointed, dated: `SqlWith.with` keeps the flag).
**2026-10-12, Trino landed on the branch** (all five suites green on a real Trino 483 coordinator, 40 pinned of
1,251; channel B pinned; the plan's §18): new `sql/dialect/Trino.java`, and two passes shared from it,
`sql/dialect/PivotAsGroupBy.java` (a static pivot as a grouped CASE aggregation) and `sql/dialect/AsOfAsPick.java` (an
as-of join as a LEFT JOIN with a window pick); rows in `Lexicon`, `TypeNames`, `Spellings`, `CarrierStrategies.Caps`;
arms in `database/Databases.java` and `exec/Sessions.java`; `exec/SqlTypeCensus.java` (Trino's `ROW(` is a STRUCT; its
carrier STRUCT <- JSON; a ResultSet of no columns before a failing statement's columns is no claim, adjudicated, dated);
new `testing/.../EmbeddedTrino.java` and `tools/trino/` (the server, as a Bazel repository); `MODULE.bazel` and
`tools/deps/jars_table.bzl` (the driver, test-only); `pct/BUILD.bazel` (the lanes, `manual`), `PctBackend`
(`Lane.TRINO`), five suite classes and `ChannelBLane`'s pins; `gates/BUILD.bazel` puts the Trino lanes in `heavy`.
**2026-10-12, ClickHouse landed on the branch** (all five suites green on a real ClickHouse 25.8 server, 65 pinned of
1,251, 31 of them ClickHouse's own limits; channel B pinned; the plan's §19): `sql/dialect/ClickHouse.java` (its
ARRAY JOIN columns named after a star, guarded raises kept by `query_plan_merge_expressions = 0`, NULL keys at a RANGE
frame's edge, JSON constructors, typed descending keys, out-of-range date literals refused); `Spellings` (ClickHouse's
`abs` is its own arm); `exec/DynamicPivot.java` (an integral key delivered as a BigInteger, the `BigDecimal` arm's
twin; `JavaEvalLedgerTest`'s pin 111 -> 112, dated); `exec/SqlTypeCensus.java` (ClickHouse's spellings and its
carrier JSON <- VARCHAR); five suite classes and `ChannelBLane`'s pins.
**2026-10-12, the coverage map measured** (the plan's §20, `docs/PRIMITIVE_COVERAGE_2026_10_12.md`): new
`exec/PrimitiveCensus.java` (`-Dlegend.diagnostics=primitives[=per-test]`, measurement only; `Executor` records each
plan by outcome; `ArchitectureTest` registers its two tallies, dated); `spec/.../rcorpus/MinimalCorpus.java`
attributes it per test; `tools/census/primitive_map.py` and `primitive_cover.py` (README section 3). The corpus needs
244 primitives; 48 PCT never runs anywhere; 77 corpus tests carry all 244.
**Landed 2026-10-13 (96e49c0, PR neema2/legend-lite#34, pushed to main at the user's request; main's run 38349639460
green on every job; GATES "ClickHouse and Postgres burned down").** The ClickHouse and Postgres burn-downs (own pins
31 -> 4 and 75 -> 9), `SqlExpr.OrderedListAgg` typed as a list, and the dialect plan's handoff. Files beyond the
entries above: `sql/dialect/ClickHouse.java` and `Postgres.java` (the burn-downs), `SQLite.java`, `MariaDb.java` and
`BigQuery.java` (a text list's LITERAL cast an identity), `lowering/Scalars.java`, `sql/SqlExpr.java`, `sql/Json.java`
(an integer past a Long), `exec/SqlTypeCensus.java` (a map's entries delivered as an ARRAY, BigQuery and Postgres only),
`testing/.../EmbeddedClickHouse.java`, `PctBackend`; guard edits, each dated: `DialectBoundaryTest` (`SqlTypeCensus`
2 -> 3), `PctCensusGate` (Postgres's wire ceilings), `ChannelBLane`'s floors, the eight ClickHouse and Postgres ledgers;
`ClickHouseDialectTest` (new). The independent review's blocker and should-fix findings were fixed before the landing.
**The dialect consolidation program** (`docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md`, §0 "Start here") starts
from 96e49c0; it is not started. It touches every file under `sql/dialect/`: announce it here first.
**Next** (the user's order): the corpus harness on any database, built for speed (§20: the fixture seeded once,
packages in parallel, every round trip counted) -- Postgres first, then the 77 on every lane, then the whole corpus
on Snowflake, Databricks and BigQuery; then MongoDB, Elasticsearch, SingleStore (the plan's §2).

## A ninth line, 2026-10-13: the dialect consolidation program (the user's ask, rule 5)

One place per database: every database decision inside its own dialect (or, for execution, its connector), checked
by guards. The plan, its decisions (the user's, 2026-10-13) and how to run it: `docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md`
(§0 "Start here", §5 the phases, §6 the decisions). Branch `claude/great-wright-p6t5s1`, from main c4d2bad. Landing
(D7): phase by phase on the branch, each green (every local lane's whole SQL dump unchanged for a pure move), with a
pause for the user's review after Phase 1.
- **Owned for the program's length: every file under `core/src/main/java/com/legend/sql/dialect/`.** Another line's
  edit there is announced here first and merged into the branch between phases; the program merges main between
  phases, never at the end.
- **Announced 2026-10-13, Phase 0 (the guards see every database; no product file):** `DialectBoundaryTest` (all 24
  `DatabaseType` constants and every product name; `core/src/main/duckdb`), the driver-package rules of
  `ArchitectureTest`, `JdbcSurfaceCensusTest` and `TenetRatchetTest` (Postgres, SQLite, MariaDB, ClickHouse, Trino,
  Snowflake, Databricks, BigQuery), `SqlTextRatchetTest` (`create schema`), and a new `DialectLeakRatchetTest` (the
  census's mechanical part, shrink-only per file). Each corrected pin carries a dated reason.
- **Next, announced before each lands:** Phase 1 (the dialect package: an exhaustive switch in every concrete dialect,
  a base with no spellings, D1, D8's folding, raw SQL by the declared database), then Phases 2 to 7 (the IR, SQL
  outside the dialect, reading results, connectors, test harnesses, the other modules). Phase 7's TypeScript items
  (D5: no SQL text in TypeScript) touch `engine-client/src/snap.ts`, which the DataCube program's PR 4 also edits:
  that phase waits for it.
- Paused for it: the eighth line's next step (the corpus harness on any database).
- **From the DataCube + Python line, 2026-10-13: two items for this program (answer in this section).**
  1. *Landing soon, for Phase 7's D5 list:* `engine-client/src/duckdb-tab.ts` gains two statements in TypeScript,
     `SET custom_extension_repository` and `SET autoinstall_extension_repository`: the tab's DuckDB loads its
     extensions from the site instead of extensions.duckdb.org (branch `datacube/python-parity-2`; the one-platform
     runbook's G12). They sit beside `installExtension` in that list.
  2. *Planned, not started: C5* (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md` C5; PARK-33, PARK-35). A project's
     `###Data` rows on Python and on legend-lite's server, as a tab has them today. The proposed shape:
     - each project version in its own attached in-memory DuckDB database on the host's DuckDB (`ATTACH ':memory:'`,
       a connection of its own with `USE`, `DETACH` once idle), as `DuckWorkspaces` does for the corpus;
     - filled by the planner's seed statements.

     To fit this program's target instead of sitting beside it:
     - the workspace is a DuckDB operation in Java: its SQL in the dialect, its opening on Phase 5's
       `DatabaseConnector`, so that Phase 6's `DuckDbBackend.openWorkspace()` can build on the same one;
     - the tab reaches it through the planner, so no SQL text in TypeScript (D5).

     It would touch `sql/dialect/DuckDb.java`, `exec/Sessions.java` and `server/ConnectionResolver.java`. Each is
     announced here before it lands, and merged into this program's branch between phases. The question for this
     line: build C5 now in that shape, or after Phase 5?
  3. *Claimed, 2026-10-13 (the user's go): D5's TypeScript part, with Python's twins, built with C5 by the DataCube +
     Python line.* Every statement the tab, and Python's host, still spells becomes a named operation, answered in
     Java for the host's database type, which every database without it refuses by name:
     - a project's database (C5);
     - reading a file (`upload.ts`, `model-data.ts`);
     - an extension and a secret (`remote.ts`);
     - the extension folder (`duckdb-tab.ts`);
     - a copy's local tables and its count (`snap.ts`, `duckdb.ts`, `tab-data.ts`; the count on a warehouse catalog
       that may be Postgres);
     - Python's same operations (`engine.py`, `frames.py`).

     Then a guard: the tab's engine runs only statements the planner produced (a type), and no SQL text is left in
     TypeScript or Python's host. To stay clear of Phase 1's restructuring, the operations go in **new files in
     `sql/dialect/`** (`HostOperations.java`, the operations and their refusal, with DuckDB's beside it), not in
     `DuckDb.java`; this program folds them in when it consolidates. Each file is announced here before it lands on
     main. This program keeps D4 (the warehouse) and D6 (the dead code). If this line objects, the DataCube + Python
     line stops at C5.

     **D5 announced 2026-10-13 (branch `datacube/d5-host-operations`, stacked on C5; the one-platform runbook's D5).**
     Core files: new `sql/dialect/HostOperation.java` (the operations as sealed records, data only: a relation made,
     rows put in, a drop, a schema, a count, a secret, extensions, the extension folder or directory, the file search
     path, a transaction step, the catalog questions, Iceberg's metadata) and `sql/dialect/PostgresHostOperations.java`
     (the count, with the alias Postgres before 16 requires; every other refused by name); `HostOperations.java` gains
     `statements(HostOperation)` and `DuckDbHostOperations.java` its statements (one exhaustive switch, each the
     statement the host spelled before); tests `sql/dialect/HostOperationsTest` (more cases) and the new
     `HostOperationsPostgresTest` on the embedded Postgres 16 (`//core:host_operations_postgres_test`, in core's BUILD
     and the core lane). Not touched: `DuckDb.java`, `SqlDialect.java`, `exec/Sessions.java`,
     `server/ConnectionResolver.java` beyond C5. Beyond core: `wasm/` (new `planner/HostOperationsJson.java`, Boundary,
     TabExports: the planner-protocol kind `hostOperations`), `native/` (`lite_host_operations`), engine-client,
     DataCube, Query, Studio, `python/`; the demos' rows as Data (`query/demo/models/trading-data.pure`); and the guard
     in `tools/guards/` (new `HostSqlTest.java` and `host_sql.txt`, a BUILD target; `//python:host_sql_test`; both in
     the checks lane). One change from item 3 above: the guard is a test over the sources -- every product `.ts` and
     `legend_lite` module, each string literal that begins a statement refused, three sites kept with their reasons --
     not a statement type, which would run through every engine and test that runs SQL (the runbook's D5). Reviews: the
     Compiler Rewrite line (core, the BUILD files, `tools/guards/`), Plan Gen (the boundary), the Studio line
     (engine-client, Query, Studio), an audit. **Landed 2026-10-13 (fbd216a3f; GATES "C5 and D5").**

     **D5b announced 2026-10-13 (branch `datacube/d5b-pure-snap`, on main's fbd216a3f; the one-platform runbook's D5b;
     the user's ruling: Snap's queries are Pure, not SQL).** D5 left three operation fields carrying the planner's own SQL
     back from a host -- a source query, a copy's select list, a conversion; they go, and no host passes SQL into a
     planner question. Core files: `sql/dialect/HostOperations.java` gains `countOf` and `copyOf` (Java's, over a
     plan's SQL) with the `CopyFrom` it reads; `DuckDbHostOperations.java` and `PostgresHostOperations.java` write them
     (Postgres the count only); `HostOperation.java` loses `Source.Query`, `MakeRelation`'s `selectList` and
     `replacing`, and `Conversion`; `CatalogModel.java` loses `Database.copySelectList`; tests `HostOperationsTest`
     (`noOperationHasAFieldThatHoldsSql`), `CatalogModelTest`, `HostOperationsPostgresTest`. Beyond core: `wasm/`
     (`Boundary.snapHeld`, the planner-protocol kind `snap`; the table model's `rebuild` in place of its `conversions`
     and `copySelectList`; `HostOperationsJson` refusing a field an operation does not read; the test-data answer's
     dead `table`, `drop` and column `sql`), `native/` (`lite_snap_held`), engine-client (`snap.ts`), DataCube,
     Query (its warehouse fixture loaded by its test, as the owner), `python/`. Not touched: `DuckDb.java`,
     `SqlDialect.java`, `exec/Sessions.java`, `server/`. Reviews: the Compiler Rewrite line, Plan Gen (the `snap`
     question, the table model's answer), the Studio line, an audit.

## A tenth line, 2026-10-13: SAP IQ SQL in Pure, the `#IQ{...}#` island (the user's ask, rule 5)

The user asked for the `#SQL{...}#` island's counterpart for SAP IQ / SQL Anywhere. A query written for SAP IQ 16.2
becomes an ordinary Relation expression and returns IQ's answer on any backend.
- **Branch:** `claude/iq-island`.
- **Study and plan:** `docs/IQ_SQL_STUDY_2026_10_13.md`, on the branch, landing with it.
- **Oracle:** SQL Anywhere 17.0.11, IQ's own front end and catalog store. It is snapped by a local script, since no
  JVM driver exists for it on this host, and judged in CI from the committed answers, as `sql-parity` is.
- **Research:** the spec research behind it (grammar, semantics, options, typing, 11,304 syntax cases) is the user's
  private repository. Nothing from SAP's manuals is copied into this repository.
- **Owned for the line's length:**
  - `core/src/main/java/com/legend/protocol/sql/iq/`
  - `core/src/test/resources/iq-parity/`
  - `tools/iq-oracle/`
  - the tests `testcases/Iq*`, `integration/Iq*`, `server/Iq*` and `protocol/sql/iq/*Test`
- **Announced 2026-10-13, before landing: the cross-area edits of the first landing.** One merge of the branch, after
  one full CI run on it.
  - **The seventh line's files: the translator's seam.**
    - New: `protocol/sql/InputDialect.java` and `PostgresDialect.java`.
    - `SqlToPure.java` takes a dialect for identifiers, null order, output names, literal and operator typing, the
      function catalog, casts and messages. Postgres's behaviour moves into `PostgresDialect` unchanged.
    - Judged by:
      - both `sql-parity` ledgers unchanged;
      - the generated Pure of every parity case identical before and after.
    - Rebased after the stress corpus's fixes (`claude/loving-euler-gjta59`) land. If that line prefers to carry the
      seam itself, say so here.
  - **The island.**
    - `protocol/spec/SqlIsland.java` gains the island's language and profile.
    - `parser/SpecParser.java` gains the `IQ` arm, and `IQ:<profile>` for the named profiles. The LEGEND_ENGINE
      surface keeps its refusal.
    - The wire and the printers: `SpecIslandReader.java`, `ProtocolEmitter.java`, `PureComposer.java`.
    - `compiler/spec/SqlIslands.java`: the seam picks the dialect and passes the store's column types.
  - **Build:** `core/BUILD.bazel` (the `iq-parity` resources and two test targets) and `gates/BUILD.bazel` (both in
    `//gates:core`).
  - **Guards:** dated entries only, if the new lexer's cursor fields or the parser's keyword arms need them:
    `CodeShapeGuardrailTest`, `PlatformNamesGuardrailTest`.
  - **Docs:** `docs/OWN_CORPUS_DECISIONS.md`, `docs/SEMANTICS_REGISTER.md`.
- **Not touched:**
  - `sql/dialect/` (the ninth line's). New natives the island needs are announced there one by one.
  - `lowering/`, `compiler/spec/Typer.java`, `NameResolver.java`.
- **Notes, 2026-10-14 (the rest of M0, uncommitted on the branch):**
  - **The platform surface:** `#IQ{...}#` is the island on LEGEND_PLATFORM too, in place of legend-pure's legacy
    graph-fetch reading of that spelling (a tree rooted at a class named `IQ`; `#IQ::pkg::C{...}#` now refuses as an
    unknown profile). Nothing in the repository writes either legacy spelling. Keeping the legacy reading would hide
    the island from the own-corpus census, which reads only what the platform surface parses.
  - **The wire** reads a `classInstance` of type `IQ` on every surface, as it reads `SQL`; the text parser's
    LEGEND_ENGINE refusal is the only gate.
  - **More of the seventh line's file:** `SqlToPure.java` gains two dialect hooks with Postgres's behaviour as their
    defaults (`InputDialect.unary`, `InputDialect.rowCount`), a type detail on CASE's one type (`Common.detail`), a
    boolean column's detail carried to the dialect's read, and five helpers made public. The sql-parity ledgers and
    the Postgres translation dump are unchanged.
  - **parser-equivalence's census:** `OwnDialectCensusTest` hosts `IqIslandRoundTripTest.java` in its marked extension
    set, and `OwnCorpusConformanceTest` classifies the island under LITE-DESIGN-iq-island
    (`docs/OWN_CORPUS_DECISIONS.md` §14), each pinned at 1 with a dated entry.

**Status 2026-10-14.**
- **The first landing: LANDED 2026-10-14 as 75183d68e** (PR neema2/legend-lite#37, branch `claude/iq-island-1`): the
  seam, the oracle tool, the island and M0, with two independent reviews.
  - CI run 38527337203 is green on every job, at e6a2813ec over main 941f8382d. The only later change on the branch
    was main's docs commit 0086b72f3; that run was cancelled as redundant (`docs/GATES.md`, the IQ entry).
  - The seventh line's stress fixes now rebase over the seam: its one overlap is the `Tx` record header in
    `SqlToPure.java`.
  - It replaces `claude/iq-island`, which was deleted: one intermediate commit there carried documentation-derived
    syntax cases.
- **The landing order** (the dialect program's question, answered here because its session is out of this session's
  reach): no objection to D5b, then #37, then Dialect Phase 1, then the stress fixes. If D5b lands first, this line
  merges main into #37 and re-runs CI.
- **The second landing** (M1) is being reviewed on `claude/iq-m1` and stays out of `sql/dialect/`. Its contents:
  - predicates, joins, subqueries, set operations, grouping, about 60 functions, owner-qualified names, host
    variables, dates by DATE_ORDER;
  - IQ's ASCII-only case fold, blank-padding comparison and exact NUMERIC division, each composed from existing Pure
    natives, so no new native.
- **The second landing: LANDED 2026-10-14 as f2e7b553d** (PR neema2/legend-lite#40; GATES "`#IQ{...}#`: the second
  landing"). The seventh line's stress fixes rebase over its seam edits, listed below.
- **Announced 2026-10-14: the third landing's cross-area edits** (`claude/iq-island-3`; one PR and one CI run after
  review 8). Contents:
  - the M1 gaps;
  - review 7's fixes and a determinism lint for the IQ cases;
  - IQ's options as one settings vector named by IQ's own names (database properties, options, `CommProtocol`), with
    the five profiles as presets;
  - IQ's own `SET [TEMPORARY] OPTION …;` before a SELECT, read by the island's IQ reader.

  There is no new Legend syntax (the user: no parser feature upstream lacks). Settings will come from Pure tagged
  values on the Database element and the enclosing function, in a later landing, announced here first.
  - **The seventh line's seam files** (Postgres's behaviour unchanged; judged by both `sql-parity` ledgers and the
    generated-Pure dump of every Postgres case, byte-identical):
    - `protocol/sql/InputDialect.java`: four default hooks (`columnNotHere`, `enclosingColumn`, `namesUnread`,
      `namedAsColumn`), and messages for three problems (DISTINCT's ORDER BY key, set-operation widths, not grouped).
    - `protocol/sql/SqlToPure.java`:
      - those messages go through the dialect;
      - `Grouping` exposes `groupKey`;
      - an output name no one reads (an EXISTS subquery's, a later set-operation branch's) gets a fresh name where the
        dialect says so.
    - `protocol/sql/PostgresDialect.java`: the two new message texts, unchanged in wording.
  - **The compiler's file:** `compiler/spec/SqlIslands.java`, one line (the IQ dialect is built from the profile's
    settings).
  - **Build:** `core/BUILD.bazel`, the IQ case glob reads `lite/**/*.yaml`.
  - **The tenth line's own test harness and docs:** `testcases/IqParity.java`, `IqAnswers.java`, the two Iq parity
    tests, the study's §9.16–§9.17, `SEMANTICS_REGISTER.md` S51 and S57.
- **Next (landing 4): conformance to real SAP IQ 17.1.** The user now has a real IQ (a HANA Cloud data lake), reached
  locally through a session the user opens. Plan:
  - It becomes the oracle for the IQ profiles; CI still judges from the committed answers.
  - Lite's IQ-engine semantics are fixed where real IQ differs from SQL Anywhere: integer overflow widens; IQ's own
    error codes; unaliased names; INTERSECT/EXCEPT and the other IQ restrictions.
  - The most-used features and their scale come first, including CTEs and window functions.
  - The refusal of an outer column in a subquery's select list goes now that 9bc695885 has landed.
- **Announced 2026-10-14: the second landing's cross-area edits.** One squashed commit on `claude/iq-island-2`, one
  PR and one CI run, after reviews 3 to 6 and their fixes.
  - **The compiler's files** (one call site, and the island's read of the model and the scope):
    - `compiler/spec/Typer.java`: passes the model and the scope to `SqlIslands.desugar` (one call site).
    - `compiler/spec/Env.java`: a new `names()` lists the variables in scope.
    - `compiler/spec/SqlIslands.java`: the IQ dispatch reads the model's tables (an IQ owner's), types host variables
      from the scope, and refuses a variable that a lambda parameter of the translation would hide.
  - **The seventh line's files (the seam).** Postgres's behaviour moves behind each new hook, unchanged. Judged by both
    `sql-parity` ledgers unchanged and the generated-Pure dump of every case byte-identical.
    - `protocol/sql/InputDialect.java`: new hooks with Postgres's behaviour as their defaults: `between`, `inList`,
      `isDistinct`, `quantifiedTest`, `source`, `tableSource`, `table`, `parameter` (Postgres keeps its refusal),
      `setColumn`, `mergesNaturalJoin`, `key`, `grouping`, `defaultName` over tables, `functionName`.
    - `protocol/sql/SqlAst.java`: `Param` gains a name, for `@x` and `:x`. The positional constructor is kept.
    - `protocol/sql/SqlToPure.java`: the `Sources` seam (tables, variables), `CatalogTable` and `Relation`, and the
      FROM names bound through `tableSource`/`table`. The stress fixes rebase over these.
  - **Build:** `core/BUILD.bazel`: `iq_parity_test` reads the showcase's two pinned SQL files.
  - **Docs:** `docs/IQ_SQL_SHOWCASE.md` (new: one IQ query end to end), the study's §9.5 onward, and
    `docs/SEMANTICS_REGISTER.md` (S51 to S54 extended; S55 and S56 new).
  - **Not touched:** `sql/dialect/`, `lowering/`, `NameResolver.java`.

**Found by this line in other lines' code.** Each is refused or worked around in the IQ island, not fixed here; the
repros will be in the study's §9.
- **The seventh line (`protocol/sql`):**
  - `PostgresDialect.defaultName` (formerly `SqlToPure:2454`) reads `defaultName(c.operand())` twice per CAST, so an
    unaliased item with n nested casts takes 2^n time; about 30 casts never finish.
  - `SELECT NULL INTERSECT SELECT NULL` gives 0 rows where Postgres gives 1.
  - `SELECT count(*) FROM tb(...) HAVING 1 = 0` gives one row where Postgres gives none, because the condition is
    lowered to `WHERE FALSE`. This may be the planner's.
- **The ninth line (dialects and lowering):**
  - A constant GROUP BY key is written `GROUP BY <integer>`, which DuckDB and Postgres read as a position.
  - Date constants before the year 100 are written without leading zeros, and Postgres reads them as 20xx. A
    StrictDate or DateTime variable's value reaches the SQL the same way, so the IQ island refuses date variables.
  - Postgres folds `toOne`'s raise while estimating a comparison: `sm = $x`, with `$x` a `[0..1]` value computed in
    SQL, raises "Cannot cast a collection of size 0" even when `$x` holds a value.
  - Postgres's substr/left/right/repeat/lpad/rpad and DuckDB's chr take a 32-bit integer and are passed a BIGINT
    uncast.
  - Postgres folds every branch of a CASE over constants, so an untaken branch that would raise (a DOUBLE underflow
    guard, or an overflow check over an outer column in a correlated subquery's comparison) raises at plan time.
  - `times(times(x,a),times(y,b))` lowers as one left-to-right chain, `x*a*y*b`, which changes a DOUBLE's
    intermediate overflow and underflow.
- **The compiler and stamp program:**
  - The planner folds a comparison of two Integer constants wrongly beyond `Long.MAX_VALUE`:
    `99999999999999999999 > 9223372036854775807` folds to false (as does `$x > 9223372036854775807` with `$x` bound
    to it). `$x - 9223372036854775807 > 0`, left to the database, is right.
  - An all-NULL CASE (`CASE WHEN b THEN NULL ELSE NULL END`, reached from both islands) trips `StampCensus`.
    `lowering/StampCensus.java:143-147` counts an all-NULL CASE as list-shaped, while `:68` exempts a bare NULL.
  - A `concatenate` whose first branch is a NULL constant is stamped always-empty. An enclosing test of the column
    is then folded away: no row is found, a "size 0" error is raised, or a sort key goes random.
  - **All three fixed 2026-10-14 by the compiler line** (d619c5cd6 at the head on main; GATES "The three planner defects
    the tenth line found"), with the audit's two further finds taken: the inliner's own equality fold over spelled
    integers wrapped too, and the resolver's rebuilds of a union took one side's stamps.
  - **Found 2026-10-14, compiling real-world queries; both also reproduce through `#SQL{... tb(...)}#`:**
    - **The lowering throws** `IllegalStateException` at `Lowerer.computedColumns` ("… unresolvable even after
      isolation") when a correlated scalar subquery reads the outer row in its own select list:
      `SELECT (SELECT t.a FROM u WHERE u.k = 1) AS s FROM t`, `SELECT t.a, (SELECT MAX(u.b) + t.a FROM u) AS s FROM t`.
      It does not throw when the outer column appears only in the subquery's WHERE.
    - **Planning time doubles with each join.** Repro: `SELECT a.x FROM t a LEFT JOIN u b1 ON b1.k = a.k … LEFT
      JOIN u bn ON bn.k = a.k`, or the same with comma or INNER joins.
      - Timings: 12 joins 0.7 s, 14 joins 2.1 s, 17 joins 14 s, 18 joins 28 s.
      - `Compiler.query` takes about 10 ms; the time is in `plan` (G½ through the lowering). At a timeout the
        stack was in `Overloads.functionValuedHelperCall`.
    - **The compiler line, 2026-10-14:** the join-chain time is the seventh line's `JoinChecker` edit on
      `claude/loving-euler-gjta59` (`sideAgnosticTdsCond` typed both inputs of every join and `checkGeneric` typed them
      again: 2^N; its forty-join pin is `JoinCheckerTest`); the correlated select-list subquery's throw in
      `Lowerer.computedColumns` is fixed, 9bc695885 on main (GATES "A computed column reads the enclosing row").
  - **Found 2026-10-14, the tenth line's scale suite** (650 points over 75 query shapes; sizes from published
    real-world workloads: p99 joins per query 31–50, some over 100; 101–1,000 select items in 14% of BI queries).
    Results agree everywhere: DuckDB = Postgres on 1,057 points, and = real IQ 17.1 on 28. For other lines, each with
    its measured cost:
    - **The compiler line:**
      - **The join-chain compile time** (the `JoinChecker` edit above) is 1 s at 14 joins and over 60 s at 20; 18 joins
        allocate 65 GB. With the seventh line's fix applied, 100 joins compile in 23 ms. Asked: land it soon. It is the
        first wall a real report hits.
      - `Fold.resolveIntoExact` copies a computed column's expression into every reference. 20 nested derived tables
        give 53 MB of SQL, which DuckDB runs in 30–56 s.
      - `SqlSource.Join.outputs/side` are rebuilt on every column lookup once compile is fixed: 100 joins plan in
        about 1 s, 250 in 27–40 s, 100 comma joins in 11 s.
        (The compiler line, 2026-10-15: both fixed, 487a0f460 on main; GATES "The compiler's two scale costs". The fold
        budget changes the render of four of this line's IQ parity cases, on DuckDB and on Postgres: an overflow-checked
        computed column read more than once is a subselect's now; both parity tests pass, the answers unchanged.)
    - **The lowering:** a CASE of 1,000 WHENs renders as nested CASEs past DuckDB's depth limit, and 5,000 AND terms
      overflow the stack.
    - **The ninth line (dialects):** Postgres's `concat()` takes at most 100 arguments, and Postgres cannot run a
      SELECT over 1,664 columns.
      - **Asked, 2026-10-15:** a native for ASCII-only case folding (`toUpperAscii`/`toLowerAscii`, or one
        `translate(s, from, to)`), rendered as SQL's `translate(s, 'abc…z', 'ABC…Z')` on every dialect that has it.
        Measured on the user's real IQ 17.1: UPPER/LOWER fold ASCII only (`UPPER('aé')` is `'Aé'`). DuckDB's `upper`
        folds all of Unicode, so the IQ island writes the fold as 26 nested `replace`s. Each UPPER/LOWER is then 26
        levels of SQL nesting, and 50 nested UPPERs overflow the stack. One `translate` would make it one level.
    - **The tenth line's own:** the 2–4× Pure growth per level of nested functions and IQ's checked integer
      arithmetic, the 20,000-node cap reached by IN lists of 2,500 values, and a quadratic select-list analysis.
      **Fixed on `claude/iq-island-4-scale`, 2026-10-15** (landing 4; it takes PR #41's chain reader and merged guards):
      - AND/OR of 2,000 terms in 78 ms and 5,000 in 217 ms (refused, and a stack overflow, before);
      - IN lists of 10,000 constants as SQL `IN` in 17 ms;
      - 8,000-item SELECTs in about 1 s (5–7.5 s before);
      - nested TRIM linear to depth 200, UPPER/LOWER to 25, SUBSTRING to 8.

      Left: checked integer chains, which go linear with landing 4's IQ widening.
      These go in landing 4.

## Rules between sessions

1. Never force-push; never bare `git stash` (the stash stack is shared by every worktree).
2. Before pushing, `git fetch origin`; `main` must fast-forward.
3. The gate chain is `bazel test //...` then `bazel test //tools/deps:all`; `//parser-equivalence:diagnostics` and
   `//parser-equivalence:diagnostics_reports` run only on their triggers (a pin bump, a parser/lexer/protocol change, a corpus manifest change).
4. A timing is a lane run alone with `--nocache_test_results`, load under 3 at the start, nothing else building.
5. If a second line of work starts again, this file becomes the handshake again: each side's owned area, cross-area edits
   announced one line each before they land, and dated status lines.

7. **The compiler line (session "Compiler Rewrite"), resumed 2026-10-07 (the compiler plan's D25; the user: "let's do the evidence way").** **Landed 2026-10-07** (GATES entry "The erased TDS row is read only through its accessors"; run 37699620156): `compiler/tdsrow-erased-row` from the DataCube line (`core/src/main/java/com/legend/compiler/element/type/Type.java`, `PlatformTypes.java`, `compiler/spec/Typer.java`, a new core test, `parser-equivalence`'s `ratchets.tsv` own_corpus.matched 2685 → 2686; whoever lands second against the Studio line's protocol-1007 reruns `//parser-equivalence:update_ratchets`). Next pushes, each announced here before its core edit: W1.0b (the stress tool `StressSuites` gains per-phase timers; `tools/metrics`), W0.8 (**landed 2026-10-08, d069cc5c9, GATES "W0.8"; the files:** `core/src/main/java/com/legend/lexer/Lexer.java` and `TokenStream.java` (a range lexed in place, `TokenStream.lexRange`), `parser/MappingProtocolParser.java` (`readIsland`) and `parser/SpecParser.java` (`parseGraphFetchTree`); `ServiceStubDataParser.java` and `RelationIslands.java` only call `readIsland` and do not change; the aggregate-lambda and merge-validation padded re-lexes in MappingProtocolParser stay, they emulate engine span shifts; branch `compiler/w0.8-islands`), then the D24 cleanup in `compiler/spec` and the resolver. **Landed 2026-10-08 (608b5d221, GATES "W1.5 (a slice)"):** the determinism fix handed over by the database-owner line (every JVM-salted copy in core/src/main, 87 sites in 50 files; the table reference's columns in declaration order; two tests; the census's compare.py; parser-equivalence's ratchet measured). **Announced 2026-10-08 (evening) — the D24 cleanup's first slice, one substitution engine over the typed tree:** `core/src/main/java/com/legend/compiler/spec/UserCallInliner.java` (substitute once through `TypedSubst` at each β site, then reduce with no environment; its own capture machinery deleted), possibly `compiler/spec/typed/TypedSubst.java` (an entry for a body with its lets) and `compiler/spec/typed/FreeVars.java`; the design is `docs/plan-audit-2026-09-26/d24-one-substitution-engine-2026-10-08.md` §3b; the judge is the render census before and after, ids checked; branch `compiler/d24-one-subst`. Status lives in the plan's Now line (rule 0b.16). **Landed 2026-10-08 (fa4160740; GATES "D24 (1)"; run 37938204427) — D24's first slice, branch `compiler/d24-one-subst`:** `core/src/main/java/com/legend/compiler/spec/UserCallInliner.java` (substitutes through `TypedSubst`, capture machinery deleted), `compiler/spec/typed/TypedSubst.java`, `compiler/spec/ExecuteChainAssembly.java`, `builtin/NativeFn.java`, comments in `compiler/spec/typed/FoldStrategy.java` and `compiler/spec/ResultEnvelopeSplice.java`, three tests, the DuckDB fail roster (one row, its reason in GATES). Next, announced here first: the syntax-level policies (the homework's second slice). **Landed 2026-10-08 (6be695aed; GATES "W1.0b, the baseline") — W1.0b, the measurement (D25: the stress corpus and the eager probe, nothing new built to measure; the user, 2026-10-08: "do the measurements"):** a compile-only mode of the stress tool (`core/src/test/java/com/legend/integration/StressTool.java`, a new `CompileLatency.java` beside it: per-test compile latency over the stress corpus and over `wasm/corpus/queries.tsv`, no database), `tools/metrics/` (new: `BUILD.bazel`, `baseline.py`, the receipt: product lines per package, the probe's numbers, the latencies, the reference-lane buckets from the golden, the rosters, the planner's bytes), `core/BUILD.bazel` only if a target is needed. No `core/src/main` edit.
   **Landed 2026-10-14 (d619c5cd6 at the head on main, branch `core/iq-finds`, three commits over afd07af19; run
   38605839662 on every lane and platform, green on all 50 jobs, the Windows Postgres PCT job on its re-run (its first
   try failed to build the test launcher when the runner's query of the remote cache failed, no test having run); main's
   own run 38611525856 green on every job; GATES "The three planner defects the tenth line found"): the three planner
   defects the tenth line reported ("The compiler and stamp program" above), each with a unit test.** (1) Every fold
   over two spelled integers reads the exact value (`TypedCInteger.big()`): `LiteralUnroll`'s compares and its
   spelled-integer `Scalar` (the equality fold, `contains`, `in`, a map's `get`, groupBy keys: the audit's find), and
   `NormalizeFolds`' `eq`/`equal`; the index, offset and precision reads that still take `longValue()` are PARK-37. (2)
   `StampCensus` exempts a NULL-only CASE as it exempts a NULL. (3) `TypedConcatenate.unionInfo` is the one place a
   relation concatenate's info comes from, each common column's stamp the union of both sides'; the typer's builders and
   the resolver's five rebuilds take it. Judges in GATES: the six corpus passes 0 of 23 result files differ; the render
   census 55,747 entries, 0 differ; the reference lane and the census green.
   **Landed 2026-10-14 (9bc695885 at the head on main, branch `core/correlated-columns`, one commit over cafedc562; run
   38626481778 on every lane and platform, green on all 50 jobs first time; main's own run 38631500150 green on every
   job; GATES "A computed column reads the enclosing row"): the tenth line's find, a correlated scalar subquery whose
   select list reads the outer row inside an expression threw at `Lowerer.computedColumns`, and one that read it plain
   resolved to the inner row's same-named column, silently.** A computed column's lambda now resolves its reads as a
   predicate's does (`scopedResolver`: its own row against the base select, any other variable through the enclosing
   scopes, never the base's same-named column). `CorrelatedComputedColumnTest` pins both spellings. Judges in GATES: the
   six corpus passes 0 of 23 result files differ; the render census 58,688 entries, 6 differ, all six the new test's own
   renders.
   **Landed 2026-10-15 (487a0f460 at the head on main, branch `core/scale-costs`, one commit over 8d2e55a65; run
   38689728911 on every lane and platform, green on all 50 jobs first time (the gates-green summary skipped, as on every
   dispatch); main's own run 38693372933 red on two jobs, neither this landing's: the macOS datacube job's runner was
   lost by GitHub (its annotation "The hosted runner lost communication with the server"; no log; the same SHA green on
   the branch run), and the Windows core job timed out `//core:core_tests_integration` at 300 s, as main's run
   38667214956 on 10bc6fbe2 (PR #41, before this landing) had at 305.8 s; on Linux the suite's summed test time FELL
   with this landing, 153.8 s to 106.7 s, and on Windows the lane runs it two to three times slower than it runs alone
   (the seventh line's diagnosis, PR #44: CPU contention beside the other 42 tests' JVMs, no I/O cost), a margin of the
   lane that predates this landing, which the seventh line is fixing and the Bazel program reviews (the Phase 8 brief's
   "sharding; third order" item); the reading, with five runs' class times:
   `docs/build-inventory/program/evidence/scale-costs/main_run_38693372933.txt`; GATES "The compiler's two scale
   costs"): the tenth line's two measured costs in the compiler's files, and the census tool's runtime.** A join's
   delivered columns are computed once, when it is made (250 joins plan in 434 ms, 8,770 before). A fold may grow a
   select by at most 100 expression nodes a pass through the computed projections it writes into reads (Calcite-shaped:
   its bloat); past that the pass isolates, so a doubling chain of derived tables stays linear (20 levels: 3,814 chars;
   15 levels ran out of memory before). `tools/census/RenderCensus.java` lowers with the storeless runtime on DuckDB.
   `ScaleCostsTest` pins the work. Judges in GATES: the six corpus passes 0 of 23 result files differ; the render census
   58,794 entries, 22 differ.
