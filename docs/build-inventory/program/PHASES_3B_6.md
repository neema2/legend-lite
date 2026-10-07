# Phases 3b and 6: the execution brief (build rebuild program)

**Applied on 2026-10-07, after this brief was written** (so a reader does not redo them): the plan's Phase 3b now
carries item 1b's cause (`shadows` comparing spellings, `superMapping`, 14 versions by a text count, F-L1's caveat
and file), item 4's two routes and the PROPERTY_AS_CALL row, item 5a's crash site and item 5b's import-scope bug
(confirmed in the code: imports keyed by full name in `Compiler.java` and `NameResolver.java`), and "Small: days" to
be sized with the user; its Phase 6 carries what experiment 8 did not test (added, not replaced; 27 repositories for
the corpus's own closure; the 4 GB heap; the throwing guard), the host-compared register, `;` as legend-pure treats it
and `m3.pure`, and F-L1 and `routeFunction` owned by 3b. The open decisions themselves are not decided.

**After the cold read (`COLD_READ_2026_10_07.md`):** 3b-O1 is the one owner of the "platform's own Pure" row kind
(the plan's decision 1 and Phase 4's D4-4 point here). 3b-O2 is partly answered by the plan (multi-file projects meet
the import-scope bug and Phase 6 needs it fixed): the plan implies option (a); confirm with the user. Phase 6's E-4
(the H2 register entry) is open, not agreed: it is 6-O7. `//spec:manifest_world_census` is expected to fail its
ceilings today (load walls 37 > 32, failing bodies 1,476 > 1,447; `PHASE_8.md` Short-21) and has no owner (Meas-9).
Line numbers in files the fix commit changed moved (in `ParkedWorkLedgerTest.java`: PARK-6 now :84-96, PARK-12 :121-123,
PARK-14 :127-130): cite anchors by name. C.1 step 5's "reflection rows renumber their function ids" is a difference in
the SQL the executor sends (seen with `-Dlegend.diagnostics=dump-sql`), not in the 22 result files, which must be
identical. The Phase 3 statements this brief found wrong (§3b.9 items 7 and 8: GATES on the 58 and on `routeFunction`;
`Typer`'s comment; PARK-12's 7 files) are corrected in Phase 3's fix commit. Phase 3b branches from `origin/main`.


Written 2026-10-07 for a fresh session that has none of the conversations behind the plan. It covers the plan's
Phase 3b ("what the bump and users need from the compiler") and Phase 6 ("the corpus on its real manifest"). The plan
stays the authority (`docs/REBUILD_PROGRAM_2026_10_06.md`, plan branch); this brief adds the files, the code as it is
today, the steps, the checks, the traps, and the decisions still open. **Nothing here is agreed design unless section
x.2 says so with its source. Every phase still starts by agreeing its plan with the user.**

**How this was checked.** Read-only: every claim was checked by reading the file named, on 2026-10-07 between about
02:00 and 03:15. No Bazel command was run. Three markers:
- no marker: read in the file named;
- **(inferred)**: my reading of the code, consistent with the measured evidence, but not run;
- **UNVERIFIED**: not checked; homework before relying on it.

**Paths.** Repository paths are relative to the repository root. "plan:" is the plan worktree (`runs/bazel-plan`,
branch `docs/bazel-first-class-plan`); "code:" is the code worktree (`runs/build-rebuild`, branch `build/phase3`).
Upstream files are written `engine:<path>` or `pure:<path>`, inside the pinned source trees at
`$(bazel info output_base)/external/+http_archive+legend_engine_src` and `.../+http_archive+legend_pure_src`
(engine 4.145.0, pure 5.99.0). A module path such as `core/pure/router/router_main.pure` is under that module's
`src/main/resources/`; appendix F maps the modules used here to their directories. Line numbers are from the working
copies at the time of reading. **Both worktrees were being edited by another session while this was written** (the
Phase 3 landing work): re-read a file before trusting a line number.

---

## 0. Read this first: what the plan does not say, or says wrong

Each item has its evidence in the sections named.

1. **3b item 5's "`Runtime` and `Mapping` are not found as type names" is one general bug, not two missing names.**
   A file's imports are recorded per element *full name*. Overloads of one function in different files share one
   full name, so all of them are name-resolved with the imports of the last file read. Users with a multi-file
   project meet the same bug. (3b.4, item 5b; inferred from the code, and it explains every census row involved.)
2. **The boot layer's twins fail because `SystemMetamodel.shadows` compares type spellings that the two sides write
   differently** (bare `String` in the system source, `meta::pure::metamodel::type::String` in the resolved upstream
   copy; `EnumerationMapping` against `EnumerationMapping<T>`). Their function ids are equal, and the duplicate check
   compares ids, so it fails them. (3b.4, item 1b; inferred.)
3. **Once the twin files load, `superMapping` will have two versions with identical parameters** (the system
   metamodel's returns `SetImplementation[0..1]`, upstream's `PropertyMappingsImplementation[0..1]`: two ids). PARK-12's
   "about 12" other upstream versions are 14 by a text count (appendix B).
4. **F-L1's "one line in ModelBuilder" is right only while two lists share the same objects.** The name resolver
   rebuilds a database's flat view list and each schema's view list separately, so a view it rewrites stops being
   the same object in both. (3b.4, item 1a.)
5. **3b item 4 also explains the reference lane's last `PROPERTY_AS_CALL` row:** `RoutingStrategy` has a plain
   property `toString` and a qualified property `toString()`; our dot call finds the plain one first and then calls
   `string::toString` on the strategy. (3b.4, item 4.)
6. **Experiment 8 added the rest of the manifest to the runner's current composition; it did not replace it.** The
   plan's Phase 6 replaces `LIBRARY_FILES` and `SHAPE_FILES`, which was never measured. One SHAPE file
   (`core_relational_duckdb/relational/connection/metamodel.pure`) is in neither candidate manifest, and
   `PreludeGenerator` reads the same two lists until Phase 4. (6.4, 6.9.)
7. **Experiment 8's harness ran every corpus pass with a 4 GB heap.** The DuckDB and warehouse passes really run with
   1 GB (`memory_mb = 1024`). Loading 655 more files was never measured at the real heap. (6.7.)
8. **legend-pure does not accept `;` as a property-mapping separator.** Its mapping rule has no `;` and no end anchor,
   so it stops at the first token it cannot use and silently ignores the rest, including whole property mappings.
   (6.4, 6.8.)
9. **"The H2 register gains its one entry" means the host-compared register**, which has been empty since 2026-09-21
   ("the host-compared register reaches zero — every assert is a verdict row"). Adding a row reopens it. (6.8.)
10. **The runner's platform-namespace guard throws; experiment 8 removed those functions beforehand in Python.**
    Production needs a decided rule. (6.4, 6.8.)
11. **F-L1 and `routeFunction` are scheduled twice**: in 3b (items 1 and 5) and in Phase 6's list of compiler gaps.
    They are 3b's; Phase 6 only checks them. (3b.9, 6.9.)

---

## 1. Shared ground (both phases)

### 1.1 Where things stand (2026-10-07)

- Phases 0, 1, 2 are on main; 2b was an experiment (plan, section 3).
- Phase 3 is built and audited on `build/phase3` (code worktree): four commits on `293318dda`, plus uncommitted
  audit fixes in 17 files (core sources, tests, `docs/GATES.md`, `docs/PARKED_WORK_LEDGER.md`,
  `docs/EXECUTION_PLAN_2026_09_26.md`, `spec/src/test/resources/reference-lane/reasons.tsv`). What is left before it
  lands is in plan:`docs/build-inventory/program/PHASE_3_LANDING.md` (untracked, written today). **Phase 3b starts on
  a new branch from main after Phase 3 lands.**
- plan:`docs/build-inventory/program/START_HERE.md` (untracked, written today) is the session guide: where things
  are, the rules, the landing commands, and every check (section 5). This brief does not repeat it; read it first.
- The reference lane's committed golden (code: `spec/src/test/resources/reference-lane/core_relational.txt`, commit
  `bc0de1f70`): AGREE 74,586, OVERLOAD 58, PACKAGE 14, DRIFT 0, PROPERTY_AS_CALL 1, bodies we fail to type 1,469,
  sources dropped 32.

### 1.2 The process for each phase (the user's rules)

1. Homework first (section x.10 of each phase): measure what is unknown.
2. Write the phase plan in plain words, with the open decisions (section x.8), and get the user's agreement.
3. Announce every core file the phase touches in `docs/IN_FLIGHT.md` on main, before the code (standing
   authorization to push IN_FLIGHT updates to main).
4. Code on a branch; during the work run only the targets the change touches; one heavy Bazel server on the machine
   at a time (check other sessions first).
5. Run the checks (section C): the six corpus passes against a fresh baseline and the reference lane before every
   compiler change lands (plan, Phase 3b "Conditions"), plus PCT and the local gate.
6. A `docs/GATES.md` entry: what moved and why, every moved reference-lane line explained.
7. An independent audit agent (`auditor`; `auditor-max` only when the user asks). Fix, rerun what the fixes touch.
8. Rebase on `origin/main`, `bazel test --lockfile_mode=error //gates:local`, tip commit with `[skip ci]`, push the
   branch, one full CI run on it (`gh workflow run gate.yml --ref <branch> -f gates= -f platforms=all`), then push
   that commit to main (`git push origin "${SHA}:refs/heads/main"`). No PRs. (START_HERE section 4.)

Rules that bite in these two phases:
- Three kinds of files, never mixed (plan section 1): upstream (only the bump changes it), ours (the implementation
  table, `meta::legend::lite`, the system metamodel), generated (only the bump). Never hand-edit a generated file
  (`prelude.pure`, `native-claims.tsv`, `DynaFn.java`, ...).
- Never invent a mechanism when one exists: the implementation table, modules and their manifests, the parser's
  dialect levels (`core/src/main/java/com/legend/parser/Dialect.java`), the ledger.
- A shortcut that must stay is a new row in `docs/PARKED_WORK_LEDGER.md` with an anchor in
  `core/src/test/java/com/legend/ParkedWorkLedgerTest.java`. A row leaves only by being fixed. A moved anchor
  (because the code moved) is re-pointed with a dated note, never loosened.
- Never bare "native": "upstream native" (upstream's `native function` keyword) or "platform-lowered" (our
  `Pure.java`).
- No home or temp paths in anything committed.

### 1.3 Words used below

- **Strict build**: `Compiler.buildModel` — the first broken element throws. The reference lane, PCT channel B and the
  census use it, and drop the offending source file and retry. **Tolerant build**: `Compiler.buildModule` — every
  broken element is recorded as a "wall" and the rest builds. The corpus runner uses it (code:
  `spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:293`).
- **Function id**: legend-pure's signature id, e.g. `meta::pure::mapping::classMappingById_Mapping_1__String_1__SetImplementation_$0_1$_`
  (`core/src/main/java/com/legend/model/FunctionId.java`). It spells types by their short names and includes the
  return type.
- **Boot layer**: the system metamodel (`core/src/main/java/com/legend/builtin/SystemMetamodel.java`, Pure source
  inside the Java file) plus the generated prelude, compiled once per process (`Compiler.boot`, code:
  `core/src/main/java/com/legend/Compiler.java:268-292`).
- **Twin**: an upstream function with the same id as one the system metamodel defines itself (29 names,
  plan:`docs/build-inventory/manifest-world/experiments/system_fqns.txt`).
- **Row**: an entry of the implementation table (`core/src/main/java/com/legend/platform/ImplementationTable.java`)
  saying how a declaration runs: `Form`, `Intrinsic`, `Body`, `Unimplemented` or `Refused`
  (`core/src/main/java/com/legend/platform/Implementation.java`).

---

# Phase 3b: what the bump and users need from the compiler

## 3b.1 Goal

Phase 6 and Phase 4 will load upstream files that today fail to build, and users can meet some of the same compiler
bugs. Phase 3b fixes exactly those: the boot layer's twins and the view lifted twice (so the upstream files that drop
today load), the qualified-property lookup, two bugs the census found (a crash and the import-scope bug), and a
user-impact review of the reference lane's 58 OVERLOAD and 14 PACKAGE rows. It does not try to type all of upstream:
the corpus passes and PCT measure what users get; the reference lane is a guard that must not get worse (plan,
Phase 3b, first paragraph).

## 3b.2 Agreed design and decisions (with sources)

| # | Decision | Source |
|---|---|---|
| D-1 | Scope: items 1 to 5 below; "Small: days" | plan, Phase 3b (re-scoped with the user 2026-10-07) |
| D-2 | Item 1: the boot layer's twins merge **by function id**; F-L1 is fixed; the 7 files that drop as "defined more than once" load | plan, Phase 3b item 1; ledger PARK-12 ("closes in Phase 3b, item 1"; acceptance: twins merged by id, each of the ~12 other versions decided "a row or a refusal", the files loading) |
| D-3 | Item 2: **no change** for the 18 files whose mappings use unsupported model-to-model features; the reference lane stays strict (`Compiler.buildModel`), drops and pins them; the features are a recorded product gap outside the program | plan, Phase 3b item 2; plan:`docs/build-inventory/manifest-world/experiments/phase3b-census/README.md`, "The decision" |
| D-4 | Item 3: review the 58 OVERLOAD and 14 PACKAGE rows for user impact; fix those that change results or a type users see; record the rest as type-only differences | plan, Phase 3b item 3 |
| D-5 | Item 4: a dot call finds a qualified property when a plain property shares its name (`Extension.serializerExtension(version)`, about 391 bodies; user models can have the shape) | plan, Phase 3b item 4 |
| D-6 | Item 5: 9 bodies crash typing (index out of bounds) instead of failing with an error; `Runtime`/`Mapping` not found as type names in the service's `from` and the router's `routeFunction` (see 3b.9: the description is wrong, the bug is real) | plan, Phase 3b item 5 (uncommitted plan edit, 2026-10-07) |
| D-7 | Not doing: typing the engine machinery (931 bodies) and upstream's unrun tests (460); the lane instrumentation; W1.1b stays with the parked compiler plan | plan, Phase 3b "Not doing"; IN_FLIGHT on main (`f306bd698`) |
| D-8 | Conditions: the reference lane and the six corpus passes (fresh baseline) before every compiler change lands; Phase 4 opens by re-measuring the library bodies | plan, Phase 3b "Conditions" |
| D-9 | Known soft spot, not fixed here: Phase 3's adjustments (`Any` with the type parameters, the kept tie-breaks; PARK-7, PARK-8) | plan, Phase 3b last sentence |
| D-10 | Debts PARK-5 to PARK-14 are fixed after the program lands, correctly, never worked around; PARK-12 is the exception (closes in 3b) | plan, section 5 "The program's debts"; code:`docs/PARKED_WORK_LEDGER.md` |

## 3b.3 Read first (in order)

1. plan:`docs/build-inventory/program/START_HERE.md` — rules, landing, every check command.
2. plan:`docs/REBUILD_PROGRAM_2026_10_06.md` — sections 1, 2 (decision 1: "the platform's own Pure" row kind), Phase 3
   (status and correction), Phase 3b, section 5.
3. plan:`docs/build-inventory/manifest-world/experiments/phase3b-census/README.md`, then `strict-gaps.tsv` (rows
   DROPPED, BROKEN) and `failures.tsv` — what fails and why.
4. code:`docs/PARKED_WORK_LEDGER.md`, rows PARK-6, PARK-11, PARK-12, PARK-14, and their anchors in
   code:`core/src/test/java/com/legend/ParkedWorkLedgerTest.java:84-115` — 3b touches the code they anchor.
5. plan:`docs/build-inventory/program/PHASE_3_LANDING.md` — what Phase 3 changed and left; the audit's findings.
6. code:`projects/FINDINGS.md:120-140` (F-L1) and code:`projects/BUILD.bazel:76-98` (the quarantine).
7. code:`core/src/main/java/com/legend/builtin/SystemMetamodel.java:1472-1588` (`shadows`, `spelling`,
   `withoutSystemShadows`) and `core/src/main/java/com/legend/Compiler.java:244-375` (the boot layer, `normalizeWithSystem`).
8. code:`core/src/main/java/com/legend/compiler/ModelBuilder.java:410-461` (`ingestDatabase`) and
   `core/src/main/java/com/legend/normalizer/LiftedViews.java` (the view lift).
9. code:`core/src/main/java/com/legend/compiler/spec/Typer.java:489-597` (the dot-call branch) and
   `core/src/main/java/com/legend/compiler/element/PureModelContext.java:394-418` (`findProperty`).
10. code:`core/src/main/java/com/legend/Compiler.java:145-209` (`parseSources`) and
    `core/src/main/java/com/legend/compiler/NameResolver.java:206-240` (per-element scope) — item 5b.
11. code:`spec/src/test/resources/reference-lane/core_relational.txt` (section "disagreement classes") and
    `reasons.tsv`; the examples file with call positions is built by `//spec:reference_lane_report` as
    `bazel-bin/spec/reference-lane/core_relational-examples.tsv` (appendix C lists one example per class).

## 3b.4 The code today, item by item

### Item 1a — F-L1: a view inside a `Schema` is lifted twice

- **Symptom.** `function 'r::Store$view$s.V' is defined more than once with the same signature` for any view declared
  inside a `Schema` (minimal model in code:`projects/FINDINGS.md:124-136`). Measured: the reference lane drops 2 files
  (`core_relational/relational/lineage/scanRelations/scanRelationsTestWithViewsAndUnions.pure`: `DB2$view$E.AltID_View`,
  `DB2$view$ViewSchema.AltID_View`; `core_relational/relational/modelJoins/testModelJoinsToRelationalJoins.pure`:
  `EntityDatabase$view$Entity.LegalEntity_View`) — plan census `strict-gaps.tsv` BROKEN rows. Experiment 8 saw a
  fourth (`TestDB$view$MySchema.View1`, engine `core_relational_test/serialization/testGrammarSerializationExtension.pure`;
  plan:`docs/build-inventory/manifest-world/experiments/e8_module_build_walls.tsv`). The projects: `firm-balance-sheet`
  (4 views in `Schema fbs`, code:`projects/firm-balance-sheet/store.pure:69,576,602,626,648`) is quarantined
  (code:`projects/BUILD.bazel:78-84`); these are "the projects' 4 build walls"
  (plan:`docs/build-inventory/manifest-world/experiments/e5_user_side_summary.txt`: `build=4` in every world).
- **Mechanism.** `FromProtocol.toDatabaseDefinition` puts every schema view into both the schema's list and the flat
  `views` list, as the same object (code:`core/src/main/java/com/legend/model/FromProtocol.java:173-177, 209-210`).
  `ModelBuilder.ingestDatabase` walks `db.views()` (all views) and then each schema's views, adding each schema view
  twice to `declared` (code:`core/src/main/java/com/legend/compiler/ModelBuilder.java:436-461`). `LiftedViews` keys
  owners by object identity (later put wins: the `SCHEMA.NAME` spelling) and keeps `order` with the view twice, so
  `functions()` returns the same lifted function twice (code:`core/src/main/java/com/legend/normalizer/LiftedViews.java:59-69, 145-169`).
  Side effect today (inferred): a schema view's bare name maps to an index entry spelled bare, whose lift is never
  produced; after the fix the bare name reaches the first schema's view ("first declared wins", the comment at
  `ModelBuilder.java:437-440`).
- **The documented fix** (code:`projects/FINDINGS.md:137-140`; plan:`docs/BAZEL_EXECUTION_LOG.md:299-302`): walk
  `DatabaseDefinition.defaultSchemaViews()` (code:`core/src/main/java/com/legend/model/DatabaseDefinition.java:85-96`)
  in the first loop. The file is `core/src/main/java/com/legend/compiler/ModelBuilder.java` (not under `compiler/element/`).
- **The catch.** `defaultSchemaViews()` subtracts schema views **by object identity**. `NameResolver.resolveDatabase`
  resolves `db.views()` and each schema's `views()` separately (code:`core/src/main/java/com/legend/compiler/NameResolver.java:1254-1275`),
  and `resolveView` returns a **new** object whenever a filter, group-by or column changes (`:1281-1286`);
  `resolveRelOp` always rebuilds a relational lambda (`:1533`) and rebuilds a column or join reference whose
  `[db]` name resolves to a different spelling (`:1524-1529`, `:1580-1590`). Such a view then has two different
  objects, and `defaultSchemaViews()` counts the flat copy as a default-schema view: the bug comes back (two
  same-named views in two schemas would again collide on the bare spelling). The three census files do not trigger
  this (their view columns use no `[db]` prefix and no lambda; engine `scanRelationsTestWithViewsAndUnions.pure:145-175`),
  so the one line fixes the measured cases. The same identity assumption is in `MetamodelSeeds.java:592,700` and
  `OpSeeds.java:106` (code:`core/src/main/java/com/legend/`), which read `defaultSchemaViews()` after resolution
  (inferred: a latent bug there too). See open decision 3b-O3.

### Item 1b — the boot layer's twins

- **Symptom.** The strict lane drops 5 files as "defined more than once" (census `strict-gaps.tsv`, DROPPED and BROKEN
  rows, 11 elements):
  `platform_dsl_mapping/functions_EnumerationMapping.pure` (`toDomainValue`), `platform_dsl_mapping/functions_Mapping.pure`
  (`enumerationMappingByName`, `classMappingById`), `platform_dsl_mapping/functions_PropertyMappingsImplementation.pure`
  (`_propertyMappingsByPropertyName`, `propertyMappingsByPropertyName`), `platform_store_relational/functions.pure`
  (`schema`, `table`, `view`, `column`, `childByJoinName`), engine `core_relational/relational/lineage/scanRelations/scanRelations.pure`
  (`relationTreeAsString`).
- **How twins are handled today.** `Compiler.normalizeWithSystem` removes a graph function that "shadows" a system
  function (code:`core/src/main/java/com/legend/Compiler.java:359-375`, calling `SystemMetamodel.withoutSystemShadows`,
  `SystemMetamodel.java:1565-1588`). `shadows` (`:1477-1498`) requires equal function ids (added by Phase 3, commit
  `165a1dbff`) **and** equal parameter spellings (`spelling`, `:1503-1511`). The duplicate check that then fails the
  pair compares ids only (`core/src/main/java/com/legend/compiler/element/ModelIntegrity.java:157-173`).
- **Why the 11 are missed (inferred, consistent with all 11).** The system elements are parsed, never name-resolved,
  before the comparison (`SystemMetamodel.ELEMENTS`, `:1520-1523`), so they spell primitives bare (`name:String[1]`,
  e.g. `classMappingById` at `:1173`). The graph's copies are resolved first (`Compiler.buildModel` → `resolveAlongside`
  with the prelude tier on, `NameResolver.java:182-188`), and the prelude tier spells a primitive as
  `meta::pure::metamodel::type::String` (`NameResolver.java:601-624`). Every one of the 11 has a `String` or
  `Boolean` parameter, except `toDomainValue`, whose upstream parameter is `EnumerationMapping<T>` (pure
  `platform_dsl_mapping/functions_EnumerationMapping.pure:18`) against the system's `EnumerationMapping`
  (`SystemMetamodel.java:1041`). Twins without such parameters (`mainTable`, `resolveStore`, `extractDBs(Mapping)`,
  ...) are removed correctly and are not in the census.
- **The other versions.** Upstream has 14 versions at the 29 names with no system twin (appendix B, a text-derived
  count; PARK-12 says "about 12"). Today they are `Body` rows (upstream's body runs) because the table's "no row means
  refused" rule covers only names Pure.java's catalog declares (`ImplementationTable.java:161-196`). One is special:
  `superMapping` has the same parameters as the system's version and another return type, so it is a different id;
  once `functions_PropertyMappingsImplementation.pure` loads, a call to `superMapping` has two candidates with
  identical parameters, which ends either in an "ambiguous overload" error or in a silent pick by a kept tie-break
  (PARK-8 lists "a duplicate signature (the first wins)") (inferred; not in the census because that file drops today).
- **Plan decision 1** (plan, section 2) says the boot layer's own versions need "one more kind of row, **the platform's
  own Pure**", and that a name-based rule does this today and Phase 3 replaces it. Phase 3 kept `shadows` with an id
  check instead (PHASE_3_LANDING section 3, finding S3; PARK-12). PARK-12's anchor is `boolean shadows(` in
  `SystemMetamodel.java` (`ParkedWorkLedgerTest.java:106-108`): the row is written to close when `shadows` goes. See
  open decision 3b-O1.
- **Where it matters later.** Phase 4's default world contains legend-pure `platform*` whole, so the boot layer will
  meet these twins at boot; the experiments already saw the boot fail without ownership handling
  (plan:`docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`, section 3 item 1: "the system metamodel's own
  `_classMappingByClass` duplicates upstream's"). The boot applies `withoutSystemShadows` to the *raw* prelude
  (`Compiler.java:278-279`), the graph path to *resolved* elements: a by-id merge works for both; a spelling rule does
  not (inferred).

### Item 3 — the 58 OVERLOAD and 14 PACKAGE rows

- Today's classes are in code:`spec/src/test/resources/reference-lane/core_relational.txt` (section "disagreement
  classes": columns kind, spelling, reference id, our id, count); one example position per class is in the examples
  file (built today, 02:52, by the Phase 3 session). Appendix C lists all 23 classes with one example each and a first
  look. Summary of the first look (one example per class read, so **UNVERIFIED** for the other calls of a class):
  - 34 calls (`greaterThan` 24, `greaterThanEqual` 1, `lessThan` 1, `lessThanEqual` 2, `contains` 2, `startsWith` 2,
    `in` 2): legend-pure types the first argument `[0..1]` (a relation column, or a path through an optional
    property) where we type `[1]`, so it picks the `[0..1]` version. In a filter the rows are the same. In a computed
    column (`in`'s example is `extend(~[isValid: c|$c.str->in([...])])`, engine
    `core_functions_standard/collection/in.pure:84-86`) a missing value could give `false` in legend-pure's own body
    and `NULL` in SQL. What legend-engine's SQL gives is the yardstick, not legend-pure's interpreter (UNVERIFIED).
  - 17 calls (`average` 3, `max` 3, `plus` 2, `sum` 7, `times` 2): legend-pure types a mixed or lambda-parameter
    number list as `Number` where we type `Integer` or `Float`. Values are the same; the result type differs
    (`max` gives `Number[0..1]` against our `Integer[0..1]`). Whether a result column's type counts as "a type users
    see" is open decision 3b-O4.
  - 5 calls (`propertyMappingsByPropertyName`): legend-pure picks the `EmbeddedSetImplementation` or
    `OtherwiseEmbeddedSetImplementation` version, which exists only in `functions_PropertyMappingsImplementation.pure`
    — a file the lane drops today (item 1b). Engine machinery (`core/pure/mapping/XStore.pure:44`,
    `core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:927`). Expect them to move with item 1b.
  - 1 call `range`: `[:5]` → legend-pure `range(Integer[1])`, ours `range(Integer[1], Integer[1])`; same values.
  - 1 call `elementToPath`: `PackageableElement` version against ours `Function`; both return a `String`
    (UNVERIFIED whether the text is the same for a function).
  - PACKAGE `divide` 3 and `plus` 2: position artefacts (the calls exist on both sides, at swapped columns;
    `reasons.tsv` rows W1.2 and W3.3 say so).
  - PACKAGE `size` 9: `evalWrapper()->eval()->size()` where `evalWrapper(): Function<{->TabularDataSet[1]}>[1]`
    (engine `core_relational/relational/router/tests/testRouting.pure:368,376`): legend-pure binds
    `collection::size(Any[*])`, we bind `relation::size(Relation[1])` (we treat a TDS as a relation); the test expects
    11 (the row count), which our pick gives. **`reasons.tsv`'s `PACKAGE size` row says the opposite** (3b.9).
- Where to record a type-only verdict is not decided (open decision 3b-O5).

### Item 4 — a dot call misses a qualified property that shares a plain property's name

- `Typer`'s dot-call branch (code:`core/src/main/java/com/legend/compiler/spec/Typer.java:504-597`) asks
  `ctx.findProperty(classFqn, name)` and goes the qualified-property way only if the answer is a `Property.Derived`
  (`:514-522`). `PureModelContext.findProperty` returns the **first property with that name**
  (`core/src/main/java/com/legend/compiler/element/PureModelContext.java:394-418`); classes list stored properties
  before derived ones (`core/src/main/java/com/legend/compiler/element/ClassCompiler.java:51, 76`). So when a stored
  property and a qualified property share a name, the stored one wins, the branch falls through to `applyGeneric`
  (`:597`), and the call is looked up as a function. `derivedOverloadArity` (`Typer.java:1614-1619`) checks the
  class's own derived properties only, no parents, and is consulted only after the `instanceof Property.Derived` test
  has already failed. The same first-by-name test sits in a sibling route earlier in the same method, for a call whose
  first argument is a variable of a class type (`Typer.java:438-459`, probe label `qp-var`); a fix must cover both
  routes. The zero-argument property *read* (`$x.name`, no parentheses, `Typer.java:1284-1289`) is right to take the
  stored property.
- Evidence: `Extension` has `serializerExtension : Function<{String[1]->String[1]}>[0..1]` and
  `serializerExtension(version : String[1])` (engine `core/pure/extensions/extension.pure:91, 97`); 391 rows of the
  census fail with "unknown function 'serializerExtension'" (`failures.tsv`, all in versioned protocol translators,
  i.e. machinery). `RoutingStrategy` has `toString : Function<{RoutingStrategy[1]->String[1]}>[1]` and
  `toString()` (engine `core/pure/router/metamodel/routing.pure:66-70`): the lane's one `PROPERTY_AS_CALL` row
  (`$fullRes.routingStrategy.toString()`, engine `core/pure/router/routing/router_routing.pure:718`, per the uncommitted
  `reasons.tsv`) is the same miss, typed as `string::toString` — a wrong function, not a failure.
- legend-pure's rule: a dot call with parentheses is a qualified-property call, matched on the class's qualified
  properties (with inheritance) by name and parameters; the plain property is only for `$x.name` without
  parentheses (inferred from upstream's own code compiling; the exact legend-pure method is UNVERIFIED).
- Anchors in the same branch: PARK-6's anchor is the receiver line `recv = synth(af.parameters().get(0), env)` in
  `Typer.java` (`ParkedWorkLedgerTest.java:84-88`); PARK-14's anchor is `af.propertyCall() || functionCandidates(af)`
  (`:112-115`). Item 4 must not fix or move them silently: re-point an anchor with a dated note if the code moves.
- The uncommitted comment at `Typer.java:497-503` says the double typing is "recorded for Phase 3b"; the ledger says
  PARK-6 is fixed after the program (3b.9).

### Item 5a — 9 bodies crash typing

- All 9 are upstream tests `meta::pure::mapping::modelToModel::test::alloy::autoMapping::testComplexTypePassThrough*`
  (`failures.tsv`, error `ArrayIndexOutOfBoundsException`, frame `compiler.spec.InferenceKernel.lambda$resolveOverload$2:1206`).
- The crash is in the **message** of the "ambiguous overload" error: `" p0=" + w.parameters().get(0)...` for a
  candidate with no parameters (code:`core/src/main/java/com/legend/compiler/spec/InferenceKernel.java:1200-1210`;
  line 1206 on the branch, 1205 on `origin/main`).
- Why two zero-parameter candidates tie: each test calls `testComplexTypePassThroughSimple()` (engine
  `core/store/m2m/tests/legend/testComplexTypeAutoMapping.pure:31,45,59`). Two functions have that name: the test
  itself (`...::autoMapping::testComplexTypePassThroughSimple():Boolean[1]`, `:28`) and the mapping helper
  (`...::autoMapping::mapping::testComplexTypePassThroughSimple(): Mapping[1]`, `:278`, imported at `:26`). Our
  resolver searches the caller's own package as an implicit import (`NameResolver.java:208-212, 625-628`); legend-pure
  searches only the section's imports, the core imports and the root package
  (pure `legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/compiler/postprocessing/functionmatch/FunctionExpressionMatcher.java:153-170`;
  `.../m3/navigation/imports/Imports.java:52-65`), so it sees only the helper. The own-package rule is a known
  difference owned by the parked compiler plan, W2.3b (code:`docs/EXECUTION_PLAN_2026_09_26.md:711-714`). Item 5a, as
  agreed, only makes the failure an error; the 9 bodies will then fail with "ambiguous overload" (not typed).
- A user can meet the same crash: a call with no arguments whose name exists in the caller's package and in an
  imported package (inferred).

### Item 5b — overloads in different files share one import scope

- `Compiler.parseSources` keeps three maps keyed by element full name, last file wins:
  `offsets.put(fqn, off)`, `elementImports.put(fqn, own)`, `elementSources.put(fqn, src.name())`
  (code:`core/src/main/java/com/legend/Compiler.java:192-204`). Inside one file, `ElementParser` keeps the first
  section's scope (`putIfAbsent`, code:`core/src/main/java/com/legend/parser/ElementParser.java:325, 393, 478`).
  `NameResolver.resolve` resolves each element with `model.elementImports().get(el.qualifiedName())`
  (`NameResolver.java:217-221`). `ParsedModel`'s maps are `Map<String, ImportScope>` etc.
- **The two census items are this bug** (inferred; it reproduces every row involved):
  - `meta::pure::mapping::from`: engine `core/pure/mapping/mappingExtension.pure` declares overloads with
    `runtime:Runtime[1]` (`:379-404`) and imports `meta::core::runtime::*` (`:24`). `core_service/service/mappingExtension.pure`
    and `core_data_space_metamodel/mappingExtension.pure` declare more `from` overloads and have **no imports**. The
    last file read sets the scope for every `from` overload, so core's `Runtime` stays unresolved. The census blames
    `core_service` (dropped first, "broken elements: 1") and then the data-space file ("KNOCK-ON: no broken element of
    its own"); after both drops, core's own imports apply and the model builds (`strict-gaps.tsv` DROPPED rows).
  - `meta::pure::router::routeFunction`: `core/pure/router/deprecated/deprecated.pure` declares overloads with
    `mapping:Mapping[1]` (`:19, :24`) and imports `meta::pure::mapping::*` (`:15`); `core/pure/router/router_main.pure`
    sorts after it and does not import that package (`:15-28`). Its scope wins, `Mapping` stays unresolved, and the
    census blames `router_main.pure`; dropping it drops the 4-argument `routeFunction` too, which is why GATES says
    "the closure lacks the 4-argument `routeFunction`" for `executionPlan`, `router::execute` and `loadCsvToDbTable`
    (code:`docs/GATES.md`, Phase 3 entry, "Bodies:" paragraph) and why 5 bodies are knock-ons of `router_main.pure`
    (census README).
- Error positions (`offsets`) and file attribution (`elementSources`) are wrong the same way for overloaded
  functions; the census's file attribution of overloads is therefore unreliable.
- Who else reads these maps: 55 uses in 15 files (core: `Compiler.java`, `KnowledgeLayer.java`, `ModelBuilder.java`,
  `NameResolver.java`, `ModelNormalizer.java`, `SystemMetamodel.java`, `test/PureTests.java`; spec: `PreludeGenerator.java`,
  `ManifestWorldCensusTest.java`, `OurResolutions.java`, `SpecBodyCensusTest.java`, `MinimalCorpus.java`,
  `EagerCorpusCompileProbe.java`; `pct/.../channelb/ChannelB.java`; `parser-equivalence/.../Sectionize.java`).
- Users: a Studio or server project with overloads of one function in two files with different imports is
  resolved with one file's imports (`Compiler.compileModel(List<ModelSource>)` → `parseSources`, `Compiler.java:425-445`).
- Phase 6 needs this fixed first: once the corpus loads core's `mappingExtension.pure` functions, the `from` overloads
  of three files meet (6.4).

## 3b.5 Steps

Each step names the files and how to verify it. Order matters: 1a and 5b change which files load, so do them before
judging item 3.

1. **Baselines, on main after Phase 3 lands (homework, no code).** Build and copy aside the six corpus passes
   (section C.1) to `runs/homework/phase3b/judges_base/`; build the reference lane report and diff it against the
   golden (expect identical; section C.2); rerun the strict-gap census (section C.3) and compare with plan's
   `phase3b-census/strict-gaps.tsv`; run `bazel test //spec:manifest_world_census` (manual; its ceilings are 32 load
   walls and 1,447 failing bodies, code:`spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java:349-355`;
   UNVERIFIED whether it still passes after Phase 3, which nobody recorded); PCT (section C.5); `bazel test //projects:tests`.
2. **Homework for the open questions** (3b.10), then the 3b plan agreed with the user (3b.8).
3. **IN_FLIGHT on main**: every core file the agreed plan touches (likely: `compiler/ModelBuilder.java`,
   `compiler/NameResolver.java`, `builtin/SystemMetamodel.java`, `Compiler.java`, `platform/ImplementationTable.java`,
   `platform/Implementation.java`, `compiler/spec/Typer.java`, `compiler/element/ModelContext.java`,
   `compiler/element/PureModelContext.java`, `compiler/spec/InferenceKernel.java`, `parser/ElementParser.java`,
   `model/ParsedModel.java` and its readers, tests), plus `projects/BUILD.bazel`, `projects/FINDINGS.md`, the
   reference-lane golden and `reasons.tsv`, `docs/PARKED_WORK_LEDGER.md`, `ParkedWorkLedgerTest`, `docs/GATES.md`.
4. **Item 1a, F-L1.** Change `ModelBuilder.ingestDatabase`'s first loop to `db.defaultSchemaViews()`; if 3b-O3 says so,
   make `NameResolver.resolveDatabase` resolve each view once and rebuild the flat list from the resolved objects.
   Tests (core): the minimal model of `projects/FINDINGS.md:124-131`; two schemas with a same-named view; a schema
   view the resolver rewrites (a `[db]`-qualified join or a relational lambda — confirm first that the grammar allows
   it in a view). Verify: `bazel test //projects:firm-balance-sheet_test` turns red (the quarantine expects the
   failure, `tools/legend/defs.bzl:9-10, 32-45`); delete `_SCHEMA_VIEW_TWICE` and the `QUARANTINE` entry
   (`projects/BUILD.bazel:78-84`), so the project also joins `//projects:graph_test` (`:95-98`); `bazel test
   //projects:tests` green; delete F-L1's row in `projects/FINDINGS.md`. `projects/BUILD.bazel` is a Bazel file: the
   audit reviews it. Census: the 2 F-L1 files no longer drop.
5. **Item 5b, import scopes** (if 3b-O2 agrees it is in scope). Make the import scope, source and offset belong to
   each element, not to its full name, through `parseSources`, `ElementParser`, `NameResolver.resolve` and the readers
   listed in 3b.4. Test: two sources, each declaring an overload of one function, with different imports and a
   parameter type only one import reaches; both overloads resolve. Verify: census — `core_service/service/mappingExtension.pure`,
   `core_data_space_metamodel/mappingExtension.pure` and `core/pure/router/router_main.pure` load; the 5
   `router_main.pure` knock-ons and the three GATES bodies (`executionPlan`, `router::execute`, `loadCsvToDbTable`)
   rechecked; PCT channel B and the corpus passes compared (they read these maps).
6. **Item 1b, twins by id** (design per 3b-O1). Verify: census shows no "defined more than once" for the 5 twin files;
   the 14 other versions each have a decided row or refusal (appendix B), `superMapping` handled; PARK-12 closed (row and
   anchor deleted together) or re-pointed with a dated note if the anchor survives by design; reference lane: the
   `propertyMappingsByPropertyName` OVERLOAD rows re-examined.
7. **Item 4.** Look up qualified properties by name and arity through the class and its parents, independent of a
   same-named plain property (one lookup in `ModelContext`/`PureModelContext`, used by both qualified-property routes
   in `Typer`, `:438-459` and `:504-597`; fold `derivedOverloadArity` into it rather than adding a second rule).
   Tests: a class with `x: String[1]` and `x(a: String[1])`: `$o.x('a')` is the qualified property, `$o.x` the stored one; a class with a stored `t` and a
   zero-parameter qualified `t()`: `$o.t()` is the qualified one; the same through a subclass. Verify: reference lane —
   FAILED drops by about 391, `PROPERTY_AS_CALL` 1 → 0 (inferred); corpus and PCT unchanged (expected; verify).
   Keep PARK-6's and PARK-14's anchors where they are, or re-point them with a dated note.
8. **Item 5a.** Make the ambiguity message safe for candidates with no parameters (`InferenceKernel.java:1200-1210`;
   print each candidate by its function id instead of `p0`). Test: two zero-parameter candidates, the caller's
   package and an imported one. Verify: the 9 census rows become `TypeInferenceException` ("ambiguous overload"), no
   `ArrayIndexOutOfBoundsException` left in `failures.tsv`.
9. **Item 3.** With 1a, 1b, 4 and 5b in, rebuild the report; for each class in appendix C, read its calls (examples
   file), decide "changes results / a type users see" or "type-only", fix the first kind (each fix is its own small
   design, agreed), record the second (3b-O5). Correct `reasons.tsv` so every class matches the golden
   (`bazel test //spec:reference_lane` checks every class has a row, `spec/src/test/java/com/legend/generators/ReferenceLaneTest.java:36-60`).
10. **Close out.** All checks (3b.6); the GATES entry (what moved in the lane, class by class; the corpus result files;
    PCT; the census before/after); ledger: PARK-12 per step 6, any new shortcut as a new row; audit; land (1.2).

## 3b.6 Checks and acceptance

Commands (section C has the exact procedures):
- `bazel test //core:core_tests //core:guardrails //core:census //spec:spec_tests` — all pass; a shrink-only guard
  that grows is a finding, not a pin to raise.
- The six corpus passes against the fresh baseline (C.1) — **every result file identical**, or each difference fixed or
  explained to the user before landing.
- The reference lane (C.2) — AGREE not down; FAILED bodies not up (expected down by about 391 + the files that now
  load); DROPPED down; every class has a `reasons.tsv` row; the new golden committed with every moved line explained.
- PCT (C.5) — identical; a channel B discovery count may move only with a reason (`bazel run //pct:update_ratchets`).
- `bazel test //projects:tests` — green with `firm-balance-sheet` out of quarantine.
- `bazel test --lockfile_mode=error //gates:local`; one full CI run.

Acceptance (from the plan, PARK-12 and this brief's findings):
- F-L1: no "defined more than once" for any lifted view; the projects' 4 build walls gone; FINDINGS row deleted.
- Twins: the 5 files load in the strict lane; each of appendix B's versions has a decided row or refusal; PARK-12
  closed (or restated by an agreed decision).
- Item 4: `Extension.serializerExtension(version)` and `RoutingStrategy.toString()` type as qualified properties.
- Item 5: no `ArrayIndexOutOfBoundsException` among census failures; with 5b, the `from` and `routeFunction` files load.
- Item 3: each of the 58 + 14 rows (23 classes) has a recorded verdict.
- Expected strict-lane drops after 3b, if all of the above land: 32 − 5 (twins) − 2 (F-L1) − 3 (import scopes) = 22
  (the 18 mapping files and the 4 unit files) (inferred; measure).

## 3b.7 Pitfalls already hit

- The census programs walk files with `Files.walk`, which does not follow the execution root's symlinks: pass
  `-Dlegend.engine.root` / `-Dlegend.pure.root` as real directories (realpath) (census README; START_HERE section 6).
- The gate's corpus checks compare committed results; they do not rerun the corpus. Phase 3 found 4 broken tests
  only by rerunning the six passes against a baseline (code:`docs/GATES.md`, Phase 3 entry; START_HERE section 5).
- A database pass refuses to run after its host pass fails: an empty database output means look at the host pass.
- Two quick fixes for PARK-5 were wrong and reverted (a second copy of the name rule as string checks, rejected by the
  identity guard; a reorder that moved the cost) (PHASE_3_LANDING section 3, S1): if a guard rejects a change, the guard
  is usually right.
- A caught failure that returns a value is forbidden (`ErrorShapeGuardrailTest`; PHASE_3_LANDING, N3): the item 5a fix
  must still throw.
- The reference lane needs about 8 GB and is manual; the warehouse corpus passes build the warehouse server
  binary (`//warehouse:server_native`, a GraalVM image), which is slow.
- zsh: `echo ====` fails; `"$c:spec/x"` is read as `$c` with the `:s` modifier ("bad substitution") — write
  `"${c}:spec/x"`, as for `"${SHA}:refs/heads/main"`.
- `bazel info` blocks while another build runs in the workspace (START_HERE section 6).
- Rebase hazard, noted once: the parked DataCube + Python work (`legend-lite-dcsnap`) carries a `Typer` fix
  (IN_FLIGHT on main, "Parked"); do not wait for it.

## 3b.8 Open decisions for the user

- **3b-O1 — How twins merge by id.**
  (a) `shadows` decides by function id alone, with a collision guard that resolves both sides the same way and fails
  loudly if the full parameter types differ; the other versions get decisions through the existing table rules.
  (b) Plan decision 1's "platform's own Pure" row kind: upstream's declaration stays the declaration; the system
  metamodel's body is its implementation by id; the "no row means refused" rule extends from the catalog's names to
  the boot layer's 29; `withoutSystemShadows` for functions goes. Evidence: decision 1 (plan section 2) asks for (b);
  PARK-12's anchor (`boolean shadows(`) is written to go away with the fix; (a) is smaller and also works for the raw
  boot prelude. Recommendation: (b) follows the agreed decision; (a) only with the user's explicit choice.
- **3b-O2 — Item 5b's scope.** The plan describes "2 elements"; the cause is general (every per-name element map).
  (a) Fix the maps (key per element), all readers updated; (b) record a ledger row and only fix the two cases (there is
  no correct narrow fix I can see: both come from the shared map). Recommendation: (a), because users meet it and
  Phase 6 needs it.
- **3b-O3 — F-L1: one line, or also the identity.** (a) The documented line (fixes every measured case); (b) also make
  `NameResolver.resolveDatabase` keep one object per view, which fixes the latent cases and the two metamodel readers.
  Evidence in 3b.4. Recommendation: (b), if homework 3b-H3 shows the grammar allows a rewritten view; otherwise (a) with
  a test pinning the assumption.
- **3b-O4 — What "a type users see" means for item 3.** Is a result column typed `Number` by legend-pure and `Integer`
  or `Float` by us a user-visible difference (whether Studio and DataCube show it is UNVERIFIED)? And is a `[0..1]` column
  typed `[1]` one? Decides whether the 17 numeric calls and the 34 optional-value calls are fixes or records.
- **3b-O5 — Where type-only verdicts are recorded.** `reasons.tsv` (per class, already required), and/or
  `docs/SEMANTICS_REGISTER.md` ("every deliberate difference from the reference", AGENTS.md "Standing documents").
- **3b-O6 — The 9 crashing bodies after item 5a.** They will fail as "ambiguous overload"; legend-pure types them
  because it has no own-package rule (W2.3b, parked). Accept that, or bring W2.3b's candidate rule in (a resolver change
  with wide reach)?
- **3b-O7 — Phase 3's 12 newly failing bodies** (GATES Phase 3 entry, "Bodies:"): by their names mostly engine
  machinery (protocol `getSignFunctions` in 5 versions, router and plan helpers; UNVERIFIED body by body). Phase 3b's
  re-scope does not type machinery; three (`executionPlan`, `router::execute`, `loadCsvToDbTable`) may type again after
  item 5b, since `router_main.pure` would load. Accept the rest as they are?

- **3b-O8 — Two of the "18 unsupported-mapping files" are not model-to-model features.** `modelJoinAdvancedSetup.pure`
  ("association ... `$person.profile` has no column binding on the Relation mapping") and `modelChainTest.pure`
  ("Embedded PM 'address' on 'S_Person' but owner class unknown") (census `strict-gaps.tsv` DROPPED rows). The "no
  change" decision was made on the description "set-routed bindings, enum transformers, explosions". Do they stay
  dropped too, or are they looked at as possible bugs users could meet (they load in the corpus's tolerant build)?
- **3b-O9 — Size.** The plan says "Small: days". Item 5b (per-element import scopes: 55 uses in 15 files) and item 1b
  done with decision 1's row kind plus 14 version decisions are each more than a one-line change; revisit the estimate
  with the user once 3b-O1 and 3b-O2 are decided.

## 3b.9 Stale or contradictory statements (location → correction)

1. plan, Phase 3b item 1, "Today 7 upstream **platform** files drop" → 7 upstream files drop: **5 are the boot
   layer's twins** (3 legend-pure `platform_dsl_mapping` files, 1 `platform_store_relational` file, 1 engine
   `core_relational` file, `scanRelations.pure`) and **2 are F-L1** (engine `core_relational` test files). Same in
   code:`docs/PARKED_WORK_LEDGER.md` PARK-12 ("7 upstream platform files drop"), which belongs to the boot layer only
   for 5 of them.
2. plan, Phase 3b item 5, "`Runtime` and `Mapping` are not found as type names in the service's `from` and the router's
   `routeFunction` (2 elements)" → overloads in different files share one import scope (keyed by full name in
   `Compiler.parseSources`); the failing overloads are core's `from` (engine `core/pure/mapping/mappingExtension.pure`)
   and `deprecated.pure`'s `routeFunction`; the blamed files are the last ones read. Same in the census README ("the
   service's `from`, the router's `routeFunction`: 2 elements") and its "12 files have broken elements of their own"
   (2 of the 12 are mis-attributed).
3. plan, Phase 6, "the compiler gaps ...: `routeFunction`'s resolution, duplicate view functions (F-L1 ...)" → both are
   3b's (items 5 and 1); Phase 6 checks them.
4. code:`docs/IN_FLIGHT.md` on main (Bazel line, 2026-10-05 note): "Not fixed by this program: F-L1 ... for the
   compiler's owner" → F-L1 is this program's (3b item 1). PHASE_3_LANDING section 5.3 says a corrected IN_FLIGHT is
   prepared in code:`runs/homework/phase3x/IN_FLIGHT.next.md` and lands before the Phase 3 code.
5. code:`projects/FINDINGS.md:137-140`, plan:`docs/BAZEL_EXECUTION_LOG.md:301` and the IN_FLIGHT note: "one line in
   ModelBuilder (walk `defaultSchemaViews()`)" → right for every measured case, but it relies on object identity that
   `NameResolver.resolveDatabase` can break (3b.4, item 1a).
6. code:`spec/src/test/resources/reference-lane/reasons.tsv`, row `PACKAGE size` (unchanged in the uncommitted
   version): "size binds to collection::size where the reference binds the TDS function" → the report's columns are
   (kind, spelling, reference id, our id, count): the reference binds `collection::size`, we bind `relation::size`
   (`core_relational.txt`, class `PACKAGE size`, 9 calls).
7. code:`docs/GATES.md`, Phase 3 entry (uncommitted working copy, near line 6812): "The 58 left are argument typing
   (Phase 3b)" → Phase 3b reviews the 58 for user impact; it does not take the argument typing (plan, Phase 3b "Not
   doing"). Same entry, "the closure lacks the 4-argument `routeFunction`" → `router_main.pure` is in the closure and is
   dropped by the import-scope bug.
8. code:`core/src/main/java/com/legend/compiler/spec/Typer.java:497-503` (uncommitted comment): "a time cost, recorded
   for Phase 3b (type each receiver once ...)" → recorded as PARK-6, fixed after the program lands (ledger PARK-6;
   plan section 5).
9. code:`docs/PARKED_WORK_LEDGER.md` PARK-12 and the plan's Phase 3 status bullet: "about 12 upstream versions" → 14
   by a text count (appendix B); PARK-12's "which of the 12 that affects was not checked" (same-parameter versions with
   another return) → `superMapping` is one.
10. plan:`docs/build-inventory/manifest-world/experiments/phase3b-census/README.md`, "The decision": lists the twins and
    F-L1, the qualified-property lookup and the review, but not item 5 (added to the plan afterwards).
11. The census README's list of twin elements names 10 of the 11 (`_propertyMappingsByPropertyName` is missing from the
    list; the count 11 is right).
12. plan, Phase 3b item 2, and the census README: the 18 files "use a model-to-model feature the platform does not
    support yet (set-routed bindings, enum transformers, explosions)" → 16 do; 2 are relational files that drop for
    other reasons (appendix A, last row), which may be compiler bugs rather than missing features (3b-O8).

## 3b.10 Unknowns: homework before coding

- **3b-H1 — Confirm the twin cause.** For each of the 11, print both copies' function id and parameter spellings as
  `shadows` sees them (a probe on the census classpath, C.3). Expected: ids equal, spellings differ as 3b.4 says.
- **3b-H2 — The other versions.** Turn appendix B (text-derived) into real ids: load the 27-module closure (as the
  census does) and list every declaration at the 29 names with its `FunctionId`, which ones twin, and which the
  corpus, PCT and the reference lane call (the lane's examples file; a corpus probe). Decide each (row, refusal, or a
  signature fix in the system metamodel, e.g. `superMapping`'s return type).
- **3b-H3 — Can a view be rewritten by the resolver?** Check the relational grammar (our parser and upstream's) for
  `[db]` prefixes and relational lambdas inside a view's columns, filter and group-by; and whether any corpus, project
  or upstream view has one. Decides 3b-O3.
- **3b-H4 — Item 5b's blast radius.** Count overloaded full names whose overloads come from files with different
  import sets in (a) the reference lane's closure, (b) the corpus composition, (c) the 56 projects. Each is a place whose
  resolution may change when the bug is fixed.
- **3b-H5 — Item 3 user impact.** For the 34 optional-value calls: run one computed column with a missing value
  (e.g. `extend(~b: x|$x.val > 2)` over a row with no `val`) on DuckDB and H2 through legend-lite, and compare with
  legend-engine's SQL for the same query (the corpus's golden SQL style), not legend-pure's interpreter. For the numeric
  calls: what type does a result column show in Studio/DataCube for `max` over integers (ours `Integer`, legend-pure
  `Number`)?
- **3b-H6 — legend-pure's qualified-property match.** Find the exact legend-pure code that matches `$x.name(args)`
  against qualified properties (name, parameter count, inheritance), so item 4 ports a rule rather than inventing one.
- **3b-H7 — Does `//spec:manifest_world_census` pass after Phase 3?** (step 1).

---

# Phase 6: the corpus on its real manifest

## 6.1 Goal

Today the corpus runner hand-picks what it loads: the relational tree, the model-to-model test models, the graph-fetch
domain, two named library files plus four test-fixture files (`LIBRARY_FILES`), and the classes of 64 named engine
files (`SHAPE_FILES`). Phase 6 makes it load what upstream says the corpus's module depends on — its manifest — with
one loading rule, so no list of ours decides the corpus's world. It runs before Phase 4 so that, when the default world
changes, the result views can move off startup into the `core` and `core_relational` modules that the corpus's
manifest includes, with nothing temporary in between (plan, Phase 6 and section 4). It also fixes the compiler gaps the
real manifest exposes and reviews PCT's own file composition the same way.

## 6.2 Agreed design and decisions (with sources)

| # | Decision | Source |
|---|---|---|
| E-1 | First re-run experiment 8 against today's prelude (it ran on the proposed default world) | plan, Phase 6, first bullet |
| E-2 | The runner loads its manifest's repositories ("the relational tree's 9 repositories and their closure, 38") with the loading rule, replacing `LIBRARY_FILES`, `SHAPE_FILES` and the folder lists | plan, Phase 6, second bullet |
| E-3 | The loading rule: each element loaded once (nothing the runner or the default world already declares); the platform's ownership applies (no function the platform owns); platform-namespace functions come from the default world only | plan, section 1 ("Test programs ... each element once, the implementation table applied, platform-namespace functions from the default world only"); plan:`docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` section 4 |
| E-4 | The H2 register gains its one entry (the test `meta::pure::executionPlan::tests::datetime::testPlanWithLocalH2ConnectionWithSQL`) | plan, Phase 6; experiments doc section 4 |
| E-5 | The compiler gaps: the parser (`;` as a property-mapping separator, `->` where we reject it), units of measure (a `Measure` as a type), `routeFunction`'s resolution, duplicate view functions (F-L1) | plan, Phase 6, third bullet (but see 6.9: two are 3b's, and the parser item is mis-described) |
| E-6 | PCT's own file composition reviewed the same way | plan, Phase 6, fourth bullet |
| E-7 | Check: the six corpus passes identical apart from recorded improvements; the projects' build walls 4 to 0 | plan, Phase 6, "Check" |
| E-8 | The reference lane stays strict; the 18 unsupported-mapping files are loaded by the corpus's tolerant build | plan, Phase 3b item 2 |
| E-9 | 6 needs 3 (the table owns what the manifest's files redefine); 6 runs after 3b (3b loads the files 6 needs) and before 4 | plan, section 4 |

## 6.3 Read first (in order)

1. plan:`docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` — sections 1, 3 (what the swap needed: the ownership filter),
   4 (experiment 8), 6 (open items 4 and 5).
2. plan:`docs/build-inventory/manifest-world/experiments/README.md` ("Setup to rerun", "Experiment 8") and the e8
   assets: `e8_extra.py`, `e8_MinimalCorpus.patch`, `e8_lanes.txt`, `e8_module_build_walls.tsv`, `e6_lanes.py`;
   plan:`docs/build-inventory/manifest-world/build.txt` (the parse walls of the whole manifest).
3. plan:`docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md` sections 4 to 6 (the practice, the contradictions, visibility).
4. code:`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java` (lines 100-460 and 783-795) and `Corpus.java`;
   code:`spec/src/gen/java/com/legend/generators/UpstreamFiles.java` (the two lists, "one list, so the prelude
   generator and the corpus admit exactly the same files").
5. code:`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:400-450, 1015-1145` (the registers: outside-body,
   host-compared, differential) and `spec/src/test/resources/rcorpus/`.
6. code:`spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java:31-118` — the existing manifest reader
   (`manifests`, `closure`): reuse it.
7. code:`spec/corpus.bzl` and `spec/BUILD.bazel:135-222` (the corpus lanes) — what each pass outputs.
8. code:`core/src/main/java/com/legend/parser/MappingProtocolParser.java:1300-1385`, `parser/SpecParser.java:330-360`,
   `parser/Dialect.java` — the parser gaps and the dialect levels.
9. code:`core/src/main/java/com/legend/model/MeasureDefinition.java` (header: "nothing in legend-lite's compiler consumes
   unit TYPES yet") and `compiler/element/TypeClassifier.java:91-107`.
10. code:`pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:56-170` and the five `ChannelB*Test.java`;
    `pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java` (channel A's composition).
11. code:`spec/src/gen/java/com/legend/generators/PreludeGenerator.java:240-290, 359-372` — it reads the same lists.

## 6.4 The code today

**The runner's composition** (code:`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java`):
- Shared fixtures: four relational files read first (`sharedSources`, `:399-410`).
- The corpus tree: every `.pure` file under `Corpus.RELATIONAL` (= `UpstreamFiles.RELATIONAL`, the `core_relational/relational`
  directory, 552 files; the `core_relational` repository has 553), sorted by a '/'-separated key (`corpusFiles`,
  `:417-427`), minus `ENGINE_IMPLEMENTATION_FILES` — `lineage/scanRelations/scanRelations.pure`, skipped by name
  because loading it made the 49 lineage tests run the engine's own `scanRelations` (`:117-127, 206-230`).
- Library sources (model only; their tests are not discovered): every file under `Corpus.M2M_TESTS` and
  `GRAPH_FETCH_DOMAIN`, plus `Corpus.LIBRARY_FILES` (6 files) (`libraryFiles`, `:429-451`). A library file that does not
  parse is skipped by name and reported (`:233-256`).
- Every source is under `refusePlatformNamespace`, which **throws** on any `meta::pure::functions::` function
  (`:783-795`; guard test `LibraryPlatformNamespaceGuardTest`). The parse of the whole set throws on any parse wall
  of the corpus's own files and on any duplicate element (`:270-285`).
- `withShapes` adds the classes and enums (not the functions) of `Corpus.SHAPE_FILES` (64 files) (`:350-397`).
- Tolerant build (`:293`); every database bound to one in-memory connection by an overlay (`:294-307`); tests
  discovered except library elements (`PureTests.discover(parsed.model(), libraryElements)`, `:310`;
  `core/src/main/java/com/legend/test/PureTests.java:62-68` skips by full name).
- The lists live in code:`spec/src/gen/java/com/legend/generators/UpstreamFiles.java:28-120` and are also read by
  `PreludeGenerator` (`spec/src/gen/java/com/legend/generators/PreludeGenerator.java:240-266`: the files the graph
  declares; they feed `owned` and `knownFqns`, `:359-372`), counted by `UpstreamPathManifestTest` ("91 on
  2026-10-05", `spec/src/test/java/com/legend/generators/UpstreamPathManifestTest.java:47-62`), and used by
  `FeatureFlagParityTest` to find `executionPlanFeature.pure` through `Corpus.SHAPE_FILES`
  (`spec/src/test/java/com/legend/generators/FeatureFlagParityTest.java:24-40`). Every one of these readers needs a
  decision when the lists leave the runner (6-O2).

**Which manifest.** `ManifestWorldCensusTest.manifests(engine, pure)` reads every `*.definition.json` (plus legend-pure's
`platform`, which has none in the tree) and `closure(name, all)` returns a module and its dependencies, dependencies
first (code:`spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java:49-118`). Counted today with the
same rule: `core_relational`'s closure is **27** repositories (the census's "27-module closure"); the 9 repositories
with files under `legend-engine-xt-relationalStore-generation/` and their closure are **38** (experiment 8's choice,
`e8_extra.py`: `corpus_repos` is a path-prefix test). The 11 in 38 but not 27: `core_external_compiler`,
`core_external_format_json_java_platform_binding`, `core_external_language_java`, `..._conventions_essential`,
`..._conventions_standard`, `..._feature_based_generation`, `core_external_store_relational_sdt`,
`core_java_platform_binding`, `core_java_platform_binding_external_format`, `core_relational_java_platform_binding`,
`core_relational_test`. `core_relational_duckdb` (whose `relational/connection/metamodel.pure` is a SHAPE file) is in
neither: it depends on `core_relational`, not the reverse (its `core_relational_duckdb.definition.json`). The only
corpus-tree reference to its class `DuckDBDatasourceSpecification` is inside a comment
(engine `core_relational/relational/extensions/grammarSerializerExtension.pure:274`); why it is a SHAPE file is
UNVERIFIED (6.10).

**Bazel inputs.** The corpus passes already declare both whole upstream trees (`UPSTREAM_TREES`,
code:`tools/generators/defs.bzl:12-29`; each tree's `:tree` is `glob(["**"])` in the archive's BUILD file), so the
manifests and every repository are already inputs: no BUILD change is needed for reading them (inferred).

**What experiment 8 did** (plan, experiments doc section 4; README "Experiment 8"): `e8_extra.py` writes, as extra
library sources, every file of the 38 repositories not already in the composition, after removing in Python each
element the composition or the given world declares, every system-metamodel element, every platform-namespace
function, and every function in an ownership list built from the prelude's footers, `native-claims.tsv`, `CoreFn`
and `e6_owned_final.txt`; it also skips `platform/pure/grammar/m3.pure` by path. A copy of `MinimalCorpus` reads them
(`e8_MinimalCorpus.patch`: appended to `libraryFiles`). Result on the proposed world: rosters and registers identical,
each ledger one line longer (the H2 test reaching its assert), the H2 database pass red on one register; pass times
37 → 40 s for DuckDB host (with the harness's 4 GB heap, 6.7). 655 files, 11,063 elements, 8.8 MB added. Whole-module
build walls: `e8_module_build_walls.tsv` (14 rows: 9 units of measure — 7 properties typed `Mass~Kilogram` or
`Mass~Pound` and 2 unit-arithmetic functions —, 1 `routeFunction`, 4 F-L1 views).

**The parse walls of the whole manifest** (plan:`docs/build-inventory/manifest-world/build.txt`, last block: 1,772
files, 5 parse walls; appendix D): two files with `;` after a property mapping's expression, one brace-less lambda
followed by `->`, one property mapping with no separator (`simpleObject.pure`, "trailing tokens", already skipped by
the runner today as a library wall), and legend-pure's `m3.pure` (`^Instance` bootstrap form).
- **What legend-pure does with them** (by reading; not run): its mapping rule is
  `mapping : (MAPPING_SRC qualifiedName)? (MAPPING_FILTER combinedExpression)? mappingLine (COMMA mappingLine)*`
  with no `;` and no end anchor (pure `legend-pure-core/legend-pure-m3-core/src/main/antlr4/org/finos/legend/pure/m3/serialization/grammar/m3parser/antlr/core/M3CoreParser.g4:81-85`),
  invoked as `parser.mapping()` (pure `.../m3/serialization/grammar/m3parser/antlr/M3AntlrParser.java:352-359`; the
  "Pure" mapping parser is `M3AntlrParser`). An ANTLR start rule without an end anchor stops at the first token it
  cannot use and ignores the rest without an error, so legend-pure accepts a trailing `;` by **ignoring everything
  after the last well-formed mapping line** — in `simpleObject.pure` it silently drops the `i : []` mapping.
  legend-engine's grammar (used for user models) requires commas and refuses both
  (engine `.../grammar/from/antlr4/mapping/pureInstanceClassMapping/PureInstanceClassMappingParserGrammar.g4:27, 34`).
  Our parser refuses `;` on purpose, citing the engine's grammar (code:`core/src/main/java/com/legend/parser/MappingProtocolParser.java:1347-1351, 1361-1370`).
  The lambda case: legend-pure's `lambdaPipe: PIPE codeBlock` and `codeBlock: programLine (END_LINE (programLine END_LINE)*)?`
  (`M3CoreParser.g4:270, 429`) let `a|$a+'eee';` end with `;`, then `->eval(...)` applies to the lambda.
- The parser's dialect levels already separate "legend-lite's own legend-pure dialect, reachable only from platform
  sources and the test harness" (`LEGEND_PLATFORM`) from the user-facing engine dialect (code:
  `core/src/main/java/com/legend/parser/Dialect.java:6-28`); the corpus parses with `LEGEND_PLATFORM`
  (`MinimalCorpus.java:256, 274`). The drop-in surface is what `SectionParseSentinelTest` guards
  (code:`parser-equivalence/src/test/java/com/legend/equivalence/SectionParseSentinelTest.java:20-75`; LENIENT may only
  fall, and "accepting because we IGNORED something is a bug wearing a superset's clothes").

**Units of measure.** `Mass~Kilogram` and `meta::pure::unit::Mass` fail in `TypeClassifier.classify` ("Unknown type",
code:`core/src/main/java/com/legend/compiler/element/TypeClassifier.java:91-107`). Measures parse and index
(`MeasureDefinition`) but "nothing in legend-lite's compiler consumes unit TYPES yet (a property typed `Mass~Gram` is a
separate, unbuilt leg)" (code:`core/src/main/java/com/legend/model/MeasureDefinition.java`, class comment). Upstream:
`core/pure/corefunctions/unit.pure` (unit arithmetic, including `meta::pure::functions::math::plus/minus` over `Mass`,
platform namespace), `core/store/m2m/tests/legend/unitMeasure.pure`, `testUnitMeasure.pure`,
`core_external_format_json/executionPlan/tests/dataTypes.pure`. Experiment 8 counted 9 elements (its namespace filter
removed `plus`/`minus`); the reference lane counts 11 in 3 files plus 1 knock-on file (census).

**The H2 register.** In experiment 8 the H2 database pass failed with
`[h2] host-compared register != committed (full run): NEW 1 (an assert was decided outside a verdict row ...)`,
naming `testPlanWithLocalH2ConnectionWithSQL` (code:`runs/homework/world/e6_lanes/e8_manifest/judge_database_h2/database.log:1606-1607`,
scratch). The register is `spec/src/test/resources/rcorpus/h2-database-host-compared-register.txt`, **empty** (0 lines), as is
DuckDB's; it is checked by `pinArtifactRegister` (code:`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:435-445, 1022-1068`),
described as "exact, shrink-only — the number that must reach zero"; it reached zero on 2026-09-21 (commit `dc80d4891`,
"the host-compared register reaches zero — every assert is a verdict row"). The test is already in both fail rosters
(`duckdb-fail-roster.txt:5`, `h2-fail-roster.txt:6`); in experiment 8 it failed with "Cast exception:
...DatabaseConnection is not a ...RelationalDatabaseConnection" (same log, line 100).

**PCT's composition.** Channel A: the PCT adapter runs inside legend-pure's interpreter and packs the model text our
compiler receives from what each test expression references (code:`pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java`,
class comment). Channel B: "lite's own compiler runs the PCT sources straight from the pinned trees"
(code:`pct/BUILD.bazel`, gate 9 comment): each suite names its model roots and scope directories as hard-coded upstream
paths ("ten across the five suites", `ChannelB.java:84-86`; e.g. `ChannelBStandardTest.java:35-45`), loads every file
of the roots (legend-pure's `platform/pure` tree and an engine functions tree), drops parsed upstream-native
declarations, and builds strictly with the same drop-a-file loop as the reference lane (`ChannelB.java:100-165`). It
asserts a wall ceiling (e.g. `ChannelBEssentialTest.java:50`, ≤ 20) and an exact discovery count from
`PctRatchets` (`:66`).

## 6.5 Steps

1. **Preconditions.** Phase 3 and 3b landed (3b's twins, F-L1 and import-scope fixes are what make the manifest's
   files load cleanly; 6.9 item 1).
2. **Homework (6.10), above all the re-run of experiment 8 (C.4) on today's prelude and the post-3b code, in the
   variants listed there, at the real heap.** Write the results down in `runs/homework/phase6/`; what later phases
   need goes into this brief or the phase plan, never left in scratch.
3. **Agree the open decisions (6.8) with the user**, then write the Phase 6 plan.
4. **IN_FLIGHT on main**: spec (`rcorpus/MinimalCorpus.java`, `rcorpus/Corpus.java`, the manifest helper moved out
   of `ManifestWorldCensusTest`, `UpstreamFiles.java` if decided, `UpstreamPathManifestTest`, `FeatureFlagParityTest`,
   `LibraryPlatformNamespaceGuardTest` if the guard's rule changes, `spec/src/test/resources/rcorpus/` registers), core
   (the parser files for the agreed dialect change: `parser/MappingProtocolParser.java`, `parser/SpecParser.java`;
   units: `compiler/element/TypeClassifier.java` and whatever the agreed scope needs), `pct/` only if the review
   changes code, `docs/GATES.md`.
5. **The manifest reader.** Move `manifests`/`closure` to a shared, non-test-annotated place in spec (they are
   package-private statics of a `@Tag("heavy")` test class today) and use them from the runner and the census; no second
   reader.
6. **The runner.** Replace the corpus tree walk, `libraryFiles`' folder lists, `LIBRARY_FILES` and `withShapes` by:
   every repository of the agreed manifest (6-O1), each file once, in the existing '/'-key order; the corpus's own
   tests discovered only from the corpus's repository (today: `Corpus.RELATIONAL`); every other element a library
   element; the platform-namespace rule as decided (6-O3); `ENGINE_IMPLEMENTATION_FILES` as decided (6-O4); `m3.pure`
   as decided (6-O6). No ownership list in Java: after Phase 3 the implementation table owns what the files redeclare
   (plan section 4: "6 needs 3"). Keep `missingInputs` loud (a moved manifest or repository fails by name). Keep the
   four shared fixture files' role: they are read first and are where the corpus-wide setups come from
   (`sharedSources`, `:399-410`; the setup scan, `:312-330`), and the corpus tree is de-duplicated against them by text
   (`:206-230`). Verify each sub-step with the six passes (C.1).
7. **The register entry**, as decided (6-O7), with its reason in GATES.
8. **The parser gaps**, as decided (6-O5), under `LEGEND_PLATFORM` only; tests in core; `SectionParseSentinelTest`
   unchanged (its drop-in surface is not touched). Verify: the manifest module built tolerantly (the census-style probe
   of experiment 8, or `ManifestWorldCensusTest` on the corpus's module) shows no parse walls for the agreed cases.
9. **Units**, scope as decided (6-O8). Verify: the 9 (corpus) / 11 (lane) unit elements build; whatever is not
   supported fails loudly at use.
10. **Check the 3b items** in the manifest world: no duplicate view functions, `routeFunction` resolves, the twins load.
11. **PCT review** (6-O9): a written finding per channel; code only if agreed.
12. **Close out**: checks (6.6), GATES entry (what the corpus now loads, counts, timings, heap), audit, land.

## 6.6 Checks and acceptance

- The six corpus passes against a baseline built on main before Phase 6 (C.1): every result file identical, apart
  from recorded improvements (plan) — each difference listed in GATES with its reason; the database passes' verdicts
  green.
- `bazel test //spec:spec_tests` (includes `LibraryPlatformNamespaceGuardTest`, `UpstreamPathManifestTest`) and
  `//core:core_tests //core:guardrails //core:census`.
- PCT identical (C.5); channel B counts move only with a reason.
- The reference lane (C.2): unchanged by the runner change; the parser and unit fixes may move it (strict lane) —
  explain every moved line.
- `bazel test //projects:tests` green; the projects' build walls 0 (from 3b; verified here).
- Peak heap of each pass within its `memory_mb` (or an agreed, dated change: CI's 7 GB macOS runner runs the H2 lane,
  `spec/BUILD.bazel:212-214`).
- Acceptance: the runner has no `LIBRARY_FILES`, `SHAPE_FILES`, folder lists or (if decided) name exclusions; its
  world is the agreed manifest by the loading rule; `missingInputs` empty; no parse or build walls left from the agreed
  gaps; the H2 register decision applied; a PCT review written.

## 6.7 Pitfalls already hit

- `e6_lanes.py` changes the commands it reruns: `-Xmx` becomes `-Xmx4g` and `-Djava.io.tmpdir` moves
  (plan:`docs/build-inventory/manifest-world/experiments/e6_lanes.py`, the loop over `args`); the real passes use
  `-Xmx1024m` (DuckDB, warehouse) and `-Xmx4096m` (H2) (`spec/BUILD.bazel:175-222`; `tools/java_run/defs.bzl:92-98`
  turns `memory_mb` into `-Xmx`). It also hard-codes the configuration directory `darwin_arm64-fastbuild` and calls
  `bazel info`/`bazel aquery` (README).
- Before a hand-made corpus command, rebuild the cached corpus targets, or the execution root lacks the upstream trees
  ("legend-engine checkout not present") (plan section 5).
- The first attempt at experiment 8 (the whole closure as the corpus's prelude) failed: test files stripped, and the
  same element in the base and in the test's module, where the runner's T4 rule drops the test's copy and
  `RelationReads` then looks only in the test's graph (experiments doc section 4, "The first attempt"). Load the
  manifest into the corpus's own module, not into the boot.
- Without an ownership rule, `^Class(...)` resolved to upstream's `new` (901 user walls) and legacy TDS
  `project`/`groupBy` with `agg`/`col` broke 107 corpus tests (experiments doc section 3 item 1) — measured before
  Phase 3. After Phase 3 the table owns forms and catalog names by id, but the legacy TDS functions are still recognized
  by resolved name with a spelling fallback (PARK-11; their rows by id come in Phase 4): loading `core/pure/tds/tds.pure`
  whole puts upstream's TDS function declarations and bodies in the corpus world for the first time (6.10, 6-H3).
- Windows: exclusion keys must use '/' (a backslash path once admitted `scanRelations.pure` and failed the 49 lineage
  tests on Windows CI) and the file order must use the '/'-key sort, not `Path` order (`MinimalCorpus.java:206-215,
  411-427`).
- `withoutSystemShadows` throws if a loaded element redefines a non-function system element
  (`SystemMetamodel.java:1572-1580`); today the system metamodel's upstream-named elements are all functions
  (`system_fqns.txt`), so this holds only while that stays true.
- Library parse walls are skipped by name; a parse wall in the corpus's own files throws (`MinimalCorpus.java:233-282`).

## 6.8 Open decisions for the user

- **6-O1 — Which manifest.** (a) `core_relational`'s closure, 27 repositories: the corpus's tests all live in that one
  repository, and it is the rule upstream uses for a module; (b) the 9 relational-generation repositories and their
  closure, 38 (experiment 8's choice, defined by a path prefix; adds Java code-generation modules and
  `core_relational_test`). Evidence: 6.4; experiment 8 measured (b) only, as an addition. Recommendation: none until
  homework 6-H1 shows whether (a) covers what the corpus needs.
- **6-O2 — `LIBRARY_FILES` and `SHAPE_FILES` until Phase 4.** `PreludeGenerator` reads them (6.4), so deleting them
  changes the generated prelude. (a) The runner stops using them; the constants stay for `PreludeGenerator` until
  Phase 4 deletes it, and `UpstreamFiles`' "one list, so the prelude generator and the corpus admit exactly the same
  files" is restated (a temporary state the plan's order meant to avoid); (b) also switch `PreludeGenerator`'s "what
  the graph declares" to the manifest (a generated-prelude change before Phase 4). Needs the user's call.
- **6-O3 — Platform-namespace functions.** The guard throws today; the rule says they come from the default world only.
  (a) The loader skips `meta::pure::functions::` functions of the manifest's other repositories, by a written rule,
  and still throws for the corpus's own files; (b) skip the repositories that are upstream core (legend-pure
  `platform*`, engine `core_functions_*`) — but today's prelude is only part of them, so (b) would lose elements until
  Phase 4. Either changes what `LibraryPlatformNamespaceGuardTest` and AGENTS.md describe.
- **6-O4 — `ENGINE_IMPLEMENTATION_FILES` (`scanRelations.pure` skipped by name).** After Phase 3 (unrowed versions of
  platform functions refused) and 3b (its `relationTreeAsString` twins merge), the file may load safely: keep the
  exclusion, or measure and delete it (6-H4).
- **6-O5 — The parser gaps.** (a) Under `LEGEND_PLATFORM` only, accept a `;` at the end of the last property mapping
  (no content lost: the two engine files) and the brace-less lambda ended by `;` before `->`; keep refusing a missing
  separator and report `simpleObject.pure` as a library wall with its reason (legend-pure silently drops its `i`
  mapping); (b) mirror legend-pure exactly (stop at the first unusable token, ignore the rest) — a silent drop;
  (c) leave all four as reported walls. Evidence: 6.4. Recommendation: (a), as the evidence supports it and nothing is
  silently dropped; the user decides.
- **6-O6 — `m3.pure`.** Experiment 8 skipped it by path. The 87 m3 declarations stay built in (experiments doc section 1,
  item 4 "Built in"; homework doc section 6, the 2026-10-06 update). (a) A written rule that the boot's own m3 bootstrap file is not loaded again;
  (b) the parser learns the top-level `^Instance` form and T4 drops the duplicates.
- **6-O7 — The H2 register entry.** (a) Add the row to `h2-database-host-compared-register.txt` with the reason in
  GATES, as the plan says; (b) first find why the assert is decided outside a verdict row and fix that, keeping the
  register at zero. Evidence: 6.4. The plan says (a); the register's history argues for asking.
- **6-O8 — Units scope.** (a) Units become types far enough for the elements to build (`Mass~Kilogram` and the measure
  type classified), and anything else (conversion, unit arithmetic) fails loudly at use, recorded as a product gap;
  (b) full unit semantics. "None of it is reached by a corpus test" (experiments doc section 4).
- **6-O9 — PCT review scope.** A written review only, or also: channel B's ten hard-coded paths become each suite's
  module and its manifest closure (the same reader), its strict loop rechecked after 3b, its upstream-native pruning
  rechecked against Phase 3's merge by id; channel A's packer left as is.
- **6-O10 — Visibility.** Upstream drops a function in a repository that is not a dependency of the caller's from the
  overload candidates; our compiler has no repositories. A manifest-loaded world with 27 or 38 repositories may see
  candidates upstream would not (homework doc section 5: "must be decided and tested"; experiments doc section 6 item 4:
  untested). Decide whether Phase 6 measures it (6-H5) and what happens if it finds a case.

## 6.9 Stale or contradictory statements (location → correction)

1. plan, Phase 6, "`routeFunction`'s resolution, duplicate view functions (F-L1 ...)" → 3b items 5 and 1 own them
   (the first is the import-scope bug, 3b.4); Phase 6 verifies.
2. plan, Phase 6, "the parser (`;` as a property-mapping separator, `->` where we reject it)" → legend-pure has no `;`
   separator: it ignores everything after the last well-formed mapping line; the `->` case is specifically a
   brace-less lambda ended by `;` (engine `core/pure/corefunctions/tests/language/testLambda.pure:71`). The plan's list
   omits the missing-separator case (`simpleObject.pure:1869`), which experiments doc section 4 lists, and `m3.pure`,
   which experiment 8 skipped by path.
3. plan, Phase 6, "replacing `LIBRARY_FILES`, `SHAPE_FILES` and the folder lists" read with experiments doc section 4
   ("works with the loading rule") → experiment 8 kept the composition and added the rest; the replacement is
   unmeasured; one SHAPE file is outside both manifests; `PreludeGenerator` reads the lists until Phase 4.
4. plan, Phase 6, "the relational tree's 9 repositories and their closure, 38" → the 9 come from a path prefix in
   `e8_extra.py`, not a manifest; the corpus's own manifest is `core_relational`'s (27).
5. plan, Phase 6, "The H2 register gains its one entry" and experiments doc section 4, "the H2 database pass's
   shrink-only register (`NEW 1`)" → the host-compared register, empty and called "the number that must reach zero".
6. Experiments README ("each corpus pass's exact Bazel command rerun by hand") and plan section 5 ("rerun each corpus
   pass's exact Bazel command by hand") → not exact: the heap is replaced by 4 GB and the temp directory moved.
7. plan section 1 ("platform-namespace functions from the default world only") against the runner, whose guard throws
   (`MinimalCorpus.java:783-795`); and AGENTS.md (reference-checkout tenet) names `Runner.registerLibrarySource`, which
   was deleted in `ff359bae2` (plan:`docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md` section 5) — the guard is
   `MinimalCorpus.refusePlatformNamespace`.
8. Experiments doc section 4, "Units of measure, 9 elements" against the census's 11 in the reference lane — both right
   for their worlds (the corpus's namespace rule removes `plus`/`minus` over `Mass`).
9. Experiments doc section 1, "The projects' 4 build walls are duplicate view functions inside the projects themselves,
   the same in every world" — right; they are F-L1 in `firm-balance-sheet` and go with 3b item 1.

## 6.10 Unknowns: homework before coding

- **6-H1 — The real Phase 6 configuration, measured.** Re-run experiment 8 (C.4) on today's prelude and post-3b code in
  three variants: (i) as it was (composition + rest, Python ownership filter), to compare with the old result;
  (ii) composition + rest **without** the Python ownership filter (the table owns), the honest preview of Phase 6;
  (iii) **manifest only** (no `LIBRARY_FILES`, `SHAPE_FILES`, folder lists), for both 27 and 38 repositories. Record
  every differing result file. Variant (iii) needs a copy of the runner, never committed.
- **6-H2 — Heap and time at the real flags.** For each variant, each pass's peak heap (e.g. `-Xlog:gc` or JFR) with
  `-Xmx1024m`/`-Xmx4096m` as the lanes run; and pass times.
- **6-H3 — Legacy TDS functions in the manifest world.** With `core/pure/tds/tds.pure` loaded whole, do `project`,
  `groupBy`, `agg`, `col`, `restrict`, ... still go to `TdsLegacy`, or does upstream's body run (PARK-11; experiments doc
  section 3 item 1)? If they break, the TDS rows by id may have to come before Phase 4 (an ordering question for the
  user).
- **6-H4 — `scanRelations.pure` admitted.** Run the six passes with the exclusion removed (after 3b); compare the 49
  lineage tests.
- **6-H5 — Visibility.** In the 27/38-repository world, count calls whose candidates include a function from a
  repository outside the calling file's repository closure; list any that change the pick.
- **6-H6 — Why `core_relational_duckdb/.../metamodel.pure` is a SHAPE file.** Remove it in a copy and see what fails;
  decide where its classes come from (a harness need is not a corpus need).
- **6-H7 — The H2 test.** Why does `testPlanWithLocalH2ConnectionWithSQL` reach a host-decided assert in the manifest
  world, and does it on today's prelude?
- **6-H8 — PCT channel B with 3b's fixes.** Rerun channel B: which wall counts and discovery counts move.
- **6-H9 — The census classifier.** The census README's split of the 1,469 bodies (931 / 460 / 52 / 26) has no committed
  script in `phase3b-census/`; Phase 4 opens by re-measuring library bodies, so commit or rewrite that classifier
  (scratch `runs/homework/phase3x/classify.py` is a different script: Phase 3's OVERLOAD split).

---

# C. Procedures (exact)

## C.1 The six corpus passes against a baseline

Targets (code:`spec/corpus.bzl:28-156`, `spec/BUILD.bazel:175-222`): `//spec:judge_host_duckdb`,
`//spec:judge_database_duckdb`, `//spec:judge_host_h2`, `//spec:judge_database_h2`, `//spec:judge_host_warehouse`,
`//spec:judge_database_warehouse`. Each pass is a cached build action (`java_run`); a pass "succeeds" as an action
whatever its tests say; the verdict is a file.

1. On the base commit (main, before the change), build the six:
   `bazel build //spec:judge_host_duckdb //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse //spec:judge_database_warehouse`.
   The warehouse passes build `//warehouse:server_native` (slow).
2. Copy each `bazel-bin/spec/judge_<pass>/` directory to `runs/homework/<phase>/judges_base/judge_<pass>/`.
3. Make the change; build the six again.
4. Compare the **result files** (22 in all): DuckDB and H2 host passes — `judge-host.tsv`, `verdict.txt` and four
   measured rosters (`<lane>-fail-roster.txt`, `<lane>-skipped-roster.txt`, `<lane>-unordered-register.txt`,
   `<lane>-engine-order-register.txt`); DuckDB and H2 database passes — `judge-database.tsv`, `verdict.txt`,
   `<lane>-database-engine-order-register.txt`; warehouse passes — the ledger and `verdict.txt` (no rosters of their
   own: `golden = False`, they check against DuckDB's committed ones). Compare sorted lines without lines starting with
   `#`; skip `*.log` (timings) and `*.params`. A ready script: code:`runs/homework/phase3x/compare_judges.py` (scratch;
   usage `python3 -I compare_judges.py <judges_base> bazel-bin/spec`; copy it, it is not committed).
5. Known benign difference: the H2 and DuckDB host passes' reflection rows renumber their function ids
   (PHASE_3_LANDING, S6). Anything else is fixed or explained to the user before landing.
6. One pass by hand, scoped to a test: `bazel run //spec:corpus_one -- <duckdb|h2> <host|database> [<test fqn>]`
   (`spec/BUILD.bazel:224-246`).
7. The lanes as tests (`bazel test //spec:corpus_duckdb //spec:corpus_h2`) check verdicts and that the committed rosters
   equal the measured ones (`update_rcorpus_<lane>_tests`); a moved roster is re-blessed only with a reason
   (`bazel run //spec:update_rcorpus_<lane>`), never to make a regression pass.

## C.2 The reference lane

- `bazel build //spec:reference_lane_report` (manual, about 8 GB, about 45 s once `//tools/reference:ref_dump` is
  cached) → `bazel-bin/spec/reference-lane/core_relational.txt` and `core_relational-examples.tsv` (one example
  position per class) (`spec/BUILD.bazel:252-277`).
- `diff bazel-bin/spec/reference-lane/core_relational.txt spec/src/test/resources/reference-lane/core_relational.txt`.
- A deliberate move: `bazel run //spec:update_reference_lane`, and say in GATES which lines moved and why.
- `bazel test //spec:reference_lane //spec:update_reference_lane_test`: the first checks every disagreement class has a
  `reasons.tsv` row (`ReferenceLaneTest`), the second is the golden's diff test (`write_source_files` with one file
  makes `<name>_test`; `ReferenceLaneTest`'s javadoc names it).
- Bar: AGREE not down; no new class without a reason; DROPPED and FAILED explained.

## C.3 The strict-gap census (3b's yardstick)

`FailureCensus.java` and `StrictGapCensus.java` (plan:`docs/build-inventory/manifest-world/experiments/phase3b-census/`)
are in package `com.legend.generators` because they call `ManifestWorldCensusTest.manifests/closure` (package-private).
Outline (census README; the exact command is UNVERIFIED): compile them against the classpath of
`//spec:reference_lane_report`'s action (a scratch copy of it, from Phase 3: code:`runs/homework/phase3x/refcp.txt`,
exec-root-relative paths), run from the execution root with `-Dlegend.engine.root=<realpath of the engine tree>
-Dlegend.pure.root=<realpath of the pure tree>`, `-Xss16m` (the report's flag), a large heap, and an output path:
`StrictGapCensus <out.tsv>`. Compare `DROPPED`/`BROKEN`/`FAILED` rows with the committed `strict-gaps.tsv`. Remember the
file attribution of overloaded functions is unreliable until item 5b is fixed.

## C.4 Experiment 8, re-run

From the experiments README and assets (plan:`docs/build-inventory/manifest-world/experiments/`):
1. Copy the scripts to a scratch homework directory `H` (with `docstart.py`, `enginepat.py`, `system_fqns.txt`,
   `e6_owned_final.txt`, which `e8_extra.py` reads); write the two pinned trees' paths into `H/et` and `H/pt`.
2. `bazel build` the six corpus targets (also puts the upstream trees in the execution root).
3. `python3 H/e8_extra.py H <repo> <world>` with `<world>` = `core/src/main/resources/com/legend/builtin/prelude.pure`
   (today's prelude) → `H/e8_extra/` and `H/e8_extra.files`. For variant (ii) of 6-H1, a copy with the ownership
   filter (`owned_fns`, the `keep()` clause "the platform owns it") removed.
4. Apply `e8_MinimalCorpus.patch` to a copy of `MinimalCorpus.java` (its context still matches,
   `MinimalCorpus.java:443-451`), compile it against the spec test classpath into `H/probe/e8classes`.
5. `EXTRA_CP=H/probe/e8classes EXTRA_JVM='-Dexperiment.extraSources=H/e8_extra.files' LABEL=e8_today python3 H/e6_lanes.py <repo> H <unused> <an empty override directory>`
   (the script reads four arguments; the third is not used — UNVERIFIED what the author passed). Outputs:
   `H/e6_lanes/e8_today/<pass>/`, compared with `bazel-bin/spec/<pass>/` the same way as C.1.
6. Remember 6.7: the script forces `-Xmx4g`; for 6-H2 edit a copy to keep each pass's own `-Xmx`.

## C.5 PCT

`bazel test //pct:pct_duckdb //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b` (4 GB per suite; `pct/BUILD.bazel:111-255`).
"Identical" means every suite passes with its pinned expected failures. Channel B's discovery counts are exact pins
from `PctRatchets` (e.g. `ChannelBEssentialTest.java:66`): when 3b or 6 makes more upstream files load, a count may
move; re-measure with `bazel run //pct:update_ratchets` and give the reason in the commit.

## C.6 Projects and the user side

- `bazel test //projects:tests` (each project alone, the graph, the contract test; `projects/BUILD.bazel:120-126`).
- The user side as in experiment 5 (`probe/UserSideProbe.java`): build walls 4 → 0 after F-L1; body walls stay 146
  (`orElse`) until Phase 4.

---

# Appendix

## A. The reference lane's 32 strict drops, by real cause

| Files | Count | Real cause | Where it is fixed |
|---|---|---|---|
| `platform_dsl_mapping/functions_EnumerationMapping.pure`, `functions_Mapping.pure`, `functions_PropertyMappingsImplementation.pure`; `platform_store_relational/functions.pure`; engine `core_relational/relational/lineage/scanRelations/scanRelations.pure` | 5 | boot-layer twins not recognized (3b.4, 1b) | 3b item 1 |
| engine `core_relational/.../scanRelationsTestWithViewsAndUnions.pure`, `core_relational/relational/modelJoins/testModelJoinsToRelationalJoins.pure` | 2 | F-L1 | 3b item 1 |
| engine `core_service/service/mappingExtension.pure`, `core_data_space_metamodel/mappingExtension.pure`, `core/pure/router/router_main.pure` | 3 | overloads share one import scope (3b.4, 5b) | 3b item 5 |
| `core/pure/corefunctions/unit.pure`, `core/store/m2m/tests/legend/unitMeasure.pure`, `core_external_format_json/executionPlan/tests/dataTypes.pure`, `core/store/m2m/tests/legend/testUnitMeasure.pure` (knock-on) | 4 | units of measure are not types | Phase 6 (corpus); the strict lane follows |
| 16 model-to-model files (set-routed bindings 6, enum transformers 6, explosions 4) and 2 relational ones (`core_relational/relational/tests/mapping/modelJoin/modelJoinAdvancedSetup.pure`: an association over a model join "has no column binding"; `core_relational/relational/tests/mft/modelChain/modelChainTest.pure`: "Embedded PM 'address' on 'S_Person' but owner class unknown") | 18 | refused eagerly by the strict build, deferred by the tolerant one | not in this program (decided; but see 3b-O8 for the 2 relational ones) |

## B. Upstream versions at the boot layer's 29 names with no system twin (text-derived; verify by id, 3b-H2)

Compared by short-name signature (the way a function id spells types). "System" is
`core/src/main/java/com/legend/builtin/SystemMetamodel.java`.

| Name | Upstream version(s) with no twin | File |
|---|---|---|
| `meta::pure::lineage::scanRelations::relationTreeAsString` | `(RelationTree[1], String[1])`, `(RelationTree[1], Boolean[1], String[1])` | engine `core_relational/relational/lineage/scanRelations/scanRelations.pure:113, 123` |
| `meta::pure::mapping::propertyMappingsByPropertyName` | `(EmbeddedSetImplementation[1], String[1])`, `(OtherwiseEmbeddedSetImplementation[1], String[1])`, `(AggregationAwareSetImplementation[1], String[1])` | pure `platform_dsl_mapping/functions_PropertyMappingsImplementation.pure:84-94` |
| `meta::pure::mapping::superMapping` | `(PropertyMappingsImplementation[1]):PropertyMappingsImplementation[0..1]` — same parameters as the system's, which returns `SetImplementation[0..1]` (`SystemMetamodel.java:1183`) | pure `platform_dsl_mapping/functions_PropertyMappingsImplementation.pure:19` |
| `meta::relational::functions::typeInference::inferRelationalType` | `(RelationalOperationElement[1], Boolean[1])`, `(…, TranslationContext[1])`, `(…, Boolean[1], TranslationContext[1])` | engine `core_relational/relational/relationalExtension.pure` |
| `meta::relational::mapping::resolvePrimaryKey` | `(InstanceSetImplementation[1])`, `(RelationalInstanceSetImplementation[1])`, `(RelationFunctionInstanceSetImplementation[1]):TableAliasColumn[*]` | engine `core_relational/relational/helperFunctions/helperFunctions.pure` |
| `meta::relational::runtime::extractDBs` | `(Mapping[*], Runtime[1])`, `(Mapping[1], Mapping[1])` | engine `core_relational/relational/helperFunctions/helperFunctions.pure` |

Total 14. Also: `meta::pure::extension::routerExtensions` has no upstream function at all — upstream's is the
qualified property `Extension.routerExtensions()` (engine `core/pure/extensions/extension.pure:46-49`).
`meta::pure::functions::meta::getLowerBound` is a system function in the platform namespace (twin in pure
`platform/pure/essential/meta/multiplicity/getLowerBound.pure`).

## C. The 58 OVERLOAD and 14 PACKAGE classes, one example each

From code:`spec/src/test/resources/reference-lane/core_relational.txt` and the examples file built 2026-10-07 02:52.
"Ref" is legend-pure's pick, "ours" is ours; the first look reads only the one example (UNVERIFIED for other calls).

| Kind | Spelling | Count | Ref → ours | Example (module path:line:col) | First look |
|---|---|---|---|---|---|
| OVERLOAD | `greaterThan` | 24 | `(Number[0..1], Number[1])` → `(Number[1], Number[1])` | `core_functions_relation/relation/tests/composition.pure:148:59` (`filter(x|$x.val > 2)` on a TDS) | optional column typed `[1]`; filter: same rows |
| OVERLOAD | `greaterThanEqual` | 1 | same shape | `core_functions_relation/relation/tests/composition.pure:1954:52` | same |
| OVERLOAD | `lessThan` | 1 | same shape | `core_functions_relation/relation/functions/transformation/extend.pure:893:36` (`filter(x | $x.id<9)`) | same |
| OVERLOAD | `lessThanEqual` | 2 | same shape | `core_functions_relation/relation/tests/composition.pure:1034:37` | same |
| OVERLOAD | `contains` | 2 | `(String[0..1], String[1])` → `(String[1], String[1])` | `core_relational/relational/tests/mapping/innerJoin/testIsolation.pure:25:68` (filter) | path through an optional property |
| OVERLOAD | `startsWith` | 2 | same shape | `core_relational/relational/tests/mapping/embedded/testEmbeddedMapping.pure:174:75` (filter) | same |
| OVERLOAD | `in` | 2 | `(Any[0..1], Any[*])` → `(Any[1], Any[*])` | `core_functions_standard/collection/in.pure:85:37` (inside `extend`) | computed column: missing value may differ (3b-H5) |
| OVERLOAD | `average` | 3 | `(Number[*]):Float[1]` → `(Integer[*]):Float[1]` | `core_relational/relational/tds/tests/testTDSRestrictDistinct.pure:198:70` | lambda parameter typed `Integer`; same result type |
| OVERLOAD | `max` | 3 | `(Number[*]):Number[0..1]` → `(Integer[*]):Integer[0..1]` | `core_relational/relational/tds/tests/testTDSRestrictDistinct.pure:196:70` | result type differs (3b-O4) |
| OVERLOAD | `plus` | 2 | `(Number[*]):Number[1]` → `(Float[*]):Float[1]` | `platform/pure/grammar/tests/composition.pure:34:51` | mixed list typed `Float` |
| OVERLOAD | `sum` | 2 | `(Number[*])` → `(Float[*])` | `core_relational/relational/tds/tests/testGroupBy.pure:310:95` | same |
| OVERLOAD | `sum` | 5 | `(Number[*])` → `(Integer[*])` | `core_relational/relational/testDataGeneration/tests/testDataGeneration.pure:1390:87` | same |
| OVERLOAD | `times` | 2 | `(Number[*])` → `(Float[*])` | `core_relational/relational/tds/tests/testGroupBy.pure:325:84` | same |
| OVERLOAD | `propertyMappingsByPropertyName` (two spellings) | 1 + 2 + 2 | `(EmbeddedSetImplementation…)` / `(OtherwiseEmbeddedSetImplementation…)` → `(InstanceSetImplementation…)` | `core/pure/mapping/XStore.pure:44:114`; `core_relational/relational/helperFunctions/helperFunctions.pure:357:96`; `core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:927:160` | ref's version is in a file we drop (item 1b) |
| OVERLOAD | `range` | 1 | `(Integer[1])` → `(Integer[1], Integer[1])` | `platform/pure/grammar/functions/math/sequence/range.pure:61:45` (`[:5]`) | desugar; same values |
| OVERLOAD | `elementToPath` | 1 | `(PackageableElement[1])` → `(Function[1])` | `core/pure/test/mft.pure:298:289` | both `String` |
| PACKAGE | `divide` | 1 + 2 | `math::divide` → `math::minus` / `math::times` | `core_functions_standard/math/aggregator/covariance.pure:19:161`; `core_functions_standard/math/trigonometry/tanh.pure:30:81` | position artefact |
| PACKAGE | `plus` | 1 + 1 | `math::plus` ↔ `string::plus` | `core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:995:195` and `:217` | position artefact (swapped) |
| PACKAGE | `size` | 9 | `collection::size(Any[*])` → `relation::size(Relation[1])` | `core_relational/relational/router/tests/testRouting.pure:368:28` | TDS treated as a relation; test expects the row count, ours gives it |

## D. The parse walls of the real manifest

From plan:`docs/build-inventory/manifest-world/build.txt` (last block) and `experiments/catalog-upstream-diff.tsv`.

| File (engine module path) | Position | Our error | The text |
|---|---|---|---|
| `core/pure/binding/executionPlan/tests/executionPlanTests.pure` | 693:52 | `;` is not a mapping separator | `fullName : $src.firstName + ' ' + $src.lastName;` then `}` |
| `core/pure/graphFetch/tests/sourceTreeCalc/subType/testOnSourceRoot.pure` | 336:127 | same | `targetAddress: $src->...getLocationStr();` then `}` |
| `core/pure/corefunctions/tests/language/testLambda.pure` | 71:42 | unsupported expression token: ARROW | `a|$a+'eee';->eval('hjhjh')` |
| `core/store/m2m/tests/legend/simpleObject.pure` | 1869:5 | trailing tokens after code block | `id : ...f($src, [])` newline `i : []` (no comma) |
| pure `platform/pure/grammar/m3.pure` | 15:10 | top-level `^Instance` must be followed by `(...)` | m3's bootstrap form |

Outside the manifest (same scan, other modules), the same two errors appear in flat-data, OpenAPI, persistence and XML
test files (`catalog-upstream-diff.tsv`).

## E. Key code sites (code worktree, 2026-10-07)

| What | Where |
|---|---|
| View index (F-L1) | `core/src/main/java/com/legend/compiler/ModelBuilder.java:410-461` (`ingestDatabase`), `viewsOf` `:1083` |
| Flat view list built | `core/src/main/java/com/legend/model/FromProtocol.java:173-177, 209-210` |
| `defaultSchemaViews()` (by identity) | `core/src/main/java/com/legend/model/DatabaseDefinition.java:85-96` |
| View lift | `core/src/main/java/com/legend/normalizer/LiftedViews.java:46-169` |
| Database name resolution | `core/src/main/java/com/legend/compiler/NameResolver.java:1254-1295`; rel-op resolution `:1520-1590` |
| Twin rule | `core/src/main/java/com/legend/builtin/SystemMetamodel.java:1472-1511` (`shadows`, `spelling`), `:1565-1588` (`withoutSystemShadows`) |
| Boot layer, graph layer | `core/src/main/java/com/legend/Compiler.java:268-292` (`boot`), `:327-350` (`withoutPreludeShadows`), `:359-375` (`normalizeWithSystem`) |
| Duplicate check (by id) | `core/src/main/java/com/legend/compiler/element/ModelIntegrity.java:150-173` |
| Implementation table, NO_ROW rule | `core/src/main/java/com/legend/platform/ImplementationTable.java:161-200`; kinds `Implementation.java` |
| Primitive spelling under the prelude tier | `core/src/main/java/com/legend/compiler/NameResolver.java:601-624`; own-package tier `:625-628`, `:208-212` |
| Per-name element maps | `core/src/main/java/com/legend/Compiler.java:145-209`; `parser/ElementParser.java:325, 393, 478`; `compiler/NameResolver.java:217-221` |
| Qualified-property routes | `core/src/main/java/com/legend/compiler/spec/Typer.java:438-459` (variable receiver), `:489-597` (dot call); `derivedOverloadArity` `:1614-1619` |
| `findProperty` | `core/src/main/java/com/legend/compiler/element/PureModelContext.java:394-418`; contract `ModelContext.java:221-230` |
| Ambiguity message crash | `core/src/main/java/com/legend/compiler/spec/InferenceKernel.java:1200-1210` |
| Ledger anchors | `core/src/test/java/com/legend/ParkedWorkLedgerTest.java:79-115` |
| Corpus runner | `spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:117-460, 783-795`; `Corpus.java:52-85` |
| The two lists | `spec/src/gen/java/com/legend/generators/UpstreamFiles.java:28-120` |
| Manifest reader | `spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java:49-118` |
| Registers | `spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:430-445, 1022-1145`; files `spec/src/test/resources/rcorpus/` |
| Parser: mapping separator, code block | `core/src/main/java/com/legend/parser/MappingProtocolParser.java:1340-1385`; `parser/SpecParser.java:351-360` |
| Dialect levels | `core/src/main/java/com/legend/parser/Dialect.java` |
| Unknown type | `core/src/main/java/com/legend/compiler/element/TypeClassifier.java:91-107` |
| Corpus lanes, reference lane | `spec/corpus.bzl`; `spec/BUILD.bazel:175-298` |
| PCT channel B | `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:56-170` |

## F. Upstream module directories used here

| Module | Directory inside the pinned tree |
|---|---|
| `core` (engine) | engine `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/` |
| `core_relational` | engine `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/` |
| `core_relational_test` | engine `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-test/src/main/resources/` |
| `core_relational_duckdb` | engine `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/` |
| `core_service` | engine `legend-engine-xts-service/legend-engine-language-pure-dsl-service-pure/src/main/resources/` |
| `core_data_space_metamodel` | engine `legend-engine-xts-data-space/legend-engine-xt-data-space-pure-metamodel/src/main/resources/` |
| `core_functions_standard` | engine `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-standard/legend-engine-pure-functions-standard-pure/src/main/resources/` |
| `platform` (legend-pure) | pure `legend-pure-core/legend-pure-m3-core/src/main/resources/` |
| `platform_dsl_mapping` | pure `legend-pure-dsl/legend-pure-dsl-mapping/legend-pure-m2-dsl-mapping-pure/src/main/resources/` |
| `platform_store_relational` | pure `legend-pure-store/legend-pure-store-relational/legend-pure-m2-store-relational-pure/src/main/resources/` |
| others (`core_functions_relation`, `core_external_format_json`, ...) | find by name: `find <tree> -name '<module>.definition.json' -not -path '*/target/*'` |
