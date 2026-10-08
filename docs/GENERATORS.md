# Every generator: what it is, when it runs, and the bump (2026-10-05)

Status: AGREED SHAPE (the user, 2026-10-05). Section 6 steps 1 to 3 and the small upstream generators are done on
`build/phase1-generators` (the program's Phase 1, 2026-10-06); see the program's Phase 1 status for what changed from
this file (ref_imports deleted; offer_queries and catalog_corpus reclassified). 2026-10-08: `//datacube:catalog_rules` and
`catalog_corpus` deleted with DataCube's TypeScript model writer (DataCube calls the compiler's writer).

**Update 2026-10-06:** the program's phases are now in `docs/REBUILD_PROGRAM_2026_10_06.md`. The homework and
experiments replaced section 2's "basic split now" (option (a)) for items 5 to 8 with the full design: the
implementation table at boot (Phase 3), the default world from upstream alone (Phase 4), and Pure.java as rows keyed
by function id (Phase 5). With those, no upstream generator keeps a hand-owned input, and section 6's steps 1 to 3, 5,
6 and 7 are Phases 1 and 7.

The evidence is in `docs/build-inventory/generators/`:
- G1 to G5 hold one dossier per generator: what it computes and why, its declared inputs against what its program
  reads, outputs, consumers, determinism, cost, what reruns it against what should, and who runs it today.
- G6 covers the machinery: every writer and diff test, `//:update_generated`, `//:generated`, the bump procedure, and
  every instruction to a human.
- `programs.txt` is the exact list of the 57 generators, from Bazel's graph at `build/rebuild` HEAD.

## 1. The rules

1. **A generator is placed on two axes.**
   - **What it is** (by where its inputs come from): upstream, ours, a measurement of our engine, a build output, or
     dead.
   - **When it runs:** automatically, or on demand (`manual`).
2. **Upstream-derived content is generated from the pinned checkouts alone, and only the bump generates it.** This is
   `docs/COMPILER_DESIGN_2026_09_25.md` tenets 4 and 5: "nothing about upstream is typed by hand", and "everything the
   platform decides for itself is a registration, in one registry". Our decisions never go into an upstream record
   and are never an upstream generator's input.
3. **The bump is self-contained.** Every upstream generator and report runs only inside `bazel run //tools/bump`.
   Nothing in the everyday build, the local gate or CI runs them, and nothing there needs the upstream archives
   except tests that use them (PCT, parser-equivalence).
4. **A generated file is generated whole.** Nobody edits it. Hand-written content lives in its own files, and what
   people edit are inputs (native membership, DynaFn resolutions, the Lite extensions).
5. **Our own generators run when our code changes**, never in the bump.

## 2. Upstream: the bump's records and reports

All are committed and regenerated only by `//:update_upstream` inside the bump. Each must be deterministic: two runs,
same bytes.

| # | Generator | Writes | What it is | Reads | Status |
|---|---|---|---|---|---|
| 1 | `//parser-equivalence:gen_fixtures` | engine-grammar-fixtures.jsonl | every grammar example from upstream's own test jars (parity corpus tier C6); read by gate 8, the manifest, the roster, the censuses | upstream only | clean; narrow (it reaches all of core today) |
| 2 | `//parser-equivalence:gen_manifest` | corpus-manifest.tsv | a sha256 of every upstream corpus source; a record read as a diff | upstream only | clean; kept as a bump record (the user) |
| 3 | `//tools/engine-runner:vocab` | vocab.tsv | every literal token upstream's grammar lexers know | upstream jars only | clean; kept as a bump record (the user); leaves keyword_coverage's tool data |
| 4 | `//spec:gen_imports` | CORE_IMPORTS | the default import list in the engine's order, from `CompileContext.META_IMPORTS` | upstream, plus its host file | split: moves to its own generated file, then clean |
| 5 | `//spec:gen_natives` | Pure.java membership signatures | upstream's exact signature text for the natives we implement | upstream, our membership list, our parser | split now; full catalog later |
| 6 | `//spec:gen_dynafn` | DynaFn.java rows | upstream's dynafunctions: name, dialects, inference | upstream, plus our resolutions read back from the file | split now; registry rows later |
| 7 | `//spec:gen_engine_handlers` | engine-handlers.tsv | upstream's handler surface (bare-name resolution for engine input) | upstream, our committed Pure.java and prelude | upstream-only later |
| 8 | `//spec:gen_prelude` | prelude.pure | the platform declarations loaded at boot | upstream, a scan of our core Java, Pure.java's hand shapes, the claims | mostly ours today; shrinks to "what Java needs at boot" in the compiler rebuild |
| 9 | `//tools/reference:ref_imports` | (report) | each source's import groups, as legend-pure's compiler uses them | upstream jars only | becomes a committed bump report; prove determinism |
| 10 | `//parser-equivalence:pmcd_reachability_census` | (report) | which upstream protocol classes are reachable from a model | upstream jars, plus our roster | commit the upstream half (reachability); the "uncovered" worklist becomes an on-demand report |
| 11 | `//spec:native_declarations` | (report) | upstream declarations of natives | upstream, plus our membership list | widen to every upstream native (the catalog); then commit |

**Items 5 to 8 (option (a), the user's decision): the basic split now.**
- Pure.java keeps its hand-written parts: the Lite extensions, legend-lite's own natives, the hand code. The
  membership signatures and overload groups move to their own fully generated class.
- DynaFn's resolutions move to a hand-owned decisions file; DynaFn.java is generated whole.
- CORE_IMPORTS gets its own generated file.
- The full upstream catalog, the one registry and the prelude shrink are compiler-rebuild work (W2.1, "the one
  registry"). Until then, items 5, 6 and 8 keep one hand-owned input each, and they regenerate in the bump.

Research behind the split (git history):
- Nobody has hand-edited a generated part. Pure.java's generated text changed only in the 4 commits that introduced
  its generator (2026-09-11).
- The hand edits are legend-lite's extensions (9 commits on Lite), DynaFn resolution decisions (1 commit, 10
  decisions), and ordinary resolver development (NameResolver.java, 93 commits).
- The problem is placement: every hand edit is an input to a generator that splices into the same file.

**Moves with a bump by itself (no bump step):** `//pct:adapter_par` (our adapter compiled against upstream; fix its
clock-stamped jar), and `//tools/reference:ref_dump`.

## 3. The bump: `bazel run //tools/bump -- <engine release>`

| Step | Today | Agreed |
|---|---|---|
| 1. Decide | release published on Central; pure release, tag commits and managed versions read from it | unchanged |
| 2. Move | rewrite release.MODULE.bazel's pins; re-pin maven_upstream and maven_runner | unchanged |
| 3. Regenerate | `//:update_generated`, ONCE: all 55 writers, ours and the ratchets, ladder and roster included. It re-blesses expected results silently, and one run is not a fixed point. | `//:update_upstream`: the upstream records only (items 1 to 8), checked to be a fixed point (a second run changes nothing) |
| 4. Reports | none (people must remember items 9 to 11) | items 9 to 11, committed |
| 5. Seal | none | write `upstream.seal`: the release, plus the sha256 of every bump record and report |
| 6. Check | `bazel test //...` (after step 3 already re-blessed things) | `bazel test //...`, nothing re-blessed first: every expected result that moved is a red diff, listed together |
| 7. Judge | a person re-pins ratchets and commits | a person reads the record diffs and reports, re-blesses each moved expected result on purpose with its reason, and commits |

**Every day, the seal check:**
- One test reads the committed bump records and `upstream.seal`, and fails if any record's hash differs. It takes
  milliseconds and needs no upstream and no generator.
- It catches a hand edit to a generated record, or one changed outside a bump. Its message says "generated by the
  bump; run `bazel run //tools/bump`".
- Whether a record is current with upstream can only change when upstream does, which is only ever a bump.
- So the bump generators never run in the everyday gate, even on a cold cache.

## 4. Ours: committed, generated from our own code (they run when that code changes)

| Generator | Writes | True trigger | Narrow to |
|---|---|---|---|
| `//scripts/corpus:gen_dense` | stress 59, 60, 64 | the dense builder, hand-written stress files, linked projects | its 14 modules; drop queries.pure (never read) |
| `//scripts/corpus:gen_stress` | stress 92 to 98 | build.py, queries.pure, stress files, gen_dense output | its 27 modules |
| `//core:stress_layout` | stress-layout.json | core/stress.bzl | (written at analysis time) |
| `//datacube:offer_facts` | offer-facts.ts (shipped) | our compiler, DataCube's query builder | the `//core:planner` closure |
| `//datacube:test_imports` | test_imports.bzl | import lines of DataCube's src and test | the files it reads, not 122 npm files |
| `//engine-client:lite_facts` | lite-facts.ts (shipped) | `compiler/element/type` | `//core:compiler_element_type` |
| `//legend-art:icons_gen` | icons.ts (shipped) | our `icons.mjs` table; rarely the react-icons pin | (not the bump) |
| `//warehouse:reachability_metadata` | the native image's metadata | Duck.java, AuthenticatedUser.java, the services table | a small library on `:server_lib` |

These stay in `//:update_generated` (run by people), with their diff tests in the everyday gate.

## 5. The rest

**Measurements of our engine (SET ASIDE by the user, 2026-10-05; unchanged for now):**
- the corpus judges (6): `//spec:judge_host_duckdb`, `judge_database_duckdb`, `judge_host_h2`, `judge_database_h2`,
  `judge_host_warehouse`, `judge_database_warehouse`;
- `//core:ladder_report`;
- the spec, parser-equivalence and PCT ratchets;
- `//fixtures/saved-queries:gen` (its records are ours; its row counts are a test);
- `//spec:reference_lane_report`;
- the coverage measurements `//parser-equivalence:gen_roster` and `//scripts/parser:keyword_coverage`;
- on demand: `corpus_census`, `grammar_keyword_census`, `eager_corpus_compile`, and `gen_own_corpus_draft` (it
  drafts gate 8's expected-differences ledger).

**Build outputs, never committed (testonly where only tests read them):**
- for the product: `//warehouse:duckdb_library` and `postgres_extension` (they rerun only on their pins), and
  `//datacube:dist` (replaced by D10's shared site rule; broken on this branch, fix `dab833263` on the held
  `bazel/exec`);
- for tests: `gen_differential` (drop the 7 stress files it never reads), `stress_index`, `offer_queries`,
  `cube_queries`, `cube_jvm_answers`, `jvm_answers`, `zone_jvm` (narrowed to `LiteralSpelling`), `ref_dump`, `pins`.

**Our tools, on demand (`manual`):**
- `//spec:native_membership_draft` (with `//core:draft_native_membership`): drafts our membership list;
- `//datacube:link_dictionary_next`: cuts the next share-link version.

**Dead (delete):**
- `//parser-equivalence:migration_sizing`: the parsers it measured were deleted (`b23f68757`);
- `//spec:eager_corpus_compile_world2`;
- `//spec:gen_claims` with `core_next` and its writer, if D2 retires native-claims.tsv. Nothing reads that file:
  ClaimRegistryTest names its path and never opens it.

## 6. The work, in order (each proven locally; Windows in the end-of-rebuild PR)

1. **Delete the dead; mark the on-demand tools and reports `manual`.**
2. **Build outputs:** testonly tags, the PCT adapter's constant jar timestamps, gen_differential's inputs.
3. **Narrow every generator to what it reads** (sections 2 and 4), and remove self-inputs (committed outputs out of
   the libraries their generators run on). Includes dropping vocab from keyword_coverage's tool data.
4. **The split (option (a)):**
   - CORE_IMPORTS into its own generated file;
   - Pure.java's membership signatures and overload groups into their own generated class;
   - DynaFn's resolutions into a hand-owned decisions file, with DynaFn.java generated whole.

   A core change: announced in IN_FLIGHT first, with a heads-up to core's owner.
5. **The reports in upstream-only form:** native_declarations widened to every upstream native; the reachability
   census's upstream half separated from the worklist. Determinism proven for all three.
6. **The self-contained bump:**
   - add `//:update_upstream`;
   - remove the upstream records and the measurements from `//:update_generated`, and tag it `manual` (it is a
     `bazel run` command);
   - the bump runs regenerate, reports, seal, test, in that order;
   - add the seal file and its everyday test, which replaces the upstream records' diff tests in the everyday gate.
7. **Wiring:** a guard that every writer and diff test belongs to a suite and a gate; the stale instructions (G6
   section 5) fixed.

Decisions still open: D2 (native-claims.tsv), and the measurement group when we return to it.
