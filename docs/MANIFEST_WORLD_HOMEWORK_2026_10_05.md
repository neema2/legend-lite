# The manifest world: does it work, and how it would work in practice (2026-10-05)

**The question (the user):**
- If every program states what it needs in a manifest, why have a prelude at all?
- Is Pure.java then just the curated set of upstream declarations we override?
- What does everyone get "for free"? Probably legend-pure's platform and the engine's core.

**Terminology:**
- "Upstream native" is upstream's `native function` keyword (a Java body).
- "Platform-lowered" is what our Pure.java declares (we have a lowering).
- Never bare "native" (UPSTREAM_BOUNDARY_PROGRAM.md section 0).

**Evidence** (in `docs/build-inventory/manifest-world/`):
- `WORLD_H.md`: the D9 trail, the rules, our loaders, how upstream loads;
- `repos.tsv`: the upstream module graph;
- `parse.txt`, `build.txt`, `bodies.txt`: probe runs of legend-lite's own compiler over candidate worlds;
- `graph.py`, `index.py`, `Probe.java`: the scripts.

## 1. What upstream is made of, and what it gives for free

**The pin (engine 4.145.0, pure 5.99.0):**
- 144 code repositories: 9 legend-pure (`platform`, from `platform.json`, plus 8 `platform_*`) and 135 legend-engine.
- 3,080 `.pure` files, 626,113 lines in all.
- Every repository names its package pattern and its dependencies.

**legend-pure: upstream's own "core".**
- `PureRuntime.loadAndCompileCore` compiles `platform*` and `core_functions*` first (`PureRuntime.java:248`).
- Manifests are enforced:
  - a package must match its repository's pattern;
  - dependencies are DIRECT only, so a function from an unlisted repository drops out of overload candidates and a
    type from one is "not visible";
  - a duplicate element is a parse error (WORLD_H section 4).

**legend-engine: a user's model.**
- It compiles against every repository on the server's classpath, minus `test_*`/`other_*` (`PureModel.java:144`),
  with no declaration by the user and no visibility check.
- The 4.145.0 distribution installs 123: all `platform*`, `core`, all `core_functions_*`, `core_relational` with 21
  dialect and SQL modules, external formats, languages and more.
- So "free" in the engine is whatever extensions the server installs.

## 2. Where what we carry today lives upstream

**Pure.java's 313 upstream FQNs** (plus 19 of legend-lite's own `meta::legend::lite`):

| Repository | FQNs |
|---|---|
| `platform` | 113 |
| `core` | 96 (mostly date and string functions in `core/pure/corefunctions`: 61 from dateExtension.pure) |
| `core_functions_standard` | 47 |
| `core_relational` | 33 (toDDL, sqlQueryToString, temp-table execution, test-data generation: spec programs the platform owns) |
| `core_functions_unclassified` | 25 |
| `platform_store_relational` | 8 |
| the rest of `core_functions_*` | 7 |

**prelude.pure's 637 declarations:**
- 87 are the m3 metamodel. Upstream defines it in `m3.pure`'s `^Instance` bootstrap form (903 KB), the one file
  our parser cannot read.
- The rest come from `platform_store_relational`, `platform`, `core`, `core_relational`, `platform_dsl_*`,
  `core_functions_*` and three engine compiler modules.

## 3. Measured feasibility: legend-lite's own compiler over candidate worlds

| World | Files | Elements | Parse walls | Parse | Build | Functions | Bodies that do not type |
|---|---|---|---|---|---|---|---|
| W0 legend-pure platform (9 repos) | 267 | 1,912 | 1 (m3.pure) | 0.16 s | 0.44 s | 1,245 | 200: 199 test functions, 1 library |
| W1 + engine `core_functions_*` | 460 | 3,283 | 1 | 0.22 s | 0.49 s | 2,407 | 232: 227 test, 5 library |
| W2 + the 76 engine files the product uses | 536 | 4,953 | 1 | 0.24 s | 0.53 s | 3,730 | 807 (closure gaps from whole mixed files) |
| corpus world: closure(core_relational), 27 repos | 1,772 | 24,451 | 5 (all in test files) | 0.63 s | n/a | n/a | n/a |

**Library bodies type almost completely.** The test-function failures are dominated by ONE gap: 190 of W0's 200
are calls to `meta::pure::functions::lang::new`.

**Build walls are only duplicates against today's boot**, so they vanish when the world replaces the copy:
- 32 (W0) and 57 (W1) are prelude copies;
- 10 are SystemMetamodel's own views over its system tables (`classMappingById`, `table`, `column`...), which
  core's system-shadow rule already handles;
- none collide with Pure.java.

**Coverage of today's prelude:**

| World | Covers | Missing |
|---|---|---|
| W0 | 341 of 637 | |
| W1 | 391 of 637 | |
| W2 | 550 of 637 | 86 m3 metamodel declarations and one stub |

So "platform plus core_functions" alone is not today's user vocabulary. The rest is in engine `core` and
`core_relational`, which mix that vocabulary with engine internals (the router, plan generation, printers, tests).
Of the 76 engine files involved:
- 21 (2,035 lines) are mostly product vocabulary;
- 55 (20,177 lines) contribute only 160 elements.

**Size, with every test element stripped** (1,343 PCT tests, 536 `<<test.Test>>`, `ExcludeModular`, `ToFix`, and
`::tests::` packages):

| Part | Size |
|---|---|
| platform plus core_functions, no tests | 0.32 MB |
| the 21 mostly-used engine files, no tests | 0.09 MB |
| only the used elements of the 55 mixed files | 0.14 MB |
| **realistic default world** | **0.55 MB** (0.53 MB without comments), plus the m3 metamodel in compact form |
| today's prelude.pure | 0.30 MB |

The earlier 3 to 4 MB was mostly tests (44% of W1), `m3.pure`'s bootstrap form, and mixed files taken whole.

These are sizing measurements made by top-level declaration segmentation. The generator would use the parser's
element spans.

## 4. How it would work in practice

1. **The registry is the curated override list:** one row per upstream declaration by `FunctionId`.
   - Rows are `Intrinsic`/`Form` (we lower it: today's Pure.java), `Body` (upstream's body is the implementation;
     the default, as the implementation table already does), `Refused` or `Unimplemented`.
   - Declarations always come from upstream. We own the verdicts.
2. **Our own additions, small:**
   - legend-lite's own `meta::legend::lite` declarations;
   - SystemMetamodel, the platform's views over its system tables;
   - the m3 metamodel bootstrap (built in, as upstream also bootstraps M3 in Java).
3. **A program WITH a manifest** (the corpus, PCT, an upstream test module) loads its closure from the pinned archive.
   That is test input, allowed. No prelude. Each module is loaded once.
4. **A program WITHOUT a manifest** (a user's model in Studio, DataCube, Query, the server) gets the DEFAULT MANIFEST.
   - That is the list of upstream modules legend-lite "installs", as a server installs extensions: upstream's own
     core (`platform*` plus `core_functions_*`) plus the slice of engine content legend-lite supports (named files
     and elements of `core` and `core_relational`).
   - The default manifest is a registration we own.
5. **The product never reads upstream at run time** (the reference-checkout tenet). So the default world ships as a
   GENERATED copy, tests stripped, about 0.6 MB, regenerated only by the bump from upstream plus the default
   manifest.
   - The WASM build has no ServiceLoader, so it is a resource generated ahead of time (WORLD_H section 3).
6. **The bump** regenerates the default world and the upstream records, writes the seal, and runs the corpus and
   PCT against the new archives.

So the "prelude" stops being a hand-built world. It becomes the generated default world, and only "what Java needs
at boot" (the m3 bootstrap, SystemMetamodel) stays built in, as the compiler design says.

## 5. What contradicts this today (WORLD_H sections 2, 7, 9)

- **Pure.java** keeps 837 signature texts and derives its 488 `AT_*` groups from them. The prelude generator drops
  upstream bodies by bare claimed name (245 FQNs). `FunctionCompiler` suppresses by FQN. All three should be keyed by
  `FunctionId`.
- **The documents disagree:**
  - AGENTS.md and TENET_CHARTER C6.3 still say "declarations only / never by LOADING", while WORLD_MAP rule 2
    (amended 2026-09-08) and the 2026-09-09 ruling allow the product to ship a generated copy of upstream's bodied
    platform functions.
  - The guard AGENTS.md names (`Runner.registerLibrarySource`) was deleted in `ff359bae2`; today's is
    `MinimalCorpus.refusePlatformNamespace`.
  - COMPILER_DESIGN 3.1 says a duplicate is an error, but the code keeps the first or drops shadows by FQN.
  - D6 says compiler internals are "never typed", while D10's compile-all types the whole world.
  - D7 says `platform` has no manifest, when it has `platform.json`.
- **Visibility:** upstream drops a function from a non-dependency repository out of overload candidates. A
  manifest-loaded corpus world that does not do the same could resolve overloads differently from the engine. This
  must be decided and tested.

## 6. Open, each with what settles it

**Update 2026-10-06:** `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md` settles several of these by measurement:
- item 1's "2b" question: upstream core whole, plus upstream's query surface, closed through runnable bodies;
- item 2: the default manifest is a module-level choice (the core and relational compiler modules), not a list of
  files or elements;
- item 3: measured in the JVM and the browser;
- item 4: the 87 declarations upstream has only in `m3.pure`'s bootstrap form stay built in.

Items 1's other sub-questions, 5 and 6 stay open, with the experiments' own open items.

1. **D9** (the compiler plan; open until its checkpoint C1). Four sub-questions:
   - the "2b" standard-library question, whose text exists only as "whole stdlib resource vs library-only is decided
     HERE, by measurement" (UPSTREAM_BOUNDARY_PROGRAM:273). Section 3's sizes ARE that measurement;
   - WORLD_MAP rule 8 (the deletion test) against loading every file;
   - a named, pinned register for roadmap test files (16 of the census's 32 walls are M2M "roadmap feature" refusals,
     all in `/tests/`);
   - whether upstream's tests of its own compiler belong in the world (the user's lean: fix what is general, wall
     internal entries by name).
2. **The default manifest's exact composition**, from evidence: which modules, files and elements the 56 model
   projects, the demo models and the corpus actually use.
3. **Boot time and the browser build** with the ~0.6 MB default world. The record has boot ~1.8 s growing to ~4.8 s
   for a larger world, and the WASM bundle is 4.2 MB raw today. Measure both.
4. **m3:** read `m3.pure`'s `^Instance` form, or keep today's respelled metamodel as the built-in bootstrap.
5. **Visibility semantics** for manifest-loaded worlds (section 5).
6. **Fix the stale documents** (section 5) once the rules are decided.
