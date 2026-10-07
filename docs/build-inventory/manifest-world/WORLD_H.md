# WORLD_H — the manifest world: D9, the rules, today's loading, upstream's loading, prior numbers

Read-only homework, 2026-10-05. Tree: `build/rebuild` at `1689703f2`; its `docs/`, `AGENTS.md`, `spec/` and
`core/src/main` equal `origin/main` (`06a290b76`) except one line of `spec/BUILD.bazel`
(`git diff --stat origin/main HEAD -- docs AGENTS.md spec core/src/main`). Pins: legend-engine 4.145.0, legend-pure
5.99.0 (`release.MODULE.bazel:24-25`).

Abbreviations used in citations:
- no prefix = the repo `the build/rebuild checkout`;
- `$PURE` / `$ENG` = `$(bazel info output_base)/external/+http_archive+legend_pure_src` / `…legend_engine_src`
  (output base `(local path)`);
- `M3` = `$PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3`;
- `STUDY` = `~/legend/platform-architecture/` (the "platform-architecture study" the repo docs cite as
  `~/legend/platform-architecture/`; it says "DRAFT … Not committed", `STUDY/PLATFORM_ARCHITECTURE.md:3`);
- `DIST` = `~/legend/engine-dist-4.145.0` (a local legend-engine server distribution, `DIST/pom.xml:8`
  `legend-engine-server-4.145.0-dist`, 862 jars in `DIST/lib`).

Terminology (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:20-53`): "upstream native" = upstream's `native function` (body in
Java); "platform-lowered" = what our `Pure.java` declares (we have a lowering), whether upstream wrote it native or
bodied. The bare word is not used below.

---

## 0. The answer in brief

1. **How legend-engine compiles a user's model.** `PureModel(PureModelContextData …)` builds the user's elements in
   Java on top of a PRE-COMPILED metadata world: `METADATA_LAZY = MetadataPelt.fromClassLoader(…,
   CodeRepositoryProviderHelper.findCodeRepositories(classLoader, true).collectIf(r -> !name.startsWith("test_") &&
   !name.startsWith("other_")))` (`$ENG/…/toPureGraph/PureModel.java:144`). So everything a user model can use
   implicitly is **every code repository whose `CodeRepositoryProvider` is on the server's classpath, minus `test_*`
   and `other_*`**, with no declaration by the user. In the 4.145.0 server distribution that is **123 repositories**
   (123 jars each register one provider; 123 distinct manifests: all 9 `platform*`, all 5 `core_functions_*`, `core`,
   `core_relational`, every relational dialect, external formats, languages, …; query in §4.3). The user compile
   applies no repository visibility (no `isVisible`/`Visibility` call in the engine compiler's main sources, §4.3).
2. **How legend-pure enforces a manifest.** A repository is `name` + `pattern` + `dependencies`
   (`M3/serialization/filesystem/repository/GenericCodeRepository.java:186-230`). `pattern` is enforced on every
   element's package (`M3/compiler/validation/validator/RepositoryPackageValidator.java:58-66`). `dependencies` are
   **direct only**: `isVisible(other) = this == other || dependencies.contains(other.getName())` (`…/GenericCodeRepository.java:60-64`);
   functions from a non-visible repository are not overload candidates
   (`M3/compiler/postprocessing/processor/valuespecification/FunctionExpressionProcessor.java:1243-1247`), and a
   non-visible type, profile or tag is an error "… is not visible in the file …"
   (`M3/compiler/validation/VisibilityValidation.java:91-106,144,206,248,264,276,299`). A duplicate element is a parse
   error (`M3/serialization/grammar/m3parser/antlr/AntlrContextToM3CoreInstance.java:3923,3956,3964`).
   `PureRuntime.loadAndCompileCore` compiles every repository named `platform*` or `core_functions*` first
   (`M3/serialization/runtime/PureRuntime.java:248`), then `loadAndCompileSystem` the rest (`:297-315`).
3. **D9 is three sub-questions plus a fourth that joined it on 2026-10-03** (§1): the "2b" stdlib question (whole
   standard library as a resource, or library-only), WORLD_MAP rule 8 (the deletion test) against "load every file",
   a named pinned register for roadmap test files, and whether upstream's unit tests of its own compiler internals
   belong in the world. Open, "to be ruled at C1" (`docs/EXECUTION_PLAN_2026_09_26.md:337`).
4. **The rules allow a generated copy in the product and allow tests to read the pinned archives** — but the rule
   texts disagree with each other (§2.6): `AGENTS.md:44-46,59-61` and `docs/TENET_CHARTER.md:196-199` still say
   "declarations only / spec by VERIFICATION, never by LOADING", while `docs/WORLD_MAP.md:189-196` (amended
   2026-09-08) and `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:175-176` (ruled 2026-09-09) put legend-pure's
   bodied functions in the generated prelude, which the product ships today.
5. **Nothing in the product thinks in repositories today** (§3.9). Three test-side programs already use upstream's
   own repository machinery: PCT channel A, the reference lane, and the PCT adapter's PAR; one census reads the
   manifests itself (`ManifestWorldCensusTest`, which ignores `pattern` and visibility).
6. **The FunctionId override model already exists in the implementation table** (rows keyed by `FunctionId`, `Body`
   by default, an `Intrinsic`/`Form` row means the body is never run; §3.3), but three things still contradict "Pure.java
   = a registry of overrides keyed by FunctionId": Pure.java still carries 837 signature texts and derives its 488
   `FunctionId` groups from them; the prelude generator drops upstream bodies by **bare claimed name** (245 FQNs); the
   compiler's merge point still suppresses by FQN (`isPlatformOwnedFunction`, the PCT twin rule).

---

## 1. D9 exactly

### 1.1 The row

> `| D9 | The manifest world | **OPEN; to be ruled at C1** (it blocks W4.4a). The 2b stdlib question,
> docs/WORLD_MAP.md §8 (the deletion test), and whether roadmap test files may be excluded by a named, pinned
> register [W4 F9] |` — `docs/EXECUTION_PLAN_2026_09_26.md:337`

C1 is the end-of-Phase-1 checkpoint: "decide: D11 (on W3.7's report), D9, D19 …" (`docs/EXECUTION_PLAN_2026_09_26.md:220`,
again `:410`).

### 1.2 Every place D9, W4.4a and W4.4b are discussed

| where | what it says |
|---|---|
| `docs/EXECUTION_PLAN_2026_09_26.md:337` | the row above |
| `…:220`, `:410` | D9 is decided at C1 |
| `…:311` | "M2M scope boundary (chains, JSON source, union) — W4.4b; boundary written at C1" |
| `…:427` | Phase 3 order: "… W4.3 steps 1–9 … → W4.4a → W5.0 …" |
| `…:436` | Phase 5 order ends "… W6.4 → W4.4b → W7" |
| `…:858-859` | "**W4.4a Load by manifest, non-M2M walls** (after D9); **W4.4b** M2M on the new passes, scope boundary from C1. Gate: census pins, boot-time growth within budget. Size 2–4 (W4.4b open-ended until its C1 boundary)." |
| `…:860-872` | finding 2026-10-03: `StatementExecutor.containsEffect` swallows `TypeInferenceException`; 23 callees (17 upstream library functions, 6 ill-typed core test helpers); 40 corpus tests reach it, 21 pass only because a `match` arm is not taken; "whether upstream's unit tests of its own compiler internals belong in the world is D9's question (the user's lean: fix what is general, wall upstream's internal-structure entries by name)" |
| `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md:12` | "B2 missing platform functions — the rebuild (W2.1 / W4.4a; D9) — part of the 191 missing overloads; never hand-ported one by one" (but see §5.4: the four named functions are not among the 191) |
| `docs/IN_FLIGHT.md:236-238` | the missing platform functions handed to the rebuild "(W2.1 / W4.4a, decision D9)" |
| `docs/plan-audit-2026-09-26/h1-plan-audit-2026-09-29/W4-middle.md:105-113` | **F9 BLOCKER** — the origin of D9: W4.4 conflicts with WORLD_MAP rule 8 (`MinimalCorpus.ENGINE_IMPLEMENTATION_FILES`, `refusePlatformNamespace`), "the open 2b question", "W2.1's duplicate refusal makes the duplicate count a precondition (census only reports it … count not verified)"; its walls (D7 classes 5, 6) are M2M features "thrown away if built before W4.1/W4.3"; needs W1.2; gate dropped (old step 9). "Change: W4.4a (non-M2M walls + rulings on 2b and rule 8) right after W3; … W4.4b the M2M features on the new IR; rule whether roadmap test files may be excluded by a named pinned register." |
| `…/W4-middle.md:57,62,141,153` | W4.4 "conflicts with the others (F9)"; size "4–6 for non-M2M walls, M2M open-ended"; verdict "split into W4.4a early and W4.4b last after rulings on 2b, rule 8 and a named exclusion register" |
| `…/h1-plan-audit-2026-09-29/README.md:52` | "D9 — The manifest world: the open 2b stdlib question, WORLD_MAP rule 8, a named register for excluded roadmap test files — W4 F9 — blocks W4.4" |
| `docs/plan-audit-2026-09-26/tractability-2026-09-29/4-w2-w7.md:49,195`; `…/README.md:57` | "D9 (blocks W4.4a) … rule both at C3 (P5)" — a C3 deadline, superseded by rev H4's C1 (`EXECUTION_PLAN:220`) |
| `docs/plan-audit-2026-09-26/meta-audit-2026-09-29/5-evidence-executability.md:71-72,93` | D9 "waits for W4.4a" was circular (fixed); W4.4b "open-ended (in no total)" |
| `…/meta-audit-2026-09-29/1-programming-language.md:166` | proposes cutting W4.4b to a second program |
| `…/meta-audit-2026-09-29/2-pure-engine.md:166` | M2M "partial | W4.4b, open-ended | name the scope boundary now" |
| `docs/plan-audit-2026-09-26/cold-read/2026-09-29-rev-H2.md:20` | "open D9 (before W4.4a)" |
| old plan, `git show 601995bc2:docs/EXECUTION_PLAN_2026_09_26.md` lines 421-437 | "Step 9 — F: load by manifest, names held constant … walls 32 → 0 … the corpus loader reads the manifest closure (27 modules, 1,772 files) strictly … Gate: walls 32 → 0; both rosters unchanged; the chain within the 12-minute budget; the census pinned; boot time back at the receipts' ~1.8s." |
| `docs/REAL_PLAN_2026_09_25.md:80-81,185-191` | "Load. The module manifests (`<name>.definition.json`) decide which files exist. No hand lists"; Step F = load by manifest |
| `docs/UPSTREAM_BOUNDARY_PROGRAM.md:276` (step 7) and `:278-398` (D7) | the full load-by-manifest design, measurements and rulings of 2026-09-25 (§2.5, §5.1) |
| `docs/COMPILER_DESIGN_2026_09_25.md:111-125` | §3.1 Load: the World (§2.4) |

### 1.3 The "2b" stdlib question — the full text found

The text exists in four places only; none defines "library-only":

- `docs/UPSTREAM_BOUNDARY_PROGRAM.md:273` (step 4): "The missing 191 overloads enter the declaration table as
  upstream's bodies (2b: whole stdlib resource vs library-only is decided HERE, by measurement)."
- `docs/UPSTREAM_BOUNDARY_PROGRAM.md:397-398`: "The 2b question (whole stdlib as a resource, or library-only) is still
  open and is decided at step 4 with the corpus and PCT numbers in hand, not before."
- `docs/OPEN_REGISTER.md:73` (F18): "2b stdlib-resource decision by measurement at step 4".
- the D9 row and the H1 audit (§1.2).

All were introduced by commit `cda91e93b` (2026-09-24, "workstream D re-chartered as the untangle";
`git log -S "2b question" -- docs/UPSTREAM_BOUNDARY_PROGRAM.md`). The label "2b" is not defined in the repo; the
study the commit came from has no "2b" either (`grep -n 2b STUDY/PLATFORM_ARCHITECTURE.md` matches nothing).
**OPEN: what "2b" and "library-only" denote** — settle with the author of `cda91e93b` or that session's transcript
(not found under `~/.claude/projects` or `(local path)` by `grep -rl "whole stdlib as a
resource"`).

The options on record that the question chooses among (each is a position some document takes; which one
"library-only" means is the OPEN above):

| option | text | status |
|---|---|---|
| A. **Upstream's own "core", whole** | "L1 stdlib — the `platform*` (9) and `core_functions_*` (5) repositories. Exactly upstream's own 'core' (`PureRuntime.java:248`) … verbatim and whole: 1,121 declarations … ~258 KB, tests excluded" (`STUDY/PLATFORM_ARCHITECTURE.md:221`); decision owed: "The stdlib boundary = upstream's 'core' (`platform*` + `core_functions_*`, including m3), taken whole" (`:464-465`); `prelude.pure` → "`stdlib.pure` (L1, whole) + `engine-api.pure` (L2, rows), generated from upstream only" (`:325`) | proposed, never ruled |
| B. **Today's rule: legend-pure's platform packages whole, engine bodies only by membership** | USER 2026-09-09: "The prelude carries legend-pure's platform packages WHOLE, functions included … T2 stands for engine files" (`docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:175-176`); "The engine's bodies are otherwise not the platform library (USER 2026-09-09)" (`spec/src/gen/java/com/legend/generators/PreludeGenerator.java:116-118`); one engine function carried by membership, `removeAll` (`PreludeGenerator.java:121-127`) | in force |
| C. **The namespace rule** | "Every bodied function upstream declares under `meta::pure::functions::` is platform library wherever its file sits" — admits 126 engine `corefunctions` functions (9 files); blocked by 26 boot-typing failures and the unowned `agg` form (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:360-371`); "measured and reverted … the rule is the target, not a slice" (`docs/GATES.md:5507-5519`) | target, not landed |
| D. **The prelude shrinks to the boot vocabulary** | load by manifest "shrinks the prelude to the boot vocabulary the platform's Java needs" (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:276`); "The prelude survives only as 'what the platform's Java itself needs at boot', which is small" (`docs/COMPILER_DESIGN_2026_09_25.md:123-125`) | written for the CORPUS world; silent on what a user program gets |

Measurements on record for option A: the whole-stdlib probe (460 files, 1 load wall, 1,741 bodies typed, "Only 3
library bodies fail"; parse+load 0.48 s, typing 0.46 s) (`STUDY/PLATFORM_ARCHITECTURE.md:492-493`,
`STUDY/stdlib-probe.txt:1-3`); the standing census types the five `core_functions_*` roots with ≤ 6 failures
(`spec/src/test/java/com/legend/generators/SpecBodyCensusTest.java:54,266-285`). The 2b decision was to be taken "by
measurement" with corpus and PCT numbers (`UPSTREAM_BOUNDARY_PROGRAM.md:397-398`); **no such measurement is recorded**
(OPEN; settle by running the corpus and PCT lanes with a generated copy of option A's 14 repositories as the boot
layer).

### 1.4 Sub-question 2 — WORLD_MAP §8 rule 8 against "load every file"

Rule 8: "An engine file is admitted as a program library only if every function in it passes the deletion test as a
program. A file mixing programs with the engine's internals is not admitted whole; tests whose SUBJECT is the
internals are walls. No laziness, no partial evaluation of a record, no phantom vocabulary to slip past a wall."
(`docs/WORLD_MAP.md:183-187`). Load by manifest is "every `.pure` file of every module, tests included"
(`docs/UPSTREAM_BOUNDARY_PROGRAM.md:276`). The closure of `core_relational` contains the engine's SQL compiler, printer,
router and plan generator (`spec/src/test/java/com/legend/rcorpus/EagerCorpusCompileProbe.java:188-207` walls 17 such
path fragments). Options on the table:

- **walls by file** for engine machinery in the corpus, **walls at the body** for engine classes' bodies in the
  prelude (USER 2026-09-09, `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:177-181`);
- **the entry is ours** (D6, user 2026-10-03): "Where upstream's Pure COMPILER … is reached from a program, the ENTRY is
  ours: a native/`Subsumed` implemented by our Java — never upstream's Pure internals typed or run. An override is either
  the real Java equivalent or a NAMED wall; never a no-op" (`docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md:46`);
- the A2 options for upstream's unit tests of its compiler: "(i) make them compile and run …; (ii) WALL their entries
  (`WalledBodies`, ENGINE_MACHINERY, with a reason) …; (iii) `Subsume` where a test only needs the value to be typed"
  (`…_2026_10_03.md:241-247`);
- the user's lean: "fix what is general, wall upstream's internal-structure entries by name"
  (`docs/EXECUTION_PLAN_2026_09_26.md:871-872`).

### 1.5 Sub-question 3 — a named, pinned register for roadmap test files

D7 made zero walls a precondition: "Precondition: the closure's walls at zero" (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:276`),
with the M2M walls "product features the corpus's own M2M tests want; build them as features with their own tests"
(`:334-337`). The question is whether those test files may instead be excluded from the world by a named, pinned
register (`W4-middle.md:113`). The evidence: of the census's 32 load walls, **16 are "… is a roadmap feature" M2M
refusals, and all 16 are files under a `/tests/` directory** (per-set routing 6, enum transformers 7 counting the json
one, explosions 4 — read from `STUDY/receipts/untangle-4b/manifest-census-core_relational.txt:96-129`; the
others are 5 parse walls, 7 unknown types, 2 duplicate view functions, 2 other model refusals). No named exclusion
register exists today beyond `MinimalCorpus.ENGINE_IMPLEMENTATION_FILES` (one entry,
`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:124-126`) and the probe's unpinned `WALLED_FILES`.

### 1.6 Recorded leans and rulings that bind D9

| date | who | ruling / lean | where |
|---|---|---|---|
| 2026-08-28 | user | checkouts are spec and test input, never runtime; the stdlib-namespace engine files (`corefunctions/*Extension.pure`, `testExtension.pure`) refused; "their nine names — remove, equalIgnoreCase, isDigit, … — are platform rows, owed" | `AGENTS.md:37-51`; `docs/LEDGER_GRANULAR_2026_09_06.md:757` |
| 2026-09-03 | user | the world map (three kinds, deletion test) | `docs/WORLD_MAP.md:3-7` |
| 2026-09-08 | user | prelude = system code generated from the spec, bodies included (rule 2 amended, rules 7–8) | `docs/WORLD_MAP.md:170-196` |
| 2026-09-08 | user | T1–T5; closure option B ("declarations closed, bodies resolved where they run") | `docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md:73-100,270-278` |
| 2026-09-09 | user | §10.3 rulings 1–4 (platform packages whole; engine bodies walled at the body / by file; no stub functions) | `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:174-186` |
| 2026-09-25 | user | D7 rulings (a)–(e): no tolerant load mode; interim hand lists pinned; world change after 4b with names constant; reject "carry any engine function the platform's Java names"; the folder keys on declarations | `docs/UPSTREAM_BOUNDARY_PROGRAM.md:373-381` |
| 2026-09-28/29 | user | D4 no tolerant modes; 0b.9 strict = collect all, poison, fail; D10 two modes (compile-all fails on any) | `docs/EXECUTION_PLAN_2026_09_26.md:131-132,332,338` |
| 2026-10-03 | user | D6 the entry is ours, upstream compiler internals never typed or run; "Everything we load must compile (it is a different thing from being able to run)" | `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md:46,153-155` |
| 2026-10-03 | user (lean) | fix what is general, wall upstream's internal-structure entries by name | `docs/EXECUTION_PLAN_2026_09_26.md:871-872` |

---

## 2. The rules any manifest world must obey

### 2.1 The reference-checkout tenet

> "The reference checkouts … are SPEC and TEST INPUT only — corpus test sources, PCT trees, parity fixtures, signature
> verification. They are NEVER runtime components: no platform behavior (resolution, typing, stdlib bodies) may depend
> on files from those checkouts. The platform stdlib (`meta::pure::functions::*`) is OURS — registry natives with
> signatures verified verbatim against the real `.pure` sources (spec by VERIFICATION, never by LOADING),
> platform-owned so parsed twins suppress. The line to draw: does the checkout feed the thing UNDER TEST (fixture —
> fine) or the thing DOING THE JUDGING/RUNNING (violation)? Mechanical guard: `Runner.registerLibrarySource` refuses
> `meta::pure::functions::` elements (`LibraryPlatformNamespaceGuardTest`)." — `AGENTS.md:37-51`

The named guard no longer exists: `Runner` was deleted in Batch 115 (`ff359bae2`, "the platform-namespace guard lives
in the kept loader"; `git log -S registerLibrarySource`). Today the guard is
`MinimalCorpus.refusePlatformNamespace`, which refuses FUNCTIONS only (classes under the package are fixtures):
`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:783-797`, applied to every library source (`:262`) and to
everything the corpus parses (`:278`); tested by
`spec/src/test/java/com/legend/rcorpus/LibraryPlatformNamespaceGuardTest.java:26-44`. It guards only the corpus
harness; ChannelB, the censuses and the eager probe load `meta::pure::functions::` bodies without it (§3).

### 2.2 WORLD_MAP

- The deletion test: "If this function vanished, would any ordinary user query stop compiling to SQL? Yes → platform
  semantics (kind 2). Own it in Java, from the spec. No → a program (kind 3). Compile it." (`docs/WORLD_MAP.md:41-54`).
- Rule 1: "Engine and pure checkouts are spec. Never loaded, never executed." (`:115-116`).
- Rule 2 as first written: "The platform ships a prelude of declarations only … No bodies." (`:117-118`), with the
  2026-09-04 amendment "behavior is curated, shapes are data" and its trigger for verbatim files: "a shipped legend-lite
  without the checkouts, or a second module needing its own prelude selection" (`:119-131`).
- **Rule 2 amended 2026-09-08**: "the prelude is system code that looks and acts like user code. Shapes AND their
  derived-property bodies AND the spec's Pure-bodied functions are GENERATED from the spec with receipts, compiled by
  the same compiler as a user query … The spec draws the native/program line: `native function` → rule 7; `function`
  with a body / a derived property → a program." (`:189-196`).
- Rule 7: "A native signature without a lowering rule may not exist. A spec `native function` is `Pure.java` + one SQL
  lowering, or a NAMED row in the permanent lowering list …" (`:176-182`). (The prelude now carries 66 respelled upstream
  natives that fail at lowering, `core/src/main/resources/com/legend/builtin/prelude.pure:6588-6589`, under
  `UPSTREAM_BOUNDARY_PROGRAM.md:150`'s D3 — whether that satisfies rule 7's "named row" is OPEN; settle by checking
  `NativeCatalogGovernanceTest`'s list against the 66.)
- Rule 8 (§1.4).

### 2.3 The compile-everything rulings (2026-09-09)

- §10.2: "The corpus + legend-pure's platform packages WHOLE closed 111 world-1 failures … but poisoned 523 elements
  (the corpus tree's copies collide). Conclusion: the library enters through the boot layer, never as graph sources."
  (`docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:169-172`)
- §10.3: "1. The prelude carries legend-pure's platform packages WHOLE, functions included (T1 as written; batch 155's
  narrowing to shapes was wrong). T2 stands for engine files. 2. Engine classes' bodies in the prelude are walled AT
  THE BODY when they are the engine's implementation of a platform concern …; engine machinery in the corpus is walled
  BY FILE. … No stub functions … 3. `defaultExtensions()` is a platform function, typing-only … 4. The four natives get
  registrations (a lowering or a named wall)." (`:174-186`)
- §10.4: of legend-pure's 257 bodied functions, "20 are the system store's …, 161 overloads share a name with a native
  or an operator form (… a library twin CAPTURES bare calls through the core imports: batch 169's 12 lost tests) …; 74
  remain and are carried" (`:188-193`).

### 2.4 The compiler design's §3.1

> "Output: an immutable `World`: for every file in the manifest closure, its parsed sections; a `DeclarationTable` …
> Rules: the closure is read from the manifests, strictly; the platform's own declarations (the catalog of natives,
> generated from upstream) are a module like any other; a declaration twice under one identity is an error (upstream
> refuses it too), not a first-wins. What this replaces: the hand lists of library and shape files, the prelude's
> membership lists, and the prelude as a hand-built world." — `docs/COMPILER_DESIGN_2026_09_25.md:113-125`

The page is marked superseded and "kept as history and as homework where the plan cites it" (`:3-4`); its corrections
banner does not touch §3.1 (`:6`). The current plan keeps part of the duplicate rule: "Refuse model↔model duplicate ids
now; native↔model twins wait for W2.8" (`docs/EXECUTION_PLAN_2026_09_26.md:664`). Upstream does refuse duplicates
(§4.2).

### 2.5 Other rules that bind a manifest world

- D7 rulings (a)–(e) and the switch: "the strict builder, one pass, at zero walls … The interim loading rule is the hand
  list … pinned shrink-only … the prelude keeps only what the platform's Java names at boot … gate: both corpus rosters
  LOST 0, the chain inside budget, the manifest census pinned" (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:373-391`); watch item:
  "a bodied library function under a name the platform implements … must lower by the implementation table's row, never
  by its loaded body" (`:389-391`).
- Strictness: rule 0b.9 (`docs/EXECUTION_PLAN_2026_09_26.md:131-132`), D4 (`:332`), D10 two modes (`:338`).
- Prelude tenets: "T1 — The prelude is what exists before any program is written … T2 — The graph is everything a
  program declares or brings in as a library, by file … T4 — A name declared on both sides is a modeling error, not a
  contest … T5 — The module is a closed library the boot layer checks" (`docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md:75-100`).
- "generated into the prelude from the spec (allowed, §4), never loaded from the checkout at run time"
  (`docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md:66-69`).
- Knowledge is eager over the declared closure, structure materialized on demand, "never force a transitive load"
  (`docs/TENETS.md:50-53,75-76`).
- The boundary program's four sentences, especially "core has ZERO upstream dependencies; every upstream fact inside it
  is GENERATED from the pinned release" (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:74-80`).

### 2.6 Where the rules conflict

1. **Bodies in the product.** `AGENTS.md:44-46` ("registry natives … never by LOADING, platform-owned so parsed twins
   suppress"), `AGENTS.md:59-61` ("The platform ships declarations … and views over its own system tables; no other Pure
   bodies") and `docs/TENET_CHARTER.md:196-199` (C6.3 "The prelude ships declarations only") against
   `docs/WORLD_MAP.md:189-196` and `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:175-176`, and against the shipped
   prelude ("485 classes, 23 enums, 153 functions (legend-pure's platform library, bodied and non-test …)",
   `prelude.pure:6584`). AGENTS and the charter were not updated after the amendments.
2. **Rule 8 vs load-every-file** (§1.4, `W4-middle.md:105-107`).
3. **The library as graph sources.** COMPILE_EVERYTHING §10.2 ("never as graph sources", `:172`) vs D7 step 7
   ("every `.pure` file of every module", `UPSTREAM_BOUNDARY_PROGRAM.md:276`): the `core_relational` closure includes
   `platform*` and `core_functions_*`, whose declarations the product's boot layer also carries.
4. **Duplicates.** COMPILER_DESIGN §3.1 (an error) vs today's first-wins and shadow drops (§3.1 below:
   `Compiler.parseSources` first wins, `withoutPreludeShadows` drops graph copies by FQN, `withoutSystemShadows` drops
   same-signature graph functions, `DeclarationTable` keeps the first and reports). The study proposes "deduplicated by
   **provenance** (same module and same source), not by name" (`STUDY/PLATFORM_ARCHITECTURE.md:230-234`) — never ruled.
5. **The namespace guard vs a manifest corpus.** `refusePlatformNamespace` (§2.1) refuses exactly the
   `meta::pure::functions::` functions a manifest closure contains (engine `core` declares such functions, e.g. the
   MISSING overloads of `string::contains`, `date::isAfterDay` in `core/pure/corefunctions/…`, `STUDY/catalog-upstream-diff.tsv`).
6. **Tolerance.** D7(a) forbids a tolerant load for the corpus, but the corpus builds with the poisoning builder
   `Compiler.buildModule` (`MinimalCorpus.java:293`; "Tolerant module build — POISON, DON'T DROP", `core/src/main/java/com/legend/Compiler.java:396-411`).
   Whether D10's two modes supersede D7(a) here is OPEN (settle: the user, at D9).
7. **"Never typed" vs compile-all.** D6 "never upstream's Pure internals typed" vs D10 "Compile-all types the whole
   world" — reconcilable only by walls refused before typing ("`SpecCompiler.compile` refuses a walled body before typing",
   `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:234`).
8. **A stale fact in D7.** "legend-pure's `platform` repository has no manifest in the tree (it is declared in Java
   upstream)" (`UPSTREAM_BOUNDARY_PROGRAM.md:285-286`; also `ManifestWorldCensusTest.java:49-52`) — the pinned tree has
   `$PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform.json` (`"name": "platform", "pattern":
   "((meta)|(system)|(apps::pure))(::.*)?", "dependencies": []`), loaded by
   `M3/PlatformCodeRepositoryProvider.java:43-47`. It is named `platform.json`, not `*.definition.json`, which is why
   the census's filter (`ManifestWorldCensusTest.java:66`) misses it.

### 2.7 The two direct questions

- **May the PRODUCT carry upstream bodied functions (the platform library) as a generated copy?** Yes for legend-pure's
  platform packages: generated copies are allowed, loading from the checkout at run time is not
  (`docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md:66-69`; `docs/WORLD_MAP.md:189-196`;
  `docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:175-176`), and the product does it (`prelude.pure:6584`). For the
  engine's `core_functions_*` (the other half of upstream's "core"), **not ruled**: excluded by
  `PreludeGenerator.java:116-118` except by membership; option A of §1.3 is unruled. The contrary texts in §2.6 item 1
  are the open conflict.
- **May TESTS load upstream files from the pinned archives?** Yes as spec and test input (`AGENTS.md:39-41`); the
  archives exist for that ("Tests read it as the SPEC … never as a runtime component", `third_party/legend_pure_src.BUILD:1-6`;
  `release.MODULE.bazel:256-273`). The test is "UNDER TEST (fine) or DOING THE JUDGING/RUNNING (violation)"
  (`AGENTS.md:47-49`). The corpus may not load platform-namespace FUNCTIONS (§2.1). Whether ChannelB — which loads the
  `platform/pure` tree and `core_functions_standard` and runs PCT tests on them without the guard
  (`pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:80-151`;
  `…/ChannelBStandardTest.java:36-44`) — is on the right side of that line is **OPEN** (settle: the user, at D9).

---

## 3. How legend-lite loads Pure today

### 3.1 The product's boot layer

- `prelude.pure` (7,018 lines, 300,247 bytes; 175 `###Pure` sections, one per spec file, `wc`/`grep -c`), GENERATED
  (`prelude.pure:4`). Its footer: 485 classes, 23 enums, 153 legend-pure bodied non-test functions (`:6584`); 1 engine
  function by membership, `removeAll` ×2 (`:6585-6587`); 66 respelled upstream natives (`:6588-6589`); engine natives not
  carried (`:6590-6599`); 245 PLATFORM-OWNED NAMES not carried (`:6600-6846`); 21 SYSTEM-OWNED FUNCTIONS (`:6847-6869`);
  147 T4 receipts (`:6870-`).
- `Prelude.java` reads the resource and parses it once at class load (`core/src/main/java/com/legend/builtin/Prelude.java:39-56`).
- `SystemMetamodel.java`: Pure source in a Java string (`:537`), parsed once at class load (`:1516-1518`); its function
  twins win over the library (`withoutSystemShadows`, `:1561-1582`).
- `Compiler.boot()`: system metamodel + prelude, resolved and normalized once per process, content-addressed by the hash
  of both sources (`core/src/main/java/com/legend/Compiler.java:245-305`). Every user graph is resolved alongside the boot
  FQNs (`:218-223`), its copies of prelude classes/enums/functions dropped by FQN (`withoutPreludeShadows`, `:327-344`),
  then joined with the boot layer (`normalizeWithSystem`, `:357-371`). `buildModel` is strict; `buildModule` poisons
  (`:396-417`). `compileAllBodies` skips boot functions (`:717-734`).

### 3.2 The generator of the prelude

`PreludeGenerator` indexes three engine roots and the whole pure tree (`spec/src/gen/java/com/legend/generators/PreludeGenerator.java:139-147,160-197`);
its demand is T1: legend-pure's nine platform roots whole, the Java vocabulary, the closure (`:372-397`). Engine
functions enter only by membership (`:114-127`). **Override by bare name:** an upstream body whose simple name is
claimed by the registry or is a `CoreFn` form is not carried (`:531-571`, `Claims.claimedBareNames()` at `:541`).

### 3.3 The platform-lowered constants and the tables

- `Pure.java`: 837 `signature("native function …")` sites, 12 `nativeClass(`, 6 `nativeEnum(` (grep counts),
  each parsed at class load (`core/src/main/java/com/legend/builtin/Pure.java:146-160,316-330,737-753`); text generated
  from `native-membership.tsv` (798 lines) by `spec/src/gen/java/com/legend/generators/NativesGenerator.java:30-58`.
  488 `AT_*` overload groups of `FunctionId`, each `FunctionId.ofAll(<parsed signature constants>)` (`Pure.java:2281-2768`).
- `FunctionId` = the qualified signature id string, from `SignatureMangle.mangle` (`core/src/main/java/com/legend/model/FunctionId.java:7-24`).
- `NativeFn`: 21 implementer family enums, each member carrying its Pure.java overloads (`core/src/main/java/com/legend/builtin/NativeFn.java:31-48,128-1102`).
- `DeclarationTable`: one declaration per `FunctionId`; a catalog declaration and upstream's bodied twin are one, the
  bodied kept; a different second body is reported, not refused (`core/src/main/java/com/legend/platform/DeclarationTable.java:17-68`).
- `Registrations`: lowering keys and feature overrides keyed by `FunctionId`; families by their overloads; **forms,
  walled natives, walled bodies and subsumed programs keyed by FQN** (`…/platform/Registrations.java:36-45`).
- `ImplementationTable.build`: total; default `Body` for a bodied declaration, `Unimplemented` for an upstream native;
  dangling and conflicts reported (`…/platform/ImplementationTable.java:58-189`); `runsByRule` = never run the body
  (`:220-224`). Per model the tables cover `Pure.all()` plus every model function, boot layer included
  (`core/src/main/java/com/legend/compiler/element/PureModelContext.java:583-601`); assembled in
  `core/src/main/java/com/legend/lowering/PlatformRegistrations.java:54-95`.
- The merge point still suppresses by FQN: `isPlatformOwnedFunction` and the PCT twin rule
  (`core/src/main/java/com/legend/compiler/element/FunctionCompiler.java:64-122`); W2.8 deletes them (`docs/EXECUTION_PLAN_2026_09_26.md:735-737`).

### 3.4 The corpus

- Hand lists: `LIBRARY_FILES` 6, `SHAPE_FILES` 64, `STDLIB_ENGINE_ROOTS` 5, `PLATFORM_ROOTS` 9
  (`spec/src/gen/java/com/legend/generators/UpstreamFiles.java:34-56,70-134,142-147,151-160`); `Corpus` resolves them
  (`spec/src/test/java/com/legend/rcorpus/Corpus.java:48-84`).
- `MinimalCorpus()`: 4 shared files (`MinimalCorpus.java:399-408`), the `core_relational/relational` tree
  (`:212-231,417-427`; 552 of the module's 553 files — `core_relational/legend/objectReference/objectReference.pure` is
  outside it, `find`), minus `ENGINE_IMPLEMENTATION_FILES` (`:124-126,222-226`); the M2M tests directory, the graphFetch
  domain and `LIBRARY_FILES` as library sources (`:247-275,429-450`); the namespace guard (`:262,278`); duplicates
  throw (`:282-285`); `SHAPE_FILES` classes and enums only (`:292,350-397`); `buildModule` (`:293`).
- `EagerCorpusCompileProbe`: world 1 = the corpus world (`spec/src/test/java/com/legend/rcorpus/EagerCorpusCompileProbe.java:38-40`);
  world 2 = + the nine platform roots whole, 400-round drop loop over `buildModule`, upstream natives dropped (`:111-176`).

### 3.5 Censuses that read upstream

`SpecBodyCensusTest` (platform roots + the five `core_functions_*`, strict pins, `SpecBodyCensusTest.java:54-299`);
`ManifestWorldCensusTest` (manifest closure; `name` and `dependencies` only, `pattern` ignored, `platform` patched in
with no dependencies, the closure flattened into one world, upstream natives dropped, `buildModel` with a drop-and-retry
loop; `spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java:54-56,62-122,134-202,351-354`);
`CatalogUpstreamDiffTest` (`spec/src/test/java/com/legend/generators/CatalogUpstreamDiffTest.java:30-201`);
`ImplementationTableTest` (catalog + whole stdlib + engine declarations at registered FQNs,
`spec/src/test/java/com/legend/generators/ImplementationTableTest.java:39-128`).

### 3.6 PCT

- Channel A runs upstream's PCT framework on upstream's INTERPRETED runtime from the pinned jars
  (`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:9-24`); the runtime finds
  repositories by ServiceLoader, including ours: `core_legend_lite_pct` (`pct/src/main/resources/core_legend_lite_pct.definition.json`:
  pattern `(meta::legend::lite::pct)(::.*)?`, dependencies `platform`, `platform_dsl_tds`, `core`), registered by
  `pct/src/test/java/org/finos/legend/pure/code/core/LegendLitePCTCodeRepositoryProvider.java:28-31` and
  `pct/src/test/resources/META-INF/services/org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProvider`;
  its PAR is compiled by upstream's own generator (`//pct:adapter_par`, `pct/BUILD.bazel:37-53`;
  `tools/par/ParGenerator.java:11-50`). Each test expression and the classes it names are packed into text lite compiles
  (`pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java:19-28`).
- Channel B: our compiler over hand-picked roots (e.g. `platform/pure` + `core_functions_standard`,
  `…/channelb/ChannelBStandardTest.java:36-44`; `core_functions_standard`'s own dependencies `core_functions_variant` and
  `core_functions_relation` are not loaded), upstream natives dropped, `buildModel` with a 200-round drop loop
  (`…/channelb/ChannelB.java:80-160`).

### 3.7 The reference lane

`tools/reference/RefResolutions.java:38-41`: `new PureRuntimeBuilder(new CompositeCodeStorage(new
ClassLoaderCodeStorage(CodeRepositoryProviderHelper.findCodeRepositories()))).build(); runtime.loadAndCompileCore();
runtime.loadAndCompileSystem();` — upstream's own manifest world from the pinned jars (`tools/reference/BUILD.bazel:65-84`).

### 3.8 A user's model

- Server: `PureV1Api.compile` → `Compiler.compileAllBodies(Compiler.compileModel(modelText(…)))`
  (`core/src/main/java/com/legend/server/PureV1Api.java:137-151`); the other endpoints use `compileModel` (`:164,194,210,234`).
  `compileModel` parses one text at the LEGEND_LITE level and calls `buildModel` (`Compiler.java:72-91`). No manifests,
  no repositories: the user sees the boot layer plus the catalog.
- WASM planner: the same `Compiler.compileModel` (`wasm/src/main/java/planner/Wasm.java:94,105,141,168,183,229,351`); the
  prelude and handler table are embedded at build time because a module has no class path
  (`wasm/src/main/java/planner/PreludeResources.java:6-23`); "`ServiceLoader` returns empty in WASM" (`wasm/README.md:103-105`)
  — so upstream's ServiceLoader discovery of repositories cannot run in the planner; a product manifest world would have
  to be a build-time resource.

### 3.9 Which of these already think in repositories or manifests

| component | repositories / manifests? |
|---|---|
| product (`Compiler`, `Prelude`, `Pure`, `SystemMetamodel`, server, WASM) | no — no code mentions repositories or `definition.json` (`grep -rln "definition.json\|CodeRepository" core/src/main/java wasm/src` is empty) |
| prelude generator, corpus, ChannelB, SpecBodyCensus | no — hand lists and hand roots; `STDLIB_ENGINE_ROOTS`' comment names upstream's `platform*`/`core_functions*` rule (`UpstreamFiles.java:136-141`) |
| `ManifestWorldCensusTest` | yes, its own reader: name + dependencies, transitive closure, no pattern, no visibility |
| PCT channel A, `//pct:adapter_par`, reference lane | yes, upstream's own machinery (ServiceLoader providers, `GenericCodeRepository`, `PureRuntime`, `PureJarGenerator`) |

---

## 4. How upstream builds its world

### 4.1 legend-pure

- `CodeRepository`: name (`[a-z]++(_[a-z0-9]++)*+`), allowed-packages pattern, abstract `isVisible`, visibility-ordered
  sort that fails on a loop (`M3/serialization/filesystem/repository/CodeRepository.java:29-67,118-205`).
- `GenericCodeRepository`: built from JSON with mandatory `name`, `pattern`, `dependencies` (`…/GenericCodeRepository.java:186-230`);
  visibility = itself or a DIRECT dependency (`:60-64`).
- Discovery: `ServiceLoader.load(CodeRepositoryProvider.class)` (`…/CodeRepositoryProviderHelper.java:83-118`); each
  provider returns `GenericCodeRepository.build("<name>.definition.json")`; `platform` is `platform.json`
  (`M3/PlatformCodeRepositoryProvider.java:43-47`). `platformAndCore` = names starting `platform` or `core`
  (`…/CodeRepositoryProviderHelper.java:32-35`).
- The platform manifests (pattern; dependencies): `platform` `((meta)|(system)|(apps::pure))(::.*)?`; []; the eight
  `platform_*` each depend on `platform`, `platform_dsl_mapping` also on `platform_dsl_store`, `platform_store_relational`
  on `platform`, `platform_dsl_mapping`, `platform_dsl_store` (the `*.definition.json` files under `$PURE`, listed by `find`).
- `PureRuntime.initialize` → `loadAndCompileCore` then `loadAndCompileSystem` (`M3/serialization/runtime/PureRuntime.java:124-160`).
  Core = every repository whose name starts `platform` or `core_functions` (`:248`), plus the `m3.pure` M4 bootstrap
  (`:257-266`); system = all remaining user files (`:297-315`). Platform repositories are immutable (`:949`).

### 4.2 How `pattern` and `dependencies` are enforced (legend-pure)

- **pattern**: `RepositoryPackageValidator` (registered at `M3/serialization/grammar/m3parser/antlr/M3AntlrParser.java:586`)
  throws "Package X is not allowed in R; only packages matching P are allowed" for any element outside its repository's
  pattern (`…/validator/RepositoryPackageValidator.java:58-66`). Patterns overlap: `core` allows `meta::pure` and
  `core_functions_standard` allows `meta::pure::functions` (the manifests in §4.3), so a package can be fed by several
  repositories.
- **dependencies**: an element's repository is the first segment of its source id (`…/usercodestorage/composite/CompositeCodeStorage.java:866-878`);
  `Visibility.isVisibleInSource` compares it with the using file's repository (`M3/compiler/visibility/Visibility.java:43-105`);
  a package is visible if any visible repository's pattern allows it (`:73-89`). Functions of a non-visible repository
  are dropped from the candidate set (`FunctionExpressionProcessor.java:1243-1247`); types, profiles, tags and values
  raise "… is not visible in the file …" (`VisibilityValidation.java:91-106,144,206,248,264,276,299`).
  **So a file in repository X may NOT reference functions or types of a repository it does not list directly**
  (visibility is not transitive: `getVisibleRepositories` = `codeRepositories.select(repository::isVisible)`,
  `CompositeCodeStorage.java:899-902`).
- **duplicates**: "The element 'X' already exists in the package 'P'" (`AntlrContextToM3CoreInstance.java:3923,3956,3964`).
- Access levels (`private`/`protected`) are a second, package-level visibility (`Visibility.java:118-157`).

### 4.3 legend-engine

- 135 `*.definition.json` outside test and target directories in `$ENG` (`find -L $ENG -name "*.definition.json" -not
  -path "*/target/*" -not -path "*/src/test/*" | wc -l`). Key manifests (pattern; direct dependencies):
  `core` — `(meta::json|meta::protocols|meta::external::format::shared|meta::pure|meta::core|meta::external::store::model|meta::alloy|meta::external::format::yaml|meta::legend)(::.*)?`;
  the 8 platform repositories + 4 `core_functions_*` (not `variant`)
  (`$ENG/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core.definition.json`);
  `core_relational` — 20 direct dependencies including `core`, `core_service` and the SQL dialect-translation modules
  (`…/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational.definition.json`);
  `core_functions_standard` — `(meta::pure::milestoning|meta::pure::functions)(::.*)?`; `platform`, `core_functions_variant`,
  `core_functions_relation`; `core_functions_unclassified` — `(meta::pure::functions|meta::pure|meta::core|meta::alloy|meta::legend|meta::vcs)(::.*)?`;
  `platform` (the `*.definition.json` files, `find`).
- Each module registers a provider, e.g. `CoreCodeRepositoryProvider` → `GenericCodeRepository.build("core.definition.json")`
  (`$ENG/…/legend-engine-pure-code-compiled-core/src/main/java/org/finos/legend/engine/pure/code/core/CoreCodeRepositoryProvider.java:21-27`).
- Build time: each Pure module is compiled by `legend-pure-maven-generation-par` (`build-pure-jar`, the module's
  definition.json as `extraRepository`) and `legend-pure-maven-generation-java` (`build-pure-compiled-jar`) with its Maven
  dependencies providing the other repositories (`$ENG/…/legend-engine-pure-code-compiled-core/pom.xml:73-110`).
- **A user's model (PureModelContextData):** `PureModel`'s constructor creates `CompiledExecutionSupport` over
  `CompiledProcessorSupport(classLoader, MetadataWrapper(root, METADATA_LAZY, this))` and a code storage of
  `ClassLoaderCodeStorage(repositories)` (`$ENG/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph/PureModel.java:189-220`),
  where `METADATA_LAZY` = every provider-registered repository on the class path except `test_*`/`other_*` (`:144`) and
  `repositories` = `platformAndCore` (`:145`). User elements go into a fresh root package (`:152`); a second user element
  of the same name is "already exists in the package" (`:1676-1700`) and a duplicated path "Duplicated element"
  (`…/toPureGraph/validator/PureModelContextDataValidator.java:53`). Bare names resolve through `META_IMPORTS`
  (`…/toPureGraph/CompileContext.java:90`, the source of our `CORE_IMPORTS`, `spec/src/gen/java/com/legend/generators/ImportsGenerator.java:6-18`).
  No repository visibility applies to user elements: no `isVisible`/`Visibility.`/`isPackageAllowed` call in the
  compiler's main sources (grep of `…/legend-engine-language-pure-compiler/src/main/java` finds only `RESERVED_PACKAGES`).
  Whether a user element may shadow a platform element of the same path is OPEN (settle: compile a PMCD declaring
  `meta::pure::functions::collection::Pair` against the 4.145.0 engine).
- **What is implicitly available, measured on the 4.145.0 server distribution:** 123 jars in `DIST/lib` each register one
  `CodeRepositoryProvider` (123 provider lines), and the jars hold 123 distinct repository manifests: `platform.json`,
  the 8 `platform_*`, `core`, the 5 `core_functions_*`, `core_relational` and 21 more `core_relational_*`/
  `core_external_store_relational_*` modules, `core_service`, `core_external_format_*` (json, xml, avro, protobuf,
  flatdata, arrow, openapi, powerbi, rosetta), languages (java, haskell, morphir, daml), `core_external_query_sql*`,
  `core_external_query_graphql*`, persistence, data space, diagram, analytics, function activators, … (query: `for j in
  DIST/lib/*.jar; do unzip -l "$j" | grep -oE "[a-z_0-9]+\.definition\.json|platform\.json"; done | sort -u` → 123;
  provider count: `unzip -p "$j" META-INF/services/org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProvider`
  per jar). Whether any of them is named `test_*`/`other_*` (and so excluded by `:144`) is OPEN (none in the printed list).

---

## 5. Prior measurements that bear on feasibility

### 5.1 The manifest census (2026-09-25, engine 4.145.0, closure of `core_relational`)

27 modules, 1,772 files; load walls 32; bodies 15,736 typed / 1,447 failed / 32 walled; per module e.g. `core`
3,726 / 850, `core_relational` 5,930 / 470, `platform` 1,014 / 1, five `core_functions_*` 963 / 5
(`docs/UPSTREAM_BOUNDARY_PROGRAM.md:305-310`; receipt `STUDY/receipts/untangle-4b/manifest-census-core_relational.txt:1-33`).
Timing: read 0.05 s, parse 0.34 s, parse + model 5.7 s, the wall-finding loop 28 rounds 72.7 s; estimate at zero walls
~6 s per corpus lane run (`UPSTREAM_BOUNDARY_PROGRAM.md:312-318`). The receipt's `duplicates=0` counts only the
parser's duplicates among loaded files (`ManifestWorldCensusTest.java:186,335`); graph copies dropped by
`withoutPreludeShadows` are not counted — **OPEN: the real duplicate count under a manifest world** (settle: count the
elements `Compiler.withoutPreludeShadows` and `SystemMetamodel.withoutSystemShadows` drop during
`//spec:manifest_world_census`). Wall classes: §1.5. The 1,447 failures by cause: `UPSTREAM_BOUNDARY_PROGRAM.md:342-358`.
Pins: walls ≤ 32, failures ≤ 1,447 (`ManifestWorldCensusTest.java:351-354`). Not re-run since 2026-09-25 (OPEN; settle:
`bazel test //spec:manifest_world_census`).

### 5.2 The eager compile and world 2 (2026-09-09)

World 1: 9,099 → 9,173 bodies, 1,605 → 1,562 failed; typing all ~1.3 s (`docs/COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:149-167`);
then 1,504 of 9,173, 1,080 walled by file across 17 fragments, residue 63 in 15 files, test bodies 361 (`:206-216`).
World 2: closed 111, poisoned 523 (`:169-172`) — measured in the same commit that moved the platform functions into
the prelude (`d961be36e`), so before graph copies of prelude functions were dropped by name; **today's world-2 numbers
are OPEN** (settle: `bazel build //spec:eager_corpus_compile_world2`).

### 5.3 The standard-library numbers

Whole-stdlib probe (2026-09-24): 460 files (267 pure, 193 engine), 1 load wall, 1,741 typed, 26 walled, 558 failed (548 one
kernel gap), parse 179 ms, load 478 ms, typing 456 ms (`STUDY/stdlib-probe.txt:1-3`). Standing pins: unwalled 0, walled
≤ 26, `core_functions_*` failures ≤ 6, load walls ≤ 5 (`SpecBodyCensusTest.java:54,248,262,283,298`). Size of a whole
copy (all 14 repositories, tests included): 460 files, 3,006,761 bytes, ~2,772 `function` header lines of which ~1,350
carry `PCT.test` and ~527 `test.Test` (grep over the 14 roots; header counts approximate); engine `core` alone 574 files,
7,568,281 bytes. The study's non-test L1 size "~258 KB" (`STUDY/PLATFORM_ARCHITECTURE.md:221`) is not reproduced here
(OPEN; settle: a generator dry run that writes the non-test copy and `wc -c`).

### 5.4 The catalog against upstream, and the 191

EXACT 787 / DIVERGENT 0 / NOT_UPSTREAM 43 / MISSING 191 at 77 FQNs (185 bodied, 6 upstream native) / 16 unreadable files;
2,128 stdlib declarations at FQNs the catalog does not declare (`STUDY/catalog-upstream-diff.tsv:1-4`); pins
`CatalogUpstreamDiffTest.java:199-201`. The 191 by repository (column 5 of the tsv): `core` 74, `core_relational` 62,
`core_functions_standard` 28, `platform` 17, `core_service` 2, `core_functions_variant` 2, `core_functions_relation` 2,
`core_data_space_metamodel` 2, `platform_store_relational` 1, `core_functions_unclassified` 1. MISSING is defined only at
catalog FQNs (`CatalogUpstreamDiffTest.java:42`). **The plan's "`isDigit`, `containsAny`, `orElse`, 2-argument `replace` —
part of the 191 missing overloads" (`docs/EXECUTION_PLAN_2026_09_26.md:864-865`) is not supported:** none of the four has
a MISSING row (grep of the tsv); `isDigit`, `containsAny`, `orElse` have no catalog FQN (0 hits in `Pure.java`);
`isDigit` and `orElse` are engine `core` functions (`$ENG/…/core/pure/corefunctions/stringExtension.pure`,
`…/langExtension.pure`), `containsAny` is `core_functions_unclassified`; upstream declares one `string::replace`, with 3
parameters (`$PURE/…/platform/pure/essential/string/transformation/replace.pure:42`).

### 5.5 Implementation table kinds

2026-09-25: Intrinsic 664, Form 217, Refused 20, Body 2,194, Unimplemented 71 (`docs/COMPILER_DESIGN_2026_09_25.md:310-311`);
now Body 2,187, Form 217, Intrinsic 671, Refused 20, Unimplemented 71 (`spec/src/test/resources/com/legend/generators/ratchets.tsv:6-10`,
asserted by `ImplementationTableTest.java:127-128`). Both total 3,166.

### 5.6 Boot and lane timings

Boot ~1.8 s → ~4.8 s per JVM from 4b.1 (resolver universe, larger prelude), task #46 (`docs/GATES.md:5667-5672`); the old
step 9's gate "boot time back at the receipts' ~1.8s" (old plan, §1.2). Boot layer cached because re-normalizing it per
compile "was 5.7ms of an 8ms compile" (`Compiler.java:249-252`). Quiet lanes at `ed85b5166`: corpus DuckDB 75.7 s, H2
79.8 s (`docs/EXECUTION_PLAN_2026_09_26.md:215-216`); 100K-model build 15.1 s (`:210`). Budgets are set at C1 from W1.0b;
until then no slice may worsen them beyond noise (`:216-217`).

### 5.7 WASM

Payload 4.2 MB raw / 1.46 MB gzip; cold first plan 557–771 ms (JVM 329–481 ms); 4.0–4.6× slower than the JVM
(`wasm/README.md:84-90`); the embedded prelude "costs ~300 KB of module" (`:46-49`); no ServiceLoader (`:103-105`). WASM
bytes and cold start are tracked numbers without budgets yet (`docs/EXECUTION_PLAN_2026_09_26.md:210`;
`docs/plan-audit-2026-09-26/meta-audit-2026-09-29/6-macro-product.md:68-72`). Class-load parsing: the plan counts "854
parses at class load" and notes "a generated class would exceed the 64 KB static-initializer limit; whether the WASM
planner can load the resource is checked first" (`docs/EXECUTION_PLAN_2026_09_26.md:660-663`); the literal sites at this
tree are 837 + 12 + 6 in `Pure.java` plus `Prelude.java:55` and `SystemMetamodel.java:1517`. The study's "stdlib.pure plus
engine-api.pure is about the size of today's 299 KB prelude" (`STUDY/PLATFORM_ARCHITECTURE.md:494`) is unverified (§5.3).

---

## 6. OPEN items, with what would settle each

1. What "2b" and "library-only" denote — the author of `cda91e93b` or its session transcript (§1.3).
2. The 2b measurement promised at step 4 — corpus and PCT lanes with a generated copy of the 14 repositories (§1.3).
3. Whether the 66 respelled upstream natives satisfy WORLD_MAP rule 7 — compare with `NativeCatalogGovernanceTest` (§2.2).
4. Whether D10 supersedes D7(a) for the corpus's poisoning build — the user (§2.6 item 6).
5. Whether ChannelB's loading of `platform`/`core_functions_standard` bodies is "under test" or "doing the running" — the user (§2.7).
6. The real duplicate count of a manifest corpus world against the boot layer — instrument the two shadow filters during
   `//spec:manifest_world_census` (§5.1).
7. Today's world-2 and manifest-census numbers — re-run `//spec:eager_corpus_compile_world2` and `//spec:manifest_world_census` (§5.1, §5.2).
8. The non-test size of a whole 14-repository copy — a generator dry run (§5.3).
9. Whether `SignatureMangle.mangle` equals upstream's element name for every declaration in the 14 repositories — compare
   with the reference dump's signature-id column (`tools/reference/README.md`); needed before `FunctionId` literals can key
   a registry with no Pure.java text.
10. Whether a user PMCD element may shadow a platform element in legend-engine — a one-element compile against 4.145.0 (§4.3).
11. Whether a `test_*`/`other_*` repository ships in the server distribution — the same `unzip` query, filtered (§4.3).
