# Compiler performance: bottlenecks, bad algorithms and slowness in the full pipeline (2026-11-13)

**The ask:** the user, through the orchestrator: "One session on looking for any bottlenecks / bad algos / slowness in
full compiler pipeline (excluding lean SQL)" — "this one is more important to be done correctly than fast."

**Scope:**
- In: A–J, plan making and PlanJson, the host side of K, the WASM planner and the server's model load.
- Out: the SQL's shape and size (stream E), the database's own time, and DataCube.

**Status: a report. No code changed.** Nothing here is to be fixed until the user rules on it. The evidence is in
[docs/compiler-perf-homework/](compiler-perf-homework/):
- the plan: PLAN.md;
- the numbers: BASELINE.md;
- `scripts/` and `raw/`, including each profile's summary in `raw/jfr/`.

**Measured at 068dbf199.** Its `core/`, `wasm/` and `native/` trees are identical to `claude/total-wall`'s 40d3bf0d5
and to main's merge commit 981a24b87 (`git diff --stat`: empty), so these numbers are main's.

Machine: 4 vCPU, product JDK 25, steal under 2%. Every number below is a median of separate JVMs with its spread in
BASELINE.md. Shares come from JFR at 1 ms sampling.

## 0. The answer in one page

| # | finding | where | share of TOTAL time on a realistic workload | fix (sketch) | gain expected | effort | confidence | tenet risk |
|---|---|---|---|---|---|---|---|---|
| 1 | The test road re-plans each test's DATA (seed) and round-trips every test plan through JSON, unconditionally | `testplan/TestKinds.java:387` (`generated` memo is per body); `testplan/TestPlanner.java:162` (`requireRoundTrip`) | **W2 (stress, executed): 55% + 36%**; the query compiler proper is 8–9% | memo the seeds by content across a model's tests, or Data-in-the-plan's references; the round-trip opt-in as the product's is | the executed stress run ~2–5× faster (est.) | M | high (profile) | **G11's run-time proof**; cross-test state. The user's ruling, and another program's area |
| 2 | **Model parse is O(n²)** in a section's element count: path dedup by `List.contains` | `parser/PmcdParser.java:182` | W3 h1000: ~70% of parse (est.); h3000: 90% (measured); stress: 0.7% (many small files) | `LinkedHashSet` | parse 6.6 MB 1.8 s → ~0.5 s; 68 MB 175 s → ~6 s | XS | high | none (order kept) |
| 3 | **Every query re-types the mapping bodies it touches:** the spec memo lives one query | `TypedQuery.java:32` (`new SpecCompiler(ctx)` per query) against `SpecCompiler.java:40-48` ("bound to the immutable ModelContext") | **W1: 15.9%** of the whole run (`ClassSources.build` 19.1% incl.); W2: 1.1% | one memo per `ModelContext` (held with the model) | −10–18% of query planning | S–M | med-high | lifetime and concurrency of the memo; G10 to re-prove |
| 4 | **The server compiles the model per request;** over a held model, generatePlan still **re-parses the whole model text** and **plans twice** | `server/PureV1Api.java:134-137` (`Models.COMPILED`); `:418` → `ModelInput.java:43` (`PmcdParser.parseModel`); `:407` + `:412` | W9 h1000: 4.2 s per request, of which the query is ~2 ms; held 1.5 s, of which ~1.47 s is the re-parse | read the connection off the held model; plan once; hold models between requests (the W9 question, §4) | held request 1.5 s → ~4 ms at h1000 | S (each part) | high | edge code (stream D's area); holding is a memory/lifetime policy |
| 5 | **Bare-call name resolution** does ~70 string concatenations and lookups per call site, and recomputes the platform tiers uncached | `compiler/NameResolver.java:348-376` (`callTierMatches`, 32 core packages); `compiler/BareNames.java:55-110` (`tiered`/`catalogTiered`, no memo); `BareNames.java:144` (a miss is never kept) | W1: 7.0%; model build: 35–60% of build (h3000, stress); 76% of build allocation | memo `catalogTiered` per name (static inputs); a static set of platform simple names for the negative answer; a per-universe simple-name index for the call tiers | model build −30–40%; names −50% (est.) | S | high | determinism of candidate order (keep it) |
| 6 | **Every island lexer allocates 48 KB** whatever the island's size | `lexer/Lexer.java:32,144` (`INITIAL_CAPACITY = 4096` floor) | stress parse: **78% of parse allocation** (0.94 of 1.21 GB per parse), 24% of parse time | a small floor for a range | parse alloc −75%, parse time ~−20% | XS | high | none |
| 7 | A mapping's include closure is **rebuilt on every binding lookup** (and each include re-resolved by name) | `model/MappingDefinition.java:449-480` (`withIncludes`/`collectIncludes`); callers `RelationalRootForm.java:223` (via `StoreResolver.substitution:3631`), `AggregationAwareRouting.viewsOf:193`, `ForeignKeyIdentity.register:44` | W1: 5.4% (+ `findMapping` 3.7%, mostly from here); W2: 0.3% | the closure once per mapping per model | −5–9% of planning on include-heavy mappings | S | medium (workload-dependent: §3.7) | none (pure function of the model) |
| 8 | **Include CHAINS are superlinear in the build** (mapping includes and store includes) | `normalizer/MappingClosures.java:210-228` (`walkSets` rebuilds below each level); `normalizer/ResolvedMapping.java:161` + `ImplicitInheritance.java:40` (two copies of the closure); `compiler/ModelBuilder.java:1227-1248` (`indexedView` walks the store chain per lookup) | only with deep chains: m100chain build +55% time / +90% alloc, s100 +75% / +50% vs flat | reuse the included mapping's closure; a per-database closure index | linearizes deep chains | S–M | high (mechanism), low (prevalence) | none |
| 9 | PlanJson builds a JSON tree, then writes it | `executionplan/PlanJson.write` → `Json.toCompact`; `MirJson.*` | W1: 7.1% (10.5% of warm query time; 1.24 GB per pass) | write straight to the output | ~−5% of query time (est.; not diagnosed further) | M | low-med | G11 round-trip must hold |

Read the ranking as impact × confidence / effort:
- For the test roads (CI, PCT, the stress suites), #1 dwarfs everything.
- For a user with one big model file, #2.
- For the product's query path, #3 then #5.
- For the server, #4.

#2, #5 and #6 are small, contained edits with no tenet risk.

**Items that touch a guard or another program's rules. These are NOT perf tweaks; each is the user's ruling:**
- **#1, the round-trip:** `TestPlanJson.requireRoundTrip` at TestPlanner.java:162 is **G11's run-time proof** (every
  test plan is portable data). Making it opt-in, as the product query's is, would change a guard.
- **#1, the seed memo:** remembering planned test data across tests touches **the Data-in-the-plan rules**
  (docs/DATA_IN_THE_PLAN_2026_11_09.md: a load carries a reference and a digest; the program L1–L8). That program
  decides it, not this stream.
- **#3 and #4 (holding):** new state that lives as long as the model. It does not change the answer (G10), but its
  lifetime, memory and lock are a policy decision.
- **#9:** must keep G11's round-trip passing, so it is proven by that guard, not exempt from it.
- **§5, the proposed ratchet,** is itself a new guard file.

None of #2, #5, #6, #7 or #8 touches a guard. Each is proven by unchanged plan hashes and the existing lanes.

**A docs fix, not a code fix:** `wasm/README.md` (Results) says the WASM planner is "4.0–4.6× slower" than the JVM,
with "warm p50 9.6–13.9 ms". That figure times `planOrError`, which compiles the model on every call
(`Boundary.plan`, wasm/src/main/java/planner/Boundary.java:142), so about 90% of it is model compile. Over a held model
(`planHeldOrError`), per-query planning in WASM is at parity with the JVM on that corpus (p50 0.99×) and 1.6× on deep
queries, with identical answers (W10). The README should say what it timed, or time the held path. That is a docs edit
for whoever owns the WASM README.

What is fine, and measured to be fine:
- per-query planning is flat across 2K–201K model elements (invariant 5 holds);
- no query shape's compile is superlinear in depth (W5);
- lowering, rendering and inlining have no hotspot;
- WASM and the JVM give byte-identical plans;
- plan JSON was identical across 30 warm passes (G10).

## 1. The workloads and the totals

Full numbers are in BASELINE.md. The four that carry the ranking:

- **W1, the stress corpus, plan only** (9,106 elements; 5,800 cases):
  - warm pass 8.2 s: plan (G½–J + assembly) 73%, PlanJson 10.5%, type 7.6%, names 7.2%;
  - model parse 0.63 s and build 0.75 s, warm;
  - per query p50 0.70 ms, p95 2.0 ms.
  - Over the whole profiled run (3 model builds + 4 passes), stages as shares of TOTAL samples (`raw/jfr/w1-shares.md`):
    parse 5.0%, build 7.6%, queries 87.4%; within that, store-resolve 43.6%, lower 9.4%, names 7.7%, PlanJson 7.1%,
    render 3.2%.
- **W2, the stress corpus executed** (`StressTool`, DuckDB): 326 s wall with about one core busy. Java samples:
  - test seed planning 55.3%;
  - test-plan JSON round-trip 36.1%;
  - SHA-256 19.1%, inside both of the above;
  - query compile: type 4.0%, store-resolve 2.8%, names 0.7%, lower 0.6%;
  - model parse+build 0.9%;
  - exec, results and JDBC under 1%.
  - Source: `raw/jfr/w2-shares.md`.
- **W3, model scale:** parse is ~quadratic from 2 MB up; build is ~linear (~80 KB allocated per element); per-query
  planning is flat.
- **W9, a server request:** the model's compile is ~99.9% of a generatePlan request at h1000 (6.6 MB).

## 2. Each finding: evidence, reproduction, fix, gain, risks, proof

### 2.1 (#1) The test road plans each test's data again, and round-trips every plan

**Evidence (W2 JFR, 268,676 samples):** `TestPlanner.plan` is 96% of samples. Below it:
- `TestDoor.planned → PlanMaker.testProgram:351 → programmed:445 → TestKinds.plan:152 → TestKinds.seed:387`: 50.8%,
  plus 4.5% at `seed:401`. The hottest leaves are `Sha256.digest` (8%), `AnsiSqlRenderer.ident` (5.2%), and
  `CsvSeed.cells`/`steps`/`records` and `RowLoad.cell`/`insert` (~10%).
- `TestPlanner.plan:162 → TestPlanJson.requireRoundTrip:80`: 36.1%. Its read re-digests every blob
  (`PlanJson.contentOf:853`, 8.1%).

**Why it is slow.**
- `TestKinds.generated` memoizes a sources value's seeds only within one body's planner (TestKinds.java:85, 387; a TestKinds per body, TestDoor.java:117), so each
  of ~6K tests plans its suite's data again: CSV parsing, INSERT rendering and SHA-256 identity.
- Each seed is rendered twice, once in the target dialect and once in the referee's (`REPLAY_REFEREE`).
- Every plan is then written to JSON, read back and compared (`requireRoundTrip`), which is the G11 proof. The product's
  query does this only under `-Dlegend.plan.roundtrip=true` (TypedQuery.java:282); the test door does it always.
- The plan carries the rows, so the round-trip's cost scales with the data.

**Minimal reproduction:** `StressTool` over any suite whose tests share one data set. The cost per test is the data's
size, not the query's.

**Fix candidates (the user's ruling):**
- (a) Memoize the planned seed steps by content (`Seeds.Sources`, dialects) across the bodies of one model's run. They
  are a pure function of their key. The precedent is the BOOT `ContentStore`.
- (b) Data-in-the-plan's design, in which a load carries a reference and a digest, not rows
  (docs/DATA_IN_THE_PLAN_2026_11_09.md). That removes most of both costs.
- (c) Make `requireRoundTrip` follow the product's opt-in. Every junit lane sets the flag, so the proof keeps running
  where it is checked.

**Gain:** with (a) and (c), ~90% of the executed stress run's Java time is in scope. An estimated 2–5×, before DuckDB's
own time, which JFR does not sample.

**Tenet risks:**
- (c) relaxes a G11 run-time proof outside the lanes. That is a guard decision: the user's, not this stream's.
- (a) is state across tests within a run, keyed by content; G10 is to be re-proven by verdict and plan-hash identity.
- This area is the Data-in-the-plan program's (and touches C4/B3's runner). It is reported here, not designed.

**Proof:** the W2 wall time and its JFR shares, plus identical verdicts (pass 5,700 / fail 339 / skipped 48) before and
after.

### 2.2 (#2) Model parse is quadratic in a section's size

**Evidence:** W3 parse, warm: 0.64 MB 62 ms, 2 MB 230 ms, 6.6 MB 1.8 s, 20 MB 16.6 s, 68 MB 175 s. Each step in size
is ×3.1–3.4, and time grows ×3.7, ×7.9, ×9.1, ×10.6, while parse allocation stays linear (26 → 2,557 MB). The h3000
JFR (`raw/jfr/parse-h3000.md`): `java.util.ArrayList.indexOfRange` is 66.7% of all samples; 11,720 samples are at
`PmcdParser.parse` line 182, out of 13,020 in parse.

**Code** (PmcdParser.java:176–185): for each section, `if (!paths.contains(e.path())) paths.add(e.path())` over an
`ArrayList`. That is O(n²) string comparisons per section, and FQNs share long prefixes.

**Killed hypotheses:**
- Splitting the model into 100 stores (s100) or 100 mappings (m100) does not change parse time, because the classes
  stay in one `###Pure` section.
- `ModelFromProtocol`'s line and offset work (`lineStarts`) is 0.8% of allocation, not the cause.

**Minimal reproduction:** `gen_scale.py --hubs 1000` (or any single section of N classes); parse time vs N.

**Fix:** a `LinkedHashSet` for the seen check, keeping the list for order. **Gain:** at 20 MB the line is 90% of parse,
so 16.6 s → ~1.7 s. Linear extrapolation gives ~0.5 s at 6.6 MB and ~6 s at 68 MB. The stress corpus (many small
files) gains ~0.7%. The parse runs in every road that reads a model's text: the server's per-request compile, the WASM
hold, the IDE, and `ModelInput.records()` (#4).

**Risks:** none. The order is kept and the output is identical; plan and protocol equality prove it. **Proof:** W3's
parse column becomes linear; the parser-equivalence corpus is unchanged.

### 2.3 (#3) Every query types the mapping bodies again

**Evidence (W1 JFR, `raw/jfr/w1-shares.md`):**
- `SpecCompiler.compile` under the resolver is 15.9% of all samples.
- `ClassSources.build` is 19.1% inclusive. Its `specs.compile(fns.get(0))` is at ClassSources.java:737.
- Also `AssociationJoins.predicateMaterial:1223`.
- Callers: `StoreResolver.collectOpChain:2986`, `AssociationJoins.associationJoin:894`,
  `NavMaterializer.demandUnboundTail:608`, `ClassSources.routeTarget:485`.

**Why:**
- `SpecCompiler`'s memo is documented as bound to the immutable `ModelContext` snapshot (SpecCompiler.java:40–48).
- But `TypedQuery`'s constructor makes `new SpecCompiler(ctx)` per query (TypedQuery.java:32), and the resolver's
  `ClassSources` is per statement (StoreResolver.java:131).
- So a mapping's lifted function (`ClassSources.build:737`) is type-checked once per query that touches it, and the
  stress corpus's 5,800 queries share a few thousand of them.

**Fix:** one demand memo per `ModelContext`: a field of the context, or held beside it as `HeldModels` holds the
context. Its key stays the F-cached `TypedFunction`'s identity. It must be safe under the server's concurrency: a
concurrent map, or the held model's lock, which `HeldModels.with` already takes.

**Gain:** most of the 15.9%. The first compile of each body remains, and so does the part of `ClassSources.build` that
is per query (bindings, stacks). Estimated −10 to −18% of query planning on W1. W2 gains ~1%, because the test road is
dominated by #1.

**Risks:**
- (i) The memo's lifetime is the model's. This is not cross-run state in the results sense: the body's type is a
  function of the model alone, which the memo's own javadoc already claims. But it is new long-lived state, so the user
  rules.
- (ii) The typed body must not depend on the query. It is keyed only by the function today, so that already holds
  within a query. To prove: G10 plan-hash identity on W1 (5,800 cases × passes) and the plan-path lanes.
- (iii) Memory: typed bodies of the mappings in use.

**Proof:** W1's plan sum, `ClassSources.build`'s share, and unchanged hashes.

### 2.4 (#4) The server: model work per request, a re-parse, and planning twice (the W9 question)

**Evidence (W9, in process, `raw/w9.log`):**

| model | compile alone | route as served | route over a held model | query + one plan |
|---|---|---|---|---|
| corpus (768 B) | 4 ms | 7.2 ms | 1.7 ms | 0.88 ms |
| h100 (0.64 MB) | 260–300 ms | 233 ms | 36 ms | 2.0 ms |
| h1000 (6.6 MB) | 4.4–5.6 s | 4.2 s | 1.5 s | 2.1–2.4 ms |

**Code:**
- `PureV1Api.route(...)` uses `Models.COMPILED` (PureV1Api.java:134–148): every request compiles its model. `hosted`
  (:805) and `ModelInput` (:29, :56) do the same.
- `generatePlan` (:404–418) calls `query.executionPlan(runtime)` to test for a constant (:407). It then calls
  `query.plan(runtime)` (:412), which calls `executionPlan` again (TypedQuery.java:234). `planned` has no memo, so the
  query is planned twice.
- It then reads the connection from `model.records()` (:418), which for a text model is `PmcdParser.parseModel(code)`
  (ModelInput.java:43–44). That is a second parse of the whole text, through the quadratic parser (#2), even when the
  model is held. It accounts for ~1.47 s of the held route's 1.5 s at h1000.

**What could be held between requests without cross-run state:** each item below is a function of the model text
alone, and so has the same answer held or not, which `PureV1Api.Models`' javadoc already states.
- parse and build (A–F): the `ModelContext`;
- the protocol records the routes read (the connection);
- the typed mapping bodies (#3).

`HeldModels.withText` already implements the hold, keyed by the text's SHA-256: 53 ms at 6.6 MB on the JVM. It is used
today by the WASM boundary only (wasm/src/main/java/planner/Boundary.java:59–62, :193).

What must stay per request: the query (names, G, G½–J) and the plan's JSON. That is ~2 ms here.

What holding costs: memory (W3: ~1.6 GB of heap per 20K elements during the build; the held context is smaller, not
measured), an eviction policy, and the held model's lock under concurrent requests.

**Fixes, in order of size:**
- (i) read the connection off the compiled model, not a re-parse;
- (ii) plan once (keep the plan from :407);
- (iii) hold models in the JVM server as the WASM boundary does.

(i) and (ii) are local. (iii) is a design question for the user. This code is the compatibility edge (stream D), so
these are reported, not proposed.

**Gain:** a held request at h1000 goes 1.5 s → ~4 ms. An unheld request loses one parse (~0.5–1.8 s at h1000,
depending on #2) and one plan.

**Proof:** W9's columns, and PureV1Api's parity tests unchanged.

### 2.5 (#5) Bare-call name resolution

**Evidence:**
- **h3000 build:** names is 56% of the build's samples and 76% of its allocation. `callTierMatches` is 17.7%
  inclusive and 23.4% of allocation; `BareNames.catalogTiered` 18.5% / 13.4%; `BareNames.tiered` 8.8% / 19.1%.
- **Stress corpus build:** names 61%.
- **W1 total:** `callTierMatches|catalogTiered` 7.0%.
- **s100 build:** the top two leaf chains are `callTierMatches:359` (10.8%) and `BareNames.tiered:61` under
  `referents:125` (8.7%).

**Why:**
- For every bare call site, `callTierMatches` (NameResolver.java:348) concatenates `pkg + "::" + name` for each
  wildcard, the own package and the 32 packages of `CoreImports.SEQUENCE`. It hashes each concatenation into the
  model's FQN set; each is a fresh string, so its hash is computed again.
- `decide` (:565–612) then calls `BareNames.catalogTiered(name)`, which has no memo. It rebuilds `tiered(name)`: 32
  more concatenations and `TierFqn` records, the form's owned FQNs sorted into a new `TreeSet`, and a `removeIf` over
  the lite partition. It also builds a new `HashMap`.
- `BareNames.referents` is memoized, but by design not on an empty answer (BareNames.java:144: unbounded growth from
  misspellings). So every bare call that names no platform function, e.g. a user function, pays the whole tier walk
  again.
- All of `tiered`/`catalogTiered`'s inputs are static platform tables.

**Fix:**
- Memoize `catalogTiered` per name, bounded by membership: answer "nothing" in O(1) from a static set of every simple
  name the tiers can produce, and memoize only names in that set. This bounds the table by the platform, which answers
  `referents`' growth concern.
- For `callTierMatches`, index the scope's FQN universe by simple name once per universe, rather than probing 34
  concatenations.

**Gain:** model build −30 to −40% (est., the share above); the names stage about halved. **Risks:** the candidates'
ORDER is semantic (tier order, the probe's records). An index must yield the same order; prove it by plan-hash
identity, `DecisionProbe` output equality, and the PCT and corpus lanes.

### 2.6 (#6) The island lexer's floor

**Evidence (W1 parse JFR, `raw/jfr/w1-parse.md`):**
- `Lexer.<init>` is 77.9% of parse allocation (3.61 GB sampled over the run's parses; ~0.94 of 1.21 GB per parse) and
  23.8% of parse samples.
- The path is `MappingProtocolParser.readIsland` → `TokenStream.lexRange` → `Lexer.tokenizeRange`.

**Code** (Lexer.java:32, 138–147): `estimated = Math.max(INITIAL_CAPACITY, (to - from) / 3)` with
`INITIAL_CAPACITY = 4096`, so three `int[4096]` arrays (48 KB) per lexer. An island (`#{ ... }#`: a test assertion's
JSON, a query) is typically tens of characters, and the stress corpus has ~20K of them.

**Fix:** use the floor only for whole sources, e.g. `max(16, len/3)` for a range. `grow()` already doubles.
**Gain:** parse allocation −75%, parse time ~−20%, with fewer GCs. **Risks:** none. The output is identical: the arrays
are capacity only, and `TokenStream` hands off `count`.

### 2.7 (#7) A mapping's include closure, rebuilt per lookup

**Evidence (W1):**
- `MappingDefinition.withIncludes` is 5.4% of all samples and `collectIncludes` is 9.9% of query-side allocation.
- `SymbolTable.resolveId:74` is the top leaf in store-resolve (4.5%); 1,045 of its 1,464 samples come from
  `collectIncludes:474` → `findMapping`.
- Callers of `classBindingsWithIncludes`: `RelationalRootForm.primaryKeyColumns:223` (1,937 samples, from
  `StoreResolver.substitution:3631` once per substitution, and from `ClassSources.build:796`),
  `AggregationAwareRouting.viewsOf:193` (607) and `ForeignKeyIdentity.register:44` (308).

**Why:**
- `withIncludes` (MappingDefinition.java:449) builds a fresh list and set and walks the include DAG on every call.
- For a bare include path, it concatenates the includer's package and looks the result up first (:462–466), so the
  string is new and its hash is computed again.
- The stress corpus's `AllMapping` includes 147 mappings.

**Prevalence (an honest limit):** the `winc` reproduction (200 hubs, K mappings in a chain or flat, 100 simple
project queries) shows no per-query change beyond noise from K=1 to 200. The cost is calls per query × closure size,
and the stress corpus's queries (graph fetch, routing, primary keys per substitution) make many more calls than a
plain project does.

**Fix:** compute the closure once per mapping per model: a `ModelContext` index, or a lazily cached field (the
mapping is immutable once built). **Gain:** ~5–9% of W1 planning; ~0 for a simple query over a flat mapping.
**Risks:** none beyond a model-lifetime cache (as #3).

### 2.8 (#8) Include chains in the build

**Evidence:**
- **winc**, same 2,000-class model: build allocation by chain depth K is 488 MB (K=1), 507 (10), 609 (50), 810 (100),
  1,436 (200); flat includes stay at 489–495 MB.
- **W3:** m100chain build 2.6 s / 3.1 GB vs m100flat 1.7 s / 1.6 GB; s100 (100 stores in a chain) 3.0 s / 2.5 GB vs
  s10 1.8 s / 1.7 GB.
- **inc200 JFR:** `MappingPrePass.prePass` is 36.7% and `ImplicitInheritance.apply → ResolvedMapping.visibleSets:161 →
  MappingClosures$Closure.sets:205` about 23%; the leaves are LinkedHashMap node creation and puts.
- **s100 JFR:** `ModelBuilder.indexedView` recursing at :1242 is ~10%.

**Why:**
- `walkSets` (MappingClosures.java:210–228) builds a fresh `local` map at every level of the recursion and `putAll`s
  it upward, so a mapping deep in a chain copies everything below it at each level.
- It never reuses the included mapping's own memoized `sets()`.
- `visibleSets()` copies the whole closure, and `ImplicitInheritance.apply:40` (also `:121`, `MappingPrePass:134`)
  copies it again.
- `indexedView` (ModelBuilder.java:1227–1248) and its siblings walk the store include chain on every lookup.

**Fix:** compose closures from the included mapping's memoized closure; return read-only views, not copies; index each
database's include closure once. **Gain:** removes the superlinear term, and matters only for deep chains. How common
deep chains are is unknown. Engine-style projects do chain includes, but rarely 50+ deep.

### 2.9 (#9) PlanJson

**Evidence:**
- W1: `PlanJson.write` is 10.5% of warm query time (859 ms of 8.2 s) and 1.24 GB per pass.
- `Json.toCompact` and `Json.writeValue` together are ~5.3%; `MirJson.outputCol` and `MirJson.type` are among the top
  allocators.
- The plan's size (p50 8 KB, max 278 KB) is stream E's.

**Why (partly diagnosed):** `PlanJson.write` builds a `Json` value tree and then serializes it. **Fix:** a streaming
writer. **Gain:** an estimated half of the 10%, not measured. **Risk:** G11 (the plan must round-trip), so it is proven
by `requireRoundTrip` on every lane. **Confidence:** low-medium; not profiled below the frame level.

## 3. Hypotheses that measurement killed

1. **`closeOverConditions` is still O(k³) and hot** (PERFORMANCE_AUDIT's suspect, Pipelines.java:238): 0.3% of W1
   query samples. Killed.
2. **`Sorts.sort` is a hotspot** (7.2% inclusive in W1): it is the wrapper that lowers its source (`Lowerer.relation:1118`);
   its own frames are ~0. Killed: an inclusive-attribution artifact.
3. **Temporal handling is 12% of store-resolve:** a regex artifact in my own first attribution. The real temporal
   classes total under 1.5%. Killed.
4. **Lowering has a hotspot:** it is flat; the largest leaf is `SqlExpr.Column.of` at 1.1% of query samples. Killed.
5. **Deep or wide queries compile superlinearly:** every shape's exponent from depth 8 to 32 is 0.49–1.0, except
   association `path` (1.27), whose plan JSON reaches 866 KB, which is stream E's size question. Killed for the compiler.
6. **Per-query planning grows with the model (lazy loading broken):** flat from 2K to 201K elements (W3:
   167–202 ms per 100 queries). Killed; invariant 5 holds.
7. **Include count drives per-query planning:** within ±15% noise for K=1–200 on simple queries (winc). Killed as
   stated; the W1 cost is call count × closure (#7).
8. **Splitting a big model into stores or mappings fixes the parse:** no; the quadratic is per section and the classes
   stay in one. Killed.
9. **The WASM planner is 4× slower:**
   - over a held model, it is at parity on the corpus queries (p50 0.99×) and 1.6× on deep queries;
   - answers are byte-identical, 123/123;
   - the README's 4× timed `planOrError`, which compiles the model per call (~90% of that figure);
   - model compile itself is 1.5× slower on the small corpus and ~4.8× on the 1K-class model.

   Killed as stated. The WASM gap is model compile, so #2, #5 and #6 help it most. Caveat: the JVM was warmed lightly
   (16 calls per query).
10. **The server's cost is the query:** at h1000 the query is ~2 ms of 4.2 s. Killed.
11. **GC is a bottleneck:** W1 GC is 1.2–1.3 s per ~60 s run; it matters only at h10000 (6.7 s in a 10-minute run).
    Killed for realistic sizes.
12. **Hashing a model text to hold it is costly:** 53–56 ms at 6.6 MB on the JVM, against a 4.4 s compile. Minor.
13. **JFR distorts the picture:** JFR runs are 10–13% slower (W2 369 vs 326 s; W1 pass ~10 vs 8.2 s). Only shares are
    read from JFR; times come from the un-profiled runs.

## 4. What W9 asks: model work that could be held between requests

Answered in §2.4. Every A–F result, the protocol records, and the typed mapping bodies are functions of the model
text alone. `HeldModels` already holds them by content hash for the WASM tab. The JVM server holds nothing:
`Models.COMPILED`, chosen deliberately ("a server holds nothing: a request has no holder", PureV1Api.java:125). Holding
is not cross-run state in G10's sense, because the answer is the same held or not. But it introduces a model lifetime,
memory and a lock: a policy for the user, not built here.

## 5. A permanent guard: a planning-cost ratchet that only gets faster

**The quantity.**
- Wall time on shared CI runners flakes; this machine alone showed ±5–15% between JVMs.
- Pin **allocated bytes** instead (`ThreadMXBean.getCurrentThreadAllocatedBytes`, the probe's measure), per stage, per
  workload. Allocation is nearly deterministic for a single-threaded compile on a fixed JDK:
  - W1's per-pass plan allocation spread is 4,634–4,674 MB (0.9%);
  - names 390–392 MB; parse 1,208.5–1,210.3 MB;
  - it moves with algorithmic change, not with machine load.

**The workloads.**
- (a) W1, the stress corpus plan-only: per-stage allocation for one warm pass, plus model parse and build allocation.
- (b) Complexity guards: the ratio of parse, and of build, allocation AND time between two sizes of the generated
  model (h100 → h300, ×3 elements). A ratio is machine-independent. Bound it, e.g. ≤ 4 for ×3; quadratic shows as ~9.
  That catches #2-class regressions that an absolute pin tuned to a small corpus would miss.
- (c) One W5 shape set at depths 8 and 32, as a depth-exponent bound.
- (d) Determinism comes free: the plan hashes of (a) must not change between passes.

**The mechanism.**
- A manual-tag Bazel test (`//core:planning_ratchet`), exclusive, on one Linux lane.
- Its ceilings live in a ratchet file under `core/src/test` with the repository's shrink-only convention
  (docs/FOUNDATIONS_PLAN.md §0.4, `JavaEvalLedgerTest`'s pattern):
  - a measured value over its ceiling + 3% fails;
  - a value more than 5% under prints the new ceiling to commit, with a dated justification;
  - ceilings never rise without one.

**Anti-flake measures.**
- Allocation, not time, gates. Wall time is reported as a trend only: median of 3 JVMs, steal recorded.
- The JIT's escape analysis can shift allocation by a few percent between JDK builds, so ceilings are re-based when
  the JDK pin moves (`MODULE.bazel`'s `remotejdk`), a dated entry.
- One warm-up pass is discarded.
- `-Xms` = `-Xmx` and the product JDK only.

This is a proposal. A ratchet is a guard file, and adding one is the user's ruling.

## 6. Out of scope, reported to the streams that own it

- **E (lean SQL):** association paths at depth 32 make 866 KB of plan JSON (W5 `path_32`; `extend_32` 148 KB). The
  planning cost there is the plan's size.
- **D (compatibility at the edge):** #4: `PureV1Api.generatePlan` re-parses the model text and plans twice; the JVM
  server holds no model.
- **Data in the plan / C4–B3 (the test road's runner and guards):** #1: test data re-planned per test, rendered in two
  dialects, digested at planning and digested again on the round-trip's read (`LoadSteps.blob:48`,
  `PlanJson.contentOf:853`, `LoadPlanner.identity:421`: 19.1% of W2 in SHA-256); `TestPlanJson.requireRoundTrip`
  unconditional (TestPlanner.java:162).
- **DataCube:** nothing measured.

## 7. Not done, and why

- **W6 (PCT plan-only replay), W7 (corpus), W8 (core suites), W11 (the native library): not run.** W2, an executed test
  road, already showed where such roads spend their time (#1, with the compiler at 8–9%). The runners are named in
  PLAN.md §2.
- **No profile inside the WASM module** (no sampling profiler was used there): the WASM numbers are timings only.
- **The fixes' gains are estimates** from profile shares. Each states the measurement that would prove it, and none was
  tried, by the brief's rule.
