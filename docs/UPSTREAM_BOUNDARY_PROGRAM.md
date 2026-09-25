# The upstream boundary — THE program

One thesis, six workstreams, nine batches (0–8). Present tense; no history.

**The documents of this program, and what each is for:**

| file | role | read it when |
|---|---|---|
| **[`UPSTREAM_BOUNDARY_PROGRAM.md`](UPSTREAM_BOUNDARY_PROGRAM.md)** (this) | the plan — thesis, target state, feature table, workstreams, batches, next actions | always first |
| [`UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md`](UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md) | the evidence — every measurement and receipt, in order of discovery; §8 is the provenance ledger | you need a number, or how it was measured |
| [`UPSTREAM_BOUNDARY_FACTS_2026_09_10.md`](UPSTREAM_BOUNDARY_FACTS_2026_09_10.md) | the opening fact sheet that started the homework — superseded, kept for provenance | never, unless tracing a correction |
| [`NATIVE_CLAIMS_CENSUS_2026_09_10.tsv`](NATIVE_CLAIMS_CENSUS_2026_09_10.tsv) | the 175 registry-unclaimed natives, one row each with class and handler | working batches 3–4 |
| [`../tools/version-report.sh`](../tools/version-report.sh) · [`../tools/upstream-drift.py`](../tools/upstream-drift.py) · [`../tools/native-axes.py`](../tools/native-axes.py) · [`../tools/classpath-convergence.sh`](../tools/classpath-convergence.sh) | the read-only measurement tools; §7 maps each number to its tool | regenerating rather than trusting |

Every number in this file is receipted in the homework; every conclusion the homework
reached that this file overrides is listed in §9.

---

## 0. Terminology first — the word "native" means two unrelated things

This governs everything below, and the homework conflated it in places. **Read this
section before any other.**

**Upstream's `native function`** is an *implementation detail*: "this function's body
is Java, not Pure." It says nothing about semantics. In the roots we read, upstream
declares **286** functions native and **14,182** with Pure bodies — the Pure platform
is 98% Pure, and which 2% is Java is upstream's private business.

**Our `Pure.java`** is a *semantic claim*: "the platform has a lowering for this
function" — a SQL rule, a language form, or a Java arm. The keyword `native` in its
signatures is a spelling accident inherited from the engine-lite port. It should be
read as **"platform-lowered"**, full stop.

The two are orthogonal axes, and every function lands in one of four cells:

|  | **we lower it** (Pure.java) | **we do not** (prelude) |
|---|---|---|
| **upstream native** (Java body) | signature in Pure.java, text verified against upstream | prelude carries a **respelled `native function`** — resolves, type-checks, fails at lowering with "not implemented" |
| **upstream bodied** (Pure body) | signature in Pure.java — we **override** the Pure body with SQL; the prelude **excludes** the body | prelude carries **upstream's body**; we interpret it — this is where most of the language lives |

So: **Pure.java is the left column. The prelude is the right column.** The upstream
row only decides *how* the prelude carries something — a declaration or a body. Nothing
about Pure.java membership should follow from whether upstream happened to write a
function in Java.

**Today's Pure.java is neither column.** Of its 491 upstream-named FQNs: 205 are
upstream-native, 238 are upstream-bodied, 37 both, 11 outside the roots we read. Half
are deliberate SQL overrides of Pure bodies (legitimate — that *is* a SQL platform);
the other half are natives ported from an earlier registry. Membership is a historical
mix, not a decision on either axis.

---

## 1. The thesis

legend-lite is a clean-room platform for the Pure language and the Legend engine's
semantics. Upstream is the spec. Two things are true today:

**The gates verify BEHAVIOUR and are strong** — 2,575 corpus tests, 1,109 PCT tests,
the parser differential, the spec census. When our behaviour diverges from upstream's,
they go red.

**Nothing verifies TRANSCRIPTION, and nothing can even measure our implemented
surface.** ~950 upstream facts are hand-typed into core (881 native signatures, 32
implicit-import packages, 17 protocol wire shapes, 20 source positions); a stale copy
produces a confidently wrong answer every behavioural test agrees with. And "which of
the 520 natives do we implement?" has **no computable answer**: lowering is spread
across four registries, `CoreFn`, a six-entry wall list, and ~80 ad-hoc dispatch sites.
The honest bound is **42 ≤ unimplemented ≤ 175** — between 8% and 34% of Pure.java.

The program makes four sentences true and keeps them true:

1. **Upstream is ONE release**, named in ONE file, enforced in CI.
2. **core has ZERO upstream dependencies**; every upstream fact inside it is GENERATED
   from the pinned release and asserted byte-equal every run.
3. **Pure.java is exactly the implemented surface**: every entry has exactly one
   registered implementer, every implementer names a Pure.java entry, and the prelude
   carries everything else upstream declares.
4. **Everything that READS upstream lives outside core.**

When those hold, a bump is: change one number, regenerate, review the diffs, re-pin the
ratchets with reasons — and "what does the platform implement" is a printed fact.

---

## 1b. How we interact with pure/engine — four modes, four answers

Everything in this program is one of four ways the repo touches upstream. Naming them
keeps "the other half" from being a list of unrelated fixes.

| mode | what it is | examples | the program's answer |
|---|---|---|---|
| **SPEC TEXT** | the checkouts, read as files | prelude generator, spec census, the relational corpus (gates 4/5), ChannelB (gate 9), the parser corpus (gate 8), 132 hardcoded paths | one pinned **tag**; read only from `spec` / `pct` / `parser-equivalence`; every path asserted (E) |
| **ORACLE JARS** | upstream's own Java, executed beside ours for a differential verdict | the reference parser (gate 8), the PCT framework + `ReportScope`s (gates 6/7), the protocol differential (F) | the **same** tag — jars exist only at tags, which is *why* the source must pin a tag (A) |
| **COPIED FACTS** | upstream declarations transcribed into `core` | Pure.java signatures, `CORE_IMPORTS`, `PlatformTypes` spellings, the 17 wire goldens, 20 positions | **generated** from the tag (C) or **deleted** in favour of a live comparison (F); membership decisions stay ours (D) |
| **CALIBRATED RATCHETS** | numbers that are functions of upstream content but name nothing | ChannelB's 5 exact pins, gate 7 ceilings, ~60 parser/census/roster rows, PCT expected-failures | re-pinned at every bump, each row with a reason; loud by construction (Phase 4 of the procedure) |

Modes 1, 2 and 4 are **behaviour** verification and are structurally sound — their
problems are *where* they live (B), *which* version they read (A), and *whether they
notice a moved file* (E). Mode 3 is the blind spot: it is transcription, and nothing
verifies transcription. The Pure.java work (D) sits on the seam between modes 2/3 — it
is a copied fact (the signature) carrying a semantic claim (the lowering), and the claim
must be ours while the text must be upstream's.

## 2. Target state

```
core/                        ZERO org.finos.legend deps — ENFORCED (Maven enforcer + ArchUnit)
  builtin/Pure.java          THE IMPLEMENTED SURFACE: membership = has a claim; signature text GENERATED
  builtin/Lite.java          our ~29 invented natives (meta::legend::lite) — hand-declared, ours
  builtin/prelude.pure       GENERATED ← spec: every upstream declaration we do NOT implement
                             (classes, enums, bodied functions WITH bodies, natives RESPELLED)
  builtin/core-imports.*     GENERATED ← spec: the 32-package implicit import group, ORDERED
  builtin/platform-names.*   GENERATED ← spec: the 111 distinguished FQN spellings
  lowering/**                every implementer CLAIMS its Pure.java entries (one registry of claims)
  src/test/                  platform tests; the claim-completeness test; NO upstream reads

spec/                        NEW — everything that READS the checkouts
  generators                 prelude, Pure.java signatures, core-imports, platform-names
  parity tests               regenerate each, assert byte-equal (the guard)
  signature oracle           every Pure.java FQN's text == upstream's declaration (native OR bodied)
  rcorpus harness            gates 4/5 (moves from core/test: 9 files, 3 helpers, 0 API widening)
  SpecBodyCensus, path manifest (132), bare-name census

parser-equivalence/          oracle JARS; gate 8; + protocol LIVE differential (goldens deleted)
pct/                         PCT JARS + checkouts; gates 6/7/9

tools/oracle-pins.env        ONE release. Pure derived from engine's pom. Jars = source = same tag.
```

The contract between `spec` and `core` is the one the prelude already honours:
**generator outside, generated resource inside, byte-parity asserted.** The program
extends it to every remaining copy, and where no upstream *file* exists to derive from
(protocol), it **deletes the copy** and compares live.

---

## 2b. Feature by feature — what it is, how it touches upstream, exactly what we do

Every feature that relates to pure/engine, with its interaction mode (§1b), what guards
it today, and the exact action. Numbers are receipted in the homework (§3n, §3o).
"Ours" = reimplemented behaviour (C1), verified by the gates; listed so the reader sees
it was considered, not forgotten.

| feature | what it is | mode | guarded today | EXACTLY what we do | batch |
|---|---|---|---|---|---|
| **`Pure.java`** | 881 overloads / 520 FQNs: the platform's claimed lowered surface; constants the compiler dispatches on | copied fact + semantic claim | `native-catalog.txt` = snapshot of its own output; nothing upstream; no implementer registry | **D1** claim registry — every overload claimed by exactly one of: `Scalars.RULES`, `Aggregates.REDUCERS`, `Windows.*`, a `CoreFn` arm, a wall, or an explicit claim from the ~80 ad-hoc sites (`CalendarAgg` 32, `AssertVerdicts` 14, …); test enforces both directions. **D2** the 70 certainly-unimplemented leave (13 reflection, 6 `toDDL`, 6 `execute::fetch*`, 4 `sqlstring`, 4 `lineage::scan*`, …); the 35 grey adjudicated one by one. **D4** signature text verified against upstream's declaration of the same FQN (native *or* bodied), then generated from membership + checkout. Delete `Scalars.KNOWN_ABSENT` (38/39 stale) | 3, 4, 5 |
| **`Lite` natives** | 29 `meta::legend::lite::*` — our inventions (desugar IR, engine-vocab shims, `INTERNAL_DESUGAR`, `ENGINE_VOCAB_SHIMS`) | ours | the two shim sets are pinned shrink-only | split into `Lite.java`, hand-declared; **not** subject to the upstream signature oracle (the ours-only bucket must equal exactly this set); the 3 in no registry (`otherwise`, `legacyAssocPredicate`, `legacyLocalProperty`) claim via their front-end desugar site | 3, 5 |
| **`Prelude.java` / `prelude.pure`** | 5,281 lines, 466 Class / 82 function / 18 Enum / **0 native**: every upstream declaration we do **not** implement — pure's platform roots whole + the engine vocabulary our Java names + closure | copied fact, **generated** | byte-parity assert (the model) | keep the generator; **D3** stop skipping upstream `native function` (today dropped by token position) — carry them respelled; key the 209-name exclusion rule on the claim registry instead of "Pure.java declares it" (8–25 bodies suppressed today); move the generator to `spec` | 4, 7 |
| **`CORE_IMPORTS`** | 32 packages — pure's `system::imports::coreImport`, walked first-match by the resolver; order is semantic | copied fact, in **main** | nothing | generate from `m3.pure`; compare **as a sequence**; the prelude generator already opens the file (~5 lines) | 5 |
| **`PlatformTypes`** | 111 FQN strings read by 91 files; a copy of our own `Pure.java` spellings | copy of a copy | `PlatformTypesDriftTest`: 6 of 111, by containment, vs our own prelude; 7 inline `…::Any` literals bypass it | generate the spellings from the same read as `Pure.java`; identity checks → `Type` methods; delete the drift test (by construction) and the 7 literals | 5 |
| **`SystemMetamodel`** | 1,507 lines: the metamodel-as-relations store — our own Pure source (Database + Mapping + navigation functions over `metamodel.*` tables), parsed at class load, injected into every build | ours (C1) **+ 104 upstream-named FQNs** (`relational::metamodel` 42, `pure::mapping` 22, `pure::metamodel` 10, …) alongside 23 `meta::lite::*` of our own | none for the 104 spellings | the 104 are the same class as `PlatformTypes` — spellings that must match upstream's declarations; fold them into the generated names table (one table, two consumers). The Pure source itself stays hand-written: it is our implementation, and "the engine bodies are the SPEC, ours read our own compile-time facts" is the right rule | 5 |
| **Lexer keywords + grammar snapshot** | `Lexer.KEYWORDS` 56 entries (ours); `docs/g4-keyword-snapshot.tsv` 538 rows = every keyword in the engine's 73 `.g4` grammars, each classified `present` / `parsed-generic` | spec text → ledger | **loud on upstream addition**: `SurfaceCensusTest` reads the `.g4` files from the checkout and fails on any engine keyword absent from the snapshot ("classify them") | keep — this is already the right shape. Add the shrink direction (a keyword upstream removed must leave the snapshot), and move the census to `spec` with the path manifest covering the `.g4` walk | 2, 7 |
| **Parser** (+ `Dialect` PLATFORM / LEGEND / LEGEND_LITE) | ours — the clean-room parser | ours (C1) | **gate 8**: live differential vs the oracle jar over 8,891 manifest-pinned sources; 25-row skew ledger + refusal ledgers | nothing structural — this is the strongest lane in the repo. Batch 1 makes the oracle the *same release* as the sources, so most skew rows lose their reason; adjudicate the rest | 1 |
| **Protocol emitter** | ours — emits the engine's wire JSON (`ProtocolEmitter`, `MappingEmitter`, …, in `core/main`, no upstream dep) | ours (C1) | **17 frozen `EXPECTED_*` goldens** captured at engine 4.133.0 + one bare SHA; `ProtocolRosterCensusTest` writes `target/protocol-roster.txt` with **no committed counterpart**; `ProbeWireShapes` is a manual instrument | **F**: live differential in `parser-equivalence` (it already holds `legend-lite-core` + the grammar jar in one JVM) — both sides emitted per source, seeded with the 17, grown to the corpus, divergences ledgered shrink-only; then **delete** the 17 goldens and the 20 captured positions from `core`. Commit the protocol-type roster as a ledger (new tags = reviewed diff) | 6 |
| **PCT channel A** | upstream's PCT framework (`PCTReportConfiguration` ×5, `PCTReportProvider`) running upstream's `<<PCT.test>>` functions through our adapter — `pct_adapter.pure`, 479 lines, our Pure implementing upstream's `GrammarExtension` | oracle jars + our adapter | gates 6/7 ceilings; `PctDisciplineTest` (no Java-side comparison; adapter size pin 485); expected-failure lists (27 + 9 + 1) | batch 1 moves the jars to the source release (today 4.133.0 vs 4.137.0+36: universes 7 tests apart); the adapter is ours and stays; the expected-failure lists are ratchets, re-pinned per bump with reasons | 1 |
| **PCT channel B** | our platform walking the checkouts for the same `PCT.test` functions, dual-verdict against A | spec text | 5 exact discovery pins (137/327/204/355/95); 10 hardcoded scope roots | path manifest covers the 10 roots; pins re-pinned per bump (the relocation arithmetic: Unclassified 95→89, Essential 327→345 at 4.145.0) | 2, 8 |
| **Relational corpus harness** (`rcorpus`) | `Corpus`, `MinimalCorpus` + 4: assembles the engine's `core_relational` test corpus (543 files) into one model, runs gates 4/5; `SHAPE_FILES` 64 (508 Class/30 Enum, 743 functions deliberately excluded) + `LIBRARY_FILES` 6 (649 functions) | spec text | `CorpusManifestTest` 8,891 SHAs (exact); rosters; **4 silent `continue`s**; `ENGINE_IMPLEMENTATION_FILES` inverted failure (49 tests) | **E**: path manifest; misses reported not skipped; exclusion keys assert. Move to `spec`. The by-file admission stays — it is the correct mechanism, it just has to be loud (batch 155 traded the prelude's parity guard for silent paths) | 2, 7 |
| **Spec census** (`SpecBodyCensusTest`) | types every body in pure's 9 platform roots; shrink-only pins (walled ≤ 23, loadWalls ≤ 1) | spec text | precheck tests **1 root of 9**; a missing root shrinks the input and the pin passes easier | precheck all 9; misses reported; move to `spec`; its native-vs-program bucketing should read the claim registry (D1) | 2, 7 |
| **Version pins** | 6 identities (source SHAs, oracle jars, PCT jars, runner, fixture filename, captured bytes) | all modes | `oracle_roots_check` (source only); INV-2/INV-3 **broken** | **A**: one `LEGEND_ENGINE_RELEASE`, pure derived, source = tag, poms read `${…}`, `--check` in CI | 1 |
| **Fixture snapshot** | `engine-grammar-fixtures-4.138.2.jsonl`, 1,552 sources harvested by executing the oracle's test-jars under shims | oracle jars → ledger | version in the **filename**; reader returns empty on absence | version recorded inside the file and asserted against the pin; absence fails | 2 |
| **Ledgers** (`version-skew-claims` 25, `refusal-allowlist` 8, `model-refuse-allowlist`, rosters, ~15 parser ratchets) | adjudicated exceptions keyed by upstream path or count | calibrated ratchets | red on any move; path keys silent on a moved file | path manifest covers the 33 path keys; every row re-adjudicated per bump, shrink-only | 2, every bump |
| **`nlq`** | our NL-query module, **squatting `org.finos.legend.engine.nlq`** (28 files); imports no real upstream class | none — a naming hazard | — | rename to `com.legend.nlq` (28 files, 1 pom `mainClass`, 1 README line) | 0 |
| **`tools/fqn-mapping.json`** | 467 constant-name → FQN rows | dead | — | **zero readers** since 2026-07-08; delete | 0 |

**What the table says in one line:** the behaviour lanes (parser, PCT, corpus, census)
are already right in *shape* and wrong only in *version*, *placement* and *loudness*;
the copied facts (`Pure.java`, `CORE_IMPORTS`, `PlatformTypes`, the 104 spellings in
`SystemMetamodel`, the 17 goldens) are the work; and `Pure.java` is the one place where a
copied fact carries a semantic claim nothing can currently verify.

## 3. Workstreams

### A — One release

Six version identities (source pins 4.137.0+36, oracle jars 4.138.2, PCT jars 4.133.0,
perf harness, a fixture filename, captured bytes) collapse to **one line** in
`tools/oracle-pins.env`. Pure's version is **derived** from the engine release's own
`<legend.pure.version>` — nobody types it. Source checkouts pin a **release tag**, so the
oracle jar is the *same release* (today the source pin is a non-tag commit that no jar
exists for; the two trees are `diverged`, 20 ahead / 11 behind, and
`docs/version-skew-claims.tsv`'s 25 rows are the rent). PCT jars move onto the same
release (today channel A and channel B referee universes 7 tests apart).
`tools/version-report.sh --check` exits 0 in CI or the push fails.

**INV-5 — classpath convergence (USER 2026-09-10).** The upstream-facing modules do not
share a dependency *set* — `pct` needs the PCT framework and the interpreted runtime,
`parser-equivalence` the grammar/compiler/extension jars. What they must share is the
**version of every artifact present in both**: with one release, every
`org.finos.legend.*` resolves at it, and every transitive third-party both pull resolves
identically. Before batch 1: 136 shared, **71 divergent** (66 legend, 5 third-party — `tools/classpath-convergence.sh`,
the authority; a hand-run said 135/70 before it parsed `test-jar` rows). **MEASURED at
batch 1 (2026-09-10): one release took it to 4, not 0** — HikariCP, commons-lang3,
httpcore, junit. Those four are NOT version drift: Maven nearest-wins resolves
legend-pure's own (older) managed versions on pct's graph and the engine's on
parser-equivalence's, at the same release. The engine release's own root pom manages all
four for its whole reactor (the PCT framework included), so the root pom now manages them
at the engine's values — DERIVED, checked by `version-report.sh` INV-6 against the pinned
engine checkout's pom — and the count is **0**. `tools/classpath-convergence.sh` measures it and also asserts the
boundary — `core` (and `spec`) resolve zero `org.finos.legend` artifacts. It is the test
that "one release" propagated *transitively*, not just at the pom-property level, and
the one instrument that would catch upstream itself pinning two versions of something
across its own modules within a release (ours to record, not to fix).

### B — The boundary

Everything that reads `legend.engine.root` / `legend.pure.root` or depends on an
`org.finos.legend` jar leaves `core`. `core/pom.xml` bans `org.finos.legend.*` (Maven
enforcer); ArchUnit bans the imports. The new `spec` module takes the rcorpus harness,
the spec census, the generators and their parity tests. Measured cost: 9 files, 47
referenced core classes all already `public`, 3 test-side helpers that move too, zero
gate-chain cost (gate 2 already installs core first).

### C — Derive, don't type

Every upstream fact in `core` becomes a generated resource with a parity test in `spec`:
- `prelude.pure` — already generated; **gains the respelled natives** (§0, right column).
- **Pure.java signature text** — generated from a committed **membership list** plus the
  pinned checkouts. Membership is OUR decision (the claim registry, workstream D); the
  spelling is upstream's. Today 881 lines are hand-typed and verified against nothing.
- `CORE_IMPORTS` — generated from `m3.pure`'s `system::imports::coreImport`, compared
  **as a sequence** (order is semantic: first match wins).
- `PlatformTypes` spellings — generated from the same read; identity checks become
  `Type` methods; the 7 inline `meta::…::Any` literals are deleted.
- Protocol — **no resource**: live differential in `parser-equivalence` (workstream F).

### D — The implemented surface: the untangle (rewritten 2026-09-24)

D as first written (claims registry, membership, generated text — batches 3, 4, 5 below)
LANDED, and the platform-architecture study of 2026-09-23/24
(`~/legend/platform-architecture/PLATFORM_ARCHITECTURE.md`) then measured what it left:
the catalog is a faithful copy of upstream's declarations (787 EXACT, **0 DIVERGENT**) but
an INCOMPLETE one (**191 overloads missing at 77 FQNs**, 185 of them bodied upstream), and
the compiler papers over the gaps with FQN-level suppressions (the PCT-twin rule in
`FunctionCompiler`, `isPlatformOwnedFunction`, `CORE_FUNCTION_PACKAGES`, `Scalars.KNOWN_ABSENT`,
bare-name `nativeKeysAt` registrations, `CoreFn.of`'s name-tail dispatch) and then throws
the resolved function away: **824 sites** re-derive identity from a string (`identity-sites.tsv`).
The `indexOf` / `max` / `stdDev` channel-B regressions the first switch attempt caused were
that incompleteness surfacing. D is therefore re-chartered as the **untangle**: a
strangler-fig over the two decision points, with a differential at every step.

**Rulings (standing, from the user).** No string identity for a resolved function — a
function's identity is its `FunctionId` (upstream's own signature id), compared whole,
never cut, affixed or prefix-tested; **no compiler reference to PCT or to any category of
function** ("that just means we are hacking around our generic capability"); a string
hack found on the path is FIXED as part of the slice, never left and never added to;
probe before switching — every switch is preceded by a logged disagreement census;
each slice goes to main on the full local chain, green; this program owns every file it
needs — everything else is frozen until it finishes.

**The two decision points.** Everything the tables replace is one of: (1) the overload
set the typer chooses from (`FunctionCompiler.functionsAt`); (2) the implementation the
lowering picks for a resolved function. Every suppression, list and string site is a
proxy for one of those two. The untangle moves each decision onto the tables and deletes
the proxy behind it.

| step | what | receipt | status |
|---|---|---|---|
| 0 | **Guardrail.** `IdentityGuardrailTest`: comment-stripped whole-file scan of nine string-identity shapes, shrink-only pins (NAME_COMPARE 214, REVERSED 94, LITERAL 65, AFFIX 53, CUTTING 106, SIGNATURE_ID_CUTTING 1, CATALOG_LOOKUP_BY_NAME 180, FAMILY_LOOKUP_BY_NAME 89, FUNCTION_CATEGORY_CHECK 19). The bleeding stops before the surgery | `identity-sites.tsv` | LANDED 2026-09-24 |
| 1 | **Census.** `CatalogUpstreamDiffTest`: every catalog overload against the pinned trees' declaration of the same id. 787 EXACT / 0 DIVERGENT (pinned 0) / 43 NOT_UPSTREAM (`meta::legend::lite`) / 191 MISSING at 77 FQNs / 16 unreadable files (none stdlib). Pins shrink-only | `catalog-upstream-diff.tsv` | LANDED 2026-09-24 |
| 2 | **Tables beside.** `com.legend.platform`: `FunctionId` (generated by `SignatureMangle.mangle`, compared whole); `DeclarationTable` (one declaration per id — a catalog native and upstream's bodied twin are ONE declaration, the bodied one kept; two different bodies refuse); `Implementation` (sealed: Form / Intrinsic / Body / Unimplemented / Refused(reason)); `ImplementationTable.build(declarations, registrations)` — total by construction, every registration read exactly (a lowering key matched whole to the catalog definition that generated it; a family member's overloads ARE definitions; forms, walls and subsumed programs name exact FQNs), dangling and conflicting registrations REPORTED never resolved; `Registrations` is a value (`current()` reads the registries; tests hand-write them). Over catalog + stdlib + upstream declarations: 3,157 rows, 0 dangling, 0 conflicts; 63 FQNs where an implemented row sits beside a Body/Unimplemented twin (146 rows) — the shadow targets | `implementation-table.tsv`; `ImplementationTableTest` (core: every builder path; spec: the real registrations) | LANDED 2026-09-24 |
| 3 | **Shadow diff, narrowed.** `DecisionProbe` (builtin, an SPI both callers reach) + `Shadow` (platform, bound by `ServiceLoader` so the layering stays acyclic), on under `LL_SHADOW`: the overload set `functionsAt` returns vs `DeclarationTable.at`; every lowering pick (`Scalars.lower`, `Aggregates.reducerFor`, `Windows`, `UserCallInliner`) vs `ImplementationTable.of`; every `CoreFn.of` dispatch vs the form that owns the FQN. Zero code paths change; all 15 suites green with it on. **Census (9 suites, 4,188 distinct lines):** picks 2,672 agree / 11 differ (6 ids, two classes: five family-implemented natives reaching the scalar funnel — `alloyConfig`×2, `setUpDataSQLs`, `createTableStatement`, `lateral` — where today throws "unregistered" and the table says Intrinsic[family]; one `$prop$` member `SQLResult.toSQLString` where `isPlatformImplementedDerived` is an FQN list no registration names). Overload sets: **0 TODAY-MORE** (the table never loses an overload), **29 TABLE-MORE** (bodies today suppresses: the catalog's missing overloads — `max/min[1..*]`×3 each, `average/median(Float\|Integer)`, `stdDev*`, `wavg`, comparison `[0..1]` forms, `date::max/min`, `timeBucket`, `isEmpty(Any[0..1])`, `or(Boolean[1..*])` — plus the platform-owned engine FQNs `toSQL*`, `toDDL::*`, `setUpDataSQLs`, `loadCsvToDbTable`, `testDataGeneration::*`), **357 BARE-NAME** (356 names: the resolver's universe holds no platform function FQN, so every native call stays bare and the typer qualifies it through the catalog's bare index — the table cannot be asked until the resolver qualifies). Forms: 0 disagreements on qualified names (`CoreFn.OWNS` holds), 51 bare. `indexOf`: no disagreement anywhere (upstream declares all three natives; the earlier regression was that switch's own defect, not the tables'); `max/min → 7.345D` IS the TABLE-MORE class (`max(Float[1..*]):Float[1]`'s body inlined once admitted). One TABLE-REFUSED: a spec model holding the prelude's `meta::pure::mapping` bodies beside upstream's originals of the same text | `shadow-census.txt`, `shadow.tsv` (study dir); GATES 2026-09-24 | LANDED 2026-09-24 |
| 4 | **Switch and delete, one consumer at a time — in the order the census dictates.** (4a) **LANDED 2026-09-24** — the PICK first: `CallNodes.mint` (the one mint of a call node) and the inliner ask `implementations.runsByRule(definition)`; a bodied declaration with an Intrinsic/Form row is a native call and lowers by its rule, never by its body (the `max[1..*]` class); the scalar funnel explains a missing rule by the row (`NoRule`). `CoreFn`/`Feature`/`WalledBodies` moved below the compiler (`platform`), `PlatformRegistrations` (lowering) assembles the registrations, `ModelContext.declarations()/implementations()` own the tables per model. Class members a family implements are `Registrations.members` matched by lifted provenance (`isPlatformImplementedDerived` deleted, with the inliner's `WalledBodies.reason`/`Subsumed.of` and `Scalars`' catalog reads). `DeclarationTable` reports duplicates (first kept) — spec records were already span-blind, so the census's refusal was a real prelude-vs-upstream textual difference. Identity pins 180→179, 89→87, 19→16. Checkpoint MET: every added branch is a row kind, every deleted one a name list. Shadow after-sweep (9 suites): PICK disagreements 11 → 0 (the `$prop$` member row and the five family natives now agree on the row); the five family natives still REACH the scalar funnel by trial dispatch — recorded as WRONG-SITE, a 4c/5 item, not counted as agreement. Overload sets unchanged (363 bare / 30 table-more / 0 today-more — 4b/4c own them). One DUPLICATES line (the spec census model). (4b) **RE-PLANNED 2026-09-24 after a failed first attempt (study §14: no inventory, 848 bodies broke, reverted).** Bare-name resolution is upstream-faithful for ENGINE input (`Handlers.java` is keyed by bare name and consulted first); what is not faithful is our `FN_BY_BARE` (every catalog native by short name, unverified). Four slices, probe before/after each: 4b.0 **LANDED 2026-09-25** — the engine handler registry generated from the pinned `Handlers.java` (836 rows; 169 undeclared engine ids pinned) as a pure census, plus two shrink-only identity shapes so 4b's deferrals cannot quietly stay: MINT_BY_NAME (`new AppliedFunction("…"`, 143 → step 5) and FORM_DISPATCH_BY_NAME (`CoreFn.of(`, 21 → 4d); 4b.1 **LANDED 2026-09-25** — the resolver's universe gains platform function FQNs, `CoreFn.of` consults `OWNS` for a qualified name (41/41 forms), synthesized programs resolve like text (E.6), and the readers that compared a call's name to a bare literal ask what it refers to (`ResolvedNames`; `StaticFold` dispatches on `FoldOp`). Gate met: bare spellings at the typer 1,554 → 599 sites, the 4 outside and 14 core names qualified (one two-package tie carries both), no candidate set shrank, PICK 0 → 0, FORM 51 → 43 (none new), corpus LOST 0. The corpus gate caught two folds keyed on a bare spelling and both were fixed at root: the static-`if` prune (`isIf` deleted) and `removeAll`, which the platform declared nowhere — the prelude generator gains an explicit engine-library membership (`ENGINE_LIBRARY_FUNCTIONS`, one row with its reason) so a fold op keys on a declaration — then (same day) the fold op and the constant were deleted too: the static interpreter evaluates a bodied library function by its body (`StaticFold.evalUserCall`), natives by their row (`contains`), and only the one-row membership remains as the declared interim for the namespace rule (D7); 4b.2 + 4b.3 **LANDED 2026-09-25** — `BareNames`, the one rule for a call the resolver left bare: the engine surface (`EngineHandlers`), the core import group (`CORE_IMPORTS`), the form's owned declarations spelled like the call; `FunctionCompiler.functionsAt(bare)` asks it and looks each FQN up exactly like a qualified call, natives and model alike. `Pure.nativeFunctionsAt` REFUSES a bare name (a declaration lookup is FQN-keyed); the bare index survives only as the lowering's registration surface (`REGISTERED_BY_BARE`, `nativeKeysAt`; 4d deletes it); `CORE_FUNCTION_PACKAGES` deleted; `ResolvedNames`, `ReceiverOwnedFunctions`, `MatchChecker`, `ProjectChecker`, `GroupBySynthesis`, the resolver's captured-name union all ask the rule or the resolved referents. The dynafunction registry's 4th column is THE DECLARATIONS a name resolves to — a PURE row's catalog FQNs generated from the catalog (232 rows, verified), a SHIM's Lite FQN — and the translator mints a PURE call carrying them as candidates, so `sqlNull`/`sqlTrue`/`sqlFalse` and every other operator reach the typer resolved (4b.3's mint spells `Pure.SQL_NULL`). Measured first (probe with a `bare`/`node` source column): 227 truly bare names / 451 sites, 138 with no candidate under any rule (property and form probes), the three tiers covering all but 6 candidate FQNs, each traced. Gate: 227 → 223 names, zero-candidate set +1 (`connectionByElement`, served by its derived property), PICK 0 → 0 disagreements, FORM 43 → 43 none new, OVERLOADS 0 disagreements, corpus LOST 0, nine suites green; pins 179 → 172, 16 → 13, 144 → 143. Rule-3 slip caught by the census and corrected (a form's owned FQNs join only under their own name — `select` must not tie with `newTDSRelationAccessor`). Compiler mints stay bare-by-engine-name until step 5. (4c) the overload set from `DeclarationTable.at` — the PCT-twin rule and `isPlatformOwnedFunction` die; the 29 TABLE-MORE FQNs become visible and the missing overloads are registered by id (same rule, more ids). (4d) `CoreFn.of`'s name-tail fallback and `Scalars.KNOWN_ABSENT`. Each switch deletes the proxy behind it in the same commit, with the shadow re-run before and after. The missing 191 overloads enter the declaration table as upstream's bodies (2b: whole stdlib resource vs library-only is decided HERE, by measurement). **CHECKPOINT: the first switch lands green with no new special case, or the program stops and the design is wrong** | corpus/PCT/census counts (expect PCT UP: suppressed bodies run); guardrail pins DOWN | |
| 5 | **Carry the resolved function.** The typer's resolution result travels as the `Function` (its `FunctionId`) through lowering; each of the 824 string sites becomes a table lookup or a typed family check; the identity pins ratchet to **0** for identity dispatch (category checks and literal compares included). The last `SignatureMangle.resolve` site (`Typer`, function-reference arm) goes with it | `IdentityGuardrailTest` pins → 0; `native-membership.tsv` / `native-claims.tsv` retire (the table is the surface) | |
| 6 | **Bazel follows the seams.** Packages and targets cut where the tables cut: declarations (parser + tables, java.base-only) / lowering / execution. Task #6/#8 territory — after 5, never before | `//tools/deps` guards | |
| 7 | **Load by manifest.** A program's world is its module plus the dependency closure upstream's `<name>.definition.json` declares — every `.pure` file of every module, tests included — loaded STRICT, one parse, one model build, no tolerant mode (ruled out 2026-09-25: a tolerance gets embedded and then relied on everywhere). Deletes `UpstreamFiles.LIBRARY_FILES` and `SHAPE_FILES` (hand guesses at a slice of the closure) and shrinks the prelude to the boot vocabulary the platform's Java needs. Precondition: the closure's walls at zero. Details, numbers and order in **D7** below | manifest census: walls 32 → 0, failing bodies 1,447 shrink-only; both corpus rosters LOST 0 | task #44 |

#### D7 — Load by manifest: everything learned on 2026-09-25, for the session that does it

**The rule.** In Pure an `import x::*;` line loads nothing; it only widens bare-name lookup over
what is already loaded. What decides which files exist is the REPOSITORY: each upstream module
carries a `<name>.definition.json` (`name`, `pattern`, `dependencies`), and the engine loads the
module's files plus every file of every module in its dependency closure, all at once, before any
name resolves. The relational corpus lives in `core_relational`, whose manifest names 20 modules
directly; the closure is 27. legend-pure's `platform` repository has no manifest in the tree (it is
declared in Java upstream); its root is `legend-pure-core/legend-pure-m3-core/src/main/resources/platform`.

**What we do instead today, and why it keeps biting.** We load the corpus test files, a hand list of
individual upstream files someone found the tests needed (`UpstreamFiles.LIBRARY_FILES`, five entries;
`SHAPE_FILES` for declarations), and the prelude, itself built from hand-kept membership lists. Each
list is a person guessing at a slice of the closure. `removeAll` was the fourth file of a module the
manifest says to load whole (engine `core`, `core/pure/corefunctions/collectionExtension.pure`) — it
"worked" only because the constant folder string-matched the bare name (4b.1 removed that; the interim
is the file on the list and no mention of removeAll in the compiler). Every time a mechanism keys on
declarations, an undeclared name in this gap surfaces. The gap is the loading rule, not the names.

**The census program.** `spec/src/test/java/com/legend/generators/ManifestWorldCensusTest.java`:
reads every manifest under both checkouts, computes a module's closure, loads every `.pure` file of
every module, types every body once, writes `manifest-census-<module>.txt` (per-module table, failure
classes, top normalized messages, load walls, every failing body) to the test outputs. Opt-in, not a
gate: `bazel test //spec:spec_tests --test_env=JAVA_TOOL_OPTIONS="-Dmanifest.census=core_relational"`;
add `-Dmanifest.census.timing=1` for the load timing only. It survives a `StackOverflowError` per body
and reports it as a `recursion` row.

**Measured (engine 4.145.0, 2026-09-25, closure of `core_relational`).** 27 modules, 1,772 files
(553 `core_relational`, 574 `core`, 244 `platform`, 116 sql_dialect_translation, 193 across the five
`core_functions_*`, the rest small). Load walls 32. Bodies: 15,736 type, 1,447 fail, 32 walled by
decision. Per module, bodies OK / failed: core_relational 5,930 / 470; core 3,726 / 850; platform
1,014 / 1; core_external_format_json 290 / 78; the five core_functions_* 963 / 5; everything else
631 / 43.

**Timing (nanoTime, warm JVM, best of 3).** Read 1,772 files 0.05s. Parse all of them 0.34s. Parse +
build the model with the walls already removed 5.7s. The census's wall-finding loop: 28 rounds,
72.7s — because `Compiler.buildModel` (the strict entry) aborts on the FIRST refused element and the
loop drops that file and re-parses and rebuilds everything. Parsing is not the cost; the retry loop
is. At zero walls the strict entry is one parse and one build, ~6s per corpus lane run (the corpus
runs twice per chain across two lanes: budget ~25s, inside the chain budget, and the 5.3s model build
on 1,772 files wants a profile — an algorithm fix if it is one, never a cache).

**The 32 walls, classified — this is the work list, cheapest first.**
1. *Parser, 4 files, all `core` tests:* `;` used as a mapping separator
   (`binding/executionPlan/tests/executionPlanTests.pure:693`, `graphFetch/tests/sourceTreeCalc/subType/testOnSourceRoot.pure:336`);
   an arrow our expression parser refuses (`corefunctions/tests/language/testLambda.pure:71`);
   trailing tokens after a code block (`store/m2m/tests/legend/simpleObject.pure:1869`).
2. *m3.pure* (`platform/pure/grammar/m3.pure`) is the M3 bootstrap instance language, not Pure:
   route it to the reader the prelude generator already has (`PreludeGenerator.m3Declarations`).
3. *Measures and units, 1 feature, 4 walls:* `corefunctions/unit.pure` declares `Measure Mass` with
   `Kilogram`; it falls out, and `HealthProfile` (declared in it) then fails three more files as an
   unknown type. `PlatformTypes.MEASURE`/`UNIT` exist; the model does not carry `Mass~Kilogram`.
4. *Unknown types in three extension files:* `Runtime` and `Mapping` in
   `core_service/service/mappingExtension.pure`, `core_data_space_metamodel/mappingExtension.pure`,
   `core/pure/router/router_main.pure` — each needs its own look (likely a shape the prelude's
   demand cut left out).
5. *Our own M2M roadmap refusals, ~14 files:* enum transformers (`EnumerationMapping … on 'type' is a
   roadmap feature`), explosions (`fullName*`), merges, unions, subtypes — `core/store/m2m/tests/**`,
   `core/pure/graphFetch/tests/**`, `core_external_format_json/executionPlan/tests/**`. These are
   product features the corpus's own M2M tests want; build them as features with their own tests.
6. *Five `core_relational` test files* refused at model level (modelChainTest, modelJoinAdvancedSetup,
   testModelJoinsToRelationalJoins, scanRelationsTestWithViewsAndUnions, testCrossStoreGraphFetch) —
   the same M2M features seen from the corpus side; today's corpus loader already walls them.

**The 1,447 failing bodies collapse into about six causes** (loading does not need them fixed — the
corpus types only what it calls — but they are the standing census after the switch):
- `serializerExtension`, 391 rows, every body in `meta::protocols::pure`: the engine's `mutateAdd`
  native we walled BY NAME reports as "unknown function" instead of "walled". The same disease as
  removeAll at scale; declare it (4b/4c ledger) and 391 rows leave.
- Column-spec and legacy TDS forms, ~150: `project expects ~[…] column specifications`, `legacy groupBy
  aggregate must be agg(mapFn, aggFn)`, `match expects a collection of branch lambdas`, `renameColumns
  expects literal pairs` — the form checkers accept the shapes the corpus uses, not upstream's full
  grammar for the form.
- Kernel generics, 164: `type variable T bound to Class<Any> cannot also bind …`, `unbound type
  variable T/U`, concentrated in `core`'s metamodel-walking programs.
- Unqualified element names, 131: `'…' is not a known class, mapping, runtime … needs a fully
  qualified name` — our resolver demands qualification where upstream resolves through imports (4b).
- NormalizeRequired bodies with non-let intermediate statements, 42 (`Typer.inlineNormalized`).
- One compiler bug: `StaticFold.inlineUserCall` guards a callee inlining ITSELF (`inlining` stack by
  signature key) but not mutual recursion through two functions — `StackOverflowError` on
  `meta::relational::functions::pureToSqlQuery::processTdsProjectExtend`.

**The namespace rule, measured 2026-09-25 (the first sub-step of this work).** "Every bodied
function upstream declares under `meta::pure::functions::` is platform library wherever its file
sits" — the predicate the corpus loader already applies in reverse (`MinimalCorpus.refusePlatformNamespace`)
— admits 126 engine `corefunctions` functions (9 files; 87 more names there are platform-owned and
stay out) and needs two things first: (1) 26 boot-typing failures — 10 bare profile references
(`doc`, `test` read as element names by the resolver), 2 body-constructed classes the closure never
seeds (`SplitTextResult`, `CamelSplit`), ~12 kernel/overload gaps in metamodel-walking functions, one
signature-id reference, one router unknown; (2) the `agg` form is UNOWNED — `GroupByChecker` matches
`fn.equals("agg")`, it is in neither `CoreFn` nor the claims, so the engine's `collection::agg` body
captured 270 DuckDB / 182 H2 corpus tests and 47 core tests. Register the form (4b.2/4d territory),
fix or wall the 26 with reasons, then the rule replaces the one-row `ENGINE_LIBRARY_FUNCTIONS`
membership, which is the declared interim until then.

**Rulings taken 2026-09-25.** (a) No tolerant load mode, ever, for the corpus or the census: the
strict builder, one pass, at zero walls. (b) The interim loading rule is the hand list, declared as
such, with the census numbers pinned shrink-only so the debt is visible. (c) Sequence: finish 4b with
the world held constant (one variable at a time — a world change and a name-resolution change in the
same stretch make roster diffs unattributable), THEN this step with names held constant. (d)
Rejected: "carry any engine function the platform's Java names" — measured 43 such FQNs outside the
catalog; they are forms, walls and test helpers, not demand. (e) The constant folder's table is the
platform's natives evaluated statically, keyed on catalog declarations; a bodied library function
(removeAll, contains upstream) is inlined by the general user-call path, never given a row.

**The switch itself, when the walls are zero.** The corpus loader (`MinimalCorpus`) reads the
closure of `core_relational` from the manifests; `LIBRARY_FILES` and `SHAPE_FILES` are deleted in the
same commit; the prelude generator's engine-file lists go with them (the prelude keeps only what the
platform's Java names at boot; T4 receipts and task #43's duplicate refusal sit exactly on this seam);
gate: both corpus rosters LOST 0, the chain inside budget, the manifest census pinned (walls 0, failing
bodies shrink-only from 1,447). Watch for the batch-169 capture: a bodied library function under a
name the platform implements (isDistinct, uniqueValueOnly in collectionExtension.pure) must lower by
the implementation table's row, never by its loaded body — 4a's rule; the rosters are the proof.

**What D's old text got wrong, kept for the record.** "Every overload has exactly one
claim" was false (a function lowers differently by position — the table records POSITIONS,
one implementation); "membership is our decision" was half right — membership of the
*implementation* is ours, but the *declaration* universe is upstream's whole and the
catalog must not be a subset of it that the compiler quietly completes with name rules.
The 2b question (whole stdlib as a resource, or library-only) is still open and is decided
at step 4 with the corpus and PCT numbers in hand, not before.

### E — Loud, not silent

All 132 hardcoded upstream paths asserted to resolve (today 4 silent `continue`s, a
census precheck that tests 1 root of 9, and an exclusion-map key whose *non*-match
admits the engine's own implementation — measured cost 49 tests, no diagnostic). The
fixture snapshot records its version **inside the file**, asserted against the pin.
Every ratchet and ledger row is keyed and reviewed at each bump.

### F — Protocol, live

`parser-equivalence` already holds our emitter and the engine's parser in one JVM. The
17 frozen wire goldens (captured at engine 4.133.0 and one bare commit SHA, in a module
with no engine dependency, re-derivable by nothing) become a **live differential**: both
sides emitted for the same source, every run. Seed with the 17, grow to the corpus,
adjudicate divergences into a shrink-only ledger, then delete the goldens and the 20
captured token positions from core. It is the one surface a bump structurally cannot
turn red today, and the one item that *removes* a hand-held artifact rather than
guarding one.

---

## 4. Batches, in dependency order

**Every batch has a done-criterion — a check that goes from absent-or-red to green — and
lands through `tools/allgates.sh`.** This is deliberately *not* the burn-down rule
"every batch moves a ratchet / mechanism-only legs are not batches" (USER 2026-09-10):
this program is structural, and several batches move no ratchet by design (2 lands
green, 7 relocates, 3 creates a new fact). The burn-down's FAIL→SKIP rule survives only
as Phase 4 of a bump: a reclassification is a re-pin and needs a written reason. D is two
batches because D1 is pure measurement/enforcement (no behaviour change) and D2–D4
change what programs resolve.

| # | Batch | Closes | Done when / what moves |
|---|---|---|---|
| 0 | **Hygiene.** `nlq` → `com.legend.nlq` (28 files, 1 pom `mainClass`, 1 README line; it imports **no** real upstream class — the squat is the only upstream-looking thing about it); delete `tools/fqn-mapping.json` (467 rows, **zero readers**, last touched 2026-07-08) | the K3 grep over-report; a dead hand list | none |
| 1 | **One release at 4.138.2.** pct → 4.138.2/5.92.0; source → the tag; pins file carries jar versions, poms read `${…}`; `--check` in CI | A | **`version-report.sh --check` exits 0 and `classpath-convergence.sh` reports 0 divergent** (INV-5); ChannelB pins, gate 7 ceilings, skew ledger (expected most of 25 rows gone — an inference from their "re-adjudicate at re-pin" annotations; **measured: NONE left, see §9**), 4 stale `5.88.1` comments. **LANDED 2026-09-10** — every move receipted in homework §5a and docs/GATES.md |
| 2 | **Loud.** Path manifest test; 4 `continue`s → reported; census precheck 1 → 9; exclusion keys assert; fixture version inside the file | E | none expected — lands green. **LANDED 2026-09-10**: `UpstreamPathManifestTest` (90 core paths + 10 ChannelB + 33 ledger keys = 133; the homework's 132 missed `MinimalCorpus.GRAPH_FETCH_DOMAIN`); the keyword-snapshot shrink direction fired at once (`mappingProvider`, added upstream after the tag) |
| 3 | **Claims.** The claim registry + completeness test; every ad-hoc dispatch site claims what it implements; `KNOWN_ABSENT` deleted | D1, D5 | a NEW printed fact: the implemented surface. Expect the test to land red and be ratcheted: unclaimed count is shrink-only. **LANDED 2026-09-10**: 881 overloads, 133 unclaimed (94 FQNs) — derived claims only (no hand table: USER), three string-switch families became closed enum types; the ledger `native-claims.tsv` is the surface; "exactly one claim" was false (112 overloads lower differently by position) |
| 4 | **Membership.** Unclaimed entries leave Pure.java; prelude carries bodies / respelled natives; exclusion rule keys on claims; 44 undeclared natives enter the prelude | D2, D3 | corpus pass count (expect **up**: suppressed bodies now run); catalog row count (down); `SpecBodyCensus` walls; unclaimed → 0. **4a LANDED 2026-09-10**: 38 FQNs left, 31 natives respelled (the pure roots' whole set — "44" counted the engine too), 10 bodies unsuppressed, unclaimed 133 → 91, corpus pass count UNCHANGED (measured: every one of the 38 was already failing where called). **4b LANDED 2026-09-10** (three commits, receipts in docs/GATES.md): `NativeFn.java` — one file, one closed enum per implementer, 19 families, every dispatch site a typed lookup; the executor kinds left `PlatformTypes`; unclaimed 91 → **0** (833 overloads). Two chain catches: `createDbConfig` is typing-consumed (51 tests), name-ownership is per-FQN (1 test) |
| 5 | **Generated text.** Signature oracle (verify), then Pure.java text generated from membership + checkout; `CORE_IMPORTS` generated, ordered; `PlatformTypes` generated, identity → methods | C, D4 | divergence buckets (same-FQN-different-sig must reach 0); `PlatformTypesDriftTest` deleted; 7 inline literals → 0 |
| 6 | **Protocol live.** Differential in parser-equivalence; seed 17, grow to corpus; delete goldens + positions from core | F | differential count up; divergence ledger shrink |
| 7 | **The `spec` module.** Pure relocation — after 2–6 so the move changes nothing it carries | B | none — that is the point |
| 8 | **Bump to 4.145.0 / 5.99.0.** The first bump *under* the program | validates A–F | the drift already measured: +9 corpus files / +14 tests, 2 new PCT functions (`substr`, `binFloor`) to implement, ChannelB Unclassified 95→89 / Essential 327→345, 15 pure test files removed |

**Why 4.138.2 first.** Batch 1 is a ~31-commit consolidation; batch 8 is twelve
releases of upstream change. Done together, "consolidation breakage" and
"upstream-change breakage" are indistinguishable. Two bumps, the first proving the
machinery on a small move.

**Batches 3–5 need a design doc before code.** The claim-registry shape (annotation?
explicit `claims(Pure.X)` calls? a registrar each lowering file implements?) and the
membership-list format are decisions with a long tail; decide them once, in writing.

---

## 5. Steady state

**A bump is one command.** `tools/bump.sh <engine release>` (batch 8's deliverable,
2026-09-11): the release must be *published* on Central; the pure version is derived from
that release's own pom (INV-1); the two tag commits come from `git ls-remote` (annotated or
lightweight — upstream's tags are lightweight since 4.14x); both checkouts move; the pin, the
root pom's two versions and four INV-6 managed versions, and the runner pom are rewritten;
core is installed at the OLD facts, every generator that writes into core runs (natives,
prelude, dynafn registry, core imports), core is re-installed, the claims ledger regenerates;
the grammar fixture is re-harvested in two tiers (the published grammar/compiler tests-jars,
then the checkout's extension test SOURCES compiled against the harvest classpath — upstream
publishes no tests-jars for relationalStore/service/persistence), deduped, renamed to the
release and censused by origin class against the committed file; the corpus manifest and
protocol roster regenerate; `version-report.sh --check` must exit 0. It stops loudly at the
first generator that refuses (a changed signature, a construct the parser does not know) —
the "fix the platform first, then re-run" case; it is idempotent. What it does NOT do is the
judgement: a human reviews **three things** — the generated-resource diffs (that *is* the
upstream change, made legible), the ratchet moves the ONE chain reports (each with a reason),
the ledger adjudications — then commits and pushes; CI runs the same gates on three platforms.
Upstream ships roughly weekly; monthly or per-minor is the cadence, and a scheduled job
opening the PR is the natural endpoint.

**Six green checks define "sane":**

| check | question it answers | where |
|---|---|---|
| `version-report.sh --check` | is upstream one release, everywhere? | CI, every push |
| `classpath-convergence.sh` | did that release propagate transitively — every shared artifact at one version, zero legend jars in `core`/`spec`? | gate 2, every chain, after the install (batch 7d) |
| path manifest | do all 132 upstream paths still resolve? | `spec`, gate |
| parity tests | do core's generated facts match the pinned release? | `spec`, gate 3 |
| claim completeness | does every Pure.java entry have exactly one implementer? | `spec`, gate 3 (the ledger is core's generated resource; its generator lives with the others) |
| ArchUnit + enforcer | no upstream deps in core; no `meta::` literal outside generated files; no new typed upstream fact | `core`, gate 1 |

**What a bump can no longer do silently:** change a native's signature under us (parity
+ oracle), move a file we read (manifest), change the wire format (live differential),
add a function we don't know about (prelude carries it; the unclaimed ledger names it),
or leave two channels refereeing different universes (one release).

---

## 6. Decisions owed

1. **Claim-registry shape** (batch 3 design doc): annotation on the lowering rule, an
   explicit `claims(Pure.X)` registration, or a `Registrar` interface every lowering
   file implements. The test is the same either way; the ergonomics differ.
2. **Prelude carries ALL upstream natives, or only the demanded ones?** §0 says the
   right column holds *everything upstream declares that we do not implement*. For
   classes the prelude already takes the platform roots whole; for natives the same rule
   adds ~44 + whatever leaves Pure.java. Recommended: whole, same as classes — "unknown
   function" should never be the answer for a function upstream declares.
3. ~~**Module topology.**~~ **RESOLVED — three upstream-facing modules, not one,
   and not four.** Measured: `pct` and `parser-equivalence` share **135 artifacts, 70 at
   different versions** — 65 legend (the 4.133/5.88 vs 4.138/5.92 spread) and 5
   third-party (HikariCP 3.4.5 vs 7.0.2, commons-lang3 3.5 vs 3.18.0, deephaven-csv,
   junit4, httpcore). **[V] The third-party ones are version drift too, not
   module-inherent**: traced, pct's `commons-lang3 3.5` arrives via
   `legend-pure-m3-core:5.88.0` and parser's `3.18.0` via
   `legend-engine-language-pure-grammar:4.138.2`; HikariCP likewise via 4.133.0 vs
   4.138.2 artifacts. So batch 1 was expected to remove all 70 — *the dependency conflicts
   are not the reason to keep the modules apart.* **Measured at batch 1: the 66 legend
   rows vanished; the third-party ones did NOT** (4 of the 5 survived at one release —
   nearest-wins, not drift; §3 A explains the fix). The topology conclusion stands. The reasons are the other two.
   `parser-equivalence`'s pom records a live hazard: upstream test-jars
   carry **ServiceLoader registrations that ALTERED the oracle** (gate 8 caught it — a
   stale error pin and 3 corpus rows flipping), so its classpath is quarantined on
   purpose. Merging PCT's interpreted-runtime jars onto it re-opens exactly that. Meanwhile
   the rcorpus harness + generators need **no upstream jars at all** — only the checkouts
   (zero `org.finos.legend` imports in `core/test`). So the shapes are genuinely three:
   **`spec`** (checkouts only), **`pct`** (PCT framework jars + checkouts),
   **`parser-equivalence`** (grammar/oracle jars, ServiceLoader-sensitive). Splitting
   rcorpus from `spec` into a fourth buys nothing: same classpath shape, same inputs,
   and gate 2 installs core once for all of them. Three modules, one pin.
4. ~~**Two-step bump**~~ **RESOLVED (USER 2026-09-10): two** — 4.138.2 first, then 4.145.0.
5. **Cadence.**
6. ~~**DuckDB 1.4.4.0 vs the engine's 1.3.0.0**~~ **OUT OF SCOPE (USER 2026-09-10): do not
   touch.** Recorded in homework §1c for whoever picks it up.

---

## 7. Reproducing the numbers — and their bounds

Every number here is regenerable, and a fresh session should regenerate rather than trust:

| numbers | command | bound |
|---|---|---|
| the six identities, releases behind, INV-1..4 | `tools/version-report.sh` | exact (Central metadata, `git ls-remote`, pom text) |
| 132 paths; per-universe drift; imported tests | `tools/upstream-drift.py [--tests] <engine> <pure>` | exact (full git trees both sides) |
| axis U: 286 / 14,182; 491 = 205 + 238 + 37 + 11; 44 undeclared | `tools/native-axes.py` | **regex census** over declaration headers — exact to within multi-line headers (3 of 42 in one earlier pass) |
| axis O registry half: 469 keys; 272 / 67 / 6 / **175** | the §3n probe (source in homework; run once, delete) | **exact** — read from the running maps |
| the 175 → 70 / 35 / 31 / 39 | `tools/native-axes.py --tsv` with the probe output present; snapshot at `docs/NATIVE_CLAIMS_CENSUS_2026_09_10.tsv` | **heuristic** — a string mention is evidence of a handler, not proof of a lowering. "70 implemented off-registry" means *implemented, verify*; the 35 grey exist because of this |
| 8 ≤ suppressed bodies ≤ 25 | intersect the 42 / the 175 with `prelude.pure`'s 209-name block | bounds, not a count; the claim registry collapses them |
| 136 shared / 71 divergent artifacts; boundary = 0 legend jars in core | `tools/classpath-convergence.sh` | exact (resolved classpaths) |
| SystemMetamodel 104 / 23; Lexer 56; nlq 28; fqn-mapping 0 readers | greps in homework §3o | exact |

**What was inference, now measured (batch 1, 2026-09-10):** the 25 skew rows did NOT
vanish at the tag — all 25 are live at one release (they are dialect rows, §9); the 70
dependency conflicts did NOT all vanish — 66 legend did, 4 third-party did not (§3 A); the
ChannelB predictions for batch 8 stand untested (batch 1 moved 355 → 350 and 137 → 136 for a
reason the arithmetic did not model: the old pin was 20 commits PAST the tag). **Still
inference:** that the live protocol differential goes red on landing.

## 8. Next actions for a fresh session

1. **The documents and tools are committed** (2026-09-10, main): this file, the homework,
   the facts sheet, `NATIVE_CLAIMS_CENSUS_2026_09_10.tsv`, and the three read-only tools.
   No gate was needed — nothing under `core/`, `pct/` or `parser-equivalence/` changed.
2. **Batch 0 LANDED** (4678b72ce, 2026-09-10): the `nlq` rename and the
   `fqn-mapping.json` delete.
3. **Batch 1 LANDED** (2026-09-10): one release; receipts in homework §5a and
   docs/GATES.md. The oracle checkouts are `/Users/neema/legend/legend-{engine,pure}` on
   the tags (the `/Users/neemsandv` checkouts are another account's — read-only here — and
   sit on the old non-tag pin).
4. **Batch 2 LANDED** (2026-09-10): loud, not silent — receipts in docs/GATES.md.
5. **Batch 3 LANDED** (2026-09-10): the claim registry as the USER approved it and then
   revised it at landing (`docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md` §1a): derived claims,
   closed enum families, a committed ledger, UNCLAIMED_MAX = 133. Decisions taken: §6.1 as
   the doc; §6.2 the prelude carries ALL upstream natives in the read roots; §6.5 every two
   weeks, Legend's cadence.
6. **Batch 4a LANDED** (2026-09-10): 38 unimplemented FQNs left Pure.java, the prelude
   carries 31 respelled natives and 10 unsuppressed bodies, the exclusion rule keys on
   claims, UNCLAIMED_MAX = 91 — receipts in docs/GATES.md.
7. **Batch 4b LANDED** (2026-09-10, af1ca50d8 / 57ab7af09 / 2d1d156f2): `NativeFn.java`,
   the native families as closed enum types (USER: no string dispatch, exhaustive
   switches, one file); the executor kinds left `PlatformTypes`; UNCLAIMED_MAX = 0.
   Next is **batch 5**: the signature text generated from `native-membership.tsv` + the
   checkouts (design doc §3), `CORE_IMPORTS` as a generated resource, the `PlatformTypes`
   type spellings verified whole. USER 2026-09-10: local allgates is the landing gate;
   do not wait on CI between batches.
8. **SUBSUMED — a third claim kind LANDED** (2026-09-10, 84cd47ffd; receipts in
   docs/GATES.md). USER catch: createDbConfig was a stub (typed, never consumed,
   there to shadow an engine program). `com.legend.builtin.Subsumed` = engine programs
   the platform subsumes: not in Pure.java, not a family, not owned; typed by the
   corpus's own declaration; value dead by governance test; count shrink-only. The
   §0 terminology gains the kind: a name is LOWERED (Pure.java + a claim), WALLED (an
   upstream native we cannot do, loud), or SUBSUMED (an engine program that is moot
   here). Before ANY catalog removal: read the constant's comment, grep the Typer and
   kernel for consumption, check ownership — grep classes are not proof (twice).
9. **Batch 5 LEG 1 LANDED** (2026-09-11, d4ff27c2a): Pure.java's signature text is
   generated from the checkouts (`NativeSignatureGeneratorTest`, membership in
   `native-membership.tsv`); divergent rows pinned shrink-only 137 → 104. USER: no
   ledger of reasons — every divergence is fixed to upstream's real signature, by leg:
   2 wildcard subsets (17), 3 widened-to-Any (38), 4 TDS/TDSRow erasure (6 + 9 getters),
   5 inventions to Lite or deleted. Receipts in docs/GATES.md.
10. **Batch 5 LEGS 2+3 LANDED** (2026-09-11): 45 more rows adopt upstream's text
   (wildcard subsets, widened-to-Any); divergent 104 → 59. USER catch: the first
   `over` typing was a kernel approximation — reverted for real pure's bidirectional
   rule (an `over` argument types after the enclosing overload, against the expected
   `_Window<T>`; `InferenceKernel.resolveOverload(…, expected)`). Runtime elements are
   `PackageableRuntime`. Leg 4 design: docs/TDS_ERASURE_DESIGN_2026_09_11.md.
   relationalExtensions is Subsumed #2 (same day, chain green, zero movement; the
   batch-147 "signature-broken" reason was stale). OWED: preval's read-through is a
   string literal (ExecuteChainAssembly) — typed lookup + the preeval.pure:63-92 receipt
   (a semantics-preserving partial evaluation), in the next gated tree.
11. **Batch 5 LEG 4 LANDED** (2026-09-11): TDS erasure — the 8 legacy TDS rows carry
   upstream's `TabularDataSet`/`TDSRow`/`Table` text, the nine TDSRow getter natives
   are gone (upstream declares them as qualified properties: `NativeFn.RowGetter`, the
   lifted prelude definition types the call, a literal name folds to the column read);
   divergent 59 → 42. USER catch: the first implementation (nominal TDSRow + a per-call
   "receiver's row" rule + site patches) was hacking to pass tests — reset, the
   representation written into the design doc §4b, implemented once: an erased-row
   class (TDSRow, execute::Row — the family's owners) in a TYPE position IS the
   late-bound row struct (`PlatformTypes.eraseTdsRow`); a TabularDataSet OUTPUT is the
   argument's relation, the kernel's one output rule (`resolveOutput(…, args)`,
   `TdsErasure`). Three chains: 19 → 8 → 0 lost, every fix at the rule, receipts in
   docs/GATES.md. Remaining 42 = leg 5 (all inventions our own checkers synthesize).
12. **Batch 5 LEG 5a/5b LANDED** (2026-09-11): arithmetic is VARIADIC — the 13 binary
   plus/minus/times inventions and the 4 stdDev/variance shapes are gone; the parser's
   n-ary carrier `plus([a,b,c])` types against upstream's `plus(Number[*])` & co.
   directly, the typer's pairwise desugar (`InfixArith`) deleted; the infix marker rides
   the typed tree (`TypedCollection.operatorRun`: SQL-lane operands, row-wise, folds to
   the operator chain across kinds); upstream's flagged `stdDev(numbers, isBiasCorrected)`
   lands; divergent 42 → 25. Five chains: 76 → 54 → 42 → 15 → 0 lost, every fix at a
   rule that assumed pairs (receipts in docs/GATES.md). Remaining 25 = leg 5c/5d (shapes
   our own checkers synthesize or CoreFn validation targets).
13. **Batch 5 MINI-AUDIT LANDED** (2026-09-11, USER: "did we hack/shortcut/defer?"): a
   guardrail dodged by its letter (LineageTreeVerdicts), a bisect hypothesis left as code
   (`rebuilt`), a sloppy bound, and the blanket SQL-lane rule for operator runs — replaced
   by PROVENANCE (`lowering/StoreLane`: a store column read folds to the SQL chain, a
   possibly-empty pure value keeps pure's drop-empty rule), pinned both ways, recorded in
   docs/MULTIPLICITY_AUDIT_2026_08_20.md §4a. Keeping every marker across rebuilds also
   restored the row-cells mark main had been dropping: H2 gained 126 tests (579 → 453
   fails), verified by toggle. Five-chain process breach on leg 5a/5b disclosed.
14. **Batch 5 LEG 5c LANDED** (2026-09-11): 19 checker-synthesized rows measured for
   their consumer and adopted or deleted — duplicates of upstream overloads we carried
   (asc/desc(ColSpec) → ascending/descending canonical; graphFetch(ColSpec)), inventions
   with no consumer (first(T,Integer), take(Relation), 1-arg from/write, lambda
   maxBy/minBy, toSQLString(SQLResult…)), re-keys (to/toMany(Variant,T), tds::groupBy
   with AggregateValue, tableReference(Database,String,String)), the engine's own
   dynaFn isDistinct(a,b) → Lite shim. SQLResult.toSQLString is the qualified property
   the toSQLString ROUTINE implements (NativeFn.JavaRoutine.implementedDerived, the
   leg-4 pattern; NativeDispatch.RoutineCall = one call shape); the typer's third
   derived route now looks properties up by SIMPLE name like its siblings. Divergent
   25 → 6. USER: audit every simple-name use — census 140 non-parser sites, bucketed
   (function identity = smell; property-on-exact-class = correct; parser = allowed) for
   the batch-5 audit. Remaining 6 = leg 5d: join/asOfJoin 5-arg prefix form, extend/
   groupBy over C[*] ×4.
15. **Batch 5 LEG 5d LANDED — DIVERGENT ZERO** (2026-09-11): all 752 upstream-claimed
   signatures byte-identical with the pinned checkouts. USER decisions: the prefix
   joins stay as LITE SURFACE (`Pure.Lite.JOIN_WITH_PREFIX` / `AS_OF_JOIN_WITH_PREFIX`,
   2 → 4); extend over a class collection deleted (no consumer); the groupBy `C[*]`
   wildcard had hidden two shapes — the legacy instance groupBy (upstream's own K[*]
   function) lands on `Lite.GROUP_BY_OVER_INSTANCES`, the mapping/view computed-key
   group-by on `Lite.GROUP_BY_COMPUTED_KEYS` over Relation<T> (INTERNAL_DESUGAR 16 → 18).
   NEXT (USER): the batch-5 audit in this order — every Lite function in Pure.java (41)
   one by one, then every catalog row's provenance, then the simple-name census (140).
16. **Batch 5 AUDIT item 0 LANDED — the dynafunction registry** (2026-09-11): the
   engine's mapping-expression operators as data. `builtin/DynaFn.java` = every
   `dynaFnToSql('<name>')` in `extensionDefaults.pure` + 20 dialect files (222 names,
   with dialects), each with the platform's resolution: PURE 156 (passes through to the
   catalog), SHIM 9 (a `Pure.Lite` identity), TRANSLATED 22 (a translator arm rewrites
   it, nothing passes through), UNSUPPORTED 35 (fails LOUD naming the operator).
   `DynaFnRegistryTest` regenerates and verifies it against the checkout. The
   registry immediately caught two audit facts: `hash` (1-arg) and `avg` are in our
   ENGINE_VOCAB_SHIMS but are NOT engine dynafunctions (findings 4–5 in
   docs/LITE_REVIEW_2026_09_11.md). Audit material produced: the Lite one-by-one
   review (that doc) and the 752-row catalog provenance
   (docs/NATIVE_PROVENANCE_2026_09_11.md). NEXT: walk the review with the user.
17. **Batch 5 AUDIT leg A LANDED** (2026-09-11, USER: "Do the full audit"): the Lite
   review's findings 1–6 — `hash`/`avg`/`sub` deleted (no engine dynafunction, no
   producer), slot `join` and the four `*Format` shims re-filed, the shim set and the
   translator's arm set DERIVED (registry SHIM rows + declared landings; the DynaFn
   members the translator's source names), `wireEmissionName` and the aggregate-name
   hand list deleted for registry/catalog derivations; the registry gains the engine's
   type-inference map as a second source (227 members). Open: review findings 7–10.
   NEXT: leg B (catalog rows by consumer + PURE dynafunction semantics), leg C
   (simple-name census 140), leg D (owed cleanups).
18. **Batch 5 AUDIT leg C LANDED** (2026-09-11): the simple-name census burned 82 → 24
   function-identity string sites — parse-time names read the resolver
   (`ResolvedNames.names`) or `CoreFn.of`, typed nodes compare exact FQNs, the legacy
   TDS vocabulary is the closed `TdsLegacy` enum, the invented `pathWithAlias` carrier
   is retired (the path node carries its alias). Remaining 24 = structure tests,
   naming conventions, and StaticFold's bare-name switch (follow-up). Leg B's reading
   (every row claimed; 108/158 PURE dynafunctions corpus-exercised) is in
   docs/NATIVE_PROVENANCE_2026_09_11.md. NEXT: leg D (owed cleanups), then batch 6.
19. **Batch 5 AUDIT leg D LANDED — THE AUDIT IS COMPLETE** (2026-09-11): the two dead
   nominal-TDSRow kernel arms deleted (leg 4's erasure made them unreachable; the chain
   measured it), the `generateSeedDataString` construct renamed back, the plus-family
   key set and the constant-fold FQN switch read the platform constants. Batch 5 is
   closed: DIVERGENT ZERO, the registry, and the four audit legs. Open findings live in
   docs/LITE_REVIEW_2026_09_11.md (7–12). NEXT: batch 6 (protocol live).
20. **Batches 1–5 AUDITED; batch 5 remainder LANDED** (2026-09-11,
   docs/BATCH_1_5_AUDIT_2026_09_11.md): `CORE_IMPORTS` is the engine's
   `META_IMPORTS` sequence, generated and held (`CoreImportsParityTest`); every
   `PlatformTypes` spelling is held against the checkouts (`PlatformNamesSpellingTest`;
   drift test deleted; inline literals gone); convergence runs in CI. Finding 5.4: the
   Package/primitive re-homing is the platform's canonical spelling (recorded).
   DEFERRED: 39 engine-root natives into the prelude (batch 4 §6.2) — the next leg,
   then batch 6.
21. **Batch 4 §6.2 COMPLETED** (2026-09-11): the engine's natives enter the prelude
   respelled (+30; 5 not carried, listed in the header — their signatures name types
   the platform reserves or excludes); their signature types seed the closure. Batches
   1–5 now hold in full. NEXT: batch 6 — own-corpus byte parity in gate 8, the 17 goldens
   out of core, the protocol roster as a ledger.
22. **Batch 6 LANDED — protocol, live** (2026-09-11): gate 8 was already the live
   differential over 8,891 upstream sources; it now also byte-compares our own test
   snippets (`OwnCorpusParityTest`: 2,292 elements matched, 0 diffs, an empty shrink-only
   ledger `docs/own-corpus-protocol-diffs.tsv`), the 17 goldens' sources are live seeds
   (`ProtocolSeedParityTest`, exact) and the golden tests are deleted from core, and the
   protocol-type roster is a committed ledger (`docs/protocol-roster.tsv`, 1,033 tags,
   279 covered; `ProtocolRosterCensusTest` holds it equal). Two emitter divergences the
   own corpus exposed are fixed (first bracketed id is the target; a root-marked
   enumeration mapping's span starts at `*`). NEXT: batch 7 (the `spec` module).
23. **Batch 7 PLANNED** (2026-09-11, docs/BATCH_7_PLAN_2026_09_11.md, after
   docs/BATCH_7_HOMEWORK_2026_09_11.md): USER — the shell that finds and runs Pure test
   functions is PRODUCT surface. Batch 7 is now three steps: 7a the test runner into
   core (`com.legend.test`: discovery, running, an observer; the harness becomes a
   caller), 7b the `spec` module (15 files, unchanged packages; nothing they pin may
   move), 7c the boundary enforced (groupId → `com.legend`; enforcer + ArchUnit rules).
24. **Batch 7a LANDED** (2026-09-11): `com.legend.test` — `PureTests` (discovery over
   the model, marks reported not applied), `PureTestRunner` (sessions, setups, the body
   through the platform, one result), `TestObserver` (the harness's seam); the corpus
   harness is a caller (926 → 699 lines); `PureTestRunnerTest` proves the runner with no
   checkout. USER decided the groupId rename (7c). NEXT: 7b (the `spec` module).
25. **Batch 7b LANDED** (2026-09-11): the `spec` module — rcorpus, tools and harness
   (20 files + the rosters) moved with unchanged package names; core publishes a test
   jar; `CoreTree` is the one root the generators write through; gates 4/5 run from
   `spec`; every pinned number identical. NEXT: 7c (groupId → `com.legend`; enforcer +
   ArchUnit bans in core and spec).
26. **Batch 7c LANDED with 7b** (2026-09-11): groupId `com.legend` in six poms; the
   Maven enforcer bans `org.finos.legend*` in core and spec (proved by a deliberate
   breach); ArchUnit bans the imports in both. BATCH 7 IS COMPLETE; the thesis's four
   sentences are enforced. NEXT: batch 8 — the bump to 4.145.0 / 5.99.0, the first under
   the program (§5: one line, regenerate, review the three diffs).
27. **Batch 7d LANDED** (2026-09-11, USER's questions on the test jar and the gate
   coupling): no test jar — `RegistryKeys` is core's one public read-only view of the
   implemented keys and the claims registry lives in spec with the generators; gate 3
   (spec parity) runs the generated-fact checks alone, gates 4/5 the corpus alone;
   convergence runs in gate 2 after the install (the batch-7 push was red in CI at the
   pre-build step). The pipeline, step by step, is in the 7d record. NEXT: batch 8.
28. **Batch 8 LANDED** (2026-09-12): the bump to 4.145.0 / 5.99.0 through
   `tools/bump.sh` (§5 "a bump is one command"). The drift the tool read (files) was
   a fraction of the drift the generators and gates found (grammar): documentation
   blocks, DataSpace's new blocks, relational lambdas, DataQuality test suites,
   unquoted time zones, `between` folded, three `elementToPath` overloads, five new
   dynafunctions, ~120 new relation PCT functions, and the engine's ONE canonical
   null placement (which closed §7 slice-2's two-spec split). Every move is in the
   GATES.md batch-8 record with its reason; A–F held: no upstream fact was copied by
   hand, every pin moved with a written reason, gate 8 is byte-exact on all 6,745
   accepted documents. Lesson for §5: the drift tool must read GRAMMAR drift too
   (the .g4 diff between tags), not only file sets. The program's eight batches are
   complete; what remains are the legs the bump opened (the record's last paragraph)
   and the cadence decision (§6 item 5).
29. **Batch 8 leg 1 LANDED** (2026-09-12): the engine's execution FEATURE FLAGS as a
   platform fact — one verified enum mirror, one set on the typed context fed by the
   engine's two carriers (the exeCtx option context; `withFeatureFlags` in the body), one
   ambient merge on the runner options, one consumer (the lowering; a flag selects an
   emission, never a tree — `FeatureRules`). First flag consumed:
   `CORRECT_SQL_SUBSTRING_INDEXING` — the substring family returns (7 PCT Essential rows;
   2 corpus tests on both lanes). USER DECISION: both PCT channels run with the flag on,
   a written deviation from the reference adapters (the engine's PCT never sets it; only
   its testable framework does, for relation-returning function tests); the corpus runs
   the uncorrected default, as the engine's relational tests do. A Lite-only native for
   the corrected form was built and REJECTED the same day (USER: "why do we need a new
   lite only function?"). Decision owed: whether the flag should also cover indexOf
   (upstream's does not). The GATES.md record has the moves.
30. **Batch 8 leg 2 LANDED** (2026-09-12): the QUANTIFICATION family — the ten quantified
   comparisons, `relation::in`, two-argument `relation::exists` — 90 of the 111 Relation
   PCT rows the bump opened, one design: a value tested against a single-column relation,
   emitted the engine's way (two-valued: `value IS NOT NULL AND value op ANY|ALL (SELECT
   col … WHERE col IS NOT NULL)`, fixed row sets isolated first). One closed family
   (`NativeFn.RelationQuantifier`), two SQL IR nodes, one routine in RelationPredicates.
   Census before code (USER: "bigger bucket first?" → the 111 rows bucketed by refusal
   text; 90 were one mechanism). Remaining: joinStrings shapes (11), sort null forms (7),
   variant columns (3). The GATES.md record has the moves.
31. **Batch 8 leg 3 LANDED** (2026-09-12, three commits): the sort null forms (7 rows:
   `SortInfo.nullOrder` on the typed sort key, `emptyFirst`/`emptyLast`, the two-argument
   direction overloads; one consumer rule at both sort sites and the window ORDER BY),
   the group-lambda aggregates (11 rows: `~c : g | $g->joinStrings(…)` / `$g->size()`
   desugared to the map/reduce form; the ordered aggregate generalized to a list of keys
   with placement), the variant emptiness conversions (2 rows). THE RELATION PCT UNIVERSE
   IS AT ITS FLOOR: 469 functions, 1 expected-failure row (the reference adapters' own
   verdict); channel B Relation 469/469. USER: "Let's burn them all down". Records in
   GATES.md ("SORT NULL FORMS", "GROUP-LAMBDA AGGREGATES + VARIANT EMPTINESS").
32. **substr is always corrected** (2026-09-12, USER relaying the engine devs): in the
   engine's relational lowering `substr` is its own registration, always index-corrected;
   `substring` is corrected only under the flag (backwards compatibility). The platform
   inlined substr's Pure body into substring and got substr WRONG with the flag off —
   now a native with the corrected rule, its body out of the prelude, proved end to end
   by `SubstrIndexingTest`. Lesson: read the lowering registration, not only the body.

## 9. Where this program overrides the homework

The homework is the evidence base and remains correct on every *measurement*; these are
the places where its *conclusions* are superseded, so a reader does not carry them
forward:

- **"Native" is two things (§0).** The homework uses the word for both axes without
  flagging it. Its §3j/§3k/§3m analysis of "the 42" is sound as a *lower bound* on
  unimplemented natives; the true figure is bounded 42–175 (§3n) and is not computable
  until the claim registry exists.
- **"8 suppressed bodies" → 8 to 25.** The homework's 8 intersected the looser 42; the
  registry-based 175 intersects the prelude's exclusion list at 25.
- **Pure.java is NOT generated from upstream's natives.** A previous draft of this
  program said so; it conflated the axes. Membership is our claim (D1); only the
  signature *text* is derived (D4).
- **There IS a warranted Pure.java split** — along the ours/theirs axis
  (`Lite.java` for the 29 inventions; generated text for the rest), not
  platform-vs-corpus (§3j was answering the wrong framing).
- **Protocol is live, not a golden** (§3d row 7 is stale; §5c is current).
- **Relocating unimplemented natives to the prelude is correct** (§3k's objection was to
  a runtime-mutable catalog; the prelude is a static resource — §3m).
- **§5 Phase 6 "new natives → new signatures" is stale**: zero new signatures at 4.145.0
  (§4b).
- **The fixture version lives inside the file**, not in its name (§6.5).
- **The ArchUnit rule is narrower than §3e claims**: it closes the literal/identifier
  classes; the observed-bytes class is closed by *deleting* the goldens (F), not by a
  rule.
- **The skew ledger was never skew rent (batch 1 measurement).** §3 A and §4 said the 25
  rows of `docs/version-skew-claims.tsv` were "the rent" of a non-tag pin and would mostly
  vanish at one release. At 4.138.2 = 4.138.2 all 25 are still live: the engine's grammar
  refuses them at the SAME release. They are legend-pure-vs-legend-engine dialect rows
  (m2-dsl-tds `#TDS` positions, `Primitive X extends Y`) and engine-refuses-its-own-file
  rows. The ledger stays, re-annotated; its name is now wrong and it is a batch-2/7 rename.
- **The tag's SHA in homework §1 is the tag OBJECT** (`1d3e236b…`); the pin carries the
  commit it points to (`28e75114f…`) because CI and `oracle_roots_check` compare HEAD.
- **"Consolidation breakage" was not purely consolidation.** The two-step bump was meant
  to keep it apart from upstream change, and mostly did — but the old non-tag pin was 20
  commits PAST the tag, so moving to the tag moved the spec BACKWARDS on one commit
  (#4900, null-safe equality): 19 corpus tests the platform passes against the newer
  spec fail against 4.138.2 and sit on the fail rosters until 4.145.0. The 5.92.0
  `stringToTDS` quoting change, by contrast, was genuine upstream change on the jar side
  (4.133.0 → 4.138.2).
