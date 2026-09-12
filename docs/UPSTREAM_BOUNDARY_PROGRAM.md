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

### D — The implemented surface (the Pure.java clean-up)

This is the workstream the homework under-specified, and the one §0 governs.

**D1. One registry of claims.** Today an implementer is one of: a key in
`Scalars.RULES` (455), `Windows.FNS` (18), `Windows.AGGREGATES` (2),
`Aggregates.REDUCERS` (97); a `CoreFn` arm (61 members); a `WALLED_NATIVES` entry (6,
with reasons); or one of **~80 files** that pattern-match a callee ad hoc (asserts,
collection lanes, calendar aggregates, the desugar IR). Make every one of those a
**claim** against a Pure.java overload — registry key, `CoreFn` arm, wall, or an explicit
`claims(Pure.X)` from an ad-hoc site. One test: every Pure.java overload has **exactly
one** claim; every claim names a Pure.java overload. The probe that motivates this is
receipted in the homework §3n: 469 registry keys + 136 CoreFn overloads + 6 walls cover
611 of 881 overloads; **270 overloads / 175 FQNs are claimed by no registry.** Classified
against the ~80 ad-hoc sites, the 175 split three ways (§3n):

| | FQNs | what it is |
|---|---:|---|
| **implemented off-registry** | **70** | a real lowering no registry sees — `CalendarAgg` handles 32 calendar dates, `AssertVerdicts` 14 asserts, `Lowerer`/`Lexicon`/`Fold`/`JoinChecker` the rest. These are the "scalar functions that DO get lowered"; the registry just cannot see them |
| **grey — front-end only** | **35** | named only in `Typer`/`LiteralUnroll`/`ExecuteChainAssembly`/`ContextReading`/…: type-checked or desugared (`dynamicNew`, `enumValues`, `sourceInformation`, the three post-processors), possibly never lowered as a call. Each needs one look |
| **certainly unimplemented** | **70** | 39 named nowhere + 31 named only as a `PlatformTypes`/`SystemMetamodel` constant: 13 reflection (`reactivate`, `pathToElement`, `openVariableValues`, …), 6 `toDDL`, 6 `execute::fetch*`/`loadCsv`, 4 `sqlstring::toSQL*`, 4 `lineage::scan*`, 3 `executionPlan`, `noDebug`, `resolveStore`, … (full list: homework §3n appendix) |

So the work is **both** things the question asks: make the 70 real implementations
*claim* what they implement (so the registry can see them), and move the 70 that nothing
implements out of Pure.java. The 35 grey are adjudicated one by one as part of D1. And
the two 70s are a coincidence of counting, not a symmetry.

**D2. Unclaimed entries leave Pure.java.** Whatever D1 leaves unclaimed (≥ 42, ≤ 175
FQNs) is not implemented and must not claim to be. Each moves to the right column: if
upstream has a Pure body, the prelude **carries the body** (114 of the 175 are
upstream-bodied — we can *run* them); if upstream is native-only, the prelude carries a
**respelled `native function`** (37 of the 175). Today **between 8 and 25** upstream
Pure implementations are *suppressed* by the prelude's platform-owned rule because we
declared a native we never lowered — those start working the day their signature leaves.

**D3. The prelude's exclusion rule keys on the claim registry**, not on "Pure.java
declares it". Once D2 lands the two coincide; the rule then stays correct by
construction. The generator also **stops skipping upstream natives** (today it drops
them by token position — `native` precedes `function`) and carries them respelled: the
**44** upstream natives we do not declare at all — `lang::new`, `lang::copy`,
`meta::newClass`, `meta::newProperty`, `meta::tag`, … — stop being "unknown function"
and become "not implemented: X", which is the truth.

**D4. Signature text verified, then generated.** Every remaining Pure.java FQN's
signature is compared against upstream's declaration of the same FQN — native *or*
bodied, the signature is identical either way. Three buckets: match; **same FQN,
different signature** (the silent divergence nothing sees today); ours-only (must be
`meta::legend::lite`). Then flip from verify to generate: the text comes from the
checkout, the membership from the claim registry, and drift is impossible rather than
detected.

**D5. Delete the stale hand lists this exposes.** `Scalars.KNOWN_ABSENT` ("names known
to be ABSENT from our catalog") has 39 entries of which **38 are now present** — 97%
stale, harmless only because its branch is dead. It is the pattern in miniature.

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

**A bump is one line.** Change `LEGEND_ENGINE_RELEASE` in `tools/oracle-pins.env`.
`tools/bump.sh` (**to build — the batch 8 deliverable; today the steps are §5 of the
homework, run by hand**) moves the checkouts, regenerates every resource, runs `--check`
and the gates. A human reviews **three things**: the generated-resource diffs (that *is* the
upstream change, made legible), the ratchet moves (each with a reason), the ledger
adjudications. CI runs it on three platforms. Upstream ships roughly weekly; monthly or
per-minor is the cadence, and a scheduled job opening the PR is the natural endpoint.

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
4. **Before batch 3**, write the design doc for the claim registry (§6.1) — shape,
   membership-list format, how the ~80 ad-hoc sites claim. Do not start coding it
   without one.
5. **Decisions still open:** §6.1, §6.2, §6.4, §6.5, §6.6.

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
