# TRAIL: the existing design trail against the upstream-only homework (2026-10-05)

Read-only reconciliation. Repo `runs/build-rebuild` at `1689703f2` (branch build/rebuild). "The trail" is:
- `docs/UPSTREAM_BOUNDARY_PROGRAM.md` (PROG)
- `docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md` (UBH)
- `docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md` (CLAIM)
- `docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md` (PMH)
- `docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md` (SPD)
- `docs/COMPILER_DESIGN_2026_09_25.md` (CD)
- `docs/EXECUTION_PLAN_2026_09_26.md` (EP)

"The new homework" is in `runs/bazel-plan/docs/`:
- `UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` (UOH)
- `build-inventory/upstream-only/R1.md`, `R2.md`, `R3.md`, and `BRIEF.md`
- `GENERATORS.md` (GEN)

**Terminology (PROG §0, lines 20-51).** Every function sits in one of four cells:

|  | platform-lowered (Pure.java) | not platform-lowered (prelude) |
|---|---|---|
| upstream-native (Java body) | signature in Pure.java, text from upstream | the prelude carries a respelled `native function` |
| upstream-bodied (Pure body) | signature in Pure.java overrides the body; the prelude excludes the body | the prelude carries upstream's body |

Bare "native" is not used below unless it quotes someone else's words.

---

## 1. What the existing design says about each upstream-derived artifact

| Artifact | Generated from upstream | A registration (ours) | Where the trail says it |
|---|---|---|---|
| **Platform-lowered signatures (Pure.java via gen_natives)** | The signature *text* is generated from the pinned checkouts. Identity is the upstream signature id, `FunctionId` (from `SignatureMangle.mangle`), compared whole. | *Membership* is ours. That means which upstream declarations the platform lowers, whichever upstream cell they sit in. | PROG §0 (the table; "Pure.java is the left column"). PROG §3 C ("membership is OUR decision… the spelling is upstream's"). PROG §9 ("Pure.java is NOT generated from upstream's natives… only the signature *text* is derived"). UBH §3n "What this changes" (l.1251-1253). CLAIM §3: membership is a TSV, deliberately the generator's fixed point, because "a generator whose input is the file it generates is circular" (l.192-196). PROG §3 D rulings (l.252-259): identity = FunctionId, no string identity. |
| **Lite (`meta::legend::lite`)** | Nothing. These are our inventions. | All of it, hand-declared. | PROG §2 target `builtin/Lite.java` (l.111). PROG §2b "Lite natives" row (l.149): split out, not subject to the signature oracle, and the ours-only bucket must equal exactly this set. CLAIM §3 (l.189-190). |
| **Catalog / DeclarationTable / FunctionId** | The declaration universe is upstream's *whole*. The catalog must not be a subset that the compiler quietly completes with name rules. A platform-lowered declaration and upstream's bodied twin with the same id are ONE declaration, and the bodied one is kept. | Which implementation each id gets: the ImplementationTable rows Form / Intrinsic / Body / Unimplemented / Refused, built from `Registrations`. | PROG §3 D step 2 (l.271). PROG "What D's old text got wrong" (l.392-398). CD §3.1 (l.111-125): "the platform's own declarations (the catalog of natives, generated from upstream) are a module like any other". CD §4 (l.272-289): one registry, total, "generated, its kind counts pinned exactly". EP W2.1 (l.660-666): "platform declarations generated at build time as a serialized resource, not 854 parses at class load". It must also add the 191 missing overloads (PROG l.243; EP l.865) and a collision guard on the mangle. EP §1b `catalog` target (l.265). EP W2.8 (l.735-737): `functionsAt` reads the declaration table only. |
| **DynaFn** | The member set, the dialects and the inference come from `dynaFnToSql` and the type-inference map (`extensionDefaults.pure` plus the dialect files). | The resolution of each name (PURE / SHIM / TRANSLATED / UNSUPPORTED), and the shim landings. The 4th column (the declarations a name resolves to) is *derived* from the catalog. | PROG §8 item 16 (l.660-671), item 17 (l.672-680; shim set and arm set derived). PROG step 4b.3 (l.273: "a PURE row's catalog FQNs generated from the catalog"). End state, CD §4 (l.283-286): "the dynafunction column" becomes a row source of the one registry. CD §3.6 (l.262-270): dialect renderings generated from `dynaFnToSql`, and the translator's separate dynafunction spellings deleted. |
| **Handler surface (engine-handlers.tsv)** | Generated from the pinned `Handlers.java`: 836 rows, with 169 undeclared engine ids pinned. | Which of its ids the platform declares (the "declared column"). The Lite surface. | PROG step 4b.0 (l.273). CD §1 (l.58-64), CD §5 (l.298-301): engine input gets the handler surface and Pure source does not. CD §4 (l.284): "the handler surface's declared column" is a row source of the registry. EP D13 (l.342): one overload rule over the engine's namespace, and the engine's first-match order is "registration order, not semantics". |
| **CORE_IMPORTS** | Generated as an ordered SEQUENCE (order is semantic, first match wins). | Nothing. | UBH §3f (l.441-452), §3d row 10. PROG §2b row (l.151) and target `builtin/core-imports.*` GENERATED (l.114). PMH §9.12 (l.302-310): the core group = m3 coreImport plus the engine's three (META_IMPORTS). Two sequences per grammar: EP W2.2 (l.673-676: "the core group becomes the reference's 29 for Pure source only after a probe"), W2.3b (l.707-710), D13; CD §5. |
| **The prelude** | Generated, verbatim and parser-delimited, under each file's imports, with a byte-parity assert. Contents: the right column of §0, meaning legend-pure's platform roots whole, plus the Java vocabulary, plus closure (T1). Upstream-native declarations are carried respelled; upstream-bodied declarations with their bodies. | The exclusion rule: "exclude the body iff a claim exists for that FQN" (not the bare name). The demand rule: by USE, with a receipt naming the Java site. The engine library functions membership (one interim row, `removeAll`). | PROG §2 target (l.112-113). CLAIM §4 (l.198-206). PMH §2 T1-T5 (l.75-100), §6 (l.142-152), §9.7 (l.258-262: "Verbatim = PARSER-DELIMITED, never brace-matched"), §9.9a (closure option B). SPD §3-§4: "Legend-engine's packages (core, core_relational, …) are NOT prelude material" (l.110-113). **End state:** the prelude SHRINKS to "what the platform's Java itself needs at boot" once loading follows the manifests: PROG step 7 and D7 (l.276-398), CD §3.1 (l.123-125), EP W4.4a (l.858), with D9 OPEN (EP l.337; the "2b" whole-stdlib question, PROG l.397). The interim namespace rule (D7, l.360-371) admits every bodied `meta::pure::functions::` function: 126 functions, 26 boot-typing failures, and the `agg` form is unowned. |
| **The claims ledger (native-claims.tsv)** | Nothing. It is a measurement of OUR registries. | The claims are derived from the registries; there is no hand table (CLAIM §1a, l.69-109). | CLAIM §2a (l.130-143): a committed ledger, "the prelude.pure contract". **It retires at untangle step 5**: "the implementation table is the surface" (PROG step 5, l.274; the headers of native-claims.tsv and native-membership.tsv say "RETIRING at untangle step 5"). |
| **PlatformTypes spellings / SystemMetamodel's 104** | First planned as a generated names table, `platform-names.*` (PROG l.115, §2b l.152-153). Landed instead as verification: every spelling must be an upstream declaration (`PlatformNamesSpellingTest`: "There is nothing to GENERATE here"). | The choice of which names are distinguished. | UBH §3g (l.454-512). PROG §8 item 20 (l.695-702). |
| **Protocol** | No resource. A live differential (gate 8). The protocol-roster ledger is a reviewed diff. | The divergence ledger. | PROG §3 F, batch 6 (l.708-716). |

**The two rules, as the trail states them.** CD §2 tenets 4 and 5 (l.92-96): "Everything the platform knows about upstream is generated from the pinned checkouts and verified by a test that reads the same checkout"; "Everything the platform decides for itself is a registration, in one registry". UBH §5b (l.1544-1553) adds: "Anything that READS upstream moves out of core. Anything upstream-DERIVED stays in core as a generated resource". PROG §2 (l.132-135) adds: "generator outside, generated resource inside, byte-parity asserted".

---

## 2. What landed, and where the program stopped

**Landed, on HEAD** (each commit was checked as an ancestor of `1689703f2`):

| Step | Commit, date | What it put in core / spec |
|---|---|---|
| Program and tools | c5121c020 09-10 | the PROG/UBH documents and the read-only tools |
| Batch 0 | 4678b72ce 09-10 | nlq rename; fqn-mapping.json deleted |
| Batch 1 | a6b1487eb 09-10 | one release (INV-1..6) |
| Batch 2 | bcbe0269e 09-10 | `UpstreamPathManifestTest`: the upstream paths fail loudly |
| Batch 3 | 3e0435666 09-10 | claim registry: `Claims` (now `spec/src/gen/.../claims/Claims.java`), `ClaimRegistryTest`, native-claims.tsv |
| Batch 4a | 3dd586e60 09-10 | unclaimed functions leave Pure.java; the prelude carries respelled upstream-native declarations |
| Batch 4b | af1ca50d8, 57ab7af09, 2d1d156f2 09-10 | `builtin/NativeFn.java` (closed families); UNCLAIMED_MAX = 0 |
| Subsumed | 84cd47ffd 09-10 | `builtin/Subsumed.java` |
| Batch 5 leg 1 | d4ff27c2a 09-11 | Pure.java signature text generated (`NativesGenerator`); `native-membership.tsv` |
| Batch 5 legs 2-5d | df351b157, 81dd95298, 1da104d99, c96ea6f12, 71fd47387 | DIVERGENT 0 |
| Batch 5 audit item 0 | a0b2ea381 | `builtin/DynaFn.java` + `DynaFnRegistryTest` |
| Batch 5 audit leg A | 721001a17 | the Lite review's findings |
| Batch 5 remainder | b7504b407 | CORE_IMPORTS generated (`ImportsGenerator`, `CoreImportsParityTest`); `PlatformNamesSpellingTest` |
| Batch 4 §6.2 | aca92fb2b | the engine's upstream-native declarations enter the prelude respelled |
| Batch 6 | a51b5d7bc | protocol live |
| Batch 7 | 2fbc633a3, 756a63666, 3d15fa015 | the spec module; enforcer + ArchUnit |
| Batch 8 | 0a4a928c6 (09-11/12) | the bump to 4.145.0 / 5.99.0 through `tools/bump` |

**The untangle (PROG §3 D, re-chartered 2026-09-24):**

| Step | Commit | What landed |
|---|---|---|
| 0 | be2208b2b | `IdentityGuardrailTest` |
| 1 | e48f7d17b | `CatalogUpstreamDiffTest`: 787 EXACT / 0 DIVERGENT pinned / 191 MISSING |
| (prep) | bc9bc83ea | functions indexed by signature id |
| 2 | 2a215a8f1, cda91e93b | `com.legend.platform.{DeclarationTable, ImplementationTable, Implementation, Registrations}` |
| 3 | 86e58b3af | the shadow probe |
| 4a | f2d4220a0 | the pick by table; `lowering/PlatformRegistrations` |
| 4b.0 | a77197dab | `builtin/EngineHandlers.java` + `engine-handlers.tsv` |
| 4b.1 | e2f201210 | the resolver qualifies platform function FQNs |
| 4b.2 + 4b.3 | 639063f9a | `compiler/BareNames.java`; DynaFn rows carry declarations |
| audit | c5caddd3b | the audit before 4c |

**Under EP, the old 4d registration half:** EP step 2 (A2), 671fbb88c (09-26), "lowering by FunctionId" (EP §3 l.361):
- `FunctionId` moved to `model/` (`core/src/main/java/com/legend/model/FunctionId.java`; `SignatureMangle.java` beside it).
- 488 generated `AT_*` overload groups in Pure.java.
- Every rule table is keyed by FunctionId.
- `REGISTERED_BY_BARE`, `KEYS_BY_NAME` and `Pure.nativeKeysAt` deleted (grep today: 0 product hits).

**Bazel:** 998f1a41c (09-23) made the generators Bazel programs behind `//:update_generated`. fb43cd0a8 (10-04) moved them to `spec/src/gen`.

**What core has today:**
- `com.legend.platform` (`CoreFn`, `DeclarationTable`, `Feature`, `Implementation`, `ImplementationTable`, `Registrations`, `WalledBodies`).
- `model.FunctionId` and `model.SignatureMangle`.
- `PureModelContext.declarations()` builds `DeclarationTable.of(Pure.all() ∪ model.functions())`, which is the catalog plus the boot and user model. It is not upstream's whole universe; that exists only in spec's `ImplementationTableTest` and `CatalogUpstreamDiffTest`.
- `core/BUILD.bazel:699-706`: six generated files, each also its generator's input (`exports_files` l.710-712).

**Still only designed:**
- `Lite.java` (never split: Pure.Lite is a nested class and the 43 Lite constants are interleaved in Pure.java).
- A generated `platform-names.*` (replaced by a verification test).
- `CORE_IMPORTS` as its own file (spliced into NameResolver.java instead).
- 4c / W2.8: `isPlatformOwnedFunction` and `SUPPRESSED_ONCE` are still in `FunctionCompiler.java`.
- Step 5 / W2.3a-W2.6: retiring native-membership.tsv and native-claims.tsv; the identity pins going to 0.
- Step 7 / W4.4a: load by manifest. `ENGINE_LIBRARY_FUNCTIONS` is still the interim at `PreludeGenerator.java:121-128`.
- W2.1: the catalog as a generated serialized resource, the 191 missing overloads, the mangle collision guard.
- The one registry (CD §4; EP W5.1a `LoweringTable`).
- The prelude shrink.
- CLAIM §4's FQN-keyed exclusion: the generator still keys on bare names (R2 #3).

**Where it stopped, per its own documents:**
- PROG: batches 0-8 are complete (§8 item 28). The untangle is at 4b.3 plus the audit. PROG says the next step is **4c**, but "SUPERSEDED 2026-09-29 for its order and steps" by EP (PROG l.231).
- In EP, the remainder sits in Phase 4. The order is W2.1 → W2.2 → W2.2b(1) → W2.3a … → W2.8 (= 4c, after W3.3) (EP l.249-252). W4.4a (= step 7) sits in Phase 3, after D9 at C1.
- EP's own "Now" (§0) is the Phase-1 D24 cleanup, before C1.
- The whole compiler rebuild is **PARKED**: IN_FLIGHT on origin/main, 2026-10-04 ("paused, coming back later"); 0e6a30afd ("compiler rebuild paused by the user").
- **So the next step of the upstream work, per the trail, is W2.1, inside a parked plan.**

---

## 3. Reconciliation: each item in the new homework against the trail

Verdicts: **AGREES**, **DUPLICATES** (already built or already designed), **CONTRADICTS** (both sides cited), **ADDS** (new).

### R1: gen_natives / native_declarations

| # | New homework item | Verdict | Against the trail |
|---|---|---|---|
| R1.1 | The signature text is RENDERED by our parser, resolver and renderer, not verbatim (R1 §0.1). | ADDS; CONTRADICTS the trail's wording | PROG §8 item 15 says "all 752 upstream-claimed signatures byte-identical with the pinned checkouts". Per R1 that means identical under our canonical rendering, not verbatim. PROG §2b's "verified against upstream's declaration" holds only modulo that rendering. |
| R1.2 | The committed prelude contributes nothing to gen_natives' bytes; it is a failure dependency only (§0.2; E9 confirms). | ADDS | Settles G1's open item. The trail never asked. |
| R1.3 | Pure.java is its own template (a self-loop): Lite constants and constant order shape the AT_ groups. | AGREES with the trail's intent; it exposes that the landing violated it | CLAIM §3 (l.192-196) chose a TSV precisely because "a generator whose input is the file it generates is circular". The landing (d4ff27c2a, then 671fbb88c's generated AT_ groups) made Pure.java an input anyway. |
| R1.4 | The committed CORE_IMPORTS resolves bare names in upstream files. The remedy offered is "use `ImportsGenerator.metaImports(CompileContext)`". | ADDS the finding. **The remedy CONTRADICTS the trail.** | Upstream `.pure` files are *Pure source*. EP W2.2 (l.673-676), W2.3b (l.707-710), D13 and CD §5 put Pure source on legend-pure's `coreImport` (29, from m3.pure) and the engine's META_IMPORTS (32) on engine input only. `CoreImportsParityTest` already reads both sequences. |
| R1.5 | ENGINE_SPEC_ROOTS is our choice; 10 upstream-native declarations lie outside it. | ADDS; AGREES with D7 | PROG D7 (l.280-295): the manifests decide which files exist, never a hand list ("each list is a person guessing at a slice of the closure"). |
| R1.6 | Membership = 265 upstream-native + 524 upstream-bodied rows (all platform-lowered). | DUPLICATES (re-measured) | PROG §0 / UBH §3n measured the same split on 2026-09-10: 205 upstream-native only, 238 upstream-bodied only, 37 both, 11 outside the roots. That is the point of §0. |
| R1.A | One upstream-only catalog: every declaration, upstream-native and upstream-bodied (~16.5k), with upstream's signature id. | AGREES; DUPLICATES the W2.1 design | EP W2.1 (l.660-666) is this resource, including the 64 KB clinit argument and "whether the WASM planner can load the resource is checked first". PROG l.392-396: the declaration universe is upstream's whole. CD §3.1. The input set already exists as spec-side measurements: `CatalogUpstreamDiffTest` reads every upstream declaration; spec `ImplementationTableTest` builds catalog + stdlib + upstream into 3,157 rows. Not built: a committed or sealed resource. |
| R1.A' | Identity from a small header scanner, without our parser. | **CONTRADICTS** PMH §9.7; ADDS the decoupling goal | PMH §9.7 (l.258-262) ruled "Verbatim = PARSER-DELIMITED, never brace-matched", after the branch's brace-matching `declarationText` was wrong for headers with tagged values. R1 §1.A describes exactly a skip-`<…>`/`{…}`/`(…)` scanner. Acceptable only with R1 risk 2's test: the scanner's id equals `SignatureMangle.mangle` of our parse for every declaration, as `CatalogUpstreamDiffTest` already computes. |
| R1.A'' | A verbatim header plus the section's imports, resolved in core. | AGREES | The same emission rule as the prelude (PMH §6, l.142-152). |
| R1.B | Pure.java hand-owned: `X = catalog("<id>")`. | AGREES with the identity ruling; partly CONTRADICTS the end state | The PROG D rulings make FunctionId the identity. But the trail's end state is "native-membership.tsv / native-claims.tsv retire (the table is the surface)" (PROG step 5): membership becomes ImplementationTable rows built from Registrations (CD §4). Pure.java constants should be *handles* that registrations name, not a second membership list. |
| R1.B' | AT_ groups computed at class init. | ADDS; mild tension with step 2 | 671fbb88c generates them "computed once" after a 2× corpus timing regression from per-call mangling. Class-init computation once is equivalent. Keep the "once". |
| R1.B'' | Lite keeps `signature(String)`, or moves to a hand `lite.pure`. | AGREES | PROG §2b Lite row and l.111 (the `Lite.java` split never landed). |
| R1.B''' | native-membership.tsv, its draft and `//core:draft_native_membership` retire. | AGREES | The membership file's own header: "RETIRING at untangle step 5". |
| R1.C | A join in core's build into an embedded resource of about 800 rows, parsed at clinit. | AGREES on the resource; CONTRADICTS W2.1's boot aim | W2.1 removes "854 parses at class load" (a serialized, pre-parsed form). R1 risk 6 keeps the parses. |
| R1 risk 1 | Do our keys map 1:1 onto upstream ids? Count collisions. | Mostly DUPLICATES | FunctionId already is upstream's id with the return type (`FunctionId.java` javadoc). `CatalogUpstreamDiffTest` pins DIVERGENT 0 by id. W2.1 already owns "a collision guard on the mangle". The new part is small: native-membership.tsv column 3 is still the pre-step-2 FQN-typed key without a return type, so re-key it with `FunctionId.of(constant)`, which the AT_ groups already compute. |
| R1.5 (catalog file) | native_declarations widened IS the catalog: merge into one target. | ADDS | — |

### R2: gen_prelude / gen_engine_handlers

| # | New homework item | Verdict | Against the trail |
|---|---|---|---|
| R2 #1 | The Java demand scan is an input of ours (E4b). | AGREES; ADDS the reframing | The trail made Java a demand source on purpose: PMH T1 and "T1 sharpened" (l.79-85, platform vocabulary by USE, with a receipt naming the Java site; phase 3 tightens demand to construct/read/signature). CD §3.1 calls the end set "what the platform's Java itself needs at boot". R2's registration plus a test running the scan is the tenet-5 form of the same rule. |
| R2 #3 | The claims exclusion is keyed on BARE names (245 FQNs). | CONTRADICTS (the landed code against the trail) | CLAIM §4 (l.198-206): "exclude the body iff `Claims` holds a claim for **that FQN**". Batch 4a recorded "the exclusion rule keys on claims", but the generator still uses `claimedBareNames` (R2 #3). The trail's rule is unmet today. |
| R2 #11(b) | A platform-root file that fails our parser is silently dropped (`PreludeGenerator.java:1443-1445`). | CONTRADICTS batch 2 (PROG §3 E) | "misses reported not skipped" (PROG l.400-406, batch 2 l.436). A silent `continue` survived. |
| R2 §2 design | The prelude emits PLATFORM_ROOTS ∪ STDLIB_ENGINE_ROOTS whole ("upstream's own core"), with no ownership logic. | **CONTRADICTS** the end state; pre-empts an OPEN decision | GEN row 8 itself, CD §3.1 (l.123-125), PROG step 7 and D7 (l.383-387): the prelude SHRINKS to boot vocabulary, and the stdlib arrives by manifest. Whole-stdlib-as-resource is the "2b" question, OPEN as EP D9 (l.337), to be ruled at C1. SPD §4 (l.110-113): engine packages are not prelude material, though `core_functions_*` is stdlib, so that is arguable. Also: the 174 → "about 7,685" figure (UOH §4.3, R2 §2a) counts ENGINE_SPEC_ROOTS, not STDLIB_ENGINE_ROOTS, so two root sets are conflated. |
| R2 §2 design | `withoutPlatformOwned` at boot, keyed on claimed names (RegistryKeys, CoreFn parse names). | **CONTRADICTS** the untangle | Step 4a (l.273): "a bodied declaration with an Intrinsic/Form row is a native call and lowers by its rule, never by its body". `DeclarationTable` merges twins into one declaration (step 2). 4c / W2.8 *delete* `isPlatformOwnedFunction` and the PCT-twin suppression. The PROG D ruling: no string identity, and "a string hack found on the path is FIXED… never added to". A boot filter keyed by name re-adds the proxy. If an interim filter is needed, key it on FunctionId through the ImplementationTable, or leave the exclusion in the generator until W2.8. |
| R2 §2 design | Engine shapes outside "core" come in through a hand `platform-shapes.tsv`, or a second module with a hand manifest, plus a test. | AGREES; ADDS the mechanism | PMH T1/T3 receipts (l.79-93); CD §3.1. The second-module variant is the D7 direction. |
| R2 risk 3 | The 6 hand enums in Pure.java (l.337-356) are upstream facts typed by hand. | AGREES | PMH phase 2 and §6a.4 (l.183-185): hand shapes survive only with a receipt naming Java that builds them before a model exists. |
| R2 §3 | engine-handlers.tsv emitted whole from Handlers.java; the declared filter moves to `EngineHandlers` class init; `undeclaredIds` moves to the test. | AGREES; builds on what is built | 4b.0 (a77197dab) built the generator. CD §4 (l.284) names the declared column as a row source; CD §5 gives engine input the surface. EP D13: handler order is registration order, not semantics, so E11's order sensitivity is not a semantic concern. |
| R2 #11(c) | Does the committed prelude feed `resolveAlongside`? | ADDS (open) | — |

### R3: DynaFn, imports, fixtures, manifest, vocab, ref_imports, census

| # | New homework item | Verdict | Against the trail |
|---|---|---|---|
| R3 §1.5 | DynaFn.java generated whole; a hand `DynaFnDecisions.java` holds the resolutions; joined at class init. | AGREES (tenets 4 and 5); ADDS as an interim | PROG §8 items 16-17. The end state is CD §4: resolution as rows of the one registry, with dialect renderings generated (CD §3.6). GEN says "registry rows later", which is consistent. |
| R3 §1.2 | The `Dialect` enum is an upstream fact typed by hand. | ADDS | A tenet-4 violation the trail did not catch. |
| R3 §1.5 | `DynaFnRegistryTest` never checks the member set against the checkout (`engineRoot()` unused). | CONTRADICTS the trail's claim | PROG §8 item 16: "`DynaFnRegistryTest` regenerates and verifies it against the checkout". Only the diff test does that. |
| R3 §2.3 | `CoreImports.java` as its own generated file; NameResolver keeps an alias. | AGREES; the landing deviated | PROG target `builtin/core-imports.*` GENERATED (l.114). b7504b407 spliced the list into NameResolver.java instead. `CoreImportsParityTest` stays (an existing test, kept). Misses the second sequence (see §5.3). |
| R3 §3-6 | gen_fixtures, gen_manifest, vocab and ref_imports are already upstream-only: narrow them and prove determinism. | ADDS (build hygiene); AGREES | PROG §3 A, §3 E; batch 2 (the fixture version inside the file). |
| R3 §7 | Split the pmcd census: the upstream reachability half becomes a bump report; our worklist goes on demand. | ADDS | The same split applies to `docs/protocol-roster.tsv` (see §5.4), which the new homework leaves unsplit. |

### GEN (GENERATORS.md) and UOH

| # | Item | Verdict | Against the trail |
|---|---|---|---|
| GEN rules 2-3 | Upstream records are generated only in the bump; a hash seal replaces the diff tests in the everyday gate; nothing everyday needs the archives except PCT and parser-equivalence. | AGREES with thesis sentence 2's "generated"; **CONTRADICTS PROG §5 and thesis sentence 2's "asserted byte-equal every run"** | PROG l.75-76, and the "six green checks" (l.477-486) put the parity tests and claim completeness in gate 3 on every chain. This is a deliberate amendment; record it in PROG. |
| GEN §2 option (a) | Membership signatures and AT_ groups move to their own generated class; Pure.java keeps Lite and the hand code. | AGREES | The PROG §2 target (generated text versus hand `Lite.java`), with the file names swapped. It still feeds membership (ours) to an upstream generator. GEN admits this as "one hand-owned input each" until W2.1, so it is an interim breach of its own rule 2. |
| GEN D2 | Retire native-claims.tsv? | DUPLICATES a trail decision | The ledger's header, the membership header and PROG step 5: it retires at untangle step 5, when "the implementation table is the surface". Caveat: `implementation-table.tsv` is not committed (`git ls-files`: absent). Retiring native-claims.tsv before step 5 loses the committed, reviewed-diff "implemented surface" (PROG thesis sentence 3; CLAIM §2a). It is ours, so it is correctly outside the bump. |
| GEN row 8 | The prelude "shrinks to what Java needs at boot". | AGREES with CD §3.1 and PROG step 7 | CONTRADICTS the UOH/R2 design, which widens it. |
| GEN row 11, §6 item 5 | "widen native_declarations to every upstream native (the catalog)". | **CONTRADICTS R1 and UOH** | R1 §0.4-0.5 and §1.A, and UOH l.56: the catalog must hold every upstream declaration, *upstream-native and upstream-bodied*. 524 of 794 platform-lowered rows are upstream-bodied, so an upstream-native-only catalog cannot serve Pure.java. |
| UOH §1 | The experiments (E1-E11) and determinism. | ADDS | No counterpart in the trail. |

---

## 4. Ambiguous or wrong uses of "native", with corrected wording

| Where | Text | Problem | Corrected wording |
|---|---|---|---|
| GEN l.26 | "people edit are inputs (native membership, …)" | Membership is platform-lowered membership; 524 of its 794 rows are upstream-bodied. | "the platform-lowered membership" |
| GEN l.40 (row 5) | "upstream's exact signature text for the natives we implement" | Two errors. "natives we implement" conflates the two axes. "exact" is wrong: R1 §0.1 shows the text is re-rendered. | "upstream's signature, re-rendered FQN-qualified by our parser, for every platform-lowered overload (upstream-native or upstream-bodied)" |
| GEN l.46 (row 11) | "upstream declarations of natives … widen to every upstream native (the catalog)" | Wrong. native_declarations already dumps both upstream cells at platform-lowered FQNs (R1 §0.5). The catalog needs every declaration. | "upstream declarations (upstream-native and upstream-bodied) at platform-lowered FQNs … widen to every upstream function declaration in the scanned roots (the catalog)" |
| GEN l.49 | "legend-lite's own natives" | Means `meta::legend::lite`, our platform-lowered inventions; there is no upstream cell. | "legend-lite's own platform-lowered functions (`meta::legend::lite`, the Lite constants)" |
| GEN l.145 (§6 item 5) | "native_declarations widened to every upstream native" | The same error as row 11. | "… widened to every upstream function declaration (upstream-native and upstream-bodied)" |
| BRIEF l.5 | "which natives we implement" | Conflates the axes. | "which upstream functions the platform lowers (platform-lowered membership)" |
| UOH l.9 | "R1: the natives and native_declarations" | Ambiguous (it means the generator). | "R1: gen_natives (the platform-lowered signatures) and native_declarations" |
| UOH l.33 | "there is no natives/prelude cycle" | Means the generators. | "there is no gen_natives ↔ gen_prelude cycle" |
| UOH l.56 | "(natives AND bodied functions, about 16.5k rows)" | Acceptable, but say both cells. | "(upstream-native AND upstream-bodied declarations, about 16.5k rows)" |
| UOH l.65 | "the natives catalog" | It is a catalog of all upstream function declarations. | "the upstream declaration catalog" |
| UOH l.70, l.73 | "**Natives:** does our key map…", "**Natives:** catalog resource size…" | Ambiguous. | "**Platform-lowered catalog:** …" |
| R1 l.17-19 | "bodied upstream `function`s are written as `native function`" | Correct, but frame it per PROG §0. | "upstream-bodied, platform-lowered functions are spelled `native function` in Pure.java (the keyword means platform-lowered; PROG §0)" |
| R1 l.75 | target/file name `native-catalog.tsv` for all ~16.5k declarations | A misnomer; it also collides with the retired `native-catalog.txt`, the old Pure.java snapshot (UBH §3c). | `upstream-declarations.tsv` (target `//spec:upstream_catalog`) |
| R1 l.87 | "natives only would fit a class" | Ambiguous. | "upstream-native rows only (372) would fit; the ~800 platform-lowered rows would too" |
| R1 l.120 | "Bodied functions declared `native` in Pure.java (524 rows) … 'natives only' is not enough" | Ambiguous. | "upstream-bodied, platform-lowered (524 rows) … an upstream-native-only catalog is not enough" |
| R2 l.27 | "66 respelled natives … 8 'engine natives not carried'" | Acceptable, but name the cell. | "66 upstream-native, not platform-lowered declarations carried respelled … 8 upstream-native engine declarations not carried" |
| R2 l.36 | "`native Class …`" | A THIRD meaning: `Pure.nativeClass`, the hand-declared bootstrap shapes (PMH §9.1 ruled prelude classes "NOT native"). | "hand-declared catalog shapes (`Pure.nativeClass` / `nativeEnum`)" |
| R2 l.81, l.93 | "the natives we implement", "membership of natives" | Conflates the axes. | "the platform-lowered functions (either upstream cell)", "platform-lowered membership" |
| R2 l.102 | "every class/enum/function/native" | Ambiguous. | "every class, enum, upstream-bodied function and upstream-native function declaration" |
| R2 l.108 | "catalog-FQN natives" | Ambiguous. | "upstream-native declarations at a platform-lowered FQN" |
| R2 l.136 | "358 of the filled FQNs are Pure.java natives" | Ambiguous. | "358 … are platform-lowered (Pure.java) FQNs" |
| target names `gen_natives`, `NativesGenerator`, `native-membership.tsv`, `native-claims.tsv`, `NativeFn` | (legacy names) | Each reads as "upstream-native" but holds platform-lowered entries. | Keep the names for continuity, but gloss them once. When files are split or renamed (GEN §6 item 4), prefer "platform-lowered" / "lowered" names. |

---

## 5. What the trail says about a self-contained bump that the new homework missed

1. **The verification tests that read upstream are not generators, and the seal cannot replace them.**
   - Tenet 4 has two halves: *generated* and *verified by a test that reads the same checkout* (CD l.92-94). The trail's verifiers are:
     - `CatalogUpstreamDiffTest` (DIVERGENT pinned 0);
     - `NativeSignatureGeneratorTest`;
     - `CoreImportsParityTest` (the pure-coreImport subset, which no diff test covers; R3 §2.3 notes it);
     - `PlatformNamesSpellingTest`;
     - `DynaFnRegistryTest`;
     - spec `ImplementationTableTest`;
     - `SpecBodyCensusTest`;
     - `ManifestWorldCensusTest`;
     - `UpstreamPathManifestTest`;
     - `SurfaceCensusTest` (the .g4 walk);
     - `CorpusManifestTest`.
   - GEN rule 3 takes the archives out of the everyday gate. These tests therefore must run in the bump's step 6, or they silently stop running.
   - GEN and UOH never list them. PROG §5's "six green checks" (l.477-486) assume they run on every chain.
2. **Upstream facts still typed by hand are not in the generator list:**
   - `PlatformTypes`' distinguished spellings, verified only by `PlatformNamesSpellingTest` (PROG §8 item 20; UBH §3g);
   - Pure.java's hand shapes (12 primitives, 6 enums, the m3 bootstrap handful).
   - Under the seal regime their only guard is that test, so it has to be a bump check (item 1).
3. **Two core import sequences, not one.**
   - EP W2.2 (l.673-676), W2.3b (l.707-710), D13 and CD §5 plan Pure source on legend-pure's `system::imports::coreImport` (29, m3.pure) and engine input on `CompileContext.META_IMPORTS` (32). The two orders differ (`CoreImportsParityTest` javadoc).
   - A self-contained `CoreImports.java` should generate both sequences, from m3.pure and from CompileContext.
   - The catalog and prelude generators should resolve upstream `.pure` files with the Pure-source group, not META_IMPORTS (this corrects R1 §1.A).
4. **Mixed ledgers with an upstream half, left unsplit:**
   - `docs/protocol-roster.tsv` (batch 6: "new tags = reviewed diff", PROG l.712-714). `//parser-equivalence:gen_roster` mixes the upstream tag list with our COVERED/UNCOVERED.
   - `docs/g4-keyword-snapshot.tsv`: upstream keywords plus our present/parsed-generic classification. `SurfaceCensusTest` is loud on additions and batch 2 added the shrink direction (PROG §2b l.154).
   - Both need the same split the homework gives the pmcd census: the upstream half sealed, our classification a registration.
5. **Grammar drift as a bump report.** The batch-8 lesson (PROG §8 item 28, l.753-754): "the drift tool must read GRAMMAR drift too (the .g4 diff between tags), not only file sets". GEN step 4's reports (items 9-11) do not include it, and `vocab` covers lexer tokens only.
6. **The engine library functions membership has an upstream-only form.**
   - The interim `ENGINE_LIBRARY_FUNCTIONS` row (`removeAll`, `PreludeGenerator.java:121-128`) is to be replaced by D7's namespace rule: every bodied `meta::pure::functions::` function, wherever it sits (PROG l.360-371). That is a rule over upstream content, not a list.
   - R2's "upstream core" (PLATFORM_ROOTS ∪ STDLIB_ENGINE_ROOTS) omits engine `core/pure/corefunctions`, where `removeAll` and 125 others live. Its blockers (26 boot-typing failures, the unowned `agg` form) are already measured.
7. **Ownership must not regress to names.** The trail already designed the boot-time answer to "upstream bodies beside platform-lowered twins":
   - `DeclarationTable` merges each pair into one id;
   - 4a's rule: the row decides, never the body;
   - W2.8 deletes `isPlatformOwnedFunction`.
   - An upstream-only prelude depends on W2.8 landing, or on an id-keyed interim. R2's name-keyed filter is not acceptable (PROG D rulings).
8. **The bump stops loudly.**
   - PROG §5 (l.468-470): the bump "stops loudly at the first generator that refuses (a changed signature, a construct the parser does not know) — fix the platform first, then re-run".
   - Moving parsing out of the generators (the header scanner, verbatim text resolved at boot) moves that stop into core's build or boot.
   - The seal design needs an explicit loud equivalent: pinned parse walls, plus the scanner ↔ SignatureMangle equality test (R1 risk 2).
9. **Ownership and timing.**
   - The large items are compiler-rebuild work: the catalog = W2.1, the prelude shrink = W4.4a under D9, registry rows = CD §4 / W5.1a.
   - That plan is parked (IN_FLIGHT 2026-10-04), and it forbids "an upstream pin bump … except as its own slice at a checkpoint" (EP §1a Risks, l.242).
   - D9 (whole stdlib or library-only) is OPEN until C1. R2's widened prelude decides it implicitly.
   - Every core edit (the split, the boot filter) needs the IN_FLIGHT announcement on main first.
10. **W2.1's boot-time aim.** The trail wants platform declarations loaded as a *serialized, pre-parsed* resource ("not 854 parses at class load"). R1 §1.C keeps parsing about 800 texts at clinit. The catalog redesign should either deliver W2.1's boot win or say why it defers it.
11. **Calibrated ratchets are a bump step with a rule.** UBH §5 Phases 4-5:
    - the ChannelB pins, gate 7 ceilings and census pins;
    - the skew and refusal ledgers keyed by upstream path;
    - the PCT expected-failure lists.
    Each is re-pinned with a reason, and "shrink-only means shrink": a pin the bump makes easier is ratcheted down in the same commit. GEN step 7 ("re-bless each moved expected result") should carry the shrink rule.
