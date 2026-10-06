# Making every bump input come from upstream alone: the homework (2026-10-05)

**Goal (the user):** the bump is self-contained. Every file derived from the pinned legend-engine and legend-pure is
generated from the pinned checkouts ALONE, and only the bump regenerates it. Our decisions live in hand-owned
registrations or hand code, never as these generators' inputs. This is `docs/COMPILER_DESIGN_2026_09_25.md`
tenets 4 and 5.

**Evidence** (in `docs/build-inventory/upstream-only/`):
- **R1:** the natives and native_declarations.
- **R2:** the prelude and the handler surface.
- **R3:** DynaFn, CORE_IMPORTS, the fixtures, manifest and vocab, ref_imports, and the reachability census.
- **experiments.txt:** 11 controlled experiments, each a one-line edit to OUR code, then all 11 upstream outputs
  regenerated and hash-compared with the baseline, then reverted.

Plus two checks:
- **Determinism:** all 11 outputs byte-identical after a from-scratch rebuild in a fresh output base (macOS;
  Linux and Windows to come in CI).
- **Line endings:** `.gitattributes` forces LF everywhere and keeps prelude.pure byte for byte (`-text`), so a seal's
  hashes hold on every platform.

## 1. What the experiments proved

| Edit to our code | Outputs that changed |
|---|---|
| E1 comment in NameResolver.java | generated NameResolver.java only (its generator copies the whole host file) |
| E2 comment in Pure.java | generated Pure.java only (host file copied) |
| E3 comment in an unrelated core file | none |
| E4 core code change naming a pure-side class absent from the prelude | none |
| **E4b core code naming an engine-side class absent from the prelude** | **prelude.pure** (the "Java demand" scan is a real input of ours) |
| E5 a membership row removed | none (a removal does not change an existing constant's text; an addition would) |
| E6 a new Lite constant | generated Pure.java only (host file copied) |
| E8 a DynaFn resolution changed | generated DynaFn.java only (carried through) |
| E9 a class removed from the committed prelude | **none: there is no natives/prelude cycle** (confirms R1, settles G1's open item) |
| E10 the first two CORE_IMPORTS swapped | none: those two decide no spelling (R1's claim needs a sharper test) |
| E11 one fqn blanked in the committed handler table | none: other `abs` rows carry the name (R3's claim needs a sharper test) |

So, measured rather than read:
- the host-file copies, which the split removes by construction;
- the prelude's Java scan;
- the resolutions carried through DynaFn.

A real code change elsewhere in core changes no upstream output (E3, E4).

## 2. Generator by generator: our inputs today, and the upstream-only design

| Generator | Our inputs that change bytes today | Upstream-only design | Size |
|---|---|---|---|
| `gen_fixtures` | none: core is a compile-time library only (R3) | already upstream-only; narrow its deps | small |
| `gen_manifest` | none: `Diagnostics.value` returns null in the action (R3) | already upstream-only; narrow | small |
| `vocab` | none: TokenDump finds no ANTLR vocabulary in our jars (R3) | already upstream-only; narrow | small |
| `ref_imports` | none (R3) | already upstream-only; sorted output (ref_dump's non-determinism, from unsorted children and anonymous ids, does not apply) | none |
| `pmcd_reachability_census` | our roster (`-Dpe.roster`) | commit the upstream half (reachability from PureModelContextData over the engine's protocol jars, sorted); our half becomes an on-demand worklist report (R3) | small |
| `gen_imports` | the host file NameResolver.java (E1) | a generated `CoreImports.java` from CompileContext alone; NameResolver keeps `CORE_IMPORTS = CoreImports.SEQUENCE` (R3) | small |
| `gen_dynafn` | the committed resolutions and Lite text (E8), the committed handler table's fqns, `Pure.SQL_*` (R3) | DynaFn.java generated whole from the engine tree (name, inference, dialects, and a generated `Dialect` enum). A hand `DynaFnDecisions.java` holds the resolutions and computes fqns at class init through `EngineHandlers.fqnsOf`. Every upstream name fits with no decision baked in. Call sites listed in R3. | medium |
| `gen_engine_handlers` | the filled-or-empty bit of the fqn column (Pure.all, the committed prelude, LITE_SURFACE) | emit upstream's table whole (name, id, derived fqn), with no core dependency. `EngineHandlers`' class init keeps a row's fqn only when the platform declares it, then appends the Lite surface. R2 measured the same `fqnsOf` for every name. | small to medium |
| `gen_natives` (+ `native_declarations`) | membership, the committed Pure.java as template (Lite constants and order shape the 488 overload groups), the committed CORE_IMPORTS, our parser and renderer (the text is RENDERED, not verbatim) (R1) | one upstream-only CATALOG resource: every upstream declaration (natives AND bodied functions, about 16.5k rows) with upstream's own signature id, verbatim header, file and imports, made by a small header scanner without our parser. Pure.java becomes hand-owned: `X = catalog("<id>")`; overload groups computed at class init; membership and its draft retire. native_declarations widened IS this catalog. | large |
| `gen_prelude` | the Java demand scan (E4b), Pure.java's hand enums, the claims and platform-owned exclusions (245 functions), our path lists and exclusion prefixes, our parser (R2) | emit legend-pure's platform roots plus the engine's core_functions roots, closed, with no ownership logic (upstream's own definition of "core"). Core filters platform-owned names at boot (`withoutPlatformOwned` beside `withoutSystemShadows`). The classes our Java names become a hand registration plus a test running today's scan (R2). | large |

## 3. What this means

- **Small, and clean now:** fixtures, manifest, vocab, ref_imports, the census split, CORE_IMPORTS.
- **Medium, clean and contained:** DynaFn (generated table plus a hand decisions class) and the handler surface
  (upstream table plus a filter at class init). Both move a decision from build time to class init, and both are
  proven equivalent by comparing `DynaFn.values()` and `fqnsOf` before and after.
- **Large:** the natives catalog and the prelude. Each is a redesign of how core gets its platform declarations, and
  is the compiler design's catalog and "what Java needs at boot" (§3.1) itself.

## 4. Still open, each with what settles it

1. **Natives:** does our key map one-to-one onto upstream's signature ids (which include the return type)? Count
   collisions over the 974 native_declarations rows. Does a header scanner reproduce `SignatureMangle` on every
   declaration? A test over all declarations settles it.
2. **Natives:** catalog resource size and loading in the WASM build (PreludeResources).
3. **Prelude:** the boot cost of the wider prelude: the engine side goes from 174 declarations to about 7,685.
   Time `Compiler.boot` before and after.
4. **Prelude:** parse walls in the new roots, and class-init order.
5. **CORE_IMPORTS spelling** (R1) and **the committed handler table's effect on DynaFn** (R3): sharper experiments
   (reorder an entry that decides a spelling; blank an fqn that appears once).
6. **Cross-platform determinism:** the same byte comparison in CI on Linux and Windows.
