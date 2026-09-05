// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The NATIVE-CATALOG governance gate (invention audit 2026-08-14 §5/§6):
 * the grammar surface is oracle-checked every gate-8 run, but the
 * FUNCTION catalog was pinned only against its own golden file — a 20th
 * invented native could land by regenerating the golden. This test
 * closes that, and pins the USER-REACHABILITY PARTITION: every native
 * in the lite-internal package must be one of the three NAMED,
 * per-name-verified sets; the internal vocabulary must be spelled by
 * the {@link Pure.Lite} constants (each bound to a registered catalog
 * native — a typo'd constant cannot survive one run); and no internal
 * name may resolve from user bare-name text.
 */
class NativeCatalogGovernanceTest {

    @Test
    void everyLiteInternalNativeIsNamedAndVerified() {
        var unaccounted = Pure.liteInternalNatives().stream()
                .map(q -> q.substring(q.lastIndexOf("::") + 2))
                .filter(bare -> !Pure.INTERNAL_DESUGAR.contains(bare)
                        && !Pure.ENGINE_VOCAB_SHIMS.contains(bare)
                        && !Pure.LITE_SURFACE.contains(bare))
                .sorted().distinct().toList();
        assertEquals(List.of(), unaccounted,
                "lite-internal natives outside the verified sets — a new"
                        + " name needs the per-name upstream check"
                        + " (LITE_INVENTION_CENSUS.md) before it exists");
    }

    @Test
    void theVerifiedSetsOnlyShrink() {
        // 11 internal-desugar + 7 engine-vocabulary shims + 2 user-facing
        // surface natives, verified per-name 2026-08-14 (maxDate/minDate/
        // variantTo/percentileCont/percentileDisc deleted; traverse +
        // _Traversal deleted — navigate subsumed it; navigate and
        // sourceUrl reclassified LITE_SURFACE: both pinned from user
        // query text by dedicated tests — navigate has zero internal
        // emitters, sourceUrl is deliberately user-callable per
        // SourceUrlUserCallableTest). Growth is a NEW invention; shrink
        // is always allowed.
        // +1 2026-08-15: adjustTemporal — date::adjust semantics with the
        // LEGACY-print channel mark (engine extensionDefaults
        // mapToDBUnitType prints dateadd units UPPERCASE, the new
        // sqlDialectTranslation defaults lowercase; TemporalFrame stamps
        // milestoning window dates; census row in
        // docs/LITE_INVENTION_CENSUS.md).
        // 12→13 (multiplicity audit slice 3): trustOne — the SQL-lane
        // to-one conformance wrap, the C2 provenance split's synth
        // spelling (user toOne is CHECKED; synth trust is not).
        // 13→14 (2026-09-02, single-table hierarchies): unionScan — the
        // identity marker around a merged single-scan union body; the
        // resolver's "is a union" facts read the node kind where the
        // concatenate shape no longer exists (lowering is erasure).
        // 14→16 (batch 72b, 2026-09-05): asorPkValue / asorDecodePkMap —
        // the store-object-reference READERS: the reference decodes IN
        // SQL (base64 + framing regex); the resolver mints them where
        // the engine's objectReferenceIn collection is a runtime value
        // and where decodeObjectReferencesAndGetPkMap reads a frame.
        assertTrue(Pure.INTERNAL_DESUGAR.size() <= 16,
                "INTERNAL_DESUGAR grew: " + Pure.INTERNAL_DESUGAR);
        // +4 2026-08-16: lessThan/lessThanEqual/greaterThan/
        // greaterThanEqual Any-shims — engine DynaFunc ordering
        // comparisons carry UNTYPED operands (RelationalParseTreeWalker
        // Literal); routing them through pure's same-family ordering
        // overloads killed whole class mappings (ledger cluster 18;
        // notEqualAnsi precedent; census row updated).
        assertTrue(Pure.ENGINE_VOCAB_SHIMS.size() <= 11,
                "ENGINE_VOCAB_SHIMS grew: " + Pure.ENGINE_VOCAB_SHIMS);
        assertTrue(Pure.LITE_SURFACE.size() <= 2,
                "LITE_SURFACE grew: " + Pure.LITE_SURFACE);
    }

    @Test
    void everyLiteConstantIsARegisteredNative() throws Exception {
        // The Pure.Lite constants are the ONLY sanctioned spellings at
        // internal emit/match/registration sites; each must be a real
        // catalog FQN so a typo fails here, not at some runtime miss.
        for (var field : Pure.Lite.class.getDeclaredFields()) {
            if (field.getType() != String.class || field.getName().equals("PKG")) {
                continue;
            }
            String fqn = (String) field.get(null);
            assertTrue(!Pure.nativeFunctionsAt(fqn).isEmpty(),
                    "Pure.Lite." + field.getName() + " = " + fqn
                            + " is not a registered catalog native");
        }
    }

    @Test
    void internalVocabularyIsNotUserResolvable() {
        // The partition itself: a bare name is a query against the USER
        // namespace, and the internal vocabulary is not in it. Shim
        // names that collide with REAL pure functions (join, hash) keep
        // their real overloads and lose only the lite-package ones.
        var leaks = new TreeSet<String>();
        for (String bare : union(Pure.INTERNAL_DESUGAR, Pure.ENGINE_VOCAB_SHIMS)) {
            for (var def : Pure.nativeFunctionsAt(bare)) {
                if (def.qualifiedName().startsWith(Pure.Lite.PKG)) {
                    leaks.add(def.qualifiedName());
                }
            }
        }
        assertEquals(Set.of(), leaks,
                "lite-internal natives reachable from user bare-name text");
    }

    @Test
    void liteSurfaceStaysResolvable() {
        for (String bare : Pure.LITE_SURFACE) {
            assertTrue(!Pure.nativeFunctionsAt(bare).isEmpty(),
                    bare + " is user-facing lite product surface"
                            + " — it must stay bare-name resolvable");
        }
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        var u = new TreeSet<>(a);
        u.addAll(b);
        return u;
    }
}
