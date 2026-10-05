package com.legend.integration;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What legend-lite does NOT yet support, as executable statements rather than comments.
 *
 * <p>Every gap here was found by building the stress corpus, and each was previously
 * recorded only as an exclusion ({@link StressExclusions#EXCLUDED},
 * {@code the stress suites test's fail reasons (runtime binding)}) or as prose in
 * {@code docs/UPSTREAM_FINDINGS.md}. An exclusion tells you a thing is skipped; it does
 * not let you SEE the behaviour, and it does not tell you when the gap closes. This does
 * both: each case asserts the CURRENT behaviour, so implementing support makes the
 * corresponding test fail and forces the exclusion to be revisited.
 *
 * <h2>None of these is a parse failure</h2>
 * legend-lite parses all of them. They fail at model building (type resolution, mapping
 * kind) or later at query lowering. That distinction matters: a parse gap would show up in
 * the parser-equivalence suite, and these do not.
 */
@DisplayName("legend-lite gaps (executable, not commentary)")
class LegendLiteGapTest {

    private String reject(String src) {
        try {
            com.legend.Compiler.compileModel(src);
            return null;
        } catch (Throwable t) {
            Throwable root = t;
            while (root.getCause() != null && root.getCause() != root) root = root.getCause();
            return String.valueOf(root.getMessage()).replaceAll("\\s+", " ");
        }
    }

    private void accepts(String label, String src) {
        assertNull(reject(src), label + " is expected to be ACCEPTED at model build");
    }

    private void rejectsWith(String label, String fragment, String src) {
        String msg = reject(src);
        assertNotNull(msg, label + " now compiles — the gap has CLOSED. Remove it from "
                + "this test and from the exclusion that skips it.");
        assertTrue(msg.contains(fragment),
                label + " still fails but with a different message, so the reason for the "
                        + "exclusion may have changed: " + msg);
    }

    @Test
    @DisplayName("a Measure PARSES; it is the unit-typed property that cannot resolve")
    void measure() {
        // The declaration itself is fine — so calling this a "parse issue" is wrong.
        accepts("a Measure element", """
                Measure demo::Money { *USD: x -> $x; EUR: x -> $x * 1.09; }
                """);
        // The gap is that the unit type is never registered as a resolvable type.
        rejectsWith("a unit-typed property", "Unknown type", """
                Measure demo::Money { *USD: x -> $x; }
                Class demo::T { amount: demo::Money~USD[1]; }
                """);
    }

    @Test
    @DisplayName("cross-store and model-level association mappings are refused by kind")
    void associationMappingKinds() {
        // An honest refusal naming the construct, which is the right behaviour for a gap.
        rejectsWith("XStore", "kind Cross not supported", """
                ###Mapping
                Mapping demo::M ( demo::A_B: XStore { x: $this.p == $that.q } )
                """);
        rejectsWith("ModelJoin", "kind ModelJoin not supported", """
                ###Mapping
                Mapping demo::M ( demo::A_B: ModelJoin { {a: demo::A[1], b: demo::B[1]|$a.x == $b.y} } )
                """);
    }

    @Test
    @DisplayName("these compile but fail later, at query lowering — not gaps in the model")
    void acceptedAtModelBuildFailLater() {
        // Recorded so nobody looks for a model-build failure that is not there. Where each
        // one actually fails is asserted by the corpus itself:
        //   ModelChainConnection  -> the stress suites test's fail reasons (runtime binding) (runtime binding)
        //   orElse                -> docs/UPSTREAM_FINDINGS.md F7 (TypeInference, on use)
        //   isNotEmpty over [0..1] -> F8 (null-strict whitelist, on use)
        accepts("ModelChainConnection", """
                ###Connection
                ModelChainConnection demo::C { mappings: [ demo::M ]; }
                """);
        accepts("orElse in a derived property", """
                Class demo::T { a: Float[0..1]; b() { $this.a->orElse(0.0) } : Float[1]; }
                """);
        accepts("isNotEmpty in a derived property", """
                Class demo::T { a: Float[0..1]; b() { $this.a->isNotEmpty() } : Boolean[1]; }
                """);
    }

    @Test
    @DisplayName("BOOLEAN is rejected here as by legend-engine (F3 — CLOSED on main)")
    void booleanColumnType() {
        // F3 recorded legend-lite as MORE permissive: the stress corpus once shipped 194
        // BOOLEAN columns no other Legend engine could parse. Main's parser closed that
        // gap (the column datatype roster is the engine's), and the corpus was rewritten
        // to BIT before it merged — so the assertion flips: acceptance would be the
        // regression now.
        rejectsWith("BOOLEAN as a column type", "unsupported column datatype: BOOLEAN", """
                ###Relational
                Database demo::DB ( Table T (ID INTEGER PRIMARY KEY, FLAG BOOLEAN) )
                """);
        accepts("BIT, which is the type legend-engine actually defines", """
                ###Relational
                Database demo::DB ( Table T (ID INTEGER PRIMARY KEY, FLAG BIT) )
                """);
    }

    @Test
    @DisplayName("an M2M explosion (prop*:) is refused by the mapping normalizer")
    void m2mExplosion() {
        rejectsWith("an M2M explosion", "M2M explosion", """
                Class demo::Src { k: String[1]; parts: String[*]; }
                Class demo::Tgt { k: String[1]; part: String[0..1]; }
                ###Mapping
                Mapping demo::M ( demo::Tgt: Pure { ~src demo::Src k: $src.k, part*: $src.parts } )
                """);
    }

    @Test
    @DisplayName("an M2M local property colliding with a declared one is refused")
    void m2mLocalPropertyCollision() {
        rejectsWith("a colliding local mapping property", "local mapping property", """
                Class demo::Src { k: String[1]; }
                Class demo::Tgt { k: String[1]; localTag: String[0..1]; }
                ###Mapping
                Mapping demo::M ( demo::Tgt: Pure { ~src demo::Src k: $src.k, +localTag: String[0..1]: 'from-m2m' } )
                """);
    }

    @Test
    @DisplayName("every corpus exclusion names a gap this test covers")
    void exclusionsAreAccountedFor() {
        // Guards against an exclusion being added with no executable evidence behind it.
        // 70-surface-store2.pure left 2026-09-27: its precise-primitive gap (Varchar(200))
        // closed when legend-lite's lattice gained legend-pure's precise primitives
        Set<String> documented = Set.of("29-money.pure", "55-canonical-store.pure",
                "71-mapping-surface2.pure", "75-surface-gaps.pure");
        assertEquals(documented, StressExclusions.EXCLUDED.keySet(),
                "StressExclusions.EXCLUDED changed. Every excluded file must have a case in "
                        + "this test showing WHY legend-lite cannot load it — an exclusion "
                        + "without executable evidence is just a comment.");
    }
}
