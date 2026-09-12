// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CHANNEL B over the STANDARD suite (One-Platform Plan Phase 4, third
 * scope — the FIRST multi-root scope): the standard functions live in
 * legend-ENGINE ({@code core_functions_standard}), importing the
 * legend-pure platform tree — {@code ChannelB.run}'s multi-root form
 * compiles the union. Diffed against channel A's ledger and the
 * engine's relational-DuckDB manifest (the frontier oracle).
 */
class ChannelBStandardTest {

    private static Path pureRoot() {
        return Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
    }

    private static Path engineRoot() {
        return Path.of(System.getProperty("legend.engine.root",
                System.getProperty("user.home") + "/legend/legend-engine"));
    }

    @Test
    void standardCensus() throws Exception {
        Path platform = pureRoot().resolve(
                "legend-pure-core/legend-pure-m3-core/src/main/resources"
                        + "/platform/pure");
        Path standard = engineRoot().resolve(
                "legend-engine-core/legend-engine-core-pure"
                        + "/legend-engine-pure-code-functions-standard"
                        + "/legend-engine-pure-functions-standard-pure"
                        + "/src/main/resources/core_functions_standard");
        java.util.List<String> walls = new java.util.ArrayList<>();
        List<ChannelB.Outcome> out = ChannelB.run(
                List.of(platform, standard), List.of(standard), walls);
        walls.forEach(w -> System.out.println("[chB-Standard-wall] " + w));
        System.out.println("[chB-Standard] walls=" + walls.size());
        // audit-of-audits #12: walls ASSERTED shrink-only (20 measured
        // 2026-08-21); growth silently shrinks the discovery universe
        assertTrue(walls.size() <= 20,
                "standard walls grew: " + walls.size() + " > 20");
        Map<ChannelB.Status, Integer> census =
                new EnumMap<>(ChannelB.Status.class);
        for (ChannelB.Outcome o : out) {
            census.merge(o.status(), 1, Integer::sum);
            System.out.println("[chB-std] " + o.status() + " " + o.testFqn()
                    + (o.detail().isEmpty() ? "" : " :: " + o.detail()));
        }
        System.out.println("[chB-std] census=" + census
                + " total=" + out.size());
        ChannelBDiff.Counts c = ChannelBDiff.report("chB-std", out,
                Path.of("src/test/java/org/finos/legend/lite/pct/"
                        + "Test_LegendLite_StandardFunctions_PCT.java"),
                Path.of("src/test/resources/oracle/"
                        + "StandardFunctions_manifest.duckdb.json"));
        // RE-MEASURED 2026-08-19 after the let-indirection adapter arm
        // un-declined the 64 window/non-identity rows (declines hide,
        // measurements name): PASS 133->180, and the previously hidden
        // tail is now the RECORDED burn queue — window frame semantics,
        // temporal-precision equality, columns(), chunk, INTEGER[]->
        // DOUBLE casts. TRUE pinned SHRINK-ONLY at measured (16); the
        // essential suite walked this exact arc to zero over five
        // slices.
        // 204 -> 205 at the 4.145.0 bump (batch 8): one PCT.test added
        // upstream; 205/205 PASS
        assertTrue(out.size() == 205,
                "standard discovery moved: " + out.size() + " != 205");
        // 100% (2026-08-19): the columns() compile-time fold burned the
        // reflection pair
        assertTrue(c.pass() >= 204, "standard PASS fell: " + c.pass());
        // 24→20→17→5→2 (slice 5: the ONE-OWNER print-form unification
        // restored the DateTime arm the tdsCell/pctCell drift had
        // dropped). Remaining: the columns() reflection pair only.
        assertTrue(c.wireBug() == 0,
                "standard WIRE-BUG census grew: " + c.wireBug());
        assertTrue(c.trueWireBug() == 0,
                "standard TRUE wire-bug census grew: " + c.trueWireBug());
        // V1 (OPEN_REGISTER): THE DUAL-VERDICT ALARM — the DB byte
        // verdict of record and the host-lattice referee may never
        // disagree silently; a disagreement fails the suite with the
        // census line (CANONICAL_FORM_SPEC §0, ratified design).
        assertTrue(com.legend.exec.CanonicalDivergence.sqlDisagreeCount() == 0,
                "DUAL-VERDICT DISAGREEMENT: "
                        + com.legend.exec.CanonicalDivergence.summary());

        // V6b (OPEN_REGISTER): the decline CEILING — the surviving
        // declines are DECLARED residue (class instances + wire-tree
        // containers, out of the byte channel's claimed domain per
        // CANONICAL_FORM_SPEC §4, + a handful of unrefinable Number
        // stamps). Shrink-only: a NEW undeclared decline family fails
        // here and must be claimed or declared.
        // 100 -> 45 BANKED DOWN (2026-08-22 X5): keyed-instance byte
        // verdicts (equality.Key canon — JSON framing, kind-tagged
        // leaves, Pair struct + List array carriers) and the Nil/empty
        // claim ('[]' canon unification) burned the class-instance and
        // empty-side buckets; the remainder is the NAMED boundary
        // (Map/mapEquals, Any wire trees, keyless classes,
        // mixed-identity F10, NUL literal).
        // BANKED DOWN 2026-08-22 F13: keyless classes CLAIMED
        // (identity as data, site-minted __id) — cumulative
        // PCT-lane declines 19 -> 13; residue = Any wire trees,
        // Pair unclaimable leaves, one canon-exec array shape,
        // mixed-kind, kind-gate.
        // 15 -> 5 BANKED DOWN (2026-08-23 F10 v1, the literal channel):
        // Any/mixed sides byte-compare in pure-literal spellings
        // (json_type-dispatched); Pair-of-Pairs claimed by
        // substitution-aware keys. Cumulative declines 13 -> 3 -> 2
        // (2026-08-23: letFn burned by the Any-root FIX-EMITTER —
        // scalarRoot boxes a judged-concrete non-JSON expr under an
        // Any/JSON label with TO_VARIANT; Bottom/Unknown never guess).
        // 3 -> 2 (2026-08-23 F13b(a)): map burned by the flatten fix.
        // 2 -> 0 (2026-08-23 F10 slice 2): mixedSort burned — mixed
        // collections ride the kind-faithful LITERAL carrier (byte
        // canon = the cell itself). ZERO declared residue: any new
        // decline (e.g. a COMPUTED mixed collection, which the
        // mixed-kind gate still guards) fails here and becomes a named
        // work item, never a silent count.
        assertTrue(com.legend.exec.CanonicalDivergence.sqlDeclinedCount() <= 0,
                "byte-verdict declines grew past the declared residue: "
                        + com.legend.exec.CanonicalDivergence.summary());
        // CONTRACT PROGRAM wire ratchets (adjudicated 2026-08-23,
        // shrink-only): DIVERGE = the true residue (hash UBIGINT,
        // percentile input-type, Number-erasure decimal delivery —
        // witnesses attached); ADOPT-PENDING = integer aggregates
        // whose CONTRACT widens at construction (testLargePlus rule).
        // 80 -> 75 (2026-08-23): hash UBIGINT family burned by the
        // dialect hashSigned conform (single owner; Lowerer's private
        // agg shift DELETED) — measured full-lane residue 74
        assertTrue(com.legend.exec.SqlTypeCensus.wireDivergeCount() <= 75,
                "wire divergence grew: "
                        + com.legend.exec.SqlTypeCensus.summary());
        assertTrue(com.legend.exec.SqlTypeCensus.wireAdoptPendingCount() <= 103,
                "wire adopt-pending grew: "
                        + com.legend.exec.SqlTypeCensus.summary());
        // TYPED-IR pin on THIS lane too (the corpus-runner pins do
        // not cover this JVM); the judge-vs-node pins retired WITH the
        // judge (parity pinned zero on every lane first).
        org.junit.jupiter.api.Assertions.assertEquals(0,
                com.legend.exec.SqlTypeCensus.mismatchCount(),
                "a label lie escaped reconciliation (the flip): "
                        + com.legend.exec.SqlTypeCensus.summary());



    }
}
