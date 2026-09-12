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
 * CHANNEL B over the GRAMMAR suite (One-Platform Plan Phase 4, second
 * scope — the same runner over {@code platform/pure/grammar}): the
 * PCT.test functions channel A's GrammarFunctions suite runs, executed
 * by OUR platform alone, diffed against channel A's ledger and the
 * engine's relational-DuckDB manifest (the frontier oracle).
 */
class ChannelBGrammarTest {

    private static Path pureRoot() {
        return Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
    }

    @Test
    void grammarCensus() throws Exception {
        Path modelRoot = pureRoot().resolve(
                "legend-pure-core/legend-pure-m3-core/src/main/resources"
                        + "/platform/pure");
        // channel A's ReportScope is /platform/pure/grammar/functions/ —
        // grammar's OTHER subtrees (tests/, m3.pure …) belong to no
        // adapter suite
        Path scope = modelRoot.resolve("grammar/functions");
        java.util.List<String> walls = new java.util.ArrayList<>();
        List<ChannelB.Outcome> out = ChannelB.run(modelRoot,
                List.of(scope), walls);
        walls.forEach(w -> System.out.println("[chB-gram-wall] " + w));
        System.out.println("[chB-gram] walls=" + walls.size());
        // audit-of-Blocker-3: the ONE suite #12 missed — walls ASSERTED
        // shrink-only like its four siblings (20 measured 2026-08-21)
        assertTrue(walls.size() <= 20,
                "grammar walls grew: " + walls.size() + " > 20");
        Map<ChannelB.Status, Integer> census =
                new EnumMap<>(ChannelB.Status.class);
        for (ChannelB.Outcome o : out) {
            census.merge(o.status(), 1, Integer::sum);
            System.out.println("[chB-gram] " + o.status() + " " + o.testFqn()
                    + (o.detail().isEmpty() ? "" : " :: " + o.detail()));
        }
        System.out.println("[chB-gram] census=" + census
                + " total=" + out.size());
        ChannelBDiff.Counts c = ChannelBDiff.report("chB-gram", out,
                Path.of("src/test/java/org/finos/legend/lite/pct/"
                        + "Test_LegendLite_GrammarFunctions_PCT.java"),
                Path.of("src/test/resources/oracle/"
                        + "GrammarFunctions_manifest.duckdb.json"));
        // measured 2026-08-19 UNDER THE CLAUSE-2c REDESIGN (K-arm
        // verdicts; the parked seam-arm numbers are superseded), after
        // the two TRUE-wire-bug burns: the engine-verbatim empty-equality
        // ladder (nullSafeEqualsOperation, witness testEqualEmpty) and
        // the numList unwrap on collection sum/product (witness
        // testPlusNumber). Discovery exact; PASS grows-only; wire-bug
        // census shrinks-only; TRUE pinned at ZERO like essential.
        // 137 -> 136 (2026-09-10, upstream boundary batch 1): the SOURCE
        // pin moved from legend-pure 5.92.0+3 back to the 5.92.0 TAG; the 3
        // newer commits had added one PCT.test to
        // grammar/functions/boolean/equality/equal.pure (11 -> 10 at the
        // tag). Channel A's jar universe is 136 too — one universe.
        // 136 -> 137 at the 4.145.0 bump (batch 8): one PCT.test added
        // upstream; channel A's jar universe is 137 too
        assertTrue(out.size() == 137,
                "grammar discovery moved: " + out.size() + " != 137");
        // 128 (slice 11): letFn ×2 (inline multi-statement hoist),
        // testSingle{Plus,Minus}Type + OneToOne (is/assertIs World-1
        // identity: type refs canonicalized, instance provenance)
        // 130 (host-logic audit slice): Decimal literal-list arithmetic
        // folds to exact BINARY DECIMAL chains at emission (DuckDB list
        // aggregates run DOUBLE — probed)
        // 132 (2026-08-23 F13c): testEq/testEqualNonPrimitive — the
        // in-SQL eq/equal arm compiles the engine relation (identity
        // __id compare / key-tree canon) on the identity lane; BOTH
        // land as B-FIXES-A (channel A excludes them — identity was
        // unobservable on its value wire; ours carries it as data).
        // 133 (2026-08-23 F13b(a)): testMapRelationshipFromManyToMany
        // ERROR -> PASS (B-FIXES-A) — to-many property nav over a
        // collection now FLATTENS (pure collections never nest).
        // 135 (2026-08-27 leg 7b R0 + the disjoint-equality fold):
        // primitive-extension FQNs published to the resolver
        // (elementFqns), and instance-vs-primitive eq folds static
        // FALSE (InstanceEquality.staticallyDisjoint) — both
        // ExtendedInteger rows join
        // 136 (2026-08-27 fold-strategy closure): MapReduce's trees are
        // CLOSED TypedLambdas — cross-tree binding died, the inliner's
        // α-renaming reaches them uniformly; testPlusInIterate joins
        // 137 (2026-08-28 metamodel-store leg): getAll::testBasic —
        // Class.all() is a mapped-class query over the seeded
        // metamodel.classes table through the ordinary store lane
        // (METAMODEL_STORE_HANDOFF.md); the FULL grammar lane. Both
        // channels pass; the channel A ledger entry removed same-commit.
        // 137 -> 136 with the universe (above): 100% of 136
        assertTrue(c.pass() >= 136, "grammar PASS fell: " + c.pass());
        assertTrue(c.wireBug() <= 1,
                "grammar WIRE-BUG census grew: " + c.wireBug());
        assertTrue(c.trueWireBug() == 0,
                "a TRUE wire bug appeared (both oracles corroborate the"
                + " platform is wrong): " + c.trueWireBug());
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
        // 100 -> 35 BANKED DOWN (2026-08-22 X5): keyed-instance byte
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
