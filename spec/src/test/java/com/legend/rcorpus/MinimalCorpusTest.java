// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The minimal harness's run (docs/HARNESS_FROM_SCRATCH_AUDIT_2026_09_06.md):
 * every runnable corpus test through {@link MinimalCorpus}, two rosters
 * written ({@code target/corpus2-pass.txt}, {@code target/corpus2-fail.txt}
 * with the reason), one summary line. Scope with {@code -Drcorpus.test=
 * <substring>}.
 *
 * <p>THE PIN (Phase 0.1, 2026-09-08 — docs/END_TO_END_PLAN_2026_09_08.md
 * "THE ORDER (v2)"): the FAIL roster is a SET of test names per lane,
 * equal to a committed file ({@code rcorpus/duckdb-fail-roster.txt},
 * {@code rcorpus/h2-fail-roster.txt}); the denominator is pinned per lane.
 * A set is both floor and ceiling: a test that starts failing is LOST, a
 * test that starts passing is GAINED, and either fails the gate until the
 * roster file is changed with a written reason (docs/GATES.md). A count
 * (the pin until batch 125, {@code pass.size() >= floor}) let a red flip
 * hide behind a green one and let manufactured passes through unseen
 * (docs/HARNESS_AUDIT_2026_09_07.md §4.2). Under {@code -Drcorpus.test}
 * the same pin holds on the scoped subset: the scoped fails equal the
 * roster restricted to the tests that ran, and the scope must select at
 * least one test (a typo never reads green). Set difference is by NAME;
 * messages are printed, never compared.
 */
@Tag("heavy")
class MinimalCorpusTest {

    /** The roster files: one test FQN per line, sorted, no messages. */
    private static final String DUCKDB_ROSTER = "/rcorpus/duckdb-fail-roster.txt";
    private static final String H2_ROSTER = "/rcorpus/h2-fail-roster.txt";
    /** The SKIPPED rosters (Phase 0.3): tests whose program reaches no
     * verdict function and that adjudicated none — never a pass. */
    /** ACCEPTED DIVERGENCES (batch 144, USER-decided 2026-09-08): one row per
     * lane and test — {@code fqn ||| bucket ||| witness}; the failure must
     * carry the witness or the row is an ordinary FAIL (the divergence
     * changed: re-decide); a row that stops failing is GAINED (trim it). */
    private static final String DUCKDB_ACCEPTED = "/rcorpus/duckdb-accepted-roster.txt";
    private static final String H2_ACCEPTED = "/rcorpus/h2-accepted-roster.txt";
    private static final String DUCKDB_SKIPPED = "/rcorpus/duckdb-skipped-roster.txt";
    private static final String H2_SKIPPED = "/rcorpus/h2-skipped-roster.txt";
    /** The ORDER-LENIENCY registers (Phase 0.5): "ordered-keys-unmappable
     * <test>" per ORDERED row verdict that fell back to the multiset compare
     * because its sort keys were not derivable or not carried by the compared
     * output — 0 firings or every one registered (the arrival-order class,
     * unordered-leniency, is run-dependent and has a ceiling instead). */
    private static final String DUCKDB_ORD = "/rcorpus/duckdb-ord-register.txt";
    private static final String H2_ORD = "/rcorpus/h2-ord-register.txt";
    /** The UNORDERED-CHAIN registers (2026-09-10): "unordered-chain <test>"
     * per test whose row verdicts compare as multisets because its chain
     * has no sort — a compile-time fact of the query, the same set on every
     * platform. Replaces a CEILING on "unordered-leniency" firings, which
     * counted arrival-order coincidences (104–109 across runs and platforms)
     * and broke the first Windows CI run. Exact, like the ord register. */
    private static final String DUCKDB_UNORDERED = "/rcorpus/duckdb-unordered-register.txt";
    private static final String H2_UNORDERED = "/rcorpus/h2-unordered-register.txt";

    /** The denominator per lane (the ceiling's other half: a pass-count
     * jump is either a GAINED name or a bigger corpus, and both must be
     * explained). 2575 = 2721 declared − 146 excluded by the engine's own
     * stereotypes (audit §9); re-derived against a corpus scan in Phase
     * 0.8.
     *
     * <p>2721/146/2575 -> 2702/144/2558 on 2026-09-10 (upstream boundary
     * batch 1): the SOURCE pin moved from 4.137.0+36 — a non-tag commit 20
     * commits PAST the 4.138.2 tag — back to the 4.138.2 TAG itself (one
     * release: the checkout is now the same release as every oracle jar).
     * The 20 newer commits carried 19 declared tests (17 discovered, 2
     * excluded) that are not in 4.138.2; they return at the 4.145.0 bump
     * (homework §4b: +14 there, on top). A denominator move, not a
     * discovery change. */
    private static final int DISCOVERED = 2613;   // 4.145.0 bump (batch 8): +55 discovered,
                                                   // +59 declared, +4 excluded — the corpus
                                                   // gained 10 files (drift read: 18 tests + 6
                                                   // parameterised) and existing files grew
    /** 2702 {@code <<test.Test>>} functions declared, 144 excluded by the
     * engine's ToFix / ExcludeAlloy (Phase 0.8; the audit's census). */
    private static final int DECLARED = 2761;
    private static final int EXCLUDED = 148;

    /** Setups the platform derives as INERT on the full run (Phase 0.2;
     * measured 2026-09-08, the names print as {@code [corpus2] inert-setup}):
     * the five are zero-arg functions of the shared fixture that are not
     * setups at all (testRuntime, testRuntimeForBQ,
     * createTestDatabaseConnection, the two typeInference maps) — the
     * arity rule in {@code MinimalCorpus.sharedSetups} nominates them; the
     * platform's effect analysis is what keeps them from running. */
    private static final int INERT_SETUPS = 0;   // batch 134: shared setups are nominated by EFFECT, so none is inert

    @Test
    void corpus() throws Exception {
        Assumptions.assumeTrue(Corpus.available(), "legend-engine checkout not present");
        // the engine's scan order for the corpus goldens (the old runner's
        // setting); restored on exit so no later test in the JVM sees it
        String scanOrder = System.getProperty("legend.exec.engineScanOrder");
        System.setProperty("legend.exec.engineScanOrder", "true");
        try {
            run();
        } finally {
            if (scanOrder == null) {
                System.clearProperty("legend.exec.engineScanOrder");
            } else {
                System.setProperty("legend.exec.engineScanOrder", scanOrder);
            }
        }
    }

    private static void run() throws Exception {
        String only = System.getProperty("rcorpus.test", "").trim();
        final boolean TRACE = "1".equals(System.getProperty("rcorpus.trace"));
        MinimalCorpus corpus = new MinimalCorpus();
        for (String w : corpus.libraryWalls()) {
            System.out.println("[corpus2] library skipped: " + w);
        }
        for (String w : corpus.engineImplementationSkips()) {
            System.out.println("[corpus2] engine-implementation skipped: " + w);
        }
        // LOUD, NOT SILENT (upstream boundary batch 2): every named upstream
        // input resolved, and every exclusion key matched a file. A miss here
        // is a moved upstream path or a stale key — never absorbed.
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), corpus.missingInputs(),
                "upstream inputs the corpus could not find (a moved upstream file, or a"
                + " stale exclusion key) — fix the path; never let the model shrink silently");
        pinCensus(corpus.census());
        List<String> pass = new ArrayList<>();
        List<String> fail = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> accepted = new ArrayList<>();
        java.util.Map<String, String[]> acceptedRegister = readAccepted(
                MinimalCorpus.H2_BACKEND ? H2_ACCEPTED : DUCKDB_ACCEPTED);
        /** the strength census of the passes (Phase 0.7) */
        java.util.Map<String, Integer> strength = new java.util.LinkedHashMap<>();
        /** every test that RAN, in discovery order, pass or fail */
        List<String> ran = new ArrayList<>();
        java.util.Map<String, Long> elapsed = new java.util.LinkedHashMap<>();
        long t0 = System.nanoTime();
        try {
            for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
                if (!only.isEmpty() && !t.fqn().contains(only)) {
                    continue;
                }
                MinimalCorpus.Result r;
                long tStart = System.nanoTime();
                if (TRACE) {
                    // -Drcorpus.trace=1: name each test BEFORE it runs, so a
                    // run the JVM never returns from (StackOverflowError,
                    // a hang) still says which test it was in
                    System.out.println("[corpus2] run " + t.fqn());
                }
                try {
                    r = corpus.run(t);
                } catch (Exception e) {
                    r = new MinimalCorpus.Result(t.fqn(), MinimalCorpus.Status.FAIL, 0,
                            "harness: " + e.getClass().getSimpleName() + ": "
                                    + MinimalCorpus.whole(e.getMessage()));
                }
                ran.add(r.fqn());
                String[] acc = acceptedRegister.get(r.fqn());
                if (acc != null && r.status() == MinimalCorpus.Status.FAIL) {
                    r = r.reason().contains(acc[1])
                            ? new MinimalCorpus.Result(r.fqn(), MinimalCorpus.Status.ACCEPTED,
                                    r.verdicts(), acc[0] + " :: " + r.reason())
                            : new MinimalCorpus.Result(r.fqn(), MinimalCorpus.Status.FAIL,
                                    r.verdicts(), "accepted-divergence WITNESS MISSING ('"
                                    + acc[1] + "') — the divergence changed, re-decide :: "
                                    + r.reason());
                }
                if (r.status() == MinimalCorpus.Status.PASS) {
                    strength.merge(r.strength().name()
                            + (r.strength() == MinimalCorpus.Strength.DIFFERENTIAL
                                    ? (r.literalToo() ? "+literal" : "-only") : ""), 1, Integer::sum);
                }
                switch (r.status()) {
                    case PASS -> pass.add(r.fqn() + " :: " + r.reason());
                    case FAIL -> fail.add(r.fqn() + " :: " + r.reason());
                    case SKIPPED -> skipped.add(r.fqn() + " :: " + r.reason());
                    case ACCEPTED -> accepted.add(r.fqn() + " :: " + r.reason());
                }
                elapsed.put(r.fqn(), (System.nanoTime() - tStart) / 1_000_000L);
            }
        } finally {
            corpus.endSession();
        }
        Files.createDirectories(Path.of("target"));
        Files.write(Path.of("target/corpus2-pass.txt"), pass);
        Files.write(Path.of("target/corpus2-fail.txt"), fail);
        Files.write(Path.of("target/corpus2-skipped.txt"), skipped);
        System.out.println("[corpus2] pass=" + pass.size() + " fail=" + fail.size()
                + " skipped=" + skipped.size() + " of " + ran.size() + " in "
                + (System.nanoTime() - t0) / 1_000_000_000L + "s");
        for (String f : fail) {
            System.out.println("[corpus2] FAIL " + f);
        }
        for (String k : skipped) {
            System.out.println("[corpus2] SKIP " + k);
        }
        // the referee's own roster: row verdicts by kind and the decline
        // buckets — DISPLAYED, no verdict flows through it
        java.util.Map<String, Long> kinds = new java.util.TreeMap<>();
        com.legend.harness.H2Verify.VERDICT_ROSTER.forEach((k, v) ->
                kinds.merge(k.substring(0, k.indexOf(' ')), v.sum(), Long::sum));
        kinds.forEach((k, v) -> System.out.println("[corpus2] referee " + k + "=" + v));
        new java.util.TreeMap<>(com.legend.harness.ReplayOracle.OUTCOMES).forEach((k, v) ->
                System.out.println("[corpus2] referee-outcome " + k + "=" + v.sum()));
        new java.util.TreeMap<>(com.legend.harness.H2Verify.UNVERIFIABLE_CENSUS).forEach((k, v) ->
                System.out.println("[corpus2] referee-declined " + v.sum() + "x " + k));
        // the slowest tests (wall time includes the package session's setups
        // when this test opened it) — the timing ledger a slow run reads
        elapsed.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(15)
                .forEach(e -> System.out.println("[corpus2] slow " + e.getValue() + "ms " + e.getKey()));
        // setups the platform derived as inert (never ran): named, and
        // pinned exactly on the full run — Phase 0.2
        for (String s : corpus.inertSetups()) {
            System.out.println("[corpus2] inert-setup " + s);
        }
        System.out.println("[corpus2] inert-setups=" + corpus.inertSetups().size());
        if (only.isEmpty()) {
            org.junit.jupiter.api.Assertions.assertEquals(INERT_SETUPS, corpus.inertSetups().size(),
                    "inert setups (the platform says the body has no effects) moved:"
                    + " a seeding setup read as inert unseeds its package silently;"
                    + " explain, then re-pin. Names: " + corpus.inertSetups());
        }
        pinRoster(only, ran, fail, "fail",
                MinimalCorpus.H2_BACKEND ? H2_ROSTER : DUCKDB_ROSTER, true);
        pinRoster(only, ran, skipped, "skipped",
                MinimalCorpus.H2_BACKEND ? H2_SKIPPED : DUCKDB_SKIPPED, false);
        pinRoster(only, ran, accepted, "accepted",
                MinimalCorpus.H2_BACKEND ? H2_ACCEPTED : DUCKDB_ACCEPTED, false);
        java.util.Map<String, Integer> acceptedByBucket = new java.util.TreeMap<>();
        for (String a : accepted) {
            String bucket = a.substring(a.indexOf(" :: ") + 4);
            bucket = bucket.substring(0, bucket.indexOf(" :: "));
            acceptedByBucket.merge(bucket, 1, Integer::sum);
        }
        System.out.println("[corpus2] accepted divergences " + accepted.size()
                + " " + acceptedByBucket);
        // the referee's ORDER census (Phase 0.5): two tags, both COMPILE-TIME
        // facts of a verdict's chain, both pinned as EXACT sets — an ORDERED
        // chain whose sort keys the compared output could not carry, and an
        // UNORDERED chain (no sort: rows compare as a multiset). Until
        // 2026-09-10 the second was a CEILING on arrival-order coincidences
        // ("unordered-leniency": fired only when the two sides also arrived
        // in different orders — 104–109 across runs and platforms; Windows CI
        // hit 109 > 108). Tagging the chain makes it the same set everywhere.
        List<String> unmappable = new ArrayList<>();
        List<String> unorderedChains = new ArrayList<>();
        for (var e : com.legend.harness.H2Verify.ORD_CENSUS.entrySet()) {
            System.out.println("[corpus2] ord " + e.getKey() + " x" + e.getValue().sum());
            if (e.getKey().startsWith(ORD_UNMAPPABLE + " ")) {
                unmappable.add(e.getKey() + " :: x" + e.getValue().sum());
            } else if (e.getKey().startsWith(ORD_UNORDERED + " ")) {
                unorderedChains.add(e.getKey() + " :: x" + e.getValue().sum());
            }
        }
        System.out.println("[corpus2] ord-unordered-chains=" + unorderedChains.size());
        List<String> ranTagged = new ArrayList<>();
        List<String> ranTaggedUnordered = new ArrayList<>();
        for (String t : ran) {
            ranTagged.add(ORD_UNMAPPABLE + " " + t);
            ranTaggedUnordered.add(ORD_UNORDERED + " " + t);
        }
        pinRoster(only, ranTagged, unmappable, "ord",
                MinimalCorpus.H2_BACKEND ? H2_ORD : DUCKDB_ORD, false);
        pinRoster(only, ranTaggedUnordered, unorderedChains, "unordered",
                MinimalCorpus.H2_BACKEND ? H2_UNORDERED : DUCKDB_UNORDERED, false);
        pinChannels(only, corpus);
        pinStrength(only, strength);
    }

    /** Phase 0.7 — the STRENGTH census of the passes (audit §3's ladder),
     * derived from listener events; pinned MONOTONE per lane: the
     * differential count may only grow, the spelling-only and
     * cardinality-only counts may only shrink. */
    private static void pinStrength(String only, java.util.Map<String, Integer> strength) {
        strength.forEach((k, v) -> System.out.println("[corpus2] strength " + k + "=" + v));
        if (!only.isEmpty()) {
            return;
        }
        int differential = strength.getOrDefault("DIFFERENTIAL+literal", 0)
                + strength.getOrDefault("DIFFERENTIAL-only", 0);
        int spelling = strength.getOrDefault("SPELLING", 0);
        int weak = strength.getOrDefault("CARDINALITY", 0);
        int[] floor = MinimalCorpus.H2_BACKEND ? H2_STRENGTH : DUCKDB_STRENGTH;
        org.junit.jupiter.api.Assertions.assertTrue(differential >= floor[0],
                "differential passes (a referee row verdict matched) SHRANK: " + differential
                + " < " + floor[0] + " — a rows leg stopped being judged; explain or fix");
        org.junit.jupiter.api.Assertions.assertTrue(spelling <= floor[1],
                "spelling-only passes (every verdict decided by text) GREW: " + spelling
                + " > " + floor[1]);
        org.junit.jupiter.api.Assertions.assertTrue(weak <= floor[2],
                "cardinality-only passes GREW: " + weak + " > " + floor[2]);
    }

    /** {differential floor, spelling ceiling, cardinality ceiling} per lane
     * (Phase 0.7; measured 2026-09-08, batch 133). */
    // {1512, 49, 22} -> {1491, 45, 20} on 2026-09-10 (upstream boundary batch 1):
    // the SOURCE pin moved to the 4.138.2 TAG — 17 discovered tests left the
    // corpus (2575 -> 2558) and the 19 pre-#4900 goldens moved to the fail
    // roster (see DUCKDB_TEXT_DECIDED), taking their row verdicts with them:
    // differential 1512 -> 1491. The spelling (49 -> 45) and cardinality
    // (22 -> 20) ceilings shrank with the departed tests and are ratcheted
    // down in the same commit (shrink-only means shrink); they grow back,
    // with reasons, when the tests return at 4.145.0.
    // {1491, 45, 20} -> {1533, 49, 22} on 2026-09-12 (upstream boundary batch 8,
    // the 4.145.0 bump): the 19 #4900 goldens RETURNED from the fail roster with
    // their row verdicts (differential 1491 -> 1533, the floor follows the
    // measurement up), and the four modelJoins constant-join tests among them
    // are text-decided (rows-underivable), so spelling grows back 45 -> 49
    // and cardinality 20 -> 22 — exactly the shrink batch 1 recorded.
    // cardinality 22 -> 24 (feature-flag leg, 2026-09-12): the two returning
    // flag tests (testSubstringIndexingCorrectedByFeatureFlag,
    // testLegacyFlagProjectionEmitsPlainEquals) are the engine's own verdict
    // shape for flags — assert(planText->contains(...)), a boolean assert —
    // which this census counts as cardinality-only; measured, both lanes
    // cardinality 24 -> 25 (enum push-down in the plan channel, 2026-09-12):
    // testExecutionPlanGenerationForLambdaFromWithEnumMapping asserts
    // assert(planText->contains(...)), the engine's plan-test shape — a boolean
    // assert this census counts as cardinality-only; measured, both lanes
    private static final int[] DUCKDB_STRENGTH = {1533, 49, 25};
    // H2 1198 → 1279 / 18 → 19 (batch 135, Phase 1): the SourceSpelling pass and
    // the one-branch explode brought 114 H2 passes back — 81 of them differential;
    // one of the gained passes carries only cardinality asserts (a new pass, not a
    // weakened one), so that ceiling moves with it
    // cardinality 19 -> 22 (batch 142): the membership rewrite (EXISTS through
    // the non-null / compact carriers) made 9 query::filter::exists tests pass
    // on H2; their asserts are assertSize — the same 22 the DuckDB lane carries
    // {1279, 56, 22} -> {1264, 51, 20} on 2026-09-10 (upstream boundary batch 1):
    // the same denominator move as DUCKDB_STRENGTH above, measured on the H2 lane
    // {1264, 51, 20} -> {1378, 55, 22} on 2026-09-12 (batch 8): the same
    // return of the #4900 goldens on the H2 lane (see DUCKDB_STRENGTH)
    private static final int[] H2_STRENGTH = {1378, 55, 25};

    /** Phase 0.6 — the verdict CHANNELS the platform and the referee
     * reported: text-decided verdicts by the arm's reason (ceilings per
     * reason), referee FAULTS (pinned at ZERO — a fault of our own machinery
     * never stands in for a verdict and never hides in a decline count),
     * and the referee's leniencies (ceilings). Counts are TESTS, not
     * firings, where a test may fire several times. */
    private static void pinChannels(String only, MinimalCorpus corpus) {
        java.util.Map<String, Integer> byReason = new java.util.LinkedHashMap<>();
        for (String k : corpus.textDecided().keySet()) {
            String reason = k.substring(0, k.indexOf(' '));
            byReason.merge(reason, 1, Integer::sum);
            System.out.println("[corpus2] text-decided " + k);
        }
        byReason.forEach((r, n) -> System.out.println("[corpus2] text-decided-tests " + r + "=" + n));
        long faults = 0;
        for (var e : com.legend.harness.H2Verify.UNVERIFIABLE_CENSUS.entrySet()) {
            if (e.getKey().startsWith("FAULT ")) {
                faults += e.getValue().sum();
            }
        }
        System.out.println("[corpus2] referee-faults=" + faults);
        java.util.Map<String, Integer> lenTests = new java.util.LinkedHashMap<>();
        for (String k : com.legend.harness.H2Verify.LENIENCY_CENSUS.keySet()) {
            lenTests.merge(k.substring(0, k.indexOf(' ')), 1, Integer::sum);
        }
        for (var kind : List.of("golden-fanout-collapsed", "golden-stitch-keys-dropped")) {
            int n = (int) com.legend.harness.H2Verify.VERDICT_ROSTER.keySet().stream()
                    .filter(k -> k.startsWith(kind + " ")).count();
            if (n > 0) {
                lenTests.put(kind, n);
            }
        }
        lenTests.forEach((t, n) -> System.out.println("[corpus2] leniency-tests " + t + "=" + n));
        if (!only.isEmpty()) {
            return;
        }
        org.junit.jupiter.api.Assertions.assertEquals(0, faults,
                "referee FAULTS (our own machinery failed — a seed would not replay,"
                + " an extension function we ship is missing, the session failed) must"
                + " be ZERO: " + com.legend.harness.H2Verify.UNVERIFIABLE_CENSUS.keySet()
                        .stream().filter(k -> k.startsWith("FAULT ")).toList());
        java.util.Map<String, Integer> ceilings = MinimalCorpus.H2_BACKEND
                ? H2_TEXT_DECIDED : DUCKDB_TEXT_DECIDED;
        List<String> over = new ArrayList<>();
        byReason.forEach((r, n) -> {
            if (n > ceilings.getOrDefault(r, 0)) {
                over.add(r + "=" + n + " > " + ceilings.getOrDefault(r, 0));
            }
        });
        org.junit.jupiter.api.Assertions.assertTrue(over.isEmpty(),
                "text-decided verdicts grew past their ceilings (a rows leg stopped"
                + " being judged): " + over + " — explain, then re-pin");
        java.util.Map<String, Integer> lenCeil = MinimalCorpus.H2_BACKEND
                ? H2_LENIENCY : DUCKDB_LENIENCY;
        List<String> overLen = new ArrayList<>();
        lenTests.forEach((t, n) -> {
            if (n > lenCeil.getOrDefault(t, 0)) {
                overLen.add(t + "=" + n + " > " + lenCeil.getOrDefault(t, 0));
            }
        });
        org.junit.jupiter.api.Assertions.assertTrue(overLen.isEmpty(),
                "referee leniencies grew past their ceilings: " + overLen
                + " — explain, then re-pin");
    }

    /** Ceilings on TESTS with a text-decided verdict, per reason (Phase 0.6;
     * measured 2026-09-08, batch 132). */
    // foreign-dialect 30 -> 31 (batch 143): testSortQuotes's forAll over
    // DatabaseType->enumValues()->filter(in) now unrolls (the verdict source
    // reduces with the literal arms on); its assertEquals is a POSTGRES
    // SQL-text golden — a foreign dialect, text is the contract, one more
    // test the arm counts (it stays in the fail roster: TEXT-ONLY)
    // oracle-declined 22 -> 36 (DuckDB) / 28 -> 42 (H2), 2026-09-10 (upstream
    // boundary batch 1): the SOURCE pin moved from 4.137.0+36 back to the
    // 4.138.2 TAG, and engine commit 096e68735dd (#4900, null-safe equality,
    // merged AFTER the tag, in 4.139.0+) is no longer in the spec. The
    // platform implements #4900's semantics (IS NOT DISTINCT FROM); the
    // tag's goldens for executionPlanTest.pure's 15 optional-parameter
    // tests carry the PRE-#4900 `optionalVarPlaceHolderOperationSelector`
    // template, which the plan-text oracle DECLINES (unbound template
    // argument) — 14 land here, all 15 are on the fail rosters with this
    // reason. Shrinks back at the 4.145.0 bump (contains #4900).
    // oracle-declined 36 -> 22 (DuckDB) / 42 -> 28 (H2), rows-underivable 29 -> 28 /
    // 38 -> 36, 2026-09-12 (upstream boundary batch 8): the 4.145.0 spec contains
    // #4900 again, the 14 pre-#4900 goldens are row-judged once more, and the
    // ceilings shrink back to the measurement (shrink-only means shrink).
    private static final java.util.Map<String, Integer> DUCKDB_TEXT_DECIDED = java.util.Map.of(
            "rows-underivable", 28, "plan-params-unbindable", 6, "oracle-declined", 22,
            "foreign-dialect:DB2", 31, "foreign-dialect:Composite", 7);
    // H2 foreign-dialect 30 -> 31 (batch 143): the same testSortQuotes arm (see above)
    private static final java.util.Map<String, Integer> H2_TEXT_DECIDED = java.util.Map.of(
            "rows-underivable", 36, "plan-params-unbindable", 6, "oracle-declined", 28,
            "foreign-dialect:DB2", 31, "foreign-dialect:Composite", 7);
    /** Ceilings on TESTS with a referee leniency, per tag (Phase 0.6). */
    private static final java.util.Map<String, Integer> DUCKDB_LENIENCY = java.util.Map.of(
            "float-10-digits", 48, "micro-floor", 7,
            "golden-fanout-collapsed", 1, "golden-stitch-keys-dropped", 8);
    private static final java.util.Map<String, Integer> H2_LENIENCY = java.util.Map.of(
            "float-10-digits", 32, "micro-floor", 7,
            "golden-fanout-collapsed", 1, "golden-stitch-keys-dropped", 8);

    private static final String ORD_UNMAPPABLE = "ordered-keys-unmappable";
    private static final String ORD_UNORDERED = "unordered-chain";

    /** Phase 0.8 — the denominator RE-DERIVED: the model's triple
     * (declared / excluded / discovered) printed, pinned to the committed
     * constants, and cross-checked against an independent comment-stripped
     * text scan of the corpus tree (the audit's own census method). Three
     * readings of one fact; a discovery rule that drops a test, or a corpus
     * that grew, disagrees somewhere and is loud. */
    private static void pinCensus(MinimalCorpus.Census c) throws IOException {
        System.out.println("[corpus2] census declared=" + c.declared() + " excluded="
                + c.excluded() + " discovered=" + c.discovered());
        org.junit.jupiter.api.Assertions.assertEquals(c.declared() - c.excluded(), c.discovered(),
                "discovery dropped a test the model declares");
        org.junit.jupiter.api.Assertions.assertEquals(new MinimalCorpus.Census(DECLARED, EXCLUDED, DISCOVERED), c,
                "the corpus denominator moved (model reading): explain, then re-pin");
        org.junit.jupiter.api.Assertions.assertEquals(c, scanCensus(),
                "the model's census disagrees with the text scan of the corpus tree");
    }

    /** {@code <<test.Test>>} functions in the corpus tree, comments stripped
     * — the audit's method (roster-and-floor.md §1): each stereotype block
     * that precedes a function name and its parameter list. */
    private static MinimalCorpus.Census scanCensus() throws IOException {
        java.util.regex.Pattern block = java.util.regex.Pattern.compile(
                "<<([^>]*)>>\\s*(?:\\{[^}]*\\}\\s*)?[\\w:]+\\s*\\(");
        int declared = 0;
        int excluded = 0;
        try (java.util.stream.Stream<Path> walk = Files.walk(Corpus.RELATIONAL)) {
            for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).toList()) {
                String src = Files.readString(f)
                        .replaceAll("(?s)/\\*.*?\\*/", "")
                        .replaceAll("//.*", "");
                java.util.regex.Matcher m = block.matcher(src);
                while (m.find()) {
                    String st = m.group(1);
                    if (st.matches("(?s).*\\btest\\.Test\\b.*")) {
                        declared++;
                        if (st.matches("(?s).*\\b(ToFix|ExcludeAlloy)\\b.*")) {
                            excluded++;
                        }
                    }
                }
            }
        }
        return new MinimalCorpus.Census(declared, excluded, declared - excluded);
    }

    /** The pin: the {@code kind} names == the committed roster (restricted
     * to the tests that ran when scoped); the denominator when not scoped. */
    private static void pinRoster(String only, List<String> ran, List<String> rows,
            String kind, String resource, boolean denominator) throws IOException {
        String lane = MinimalCorpus.H2_BACKEND ? "h2" : "duckdb";
        List<String> roster = readRoster(resource);
        Set<String> failNames = new LinkedHashSet<>();
        for (String f : rows) {
            failNames.add(f.substring(0, f.indexOf(" :: ")));
        }
        Set<String> rosterNames = new HashSet<>(roster);
        Set<String> ranNames = new HashSet<>(ran);
        if (only.isEmpty() && denominator) {
            org.junit.jupiter.api.Assertions.assertEquals(DISCOVERED, ran.size(),
                    "[" + lane + "] the corpus denominator moved (" + ran.size()
                    + " tests ran, " + DISCOVERED + " pinned): a bigger or smaller"
                    + " corpus must be explained, never absorbed");
        } else if (denominator) {
            org.junit.jupiter.api.Assertions.assertFalse(ran.isEmpty(),
                    "[" + lane + "] -Drcorpus.test=" + only + " selected no test");
        }
        // LOST: failing now, not in the roster. GAINED: in the roster (and
        // ran), passing now. Both in discovery/roster order — no sort site.
        List<String> lost = new ArrayList<>();
        for (String f : failNames) {
            if (!rosterNames.contains(f)) {
                lost.add(f);
            }
        }
        List<String> gained = new ArrayList<>();
        for (String r : roster) {
            if (ranNames.contains(r) && !failNames.contains(r)) {
                gained.add(r);
            }
        }
        if (!lost.isEmpty() || !gained.isEmpty()) {
            StringBuilder sb = new StringBuilder("[" + lane + "] " + kind + " roster != "
                    + "committed roster (" + (only.isEmpty() ? "full run" : "scoped to '" + only + "'")
                    + "): LOST " + lost.size() + " (" + kind + " now, not in the roster)"
                    + ", GAINED " + gained.size() + " (in the roster, not " + kind + " now)."
                    + " Every change to the roster file carries a written reason"
                    + " in docs/GATES.md.");
            for (String l : lost) {
                sb.append("\n  LOST   ").append(l);
            }
            for (String g : gained) {
                sb.append("\n  GAINED ").append(g);
            }
            org.junit.jupiter.api.Assertions.fail(sb.toString());
        }
        System.out.println("[corpus2] roster " + lane + " EXACT: " + failNames.size()
                + " " + kind + " of " + ran.size() + (only.isEmpty() ? "" : " (scoped)")
                // USER DECISION 2026-09-08: the H2 lane is KEPT as a
                // PORTABILITY check — its golden runs on the same connection
                // as our query, so it is not an independent oracle; the
                // DuckDB lane with the H2 mirror is
                + (MinimalCorpus.H2_BACKEND ? " oracle=same-session" : " oracle=h2-mirror"));
    }

    /** The accepted-divergence register: {@code fqn -> {bucket, witness}}. */
    private static java.util.Map<String, String[]> readAccepted(String resource) throws IOException {
        java.util.Map<String, String[]> out = new java.util.LinkedHashMap<>();
        try (InputStream in = MinimalCorpusTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("accepted register missing on the classpath: " + resource);
            }
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                String s = line.trim();
                if (s.isEmpty()) {
                    continue;
                }
                String[] parts = s.split(" \\|\\|\\| ");
                if (parts.length != 3) {
                    throw new IllegalStateException("accepted register row needs"
                            + " 'fqn ||| bucket ||| witness': " + s);
                }
                out.put(parts[0], new String[]{parts[1], parts[2]});
            }
        }
        return out;
    }

    private static List<String> readRoster(String resource) throws IOException {
        try (InputStream in = MinimalCorpusTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("roster file missing on the classpath: " + resource);
            }
            List<String> out = new ArrayList<>();
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                String s = line.trim();
                int ann = s.indexOf(" ||| ");
                if (ann >= 0) {
                    s = s.substring(0, ann);   // the annotated registers: name first
                }
                if (!s.isEmpty()) {
                    if (!out.isEmpty() && s.compareTo(out.get(out.size() - 1)) <= 0) {
                        throw new IllegalStateException("roster " + resource
                                + " is not sorted-unique at: " + s);
                    }
                    out.add(s);
                }
            }
            return out;
        }
    }
}
