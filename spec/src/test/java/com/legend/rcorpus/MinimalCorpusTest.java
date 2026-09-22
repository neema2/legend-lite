// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import com.legend.testing.Repo;
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
    /** The ENGINE-ORDER registers (leg 3.4 step 2, user ruling 2026-09-20):
     * "engine-order <test>" per test at least one of whose statements the
     * TEST-LANE scan-order emulation ({@code StableScanOrder}, behind
     * {@code legend.exec.engineScanOrder} — set here and nowhere in
     * product) CHANGED. Product SQL never carries an order it did not ask
     * for; the tests that lean on H2's insertion order are named here so
     * the test-only feature is never dropped by accident. Pinned EXACT per
     * lane and per judge mode (database mode sends one statement per body,
     * so the set of changed statements differs from host mode's). */
    private static final String DUCKDB_ENGINE_ORDER = "/rcorpus/duckdb-engine-order-register.txt";
    private static final String H2_ENGINE_ORDER = "/rcorpus/h2-engine-order-register.txt";
    private static final String DUCKDB_DATABASE_ENGINE_ORDER =
            "/rcorpus/duckdb-database-engine-order-register.txt";
    private static final String H2_DATABASE_ENGINE_ORDER =
            "/rcorpus/h2-database-engine-order-register.txt";
    private static final String ENGINE_ORDER = "engine-order";
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
        if ("database".equalsIgnoreCase(System.getProperty("legend.judge.mode", "host"))) {
            // a divergence the DATABASE verdict names and the host's kept
            // tolerance hides (homework §4j: the calendar goldens computed
            // in H2's decimal arithmetic) is accepted in database mode only
            acceptedRegister.putAll(readAccepted("/rcorpus/"
                    + (MinimalCorpus.H2_BACKEND ? "h2" : "duckdb")
                    + "-database-accepted-register.txt"));
        }
        /** the strength census of the passes (Phase 0.7) */
        java.util.Map<String, Integer> strength = new java.util.LinkedHashMap<>();
        /** every test that RAN, in discovery order, pass or fail */
        List<String> ran = new ArrayList<>();
        java.util.Map<String, Long> elapsed = new java.util.LinkedHashMap<>();
        /** the tests whose statements the test-lane scan-order emulation changed */
        List<String> engineOrder = new ArrayList<>();
        List<String> originRows = new ArrayList<>();
        List<String> shapeRows = new ArrayList<>();
        List<String> artifactRows = new ArrayList<>();
        List<String> hostComparedRows = new ArrayList<>();
        List<String> fallbackRows = new ArrayList<>();
        java.util.Map<com.legend.exec.StatementOrigin, java.util.Map<String, Long>> originTop = new java.util.EnumMap<>(com.legend.exec.StatementOrigin.class);
        long t0 = System.nanoTime();
        try {
            for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
                if (!only.isEmpty() && !t.fqn().contains(only)) {
                    continue;
                }
                MinimalCorpus.Result r;
                long tStart = System.nanoTime();
                long firingsBefore = com.legend.exec.Census.count(com.legend.exec.Census.Key.SCAN_ORDER_FIRINGS);
                long[] originsBefore = com.legend.exec.StatementOrigin.snapshot();
                int fallbacksBefore = com.legend.exec.Census.FALLBACK_REASONS.size();
                long flushesBefore = com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_FLUSHES);
                long hostDecidedBefore = com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_HOST_DECIDED);
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
                long[] originsAfter = com.legend.exec.StatementOrigin.snapshot();
                StringBuilder originRow = new StringBuilder(r.fqn());
                for (var o : com.legend.exec.StatementOrigin.values()) {
                    long d = originsAfter[o.ordinal()] - originsBefore[o.ordinal()];
                    originRow.append('\t').append(d);
                    if (d > 0) {
                        originTop.computeIfAbsent(o, k -> new java.util.HashMap<>()).merge(r.fqn(), d, Long::sum);
                    }
                }
                originRows.add(originRow.toString());
                String shape = corpus.bodyShape(r.fqn());
                String outside = outsideBody(originsBefore, originsAfter, shape,
                        com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_FLUSHES) - flushesBefore);
                if (outside != null) {
                    artifactRows.add(r.fqn() + " ||| " + outside);
                }
                long hostDecided = com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_HOST_DECIDED) - hostDecidedBefore;
                if (hostDecided > 0) {
                    hostComparedRows.add(r.fqn() + " ||| host-decided=" + hostDecided);
                }
                shapeRows.add(r.fqn() + "\t" + (shape == null ? "" : shape));
                for (int fi = fallbacksBefore; fi < com.legend.exec.Census.FALLBACK_REASONS.size(); fi++) {
                    fallbackRows.add(r.fqn() + "\t" + com.legend.exec.Census.FALLBACK_REASONS.get(fi));
                }
                long fired = com.legend.exec.Census.count(com.legend.exec.Census.Key.SCAN_ORDER_FIRINGS) - firingsBefore;
                if (fired > 0) {
                    engineOrder.add(ENGINE_ORDER + " " + r.fqn() + " :: x" + fired);
                }
            }
        } finally {
            corpus.endSession();
        }
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("corpus2-pass.txt"), pass);
        Files.write(Repo.out("corpus2-fail.txt"), fail);
        Files.write(Repo.out("corpus2-skipped.txt"), skipped);
        Files.write(Repo.out("corpus2-engine-order.txt"), engineOrder);
        // the per-test timing ledger, every test (ms, discovery order) — the
        // input a mode-vs-mode or run-vs-run time diff reads
        List<String> timing = new ArrayList<>();
        for (var e : elapsed.entrySet()) {
            timing.add(e.getValue() + "\t" + e.getKey());
        }
        Files.write(Repo.out("corpus2-elapsed.txt"), timing);
        // THE STATEMENT-ORIGIN CENSUS (2026-09-20): every statement sent this
        // JVM by where it came from, and per test — the north star is ONE
        // statement per body, so every origin but BODY is what is left outside it
        originRows.add(0, "test\t" + String.join("\t", java.util.Arrays.stream(
                com.legend.exec.StatementOrigin.values()).map(Enum::name).toList()));
        Files.write(Repo.out("corpus2-statement-origins.tsv"), originRows);
        // THE BODY-SHAPE CENSUS (block-compiler homework 2026-09-21): one letter per
        // statement (F frame let, L let, A assert, X assertError, E effect, O other)
        Files.write(Repo.out("corpus2-body-shapes.tsv"), shapeRows);
        Files.write(Repo.out("corpus2-fallbacks.tsv"), fallbackRows);
        int pure = 0;
        int effectful = 0;
        int interleaved = 0;
        int raising = 0;
        int maxAsserts = 0;
        int maxFrames = 0;
        for (String row : shapeRows) {
            String sh = row.substring(row.indexOf('\t') + 1);
            boolean eff = sh.indexOf('E') >= 0 || sh.indexOf('X') >= 0;
            if (eff) {
                effectful++;
            } else {
                pure++;
            }
            int firstA = sh.indexOf('A');
            if (firstA >= 0 && (sh.indexOf('E', firstA) >= 0 || sh.indexOf('X', firstA) >= 0)) {
                interleaved++;
            }
            if (sh.indexOf('X') >= 0) {
                raising++;
            }
            maxAsserts = Math.max(maxAsserts, (int) sh.chars().filter(c -> c == 'A').count());
            maxFrames = Math.max(maxFrames, (int) sh.chars().filter(c -> c == 'F').count());
        }
        System.out.println("[corpus2] body-shapes pure=" + pure + " effectful=" + effectful
                + " interleaved(effect-after-assert)=" + interleaved + " assertError=" + raising
                + " max-asserts=" + maxAsserts + " max-frames=" + maxFrames);
        System.out.println("[corpus2] body-compiler effect-statements-in-scripts=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.EFFECTS_IN_SCRIPT));
        java.util.Map<String, Integer> fallbackByReason = new java.util.TreeMap<>();
        for (String row : fallbackRows) {
            String reason = row.substring(row.indexOf('\t') + 1);
            fallbackByReason.merge(reason.length() > 90 ? reason.substring(0, 90) : reason, 1, Integer::sum);
        }
        fallbackByReason.forEach((k, v) -> System.out.println("[corpus2] fallback-reason " + v + " " + k));
        System.out.println("[corpus2] statement-origins " + com.legend.exec.StatementOrigin.census(
                com.legend.exec.StatementOrigin.snapshot()));
        for (var o : com.legend.exec.StatementOrigin.values()) {
            var per = originTop.getOrDefault(o, java.util.Map.of());
            if (per.isEmpty() || o == com.legend.exec.StatementOrigin.BODY) {
                continue;
            }
            long sum = per.values().stream().mapToLong(Long::longValue).sum();
            String top = per.entrySet().stream()
                    .sorted((x, y) -> Long.compare(y.getValue(), x.getValue())).limit(5)
                    .map(e -> e.getKey().replaceFirst("^meta::", "") + "=" + e.getValue())
                    .collect(java.util.stream.Collectors.joining(", "));
            System.out.println("[corpus2] statement-origin " + o.name().toLowerCase(java.util.Locale.ROOT)
                    + " tests=" + per.size() + " statements=" + sum + " top: " + top);
        }
        System.out.println("[corpus2] engine-order tests=" + engineOrder.size()
                + " (statements the test-lane scan-order emulation changed; product never opts in)");
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
        List<String> ranTaggedEngineOrder = new ArrayList<>();
        for (String t : ran) {
            ranTaggedEngineOrder.add(ENGINE_ORDER + " " + t);
        }
        boolean databaseMode = "database".equalsIgnoreCase(
                System.getProperty("legend.judge.mode", "host"));
        pinRoster(only, ranTaggedEngineOrder, engineOrder, ENGINE_ORDER,
                MinimalCorpus.H2_BACKEND
                        ? (databaseMode ? H2_DATABASE_ENGINE_ORDER : H2_ENGINE_ORDER)
                        : (databaseMode ? DUCKDB_DATABASE_ENGINE_ORDER : DUCKDB_ENGINE_ORDER), false);
        if (databaseMode) {
            Files.write(Repo.out("corpus2-outside-body.txt"), artifactRows);
            pinArtifactRegister(only, ran, artifactRows,
                    "/rcorpus/" + (MinimalCorpus.H2_BACKEND ? "h2" : "duckdb")
                            + "-database-outside-body-register.txt");
            // THE HOST-COMPARED REGISTER (2026-09-21): under the database judge, every test
            // with an assert decided WITHOUT a verdict row (a comparison in Java over two
            // database-computed sides — the lineage, TDG, identity and metadata arms) is a
            // named row; exact, shrink-only — the number that must reach zero
            Files.write(Repo.out("corpus2-host-compared.txt"), hostComparedRows);

            pinArtifactRegister(only, ran, hostComparedRows,
                    "/rcorpus/" + (MinimalCorpus.H2_BACKEND ? "h2" : "duckdb")
                            + "-database-host-compared-register.txt", "host-compared",
                    "an assert was decided outside a verdict row and its test is not registered");
        }
        pinChannels(only, corpus);
        pinStrength(only, strength);
        pinJudgeDifferential(only);
    }

    /** LEG 3.3 — THE DIFFERENTIAL GATE (docs/JUDGING_TWO_MODES_2026_09_17.md §4): a
     * DATABASE-mode lane that wrote its per-assert ledger ({@code legend.judge.ledger})
     * and was handed the HOST lane's ({@code legend.judge.ledger.host}) joins the two
     * per assert: the same verdict everywhere the registers do not name — unregistered
     * disagreements and one-sided adjudications pinned at ZERO; the database judge's
     * declines pinned by {@code rcorpus/<lane>-judge-unjudged-ceiling.txt}. */
    private static void pinJudgeDifferential(String only) throws java.io.IOException {
        String h = System.getProperty("legend.judge.ledger.host", "").trim();
        String d = System.getProperty(JudgeLedger.PROPERTY, "").trim();
        if (h.isEmpty() || d.isEmpty() || !only.isEmpty()
                || !"database".equalsIgnoreCase(System.getProperty("legend.judge.mode", "host"))) {
            return;
        }
        String lane = MinimalCorpus.H2_BACKEND ? "h2" : "duckdb";
        JudgeLedger.Differential x = JudgeLedger.diff(
                JudgeLedger.read(java.nio.file.Path.of(h)), JudgeLedger.read(java.nio.file.Path.of(d)));
        java.util.Set<String> registered = new java.util.HashSet<>();
        // "differential": a test that FAILS in both modes for the same reason but
        // whose per-assert ledgers differ in WHERE the failure lands — since one
        // statement per body (2026-09-20) a let's frame rides the fused statement,
        // so a broken frame fails at the assert in database mode and at the let in
        // host mode; named with its reason, never absorbed
        for (String r : List.of("lost", "gained", "accepted", "differential")) {
            registered.addAll(registerTests("/rcorpus/" + lane + "-database-" + r + "-register.txt"));
        }
        registered.addAll(registerTests(MinimalCorpus.H2_BACKEND ? H2_ACCEPTED : DUCKDB_ACCEPTED));
        List<String> unregistered = x.unregistered(registered);
        System.out.println("[judge-differential] " + lane + ": asserts agree=" + x.agree()
                + " disagree=" + x.disagree().size() + " unjudged-in-database=" + x.unjudged().size()
                + " host-only=" + x.hostOnly().size() + " database-only=" + x.databaseOnly().size()
                + " | unregistered=" + unregistered.size() + " | unjudged by family "
                + x.unjudgedByFamily());
        for (String u : unregistered) {
            System.out.println("[judge-differential] UNREGISTERED " + u);
        }
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), unregistered,
                "the host and database judges DISAGREE on an assert of a test no register names"
                + " (a verdict pair, or one judge adjudicating what the other never reached) — a bug"
                + " in one mode (host is the reference): fix it, or name the test on the lane's"
                + " register with its reason");
        java.nio.file.Path ceilingFile = java.nio.file.Path.of(
                "src/test/resources/rcorpus/" + lane + "-judge-unjudged-ceiling.txt");
        int ceiling = java.nio.file.Files.exists(ceilingFile)
                ? Integer.parseInt(java.nio.file.Files.readString(ceilingFile).trim()) : Integer.MAX_VALUE;
        org.junit.jupiter.api.Assertions.assertTrue(x.unjudged().size() <= ceiling,
                "asserts the database judge declines grew: " + x.unjudged().size() + " > " + ceiling
                + " — a shape the host judges and the database does not is a work item, never a"
                + " fallback (" + x.unjudgedByFamily() + ")");
        if (x.unjudged().size() < ceiling && java.nio.file.Files.exists(ceilingFile)) {
            System.out.println("[judge-differential] unjudged ceiling " + ceiling + " -> "
                    + x.unjudged().size() + " (re-pin: headroom is not a pin)");
        }
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
        if ("database".equalsIgnoreCase(System.getProperty("legend.judge.mode", "host"))) {
            // leg 3.1: database mode reports its strength against host mode's
            // floors; the differential gate (leg 3.3) pins it
            System.out.println("[corpus2] database-mode strength differential=" + differential
                    + " (host floor " + floor[0] + ") spelling=" + spelling + " weak=" + weak);
            return;
        }
        org.junit.jupiter.api.Assertions.assertTrue(differential >= floor[0],
                "differential passes (a referee row verdict matched) SHRANK: " + differential
                + " < " + floor[0] + " — a rows leg stopped being judged; explain or fix");
        org.junit.jupiter.api.Assertions.assertTrue(spelling <= floor[1],
                "spelling-only passes (every verdict decided by text) GREW: " + spelling
                + " > " + floor[1]);
        org.junit.jupiter.api.Assertions.assertTrue(weak <= floor[2],
                "cardinality-only passes GREW: " + weak + " > " + floor[2]);
    }

    // cardinality +1 per lane (store-substitution leg, 2026-09-13): the two
    // returning rows assert IDENTITY (assertIs over element rows, assertSize +
    // an element equality) — boolean verdicts by the engine test's own shape;
    // the rows' content is judged by MetamodelStoreSubstitutionTest's name
    // projections (a rows witness), so the weak class grows by the one test
    // whose every assert is an identity condition.
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
    // {1533, 49, 25} -> {1539, 44, 25} on 2026-09-12 (fixture on demand): nine
    // more tests per lane derive their rows (see DUCKDB_TEXT_DECIDED); the
    // differential floor follows the measurement up and the spelling ceiling
    // shrinks with the passes that are now row-judged — measured, both lanes
    // spelling 44 -> 46 (join order by first read + StrictDate plan
    // parameters, 2026-09-12): the two calendar plan rows pass by TEXT —
    // their fixture holds no calendar row for the plan's date, so the
    // referee's row verdict declines on an empty Allocation and the equal
    // text decides; new passes, not weakened ones — measured, both lanes
    // {1539, 46, 25} -> {1543, 53, 25} (the mapping-less plan arm, 2026-09-12):
    // the plan tests of that shape leave the LITERAL bucket (a plain literal
    // compare, never counted as text-decided) — seven are decided by text
    // (the same passes, now counted where they belong: LITERAL 862 -> 853,
    // SPELLING 46 -> 53) and four are judged by ROWS (differential 1539 ->
    // 1543, two of them new passes: testTwoMappingsOneRuntime ×2); measured
    // SPELLING 53 -> 60 (2026-09-17, the plan producer behind a helper): the
    // eleven helper-shaped plan asserts counted for the first time (see the
    // oracle-declined ceiling note) — seven of them are passes whose every
    // verdict is text-decided; the same passes, now counted where they belong
    // DuckDB differential 1543 -> 1020, spelling 60 -> 22 (2026-09-21, rung 2a + option 1):
    // see H2_STRENGTH — 523 passes witnessed by a referee row match are now decided by
    // the held text row (LITERAL); text-decided (SPELLING) passes fall the same way.
    // DuckDB cardinality 26 -> 27 (2026-09-22, views stage 4): testRelationStoreAccessorOnView
    // passes as ORDINARY compiled Pure — its two asserts are `assert($json->contains(…))` over
    // the executeLegendQuery result string, boolean verdicts by the engine test's own shape;
    // their content is the engine's activities SQL (byte-exact) and the engine's result JSON
    // (the serializer's own bytes), which the census cannot see behind a bare assert.
    private static final int[] DUCKDB_STRENGTH = {1020, 22, 27};
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
    // {1378, 55, 25} -> {1384, 50, 25} on 2026-09-12 (fixture on demand): the
    // same move on the H2 lane (see DUCKDB_STRENGTH)
    // spelling 50 -> 52 (the same two calendar plan rows on the H2 lane)
    // {1384, 52, 25} -> {1387, 60, 25}: the same bucket move on the H2 lane
    // (LITERAL 684 -> 675, SPELLING 52 -> 60, differential 1384 -> 1387)
    // H2 SPELLING 60 -> 67 (2026-09-17, the plan producer behind a helper): the
    // same seven helper-shaped plan asserts as the DuckDB lane — passes whose
    // every verdict is text-decided, counted where they belong
    // H2 differential 1387 -> 953 (2026-09-21, block-compiler rung 2a + the user's
    // option 1): a text assert whose text is byte-equal to the golden IS the verdict
    // (the engine's own) and the referee is the APPEAL on a failed text only — 434
    // passes that were witnessed by a referee row match are now decided by the text
    // row itself (LITERAL strength). A rows leg did not stop being judged: it is no
    // longer needed for a held text. The DuckDB floor moves the same way.
    // H2 cardinality 26 -> 27 (2026-09-22, views stage 4): the same pass on H2 — the envelope
    // is plain string building, no JSON function; see DUCKDB_STRENGTH's note.
    private static final int[] H2_STRENGTH = {953, 22, 27};

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
        corpus.fixturesProvided().forEach((k, setup) ->
                System.out.println("[corpus2] fixture-on-demand " + k + " <- " + setup));
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
        // leg 3.0 (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4): the SQL
        // canon's CLAIM / DECLINE census per assert family and reason, the
        // host-vs-byte disagreements (database mode's bug list now that
        // host is the verdict of record), the policy counts. Printed only.
        com.legend.exec.CanonicalDivergence.sqlCensus().forEach(
                (k, v) -> System.out.println("[corpus2] sql-census " + k + "=" + v));
        System.out.println("[corpus2] sql-census policy ulp="
                + com.legend.exec.CanonicalDivergence.sqlUlpPolicyCount()
                + " tdsnull=" + com.legend.exec.CanonicalDivergence.sqlTdsNullPolicyCount()
                + " decimal-scale-only=" + com.legend.exec.CanonicalDivergence.decimalScaleOnlyCount()
                + " disagree=" + com.legend.exec.CanonicalDivergence.sqlDisagreeCount()
                + " declined=" + com.legend.exec.CanonicalDivergence.sqlDeclinedCount()
                + " wire-retyped=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.WIRE_RETYPED)
                + " wire-slot-skew=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.WIRE_SLOT_SKEW)
                + " batch-statements=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_FUSED)
                + " batch-fallbacks=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.VERDICT_FALLBACKS)
                + " frames[" + com.legend.exec.VerdictBatch.frameCensus() + "]");
        System.out.println("[corpus2] sql-census round-trips="
                + com.legend.exec.Census.count(com.legend.exec.Census.Key.SQL_ROUND_TRIPS) + " (every statement the executor"
                + " sent this JVM: setups, sides, frames, referee replays)"
                + " sql-chars=" + com.legend.exec.Census.count(com.legend.exec.Census.Key.SQL_CHARS)
                + " detaches=" + DuckWorkspaces.DETACHES.get()
                + " detach-ms=" + DuckWorkspaces.DETACH_NANOS.get() / 1_000_000L
                + " detach-max-ms=" + DuckWorkspaces.DETACH_MAX_NANOS.get() / 1_000_000L);
        com.legend.exec.CanonicalDivergence.sqlDisagreeSamples().forEach(
                r -> System.out.println("[corpus2] sql-disagree " + r.family() + " " + r.detail()));
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
    // rows-underivable 28 -> 19 (DuckDB) / 36 -> 27 (H2), oracle-declined 22 -> 27 /
    // 28 -> 33, 2026-09-12 (corpus-zero program, FIXTURE ON DEMAND): the rows leg
    // seeds the store a golden's mapping reads when exactly one corpus fixture
    // seeds it and no already-run setup shares a table (four stores per lane:
    // datePeriods myDB, modelToModel relationalDB, sqlFunction myDB,
    // contractmoneyscenario db), so nine more tests per lane DERIVE their rows;
    // five of those the referee then declines for reasons it already models
    // (an allocation with no fixture row, unformatted plan text, the golden's
    // productSchema missing on the referee, datediff-to-now), which is a
    // more precise decline than "underivable" and moves the count between
    // the two reasons; the other four are judged by rows (one passes:
    // testTemporalDateVariableInFunctionExpressionWithPropagation).
    // rows-underivable 19 -> 24 (DuckDB) / 27 -> 33 (H2), oracle-declined 27 -> 28 /
    // 33 -> 34, 2026-09-12 (corpus-zero cluster B, the MAPPING-LESS plan arm):
    // plain assertEquals over executionPlan(lambda, extensions) text now routes
    // to the plan arm (rows first) instead of a plain literal compare, so the
    // tests of that shape are COUNTED for the first time: five read stores no
    // fixture seeds (the plan tests' own Firm / SPerson tables — fixture on
    // demand finds no unique seeder) and decline as underivable, one hits the
    // in-list temp table the referee lacks; all six still pass by their equal
    // text, and two more (testTwoMappingsOneRuntime ×2) pass by ROWS
    // oracle-declined 28 -> 39 (DuckDB), 2026-09-17 (the plan producer BEHIND
    // A HELPER: the plan arm now looks through a user function that builds
    // the query and the plan — the platform's inliner, parameters bound to
    // the call's arguments): eleven helper-shaped plan asserts reach the arm
    // for the first time. Ten are the relationalMapper family, whose goldens
    // name database-mapper-rewritten schemas the referee's mirror does not
    // hold (SNDB, SNDBDEFAULT, PRODUCTSCHEMANEWDBINC), one hits a template
    // operation the plan text does not model (renderCollectionWithTz); all
    // eleven were host TEXT compares before, uncounted, and keep the same
    // outcome. The five datetime plan rows of the same shape are now judged
    // by ROWS and pass (they lost their text match to the boundary cast).
    // foreign-dialect:Composite 7 -> 8 (2026-09-21, block-compiler rung 1 — the batch
    // flushes only before an EFFECT): sqlstring::testSqlGenerationDivide_AllDBs fails on
    // its first assert as before; the old rule raised at the flush before its next let,
    // now its later Composite text assert is evaluated before the body's end raises
    // the same first failure — one more text-decided verdict, the same verdict.
    // foreign-dialect:Composite 8 -> 9 (2026-09-21, task #14 leg 2 — text-decided
    // declines are verdict ROWS): tds::sort::testSortQuotes loops over drivers; its DB2
    // text assert used to raise in Java at the assert, now the row is judged at the
    // flush, so the later Composite text assert is reached and counted. The DB2 row
    // fails at the flush as before (fail rosters unchanged).
    private static final java.util.Map<String, Integer> DUCKDB_TEXT_DECIDED = java.util.Map.of(
            "rows-underivable", 24, "plan-params-unbindable", 6, "oracle-declined", 39,
            "foreign-dialect:DB2", 31, "foreign-dialect:Composite", 9);
    // H2 foreign-dialect 30 -> 31 (batch 143): the same testSortQuotes arm (see above)
    // H2 oracle-declined 34 -> 45 (2026-09-17): the same eleven helper-shaped
    // plan asserts as the DuckDB lane (the plan producer behind a helper),
    // counted for the first time on this lane too
    // H2 rows-underivable 33 -> 26 and oracle-declined 45 -> 48 (2026-09-19, judging
    // H2 quick wins): H2 now spells epoch(ts) as EXTRACT(EPOCH FROM ts) and
    // regexp_extract as REGEXP_SUBSTR, so seven rows-underivable statements EXECUTE
    // — four are judged by ROWS now (host roster -3, view rows; one distinct row),
    // three (sqlstring dateDiff-to-now, Hours/Minutes/Seconds) reach the referee's
    // datediff-to-now arm and decline the replay as two instants (text-decided by
    // a different reason, the same outcome). rows-underivable shrinks (shrink-only).
    // H2 rows-underivable 26 -> 27 (2026-09-20, one statement per body — measured
    // identical on 65b71fc83 and on the rung-12 tree): stringToDate::
    // testToSQLStringconvertToDateinH2UserDefinedFormat fails on BOTH judges (our H2
    // parsedatetime spells 'MMMyyyy' over "Nov1995" without the engine's concat('01', …)
    // day prefix — a product row on the H2 roster). Host mode fails at the let's eager
    // run; database mode no longer runs the let, so the same DataError surfaces in the
    // SQL-text referee's rows leg and is counted here. Same failure, later stage.
    // H2 foreign-dialect:Composite 7 -> 8 (2026-09-21, rung 1): the same
    // testSqlGenerationDivide_AllDBs row as the DuckDB lane — its later Composite text
    // assert now evaluates before the body's end raises the same first failure.
    // H2 foreign-dialect:Composite 8 -> 9 (2026-09-21, leg 2): the same testSortQuotes
    // row as the DuckDB lane (its DB2 text is a verdict row judged at the flush).
    private static final java.util.Map<String, Integer> H2_TEXT_DECIDED = java.util.Map.of(
            "rows-underivable", 27, "plan-params-unbindable", 6, "oracle-declined", 48,
            "foreign-dialect:DB2", 31, "foreign-dialect:Composite", 9);
    /** Ceilings on TESTS with a referee leniency, per tag (Phase 0.6). */
    // float-10-digits 48 -> 49 (DuckDB) / 32 -> 33 (H2), 2026-09-12 (fixture on
    // demand): one of the newly row-judged tests compares a float column
    private static final java.util.Map<String, Integer> DUCKDB_LENIENCY = java.util.Map.of(
            "float-2ulp", 23, "micro-floor", 7,
            "golden-fanout-collapsed", 1, "golden-stitch-keys-dropped", 8);
    private static final java.util.Map<String, Integer> H2_LENIENCY = java.util.Map.of(
    // float-10-digits 33 -> 34 (2026-09-17): one of the twelve group-by/average
    // rows the H2 lane gained (H2AvgDelivers: H2's DECFLOAT average cast to
    // DOUBLE on its own wire) is judged with the H2 float leniency
            // float-10-digits 34 -> 36 (2026-09-19, bucket 8): twelve H2 host-roster
            // rows execute now that the float canon's exponent cast no longer folds
            // to CAST('' AS INTEGER) over a constant golden cell; two of them pass
            // through the referee's bounded float tolerance (H2's DOUBLE print)
            "float-2ulp", 5, "micro-floor", 7,
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
        if ("database".equalsIgnoreCase(System.getProperty("legend.judge.mode", "host"))) {
            // leg 3.1: DATABASE mode is judged AGAINST host mode's roster —
            // the differential is printed (LOST = fails in database mode
            // only; GAINED = passes in database mode only). The FAIL
            // differential is PINNED per lane (P0, homework §4h): every
            // lost and every gained test is a NAMED register row with its
            // reason; a new lost row is red, a register row no longer lost
            // is red (shrink the register, with the reason in GATES.md).
            // The differential gate (leg 3.3) refines this to per-assert.
            System.out.println("[corpus2] database-mode " + kind + " diff vs host roster:"
                    + " lost=" + lost.size() + " gained=" + gained.size());
            for (String l : lost) {
                System.out.println("[corpus2] database-mode LOST " + l);
            }
            for (String g : gained) {
                System.out.println("[corpus2] database-mode GAINED " + g);
            }
            if (kind.equals("fail")) {
                pinDifferential(lane, "lost", lost, ranNames,
                        "/rcorpus/" + lane + "-database-lost-register.txt");
                pinDifferential(lane, "gained", gained, ranNames,
                        "/rcorpus/" + lane + "-database-gained-register.txt");
            }
            return;
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

    /** THE ARTIFACT RULE (block-compiler program, 2026-09-21): a test body is ONE
     * artifact — one fused statement for a pure body, one script for an effect
     * body. This names what is not there yet: a body that sends a PRODUCT-OWNED
     * statement outside its artifact (raw, side, statement, tdg, probe, fallback,
     * let, value, other — never the referee's, the seeding's or the session's), or
     * a PURE body cut into several sends (flushes). Null when the body is one
     * artifact. */
    private static @com.legend.Nullable String outsideBody(long[] before, long[] after,
            @com.legend.Nullable String shape, long flushes) {
        StringBuilder sb = new StringBuilder();
        for (var o : com.legend.exec.StatementOrigin.values()) {
            long d = after[o.ordinal()] - before[o.ordinal()];
            if (d > 0 && PRODUCT_OWNED.contains(o)) {
                sb.append(sb.length() == 0 ? "" : " ").append(o.name().toLowerCase(java.util.Locale.ROOT))
                        .append('=').append(d);
            }
        }
        // a PURE body cut into several sends (a send over several connections is
        // one send: a body that asserts over the metamodel AND the session
        // cannot be one statement, and is not split)
        boolean pure = shape != null && shape.indexOf('E') < 0 && shape.indexOf('X') < 0;
        if (pure && flushes > 1) {
            sb.append(sb.length() == 0 ? "" : " ").append("flushes=").append(flushes);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static final java.util.Set<com.legend.exec.StatementOrigin> PRODUCT_OWNED = java.util.EnumSet.of(
            com.legend.exec.StatementOrigin.RAW, com.legend.exec.StatementOrigin.SIDE,
            com.legend.exec.StatementOrigin.STATEMENT, com.legend.exec.StatementOrigin.TDG,
            com.legend.exec.StatementOrigin.PROBE, com.legend.exec.StatementOrigin.FALLBACK,
            com.legend.exec.StatementOrigin.LET, com.legend.exec.StatementOrigin.VALUE,
            com.legend.exec.StatementOrigin.OTHER);

    /** The OUTSIDE-BODY register, exact per lane in database judge mode: every
     * test not yet one artifact is a named row; a NEW name is red (a body
     * regressed), a STALE name is red (a leg made it one artifact — shrink the
     * register with the reason in docs/GATES.md). The register can only shrink. */
    private static void pinArtifactRegister(String only, List<String> ran, List<String> rows,
            String resource) throws IOException {
        pinArtifactRegister(only, ran, rows, resource, "outside-body",
                "a body sends a product-owned statement outside its artifact and is not registered");
    }

    private static void pinArtifactRegister(String only, List<String> ran, List<String> rows,
            String resource, String kind, String newMeans) throws IOException {
        String lane = MinimalCorpus.H2_BACKEND ? "h2" : "duckdb";
        Set<String> registered = registerTests(resource);
        Set<String> ranNames = new HashSet<>(ran);
        Set<String> now = new LinkedHashSet<>();
        for (String row : rows) {
            now.add(row.substring(0, row.indexOf(" |||")));
        }
        List<String> fresh = new ArrayList<>();
        for (String n : now) {
            if (!registered.contains(n)) {
                fresh.add(n);
            }
        }
        List<String> stale = new ArrayList<>();
        for (String n : registered) {
            if (ranNames.contains(n) && !now.contains(n)) {
                stale.add(n);
            }
        }
        System.out.println("[corpus2] " + kind + " " + lane + ": " + now.size()
                + " tests (register " + registered.size() + ")");
        if (!fresh.isEmpty() || !stale.isEmpty()) {
            StringBuilder sb = new StringBuilder("[" + lane + "] " + kind + " register != committed"
                    + " (" + (only.isEmpty() ? "full run" : "scoped to '" + only + "'") + "): NEW "
                    + fresh.size() + " (" + newMeans + "), STALE " + stale.size()
                    + " (registered, no longer so — shrink the register). Reasons in docs/GATES.md.");
            for (String f : fresh) {
                sb.append("\n  NEW    ").append(f);
            }
            for (String s : stale) {
                sb.append("\n  STALE  ").append(s);
            }
            org.junit.jupiter.api.Assertions.fail(sb.toString());
        }
    }

    /** The accepted-divergence register: {@code fqn -> {bucket, witness}}. */
    /** The test names a register file names (its first {@code |||} column). */
    static java.util.Set<String> registerTests(String resource) throws java.io.IOException {
        java.util.Set<String> out = new java.util.HashSet<>();
        try (var in = MinimalCorpusTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                return out;
            }
            for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                    .split("\n")) {
                if (!line.isBlank()) {
                    out.add(line.split("\\|\\|\\|")[0].trim());
                }
            }
        }
        return out;
    }

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

    /** P0 (homework §4h): the database-mode FAIL differential is a NAMED
     * register per lane and direction. {@code rows} = the tests lost (or
     * gained) now; the register must hold exactly those that RAN — a new
     * row is a regression named here, a register row that no longer
     * differs is a moved pin (delete it, reason in GATES.md). */
    private static void pinDifferential(String lane, String direction, List<String> rows,
            Set<String> ranNames, String resource) throws IOException {
        Set<String> register = new HashSet<>(readRoster(resource));
        List<String> unregistered = new ArrayList<>();
        for (String r : rows) {
            if (!register.contains(r)) {
                unregistered.add(r);
            }
        }
        Set<String> now = new HashSet<>(rows);
        List<String> stale = new ArrayList<>();
        for (String r : readRoster(resource)) {
            if (ranNames.contains(r) && !now.contains(r)) {
                stale.add(r);
            }
        }
        if (!unregistered.isEmpty() || !stale.isEmpty()) {
            StringBuilder sb = new StringBuilder("[" + lane + "] database-mode " + direction
                    + " differential != " + resource + ": " + unregistered.size()
                    + " new (not in the register), " + stale.size()
                    + " stale (in the register, not " + direction + " now)."
                    + " Every register change carries a written reason in docs/GATES.md.");
            for (String u : unregistered) {
                sb.append("\n  NEW    ").append(u);
            }
            for (String s : stale) {
                sb.append("\n  STALE  ").append(s);
            }
            org.junit.jupiter.api.Assertions.fail(sb.toString());
        }
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
