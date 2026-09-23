package com.legend.integration;

import com.legend.testing.Repo;
import com.legend.test.PureTestRunner;
import com.legend.test.ServiceTestRunner;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.DriverManager;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * THE STRESS CORPUS, EXECUTED: every {@code stress::} service's test suites run
 * through {@link ServiceTestRunner} on DuckDB — seed data provisioned from the
 * corpus's own {@code ###Data} element, the query executed through the platform,
 * the answer judged against the oracle's {@code EqualToJson} expectation.
 *
 * <p>This is the measurement leg (2026-09-16): it records the per-phase times and
 * the verdict census, writes the pass / fail / skipped ledgers under
 * {@code target/}, and does not yet pin a count. The ratchet arrives with the
 * gate once the numbers are known.
 */
@org.junit.jupiter.api.Tag("stress")   // GATE 10 (//core:stress_suites): its own gate, excluded from the core suite by tag
@DisplayName("Stress corpus: service test suites through legend-lite")
class StressServiceSuitesTest {

    @Test
    void suites() throws Exception {
        long t0 = System.nanoTime();
        String model = StressCorpus.model();
        StressCorpus.reportExclusions();
        var ctx = com.legend.Compiler.compileModel(model);
        long tModel = System.nanoTime();
        System.out.printf("[suites] model: %d KB, parse+build %d ms%n",
                model.length() / 1024, (tModel - t0) / 1_000_000);

        var services = com.legend.testing.Own.model(model).elements().stream()
                .filter(el -> el instanceof com.legend.model.ServiceDefinition svc
                        && svc.qualifiedName().startsWith("stress::")
                        && svc.testSuites() != null)
                .map(el -> (com.legend.model.ServiceDefinition) el)
                .sorted(Comparator.comparing(com.legend.model.ServiceDefinition::qualifiedName))
                .toList();
        assertFalse(services.isEmpty(), "no stress:: services with test suites");

        String only = System.getProperty("stress.only", "").trim();
        // -Dstress.backend=duckdb (default) | h2 — the session the test runtime is typed as
        boolean h2 = "h2".equalsIgnoreCase(System.getProperty("stress.backend", "duckdb"));
        // TWO LANES, two purposes (user ruling 2026-09-16):
        //   H2     = the engine-faithful reference: a fresh, freshly seeded session
        //            per test, exactly the engine's shape (~1.9 min; the engine ~1 h);
        //   DuckDB = the fast gate: one seeded session per distinct provisioning,
        //            SHARED across the tests that declare it. Isolation is by write
        //            detection — a session that executed a statement with an effect
        //            is re-seeded before the next test — so a read-only corpus sees
        //            a pristine seed every test and the pass count equals the
        //            fresh-per-test run's (measured 2026-09-16: 2,765 both ways;
        //            fresh took 27 min, shared ~30 s).
        // -Dstress.sessions=fresh|shared overrides either lane's default.
        String sessionsDefault = h2 ? "fresh" : "shared";
        ServiceTestRunner.Sessions policy = "shared".equalsIgnoreCase(
                System.getProperty("stress.sessions", sessionsDefault))
                ? ServiceTestRunner.Sessions.SHARED : ServiceTestRunner.Sessions.FRESH_PER_TEST;
        // both: a private in-memory database per session; H2 carries the engine's own
        // session settings (H2Settings: NON_KEYWORDS incl VALUE/YEAR, MODE=LEGACY), as the
        // engine's test connection does — a bare jdbc:h2:mem: refused ESG_METRIC.VALUE
        String jdbcUrl = h2 ? "jdbc:h2:mem:" + com.legend.exec.H2Settings.SETTINGS : "jdbc:duckdb:";
        var sessionType = h2 ? com.legend.model.ConnectionDefinition.DatabaseType.H2
                : com.legend.model.ConnectionDefinition.DatabaseType.DuckDB;
        System.out.println("[suites] sessions: " + policy + " on " + sessionType);
        List<String> pass = new ArrayList<>(), fail = new ArrayList<>(), skipped = new ArrayList<>();
        Map<String, Integer> failBuckets = new TreeMap<>();
        long execNs = 0;
        List<ServiceTestRunner.Result> slow = new ArrayList<>();
        Files.createDirectories(Repo.outDir());
        // per-test progress, flushed as it happens, so a long run can be
        // watched: `tail -f core/target/stress-suites-progress.txt`
        Path progressPath = Repo.out("stress-suites-progress" + (h2 ? "-h2" : "") + ".txt");
        int done = 0;
        long lastReport = System.nanoTime();
        try (var runner = new ServiceTestRunner(ctx,
                () -> DriverManager.getConnection(jdbcUrl), policy, sessionType);
             var progress = Files.newBufferedWriter(progressPath)) {
            for (var svc : services) {
                if (!only.isEmpty() && !svc.qualifiedName().contains(only)) {
                    continue;
                }
                long s0 = System.nanoTime();
                for (var r : runner.run(svc)) {
                    progress.write(r.status() + "\t" + r.millis() + "ms\t" + r.serviceFqn()
                            + "\t" + r.testId() + "\n");
                    progress.flush();
                    done++;
                    if (done % 200 == 0) {
                        System.out.printf("[suites] progress %d tests, last 200 in %d ms"
                                + " (pass=%d fail=%d skipped=%d so far)%n", done,
                                (System.nanoTime() - lastReport) / 1_000_000,
                                pass.size(), fail.size(), skipped.size());
                        lastReport = System.nanoTime();
                    }
                    String line = r.serviceFqn() + " / " + r.suiteId() + " / " + r.testId()
                            + " :: " + r.reason();
                    switch (r.status()) {
                        case PASS -> pass.add(line);
                        case FAIL -> {
                            fail.add(line);
                            failBuckets.merge(bucket(r.reason()), 1, Integer::sum);
                        }
                        case SKIPPED -> skipped.add(line);
                    }
                    slow.add(r);
                }
                execNs += System.nanoTime() - s0;
            }
            System.out.printf("[suites] sessions opened: %d%n", runner.sessions().size());
        }
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("stress-suites-pass" + (h2 ? "-h2" : "") + ".txt"), pass);
        Files.write(Repo.out("stress-suites-fail" + (h2 ? "-h2" : "") + ".txt"), fail);
        Files.write(Repo.out("stress-suites-skipped" + (h2 ? "-h2" : "") + ".txt"), skipped);
        System.out.printf("[suites] pass=%d fail=%d skipped=%d of %d tests in %d ms (execution)"
                + " — %d ms wall%n", pass.size(), fail.size(), skipped.size(),
                pass.size() + fail.size() + skipped.size(), execNs / 1_000_000,
                (System.nanoTime() - t0) / 1_000_000);
        failBuckets.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .limit(40)
                .forEach(e -> System.out.printf("[suites] FAIL %5d  %s%n", e.getValue(), e.getKey()));
        slow.sort((a, b) -> Long.compare(b.millis(), a.millis()));
        slow.stream().limit(10).forEach(r -> System.out.printf("[suites] slow %d ms %s%n",
                r.millis(), r.serviceFqn()));
        skipped.stream().map(l -> l.substring(l.indexOf(" :: ") + 4))
                .collect(java.util.stream.Collectors.groupingBy(s -> s, TreeMap::new,
                        java.util.stream.Collectors.counting()))
                .forEach((k, v) -> System.out.printf("[suites] SKIP %5d  %s%n", v, k));
        assertFalse(pass.isEmpty(), "nothing passed — the harness itself is broken");
        if (only.isEmpty()) {
            // THE RATCHET (2026-09-16, first full run: 2,702 / 2,028 / 6 of 4,736 in
            // 8.5 s execution, 20.7 s wall): the count judged EQUAL to the oracle may
            // only grow. Raise it with every leg that burns a bucket; never lower it.
            int floor = h2 ? MIN_PASS_H2 : MIN_PASS;
            assertTrue(pass.size() >= floor, pass.size() + " tests passed, below the"
                    + " ratchet " + floor + " — see target/stress-suites-fail" + (h2 ? "-h2" : "") + ".txt");
        }
    }

    /** Tests judged equal to the oracle on the first full run. Shrink-proof. */
    /** The H2 lane's own floor (fresh session per test; the engine's shape): the
     *  DuckDB floor minus the H2 walls (EPOCH_MS/REVERSE, last-digit floats,
     *  timestamp text — ledger F-P). */
    private static final int MIN_PASS_H2 = 4622;   // 4509 -> 4571 -> 4596 -> 4600 -> 4607 -> 4612 (2-ULP judge policy F-AE; 2026-09-16/17: orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; CORRECTION: the 4602 written at a4c4a883d was measured with the graph-envelope +0000 spelling in the tree, reverted before the commit (ledger F-AA) — 4596 is that commit's measured count; +4 = isAlphaNumeric/splitPart on H2)
    private static final int MIN_PASS = 4700;   // 2702 -> 2765 -> 4203 -> 4564 -> 4626 -> 4654 -> 4672 -> 4679 -> 4689 (2-ULP judge policy F-AE; 2026-09-16/17: double division; pins; association anchors; F-M; F-O; orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; isAlphaNumeric + firstHourOfDay on DuckDB + splitPart index base; view column kinds + grouped-predicate scoping + routed sub-join key demand)

    /** A failure reason with its specifics elided, so alike failures count together. */
    private static String bucket(String reason) {
        String r = reason.replaceAll("'[^']*'", "'…'").replaceAll("\\$[^ ]*", "\\$…")
                .replaceAll("\\d+", "N");
        return r.length() > 140 ? r.substring(0, 140) : r;
    }
}
