package com.legend.integration;

import com.legend.test.ServiceTestRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE STRESS CORPUS, EXECUTED, once: every {@code stress::} service's test suites run through
 * {@link ServiceTestRunner} — seed data provisioned from the corpus's own {@code ###Data} element, the query executed
 * through the platform, the answer judged against the oracle's {@code EqualToJson} expectation. The gates
 * ({@link StressServiceSuitesTest} on DuckDB, {@link StressServiceSuitesH2Test} on H2) and the measuring tool
 * ({@code bazel run //core:stress_tool}, {@link StressTool}) each run it with their own {@link Config} (Bazel workplan
 * P3-12: the knobs left the test).
 */
final class StressSuites {

    private StressSuites() {}

    /**
     * One run's settings.
     *
     * @param h2 the session the test runtime is typed as: H2 (the engine-faithful reference) or DuckDB (the fast gate)
     * @param sessions the session policy, or null for the backend's default (H2 fresh per test, DuckDB shared)
     * @param only run only the services whose name contains this, or "" for all
     * @param overrides files whose elements replace the corpus elements of the same name (rebuild D23: a damaged
     *     {@code ###Data} element in place of a seed; the corpus files are never edited)
     * @param rowsDir every test's computed answer, one JSON file per test, whatever the verdict, or null
     * @param outDir where the pass, fail, skipped and progress ledgers go
     */
    record Config(boolean h2, ServiceTestRunner.Sessions sessions, String only, List<Path> overrides,
            Path rowsDir, Path outDir) {}

    /** The verdicts, one line per test ({@code service / suite / test :: reason}). */
    record Result(List<String> pass, List<String> fail, List<String> skipped) {}

    static Result run(Config c) throws Exception {
        long t0 = System.nanoTime();
        StressCorpus.reportExclusions();
        List<Path> overrides = c.overrides();
        Path rowsDir = c.rowsDir();
        if (rowsDir != null) {
            Files.createDirectories(rowsDir);
        }
        var module = com.legend.Compiler.parseSources(StressCorpus.sources(overrides));
        for (String d : module.duplicateElements()) {
            System.out.println("[suites] " + (overrides.isEmpty() ? "DUPLICATE " : "REPLACED  ") + d);
        }
        var ctx = com.legend.Compiler.buildModel(module.model());
        long tModel = System.nanoTime();
        System.out.printf("[suites] model: %d elements, parse+build %d ms%n",
                module.model().elements().size(), (tModel - t0) / 1_000_000);

        var services = module.model().elements().stream()
                .filter(el -> el instanceof com.legend.model.ServiceDefinition svc
                        && svc.qualifiedName().startsWith("stress::")
                        && svc.testSuites() != null)
                .map(el -> (com.legend.model.ServiceDefinition) el)
                .sorted(Comparator.comparing(com.legend.model.ServiceDefinition::qualifiedName))
                .toList();
        if (services.isEmpty()) {
            throw new IllegalStateException("no stress:: services with test suites");
        }

        String only = c.only();
        boolean h2 = c.h2();
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
        // //core:stress_tool --sessions overrides either lane's default.
        ServiceTestRunner.Sessions policy = c.sessions() != null ? c.sessions()
                : h2 ? ServiceTestRunner.Sessions.FRESH_PER_TEST : ServiceTestRunner.Sessions.SHARED;
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
        Files.createDirectories(c.outDir());
        // per-test progress, flushed as it happens
        Path progressPath = c.outDir().resolve("stress-suites-progress" + (h2 ? "-h2" : "") + ".txt");
        int done = 0;
        long lastReport = System.nanoTime();
        java.util.function.Consumer<ServiceTestRunner.Rows> sink = rowsDir == null ? null : rows -> {
            String base = (rows.suiteId() + " / " + rows.testId()).replaceAll("[^A-Za-z0-9_.-]", "_");
            try {
                Files.writeString(rowsDir.resolve(base + ".rows.json"),
                        com.legend.sql.Json.canonical(rows.actual()));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
        try (var runner = new ServiceTestRunner(ctx,
                () -> DriverManager.getConnection(jdbcUrl), policy, sessionType, sink);
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
        Files.write(c.outDir().resolve("stress-suites-pass" + (h2 ? "-h2" : "") + ".txt"), pass);
        Files.write(c.outDir().resolve("stress-suites-fail" + (h2 ? "-h2" : "") + ".txt"), fail);
        Files.write(c.outDir().resolve("stress-suites-skipped" + (h2 ? "-h2" : "") + ".txt"), skipped);
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
        return new Result(pass, fail, skipped);
    }

    /** A failure reason with its specifics elided, so alike failures count together. */
    private static String bucket(String reason) {
        String r = reason.replaceAll("'[^']*'", "'…'").replaceAll("\\$[^ ]*", "\\$…")
                .replaceAll("\\d+", "N");
        return r.length() > 140 ? r.substring(0, 140) : r;
    }
}
