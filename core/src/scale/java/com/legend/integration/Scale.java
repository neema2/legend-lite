package com.legend.integration;

import java.util.Map;

/**
 * THE SCALE BENCHMARK (Bazel workplan P3-05, A11): generated models far past the 1K baseline, timed.
 *
 * <pre>
 *   bazel run //core:scale -- 10k | 100k | dense | complex | chaotic | profile
 * </pre>
 *
 * Each word runs one of the scale classes on the JUnit Platform: they assert nothing beyond "no failures" and print
 * their timings, so they are a benchmark someone runs, not a test in the graph. Exits non-zero on a failure.
 */
public final class Scale {

    private static final Map<String, String> CLASSES = Map.of(
            "10k", "StressTest10K",
            "100k", "StressTest100K",
            "dense", "StressTestDense",
            "complex", "StressTestComplexQueries",
            "chaotic", "StressTestChaotic",
            "profile", "ProfileBuildCost");

    private Scale() {}

    public static void main(String[] args) throws Exception {
        String cls = args.length == 1 ? CLASSES.get(args[0]) : null;
        if (cls == null) {
            System.err.println("usage: bazel run //core:scale -- <" + String.join(" | ", new java.util.TreeSet<>(CLASSES.keySet())) + ">");
            System.exit(2);
        }
        var request = org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request()
                .selectors(org.junit.platform.engine.discovery.DiscoverySelectors.selectClass(
                        "com.legend.integration." + cls))
                .build();
        var summary = new org.junit.platform.launcher.listeners.SummaryGeneratingListener();
        org.junit.platform.launcher.core.LauncherFactory.create().execute(request, summary);
        summary.getSummary().printTo(new java.io.PrintWriter(System.out, true));
        summary.getSummary().printFailuresTo(new java.io.PrintWriter(System.err, true), 20);
        long ran = summary.getSummary().getTestsSucceededCount() + summary.getSummary().getTotalFailureCount();
        System.exit(ran == 0 || summary.getSummary().getTotalFailureCount() > 0 ? 1 : 0);
    }
}
