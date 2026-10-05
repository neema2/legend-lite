package com.legend.integration;

import com.legend.test.ServiceTestRunner;
import com.legend.testing.Programs;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * The stress corpus's measuring tool (Bazel workplan P3-12: the knobs left the test):
 * <pre>
 *   bazel run //core:stress_tool -- [--backend duckdb|h2] [--sessions fresh|shared] [--only NAME]
 *       [--data FILE.pure,...] [--rows DIR] [--out DIR]
 * </pre>
 * {@code --data}: each file's elements replace the corpus elements of the same name (rebuild D23); then no verdict is
 * judged and {@code --rows} (every test's computed answer, one JSON file per test) is the output. The ledgers go to
 * {@code --out}, by default {@code stress-tool-out}; paths are from where it is run.
 */
public final class StressTool {

    private StressTool() {}

    public static void main(String[] args) throws Exception {
        String backend = option(args, "--backend", "duckdb");
        String sessions = option(args, "--sessions", "");
        if (!backend.matches("duckdb|h2") || !sessions.matches("|fresh|shared")) {
            throw new IllegalArgumentException("--backend duckdb|h2, --sessions fresh|shared");
        }
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                throw new IllegalArgumentException(a + ": give the value as its own argument (--name value)");
            }
        }
        String data = option(args, "--data", "");
        String rows = option(args, "--rows", "");
        List<Path> overrides = Arrays.stream(data.split(",")).map(String::trim).filter(x -> !x.isEmpty())
                .map(Programs::argument).toList();
        Path rowsDir = rows.isEmpty() ? null : java.nio.file.Files.createDirectories(Programs.argument(rows));
        Path outDir = Programs.argument(option(args, "--out", "stress-tool-out"));
        StressSuites.Result r = StressSuites.run(new StressSuites.Config("h2".equals(backend),
                sessions.isEmpty() ? null : sessions.equals("fresh") ? ServiceTestRunner.Sessions.FRESH_PER_TEST
                        : ServiceTestRunner.Sessions.SHARED,
                option(args, "--only", ""), overrides, rowsDir, outDir));
        if (!overrides.isEmpty()) {
            System.out.println("[stress_tool] data overridden: no verdict is judged; the rows are the output");
        }
        System.out.println("[stress_tool] pass=" + r.pass().size() + " fail=" + r.fail().size() + " skipped="
                + r.skipped().size() + "; ledgers in " + outDir);
    }

    private static String option(String[] args, String name, String fallback) {
        String v = Programs.option(args, name);
        return v == null ? fallback : v;
    }
}
