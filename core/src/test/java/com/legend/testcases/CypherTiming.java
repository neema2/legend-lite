// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.exec.ExecutionResult;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * THROWAWAY (branch debug/cypher-parity-timing, 2026-10-19, never merged): where the Cypher parity lanes spend their
 * time. //core:cypher_parity_test timed out at 900 s on Windows CI (runs 39164681359 and 39262226227); this records,
 * per case, the wall time of each step of {@code QueryService.execute} (compile the model, parse and resolve the
 * query, plan and execute), the test thread's CPU time and the process's, and a JFR recording of the whole lane, all
 * written to TEST_UNDECLARED_OUTPUTS_DIR (CI uploads test.outputs).
 */
public final class CypherTiming {

    private static final List<String> ROWS = Collections.synchronizedList(new ArrayList<>());

    /** The case in flight: compile, resolve, execute (ns), set by {@link #execute}. */
    private static final long[] PHASES = new long[3];

    /** Running sums for the progress line: compile, resolve, execute, seed, case wall, thread CPU, process CPU. */
    private static final long[] SUMS = new long[7];

    private static final long[] STARTED = new long[1];

    /** The SQL the case in flight sent, as the connection saw it. */
    private static final List<String> SQL = new ArrayList<>();

    /** The lane and its outputs directory, set by {@link #record}. */
    private static final String[] LANE = new String[2];

    private static final int[] SLOW = new int[1];

    private CypherTiming() {
    }

    /** {@code QueryService.execute(model, island, runtime, connection)}, step by step, each step timed. */
    public static ExecutionResult execute(String model, String island, String runtime, Connection connection)
            throws Exception {
        long a = System.nanoTime();
        long b = a;
        long c = a;
        try {
            com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(model);
            b = System.nanoTime();
            com.legend.protocol.spec.ValueSpecification parsed = com.legend.Compiler.parseQuery(island);
            List<com.legend.protocol.spec.ValueSpecification> statements =
                    parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                            ? lf.body() : List.of(parsed);
            com.legend.protocol.spec.ValueSpecification resolved = com.legend.Compiler.resolveQuery(statements,
                    new com.legend.model.ImportScope(List.of()), ctx);
            c = System.nanoTime();
            return java.util.Objects.requireNonNull(com.legend.Execution.executeResolved(resolved, ctx, runtime,
                    capturing(connection), null, null, com.legend.ExecuteOptions.NONE), "query produced no result");
        } finally {
            long d = System.nanoTime();
            if (b == a) {
                PHASES[0] = d - a;
            } else if (c == a) {
                PHASES[0] = b - a;
                PHASES[1] = d - b;
            } else {
                PHASES[0] = b - a;
                PHASES[1] = c - b;
                PHASES[2] = d - c;
            }
        }
    }

    /** {@code connection}, every SQL text it is handed recorded in {@link #SQL} (prepareStatement, and a Statement's
     *  execute*). */
    private static Connection capturing(Connection connection) {
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("prepare") && args != null && args.length > 0
                            && args[0] instanceof String text) {
                        SQL.add(text);
                    }
                    Object out;
                    try {
                        out = method.invoke(connection, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (out instanceof java.sql.Statement st && !(out instanceof java.sql.PreparedStatement)) {
                        return java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                new Class<?>[] {java.sql.Statement.class}, (p2, m2, a2) -> {
                                    if (m2.getName().startsWith("execute") && a2 != null && a2.length > 0
                                            && a2[0] instanceof String text) {
                                        SQL.add(text);
                                    }
                                    try {
                                        return m2.invoke(st, a2);
                                    } catch (java.lang.reflect.InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
                    }
                    return out;
                });
    }

    static void startCase() {
        SQL.clear();
        PHASES[0] = 0;
        PHASES[1] = 0;
        PHASES[2] = 0;
    }

    static long threadCpu() {
        return ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    }

    static long processCpu() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }

    static void seeded(String graph, int cases, long nanos) {
        SUMS[3] += nanos;
        ROWS.add(String.join("\t", "SEED", graph, String.valueOf(cases), "", ms(nanos), "", "", "", "", ""));
    }

    static void finishCase(String key, String graph, String status, long wall, long threadCpu, long processCpu,
            int done, int total) {
        SUMS[0] += PHASES[0];
        SUMS[1] += PHASES[1];
        SUMS[2] += PHASES[2];
        SUMS[4] += wall;
        SUMS[5] += threadCpu;
        SUMS[6] += processCpu;
        int chars = SQL.stream().mapToInt(String::length).sum();
        ROWS.add(String.join("\t", "CASE", graph, key, status, ms(wall), ms(PHASES[0]), ms(PHASES[1]),
                ms(PHASES[2]), ms(threadCpu), ms(processCpu), String.valueOf(SQL.size()), String.valueOf(chars)));
        String dump = System.getProperty("cypher.timing.sql");
        if ((wall > 1_000_000_000L || dump != null && key.startsWith(dump)) && LANE[1] != null && SLOW[0] < 40) {
            SLOW[0] += 1;
            try {
                Files.writeString(Path.of(LANE[1]).resolve(String.format(Locale.ROOT, "slow-%s-%02d.sql", LANE[0],
                        SLOW[0])), "-- " + key + " (" + ms(wall) + " ms, " + SQL.size() + " statements, " + chars
                        + " characters)\n" + String.join(";\n-- next statement\n", SQL) + "\n",
                        StandardCharsets.UTF_8);
            } catch (IOException e) {
                System.err.println("[cypher-timing] could not write the slow SQL: " + e);
            }
        }
        if (done % 250 == 0 || done == total) {
            System.err.printf(Locale.ROOT, "[cypher-timing] %d/%d cases, %.0f s since the lane began; cases %.0f s"
                    + " (compile %.0f, resolve %.0f, plan+execute %.0f), seeding %.0f; thread CPU %.0f s, process CPU"
                    + " %.0f s; heap used %d MB of %d MB%n", done, total, (System.nanoTime() - STARTED[0]) / 1e9,
                    SUMS[4] / 1e9, SUMS[0] / 1e9, SUMS[1] / 1e9, SUMS[2] / 1e9, SUMS[3] / 1e9, SUMS[5] / 1e9,
                    SUMS[6] / 1e9, used() >> 20, Runtime.getRuntime().maxMemory() >> 20);
        }
    }

    private static long used() {
        Runtime r = Runtime.getRuntime();
        return r.totalMemory() - r.freeMemory();
    }

    private static String ms(long nanos) {
        return String.format(Locale.ROOT, "%.3f", nanos / 1e6);
    }

    /** The lane's recording: JFR (the "profile" settings) from now; on close, the recording and the timings written
     *  to the test's undeclared outputs. */
    public static AutoCloseable record(String lane) throws Exception {
        STARTED[0] = System.nanoTime();
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        LANE[0] = lane;
        LANE[1] = outputs == null ? System.getProperty("java.io.tmpdir") : outputs;
        System.err.printf(Locale.ROOT, "[cypher-timing] %s lane: %s %s, %d processors, max heap %d MB, JVM %s%n", lane,
                System.getProperty("os.name"), System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory() >> 20,
                ManagementFactory.getRuntimeMXBean().getInputArguments());
        jdk.jfr.Recording recording = new jdk.jfr.Recording(jdk.jfr.Configuration.getConfiguration("profile"));
        recording.start();
        return () -> {
            String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
            Path out = Path.of(dir == null ? System.getProperty("java.io.tmpdir") : dir);
            try {
                recording.dump(out.resolve("cypher-" + lane + ".jfr"));
            } finally {
                recording.close();
                write(out.resolve("cypher-timing-" + lane + ".tsv"));
                System.err.printf(Locale.ROOT, "[cypher-timing] %s lane done in %.0f s; outputs in %s%n", lane,
                        (System.nanoTime() - STARTED[0]) / 1e9, out);
            }
        };
    }

    private static void write(Path file) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("kind\tgraph\tcase\tstatus\twall_ms\tcompile_ms\tresolve_ms\texecute_ms\tthread_cpu_ms\tprocess_cpu_ms"
                + "\tsql_statements\tsql_chars");
        synchronized (ROWS) {
            lines.addAll(ROWS);
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
    }
}
