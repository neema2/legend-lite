// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PRODUCT SQL, MEASURED (E3 of the total-wall program, 2026-11-12, docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md): a
 * measuring tool, never a test (as {@code CompileLatency}).
 *
 * <ul>
 * <li>{@code measure <cases.jsonl> <out>}: each case ({@code {"model", "expression"}}, PCT channel A's as
 * {@code -Dlegend.capture.channela} captures them) planned by the product's one planning entry
 * ({@code TypedQuery.executionPlan}) on every database the platform implements -- planning needs none, so one JVM
 * measures them all. Writes {@code sizes.tsv} (database, case, statement, characters, the node's kind) and
 * {@code sql-<database>.jsonl} (each statement's text): the plan's own statements, never a target's loads or session
 * statements.</li>
 * <li>{@code time <pairs dir> <out>}: each before/after pair in {@code pairs-<database>.jsonl} (a rewrite of a
 * product statement) run on that in-process database: the same rows or the pair is not applicable; each side the
 * median of its runs after a warm one. Writes {@code timing.tsv}.</li>
 * </ul>
 */
public final class ProductSqlMeasure {

    private ProductSqlMeasure() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !(args[0].equals("measure") || args[0].equals("time"))) {
            throw new IllegalArgumentException("usage: measure <cases.jsonl> <out> | time <pairs dir> <out>");
        }
        Path out = Path.of(args[2]);
        Files.createDirectories(out);
        if (args[0].equals("measure")) {
            measure(Path.of(args[1]), out);
        } else {
            time(Path.of(args[1]), out, Integer.getInteger("legend.measure.runs", 7));
        }
    }

    static void measure(Path cases, Path out) throws IOException {
        List<String> lines = Files.readAllLines(cases, StandardCharsets.UTF_8);
        try (Writer sizes = Files.newBufferedWriter(out.resolve("sizes.tsv"), StandardCharsets.UTF_8)) {
            sizes.write("dialect\tcase\tstatement\tchars\tkind\n");
            for (DatabaseType type : com.legend.database.Databases.implemented()) {
                int planned = 0;
                int refused = 0;
                StringBuilder sql = new StringBuilder();
                StringBuilder rows = new StringBuilder();
                for (int i = 0; i < lines.size(); i++) {
                    Json.Obj c = Json.parseObject(lines.get(i));
                    String model = ((Json.Str) c.get("model")).value();
                    String expression = ((Json.Str) c.get("expression")).value();
                    List<String[]> statements = new ArrayList<>();
                    try {
                        ExecutionPlan plan = Compiler.query(Compiler.compileModel(withRuntime(model, type)),
                                expression).executionPlan(StorelessRuntime.RUNTIME,
                                Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING));
                        collect(plan.root(), statements);
                        planned++;
                    } catch (RuntimeException | StackOverflowError e) {
                        refused++;
                        rows.append(type).append('\t').append(i).append("\t-1\t-1\trefused:")
                                .append(e.getClass().getSimpleName()).append('\n');
                        continue;
                    }
                    for (int s = 0; s < statements.size(); s++) {
                        String[] st = statements.get(s);
                        rows.append(type).append('\t').append(i).append('\t').append(s).append('\t')
                                .append(st[1].length()).append('\t').append(st[0]).append('\n');
                        sql.append(Json.toCompact(Map.of("case", i, "statement", s, "kind", st[0], "sql", st[1])))
                                .append('\n');
                    }
                }
                System.err.println("[measure] " + type + " planned=" + planned + " refused=" + refused);
                if (planned > 0) {
                    sizes.write(rows.toString());
                    Files.writeString(out.resolve("sql-" + type + ".jsonl"), sql, StandardCharsets.UTF_8);
                }
            }
        }
    }

    /** {@code model} with the storeless runtime on {@code type}: its in-process declaration where the connector has
     *  one, else a server's coordinates (planning opens nothing). */
    static String withRuntime(String model, DatabaseType type) {
        try {
            return StorelessRuntime.with(model, type);
        } catch (RuntimeException e) {
            return StorelessRuntime.onServer(model, type, "localhost", 1, "legend");
        }
    }

    /** Every statement the plan's nodes send, with its node's kind: the query's own, never a target's loads. */
    static void collect(ExecutionPlan.Node node, List<String[]> out) {
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> collect(step, out));
            case ExecutionPlan.TdsResult t -> out.add(new String[] {"result", t.sql().statement()});
            case ExecutionPlan.Keyed k -> {
                out.add(new String[] {"keys", k.keys().statement()});
                out.add(new String[] {"keyed-body", k.body().statement()});
            }
            case ExecutionPlan.Update u -> out.add(new String[] {"update", u.sql().statement()});
            case ExecutionPlan.Effect e -> out.add(new String[] {"effect", e.sql().statement()});
            case ExecutionPlan.Check c -> out.add(new String[] {"check", c.sql().statement()});
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
    }

    static void time(Path dir, Path out, int runs) throws Exception {
        try (Writer timing = Files.newBufferedWriter(out.resolve("timing.tsv"), StandardCharsets.UTF_8)) {
            timing.write("dialect\tcase\tbefore_chars\tafter_chars\tbefore_us\tafter_us\tverdict\n");
            for (TestBackend backend : TestBackends.inProcess()) {
                Path pairs = dir.resolve("pairs-" + backend.type() + ".jsonl");
                if (!Files.exists(pairs)) {
                    continue;
                }
                int same = 0;
                int differ = 0;
                int failed = 0;
                for (String line : Files.readAllLines(pairs, StandardCharsets.UTF_8)) {
                    Json.Obj p = Json.parseObject(line);
                    String before = ((Json.Str) p.get("before")).value();
                    String after = ((Json.Str) p.get("after")).value();
                    String id = Json.toCompact(p.get("case"));
                    String row = backend.type() + "\t" + id + "\t" + before.length() + "\t" + after.length() + "\t";
                    try (java.sql.Connection c = backend.open()) {
                        List<String> a;
                        try {
                            a = rows(c, before);
                        } catch (java.sql.SQLException e) {
                            continue;   // the statement raises by design (a guard): not a timing case
                        }
                        List<String> b;
                        try {
                            b = rows(c, after);
                        } catch (java.sql.SQLException e) {
                            failed++;
                            timing.write(row + "-\t-\tafter-failed\n");
                            continue;
                        }
                        if (!a.equals(b)) {
                            differ++;
                            timing.write(row + "-\t-\tdiffer\n");
                            continue;
                        }
                        same++;
                        timing.write(row + median(c, before, runs) + "\t" + median(c, after, runs) + "\tsame\n");
                    }
                }
                System.err.println("[pairs] " + backend.type() + " same=" + same + " differ=" + differ
                        + " after-failed=" + failed);
            }
        }
    }

    private static List<String> rows(java.sql.Connection c, String sql) throws java.sql.SQLException {
        List<String> out = new ArrayList<>();
        try (java.sql.Statement st = c.createStatement(); java.sql.ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(rs.getString(i)).append('\u0001');
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    /** The median microseconds of {@code runs} executions (prepare, run, read every row), after one warm run. */
    private static long median(java.sql.Connection c, String sql, int runs) throws java.sql.SQLException {
        rows(c, sql);
        long[] us = new long[runs];
        for (int i = 0; i < runs; i++) {
            long t = System.nanoTime();
            rows(c, sql);
            us[i] = (System.nanoTime() - t) / 1000;
        }
        java.util.Arrays.sort(us);
        return us[runs / 2];
    }
}
