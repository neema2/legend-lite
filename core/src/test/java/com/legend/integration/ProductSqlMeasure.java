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
        if (args.length == 2 && args[0].equals("probe")) {
            probe(Path.of(args[1]));
            return;
        }
        if (args.length != 3 || !(args[0].equals("measure") || args[0].equals("time") || args[0].equals("mixed"))) {
            throw new IllegalArgumentException("usage: measure <cases.jsonl> <out> | time <pairs dir> <out>"
                    + " | mixed <lean-sql dir> <out>");
        }
        Path out = Path.of(args[2]);
        Files.createDirectories(out);
        if (args[0].equals("mixed")) {
            mixed(Path.of(args[1]), out);
        } else if (args[0].equals("measure")) {
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

    /** {@code probe <file>}: each line of {@code file} (a statement; {@code #} a comment) run on every in-process
     *  database, its rows or its error printed (P1a.2's homework: what a database evaluates of a one-row derived
     *  table). */
    static void probe(Path file) throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String sql = line.replace("{RAISE}", backend.type().name().equals("H2") ? "SIGNAL('45000', 'raised')"
                        : backend.type().name().equals("SQLite") ? "json_extract('{}', '$raised')"
                        : "error('raised')");
                String answer;
                try (java.sql.Connection c = backend.open()) {
                    answer = "rows " + rows(c, sql);
                } catch (java.sql.SQLException e) {
                    answer = "RAISED " + String.valueOf(e.getMessage()).lines().findFirst().orElse("");
                }
                System.out.println("[probe] " + backend.type() + " | " + sql + " | " + answer);
            }
        }
    }

    /** {@code mixed <lean-sql dir> <out>} (the orchestrator's question, 2026-11-12, facts only): which queries of the
     *  lean-SQL sets hold a collection LITERAL whose elements' types differ -- {@code numbers} (Integer, Float,
     *  Decimal, Number), {@code dates} (StrictDate, DateTime, Date), else {@code mixed} -- and their product SQL's
     *  characters per database, beside every query's. Writes {@code mixed.tsv}: set, database, kind, queries,
     *  characters. (A list mixed by an expression rather than a literal -- a concatenation, a map -- is not seen.) */
    static void mixed(Path dir, Path out) throws IOException {
        Map<String, long[]> totals = new java.util.TreeMap<>();
        // channel A on every database
        List<String> a = Files.readAllLines(dir.resolve("channel_a.jsonl"), StandardCharsets.UTF_8);
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            if (com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).ownSessions()) {
                continue;
            }
            for (String line : a) {
                Json.Obj c = Json.parseObject(line);
                tally(totals, "channel_a", type.name(), () -> Compiler.query(Compiler.compileModel(
                        withRuntime(((Json.Str) c.get("model")).value(), type)),
                        ((Json.Str) c.get("expression")).value()), StorelessRuntime.RUNTIME);
            }
        }
        // core's on their own runtimes
        Map<Long, com.legend.compiler.element.ModelContext> models = new java.util.HashMap<>();
        Map<Long, String> texts = new java.util.HashMap<>();
        for (String line : Files.readAllLines(dir.resolve("core.jsonl"), StandardCharsets.UTF_8)) {
            Json.Obj c = Json.parseObject(line);
            long id = ((Json.Num) c.get("model_id")).longValue();
            if (c.has("model")) {
                texts.put(id, ((Json.Str) c.get("model")).value());
                continue;
            }
            String rt = ((Json.Str) c.get("runtime")).value();
            tally(totals, "core", null, () -> Compiler.query(models.computeIfAbsent(id,
                    k -> Compiler.compileModel(texts.get(k))), ((Json.Str) c.get("query")).value()),
                    rt.isEmpty() ? null : rt);
        }
        StringBuilder b = new StringBuilder("set\tdatabase\tkind\tqueries\tchars\n");
        totals.forEach((k, v) -> b.append(k).append('\t').append(v[0]).append('\t').append(v[1]).append('\n'));
        Files.writeString(out.resolve("mixed.tsv"), b, StandardCharsets.UTF_8);
        System.err.print(b);
    }

    private static void tally(Map<String, long[]> totals, String set, @com.legend.base.Nullable String database,
            java.util.function.Supplier<com.legend.TypedQuery> typed, @com.legend.base.Nullable String runtime) {
        try {
            com.legend.TypedQuery q = typed.get();
            String kind = "none";
            for (com.legend.compiler.spec.typed.TypedSpec s : q.body()) {
                kind = worse(kind, mixedKind(s));
            }
            ExecutionPlan plan = q.executionPlan(runtime,
                    Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING));
            List<String[]> statements = new ArrayList<>();
            collect(plan.root(), statements);
            long chars = statements.stream().mapToLong(x -> x[1].length()).sum();
            String db = database != null ? database : plan.answers() && plan.sendsTheAnswer()
                    ? plan.answerSql().target().database().type().name() : "constant";
            for (String k : List.of(kind, "all")) {
                long[] t = totals.computeIfAbsent(set + "\t" + db + "\t" + k, x -> new long[2]);
                t[0]++;
                t[1] += chars;
            }
        } catch (RuntimeException | StackOverflowError e) {
            // refused at planning: not counted (the baseline counts refusals)
        }
    }

    private static final List<String> ORDER = List.of("none", "numbers", "dates", "mixed");

    private static String worse(String a, String b) {
        return ORDER.indexOf(a) >= ORDER.indexOf(b) ? a : b;
    }

    /** The worst mixed collection literal in {@code s}'s tree: none, numbers, dates or mixed. */
    static String mixedKind(com.legend.compiler.spec.typed.TypedSpec s) {
        String kind = "none";
        if (s instanceof com.legend.compiler.spec.typed.TypedCollection c && c.elements().size() > 1) {
            java.util.Set<String> types = new java.util.TreeSet<>();
            for (com.legend.compiler.spec.typed.TypedSpec e : c.elements()) {
                types.add(e.info().type().typeName());
            }
            if (types.size() > 1) {
                java.util.Set<String> numbers = java.util.Set.of("Integer", "Float", "Decimal", "Number");
                java.util.Set<String> dates = java.util.Set.of("StrictDate", "DateTime", "Date");
                kind = numbers.containsAll(types) ? "numbers" : dates.containsAll(types) ? "dates" : "mixed";
            }
        }
        for (com.legend.compiler.spec.typed.TypedSpec k : s.children()) {
            kind = worse(kind, mixedKind(k));
        }
        return kind;
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
