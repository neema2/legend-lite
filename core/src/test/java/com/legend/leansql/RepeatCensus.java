// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.json.Json;
import com.legend.lowering.LetBindingFacts;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.testbackend.StorelessRuntime;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * P1B'S CENSUS (the lean program, 2026-11-15; measure only): the repeats left in channel A's statements, by ORIGIN.
 * Each case is lowered on every relational database, the dialect's passes run, and every expression the tree holds
 * more than once (by structure; the outermost copy only, its parts counted with it) is named by the first reason P1a
 * does not bind it: it can raise (the laziness rule), it reads a row's column, a lambda's parameter, it aggregates, it
 * reads a subquery, it is tagged or checked, or it is closed and plain but sits where P1a does not bind (not the
 * root's FROM-less select). Its excess is (copies - 1) x nodes. Beside it, the rendered statement's repeated
 * characters ({@link LeanSqlBaseline#shape}): what the tree does not hold is the dialect's own render repeating.
 * {@code bazel run //core:repeat_census} prints the table and each reason's largest cases.
 */
public final class RepeatCensus {

    private RepeatCensus() {
    }

    /** The smallest repeat counted, in nodes: a column with a cast and the like are not worth a binding. */
    private static final int MIN_NODES = 4;

    static final String[] REASONS = {"raises", "row", "lambda", "aggregate", "subquery", "tagged-checked",
            "closed-not-root", "closed-eligible"};

    public static void main(String[] args) throws IOException {
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        Path set = Path.of(workspace == null ? "." : workspace, "core/src/test/resources/lean-sql/channel_a.jsonl");
        List<String> lines = Files.readAllLines(set, StandardCharsets.UTF_8);
        StringBuilder report = new StringBuilder("database\treason\trepeats\texcess_nodes\n");
        StringBuilder rendered = new StringBuilder("database\tstatements\tchars\trendered_repeated_chars\n");
        StringBuilder worst = new StringBuilder();
        StringBuilder renderHeads = new StringBuilder("database\trepeated_chars\tcopies\twritten after\n");
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            if (!LeanSqlBaseline.relational(type)) {
                continue;
            }
            com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(type);
            Map<String, long[]> byReason = new LinkedHashMap<>();
            for (String r : REASONS) {
                byReason.put(r, new long[2]);
            }
            Map<String, List<String[]>> examples = new TreeMap<>();
            Map<String, long[]> heads = new java.util.HashMap<>();
            long statements = 0;
            long chars = 0;
            long repeatedChars = 0;
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj c = Json.parseObject(line);
                String model = ((Json.Str) c.get("model")).value();
                String expression = ((Json.Str) c.get("expression")).value();
                SqlQuery passed;
                String text;
                try {
                    TypedQuery q = Compiler.query(Compiler.compileModel(LeanSqlBaseline.withRuntime(model, type)),
                            expression);
                    SqlQuery lowered = com.legend.planner.Lowered.lower(q, StorelessRuntime.RUNTIME, false, List.of(),
                            Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING)).plan();
                    passed = com.legend.sql.dialect.PassedTrees.passed(dialect, lowered);
                    text = dialect.render(lowered);
                } catch (RuntimeException | StackOverflowError e) {
                    continue;
                }
                statements++;
                chars += text.length();
                repeatedChars += LeanSqlBaseline.shape(text)[1];
                for (int[] g : LeanSqlBaseline.repeatedCopies(text)) {
                    long[] h = heads.computeIfAbsent(head(text, g[0]), x -> new long[2]);
                    h[0]++;
                    h[1] += g[1] - g[0];
                }
                Map<SqlExpr, Integer> counts = new java.util.HashMap<>();
                Set<SqlExpr> rootTerms = rootTerms(passed);
                walk(passed, counts);
                for (var e : counts.entrySet()) {
                    int k = e.getValue();
                    long w = weight(e.getKey());
                    if (k < 2 || w < MIN_NODES) {
                        continue;
                    }
                    String reason = reason(e.getKey(), rootTerms);
                    long excess = (k - 1) * w;
                    long[] t = byReason.get(reason);
                    t[0]++;
                    t[1] += excess;
                    examples.computeIfAbsent(reason, x -> new ArrayList<>()).add(new String[] {
                            Long.toString(excess), k + "x" + w + " " + e.getKey().getClass().getSimpleName()
                                    + (e.getKey() instanceof SqlExpr.Call call ? "(" + call.fn() + ")" : "")
                                    + " in " + expression.replace('\n', ' ')});
                }
            }
            long total = 0;
            for (long[] t : byReason.values()) {
                total += t[1];
            }
            for (var e : byReason.entrySet()) {
                report.append(type.name()).append('\t').append(e.getKey()).append('\t').append(e.getValue()[0])
                        .append('\t').append(e.getValue()[1]).append(total == 0 ? "" : String.format(java.util.Locale.ROOT, "\t%.0f%%",
                                100.0 * e.getValue()[1] / total)).append('\n');
            }
            List<Map.Entry<String, long[]>> hl = new ArrayList<>(heads.entrySet());
            hl.sort((a, b) -> Long.compare(b.getValue()[1], a.getValue()[1]));
            for (int i = 0; i < Math.min(12, hl.size()); i++) {
                renderHeads.append(type.name()).append('\t').append(hl.get(i).getValue()[1]).append('\t')
                        .append(hl.get(i).getValue()[0]).append('\t').append(hl.get(i).getKey()).append('\n');
            }
            rendered.append(type.name()).append('\t').append(statements).append('\t').append(chars).append('\t')
                    .append(repeatedChars).append('\n');
            for (var e : examples.entrySet()) {
                List<String[]> l = e.getValue();
                l.sort((a, b) -> Long.compare(Long.parseLong(b[0]), Long.parseLong(a[0])));
                for (int i = 0; i < Math.min(3, l.size()); i++) {
                    String ex = l.get(i)[1];
                    worst.append(type.name()).append(' ').append(e.getKey()).append(" excess ").append(l.get(i)[0])
                            .append(": ").append(ex.length() > 260 ? ex.substring(0, 260) + "..." : ex).append('\n');
                }
            }
        }
        String out = report + "\n" + rendered + "\n" + renderHeads + "\n" + worst;
        System.out.println(out);
        if (args.length > 0) {
            Files.writeString(Path.of(args[0]), out, StandardCharsets.UTF_8);
        }
    }

    /** What a rendered repeat is written after: the 24 characters before its parenthesis (its function, its keyword). */
    private static String head(String text, int at) {
        String before = text.substring(Math.max(0, at - 24), at).replace('\n', ' ').replace('\t', ' ');
        String inside = text.substring(at, Math.min(text.length(), at + 16)).replace('\n', ' ').replace('\t', ' ');
        return before + "|" + inside;
    }

    /** The first reason P1a does not bind {@code e}. */
    static String reason(SqlExpr e, Set<SqlExpr> rootTerms) {
        if (LetBindingFacts.canRaise(e)) {
            return "raises";
        }
        Facts f = new Facts();
        f.scan(e, new HashSet<>());
        if (f.row) {
            return "row";
        }
        if (f.lambda) {
            return "lambda";
        }
        if (f.aggregate) {
            return "aggregate";
        }
        if (f.subquery) {
            return "subquery";
        }
        if (!LetBindingFacts.eligible(e)) {
            return "tagged-checked";
        }
        return rootTerms.contains(e) ? "closed-eligible" : "closed-not-root";
    }

    /** What a repeat reads, from its tree. */
    private static final class Facts {
        boolean row;
        boolean lambda;
        boolean aggregate;
        boolean subquery;

        void scan(SqlExpr e, Set<String> params) {
            switch (e) {
                case SqlExpr.Column c -> {
                    if (c.table() != null) {
                        row = true;
                    } else if (!params.contains(c.name())) {
                        lambda = true;
                    }
                }
                case SqlExpr.Lambda l -> {
                    Set<String> inner = new HashSet<>(params);
                    inner.addAll(l.params());
                    scan(l.body(), inner);
                    return;
                }
                case SqlAgg.Reducer r -> aggregate = true;
                case SqlExpr.WindowCall w -> aggregate = true;
                case SqlExpr.OrderedListAgg a -> aggregate = true;
                case SqlExpr.JsonArrayAgg a -> aggregate = true;
                case SqlExpr.ScalarSubquery q -> subquery = true;
                case SqlExpr.Exists q -> subquery = true;
                case SqlExpr.InSubquery q -> subquery = true;
                default -> {
                }
            }
            for (SqlExpr c : e.children()) {
                scan(c, params);
            }
        }
    }

    /** Every expression of the root's FROM-less select at the bottom of its derived tables (where P1a binds). */
    private static Set<SqlExpr> rootTerms(SqlQuery q) {
        Set<SqlExpr> out = new HashSet<>();
        if (q instanceof com.legend.sql.SqlSelect s) {
            if (s.from() instanceof com.legend.sql.SqlSource.Subselect sub) {
                return rootTerms(sub.inner());
            }
            if (s.from() instanceof com.legend.sql.SqlSource.Dual) {
                List<SqlExpr> terms = new ArrayList<>();
                s.projections().forEach(p -> terms.add(p.expr()));
                if (s.where() != null) {
                    terms.add(s.where());
                }
                for (SqlExpr t : terms) {
                    all(t, out);
                }
            }
        }
        return out;
    }

    private static void all(SqlExpr e, Set<SqlExpr> out) {
        out.add(e);
        for (SqlExpr c : e.children()) {
            all(c, out);
        }
    }

    /** Counts every expression the tree holds, by structure; at a node's second copy its parts are not walked again. */
    private static void walk(Object node, Map<SqlExpr, Integer> counts) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlExpr e) {
            if (counts.merge(e, 1, Integer::sum) > 1) {
                return;
            }
        }
        if (node instanceof List<?> l) {
            for (Object o : l) {
                walk(o, counts);
            }
            return;
        }
        Class<?> k = node.getClass();
        if (!k.isRecord() || !k.getName().startsWith("com.legend.sql.") || k.getName().startsWith("com.legend.sql.Type")
                || k.getName().startsWith("com.legend.sql.SqlType") || k.getName().startsWith("com.legend.sql.OutputCol")) {
            return;
        }
        for (RecordComponent rc : k.getRecordComponents()) {
            try {
                walk(rc.getAccessor().invoke(node), counts);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private static long weight(SqlExpr e) {
        long w = 1;
        for (SqlExpr c : e.children()) {
            w += weight(c);
        }
        return w;
    }
}
