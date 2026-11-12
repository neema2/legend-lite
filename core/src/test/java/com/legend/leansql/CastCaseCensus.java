// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.json.Json;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.testbackend.StorelessRuntime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE CAST-AND-CASE CENSUS (E-4 of the total-wall program, the user's rule ruled 2026-11-12: every CAST and CASE in
 * product SQL traces to the user's own call or to a named product rule; docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md
 * §5.8). Channel A's cases lowered on every relational database the platform implements, each dialect's passes run,
 * and every {@link SqlExpr.Cast} and {@link SqlExpr.Case} counted by the {@link SqlExpr.Reason} the node that made it
 * named (a node cannot be made without one). The counts per database and reason are pinned in
 * {@code lean-sql/cast-case-census.tsv}: a count that GROWS fails (a new cast or case names its reason and is re-pinned
 * with a dated justification), one that shrinks past 1% asks for a re-pin. Re-pin with
 * {@code bazel run //core:update_cast_case_census}.
 */
public final class CastCaseCensus {

    private CastCaseCensus() {
    }

    /** The pins' file in the source tree. */
    static final String PINS = "core/src/test/resources/lean-sql/cast-case-census.tsv";

    static final String HEADER = "database\treason\tcasts\tcases";

    /** One row's key. */
    public record Key(String database, String reason) implements Comparable<Key> {
        @Override
        public int compareTo(Key o) {
            int c = database.compareTo(o.database);
            return c != 0 ? c : reason.compareTo(o.reason);
        }
    }

    /** Channel A's set ({@code {"model", "expression"}} lines) counted on every relational database. */
    public static Map<Key, long[]> measure(Path set) throws IOException {
        Map<Key, long[]> out = new TreeMap<>();
        List<String> lines = Files.readAllLines(set, StandardCharsets.UTF_8);
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            if (!LeanSqlBaseline.relational(type)) {
                continue;
            }
            com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(type);
            for (SqlExpr.Reason r : SqlExpr.Reason.values()) {
                out.put(new Key(type.name(), r.code()), new long[2]);
            }
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj c = Json.parseObject(line);
                String model = ((Json.Str) c.get("model")).value();
                String expression = ((Json.Str) c.get("expression")).value();
                SqlQuery passed;
                try {
                    TypedQuery q = Compiler.query(Compiler.compileModel(LeanSqlBaseline.withRuntime(model, type)),
                            expression);
                    SqlQuery lowered = q.lower(StorelessRuntime.RUNTIME, false, List.of(),
                            java.util.Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING)).plan();
                    passed = com.legend.sql.dialect.PassedTrees.passed(dialect, lowered);
                } catch (RuntimeException | StackOverflowError e) {
                    continue;   // a case refused at planning (the size baseline counts those)
                }
                new Counter(type.name(), out).rewriteRoot(passed);
            }
        }
        return out;
    }

    /** Counts each cast and case it visits, by reason. */
    private static final class Counter extends com.legend.sql.SqlRewriter {

        private final String database;
        private final Map<Key, long[]> into;

        Counter(String database, Map<Key, long[]> into) {
            this.database = database;
            this.into = into;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            switch (e) {
                case SqlExpr.Cast c -> into.get(new Key(database, c.reason().code()))[0]++;
                case SqlExpr.Case c -> into.get(new Key(database, c.reason().code()))[1]++;
                default -> {
                }
            }
            return e;
        }
    }

    /** What changed from {@code pinned} to {@code measured}, by the rule: growth fails, a shrink past 1% asks. */
    public static List<String> drift(Map<Key, long[]> pinned, Map<Key, long[]> measured) {
        List<String> out = new ArrayList<>();
        for (var e : measured.entrySet()) {
            long[] was = pinned.getOrDefault(e.getKey(), new long[2]);
            String[] names = {"casts", "cases"};
            for (int i = 0; i < 2; i++) {
                long now = e.getValue()[i];
                if (now > was[i]) {
                    out.add(e.getKey() + ": " + names[i] + " GREW " + was[i] + " -> " + now
                            + " (a new cast or case names its reason and is re-pinned with a dated justification)");
                } else if (was[i] - now > Math.max(1, was[i] / 100)) {
                    out.add(e.getKey() + ": " + names[i] + " shrank " + was[i] + " -> " + now
                            + " -- re-pin (bazel run //core:update_cast_case_census), dated in the commit");
                }
            }
        }
        return out;
    }

    static String write(Map<Key, long[]> rows) {
        StringBuilder b = new StringBuilder(HEADER).append('\n');
        new TreeMap<>(rows).forEach((k, v) -> b.append(k.database()).append('\t').append(k.reason()).append('\t')
                .append(v[0]).append('\t').append(v[1]).append('\n'));
        return b.toString();
    }

    static Map<Key, long[]> read(String text) {
        Map<Key, long[]> out = new TreeMap<>();
        for (String line : text.split("\n")) {
            if (line.isEmpty() || line.equals(HEADER) || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t");
            out.put(new Key(f[0], f[1]), new long[] {Long.parseLong(f[2]), Long.parseLong(f[3])});
        }
        return out;
    }

    /** Where the {@code print} nodes sit (the compatibility frame's §6, 2026-11-12; a measurement, unpinned): AT THE
     *  RESULT -- the top of a root projection's expression, or a chain of print nodes from it (presentation: a writer
     *  could spell it) -- or INSIDE the query (a filter, a join, a key, a concatenation, an argument: printing the
     *  query computes with). Per database: {result casts, result cases, inside casts, inside cases}. */
    public static Map<String, long[]> printPlacement(Path set) throws IOException {
        Map<String, long[]> out = new TreeMap<>();
        List<String> lines = Files.readAllLines(set, StandardCharsets.UTF_8);
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            if (!LeanSqlBaseline.relational(type)) {
                continue;
            }
            com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(type);
            long[] counts = out.computeIfAbsent(type.name(), k -> new long[4]);
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj c = Json.parseObject(line);
                SqlQuery passed;
                try {
                    TypedQuery q = Compiler.query(Compiler.compileModel(LeanSqlBaseline.withRuntime(
                            ((Json.Str) c.get("model")).value(), type)), ((Json.Str) c.get("expression")).value());
                    passed = com.legend.sql.dialect.PassedTrees.passed(dialect, q.lower(StorelessRuntime.RUNTIME,
                            false, List.of(), java.util.Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING))
                            .plan());
                } catch (RuntimeException | StackOverflowError e) {
                    continue;
                }
                java.util.Set<SqlExpr> atResult = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
                if (passed instanceof com.legend.sql.SqlSelect root) {
                    for (com.legend.sql.SqlSelect.Projection p : root.projections()) {
                        resultChain(p.expr(), atResult);
                    }
                }
                new com.legend.sql.SqlRewriter() {
                    @Override
                    protected SqlExpr expr(SqlExpr e) {
                        if (type.name().equals("DuckDB") && !isPrint(e)) {
                            for (SqlExpr child : e.children()) {
                                if (isPrint(child)) {
                                    String label = switch (e) {
                                        case SqlExpr.Call k -> "call " + k.fn();
                                        case SqlExpr.Cast k -> "cast(" + k.reason().code() + ")";
                                        case SqlExpr.Case k -> "case(" + k.reason().code() + ")";
                                        default -> e.getClass().getSimpleName();
                                    };
                                    HOLDERS.merge(label, 1L, Long::sum);
                                }
                            }
                        }
                        boolean result = atResult.contains(e);
                        switch (e) {
                            case SqlExpr.Cast x when x.reason() == SqlExpr.Reason.PRINT -> counts[result ? 0 : 2]++;
                            case SqlExpr.Case x when x.reason() == SqlExpr.Reason.PRINT -> counts[result ? 1 : 3]++;
                            default -> {
                            }
                        }
                        return e;
                    }
                }.rewriteRoot(passed);
            }
        }
        return out;
    }

    /** DuckDB's inside print chains, by the node holding each (a measurement's scratch; the manual mode alone). */
    private static final Map<String, Long> HOLDERS = new TreeMap<>();

    private static boolean isPrint(SqlExpr e) {
        return (e instanceof SqlExpr.Cast x && x.reason() == SqlExpr.Reason.PRINT)
                || (e instanceof SqlExpr.Case y && y.reason() == SqlExpr.Reason.PRINT);
    }

    /** {@code e} and, while it is a print node, its children: the result's own printing. */
    private static void resultChain(SqlExpr e, java.util.Set<SqlExpr> into) {
        boolean print = (e instanceof SqlExpr.Cast x && x.reason() == SqlExpr.Reason.PRINT)
                || (e instanceof SqlExpr.Case y && y.reason() == SqlExpr.Reason.PRINT);
        if (!print) {
            return;
        }
        into.add(e);
        for (SqlExpr child : e.children()) {
            resultChain(child, into);
        }
    }

    /** {@code bazel run //core:update_cast_case_census}: measured again and written over the source tree's pins
     *  ({@code -- placement}: the print nodes' placement printed instead, nothing written). */
    public static void main(String[] args) throws IOException {
        if (args.length == 1 && args[0].equals("placement")) {
            printPlacement(com.legend.testing.Runfile.property("legend.lean_sql.channel_a")).forEach((d, v) ->
                    System.out.println("print placement " + d + ": at the result " + v[0] + " casts, " + v[1]
                            + " cases; inside the query " + v[2] + " casts, " + v[3] + " cases"));
            HOLDERS.entrySet().stream().sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .forEach(h -> System.out.println("print held by (DuckDB) " + h.getKey() + ": " + h.getValue()));
            return;
        }
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("run it with bazel run //core:update_cast_case_census");
        }
        Map<Key, long[]> measured = measure(com.legend.testing.Runfile.property("legend.lean_sql.channel_a"));
        Files.writeString(Path.of(workspace, PINS), write(measured), StandardCharsets.UTF_8);
        Map<String, long[]> byReason = new TreeMap<>();
        measured.forEach((k, v) -> {
            long[] t = byReason.computeIfAbsent(k.reason(), r -> new long[2]);
            t[0] += v[0];
            t[1] += v[1];
        });
        byReason.forEach((r, v) -> System.out.println("cast-case census " + r + ": casts " + v[0] + ", cases " + v[1]));
    }
}
