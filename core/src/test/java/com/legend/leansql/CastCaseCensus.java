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

    /** {@code bazel run //core:update_cast_case_census}: measured again and written over the source tree's pins. */
    public static void main(String[] args) throws IOException {
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
