// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A QUERY'S {@code data:} SOURCES AS A LOAD (docs/DATA_IN_THE_PLAN_2026_11_09.md L6b, 2026-11-10: data written in the
 * model travels with the plan, and the database receives it through one load at every size -- never a SQL literal):
 * each distinct content a session table named by its digest ({@code legend_data_<16 hex>}), made and filled by a load
 * on the query's target, established on acquisition, and read by the query where the source stood (at any depth: a
 * subquery's, a union's). Two sources of equal content read one table. The table is TEMPORARY -- the session's own,
 * gone with it, as the metamodel's per-query tables are -- and remade on each acquisition (dropped first), so a run
 * reads exactly the plan's rows. The payload is split into its elements verbatim, never re-printed: the database reads
 * each element's JSON text, as it read the whole array until then.
 *
 * @param query   the query reading each source's table
 * @param steps   the load's steps: per content, the table dropped, created and filled
 * @param content the bundle the steps read: each payload's elements, a content blob of inserts (A2)
 */
record DataSources(SqlQuery query, List<ExecutionPlan.LoadStep> steps, Map<String, String> content) {

    /** The one column a source's table holds, as the source's single output names it. */
    static final String COLUMN = "data";

    /** {@code q}'s {@code data:} sources read from their tables, and the load that makes them, on {@code dialect}'s
     *  database (a dialect with no JSON source refuses by name, as it refused the source until then). */
    static DataSources of(SqlQuery q, SqlDialect dialect) {
        Map<String, String> tables = new LinkedHashMap<>();
        List<ExecutionPlan.LoadStep> steps = new ArrayList<>();
        Map<String, String> bundle = new LinkedHashMap<>();
        SqlQuery read = new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (!(s instanceof SqlSource.SourceUrl u) || !u.url().startsWith("data:")) {
                    return s;
                }
                SqlDdl.ColumnType type = dialect.dataSourceColumn(u.url());
                String content = content(u.url());
                String table = tables.computeIfAbsent(content, c -> {
                    String name = "legend_data_" + com.legend.cache.Hash.ofUtf8(c).hex().substring(0, 16);
                    steps.add(new ExecutionPlan.LoadStep.Statement(dialect.render(
                            new SqlDdl.DropTable(null, name, NameQuoting.BARE))));
                    steps.add(new ExecutionPlan.LoadStep.Statement(dialect.render(new SqlDdl.CreateTable(null, name,
                            NameQuoting.BARE, List.of(new SqlDdl.Column(COLUMN, false, type, false, false)), true))));
                    List<List<String>> rows = new ArrayList<>();
                    for (String element : elements(c, u.url())) {
                        rows.add(List.of(element));
                    }
                    // the elements as a content blob of inserts, each element its column's typed literal (A2)
                    com.legend.setup.LoadSteps planned = com.legend.setup.LoadSteps.rows(new com.legend.setup.RowLoad(
                            null, name, NameQuoting.BARE, List.of(COLUMN), Set.of(), 1, rows, List.of(type)), dialect);
                    steps.addAll(planned.steps());
                    bundle.putAll(planned.content());
                    return name;
                });
                return new SqlSource.Table(table, u.alias(), u.outputs(), false, Map.of(COLUMN, type),
                        NameQuoting.BARE);
            }
        }.rewrite(q);
        return new DataSources(read, List.copyOf(steps), java.util.Collections.unmodifiableMap(new LinkedHashMap<>(bundle)));
    }

    /** {@code target} with the load that makes the tables, established on acquisition; {@code target} as it is where
     *  the query reads no {@code data:} source. */
    ExecutionPlan.Target onto(ExecutionPlan.Target target) {
        if (steps.isEmpty()) {
            return target;
        }
        List<ExecutionPlan.Load> loads = new ArrayList<>(target.loads());
        loads.add(LoadPlanner.load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare(), content));
        return new ExecutionPlan.Target(target.database(), target.session(), loads);
    }

    /** The payload after the {@code data:} URL's comma. */
    private static String content(String url) {
        int comma = url.indexOf(',');
        if (comma < 0) {
            throw new IllegalStateException("invalid data: URI (no comma): " + url);
        }
        return url.substring(comma + 1);
    }

    /**
     * A JSON array's elements, each its text as written (whitespace around it dropped): split at the array's own commas,
     * a string's escapes and the nesting of objects and arrays followed. A payload that is not one JSON array is
     * refused by name (the database read it as {@code JSON[]}, which refused it too).
     */
    static List<String> elements(String content, String url) {
        String t = content.strip();
        if (t.length() < 2 || t.charAt(0) != '[' || t.charAt(t.length() - 1) != ']') {
            throw new com.legend.error.NotImplementedException("a data: source's payload is one JSON array: " + url);
        }
        List<String> out = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 1;
        for (int i = 1; i < t.length() - 1; i++) {
            char c = t.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> inString = true;
                case '[', '{' -> depth++;
                case ']', '}' -> depth--;
                case ',' -> {
                    if (depth == 0) {
                        out.add(element(t, start, i, url));
                        start = i + 1;
                    }
                }
                default -> {
                }
            }
            if (depth < 0) {
                throw new com.legend.error.NotImplementedException("a data: source's payload is one JSON array: "
                        + url);
            }
        }
        if (inString || depth != 0) {
            throw new com.legend.error.NotImplementedException("a data: source's payload is one JSON array: " + url);
        }
        String last = t.substring(start, t.length() - 1).strip();
        if (!last.isEmpty() || !out.isEmpty()) {
            out.add(element(t, start, t.length() - 1, url));
        }
        return out;
    }

    private static String element(String t, int from, int to, String url) {
        String e = t.substring(from, to).strip();
        if (e.isEmpty()) {
            throw new com.legend.error.NotImplementedException("a data: source's payload has an empty element: " + url);
        }
        return e;
    }
}
