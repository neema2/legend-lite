// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
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
 * subquery's, a union's). Two sources of equal content read one table. The table is the session's own, made, read and
 * dropped as its database spells a session table ({@link SqlDialect#dataTable}: never a user's table of the name,
 * docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rules 2 and 4, A4), dropped when the request lets its session go (the
 * target's AT_RELEASE load); a run reads exactly the plan's rows. The payload is split into its elements verbatim,
 * never re-printed: the database reads each element's JSON text. A JSON connection's FRAME (B2a, 2026-11-11: the
 * source reads {@link TypedSourceUrl#ORDINAL}) takes its connection's forms -- an array of records, one record, or a
 * stream of them; an empty payload no record -- and its table holds each record's place beside its text.
 *
 * @param query   the query reading each source's table
 * @param steps   the load's steps: per content, the table dropped, created and filled
 * @param release the release's steps: per content, the table dropped
 * @param content the bundle the steps read: each payload's elements, a content blob of inserts (A2)
 */
public record DataSources(SqlQuery query, List<ExecutionPlan.LoadStep> steps, List<ExecutionPlan.LoadStep> release,
        Map<String, String> content) {

    /** The column of each element's text, as the source's output names it. */
    static final String COLUMN = "data";

    /** A frame's column of each record's place (a JSON connection's: {@link TypedSourceUrl#ORDINAL}). */
    static final String ORDINAL = TypedSourceUrl.ORDINAL;

    /** {@code q}'s {@code data:} sources read from their tables, and the load that makes them, on {@code dialect}'s
     *  database (a dialect with no JSON source refuses by name, as it refused the source until then). */
    static DataSources of(SqlQuery q, SqlDialect dialect) {
        Tables tables = new Tables(dialect);
        SqlQuery read = tables.read(q);
        Tables.Made made = tables.take();
        return new DataSources(read, made.steps(), made.release(), made.content());
    }

    /**
     * The {@code data:} tables of one plan (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §4, A5: a {@code data:} source is
     * rewritten wherever a statement is planned -- a query's, a program's statements and staged lets, a store write's
     * rows, a test body's): each content one table, made by the steps {@link #take} hands on since the last take, read
     * by every statement after. A product query's are its target's acquisition load ({@link #onto}); a program's its
     * own load at the step before the first statement that reads it, dropped after the program's answer
     * ({@code ProgramState.read}). A test body's are one for the body (B2a, 2026-11-11): its judges' statements, planned
     * before its program, read through the same tables as its program's, which places what they made ahead of the step
     * that reads it ({@code ProgramKinds.dataTables}).
     */
    public static final class Tables {
        private final SqlDialect dialect;
        private final Map<Key, SqlDialect.DataTable> tables = new LinkedHashMap<>();
        private final List<ExecutionPlan.LoadStep> steps = new ArrayList<>();
        private final List<ExecutionPlan.LoadStep> release = new ArrayList<>();
        private final List<String> drops = new ArrayList<>();
        private final Map<String, String> bundle = new LinkedHashMap<>();

        /** The tables of one plan written in {@code dialect}. */
        public Tables(SqlDialect dialect) {
            this.dialect = dialect;
        }

        /** The dialect the tables' statements are written in. */
        SqlDialect dialect() {
            return dialect;
        }

        /** A table's identity: its content, and whether it is a frame's (which holds each record's place too). */
        private record Key(String content, boolean frame) {
        }

        /** The steps made since the last take: the new tables' drops, creates and fills, their drops at release (as
         *  a release's steps, and as their statements), and the bundle the fills read. */
        record Made(List<ExecutionPlan.LoadStep> steps, List<ExecutionPlan.LoadStep> release, List<String> drops,
                Map<String, String> content) {
        }

        /** What the reads since the last take made, handed on once. */
        Made take() {
            Made made = new Made(List.copyOf(steps), List.copyOf(release), List.copyOf(drops),
                    java.util.Collections.unmodifiableMap(new LinkedHashMap<>(bundle)));
            steps.clear();
            release.clear();
            drops.clear();
            bundle.clear();
            return made;
        }

        /** {@code q} reading each {@code data:} source's table, each table not made before made by the steps the next
         *  take hands on. */
        public SqlQuery read(SqlQuery q) {
            return rewriter().rewrite(q);
        }

        /** {@code dml}'s rows and conditions reading each {@code data:} source's table. */
        SqlDml read(SqlDml dml) {
            Rewriter r = rewriter();
            return switch (dml) {
                case SqlDml.InsertSelect i -> new SqlDml.InsertSelect(i.schema(), i.table(), i.quoting(), i.columns(),
                        i.quotedColumns(), r.rewrite(i.query()));
                case SqlDml.Merge m -> new SqlDml.Merge(m.schema(), m.table(), m.quoting(), m.columns(),
                        m.quotedColumns(), m.keys(), r.rewrite(m.query()), m.alias(), m.sourceAlias());
                case SqlDml.Update u -> new SqlDml.Update(u.schema(), u.table(), u.quoting(), u.alias(),
                        u.storedTypes(), u.assignments().stream().map(a -> new SqlDml.Update.Assignment(a.column(),
                                a.quoted(), r.expression(a.value()))).toList(),
                        u.where() == null ? null : r.expression(u.where()));
                case SqlDml.DeleteWhere d -> new SqlDml.DeleteWhere(d.schema(), d.table(), d.quoting(), d.alias(),
                        d.storedTypes(), d.where() == null ? null : r.expression(d.where()));
                case SqlDml.CreateTempTableAs c -> new SqlDml.CreateTempTableAs(c.table(), r.rewrite(c.query()),
                        c.columns(), c.staged());
                // rows written as literals, a staged table's name, a file's path: no query source in them
                case SqlDml.InsertValues v -> v;
                case SqlDml.InsertFromTable t -> t;
                case SqlDml.DeleteAll d -> d;
                case SqlDml.LoadCsv l -> l;
            };
        }

        private Rewriter rewriter() {
            return new Rewriter();
        }

        /** The rewriter: a {@code data:} source becomes its content's table, made the first time it is read. */
        private final class Rewriter extends SqlRewriter {
            SqlExpr expression(SqlExpr e) {
                return rewriteExpr(e);
            }

            @Override
            protected SqlSource source(SqlSource s) {
                if (!(s instanceof SqlSource.SourceUrl u) || !u.url().startsWith("data:")) {
                    return s;
                }
                SqlDdl.ColumnType type = dialect.dataSourceColumn(u.url());
                boolean frame = u.outputs().stream().anyMatch(o -> o.name().equals(ORDINAL));
                SqlDialect.DataTable table = tables.computeIfAbsent(new Key(content(u.url()), frame),
                        k -> made(k, u.url(), type));
                Map<String, SqlDdl.ColumnType> stored = frame ? Map.of(COLUMN, type, ORDINAL, ORDINAL_TYPE)
                        : Map.of(COLUMN, type);
                return new SqlSource.Table(table.read(), u.alias(), u.outputs(), false, stored, NameQuoting.BARE);
            }
        }

        /** The table of {@code key}: made, filled and released as its database spells a session table. */
        private SqlDialect.DataTable made(Key key, String url, SqlDdl.ColumnType type) {
            String base = "legend_data_" + com.legend.cache.Hash.ofUtf8(key.content()).hex().substring(0, 16)
                    + (key.frame() ? "_o" : "");
            List<SqlDdl.Column> columns = new ArrayList<>(List.of(new SqlDdl.Column(COLUMN, false, type, false, false)));
            List<SqlDdl.ColumnType> types = new ArrayList<>(List.of(type));
            if (key.frame()) {
                columns.add(new SqlDdl.Column(ORDINAL, false, ORDINAL_TYPE, false, false));
                types.add(ORDINAL_TYPE);
            }
            SqlDialect.DataTable table = dialect.dataTable(base, columns);
            for (String statement : table.make()) {
                steps.add(new ExecutionPlan.LoadStep.Statement(statement));
            }
            for (String statement : table.release()) {
                release.add(new ExecutionPlan.LoadStep.Statement(statement));
                drops.add(statement);
            }
            List<String> elements = key.frame() ? records(key.content(), url) : elements(key.content(), url);
            List<List<String>> rows = new ArrayList<>();
            for (int i = 0; i < elements.size(); i++) {
                rows.add(key.frame() ? List.of(elements.get(i), String.valueOf(i)) : List.of(elements.get(i)));
            }
            // the elements as a content blob of inserts, each element its column's typed literal (A2)
            com.legend.setup.LoadSteps planned = com.legend.setup.LoadSteps.rows(new com.legend.setup.RowLoad(
                    table.schema(), table.name(), NameQuoting.BARE, columns.stream().map(SqlDdl.Column::name).toList(),
                    Set.of(), columns.size(), rows, types), dialect,
                    "the data: source's payload (" + rows.size() + " element(s), its session table " + table.read()
                            + ")");
            steps.addAll(planned.steps());
            bundle.putAll(planned.content());
            return table;
        }
    }

    /** A frame's place column's type. */
    private static final SqlDdl.ColumnType ORDINAL_TYPE = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER);

    /** {@code target} with the load that makes the tables, established on acquisition; {@code target} as it is where
     *  the query reads no {@code data:} source. */
    ExecutionPlan.Target onto(ExecutionPlan.Target target) {
        if (steps.isEmpty()) {
            return target;
        }
        List<ExecutionPlan.Load> loads = new ArrayList<>(target.loads());
        loads.add(LoadPlanner.load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare(), content));
        loads.add(LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_RELEASE, release, target.bare(), Map.of()));
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

    /**
     * A JSON connection's records, each an object's text as written: an array's objects, one object, or a stream of
     * objects (the connection's concatenated-rows spelling, whitespace between them); an empty payload no record. Cut
     * lexically, a string's escapes and the nesting followed, as {@link #elements} cuts an array; anything else is
     * refused by name.
     */
    static List<String> records(String content, String url) {
        String t = content.strip();
        if (t.isEmpty()) {
            return List.of();
        }
        if (t.charAt(0) == '[') {
            List<String> out = elements(t, url);
            for (String e : out) {
                if (e.charAt(0) != '{' || objectEnd(e, 0, url) != e.length()) {
                    throw notRecords(url);
                }
            }
            return out;
        }
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < t.length()) {
            if (Character.isWhitespace(t.charAt(i))) {
                i++;
                continue;
            }
            if (t.charAt(i) != '{') {
                throw notRecords(url);
            }
            int end = objectEnd(t, i, url);
            out.add(t.substring(i, end));
            i = end;
        }
        return out;
    }

    /** The index just past the object that opens at {@code from}; an object left open refused by name. */
    private static int objectEnd(String t, int from, String url) {
        int depth = 0;
        boolean inString = false;
        for (int i = from; i < t.length(); i++) {
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
                case ']', '}' -> {
                    depth--;
                    if (depth == 0) {
                        return i + 1;
                    }
                }
                default -> {
                }
            }
        }
        throw notRecords(url);
    }

    private static com.legend.error.NotImplementedException notRecords(String url) {
        String shown = url.length() > 120 ? url.substring(0, 120) + "..." : url;
        return new com.legend.error.NotImplementedException("a JSON connection's payload is an object, an array of"
                + " objects or a stream of objects: " + shown);
    }

    private static String element(String t, int from, int to, String url) {
        String e = t.substring(from, to).strip();
        if (e.isEmpty()) {
            throw new com.legend.error.NotImplementedException("a data: source's payload has an empty element: " + url);
        }
        return e;
    }
}
