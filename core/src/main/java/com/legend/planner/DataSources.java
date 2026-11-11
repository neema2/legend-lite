// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A STATEMENT'S {@code data:} SOURCES, each read where it stands -- at any depth: a subquery's, a union's, a write's rows
 * -- from the source its database's dialect answers ({@link SqlDialect#deliver}; the JSON pass, 2026-11-11, the user's
 * rulings "move everything off temp table" and, for H2, "inline so we can get rid of all the temp table code
 * completely"): the payload as one bound parameter the database splits, or the statement's own rows. The payload is cut
 * once into its elements verbatim, never re-printed -- an array's elements, or a JSON connection's FRAME's records
 * (its form {@code RECORDS}: an array of records, one record, or a stream of them; an empty payload no record), each
 * beside its place -- and handed to the dialect, whose answer is placed here: the planner decides nothing about how a
 * payload reaches its database. No load, no session table, no release: a statement reads its payload itself, so a
 * rewrite holds no state and every statement is read in the dialect it is written in. (Until the JSON pass each
 * content was a session table its load made, its release dropped, and one body's tables were held to one database.)
 */
public final class DataSources {

    private DataSources() {
    }

    /** {@code q} reading each {@code data:} source as {@code dialect}'s database takes it, in the payload's order where
     *  {@code q} asks none ({@link #inPayloadOrder}). */
    public static SqlQuery read(SqlQuery q, SqlDialect dialect) {
        SqlQuery ordered = inPayloadOrder(q);
        return new Rewriter(dialect, RunValues.in(ordered)).rewrite(ordered);
    }

    /**
     * {@code q} in its payload's order where it asks none (the orchestrator's finding of 2026-11-11, the Databricks
     * lane: a JSON connection's model-to-model query wrote its records' order nowhere), as legend-engine yields a JSON
     * connection's instances: in the array's order (its {@code JsonDataReader} queues the array's elements as it reads
     * them, first in first out). Every delivery numbers the payload's elements from 0, and a select reads each record's
     * place ({@link SqlSource.SourceUrl#PLACE}) where it orders by it:
     * <ul>
     * <li>EVERY SELECT, AT ANY DEPTH, WHOSE LIMIT OR OFFSET PICKS A PAYLOAD'S RECORDS without a sort of its own -- a
     * {@code take}, {@code slice}, {@code drop}, {@code limit} or {@code first} under a filter, a union's arm, a
     * grouping, a subquery or a common table expression (the B checkpoint's tenets audit, M1, 2026-11-11: only the
     * root was ordered, so {@code ->take(2)->filter(...)} kept whichever two records the database read first) -- is
     * ordered by the place, read through the selects between it and the payload where each keeps its records (a
     * filter, a projection, a window: {@link #carried}); a select whose rows are groups (a grouping, an aggregate, a
     * DISTINCT, there or below) by each group's first record's place, its DISTINCT a grouping by its columns, so its
     * LIMIT keeps the first groups as the payload names them;</li>
     * <li>the ROOT, where it asks no order and keeps its records, as the statement's rows: its answer in the payload's
     * order.</li>
     * </ul>
     * Any other select is as its query wrote it: a sort is the query's own (and decides the order above it), and a
     * join, a union, a common table expression's reference or a grouping without a LIMIT asks no order of the
     * payload's.
     */
    static SqlQuery inPayloadOrder(SqlQuery q) {
        SqlQuery limited = new SqlRewriter() {
            @Override
            protected SqlQuery select(SqlSelect s) {
                if (s.limit() == null && s.offset() == null || !s.orderBy().isEmpty()) {
                    return s;
                }
                Carried c = carried(s, true);
                return c == null ? s : c.select().withOrderBy(List.of(SqlSelect.SortKey.asc(c.place())));
            }
        }.rewrite(q);
        if (limited instanceof SqlSelect root && root.orderBy().isEmpty()) {
            Carried c = carried(root, false);
            if (c != null) {
                return c.select().withOrderBy(List.of(SqlSelect.SortKey.asc(c.place())));
            }
        }
        return limited;
    }

    /** A select reading its records' place: {@code place} the expression, in the select's own scope, its order by the
     *  payload's is. */
    private record Carried(SqlSelect select, SqlExpr place) {
    }

    /**
     * {@code s} reading the place of the payload's records it reads -- its source a {@code data:} payload's (placed:
     * {@link SqlSource.SourceUrl#placed}), or a subquery that keeps them and exposes their place (a projection of its
     * own, never seen by a star above it: a star is named as its source's columns) -- with the place in {@code s}'s
     * scope: a record's own, or a group's first record's ({@code MIN}) where {@code s} groups ({@code groups}, a DISTINCT
     * then a grouping by its columns). Null where {@code s} reads no payload's records through such selects, sorts by a
     * key of its own, joins, or groups when {@code groups} is false (the root rule: a grouping asks no order of its
     * own) or windows there.
     */
    private static @com.legend.base.Nullable Carried carried(SqlSelect s, boolean groups) {
        boolean windows = s.qualify() != null || s.projections().stream().anyMatch(p -> windows(p.expr()));
        boolean aggregates = s.projections().stream().anyMatch(p -> com.legend.sql.SqlAgg.aggregatesSelect(p.expr()));
        boolean grouped = s.distinct() || !s.groupBy().isEmpty() || s.having() != null || aggregates;
        if (grouped && !groups || windows && !groups
                || s.distinct() && (!s.groupBy().isEmpty() || s.having() != null || aggregates || windows)) {
            return null;
        }
        SqlSource from = s.from();
        List<OutputCol> own = from.outputs();
        SqlSource placedFrom;
        SqlExpr rowPlace;
        if (from instanceof SqlSource.SourceUrl u && u.url().startsWith("data:")
                && !SqlSource.SourceUrl.namesByDigest(u.url())) {
            OutputCol place = u.outputs().stream().filter(o -> o.name().equals(SqlSource.SourceUrl.PLACE)).findFirst()
                    .orElse(new OutputCol(SqlSource.SourceUrl.PLACE, com.legend.sql.SqlType.Scalar.BIGINT, false,
                            OutputCol.Origin.PHYSICAL));
            List<OutputCol> outputs = new ArrayList<>(u.outputs());
            if (!u.placed()) {
                outputs.add(place);
            }
            placedFrom = new SqlSource.SourceUrl(u.url(), u.alias(), outputs, u.form());
            rowPlace = SqlExpr.Column.of(u.alias(), place);
        } else if (from instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect inner) {
            Carried below = carried(inner, groups);
            if (below == null) {
                return null;
            }
            SqlSelect exposed = below.select();
            OutputCol place = exposed.outputs().stream().filter(o -> o.name().equals(SqlSource.SourceUrl.PLACE))
                    .findFirst().orElse(null);
            if (place == null) {
                place = new OutputCol(SqlSource.SourceUrl.PLACE, com.legend.sql.SqlType.Scalar.BIGINT, false,
                        OutputCol.Origin.DERIVED);
                List<SqlSelect.Projection> ps = new ArrayList<>(exposed.projections());
                ps.add(new SqlSelect.Projection(below.place(), SqlSource.SourceUrl.PLACE, place));
                exposed = exposed.withProjections(ps);
                place = exposed.outputs().get(exposed.outputs().size() - 1);
            }
            placedFrom = new SqlSource.Subselect(exposed, sub.alias(), sub.frameName());
            rowPlace = SqlExpr.Column.of(sub.alias(), place);
        } else {
            return null;
        }
        SqlExpr place = grouped ? com.legend.sql.SqlAgg.Reducer.of(com.legend.sql.SqlAgg.Fn.MIN, rowPlace) : rowPlace;
        if (!s.orderBy().isEmpty() && !placeOrder(s.orderBy())) {
            return null;   // a sort of the query's own: it decides the order
        }
        List<SqlSelect.Projection> named = named(s.projections(), own, from.alias());
        if (named == null) {
            return null;
        }
        SqlSelect out = s.withFrom(placedFrom).withProjections(named);
        if (s.distinct()) {
            // a DISTINCT, a grouping by its columns: ordered by each group's first record
            out = new SqlSelect(out.projections(), false, out.from(), out.where(), named.stream()
                    .map(SqlSelect.Projection::expr).toList(), null, null, out.orderBy(), out.limit(), out.offset(),
                    out.outputs());
        }
        return new Carried(out, place);
    }

    /** {@code projections} with each star named as the columns of {@code own}, its source's ({@code alias}) before the
     *  place was read, so the place is never one of the select's rows; the star of an empty list too. Null where a star
     *  names another source. */
    private static @com.legend.base.Nullable List<SqlSelect.Projection> named(List<SqlSelect.Projection> projections,
            List<OutputCol> own, String alias) {
        if (projections.isEmpty()) {
            return own.stream().map(o -> new SqlSelect.Projection(SqlExpr.Column.of(alias, o), o.name(), o)).toList();
        }
        List<SqlSelect.Projection> out = new ArrayList<>();
        for (SqlSelect.Projection p : projections) {
            java.util.Set<String> except;
            String table;
            if (p.expr() instanceof SqlExpr.Star star) {
                table = star.table();
                except = java.util.Set.of();
            } else if (p.expr() instanceof SqlExpr.StarExcept se) {
                table = se.table();
                except = java.util.Set.copyOf(se.except());
            } else {
                out.add(p);
                continue;
            }
            if (table != null && !table.equals(alias)) {
                return null;
            }
            for (OutputCol o : own) {
                if (!except.contains(o.name())) {
                    out.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, o), o.name(), o));
                }
            }
        }
        return out;
    }

    /** Whether {@code keys} is the payload's order {@link #carried} wrote: one ascending key on a place (the reserved
     *  column {@link SqlSource.SourceUrl#PLACE}), a record's or a group's first record's. */
    private static boolean placeOrder(List<SqlSelect.SortKey> keys) {
        if (keys.size() != 1 || !keys.get(0).ascending()) {
            return false;
        }
        SqlExpr e = keys.get(0).expr();
        if (e instanceof com.legend.sql.SqlAgg.Reducer r && r.fn() == com.legend.sql.SqlAgg.Fn.MIN
                && r.args().size() == 1) {
            e = r.args().get(0);
        }
        return e instanceof SqlExpr.Column c && c.name().equals(SqlSource.SourceUrl.PLACE);
    }

    /** Whether {@code e} windows its rows, at any depth. */
    private static boolean windows(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (windows(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code dml}'s rows and conditions reading each {@code data:} source as {@code dialect}'s database takes it. */
    public static SqlDml read(SqlDml dml, SqlDialect dialect) {
        Rewriter r = new Rewriter(dialect, RunValues.in(dml));
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

    /** {@code tree}, a statement's typed tree as its plan carries it, each {@code data:} payload named by its digest
     *  ({@link SqlSource.SourceUrl#byDigest}), the payload once in the plan (the JSON pass, 2026-11-11): a bound one's
     *  -- one of {@code digests}, its statement's content -- and one written as the statement's own rows
     *  ({@link SqlSource.Values#payload}: the rows the statement's text; until the orchestrator's finding of
     *  2026-11-11 the tree carried every row again as typed nodes, a plan 8 times its payload on Databricks). */
    static SqlQuery byDigest(SqlQuery tree, java.util.Set<String> digests) {
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Values v && v.payload() != null) {
                    return v.payload();
                }
                if (s instanceof SqlSource.SourceUrl u && u.url().startsWith("data:")
                        && !SqlSource.SourceUrl.namesByDigest(u.url())) {
                    String digest = com.legend.cache.Hash.ofUtf8(content(u.url())).hex();
                    if (digests.contains(digest)) {
                        return new SqlSource.SourceUrl(SqlSource.SourceUrl.byDigest(digest), u.alias(), u.outputs(),
                                u.form());
                    }
                }
                return s;
            }
        }.rewrite(tree);
    }

    /** The rewriter: a {@code data:} source becomes the source its dialect delivers it as -- told what its statement
     *  binds besides (the payloads delivered bound before it, each slot counted; whether a value only the run knows is
     *  bound: the B checkpoint's correctness audit, C3, 2026-11-11), so a dialect whose driver bounds a statement's
     *  parameters together decides per statement. One per statement. */
    private static final class Rewriter extends SqlRewriter {
        private final SqlDialect dialect;
        private final boolean runValues;
        /** The characters of the payloads delivered bound so far in the statement. */
        private long bound;

        Rewriter(SqlDialect dialect, boolean runValues) {
            this.dialect = dialect;
            this.runValues = runValues;
        }

        SqlExpr expression(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (!(s instanceof SqlSource.SourceUrl u) || !u.url().startsWith("data:")) {
                return s;
            }
            String content = content(u.url());
            List<String> elements = u.form() == SqlSource.SourceUrl.Form.RECORDS ? records(content, u.url())
                    : elements(content, u.url());
            SqlDialect.DataPayload payload = new SqlDialect.DataPayload(u, elements, bound, runValues);
            SqlSource delivered = dialect.deliver(payload);
            if (delivered instanceof SqlSource.SourceUrl) {
                // bound: one more parameter of the payload's characters
                bound += payload.array().length();
            }
            // a payload written as the statement's own rows is named by its digest (as a bound one's text is), which
            // the typed tree its plan carries reads it by (byDigest): the rows once, the statement's text
            return delivered instanceof SqlSource.Values v ? new SqlSource.Values(v.rows(), v.columns(), v.alias(),
                    v.outputs(), new SqlSource.SourceUrl(SqlSource.SourceUrl.byDigest(com.legend.cache.Hash.ofUtf8(
                            payload.array()).hex()), u.alias(), u.outputs(), u.form())) : delivered;
        }
    }

    /** Whether a statement binds a value only its run knows -- a declared parameter, a keyed region's key, a live
     *  source's admitted path -- whose size no plan can count. */
    private static final class RunValues extends SqlRewriter {
        private boolean found;

        static boolean in(SqlQuery q) {
            RunValues scan = new RunValues();
            scan.rewrite(q);
            return scan.found;
        }

        static boolean in(SqlDml dml) {
            RunValues scan = new RunValues();
            switch (dml) {
                case SqlDml.InsertSelect i -> scan.rewrite(i.query());
                case SqlDml.Merge m -> scan.rewrite(m.query());
                case SqlDml.Update u -> {
                    u.assignments().forEach(a -> scan.rewriteExpr(a.value()));
                    if (u.where() != null) {
                        scan.rewriteExpr(u.where());
                    }
                }
                case SqlDml.DeleteWhere d -> {
                    if (d.where() != null) {
                        scan.rewriteExpr(d.where());
                    }
                }
                case SqlDml.CreateTempTableAs c -> scan.rewrite(c.query());
                // rows written as literals, a staged table's name, a file's path: no query source in them
                case SqlDml.InsertValues v -> {
                }
                case SqlDml.InsertFromTable t -> {
                }
                case SqlDml.DeleteAll d -> {
                }
                case SqlDml.LoadCsv l -> {
                }
            }
            return scan.found;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            found |= e instanceof SqlExpr.PlanParam || e instanceof SqlExpr.KeyCell;
            return e;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            found |= s instanceof SqlSource.SourceUrl u && !u.url().startsWith("data:");
            return s;
        }
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
