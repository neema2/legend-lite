// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
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

    /** {@code q} reading each {@code data:} source as {@code dialect}'s database takes it. */
    public static SqlQuery read(SqlQuery q, SqlDialect dialect) {
        return new Rewriter(dialect).rewrite(q);
    }

    /** {@code dml}'s rows and conditions reading each {@code data:} source as {@code dialect}'s database takes it. */
    public static SqlDml read(SqlDml dml, SqlDialect dialect) {
        Rewriter r = new Rewriter(dialect);
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

    /** {@code tree}, a statement's typed tree as its plan carries it, each {@code data:} source whose payload is one of
     *  {@code digests} (its statement's content, bound) named by its digest ({@link SqlSource.SourceUrl#byDigest}):
     *  the payload's text once in the plan, the statement's content (the JSON pass, 2026-11-11). */
    static SqlQuery byDigest(SqlQuery tree, java.util.Set<String> digests) {
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
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

    /** The rewriter: a {@code data:} source becomes the source its dialect delivers it as. */
    private static final class Rewriter extends SqlRewriter {
        private final SqlDialect dialect;

        Rewriter(SqlDialect dialect) {
            this.dialect = dialect;
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
            return dialect.deliver(new SqlDialect.DataPayload(u, elements));
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
