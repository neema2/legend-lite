// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * THE STORED READS (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, 4.3; ruled 2026-10-02): a table
 * column whose DECLARED store type the dialect cannot use as the database holds it -- a type
 * Pure cannot name, read as text; on Postgres a nested value, read as {@code jsonb} -- is read
 * by that type at EVERY reference: projected, filtered, joined on, grouped, sorted, partitioned.
 * A Pure String then behaves as a string in the database, and every clause sees the same value.
 *
 * <p>The facts: each scanned {@link SqlSource.Table} carries its columns' stored types (the
 * Lowerer stamps them); the dialect says which it reads ({@code reads}). Aliases are unique per
 * statement (the {@link SourceSpelling} precedent), so a reference qualified by a table's alias
 * is a reference to that table's column, over the whole statement, correlated ones included.
 * Each becomes a {@link SqlExpr.StoredRead}. A star over such a table is spelled out, column by
 * column, so the read reaches the columns it would have carried unread; each read keeps its
 * column's label (the dialect's {@code implicitLabel}).
 *
 * <p>An UNQUALIFIED reference to a read column cannot be resolved without guessing its scope,
 * and the lowering builds none (measured 2026-10-02: no unqualified physical reference to a
 * table in its own select's FROM across the core suite, the stress suites, every spec corpus and
 * PCT on DuckDB and H2): one is refused by name, never read raw. So is a PIVOT straight over such
 * a table, whose arguments are unqualified by construction.
 *
 * <p>Registered LAST on every dialect ({@code renderPasses}): every source the passes introduced
 * is in scope, and every reference they built is read. A statement over no such table is
 * returned as it came (the same instance): wherever the read is identity, nothing changes.
 */
final class StoredReads extends SqlRewriter {

    private final Predicate<SqlDdl.ColumnType> reads;
    /** alias of a scanned table -> its columns this dialect reads, by name -> stored type. */
    private final Map<String, Map<String, SqlDdl.ColumnType>> read = new HashMap<>();

    StoredReads(Predicate<SqlDdl.ColumnType> reads) {
        this.reads = reads;
    }

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        read.clear();
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Table t) {
                    Map<String, SqlDdl.ColumnType> cols = new HashMap<>();
                    t.storedTypes().forEach((name, type) -> {
                        if (reads.test(type)) {
                            cols.put(name, type);
                        }
                    });
                    if (!cols.isEmpty()) {
                        read.put(t.alias(), java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(cols)));
                    }
                }
                return s;
            }
        }.rewrite(q);
        return read.isEmpty() ? q : rewrite(q);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Column c && c.table() != null) {
            Map<String, SqlDdl.ColumnType> cols = read.get(c.table());
            if (cols != null && cols.containsKey(c.name())) {
                return new SqlExpr.StoredRead(c, cols.get(c.name()));
            }
        }
        return e;
    }

    @Override
    protected SqlSource source(SqlSource s) {
        if (s instanceof SqlSource.Pivot p) {
            List<SqlSource.Table> under = new ArrayList<>();
            leaves(p.source(), under);
            for (SqlSource.Table t : under) {
                if (read.containsKey(t.alias())) {
                    throw new DialectCapability("a PIVOT straight over table '" + t.name()
                            + "' cannot read its columns " + read.get(t.alias()).keySet()
                            + " by their declared types");
                }
            }
        }
        return s;
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        List<SqlSource> leaves = Stars.sources(s.from());
        Map<String, String> unqualified = new HashMap<>();   // read column -> its table
        boolean any = false;
        for (SqlSource l : leaves) {
            if (l instanceof SqlSource.Table t && read.containsKey(t.alias())) {
                any = true;
                read.get(t.alias()).keySet().forEach(n -> unqualified.put(n, t.name()));
            }
        }
        if (!any) {
            return s;
        }
        refuseUnqualified(s, unqualified);
        if (s.projections().isEmpty()) {
            return withProjections(s, everyColumn(leaves, null, List.of()));
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        boolean expanded = false;
        for (SqlSelect.Projection p : s.projections()) {
            List<SqlSelect.Projection> spelled = p.expr() instanceof SqlExpr.Star st
                    ? starOver(leaves, st.table(), List.of())
                    : p.expr() instanceof SqlExpr.StarExcept se ? starOver(leaves, se.table(), se.except())
                    : null;
            if (spelled == null) {
                ps.add(p);
            } else {
                ps.addAll(spelled);
                expanded = true;
            }
        }
        return expanded ? withProjections(s, ps) : s;
    }

    /** A star's columns spelled out, when it reaches a read table; null when it does not. */
    private @com.legend.base.Nullable List<SqlSelect.Projection> starOver(List<SqlSource> leaves,
            @com.legend.base.Nullable String table, List<String> except) {
        boolean reaches = table == null
                ? leaves.stream().anyMatch(l -> l instanceof SqlSource.Table t && read.containsKey(t.alias()))
                : read.containsKey(table);
        return reaches ? everyColumn(leaves, table, except) : null;
    }

    /** The columns {@code *} (or {@code table.*}) stands for, in FROM order, each read as stored. */
    private List<SqlSelect.Projection> everyColumn(List<SqlSource> leaves,
            @com.legend.base.Nullable String table, List<String> except) {
        return Stars.columns(leaves, table, except, this::expr);
    }

    private static SqlSelect withProjections(SqlSelect s, List<SqlSelect.Projection> ps) {
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(),
                s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    /** Refuses, by name, an unqualified physical reference in this select's own clauses to a read
     *  column of a table in its FROM (subqueries are their own selects). */
    private static void refuseUnqualified(SqlSelect s, Map<String, String> readNames) {
        List<SqlExpr> own = new ArrayList<>();
        s.projections().forEach(p -> own.add(p.expr()));
        if (s.where() != null) {
            own.add(s.where());
        }
        own.addAll(s.groupBy());
        if (s.having() != null) {
            own.add(s.having());
        }
        if (s.qualify() != null) {
            own.add(s.qualify());
        }
        s.orderBy().forEach(k -> own.add(k.expr()));
        joinConditions(s.from(), own);
        for (SqlExpr e : own) {
            refuseUnqualified(e, readNames);
        }
    }

    private static void refuseUnqualified(SqlExpr e, Map<String, String> readNames) {
        if (e instanceof SqlExpr.Column c && c.table() == null && c.origin() != OutputCol.Origin.DERIVED
                && readNames.containsKey(c.name())) {
            throw new DialectCapability("an unqualified reference to column '" + c.name() + "' of table '"
                    + readNames.get(c.name()) + "' cannot be read by its declared type");
        }
        for (SqlExpr child : e.children()) {
            refuseUnqualified(child, readNames);
        }
    }

    private static void joinConditions(SqlSource s, List<SqlExpr> out) {
        if (s instanceof SqlSource.Join j) {
            joinConditions(j.left(), out);
            joinConditions(j.right(), out);
            if (j.on() != null) {
                out.add(j.on());
            }
        }
    }

    private static void leaves(SqlSource s, List<SqlSource.Table> out) {
        for (SqlSource l : Stars.sources(s)) {
            if (l instanceof SqlSource.Table t) {
                out.add(t);
            }
        }
    }
}
