// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE OPERATIONS A DIALECT WRITES AS QUERIES OF THEIR OWN ({@link SelectMerge}'s {@code queried} rule, 2026-11-19, the
 * compatibility program's H11 record §8 P1): a window's or an aggregate's value merged under one is computed over that
 * query's rows, so the merge keeps it a column of its level there. Each dialect's declaration is held to what its
 * renderer writes, measured here on every {@link SqlFn} over typed columns and literals: SQLite's and Postgres's sets
 * name exactly the operations whose operands they write inside a query (a correlated subquery, a recursive one, a
 * table function in a FROM), less the ones this census cannot build (named below, each read in its arm); DuckDB, H2,
 * ClickHouse and Snowflake write every operation, and every cast, in place (their UNNEST is row-multiplying, refused
 * by the merge as a level of its own). Then the rule at work: a window read through a list operation stays a level on
 * SQLite and Postgres, and merges on DuckDB as before.
 */
class SelectMergeQueriedOperandsTest {

    private static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(), false, Map.of(),
            com.legend.sql.NameQuoting.BARE);

    private static final List<SqlType> TYPES = List.of(SqlType.Scalar.VARCHAR, SqlType.Scalar.BIGINT,
            SqlType.Scalar.DOUBLE, SqlType.Scalar.BOOLEAN, SqlType.Scalar.DATE, SqlType.Scalar.TIMESTAMP,
            SqlType.Scalar.JSON, new SqlType.Array(SqlType.Scalar.VARCHAR), new SqlType.Array(SqlType.Scalar.BIGINT));

    /** The operations the census cannot build (a lambda, an entry list, a map, a format), each read in its arm. */
    private static final Set<SqlFn> SQLITE_UNPROBED = EnumSet.of(SqlFn.LIST_FILTER, SqlFn.LIST_TRANSFORM,
            SqlFn.LIST_EXISTS, SqlFn.LIST_FOR_ALL, SqlFn.LIST_REDUCE, SqlFn.MAP_FROM_ENTRIES);
    private static final Set<SqlFn> POSTGRES_UNPROBED = EnumSet.of(SqlFn.LIST_FILTER, SqlFn.LIST_TRANSFORM,
            SqlFn.LIST_EXISTS, SqlFn.LIST_FOR_ALL, SqlFn.LIST_REDUCE, SqlFn.MAP_FROM_ENTRIES, SqlFn.MAP_EXTRACT,
            SqlFn.MAP_KEYS, SqlFn.MAP_VALUES, SqlFn.FORMAT);

    private static SqlExpr col(String n, SqlType t) {
        return SqlExpr.Column.of("t0", n, t, true, OutputCol.Origin.PHYSICAL);
    }

    /** Whether {@code sql}, one select over {@code T}, writes a query inside its projection. */
    private static boolean nested(String sql) {
        String u = sql.toUpperCase(java.util.Locale.ROOT);
        return u.split("\\bSELECT\\b", -1).length - 1 > 1 || u.contains("WITH RECURSIVE") || u.contains("JSON_EACH(")
                || u.contains("JSONB_EACH") || u.contains("UNNEST(") || u.contains("GENERATE_SERIES(");
    }

    private static String rendered(SqlDialect d, SqlExpr e) {
        return d.render(SqlSelect.starOf(T).withProjections(List.of(new SqlSelect.Projection(e, "x", null))));
    }

    /** The operations {@code d} writes with a query of its own on some probe: every arity to four, every probe type,
     *  the operands columns, or a column and literals. A probe the dialect refuses is not this census's. */
    private static Set<SqlFn> writtenAsQueries(SqlDialect d) {
        Set<SqlFn> out = EnumSet.noneOf(SqlFn.class);
        for (SqlFn fn : SqlFn.values()) {
            probes:
            for (int arity = 0; arity <= 4; arity++) {
                for (SqlType t : TYPES) {
                    for (int mode = 0; mode < 3; mode++) {
                        List<SqlExpr> args = new ArrayList<>();
                        for (int i = 0; i < arity; i++) {
                            args.add(i == 0 || mode == 0 ? col("a" + i, t)
                                    : mode == 1 ? new SqlExpr.StringLit("x") : new SqlExpr.IntLit(1));
                        }
                        try {
                            if (nested(rendered(d, new SqlExpr.Call(fn, args)))) {
                                out.add(fn);
                                break probes;
                            }
                        } catch (RuntimeException | StackOverflowError notThisProbe) {
                            // not this arity, or not this type: refused by the dialect, not written
                        }
                    }
                }
            }
        }
        return out;
    }

    private static Set<String> names(Set<SqlFn> fns) {
        Set<String> out = new TreeSet<>();
        fns.forEach(f -> out.add(f.name()));
        return out;
    }

    @Test
    void sqliteDeclaresExactlyTheOperationsItWritesAsQueries() {
        declaredExactly(new SQLite(), SQLite.OPERANDS_IN_A_QUERY, SQLITE_UNPROBED);
    }

    @Test
    void postgresDeclaresExactlyTheOperationsItWritesAsQueries() {
        declaredExactly(new Postgres(), Postgres.OPERANDS_IN_A_QUERY, POSTGRES_UNPROBED);
    }

    private static void declaredExactly(SqlDialect d, Set<SqlFn> declared, Set<SqlFn> unprobed) {
        Set<SqlFn> measured = writtenAsQueries(d);
        Set<SqlFn> undeclared = EnumSet.copyOf(measured);
        undeclared.removeAll(declared);
        assertEquals(Set.of(), names(undeclared), d.getClass().getSimpleName()
                + " writes these as queries of their own and does not declare them (OPERANDS_IN_A_QUERY): a window"
                + " merged under one is computed over that query's rows");
        Set<SqlFn> stale = EnumSet.copyOf(declared);
        stale.removeAll(measured);
        stale.removeAll(unprobed);
        assertEquals(Set.of(), names(stale), d.getClass().getSimpleName()
                + " declares these and writes none as a query: a merge refused for nothing");
        Set<SqlFn> unprobedButMeasured = EnumSet.copyOf(unprobed);
        unprobedButMeasured.retainAll(measured);
        assertEquals(Set.of(), names(unprobedButMeasured), "measured now: off the unprobed list");
        assertTrue(declared.containsAll(unprobed), "every unprobed operation is declared");
    }

    @Test
    void theOtherMergingDialectsWriteEveryOperationAndCastInPlace() {
        Map<String, SqlDialect> inPlace = new LinkedHashMap<>();
        inPlace.put("DuckDB", new DuckDb());
        inPlace.put("H2", new H2());
        inPlace.put("ClickHouse", new ClickHouse());
        inPlace.put("Snowflake", new Snowflake());
        for (var d : inPlace.entrySet()) {
            Set<SqlFn> queries = writtenAsQueries(d.getValue());
            // UNNEST multiplies rows: SelectMerge keeps it a level of its own on every dialect (grouping)
            queries.remove(SqlFn.UNNEST);
            assertEquals(Set.of(), names(queries), d.getKey() + " writes these as queries of their own: its"
                    + " SelectMerge is IN_PLACE, so a window merged under one would be computed there");
            for (SqlType from : TYPES) {
                for (SqlType to : TYPES) {
                    for (SqlExpr.Reason reason : List.of(SqlExpr.Reason.USER, SqlExpr.Reason.OPERATOR)) {
                        String sql;
                        try {
                            sql = rendered(d.getValue(), new SqlExpr.Cast(col("a0", from), to, reason));
                        } catch (RuntimeException notThisCast) {
                            continue;
                        }
                        assertFalse(nested(sql), d.getKey() + " writes a cast " + from + " -> " + to
                                + " as a query of its own: " + sql);
                    }
                }
            }
        }
    }

    /** {@code joinStrings(sort(split(w, ':')), '')} over a select whose {@code w} is a group's names joined by a
     *  window: the shape of PCT's testExtendJoinStringOnNull, as the lowering makes it (the window a level below). */
    private static SqlSelect windowReadThroughAList() {
        return readThroughAList(true);
    }

    /** The same over {@code w} a row's own value (the name itself) where {@code windowed} is false. */
    private static SqlSelect readThroughAList(boolean windowed) {
        SqlExpr grp = SqlExpr.Column.of("t0", "grp", SqlType.Scalar.BIGINT, true, OutputCol.Origin.PHYSICAL);
        SqlExpr name = SqlExpr.Column.of("t0", "name", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);
        OutputCol w = new OutputCol("w", SqlType.Scalar.VARCHAR, true);
        SqlExpr window = windowed ? new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(name, new SqlExpr.StringLit(":")), false, List.of()), List.of(grp), List.of(), null) : name;
        SqlSelect below = new SqlSelect(List.of(new SqlSelect.Projection(grp, "grp", null),
                new SqlSelect.Projection(window, "w", w)), false, T, null, List.of(), null, null, List.of(), null,
                null, List.of(new OutputCol("grp", SqlType.Scalar.BIGINT, true), w));
        SqlExpr read = SqlExpr.Column.of("t1", w);
        SqlExpr sorted = SqlExpr.Call.of(SqlFn.LIST_SORT, SqlExpr.Call.of(SqlFn.SPLIT, read,
                new SqlExpr.StringLit(":")));
        SqlExpr joined = SqlExpr.Call.of(SqlFn.LENGTH, SqlExpr.Call.of(SqlFn.LIST_REVERSE, sorted));
        return SqlSelect.starOf(new SqlSource.Subselect(below, "t1", null)).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of("t1", "grp", SqlType.Scalar.BIGINT, true,
                        OutputCol.Origin.DERIVED), "grp", null),
                new SqlSelect.Projection(joined, "n", null)));
    }

    @Test
    void aWindowReadThroughAListOperationStaysALevelWhereTheListIsAQuery() {
        SqlSelect q = windowReadThroughAList();
        for (var keeps : List.of(
                new SelectMerge(java.util.function.UnaryOperator.identity(),
                        SelectMerge.queriedBy(SQLite.OPERANDS_IN_A_QUERY)),
                new SelectMerge(n -> n.toLowerCase(java.util.Locale.ROOT),
                        SelectMerge.queriedBy(Postgres.OPERANDS_IN_A_QUERY)))) {
            SqlQuery out = keeps.rewriteRoot(q);
            SqlSelect top = assertInstanceOf(SqlSelect.class, out);
            assertInstanceOf(SqlSource.Subselect.class, top.from(), "the window's level kept: " + out);
        }
        // DuckDB writes the list operations in place: the window merged into its reader, one level, as before
        SqlSelect merged = assertInstanceOf(SqlSelect.class, new SelectMerge(java.util.function.UnaryOperator.identity(),
                SelectMerge.IN_PLACE).rewriteRoot(q));
        assertInstanceOf(SqlSource.Table.class, merged.from(), "DuckDB's one level: " + merged);
        // and the same select's plain column (not a level's value) still merges on SQLite: only a window's, an
        // aggregate's or an UNNEST's value is kept below
        SqlSelect plain = readThroughAList(false);
        SqlSelect plainMerged = assertInstanceOf(SqlSelect.class, new SelectMerge(
                java.util.function.UnaryOperator.identity(), SelectMerge.queriedBy(SQLite.OPERANDS_IN_A_QUERY))
                .rewriteRoot(plain));
        assertInstanceOf(SqlSource.Table.class, plainMerged.from(), "a row's value merges: " + plainMerged);
    }
}
