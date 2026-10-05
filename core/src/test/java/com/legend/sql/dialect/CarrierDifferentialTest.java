// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DIFFERENTIAL ORACLE (CARRIER_REDESIGN.md tenet #2): every
 * semantic collection node executes on DuckDB through BOTH strategies —
 * the native rule and the portable rule — and the rows must be EQUAL.
 * A strategy pair that can diverge is a bug this gate catches before
 * any backend sees it.
 */
class CarrierDifferentialTest {

    /** ReduceCollection over a collecting subselect: DuckDB renders
     * list_aggregate; PORTABLE fuses into the subselect (the engine's
     * grouped-subselect shape). Same rows, both strategies, on DuckDB. */
    @Test
    void reduceCollectionFusionRowEqual() throws Exception {
        SqlSelect collect = SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.Column("s", "v")), false,
                                        List.of()),
                                null, null)));
        SqlExpr reduce = new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG,
                new SqlExpr.ScalarSubquery(collect),
                List.of(new SqlExpr.StringLit("*")));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                                new SqlSelect.Projection(reduce, "joined", null)));

        String nativeSql = new DuckDb().render(q);
        // the PORTABLE strategy must also be VALID DuckDB — that is the
        // whole differential contract
        String portableSql = new AnsiSqlRenderer(Lexicon.DUCKDB,
                TypeNames.DUCKDB, Spellings.DUCKDB).render(q);
        assertTrue(nativeSql.contains("list_aggregate"),
                "native strategy should use the list carrier: " + nativeSql);
        assertTrue(portableSql.contains("STRING_AGG"),
                "portable strategy should fuse into the subselect: "
                + portableSql);

        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE t (v VARCHAR)");
            st.execute("INSERT INTO t VALUES ('a'), ('b'), ('c')");
            assertEquals(one(st, nativeSql), one(st, portableSql),
                    "strategy divergence:\nnative:   " + nativeSql
                    + "\nportable: " + portableSql);
        }
    }

    /** The witnessed corpus shape (R1b): LIST_TRANSFORM between collect
     * and reduce — the element transform substitutes into the fused
     * projection. Both strategies row-equal on DuckDB. */
    @Test
    void transformedReduceFusionRowEqual() throws Exception {
        SqlSelect collect = SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.Column("s", "v")), false,
                                        List.of()),
                                null, null)));
        SqlExpr transformed = SqlExpr.Call.of(
                com.legend.sql.SqlFn.LIST_TRANSFORM,
                new SqlExpr.ScalarSubquery(collect),
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.UPPER,
                                new SqlExpr.Column(null, "x"))));
        SqlExpr reduce = new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG,
                transformed, List.of(new SqlExpr.StringLit(",")));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                                new SqlSelect.Projection(reduce, "joined", null)));
        String nativeSql = new DuckDb().render(q);
        String portableSql = new AnsiSqlRenderer(Lexicon.DUCKDB,
                TypeNames.DUCKDB, Spellings.DUCKDB).render(q);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE t (v VARCHAR)");
            st.execute("INSERT INTO t VALUES ('a'), ('b')");
            assertEquals(one(st, nativeSql), one(st, portableSql),
                    "strategy divergence:\nnative:   " + nativeSql
                    + "\nportable: " + portableSql);
        }
    }

    /** Literal-collection join (R1c): STRING_AGG over a transformed
     * ArrayLit expands to the CONCAT chain — both strategies row-equal. */
    @Test
    void literalArrayJoinRowEqual() throws Exception {
        SqlExpr transformed = SqlExpr.Call.of(
                com.legend.sql.SqlFn.LIST_TRANSFORM,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("a"),
                        new SqlExpr.StringLit("b"),
                        new SqlExpr.StringLit("c"))),
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.UPPER,
                                new SqlExpr.Column(null, "x"))));
        SqlExpr reduce = new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG,
                transformed, List.of(new SqlExpr.StringLit("|")));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                                new SqlSelect.Projection(reduce, "joined", null)));
        String nativeSql = new DuckDb().render(q);
        String portableSql = new AnsiSqlRenderer(Lexicon.DUCKDB,
                TypeNames.DUCKDB, Spellings.DUCKDB).render(q);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            assertEquals(one(st, nativeSql), one(st, portableSql),
                    "strategy divergence:\nnative:   " + nativeSql
                    + "\nportable: " + portableSql);
        }
    }

    /** Membership NULL truth table (R2 crux): needle NULL, element
     * NULL + absent needle, present needle, empty — both strategies on
     * DuckDB. Filter-position equality is the row contract; the
     * projected false-vs-NULL difference on absent-with-NULL-element is
     * ABSORBED by the emission sites' COALESCE(_, false) wrapper, which
     * this fixture reproduces. */
    @Test
    void membershipNullEdgesRowEqual() throws Exception {
        record Cse(SqlExpr needle, java.util.List<SqlExpr> elems) { }
        java.util.List<Cse> cases = java.util.List.of(
                new Cse(new SqlExpr.StringLit("a"),
                        java.util.List.of(new SqlExpr.StringLit("a"),
                                new SqlExpr.NullLit())),
                new Cse(new SqlExpr.StringLit("x"),
                        java.util.List.of(new SqlExpr.StringLit("a"),
                                new SqlExpr.NullLit())),
                new Cse(new SqlExpr.NullLit(),
                        java.util.List.of(new SqlExpr.StringLit("a"))),
                new Cse(new SqlExpr.StringLit("a"), java.util.List.of()));
        for (Cse cse : cases) {
            SqlExpr member = SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE,
                    new SqlExpr.Membership(cse.needle(),
                            new SqlExpr.ArrayLit(cse.elems())),
                    new SqlExpr.BoolLit(false));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                                    new SqlSelect.Projection(member, "m", null)));
            String nativeSql = new DuckDb().render(q);
            String portableSql = new AnsiSqlRenderer(Lexicon.DUCKDB,
                    TypeNames.DUCKDB, Spellings.DUCKDB).render(q);
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                assertEquals(one(st, nativeSql), one(st, portableSql),
                        "membership divergence:\nnative:   " + nativeSql
                        + "\nportable: " + portableSql);
            }
        }
    }

    /** Explode-of-collect (R5b): {@code unnest((SELECT LIST(v) FROM t))}
     * IS the collecting row set — both strategies row-equal IN ORDER. */
    @Test
    void explodeOfCollectRowEqual() throws Exception {
        SqlSelect q = unnestOverDual(collectOfV(), "e");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            assertEquals(all(st, new DuckDb().render(q)),
                    all(st, portable().render(q)),
                    "explode divergence");
        }
    }

    /** Sorted explode (R5b): {@code unnest(LIST_SORT(collect))} — the
     * probed list_sort contract is ASC NULLS LAST; the NULL row in the
     * seed pins the placement. */
    @Test
    void explodeOfSortedCollectRowEqual() throws Exception {
        SqlSelect q = unnestOverDual(SqlExpr.Call.of(
                com.legend.sql.SqlFn.LIST_SORT, collectOfV()), "e");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            st.execute("INSERT INTO t VALUES (NULL)");
            assertEquals(all(st, new DuckDb().render(q)),
                    all(st, portable().render(q)),
                    "sorted explode divergence");
        }
    }

    /** unnest(NULL) is ZERO rows on both strategies (R5b). */
    @Test
    void explodeOfNullRowEqual() throws Exception {
        SqlSelect q = unnestOverDual(new SqlExpr.NullLit(), "e");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            assertEquals(all(st, new DuckDb().render(q)),
                    all(st, portable().render(q)),
                    "null explode divergence");
        }
    }

    /** Concat fold + literal explode (R5b): unnest(list_concat(a, b))
     * over compile-time collections — branch order preserved. */
    @Test
    void concatFoldedExplodeRowEqual() throws Exception {
        SqlExpr arg = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_CONCAT,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("a"),
                        new SqlExpr.StringLit("b"))),
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("c"))));
        SqlSelect q = unnestOverDual(arg, "e");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            assertEquals(all(st, new DuckDb().render(q)),
                    all(st, portable().render(q)),
                    "concat explode divergence");
        }
    }

    /** Through-subselect cells, k=1 (R5b): SELECT unnest(c) FROM
     * (SELECT [v] AS c FROM t) — the single-cell explode is EXACT. */
    @Test
    void throughSubselectSingletonRowEqual() throws Exception {
        SqlSelect inner = SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlExpr.ArrayLit(List.of(
                                        new SqlExpr.Column("s", "v"))), "c", null)));
        SqlSelect q = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, "sub", null))
                .withProjections(List.of(new SqlSelect.Projection(
                                SqlExpr.Call.of(com.legend.sql.SqlFn.UNNEST,
                                        new SqlExpr.Column("sub", "c")), "e", null)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            assertEquals(all(st, new DuckDb().render(q)),
                    all(st, portable().render(q)),
                    "through-subselect explode divergence");
        }
    }

    /** Through-subselect cells, k=2 (R5b): DuckDB unnest is ROW-major,
     * the UNION-ALL branches are COLUMN-major — the VALUE SET is equal
     * (multiset compare); observed order divergence is the sweep's
     * call, never silent (documented in the strategy arm). */
    @Test
    void throughSubselectPairMultisetEqual() throws Exception {
        SqlSelect inner = SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlExpr.ArrayLit(List.of(
                                        new SqlExpr.Column("s", "v"),
                                        SqlExpr.Call.of(
                                                com.legend.sql.SqlFn.UPPER,
                                                new SqlExpr.Column("s", "v")))),
                                "c", null)));
        SqlSelect q = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, "sub", null))
                .withProjections(List.of(new SqlSelect.Projection(
                                SqlExpr.Call.of(com.legend.sql.SqlFn.UNNEST,
                                        new SqlExpr.Column("sub", "c")), "e", null)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            java.util.List<String> nat =
                    new java.util.ArrayList<>(all(st, new DuckDb().render(q)));
            java.util.List<String> port =
                    new java.util.ArrayList<>(all(st, portable().render(q)));
            java.util.Collections.sort(nat);
            java.util.Collections.sort(port);
            assertEquals(nat, port, "pair explode multiset divergence");
        }
    }

    /** Through-subselect row-major join (R5c): STRING_AGG over
     * FLATTEN(collect) whose collected column resolves to an ArrayLit
     * in the INNER subselect — per-row CONCAT substitutes inside;
     * sorted variant explodes per-cell branches with a global sort. */
    @Test
    void throughSubselectRowMajorJoinRowEqual() throws Exception {
        for (boolean sorted : new boolean[] {false, true}) {
            SqlExpr flatten = SqlExpr.Call.of(
                    com.legend.sql.SqlFn.LIST_FLATTEN,
                    new SqlExpr.ScalarSubquery(cellsCollect()));
            SqlExpr chain = sorted
                    ? SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_SORT, flatten)
                    : flatten;
            SqlExpr transformed = SqlExpr.Call.of(
                    com.legend.sql.SqlFn.LIST_TRANSFORM, chain,
                    new SqlExpr.Lambda(List.of("x"),
                            SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE,
                                    new SqlExpr.Column(null, "x"),
                                    new SqlExpr.StringLit("~"))));
            SqlExpr reduce = new SqlExpr.ReduceCollection(
                    SqlAgg.Fn.STRING_AGG, transformed,
                    List.of(new SqlExpr.StringLit("|")));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(reduce, "j", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                seed(st);
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "row-major join divergence (sorted=" + sorted + ")");
            }
        }
    }

    /** Literal pick (R5c): LIST_GET over a compile-time collection —
     * probed list_extract contract (1-based, -1 last, 0/OOB NULL). */
    @Test
    void listGetLiteralPickRowEqual() throws Exception {
        for (long ix : new long[] {2, -1, 0, 5}) {
            SqlExpr get = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_GET,
                    new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("a"),
                            new SqlExpr.StringLit("b"),
                            new SqlExpr.StringLit("c"))),
                    new SqlExpr.IntLit(ix));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(get, "e", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "literal pick divergence at " + ix);
            }
        }
    }

    /** Collect pick (R5c): LIST_GET(collect, n) is the ORDER-carrying
     * LIMIT/OFFSET form. */
    @Test
    void listGetOverCollectRowEqual() throws Exception {
        for (long ix : new long[] {1, 2}) {
            SqlExpr get = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_GET,
                    collectOfV(), new SqlExpr.IntLit(ix));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(get, "e", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                seed(st);
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "collect pick divergence at " + ix);
            }
        }
    }

    /** Split token pick (R5c): LIST_GET(SPLIT(s, ','), n) becomes
     * SPLIT_PART — empty tokens and missing tokens included. */
    @Test
    void splitTokenPickRowEqual() throws Exception {
        for (long ix : new long[] {1, 2, 3}) {
            SqlExpr get = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_GET,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.SPLIT,
                            new SqlExpr.Column("s", "v"),
                            new SqlExpr.StringLit(",")),
                    new SqlExpr.IntLit(ix));
            SqlSelect q = SqlSelect.starOf(
                            new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                    .withProjections(List.of(
                            new SqlSelect.Projection(get, "e", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (v VARCHAR)");
                st.execute("INSERT INTO t VALUES ('a,b,c'), ('a,,c'), ('a')");
                assertEquals(all(st, new DuckDb().render(q)),
                        all(st, portable().render(q)),
                        "split token divergence at " + ix);
            }
        }
    }

    /** First-non-null idiom (R5c): LIST_GET(LIST_FILTER(lit, x | x IS
     * NOT NULL), 1) is COALESCE over the elements. */
    @Test
    void coalesceIdiomRowEqual() throws Exception {
        SqlExpr filtered = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_FILTER,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.NullLit(),
                        new SqlExpr.StringLit("x"))),
                new SqlExpr.Lambda(List.of("e"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NOT_NULL,
                                new SqlExpr.Column(null, "e"))));
        SqlExpr get = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_GET,
                filtered, new SqlExpr.IntLit(1));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                        new SqlSelect.Projection(get, "e", null)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            assertEquals(one(st, new DuckDb().render(q)),
                    one(st, portable().render(q)),
                    "coalesce idiom divergence");
        }
    }

    /** TYPEOF date dispatch (R5d): typeof(e) = 'DATE' vs the portable
     * VARCHAR-cast LENGTH probe — DATE and TIMESTAMP operands. */
    @Test
    void typeofDateDispatchRowEqual() throws Exception {
        for (String col : new String[] {"d", "ts"}) {
            SqlExpr probe = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                            SqlExpr.Call.of(com.legend.sql.SqlFn.TYPEOF,
                                    new SqlExpr.Column("s", col)),
                            new SqlExpr.StringLit("DATE")),
                    new SqlExpr.StringLit("D"))),
                    new SqlExpr.StringLit("TS"));
            SqlSelect q = SqlSelect.starOf(
                            new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                    .withProjections(List.of(
                            new SqlSelect.Projection(probe, "k", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (d DATE, ts TIMESTAMP)");
                st.execute("INSERT INTO t VALUES (DATE '2015-08-26',"
                        + " TIMESTAMP '2015-08-26 01:02:03')");
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "typeof dispatch divergence on " + col);
            }
        }
    }

    /** Sorted collect VALUE (R5d): LIST_SORT(collect) is the collect
     * ordered by its value ASC NULLS LAST (probed list_sort contract);
     * the NULL row pins the placement. */
    @Test
    void sortedCollectValueRowEqual() throws Exception {
        SqlExpr sorted = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_SORT,
                collectOfV());
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                        new SqlSelect.Projection(sorted, "l", null)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            st.execute("INSERT INTO t VALUES (NULL)");
            assertEquals(one(st, new DuckDb().render(q)),
                    one(st, portable().render(q)),
                    "sorted collect divergence");
        }
    }

    /** Filtered collect VALUE (R5d): LIST_FILTER(collect, lam) pushes
     * the element predicate into the collect's WHERE. */
    @Test
    void filteredCollectValueRowEqual() throws Exception {
        SqlExpr filtered = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_FILTER,
                collectOfV(),
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL,
                                new SqlExpr.Column(null, "x"))));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(
                        new SqlSelect.Projection(filtered, "l", null)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = c.createStatement()) {
            seed(st);
            st.execute("INSERT INTO t VALUES (NULL)");
            assertEquals(one(st, new DuckDb().render(q)),
                    one(st, portable().render(q)),
                    "filtered collect divergence");
        }
    }

    /** Membership push-down (R5d): membership distributes over
     * LIST_CONCAT (OR) and CASE (per-branch) — runtime-branched
     * literal collections, needle present/absent. */
    @Test
    void membershipPushDownRowEqual() throws Exception {
        for (String needle : new String[] {"a", "z"}) {
            SqlExpr branched = new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(
                            SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                                    new SqlExpr.IntLit(1),
                                    new SqlExpr.IntLit(1)),
                            new SqlExpr.ArrayLit(List.of(
                                    new SqlExpr.StringLit("a"),
                                    new SqlExpr.StringLit("b"))))),
                    new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("c"))));
            SqlExpr concat = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_CONCAT,
                    branched,
                    new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("d"))));
            SqlExpr member = SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE,
                    new SqlExpr.Membership(new SqlExpr.StringLit(needle),
                            concat),
                    new SqlExpr.BoolLit(false));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(member, "m", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "membership push-down divergence on " + needle);
            }
        }
    }

    /** Literal reducer folds (R5d): BOOL_AND/BOOL_OR/SUM/PRODUCT over
     * compile-time collections — null-ignoring, all-null -> NULL
     * (probed aggregate contract). */
    @Test
    void literalReducerFoldsRowEqual() throws Exception {
        SqlExpr nullInt = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                new SqlExpr.BoolLit(false), new SqlExpr.IntLit(1))), null);
        record Fx(com.legend.sql.SqlFn fn, List<SqlExpr> elems) { }
        List<Fx> fixtures = List.of(
                new Fx(com.legend.sql.SqlFn.LIST_BOOL_AND, List.of(
                        new SqlExpr.BoolLit(true), new SqlExpr.BoolLit(true))),
                new Fx(com.legend.sql.SqlFn.LIST_BOOL_OR, List.of(
                        new SqlExpr.BoolLit(false),
                        new SqlExpr.BoolLit(false))),
                new Fx(com.legend.sql.SqlFn.LIST_SUM, List.of(
                        new SqlExpr.IntLit(1), nullInt,
                        new SqlExpr.IntLit(2))),
                new Fx(com.legend.sql.SqlFn.LIST_SUM, List.of(nullInt)),
                new Fx(com.legend.sql.SqlFn.LIST_PRODUCT, List.of(
                        new SqlExpr.IntLit(2), nullInt,
                        new SqlExpr.IntLit(4))));
        for (Fx fx : fixtures) {
            SqlExpr fold = SqlExpr.Call.of(fx.fn(),
                    new SqlExpr.ArrayLit(fx.elems()));
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(fold, "r", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "literal fold divergence on " + fx.fn());
            }
        }
    }

    /** LIST_LENGTH (P2): literal collections fold to their size (NULL
     * elements count); collects become COUNT(*) over the same rows. */
    @Test
    void listLengthRowEqual() throws Exception {
        SqlExpr litLen = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("a"),
                        new SqlExpr.NullLit(), new SqlExpr.StringLit("b"))));
        SqlExpr collLen = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH,
                collectOfV());
        for (SqlExpr len : List.of(litLen, collLen)) {
            SqlSelect q = SqlSelect.starOf(new SqlSource.Dual())
                    .withProjections(List.of(
                            new SqlSelect.Projection(len, "n", null)));
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement st = c.createStatement()) {
                seed(st);
                st.execute("INSERT INTO t VALUES (NULL)");
                assertEquals(one(st, new DuckDb().render(q)),
                        one(st, portable().render(q)),
                        "list length divergence");
            }
        }
    }

    /** The inner cells collect for the row-major fixtures: SELECT
     * LIST(c) FROM (SELECT [v, upper(v)] AS c FROM t) sub. */
    private static SqlSelect cellsCollect() {
        SqlSelect inner = SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlExpr.ArrayLit(List.of(
                                        new SqlExpr.Column("s", "v"),
                                        SqlExpr.Call.of(
                                                com.legend.sql.SqlFn.UPPER,
                                                new SqlExpr.Column("s", "v")))),
                                "c", null)));
        return SqlSelect.starOf(new SqlSource.Subselect(inner, "sub", null))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.Column("sub", "c")),
                                        false, List.of()),
                                "l", null)));
    }

    // ---- R5b fixture plumbing ----

    private static void seed(Statement st) throws Exception {
        st.execute("CREATE TABLE t (v VARCHAR)");
        st.execute("INSERT INTO t VALUES ('b'), ('a'), ('c')");
    }

    private static SqlExpr collectOfV() {
        return new SqlExpr.ScalarSubquery(SqlSelect.starOf(
                        new SqlSource.Table("t", "s", List.of(), false, java.util.Map.of()))
                .withProjections(List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.Column("s", "v")), false,
                                        List.of()),
                                null, null))));
    }

    private static SqlSelect unnestOverDual(SqlExpr arg, String alias) {
        return SqlSelect.starOf(new SqlSource.Dual())
                .withProjections(List.of(new SqlSelect.Projection(
                                SqlExpr.Call.of(com.legend.sql.SqlFn.UNNEST,
                                        arg), alias, null)));
    }

    private static AnsiSqlRenderer portable() {
        return new AnsiSqlRenderer(Lexicon.DUCKDB, TypeNames.DUCKDB,
                Spellings.DUCKDB);
    }

    private static java.util.List<String> all(Statement st, String sql)
            throws Exception {
        java.util.List<String> out = new java.util.ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(String.valueOf(rs.getObject(1)));
            }
        }
        return out;
    }

    private static String one(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return String.valueOf(rs.getObject(1));
        }
    }
}
