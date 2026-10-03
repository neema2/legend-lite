// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * COMPILER_SHORTCUT_AUDIT burn lane (ratified item e) — the confirmed
 * wrong answers, pinned at the seam each was fixed at.
 */
class BurnLaneTest {

    // ---- §3a: exists-dedupe by bare name ----

    @Test
    @DisplayName("§3a: two same-named inner correlation keys keep DISTINCT projections and honest ON pairing")
    void existsDedupeByIdentityNotBareName() {
        // outer table O(A, B); inner join P(NAME) x D(NAME): correlations
        // O.A = P.NAME and O.B = D.NAME — the old bare-name dedupe
        // collapsed both onto ONE projected NAME and the ON compared O.B
        // against the person's name (0 rows where 1 was right).
        SqlSource.Table p = new SqlSource.Table("T_PERSON", "p",
                List.of(new OutputCol("NAME", SqlType.Scalar.VARCHAR, true)), false, java.util.Map.of());
        SqlSource.Table d = new SqlSource.Table("T_DEPT", "d",
                List.of(new OutputCol("NAME", SqlType.Scalar.VARCHAR, true)), false, java.util.Map.of());
        SqlSource inner = new SqlSource.Join(p, d, SqlSource.Join.Kind.INNER,
                new SqlExpr.BoolLit(true));
        SqlSelect sub = SqlSelect.starOf(inner).withWhere(
                Fold.mergeAnd(
                        SqlExpr.Call.of(SqlFn.EQUAL,
                                new SqlExpr.Column("o", "A"),
                                new SqlExpr.Column("p", "NAME")),
                        SqlExpr.Call.of(SqlFn.EQUAL,
                                new SqlExpr.Column("o", "B"),
                                new SqlExpr.Column("d", "NAME"))));
        SqlSource.Table o = new SqlSource.Table("T_OUTER", "o",
                List.of(new OutputCol("A", SqlType.Scalar.VARCHAR, true),
                        new OutputCol("B", SqlType.Scalar.VARCHAR, true)), false, java.util.Map.of());
        SqlSelect outer = SqlSelect.starOf(o)
                .withWhere(new SqlExpr.Exists(sub));
        AtomicInteger n = new AtomicInteger();
        SqlSelect out = ExistsJoinForm.rewrite(outer,
                () -> "x" + n.incrementAndGet(), w -> null);
        String sql = new com.legend.sql.dialect.DuckDb().render(out);
        // both keys projected (one aliased), and the ON references the
        // DISAMBIGUATED alias — never the same column twice
        assertTrue(sql.contains("NAME_1"),
                "same-named keys must disambiguate, got:\n" + sql);
        assertTrue(sql.contains("o.A = ") && sql.contains("o.B = "),
                "both correlations must survive, got:\n" + sql);
        assertTrue(!sql.replaceFirst("o\\.A = (\\S+)", "")
                        .replaceFirst("o\\.B = (\\S+)", "").contains("o.A ="),
                sql);
    }

    // ---- §3b: the ordering obligation sees through wrapping calls ----

    @Test
    @DisplayName("§3b: 3-arg joinStrings (CONCAT wrap) still mints u_ord")
    void orderObligationSeesThroughConcat() {
        SqlSelect b1 = new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.StringLit("a"), "v", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null,
                List.of(new OutputCol("v", SqlType.Scalar.VARCHAR, false)));
        SqlSelect b2 = new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.StringLit("b"), "v", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null,
                List.of(new OutputCol("v", SqlType.Scalar.VARCHAR, false)));
        SqlUnion u = new SqlUnion(List.of(b1, b2), true,
                List.of(new OutputCol("v", SqlType.Scalar.VARCHAR, false)));
        SqlSource.Subselect sub = new SqlSource.Subselect(u, "t0", null);
        SqlSelect base = SqlSelect.starOf(sub);
        SqlAgg.Reducer agg = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(new SqlExpr.Column("t0", "v"),
                        new SqlExpr.StringLit(",")), false, List.of());
        // the 3-arg joinStrings shape: CONCAT(prefix, agg, suffix)
        SqlExpr wrapped = SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit("["), agg, new SqlExpr.StringLit("]"));
        Fold.OrderedAggExpr oa = Fold.orderUnionAggregateExpr(base, wrapped);
        assertTrue(oa != null,
                "the obligation must fire through the CONCAT wrap");
        String sql = new com.legend.sql.dialect.DuckDb()
                .render(oa.base().withProjections(List.of(
                        new SqlSelect.Projection(oa.expr(), "r",
                                new OutputCol("r",
                                        SqlType.Scalar.VARCHAR, false)))));
        assertTrue(sql.contains("u_ord"),
                "u_ord must be minted and ordered by, got:\n" + sql);
        assertTrue(sql.contains("ORDER BY") || sql.contains("order by"),
                "the reducer must gain ORDER BY u_ord, got:\n" + sql);
    }

    // ---- DEEP_AUDIT §3 (D2): singleton literals through collection ops ----

    @Test
    @DisplayName("D2: c1-collapsed singletons box by stamp at collection consumers")
    void singletonCollectionOps() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            record P(String q, String want) {
            }
            for (P p : List.of(
                    // the six former hard Binder errors
                    new P("{|[7]->take(1)}", "[7]"),
                    new P("{|[7]->drop(0)}", "[7]"),
                    new P("{|[7]->slice(0,1)}", "[7]"),
                    new P("{|[7]->contains(7)}", "true"),
                    new P("{|[7]->exists(x|$x > 1)}", "true"),
                    new P("{|[7]->zip([8])}", "[{first=7, second=8}]"),
                    // ADJUDICATED pure-faithful (pure's [x] == x law: the
                    // checker resolves string::contains for a String[1]
                    // operand — substring semantics IS pure's answer)
                    new P("{|['ACTIVE']->contains('TIV')}", "true"),
                    // D6: the 1-arg collection isDistinct was an
                    // ArrayIndexOutOfBoundsException on ANY input
                    new P("{|[1,2,3]->isDistinct()}", "true"),
                    new P("{|[1,2,2]->isDistinct()}", "false"),
                    new P("{|[7]->isDistinct()}", "true"))) {
                ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), p.q(), StorelessRuntime.RUNTIME, conn);
                Object v = r instanceof ExecutionResult.Scalar s ? s.value()
                        : r instanceof ExecutionResult.Collection c
                                ? c.values() : r;
                assertEquals(p.want(), String.valueOf(v), p.q());
            }
        }
    }

    // ---- cast(): cross-kind raises pure's message ----

    @Test
    @DisplayName("cast: cross-kind primitive casts raise pure's Cast exception")
    void crossKindCastRaises() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // NOTE: string<->numeric/temporal casts CONVERT — the
            // standing product contract, pinned by
            // TypeConversionCheckerTest (the corpus referee rejected the
            // blanket raise). The uncontested impossible classes raise:
            for (String q : new String[] {
                    "{|1->cast(@Boolean)}",
                    "{|true->cast(@Float)}",
                    "{|%2014-01-01->cast(@Integer)}"}) {
                var ex = assertThrows(Exception.class,
                        () -> Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), q, StorelessRuntime.RUNTIME, conn));
                assertTrue(ex.getMessage().contains("Cast exception"),
                        q + " must raise pure's Cast exception, got: "
                                + ex.getMessage());
            }
            // widening stays an assertion; same-kind narrowing keeps the
            // standing conversion contract
            ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), "{|1->cast(@Number)}", StorelessRuntime.RUNTIME, conn);
            assertEquals(1L, ((Number) ((ExecutionResult.Scalar) r).value())
                    .longValue());
        }
    }
}
