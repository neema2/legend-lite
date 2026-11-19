// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link SqlCanonConformanceTest}'s battery on H2 (moved here from it by the dialect program's Phase 6, 2026-10-16):
 * a Float's canon is one text exactly for equal doubles (W10 (b), 2026-11-16).
 */
class SqlCanonConformanceH2Test {

    /** The battery on H2, whose DOUBLE prints as Java does ({@code 1.3421E-8}, {@code 1.0E7}); and the dialect's
     * epoch ({@code EXTRACT(EPOCH FROM)}).
     * Over a COLUMN, as the canon runs — H2 folds constant expressions at
     * prepare time, branch-blind, so a literal battery would cast the
     * untaken exponent branch's '' before any row is read. */
    @Test
    void floatCanonIsEquality() throws Exception {
        double[] battery = SqlCanonConformanceTest.FLOAT_BATTERY;
        List<String> canons = new java.util.ArrayList<>();
        com.legend.sql.dialect.H2 h2 = new com.legend.sql.dialect.H2();
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            try (Statement st = c.createStatement()) {
                for (String setup : h2.sessionSetup()) {
                    st.execute(setup);
                }
                st.execute("CREATE TABLE B(\"k\" INT, \"v\" DOUBLE PRECISION)");
                for (int i = 0; i < battery.length; i++) {
                    st.execute("INSERT INTO B VALUES (" + i + ", CAST('" + battery[i] + "' AS DOUBLE PRECISION))");
                }
            }
            OutputCol v = new OutputCol("v", SqlType.Scalar.DOUBLE, true);
            OutputCol k = new OutputCol("k", SqlType.Scalar.INTEGER, false);
            SqlSource.Table b = new SqlSource.Table("B", "b", List.of(k, v), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
            for (int i = 0; i < battery.length; i++) {
                SqlExpr canon = CanonicalRenderSql.scalarCanon(
                        SqlExpr.Column.of("b", "v", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED),
                        Type.Primitive.FLOAT);
                SqlSelect q = new SqlSelect(
                        List.of(new SqlSelect.Projection(java.util.Objects.requireNonNull(canon), "canon", null)),
                        false, b, SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                                SqlExpr.Column.of("b", "k", SqlType.Scalar.INTEGER, false, OutputCol.Origin.DERIVED),
                                new SqlExpr.IntLit(i)),
                        List.of(), null, null, List.of(), null, null,
                        List.of(new OutputCol("canon", SqlType.Scalar.VARCHAR, true)));
                try (Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery(h2.render(q))) {
                    rs.next();
                    canons.add(rs.getString(1));
                }
            }
            // a column's value and the same canon over a column: one carrier here (H2 folds literals at prepare)
            SqlCanonConformanceTest.assertCanonIsEquality(canons, canons, "H2: ");
            SqlSelect epoch = new SqlSelect(
                    List.of(new SqlSelect.Projection(SqlExpr.Call.of(com.legend.sql.SqlFn.EPOCH_SECONDS,
                            new SqlExpr.TimestampLit("2014-12-04T15:22:23.123")), "canon", null)),
                    false, new SqlSource.Dual(), null, List.of(), null, null,
                    List.of(), null, null,
                    List.of(new OutputCol("canon", SqlType.Scalar.VARCHAR, true)));
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery(h2.render(epoch))) {
                rs.next();
                assertEquals("1417706543.123", rs.getString(1));
            }
        }
    }
}
