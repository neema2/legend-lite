// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
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
 * THE CANON CONFORMANCE BATTERY: the database judge's canons hold the
 * compare's contract on the platform's engine (a Float's: one text exactly
 * for equal doubles, W10 (b) 2026-11-16; the other kinds' spellings below);
 * H2's battery is {@code SqlCanonConformanceH2Test}'s.
 */
class SqlCanonConformanceTest {

    /** A Float's judge canon is the database's own text of the double, every zero one text (W10 (b), 2026-11-16:
     *  Pure's literal spelling, which this battery pinned until then, was text no compare needs). Its contract is the
     *  compare's: two values have one canon exactly when they are equal doubles ({@code -0.0 == 0.0}), across
     *  magnitudes the old DECIMAL(38,18) unfold zeroed (1e-30, 1e300), and whatever the value's carrier (a literal, a
     *  value cast from its text). */
    @Test
    void floatCanonIsEqualityAcrossMagnitudes() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            List<String> literal = new java.util.ArrayList<>();
            List<String> fromText = new java.util.ArrayList<>();
            for (double d : FLOAT_BATTERY) {
                literal.add(dbCanon(c, d, Type.Primitive.FLOAT));
                fromText.add(dbCanon(c, new SqlExpr.Cast(new SqlExpr.StringLit(Double.toString(d)),
                        SqlType.Scalar.DOUBLE, SqlExpr.Reason.USER), Type.Primitive.FLOAT));
            }
            assertCanonIsEquality(literal, fromText, "");
        }
    }

    /** Values of every magnitude, with equal pairs written differently (17.000 and 17.0, 0.0 and -0.0). */
    static final double[] FLOAT_BATTERY = {
            3.14, 17.000, 17.0, 1.3421e8, 134.21e-10, .01, 0.0, -0.0,
            -5.59, 5.59, 1.5e-30, 1e300, -2.5e-15, 123456.789,
            9007199254740993.0, 1e16, 1e7, 12345678.9,
    };

    /** {@code a.get(i)} and {@code b.get(i)} are the canons of {@link #FLOAT_BATTERY}'s i-th value through two carriers:
     *  every canon equals every other exactly when the two values are equal doubles. */
    static void assertCanonIsEquality(List<String> a, List<String> b, String lane) {
        for (int i = 0; i < FLOAT_BATTERY.length; i++) {
            org.junit.jupiter.api.Assertions.assertNotNull(a.get(i), lane + "no canon for " + FLOAT_BATTERY[i]);
            assertEquals(a.get(i), b.get(i), lane + "the canon of " + FLOAT_BATTERY[i] + " depends on its carrier");
            for (int j = 0; j < FLOAT_BATTERY.length; j++) {
                boolean equal = FLOAT_BATTERY[i] == FLOAT_BATTERY[j];
                assertEquals(equal, a.get(i).equals(a.get(j)), lane + "canons of " + FLOAT_BATTERY[i] + " and "
                        + FLOAT_BATTERY[j] + ": " + a.get(i) + " / " + a.get(j));
            }
        }
    }

    @Test
    void otherKindsAgree() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            assertEquals("42", dbCanon(c, new SqlExpr.IntLit(42),
                    Type.Primitive.INTEGER));
            assertEquals("true", dbCanon(c, new SqlExpr.BoolLit(true),
                    Type.Primitive.BOOLEAN));
            // X2: scale-preserving Decimal canon
            assertEquals("8.00", dbCanon(c,
                    new SqlExpr.Cast(new SqlExpr.StringLit("8.00"),
                            new SqlType.Decimal(10, 2), SqlExpr.Reason.USER),
                    Type.Primitive.DECIMAL));
            assertEquals("3.80", dbCanon(c,
                    new SqlExpr.Cast(new SqlExpr.StringLit("3.80"),
                            new SqlType.Decimal(10, 2), SqlExpr.Reason.USER),
                    Type.Primitive.DECIMAL));
        }
    }


    private static String dbCanon(Connection c, double d, Type t)
            throws Exception {
        return dbCanon(c, new SqlExpr.FloatLit(d), t);
    }

    private static String dbCanon(Connection c, SqlExpr v, Type t)
            throws Exception {
        SqlExpr canon = CanonicalRenderSql.scalarCanon(v, t);
        SqlSelect q = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        java.util.Objects.requireNonNull(canon), "canon", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null,
                List.of(new OutputCol("canon", SqlType.Scalar.VARCHAR, true)));
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(TestBackends.platform().dialect().render(q))) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void keyedInstanceCanonAgrees() throws Exception {
        // X5 pin — the keyed-instance canon: JSON framing (escaping-
        // safe), '_type' carries the classifier, leaves are PURE
        // LITERAL SPELLINGS (bare 1, quoted 'a, b' — the engine's own
        // kind-in-text grammar, no invented tags). Key order = model
        // order.
        var keys = new com.legend.compiler.element.EqualityKeys(
                "meta::pure::functions::collection::Pair",
                java.util.List.of(
                        new com.legend.compiler.element.EqualityKeys.Key(
                                "first", false, null),
                        new com.legend.compiler.element.EqualityKeys.Key(
                                "second", false, null)));
        SqlType layout = new SqlType.Struct(java.util.List.of(
                new SqlType.Struct.Field("first", SqlType.Scalar.BIGINT),
                new SqlType.Struct.Field("second", SqlType.Scalar.VARCHAR)));
        SqlExpr v = new SqlExpr.StructLit(java.util.List.of(
                new SqlExpr.StructLit.Field("first", new SqlExpr.IntLit(1)),
                new SqlExpr.StructLit.Field("second",
                        new SqlExpr.StringLit("a, b"))));
        SqlExpr canon = java.util.Objects.requireNonNull(
                CanonicalRenderSql.instanceCanon(v, keys, layout));
        try (Connection c = TestBackends.platform().open()) {
            assertEquals("{\"_type\":\"meta::pure::functions::collection"
                    + "::Pair\",\"first\":\"1\",\"second\":\"'a, b'\"}",
                    dbText(c, new SqlExpr.Cast(canon,
                            SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER)));
        }
    }

    @Test
    void listCarrierCanonAgrees() throws Exception {
        // the bare-array carrier (List<T>): the SQL value IS the one
        // to-many key's collection; empty and NULL both render []
        var keys = new com.legend.compiler.element.EqualityKeys(
                "meta::pure::functions::collection::List",
                java.util.List.of(
                        new com.legend.compiler.element.EqualityKeys.Key(
                                "values", true, null)));
        SqlExpr v = new SqlExpr.ArrayLit(java.util.List.of(
                new SqlExpr.IntLit(1), new SqlExpr.IntLit(2)));
        SqlExpr canon = java.util.Objects.requireNonNull(
                CanonicalRenderSql.instanceCanon(v, keys,
                        new SqlType.Array(SqlType.Scalar.BIGINT)));
        try (Connection c = TestBackends.platform().open()) {
            assertEquals("{\"_type\":\"meta::pure::functions::collection"
                    + "::List\",\"values\":[\"1\",\"2\"]}",
                    dbText(c, new SqlExpr.Cast(canon,
                            SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER)));
        }
    }

    private static String dbText(Connection c, SqlExpr expr)
            throws Exception {
        SqlSelect q = new SqlSelect(
                List.of(new SqlSelect.Projection(expr, "t", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null,
                List.of(new OutputCol("t", SqlType.Scalar.VARCHAR, true)));
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(TestBackends.platform().dialect().render(q))) {
            rs.next();
            return rs.getString(1);
        }
    }
}
