// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A GUARDED RAISE INSIDE A LAMBDA, ON THE EMBEDDED CLICKHOUSE (2026-11-20, the warm lane's finding): ClickHouse evaluates
 * every branch of a CASE inside a lambda for every element, so a substring's range guard in {@code map}'s lambda raised
 * {@code SUBSTRING_OUT_OF_RANGE} over valid text. On the product road: valid text answers, and a text out of range still
 * raises the product's kind. The SQL's form is {@code ClickHouseLambdaRaiseTest}'s. Its own target
 * ({@code //core:clickhouse_lambda_raise_test}): it needs the embedded ClickHouse in its runfiles.
 */
class ClickHouseLambdaRaiseServerTest {

    @Test
    void aGuardedRaiseInALambdaRaisesWhereItsGuardHoldsAlone() throws Exception {
        TestBackend backend = TestBackends.of(DatabaseType.ClickHouse);
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            Lean.Result ok = Lean.run(model,
                    "|['Doe', 'Smith', 'Branche']->map(x|$x->substring(1, 2))->joinStrings(',')",
                    StorelessRuntime.RUNTIME, c);
            assertEquals("o,m,r", ok.rows().get(0).get(0), "every element in range: no raise");
            Exception e = assertThrows(Exception.class, () -> Lean.run(model,
                    "|['Doe', 'S', 'Branche']->map(x|$x->substring(1, 2))->joinStrings(',')",
                    StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage()).contains(
                    com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE.sentence()),
                    "an element out of range still raises the product's kind: " + e.getMessage());
        }
    }

    /** A NULL condition (the review's "should", 2026-11-20), at run time on the server: the dialect's own SQL for
     *  {@code arrayMap(x -> CASE ...)} over a list holding a NULL. A branch whose condition is NULL for an element is
     *  not taken, so its raise does not raise there; and a later branch's raise, its own condition true there, still
     *  raises (its guard reads the earlier condition as not true, never as NULL). */
    @Test
    void aNullConditionNeitherRaisesFalselyNorHidesALaterRaise() throws Exception {
        TestBackend backend = TestBackends.of(DatabaseType.ClickHouse);
        com.legend.sql.SqlExpr x = com.legend.sql.SqlExpr.Column.param("x",
                new com.legend.sql.SqlExpr.Column(null, "x"));
        com.legend.sql.SqlExpr list = new com.legend.sql.SqlExpr.ArrayLit(java.util.List.of(
                new com.legend.sql.SqlExpr.StringLit("a"), new com.legend.sql.SqlExpr.NullLit(),
                new com.legend.sql.SqlExpr.StringLit("abcdef")));
        com.legend.sql.SqlExpr len = com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH, x);
        com.legend.sql.SqlExpr raise = com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
                new com.legend.sql.SqlExpr.StringLit("boom"));
        try (Connection c = backend.open(); java.sql.Statement st = c.createStatement()) {
            // no element is longer than 9; the NULL element's condition is NULL: nothing raises, the NULL stays NULL
            String quiet = backend.dialect().render(mapped(list, new com.legend.sql.SqlExpr.Case(java.util.List.of(
                    new com.legend.sql.SqlExpr.Case.When(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.GREATER,
                            len, new com.legend.sql.SqlExpr.IntLit(9)), raise)), x,
                    com.legend.sql.SqlExpr.Reason.RAISE)));
            try (java.sql.ResultSet rs = st.executeQuery(quiet)) {
                assertTrue(rs.next(), quiet);
                assertEquals("[a, NULL, abcdef]", rs.getString(1), "no false raise, the NULL kept: " + quiet);
            }
            // the first condition NULL for the NULL element, the second true there: the second's raise raises
            String loud = backend.dialect().render(mapped(list, new com.legend.sql.SqlExpr.Case(java.util.List.of(
                    new com.legend.sql.SqlExpr.Case.When(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.GREATER,
                            len, new com.legend.sql.SqlExpr.IntLit(9)), new com.legend.sql.SqlExpr.StringLit("long")),
                    new com.legend.sql.SqlExpr.Case.When(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL, x),
                            raise)), x, com.legend.sql.SqlExpr.Reason.RAISE)));
            java.sql.SQLException e = assertThrows(java.sql.SQLException.class, () -> st.executeQuery(loud).close(),
                    loud);
            assertTrue(String.valueOf(e.getMessage()).contains("boom"), "the later branch's raise: " + e.getMessage());
        }
    }

    /** {@code SELECT arrayMap(x -> body, list) AS v}. */
    private static com.legend.sql.SqlSelect mapped(com.legend.sql.SqlExpr list, com.legend.sql.SqlExpr body) {
        return com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.Dual()).withProjections(java.util.List.of(
                new com.legend.sql.SqlSelect.Projection(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_TRANSFORM,
                        list, new com.legend.sql.SqlExpr.Lambda(java.util.List.of("x"), body)), "v", null)));
    }

    /** The range raise this dialect writes for date arithmetic (ClickHouse clamps a date past 2299): dates in range,
     *  adjusted in a map's lambda, answer; one adjusted past 2299 still raises the product's kind. */
    @Test
    void dateArithmeticInALambdaRaisesPastTheYearsAlone() throws Exception {
        TestBackend backend = TestBackends.of(DatabaseType.ClickHouse);
        String model = backend.withStorelessRuntime("");
        String adjust = "->map(d|$d->meta::pure::functions::date::adjust(1, meta::pure::functions::date::DurationUnit.DAYS)"
                + "->meta::pure::functions::date::dayOfMonth())->sum()";
        try (Connection c = backend.open()) {
            Lean.Result ok = Lean.run(model, "|[%2024-01-01, %2024-02-01]" + adjust, StorelessRuntime.RUNTIME, c);
            assertEquals(4L, ((Number) ok.rows().get(0).get(0)).longValue(), "both in range: 2 + 2");
            Exception e = assertThrows(Exception.class, () -> Lean.run(model, "|[%2024-01-01, %2299-12-31]" + adjust,
                    StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage()).contains(com.legend.error.ErrorKind.VALUE_NOT_HELD.sentence()),
                    "a date adjusted past 2299 still raises the product's kind: " + e.getMessage());
        }
    }
}
