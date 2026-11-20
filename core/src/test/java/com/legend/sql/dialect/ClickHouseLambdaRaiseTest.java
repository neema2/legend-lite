// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;
import com.legend.testbackend.StorelessRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A GUARDED RAISE INSIDE A LAMBDA ON CLICKHOUSE (2026-11-20, the warm lane's finding, measured with {@code clickhouse
 * local} 26.3.60.2): ClickHouse evaluates every branch of a CASE inside a lambda for every element, short-circuit
 * evaluation or not, so a guarded raise ({@code CASE WHEN cond THEN <raise> ELSE value END}) raised for valid data -- a
 * substring's range guard in {@code map}'s lambda raised {@code SUBSTRING_OUT_OF_RANGE} over {@code ['Doe', 'Smith',
 * 'Branche']}. Written in a lambda, the raise carries the condition its branch is taken on ({@code toInt64(if(cond,
 * <message>, '0'))}); outside one, where ClickHouse short-circuits, it is written as before. The answers on the server
 * are {@code ClickHouseLambdaRaiseServerTest}'s.
 */
class ClickHouseLambdaRaiseTest {

    private static String sql(String query) {
        String model = StorelessRuntime.onServer("", DatabaseType.ClickHouse, "localhost", 1, "legend");
        return Compiler.query(Compiler.compileModel(model), query).plan(StorelessRuntime.RUNTIME).sql();
    }

    @Test
    void aSubstringsGuardInAMapsLambdaCarriesItsCondition() {
        String s = sql("|['Doe', 'Smith', 'Branche']->map(x|$x->substring(1, 2))->joinStrings(',')");
        assertTrue(s.contains("arrayMap("), "the map is a lambda: " + s);
        assertTrue(s.contains("toInt64(if("), "the raise in the lambda carries its condition: " + s);
        assertFalse(s.contains("toInt64(concat("), "no unguarded raise in the lambda: " + s);
    }

    @Test
    void aRaiseOutsideALambdaIsWrittenAsBefore() {
        String s = sql("|'Doe'->substring(1, 2)");
        assertTrue(s.contains("toInt64(concat("), "the CASE short-circuits outside a lambda: " + s);
        assertFalse(s.contains("toInt64(if("), s);
    }

    /** The raises this dialect writes in its own CASEs carry their conditions too: date arithmetic's range raise
     *  (ClickHouse clamps a date past its years) inside a map's lambda. */
    @Test
    void dateArithmeticsRangeRaiseInALambdaCarriesItsCheck() {
        String s = sql("|[%2024-01-01, %2024-02-01]->map(d|$d->meta::pure::functions::date::adjust(1,"
                + " meta::pure::functions::date::DurationUnit.DAYS))->size()");
        assertTrue(s.contains("arrayMap("), "the map is a lambda: " + s);
        assertTrue(s.contains("toInt64(if("), "the range raise in the lambda carries its check: " + s);
        assertFalse(s.contains("toInt64(concat("), "no unguarded raise in the lambda: " + s);
        String plain = sql("|%2024-01-01->meta::pure::functions::date::adjust(1,"
                + " meta::pure::functions::date::DurationUnit.DAYS)");
        assertTrue(plain.contains("toInt64(concat(") && !plain.contains("toInt64(if("), "outside a lambda: " + plain);
    }

    /** A date parameter's placeholder, a CASE this dialect writes, raises for a year ClickHouse cannot hold or a digit
     *  past the microsecond: in a lambda, each raise carries its own condition, the second also the first's not true;
     *  outside one, as before. */
    @Test
    void aDateParametersRaisesInALambdaCarryTheirConditions() {
        SqlExpr.PlanParam p = new SqlExpr.PlanParam("d", SqlExpr.PlanParam.ValueType.DATETIME, false,
                new com.legend.sql.TypeFact.Typed(SqlType.Scalar.TIMESTAMP, true), com.legend.sql.DeclaredKind.DATE_TIME);
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, new SqlExpr.ArrayLit(List.of(new SqlExpr.IntLit(1))),
                new SqlExpr.Lambda(List.of("x"), p));
        String s = new ClickHouse().renderStatement(SqlSelect.starOf(new com.legend.sql.SqlSource.Dual())
                .withProjections(List.of(new SqlSelect.Projection(mapped, "v", null)))).sql();
        assertEquals(2, s.split("toInt64\\(if\\(", -1).length - 1, "both raises guarded: " + s);
        assertTrue(s.contains("toInt64(if(NOT coalesce(toInt32(substring("), "the fraction's raise, the year's not true: "
                + s);
        String plain = new ClickHouse().renderStatement(SqlSelect.starOf(new com.legend.sql.SqlSource.Dual())
                .withProjections(List.of(new SqlSelect.Projection(p, "v", null)))).sql();
        assertEquals(2, plain.split("toInt64\\(concat\\(", -1).length - 1, "outside a lambda, as before: " + plain);
        assertFalse(plain.contains("toInt64(if("), plain);
    }

    /** Each branch's guard: the THEN's its condition; a later branch's its condition with every earlier one not true
     *  (false or NULL); the ELSE's every condition not true. */
    @Test
    void eachBranchsRaiseIsGuardedByTheConditionItIsTakenOn() {
        SqlExpr x = SqlExpr.Column.param("x", new SqlExpr.Column(null, "x"));
        SqlExpr a = SqlExpr.Call.of(SqlFn.GREATER, x, new SqlExpr.IntLit(10));
        SqlExpr b = SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0));
        SqlExpr raise = SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit("boom"));
        SqlExpr body = new SqlExpr.Case(List.of(new SqlExpr.Case.When(a, raise), new SqlExpr.Case.When(b, raise)),
                raise, SqlExpr.Reason.RAISE);
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, new SqlExpr.ArrayLit(List.of(new SqlExpr.IntLit(1))),
                new SqlExpr.Lambda(List.of("x"), body));
        String s = new ClickHouse().render(SqlSelect.starOf(new com.legend.sql.SqlSource.Dual())
                .withProjections(List.of(new SqlSelect.Projection(mapped, "v", null))));
        assertEquals(3, s.split("toInt64\\(if\\(", -1).length - 1, "three guarded raises: " + s);
        assertTrue(s.contains("THEN toInt64(if(\"x\" > toInt64(10), concat("), "the first branch's guard: " + s);
        assertTrue(s.contains("THEN toInt64(if(NOT (coalesce(\"x\" > toInt64(10), FALSE)) AND \"x\" < toInt64(0),"
                + " concat("), "the second branch's guard: " + s);
        assertTrue(s.contains("ELSE toInt64(if(NOT (coalesce(\"x\" > toInt64(10), FALSE)) AND NOT (coalesce(\"x\" <"
                + " toInt64(0), FALSE)), concat("), "the ELSE's guard: " + s);
        // and the same CASE outside a lambda: unguarded, as before
        String plain = new ClickHouse().render(SqlSelect.starOf(new com.legend.sql.SqlSource.Dual())
                .withProjections(List.of(new SqlSelect.Projection(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER, new SqlExpr.IntLit(1), new SqlExpr.IntLit(10)), raise)),
                        new SqlExpr.IntLit(1), SqlExpr.Reason.RAISE), "v",
                        new com.legend.sql.OutputCol("v", SqlType.Scalar.BIGINT, false)))));
        assertFalse(plain.contains("toInt64(if("), plain);
    }
}
