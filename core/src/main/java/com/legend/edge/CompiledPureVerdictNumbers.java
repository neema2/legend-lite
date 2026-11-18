// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * COMPILED LEGEND-PURE'S NUMBER EQUALITY, THE DATABASE JUDGE'S (train 17, the orchestrator's item 4 of 2026-11-18:
 * moved from the product's VerdictSql, switched on until then by the plan's judging rules alone): the edge's form of
 * the two-form {@link com.legend.lowering.VerdictNumbers} kind -- two Floats within 2 ULP equal (S15/S132), a
 * Decimal's scale part of its value (S139), as the host judge's {@link CompiledPureHostForms}.
 */
public enum CompiledPureVerdictNumbers implements com.legend.lowering.VerdictNumbers {
    INSTANCE;

    @Override
    public boolean floatTolerance() {
        return true;
    }

    @Override
    public boolean decimalByValue(Type kind) {
        return false;
    }

    /** Two finite doubles within {@code FLOAT_ULPS * ulp(max(|x|, |y|))} (the host judge's same number,
     *  {@link CompiledPureHostForms#FLOAT_ULPS}): S15's Float leniency over two values, ulp spelled {@code 2^(floor(log2(max)) - 52)}
     *  through {@code ln} (a boundary at an exact power of two may differ from Math.ulp by one binade). The leniency's
     *  constants are DOUBLES, said so: a dialect whose numeric charter spells a Float literal as an exact Decimal
     *  (ClickHouse) refused the arithmetic over Float64 cells (channel B on ClickHouse, step 9, 2026-10-19). */
    @Override
    public SqlExpr floatsWithin(SqlExpr x, SqlExpr y) {
        SqlExpr big = SqlExpr.Call.of(SqlFn.GREATEST, SqlExpr.Call.of(SqlFn.ABS, x), SqlExpr.Call.of(SqlFn.ABS, y));
        SqlExpr zero = new SqlExpr.Cast(new SqlExpr.FloatLit(0.0), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr two = new SqlExpr.Cast(new SqlExpr.FloatLit(2.0), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr ulps = new SqlExpr.Cast(new SqlExpr.FloatLit(CompiledPureHostForms.FLOAT_ULPS), SqlType.Scalar.DOUBLE,
                SqlExpr.Reason.JUDGE);
        SqlExpr twoUlp = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, big, zero), zero)),
                SqlExpr.Call.of(SqlFn.TIMES, ulps, SqlExpr.Call.of(SqlFn.POW, two, SqlExpr.Call.of(SqlFn.MINUS,
                        SqlExpr.Call.of(SqlFn.FLOOR, SqlExpr.Call.of(SqlFn.DIVIDE, SqlExpr.Call.of(SqlFn.LN, big),
                                SqlExpr.Call.of(SqlFn.LN, two))), new SqlExpr.IntLit(52)))), SqlExpr.Reason.JUDGE);
        SqlExpr finite = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(Double.MAX_VALUE)),
                SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, y), new SqlExpr.FloatLit(Double.MAX_VALUE)));
        return SqlExpr.Call.of(SqlFn.AND, finite, SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, x, y)), twoUlp));
    }
}
