// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import java.util.List;

/**
 * THE COMPATIBILITY RULES' RAISES ({@link com.legend.platform.PlanOption#COMPATIBILITY_RAISES}; the user's ruling of
 * 2026-11-16, register S-batch-3): where legend-pure raises and lite's product answers, a query planned under the
 * compatibility rules raises pure's exception in pure's words. Lite's own rules never plan one.
 *
 * <p>One today: {@code round(x, scale)} over a Float that is NaN or an infinity. Legend-pure reads the Float through
 * {@code BigDecimal.valueOf} ({@code CoreHelper.round(Double, long)}, 5.99.0), which raises a NumberFormatException;
 * its words are the JDK's, and legend-engine 4.145.0 builds, tests and releases on JDK 17 (its workflows'
 * {@code java-version: 17}), whose {@code BigDecimal} names the first character it cannot read -- {@code N} for
 * {@code NaN}, {@code I} for either infinity (its sign is read) -- measured on JDK 21, the same text. JDK 25 says
 * {@code Infinite or NaN} instead (the register row records it). The product passes the value through, as IEEE and the
 * databases do.
 */
final class CompatibilityRaises {

    /** Legend-pure's words for {@code round(NaN, scale)}. */
    static final String NAN = "Character N is neither a decimal digit number, decimal point, nor \"e\" notation"
            + " exponential mark.";

    /** Legend-pure's words for {@code round(x, scale)} over either infinity. */
    static final String INFINITY = "Character I is neither a decimal digit number, decimal point, nor \"e\" notation"
            + " exponential mark.";

    private CompatibilityRaises() {
    }

    /** {@code lowered}, {@code call}'s product lowering, raising pure's exception where pure raises and the product
     *  answers; {@code lowered} itself for every other call. */
    static SqlExpr of(TypedNativeCall call, List<SqlExpr> args, SqlExpr lowered) {
        if (!Pure.AT_MATH_ROUND.contains(call.callee().id()) || args.size() != 2) {
            return lowered;
        }
        // a Float, or a Number that may be one: a Decimal is always finite
        Type t = call.args().get(0).info().type();
        if (t != Type.Primitive.FLOAT && t != Type.Primitive.NUMBER) {
            return lowered;
        }
        SqlExpr x = args.get(0);
        // x - x = 0 holds for a finite double alone (measured on DuckDB and H2: false for NaN and both infinities)
        SqlExpr nonFinite = SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, x),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.MINUS, x, x),
                        new SqlExpr.IntLit(0))));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NAN, x),
                        SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(NAN))),
                new SqlExpr.Case.When(nonFinite, SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(INFINITY)))),
                lowered);
    }
}
