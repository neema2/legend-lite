// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Portable IEEE-754 double-precision decomposition of {@link SqlFn#ERF}
 * ({@code erf(x) = (2 / sqrt(pi)) * integral_0^x exp(-t^2) dt}), using W. J. Cody's
 * three-interval Chebyshev rational minimax approximation (Mathematics of Computation
 * 23(107), 1969, pp. 631&ndash;637; max absolute error {@code <= 2.22e-16} across all
 * real {@code x}).
 *
 * <p>Dialects with a native {@code erf} builtin ({@link ClickHouse}, {@link Databricks})
 * or a list-lambda binding ({@link DuckDb}) set {@link AnsiSqlRenderer#spellsErf()};
 * all other dialects expand {@link SqlFn#ERF} through {@link Expansion}.
 */
final class ErfMath {

    private ErfMath() {
    }

    static final class Expansion extends SqlRewriter {

        private final boolean bindArg;
        private final int[] fresh = {0};
        private final Map<SqlExpr, SqlExpr> boundCalls = new HashMap<>();

        Expansion(boolean bindArg) {
            this.bindArg = bindArg;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.ERF) {
                SqlExpr rawArg = expr(c.args().get(0));
                if (!bindArg || isSimple(rawArg) || hasUnqualifiedColumn(rawArg)) {
                    return of(rawArg);
                }
                SqlExpr known = boundCalls.get(rawArg);
                if (known != null) {
                    return known;
                }
                String alias = "__erf" + fresh[0]++;
                OutputCol xCol = new OutputCol("x", SqlType.Scalar.DOUBLE, true);
                SqlExpr ref = SqlExpr.Column.of(alias, "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
                SqlSelect bind = new SqlSelect(
                        List.of(new SqlSelect.Projection(rawArg, "x", xCol)),
                        false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
                SqlExpr body = of(ref);
                SqlSelect use = new SqlSelect(
                        List.of(new SqlSelect.Projection(body, "v", new OutputCol("v", SqlType.Scalar.DOUBLE, true))),
                        false, new SqlSource.Subselect(bind, alias, null), null, List.of(), null, null, List.of(),
                        null, null, List.of());
                SqlExpr sq = new SqlExpr.ScalarSubquery(use);
                boundCalls.put(rawArg, sq);
                return sq;
            }
            return super.expr(e);
        }

        private static boolean isSimple(SqlExpr e) {
            return e instanceof SqlExpr.Column
                    || e instanceof SqlExpr.FloatLit
                    || e instanceof SqlExpr.IntLit
                    || e instanceof SqlExpr.DecimalLit;
        }

        private static boolean hasUnqualifiedColumn(SqlExpr e) {
            Set<String> cols = new HashSet<>();
            collectUnqualified(e, cols);
            return !cols.isEmpty();
        }

        private static void collectUnqualified(SqlExpr e, Set<String> out) {
            if (e instanceof SqlExpr.Column c && c.table() == null) {
                out.add(c.name());
            }
            for (SqlExpr ch : e.children()) {
                collectUnqualified(ch, out);
            }
        }
    }

    /** Cody (1969) three-region rational minimax expression for {@code erf(x)} over SQL primitives. */
    static SqlExpr of(SqlExpr rawX) {
        return ofBound(new SqlExpr.Cast(rawX, SqlType.Scalar.DOUBLE));
    }

    /** Cody (1969) three-region rational minimax expression when {@code x} is already a bound {@code DOUBLE} reference. */
    static SqlExpr ofBound(SqlExpr x) {
        SqlExpr ax = call(SqlFn.ABS, x);
        SqlExpr xNonNeg = call(SqlFn.GREATER_EQUAL, x, lit(0.0));

        // Region 1: |x| <= 0.46875 -> x + x * P(x^2) / Q(x^2)
        SqlExpr xSq = mul(ax, ax);
        SqlExpr p1 = horner(xSq,
                -2.37630166566501626084e-05,
                -5.77027029648944159157e-03,
                -2.84817495755985104766e-02,
                -3.25042107247001499370e-01,
                1.28379167095512558561e-01);
        SqlExpr q1 = horner(xSq,
                -3.96022827877536812320e-06,
                1.32494738004321644526e-04,
                5.08130628187576562776e-03,
                6.50222499887672944485e-02,
                3.97917223959155352819e-01,
                1.0);
        SqlExpr branch1 = add(x, mul(x, div(p1, q1)));

        // Guarded exp(-min(x^2, 700.0)) so eager evaluation on huge |x| cannot overflow x^2 or underflow noisily
        SqlExpr expNegSq = call(SqlFn.EXP, neg(call(SqlFn.LEAST, xSq, lit(700.0))));

        // Region 2: 0.46875 < |x| <= 4.0 -> signed (1 - exp(-x^2) * R(|x|) / S(|x|))
        SqlExpr p2 = horner(ax,
                2.15311535474403846e-08,
                5.64188496988670089e-01,
                8.88314979438837594e+00,
                6.61191906371416295e+01,
                2.98635138197400131e+02,
                8.81952221241769090e+02,
                1.71204761263407058e+03,
                2.05107837782607147e+03,
                1.23033935479799725e+03);
        SqlExpr q2 = horner(ax,
                1.0,
                1.57449261107098347e+01,
                1.17693950891312499e+02,
                5.37181101862009858e+02,
                1.62138957456669019e+03,
                3.29079923573345963e+03,
                4.36261909014324716e+03,
                3.43936767414372164e+03,
                1.23033935480374942e+03);
        SqlExpr erfc2 = mul(expNegSq, div(p2, q2));
        SqlExpr branch2 = signedComplement(xNonNeg, erfc2);

        // Region 3: 4.0 < |x| < 27.0 -> asymptotic expansion in y^2 = 1 / max(|x|, 4.0)^2 (zero-div safe everywhere)
        SqlExpr safeAx = call(SqlFn.GREATEST, ax, lit(4.0));
        SqlExpr invSq = div(lit(1.0), mul(safeAx, safeAx));
        SqlExpr p3 = horner(invSq,
                1.63153871373020978e-02,
                3.05326634961232344e-01,
                3.60344899949804439e-01,
                1.25781726111229246e-01,
                1.60837851487422766e-02,
                6.58749161529837803e-04);
        SqlExpr q3 = horner(invSq,
                1.0,
                2.56852019228982242e+00,
                1.87295284992346047e+00,
                5.27905102951428412e-01,
                6.05183413124413191e-02,
                2.33520497626869185e-03);
        SqlExpr oneOverSqrtPi = div(lit(1.0), call(SqlFn.SQRT, new SqlExpr.Call(SqlFn.PI, List.of())));
        SqlExpr erfc3 = div(mul(expNegSq, sub(oneOverSqrtPi, mul(invSq, div(p3, q3)))), safeAx);
        SqlExpr branch3 = signedComplement(xNonNeg, erfc3);

        // Region 4: |x| >= 27.0 -> +/- 1.0 to IEEE-754 double precision
        SqlExpr branch4 = new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(xNonNeg, lit(1.0))),
                lit(-1.0));

        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(call(SqlFn.LESS_EQUAL, ax, lit(0.46875)), branch1),
                new SqlExpr.Case.When(call(SqlFn.LESS_EQUAL, ax, lit(4.0)), branch2),
                new SqlExpr.Case.When(call(SqlFn.LESS, ax, lit(27.0)), branch3)),
                branch4);
    }

    private static SqlExpr signedComplement(SqlExpr xNonNeg, SqlExpr erfc) {
        SqlExpr sign = new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(xNonNeg, lit(1.0))),
                lit(-1.0));
        return mul(sign, sub(lit(1.0), erfc));
    }

    private static SqlExpr horner(SqlExpr z, double... coeffs) {
        SqlExpr acc = lit(coeffs[0]);
        for (int i = 1; i < coeffs.length; i++) {
            acc = add(mul(acc, z), lit(coeffs[i]));
        }
        return acc;
    }

    private static SqlExpr lit(double v) {
        return new SqlExpr.FloatLit(v);
    }

    private static SqlExpr add(SqlExpr a, SqlExpr b) {
        return SqlExpr.Call.of(SqlFn.PLUS, a, b);
    }

    private static SqlExpr sub(SqlExpr a, SqlExpr b) {
        return SqlExpr.Call.of(SqlFn.MINUS, a, b);
    }

    private static SqlExpr mul(SqlExpr a, SqlExpr b) {
        return SqlExpr.Call.of(SqlFn.TIMES, a, b);
    }

    private static SqlExpr div(SqlExpr a, SqlExpr b) {
        return SqlExpr.Call.of(SqlFn.DIVIDE, a, b);
    }

    private static SqlExpr neg(SqlExpr a) {
        return SqlExpr.Call.of(SqlFn.NEGATE, a);
    }

    private static SqlExpr call(SqlFn fn, SqlExpr... args) {
        return new SqlExpr.Call(fn, List.of(args));
    }
}
