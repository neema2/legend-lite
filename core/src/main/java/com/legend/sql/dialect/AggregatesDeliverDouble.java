// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlType;

import java.util.Set;

/**
 * The dialect DELIVERS the platform's type facts on its wire. The platform types these aggregates
 * DOUBLE (DuckDB's truth); a dialect whose database answers them otherwise names them, and each is
 * cast to DOUBLE over its exact result:
 * <ul>
 * <li>H2 computes every average as DECFLOAT whatever its input (probed 2026-09-17: {@code
 * AVG(CAST(1.0 AS DOUBLE) * age)} is DECFLOAT), which a Float root then prints as
 * {@code 35.50000000000} or {@code 3E+1};</li>
 * <li>Postgres answers avg and the moments (stddev, variance, corr, covar) over an integer or a
 * numeric as numeric ({@code 1.00000000000000000000}, its PCT lane, 2026-10-02). The exact result,
 * cast, is the correctly rounded double on every platform; computing in double precision instead
 * gave {@code 3.1399999999999997} for 3.14 on x86 Linux (CI, 2026-10-02).</li>
 * </ul>
 * Grouped aggregates only: a windowed one is cast around its OVER clause by the dialect. The
 * boundary's no-cast decision for a DOUBLE-typed wire stays a platform decision.
 */
public final class AggregatesDeliverDouble extends SqlRewriter {

    private final Set<SqlAgg.Fn> fns;

    public AggregatesDeliverDouble(Set<SqlAgg.Fn> fns) {
        this.fns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(fns));
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlAgg a && fns.contains(a.fn())) {
            return new SqlExpr.Cast(super.expr(e), SqlType.Scalar.DOUBLE);
        }
        return super.expr(e);
    }
}
