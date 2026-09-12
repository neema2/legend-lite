// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/** Read-only MIR probes shared by scalar lowering rules. */
final class SqlProbes {

    private SqlProbes() {
    }

    /**
     * Star + plain-column renames, nothing else — the shape a prefixed join
     * produces. Such a select adds no row semantics; it can host further
     * joins with its renames carried forward (the Lowerer's join site).
     */
    static boolean isRenameOnlySelect(SqlSelect s) {
        if (s.projections().isEmpty() || s.distinct()
                || s.where() != null || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || !s.orderBy().isEmpty()
                || s.limit() != null || s.offset() != null) {
            return false;
        }
        if (!(s.from() instanceof SqlSource.Join || s.from() instanceof SqlSource.Table)) {
            return false;
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (!(p.expr() instanceof SqlExpr.Star || p.expr() instanceof SqlExpr.Column)) {
                return false;
            }
        }
        return true;
    }

    /** Whether the expression tree carries a scalar subquery or exists
     * (walked via the shared MIR rewriter; probe-only). */
    static boolean containsSubquery(SqlExpr e) {
        boolean[] hit = {false};
        var probe = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.ScalarSubquery
                        || x instanceof SqlExpr.Exists) {
                    hit[0] = true;
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        };
        probe.scan(e);
        return hit[0];
    }
}
