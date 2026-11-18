// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.function.Supplier;

/**
 * The engine's SECOND exists emission at lowering ({@link com.legend.sql.ExistsJoins}, where the rewrite lives since the
 * edge audit's J19, 2026-11-18, so a dialect can decompose an EXISTS at render too): every qualifying conjunct
 * rewritten, the DISTINCT-key subselect's temporal conditions first by the lowering's resolver zones.
 */
final class ExistsJoinForm {

    private ExistsJoinForm() {
    }

    /** {@code outer}'s qualifying EXISTS conjuncts as joins to the distinct keys ({@link com.legend.sql.ExistsJoins}). */
    static SqlSelect rewrite(SqlSelect outer, Supplier<String> alias,
            java.util.function.Function<SqlExpr, WhereMerge.@com.legend.base.Nullable Zones> zones) {
        return com.legend.sql.ExistsJoins.rewrite(outer, alias, w -> {
            WhereMerge.Zones z = zones.apply(w);
            return z == null ? null : new com.legend.sql.ExistsJoins.Zones(z.user(), z.temporal());
        }, ex -> true);
    }
}
