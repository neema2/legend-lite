// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.function.Supplier;

/**
 * The sites of the engine's SECOND exists emission at lowering (the rewrite itself the edge's form's call, the lens audit's
 * item 1, 2026-11-18) ({@link com.legend.sql.ExistsJoins}, where the rewrite lives since the
 * edge audit's J19, 2026-11-18, so a dialect can decompose an EXISTS at render too): every qualifying conjunct
 * rewritten, the DISTINCT-key subselect's temporal conditions first by the lowering's resolver zones.
 */
final class ExistsJoinForm {

    private ExistsJoinForm() {
    }

    /** The statement's sites an EXISTS join reads ({@link ExistsSites}): {@code alias}, and each
     *  conjunct's resolver zones as the sql layer reads them. */
    static ExistsSites sites(Supplier<String> alias,
            java.util.function.Function<SqlExpr, WhereMerge.@com.legend.base.Nullable Zones> zones) {
        return new ExistsSites(alias, w -> {
            WhereMerge.Zones z = zones.apply(w);
            return z == null ? null : new com.legend.sql.ExistsJoins.Zones(z.user(), z.temporal());
        });
    }
}
