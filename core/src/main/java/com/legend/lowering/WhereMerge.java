// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * WHERE conjunct ZONE ordering (engine parity): user conds first, then
 * exists CORRELATION conds, then MILESTONING type filters &mdash;
 * {@code buildExistsPredicate} seeds the subselect with the user
 * predicate, the join correlation appends, and
 * {@code applyMilestoningTypeFilters} appends LAST. The JOIN-DISTINCT
 * exists form spells the OPPOSITE order (temporal first &mdash; the
 * child's milestoning is applied during child processing, the pred
 * concatenates after), so {@link ExistsJoinForm} consumes the zones too.
 */
final class WhereMerge {

    private WhereMerge() {
    }

    /** A WHERE split into the engine's conjunct zones; each nullable. */
    record Zones(@com.legend.base.Nullable SqlExpr user,
            @com.legend.base.Nullable SqlExpr corr,
            @com.legend.base.Nullable SqlExpr temporal) { }

    /**
     * Merge {@code predicate} into {@code existing} in zone order,
     * consulting and updating {@code registry} (whereExpr identity &rarr;
     * zone split; an unregistered where is all user-zone — the historical
     * append behavior).
     */
    static SqlExpr merge(
            java.util.IdentityHashMap<SqlExpr, Zones> registry,
            @com.legend.base.Nullable SqlExpr existing, SqlExpr predicate,
            com.legend.compiler.spec.typed.TypedFilter.Stamp stamp) {
        Zones z = existing == null
                ? new Zones(null, null, null)
                : registry.getOrDefault(existing,
                        new Zones(existing, null, null));
        Zones nz = switch (stamp) {
            case NONE -> new Zones(and(z.user(), predicate),
                    z.corr(), z.temporal());
            case CORRELATION -> new Zones(z.user(),
                    and(z.corr(), predicate), z.temporal());
            case TEMPORAL -> new Zones(z.user(), z.corr(),
                    and(z.temporal(), predicate));
        };
        SqlExpr merged = and(and(nz.user(), nz.corr()), nz.temporal());
        if (nz.corr() != null || nz.temporal() != null) {
            registry.put(merged, nz);
        }
        return java.util.Objects.requireNonNull(merged, "empty WHERE merge");
    }

    static @com.legend.base.Nullable SqlExpr and(@com.legend.base.Nullable SqlExpr a,
            @com.legend.base.Nullable SqlExpr b) {
        return a == null ? b : b == null ? a : Fold.mergeAnd(a, b);
    }
}
