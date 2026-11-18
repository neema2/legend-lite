// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.sql.ExistsJoins;
import com.legend.sql.SqlExpr;

/**
 * LEGEND-ENGINE'S WHERE CONJUNCT ZONES (moved from lowering.WhereMerge, L10, the lens audit's L-A.unionAlias,
 * 2026-11-18): user conditions first, then exists CORRELATION conditions, then MILESTONING type filters --
 * {@code buildExistsPredicate} seeds the subselect with the user predicate, the join correlation appends, and
 * {@code applyMilestoningTypeFilters} appends LAST. The join-to-distinct-keys exists form ({@code existsJoin}) spells the
 * opposite order (temporal first), so it reads each merged WHERE's split ({@link #of}). One per lowering: the
 * registry is keyed by the WHERE's identity.
 */
final class EngineWhereZones implements com.legend.lowering.WhereZones {

    /** A WHERE split into legend-engine's conjunct zones; each nullable. */
    private record Zones(@com.legend.base.Nullable SqlExpr user, @com.legend.base.Nullable SqlExpr corr,
            @com.legend.base.Nullable SqlExpr temporal) {
    }

    /** whereExpr identity -> its zone split: written whenever a resolver-stamped filter folds, consumed as later
     *  filters fold onto the same WHERE and by the exists form. */
    private final java.util.IdentityHashMap<SqlExpr, Zones> registry = new java.util.IdentityHashMap<>();

    /** {@code predicate} merged into {@code existing} in zone order; an unregistered WHERE is all user zone. */
    @Override
    public SqlExpr merge(@com.legend.base.Nullable SqlExpr existing, SqlExpr predicate, TypedFilter.Stamp stamp) {
        Zones z = existing == null ? new Zones(null, null, null)
                : registry.getOrDefault(existing, new Zones(existing, null, null));
        Zones nz = switch (stamp) {
            case NONE -> new Zones(and(z.user(), predicate), z.corr(), z.temporal());
            case CORRELATION -> new Zones(z.user(), and(z.corr(), predicate), z.temporal());
            case TEMPORAL -> new Zones(z.user(), z.corr(), and(z.temporal(), predicate));
        };
        SqlExpr merged = and(and(nz.user(), nz.corr()), nz.temporal());
        if (nz.corr() != null || nz.temporal() != null) {
            registry.put(merged, nz);
        }
        return java.util.Objects.requireNonNull(merged, "empty WHERE merge");
    }

    /** legend-engine's DISTINCT-key subselect WHERE: its temporal conditions first, then the user predicate (the
     *  child's milestoning applies during child processing; buildExistsAsJoinWithNullCheck concatenates the predicate)
     *  -- only where the zone split exactly accounts for the local conjuncts; else encounter order (moved from
     *  sql.ExistsJoins.keysWhere, L10, 2026-11-18). */
    @Override
    public @com.legend.base.Nullable SqlExpr keysWhere(java.util.List<SqlExpr> local,
            @com.legend.base.Nullable SqlExpr where) {
        Zones z = where == null ? null : registry.get(where);
        if (z != null && z.temporal() != null) {
            SqlExpr reordered = z.user() == null ? z.temporal() : ExistsJoins.mergeAnd(z.temporal(), z.user());
            java.util.List<SqlExpr> flat = new java.util.ArrayList<>();
            flatten(reordered, flat);
            if (flat.size() == local.size() && new java.util.HashSet<>(flat).equals(new java.util.HashSet<>(local))) {
                return reordered;
            }
        }
        return ExistsJoins.encounterOrder(local, where);
    }

    /** {@code e}'s AND chain flattened, groups of ANDs included (ExistsJoins' own rule). */
    private static void flatten(SqlExpr e, java.util.List<SqlExpr> out) {
        if (e instanceof SqlExpr.Group g && g.inner() instanceof SqlExpr.Call c && c.fn() == com.legend.sql.SqlFn.AND) {
            c.args().forEach(x -> flatten(x, out));
        } else if (e instanceof SqlExpr.Call c2 && c2.fn() == com.legend.sql.SqlFn.AND) {
            c2.args().forEach(x -> flatten(x, out));
        } else {
            out.add(e);
        }
    }

    private static @com.legend.base.Nullable SqlExpr and(@com.legend.base.Nullable SqlExpr a,
            @com.legend.base.Nullable SqlExpr b) {
        return a == null ? b : b == null ? a : ExistsJoins.mergeAnd(a, b);
    }
}
