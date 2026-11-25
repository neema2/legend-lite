// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * LEGEND-ENGINE'S COMPARISONS ACROSS TYPES (B3 step 5, the one equality; the user's ruling of 2026-11-24, "Love it":
 * strict typing is lite's alone). legend-engine sends a comparison to the database as written, so {@code 1 == 1.0} is
 * the database's {@code 1 = 1.0} (true) and an enumeration column against a String compares the column's stored value
 * (the lowering's {@code EnumSourceValues}). At legend-engine's door each comparison the typer marked as across types
 * is that comparison again, the database's -- the call the typer typed, unchanged; lite's planner refuses any marker
 * that reaches it.
 */
public final class EngineComparisons {

    private EngineComparisons() {
    }

    /** {@code query} with each comparison across types answered by the database, as legend-engine sends it; the query
     *  itself where it has none. */
    public static TypedQuery asTheDatabases(TypedQuery query) {
        List<TypedSpec> out = new ArrayList<>(query.body().size());
        boolean changed = false;
        for (TypedSpec statement : query.body()) {
            TypedSpec rewritten = rewrite(statement, TypedCrossTypeComparison::comparison);
            changed |= rewritten != statement;
            out.add(rewritten);
        }
        return changed ? query.withBody(out) : query;
    }

    /** {@code n} with each comparison across types replaced by {@code door}'s answer for it, innermost first: the walk
     *  both doors share (legend-engine's, legend-pure's {@link PureComparisons}). */
    static TypedSpec rewrite(TypedSpec n, java.util.function.Function<TypedCrossTypeComparison, TypedSpec> door) {
        TypedSpec rebuilt = n.mapChildren(k -> rewrite(k, door));
        return rebuilt instanceof TypedCrossTypeComparison c ? door.apply(c) : rebuilt;
    }
}
