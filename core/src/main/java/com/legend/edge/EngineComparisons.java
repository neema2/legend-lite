// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.compiler.spec.CrossTypeRule;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * LEGEND-ENGINE'S COMPARISONS ACROSS TYPES (B3 step 5, the one equality; the user's ruling of 2026-11-24, "Love it":
 * strict typing is lite's alone). legend-engine sends a comparison to the database as written, so {@code 1 == 1.0} is
 * the database's {@code 1 = 1.0} (true) and an enumeration column against a String compares the column's stored value
 * (the lowering's {@code EnumSourceValues}). This is legend-engine's door's comparison setting
 * ({@link CrossTypeRule}; the edition program's step 2b, 2026-11-26): the type-checker asks it wherever it types a
 * comparison across types, in the query and in every body the query reaches, and the answer is that comparison
 * again, the database's -- the call the typer typed, unchanged. Until step 2b the door rewrote the typed query's
 * markers after it was typed, and a body was never asked about.
 */
public final class EngineComparisons implements CrossTypeRule {

    /** legend-engine's setting. */
    static final EngineComparisons RULE = new EngineComparisons();

    private EngineComparisons() {
    }

    /** {@code query} typed with each comparison across types answered by the database, as legend-engine sends it. The
     *  door asks where it types the query, before anything reads its body ({@code TypedQuery.comparing}). */
    public static TypedQuery asTheDatabases(TypedQuery query) {
        return query.comparing(RULE);
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }

    @Override
    public boolean readsBodies() {
        return false;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }
}
