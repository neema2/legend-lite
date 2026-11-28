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

    /** A QUERY AS LEGEND-ENGINE'S DOOR ADMITS IT: typed, with the door's comparison setting handed over before
     *  anything reads its body or a body it reaches. The door's ONE way to type a query
     *  ({@code ArchitectureTest.legendEnginesDoorTypesAQueryThroughItsSetting}; the audit of the edition program's
     *  steps 4 and 5, 2026-11-26): a route that typed one on its own ({@code Compiler.query}) was handed lite's
     *  setting in silence, and once lite read bodies (step 4) the test-data routes refused a legend-engine model whose
     *  function compares across types -- through the door's own raw-SQL check, which types the bodies a query
     *  reaches. */
    static TypedQuery query(com.legend.compiler.element.ModelContext ctx, String query) {
        return com.legend.Compiler.query(ctx, query).comparing(RULE);
    }

    /** {@link #query(com.legend.compiler.element.ModelContext, String)} for a query given as a value. */
    static TypedQuery query(com.legend.compiler.element.ModelContext ctx,
            com.legend.protocol.spec.ValueSpecification query) {
        return com.legend.Compiler.query(ctx, query).comparing(RULE);
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }

    @Override
    public boolean readsBodies() {
        return false;
    }

    /** SPIKE (slice 7): legend-engine's comparisons -- three number types, and an equality's empty side by where it
     *  runs. */
    @Override
    public com.legend.compiler.spec.typed.ComparisonReading reading() {
        return com.legend.compiler.spec.typed.ComparisonReading.THREE_NUMBER_TYPES_EMPTY_SIDE_BY_PLACE;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return crossing.comparison();
    }
}
