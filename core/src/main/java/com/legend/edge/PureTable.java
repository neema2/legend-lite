// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.lowering.EditionTable;
import com.legend.lowering.NamedRule;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.planner.Edition;
import com.legend.platform.Feature;

import java.util.Set;

/**
 * LEGEND-PURE'S EDITION AND ITS TABLE (the edition program's step 6, slice 6.2; the user's rulings of 2026-11-26
 * ~23:10): what legend-pure's door hands the planner ({@link Edition}) and, through it, the lowering
 * ({@link EditionTable}). The door is the style {@link PureText#UPSTREAM}: PCT's legend-pure run and every plan made
 * in that style. Until this slice that style handed the product's own edition.
 *
 * <p>Its only rows are {@link UpstreamTies}': where equal numbers of different types land in a sort and which of
 * them {@code max}, {@code min}, {@code greatest} and {@code least} keep. Everything else is the product's own rule,
 * and every check the product writes is written.
 *
 * <p>Everything at this door is one audience's code, legend-pure's, so nothing is stamped and no flag is read, as at
 * lite's own doors; and the rows are the same on every database, so one table answers for all of them.
 */
final class PureTable implements EditionTable, Edition {

    /** legend-pure's door's edition, which is also its table on every database. */
    static final PureTable DOOR = new PureTable();

    private PureTable() {
    }

    // ---- the lowering's table ---------------------------------------------------------------------------------------

    @Override
    public @Nullable NamedRule lowersAs(TypedNativeCall call, Set<Feature> features) {
        NamedRule tie = UpstreamTies.rule(call);
        // ...and an empty TDS cell's text, legend-engine's TDS's own Pure, which legend-pure runs in a relational
        // test's own code (EmptyCellTexts; the edition program's step 8, 2026-11-28)
        return tie != null ? tie : EmptyCellTexts.rule(call);
    }

    /** legend-pure checks as the product does: every check is written. */
    @Override
    public boolean checks(com.legend.sql.RaiseKind kind, @Nullable TypedNativeCall call) {
        return true;
    }

    // ---- the planner's edition --------------------------------------------------------------------------------------

    @Override
    public boolean stampsOutsideStore() {
        return false;
    }

    @Override
    public boolean stampsTestBodies() {
        return false;
    }

    @Override
    public boolean constraintsOutsideStore() {
        return false;
    }

    @Override
    public boolean transformsOutsideStore(boolean fetchesAGraph, boolean chained) {
        return false;
    }

    @Override
    public Set<Feature> writtenFlagsRead() {
        return Set.of();
    }

    @Override
    public boolean rowsByDatabase() {
        return false;
    }

    @Override
    public EditionTable table(@Nullable DatabaseType database) {
        return this;
    }
}
