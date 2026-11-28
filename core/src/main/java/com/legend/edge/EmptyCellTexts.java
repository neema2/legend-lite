// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.lowering.NamedRule;
import com.legend.model.FunctionId;

import java.util.Set;

/**
 * AN EMPTY TDS CELL'S TEXT AT THE COMPATIBILITY DOORS: A ROW OF BOTH TABLES (the edition program's step 8,
 * 2026-11-28; the user's ruling of 2026-11-26: "tdsnull what are downsides if treat as table row? Seems like no
 * brainer ?"). legend-engine's TDS holds an empty cell as an instance of its class {@code TDSNull}, whose
 * {@code toString()} answers {@code TDSNull} (legend-engine 4.145.0, {@code core/pure/tds/tds.pure}:127-133), so a
 * cell's text, a list of cells joined and a row's cells joined each write that word for an empty cell where lite's
 * product writes nothing and its join skips the cell. The calls that read a cell's text are named here once, and
 * both doors' tables ({@link EngineTable}, {@link PureTable}) answer them with the one rule that writes the word
 * ({@link NamedRule#EMPTY_CELL_TEXT_AS_TDS_NULL}).
 *
 * <p>Like the rows of {@link UpstreamTies} it answers inside a store query and outside one, on every database, and
 * is never refused: the word is upstream's own Pure, the same whichever side of legend-engine runs the code, and a
 * relational test's own code joins {@code $row.values} outside any {@code execute()}. Until this step a pass of the
 * edge's dialects rewrote every edge plan's SQL to these forms after the lowering ({@code PurePrintForms}, deleted),
 * on every database and whatever the call's stamp: the rows reach what the pass reached, so no answer moves.
 *
 * <p>The calls: a TDS cell's own text ({@code cellText}, the typer's call for a cell's {@code toString});
 * {@code makeString}, whose join writes each element's text; and a {@code joinStrings} over a row's cells
 * ({@code $row.values}, the typer's row-cells list) -- any other {@code joinStrings} reads no cell's text and keeps
 * its own rule, so it is no row.
 */
final class EmptyCellTexts {

    private EmptyCellTexts() {
    }

    private static final Set<FunctionId> ALWAYS = Set.copyOf(FunctionId.all(Pure.AT_LEGEND_LITE_CELL_TEXT,
            Pure.AT_STRING_MAKE_STRING));

    private static final Set<FunctionId> OVER_A_ROWS_CELLS = Set.copyOf(FunctionId.all(Pure.AT_STRING_JOIN_STRINGS,
            Pure.AT_RELATION_JOIN_STRINGS));

    /** The rule that lowers {@code call} with an empty cell's text written, or null where the call reads no cell's
     *  text. */
    static @Nullable NamedRule rule(TypedNativeCall call) {
        FunctionId id = call.callee().id();
        if (ALWAYS.contains(id) || OVER_A_ROWS_CELLS.contains(id) && !call.args().isEmpty()
                && call.args().get(0) instanceof TypedCollection cells && cells.rowCells()) {
            return NamedRule.EMPTY_CELL_TEXT_AS_TDS_NULL;
        }
        return null;
    }
}
