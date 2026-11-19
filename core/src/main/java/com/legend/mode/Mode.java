// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.mode;

import com.legend.database.PlanStyle;
import com.legend.parser.GrammarLevel;

/**
 * A MODE, AS DATA (the user's ruling of 2026-11-18 ~19:45, "Yes reframe as 3 modes"; S1m of the presentation program,
 * docs/records/presentation-program-design-2026-11-18.md §3.1, on claude/presentation-design). One compiler, one
 * planner and one runner serve three modes; a mode decides only which grammar a door parses, which computations differ
 * from lite's own, and (from S1) which writers present the answer. A door chooses one mode, and every difference
 * between modes is one of this record's components, read where it applies: no code branches on a mode outside the
 * records (the design's G6, {@code ModeBranchCensusTest}, report-only until S7).
 *
 * <p>{@link #LITE} is the product's: lite's grammar and lite's own spellings. The compatibility edge holds the other
 * two ({@code com.legend.edge.Modes}: legend-engine's and legend-pure's), since the edge depends on the product and
 * never the reverse (G5).
 *
 * <p>Two components arrive later, each in its slice: {@code computation} is today's {@link PlanStyle} until S7 folds
 * the hook surface into the mode's computation records, and the presenter family joins in S1.
 *
 * @param name        the mode's name, as a plan will carry it from S1: {@code "lite"}, {@code "legend-engine"},
 *                    {@code "legend-pure"}
 * @param grammar     the grammar a door of this mode parses its text at
 * @param computation the dialects and the forms its plans are written in
 */
public record Mode(String name, GrammarLevel grammar, PlanStyle computation) {

    /** LITE, the product's mode: lite's grammar ({@link GrammarLevel#LEGEND_LITE}) and its own style. */
    public static final Mode LITE = new Mode("lite", GrammarLevel.LEGEND_LITE, PlanStyle.PRODUCT);
}
