// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

/**
 * SPIKE (H11): A RELATION-TEXT ASSERT'S COMPARISON, THE EDGE'S RECORD IN THE PLAN, held by its verdict's
 * {@code EdgeJudged} judgment as {@link EngineTextJudgment} is: {@code assertEquals} (or {@code assertNotEquals}) over
 * two sides of which {@code printed} (bit 0 the first side, bit 1 the second) are a RELATION'S ROWS the edge presents as
 * legend-pure's or legend-engine's text of the side's form ({@code eForm}, {@code aForm}: {@code TDS},
 * {@code CSV}, {@code CSV+null}, {@code CSVJOIN:<sep>}, {@code CSVJOIN+null:<sep>}, {@code TDSCSV}; empty for a side
 * not printed, a String the plan reads) before it compares. {@code mainForm}: the render form main's host judge names
 * the pair by (null: main's generic equality); {@code renderedIsActual} which side main reads as rendered;
 * {@code sorted} the order fact main's rendered judgment carries. Data alone: an instruction to the presenter, never a
 * text in the plan.
 */
public record RelationTextJudgment(String name, boolean wantEqual, int printed, String eForm, String aForm,
        @com.legend.base.Nullable String mainForm, boolean renderedIsActual, boolean sorted) implements Extension {
}
