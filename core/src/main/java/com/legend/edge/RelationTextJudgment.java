// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

/**
 * SPIKE (H11): A RELATION-TEXT ASSERT'S COMPARISON, THE EDGE'S RECORD IN THE PLAN, held by its verdict's
 * {@code EdgeJudged} judgment as {@link EngineTextJudgment} is: {@code assertEquals} over two sides of which
 * {@code printed} (bit 0 the first side, bit 1 the second) are a RELATION'S ROWS the edge presents as legend-pure's
 * text of {@code form} ({@code TDS}: a relation's toString) before it compares; a side not printed is a String the
 * plan reads. Data alone: an instruction to the presenter, never a text in the plan.
 */
public record RelationTextJudgment(String name, boolean wantEqual, int printed, String form) implements Extension {
}
