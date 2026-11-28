// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.Extension;

import java.util.List;
import java.util.Map;

/**
 * A seed step's REFEREE'S COPY, planned by the compatibility edge at the step (B2b, 2026-11-11,
 * docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6; docs/PLAN_AS_DATA_2026_11_07.md 2.7): the step's load as the replay
 * referee's own database runs it -- one step for each of the plan's load's steps, in order, written in the referee's
 * dialect: a statement, the rows as a content blob of the referee's inserts (read from {@code content} by digest), a
 * file by the plan's own reference, read by the referee's own reader -- data as a load, never as text the edge wrote
 * down (until B2b {@code SeedTexts}, census M3, and the file's cells split by Java, M15). The run tells the edge which
 * of the plan's steps ran ({@link CompatEdge#seeded}), and the edge records the same steps of this copy.
 */
public record RefereeLoad(List<ExecutionPlan.LoadStep> steps, Map<String, String> content) implements Extension {

    public RefereeLoad {
        steps = List.copyOf(steps);
        // in its own order, never a salted one: the codec writes it sorted, and a copy's iteration may reach a text
        content = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(content));
        for (ExecutionPlan.LoadStep step : steps) {
            if (step instanceof ExecutionPlan.LoadStep.Statements s && !content.containsKey(s.ref().digest())) {
                throw new IllegalArgumentException("the referee's copy of a load reads a blob its bundle lacks: "
                        + s.ref());
            }
        }
    }
}
