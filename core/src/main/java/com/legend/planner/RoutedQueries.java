// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * AN EXECUTE'S ROUTING ACTIVITY TEXT, A TWO-FORM HOOK OF THE PLAN STYLE (J5 of the edge audit, 2026-11-17,
 * {@code testplan.PlanStyleFacets.TWO_FORMS}): the text an execute's activity rows record for a query routed through an
 * aggregation-aware mapping. The product's form records no routing activity -- lite's activities are the statements it
 * runs ({@link PlanAllocations#NO_ROUTING_ACTIVITY}); the compatibility edge's is legend-engine's
 * {@code AggregationAwareActivity}, the routed query's print.
 */
public interface RoutedQueries {

    /** The routed query's text of an execute's {@code chain}, or null where the chain records no routing activity. */
    @com.legend.base.Nullable
    String rewrittenQuery(TypedSpec chain, ModelContext ctx, SpecCompiler specs);
}
