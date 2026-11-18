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

    /** The routing activity an execute's {@code chain} records before its statement's, or null where it records none
     *  (J37 of the edge audit, 2026-11-18: the whole row the style's, its place the planner's): the row's columns after
     *  its id, scope and index -- its kind, sql, comment and rewritten query, the system metamodel's activity table's. */
    @com.legend.base.Nullable
    java.util.List<String> routingActivity(TypedSpec chain, ModelContext ctx, SpecCompiler specs);
}
