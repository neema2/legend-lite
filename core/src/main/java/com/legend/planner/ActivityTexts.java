// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.List;

/**
 * THE SQL AN EXECUTE'S ACTIVITY RECORD REPORTS, A TWO-FORM HOOK OF THE PLAN STYLE ({@code RelationalActivity.sql};
 * S310, RULED by the user 2026-11-14, DE2 (a); J40.PlanStyle, the orchestrator's ruling of 2026-11-18, as
 * {@link RoutedQueries}: until then the planner computed both texts and the style picked one,
 * {@code PlanStyle.Activity}). The product's form is lite's own SQL for the chain, what actually runs
 * ({@link PlanAllocations#LITE_ACTIVITY}); the compatibility edge's is legend-engine's text, its own upstream plan
 * facet's -- each form computes its own text alone.
 */
public interface ActivityTexts {

    /** The activity SQL of an execute's {@code chain} under {@code mappingFqn}; {@code recorded}: its runtime's
     *  post-processors applied. */
    String activitySql(List<TypedSpec> chain, @com.legend.base.Nullable String mappingFqn, SpecCompiler specs,
            PlanEnv env, boolean recorded);
}
