// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import java.util.List;

/**
 * THE CLOSED LIST of the plan style's test-planning facets (the orchestrator's condition of 2026-11-16 on
 * {@code PlanStyle.facet}: a typed accessor, never an open service locator). Every style answers exactly these kinds --
 * the product refusing each by name, the edge answering each -- and refuses any other kind by name
 * ({@code PlanStyleFacetsTest} holds every implementer to this list). A new kind is added here, with its reason.
 */
public final class PlanStyleFacets {

    private PlanStyleFacets() {
    }

    /** The facet kinds, in the order they were added: the engine-text assert's arm (W9's judge half, 2026-11-16);
     *  upstream's plan text at planning (W9's planner half, 2026-11-16: {@code planner.UpstreamPlans}); a relation's
     *  text at lowering (the separation census's B17, 2026-11-17: {@code lowering.RelationTexts}, asked with its
     *  call's refusal). */
    public static final List<Class<?>> KINDS = List.of(EngineTestText.class, com.legend.planner.UpstreamPlans.class,
            com.legend.lowering.RelationTexts.class);
}
