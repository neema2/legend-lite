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
     *  call's refusal); a text only legend-pure writes at lowering (B8 (i), 2026-11-17: {@code lowering.PureOnlyTexts}); legend-engine's result
     *  envelope at lowering (Phase 6's B5, 2026-11-17: {@code lowering.ResultEnvelopes}, asked with its call's refusal). */
    public static final List<Class<?>> KINDS = List.of(EngineTestText.class, com.legend.planner.UpstreamPlans.class,
            com.legend.lowering.RelationTexts.class, com.legend.lowering.PureOnlyTexts.class,
            com.legend.lowering.ResultEnvelopes.class);

    /** The TWO-FORM kinds ({@code PlanStyle.twoForm}, the orchestrator's ruling of 2026-11-17, census B9): each a value
     *  the product answers in its own canonical form and the edge in upstream's. A closed list, each kind added with its
     *  family and its dated reason: a DateTime's text in a message (census B9, 2026-11-17: {@code lowering.DateTimeTexts},
     *  lite's DATETIME_TO_TEXT in the product, legend-pure's print form at the edge). */
    public static final List<Class<?>> TWO_FORMS = List.of(com.legend.lowering.DateTimeTexts.class);
}
