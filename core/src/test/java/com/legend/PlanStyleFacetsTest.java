// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.legend.error.NotImplementedException;
import com.legend.testplan.PlanStyleFacets;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE FACET KINDS ARE CLOSED (the orchestrator's condition of 2026-11-16 on {@code PlanStyle.facet}): every style
 * answers exactly {@link PlanStyleFacets#KINDS} -- the edge each kind, the product none (each refused by name) -- and a
 * kind outside the list fails by name at the call, never null and never a default.
 */
class PlanStyleFacetsTest {

    /** Every style ({@code ArchitectureTest.theFacetTestsHoldEveryStyle} holds it to the imported implementers). */
    static final List<PlanStyle> STYLES = List.of(PlanStyle.PRODUCT, com.legend.edge.PureText.UPSTREAM,
            com.legend.edge.PureText.PURE_V1);

    @Test
    void theEdgeAnswersEveryDeclaredKind() {
        for (Class<?> kind : PlanStyleFacets.KINDS) {
            Object f = com.legend.edge.PureText.UPSTREAM.facet(kind);
            assertTrue(kind.isInstance(f), kind.getName());
        }
    }

    @Test
    void theProductRefusesEveryDeclaredKindByName() {
        for (Class<?> kind : PlanStyleFacets.KINDS) {
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> PlanStyle.PRODUCT.facet(kind));
            assertTrue(e.getMessage().contains(kind.getName()) && e.getMessage().contains("compatibility edge"),
                    e.getMessage());
        }
    }

    /** errorWords (2026-11-16, the PCT window-frame fix, the orchestrator's conditions): the product's style answers
     *  an error's OWN message, never the edge's words; the edge's answers upstream's sentence of a compile kind. */
    @Test
    void theProductsErrorWordsAreItsOwnTheEdgesUpstreams() {
        RuntimeException frame = new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                com.legend.error.CompileKind.WINDOW_FRAME_INVERTED, List.of());
        String product = PlanStyle.PRODUCT.errorWords(frame);
        assertTrue(product.equals(frame.getMessage()) && product.contains("window frame starts after it ends")
                && !product.contains("Invalid window frame boundary"), product);
        String edge = com.legend.edge.PureText.UPSTREAM.errorWords(frame);
        assertTrue(edge.contains("Invalid window frame boundary - lower bound of window frame cannot be greater"), edge);
    }

    @Test
    void aKindOutsideTheListFailsByNameOnEveryStyle() {
        for (PlanStyle style : STYLES) {
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> style.facet(Runnable.class));
            assertTrue(e.getMessage().contains(Runnable.class.getName()), e.getMessage());
        }
    }
}
