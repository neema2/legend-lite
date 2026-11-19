// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testplan.EngineTestText;

import java.util.List;

/** A test-planning facet PLANTED outside the edge, for one test alone (W9's judge half, 2026-11-16):
 *  {@code ArchitectureTest.theHookRuleBitesAPlantedFacet} shows the edge-only rule fails on it. Narrowed with the hook
 *  (A4 of the compatibility program, stage A ratified by the user 2026-11-19: the database judge's five asks gone). */
final class PlantedEngineTestText implements EngineTestText {

    @Override
    public com.legend.testplan.@com.legend.base.Nullable TextPlanned plan(com.legend.testplan.TextAsk ask) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public boolean isRoot(String fqn) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public boolean reachesUpstreamText(String name, boolean wantEqual, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public boolean notAConstant(com.legend.testplan.HostSides.@com.legend.base.Nullable Check check) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String why(com.legend.testplan.HostSides.@com.legend.base.Nullable Check check) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public com.legend.database.PlanStyle recording(com.legend.database.PlanStyle template) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public java.util.Map<String, java.util.Map<Integer, String>> siteWords(
            com.legend.database.PlanStyle recording) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String sizeWords(String multiplicity) {
        throw new UnsupportedOperationException("planted");
    }
}
