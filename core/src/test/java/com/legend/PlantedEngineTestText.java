// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testplan.AssertVerdicts;
import com.legend.testplan.BodyDriver;
import com.legend.testplan.EngineTestText;

import java.util.List;

/** A test-planning facet PLANTED outside the edge, for one test alone (W9's judge half, 2026-11-16):
 *  {@code ArchitectureTest.theHookRuleBitesAPlantedFacet} shows the edge-only rule fails on it. */
final class PlantedEngineTestText implements EngineTestText {

    @Override
    public com.legend.testplan.@com.legend.base.Nullable TextPlanned plan(com.legend.testplan.TextAsk ask) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArm(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmSameSql(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmDualGolden(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public com.legend.testplan.BatchTexts batchTexts() {
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
    public com.legend.testplan.DatabaseGate gate(com.legend.testexecutionplan.Extension judgment,
            java.util.function.Supplier<String> shape) {
        throw new UnsupportedOperationException("planted");
    }
}
