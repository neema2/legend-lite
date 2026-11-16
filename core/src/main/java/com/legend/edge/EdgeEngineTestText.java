// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testplan.AssertVerdicts;
import com.legend.testplan.BodyDriver;
import com.legend.testplan.EngineTestText;

import java.util.List;

/**
 * The edge's engine-text assert arm (W9's judge half, 2026-11-16): the facet {@link EngineTestText} the edge's style
 * answers, over the arm itself ({@link SqlTextVerdicts}, moved here from the test planner in S2c).
 */
final class EdgeEngineTestText implements EngineTestText {

    static final EdgeEngineTestText INSTANCE = new EdgeEngineTestText();

    private EdgeEngineTestText() {
    }

    @Override
    public EngineTestText.@com.legend.base.Nullable Planned plan(EngineTestText.Ask ask) {
        return SqlTextVerdicts.plan(ask);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArm(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArm(name, wantEqual, args, letPrefix, specs, env, hook);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmSameSql(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArmSameSql(root, letPrefix, specs, env, hook);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmDualGolden(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArmDualGolden(root, letPrefix, specs, env, hook);
    }

    @Override
    public EngineTestText.BatchTexts batchTexts() {
        return new SqlTextVerdicts.BatchTexts();
    }
}
