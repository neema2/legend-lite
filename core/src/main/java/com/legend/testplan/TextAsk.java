// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/** One assert the test planner hands the arm: opaque to everyone but the test planner. */
public final class TextAsk {
    final TypedSpec bare;
    final List<TypedSpec> letPrefix;
    final SpecCompiler specs;
    final BodyDriver.ExecEnv env;
    final AssertVerdicts.@com.legend.base.Nullable SpliceHook hook;

    TextAsk(TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        this.bare = bare;
        this.letPrefix = letPrefix;
        this.specs = specs;
        this.env = env;
        this.hook = hook;
    }
}
