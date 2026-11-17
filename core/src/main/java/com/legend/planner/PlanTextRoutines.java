// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.NativeDispatch;
import com.legend.compiler.spec.SpecCompiler;

import java.util.Map;

/**
 * THE PLAN-TEXT NATIVES' ROUTINE TABLE (moved off {@link UpstreamPlans} 2026-11-17, the terminal-hook criteria: a hook
 * declares signatures only): each routine asks the plan style's {@link UpstreamPlans} facet when it is called, so a
 * body that names none plans on any style, and one that does is refused by name by the product's at that call.
 */
public final class PlanTextRoutines {

    private PlanTextRoutines() {
    }

    /** The routine table of the plan-text natives (the catalog's JAVA_ROUTINE rows), each asking the style's facet
     *  when it is CALLED: a body that names none plans on any style, and one that does is refused by name by the
     *  product's at that call, as before. */
    public static Map<String, NativeDispatch.Routine> of(SpecCompiler specs, PlanEnv env) {
        NativeDispatch.Routine text = (call, letPrefix) -> java.util.Objects.requireNonNull(
                env.style().facet(UpstreamPlans.class).planToString(call, letPrefix, specs, env), "plan text");
        NativeDispatch.Routine sqlText = (call, letPrefix) -> java.util.Objects.requireNonNull(
                env.style().facet(UpstreamPlans.class).toSqlString(call, letPrefix, specs, env), "sql text");
        return Map.of(
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING.fqn(), text,
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING_WITHOUT_FORMATTING.fqn(),
                (call, letPrefix) -> text.value(call, letPrefix).replace("\n", "").replace(" ", ""),
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING.fqn(), sqlText,
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING_PRETTY.fqn(), sqlText,
                com.legend.builtin.NativeFn.JavaRoutine.TO_NON_EXECUTABLE_SQL_STRING.fqn(), sqlText);
    }
}
