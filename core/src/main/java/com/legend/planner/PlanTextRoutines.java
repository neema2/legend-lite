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
     *  for its text when it is CALLED: a body that names none plans on any style, and one that does is refused by name
     *  by the product's at that call. */
    public static Map<String, NativeDispatch.Routine> of(SpecCompiler specs, PlanEnv env) {
        Map<String, NativeDispatch.Routine> table = new java.util.LinkedHashMap<>();
        for (com.legend.builtin.NativeFn.JavaRoutine routine : com.legend.builtin.NativeFn.JavaRoutine.values()) {
            table.put(routine.fqn(), (call, letPrefix) -> env.style().facet(UpstreamPlans.class)
                    .routineText(routine, call, letPrefix, specs, env));
        }
        return java.util.Collections.unmodifiableMap(table);
    }
}
