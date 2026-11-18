// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.List;
import java.util.Optional;

/**
 * THE RUNTIME ARGUMENT'S BOUND CONTEXT of legend-engine's test natives ({@code executionPlan}, {@code execute},
 * {@code toSQLString}): the argument brought to its value and read once (moved from planner.BodyPlanner, J40's
 * only-edge-callers census, 2026-11-18: its callers were EnginePlans and the SQL-text arm alone).
 */
public final class RuntimeContexts {

    private RuntimeContexts() {
    }

    /** The execution context an executionPlan / execute / toSQLString call
     * binds through its RUNTIME argument: the argument is brought to its
     * VALUE (helper calls inlined) and read ONCE. */
    public static ExecutionContext boundContext(
            TypedSpec runtimeArg, SpecCompiler specs) {
        com.legend.base.RunScope.planning("bind a runtime's context");
        TypedSpec value = new UserCallInliner(specs)
                .inlineBody(List.of(runtimeArg)).get(0);
        return ExecutionContext.reader()
                .read(Optional.empty(), value);
    }

    /** The same read under a statement: the argument and the values it
     * names (a let-bound runtime, a let-bound hook operand) chase the
     * statement's preceding lets. */
    public static ExecutionContext boundContext(
            TypedSpec runtimeArg, List<TypedSpec> letPrefix, SpecCompiler specs) {
        com.legend.base.RunScope.planning("bind a runtime's context");
        TypedSpec value = new UserCallInliner(specs)
                .inlineBody(List.of(Lets.bound(runtimeArg, letPrefix))).get(0);
        return ExecutionContext.reader()
                .bind(v -> Lets.bound(v, letPrefix))
                .read(Optional.empty(), value);
    }
}
