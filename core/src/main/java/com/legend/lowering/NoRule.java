// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.DecisionProbe;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.error.NotImplementedException;
import com.legend.platform.Implementation;
import com.legend.platform.ImplementationTable;

/**
 * WHY THE SCALAR FUNNEL HAS NO RULE for a resolved call: the implementation
 * table's row says (untangle step 4a) — a refused native names its wall; an
 * upstream native the platform does not implement is named as such, never
 * "unknown function"; a family-implemented native reaching the funnel is the
 * wrong site; anything else is the registration bug.
 */
final class NoRule {

    private NoRule() {
    }

    static RuntimeException explain(TypedNativeCall call, ImplementationTable implementations) {
        String fqn = call.callee().qualifiedName();
        Implementation row = call.callee().definition() == null ? null
                : implementations.rowOf(call.callee().definition());
        return switch (row) {
            case Implementation.Refused refused -> {
                DecisionProbe.pick(call.callee().definition(), "WALLED-NATIVE");
                yield new NotImplementedException("walled native '" + fqn + "': " + refused.why());
            }
            case Implementation.Unimplemented unimplemented -> {
                DecisionProbe.pick(call.callee().definition(), "UNIMPLEMENTED");
                yield new NotImplementedException("upstream native '" + fqn
                        + "' is declared by the spec and not implemented by the platform");
            }
            case Implementation.Intrinsic in when in.positions().isEmpty() -> {
                DecisionProbe.pick(call.callee().definition(), "FAMILY");
                yield new IllegalStateException("no scalar lowering registered for resolved overload '"
                        + fqn + "': implemented by "
                        + in.families().stream().map(Class::getSimpleName).toList() + ", not as a scalar");
            }
            case null, default -> {
                DecisionProbe.pick(call.callee().definition(), "UNREGISTERED");
                yield new IllegalStateException("no scalar lowering registered for resolved overload '"
                        + fqn + "' with " + call.callee().parameters().size() + " parameter(s)");
            }
        };
    }
}
