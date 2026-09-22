// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import java.util.List;

/**
 * THE ONE CALLEE-NAME HELPER (cleanup move 4, 2026-09-22): a call's callee FQN and
 * arguments read the same way for either call kind — a native call or a user call.
 */
public final class Calls {

    private Calls() {
    }

    /** The callee FQN of either call kind; null for a non-call. */
    public static @com.legend.base.Nullable String calleeOf(TypedSpec n) {
        return switch (n) {
            case TypedNativeCall c -> c.callee().qualifiedName();
            case TypedUserCall u -> u.callee().qualifiedName();
            default -> null;
        };
    }

    /** The arguments of either call kind; empty for a non-call. */
    public static List<TypedSpec> argsOf(TypedSpec n) {
        return switch (n) {
            case TypedNativeCall c -> c.args();
            case TypedUserCall u -> u.args();
            default -> List.of();
        };
    }
}
