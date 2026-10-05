// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

/**
 * An operation class mapping's operation ({@code cls: Operation { fqn(p1, p2) }}): the wire's
 * {@code operation} discriminator and the router function the grammar spells it with (upstream's
 * {@code OperationClassMapping.opsToFunc}). Spelled once, here, for the parser that reads the call
 * and the model printer that writes it back.
 */
public enum MappingOperation {
    ROUTER_UNION("meta::pure::router::operations::special_union_OperationSetImplementation_1__SetImplementation_MANY_"),
    STORE_UNION("meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_"),
    INHERITANCE("meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_"),
    MERGE("meta::pure::router::operations::merge_OperationSetImplementation_1__SetImplementation_MANY_");

    /** The router function's full path, as the grammar spells the call. */
    public final String function;

    MappingOperation(String function) {
        this.function = function;
    }

    /** The operation a router function's EXACT full path names, or null for any other function. */
    public static @com.legend.base.Nullable MappingOperation byFunction(String fqn) {
        for (MappingOperation op : values()) {
            if (op.function.equals(fqn)) {
                return op;
            }
        }
        return null;
    }
}
