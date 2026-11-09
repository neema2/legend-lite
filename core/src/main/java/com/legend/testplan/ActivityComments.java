// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE ACTIVITY COMMENT a body's planning reserves (slice 5, 2026-11-09): under a verdict batch a frame's activity row
 * names the trace comment of the body's fused statement, the statement that will run it -- minted once, at planning,
 * shared by every copy of the body's environment. Planning's own (until slice 5 it borrowed exec's execution trace).
 */
final class ActivityComments {

    private @com.legend.base.Nullable String reserved;

    /** The comment reserved for the body's fused statement, minted at the first ask. */
    String reserve() {
        String r = reserved;
        if (r == null) {
            r = com.legend.executionplan.ExecutionPlan.traceComment(java.util.UUID.randomUUID());
            reserved = r;
        }
        return r;
    }
}
