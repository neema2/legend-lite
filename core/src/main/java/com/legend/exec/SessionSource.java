// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

/**
 * Where a reading plan's session comes from, for the target its answer names (the server's project database; a caller's
 * own session, {@link #given}). The door's {@code Execution.Source} until slice 5 (2026-11-09): since hole 7 it takes
 * the plan's target and nothing of the model, so it is the runtime's contract, beside {@link PlanSessions.Source} (a
 * plan's sessions for each statement's target).
 */
@FunctionalInterface
public interface SessionSource {

    Sessions.Session open(ExecutionPlan.Target target);

    /** A source of {@code session} as the caller holds it: a non-closing view of it for every target (D68,
     *  2026-10-30: the test runners hold sessions, not connections). It checks nothing at open: the runner checks each
     *  statement's target against the session it is sent on ({@code JdbcStatements.check}). */
    static SessionSource given(Sessions.Session session) {
        return target -> Sessions.view(session);
    }
}
