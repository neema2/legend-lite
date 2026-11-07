// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.exec.PlanRunner;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;

import java.util.function.Function;

/**
 * THE READER AND SESSIONS OF A JUDGMENT in progress: published by the moved body's sink around each judgment it runs,
 * and by {@link Appeals} around each held appeal it decides after the body's run (P1.2, 2026-11-07); cleared after it,
 * so outside a judgment there is none.
 *
 * <p>The READER is the one a judgment reads a planned side through: inside the body's run, the program's own
 * {@link PlanRunner.SideReader}; for a held appeal, the edge's, each side a plan of its own run on the body's sessions
 * after the body's run (the body's run sends its plan's statements and nothing else). The SESSIONS by target (I3,
 * 2026-11-07): planning names a target and never holds a session, so a judgment that needs a session itself -- the
 * compatibility referee replays the golden beside the body's database -- asks for the one its target opened
 * ({@link #session}).
 */
final class RunReader {

    private RunReader() {
    }

    private record Published(PlanRunner.@Nullable SideReader reader,
            @Nullable Function<ExecutionPlan.Target, Sessions.Session> sessions) {
    }

    private static final ThreadLocal<@Nullable Published> CURRENT = new ThreadLocal<>();

    /** A judgment's scope with {@code reader} and the run's {@code sessions} published (null: none). */
    static Scope judging(PlanRunner.@Nullable SideReader reader,
            @Nullable Function<ExecutionPlan.Target, Sessions.Session> sessions) {
        Published prior = CURRENT.get();
        CURRENT.set(new Published(reader, sessions));
        return () -> {
            if (prior == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(prior);
            }
        };
    }

    /** The session the body's run opened for {@code target}: a published judgment alone asks. */
    static Sessions.Session session(ExecutionPlan.Target target) {
        Published p = CURRENT.get();
        if (p == null || p.sessions() == null) {
            throw new IllegalStateException("a session is the run's: asked outside a body's run, for " + target);
        }
        return p.sessions().apply(target);
    }

    /** The reader of the judgment in progress, or null. */
    static PlanRunner.@Nullable SideReader current() {
        Published p = CURRENT.get();
        return p == null ? null : p.reader();
    }

    /** A judgment's scope, closed when the judgment ends. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
