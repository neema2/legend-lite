// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.exec.PlanRunner;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;

import java.util.function.Function;

/**
 * THE RUN'S READER while a moved body's sink judges (the walk deletion's step 3, 2026-11-06): the program's own
 * {@link PlanRunner.SideReader}, published for the judgment the sink runs, so a compatibility appeal reads the rows
 * planned with its assert INSIDE the body's run -- a side the runner reads on the program's sessions -- and never
 * starts a run of its own (THE WALL's rule 4; the named residue {@code engine-text-appeal} existed for those runs).
 * Set by the sink around one judgment, cleared after it; outside a judgment there is none.
 *
 * <p>With it, the run's SESSIONS by target (I3, 2026-11-07): planning names a target and never holds a session, so a
 * judgment that needs the body's session itself -- the compatibility referee replays the golden beside the body's
 * database -- asks the run for the session its target opened ({@link #session}).
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

    /** The session the run opened for {@code target}: a judgment inside the body's run alone asks. */
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
