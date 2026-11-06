// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.exec.PlanRunner;

/**
 * THE RUN'S READER while a moved body's sink judges (the walk deletion's step 3, 2026-11-06): the program's own
 * {@link PlanRunner.SideReader}, published for the judgment the sink runs, so a compatibility appeal reads the rows
 * planned with its assert INSIDE the body's run -- a side the runner reads on the program's sessions -- and never
 * starts a run of its own (THE WALL's rule 4; the named residue {@code engine-text-appeal} existed for those runs).
 * Set by the sink around one judgment, cleared after it; outside a judgment (the walk) there is none.
 */
final class RunReader {

    private RunReader() {
    }

    private static final ThreadLocal<PlanRunner.@Nullable SideReader> CURRENT = new ThreadLocal<>();

    /** A judgment's scope with {@code reader} published (null: none, as outside a run). */
    static Scope judging(PlanRunner.@Nullable SideReader reader) {
        PlanRunner.SideReader prior = CURRENT.get();
        CURRENT.set(reader);
        return () -> {
            if (prior == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(prior);
            }
        };
    }

    /** The reader of the judgment in progress, or null. */
    static PlanRunner.@Nullable SideReader current() {
        return CURRENT.get();
    }

    /** A judgment's scope, closed when the judgment ends. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
