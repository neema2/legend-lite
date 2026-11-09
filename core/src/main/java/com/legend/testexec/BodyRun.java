// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TypedSide;

/**
 * THE RUN RECORD of one body's run (the plan-as-data design's hole 6, 2026-11-09): what the run's judges share while
 * the plan runs -- the compatibility appeals they hold, the listener that keeps reports back while one is held, and
 * whether the assert being judged carries a caller's message. Made by {@link TestRunner#run} and handed to each judge
 * that needs it, explicitly: until hole 6 three thread-locals carried it ({@code Appeals.OPEN},
 * {@code CallerMessage.DEPTH}, {@code RunReader.CURRENT}), state no signature showed. The run's output, never planned
 * and never read back into a run.
 */
final class BodyRun {

    private final Appeals appeals;
    /** How many messaged asserts are being judged (nested: an assert inside a messaged one's helper). */
    private int messaged;

    BodyRun(Appeals appeals) {
        this.appeals = appeals;
    }

    Appeals appeals() {
        return appeals;
    }

    /** The listener the run's judges tell: the caller's, its reports kept back from the first held appeal on. */
    @Nullable AssertListener listener() {
        return appeals.listener();
    }

    /** A messaged assert is being judged until the scope closes. */
    Scope messaged() {
        messaged++;
        return () -> messaged--;
    }

    /** The appeal numbered {@code number} heard over the failed text of {@code name}: held for after the run, the
     *  assert holding until then ({@link Appeals#hold}). */
    TypedSide hold(String name, int number, String golden, String ours, boolean textEqual) {
        return appeals.hold(name, number, golden, ours, textEqual, messaged > 0);
    }

    /** A second hearing of an appeal another half holds ({@link Appeals#join}). */
    TypedSide join(String name, int number, String golden, String ours) {
        return appeals.join(name, number, golden, ours);
    }

    /** A step of the body that writes is about to run ({@link Appeals#writing}). */
    void writing() {
        appeals.writing();
    }

    /** A judgment's scope, closed when it ends. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
