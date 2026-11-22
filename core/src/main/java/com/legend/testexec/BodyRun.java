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

    /** The run's hearing of the compatibility appeals (the edge's, or the product's refusal: {@link EdgeRun}). */
    private final EdgeHearing hearing;
    /** How many messaged asserts are being judged (nested: an assert inside a messaged one's helper). */
    private int messaged;
    /** The program's reader while it runs (an unrolled element's caller's message read through it where that
     *  element fails: item 3's S4 step 3). */
    private @Nullable SideReader reader;

    BodyRun(EdgeHearing hearing) {
        this.hearing = hearing;
    }

    EdgeHearing hearing() {
        return hearing;
    }

    /** THE MESSAGE an assertError matches a caught error by (K10b of the edge audit, the orchestrator's ruling (A) of
     *  2026-11-17): under lite's own rules its own message; under the compatibility rules (the run hook's words), a raise of a
     *  planned site is legend-pure's words of that site, worded from its record after the run (stage B's B1,
     *  2026-11-20); any other error its own message. */
    String caughtMessage(RuntimeException e) {
        // whose words: the run hook's (census J29, 2026-11-18), never a branch of the judge's
        return hearing.hostForms().caught(e, hearing);
    }

    /** The listener the run's judges tell: the caller's, its reports kept back while an appeal is held. */
    @Nullable AssertListener listener() {
        return hearing.listener();
    }

    /** The program's reader, while it runs (PlanRunner.ProgramSink#reader). */
    void reader(@Nullable SideReader r) {
        this.reader = r;
    }

    /** An unrolled element's caller's message (item 3's S4 step 3, the user's decision 4): {@code side} read through
     *  the program's reader and shaped only when the judge asks, its element having failed; null where it has none. */
    java.util.function.@Nullable Supplier<TypedSide> message(
            com.legend.testexecutionplan.TestPlan.@Nullable Side side,
            com.legend.testexecutionplan.@Nullable SideShape shape) {
        return side == null || shape == null ? null : () -> SideShapes.shape(shape, read(side));
    }

    /** {@code side} read now, through the program's reader: a part's caller's message, its element having failed. */
    com.legend.exec.PlanRunner.Rows read(com.legend.testexecutionplan.TestPlan.Side side) {
        SideReader r = java.util.Objects.requireNonNull(reader, "a side read on demand with no program running");
        try {
            return r.read(side);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A messaged assert is being judged until the scope closes. */
    Scope messaged() {
        messaged++;
        return () -> messaged--;
    }

    /** An edge-judged comparison at its verdict step ({@link EdgeHearing#judged}). */
    TypedSide judged(com.legend.testexecutionplan.HostJudgment.EdgeJudged e, java.util.List<TypedSide> sides) {
        return hearing.judged(e, sides, messaged > 0);
    }

    /** A step of the body that writes is about to run ({@link EdgeHearing#writing}). */
    void writing() {
        hearing.writing();
    }

    /** A judgment's scope, closed when it ends. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
