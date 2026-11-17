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

    BodyRun(EdgeHearing hearing) {
        this.hearing = hearing;
    }

    EdgeHearing hearing() {
        return hearing;
    }

    /** THE MESSAGE an assertError matches a caught error by (K10b of the edge audit, the orchestrator's ruling (A) of
     *  2026-11-17): under lite's own rules its own message; under the compatibility rules ({@code rules}), a raise of a
     *  planned site is legend-pure's words of that site, looked up in the plan's records by the raising statement's text
     *  and the site's number ({@link EdgeHearing#raisedWords}); any other error its own message. */
    String caughtMessage(RuntimeException e, com.legend.testexecutionplan.TestPlan.JudgingRules rules) {
        if (!HostJudgments.lite(rules) && e instanceof com.legend.error.DataError
                && e.getCause() instanceof com.legend.exec.RaisedErrors.Raised r && r.site() != null) {
            return hearing.raisedWords(java.util.Objects.requireNonNull(r.statement(),
                    "a raise of a planned site carries its statement's text"), r.site().n());
        }
        return String.valueOf(e.getMessage());
    }

    /** The listener the run's judges tell: the caller's, its reports kept back while an appeal is held. */
    @Nullable AssertListener listener() {
        return hearing.listener();
    }

    /** A messaged assert is being judged until the scope closes. */
    Scope messaged() {
        messaged++;
        return () -> messaged--;
    }

    /** Whether a messaged assert is being judged. */
    boolean judgingMessaged() {
        return messaged > 0;
    }

    /** An edge-judged comparison at its verdict step ({@link EdgeHearing#judged}). */
    TypedSide judged(com.legend.testexecutionplan.HostJudgment.EdgeJudged e, java.util.List<TypedSide> sides,
            com.legend.testexecutionplan.TestPlan.JudgingRules rules) {
        return hearing.judged(e, sides, rules, messaged > 0);
    }

    /** legend-pure's words of an assert's failure ({@link EdgeHearing#pureWords}): asked under the compatibility rules
     *  alone. */
    PureWords pureWords() {
        return hearing.pureWords();
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
