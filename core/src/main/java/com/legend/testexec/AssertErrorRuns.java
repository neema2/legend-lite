// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;

final class AssertErrorRuns {

    private AssertErrorRuns() {
    }


    /** THE ARM'S VERDICT at its step (the walker-deletion program, 2026-11-03): the error {@code f} raised -- while it
     *  was planned ({@code a.caught()}), else reading {@code sides}, f's last statement run -- judged against the expected
     *  message, line and column with the pure {@code assertError/4} body's exact spellings, the walk's and the program
     *  road's one rule; a side that raises nothing fails ("No error was thrown"), an error no arm takes (a wall) is
     *  raised as it was. */
    static TypedSide judge(HostJudgment.AssertError a, java.util.function.Supplier<List<TypedSide>> sides,
            com.legend.testexecutionplan.TestPlan.JudgingRules rules, BodyRun run) {
        if (a.inner() != null) {
            // f IS an assert (2026-11-04): its failure is the error assertError matches, as upstream's assertError
            // catches an assert's failure as any error; the inner assert judged over its sides as any assert, by the
            // same rules -- a side that raises is that error too
            String message = null;
            try {
                // the inner assert's sides were read with the step's, its caller's message among them: no reader
                if (com.legend.testexec.HostJudgments.judge(a.inner().judgment(), sides, rules, null, run) == null) {
                    throw new com.legend.error.NotImplementedException(a.inner().assertion().name()
                            + " inside assertError: the host judge declined the sides it read");
                }
            } catch (com.legend.error.AssertFailed | com.legend.error.DataError | com.legend.error.ModelException e) {
                message = String.valueOf(e.getMessage());
            }
            return judged(a.expected(), a.line(), a.column(), message, null);
        }
        com.legend.testexecutionplan.Failure planned = a.caught();
        if (planned != null) {
            // the error f raised while it was planned, as the plan's data: its message; no position (planning sends
            // no statement, so no database's raise carries one)
            return judged(a.expected(), a.line(), a.column(), String.valueOf(planned.message()), null);
        }
        RuntimeException raised = null;
        try {
            sides.get();
        } catch (com.legend.error.DataError | com.legend.error.ModelException e) {
            // the errors the arm takes (caught's): any other is a wall and stays loud, raised as it was
            raised = e;
        }
        return judged(a.expected(), a.line(), a.column(), raised == null ? null : String.valueOf(raised.getMessage()),
                raised instanceof com.legend.error.DataError ? raised.getCause() : null);
    }

    private static TypedSide judged(String expected, @com.legend.base.Nullable Long expectedLine,
            @com.legend.base.Nullable Long expectedColumn, @com.legend.base.Nullable String caughtMessage,
            @com.legend.base.Nullable Throwable caughtCause) {
        if (caughtMessage == null) {
            // interpreted AssertError.java: PureAssertFail("No error was
            // thrown") — a FAIL, not an orchestration error
            throw new com.legend.error.AssertFailed("No error was thrown");
        }
        // B7 (RaisedErrors): messages arrive ALREADY clean — the
        // Executor funnel unwraps the transport envelope from
        // platform-raised text (provenance-sentinel scoped); a native
        // error keeps its class and envelope, and a mismatch against
        // pure's expectation is then an HONEST failure, never a strip
        // coincidence. The old broad prefix regex is deleted.
        String actual = caughtMessage;
        if (!actual.equals(expected)) {
            // assertError.pure:24 — the /4 body's assertEquals format,
            // verbatim
            throw new com.legend.error.AssertFailed(
                    "Execution error message mismatch.\nThe actual message"
                    + " was \"" + actual + "\"\nwhere the expected"
                    + " message was:\"" + expected + "\"");
        }
        // POSITION adjudication (leg 2, assertError.pure:25-26 — message
        // first, then line, then column, each only when expected): the
        // raise emission threads the raising call's source span through
        // the provenance envelope (PureSql.raise -> RaisedErrors
        // .Positioned). Interpreted pure's matcher receives the raising
        // expression's source info the same way (AssertError.java:68).
        // An expectation over an error WITHOUT a captured span is LOUD —
        // interpreted si.line->toOne() on empty raises too, and a quiet
        // pass here would launder a native error's missing provenance.
        if (expectedLine != null || expectedColumn != null) {
            if (!(caughtCause instanceof com.legend.exec.RaisedErrors.Positioned p)) {
                throw new com.legend.error.AssertFailed(
                        "assertError line/column: the caught error carries"
                        + " no source position (a native database error, or"
                        + " a raise emission without provenance)");
            }
            if (expectedLine != null && p.line() != expectedLine) {
                // assertError.pure:25 — the /4 body's format, verbatim
                throw new com.legend.error.AssertFailed(
                        "Execution error line mismatch. Actual: " + p.line()
                        + " where expected: " + expectedLine);
            }
            if (expectedColumn != null && p.column() != expectedColumn) {
                // assertError.pure:26 — the /4 body's format, verbatim
                throw new com.legend.error.AssertFailed(
                        "Execution error column mismatch. Actual: "
                        + p.column() + " where expected: " + expectedColumn);
            }
        }
        return new TypedSide.Scalar(Boolean.TRUE,
                "Boolean");
    }
}
