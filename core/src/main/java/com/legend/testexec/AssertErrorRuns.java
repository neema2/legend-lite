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
            BodyRun run) {
        if (a.inner() != null) {
            // f IS an assert (2026-11-04): its failure is the error assertError matches, as upstream's assertError
            // catches an assert's failure as any error; the inner assert judged over its sides as any assert, by the
            // same rules -- a side that raises is that error too
            String message = null;
            try {
                // the inner assert's sides were read with the step's, its caller's message among them: no reader
                if (com.legend.testexec.HostJudgments.judge(a.inner().judgment(), sides, null, run) == null) {
                    throw new com.legend.error.NotImplementedException(a.inner().assertion().name()
                            + " inside assertError: the host judge declined the sides it read");
                }
            } catch (com.legend.error.AssertFailed | com.legend.error.DataError
                    | com.legend.error.LegendCompileException e) {
                // the errors upstream's assertError catches, per code as the branch below decides (the BE audit 2's
                // S3, 2026-11-21: by class until then, so a row's cell offset escaped the arm)
                if (!com.legend.testexecutionplan.Failures.atItsTurn(e)) {
                    throw e;
                }
                message = run.caughtMessage(e);
            }
            return judged(a.expected(), a.line(), a.column(), message, null, run);
        }
        if (a.caught() != null) {
            // the error f raised while it was planned, as the plan's data, rebuilt and worded at the verdict's turn by
            // the run's words as a caught error is, from its record (stage B's B1, 2026-11-20: until then the plan's
            // style worded it at planning); no position (planning sends no statement, so no raise carries one)
            RuntimeException planned = com.legend.testexecutionplan.Failures.raised(a.caught());
            return judged(a.expected(), a.line(), a.column(), run.caughtMessage(planned), null, run);
        }
        RuntimeException raised = null;
        try {
            sides.get();
        } catch (com.legend.error.DataError | com.legend.error.AssertFailed
                | com.legend.error.LegendCompileException e) {
            // the errors the arm takes, caught's: lite's error families, each taken where upstream raises it at run, as
            // upstream's assertError catches it (the BE audit's finding 6, 2026-11-21; until then a DataError or a
            // ModelException alone); any other is a wall and stays loud, raised as it was
            if (!com.legend.testexecutionplan.Failures.atItsTurn(e)) {
                throw e;
            }
            raised = e;
        }
        return judged(a.expected(), a.line(), a.column(), raised == null ? null : run.caughtMessage(raised),
                raised instanceof com.legend.error.DataError ? raised.getCause() : null, run);
    }


    private static TypedSide judged(String expected, @com.legend.base.Nullable Long expectedLine,
            @com.legend.base.Nullable Long expectedColumn, @com.legend.base.Nullable String caughtMessage,
            @com.legend.base.Nullable Throwable caughtCause, BodyRun run) {
        // B7 (RaisedErrors): messages arrive ALREADY clean -- the Executor funnel unwraps the transport envelope from
        // platform-raised text (provenance-sentinel scoped); a native error keeps its class and envelope, and a mismatch
        // against pure's expectation is then an HONEST failure, never a strip coincidence. The raise emission threads
        // the raising call's source span through the provenance envelope (PureSql.raise -> RaisedErrors.Positioned), as
        // interpreted pure's matcher receives the raising expression's source info (AssertError.java:68).
        com.legend.error.ErrorRecord failure = caughtCause instanceof com.legend.exec.RaisedErrors.Positioned p
                ? AssertRecords.errorFailure(expected, expectedLine, expectedColumn, caughtMessage, (long) p.line(),
                        (long) p.column())
                : AssertRecords.errorFailure(expected, expectedLine, expectedColumn, caughtMessage, null, null);
        if (failure != null) {
            throw new com.legend.error.AssertFailed(failure);
        }
        return new TypedSide.Scalar(Boolean.TRUE, "Boolean");
    }
}
