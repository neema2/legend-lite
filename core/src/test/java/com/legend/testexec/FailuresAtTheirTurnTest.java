// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.error.AssertFailed;
import com.legend.error.CompileKind;
import com.legend.error.DataError;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.error.TypeInferenceException;
import com.legend.sql.DialectCapability;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.Failures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A TEST BODY'S PLANNING FAILURE, BY WHERE UPSTREAM RAISES IT (the user's ruling of 2026-11-21, "D"; the one error
 * model's §4): an error legend-pure raises when the expression runs is planned and raised at its turn; any other --
 * a compile error upstream raises compiling, a refusal, a defect -- refuses the body at planning, the error itself
 * thrown, as a query is refused.
 */
class FailuresAtTheirTurnTest {

    @Test
    void anErrorUpstreamRaisesAtRunIsPlannedAtItsTurn() {
        Failure database = Failures.planned(new DataError("division by zero"), "the test");
        assertEquals(Failure.Kind.DATA, database.kind());
        Failure decided = Failures.planned(new AssertFailed("expected 1, got 2"), "the test");
        assertEquals(Failure.Kind.ASSERT_FAILED, decided.kind());
        // the window frame: legend-engine's rows()/_range() assert, raised when the frame is built (rows.pure)
        Failure frame = Failures.planned(new ModelException(LegendCompileException.Phase.TYPE,
                CompileKind.WINDOW_FRAME_INVERTED, List.of()), "the test");
        assertEquals(Failure.Kind.MODEL, frame.kind());
        assertNotNull(frame.record(), "a code at run rides the plan as its record");
        // a row's cell offset: legend-pure's at() native
        Failure cell = Failures.planned(new TypeInferenceException(CompileKind.ELEMENT_AT_OFFSET, List.of("3", "2")),
                "the test");
        assertEquals(Failure.Kind.TYPE_INFERENCE, cell.kind());
        // framed (a function's body, inlined): the record's own code decides, its frames lite's context
        TypeInferenceException framed = new TypeInferenceException(new com.legend.error.ErrorRecord.Frame(
                com.legend.error.FrameKind.IN_FUNCTION, List.of("a::f")),
                new TypeInferenceException(CompileKind.ELEMENT_AT_OFFSET, List.of("3", "2")));
        assertNotNull(Failures.planned(framed, "the test").record(), "a framed code at run rides the plan");
    }

    @Test
    void theDatabaseJudgesDeclineIsLitesRefusal() {
        // an assert's shape the database judge does not judge (UNJUDGED): legend-pure judges it, so it is lite's own
        // refusal, refused at planning (the BE audit's finding 3, 2026-11-21)
        RuntimeException declined = AssertFailed.unjudged("a json golden that is not a literal",
                "assertJsonStringsEqual: UNJUDGED in database mode");
        assertSame(declined, assertThrows(AssertFailed.class, () -> Failures.planned(declined, "the test")));
    }

    @Test
    void anyOtherErrorRefusesTheBodyAtPlanning() {
        RuntimeException compile = new ModelException(LegendCompileException.Phase.TYPE, CompileKind.MISSING_STORE,
                List.of("a::NoDb"));
        assertSame(compile, assertThrows(ModelException.class, () -> Failures.planned(compile, "the test")),
                "a compile error upstream raises compiling: the error itself, thrown");
        RuntimeException unkinded = new ModelException(LegendCompileException.Phase.LOWER, "no kind yet");
        assertSame(unkinded, assertThrows(ModelException.class, () -> Failures.planned(unkinded, "the test")));
        RuntimeException refusal = new NotImplementedException("class-result mapper ignores its parameter");
        assertSame(refusal, assertThrows(NotImplementedException.class, () -> Failures.planned(refusal, "the test")),
                "a refusal: legend-pure would run it, lite cannot plan it -- refused, reached or not");
        RuntimeException capability = new DialectCapability("LIST_FILTER reached a dialect without a list encoding");
        assertSame(capability, assertThrows(DialectCapability.class, () -> Failures.planned(capability, "the test")));
        RuntimeException defect = new IllegalStateException("a defect");
        assertSame(defect, assertThrows(IllegalStateException.class, () -> Failures.planned(defect, "the test")),
                "a defect fails loud");
    }

    @Test
    void theKindsUpstreamRaisesAtRunAreTheCitedOnes() {
        List<CompileKind> atRun = java.util.Arrays.stream(CompileKind.values()).filter(CompileKind::atRun).toList();
        assertEquals(List.of(CompileKind.ELEMENT_AT_OFFSET, CompileKind.WINDOW_FRAME_INVERTED,
                CompileKind.TYPE_NOT_SUPPORTED_YET,
                CompileKind.STRICT_DATE_BUCKET_UNIT), atRun,
                "each read from the pinned sources and cited on its kind: a kind added at run is named here");
    }
}
