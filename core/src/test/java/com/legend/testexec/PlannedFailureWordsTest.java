// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.edge.PureMessages;
import com.legend.error.CompileKind;
import com.legend.error.ErrorRecord;
import com.legend.error.FrameKind;
import com.legend.error.TypeInferenceException;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.Failures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A FAILURE'S WORDS FROM ITS RECORD, PLANNED OR LIVE (stage B's B1, 2026-11-20; the one error model, BE, 2026-11-21):
 * lite's message is its record's sentence; the record rides the test plan with no message of its own, and the run
 * rebuilds the error from it at its turn, so every audience's words of the planned failure are its words of the live
 * one -- a compile error inside its frames, a load's file past its limit, and the run's refusal of no rows.
 */
class PlannedFailureWordsTest {

    private static TypeInferenceException framed() {
        TypeInferenceException kinded = new TypeInferenceException(CompileKind.MISSING_STORE, List.of("a::NoDb"));
        TypeInferenceException body = new TypeInferenceException(
                new ErrorRecord.Frame(FrameKind.IN_FUNCTION, List.of("a::f")), kinded);
        return new TypeInferenceException(new ErrorRecord.Frame(FrameKind.INLINED_VIA, List.of("a::g", "a::f")), body);
    }

    @Test
    void aCompileErrorInsideItsFramesIsWordedFromItsRecord_plannedAsLive() {
        TypeInferenceException live = framed();
        // one catalog (stage B's BE, 2026-11-21): the kind's sentence is legend-engine's on the product and at the edge
        assertEquals("in function 'a::f': The store 'a::NoDb' can't be found. [inlined via a::g -> a::f]",
                live.getMessage());
        Failure planned = Failures.of(live, "the test");
        assertNotNull(planned.record());
        assertNull(planned.ownMessage(), "the record, and no message, rides the plan");
        assertEquals(live.getMessage(), planned.message(), "its message is its record's sentence");
        String upstream = "in function 'a::f': The store 'a::NoDb' can't be found. [inlined via a::g -> a::f]";
        assertEquals(upstream, PureMessages.compile(live));
        RuntimeException rebuilt = Failures.raised(planned);
        assertEquals(live.getMessage(), rebuilt.getMessage(), "the message, as planned");
        assertEquals(upstream, PureMessages.compile(rebuilt), "the edge's words of the planned failure are the live one's");
    }

    @Test
    void anErrorWithNoRecordKeepsItsOwnWords_neverLookedThrough() {
        TypeInferenceException kinded = new TypeInferenceException(CompileKind.MISSING_STORE, List.of("a::NoDb"));
        TypeInferenceException unstated = new TypeInferenceException("somewhere: " + kinded.getMessage(), kinded);
        assertNull(Failures.of(unstated, "the test").record(), "a wrapper that states only its text has no record");
        assertEquals(unstated.getMessage(), PureMessages.compile(unstated));
    }

    @Test
    void aFileTooLargeIsWordedFromItsRecord_plannedAsLive() {
        RuntimeException live = com.legend.error.DataError.fileTooLarge("/big.csv", 1048577, 1048577, 1,
                com.legend.base.DataSize.refusal("/big.csv", 1048577, 1048577, 1));
        String upstream = "Unable to read the CSV file '/big.csv' Execution error at ??, \"File is too large, file was"
                + " 1.00 Mb, limit is 1 Mb\"";
        // the one catalog (stage B's BE, 2026-11-21): legend-pure's sentence on the product too
        assertEquals(upstream, live.getMessage());
        Failure planned = Failures.of(live, "the test");
        assertNotNull(planned.record());
        assertNull(planned.ownMessage());
        assertEquals(upstream, PureMessages.compile(live));
        RuntimeException rebuilt = Failures.raised(planned);
        assertEquals(live.getMessage(), rebuilt.getMessage());
        assertEquals(upstream, PureMessages.compile(rebuilt));
    }

    @Test
    void theRunsRefusalOfNoRowsIsWordedFromItsSite() {
        SizeRefusal refused = new SizeRefusal("[1]");
        assertEquals("Cannot cast a collection of size 0 to multiplicity [1]", refused.getMessage());
        assertEquals("Cannot cast a collection of size 0 to multiplicity [1]", PureMessages.compile(refused));
    }
}
