// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.edge.PureMessages;
import com.legend.error.CompileKind;
import com.legend.error.TypeInferenceException;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.Failures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A FAILURE'S WORDS FROM ITS RECORD, PLANNED OR LIVE (stage B's B1, 2026-11-20, errors as records): lite's message is
 * its own; the facts an audience words it from ride the failure the run rebuilds at its turn, so the edge's words of a
 * planned failure are the words of the live one -- a kinded compile error through lite's stated wrappers, a load's file
 * past its limit, and the run's refusal of no rows.
 */
class PlannedFailureWordsTest {

    private static TypeInferenceException wrapped() {
        TypeInferenceException kinded = new TypeInferenceException(CompileKind.MISSING_STORE, List.of("a::NoDb"));
        TypeInferenceException body = new TypeInferenceException("in function 'a::f': ", kinded, "");
        return new TypeInferenceException("", body, " [inlined via a::g -> a::f]");
    }

    @Test
    void aKindedErrorThroughStatedWrappersIsWordedFromItsRecord_plannedAsLive() {
        TypeInferenceException live = wrapped();
        assertEquals("in function 'a::f': store 'a::NoDb' is not defined [inlined via a::g -> a::f]", live.getMessage());
        Failure planned = Failures.of(live, "the test");
        Failure.Compile c = assertInstanceOf(Failure.Compile.class, planned.fact());
        assertEquals("in function 'a::f': ", c.prefix());
        assertEquals(" [inlined via a::g -> a::f]", c.suffix());
        String upstream = "in function 'a::f': The store 'a::NoDb' can't be found. [inlined via a::g -> a::f]";
        assertEquals(upstream, PureMessages.compile(live));
        RuntimeException rebuilt = Failures.raised(planned);
        assertEquals(live.getMessage(), rebuilt.getMessage(), "lite's message, as planned");
        assertEquals(upstream, PureMessages.compile(rebuilt), "the edge's words of the planned failure are the live one's");
    }

    @Test
    void aWrapperThatStatesNoTextKeepsLitesWords() {
        TypeInferenceException kinded = new TypeInferenceException(CompileKind.MISSING_STORE, List.of("a::NoDb"));
        TypeInferenceException unstated = new TypeInferenceException("somewhere: " + kinded.getMessage(), kinded);
        assertNull(Failures.of(unstated, "the test").fact(), "nothing is searched: an unstated wrapper has no record");
        assertEquals(unstated.getMessage(), PureMessages.compile(unstated));
    }

    @Test
    void aFileTooLargeIsWordedFromItsFacts_plannedAsLive() {
        RuntimeException live = new com.legend.error.DataError(com.legend.error.ErrorKind.FILE_TOO_LARGE, "/big.csv",
                com.legend.base.DataSize.refusal("/big.csv", 1048577, 1048577, 1));
        Failure planned = Failures.of(live, "the test");
        assertInstanceOf(Failure.FileTooLarge.class, planned.fact());
        String upstream = "Unable to read the CSV file '/big.csv' Execution error at ??, \"File is too large, file was"
                + " 1.00 Mb, limit is 1 Mb\"";
        assertEquals(upstream, PureMessages.compile(live));
        RuntimeException rebuilt = Failures.raised(planned);
        assertEquals(live.getMessage(), rebuilt.getMessage());
        assertEquals(upstream, PureMessages.compile(rebuilt));
    }

    @Test
    void theRunsRefusalOfNoRowsIsWordedFromItsSite() {
        SizeRefusal refused = new SizeRefusal("[1]");
        assertEquals(com.legend.error.ErrorKind.SIZE_NOT_MULTIPLICITY.sentence() + ": [1]", refused.getMessage());
        assertEquals("Cannot cast a collection of size 0 to multiplicity [1]", PureMessages.compile(refused));
    }
}
