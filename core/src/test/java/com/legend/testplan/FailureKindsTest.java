// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.Failures;

import org.junit.jupiter.api.Test;

/**
 * A PLANNED REFUSAL IS RECORDED, NEVER A CRASH (K9 of the edge audit, 2026-11-17: Failure.Kind named the stale class
 * {@code SqlTextVerdicts$NotAConstant} after it moved, so a planned refusal of a non-constant engine text became an
 * IllegalStateException). The exception names its own kind ({@code Failure.Kinded}); and every kind that names a class
 * names one that loads and is a RuntimeException -- a moved class can no longer leave a stale name behind. (Drafted
 * by F; landed by E-Java.)
 */
class FailureKindsTest {

    @Test
    void aNotAConstantRefusalIsRecordedByItsOwnKind() {
        Failure f = Failures.of(new com.legend.edge.NotAConstant("a side"), "the test");
        assertEquals(Failure.Kind.NOT_A_CONSTANT, f.kind());
        assertTrue(String.valueOf(f.message()).contains("a side"), String.valueOf(f.message()));
    }

    /** A defect of lite's planner is no planned failure (the PR #118 audit's JUDGES-4, track A5, 2026-11-19): a JDK
     *  exception off the closed list is raised as itself, its stack kept -- by {@code of}, never wrapped in an
     *  IllegalStateException the catch around that planning would record, and by {@code planned}, which the decline
     *  and message catches ask first; lite's own refusals (an IllegalStateException among them) stay planned. */
    @Test
    void aPlannersDefectIsRaisedAsItselfAndLitesRefusalsStayPlanned() {
        for (RuntimeException defect : java.util.List.of(new NullPointerException("a defect"),
                new ClassCastException("a defect"), new IndexOutOfBoundsException("a defect"),
                new ArrayIndexOutOfBoundsException("a defect"), new java.util.NoSuchElementException("a defect"))) {
            RuntimeException raised = assertThrows(RuntimeException.class, () -> Failures.of(defect, "the test"));
            assertSame(defect, raised, defect.getClass().getSimpleName() + " raised as itself by of");
            assertSame(defect, assertThrows(RuntimeException.class, () -> Failures.planned(defect)),
                    defect.getClass().getSimpleName() + " raised as itself by planned");
        }
        for (RuntimeException refusal : java.util.List.of(new IllegalStateException("lite's refusal"),
                new com.legend.error.DataError("a data error"), new com.legend.error.NotImplementedException("not yet"),
                new UnsupportedOperationException("a dialect's arm"))) {
            assertSame(refusal, Failures.planned(refusal));
            assertEquals(refusal.getMessage(), Failures.of(refusal, "the test").message());
        }
        assertEquals(Failure.Kind.ILLEGAL_STATE, Failures.of(new IllegalStateException("x"), "the test").kind());
    }

    @Test
    void everyNamedKindsClassLoadsAndIsARuntimeException() throws Exception {
        ClassLoader loader = FailureKindsTest.class.getClassLoader();
        for (Failure.Kind kind : Failure.Kind.values()) {
            String name = kind.className();
            if (name == null) {
                continue;   // a kind its exception names itself (Failure.Kinded)
            }
            Class<?> c = Class.forName(name, false, loader);
            assertTrue(RuntimeException.class.isAssignableFrom(c), kind + " names " + name + ", no RuntimeException");
        }
    }
}
