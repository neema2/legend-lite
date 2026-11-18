// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        Failure f = Failures.of(new NotAConstant("a side"), "the test");
        assertEquals(Failure.Kind.NOT_A_CONSTANT, f.kind());
        assertTrue(String.valueOf(f.message()).contains("a side"), String.valueOf(f.message()));
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
