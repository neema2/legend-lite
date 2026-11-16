// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.values;

/**
 * THE FLOAT TOLERANCE, ONE CONSTANT FOR BOTH JUDGES (W10's condition 4, the orchestrator's approval of 2026-11-16):
 * compatibility mode's leniency between two finite Floats (register S15/S132) is {@code |x - y| <= ULPS *
 * ulp(max(|x|, |y|))}. The host judge applies it to two doubles ({@code testexec.Equality.withinTwoUlp}); the database
 * judge writes the same bound into its verdict statement ({@code lowering.VerdictSql}). Both read this constant, so
 * the two judges cannot drift apart on it (G2).
 */
public final class FloatTolerance {

    /** The bound, in units in the last place of the larger magnitude. */
    public static final int ULPS = 2;

    private FloatTolerance() {
    }
}
