// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * H10 SPIKE: compiled legend-pure's Float tolerance (S15/S132), what remains of the edge's form of the deleted
 * {@code HostForms}: the constant and the comparison the edge's rendered-text kind and its SQL verdict numbers use.
 * S127 and S139 (two number kinds equal by their texts; a Decimal's scale part of its value) were used by no lane
 * (H6 section 6.1) and go; S15 on a scalar Float is the door rewrite R-S15's, not the judge's.
 */
public final class CompiledPureHostForms {

    private CompiledPureHostForms() {
    }

    /** The units in the last place two Floats may differ by and be equal: compiled legend-pure's 2 (S15/S132). */
    public static final int FLOAT_ULPS = 2;

    /** Two finite doubles within {@code FLOAT_ULPS * ulp(max(|x|, |y|))} (moved from testexec.Equality, train 17,
     *  2026-11-18). */
    public static boolean withinUlps(double x, double y) {
        if (!Double.isFinite(x) || !Double.isFinite(y)) {
            return false;
        }
        double ulp = Math.ulp(Math.max(Math.abs(x), Math.abs(y)));
        boolean ok = Math.abs(x - y) <= FLOAT_ULPS * ulp;
        if (ok && x != y) {
            com.legend.diagnostics.Census.inc(com.legend.diagnostics.Census.Key.ULP_FIRINGS);
            if (System.getenv("LL_TOL_COUNT") != null) {
                System.err.println("[tol] ulp " + x + " vs " + y);
            }
        }
        return ok;
    }
}
