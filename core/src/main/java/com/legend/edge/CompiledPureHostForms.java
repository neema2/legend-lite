// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * THE EDGE'S FORM OF THE RUN HOOK'S {@link com.legend.testexec.HostForms} (census C1's step 2c, the orchestrator's ruling
 * of 2026-11-17: moved from {@code testexec.Equality}): what is left of compatibility's own host rules while B3 lands its
 * one rulebook, lite's (E1 withdrawn 2026-11-20 ~21:00). Its crossing, date and Decimal rules went 2026-11-24 (B3
 * step 1: they moved no test); the Float tolerance (S15/S132) goes once the tests it carries are placed against
 * legend-engine's own answer; the caught error's words are presentation and stay.
 */
public enum CompiledPureHostForms implements com.legend.testexec.HostForms {
    INSTANCE;

    /** The units in the last place two Floats may differ by and be equal: compiled legend-pure's 2 (S15/S132). */
    public static final int FLOAT_ULPS = 2;

    /** Two Floats within {@link #FLOAT_ULPS} ULP are equal (S15/S132; train 17, 2026-11-18: moved from
     *  testexec.Equality). */
    @Override
    public boolean equalFloatsApart(double e, double a) {
        boolean within = withinUlps(e, a);
        if (within) {
            // the exact-comparison report (S15/S132 narrowed, 2026-11-05): the tolerance, not equality, decided
            com.legend.diagnostics.Diagnostics.judgeSend("ulp-tolerance", "host " + e + " ~ " + a);
        }
        return within;
    }

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

    /** legend-pure's words of a caught error (K10 of the edge audit, 2026-11-18): a raise of a planned site worded
     *  from its site's record, after the run (stage B's B1, 2026-11-20: {@link PureMessages#worded}); any other error
     *  its own message -- the edge's one wording of an error, the pure/v1 routes' too. */
    @Override
    public String caught(RuntimeException error, com.legend.testexec.EdgeHearing hearing) {
        return PureMessages.compile(error);
    }
}
