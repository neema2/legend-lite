// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * THE EDGE'S FORM OF THE RUN HOOK'S {@link com.legend.testexec.HostForms} (census C1's step 2c, the orchestrator's ruling
 * of 2026-11-17: moved from {@code testexec.Equality}): what is left of compatibility's own host rules while B3 lands its
 * one rulebook, lite's (E1 withdrawn 2026-11-20 ~21:00). Its crossing, date and Decimal rules went 2026-11-24 (B3
 * step 1a: they moved no test), and its Float tolerance for two values (S15) the same day (B3 step 1b: legend-engine
 * compares exactly, and the two tests it carried were a lite bug, fixed); the 2-ULP bound stays for S132's printed
 * texts until B3's lift (and for the referee's and EqualToJson's own comparisons); the caught error's words are
 * presentation.
 */
public enum CompiledPureHostForms implements com.legend.testexec.HostForms {
    INSTANCE;

    /** The units in the last place two Floats may differ by and be equal: compiled legend-pure's 2 -- S132's printed
     *  texts alone since S15's deletion (2026-11-24), and the corpus referee's own comparison. */
    public static final int FLOAT_ULPS = 2;

    // (equalFloatsApart, two Floats within FLOAT_ULPS equal on the host judge's compatibility route, S15: deleted
    // 2026-11-24, B3 step 1b)

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

    /** The edge's asserts keep an Integer, a Float and a Decimal three types never equal, as they were judged before
     *  the product's numbers became two families (the user's ruling of 2026-11-27: the other two modes keep their
     *  own behaviour). */
    @Override
    public boolean exactNumbersByValue() {
        return false;
    }

    /** legend-pure's words of a caught error (K10 of the edge audit, 2026-11-18): a raise of a planned site worded
     *  from its site's record, after the run (stage B's B1, 2026-11-20: {@link PureMessages#worded}); any other error
     *  its own message -- the edge's one wording of an error, the pure/v1 routes' too. */
    @Override
    public String caught(RuntimeException error, com.legend.testexec.EdgeHearing hearing) {
        return PureMessages.compile(error);
    }
}
