// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE HOST JUDGE'S BEHAVIOURS THE RUN HOOK ANSWERS (census C1's step 2c, the orchestrator's ruling of 2026-11-17: the
 * judging rules keep data alone; a behaviour is the run hook's, never the rules'): asked of the run's hearing ({@link
 * EdgeHearing#hostForms}). Lite's runs answer {@link LiteHostForms#STRICT}; the edge's form is compiled legend-pure's.
 * Signatures only; lite's form is {@code LiteHostForms}.
 */
public interface HostForms {

    /** Whether {@code e} of kind {@code ek} equals {@code a} of the different kind {@code ak}. */
    boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a);

    /** Whether two dates of one kind are equal (register S399, the user's ruling of 2026-11-22): lite's by kind and
     *  instant, the edge's by legend-pure's written form. Either side may be a driver's temporal carrier. */
    boolean equalDates(Object e, Object a);

    /** Whether two Decimals are equal (train 17, 2026-11-18: until then the plan's {@code decimalScale} rule). */
    boolean equalDecimals(java.math.BigDecimal e, java.math.BigDecimal a);

    /** Whether two finite Floats whose values differ are equal all the same (train 17, 2026-11-18: until then the
     *  plan's {@code floatUlps} rule). */
    boolean equalFloatsApart(double e, double a);

    /** The message an assertError matches a caught {@code error} by (K10 of the edge audit, the orchestrator's ruling
     *  (A) of 2026-11-17): lite's, the error's own message; legend-pure's, a raise of a planned site worded from its
     *  record after the run (stage B's B1, 2026-11-20). An error's words, never an assert's failure: those are one
     *  catalog's on every route (item 3, 2026-11-21: until then HostWords' 26th method, beside its two forms of the
     *  assert words, deleted). */
    String caught(RuntimeException error, EdgeHearing hearing);
}
