// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE HOST JUDGE'S BEHAVIOURS THE RUN HOOK ANSWERS (census C1's step 2c, the orchestrator's ruling of 2026-11-17: the
 * judging rules keep data alone; a behaviour is the run hook's, never the rules'): asked of the run's hearing ({@link
 * EdgeHearing#hostForms}). Lite's runs answer {@link LiteHostForms#STRICT}; the edge's form is compiled legend-pure's.
 * Signatures only; lite's form is {@code LiteHostForms}. (Its number-kind, date and Decimal rules went 2026-11-24, B3
 * step 1a: one rulebook, the judge's own, in {@code Equality}.)
 */
public interface HostForms {

    // (equalFloatsApart, whether two Floats whose values differ are equal all the same -- compiled legend-pure's 2 ULP
    // at the edge, exact in lite: deleted 2026-11-24, B3 step 1b; a Float compared exactly on every route, S15)

    /** The message an assertError matches a caught {@code error} by (K10 of the edge audit, the orchestrator's ruling
     *  (A) of 2026-11-17): lite's, the error's own message; legend-pure's, a raise of a planned site worded from its
     *  record after the run (stage B's B1, 2026-11-20). An error's words, never an assert's failure: those are one
     *  catalog's on every route (item 3, 2026-11-21: until then HostWords' 26th method, beside its two forms of the
     *  assert words, deleted). */
    String caught(RuntimeException error, EdgeHearing hearing);
}
