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

    /** The words a failure is worded in (census J29, 2026-11-18: the hook, never a flag): lite's runs
     *  {@link LiteHostWords}, the compatibility edge's legend-pure's. */
    HostWords words();
}
