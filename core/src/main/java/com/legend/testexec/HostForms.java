// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE HOST JUDGE'S BEHAVIOURS THE RUN HOOK ANSWERS (census C1's step 2c, the orchestrator's ruling of 2026-11-17: the
 * judging rules keep data alone; a behaviour is the run hook's, never the rules'): asked of the run's hearing
 * ({@link EdgeHearing#hostForms}). Lite's runs answer {@link Product#STRICT}; the edge's form is compiled
 * legend-pure's. Signatures only, and lite's form.
 */
public interface HostForms {

    /** Lite's form (register S127, the user's ruling of 2026-11-04): two numbers of different kinds are never equal --
     *  the type is part of the value. */
    enum Product implements HostForms {
        STRICT;

        @Override
        public boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a) {
            return false;
        }
    }

    /** Whether {@code e} of kind {@code ek} equals {@code a} of the different kind {@code ak}. */
    boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a);
}
