// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexec.Equality;

/**
 * COMPILED LEGEND-PURE'S HOST EQUALITY (register S127; census C1's step 2c, the orchestrator's ruling of 2026-11-17:
 * moved from {@code testexec.Equality}): the edge's form of the run hook's {@link com.legend.testexec.HostForms} --
 * {@code CompiledSupport.eq(Number, Number)}, what a test's assert runs upstream: an Integer and a Decimal equal by
 * their texts ({@code 1D == 1}), a Float equal to neither.
 */
public enum CompiledPureHostForms implements com.legend.testexec.HostForms {
    INSTANCE;

    @Override
    public boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a) {
        boolean integerAndDecimal = ek == Equality.Kind.INTEGER && ak == Equality.Kind.DECIMAL
                || ek == Equality.Kind.DECIMAL && ak == Equality.Kind.INTEGER;
        return integerAndDecimal && String.valueOf(e).equals(String.valueOf(a));
    }
}
