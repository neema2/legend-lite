// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE PRODUCT'S FORM OF {@link HostForms} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind is
 * types only, its product form a class of its own beside it). Lite's form (register S127, the user's ruling of
 * 2026-11-04): two numbers of different kinds are never equal -- the type is part of the value.
 */
public enum LiteHostForms implements HostForms {
    STRICT;

    @Override
    public boolean equalAcrossNumberKinds(Equality.Kind ek, Equality.Kind ak, Object e, Object a) {
        return false;
    }

    /** Lite compares a Decimal by its value (register S139, the user's ruling of 2026-11-05), as {@code ==} does in
     *  the database. */
    @Override
    public boolean equalDecimals(java.math.BigDecimal e, java.math.BigDecimal a) {
        return e.compareTo(a) == 0;
    }

    /** Lite compares a Float exactly (S15/S132, the user's ruling of 2026-11-05: "I like our clean float"). */
    @Override
    public boolean equalFloatsApart(double e, double a) {
        return false;
    }

    @Override
    public HostWords words() {
        return LiteHostWords.INSTANCE;
    }
}
