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
}
