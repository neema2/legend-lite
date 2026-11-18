// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of dd5deb781f, 2026-11-18, MUST-FIX 2): a dialect throwing the function to its caller. */
public abstract class PlantedLaunderThrow extends AnsiSqlRenderer {
    protected PlantedLaunderThrow() {
        super(null);
    }

    static void fail() {
        throw new Carrier(com.legend.sql.SqlFn.ABS);
    }

    static final class Carrier extends RuntimeException {
        final transient Object fn;

        Carrier(Object fn) {
            this.fn = fn;
        }
    }
}
