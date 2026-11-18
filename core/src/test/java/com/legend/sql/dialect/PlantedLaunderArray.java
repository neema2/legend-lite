// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of dd5deb781f, 2026-11-18, MUST-FIX 2): a dialect storing the function into an array another
 *  class can read. */
public abstract class PlantedLaunderArray extends AnsiSqlRenderer {
    static final Object[] KEPT = new Object[1];

    protected PlantedLaunderArray() {
        super(null);
    }

    static void keep() {
        KEPT[0] = com.legend.sql.SqlFn.ABS;
    }
}
