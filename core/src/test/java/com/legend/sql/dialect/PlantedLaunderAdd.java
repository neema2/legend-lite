// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of dd5deb781f, 2026-11-18, MUST-FIX 2): a dialect adding the function to a collection. */
public abstract class PlantedLaunderAdd extends AnsiSqlRenderer {
    static final java.util.List<Object> LIST = new java.util.ArrayList<>();

    protected PlantedLaunderAdd() {
        super(null);
    }

    static void keep() {
        LIST.add(com.legend.sql.SqlFn.ABS);
    }
}
