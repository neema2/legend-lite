// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of dd5deb781f, 2026-11-18, MUST-FIX 2): a dialect handing the function to its own setter. */
public abstract class PlantedLaunderSetter extends AnsiSqlRenderer {
    static Object held;

    protected PlantedLaunderSetter() {
        super(null);
    }

    static void put(Object fn) {
        held = fn;
    }

    static void use() {
        put(com.legend.sql.SqlFn.ABS);
    }
}
