// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a helper in the dialects' package nesting, two levels
 *  down, a class implementing SqlDialect -- no dialect -- whose read is named. */
public final class PlantedRendererNest {
    private PlantedRendererNest() {
    }

    public static final class A {
        public abstract static class R implements SqlDialect {
            static com.legend.sql.SqlFn f() {
                return com.legend.sql.SqlFn.ABS;
            }
        }
    }

    static Object use() {
        return com.legend.sql.SqlExpr.Call.of(A.R.f(), new com.legend.sql.SqlExpr.IntLit(1));
    }
}
