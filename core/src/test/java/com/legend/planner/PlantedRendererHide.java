// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A PLANTED VIOLATOR for {@code EdgeSeparationTest.eachGuardBites} (the review of 11de9ae229, 2026-11-18, MUST-FIX 1): a
 * product class outside the dialects that reads a function through a nested class implementing SqlDialect -- a renderer
 * by type, not by home -- and builds the call with it. Test sources only; never in a product jar.
 */
public final class PlantedRendererHide {

    private PlantedRendererHide() {
    }

    /** A renderer by type alone, hiding the read. */
    public abstract static class R implements com.legend.sql.dialect.SqlDialect {
        static com.legend.sql.SqlFn fn() {
            return com.legend.sql.SqlFn.ABS;
        }
    }

    static Object use() {
        return com.legend.sql.SqlExpr.Call.of(R.fn(), new com.legend.sql.SqlExpr.IntLit(1));
    }
}
