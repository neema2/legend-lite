// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * A PLANTED VIOLATOR for {@code EdgeSeparationTest.eachGuardBites} (the edge audit's K16, 2026-11-17: the CODE guard
 * skipped every {@code com.legend.sql} class's field reads): a class of the sql package that reads a SqlFn constant
 * (ABS, planted as edge-only).
 * Test sources only; never in a product jar.
 */
public final class PlantedSqlReach {

    private PlantedSqlReach() {
    }

    static Object reach() {
        return SqlFn.ABS;
    }
}
