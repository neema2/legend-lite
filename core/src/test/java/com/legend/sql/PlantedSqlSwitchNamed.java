// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * A PLANTED VIOLATOR for {@code EdgeSeparationTest.eachGuardBites} (the review of 11de9ae229, 2026-11-18): a sql class
 * whose ordinary field is named like javac's switch map -- no synthetic field, so no exemption -- and which reads a
 * function. Test sources only.
 */
public final class PlantedSqlSwitchNamed {

    static final int[] $SwitchMap$none = new int[0];

    private PlantedSqlSwitchNamed() {
    }

    static Object read() {
        return SqlFn.ABS;
    }
}
