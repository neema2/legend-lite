// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.TypedQuery;
import com.legend.planner.PlanMaker;

/**
 * A class PLANTED in the server's package to reach the program road's test entry past the test door, for one test alone
 * (the final audit of 9af16fbe58's J2, 2026-11-14): {@code ArchitectureTest.PROGRAM_ROAD_TEST_ENTRY} fails on it. A test
 * class: the rule's real checks read the product's classes and the hosts' doors, never this one.
 */
public final class PlantedTestProgram {

    private PlantedTestProgram() {
    }

    /** A test's program planned outside the test door: the defect the rule refuses. */
    public static Object planned(TypedQuery query) {
        return PlanMaker.testProgram(query, "r", java.util.Set.of(), null, false);
    }
}
