// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A root door PLANTED for one test alone (the next PR's item 1, the closed planner surface, 2026-11-13): a public method
 * no row of {@code PlannerSurfaceTest.ROOT_DOORS} lists, its argument a statement.
 * {@code PlannerSurfaceTest.theRootDoorsRefuseAPlantedDoor} shows the check names it. A test class: the check's real
 * run reads the five root doors, never this one.
 */
public final class PlantedRootDoor {

    private PlantedRootDoor() {
    }

    /** A door no row lists: the defect the check refuses. */
    public static String runSql(String sql) {
        return sql;
    }

    /** TypedQuery's old lowering door, re-added here (the user's ruling of 2026-11-13, option (b), took it off the
     *  root): a lowering apart from the planner's one seed entry, which the check names. */
    public static String lower(String runtime, boolean streaming) {
        return runtime + streaming;
    }

    /** A public nested type: its constructor takes a statement, and its accessor is a door too (the C4 audit's M3,
     *  2026-11-13: the census read the root doors' own methods alone). */
    public record Statement(String sql) {
    }
}
