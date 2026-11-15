// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A planner surface PLANTED for one test alone (the next PR's item 1, "separation made foolproof", the closed planner
 * surface, 2026-11-13): a listed door ({@code snapCopy}) whose request carries a statement ({@code sql}), and a door no
 * row lists ({@code runSql}). {@code PlannerSurfaceTest.theSurfaceRefusesAPlantedDoor} shows the surface's check names
 * both. A test class: the check's real run reads {@code planner.Boundary}, never this one.
 */
public final class PlantedBoundary {

    private PlantedBoundary() {
    }

    /** A snapshot's copy whose request carries a statement: the defect the surface refuses. */
    public static String snapCopy(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a planted snapshot's copy", "databaseType", "model", "runtime", "lambda", "schema", "table", "sql");
        return in.getString("sql");
    }

    /** A door no row lists, its argument a statement. */
    public static String runSql(String sql) {
        return sql;
    }

    private static void only(com.legend.json.Json.Obj o, String what, String... keys) {
        java.util.Set<String> allowed = java.util.Set.of(keys);
        for (String key : o.fields().keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException(what + " has no key " + key);
            }
        }
    }
}
