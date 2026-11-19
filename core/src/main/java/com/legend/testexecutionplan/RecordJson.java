// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.json.Json;

import java.util.Map;

/**
 * THE TEST PLAN'S RECORDS AS JSON, FOR AN EXTENSION'S CODEC (census C2, 2026-11-17): an edge record that holds the
 * plan's own records -- a side, a side's shape, a failure, a kind class -- writes and reads them by the plan's codec,
 * one spelling for both. Data in, data out.
 */
public final class RecordJson {

    private RecordJson() {
    }

    public static Map<String, Object> side(TestPlan.Side side) {
        return TestPlanJson.side(side);
    }

    public static TestPlan.Side readSide(Json.Obj o) {
        return TestPlanJson.readSide(o);
    }

    public static Map<String, Object> failure(Failure f) {
        return HostJson.wFailure(f);
    }

    public static Failure readFailure(Json.Obj o) {
        return HostJson.rFailure(o);
    }

    public static Map<String, Object> sideShape(SideShape s) {
        return HostJson.wSideShape(s);
    }

    public static SideShape readSideShape(Json.Obj o) {
        return HostJson.rSideShape(o);
    }

    public static Map<String, Object> kindClass(KindClass k) {
        return HostJson.wKindClass(k);
    }

    public static KindClass readKindClass(Json.Obj o) {
        return HostJson.rKindClass(o);
    }
}
