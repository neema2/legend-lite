// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.executionplan.ExecutionPlan;

/**
 * THE TEST DOOR (the test-is-the-product plan's M3, brief B12 items 1 and 3; B4 brought forward by R17 C7): a test
 * body planned whole as one program on the product's program road, whatever it holds -- the statements a product
 * query holds planned by the planner, the kinds only a test's program holds ({@code QueryDoor.TestOnly}) by this
 * library. The test-planning library is its own target ({@code //core:testplan}): nothing on the plan side
 * ({@code //core:plan_side}, which the WebAssembly and native planners carry) and no server class reaches it, so the
 * product's query door can plan none of a test's kinds (ArchitectureTest). Its one caller is the driver; until a body
 * class moves to it (M5, R16), its callers are its tests.
 */
public final class TestDoor {

    private TestDoor() {
    }

    /** {@code body}'s plan on {@code runtime}, under {@code features}, for a session whose server is
     *  {@code serverVersion} (null: the dialect's declared target). */
    public static ExecutionPlan plan(TypedQuery body, String runtime,
            java.util.Set<com.legend.platform.Feature> features, @com.legend.base.Nullable String serverVersion) {
        return com.legend.planner.PlanMaker.testProgram(body, runtime, features, serverVersion,
                new TestKinds(body, runtime));
    }
}
