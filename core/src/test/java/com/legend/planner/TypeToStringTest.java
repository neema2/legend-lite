// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A TYPE VALUE PRINTS ITS NAME (D3, the orchestrator's go of 2026-11-13; docs/D3_TYPE_TOSTRING_DIAGNOSIS_2026_11_13.md).
 * {@code type()} answers {@code Type[1]}, a class, and its {@code toString} is legend-pure's: a packageable element's
 * name ({@code testClassToString}'s rule). Until D3 it fell to the instance print, an {@code __id} read off the type's
 * name, which every database rejected. A type known only at run time is refused by name.
 */
class TypeToStringTest {

    private static final String MODEL = "Class d3::Car { name: String[1]; }\n";

    @Test
    void aTypeValuePrintsItsNameOnEveryDatabase() throws Exception {
        Map<String, String> cases = Map.of(
                "|1->type()->toString()", "[{\"value\":\"Integer\"}]",
                "|'a'->type()->toString()", "[{\"value\":\"String\"}]",
                "|^d3::Car(name='x')->type()->toString()", "[{\"value\":\"Car\"}]");
        for (TestBackend backend : TestBackends.inProcess()) {
            String model = StorelessRuntime.with(MODEL, backend.type());
            for (Map.Entry<String, String> c : cases.entrySet()) {
                ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), c.getKey())
                        .executionPlan(StorelessRuntime.RUNTIME);
                try (Connection conn = backend.open()) {
                    assertEquals(c.getValue(), PlanCases.write(plan, conn, Map.of(), PlanCases.Written.JSON::writer),
                            backend.type() + ": " + c.getKey());
                }
            }
        }
    }

    /** A mixed list's elements' types, mapped: each element's kind is decided at run time, so {@code type()} -- and
     *  its {@code toString} -- is refused by name (S341, awaiting the user's ruling), never the static Any's path or
     *  the database's own type name ('JSON'), never a database failure. A database with no mixed-list placement
     *  refuses the map by name first. */
    @Test
    void aTypeKnownOnlyAtRunTimeIsRefusedByName() {
        for (TestBackend backend : TestBackends.inProcess()) {
            String model = StorelessRuntime.with(MODEL, backend.type());
            for (String q : java.util.List.of("|[1, 'a']->map(x|$x->type()->toString())", "|[1, 'a']->map(x|$x->type())",
                    "|[1, 2.0]->map(x|$x->type())")) {
                RuntimeException refused = assertThrows(RuntimeException.class, () -> Compiler.query(
                        Compiler.compileModel(model), q).executionPlan(StorelessRuntime.RUNTIME), backend.type() + q);
                assertTrue(refused.getMessage().contains("type() of a value whose kind is decided at run time")
                        || refused instanceof com.legend.sql.DialectCapability, backend.type() + ": " + q + ": " + refused);
            }
        }
    }
}
