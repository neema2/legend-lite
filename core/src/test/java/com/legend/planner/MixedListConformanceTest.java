// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A MIXED LIST KEEPS KINDS, ANSWERING AS UPSTREAM (O2, the user's ruling of 2026-11-13; docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md
 * §5.7). The product door's answers, written as JSON rows, on every in-process database. A database whose dialect
 * has no mixed-list placement refuses each case by name at planning (a capability gap outside O2, the orchestrator's
 * found items, 2026-11-13), never with another answer. Each expected answer is
 * legend-pure's, read from its pinned sources (§5.7's box): an Integer and a Float are two kinds; a StrictDate is not
 * its midnight DateTime; the order across kinds is number, date, boolean, string, then the value; within dates, a
 * coarser date before a finer one at the same instant ({@code DateFunctions.compare}). These pins hold the carrier's
 * answers still while the carrier changes (the orchestrator's condition (a) for the JSON-array carrier).
 */
class MixedListConformanceTest {

    private static final Map<String, String> CASES = new LinkedHashMap<>();

    static {
        // E-R1: [1, 1.0] is two elements (CompiledSupport.eq: a Long is never a Double)
        CASES.put("|[1, 1.0]->removeDuplicates()", "[{\"value\":1},{\"value\":1.0}]");
        // E-R2: the Integer keeps its kind
        CASES.put("|[1, 2.0]->at(0)", "[{\"value\":1}]");
        CASES.put("|[1, 2.0]->at(0)->type()", "[{\"value\":\"Integer\"}]");
        // E-R3: a StrictDate and its midnight DateTime are two elements
        CASES.put("|[%2025-02-10, %2025-02-10T00:00:00]->removeDuplicates()",
                "[{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00+0000\"}]");
        // E-R5: a date read back from a mixed list is the StrictDate
        CASES.put("|[1, %2025-02-10]->at(1)", "[{\"value\":\"2025-02-10\"}]");
        // E-R4: across kinds, number < date < boolean < string, then the value
        CASES.put("|['b', 1, 'a', 2.5, %2020-01-01, true]->sort()",
                "[{\"value\":1},{\"value\":2.5},{\"value\":\"2020-01-01\"},{\"value\":true},{\"value\":\"a\"},{\"value\":\"b\"}]");
        CASES.put("|['1', 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort()",
                "[{\"value\":1},{\"value\":2},{\"value\":3},{\"value\":\"1\"},{\"value\":\"3\"}]");
        // D1: at one instant, the coarser date first
        CASES.put("|[%2025-02-10T00:00:00, %2025-02-10]->sort()",
                "[{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00+0000\"}]");
        CASES.put("|['a', %2025-02-10T00:00:00, 1, %2025-02-10]->sort()",
                "[{\"value\":1},{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00+0000\"},{\"value\":\"a\"}]");
        CASES.put("|[%2025-02, %2025-02-01, %2025]->sort()",
                "[{\"value\":\"2025\"},{\"value\":\"2025-02\"},{\"value\":\"2025-02-01\"}]");
        // D2: the extremes across kinds, by the same order
        CASES.put("|['b', 1, 'a']->min()", "[{\"value\":1}]");
        CASES.put("|['b', 1, 'a']->max()", "[{\"value\":\"b\"}]");
        CASES.put("|['b', 1, 'a']->least()", "[{\"value\":1}]");
        CASES.put("|['b', 1, 'a']->greatest()", "[{\"value\":\"b\"}]");
        CASES.put("|[true, 'x', 2.5, %2020-01-01]->min()", "[{\"value\":2.5}]");
        CASES.put("|[true, 'x', 2.5, %2020-01-01]->max()", "[{\"value\":\"x\"}]");
    }

    @Test
    void aMixedListAnswersAsUpstreamOnEveryDatabase_orIsRefusedByName() throws Exception {
        int answered = 0;
        for (com.legend.testbackend.TestBackend backend : TestBackends.inProcess()) {
            String model = StorelessRuntime.with("", backend.type());
            for (Map.Entry<String, String> c : CASES.entrySet()) {
                ExecutionPlan plan;
                try {
                    plan = Compiler.query(Compiler.compileModel(model), c.getKey())
                            .executionPlan(StorelessRuntime.RUNTIME);
                } catch (com.legend.sql.DialectCapability refusedByName) {
                    continue;
                }
                try (Connection conn = backend.open()) {
                    assertEquals(c.getValue(), PlanCases.write(plan, conn, Map.of(), PlanCases.Written.JSON::writer),
                            backend.type() + ": " + c.getKey());
                }
                answered++;
            }
        }
        // at least two databases answer every case (the in-process set holds two that place a mixed list)
        assertTrue(answered >= 2 * CASES.size(), "answered " + answered + " of " + CASES.size() + " cases a database");
    }
}
