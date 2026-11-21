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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A MIXED LIST KEEPS KINDS, ANSWERING AS UPSTREAM (O2, the user's ruling of 2026-11-13; docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md
 * §5.7). The product door's answers, written as JSON rows, on every in-process database. A database whose dialect
 * has no mixed-list placement refuses each case by name at planning (a capability gap outside O2, the orchestrator's
 * found items, 2026-11-13), never with another answer. Each expected answer is
 * legend-pure's, read from its pinned sources (§5.7's box): an Integer and a Float are two kinds; a StrictDate is not
 * its midnight DateTime; the order across kinds is number, date, boolean, string, then the value; within dates, a
 * coarser date before a finer one at the same instant ({@code DateFunctions.compare}). These pins hold the carrier's
 * answers still while the carrier changes (the orchestrator's condition (a) for the JSON-array carrier). A
 * date-time is spelled as the product spells one (register S312: six fraction digits and Z; W4, 2026-11-16): the
 * kinds and the order are upstream's, the text the product's.
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
                "[{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00.000000Z\"}]");
        // E-R5: a date read back from a mixed list is the StrictDate
        CASES.put("|[1, %2025-02-10]->at(1)", "[{\"value\":\"2025-02-10\"}]");
        // E-R4: across kinds, number < date < boolean < string, then the value
        CASES.put("|['b', 1, 'a', 2.5, %2020-01-01, true]->sort()",
                "[{\"value\":1},{\"value\":2.5},{\"value\":\"2020-01-01\"},{\"value\":true},{\"value\":\"a\"},{\"value\":\"b\"}]");
        CASES.put("|['1', 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort()",
                "[{\"value\":1},{\"value\":2},{\"value\":3},{\"value\":\"1\"},{\"value\":\"3\"}]");
        // D1: at one instant, the coarser date first
        CASES.put("|[%2025-02-10T00:00:00, %2025-02-10]->sort()",
                "[{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00.000000Z\"}]");
        CASES.put("|['a', %2025-02-10T00:00:00, 1, %2025-02-10]->sort()",
                "[{\"value\":1},{\"value\":\"2025-02-10\"},{\"value\":\"2025-02-10T00:00:00.000000Z\"},{\"value\":\"a\"}]");
        CASES.put("|[%2025-02, %2025-02-01, %2025]->sort()",
                "[{\"value\":\"2025\"},{\"value\":\"2025-02\"},{\"value\":\"2025-02-01\"}]");
        // D2: the extremes across kinds, by the same order
        CASES.put("|['b', 1, 'a']->min()", "[{\"value\":1}]");
        CASES.put("|['b', 1, 'a']->max()", "[{\"value\":\"b\"}]");
        CASES.put("|['b', 1, 'a']->least()", "[{\"value\":1}]");
        CASES.put("|['b', 1, 'a']->greatest()", "[{\"value\":\"b\"}]");
        CASES.put("|[true, 'x', 2.5, %2020-01-01]->min()", "[{\"value\":2.5}]");
        CASES.put("|[true, 'x', 2.5, %2020-01-01]->max()", "[{\"value\":\"x\"}]");
        // B4 (the PR1 re-review, 2026-11-16): two number kinds with no possible tie answer (were refused)
        CASES.put("|['b', 1, 2.5]->max()", "[{\"value\":\"b\"}]");
        CASES.put("|['b', 1, 2.5]->min()", "[{\"value\":1}]");
        CASES.put("|[-1, 2.5]->max()", "[{\"value\":2.5}]");
        CASES.put("|[-1, 2.5]->min()", "[{\"value\":-1}]");
        // B2 refuses only a collision: a Decimal the double does not hold, alone, orders right
        CASES.put("|[1.23D, 2, 0.5]->min()", "[{\"value\":0.5}]");
        // a Number list's extreme picks the FIRST element of the winning rank, Pure's own rule for a tie of numbers
        // (TaggedSelectionTest: [3.0, 3]->max() is the Float): a tie there answers, never refused (B4's correction)
        CASES.put("|[1, 1.0, 2.5]->min()", "[{\"value\":1}]");
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

    /**
     * A mixed list's extreme across DATE kinds (a StrictDate and a DateTime) is refused by name at planning: the
     * orchestrator's ruling of 2026-11-15 under the user's delegation (docs/records/pr1-rulings-2026-11-15.md, R3),
     * the same rule as S341's run-time kind -- it waits for the run-time kind carrier (O2 step 3). A database whose
     * dialect places no mixed list refuses it first, by its own name.
     */
    @Test
    void anExtremeAcrossDateKindsIsRefusedByName() {
        int refused = 0;
        for (com.legend.testbackend.TestBackend backend : TestBackends.inProcess()) {
            String model = StorelessRuntime.with("", backend.type());
            for (String fn : new String[] {"min", "max", "least", "greatest"}) {
                String query = "|[%2025-02-10, %2025-02-10T00:00:00, %2024-01-01]->" + fn + "()";
                RuntimeException e = assertThrows(RuntimeException.class,
                        () -> Compiler.query(Compiler.compileModel(model), query)
                                .executionPlan(StorelessRuntime.RUNTIME), backend.type() + ": " + query);
                if (e instanceof com.legend.sql.DialectCapability) {
                    continue;
                }
                assertTrue(e instanceof com.legend.error.NotImplementedException, backend.type() + ": " + e);
                // R3 as revised by B4 (2026-11-16): a real literal tie across date kinds
                assertTrue(e.getMessage().contains("of a mixed list holding DateTime and StrictDate: two of its"
                        + " elements of different kinds compare equal"), e.getMessage());
                refused++;
            }
        }
        assertTrue(refused >= 2 * 4, "refused " + refused);
    }

    /**
     * A mixed list's numbers rank as DOUBLE: a literal the double holds exactly ranks exactly and plans (round 55f,
     * 2026-11-15: PCT's testMixedSortNoComparator, a Decimal literal, had been refused); a Decimal literal it does not
     * hold (0.1D), or an integer beyond 2^53, is refused by name.
     */
    @Test
    void aMixedListRanksAnExactLiteralAndRefusesAnInexactOne() {
        String model = StorelessRuntime.with("", TestBackends.platform().type());
        Compiler.query(Compiler.compileModel(model), "|['b', 1, 2.5D, 'a']->sort()")
                .executionPlan(StorelessRuntime.RUNTIME);
        // PCT's testMixedSortNoComparator: Integers and Floats, one negative (round 55f: refused as "a Decimal")
        Compiler.query(Compiler.compileModel(model), "|[342, 5.0, -2.0, 171, 1]->sort()")
                .executionPlan(StorelessRuntime.RUNTIME);
        // B5 (the PR1 re-review, 2026-11-16): an integer literal the double holds exactly ranks, 2^53 and 2^60 too
        Compiler.query(Compiler.compileModel(model), "|['b', 9007199254740992, 1152921504606846976]->sort()")
                .executionPlan(StorelessRuntime.RUNTIME);
        for (String[] c : new String[][] {{"|['b', 1, 0.1D]->sort()", "the Decimal 0.1 (not exact as a DOUBLE)"},
                {"|['b', 9007199254740993, 9007199254740992]->sort()", "the integer 9007199254740993 (not exact"},
                // B2: a Number list's extreme ranks as DOUBLE too (the mixed-elements route)
                {"|[9007199254740992, 9007199254740993, 0.5]->max()", "9007199254740992 and 9007199254740993: its"},
                {"|[1.00000000000000001D, 1.00000000000000002D, 0.5]->min()", "1.00000000000000001 and 1.000000000"},
                // B4: a real literal tie across number kinds
                {"|['b', 1, 1.0]->max()", "two of its elements of different kinds compare equal"}}) {
            com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> Compiler.query(Compiler.compileModel(model), c[0]).executionPlan(StorelessRuntime.RUNTIME),
                    c[0]);
            assertTrue(e.getMessage().contains(c[1]), c[0] + ": " + e.getMessage());
        }
    }

    /**
     * A COMPUTED Integer beyond 2^53 in a mixed list's order is not known at planning: the database raises when it
     * meets one (the PR1 re-review's A2, 2026-11-16; docs/records/pr1-rulings-2026-11-15.md, R4), on the sort's route
     * and on a Number list's extreme's. One within 2^53 answers.
     */
    @Test
    void aComputedIntegerBeyond2To53InAMixedRankRaisesAtRun() throws Exception {
        com.legend.testbackend.TestBackend backend = TestBackends.platform();
        String model = StorelessRuntime.with("", backend.type());
        for (String q : new String[] {"|let n = 9007199254740990 + 3; ['b', $n, 9007199254740991]->sort();",
                "|let n = 9007199254740990 + 3; [$n, 0.5]->max();"}) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), q).executionPlan(StorelessRuntime.RUNTIME);
            try (Connection conn = backend.open()) {
                // the database raises VALUE_NOT_HELD, worded by its kind: lite's own sentence, its place (BE-P makes
                // the place facts)
                com.legend.error.DataError e = assertThrows(com.legend.error.DataError.class,
                        () -> PlanCases.write(plan, conn, Map.of(), PlanCases.Written.JSON::writer), q);
                assertTrue(String.valueOf(e.getMessage()).contains("beyond 2^53"), q + ": " + e);
            }
        }
        ExecutionPlan within = Compiler.query(Compiler.compileModel(model),
                "|let n = 9007199254740980 + 3; [$n, 0.5]->max();").executionPlan(StorelessRuntime.RUNTIME);
        try (Connection conn = backend.open()) {
            assertEquals("[{\"value\":9007199254740983}]",
                    PlanCases.write(within, conn, Map.of(), PlanCases.Written.JSON::writer));
        }
    }
}
