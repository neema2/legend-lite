// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * A PIVOT'S CELL FOR A (GROUP, KEY) PAIR WITH NO ROWS is empty for every aggregate, a count's too, as Pure's pivot
 * defines it (TestTDS.applyPivot, both runtimes; register S379, proposed 2026-11-19 by the orchestrator under the user's
 * goal of ~13:25 UTC, "lite follows Pure", the user's ruling of this case pending): until then a count answered 0 there, legend-engine's database PIVOT's answer. A pair whose rows are present and
 * whose values are all empty still counts 0. On every database the platform runs in-process, the dynamic pivot (a keyed
 * plan) and the static one (its values named: a native PIVOT, or the emulation of a database with none).
 */
class PivotAbsentCellTest {

    /** LON has no 2011 row (absent: empty); NYC's 2000 row has no v (present: a count of 0). The keys appear in their
     *  sorted order, so the dynamic pivot's columns read alike on every database. */
    private static final String DATA = "|#TDS\n city:String, year:Integer, v:Integer\n NYC, 2000, null\n NYC, 2011, 5\n"
            + " LON, 2000, 7\n LON, 2000, 9\n#";

    private static String answer(TestBackend backend, String query) throws Exception {
        ExecutionPlan typed = Compiler.query(Compiler.compileModel(PlanMakerTest.model(backend.type().name())),
                DATA + query + "->sort(~city->ascending())").executionPlan("s::RT");
        try (Connection c = backend.open()) {
            return PlanCases.write(typed, c, java.util.Map.of(), PlanCases.Written.JSON::writer);
        }
    }

    @Test
    void aCountOfNoRowsIsEmptyAndACountOfEmptyValuesIsZero() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals("[{\"city\":\"LON\",\"2000__|__c\":2,\"2011__|__c\":null},"
                    + "{\"city\":\"NYC\",\"2000__|__c\":0,\"2011__|__c\":1}]",
                    answer(backend, "->pivot(~[year], ~['c' : x | $x.v : y | $y->count()])"), backend.name() + " dynamic");
            assertEquals("[{\"city\":\"LON\",\"2000__|__c\":2,\"2011__|__c\":null},"
                    + "{\"city\":\"NYC\",\"2000__|__c\":0,\"2011__|__c\":1}]",
                    answer(backend, "->pivot(~year, [2000, 2011], ~c : x | $x.v : y | $y->count())"),
                    backend.name() + " static");
        }
    }

    /** Every other aggregate was already empty there: a sum, for one. */
    @Test
    void aSumOfNoRowsIsEmptyToo() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals("[{\"city\":\"LON\",\"2000__|__s\":16,\"2011__|__s\":null},"
                    + "{\"city\":\"NYC\",\"2000__|__s\":null,\"2011__|__s\":5}]",
                    answer(backend, "->pivot(~[year], ~['s' : x | $x.v : y | $y->sum()])"), backend.name() + " sum");
        }
    }
}
