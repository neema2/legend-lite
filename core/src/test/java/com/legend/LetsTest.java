// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A let lives in the database or nowhere (docs/PLAN_PATH_PROGRAM_2026_10_17.md §5, R11; step 5, 2026-10-18): a query
 * let is dissolved -- its expression substituted where the name is read, the database computing it there -- and no
 * value is ever fetched and bound by Java. A clock read ({@code now}) is one value for the statement, so a let over
 * it dissolves too. A let over a value the database computes anew at each call ({@code generateGuid}), read more
 * than once, cannot be dissolved and is refused by name until materialization is built.
 */
class LetsTest {
    static final String MODEL = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID\\n2\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    @Test
    void aLetIsDissolved_aClockReadIsOneValue_aValueComputedAnewEachCallReadTwiceIsRefused() throws Exception {
        var ctx = Compiler.compileModel(MODEL);
        try (Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            ExecutionPlan twice = Compiler.query(ctx, "|let x = 2 + 3; $x * $x;").executionPlan("s::RT");
            assertTrue(twice.answerSql().statement().contains("2 + 3"));
            assertEquals("[{\"value\":25}]", PlanCases.write(twice, c, java.util.Map.of(), PlanCases.Written.JSON::writer));
            ExecutionPlan clock = Compiler.query(ctx, "|let n = now(); $n == $n;").executionPlan("s::RT");
            assertEquals("[{\"value\":true}]", PlanCases.write(clock, c, java.util.Map.of(), PlanCases.Written.JSON::writer));
            ExecutionPlan once = Compiler.query(ctx, "|let g = generateGuid(); $g->length();").executionPlan("s::RT");
            assertEquals("[{\"value\":36}]", PlanCases.write(once, c, java.util.Map.of(), PlanCases.Written.JSON::writer));
        }
        var refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.query(ctx, "|let g = generateGuid(); $g == $g;").executionPlan("s::RT"));
        assertTrue(refused.getMessage().contains("read 2 times"), refused.getMessage());
    }
}
