// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.error.NotImplementedException;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link WritePlansTest}'s write plans on H2 (in process), and a write plan on a database the runner shares: an H2
 *  connection's declared test data, the in-memory database decision A shares by its target. */
class WritePlansH2Test {

    @Test
    void plans() throws Exception {
        WritePlansTest.plans(TestBackends.of(DatabaseType.H2));
    }

    /** A write plan on a database the runner shares runs on one of its own, set up from the target: the shared one is
     *  never changed, so the next run reads it as set up (decision A's second rule). */
    @Test
    void aWritePlanNeverChangesADatabaseTheRunnerShares() throws Exception {
        String model = """
                ###Relational
                Database w::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
                ###Connection
                RelationalDatabaseConnection w::Conn {
                  store: w::DB;
                  type: H2;
                  specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME\\n1,a\\n2,b\\n'; };
                  auth: DefaultH2;
                }
                ###Runtime
                Runtime w::RT { mappings: [ ]; connections: [ w::DB: [ c: w::Conn ] ]; }
                """;
        var ctx = com.legend.Compiler.compileModel(model);
        ExecutionPlan delete = com.legend.Compiler.query(ctx, "|#>{w::DB.T}#->delete(r|$r.ID == 1)")
                .executionPlan("w::RT");
        ExecutionPlan read = com.legend.Compiler.query(ctx, "|#>{w::DB.T}#->select(~[NAME])->sort(~NAME->ascending())")
                .executionPlan("w::RT");
        ExecutionPlan count = com.legend.Compiler.query(ctx, "|1").executionPlan("w::RT");
        String before = WritePlansTest.run(read, PlanSessions.shared(), Map.of());
        assertTrue(before.contains("\"a\"") && before.contains("\"b\""), before);
        // the delete's count, on a database of the run's own, set up as the shared one is
        assertEquals(WritePlansTest.run(count, PlanSessions.shared(), Map.of()),
                WritePlansTest.run(delete, PlanSessions.shared(), Map.of()));
        assertEquals(before, WritePlansTest.run(read, PlanSessions.shared(), Map.of()));
    }

    /** Over upstream's execute (E8), a program whose answer is a count (a value, whose serialization there is unprobed,
     *  as a read's is) is refused by name before its plan runs: its write never lands behind the refused answer. */
    @Test
    void executeRefusesACountAnswerBeforeItsWriteRuns() throws Exception {
        TestBackend h2 = TestBackends.of(DatabaseType.H2);
        String model = WritePlansTest.MODEL.formatted(h2.connectionBody());
        try (Connection c = h2.openWorkspace()) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
                s.execute("INSERT INTO T VALUES (1, 'a', 10)");
            }
            var delete = com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                    com.legend.Compiler.parseQuery("|#>{w::DB.T}#->delete(r|true)"));
            NotImplementedException refused = assertThrows(NotImplementedException.class,
                    () -> com.legend.Execution.run(delete, "w::RT", (target, ctx) -> {
                        throw new AssertionError("a program that writes opens no query session");
                    }, PlanSessions.given(c), com.legend.Execution.Establish.DECLARED_SETUP,
                            plan -> com.legend.server.PureV1Api.engineWriter(delete, plan, new StringWriter())));
            assertTrue(refused.getMessage().contains("SCALAR result's serialization is unprobed"),
                    refused.getMessage());
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM T")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "the refused program wrote nothing");
            }
        }
    }
}
