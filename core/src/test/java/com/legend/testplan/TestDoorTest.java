// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.test.StorelessRuntime;
import com.legend.testcases.PlanRoundTrip;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * THE TEST DOOR (the test-is-the-product plan's M3c, B12 items 1 and 3): a body planned whole as one program on the
 * product's program road; its target carries no setup (R17 C6: a test's seeds are its own steps, M3e); a kind only a
 * test's program holds is the test-planning library's, refused by name until the sub-step that plans it.
 */
class TestDoorTest {

    private static final String SEEDED = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::Conn
            {
                store: x::DB;
                type: H2;
                specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME\\n1,Acme\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
            """;

    @Test
    void aBodyIsOneProgramWhoseTargetCarriesNoSetup() {
        TypedQuery body = Compiler.query(Compiler.compileModel(SEEDED),
                "|x::Firm.all()->project([f|$f.name], ['name'])");
        ExecutionPlan test = TestDoor.plan(body, "x::RT", Set.of(), null);
        PlanRoundTrip.assertRoundTrips(test, "a body through the test door");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, test.root());
        assertEquals(1, seq.steps().size());
        ExecutionPlan.TdsResult answer = assertInstanceOf(ExecutionPlan.TdsResult.class, seq.steps().get(0));
        assertEquals(List.of(), answer.sql().target().setup(), "R17 C6: the test door's target carries no setup");
        // the statement is the query door's: one road
        ExecutionPlan query = body.executionPlan("x::RT");
        assertEquals(query.answerSql().statement(), answer.sql().statement());
        assertFalse(query.answerSql().target().setup().isEmpty(), "the query door's target: the declared rows");
    }

    @Test
    void aTestsKindIsTheLibrarysRefusedByNameUntilItsStep() {
        TypedQuery body = Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)),
                "{|let x = 1; assertEquals(1, $x);}");
        assertEquals("an assert on the test door's program road: not planned yet (the test-is-the-product plan's M6,"
                + " the verdict record)", assertThrows(NotImplementedException.class,
                        () -> TestDoor.plan(body, StorelessRuntime.RUNTIME, Set.of(), null)).getMessage());
    }
}
