// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.RaiseKind;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PLAN DECLARES EVERY RAISE ITS STATEMENTS CAN SEND, ON EVERY DATABASE (audit B's item 1, 2026-11-16): the kinds are
 * recorded where the renderer writes a raise ({@code AnsiSqlRenderer.raiseMessage}), carried by each statement
 * ({@code ExecutionPlan.Sql#raises}) and gathered into the plan's {@code raises} ({@link PlanJson#raises}) -- typed, never
 * read back from a statement's text. Until 2026-11-16 the plan's raises were a scan of its texts for the raise's mark,
 * which missed a dialect that spells the mark in its own escape (BigQuery's), so exec refused the raise as undeclared.
 */
class PlanDeclaresItsRaisesTest {

    /** {@code query} on a connection of {@code type}: planning alone, no database. */
    private static ExecutionPlan planned(DatabaseType type, String query) {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, D INTEGER NOT NULL ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,0\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).executionPlan("s::RT");
    }

    @Test
    void aDivisionsRaiseIsDeclaredOnEveryRelationalDatabase() {
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            ExecutionPlan plan = planned(type, "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)");
            Map<String, String> raises = PlanJson.raises(plan);
            assertTrue(raises.containsKey(RaiseKind.DIVIDE_BY_ZERO.name()), type + ": " + raises);
            // the JSON carries each statement's kinds and the plan's map, and reads back the same
            ExecutionPlan back = PlanJson.read(PlanJson.write(plan));
            assertEquals(plan, back, type.toString());
            assertEquals(raises, PlanJson.raises(back), type.toString());
        }
    }

    @Test
    void aPlanThatRaisesNothingDeclaresNothing() {
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            assertEquals(Map.of(), PlanJson.raises(planned(type, "|#>{s::DB.T}#->select(~[ID])")), type.toString());
        }
    }
}
