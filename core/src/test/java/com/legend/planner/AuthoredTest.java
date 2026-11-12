// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A statement a person wrote, as a plan (Studio's SQL playground; the user's ruling of 2026-11-12, "Yes do 5 and 6"). */
class AuthoredTest {

    @Test
    void thePersonsTextIsThePlansOneStatement_asWritten() {
        // two statements, a comment, odd spacing: none of it read -- one text, one statement
        String text = "INSERT INTO t VALUES (1);  -- mine\n  SELECT  * FROM t ";
        DuckDb dialect = new DuckDb();
        ExecutionPlan plan = Authored.plan(dialect, DatabaseType.DuckDB, text);
        ExecutionPlan.TdsResult root = assertInstanceOf(ExecutionPlan.TdsResult.class, plan.root());
        assertEquals(ExecutionPlan.TdsResult.Shape.RELATION, root.shape());
        assertEquals(List.of(), root.columns(), "the relation the database reports: no column declared");
        assertEquals(text, root.sql().statement(), "the text as written");
        assertEquals(List.of(), root.sql().slots(), "no value bound");
        assertEquals(Targets.of(new ExecutionPlan.Database.Platform(DatabaseType.DuckDB, null), dialect),
                root.sql().target());
        assertTrue(plan.writes(), "a person's text may write (an INSERT): the plan says it may");
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
    }

    @Test
    void anEmptyTextIsRefusedByName() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> Authored.plan(new DuckDb(), DatabaseType.DuckDB, " \n "));
        assertEquals("an authored statement is the person's text, and this one is empty", refused.getMessage());
    }
}
