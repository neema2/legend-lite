// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.legend.planner.PlanMakerTest.plan;
import static com.legend.planner.PlanMakerTest.target;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What DuckDB's plans say ({@link PlanMakerTest}'s cases, DuckDB's own answers; moved here from it by the dialect
 * program's Phase 6, 2026-10-16): its session, its bulk-loaded rows and the parameters it binds bare.
 */
class PlanMakerDuckDbTest {

    private static final DatabaseType DUCKDB = DatabaseType.DuckDB;

    @Test
    void theTargetIsWhole_itsDatabaseSessionAndSetup() {
        ExecutionPlan.Target duck = target(plan(DUCKDB, "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])"
                + "->sort(~ID->ascending())", com.legend.testcases.PlanCases.Written.JSON));
        assertEquals(List.of("SET TimeZone='UTC'"), duck.session());
        // the rows are statements in a content blob, each cell its column's typed literal (A2, 2026-11-10: until then
        // DuckDB's bulk loader took them as text)
        ExecutionPlan.Load load = duck.loads().get(0);
        ExecutionPlan.LoadStep.Statements rows = (ExecutionPlan.LoadStep.Statements) load.steps().get(load.steps().size() - 1);
        assertEquals(List.of("INSERT INTO T (ID, NAME, PRICE) VALUES (1, 'a', 1.50), (2, 'O''Brien', NULL), (3, NULL, 3.25);"),
                com.legend.executionplan.DataRef.Statements.decode(load.content().get(rows.ref().digest())));
    }

    /** DuckDB binds a parameter typed by its value bare, as the database types the value (H2 casts it to a type hole:
     *  {@link PlanMakerH2Test}). */
    @Test
    void aParameterTypedByItsValueIsBoundBare() {
        ExecutionPlan.Sql duck = plan(DUCKDB, "{f: Float[1]|#>{s::DB.T}#"
                + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])}", com.legend.testcases.PlanCases.Written.JSON).answerSql();
        assertEquals(List.of(new ExecutionPlan.Slot("f", new ExecutionPlan.Binding.One("DECIMAL", null))),
                duck.slots());
    }

    /** A DateTime parameter is upstream's own type, DuckDB's microsecond TIMESTAMP, whatever the value, bound as its
     *  whole text (the user's ruling, 2026-11-09: "Duckdb timestamp is fine"; until then TIMESTAMP_NS, which holds 1677
     *  to 2262 only and refused every milestoned table's 9999-12-31). */
    @Test
    void aDateTimeParameterIsCastToTimestamp() {
        assertEquals("CAST(? AS TIMESTAMP) text:null", PlanMakerTest.dateTimeCast(DUCKDB));
    }
}
