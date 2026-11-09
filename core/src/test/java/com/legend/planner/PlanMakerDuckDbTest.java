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
        // DuckDB's rows go to its bulk loader: the create, the rows, the copy and the drop
        ExecutionPlan.LoadStep.Rows rows = (ExecutionPlan.LoadStep.Rows) duck.loads().get(0).steps().get(duck.loads().get(0).steps().size() - 1);
        assertEquals(List.of(List.of("1", "a", "1.50"), java.util.Arrays.asList("2", "O'Brien", null),
                java.util.Arrays.asList("3", null, "3.25")), rows.rows());
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

    /** A DateTime parameter is DuckDB's finest timestamp, a TIMESTAMP_NS, whatever the value (D1, P4: until then the
     *  type of the value's literal, TIMESTAMP_NS only for a value finer than a microsecond). */
    @Test
    void aDateTimeParameterIsCastToTimestampNs() {
        assertEquals("CAST(? AS TIMESTAMP_NS) text:null", PlanMakerTest.dateTimeCast(DUCKDB));
    }
}
