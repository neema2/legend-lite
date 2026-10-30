// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.legend.PlanMakerTest.plan;
import static com.legend.PlanMakerTest.target;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What DuckDB's plans say ({@link PlanMakerTest}'s cases, DuckDB's own answers; moved here from it by the dialect
 * program's Phase 6, 2026-10-16): its session, its bulk-loaded rows and the parameters it binds bare.
 */
class PlanMakerDuckDbTest {

    private static final DatabaseType DUCKDB = DatabaseType.DuckDB;

    @Test
    void theTargetIsWhole_itsDatabaseVersionsSessionAndSetup() {
        ExecutionPlan.Target duck = target(plan(DUCKDB, "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])"
                + "->sort(~ID->ascending())", com.legend.testcases.PlanCases.Written.JSON));
        assertEquals(new ExecutionPlan.Servers.Every(), duck.servers());
        assertEquals(List.of("SET TimeZone='UTC'"), duck.session());
        // DuckDB's rows go to its bulk loader: the create, the rows, the copy and the drop
        ExecutionPlan.SetupStep.Rows rows = (ExecutionPlan.SetupStep.Rows) duck.setup().get(duck.setup().size() - 1);
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

    /** A DateTime parameter is cast to the type of its value's literal: DuckDB's is a TIMESTAMP_NS only when the value
     *  has digits finer than a microsecond (probes/timestamp-results.txt). */
    @Test
    void aDateTimeParameterIsCastToItsLiteralsType() {
        assertEquals(java.util.Map.of(
                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                        ExecutionPlan.Digits.NONE),
                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP_NS",
                        ExecutionPlan.Digits.NONE)), PlanMakerTest.dateTimeBinding(DUCKDB).hole().types());
    }
}
