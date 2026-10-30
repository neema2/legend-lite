// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What Postgres's plans say, made without a server ({@link PlanMakerTest}'s cases; moved here from it by the dialect
 * program's Phase 6, 2026-10-16). A plan run on Postgres is {@code PostgresArmTest}'s (it needs the embedded Postgres).
 */
class PlanMakerPostgresTest {

    /** A DateTime parameter is cast to the type of its value's literal: Postgres's keeps six digits and cuts the rest,
     *  as its literal writer does (probes/timestamp-results.txt). */
    @Test
    void aDateTimeParameterIsCastToItsLiteralsType() {
        ExecutionPlan.Binding.One one = PlanMakerTest.dateTimeBinding(DatabaseType.Postgres);
        assertEquals(java.util.Map.of(
                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                        ExecutionPlan.Digits.NONE),
                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                        ExecutionPlan.Digits.NONE, 6)), one.hole().types());
        assertEquals(ExecutionPlan.ValueKind.DATE_TIME, one.hole().absent());
        assertEquals("TIMESTAMP", one.nullType());
    }
}
