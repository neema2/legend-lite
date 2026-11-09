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

    /** A DateTime parameter is a TIMESTAMP, its text cut to six digits of a second as Postgres's literal writer cuts
     *  them (probes/timestamp-results.txt; D1, P4). */
    @Test
    void aDateTimeParameterIsCastToTimestamp() {
        assertEquals("CAST(? AS TIMESTAMP) text:6", PlanMakerTest.dateTimeCast(DatabaseType.Postgres));
    }
}
