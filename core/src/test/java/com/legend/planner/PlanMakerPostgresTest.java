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

    /** A DateTime parameter is a TIMESTAMP of six digits of a second, a value of a finer digit raised by the statement,
     *  as its literal is refused at planning (S181, A7, 2026-11-10: until then the runner cut the text to six, as the
     *  literal writer cut the literal's; D1, P4). */
    @Test
    void aDateTimeParameterIsCastToTimestamp() {
        assertEquals("CAST(CASE WHEN LENGTH(CAST(? AS VARCHAR)) > 19 AND SUBSTRING(CAST(? AS VARCHAR), LENGTH(CAST(? AS VARCHAR)) - 2) <> '000' THEN … WHEN LENGTH(CAST(? AS VARCHAR)) > 19 THEN SUBSTRING(CAST(? AS VARCHAR), 1, LENGTH(CAST(? AS VARCHAR)) - 3) ELSE CAST(? AS VARCHAR) END AS TIMESTAMP) text", PlanMakerTest.dateTimeCast(DatabaseType.Postgres));
    }
}
