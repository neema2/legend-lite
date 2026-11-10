// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.DialectCapability;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CSV file's load on Postgres (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.3, A3, option 1, the user's go on the
 * design, 2026-11-10): refused by name -- Postgres reads no file of the runner's, and the platform splits none. Until A3
 * the runner streamed the file's cells into Postgres's insert (the user's ruling of 2026-11-10, superseded by the
 * design). A native loader ({@code COPY ... FROM STDIN}) is added when someone needs it, as the database reading the
 * file. The door's refusal, naming the file, is the data delivery matrix's ({@code DataDeliveryMatrixPostgresTest}).
 */
class DataLoadsPostgresTest {

    @Test
    void postgresRefusesAFilesLoadByName() {
        DialectCapability refused = assertThrows(DialectCapability.class,
                () -> TestBackends.of(DatabaseType.Postgres).dialect().csvLoad(DataLoadsTest.LOAD));
        assertTrue(refused.getMessage().contains("reads no file"), refused.getMessage());
    }
}
