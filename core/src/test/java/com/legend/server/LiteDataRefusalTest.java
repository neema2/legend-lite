// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.error.DataError;
import com.legend.error.ErrorKind;
import org.junit.jupiter.api.Test;

/**
 * A RUN'S FAILURE AS LITE'S ONE ERROR SHAPE (lite's API v1 spec §3; PR #118 fix round, 2026-11-18, N3 -- the PR #118
 * audit's PRODUCT-9, the kinds ruled by the user 2026-11-18, "N3 okay"): {@code raise} is a raise's kind alone, never
 * the database's own failure's or a load's refused file's, and a raise's {@code where} is its site's; a connection that
 * could not open or was lost is {@code internal}. Read by the one classifier every binding reads
 * ({@link LiteV1Api#refusal}).
 */
class LiteDataRefusalTest {

    private static final RuntimeException CAUSE = new RuntimeException("the driver's");

    @Test
    void aRaiseIsDataItsKindInRaiseAndItsSitesWhereInWhere() {
        assertEquals("{\"kind\":\"data\",\"message\":\"collection size does not match the multiplicity\","
                        + "\"where\":\"[1]\",\"raise\":\"sizeNotMultiplicity\"}",
                LiteV1Api.refusal(new DataError(ErrorKind.SIZE_NOT_MULTIPLICITY, "[1]",
                        ErrorKind.SIZE_NOT_MULTIPLICITY.sentence(), CAUSE)).json());
        assertEquals("{\"kind\":\"data\",\"message\":\"division by zero\",\"raise\":\"divideByZero\"}",
                LiteV1Api.refusal(new DataError(ErrorKind.DIVIDE_BY_ZERO, CAUSE)).json());
    }

    @Test
    void theDatabasesOwnRefusalIsDataWithNoRaise() {
        assertEquals("{\"kind\":\"data\",\"message\":\"Conversion Error: abc\"}", LiteV1Api.refusal(
                DataError.database(ErrorKind.DATABASE_ERROR, "22018", "Conversion Error: abc", "Conversion Error: abc",
                        CAUSE)).json());
        assertEquals(422, LiteV1Api.refusal(DataError.database(ErrorKind.DATABASE_ERROR, null, "x", "x", CAUSE))
                .answer().status());
    }

    @Test
    void aLostConnectionIsInternalNotTheCallersData() {
        LiteV1Api.Refusal r = LiteV1Api.refusal(DataError.database(ErrorKind.CONNECTION_ERROR, "08006",
                "Connection refused", "Connection refused", CAUSE));
        assertEquals("{\"kind\":\"internal\",\"message\":\"Connection refused\"}", r.json());
        assertEquals(500, r.answer().status());
    }

    @Test
    void aLoadsRefusedFileIsDataItsPathInTheMessageNotWhere() {
        assertEquals("{\"kind\":\"data\",\"message\":\"the file is larger than the load admits: /data/big.csv\"}",
                LiteV1Api.refusal(DataError.fileTooLarge("/data/big.csv", 1048577, 1048577, 1, CAUSE)).json());
    }

    @Test
    void aFailureOfNoKindIsDataWithNoRaise() {
        assertEquals("{\"kind\":\"data\",\"message\":\"the plan's data 'd' was refused\"}",
                LiteV1Api.refusal(new DataError("the plan's data 'd' was refused", CAUSE)).json());
    }
}
