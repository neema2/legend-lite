// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.error;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The error layer's kinds are the plan's raise kinds, by name, both ways (phase D's D1.1): exec maps one to the other
 *  by name, so a kind added to either is added to both. */
class ErrorKindTest {

    @Test
    void theErrorKindsAreThePlansRaiseKinds() {
        // (the database's own failures, DATABASE_ERROR and CONNECTION_ERROR, are no raise a statement writes: phase D's
        // Phase 2, item 4, 2026-11-16; nor a load's refused file, FILE_TOO_LARGE: J25 of the edge audit, 2026-11-17)
        assertEquals(Arrays.stream(com.legend.sql.RaiseKind.values()).map(Enum::name).toList(),
                Arrays.stream(ErrorKind.values()).filter(ErrorKind::isRaise).map(Enum::name).toList());
        for (ErrorKind k : ErrorKind.values()) {
            if (!k.isRaise()) {
                assertEquals(java.util.List.of(), Arrays.stream(com.legend.sql.RaiseKind.values())
                        .filter(r -> r.name().equals(k.name())).toList(), k.name());
            }
        }
    }
}
