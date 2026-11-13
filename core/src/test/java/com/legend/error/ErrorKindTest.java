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
        assertEquals(Arrays.stream(com.legend.sql.RaiseKind.values()).map(Enum::name).toList(),
                Arrays.stream(ErrorKind.values()).map(Enum::name).toList());
    }
}
