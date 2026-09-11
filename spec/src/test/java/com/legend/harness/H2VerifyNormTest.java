// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The referee's cell normalizer on timestamp SPELLINGS (Phase 0.5; audit
 * referee.md §3): the two forms it exists to equate — a bare second and a
 * {@code .0} fraction — must normalize to the SAME string, and a
 * fraction-less spelling must never lose its seconds (the old
 * {@code \\.?0+$} turned {@code 00:00:10} into {@code 00:00:1}).
 */
class H2VerifyNormTest {

    @Test
    void bareSecondsAndZeroFractionAgree() {
        assertEquals("2015-08-26 00:00:00", H2Verify.norm("2015-08-26 00:00:00.0"));
        assertEquals("2015-08-26 00:00:00", H2Verify.norm("2015-08-26 00:00:00"));
        assertEquals("2015-08-26 00:00:10", H2Verify.norm("2015-08-26 00:00:10"));
        assertEquals("2015-08-26 00:00:10", H2Verify.norm("2015-08-26 00:00:10.000"));
    }

    @Test
    void significantFractionKept() {
        assertEquals("2015-08-26 00:00:10.5", H2Verify.norm("2015-08-26 00:00:10.500"));
        assertEquals("2015-08-26 00:00:10.123", H2Verify.norm("2015-08-26 00:00:10.123"));
    }
}
