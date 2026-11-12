// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.testing.KnownDefect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Stage reading A03 defect 8: with component validation off (the LEGEND_ENGINE dialect), a date literal with a
 *  non-zero timezone offset and an out-of-range component crashes in PureDateLiteral.shift's LocalDateTime.of
 *  (PureDateLiteral.java:585) with a raw java.time.DateTimeException that SpecParser (catching only
 *  IllegalArgumentException, SpecParser.java:941) lets escape without a position. */
class OffsetDateLiteralKnownDefectTest {

    @Test
    @KnownDefect(owner = "W1.2", reason = "an unvalidated date literal with a timezone offset escapes the parser as a"
            + " raw DateTimeException instead of parsing (validation deferred) or refusing with a position",
            expected = java.time.DateTimeException.class)
    void offsetDateWithUnvalidatedDayParsesOrRefusesWithPosition() {
        ValueSpecification v;
        try {
            v = SpecParser.parse("%2024-02-30T10:00+0500", GrammarLevel.LEGEND_ENGINE);
        } catch (ParseException refused) {
            assertEquals(1, refused.line(), "a refusal must be positioned");
            assertEquals(1, refused.column(), "a refusal must be positioned at the literal");
            return;
        }
        assertInstanceOf(CDate.class, v, "validation deferred: the literal parses like its +0000 twin");
    }
}
