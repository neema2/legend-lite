// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A date's spellings written by hand (the payoff measurement, 2026-11-01: the formatter's parse of its pattern cost a
 * fifth of a JSON or CSV writer's time) are the formatter's, character for character: every precision, a year of one
 * to five digits and a negative one, every field at its edges, every subsecond length.
 */
class PureDateSpellingTest {

    private static final List<Integer> YEARS = List.of(-44, 0, 1, 44, 999, 1999, 2024, 9999, 10000, 275760);
    private static final List<Integer> FIELDS = List.of(0, 1, 9, 10, 12, 23, 31, 59);
    private static final List<String> SUBSECONDS = List.of("0", "1", "05", "123", "123456", "000000001", "999999999",
            "1234567891");

    @Test
    void theLiteralBodiesAreTheFormattersAtEveryPrecision() {
        for (int y : YEARS) {
            for (int f : FIELDS) {
                int month = Math.max(1, Math.min(12, f));
                int day = Math.max(1, Math.min(28, f));
                int hour = Math.min(23, f);
                assertEquals(String.format(Locale.ROOT, "%d-%02d", y, month),
                        new PureDateLiteral.YearMonth(y, month).toEngineString());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02d", y, month, day),
                        new PureDateLiteral.StrictDate(y, month, day).toEngineString());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d", y, month, day, hour),
                        new PureDateLiteral.DateWithHour(y, month, day, hour).toEngineString());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d", y, month, day, hour, f),
                        new PureDateLiteral.DateWithMinute(y, month, day, hour, f).toEngineString());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d:%02d", y, month, day, hour, f, f),
                        new PureDateLiteral.DateWithSecond(y, month, day, hour, f, f).toEngineString());
                for (String s : SUBSECONDS) {
                    PureDateLiteral.DateWithSubsecond ss =
                            new PureDateLiteral.DateWithSubsecond(y, month, day, hour, f, f, s);
                    assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d:%02d.%s", y, month, day, hour, f,
                            f, s), ss.toEngineString());
                    assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d:%02d.%s+0000", y, month, day,
                            hour, f, f, (s + "000000000").substring(0, 9)), ss.toEngineJson());
                }
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:00:00.000000000+0000", y, month, day,
                        hour), new PureDateLiteral.DateWithHour(y, month, day, hour).toEngineJson());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d:00.000000000+0000", y, month, day,
                        hour, f), new PureDateLiteral.DateWithMinute(y, month, day, hour, f).toEngineJson());
                assertEquals(String.format(Locale.ROOT, "%d-%02d-%02dT%02d:%02d:%02d.000000000+0000", y, month, day,
                        hour, f, f), new PureDateLiteral.DateWithSecond(y, month, day, hour, f, f).toEngineJson());
            }
        }
    }

    @Test
    void aTimestampsNineDigitsAreTheFormatters() {
        for (int nanos : List.of(0, 1, 7, 10, 123_000_000, 123_456_000, 999_999_999, 500_000_001)) {
            LocalDateTime t = LocalDateTime.of(2024, 1, 2, 3, 4, 5, nanos);
            PureDateLiteral read = PureDateLiteral.fromDatabaseTimestamp(t);
            assertEquals(String.format(Locale.ROOT, "2024-01-02T03:04:05.%09d", nanos), read.toEngineString());
            PureDateLiteral canonical = PureDateLiteral.fromLocalDateTime(t);
            assertEquals(nanos == 0 ? "2024-01-02T03:04:05"
                    : "2024-01-02T03:04:05." + String.format(Locale.ROOT, "%09d", nanos).replaceFirst("0+$", ""),
                    canonical.toEngineString());
        }
    }
}
