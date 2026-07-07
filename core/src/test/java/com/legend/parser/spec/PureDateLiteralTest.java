package com.legend.parser.spec;

import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PureDateLiteral}. Covers every variant, the
 * full grammar accepted by {@link PureDateLiteral#parse}, component
 * validation (year/month/day/hour/minute/second/subsecond), timezone
 * normalisation to GMT, and {@code toEngineString} round-tripping.
 *
 * <p>Test names follow the pattern
 * {@code <variant>_<aspect>_<expected>}, e.g.
 * {@code strictDate_invalidMonth_throws}.
 */
class PureDateLiteralTest {

    // ----- variant parse: positive cases -------------------------------

    @Test
    void year_parsesToYear() {
        assertEquals(new PureDateLiteral.Year(2024),
                PureDateLiteral.parse("2024"));
    }

    @Test
    void yearMonth_parsesToYearMonth() {
        assertEquals(new PureDateLiteral.YearMonth(2024, 1),
                PureDateLiteral.parse("2024-01"));
    }

    @Test
    void strictDate_parsesToStrictDate() {
        assertEquals(new PureDateLiteral.StrictDate(2024, 1, 15),
                PureDateLiteral.parse("2024-01-15"));
    }

    @Test
    void dateWithHour_parsesToDateWithHour() {
        assertEquals(new PureDateLiteral.DateWithHour(2024, 1, 15, 10),
                PureDateLiteral.parse("2024-01-15T10"));
    }

    @Test
    void dateWithMinute_parsesToDateWithMinute() {
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 10, 30),
                PureDateLiteral.parse("2024-01-15T10:30"));
    }

    @Test
    void dateWithSecond_parsesToDateWithSecond() {
        assertEquals(new PureDateLiteral.DateWithSecond(2024, 1, 15, 10, 30, 45),
                PureDateLiteral.parse("2024-01-15T10:30:45"));
    }

    @Test
    void dateWithSubsecond_parsesToDateWithSubsecond() {
        assertEquals(new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, "123"),
                PureDateLiteral.parse("2024-01-15T10:30:45.123"));
    }

    @Test
    void dateWithSubsecond_arbitraryPrecisionPreserved() {
        // "123", "123456", "123456789" are all legal Pure subsecond
        // strings and must round-trip byte-exact (engine fidelity).
        assertEquals(new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, "123456789"),
                PureDateLiteral.parse("2024-01-15T10:30:45.123456789"));
    }

    @Test
    void negativeYear_parsesAsBcDate() {
        // %-44-03-15 is the Ides of March, 44 BC. Engine grammar permits
        // a leading '-' on the year.
        assertEquals(new PureDateLiteral.StrictDate(-44, 3, 15),
                PureDateLiteral.parse("-44-03-15"));
    }

    @Test
    void negativeYear_yearOnly() {
        assertEquals(new PureDateLiteral.Year(-44),
                PureDateLiteral.parse("-44"));
    }

    // ----- timezone normalisation --------------------------------------

    @Test
    void timezone_zPrefixIsGmt() {
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 10, 30),
                PureDateLiteral.parse("2024-01-15T10:30Z"));
    }

    @Test
    void timezone_gmtOffsetNoShift() {
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 10, 30),
                PureDateLiteral.parse("2024-01-15T10:30+0000"));
    }

    @Test
    void timezone_positiveOffsetShiftsBackwards() {
        // +0500 means "this time is 5h ahead of GMT"; shift -5h to land
        // in GMT.
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 5, 0),
                PureDateLiteral.parse("2024-01-15T10:00+0500"));
    }

    @Test
    void timezone_negativeOffsetShiftsForwards() {
        // -0500 means "this time is 5h behind GMT"; shift +5h to land
        // in GMT.
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 15, 0),
                PureDateLiteral.parse("2024-01-15T10:00-0500"));
    }

    @Test
    void timezone_shiftCrossesDayBoundary() {
        // 23:00+0500 -> 18:00 same day.
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 15, 18, 0),
                PureDateLiteral.parse("2024-01-15T23:00+0500"));
        // 23:00-0500 -> 04:00 next day.
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 1, 16, 4, 0),
                PureDateLiteral.parse("2024-01-15T23:00-0500"));
    }

    @Test
    void timezone_shiftCrossesMonthBoundary() {
        // Jan 31 23:00-0500 -> Feb 1 04:00.
        assertEquals(new PureDateLiteral.DateWithMinute(2024, 2, 1, 4, 0),
                PureDateLiteral.parse("2024-01-31T23:00-0500"));
    }

    @Test
    void timezone_shiftCrossesYearBoundary() {
        // Dec 31 23:00-0500 -> Jan 1 next year 04:00.
        assertEquals(new PureDateLiteral.DateWithMinute(2025, 1, 1, 4, 0),
                PureDateLiteral.parse("2024-12-31T23:00-0500"));
    }

    @Test
    void timezone_preservesSecondAndSubsecond() {
        // Minute-granularity offset doesn't disturb sub-minute fields.
        assertEquals(new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 5, 30, 45, "999"),
                PureDateLiteral.parse("2024-01-15T10:30:45.999+0500"));
    }

    @Test
    void timezone_onlyOnMinuteBearingForms() {
        // Engine grammar puts TZ after DateTime, but parse path
        // requires at least ':minute' before TZ is admissible. After
        // bare hour, the next char must be ':'.
        assertParseError("2024-01-15T10+0500", "':'");
    }

    // ----- component validation ----------------------------------------

    @Test
    void invalid_month_13_throws() {
        assertParseError("2024-13-15", "month");
    }

    @Test
    void invalid_month_0_throws() {
        assertParseError("2024-00-15", "month");
    }

    @Test
    void invalid_day_32_throws() {
        assertParseError("2024-01-32", "day");
    }

    @Test
    void invalid_day_zero_throws() {
        assertParseError("2024-01-00", "day");
    }

    @Test
    void invalid_feb29_nonLeapYear_throws() {
        // 2023 is not a leap year; Feb 29 invalid.
        assertParseError("2023-02-29", "day");
    }

    @Test
    void valid_feb29_leapYear() {
        // 2024 is a leap year.
        assertEquals(new PureDateLiteral.StrictDate(2024, 2, 29),
                PureDateLiteral.parse("2024-02-29"));
    }

    @Test
    void invalid_feb29_centuryNonLeap_throws() {
        // 1900 was NOT a leap year (divisible by 100 but not 400).
        assertParseError("1900-02-29", "day");
    }

    @Test
    void valid_feb29_quadCenturyLeap() {
        // 2000 WAS a leap year (divisible by 400).
        assertEquals(new PureDateLiteral.StrictDate(2000, 2, 29),
                PureDateLiteral.parse("2000-02-29"));
    }

    @Test
    void invalid_april31_throws() {
        // April has 30 days.
        assertParseError("2024-04-31", "day");
    }

    @Test
    void invalid_hour_24_throws() {
        assertParseError("2024-01-15T24:00", "hour");
    }

    @Test
    void invalid_minute_60_throws() {
        assertParseError("2024-01-15T10:60", "minute");
    }

    @Test
    void invalid_second_60_throws() {
        // Pure does not model leap seconds; 60 is invalid.
        assertParseError("2024-01-15T10:30:60", "second");
    }

    @Test
    void invalid_emptySubsecond_throws() {
        // The grammar requires Digit+ after '.', so the parser surfaces
        // this as missing digits.
        assertParseError("2024-01-15T10:30:45.", "subsecond");
    }

    @Test
    void invalid_tzNotFourDigits_throws() {
        assertParseError("2024-01-15T10:30+05", "timezone");
    }

    @Test
    void invalid_emptyInput_throws() {
        assertParseError("", "empty");
    }

    @Test
    void invalid_trailingTextAfterZ_throws() {
        assertParseError("2024-01-15T10:30Zfoo", "trailing");
    }

    // ----- accepted shape variations -----------------------------------

    @Test
    void loose_padding_singleDigitMonth_accepted() {
        // Engine grammar is 'Digit+', not exactly two digits;
        // %2024-1-5 is legal source. Pin the relaxed acceptance.
        assertEquals(new PureDateLiteral.StrictDate(2024, 1, 5),
                PureDateLiteral.parse("2024-1-5"));
    }

    @Test
    void loose_padding_leadingZerosInYear_normalised() {
        // %0024 parses as Year(24); toEngineString emits '24' (no
        // leading zeros). Round-trip is not byte-exact for over-padded
        // sources; this pins the normalising behaviour.
        PureDateLiteral parsed = PureDateLiteral.parse("0024");
        assertEquals(new PureDateLiteral.Year(24), parsed);
        assertEquals("24", parsed.toEngineString());
    }

    @Test
    void subsecond_leadingZerosPreservedByteExact() {
        // "001" is semantically different from "01" or "1" in Pure's
        // arbitrary-precision subsecond model. Must round-trip exact.
        PureDateLiteral parsed = PureDateLiteral.parse("2024-01-15T10:30:45.001");
        assertEquals(new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, "001"),
                parsed);
        assertEquals("2024-01-15T10:30:45.001", parsed.toEngineString());
    }

    @Test
    void negativeYear_toEngineStringEmitsMinusPrefix() {
        assertEquals("-44", new PureDateLiteral.Year(-44).toEngineString());
        assertEquals("-44-03-15",
                new PureDateLiteral.StrictDate(-44, 3, 15).toEngineString());
    }

    // ----- constructor-level validation --------------------------------

    @Test
    void strictDate_constructor_validatesMonth() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureDateLiteral.StrictDate(2024, 13, 1));
        assertTrue(ex.getMessage().contains("month"), ex.getMessage());
    }

    @Test
    void strictDate_constructor_validatesDayForMonth() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureDateLiteral.StrictDate(2024, 4, 31));
        assertTrue(ex.getMessage().contains("day"), ex.getMessage());
    }

    @Test
    void dateWithSubsecond_constructor_rejectsNullSubsecond() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, null));
        assertTrue(ex.getMessage().contains("subsecond"), ex.getMessage());
    }

    @Test
    void dateWithSubsecond_constructor_rejectsEmptySubsecond() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, ""));
        assertTrue(ex.getMessage().contains("subsecond"), ex.getMessage());
    }

    @Test
    void dateWithSubsecond_constructor_rejectsNonDigitSubsecond() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 45, "12a"));
        assertTrue(ex.getMessage().contains("subsecond"), ex.getMessage());
    }

    // ----- toEngineString round-trip -----------------------------------

    @Test
    void roundTrip_year() {
        roundTrip("2024");
    }

    @Test
    void roundTrip_yearMonth() {
        roundTrip("2024-01");
    }

    @Test
    void roundTrip_strictDate() {
        roundTrip("2024-01-15");
    }

    @Test
    void roundTrip_dateWithHour() {
        roundTrip("2024-01-15T10");
    }

    @Test
    void roundTrip_dateWithMinute() {
        roundTrip("2024-01-15T10:30");
    }

    @Test
    void roundTrip_dateWithSecond() {
        roundTrip("2024-01-15T10:30:45");
    }

    @Test
    void roundTrip_dateWithSubsecond() {
        roundTrip("2024-01-15T10:30:45.123456789");
    }

    @Test
    void roundTrip_dateWithMinuteWithGmtTimezone_normalisesAway() {
        // GMT TZ is normalised away; the round-trip therefore yields
        // the TZ-stripped form.
        PureDateLiteral parsed = PureDateLiteral.parse("2024-01-15T10:30+0000");
        assertEquals("2024-01-15T10:30", parsed.toEngineString());
    }

    @Test
    void roundTrip_dateWithMinuteWithEasternTimezone_normalisesToGmt() {
        // +0500 shifts -5h to GMT; round-trip yields the shifted form.
        PureDateLiteral parsed = PureDateLiteral.parse("2024-01-15T10:30+0500");
        assertEquals("2024-01-15T05:30", parsed.toEngineString());
    }

    /**
     * Helper: assert that the canonical engine spelling parses back to
     * an equal {@link PureDateLiteral}. Used for forms that don't
     * involve TZ normalisation (where round-trip is identity).
     */
    private static void roundTrip(String source) {
        PureDateLiteral parsed = PureDateLiteral.parse(source);
        assertEquals(source, parsed.toEngineString());
        assertEquals(parsed, PureDateLiteral.parse(parsed.toEngineString()));
    }

    /**
     * Helper: assert {@link PureDateLiteral#parse} rejects {@code source}
     * with an {@link IllegalArgumentException} whose message contains
     * {@code expectedSubstring}. Tightens vs bare {@code assertThrows}
     * by pinning <em>which</em> validation fired &mdash; so a
     * regression that changes "invalid month" to "invalid year" fails
     * the test instead of silently passing.
     */
    private static void assertParseError(String source, String expectedSubstring) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> PureDateLiteral.parse(source));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains(expectedSubstring),
                "expected message to contain '" + expectedSubstring
                        + "' for source '" + source + "', got: " + ex.getMessage());
    }
}
