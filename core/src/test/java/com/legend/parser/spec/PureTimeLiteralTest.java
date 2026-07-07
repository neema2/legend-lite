package com.legend.parser.spec;

import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PureTimeLiteral}: parse, validation, round-trip. */
class PureTimeLiteralTest {

    @Test
    void timeWithMinute_parses() {
        assertEquals(new PureTimeLiteral.TimeWithMinute(10, 30),
                PureTimeLiteral.parse("10:30"));
    }

    @Test
    void timeWithSecond_parses() {
        assertEquals(new PureTimeLiteral.TimeWithSecond(10, 30, 45),
                PureTimeLiteral.parse("10:30:45"));
    }

    @Test
    void timeWithSubsecond_parses() {
        assertEquals(new PureTimeLiteral.TimeWithSubsecond(10, 30, 45, "123456789"),
                PureTimeLiteral.parse("10:30:45.123456789"));
    }

    @Test
    void midnight_parses() {
        assertEquals(new PureTimeLiteral.TimeWithSecond(0, 0, 0),
                PureTimeLiteral.parse("00:00:00"));
    }

    @Test
    void endOfDay_parses() {
        assertEquals(new PureTimeLiteral.TimeWithSecond(23, 59, 59),
                PureTimeLiteral.parse("23:59:59"));
    }

    // ----- validation --------------------------------------------------

    @Test
    void invalid_hour_24_throws() {
        assertParseError("24:00", "hour");
    }

    @Test
    void invalid_minute_60_throws() {
        assertParseError("10:60", "minute");
    }

    @Test
    void invalid_second_60_throws() {
        assertParseError("10:30:60", "second");
    }

    @Test
    void invalid_emptySubsecond_throws() {
        assertParseError("10:30:45.", "subsecond");
    }

    @Test
    void invalid_missingMinute_throws() {
        // Strict-time always requires at least minute precision; bare
        // hour is not reachable in this grammar (lexer routes elsewhere).
        assertParseError("10", "':'");
    }

    @Test
    void invalid_emptyInput_throws() {
        assertParseError("", "empty");
    }

    @Test
    void subsecond_leadingZerosPreservedByteExact() {
        PureTimeLiteral parsed = PureTimeLiteral.parse("10:30:45.001");
        assertEquals(new PureTimeLiteral.TimeWithSubsecond(10, 30, 45, "001"), parsed);
        assertEquals("10:30:45.001", parsed.toEngineString());
    }

    @Test
    void timeWithSubsecond_constructor_rejectsEmptySubsecond() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureTimeLiteral.TimeWithSubsecond(10, 30, 45, ""));
        assertTrue(ex.getMessage().contains("subsecond"), ex.getMessage());
    }

    @Test
    void timeWithSubsecond_constructor_rejectsNonDigitSubsecond() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PureTimeLiteral.TimeWithSubsecond(10, 30, 45, "12a"));
        assertTrue(ex.getMessage().contains("subsecond"), ex.getMessage());
    }

    // ----- round-trip --------------------------------------------------

    @Test
    void roundTrip_timeWithMinute() {
        roundTrip("10:30");
    }

    @Test
    void roundTrip_timeWithSecond() {
        roundTrip("10:30:45");
    }

    @Test
    void roundTrip_timeWithSubsecond() {
        roundTrip("10:30:45.123");
    }

    private static void roundTrip(String source) {
        PureTimeLiteral parsed = PureTimeLiteral.parse(source);
        assertEquals(source, parsed.toEngineString());
        assertEquals(parsed, PureTimeLiteral.parse(parsed.toEngineString()));
    }

    /**
     * Helper: assert parse rejects {@code source} with an
     * {@link IllegalArgumentException} whose message contains
     * {@code expectedSubstring}. Pins <em>which</em> validation fired.
     */
    private static void assertParseError(String source, String expectedSubstring) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> PureTimeLiteral.parse(source));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains(expectedSubstring),
                "expected message to contain '" + expectedSubstring
                        + "' for source '" + source + "', got: " + ex.getMessage());
    }
}
