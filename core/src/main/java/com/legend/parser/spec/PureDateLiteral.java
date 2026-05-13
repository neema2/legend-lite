package com.legend.parser.spec;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Pure-language date literal in structured form, port of engine's
 * {@code meta::pure::metamodel::type::Date} hierarchy
 * (see {@code legend-pure-core/legend-pure-m4/.../primitive/date/}).
 *
 * <p>Pure's Date type admits seven literal shapes of progressively
 * increasing precision. Engine models each as a separate class
 * ({@code Year}, {@code YearMonth}, {@code StrictDate},
 * {@code DateWithHour}, {@code DateWithMinute}, {@code DateWithSecond},
 * {@code DateWithSubsecond}); we mirror that hierarchy here.
 *
 * <h2>Source grammar</h2>
 * From engine's {@code CoreFragmentGrammar.g4}:
 * <pre>{@code
 *   Date     : '%' ('-')? Digit+ ('-' Digit+ ('-' Digit+ ('T' DateTime TimeZone?)?)?)?
 *   DateTime : Digit+ (':' Digit+ (':' Digit+ ('.' Digit+)?)?)?
 *   TimeZone : 'Z' | ('+' | '-') Digit Digit Digit Digit
 * }</pre>
 *
 * <p>Examples (all legal source, all parsed by {@link #parse}):
 * <ul>
 *   <li>{@code 2024}                    &rarr; {@link Year}</li>
 *   <li>{@code 2024-01}                 &rarr; {@link YearMonth}</li>
 *   <li>{@code 2024-01-15}              &rarr; {@link StrictDate}</li>
 *   <li>{@code 2024-01-15T10}           &rarr; {@link DateWithHour}</li>
 *   <li>{@code 2024-01-15T10:30}        &rarr; {@link DateWithMinute}</li>
 *   <li>{@code 2024-01-15T10:30:45}     &rarr; {@link DateWithSecond}</li>
 *   <li>{@code 2024-01-15T10:30:45.123} &rarr; {@link DateWithSubsecond}</li>
 *   <li>{@code -44-03-15}               &rarr; {@link StrictDate} (BC year)</li>
 *   <li>{@code 2024-01-15T10:30+0500}   &rarr; {@link DateWithMinute} (TZ-normalised)</li>
 * </ul>
 *
 * <h2>Timezone handling</h2>
 * Engine normalises timezones to GMT at parse time, discarding the
 * original offset. We mirror that: a TZ suffix shifts the date/time
 * components and the resulting literal carries no TZ information. See
 * {@code DateFormat.parsePureDate} in engine for the reference
 * implementation.
 *
 * <p>Consequence: {@code %2024-01-15T10:00+0500} and
 * {@code %2024-01-15T05:00} parse to equal {@link DateWithMinute}
 * values. Round-tripping via {@link #toEngineString} produces the
 * latter form.
 *
 * <h2>Validation</h2>
 * Every record validates its components in the compact constructor.
 * Invalid combinations ({@code month = 13}, {@code day = 30} in
 * February of a non-leap year, hour {@code 24}, empty subsecond)
 * throw {@link IllegalArgumentException} at construction. The parser
 * surfaces these as parse errors.
 *
 * <h2>Subsecond representation</h2>
 * Carried as {@link String} (not numeric) to preserve arbitrary
 * precision: {@code "123"}, {@code "123456"}, and {@code "123456789"}
 * are all legal Pure values and must round-trip byte-exact. Engine
 * makes the same choice ({@code DateWithSubsecond.subsecond} is a
 * {@code String}).
 *
 * <h2>Type semantics</h2>
 * The parser commits only to the literal's structural shape, not its
 * Pure {@code Class} type. Type assignment is the type-checker's job
 * downstream and follows engine's rules:
 * <ul>
 *   <li>{@link Year}, {@link YearMonth} &rarr; {@code Date} (not {@code StrictDate})</li>
 *   <li>{@link StrictDate} &rarr; {@code StrictDate} (and {@code Date})</li>
 *   <li>{@link DateWithHour}, {@link DateWithMinute}, {@link DateWithSecond},
 *       {@link DateWithSubsecond} &rarr; {@code DateTime} (and {@code Date})</li>
 * </ul>
 */
public sealed interface PureDateLiteral
        permits PureDateLiteral.Year,
                PureDateLiteral.YearMonth,
                PureDateLiteral.StrictDate,
                PureDateLiteral.DateWithHour,
                PureDateLiteral.DateWithMinute,
                PureDateLiteral.DateWithSecond,
                PureDateLiteral.DateWithSubsecond {

    /**
     * Canonical engine-faithful spelling without the leading {@code %}
     * prefix and with timezone normalised to GMT (no TZ suffix).
     * Round-trip identity: {@code parse(x.toEngineString())} equals
     * {@code x}.
     */
    String toEngineString();

    // ---------------------------------------------------------------
    // Variants
    // ---------------------------------------------------------------

    record Year(int year) implements PureDateLiteral {
        public Year { validateYear(year); }
        @Override public String toEngineString() { return Integer.toString(year); }
    }

    record YearMonth(int year, int month) implements PureDateLiteral {
        public YearMonth { validateYear(year); validateMonth(month); }
        @Override public String toEngineString() {
            return String.format("%d-%02d", year, month);
        }
    }

    record StrictDate(int year, int month, int day) implements PureDateLiteral {
        public StrictDate {
            validateYear(year);
            validateMonth(month);
            validateDay(year, month, day);
        }
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02d", year, month, day);
        }
    }

    record DateWithHour(int year, int month, int day, int hour) implements PureDateLiteral {
        public DateWithHour {
            validateYear(year);
            validateMonth(month);
            validateDay(year, month, day);
            validateHour(hour);
        }
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d", year, month, day, hour);
        }
    }

    record DateWithMinute(int year, int month, int day, int hour, int minute) implements PureDateLiteral {
        public DateWithMinute {
            validateYear(year);
            validateMonth(month);
            validateDay(year, month, day);
            validateHour(hour);
            validateMinute(minute);
        }
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d", year, month, day, hour, minute);
        }
    }

    record DateWithSecond(int year, int month, int day, int hour, int minute, int second) implements PureDateLiteral {
        public DateWithSecond {
            validateYear(year);
            validateMonth(month);
            validateDay(year, month, day);
            validateHour(hour);
            validateMinute(minute);
            validateSecond(second);
        }
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d:%02d",
                    year, month, day, hour, minute, second);
        }
    }

    record DateWithSubsecond(int year, int month, int day,
                             int hour, int minute, int second,
                             String subsecond) implements PureDateLiteral {
        public DateWithSubsecond {
            validateYear(year);
            validateMonth(month);
            validateDay(year, month, day);
            validateHour(hour);
            validateMinute(minute);
            validateSecond(second);
            validateSubsecond(subsecond);
        }
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d:%02d.%s",
                    year, month, day, hour, minute, second, subsecond);
        }
    }

    // ---------------------------------------------------------------
    // Parse
    // ---------------------------------------------------------------

    /**
     * Parse the body of a Pure date literal (the source text after
     * the {@code %} prefix). Throws {@link IllegalArgumentException}
     * for malformed shape or invalid component values.
     *
     * <p>Mirrors engine's {@code DateFormat.parsePureDate} algorithm:
     * progressive component consumption ({@code year} &rarr; optional
     * {@code -month} &rarr; ...), with TZ normalisation to GMT for
     * time-bearing forms.
     *
     * @param source body without leading {@code %}; never null/empty
     */
    static PureDateLiteral parse(String source) {
        Objects.requireNonNull(source, "source");
        if (source.isEmpty()) {
            throw new IllegalArgumentException("empty date literal");
        }
        return new Parser(source).parse();
    }

    /**
     * Parser state machine. Held as a helper class so the cursor and
     * source string are visible to every component-parse step without
     * re-passing them. Single-shot: a fresh {@code Parser} is
     * constructed per {@link #parse} call. Hidden behind {@code parse}
     * so this is not part of the public surface.
     */
    final class Parser {
        // Implicit public-static-nested per Java interface rules; the
        // language forbids 'private' here. {@code parse(String)} is the
        // documented entry point.
        private final String src;
        private int pos;

        Parser(String src) {
            this.src = src;
            this.pos = 0;
        }

        PureDateLiteral parse() {
            // Optional leading '-' for BC years
            int yearSign = 1;
            if (pos < src.length() && src.charAt(pos) == '-') {
                yearSign = -1;
                pos++;
            }
            int year = yearSign * readDigits("year");
            if (atEnd()) {
                return new Year(year);
            }
            expect('-', "expected '-' after year");
            int month = readDigits("month");
            if (atEnd()) {
                return new YearMonth(year, month);
            }
            expect('-', "expected '-' after month");
            int day = readDigits("day");
            if (atEnd()) {
                return new StrictDate(year, month, day);
            }
            expect('T', "expected 'T' after day");
            int hour = readDigits("hour");
            if (atEnd()) {
                return new DateWithHour(year, month, day, hour);
            }
            // hour must be followed by ':' (TZ-after-hour is not legal per engine)
            expect(':', "expected ':' after hour");
            int minute = readDigits("minute");
            if (atEnd()) {
                return new DateWithMinute(year, month, day, hour, minute);
            }
            // Either a ':' (more time precision) or a TZ suffix.
            char c = src.charAt(pos);
            if (c != ':') {
                int offsetMinutes = readTimeZone();
                return shift(new DateWithMinute(year, month, day, hour, minute), offsetMinutes);
            }
            pos++; // consume ':'
            int second = readDigits("second");
            if (atEnd()) {
                return new DateWithSecond(year, month, day, hour, minute, second);
            }
            String subsecond = null;
            if (src.charAt(pos) == '.') {
                pos++; // consume '.'
                int subStart = pos;
                while (pos < src.length() && isDigit(src.charAt(pos))) pos++;
                if (pos == subStart) {
                    throw new IllegalArgumentException(
                            "expected digits after '.' in subsecond at position " + subStart);
                }
                subsecond = src.substring(subStart, pos);
            }
            if (atEnd()) {
                return subsecond == null
                        ? new DateWithSecond(year, month, day, hour, minute, second)
                        : new DateWithSubsecond(year, month, day, hour, minute, second, subsecond);
            }
            int offsetMinutes = readTimeZone();
            PureDateLiteral base = subsecond == null
                    ? new DateWithSecond(year, month, day, hour, minute, second)
                    : new DateWithSubsecond(year, month, day, hour, minute, second, subsecond);
            return shift(base, offsetMinutes);
        }

        private boolean atEnd() { return pos >= src.length(); }

        private void expect(char c, String message) {
            if (atEnd() || src.charAt(pos) != c) {
                throw new IllegalArgumentException(
                        message + " at position " + pos + " in '" + src + "'");
            }
            pos++;
        }

        /**
         * Read a run of digits, returning their integer value. Used
         * for year/month/day/hour/minute/second components. Throws if
         * no digits are present or the number overflows int.
         */
        private int readDigits(String component) {
            int start = pos;
            while (pos < src.length() && isDigit(src.charAt(pos))) pos++;
            if (pos == start) {
                throw new IllegalArgumentException(
                        "expected digits for " + component + " at position " + start
                                + " in '" + src + "'");
            }
            try {
                return Integer.parseInt(src.substring(start, pos));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        component + " value out of int range: '"
                                + src.substring(start, pos) + "'", e);
            }
        }

        /**
         * Read a {@code Z} or {@code (+|-)HHMM} timezone suffix and
         * return its offset in minutes (positive east of GMT).
         * Cursor must be at the start of the suffix; consumes to end.
         */
        private int readTimeZone() {
            if (atEnd()) {
                throw new IllegalArgumentException("expected timezone, got end of input");
            }
            char c = src.charAt(pos);
            if (c == 'Z') {
                pos++;
                if (!atEnd()) {
                    throw new IllegalArgumentException(
                            "unexpected trailing text after 'Z': '"
                                    + src.substring(pos) + "'");
                }
                return 0;
            }
            if (c != '+' && c != '-') {
                throw new IllegalArgumentException(
                        "expected '+' or '-' or 'Z' for timezone at position "
                                + pos + " in '" + src + "'");
            }
            int sign = (c == '+') ? 1 : -1;
            pos++;
            if (src.length() - pos != 4) {
                throw new IllegalArgumentException(
                        "timezone offset must be exactly 4 digits (HHMM) in '"
                                + src + "'");
            }
            for (int i = pos; i < src.length(); i++) {
                if (!isDigit(src.charAt(i))) {
                    throw new IllegalArgumentException(
                            "non-digit in timezone offset: '"
                                    + src.substring(pos) + "'");
                }
            }
            int hh = Integer.parseInt(src.substring(pos, pos + 2));
            int mm = Integer.parseInt(src.substring(pos + 2, pos + 4));
            if (hh > 23 || mm > 59) {
                throw new IllegalArgumentException(
                        "invalid timezone offset HHMM values in '" + src + "'");
            }
            pos += 4;
            return sign * (hh * 60 + mm);
        }
    }

    /**
     * Shift a minute-bearing date literal by a TZ offset to normalise
     * to GMT, returning a new literal of the same kind. Engine drops
     * the offset and stores the shifted components.
     *
     * <p>Uses {@link LocalDateTime} for arithmetic (handles
     * cross-day, cross-month, cross-year boundaries correctly) but
     * never escapes; only the (year, month, day, hour, minute)
     * components are read back out. Second and subsecond are not
     * affected by minute-granularity offsets.
     *
     * <p>Exhaustive over the sealed hierarchy: the four non-minute
     * variants ({@link Year}, {@link YearMonth}, {@link StrictDate},
     * {@link DateWithHour}) throw because the grammar forbids a TZ
     * suffix on those shapes, but they appear here as defensive
     * arms enforcing the invariant at the type-system level.
     */
    private static PureDateLiteral shift(PureDateLiteral lit, int offsetMinutes) {
        if (offsetMinutes == 0) {
            return lit;
        }
        return switch (lit) {
            case DateWithMinute(int y, int mo, int d, int h, int mi) -> {
                LocalDateTime s = LocalDateTime.of(y, mo, d, h, mi).minusMinutes(offsetMinutes);
                yield new DateWithMinute(s.getYear(), s.getMonthValue(),
                        s.getDayOfMonth(), s.getHour(), s.getMinute());
            }
            case DateWithSecond(int y, int mo, int d, int h, int mi, int sec) -> {
                LocalDateTime s = LocalDateTime.of(y, mo, d, h, mi).minusMinutes(offsetMinutes);
                yield new DateWithSecond(s.getYear(), s.getMonthValue(),
                        s.getDayOfMonth(), s.getHour(), s.getMinute(), sec);
            }
            case DateWithSubsecond(int y, int mo, int d, int h, int mi, int sec, String sub) -> {
                LocalDateTime s = LocalDateTime.of(y, mo, d, h, mi).minusMinutes(offsetMinutes);
                yield new DateWithSubsecond(s.getYear(), s.getMonthValue(),
                        s.getDayOfMonth(), s.getHour(), s.getMinute(), sec, sub);
            }
            case Year ignored ->
                    throw new IllegalStateException("timezone on Year is unreachable (grammar forbids)");
            case YearMonth ignored ->
                    throw new IllegalStateException("timezone on YearMonth is unreachable (grammar forbids)");
            case StrictDate ignored ->
                    throw new IllegalStateException("timezone on StrictDate is unreachable (grammar forbids)");
            case DateWithHour ignored ->
                    throw new IllegalStateException("timezone on DateWithHour is unreachable (grammar forbids)");
        };
    }

    // ---------------------------------------------------------------
    // Validation helpers
    // ---------------------------------------------------------------

    private static boolean isDigit(char c) { return c >= '0' && c <= '9'; }

    private static void validateYear(int year) {
        // Mirror java.time.Year range used by engine. The lexer
        // already bounds source to plausible widths so this rarely
        // fires in practice; kept defensively for direct constructor
        // callers.
        if (year < java.time.Year.MIN_VALUE || year > java.time.Year.MAX_VALUE) {
            throw new IllegalArgumentException("invalid year: " + year);
        }
    }

    private static void validateMonth(int month) {
        if (month < 1 || month > 12) {
            throw new IllegalArgumentException("invalid month: " + month);
        }
    }

    private static void validateDay(int year, int month, int day) {
        if (day < 1 || day > daysInMonth(year, month)) {
            throw new IllegalArgumentException(
                    "invalid day for " + year + "-" + month + ": " + day);
        }
    }

    private static void validateHour(int hour) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException("invalid hour: " + hour);
        }
    }

    private static void validateMinute(int minute) {
        if (minute < 0 || minute > 59) {
            throw new IllegalArgumentException("invalid minute: " + minute);
        }
    }

    private static void validateSecond(int second) {
        if (second < 0 || second > 59) {
            throw new IllegalArgumentException("invalid second: " + second);
        }
    }

    private static void validateSubsecond(String subsecond) {
        if (subsecond == null || subsecond.isEmpty()) {
            throw new IllegalArgumentException("subsecond cannot be null or empty");
        }
        for (int i = 0; i < subsecond.length(); i++) {
            char c = subsecond.charAt(i);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException(
                        "invalid subsecond (non-digit): '" + subsecond + "'");
            }
        }
    }

    /** Days in the given month, accounting for leap years. */
    private static int daysInMonth(int year, int month) {
        return switch (month) {
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            case 4, 6, 9, 11 -> 30;
            case 2 -> isLeapYear(year) ? 29 : 28;
            default -> throw new IllegalArgumentException("invalid month: " + month);
        };
    }

    /** Gregorian leap year rule. */
    private static boolean isLeapYear(int year) {
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }
}
