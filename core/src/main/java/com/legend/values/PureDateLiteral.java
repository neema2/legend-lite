package com.legend.values;

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

    /** THE ENGINE'S JSON SPELLING of a date value (its ServiceTestRunner /
     * PURE_TDSOBJECT value transformer, witnessed 2026-09-16 on the stress
     * corpus): a date-only value prints its literal body; a time-bearing
     * value prints seconds, NINE fractional digits and the GMT
     * {@code +0000} suffix whatever precision was written —
     * {@code 2024-06-03T09:07:00.000000000+0000}. One owner beside
     * {@link #toEngineString} (the literal body) and {@link #toString}
     * (Pure's print form). */
    default String toEngineJson() {
        return switch (this) {
            case Year y -> y.toEngineString();
            case YearMonth ym -> ym.toEngineString();
            case StrictDate d -> d.toEngineString();
            case DateWithHour h -> String.format("%d-%02d-%02dT%02d:00:00.000000000+0000",
                    h.year(), h.month(), h.day(), h.hour());
            case DateWithMinute m -> String.format("%d-%02d-%02dT%02d:%02d:00.000000000+0000",
                    m.year(), m.month(), m.day(), m.hour(), m.minute());
            case DateWithSecond sec -> String.format("%d-%02d-%02dT%02d:%02d:%02d.000000000+0000",
                    sec.year(), sec.month(), sec.day(), sec.hour(), sec.minute(), sec.second());
            case DateWithSubsecond ss -> String.format("%d-%02d-%02dT%02d:%02d:%02d.%s+0000",
                    ss.year(), ss.month(), ss.day(), ss.hour(), ss.minute(), ss.second(),
                    (ss.subsecond() + "000000000").substring(0, 9));
        };
    }

    /**
     * Canonical engine-faithful spelling without the leading {@code %}
     * prefix and with timezone normalised to GMT (no TZ suffix).
     * Round-trip identity: {@code parse(x.toEngineString())} equals
     * {@code x}.
     */
    String toEngineString();

    /**
     * THE precision ladder (remediation T3.1) &mdash; one ordered scale
     * defined WITH the hierarchy. Ordinal order is the precision order, so
     * {@link #atLeast} is the only comparison consumers need; the
     * mutually-incompatible integer scales that re-derived this downstream
     * are gone.
     */
    enum Precision {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, SUBSECOND;

        public boolean atLeast(Precision p) {
            return ordinal() >= p.ordinal();
        }
    }

    /**
     * COMPONENT-RANGE validation, explicit — the records are dumb carriers
     * like the engine's ({@code DateFormat} stores digit runs; range
     * checking is the compiler's). {@link #parse(String)} validates by
     * default (the platform dialect's rule); the strict drop-in surface
     * parses with validation OFF because the engine accepts
     * {@code %2024-02-30} at parse (adversarial audit, oracle-verified).
     */
    default void validateComponents() {
        switch (this) {
            case Year y -> validateYear(y.year());
            case YearMonth ym -> {
                validateYear(ym.year());
                validateMonth(ym.month());
            }
            case StrictDate d -> {
                validateYear(d.year());
                validateMonth(d.month());
                validateDay(d.year(), d.month(), d.day());
            }
            case DateWithHour d -> {
                validateYear(d.year());
                validateMonth(d.month());
                validateDay(d.year(), d.month(), d.day());
                validateHour(d.hour());
            }
            case DateWithMinute d -> {
                validateYear(d.year());
                validateMonth(d.month());
                validateDay(d.year(), d.month(), d.day());
                validateHour(d.hour());
                validateMinute(d.minute());
            }
            case DateWithSecond d -> {
                validateYear(d.year());
                validateMonth(d.month());
                validateDay(d.year(), d.month(), d.day());
                validateHour(d.hour());
                validateMinute(d.minute());
                validateSecond(d.second());
            }
            case DateWithSubsecond d -> {
                validateYear(d.year());
                validateMonth(d.month());
                validateDay(d.year(), d.month(), d.day());
                validateHour(d.hour());
                validateMinute(d.minute());
                validateSecond(d.second());
                validateSubsecond(d.subsecond());
            }
        }
    }

    /** This literal's written precision. */
    default Precision precision() {
        return switch (this) {
            case Year ignored -> Precision.YEAR;
            case YearMonth ignored -> Precision.MONTH;
            case StrictDate ignored -> Precision.DAY;
            case DateWithHour ignored -> Precision.HOUR;
            case DateWithMinute ignored -> Precision.MINUTE;
            case DateWithSecond ignored -> Precision.SECOND;
            case DateWithSubsecond ignored -> Precision.SUBSECOND;
        };
    }

    /**
     * The DAY this literal names, as a {@link StrictDate} — structural
     * truncation of any day-carrying variant. Null for {@link Year} and
     * {@link YearMonth}, which name no day. (Replaces
     * {@code substring(0, 10)} surgery, which mis-truncated any year not
     * exactly four digits: remediation T1.2.)
     */
    default @com.legend.base.Nullable StrictDate strictDatePart() {
        return switch (this) {
            case StrictDate d -> d;
            case DateWithHour d -> new StrictDate(d.year(), d.month(), d.day());
            case DateWithMinute d -> new StrictDate(d.year(), d.month(), d.day());
            case DateWithSecond d -> new StrictDate(d.year(), d.month(), d.day());
            case DateWithSubsecond d -> new StrictDate(d.year(), d.month(), d.day());
            case Year ignored -> null;
            case YearMonth ignored -> null;
        };
    }

    // ---------------------------------------------------------------
    // Wire-value model (D-arc 2026-08-21: PureDateLiteral is THE
    // temporal wire carrier — sql/java.time temporals never escape the
    // fetch seam; these are the seam's constructors and the comparison
    // layer's ordering floor)
    // ---------------------------------------------------------------

    /** The fetch seam's DATE decode: a driver date is day-precision. */
    static StrictDate fromLocalDate(java.time.LocalDate d) {
        return new StrictDate(d.getYear(), d.getMonthValue(),
                d.getDayOfMonth());
    }

    /**
     * The fetch seam's TIMESTAMP decode: canonical-MINIMAL — second
     * precision, subseconds only when nonzero (trailing zeros
     * stripped). ADJUDICATED against the engine's two conventions
     * (2026-08-22): the engine's relational reads are 9-digit
     * ({@code fromSQLTimestamp %09d}) while its computed/interpreted
     * values carry derived precision — one decode cannot match both
     * SPELLINGS, so the wire carries the minimal canonical form and
     * the CORPUS harness compares temporal goldens BY INSTANT
     * (goldenEqualScalar — precision-blind by contract, mirroring the
     * engine's own cross-lane leniency), while PCT written-precision
     * functions ride the precision-faithful VARCHAR convention through
     * {@link #parse}.
     */
    static PureDateLiteral fromLocalDateTime(java.time.LocalDateTime t) {
        if (t.getNano() == 0) {
            return new DateWithSecond(t.getYear(), t.getMonthValue(),
                    t.getDayOfMonth(), t.getHour(), t.getMinute(),
                    t.getSecond());
        }
        String frac = String.format("%09d", t.getNano())
                .replaceFirst("0+$", "");
        return new DateWithSubsecond(t.getYear(), t.getMonthValue(),
                t.getDayOfMonth(), t.getHour(), t.getMinute(),
                t.getSecond(), frac);
    }

    /**
     * The EARLIEST instant this value names (its period's floor), as a
     * naive-UTC {@code LocalDateTime} — the comparison layer's ordering
     * key (pure sorts temporals by instant, P2-2). Subseconds order by
     * the fractional string zero-padded to nanos.
     */
    default java.time.LocalDateTime toInstantFloor() {
        return switch (this) {
            case Year y -> java.time.LocalDateTime.of(y.year(), 1, 1, 0, 0);
            case YearMonth ym ->
                    java.time.LocalDateTime.of(ym.year(), ym.month(), 1, 0, 0);
            case StrictDate d ->
                    java.time.LocalDateTime.of(d.year(), d.month(), d.day(), 0, 0);
            case DateWithHour d -> java.time.LocalDateTime.of(
                    d.year(), d.month(), d.day(), d.hour(), 0);
            case DateWithMinute d -> java.time.LocalDateTime.of(
                    d.year(), d.month(), d.day(), d.hour(), d.minute());
            case DateWithSecond d -> java.time.LocalDateTime.of(
                    d.year(), d.month(), d.day(), d.hour(), d.minute(),
                    d.second());
            case DateWithSubsecond d -> java.time.LocalDateTime.of(
                    d.year(), d.month(), d.day(), d.hour(), d.minute(),
                    d.second(), Integer.parseInt(
                            (d.subsecond() + "00000000").substring(0, 9)));
        };
    }

    // ---------------------------------------------------------------
    // Variants (each toString IS toEngineString — the wire value's text
    // is its pure spelling, so no stringification site can ever print
    // record syntax)
    // ---------------------------------------------------------------

    record Year(int year) implements PureDateLiteral {
        @Override public String toEngineString() { return Integer.toString(year); }

        @Override public String toString() { return toEngineString(); }
    }

    record YearMonth(int year, int month) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d", year, month);
        }

        @Override public String toString() { return toEngineString(); }
    }

    record StrictDate(int year, int month, int day) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02d", year, month, day);
        }

        @Override public String toString() { return toEngineString(); }
    }

    record DateWithHour(int year, int month, int day, int hour) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d", year, month, day, hour);
        }

        /** Pure's print form: time-bearing values carry the
         * GMT-normalized {@code +0000} suffix (H1 toString spec);
         * {@link #toEngineString} stays the literal body. */
        @Override public String toString() { return toEngineString() + "+0000"; }
    }

    record DateWithMinute(int year, int month, int day, int hour, int minute) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d", year, month, day, hour, minute);
        }

        /** Pure's print form: time-bearing values carry the
         * GMT-normalized {@code +0000} suffix (H1 toString spec);
         * {@link #toEngineString} stays the literal body. */
        @Override public String toString() { return toEngineString() + "+0000"; }
    }

    record DateWithSecond(int year, int month, int day, int hour, int minute, int second) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d:%02d",
                    year, month, day, hour, minute, second);
        }

        /** Pure's print form: time-bearing values carry the
         * GMT-normalized {@code +0000} suffix (H1 toString spec);
         * {@link #toEngineString} stays the literal body. */
        @Override public String toString() { return toEngineString() + "+0000"; }
    }

    record DateWithSubsecond(int year, int month, int day,
                             int hour, int minute, int second,
                             String subsecond) implements PureDateLiteral {
        @Override public String toEngineString() {
            return String.format("%d-%02d-%02dT%02d:%02d:%02d.%s",
                    year, month, day, hour, minute, second, subsecond);
        }

        /** Pure's print form: time-bearing values carry the
         * GMT-normalized {@code +0000} suffix (H1 toString spec);
         * {@link #toEngineString} stays the literal body. */
        @Override public String toString() { return toEngineString() + "+0000"; }
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
        return parse(source, true);
    }

    /** {@link #parse(String)} with component-range validation optional —
     *  see {@link #validateComponents()}. Structural/syntax errors always
     *  refuse. */
    static PureDateLiteral parse(String source, boolean validateComponents) {
        PureDateLiteral result = parseRaw(source);
        if (validateComponents) {
            result.validateComponents();
        }
        return result;
    }

    private static PureDateLiteral parseRaw(String source) {
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
            throw new IllegalArgumentException("Invalid month: " + month);
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

    private static void validateSubsecond(@com.legend.base.Nullable String subsecond) {
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
